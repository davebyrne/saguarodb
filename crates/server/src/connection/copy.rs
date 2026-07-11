use common::{DbError, Result, SqlState};
use executor::CopyJob;
use protocol::{PostgresCodec, ServerMessage};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::query::{CopyInChunk, CopySnapshots};
use crate::shutdown::InFlightQueryGuard;

use super::{
    CopyInSession, Session, TransactionState, error_response, protocol_error, wait_cancelable,
    write_messages,
};

impl Session {
    /// Begin `COPY ... FROM STDIN`: send `CopyInResponse`, spawn the blocking
    /// insert task (which owns the transaction, moved out of the session), and
    /// record the copy-in state so subsequent `CopyData` is routed to it. Returns
    /// without waiting — finalization happens on `CopyDone`/`CopyFail`.
    pub(super) async fn begin_copy_in<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        job: CopyJob,
        snapshots: CopySnapshots,
        guard: InFlightQueryGuard,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let column_formats = vec![0i16; job.columns.len()];
        write_messages(
            stream,
            codec,
            &[ServerMessage::CopyInResponse {
                overall_format: 0,
                column_formats,
            }],
        )
        .await?;

        // A bounded channel gives TCP backpressure: when the insert task lags, the
        // forwarder's `send` awaits and the socket read stalls.
        let (sender, receiver) = mpsc::channel::<CopyInChunk>(64);
        let service = self.app.query_service.clone();
        let txn = self.txn.take();
        let cancel = self.cancel_token();
        let session = self.query_session_context(cancel);
        let task = tokio::task::spawn_blocking(move || {
            service.run_copy_in_stream(job, txn, session, snapshots, receiver)
        });
        self.copy_in = Some(CopyInSession {
            sender: Some(sender),
            task: Some(task),
            insert_failed: false,
            draining_after_cancel: false,
            _guard: guard,
        });
        Ok(())
    }

    /// Forward one `CopyData` payload to the insert task. If the task has exited
    /// early (a row failed), discard further data until the terminator.
    pub(super) async fn handle_copy_data<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        bytes: Vec<u8>,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let Some(copy) = self.copy_in.as_ref() else {
            return Err(protocol_error(
                "CopyData received outside of an active COPY",
            ));
        };
        if copy.draining_after_cancel {
            return Ok(());
        }
        let sender = copy
            .sender
            .as_ref()
            .expect("running COPY has an input sender")
            .clone();
        let insert_failed = copy.insert_failed;
        let send =
            wait_cancelable(self.cancel.as_ref(), sender.send(CopyInChunk::Chunk(bytes))).await;
        if send.is_err() {
            drop(sender);
            return self.cancel_copy_in(stream, codec).await;
        }
        if !insert_failed && send.expect("cancellation handled above").is_err() {
            // The receiver was dropped because the insert task exited on a row error.
            if let Some(copy) = self.copy_in.as_mut() {
                copy.insert_failed = true;
            }
        }
        Ok(())
    }

    /// Finalize a `COPY ... FROM STDIN` on `CopyDone` (`fail_message` `None`) or
    /// `CopyFail` (`Some(message)`): signal the task, await it, restore the session
    /// transaction, and reply. On any failure the inbound stream has already been
    /// drained to the terminator, so `ReadyForQuery` is emitted last.
    pub(super) async fn finish_copy_in<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        fail_message: Option<String>,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let copy = self
            .copy_in
            .take()
            .expect("finish_copy_in called with no active COPY");
        if copy.draining_after_cancel {
            self.stop_statement_timer().await;
            return write_messages(
                stream,
                codec,
                &[ServerMessage::ReadyForQuery(self.status_byte())],
            )
            .await;
        }
        let insert_failed = copy.insert_failed;
        let sender = copy.sender.expect("running COPY has an input sender");
        if !insert_failed {
            // Signal a clean end (`Done` → commit) or a client abort (`Fail`).
            let signal = if fail_message.is_some() {
                CopyInChunk::Fail
            } else {
                CopyInChunk::Done
            };
            let _ = sender.send(signal).await;
        }
        drop(sender);
        let task = copy.task.expect("running COPY has a worker task");
        let (txn, result) = match task.await {
            Ok(pair) => pair,
            Err(join_err) => (
                None,
                Err(DbError::internal(format!("COPY task failed: {join_err}"))),
            ),
        };
        self.txn = txn;
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();
        self.end_activity();

        let response = match result {
            Ok(count) => {
                write_messages(
                    stream,
                    codec,
                    &[
                        ServerMessage::CommandComplete(format!("COPY {count}")),
                        ServerMessage::ReadyForQuery(status),
                    ],
                )
                .await
            }
            Err(task_err) => {
                // A client CopyFail (with no prior insert error) reports the client's
                // message; otherwise the insert/row error.
                let err = match fail_message {
                    Some(message) if !insert_failed => DbError::execute(
                        SqlState::QueryCanceled,
                        format!("COPY from stdin failed: {message}"),
                    ),
                    _ => task_err,
                };
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery(status)],
                )
                .await
            }
        };
        self.stop_statement_timer().await;
        response
    }

    /// Stop a canceled COPY FROM worker, report the cancellation once, then retain a
    /// lightweight draining state until the client sends CopyDone/CopyFail.
    pub(super) async fn cancel_copy_in<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let (sender, task) = {
            let copy = self
                .copy_in
                .as_mut()
                .expect("cancel_copy_in called with no active COPY");
            if copy.draining_after_cancel {
                return Ok(());
            }
            (copy.sender.take(), copy.task.take())
        };

        if let Some(sender) = sender {
            let _ = sender.try_send(CopyInChunk::Fail);
            drop(sender);
        }
        let (txn, _) = match task.expect("running COPY has a worker task").await {
            Ok(pair) => pair,
            Err(_) => (None, Err(DbError::internal("timed-out COPY task failed"))),
        };
        self.txn = txn;
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        self.end_activity();
        self.copy_in
            .as_mut()
            .expect("COPY draining state remains installed")
            .draining_after_cancel = true;
        self.stop_statement_timer().await;

        let err = match self.cancel.check() {
            Err(err) => err,
            Ok(()) => DbError::execute(
                SqlState::QueryCanceled,
                "canceling statement due to statement timeout",
            ),
        };
        write_messages(stream, codec, &[error_response(&err)]).await
    }

    /// Run `COPY ... TO STDOUT` inline: send `CopyOutResponse`, stream rendered
    /// frames from the blocking producer to the socket, then `CopyDone` +
    /// `CommandComplete` (or `ErrorResponse` on failure, with no `CopyDone`).
    pub(super) async fn run_copy_out<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        job: CopyJob,
        snapshots: CopySnapshots,
        // Held for the COPY's lifetime so it counts as an in-flight query during the
        // streaming scan; dropped when this returns.
        _guard: InFlightQueryGuard,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let column_formats = vec![0i16; job.columns.len()];
        write_messages(
            stream,
            codec,
            &[ServerMessage::CopyOutResponse {
                overall_format: 0,
                column_formats,
            }],
        )
        .await?;

        let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(8);
        let service = self.app.query_service.clone();
        let txn = self.txn.take();
        let cancel = self.cancel_token();
        let io_cancel = cancel.clone();
        let session = self.query_session_context(cancel);
        let task = tokio::task::spawn_blocking(move || {
            service.run_copy_out_stream(job, txn, session, snapshots, frame_tx)
        });

        let mut write_err = None;
        let mut stream_cancel = None;
        loop {
            let frame = match wait_cancelable(io_cancel.as_ref(), frame_rx.recv()).await {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(err) => {
                    stream_cancel = Some(err);
                    break;
                }
            };
            if let Err(err) = wait_cancelable(
                io_cancel.as_ref(),
                write_messages(stream, codec, &[ServerMessage::CopyData(frame)]),
            )
            .await
            .and_then(|result| result)
            {
                write_err = Some(err);
                break;
            }
        }
        // Drop the receiver so the producer's next send fails fast if we broke out
        // early on a socket error.
        drop(frame_rx);

        let (txn, mut result) = match task.await {
            Ok(pair) => pair,
            Err(join_err) => (
                None,
                Err(DbError::internal(format!("COPY task failed: {join_err}"))),
            ),
        };
        self.txn = txn;
        if let Some(err) = stream_cancel {
            if let Some(txn) = self.txn.as_mut() {
                txn.mark_failed();
            }
            result = Err(err);
        }
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();

        if let Some(err) = write_err {
            self.end_activity();
            return Err(err);
        }
        self.end_activity();
        match result {
            Ok(count) => {
                write_messages(
                    stream,
                    codec,
                    &[
                        ServerMessage::CopyDone,
                        ServerMessage::CommandComplete(format!("COPY {count}")),
                        ServerMessage::ReadyForQuery(status),
                    ],
                )
                .await
            }
            // A producer error after CopyOutResponse: ErrorResponse, no CopyDone.
            Err(err) => {
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery(status)],
                )
                .await
            }
        }
    }
}
