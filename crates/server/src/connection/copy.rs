use common::{DbError, Result, SqlState};
use executor::CopyJob;
use protocol::{PostgresCodec, ServerMessage};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::query::CopyInChunk;
use crate::shutdown::InFlightQueryGuard;

use super::{
    CopyInSession, Session, TransactionState, error_response, protocol_error, write_messages,
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
        let cancel = self.begin_cancelable();
        let session = self.query_session_context(cancel);
        let task = tokio::task::spawn_blocking(move || {
            service.run_copy_in_stream(job, txn, session, receiver)
        });
        self.copy_in = Some(CopyInSession {
            sender,
            task,
            insert_failed: false,
            _guard: guard,
        });
        Ok(())
    }

    /// Forward one `CopyData` payload to the insert task. If the task has exited
    /// early (a row failed), discard further data until the terminator.
    pub(super) async fn handle_copy_data(&mut self, bytes: Vec<u8>) -> Result<()> {
        let Some(copy) = self.copy_in.as_mut() else {
            return Err(protocol_error(
                "CopyData received outside of an active COPY",
            ));
        };
        if !copy.insert_failed && copy.sender.send(CopyInChunk::Chunk(bytes)).await.is_err() {
            // The receiver was dropped because the insert task exited on a row error.
            copy.insert_failed = true;
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
        let insert_failed = copy.insert_failed;
        if !insert_failed {
            // Signal a clean end (`Done` → commit) or a client abort (`Fail`).
            let signal = if fail_message.is_some() {
                CopyInChunk::Fail
            } else {
                CopyInChunk::Done
            };
            let _ = copy.sender.send(signal).await;
        }
        drop(copy.sender);
        let (txn, result) = match copy.task.await {
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

        match result {
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
        }
    }

    /// Run `COPY ... TO STDOUT` inline: send `CopyOutResponse`, stream rendered
    /// frames from the blocking producer to the socket, then `CopyDone` +
    /// `CommandComplete` (or `ErrorResponse` on failure, with no `CopyDone`).
    pub(super) async fn run_copy_out<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        job: CopyJob,
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
        let cancel = self.begin_cancelable();
        let session = self.query_session_context(cancel);
        let task = tokio::task::spawn_blocking(move || {
            service.run_copy_out_stream(job, txn, session, frame_tx)
        });

        let mut write_err = None;
        while let Some(frame) = frame_rx.recv().await {
            if let Err(err) = write_messages(stream, codec, &[ServerMessage::CopyData(frame)]).await
            {
                write_err = Some(err);
                break;
            }
        }
        // Drop the receiver so the producer's next `blocking_send` fails fast if we
        // broke out early on a socket error.
        drop(frame_rx);

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
