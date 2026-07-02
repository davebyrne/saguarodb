use common::{ColumnInfo, DataType, DbError, Result, Row};
use executor::ExecutionResult;
use protocol::{PostgresCodec, ServerMessage};
use std::ops::ControlFlow;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::query::{STREAM_CHANNEL_CAPACITY, StreamMessage, StreamOutcome};

use super::{
    Session, TransactionState, command_complete_tag, encode_row, error_response,
    streamed_task_result, write_messages,
};

impl Session {
    pub(super) async fn run_query<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        sql: String,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        // A simple query clears any aborted extended-query sequence, matching
        // PostgreSQL. The transaction-block status (`self.tx`) is owned by the
        // explicit transaction lifecycle and is updated from the slot returned
        // below, not reset here.
        self.failed = false;
        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                // Reject due to shutdown: report the current (pre-statement)
                // status and close. The open transaction (if any) is aborted when
                // the session drops.
                write_messages(
                    stream,
                    codec,
                    &[
                        error_response(&err),
                        ServerMessage::ReadyForQuery(self.status_byte()),
                    ],
                )
                .await?;
                return Ok(ControlFlow::Break(()));
            }
        };
        let service = self.app.query_service.clone();
        let cancel = self.begin_cancelable();
        let session_sequences = self.session_sequences.clone();
        // A SELECT streams its rows through this bounded channel: the blocking
        // producer sends `Start` (columns) then `Rows` batches; this async task
        // drains them to the socket while the producer runs, giving TCP
        // backpressure and a bounded memory ceiling (`docs/specs/streaming.md` §4).
        let (row_tx, mut row_rx) = mpsc::channel::<StreamMessage>(STREAM_CHANNEL_CAPACITY);
        // Move the session's transaction slot AND default isolation into the blocking
        // task so the whole statement (including any owned write guard) runs on one
        // thread, then take them both back along with the outcome. The default is
        // threaded in/out like the slot so `SET SESSION CHARACTERISTICS` persists it
        // and a new `BEGIN` inherits it (`docs/specs/mvcc.md` §10 Milestone G2).
        let txn = self.txn.take();
        let default_isolation = self.default_isolation;
        // Do NOT await the task yet: it must run while we drain `row_rx`, or a
        // result larger than the channel would deadlock (producer blocked on a full
        // channel, consumer not yet reading).
        let task = tokio::task::spawn_blocking(move || {
            service.execute_simple_streamed(
                &sql,
                txn,
                default_isolation,
                &cancel,
                session_sequences,
                row_tx,
            )
        });

        // Stream rows to the socket as they arrive. `RowDescription` comes from the
        // first `Start` message; a socket-write or encode failure stops the drain
        // and is surfaced only after the task is joined (so the transaction slot is
        // never lost). The `SELECT n` count is taken from the producer's outcome,
        // not re-derived here.
        let mut write_err: Option<DbError> = None;
        while let Some(message) = row_rx.recv().await {
            let write_result = match message {
                StreamMessage::Start { columns } => {
                    write_messages(
                        stream,
                        codec,
                        &[ServerMessage::RowDescription {
                            columns,
                            formats: Vec::new(),
                        }],
                    )
                    .await
                }
                StreamMessage::Rows(rows) => match encode_data_rows(&rows) {
                    Ok(messages) => write_messages(stream, codec, &messages).await,
                    Err(err) => Err(err),
                },
            };
            if let Err(err) = write_result {
                write_err = Some(err);
                break;
            }
        }
        // Drop the receiver so the producer's next `blocking_send` fails fast if we
        // broke out early (matching the COPY-out driver).
        drop(row_rx);

        // `guard` (the in-flight-query guard) is held across the whole stream and
        // released per arm below: the normal arms drop it before the terminal
        // message; the COPY arms hand it to the streaming driver so the COPY keeps
        // counting as in-flight for its whole lifetime (graceful-shutdown).
        let (txn, default_isolation, outcome) =
            streamed_task_result(task.await, self.default_isolation);
        self.txn = txn;
        self.default_isolation = default_isolation;
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();

        // A socket-write or encode failure while streaming means the connection is
        // broken; surface it (closing the connection) rather than trying to write a
        // terminal message the client cannot receive.
        if let Some(err) = write_err {
            drop(guard);
            return Err(err);
        }

        match outcome {
            // A streamed SELECT: `RowDescription` and `DataRow`s were already
            // written above; finish with the command tag (the producer's
            // authoritative row count) and status.
            Ok(StreamOutcome::Streamed { count }) => {
                drop(guard);
                write_messages(
                    stream,
                    codec,
                    &[
                        ServerMessage::CommandComplete(format!("SELECT {count}")),
                        ServerMessage::ReadyForQuery(status),
                    ],
                )
                .await?
            }
            // COPY enters its sub-protocol instead of returning a finished result:
            // `BeginCopyIn` spawns the streaming insert and routes subsequent
            // CopyData; `BeginCopyOut` streams the table out inline. Both recompute
            // the transaction status themselves, so the `status` above is unused here.
            Ok(StreamOutcome::Direct(ExecutionResult::BeginCopyIn(job))) => {
                self.begin_copy_in(stream, codec, job, guard).await?
            }
            Ok(StreamOutcome::Direct(ExecutionResult::BeginCopyOut(job))) => {
                self.run_copy_out(stream, codec, job, guard).await?
            }
            // A non-streamed result (DML, DML RETURNING, or EXPLAIN); a `SELECT`
            // never lands here because reads stream when a sink is supplied.
            Ok(StreamOutcome::Direct(result)) => {
                drop(guard);
                write_execution_result(stream, codec, result, status).await?
            }
            Err(err) => {
                drop(guard);
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery(status)],
                )
                .await?
            }
        }
        Ok(ControlFlow::Continue(()))
    }
}

/// Encode a batch of result rows as `DataRow` messages in the simple-query
/// protocol's default (text) format.
fn encode_data_rows(rows: &[Row]) -> Result<Vec<ServerMessage>> {
    rows.iter()
        .map(|row| Ok(ServerMessage::DataRow(encode_row(row, &[])?)))
        .collect()
}

/// Write the result of a simple query, terminated by a `ReadyForQuery` carrying
/// the session's current transaction-status `status` byte.
async fn write_execution_result<S>(
    socket: &mut S,
    codec: &PostgresCodec,
    result: ExecutionResult,
    status: u8,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    match result {
        ExecutionResult::Query { columns, rows } => {
            let mut messages = Vec::with_capacity(rows.len() + 3);
            messages.push(ServerMessage::RowDescription {
                columns,
                formats: Vec::new(),
            });
            for row in rows {
                messages.push(ServerMessage::DataRow(encode_row(&row, &[])?));
            }
            let count = messages.len().saturating_sub(1);
            messages.push(ServerMessage::CommandComplete(format!("SELECT {count}")));
            messages.push(ServerMessage::ReadyForQuery(status));
            write_messages(socket, codec, &messages).await
        }
        ExecutionResult::Modified { command, count } => {
            let tag = command_complete_tag(&command, count);
            write_messages(
                socket,
                codec,
                &[
                    ServerMessage::CommandComplete(tag),
                    ServerMessage::ReadyForQuery(status),
                ],
            )
            .await
        }
        // A DML statement with RETURNING: a result set (RowDescription + DataRows)
        // followed by the DML command tag (e.g. `INSERT 0 n`), not `SELECT n`.
        ExecutionResult::ModifiedReturning {
            command,
            count,
            columns,
            rows,
        } => {
            let mut messages = Vec::with_capacity(rows.len() + 3);
            messages.push(ServerMessage::RowDescription {
                columns,
                formats: Vec::new(),
            });
            for row in rows {
                messages.push(ServerMessage::DataRow(encode_row(&row, &[])?));
            }
            messages.push(ServerMessage::CommandComplete(command_complete_tag(
                &command, count,
            )));
            messages.push(ServerMessage::ReadyForQuery(status));
            write_messages(socket, codec, &messages).await
        }
        ExecutionResult::Explanation { text } => {
            write_messages(
                socket,
                codec,
                &[
                    ServerMessage::RowDescription {
                        columns: vec![ColumnInfo {
                            name: "QUERY PLAN".to_string(),
                            data_type: DataType::Text,
                            table_id: None,
                            column_id: None,
                        }],
                        formats: Vec::new(),
                    },
                    ServerMessage::DataRow(vec![Some(text.into_bytes())]),
                    ServerMessage::CommandComplete("EXPLAIN".to_string()),
                    ServerMessage::ReadyForQuery(status),
                ],
            )
            .await
        }
        // COPY requests are intercepted by `run_query` and driven by the COPY
        // sub-protocol; they never reach the generic result writer.
        ExecutionResult::BeginCopyIn(_) | ExecutionResult::BeginCopyOut(_) => Err(
            DbError::internal("COPY result must be handled by the connection loop"),
        ),
    }
}
