use common::{ColumnInfo, DataType, DbError, Result};
use executor::ExecutionResult;
use protocol::{PostgresCodec, ServerMessage};
use std::ops::ControlFlow;
use tokio::io::AsyncWrite;

use super::{
    Session, TransactionState, command_complete_tag, encode_row, error_response, write_messages,
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
        // Move the session's transaction slot AND default isolation into the blocking
        // task so the whole statement (including any owned write guard) runs on one
        // thread, then take them both back along with the result. The default is
        // threaded in/out like the slot so `SET SESSION CHARACTERISTICS` persists it
        // and a new `BEGIN` inherits it (`docs/specs/mvcc.md` §10 Milestone G2).
        let txn = self.txn.take();
        let default_isolation = self.default_isolation;
        let task = tokio::task::spawn_blocking(move || {
            service.execute_simple_with_session_sequences(
                &sql,
                txn,
                default_isolation,
                &cancel,
                session_sequences,
            )
        })
        .await;
        // `guard` (the in-flight-query guard) is dropped per result arm below: the
        // normal arms drop it before writing the response (the query work is done);
        // the COPY arms hand it to the streaming driver so the COPY keeps counting
        // as in-flight for its whole lifetime (graceful-shutdown coordination).
        let result = match task {
            Ok((txn, default_isolation, result)) => {
                self.txn = txn;
                self.default_isolation = default_isolation;
                result
            }
            Err(join_err) => {
                // The blocking task panicked and lost the transaction slot. Treat
                // the connection as having no open transaction (the panic firewall
                // surfaces an internal error); the guard/registry entry for a lost
                // txn cannot be recovered here, so this is best-effort. The session
                // default is left as-is (it never moved into a committed effect).
                self.txn = None;
                Err(DbError::internal(format!("query task failed: {join_err}")))
            }
        };
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();
        match result {
            // COPY enters its sub-protocol instead of returning a finished result:
            // `BeginCopyIn` spawns the streaming insert and routes subsequent
            // CopyData; `BeginCopyOut` streams the table out inline. Both recompute
            // the transaction status themselves, so the `status` above is unused here.
            Ok(ExecutionResult::BeginCopyIn(job)) => {
                self.begin_copy_in(stream, codec, job, guard).await?
            }
            Ok(ExecutionResult::BeginCopyOut(job)) => {
                self.run_copy_out(stream, codec, job, guard).await?
            }
            Ok(result) => {
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
