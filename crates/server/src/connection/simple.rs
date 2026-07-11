use common::{ColumnInfo, DataType, DbError, Result, Row, SqlState};
use executor::ExecutionResult;
use parser::{FetchCount, Statement};
use protocol::{PostgresCodec, ServerMessage};
use std::ops::ControlFlow;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::query::{
    CursorFetchStatus, QueryService, STREAM_CHANNEL_CAPACITY, StreamMessage, StreamOutcome,
};
use crate::shutdown::InFlightQueryGuard;

use super::{
    Session, SqlCursor, TransactionState, apply_stream_consumer_cancel, command_complete_tag,
    encode_row, error_response, streamed_task_result, wait_cancelable, write_messages,
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
        self.start_statement_timer().await;
        let result = self.run_query_inner(stream, codec, sql).await;
        if self.copy_in.is_none() {
            self.stop_statement_timer().await;
        }
        result
    }

    async fn run_query_inner<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        sql: String,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        // The connection router only admits a simple Query when no extended
        // error is waiting for Sync. The transaction-block status (`self.tx`) is
        // owned by the explicit transaction lifecycle and is updated from the
        // slot returned below, not reset here.
        self.failed = false;
        self.close_autocommit_suspended_portals();
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
        match parse_statement_for_connection_routing(&sql) {
            Ok(Some(statement @ Statement::DeclareCursor { .. }))
            | Ok(Some(statement @ Statement::FetchCursor { .. }))
            | Ok(Some(statement @ Statement::CloseCursor { .. })) => {
                return self
                    .run_sql_cursor_statement(stream, codec, &sql, statement, guard)
                    .await;
            }
            Ok(Some(statement)) => {
                let closes_cursors_on_success =
                    matches!(statement, Statement::RollbackToSavepoint { .. });
                return self
                    .run_general_simple_query(stream, codec, sql, closes_cursors_on_success, guard)
                    .await;
            }
            Ok(None) => {}
            Err(err) => {
                self.mark_current_transaction_failed();
                drop(guard);
                write_messages(
                    stream,
                    codec,
                    &[
                        error_response(&err),
                        ServerMessage::ReadyForQuery(self.status_byte()),
                    ],
                )
                .await?;
                return Ok(ControlFlow::Continue(()));
            }
        }
        self.run_general_simple_query(stream, codec, sql, false, guard)
            .await
    }

    async fn run_general_simple_query<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        sql: String,
        closes_cursors_on_success: bool,
        guard: InFlightQueryGuard,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        let service = self.app.query_service.clone();
        let cancel = self.cancel_token();
        let io_cancel = cancel.clone();
        let session = self.query_session_context(cancel);
        self.begin_activity(&sql);
        // A SELECT streams its rows through this bounded channel: the blocking
        // producer sends `Start` (columns) then `Rows` batches; this async task
        // drains them to the socket while the producer runs, giving TCP
        // backpressure and a bounded memory ceiling (`docs/specs/streaming.md` §4).
        let (row_tx, mut row_rx) = mpsc::channel::<StreamMessage>(STREAM_CHANNEL_CAPACITY);
        // Move the session's transaction slot AND default isolation into the blocking
        // task so the whole statement (including any owned write guard) runs on one
        // thread, then take them both back along with the outcome. The default is
        // threaded in/out like the slot so committed default-isolation changes
        // persist and a new `BEGIN` inherits them (`docs/specs/mvcc.md` §10
        // Milestone G2).
        let txn = self.txn.take();
        let default_isolation = self.default_isolation;
        // Do NOT await the task yet: it must run while we drain `row_rx`, or a
        // result larger than the channel would deadlock (producer blocked on a full
        // channel, consumer not yet reading).
        let task = tokio::task::spawn_blocking(move || {
            service.execute_simple_streamed(&sql, txn, default_isolation, session, row_tx)
        });

        // Stream rows to the socket as they arrive. `RowDescription` comes from the
        // first `Start` message; a socket-write or encode failure stops the drain
        // and is surfaced only after the task is joined (so the transaction slot is
        // never lost). The `SELECT n` count is taken from the producer's outcome,
        // not re-derived here.
        let mut write_err: Option<DbError> = None;
        let mut stream_cancel: Option<DbError> = None;
        // The `Start` message carries the result columns; keep them so each `Rows`
        // batch can encode each value against its declared wire type.
        let mut stream_columns: Vec<ColumnInfo> = Vec::new();
        loop {
            let message = match wait_cancelable(io_cancel.as_ref(), row_rx.recv()).await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(err) => {
                    stream_cancel = Some(err);
                    break;
                }
            };
            let write_result = match message {
                StreamMessage::Start { columns } => {
                    stream_columns = columns.clone();
                    wait_cancelable(
                        io_cancel.as_ref(),
                        write_messages(
                            stream,
                            codec,
                            &[ServerMessage::RowDescription {
                                columns,
                                formats: Vec::new(),
                            }],
                        ),
                    )
                    .await
                    .and_then(|result| result)
                }
                StreamMessage::Rows(rows) => match encode_data_rows(&rows, &stream_columns) {
                    Ok(messages) => wait_cancelable(
                        io_cancel.as_ref(),
                        write_messages(stream, codec, &messages),
                    )
                    .await
                    .and_then(|result| result),
                    Err(err) => Err(err),
                },
            };
            if let Err(err) = write_result {
                write_err = Some(err);
                break;
            }
        }
        // Drop the receiver so the producer's next send fails fast if we broke out
        // early (matching the COPY-out driver).
        drop(row_rx);

        // `guard` (the in-flight-query guard) is held across the whole stream and
        // released per arm below: the normal arms drop it before the terminal
        // message; the COPY arms hand it to the streaming driver so the COPY keeps
        // counting as in-flight for its whole lifetime (graceful-shutdown).
        let (txn, default_isolation, mut outcome) =
            streamed_task_result(task.await, self.default_isolation);
        self.txn = txn;
        self.default_isolation = default_isolation;
        if let Some(err) = stream_cancel {
            apply_stream_consumer_cancel(&mut self.txn, &mut outcome, err);
        }
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let transaction_cleanup = self.txn.is_none() || successful_rollback_command(&outcome);
        let statement_cleanup = closes_cursors_on_success && outcome.is_ok();
        if transaction_cleanup || statement_cleanup {
            self.close_transaction_scoped_suspended_portals();
            self.close_sql_cursors();
        }
        let status = self.status_byte();

        // A socket-write or encode failure while streaming means the connection is
        // broken; surface it (closing the connection) rather than trying to write a
        // terminal message the client cannot receive.
        if let Some(err) = write_err {
            drop(guard);
            self.end_activity();
            return Err(err);
        }

        if let Some(message) = self.application_name_status_change() {
            if outcome.is_ok() {
                wait_cancelable(
                    io_cancel.as_ref(),
                    write_messages(stream, codec, &[message]),
                )
                .await
                .and_then(|result| result)?;
            } else {
                write_messages(stream, codec, &[message]).await?;
            }
        }

        match outcome {
            // A streamed SELECT: `RowDescription` and `DataRow`s were already
            // written above; finish with the command tag (the producer's
            // authoritative row count) and status.
            Ok(StreamOutcome::Streamed { count }) => {
                drop(guard);
                self.end_activity();
                wait_cancelable(
                    io_cancel.as_ref(),
                    write_messages(
                        stream,
                        codec,
                        &[
                            ServerMessage::CommandComplete(format!("SELECT {count}")),
                            ServerMessage::ReadyForQuery(status),
                        ],
                    ),
                )
                .await
                .and_then(|result| result)?
            }
            // COPY enters its sub-protocol instead of returning a finished result:
            // `BeginCopyIn` spawns the streaming insert and routes subsequent
            // CopyData; `BeginCopyOut` streams the table out inline. Both recompute
            // the transaction status themselves, so the `status` above is unused here.
            Ok(StreamOutcome::BeginCopyIn { job, snapshots }) => {
                self.begin_copy_in(stream, codec, job, snapshots, guard)
                    .await?
            }
            Ok(StreamOutcome::BeginCopyOut { job, snapshots }) => {
                self.run_copy_out(stream, codec, job, snapshots, guard)
                    .await?
            }
            Ok(StreamOutcome::SessionReset(result)) => {
                self.prepared.clear();
                self.portals.clear();
                self.close_sql_cursors();
                drop(guard);
                self.end_activity();
                wait_cancelable(
                    io_cancel.as_ref(),
                    write_execution_result(stream, codec, result, status),
                )
                .await
                .and_then(|result| result)?
            }
            // A non-streamed result (DML, DML RETURNING, or EXPLAIN); a `SELECT`
            // never lands here because reads stream when a sink is supplied.
            Ok(StreamOutcome::Direct(result) | StreamOutcome::Durable(result)) => {
                drop(guard);
                self.end_activity();
                wait_cancelable(
                    io_cancel.as_ref(),
                    write_execution_result(stream, codec, result, status),
                )
                .await
                .and_then(|result| result)?
            }
            Err(err) => {
                drop(guard);
                self.end_activity();
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

    async fn run_sql_cursor_statement<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        sql: &str,
        statement: Statement,
        guard: InFlightQueryGuard,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        match statement {
            Statement::DeclareCursor { name, query } => {
                self.declare_sql_cursor(stream, codec, sql, name, query, guard)
                    .await
            }
            Statement::FetchCursor { name, count } => {
                self.fetch_sql_cursor(stream, codec, name, count, guard)
                    .await
            }
            Statement::CloseCursor { name } => {
                self.close_sql_cursor(stream, codec, name, guard).await
            }
            _ => unreachable!("caller passes only SQL cursor statements"),
        }
    }

    async fn declare_sql_cursor<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        sql: &str,
        name: String,
        query: parser::Query,
        guard: InFlightQueryGuard,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        if let Err(err) = self.require_healthy_sql_cursor_transaction() {
            drop(guard);
            self.write_sql_cursor_error(stream, codec, err, false)
                .await?;
            return Ok(ControlFlow::Continue(()));
        }
        if self.cursors.contains_key(&name) {
            let err = DbError::execute(
                SqlState::DuplicateCursor,
                format!("cursor \"{name}\" already exists"),
            );
            drop(guard);
            self.write_sql_cursor_error(stream, codec, err, true)
                .await?;
            return Ok(ControlFlow::Continue(()));
        }
        let service = self.app.query_service.clone();
        let cancel = self.cancel_token();
        let session = self.query_session_context(cancel);
        self.begin_activity(sql);
        let txn = self
            .txn
            .take()
            .expect("DECLARE cursor requires an open transaction");
        let default_isolation = self.default_isolation;
        let (txn, default_isolation, started) =
            QueryService::start_sql_cursor(service, query, txn, default_isolation, session).await;
        self.txn = txn;
        self.default_isolation = default_isolation;
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        let status = self.status_byte();
        drop(guard);
        self.end_activity();

        match started {
            Ok(started) => {
                self.cursors.insert(
                    name,
                    SqlCursor {
                        handle: Some(started.handle),
                        columns: started.columns,
                        query_text: sql.to_string(),
                    },
                );
                wait_cancelable(
                    self.cancel.as_ref(),
                    write_messages(
                        stream,
                        codec,
                        &[
                            ServerMessage::CommandComplete("DECLARE CURSOR".to_string()),
                            ServerMessage::ReadyForQuery(status),
                        ],
                    ),
                )
                .await
                .and_then(|result| result)?;
                Ok(ControlFlow::Continue(()))
            }
            Err(err) => {
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery(status)],
                )
                .await?;
                Ok(ControlFlow::Continue(()))
            }
        }
    }

    async fn fetch_sql_cursor<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        name: String,
        count: FetchCount,
        guard: InFlightQueryGuard,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        if let Err(err) = self.require_healthy_sql_cursor_transaction() {
            drop(guard);
            self.write_sql_cursor_error(stream, codec, err, false)
                .await?;
            return Ok(ControlFlow::Continue(()));
        }
        let Some(mut cursor) = self.cursors.remove(&name) else {
            let err = DbError::execute(
                SqlState::InvalidCursorName,
                format!("cursor \"{name}\" does not exist"),
            );
            drop(guard);
            self.write_sql_cursor_error(stream, codec, err, true)
                .await?;
            return Ok(ControlFlow::Continue(()));
        };
        self.begin_activity(&cursor.query_text);
        let fetch =
            fetch_sql_cursor_rows(stream, codec, &mut cursor, count, self.cancel.as_ref()).await;
        drop(guard);
        self.end_activity();

        match fetch {
            Ok(fetched) => {
                self.cursors.insert(name, cursor);
                wait_cancelable(
                    self.cancel.as_ref(),
                    write_messages(
                        stream,
                        codec,
                        &[
                            ServerMessage::CommandComplete(format!("FETCH {fetched}")),
                            ServerMessage::ReadyForQuery(self.status_byte()),
                        ],
                    ),
                )
                .await
                .and_then(|result| result)?;
                Ok(ControlFlow::Continue(()))
            }
            Err(SqlCursorFetchError::Stream(err)) => Err(err),
            Err(SqlCursorFetchError::Canceled(err) | SqlCursorFetchError::Worker(err)) => {
                self.mark_current_transaction_failed();
                write_messages(
                    stream,
                    codec,
                    &[
                        error_response(&err),
                        ServerMessage::ReadyForQuery(self.status_byte()),
                    ],
                )
                .await?;
                Ok(ControlFlow::Continue(()))
            }
        }
    }

    async fn close_sql_cursor<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        name: String,
        guard: InFlightQueryGuard,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        if let Err(err) = self.require_healthy_sql_cursor_transaction() {
            drop(guard);
            self.write_sql_cursor_error(stream, codec, err, false)
                .await?;
            return Ok(ControlFlow::Continue(()));
        }
        if self.cursors.remove(&name).is_none() {
            let err = DbError::execute(
                SqlState::InvalidCursorName,
                format!("cursor \"{name}\" does not exist"),
            );
            drop(guard);
            self.write_sql_cursor_error(stream, codec, err, true)
                .await?;
            return Ok(ControlFlow::Continue(()));
        }
        drop(guard);
        wait_cancelable(
            self.cancel.as_ref(),
            write_messages(
                stream,
                codec,
                &[
                    ServerMessage::CommandComplete("CLOSE CURSOR".to_string()),
                    ServerMessage::ReadyForQuery(self.status_byte()),
                ],
            ),
        )
        .await
        .and_then(|result| result)?;
        Ok(ControlFlow::Continue(()))
    }

    fn require_healthy_sql_cursor_transaction(&self) -> Result<()> {
        match &self.txn {
            None => Err(DbError::execute(
                SqlState::NoActiveSqlTransaction,
                "SQL cursors require an explicit transaction block",
            )),
            Some(txn) if txn.is_failed() => Err(DbError::execute(
                SqlState::InFailedSqlTransaction,
                "current transaction is aborted, commands ignored until end of transaction block",
            )),
            Some(_) => Ok(()),
        }
    }

    async fn write_sql_cursor_error<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        err: DbError,
        fail_transaction: bool,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        if fail_transaction {
            self.mark_current_transaction_failed();
        }
        write_messages(
            stream,
            codec,
            &[
                error_response(&err),
                ServerMessage::ReadyForQuery(self.status_byte()),
            ],
        )
        .await
    }

    pub(super) fn mark_current_transaction_failed(&mut self) {
        if let Some(txn) = self.txn.as_mut() {
            txn.mark_failed();
            self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        }
    }
}

enum SqlCursorFetchError {
    /// Cancellation observed while waiting between protocol frames. The socket
    /// remains framed, so the statement can report 57014 and keep the connection.
    Canceled(DbError),
    /// Encoding/socket-write failure, including cancellation that may have
    /// interrupted a partially written frame. The connection must close.
    Stream(DbError),
    Worker(DbError),
}

fn parse_statement_for_connection_routing(sql: &str) -> Result<Option<Statement>> {
    if !routing_first_keyword_matches(sql, &["declare", "fetch", "close", "rollback"]) {
        return Ok(None);
    }
    let statement = parser::parse(sql)?;
    match statement {
        Statement::DeclareCursor { .. }
        | Statement::FetchCursor { .. }
        | Statement::CloseCursor { .. }
        | Statement::RollbackToSavepoint { .. } => Ok(Some(statement)),
        _ => Ok(None),
    }
}

fn routing_first_keyword_matches(sql: &str, expected: &[&str]) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0c => index += 1,
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && !matches!(bytes[index], b'\n' | b'\r') {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                if index + 1 >= bytes.len() {
                    return false;
                }
                index += 2;
            }
            ch if ch.is_ascii_alphabetic() || ch == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                return expected
                    .iter()
                    .any(|candidate| sql[start..index].eq_ignore_ascii_case(candidate));
            }
            _ => return false,
        }
    }
    false
}

async fn fetch_sql_cursor_rows<S>(
    stream: &mut S,
    codec: &PostgresCodec,
    cursor: &mut SqlCursor,
    count: FetchCount,
    cancel: &common::QueryCancel,
) -> std::result::Result<u64, SqlCursorFetchError>
where
    S: AsyncWrite + Unpin,
{
    let max_rows = match count {
        FetchCount::One => Some(1),
        FetchCount::Count(count) => Some(count),
        FetchCount::All => None,
    };
    if cursor.handle.is_none() {
        wait_cancelable(
            cancel,
            write_messages(
                stream,
                codec,
                &[ServerMessage::RowDescription {
                    columns: cursor.columns.clone(),
                    formats: Vec::new(),
                }],
            ),
        )
        .await
        .and_then(|result| result)
        .map_err(SqlCursorFetchError::Stream)?;
        return Ok(0);
    }

    let handle = cursor
        .handle
        .take()
        .expect("checked cursor handle is present");
    let (row_tx, row_rx) = mpsc::channel::<StreamMessage>(STREAM_CHANNEL_CAPACITY);
    let reply_rx = handle
        .start_fetch(max_rows, row_tx)
        .await
        .map_err(SqlCursorFetchError::Worker)?;
    wait_cancelable(
        cancel,
        write_messages(
            stream,
            codec,
            &[ServerMessage::RowDescription {
                columns: cursor.columns.clone(),
                formats: Vec::new(),
            }],
        ),
    )
    .await
    .and_then(|result| result)
    .map_err(SqlCursorFetchError::Stream)?;
    drain_sql_cursor_rows(stream, codec, row_rx, &cursor.columns, cancel).await?;
    let status = reply_rx
        .await
        .map_err(|_| DbError::internal("cursor worker stopped before fetch completed"))
        .and_then(|result| result)
        .map_err(SqlCursorFetchError::Worker)?;
    match status {
        CursorFetchStatus::Suspended { count } => {
            cursor.handle = Some(handle);
            Ok(count)
        }
        CursorFetchStatus::Exhausted { count } => Ok(count),
    }
}

async fn drain_sql_cursor_rows<S>(
    stream: &mut S,
    codec: &PostgresCodec,
    mut row_rx: mpsc::Receiver<StreamMessage>,
    columns: &[ColumnInfo],
    cancel: &common::QueryCancel,
) -> std::result::Result<(), SqlCursorFetchError>
where
    S: AsyncWrite + Unpin,
{
    loop {
        let Some(message) = wait_cancelable(cancel, row_rx.recv())
            .await
            .map_err(SqlCursorFetchError::Canceled)?
        else {
            break;
        };
        match message {
            StreamMessage::Start { .. } => {}
            StreamMessage::Rows(rows) => {
                let messages =
                    encode_data_rows(&rows, columns).map_err(SqlCursorFetchError::Stream)?;
                wait_cancelable(cancel, write_messages(stream, codec, &messages))
                    .await
                    .and_then(|result| result)
                    .map_err(SqlCursorFetchError::Stream)?;
            }
        }
    }
    Ok(())
}

fn successful_rollback_command(outcome: &Result<StreamOutcome>) -> bool {
    matches!(
        outcome,
        Ok(StreamOutcome::Direct(ExecutionResult::Modified { command, .. })
            | StreamOutcome::Durable(ExecutionResult::Modified { command, .. }))
            if command == "ROLLBACK"
    )
}

/// Encode a batch of result rows as `DataRow` messages in the simple-query
/// protocol's default (text) format, using `columns` for each value's declared
/// wire type.
fn encode_data_rows(rows: &[Row], columns: &[ColumnInfo]) -> Result<Vec<ServerMessage>> {
    rows.iter()
        .map(|row| Ok(ServerMessage::DataRow(encode_row(row, columns, &[])?)))
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
            let mut data_rows = Vec::with_capacity(rows.len());
            for row in &rows {
                data_rows.push(ServerMessage::DataRow(encode_row(row, &columns, &[])?));
            }
            let count = data_rows.len();
            let mut messages = Vec::with_capacity(count + 3);
            messages.push(ServerMessage::RowDescription {
                columns,
                formats: Vec::new(),
            });
            messages.extend(data_rows);
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
            let mut data_rows = Vec::with_capacity(rows.len());
            for row in &rows {
                data_rows.push(ServerMessage::DataRow(encode_row(row, &columns, &[])?));
            }
            let mut messages = Vec::with_capacity(data_rows.len() + 3);
            messages.push(ServerMessage::RowDescription {
                columns,
                formats: Vec::new(),
            });
            messages.extend(data_rows);
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
                            pg_type: None,
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

#[cfg(test)]
mod tests {
    use common::{CancelReason, QueryCancel, SqlState};

    use super::*;

    #[tokio::test]
    async fn cursor_receive_cancellation_is_protocol_recoverable() {
        let (row_tx, row_rx) = mpsc::channel(1);
        let cancel = QueryCancel::new();
        cancel.request(CancelReason::StatementTimeout);
        let mut stream = tokio::io::sink();

        let err = drain_sql_cursor_rows(&mut stream, &PostgresCodec::new(), row_rx, &[], &cancel)
            .await
            .unwrap_err();

        drop(row_tx);
        assert!(matches!(
            err,
            SqlCursorFetchError::Canceled(err) if err.code == SqlState::QueryCanceled
        ));
    }
}
