use common::{ColumnInfo, DbError, PgType, Result, Row, SqlState, Value};
use executor::ExecutionResult;
use protocol::{PostgresCodec, ServerMessage, StatementKind};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::query::{
    CursorFetchStatus, PreparedStatement, QueryService, STREAM_CHANNEL_CAPACITY, StartedCursor,
    StreamMessage, StreamOutcome,
};

use super::{
    BoundPortal, Portal, Session, SuspendedPortal, TransactionState, apply_stream_consumer_cancel,
    command_complete_tag, encode_row, error_response, protocol_error, resolve_format,
    resolve_result_formats, streamed_task_result, wait_cancelable, wait_cancelable_write,
    write_messages, write_terminal_response,
};

struct LimitedBoundExecute {
    statement: Arc<PreparedStatement>,
    params: Vec<Value>,
    result_formats: Vec<i16>,
    max_rows: u64,
}

impl Session {
    pub(super) async fn run_execute<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        portal_name: &str,
        max_rows: i32,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let result = self
            .run_execute_inner(stream, codec, portal_name, max_rows)
            .await;
        self.stop_statement_timer().await;
        result
    }

    async fn run_execute_inner<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        portal_name: &str,
        max_rows: i32,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let Some(portal) = self.portals.get(portal_name) else {
            self.failed = true;
            let err = protocol_error(format!("portal \"{portal_name}\" does not exist"));
            return write_messages(stream, codec, &[error_response(&err)]).await;
        };

        if matches!(portal, Portal::Suspended(_)) {
            if self
                .reject_failed_transaction_execute(stream, codec)
                .await?
            {
                return Ok(());
            }
            let Some(Portal::Suspended(portal)) = self.portals.remove(portal_name) else {
                unreachable!("portal state changed after suspended match");
            };
            return self
                .run_suspended_execute(stream, codec, portal_name, portal, max_rows)
                .await;
        }

        let Portal::Bound(portal) = portal else {
            unreachable!("suspended portal handled above");
        };
        if max_rows > 0
            && self
                .app
                .query_service
                .prepared_supports_read_only_portal_suspension(&portal.statement, &portal.params)
        {
            if self
                .reject_failed_transaction_execute(stream, codec)
                .await?
            {
                return Ok(());
            }
            let Some(Portal::Bound(portal)) = self.portals.remove(portal_name) else {
                unreachable!("portal state changed after bound match");
            };
            return self
                .run_limited_bound_execute(
                    stream,
                    codec,
                    portal_name,
                    LimitedBoundExecute {
                        statement: portal.statement,
                        params: portal.params,
                        result_formats: portal.result_formats,
                        max_rows: max_rows as u64,
                    },
                )
                .await;
        }

        let statement = portal.statement.clone();
        let params = portal.params.clone();
        let result_formats = portal.result_formats.clone();
        let query_text = statement.sql().to_string();

        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                self.failed = true;
                return write_messages(stream, codec, &[error_response(&err)]).await;
            }
        };
        let service = self.app.query_service.clone();
        let cancel = self.cancel_token();
        let io_cancel = cancel.clone();
        let session = self.query_session_context(cancel);
        self.begin_activity(&query_text);

        // When an explicit transaction is open on this session, the extended-
        // protocol `Execute` participates in THAT transaction rather than starting
        // an independent autocommit unit. Routing both protocols through the one
        // transaction slot guarantees the session acquires the exclusive write
        // guard at most once: an open write transaction holds its single guard, and
        // an in-transaction `Execute` reuses it (or lazily acquires it once on the
        // first write) instead of re-acquiring it and self-deadlocking. A
        // transaction-control `Execute` (BEGIN/COMMIT/ROLLBACK) is also routed
        // through the session path even with no transaction open, so it drives
        // `self.txn` like a simple-query control statement. Otherwise (a data
        // statement with no open transaction), `Execute` stays a self-contained
        // autocommit unit.
        let route_through_session = self.txn.is_some()
            || statement.is_transaction_control()
            || statement.is_maintenance()
            || statement.is_session_config();

        // A SELECT streams its rows through this bounded channel while the producer
        // runs (`docs/specs/streaming.md` §4). Both routing branches return a
        // unified `(slot, default_isolation, StreamOutcome)` so the drain + join
        // below is shared; the autocommit branch simply carries `slot = None`.
        let (row_tx, mut row_rx) = mpsc::channel::<StreamMessage>(STREAM_CHANNEL_CAPACITY);
        let task = if route_through_session {
            // Move the transaction slot AND default isolation into the blocking task
            // (like the simple-query path) so the whole statement, including any owned
            // write guard, runs on one thread; take them both back with the outcome.
            let txn = self.txn.take();
            let default_isolation = self.default_isolation;
            tokio::task::spawn_blocking(move || {
                service.execute_prepared_in_session_streamed(
                    &statement,
                    &params,
                    txn,
                    default_isolation,
                    session,
                    row_tx,
                )
            })
        } else {
            let default_isolation = self.default_isolation;
            tokio::task::spawn_blocking(move || {
                let result = service.execute_prepared_cancelable_streamed(
                    &statement,
                    &params,
                    session,
                    default_isolation,
                    row_tx,
                );
                (None, default_isolation, result)
            })
        };

        // Drain rows to the socket as they arrive. Unlike the simple-query path, the
        // extended protocol's `RowDescription` came from `Describe`, so `Start` is
        // consumed without emitting one; `DataRow`s use the portal's result formats;
        // and no `ReadyForQuery` is sent here (`Sync` emits it).
        let mut write_err: Option<DbError> = None;
        let mut stream_cancel: Option<DbError> = None;
        // `RowDescription` already came from `Describe`, but keep `Start`'s columns
        // so each `Rows` batch can encode each value against its declared wire type
        // (the portal's result formats may be binary).
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
                    stream_columns = columns;
                    Ok(())
                }
                StreamMessage::Rows(rows) => {
                    match encode_portal_rows(&rows, &stream_columns, &result_formats) {
                        Ok(messages) => wait_cancelable_write(
                            io_cancel.as_ref(),
                            write_messages(stream, codec, &messages),
                        )
                        .await
                        .and_then(|result| result),
                        Err(err) => Err(err),
                    }
                }
            };
            if let Err(err) = write_result {
                write_err = Some(err);
                break;
            }
        }
        drop(row_rx);

        let (txn, default_isolation, mut outcome) =
            streamed_task_result(task.await, self.default_isolation);
        self.txn = txn;
        self.default_isolation = default_isolation;
        if let Some(err) = stream_cancel {
            apply_stream_consumer_cancel(&mut self.txn, &mut outcome, err);
        }
        if let Err(err) = io_cancel.check() {
            apply_stream_consumer_cancel(&mut self.txn, &mut outcome, err);
        }
        let transaction_holds_writer = self
            .txn
            .as_ref()
            .is_some_and(crate::query::Transaction::holds_write_guard);
        if !transaction_holds_writer {
            drop(guard);
        }
        // Keep the reported transaction-block status in sync with the slot, so the
        // `ReadyForQuery` that `Sync` later emits carries the right `I`/`T`/`E` byte.
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
        if self.txn.is_none() {
            self.close_transaction_scoped_suspended_portals();
            self.close_sql_cursors();
        }

        // A socket-write failure while streaming means the connection is broken;
        // surface it (closing the connection) rather than writing a terminal
        // message the client cannot receive.
        if let Some(err) = write_err {
            self.end_activity();
            return Err(err);
        }

        match outcome {
            // A streamed SELECT: `DataRow`s were already written above; finish with
            // the DML-less `SELECT n` tag (no `RowDescription`, no `ReadyForQuery`).
            Ok(StreamOutcome::Streamed { count }) => {
                self.end_activity();
                let mut messages = Vec::new();
                if let Some(message) = self.application_name_status_change() {
                    messages.push(message);
                }
                messages.push(ServerMessage::CommandComplete(format!("SELECT {count}")));
                wait_cancelable_write(io_cancel.as_ref(), write_messages(stream, codec, &messages))
                    .await
                    .and_then(|result| result)
            }
            Ok(StreamOutcome::SessionReset(result)) => {
                self.prepared.clear();
                self.portals.clear();
                self.end_activity();
                if let Some(message) = self.application_name_status_change() {
                    write_terminal_response(
                        io_cancel.as_ref(),
                        write_messages(stream, codec, &[message]),
                    )
                    .await?;
                }
                write_terminal_response(
                    io_cancel.as_ref(),
                    write_portal_result(stream, codec, result, &result_formats),
                )
                .await
            }
            Ok(StreamOutcome::Direct(result)) => {
                self.end_activity();
                if let Some(message) = self.application_name_status_change() {
                    wait_cancelable_write(
                        io_cancel.as_ref(),
                        write_messages(stream, codec, &[message]),
                    )
                    .await
                    .and_then(|result| result)?;
                }
                wait_cancelable_write(
                    io_cancel.as_ref(),
                    write_portal_result(stream, codec, result, &result_formats),
                )
                .await
                .and_then(|result| result)
            }
            Ok(StreamOutcome::Durable(result)) => {
                self.end_activity();
                if let Some(message) = self.application_name_status_change() {
                    write_terminal_response(
                        io_cancel.as_ref(),
                        write_messages(stream, codec, &[message]),
                    )
                    .await?;
                }
                write_terminal_response(
                    io_cancel.as_ref(),
                    write_portal_result(stream, codec, result, &result_formats),
                )
                .await
            }
            Ok(StreamOutcome::BeginCopyIn { .. } | StreamOutcome::BeginCopyOut { .. }) => {
                self.failed = true;
                self.end_activity();
                let err = DbError::internal("COPY outcome reached extended-protocol Execute");
                write_messages(stream, codec, &[error_response(&err)]).await
            }
            Err(err) => {
                self.failed = true;
                self.end_activity();
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }

    async fn reject_failed_transaction_execute<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
    ) -> Result<bool>
    where
        S: AsyncWrite + Unpin,
    {
        if TransactionState::from(crate::query::slot_status(&self.txn)) != TransactionState::Failed
        {
            return Ok(false);
        }
        self.failed = true;
        let err = DbError::execute(
            SqlState::InFailedSqlTransaction,
            "current transaction is aborted, commands ignored until end of transaction block",
        );
        write_messages(stream, codec, &[error_response(&err)]).await?;
        Ok(true)
    }

    async fn run_limited_bound_execute<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        portal_name: &str,
        execute: LimitedBoundExecute,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                self.failed = true;
                return write_messages(stream, codec, &[error_response(&err)]).await;
            }
        };
        let LimitedBoundExecute {
            statement,
            params,
            result_formats,
            max_rows,
        } = execute;
        let query_text = statement.sql().to_string();
        let default_isolation = self.default_isolation;
        let txn = self.txn.take();
        let transaction_scoped = txn.is_some();
        let service = self.app.query_service.clone();
        let cancel = self.cancel_token();
        let session = self.query_session_context(cancel);
        self.begin_activity(&query_text);

        let (txn, default_isolation, started) = QueryService::start_prepared_cursor(
            service,
            statement,
            params,
            txn,
            default_isolation,
            session,
        )
        .await;
        self.txn = txn;
        self.default_isolation = default_isolation;
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));

        let started = match started {
            Ok(started) => started,
            Err(err) => {
                self.failed = true;
                self.end_activity();
                let _guard = self.retain_query_guard_for_writer(guard);
                return write_messages(stream, codec, &[error_response(&err)]).await;
            }
        };

        let fetch = fetch_cursor_rows(
            stream,
            codec,
            &started,
            &result_formats,
            Some(max_rows),
            self.cancel.as_ref(),
        )
        .await;
        let _guard = self.retain_query_guard_for_writer(guard);
        match fetch {
            Ok(CursorFetchStatus::Suspended { count }) => {
                self.end_activity();
                self.portals.insert(
                    portal_name.to_string(),
                    Portal::Suspended(SuspendedPortal {
                        cursor: started.handle,
                        result_formats,
                        columns: started.columns,
                        query_text,
                        rows_sent: count,
                        transaction_scoped,
                    }),
                );
                wait_cancelable_write(
                    self.cancel.as_ref(),
                    write_messages(stream, codec, &[ServerMessage::PortalSuspended]),
                )
                .await
                .and_then(|result| result)
            }
            Ok(CursorFetchStatus::Exhausted { count }) => {
                self.end_activity();
                wait_cancelable_write(
                    self.cancel.as_ref(),
                    write_messages(
                        stream,
                        codec,
                        &[ServerMessage::CommandComplete(format!("SELECT {count}"))],
                    ),
                )
                .await
                .and_then(|result| result)
            }
            Err(PortalFetchError::Stream(err)) => Err(err),
            Err(PortalFetchError::Canceled(err) | PortalFetchError::Worker(err)) => {
                if let Some(txn) = self.txn.as_mut() {
                    txn.mark_failed();
                    self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
                }
                self.failed = true;
                self.end_activity();
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }

    async fn run_suspended_execute<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        portal_name: &str,
        mut portal: SuspendedPortal,
        max_rows: i32,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                self.failed = true;
                return write_messages(stream, codec, &[error_response(&err)]).await;
            }
        };
        self.begin_activity(&portal.query_text);
        let max_rows = if max_rows <= 0 {
            None
        } else {
            Some(max_rows as u64)
        };
        let fetch =
            fetch_suspended_rows(stream, codec, &portal, max_rows, self.cancel.as_ref()).await;
        let _guard = self.retain_query_guard_for_writer(guard);
        match fetch {
            Ok(CursorFetchStatus::Suspended { count }) => {
                self.end_activity();
                portal.rows_sent = portal.rows_sent.saturating_add(count);
                self.portals
                    .insert(portal_name.to_string(), Portal::Suspended(portal));
                wait_cancelable_write(
                    self.cancel.as_ref(),
                    write_messages(stream, codec, &[ServerMessage::PortalSuspended]),
                )
                .await
                .and_then(|result| result)
            }
            Ok(CursorFetchStatus::Exhausted { count }) => {
                self.end_activity();
                let total = portal.rows_sent.saturating_add(count);
                wait_cancelable_write(
                    self.cancel.as_ref(),
                    write_messages(
                        stream,
                        codec,
                        &[ServerMessage::CommandComplete(format!("SELECT {total}"))],
                    ),
                )
                .await
                .and_then(|result| result)
            }
            Err(PortalFetchError::Stream(err)) => Err(err),
            Err(PortalFetchError::Canceled(err) | PortalFetchError::Worker(err)) => {
                if let Some(txn) = self.txn.as_mut() {
                    txn.mark_failed();
                    self.tx = TransactionState::from(crate::query::slot_status(&self.txn));
                }
                self.failed = true;
                self.end_activity();
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }

    pub(super) async fn process_parse(
        &mut self,
        name: String,
        query: String,
        param_type_oids: &[i32],
    ) -> Result<Vec<ServerMessage>> {
        let declared = param_type_oids
            .iter()
            .map(|oid| oid_to_pg_type(*oid))
            .collect::<Result<Vec<_>>>()?;
        let service = self.app.query_service.clone();
        let cancel = self.cancel.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            service.prepare_sql_cancelable(&query, &declared, cancel.as_ref())
        })
        .await
        .map_err(|_| DbError::internal("statement preparation panicked"))??;
        self.cancel.check()?;
        self.prepared.insert(name, Arc::new(prepared));
        Ok(vec![ServerMessage::ParseComplete])
    }

    pub(super) fn process_bind(
        &mut self,
        portal: String,
        statement: &str,
        param_formats: &[i16],
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<Vec<ServerMessage>> {
        let prepared = self.prepared.get(statement).cloned().ok_or_else(|| {
            protocol_error(format!("prepared statement \"{statement}\" does not exist"))
        })?;
        let params = decode_bind_params(&prepared, param_formats, &params)?;
        self.cancel.check()?;
        self.portals.insert(
            portal,
            Portal::Bound(BoundPortal {
                statement: prepared,
                params,
                result_formats,
            }),
        );
        Ok(vec![ServerMessage::BindComplete])
    }

    pub(super) fn process_describe(
        &self,
        kind: StatementKind,
        name: &str,
    ) -> Result<Vec<ServerMessage>> {
        let messages = match kind {
            StatementKind::Statement => {
                let prepared = self.prepared.get(name).ok_or_else(|| {
                    protocol_error(format!("prepared statement \"{name}\" does not exist"))
                })?;
                let oids = prepared.param_pg_types().iter().map(PgType::oid).collect();
                vec![
                    ServerMessage::ParameterDescription(oids),
                    row_description_or_no_data(prepared.result_columns(), &[]),
                ]
            }
            StatementKind::Portal => {
                let portal = self
                    .portals
                    .get(name)
                    .ok_or_else(|| protocol_error(format!("portal \"{name}\" does not exist")))?;
                let message = match portal {
                    Portal::Bound(portal) => row_description_or_no_data(
                        portal.statement.result_columns(),
                        &portal.result_formats,
                    ),
                    Portal::Suspended(portal) => ServerMessage::RowDescription {
                        columns: portal.columns.clone(),
                        formats: resolve_result_formats(&portal.result_formats, &portal.columns),
                    },
                };
                vec![message]
            }
        };
        self.cancel.check()?;
        Ok(messages)
    }

    pub(super) fn process_close(&mut self, kind: StatementKind, name: &str) -> Vec<ServerMessage> {
        match kind {
            StatementKind::Statement => {
                self.prepared.remove(name);
            }
            StatementKind::Portal => {
                self.portals.remove(name);
            }
        }
        vec![ServerMessage::CloseComplete]
    }

    pub(super) async fn reply_or_fail<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        result: Result<Vec<ServerMessage>>,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        match result {
            Ok(messages) => wait_cancelable_write(
                self.cancel.as_ref(),
                write_messages(stream, codec, &messages),
            )
            .await
            .and_then(|result| result),
            Err(err) => {
                self.stop_statement_timer().await;
                self.failed = true;
                self.mark_current_transaction_failed();
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }
}

enum PortalFetchError {
    /// Cancellation between frames: safe to send a protocol ErrorResponse.
    Canceled(DbError),
    /// Encoding/socket-write failure, including cancellation that may leave a
    /// partial frame: close the connection without appending another frame.
    Stream(DbError),
    Worker(DbError),
}

async fn fetch_cursor_rows<S>(
    stream: &mut S,
    codec: &PostgresCodec,
    started: &StartedCursor,
    result_formats: &[i16],
    max_rows: Option<u64>,
    cancel: &common::QueryCancel,
) -> std::result::Result<CursorFetchStatus, PortalFetchError>
where
    S: AsyncWrite + Unpin,
{
    let (row_tx, row_rx) = mpsc::channel::<StreamMessage>(STREAM_CHANNEL_CAPACITY);
    let reply_rx = started
        .handle
        .start_fetch(max_rows, row_tx)
        .await
        .map_err(PortalFetchError::Worker)?;
    drain_cursor_rows(stream, codec, row_rx, result_formats, cancel).await?;
    reply_rx
        .await
        .map_err(|_| {
            PortalFetchError::Worker(DbError::internal(
                "cursor worker stopped before fetch completed",
            ))
        })?
        .map_err(PortalFetchError::Worker)
}

async fn fetch_suspended_rows<S>(
    stream: &mut S,
    codec: &PostgresCodec,
    portal: &SuspendedPortal,
    max_rows: Option<u64>,
    cancel: &common::QueryCancel,
) -> std::result::Result<CursorFetchStatus, PortalFetchError>
where
    S: AsyncWrite + Unpin,
{
    let (row_tx, row_rx) = mpsc::channel::<StreamMessage>(STREAM_CHANNEL_CAPACITY);
    let reply_rx = portal
        .cursor
        .start_fetch(max_rows, row_tx)
        .await
        .map_err(PortalFetchError::Worker)?;
    drain_cursor_rows(stream, codec, row_rx, &portal.result_formats, cancel).await?;
    reply_rx
        .await
        .map_err(|_| {
            PortalFetchError::Worker(DbError::internal(
                "cursor worker stopped before fetch completed",
            ))
        })?
        .map_err(PortalFetchError::Worker)
}

async fn drain_cursor_rows<S>(
    stream: &mut S,
    codec: &PostgresCodec,
    mut row_rx: mpsc::Receiver<StreamMessage>,
    result_formats: &[i16],
    cancel: &common::QueryCancel,
) -> std::result::Result<(), PortalFetchError>
where
    S: AsyncWrite + Unpin,
{
    let mut stream_columns: Vec<ColumnInfo> = Vec::new();
    loop {
        let Some(message) = wait_cancelable(cancel, row_rx.recv())
            .await
            .map_err(PortalFetchError::Canceled)?
        else {
            break;
        };
        match message {
            StreamMessage::Start { columns } => {
                stream_columns = columns;
            }
            StreamMessage::Rows(rows) => {
                let messages = encode_portal_rows(&rows, &stream_columns, result_formats)
                    .map_err(PortalFetchError::Stream)?;
                wait_cancelable_write(cancel, write_messages(stream, codec, &messages))
                    .await
                    .and_then(|result| result)
                    .map_err(PortalFetchError::Stream)?;
            }
        }
    }
    Ok(())
}

/// Map a client-declared parameter type OID to its wire type. Accepts the
/// exposed PostgreSQL wire identities; `0` is the unspecified marker (the server
/// infers the type). Text-backed catalog vector/array identities are accepted so
/// catalog-driven probes can feed `pg_proc.proargtypes` back into Parse, though
/// they still decode through the collapsed text storage type. The wire type is
/// remembered so `ParameterDescription` can echo the exact OID the client
/// declared, and its `DataType` drives binding/decoding.
fn oid_to_pg_type(oid: i32) -> Result<Option<PgType>> {
    let pg_type = match oid {
        0 => return Ok(None),
        16 => PgType::Bool,
        17 => PgType::Bytea,
        20 => PgType::Int8,
        21 => PgType::Int2,
        22 => PgType::Int2Vector,
        23 => PgType::Int4,
        25 => PgType::Text,
        26 => PgType::Oid,
        30 => PgType::OidVector,
        700 => PgType::Float4,
        701 => PgType::Float8,
        1005 => PgType::Int2Array,
        1028 => PgType::OidArray,
        1042 => PgType::Bpchar(None),
        1043 => PgType::Varchar(None),
        1082 => PgType::Date,
        1083 => PgType::Time,
        1114 => PgType::Timestamp,
        1184 => PgType::Timestamptz,
        1186 => PgType::Interval,
        1700 => PgType::Numeric {
            precision: None,
            scale: 0,
        },
        2950 => PgType::Uuid,
        other => {
            return Err(protocol_error(format!(
                "unsupported parameter type OID {other}"
            )));
        }
    };
    Ok(Some(pg_type))
}

fn decode_bind_params(
    prepared: &PreparedStatement,
    param_formats: &[i16],
    params: &[Option<Vec<u8>>],
) -> Result<Vec<Value>> {
    let types = prepared.param_pg_types();
    if params.len() != types.len() {
        return Err(protocol_error(format!(
            "bind message supplies {} parameter value(s), but the statement requires {}",
            params.len(),
            types.len()
        )));
    }
    params
        .iter()
        .enumerate()
        .map(|(index, raw)| match raw {
            None => Ok(Value::Null),
            Some(bytes) => protocol::decode_value_with_type(
                bytes,
                &types[index],
                resolve_format(param_formats, index),
            ),
        })
        .collect()
}

fn row_description_or_no_data(columns: Option<&[ColumnInfo]>, formats: &[i16]) -> ServerMessage {
    match columns {
        Some(columns) => ServerMessage::RowDescription {
            formats: resolve_result_formats(formats, columns),
            columns: columns.to_vec(),
        },
        None => ServerMessage::NoData,
    }
}

/// Encode a batch of streamed result rows as `DataRow` messages in the portal's
/// result formats, using `columns` for each value's declared wire type.
fn encode_portal_rows(
    rows: &[Row],
    columns: &[ColumnInfo],
    result_formats: &[i16],
) -> Result<Vec<ServerMessage>> {
    rows.iter()
        .map(|row| {
            Ok(ServerMessage::DataRow(encode_row(
                row,
                columns,
                result_formats,
            )?))
        })
        .collect()
}

/// Write the result of an extended-protocol `Execute`: data rows (in the
/// portal's result formats) and `CommandComplete`. Unlike the simple-query path
/// it sends no `RowDescription` (that comes from `Describe`) and no
/// `ReadyForQuery` (that comes from `Sync`).
async fn write_portal_result<S>(
    socket: &mut S,
    codec: &PostgresCodec,
    result: ExecutionResult,
    result_formats: &[i16],
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    match result {
        ExecutionResult::Query { columns, rows } => {
            let mut messages = Vec::with_capacity(rows.len() + 1);
            for row in &rows {
                messages.push(ServerMessage::DataRow(encode_row(
                    row,
                    &columns,
                    result_formats,
                )?));
            }
            messages.push(ServerMessage::CommandComplete(format!(
                "SELECT {}",
                rows.len()
            )));
            write_messages(socket, codec, &messages).await
        }
        ExecutionResult::Modified { command, count } => {
            write_messages(
                socket,
                codec,
                &[ServerMessage::CommandComplete(command_complete_tag(
                    &command, count,
                ))],
            )
            .await
        }
        // A DML statement with RETURNING streams its DataRows (the RowDescription
        // came from Describe) then the DML command tag, like the simple path but
        // without RowDescription/ReadyForQuery.
        ExecutionResult::ModifiedReturning {
            command,
            count,
            columns,
            rows,
        } => {
            let mut messages = Vec::with_capacity(rows.len() + 1);
            for row in &rows {
                messages.push(ServerMessage::DataRow(encode_row(
                    row,
                    &columns,
                    result_formats,
                )?));
            }
            messages.push(ServerMessage::CommandComplete(command_complete_tag(
                &command, count,
            )));
            write_messages(socket, codec, &messages).await
        }
        ExecutionResult::Explanation { text } => {
            write_messages(
                socket,
                codec,
                &[
                    ServerMessage::DataRow(vec![Some(text.into_bytes())]),
                    ServerMessage::CommandComplete("EXPLAIN".to_string()),
                ],
            )
            .await
        }
        // COPY is rejected in the extended query protocol before execution, so a
        // portal never yields a COPY request.
        ExecutionResult::BeginCopyIn(_) | ExecutionResult::BeginCopyOut(_) => Err(
            DbError::internal("COPY is not valid in the extended query protocol"),
        ),
    }
}
