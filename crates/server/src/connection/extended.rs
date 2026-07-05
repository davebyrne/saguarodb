use common::{ColumnInfo, DbError, PgType, Result, Row, Value};
use executor::ExecutionResult;
use protocol::{PostgresCodec, ServerMessage, StatementKind};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::query::{
    PreparedStatement, QuerySessionContext, STREAM_CHANNEL_CAPACITY, StreamMessage, StreamOutcome,
};

use super::{
    Portal, Session, TransactionState, command_complete_tag, encode_row, error_response,
    protocol_error, resolve_format, streamed_task_result, write_messages,
};

impl Session {
    pub(super) async fn run_execute<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        portal_name: &str,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let Some(portal) = self.portals.get(portal_name) else {
            self.failed = true;
            let err = protocol_error(format!("portal \"{portal_name}\" does not exist"));
            return write_messages(stream, codec, &[error_response(&err)]).await;
        };
        let statement = portal.statement.clone();
        let params = portal.params.clone();
        let result_formats = portal.result_formats.clone();

        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                self.failed = true;
                return write_messages(stream, codec, &[error_response(&err)]).await;
            }
        };
        let service = self.app.query_service.clone();
        let cancel = self.begin_cancelable();
        let session_sequences = self.session_sequences.clone();
        let session_info = self.session_info.clone();
        let session_gucs = self.session_gucs.clone();
        let session =
            QuerySessionContext::new(cancel, session_sequences, session_info, session_gucs);

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
                let result = service
                    .execute_prepared_cancelable_streamed(&statement, &params, session, row_tx);
                (None, default_isolation, result)
            })
        };

        // Drain rows to the socket as they arrive. Unlike the simple-query path, the
        // extended protocol's `RowDescription` came from `Describe`, so `Start` is
        // consumed without emitting one; `DataRow`s use the portal's result formats;
        // and no `ReadyForQuery` is sent here (`Sync` emits it).
        let mut write_err: Option<DbError> = None;
        // `RowDescription` already came from `Describe`, but keep `Start`'s columns
        // so each `Rows` batch can encode each value against its declared wire type
        // (the portal's result formats may be binary).
        let mut stream_columns: Vec<ColumnInfo> = Vec::new();
        while let Some(message) = row_rx.recv().await {
            let write_result = match message {
                StreamMessage::Start { columns } => {
                    stream_columns = columns;
                    Ok(())
                }
                StreamMessage::Rows(rows) => {
                    match encode_portal_rows(&rows, &stream_columns, &result_formats) {
                        Ok(messages) => write_messages(stream, codec, &messages).await,
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

        let (txn, default_isolation, outcome) =
            streamed_task_result(task.await, self.default_isolation);
        self.txn = txn;
        self.default_isolation = default_isolation;
        drop(guard);
        // Keep the reported transaction-block status in sync with the slot, so the
        // `ReadyForQuery` that `Sync` later emits carries the right `I`/`T`/`E` byte.
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));

        // A socket-write failure while streaming means the connection is broken;
        // surface it (closing the connection) rather than writing a terminal
        // message the client cannot receive.
        if let Some(err) = write_err {
            return Err(err);
        }

        match outcome {
            // A streamed SELECT: `DataRow`s were already written above; finish with
            // the DML-less `SELECT n` tag (no `RowDescription`, no `ReadyForQuery`).
            Ok(StreamOutcome::Streamed { count }) => {
                let mut messages = Vec::new();
                if let Some(message) = self.application_name_status_change() {
                    messages.push(message);
                }
                messages.push(ServerMessage::CommandComplete(format!("SELECT {count}")));
                write_messages(stream, codec, &messages).await
            }
            Ok(StreamOutcome::SessionReset(result)) => {
                self.prepared.clear();
                self.portals.clear();
                if let Some(message) = self.application_name_status_change() {
                    write_messages(stream, codec, &[message]).await?;
                }
                write_portal_result(stream, codec, result, &result_formats).await
            }
            Ok(StreamOutcome::Direct(result)) => {
                if let Some(message) = self.application_name_status_change() {
                    write_messages(stream, codec, &[message]).await?;
                }
                write_portal_result(stream, codec, result, &result_formats).await
            }
            Err(err) => {
                self.failed = true;
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }

    pub(super) fn process_parse(
        &mut self,
        name: String,
        query: String,
        param_type_oids: &[i32],
    ) -> Result<Vec<ServerMessage>> {
        let declared = param_type_oids
            .iter()
            .map(|oid| oid_to_pg_type(*oid))
            .collect::<Result<Vec<_>>>()?;
        let prepared = self.app.query_service.prepare_sql(&query, &declared)?;
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
        self.portals.insert(
            portal,
            Portal {
                statement: prepared,
                params,
                result_formats,
            },
        );
        Ok(vec![ServerMessage::BindComplete])
    }

    pub(super) fn process_describe(
        &self,
        kind: StatementKind,
        name: &str,
    ) -> Result<Vec<ServerMessage>> {
        match kind {
            StatementKind::Statement => {
                let prepared = self.prepared.get(name).ok_or_else(|| {
                    protocol_error(format!("prepared statement \"{name}\" does not exist"))
                })?;
                let oids = prepared.param_pg_types().iter().map(PgType::oid).collect();
                Ok(vec![
                    ServerMessage::ParameterDescription(oids),
                    row_description_or_no_data(prepared.result_columns(), &[]),
                ])
            }
            StatementKind::Portal => {
                let portal = self
                    .portals
                    .get(name)
                    .ok_or_else(|| protocol_error(format!("portal \"{name}\" does not exist")))?;
                Ok(vec![row_description_or_no_data(
                    portal.statement.result_columns(),
                    &portal.result_formats,
                )])
            }
        }
    }

    pub(super) fn process_close(
        &mut self,
        kind: StatementKind,
        name: &str,
    ) -> Result<Vec<ServerMessage>> {
        match kind {
            StatementKind::Statement => {
                self.prepared.remove(name);
            }
            StatementKind::Portal => {
                self.portals.remove(name);
            }
        }
        Ok(vec![ServerMessage::CloseComplete])
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
            Ok(messages) => write_messages(stream, codec, &messages).await,
            Err(err) => {
                self.failed = true;
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }
}

/// Map a client-declared parameter type OID to its wire type. Accepts the
/// distinct integer widths (int2/int4/int8) and character kinds
/// (text/varchar/bpchar); `0` is the unspecified marker (the server infers the
/// type). The wire type is remembered so `ParameterDescription` can echo the
/// exact OID the client declared, and its `DataType` drives binding/decoding.
fn oid_to_pg_type(oid: i32) -> Result<Option<PgType>> {
    let pg_type = match oid {
        0 => return Ok(None),
        16 => PgType::Bool,
        17 => PgType::Bytea,
        20 => PgType::Int8,
        21 => PgType::Int2,
        23 => PgType::Int4,
        25 => PgType::Text,
        700 => PgType::Float4,
        701 => PgType::Float8,
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
            Some(bytes) => protocol::decode_value(
                bytes,
                types[index].data_type(),
                resolve_format(param_formats, index),
            ),
        })
        .collect()
}

fn row_description_or_no_data(columns: Option<&[ColumnInfo]>, formats: &[i16]) -> ServerMessage {
    match columns {
        Some(columns) => ServerMessage::RowDescription {
            formats: resolve_formats(formats, columns.len()),
            columns: columns.to_vec(),
        },
        None => ServerMessage::NoData,
    }
}

fn resolve_formats(formats: &[i16], count: usize) -> Vec<i16> {
    (0..count)
        .map(|index| resolve_format(formats, index))
        .collect()
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
