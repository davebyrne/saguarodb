use common::{ColumnInfo, DataType, DbError, Result, Value};
use executor::ExecutionResult;
use protocol::{PostgresCodec, ServerMessage, StatementKind};
use std::sync::Arc;
use tokio::io::AsyncWrite;

use crate::query::PreparedStatement;

use super::{
    Portal, Session, TransactionState, command_complete_tag, encode_row, error_response,
    protocol_error, query_task_result, resolve_format, write_messages,
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
        let route_through_session =
            self.txn.is_some() || statement.is_transaction_control() || statement.is_maintenance();
        let result = if route_through_session {
            // Move the transaction slot AND default isolation into the blocking task
            // (like the simple-query path) so the whole statement, including any owned
            // write guard, runs on one thread; take them both back with the result. A
            // transaction-control `Execute` (e.g. BEGIN, or SET SESSION CHARACTERISTICS
            // routed via the session path) reads/updates the default here.
            let txn = self.txn.take();
            let default_isolation = self.default_isolation;
            let task = tokio::task::spawn_blocking(move || {
                service.execute_prepared_in_session_with_session_sequences(
                    &statement,
                    &params,
                    txn,
                    default_isolation,
                    &cancel,
                    session_sequences,
                )
            })
            .await;
            drop(guard);
            match task {
                Ok((txn, default_isolation, result)) => {
                    self.txn = txn;
                    self.default_isolation = default_isolation;
                    result
                }
                Err(join_err) => {
                    // The blocking task panicked and lost the transaction slot; the
                    // guard/registry entry for the lost txn cannot be recovered
                    // here. Treat the session as having no open transaction (the
                    // simple-query path makes the same best-effort choice).
                    self.txn = None;
                    Err(DbError::internal(format!("query task failed: {join_err}")))
                }
            }
        } else {
            let result = query_task_result(
                tokio::task::spawn_blocking(move || {
                    service.execute_prepared_cancelable_with_session_sequences(
                        &statement,
                        &params,
                        &cancel,
                        session_sequences,
                    )
                })
                .await,
            );
            drop(guard);
            result
        };

        // Keep the reported transaction-block status in sync with the slot, so the
        // `ReadyForQuery` that `Sync` later emits carries the right `I`/`T`/`E` byte.
        self.tx = TransactionState::from(crate::query::slot_status(&self.txn));

        match result {
            Ok(result) => write_portal_result(stream, codec, result, &result_formats).await,
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
            .map(|oid| oid_to_data_type(*oid))
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
                let oids = prepared
                    .param_types()
                    .iter()
                    .map(protocol::type_oid)
                    .collect();
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

fn oid_to_data_type(oid: i32) -> Result<Option<DataType>> {
    match oid {
        0 => Ok(None),
        20 => Ok(Some(DataType::Integer)),
        25 => Ok(Some(DataType::Text)),
        16 => Ok(Some(DataType::Boolean)),
        1082 => Ok(Some(DataType::Date)),
        1114 => Ok(Some(DataType::Timestamp)),
        17 => Ok(Some(DataType::Bytea)),
        2950 => Ok(Some(DataType::Uuid)),
        701 => Ok(Some(DataType::Double)),
        700 => Ok(Some(DataType::Real)),
        1083 => Ok(Some(DataType::Time)),
        1184 => Ok(Some(DataType::TimestampTz)),
        1186 => Ok(Some(DataType::Interval)),
        1700 => Ok(Some(DataType::Numeric {
            precision: None,
            scale: 0,
        })),
        other => Err(protocol_error(format!(
            "unsupported parameter type OID {other}"
        ))),
    }
}

fn decode_bind_params(
    prepared: &PreparedStatement,
    param_formats: &[i16],
    params: &[Option<Vec<u8>>],
) -> Result<Vec<Value>> {
    let types = prepared.param_types();
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
                types[index].clone(),
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
        ExecutionResult::Query { rows, .. } => {
            let mut messages = Vec::with_capacity(rows.len() + 1);
            for row in &rows {
                messages.push(ServerMessage::DataRow(encode_row(row, result_formats)?));
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
            rows,
            ..
        } => {
            let mut messages = Vec::with_capacity(rows.len() + 1);
            for row in &rows {
                messages.push(ServerMessage::DataRow(encode_row(row, result_formats)?));
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
