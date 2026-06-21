use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use common::{ColumnInfo, DataType, DbError, Result, Row, SqlState, Value};
use executor::ExecutionResult;
use protocol::{
    ClientMessage, ConnectionState, PostgresCodec, PostgresConnectionState, ProtocolCodec,
    ServerMessage, StatementKind,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::app::AppState;
use crate::query::PreparedStatement;

/// Accept a connection, run optional SSL/GSS negotiation, then serve the
/// protocol over the resulting (plaintext or TLS) stream.
///
/// Before startup a client may send a `GSSENCRequest` and/or an `SSLRequest`.
/// GSSAPI transport encryption is unsupported, so it is declined with a single
/// `N` byte and negotiation continues. For an `SSLRequest`, when the server has
/// TLS configured it replies `SslAccepted` (`S`) and upgrades the socket;
/// otherwise it replies `SslRejected` (`N`) and the client continues in
/// plaintext. A client that opens with a `StartupMessage` is served in plaintext
/// directly.
pub async fn handle_connection(mut socket: TcpStream, app: Arc<AppState>) -> Result<()> {
    let mut codec = PostgresCodec::new();
    let mut buf = [0; 8192];

    loop {
        // Read until the first client message of this negotiation step is
        // buffered. Looping keeps negotiation correct even when the small
        // request packet is split across reads.
        let initial = loop {
            let read = socket
                .read(&mut buf)
                .await
                .map_err(|err| DbError::io(format!("failed to read socket: {err}")))?;
            if read == 0 {
                return Ok(());
            }
            match codec.decode(&buf[..read]) {
                Ok(messages) if !messages.is_empty() => break messages,
                Ok(_) => continue,
                Err(err) => {
                    write_messages(
                        &mut socket,
                        &codec,
                        &[error_response(&err), ServerMessage::ReadyForQuery],
                    )
                    .await?;
                    return Ok(());
                }
            }
        };

        // A negotiation request must arrive alone; the client waits for the
        // single-byte reply before sending anything else.
        let is_negotiation = matches!(
            initial.first(),
            Some(ClientMessage::GssEncRequest | ClientMessage::SslRequest)
        );
        if is_negotiation && initial.len() > 1 {
            let err = DbError::protocol(
                SqlState::SyntaxError,
                "client sent data before completing connection negotiation",
            );
            write_messages(
                &mut socket,
                &codec,
                &[error_response(&err), ServerMessage::ReadyForQuery],
            )
            .await?;
            return Ok(());
        }

        match initial.first() {
            // GSSAPI transport encryption is unsupported: decline with the single
            // `N` byte (same as SSL rejection) and keep negotiating, since the
            // client typically follows with an SSLRequest or StartupMessage.
            Some(ClientMessage::GssEncRequest) => {
                write_messages(&mut socket, &codec, &[ServerMessage::SslRejected]).await?;
                continue;
            }
            Some(ClientMessage::SslRequest) => {
                // Clone the acceptor (a cheap `Arc`) so `app` stays free to move
                // into `serve`.
                return match app.components.tls.clone() {
                    Some(acceptor) => {
                        write_messages(&mut socket, &codec, &[ServerMessage::SslAccepted]).await?;
                        socket.flush().await.map_err(|err| {
                            DbError::io(format!("failed to flush SSL response: {err}"))
                        })?;
                        let tls = acceptor
                            .accept(socket)
                            .await
                            .map_err(|err| DbError::io(format!("TLS handshake failed: {err}")))?;
                        // Serve the encrypted session with a fresh codec: only the
                        // lone SSLRequest is legitimate before the handshake, so a
                        // new decode buffer ensures no stray pre-handshake
                        // plaintext can bleed into the decrypted stream.
                        serve(tls, PostgresCodec::new(), app, Vec::new()).await
                    }
                    None => {
                        write_messages(&mut socket, &codec, &[ServerMessage::SslRejected]).await?;
                        serve(socket, codec, app, Vec::new()).await
                    }
                };
            }
            _ => {}
        }

        return serve(socket, codec, app, initial).await;
    }
}

/// A bound portal: a prepared statement plus its parameter values and the
/// requested result column formats.
struct Portal {
    statement: Arc<PreparedStatement>,
    params: Vec<Value>,
    result_formats: Vec<i16>,
}

/// Per-connection state for the simple and extended query protocols.
struct Session {
    app: Arc<AppState>,
    state: PostgresConnectionState,
    prepared: HashMap<String, Arc<PreparedStatement>>,
    portals: HashMap<String, Portal>,
    /// Set after an error inside an extended-query sequence; subsequent extended
    /// messages are skipped until the client sends `Sync`.
    failed: bool,
}

/// Drive the protocol over an established stream, starting with any messages
/// already decoded during negotiation. Generic over the stream type so it serves
/// both plaintext `TcpStream` and TLS-upgraded connections.
async fn serve<S>(
    mut stream: S,
    mut codec: PostgresCodec,
    app: Arc<AppState>,
    mut batch: Vec<ClientMessage>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = Session::new(app);
    let mut buf = [0; 8192];

    loop {
        for message in batch {
            if session
                .handle(&mut stream, &codec, message)
                .await?
                .is_break()
            {
                return Ok(());
            }
        }

        let read = stream
            .read(&mut buf)
            .await
            .map_err(|err| DbError::io(format!("failed to read socket: {err}")))?;
        if read == 0 {
            return Ok(());
        }
        batch = match codec.decode(&buf[..read]) {
            Ok(messages) => messages,
            Err(err) => {
                write_messages(
                    &mut stream,
                    &codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery],
                )
                .await?;
                return Ok(());
            }
        };
    }
}

impl Session {
    fn new(app: Arc<AppState>) -> Self {
        Self {
            app,
            state: PostgresConnectionState::new(),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            failed: false,
        }
    }

    /// Handle one decoded client message. Returns `Break` when the connection
    /// should close (Terminate, or a shutdown-rejected simple query).
    async fn handle<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        message: ClientMessage,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        match message {
            ClientMessage::Query(sql) => return self.run_query(stream, codec, sql).await,
            ClientMessage::Sync => {
                self.failed = false;
                write_messages(stream, codec, &[ServerMessage::ReadyForQuery]).await?;
            }
            ClientMessage::Flush => {
                stream
                    .flush()
                    .await
                    .map_err(|err| DbError::io(format!("failed to flush socket: {err}")))?;
            }
            ClientMessage::Parse {
                name,
                query,
                param_types,
            } if !self.failed => {
                let result = self.process_parse(name, query, &param_types);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            } if !self.failed => {
                let result =
                    self.process_bind(portal, &statement, &param_formats, params, result_formats);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Describe { kind, name } if !self.failed => {
                let result = self.process_describe(kind, &name);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Close { kind, name } if !self.failed => {
                let result = self.process_close(kind, &name);
                self.reply_or_fail(stream, codec, result).await?;
            }
            ClientMessage::Execute { portal, .. } if !self.failed => {
                self.run_execute(stream, codec, &portal).await?;
            }
            // Extended messages while in the failed state are skipped until Sync.
            ClientMessage::Parse { .. }
            | ClientMessage::Bind { .. }
            | ClientMessage::Describe { .. }
            | ClientMessage::Close { .. }
            | ClientMessage::Execute { .. } => {}
            other => {
                for response in self.state.handle_message(other)? {
                    write_messages(stream, codec, &[response]).await?;
                }
                if self.state.is_terminated() {
                    return Ok(ControlFlow::Break(()));
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn run_query<S>(
        &mut self,
        stream: &mut S,
        codec: &PostgresCodec,
        sql: String,
    ) -> Result<ControlFlow<()>>
    where
        S: AsyncWrite + Unpin,
    {
        // A simple query is a transaction boundary that clears any aborted
        // extended-query sequence, matching PostgreSQL.
        self.failed = false;
        let guard = match self.app.components.shutdown.begin_query() {
            Ok(guard) => guard,
            Err(err) => {
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery],
                )
                .await?;
                return Ok(ControlFlow::Break(()));
            }
        };
        let service = self.app.query_service.clone();
        let result =
            query_task_result(tokio::task::spawn_blocking(move || service.execute_sql(&sql)).await);
        drop(guard);
        match result {
            Ok(result) => write_execution_result(stream, codec, result).await?,
            Err(err) => {
                write_messages(
                    stream,
                    codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery],
                )
                .await?
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn run_execute<S>(
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
        let result = query_task_result(
            tokio::task::spawn_blocking(move || service.execute_prepared(&statement, &params))
                .await,
        );
        drop(guard);
        match result {
            Ok(result) => write_portal_result(stream, codec, result, &result_formats).await,
            Err(err) => {
                self.failed = true;
                write_messages(stream, codec, &[error_response(&err)]).await
            }
        }
    }

    fn process_parse(
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

    fn process_bind(
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

    fn process_describe(&self, kind: StatementKind, name: &str) -> Result<Vec<ServerMessage>> {
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

    fn process_close(&mut self, kind: StatementKind, name: &str) -> Result<Vec<ServerMessage>> {
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

    async fn reply_or_fail<S>(
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

/// Resolve a PostgreSQL format-code array (`0` codes = all text, `1` code =
/// applies to every item, `n` codes = per item) to the code for one position.
fn resolve_format(formats: &[i16], index: usize) -> i16 {
    match formats {
        [] => 0,
        [single] => *single,
        many => many.get(index).copied().unwrap_or(0),
    }
}

fn resolve_formats(formats: &[i16], count: usize) -> Vec<i16> {
    (0..count)
        .map(|index| resolve_format(formats, index))
        .collect()
}

fn protocol_error(message: impl Into<String>) -> DbError {
    DbError::protocol(SqlState::SyntaxError, message)
}

/// Map the outcome of the query `spawn_blocking` task into a query result. A
/// panic in parse/bind/plan/execute (or a cancelled task) surfaces as a
/// `JoinError`; converting it to an internal error lets the caller report it and
/// keep the connection open instead of dropping the socket silently. The wire
/// codec buffer is unaffected and statement guards/page pins release on unwind.
fn query_task_result(
    join: std::result::Result<Result<ExecutionResult>, tokio::task::JoinError>,
) -> Result<ExecutionResult> {
    join.unwrap_or_else(|join_err| Err(DbError::internal(format!("query task failed: {join_err}"))))
}

async fn write_execution_result<S>(
    socket: &mut S,
    codec: &PostgresCodec,
    result: ExecutionResult,
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
            messages.push(ServerMessage::ReadyForQuery);
            write_messages(socket, codec, &messages).await
        }
        ExecutionResult::Modified { command, count } => {
            let tag = command_complete_tag(&command, count);
            write_messages(
                socket,
                codec,
                &[
                    ServerMessage::CommandComplete(tag),
                    ServerMessage::ReadyForQuery,
                ],
            )
            .await
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
                    ServerMessage::ReadyForQuery,
                ],
            )
            .await
        }
    }
}

async fn write_messages<S>(
    socket: &mut S,
    codec: &PostgresCodec,
    messages: &[ServerMessage],
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    for message in messages {
        socket
            .write_all(&codec.encode(message))
            .await
            .map_err(|err| DbError::io(format!("failed to write socket response: {err}")))?;
    }
    Ok(())
}

/// Encode each column of a result row to its wire bytes in the given format
/// code (`0` = text, `1` = binary), or `None` for SQL NULL.
/// Encode each column of a result row to its wire bytes, choosing each column's
/// format from the (text/binary) format-code array (empty = all text).
fn encode_row(row: &Row, formats: &[i16]) -> Result<Vec<Option<Vec<u8>>>> {
    row.values
        .iter()
        .enumerate()
        .map(|(index, value)| protocol::encode_value(value, resolve_format(formats, index)))
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
    }
}

fn command_complete_tag(command: &str, count: u64) -> String {
    match command {
        "INSERT" => format!("INSERT 0 {count}"),
        "UPDATE" | "DELETE" => format!("{command} {count}"),
        _ => command.to_string(),
    }
}

fn error_response(err: &DbError) -> ServerMessage {
    ServerMessage::ErrorResponse {
        severity: "ERROR".to_string(),
        code: sqlstate_code(err.code).to_string(),
        message: err.message.clone(),
    }
}

fn sqlstate_code(code: SqlState) -> &'static str {
    match code {
        SqlState::SuccessfulCompletion => "00000",
        SqlState::SyntaxError => "42601",
        SqlState::UndefinedTable => "42P01",
        SqlState::UndefinedColumn => "42703",
        SqlState::DuplicateTable => "42P07",
        SqlState::DatatypeMismatch => "42804",
        SqlState::DivisionByZero => "22012",
        SqlState::NumericValueOutOfRange => "22003",
        SqlState::NotNullViolation => "23502",
        SqlState::UniqueViolation => "23505",
        SqlState::IoError => "58030",
        SqlState::InternalError => "XX000",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use common::{ErrorKind, Result};
    use executor::ExecutionResult;

    use super::{handle_connection, query_task_result};
    use crate::app::AppState;

    #[tokio::test]
    async fn panicked_query_task_becomes_internal_error() {
        // A panicked spawn_blocking task yields a real JoinError; the firewall
        // must map it to an internal error (so the caller keeps the connection
        // open) rather than letting it escape and drop the connection.
        let join = tokio::task::spawn_blocking(|| -> Result<ExecutionResult> {
            panic!("intentional test panic");
        })
        .await;

        let err = query_task_result(join).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Internal);
    }

    #[tokio::test]
    async fn loopback_startup_and_simple_query_return_protocol_rows() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client
            .write_all(&query_bytes(
                "create table users (id integer primary key, name text)",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes(
                "insert into users (id, name) values (1, 'Ada')",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes("select id, name from users"))
            .await
            .unwrap();
        let response = read_until_ready(&mut client).await;

        assert!(response.windows(3).any(|window| window == b"Ada"));
        assert!(response.windows(9).any(|window| window == b"SELECT 1\0"));

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    fn startup_bytes(user: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
        body.push(0);

        let mut packet = Vec::new();
        packet.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        packet.extend_from_slice(&body);
        packet
    }

    fn query_bytes(sql: &str) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.push(b'Q');
        packet.extend_from_slice(&(sql.len() as i32 + 5).to_be_bytes());
        packet.extend_from_slice(sql.as_bytes());
        packet.push(0);
        packet
    }

    fn terminate_bytes() -> Vec<u8> {
        vec![b'X', 0, 0, 0, 4]
    }

    fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut packet = vec![tag];
        packet.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn parse_bytes(name: &str, query: &str, param_oids: &[i32]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(query.as_bytes());
        body.push(0);
        body.extend_from_slice(&i16::try_from(param_oids.len()).unwrap().to_be_bytes());
        for oid in param_oids {
            body.extend_from_slice(&oid.to_be_bytes());
        }
        tagged(b'P', &body)
    }

    fn bind_bytes(
        portal: &str,
        statement: &str,
        param_formats: &[i16],
        params: &[Option<&[u8]>],
        result_formats: &[i16],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(portal.as_bytes());
        body.push(0);
        body.extend_from_slice(statement.as_bytes());
        body.push(0);
        body.extend_from_slice(&i16::try_from(param_formats.len()).unwrap().to_be_bytes());
        for format in param_formats {
            body.extend_from_slice(&format.to_be_bytes());
        }
        body.extend_from_slice(&i16::try_from(params.len()).unwrap().to_be_bytes());
        for param in params {
            match param {
                Some(bytes) => {
                    body.extend_from_slice(&i32::try_from(bytes.len()).unwrap().to_be_bytes());
                    body.extend_from_slice(bytes);
                }
                None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        body.extend_from_slice(&i16::try_from(result_formats.len()).unwrap().to_be_bytes());
        for format in result_formats {
            body.extend_from_slice(&format.to_be_bytes());
        }
        tagged(b'B', &body)
    }

    fn describe_portal_bytes(name: &str) -> Vec<u8> {
        let mut body = vec![b'P'];
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        tagged(b'D', &body)
    }

    fn describe_statement_bytes(name: &str) -> Vec<u8> {
        let mut body = vec![b'S'];
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        tagged(b'D', &body)
    }

    fn execute_bytes(portal: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(portal.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        tagged(b'E', &body)
    }

    fn sync_bytes() -> Vec<u8> {
        tagged(b'S', &[])
    }

    #[tokio::test]
    async fn extended_protocol_runs_parameterized_query_text_and_binary() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;
        for sql in [
            "create table users (id integer primary key, name text)",
            "insert into users (id, name) values (1, 'Ada')",
            "insert into users (id, name) values (2, 'Bo')",
        ] {
            client.write_all(&query_bytes(sql)).await.unwrap();
            read_until_ready(&mut client).await;
        }

        // Text parameter (OID unspecified -> inferred Integer), text results.
        let mut seq = parse_bytes("", "select name from users where id = $1", &[0]);
        seq.extend(bind_bytes("", "", &[0], &[Some(b"1")], &[0]));
        seq.extend(describe_portal_bytes(""));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(5).any(|w| w == [b'1', 0, 0, 0, 4]),
            "ParseComplete"
        );
        assert!(
            response.windows(5).any(|w| w == [b'2', 0, 0, 0, 4]),
            "BindComplete"
        );
        assert!(response.windows(3).any(|w| w == b"Ada"), "row value");
        assert!(
            response.windows(9).any(|w| w == b"SELECT 1\0"),
            "CommandComplete"
        );

        // Binary parameter (int8), binary results.
        let id = 2i64.to_be_bytes();
        let mut seq = parse_bytes("", "select name from users where id = $1", &[20]);
        seq.extend(bind_bytes("", "", &[1], &[Some(&id[..])], &[1]));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(2).any(|w| w == b"Bo"),
            "binary-parameter row value"
        );

        // Binary INTEGER result column: the value is the 8-byte big-endian
        // encoding, distinguishing binary from text result encoding.
        let mut seq = parse_bytes("", "select id from users where id = $1", &[20]);
        seq.extend(bind_bytes("", "", &[1], &[Some(&id[..])], &[1]));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(8).any(|w| w == 2i64.to_be_bytes()),
            "binary int8 result value"
        );

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn extended_protocol_error_is_recoverable_via_sync() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        // Parse referencing a missing table fails; the Execute that follows is
        // skipped, and Sync recovers the connection with ReadyForQuery.
        let mut seq = parse_bytes("", "select id from missing where id = $1", &[0]);
        seq.extend(bind_bytes("", "", &[0], &[Some(b"1")], &[0]));
        seq.extend(execute_bytes(""));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(5).any(|w| w == b"42P01"),
            "undefined-table error"
        );
        // The Bind/Execute after the failed Parse were skipped until Sync.
        assert!(
            !response.windows(5).any(|w| w == [b'2', 0, 0, 0, 4]),
            "Bind should have been skipped"
        );

        // The connection still works after Sync.
        client
            .write_all(&query_bytes("create table t (id integer primary key)"))
            .await
            .unwrap();
        let response = read_until_ready(&mut client).await;
        assert!(
            response.windows(13).any(|w| w == b"CREATE TABLE\0"),
            "connection recovered"
        );

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn extended_protocol_named_statement_and_describe() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;
        for sql in [
            "create table users (id integer primary key, name text)",
            "insert into users (id, name) values (7, 'Cy')",
        ] {
            client.write_all(&query_bytes(sql)).await.unwrap();
            read_until_ready(&mut client).await;
        }

        // Named statement + named portal, with Describe of the statement.
        let mut seq = parse_bytes("stmt1", "select name from users where id = $1", &[20]);
        seq.extend(describe_statement_bytes("stmt1"));
        seq.extend(bind_bytes("p1", "stmt1", &[0], &[Some(b"7")], &[0]));
        seq.extend(execute_bytes("p1"));
        seq.extend(sync_bytes());
        client.write_all(&seq).await.unwrap();
        let response = read_until_ready(&mut client).await;

        // ParameterDescription (tag 't'): one parameter, int8 (OID 20).
        assert!(
            response
                .windows(11)
                .any(|w| w == [b't', 0, 0, 0, 10, 0, 1, 0, 0, 0, 20]),
            "ParameterDescription with int8 parameter"
        );
        assert!(response.first() == Some(&b'1'), "ParseComplete first");
        assert!(response.windows(1).any(|w| w == b"T"), "RowDescription");
        assert!(response.windows(2).any(|w| w == b"Cy"), "row value");

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn loopback_boolean_query_uses_postgres_text_bool_format() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client
            .write_all(&query_bytes(
                "create table users (id integer primary key, active boolean)",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes(
                "insert into users (id, active) values (1, true)",
            ))
            .await
            .unwrap();
        read_until_ready(&mut client).await;
        client
            .write_all(&query_bytes("select active from users"))
            .await
            .unwrap();
        let response = read_until_ready(&mut client).await;

        assert!(
            response
                .windows(5)
                .any(|window| window == [0, 0, 0, 1, b't'])
        );
        assert!(!response.windows(4).any(|window| window == b"true"));

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    fn ssl_request_bytes() -> Vec<u8> {
        negotiation_request_bytes(80_877_103)
    }

    fn gssenc_request_bytes() -> Vec<u8> {
        negotiation_request_bytes(80_877_104)
    }

    fn negotiation_request_bytes(code: i32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&code.to_be_bytes());
        bytes
    }

    /// Generate a self-signed `localhost` cert into `dir`, open a TLS-enabled
    /// test app, and return it with the cert PEM (for the client's trust root).
    fn open_app_with_tls(dir: &std::path::Path) -> (Arc<AppState>, String) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = generated.cert.pem();
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, generated.signing_key.serialize_pem()).unwrap();

        let mut config = crate::recovery::data_dir_for_test(dir);
        config.tls_cert_file = Some(cert_path);
        config.tls_key_file = Some(key_path);
        (
            Arc::new(crate::recovery::open_app(config).unwrap()),
            cert_pem,
        )
    }

    /// Complete a client-side TLS handshake over `tcp`, trusting `cert_pem`.
    async fn connect_tls_client(
        cert_pem: &str,
        tcp: TcpStream,
    ) -> tokio_rustls::client::TlsStream<TcpStream> {
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::crypto::ring;
        use tokio_rustls::rustls::pki_types::ServerName;
        use tokio_rustls::rustls::{ClientConfig, RootCertStore};

        let mut roots = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut cert_pem.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        let domain = ServerName::try_from("localhost").unwrap();
        connector.connect(domain, tcp).await.unwrap()
    }

    #[tokio::test]
    async fn ssl_request_is_rejected_when_tls_is_disabled_then_plaintext_proceeds() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&ssl_request_bytes()).await.unwrap();

        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"N");

        // The same connection then completes a plaintext startup.
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_negotiation_upgrades_then_query_runs_over_encrypted_stream() {
        let dir = tempfile::tempdir().unwrap();
        let (app, cert_pem) = open_app_with_tls(dir.path());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let app = app.clone();
            tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                handle_connection(socket, app).await.unwrap();
            })
        };

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&ssl_request_bytes()).await.unwrap();
        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"S");

        let mut tls = connect_tls_client(&cert_pem, client).await;

        tls.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes(
            "create table users (id integer primary key, name text)",
        ))
        .await
        .unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes(
            "insert into users (id, name) values (1, 'Ada')",
        ))
        .await
        .unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes("select id, name from users"))
            .await
            .unwrap();
        let response = read_until_ready(&mut tls).await;

        assert!(response.windows(3).any(|window| window == b"Ada"));
        assert!(response.windows(9).any(|window| window == b"SELECT 1\0"));

        tls.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gssenc_request_is_declined_then_plaintext_startup_proceeds() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&gssenc_request_bytes()).await.unwrap();

        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"N");

        // After declining GSS the same connection completes a plaintext startup.
        client.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut client).await;

        client.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gssenc_decline_then_ssl_upgrade_serves_encrypted_session() {
        let dir = tempfile::tempdir().unwrap();
        let (app, cert_pem) = open_app_with_tls(dir.path());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket, app).await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();

        // Decline GSS, then upgrade via SSL on the same connection.
        client.write_all(&gssenc_request_bytes()).await.unwrap();
        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"N");

        client.write_all(&ssl_request_bytes()).await.unwrap();
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"S");

        // The TLS handshake succeeding here proves the SSLRequest after a GSS
        // decline still reaches the upgrade path rather than being mishandled.
        let mut tls = connect_tls_client(&cert_pem, client).await;
        tls.write_all(&startup_bytes("dave")).await.unwrap();
        read_until_ready(&mut tls).await;
        tls.write_all(&query_bytes("create table t (id integer primary key)"))
            .await
            .unwrap();
        let response = read_until_ready(&mut tls).await;
        assert!(
            response
                .windows(13)
                .any(|window| window == b"CREATE TABLE\0")
        );

        tls.write_all(&terminate_bytes()).await.unwrap();
        server.await.unwrap();
    }

    async fn read_until_ready<S: AsyncRead + Unpin>(client: &mut S) -> Vec<u8> {
        let mut response = Vec::new();
        let mut buf = [0; 1024];
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let read = client.read(&mut buf).await.unwrap();
                assert_ne!(read, 0, "connection closed before ReadyForQuery");
                response.extend_from_slice(&buf[..read]);
                if response.windows(6).any(|window| window == b"Z\0\0\0\x05I") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        response
    }
}
