use std::sync::Arc;

use common::{ColumnInfo, DataType, DbError, Result, Row, SqlState, Value};
use executor::ExecutionResult;
use protocol::{
    ClientMessage, ConnectionState, PostgresCodec, PostgresConnectionState, ProtocolCodec,
    ServerMessage,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::app::AppState;

/// Accept a connection, run optional SSL negotiation, then serve the protocol
/// over the resulting (plaintext or TLS) stream.
///
/// A TLS-capable client leads with an `SSLRequest`. When the server has TLS
/// configured, it replies `SslAccepted` (`S`) and upgrades the socket; otherwise
/// it replies `SslRejected` (`N`) and the client continues in plaintext. A
/// client that opens with a `StartupMessage` is served in plaintext directly.
pub async fn handle_connection(mut socket: TcpStream, app: Arc<AppState>) -> Result<()> {
    let mut codec = PostgresCodec::new();
    let mut buf = [0; 8192];

    // Read until the first client message is fully buffered. The leading message
    // decides whether we negotiate TLS (SSLRequest) or serve in plaintext
    // (StartupMessage). Looping keeps negotiation correct even when the small
    // SSLRequest packet is split across reads.
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

    if matches!(initial.first(), Some(ClientMessage::SslRequest)) {
        // The client must wait for the negotiation reply before sending more, so
        // anything bundled after the SSLRequest is a protocol violation.
        if initial.len() > 1 {
            let err = DbError::protocol(
                SqlState::SyntaxError,
                "client sent data before completing SSL negotiation",
            );
            write_messages(
                &mut socket,
                &codec,
                &[error_response(&err), ServerMessage::ReadyForQuery],
            )
            .await?;
            return Ok(());
        }

        // Clone the acceptor (a cheap `Arc`) so `app` stays free to move into
        // `serve`.
        return match app.components.tls.clone() {
            Some(acceptor) => {
                write_messages(&mut socket, &codec, &[ServerMessage::SslAccepted]).await?;
                socket
                    .flush()
                    .await
                    .map_err(|err| DbError::io(format!("failed to flush SSL response: {err}")))?;
                let tls = acceptor
                    .accept(socket)
                    .await
                    .map_err(|err| DbError::io(format!("TLS handshake failed: {err}")))?;
                // Serve the encrypted session with a fresh codec: only the lone
                // SSLRequest is legitimate before the handshake, so a new decode
                // buffer ensures no stray pre-handshake plaintext can bleed into
                // the decrypted stream.
                serve(tls, PostgresCodec::new(), app, Vec::new()).await
            }
            None => {
                write_messages(&mut socket, &codec, &[ServerMessage::SslRejected]).await?;
                serve(socket, codec, app, Vec::new()).await
            }
        };
    }

    serve(socket, codec, app, initial).await
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
    let mut state = PostgresConnectionState::new();
    let mut buf = [0; 8192];

    loop {
        for message in batch {
            match message {
                ClientMessage::Query(sql) => {
                    let guard = match app.components.shutdown.begin_query() {
                        Ok(guard) => guard,
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

                    let service = app.query_service.clone();
                    let result = query_task_result(
                        tokio::task::spawn_blocking(move || service.execute_sql(&sql)).await,
                    );
                    match result {
                        Ok(result) => write_execution_result(&mut stream, &codec, result).await?,
                        Err(err) => {
                            write_messages(
                                &mut stream,
                                &codec,
                                &[error_response(&err), ServerMessage::ReadyForQuery],
                            )
                            .await?;
                        }
                    }
                    drop(guard);
                }
                other => {
                    for response in state.handle_message(other)? {
                        stream
                            .write_all(&codec.encode(&response))
                            .await
                            .map_err(|err| {
                                DbError::io(format!("failed to write socket response: {err}"))
                            })?;
                    }
                    if state.is_terminated() {
                        return Ok(());
                    }
                }
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
            messages.push(ServerMessage::RowDescription(columns));
            for row in rows {
                messages.push(ServerMessage::DataRow(format_row(&row)));
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
                    ServerMessage::RowDescription(vec![ColumnInfo {
                        name: "QUERY PLAN".to_string(),
                        data_type: DataType::Text,
                        table_id: None,
                        column_id: None,
                    }]),
                    ServerMessage::DataRow(vec![Some(text)]),
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

fn format_row(row: &Row) -> Vec<Option<String>> {
    row.values.iter().map(format_value).collect()
}

fn format_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Boolean(true) => Some("t".to_string()),
        Value::Boolean(false) => Some("f".to_string()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Text(value) => Some(value.clone()),
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
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&80_877_103i32.to_be_bytes());
        bytes
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
        use std::sync::Arc as StdArc;

        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::crypto::ring;
        use tokio_rustls::rustls::pki_types::ServerName;
        use tokio_rustls::rustls::{ClientConfig, RootCertStore};

        let dir = tempfile::tempdir().unwrap();
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = generated.cert.pem();
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, generated.signing_key.serialize_pem()).unwrap();

        let mut config = crate::recovery::data_dir_for_test(dir.path());
        config.tls_cert_file = Some(cert_path);
        config.tls_key_file = Some(key_path);
        let app = StdArc::new(crate::recovery::open_app(config).unwrap());

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

        let mut roots = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut cert_pem.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let client_config =
            ClientConfig::builder_with_provider(StdArc::new(ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        let connector = TlsConnector::from(StdArc::new(client_config));
        let domain = ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(domain, client).await.unwrap();

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
