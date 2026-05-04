use std::sync::Arc;

use common::{ColumnInfo, DataType, DbError, Result, Row, SqlState, Value};
use executor::ExecutionResult;
use protocol::{
    ClientMessage, ConnectionState, PostgresCodec, PostgresConnectionState, ProtocolCodec,
    ServerMessage,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::app::AppState;

pub async fn handle_connection(_socket: TcpStream, _app: Arc<AppState>) -> Result<()> {
    let mut socket = _socket;
    let app = _app;
    let mut codec = PostgresCodec::new();
    let mut state = PostgresConnectionState::new();
    let mut buf = [0; 8192];

    loop {
        let read = socket
            .read(&mut buf)
            .await
            .map_err(|err| DbError::io(format!("failed to read socket: {err}")))?;
        if read == 0 {
            return Ok(());
        }

        let messages = match codec.decode(&buf[..read]) {
            Ok(messages) => messages,
            Err(err) => {
                write_messages(
                    &mut socket,
                    &codec,
                    &[error_response(&err), ServerMessage::ReadyForQuery],
                )
                .await?;
                continue;
            }
        };

        for message in messages {
            match message {
                ClientMessage::Query(sql) => {
                    let guard = match app.components.shutdown.begin_query() {
                        Ok(guard) => guard,
                        Err(err) => {
                            write_messages(
                                &mut socket,
                                &codec,
                                &[error_response(&err), ServerMessage::ReadyForQuery],
                            )
                            .await?;
                            return Ok(());
                        }
                    };

                    let service = app.query_service.clone();
                    let result = tokio::task::spawn_blocking(move || service.execute_sql(&sql))
                        .await
                        .map_err(|err| DbError::internal(format!("query task failed: {err}")))?;
                    match result {
                        Ok(result) => write_execution_result(&mut socket, &codec, result).await?,
                        Err(err) => {
                            write_messages(
                                &mut socket,
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
                        socket
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
    }
}

async fn write_execution_result(
    socket: &mut TcpStream,
    codec: &PostgresCodec,
    result: ExecutionResult,
) -> Result<()> {
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

async fn write_messages(
    socket: &mut TcpStream,
    codec: &PostgresCodec,
    messages: &[ServerMessage],
) -> Result<()> {
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

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::handle_connection;
    use crate::app::AppState;

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

    async fn read_until_ready(client: &mut TcpStream) -> Vec<u8> {
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
