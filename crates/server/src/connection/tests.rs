use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use common::{ErrorKind, IsolationLevel, Result};

use super::{StreamOutcome, TransactionState, handle_connection, streamed_task_result};
use crate::app::AppState;

#[test]
fn transaction_state_maps_to_postgres_status_byte() {
    assert_eq!(TransactionState::Idle.status_byte(), b'I');
    assert_eq!(TransactionState::InTransaction.status_byte(), b'T');
    assert_eq!(TransactionState::Failed.status_byte(), b'E');
}

#[tokio::test]
async fn panicked_query_task_becomes_internal_error() {
    // A panicked spawn_blocking task yields a real JoinError; the firewall
    // must map it to an internal error with no open transaction (so the caller
    // keeps the connection open) rather than letting it escape and drop the
    // connection.
    let join = tokio::task::spawn_blocking(
        || -> (Option<super::Transaction>, IsolationLevel, Result<StreamOutcome>) {
            panic!("intentional test panic");
        },
    )
    .await;

    let (slot, _default, result) = streamed_task_result(join, IsolationLevel::default());
    assert!(slot.is_none(), "a panicked task leaves no open transaction");
    assert_eq!(result.unwrap_err().kind, ErrorKind::Internal);
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

#[tokio::test]
async fn session_characteristics_default_is_per_connection_over_the_wire() {
    // End-to-end (Milestone G2): `SET SESSION CHARACTERISTICS ... REPEATABLE READ`
    // on one connection makes a later plain `BEGIN` on THAT connection default to
    // Repeatable Read (its second SELECT does not see a row committed in between).
    // A fresh connection resets to Read Committed and DOES see such a row.
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Accept connections for the duration of the test on a background task.
    let accept_app = app.clone();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let app = accept_app.clone();
            tokio::spawn(async move {
                let _ = handle_connection(socket, app).await;
            });
        }
    });

    // Setup connection: create the table and seed nothing.
    let mut setup = TcpStream::connect(addr).await.unwrap();
    setup.write_all(&startup_bytes("dave")).await.unwrap();
    read_until_ready(&mut setup).await;
    setup
        .write_all(&query_bytes("create table t (id integer primary key)"))
        .await
        .unwrap();
    read_until_ready(&mut setup).await;

    // Connection A: set the session default to Repeatable Read, then open a txn.
    let mut conn_a = TcpStream::connect(addr).await.unwrap();
    conn_a.write_all(&startup_bytes("dave")).await.unwrap();
    read_until_ready(&mut conn_a).await;
    conn_a
        .write_all(&query_bytes(
            "set session characteristics as transaction isolation level repeatable read",
        ))
        .await
        .unwrap();
    let response = read_until_ready(&mut conn_a).await;
    assert!(
        response.windows(4).any(|w| w == b"SET\0"),
        "SET SESSION CHARACTERISTICS completes with a SET tag"
    );

    conn_a.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut conn_a).await;
    conn_a
        .write_all(&query_bytes("select id from t"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut conn_a).await;
    assert!(response.windows(9).any(|w| w == b"SELECT 0\0"));

    // The setup connection commits a row while conn A's RR txn is open.
    setup
        .write_all(&query_bytes("insert into t (id) values (1)"))
        .await
        .unwrap();
    read_until_ready(&mut setup).await;

    // Conn A's second SELECT does NOT see the new row (it defaulted to RR).
    conn_a
        .write_all(&query_bytes("select id from t"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut conn_a).await;
    assert!(
        response.windows(9).any(|w| w == b"SELECT 0\0"),
        "the inherited RR transaction does not see the concurrently-committed row"
    );
    conn_a.write_all(&query_bytes("commit")).await.unwrap();
    read_until_ready(&mut conn_a).await;
    conn_a.write_all(&terminate_bytes()).await.unwrap();

    // Connection B (fresh): resets to Read Committed regardless of conn A's
    // setting. Its open transaction's second SELECT DOES see a new committed row.
    let mut conn_b = TcpStream::connect(addr).await.unwrap();
    conn_b.write_all(&startup_bytes("dave")).await.unwrap();
    read_until_ready(&mut conn_b).await;
    conn_b.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut conn_b).await;
    conn_b
        .write_all(&query_bytes("select id from t"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut conn_b).await;
    assert!(response.windows(9).any(|w| w == b"SELECT 1\0"));
    setup
        .write_all(&query_bytes("insert into t (id) values (2)"))
        .await
        .unwrap();
    read_until_ready(&mut setup).await;
    conn_b
        .write_all(&query_bytes("select id from t"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut conn_b).await;
    assert!(
        response.windows(9).any(|w| w == b"SELECT 2\0"),
        "a fresh connection resets to Read Committed and sees the new row"
    );
    conn_b.write_all(&query_bytes("commit")).await.unwrap();
    read_until_ready(&mut conn_b).await;
    conn_b.write_all(&terminate_bytes()).await.unwrap();
    setup.write_all(&terminate_bytes()).await.unwrap();

    server.abort();
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
async fn startup_sends_backend_key_data() {
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
    let response = read_until_ready(&mut client).await;

    // BackendKeyData: tag 'K', length 12.
    assert!(
        response.windows(5).any(|w| w == [b'K', 0, 0, 0, 12]),
        "startup reply includes BackendKeyData"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn cancel_request_signals_the_target_backend() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    // Register a flag as if a connection were running a query.
    let flag = Arc::new(AtomicBool::new(false));
    let key = app.components.cancel_registry.register(flag.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_app = app.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        handle_connection(socket, server_app).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(&cancel_request_bytes(key.process_id, key.secret_key))
        .await
        .unwrap();

    // The server signals the backend and closes without any reply.
    let mut buf = [0u8; 8];
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "CancelRequest gets no reply and the socket closes");
    assert!(flag.load(Ordering::Relaxed), "target backend was signaled");

    server.await.unwrap();
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

fn cancel_request_bytes(process_id: i32, secret_key: i32) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&16i32.to_be_bytes());
    bytes.extend_from_slice(&80_877_102i32.to_be_bytes());
    bytes.extend_from_slice(&process_id.to_be_bytes());
    bytes.extend_from_slice(&secret_key.to_be_bytes());
    bytes
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

/// Like [`read_until_ready`] but breaks on a `ReadyForQuery` carrying ANY
/// transaction-status byte (`I`/`T`/`E`), so it can drain a reply sent while a
/// transaction block is open (status `'T'`), not just an idle one.
async fn read_until_ready_any<S: AsyncRead + Unpin>(client: &mut S) -> Vec<u8> {
    let mut response = Vec::new();
    let mut buf = [0; 1024];
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let read = client.read(&mut buf).await.unwrap();
            assert_ne!(read, 0, "connection closed before ReadyForQuery");
            response.extend_from_slice(&buf[..read]);
            let ready = response.windows(6).any(|window| {
                window[..5] == [b'Z', 0, 0, 0, 5] && matches!(window[5], b'I' | b'T' | b'E')
            });
            if ready {
                break;
            }
        }
    })
    .await
    .unwrap();
    response
}
