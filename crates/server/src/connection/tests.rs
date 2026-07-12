use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use common::{
    CancelReason, ColumnInfo, DataType, ErrorKind, IsolationLevel, PgType, QueryCancel, Result,
    Row, SqlState, Value,
};
use protocol::{PostgresCodec, ServerMessage, StatementKind};

use super::{
    CopyInSession, Session, StreamOutcome, TransactionState, apply_stream_consumer_cancel,
    encode_row, handle_connection, resolve_result_formats, sqlstate_code, streamed_task_result,
};
use crate::app::AppState;
use crate::query::CopyInChunk;

#[test]
fn transaction_state_maps_to_postgres_status_byte() {
    assert_eq!(TransactionState::Idle.status_byte(), b'I');
    assert_eq!(TransactionState::InTransaction.status_byte(), b'T');
    assert_eq!(TransactionState::Failed.status_byte(), b'E');
}

#[test]
fn late_consumer_cancellation_preserves_only_explicitly_durable_results() {
    let mut txn = None;
    let mut outcome = Ok(StreamOutcome::Durable(
        executor::ExecutionResult::Modified {
            command: "INSERT".to_string(),
            count: 1,
        },
    ));
    apply_stream_consumer_cancel(
        &mut txn,
        &mut outcome,
        common::DbError::execute(SqlState::QueryCanceled, "late timeout"),
    );
    assert!(matches!(outcome, Ok(StreamOutcome::Durable(_))));

    let mut direct = Ok(StreamOutcome::Direct(
        executor::ExecutionResult::Explanation {
            text: "SeqScan".to_string(),
        },
    ));
    apply_stream_consumer_cancel(
        &mut txn,
        &mut direct,
        common::DbError::execute(SqlState::QueryCanceled, "explain timeout"),
    );
    assert!(matches!(direct, Err(err) if err.code == SqlState::QueryCanceled));

    let mut session_reset = Ok(StreamOutcome::SessionReset(
        executor::ExecutionResult::Modified {
            command: "DISCARD ALL".to_string(),
            count: 0,
        },
    ));
    apply_stream_consumer_cancel(
        &mut txn,
        &mut session_reset,
        common::DbError::execute(SqlState::QueryCanceled, "discard timeout"),
    );
    assert!(matches!(session_reset, Ok(StreamOutcome::SessionReset(_))));

    let mut streamed = Ok(StreamOutcome::Streamed { count: 1 });
    apply_stream_consumer_cancel(
        &mut txn,
        &mut streamed,
        common::DbError::execute(SqlState::QueryCanceled, "stream timeout"),
    );
    assert!(matches!(streamed, Err(err) if err.code == SqlState::QueryCanceled));
}

#[tokio::test]
async fn wait_cancelable_checks_cancellation_when_future_is_already_ready() {
    let cancel = QueryCancel::new();
    cancel.request(CancelReason::StatementTimeout);

    let err = super::wait_cancelable(&cancel, async { None::<()> })
        .await
        .unwrap_err();

    assert_eq!(err.code, SqlState::QueryCanceled);
}

#[tokio::test]
async fn cancelable_write_completes_a_ready_frame_when_cancellation_is_pending() {
    let codec = PostgresCodec::new();
    let cancel = QueryCancel::new();
    cancel.request(CancelReason::StatementTimeout);
    let (mut writer, mut reader) = tokio::io::duplex(128);

    super::wait_cancelable_write(
        &cancel,
        super::write_messages(&mut writer, &codec, &[ServerMessage::ParseComplete]),
    )
    .await
    .unwrap()
    .unwrap();

    let mut frame = [0; 5];
    reader.read_exact(&mut frame).await.unwrap();
    assert_eq!(frame, [b'1', 0, 0, 0, 4]);
}

#[tokio::test]
async fn authoritative_terminal_response_writes_complete_frame() {
    let codec = PostgresCodec::new();
    let cancel = QueryCancel::new();
    let (mut writer, mut reader) = tokio::io::duplex(128);

    super::write_terminal_response(
        &cancel,
        super::write_messages(
            &mut writer,
            &codec,
            &[ServerMessage::CommandComplete("INSERT 0 1".to_string())],
        ),
    )
    .await
    .unwrap();

    let mut tag = [0; 1];
    reader.read_exact(&mut tag).await.unwrap();
    assert_eq!(tag, [b'C']);
}

#[tokio::test]
async fn authoritative_terminal_response_cancels_a_blocked_partial_write() {
    let codec = PostgresCodec::new();
    let cancel = QueryCancel::new();
    let (mut writer, _reader) = tokio::io::duplex(1);
    let messages = [ServerMessage::CommandComplete("INSERT 0 1".to_string())];
    let response = super::write_terminal_response(
        &cancel,
        super::write_messages(&mut writer, &codec, &messages),
    );
    let request_cancel = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.request(CancelReason::StatementTimeout);
    };

    let (result, ()) = tokio::join!(response, request_cancel);

    assert_eq!(result.unwrap_err().code, SqlState::QueryCanceled);
}

#[tokio::test]
async fn completed_copy_in_releases_guard_before_terminal_write() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let shutdown = app.components.shutdown.clone();
    let mut session = Session::new(app);
    let guard = shutdown.begin_query().unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let task = tokio::spawn(async move {
        assert!(matches!(receiver.recv().await, Some(CopyInChunk::Done)));
        (None, Ok(1))
    });
    session.copy_in = Some(CopyInSession {
        sender: Some(sender),
        task: Some(task),
        insert_failed: false,
        draining_after_cancel: false,
        guard: Some(guard),
    });
    let codec = PostgresCodec::new();
    // One byte lets the terminal frame start but keeps it blocked without a reader.
    let (mut writer, _reader) = tokio::io::duplex(1);

    let finish =
        tokio::spawn(async move { session.finish_copy_in(&mut writer, &codec, None).await });

    shutdown
        .wait_for_idle(Duration::from_secs(1))
        .await
        .expect("joined COPY worker must release its in-flight guard");
    assert!(
        !finish.is_finished(),
        "terminal response should still be blocked by the unread socket"
    );
    finish.abort();
    let _ = finish.await;
}

#[test]
fn program_limit_exceeded_maps_to_sqlstate_54000() {
    assert_eq!(sqlstate_code(SqlState::ProgramLimitExceeded), "54000");
}

#[tokio::test]
async fn extended_control_work_does_not_publish_after_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let mut session = Session::new(app);

    session.cancel.request(CancelReason::StatementTimeout);
    let err = session
        .process_parse("timed_out".to_string(), "select 1".to_string(), &[])
        .await
        .unwrap_err();
    assert_eq!(err.code, SqlState::QueryCanceled);
    assert!(!session.prepared.contains_key("timed_out"));

    session.cancel.reset();
    session
        .process_parse("statement".to_string(), "select 1".to_string(), &[])
        .await
        .unwrap();
    session.cancel.request(CancelReason::StatementTimeout);
    let err = session
        .process_bind(
            "timed_out".to_string(),
            "statement",
            &[],
            Vec::new(),
            Vec::new(),
        )
        .unwrap_err();
    assert_eq!(err.code, SqlState::QueryCanceled);
    assert!(!session.portals.contains_key("timed_out"));

    let err = session
        .process_describe(StatementKind::Statement, "statement")
        .unwrap_err();
    assert_eq!(err.code, SqlState::QueryCanceled);
}

#[tokio::test]
async fn extended_control_error_marks_open_transaction_failed() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let mut session = Session::new(app);
    let codec = PostgresCodec::new();
    let (mut server_stream, _client_stream) = tokio::io::duplex(4_096);

    let _ = session
        .run_query(&mut server_stream, &codec, "begin".to_string())
        .await
        .unwrap();
    assert_eq!(session.tx, TransactionState::InTransaction);

    session.cancel.request(CancelReason::StatementTimeout);
    let result = session
        .process_parse("timed_out".to_string(), "select 1".to_string(), &[])
        .await;
    session
        .reply_or_fail(&mut server_stream, &codec, result)
        .await
        .unwrap();

    assert_eq!(session.tx, TransactionState::Failed);
    assert!(session.txn.as_ref().unwrap().is_failed());
}

#[tokio::test]
async fn extended_parse_schema_guard_wait_is_cancelable() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let exclusive = app.components.concurrency.begin_checkpoint().unwrap();
    let mut session = Session::new(app);
    let cancel = session.cancel.clone();
    let request = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancel.request(CancelReason::StatementTimeout);
    });

    let err = tokio::time::timeout(
        Duration::from_secs(1),
        session.process_parse("blocked".to_string(), "select 1".to_string(), &[]),
    )
    .await
    .expect("Parse should observe cancellation while waiting for the schema guard")
    .unwrap_err();

    assert_eq!(err.code, SqlState::QueryCanceled);
    assert!(!session.prepared.contains_key("blocked"));
    request.await.unwrap();
    drop(exclusive);
}

#[test]
fn catalog_vector_and_array_results_stay_text_when_binary_is_requested() {
    let columns = vec![
        ColumnInfo {
            name: "indkey".to_string(),
            data_type: DataType::Text,
            table_id: None,
            column_id: None,
            pg_type: Some(PgType::Int2Vector),
        },
        ColumnInfo {
            name: "conkey".to_string(),
            data_type: DataType::Text,
            table_id: None,
            column_id: None,
            pg_type: Some(PgType::CatalogInt2ArrayText),
        },
        ColumnInfo {
            name: "n".to_string(),
            data_type: DataType::Integer,
            table_id: None,
            column_id: None,
            pg_type: Some(PgType::Int4),
        },
    ];
    assert_eq!(resolve_result_formats(&[1], &columns), vec![0, 0, 1]);

    let row = Row {
        values: vec![
            Value::Text("1 2".to_string()),
            Value::Text("{1,2}".to_string()),
            Value::Integer(7),
        ],
    };
    let encoded = encode_row(&row, &columns, &[1]).unwrap();
    assert_eq!(encoded[0], Some(b"1 2".to_vec()));
    assert_eq!(encoded[1], Some(b"{1,2}".to_vec()));
    assert_eq!(encoded[2], Some(7i32.to_be_bytes().to_vec()));
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
async fn loopback_sql_cursor_fetch_close_lifecycle() {
    let mut test = open_loopback_test().await;
    for sql in [
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3), (4), (5)",
    ] {
        test.client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut test.client).await;
    }

    test.client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes(
            "/* cursor route */ declare c cursor for select id from users order by id",
        ))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert!(
        response.windows(15).any(|w| w == b"DECLARE CURSOR\0"),
        "DECLARE command tag"
    );

    test.client
        .write_all(&query_bytes("fetch 2 from c"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert_eq!(message_tag_count(&response, b'T'), 1, "RowDescription");
    assert_eq!(message_tag_count(&response, b'D'), 2, "two fetched rows");
    assert_eq!(
        data_row_text_fields(&response),
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
    assert!(response.windows(8).any(|w| w == b"FETCH 2\0"));

    test.client
        .write_all(&query_bytes("fetch all from c"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert_eq!(message_tag_count(&response, b'D'), 3, "remaining rows");
    assert_eq!(
        data_row_text_fields(&response),
        vec![
            vec!["3".to_string()],
            vec!["4".to_string()],
            vec!["5".to_string()]
        ]
    );
    assert!(response.windows(8).any(|w| w == b"FETCH 3\0"));

    test.client
        .write_all(&query_bytes("fetch all from c"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert_eq!(
        message_tag_count(&response, b'D'),
        0,
        "exhausted cursor returns no rows"
    );
    assert_eq!(data_row_text_fields(&response), Vec::<Vec<String>>::new());
    assert_eq!(
        message_tag_count(&response, b'T'),
        1,
        "exhausted fetch still describes the result"
    );
    assert!(response.windows(8).any(|w| w == b"FETCH 0\0"));

    test.client
        .write_all(&query_bytes("close c"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert!(
        response.windows(13).any(|w| w == b"CLOSE CURSOR\0"),
        "CLOSE command tag"
    );
    test.client.write_all(&query_bytes("commit")).await.unwrap();
    read_until_ready(&mut test.client).await;

    test.client.write_all(&terminate_bytes()).await.unwrap();
    test.server.await.unwrap();
}

#[tokio::test]
async fn loopback_sql_cursor_errors_use_expected_sqlstates() {
    let mut test = open_loopback_test().await;

    test.client
        .write_all(&query_bytes("declare c cursor for select 1"))
        .await
        .unwrap();
    let response = read_until_ready(&mut test.client).await;
    assert!(response.windows(5).any(|w| w == b"25P01"));

    test.client
        .write_all(&query_bytes("create sequence s"))
        .await
        .unwrap();
    read_until_ready(&mut test.client).await;
    test.client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("declare c cursor for select 1"))
        .await
        .unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("declare c cursor for select 1"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert!(response.windows(5).any(|w| w == b"42P03"));
    assert_ready_status(&response, b'E');
    test.client
        .write_all(&query_bytes("rollback"))
        .await
        .unwrap();
    read_until_ready(&mut test.client).await;

    test.client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("fetch from missing"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert!(response.windows(5).any(|w| w == b"34000"));
    assert_ready_status(&response, b'E');
    test.client
        .write_all(&query_bytes("rollback"))
        .await
        .unwrap();
    read_until_ready(&mut test.client).await;

    test.client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("declare c cursor for select nextval('s')"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert!(response.windows(5).any(|w| w == b"0A000"));
    assert_ready_status(&response, b'E');
    test.client
        .write_all(&query_bytes("rollback"))
        .await
        .unwrap();
    read_until_ready(&mut test.client).await;

    test.client.write_all(&terminate_bytes()).await.unwrap();
    test.server.await.unwrap();
}

#[tokio::test]
async fn loopback_sql_cursors_close_on_transaction_cleanup() {
    let mut test = open_loopback_test().await;
    test.client
        .write_all(&query_bytes("create table users (id integer primary key)"))
        .await
        .unwrap();
    read_until_ready(&mut test.client).await;

    test.client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("declare c cursor for select id from users"))
        .await
        .unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client.write_all(&query_bytes("commit")).await.unwrap();
    read_until_ready(&mut test.client).await;
    assert_cursor_missing_in_new_transaction(&mut test.client, "c").await;

    test.client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("declare r cursor for select id from users"))
        .await
        .unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("rollback"))
        .await
        .unwrap();
    read_until_ready(&mut test.client).await;
    assert_cursor_missing_in_new_transaction(&mut test.client, "r").await;

    test.client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("savepoint s"))
        .await
        .unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("declare sp cursor for select id from users"))
        .await
        .unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("rollback to savepoint s"))
        .await
        .unwrap();
    read_until_ready_any(&mut test.client).await;
    test.client
        .write_all(&query_bytes("fetch from sp"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut test.client).await;
    assert!(response.windows(5).any(|w| w == b"34000"));
    test.client
        .write_all(&query_bytes("rollback"))
        .await
        .unwrap();
    read_until_ready(&mut test.client).await;

    test.client.write_all(&terminate_bytes()).await.unwrap();
    test.server.await.unwrap();
}

#[tokio::test]
async fn loopback_sql_cursor_snapshot_survives_delete_and_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let app_for_assert = app.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = {
        let app = app.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let app = app.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(socket, app).await;
                });
            }
        })
    };

    let mut cursor_client = TcpStream::connect(addr).await.unwrap();
    cursor_client
        .write_all(&startup_bytes("cursor_owner"))
        .await
        .unwrap();
    read_until_ready(&mut cursor_client).await;
    let mut vacuum_client = TcpStream::connect(addr).await.unwrap();
    vacuum_client
        .write_all(&startup_bytes("vacuum_owner"))
        .await
        .unwrap();
    read_until_ready(&mut vacuum_client).await;

    for sql in [
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2)",
    ] {
        cursor_client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut cursor_client).await;
    }
    assert_eq!(app_for_assert.components.active_txns.oldest_xmin(), None);

    cursor_client
        .write_all(&query_bytes("begin"))
        .await
        .unwrap();
    read_until_ready_any(&mut cursor_client).await;
    cursor_client
        .write_all(&query_bytes(
            "declare c cursor for select id from users order by id",
        ))
        .await
        .unwrap();
    read_until_ready_any(&mut cursor_client).await;
    wait_for_advertised_snapshot(&app_for_assert).await;

    vacuum_client
        .write_all(&query_bytes("delete from users where id = 1"))
        .await
        .unwrap();
    read_until_ready(&mut vacuum_client).await;
    vacuum_client
        .write_all(&query_bytes("vacuum users"))
        .await
        .unwrap();

    cursor_client
        .write_all(&query_bytes("fetch all from c"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut cursor_client).await;
    assert_eq!(
        data_row_text_fields(&response),
        vec![vec!["1".to_string()], vec!["2".to_string()]],
        "cursor snapshot must retain rows visible at DECLARE despite later VACUUM"
    );
    cursor_client
        .write_all(&query_bytes("commit"))
        .await
        .unwrap();
    read_until_ready(&mut cursor_client).await;
    read_until_ready(&mut vacuum_client).await;
    wait_for_no_advertised_snapshot(&app_for_assert).await;

    cursor_client.write_all(&terminate_bytes()).await.unwrap();
    vacuum_client.write_all(&terminate_bytes()).await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn extended_protocol_rejects_sql_cursor_parse() {
    let mut test = open_loopback_test().await;

    let mut seq = parse_bytes("", "declare c cursor for select 1", &[]);
    seq.extend(sync_bytes());
    test.client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut test.client).await;

    assert!(response.windows(5).any(|w| w == b"0A000"));
    test.client.write_all(&terminate_bytes()).await.unwrap();
    test.server.await.unwrap();
}

#[tokio::test]
async fn system_information_functions_use_startup_session_info() {
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
    client
        .write_all(&startup_bytes_with_database("driver_user", "driver_db"))
        .await
        .unwrap();
    let startup = read_until_ready(&mut client).await;
    let backend_pid = backend_pid_from_startup(&startup);

    client
        .write_all(&query_bytes(
            "select current_user, current_database(), current_catalog, current_schema, \
             pg_backend_pid()",
        ))
        .await
        .unwrap();
    let response = read_until_ready(&mut client).await;
    let rows = data_row_text_fields(&response);

    assert_eq!(
        rows,
        vec![vec![
            "driver_user".to_string(),
            "driver_db".to_string(),
            "driver_db".to_string(),
            "public".to_string(),
            backend_pid.to_string(),
        ]]
    );

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
    startup_bytes_with_database_opt(user, None)
}

fn startup_bytes_with_database(user: &str, database: &str) -> Vec<u8> {
    startup_bytes_with_database_opt(user, Some(database))
}

fn startup_bytes_with_database_opt(user: &str, database: Option<&str>) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&196608i32.to_be_bytes());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.push(0);
    if let Some(database) = database {
        body.extend_from_slice(b"database\0");
        body.extend_from_slice(database.as_bytes());
        body.push(0);
    }
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

struct LoopbackTest {
    _dir: tempfile::TempDir,
    client: TcpStream,
    server: tokio::task::JoinHandle<()>,
}

async fn open_loopback_test() -> LoopbackTest {
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

    LoopbackTest {
        _dir: dir,
        client,
        server,
    }
}

fn terminate_bytes() -> Vec<u8> {
    vec![b'X', 0, 0, 0, 4]
}

fn backend_pid_from_startup(response: &[u8]) -> i32 {
    backend_key_from_startup(response).0
}

fn backend_key_from_startup(response: &[u8]) -> (i32, i32) {
    let mut offset = 0;
    while offset + 5 <= response.len() {
        let tag = response[offset];
        let len = i32::from_be_bytes(response[offset + 1..offset + 5].try_into().unwrap());
        let len = usize::try_from(len).unwrap();
        let body_start = offset + 5;
        let body_end = offset + 1 + len;
        if tag == b'K' {
            return (
                i32::from_be_bytes(response[body_start..body_start + 4].try_into().unwrap()),
                i32::from_be_bytes(response[body_start + 4..body_start + 8].try_into().unwrap()),
            );
        }
        offset = body_end;
    }
    panic!("startup response did not include BackendKeyData");
}

fn data_row_text_fields(response: &[u8]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut offset = 0;
    while offset + 5 <= response.len() {
        let tag = response[offset];
        let len = i32::from_be_bytes(response[offset + 1..offset + 5].try_into().unwrap());
        let len = usize::try_from(len).unwrap();
        let body_start = offset + 5;
        let body_end = offset + 1 + len;
        if tag == b'D' {
            let mut body = body_start;
            let fields = u16::from_be_bytes(response[body..body + 2].try_into().unwrap());
            body += 2;
            let mut row = Vec::new();
            for _ in 0..fields {
                let field_len = i32::from_be_bytes(response[body..body + 4].try_into().unwrap());
                body += 4;
                assert!(field_len >= 0, "test query should not return NULL fields");
                let field_len = usize::try_from(field_len).unwrap();
                let text = std::str::from_utf8(&response[body..body + field_len]).unwrap();
                row.push(text.to_string());
                body += field_len;
            }
            rows.push(row);
        }
        offset = body_end;
    }
    rows
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
    execute_bytes_with_max(portal, 0)
}

fn execute_bytes_with_max(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(&max_rows.to_be_bytes());
    tagged(b'E', &body)
}

fn sync_bytes() -> Vec<u8> {
    tagged(b'S', &[])
}

fn flush_bytes() -> Vec<u8> {
    tagged(b'H', &[])
}

fn close_portal_bytes(name: &str) -> Vec<u8> {
    let mut body = vec![b'P'];
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    tagged(b'C', &body)
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
    let cancel = Arc::new(QueryCancel::new());
    let key = app
        .components
        .cancel_registry
        .register(cancel.clone(), Arc::new(tokio::sync::Notify::new()));

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
    assert_eq!(
        cancel.reason(),
        Some(CancelReason::UserRequest),
        "target backend was signaled"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn idle_cancel_does_not_interrupt_standalone_extended_close() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut connections = Vec::new();
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let app = app.clone();
            connections.push(tokio::spawn(async move {
                handle_connection(socket, app).await.unwrap();
            }));
        }
        for connection in connections {
            connection.await.unwrap();
        }
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&startup_bytes("dave")).await.unwrap();
    let startup = read_until_ready(&mut client).await;
    let (process_id, secret_key) = backend_key_from_startup(&startup);

    let mut setup = parse_bytes("statement", "select 1", &[]);
    setup.extend(bind_bytes("portal", "statement", &[], &[], &[]));
    setup.extend(sync_bytes());
    client.write_all(&setup).await.unwrap();
    read_until_ready(&mut client).await;

    let mut cancel = TcpStream::connect(addr).await.unwrap();
    cancel
        .write_all(&cancel_request_bytes(process_id, secret_key))
        .await
        .unwrap();
    let mut eof = [0; 1];
    assert_eq!(cancel.read(&mut eof).await.unwrap(), 0);

    let mut close = close_portal_bytes("portal");
    close.extend(sync_bytes());
    client.write_all(&close).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(message_tag_count(&response, b'3'), 1);

    client.write_all(&query_bytes("select 1")).await.unwrap();
    read_until_ready(&mut client).await;
    client.write_all(&terminate_bytes()).await.unwrap();
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

    // Driver-common array parameter: declared int4[] OID, text array payload.
    let mut seq = parse_bytes("", "select name from users where id = ANY($1)", &[1007]);
    seq.extend(bind_bytes("", "", &[0], &[Some(b"{2,3}")], &[0]));
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(2).any(|w| w == b"Bo"),
        "array-parameter row value"
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

    // Binary INTEGER result column: `id` is INTEGER (int4), so the value is the
    // 4-byte big-endian encoding, distinguishing binary from text result encoding.
    let mut seq = parse_bytes("", "select id from users where id = $1", &[20]);
    seq.extend(bind_bytes("", "", &[1], &[Some(&id[..])], &[1]));
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    // The DataRow field is a 4-byte length prefix (int4 width) followed by the
    // 4-byte value; asserting the pair distinguishes int4 from an int8 regression
    // (whose length prefix would be 8), not merely binary from text.
    let mut expected_field = 4i32.to_be_bytes().to_vec();
    expected_field.extend_from_slice(&2i32.to_be_bytes());
    assert!(
        response.windows(8).any(|w| w == expected_field),
        "binary int4 result value (length 4 + value)"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_parse_timeout_reports_error_while_waiting_for_sync() {
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
    client
        .write_all(&query_bytes("set statement_timeout = '100 ms'"))
        .await
        .unwrap();
    read_until_ready(&mut client).await;

    client
        .write_all(&parse_bytes("waiting", "select 1", &[]))
        .await
        .unwrap();
    let response = read_until_message(&mut client, b"57014").await;
    assert!(
        response
            .windows(5)
            .any(|window| window == [b'1', 0, 0, 0, 4]),
        "Parse completes before the idle extended cycle times out"
    );
    assert!(
        response
            .windows(b"statement timeout".len())
            .any(|window| window == b"statement timeout")
    );

    let mut recovery = tagged(b'd', b"unexpected copy data");
    recovery.extend(query_bytes(
        "create table skipped_query (id integer primary key)",
    ));
    recovery.extend(sync_bytes());
    client.write_all(&recovery).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_ready_status(&response, b'I');
    assert_eq!(
        message_tag_count(&response, b'Z'),
        1,
        "the skipped simple Query must not emit its own ReadyForQuery"
    );

    client
        .write_all(&query_bytes(
            "create table skipped_query (id integer primary key)",
        ))
        .await
        .unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response
            .windows(13)
            .any(|window| window == b"CREATE TABLE\0"),
        "the Query sent before Sync was skipped"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_execute_max_rows_suspends_and_resumes_portal() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3), (4), (5)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("", "select id from users order by id", &[]);
    seq.extend(bind_bytes("", "", &[], &[], &[0]));
    seq.extend(describe_portal_bytes(""));
    seq.extend(execute_bytes_with_max("", 2));
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;
    assert_eq!(message_tag_count(&response, b'D'), 2);
    assert!(
        response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "PortalSuspended"
    );

    let mut seq = execute_bytes_with_max("", 2);
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;
    assert_eq!(message_tag_count(&response, b'D'), 2);

    let mut seq = execute_bytes("");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(
        response.windows(9).any(|w| w == b"SELECT 5\0"),
        "final CommandComplete reports total rows"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_autocommit_sync_closes_suspended_portal() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("", "select id from users order by id", &[]);
    seq.extend(bind_bytes("", "", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "PortalSuspended before Sync cleanup"
    );

    let mut seq = execute_bytes("");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    let missing_portal = b"portal \"\" does not exist";
    assert!(
        response
            .windows(missing_portal.len())
            .any(|w| w == missing_portal),
        "autocommit Sync closed the suspended portal"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_transaction_suspended_portal_survives_sync() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
        "begin",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready_any(&mut client).await;
    }

    let mut seq = parse_bytes("", "select id from users order by id", &[]);
    seq.extend(bind_bytes("", "", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert!(
        response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "PortalSuspended before ReadyForQuery"
    );
    assert!(
        response.windows(6).any(|w| w == b"Z\0\0\0\x05T"),
        "transaction remains open"
    );

    let mut seq = execute_bytes("");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 2);
    assert!(
        response.windows(9).any(|w| w == b"SELECT 3\0"),
        "remaining rows drained after Sync"
    );

    client.write_all(&query_bytes("commit")).await.unwrap();
    read_until_ready(&mut client).await;
    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_transaction_suspended_portal_closes_on_commit() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
        "begin",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready_any(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert!(
        response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "PortalSuspended before COMMIT"
    );

    let mut seq = parse_bytes("commit_stmt", "commit", &[]);
    seq.extend(bind_bytes("", "commit_stmt", &[], &[], &[]));
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    read_until_ready(&mut client).await;

    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    let missing_portal = b"portal \"rows\" does not exist";
    assert!(
        response
            .windows(missing_portal.len())
            .any(|w| w == missing_portal),
        "COMMIT closed the transaction-scoped suspended portal"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_rollback_to_savepoint_closes_transaction_suspended_portal() {
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
        "create table users (id integer primary key)",
        "begin",
        "insert into users (id) values (1)",
        "savepoint s",
        "insert into users (id) values (2)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready_any(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(
        response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "PortalSuspended before ROLLBACK TO"
    );

    client
        .write_all(&query_bytes("rollback to savepoint s"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert!(
        response.windows(9).any(|w| w == b"ROLLBACK\0"),
        "ROLLBACK TO completed successfully"
    );
    assert!(
        response.windows(6).any(|w| w == b"Z\0\0\0\x05T"),
        "transaction remains open after ROLLBACK TO"
    );

    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert_eq!(
        message_tag_count(&response, b'D'),
        0,
        "stale savepoint cursor must not resume rows"
    );
    let missing_portal = b"portal \"rows\" does not exist";
    assert!(
        response
            .windows(missing_portal.len())
            .any(|w| w == missing_portal),
        "ROLLBACK TO closed the transaction-scoped suspended portal"
    );

    client.write_all(&query_bytes("commit")).await.unwrap();
    read_until_ready(&mut client).await;
    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_suspended_portal_rejects_resume_in_failed_transaction() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
        "begin",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready_any(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert!(
        response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "PortalSuspended before transaction failure"
    );

    client
        .write_all(&query_bytes("insert into users (id) values (1)"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert!(
        response.windows(6).any(|w| w == b"Z\0\0\0\x05E"),
        "duplicate insert leaves the transaction failed"
    );

    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert_eq!(
        message_tag_count(&response, b'D'),
        0,
        "failed transaction resume must not return rows"
    );
    assert!(
        response.windows(5).any(|w| w == b"25P02"),
        "resume in failed transaction reports 25P02"
    );
    assert!(
        response.windows(6).any(|w| w == b"Z\0\0\0\x05E"),
        "failed transaction remains failed after rejected resume"
    );

    client.write_all(&query_bytes("rollback")).await.unwrap();
    read_until_ready(&mut client).await;
    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    let missing_portal = b"portal \"rows\" does not exist";
    assert!(
        response
            .windows(missing_portal.len())
            .any(|w| w == missing_portal),
        "ROLLBACK closed the transaction-scoped suspended portal"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_limited_execute_in_failed_transaction_preserves_bound_portal() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
        "begin",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready_any(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    read_until_ready_any(&mut client).await;

    client
        .write_all(&query_bytes("insert into users (id) values (1)"))
        .await
        .unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert!(
        response.windows(6).any(|w| w == b"Z\0\0\0\x05E"),
        "duplicate insert leaves the transaction failed"
    );

    let mut seq = execute_bytes_with_max("rows", 1);
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready_any(&mut client).await;
    assert_eq!(
        message_tag_count(&response, b'D'),
        0,
        "failed transaction limited Execute must not return rows"
    );
    assert!(
        response.windows(5).any(|w| w == b"25P02"),
        "limited Execute in failed transaction reports 25P02"
    );

    client.write_all(&query_bytes("rollback")).await.unwrap();
    read_until_ready(&mut client).await;

    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(
        message_tag_count(&response, b'D'),
        3,
        "rejected limited Execute left the bound portal intact"
    );
    assert!(
        response.windows(9).any(|w| w == b"SELECT 3\0"),
        "bound portal executes normally after rollback"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_simple_query_closes_autocommit_suspended_portal() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;
    assert!(
        response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "PortalSuspended before simple query"
    );

    client.write_all(&query_bytes("select 42")).await.unwrap();
    read_until_ready(&mut client).await;

    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    let missing_portal = b"portal \"rows\" does not exist";
    assert!(
        response
            .windows(missing_portal.len())
            .any(|w| w == missing_portal),
        "simple query closed the autocommit suspended portal"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_max_rows_does_not_suspend_sequence_mutating_select() {
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
        "create sequence users_id_seq",
        "create table anchor (id integer primary key)",
        "insert into anchor (id) values (1)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("", "select nextval('users_id_seq') from anchor", &[]);
    seq.extend(bind_bytes("", "", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(
        response.windows(9).any(|w| w == b"SELECT 1\0"),
        "sequence-mutating SELECT still executes normally"
    );
    assert!(
        !response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "sequence-mutating SELECT is not portal-suspended"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_limited_execute_exhaustion_closes_portal() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 10));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 3);
    assert!(
        response.windows(9).any(|w| w == b"SELECT 3\0"),
        "limited Execute exhausted the portal"
    );
    assert!(
        !response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "exhausted limited Execute does not suspend"
    );

    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    let missing_portal = b"portal \"rows\" does not exist";
    assert!(
        response
            .windows(missing_portal.len())
            .any(|w| w == missing_portal),
        "exhausted limited Execute closed the portal"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_max_rows_does_not_suspend_explain() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("", "explain select id from users order by id", &[]);
    seq.extend(bind_bytes("", "", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(
        response.windows(8).any(|w| w == b"EXPLAIN\0"),
        "EXPLAIN completes normally"
    );
    assert!(
        !response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "EXPLAIN is not portal-suspended"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_suspended_portal_preserves_binary_result_formats() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("", "select id from users order by id", &[]);
    seq.extend(bind_bytes("", "", &[], &[], &[1]));
    seq.extend(execute_bytes_with_max("", 1));
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(contains_binary_int4_field(&response, 1));

    let mut seq = execute_bytes_with_max("", 1);
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(contains_binary_int4_field(&response, 2));

    let mut seq = execute_bytes("");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(contains_binary_int4_field(&response, 3));
    assert!(
        response.windows(9).any(|w| w == b"SELECT 3\0"),
        "final CommandComplete reports total rows"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_close_drops_suspended_portal() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;

    let mut seq = close_portal_bytes("rows");
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    read_until_message(&mut client, &[b'3', 0, 0, 0, 4]).await;

    let mut seq = execute_bytes("rows");
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    let missing_portal = b"portal \"rows\" does not exist";
    assert!(
        response
            .windows(missing_portal.len())
            .any(|w| w == missing_portal),
        "Close Portal dropped the suspended portal"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_rebind_replaces_suspended_portal() {
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;

    let mut seq = parse_bytes("one_stmt", "select 99", &[]);
    seq.extend(bind_bytes("rows", "one_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes("rows"));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert_eq!(message_tag_count(&response, b'D'), 1);
    assert!(
        response.windows(2).any(|w| w == b"99"),
        "replacement Bind executes the new portal"
    );
    assert!(
        response.windows(9).any(|w| w == b"SELECT 1\0"),
        "replacement portal completed normally"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_disconnect_closes_suspended_worker_and_releases_gc_pin() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let app_for_assert = app.clone();
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
        "create table users (id integer primary key)",
        "insert into users (id) values (1), (2), (3)",
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }
    assert_eq!(
        app_for_assert.components.active_txns.oldest_xmin(),
        None,
        "setup should leave no advertised snapshot"
    );

    let mut seq = parse_bytes("rows_stmt", "select id from users order by id", &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(flush_bytes());
    client.write_all(&seq).await.unwrap();
    read_until_message(&mut client, &[b's', 0, 0, 0, 4]).await;
    wait_for_advertised_snapshot(&app_for_assert).await;

    drop(client);
    server.await.unwrap();
    wait_for_no_advertised_snapshot(&app_for_assert).await;
}

#[tokio::test]
async fn extended_protocol_cancel_request_aborts_active_suspended_portal_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
    let app_for_assert = app.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = {
        let app = app.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let app = app.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(socket, app).await;
                });
            }
        })
    };

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&startup_bytes("dave")).await.unwrap();
    let startup = read_until_ready(&mut client).await;
    let (process_id, secret_key) = backend_key_from_startup(&startup);
    client
        .write_all(&query_bytes("create table users (id integer primary key)"))
        .await
        .unwrap();
    read_until_ready(&mut client).await;

    const ROW_COUNT: i32 = 10_000;
    const CHUNK: i32 = 1_000;
    for start in (1..=ROW_COUNT).step_by(CHUNK as usize) {
        let end = (start + CHUNK - 1).min(ROW_COUNT);
        let values = (start..=end)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(", ");
        client
            .write_all(&query_bytes(&format!(
                "insert into users (id) values {values}"
            )))
            .await
            .unwrap();
        read_until_ready_with_timeout(&mut client, Duration::from_secs(10)).await;
    }

    let sql = format!("select id from users where id + 0 = {ROW_COUNT}");
    let mut seq = parse_bytes("rows_stmt", &sql, &[]);
    seq.extend(bind_bytes("rows", "rows_stmt", &[], &[], &[0]));
    seq.extend(execute_bytes_with_max("rows", 1));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();

    wait_for_advertised_snapshot(&app_for_assert).await;
    let mut cancel = TcpStream::connect(addr).await.unwrap();
    cancel
        .write_all(&cancel_request_bytes(process_id, secret_key))
        .await
        .unwrap();
    let mut eof = [0; 1];
    assert_eq!(
        cancel.read(&mut eof).await.unwrap(),
        0,
        "CancelRequest connection closes without a reply"
    );

    let response = read_until_ready_with_timeout(&mut client, Duration::from_secs(10)).await;
    assert_eq!(
        message_tag_count(&response, b'D'),
        0,
        "canceled portal fetch must not return rows"
    );
    assert!(
        !response.windows(5).any(|w| w == [b's', 0, 0, 0, 4]),
        "canceled portal fetch must not suspend"
    );
    assert!(
        response.windows(5).any(|w| w == b"57014"),
        "canceled portal fetch reports QueryCanceled"
    );
    wait_for_no_advertised_snapshot(&app_for_assert).await;

    client.write_all(&terminate_bytes()).await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn extended_protocol_parameterized_toasted_text_and_bytea_round_trip() {
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
    client
        .write_all(&query_bytes(
            "create table docs (id integer primary key, body text, payload bytea) \
             with (toast = aggressive, toast_tuple_target = 512, \
                   toast_min_value_size = 128, toast_compression = none)",
        ))
        .await
        .unwrap();
    read_until_ready(&mut client).await;

    let body = "extended-param-toast-body-".repeat(150);
    let payload = vec![0xcd; 1400];
    let expected_payload = format!("\\x{}", "cd".repeat(payload.len()));

    let mut seq = parse_bytes(
        "",
        "insert into docs (id, body, payload) values ($1, $2, $3) returning body, payload",
        &[20, 25, 17],
    );
    seq.extend(bind_bytes(
        "",
        "",
        &[0, 0, 1],
        &[Some(b"1"), Some(body.as_bytes()), Some(&payload)],
        &[0, 0],
    ));
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response
            .windows(body.len())
            .any(|window| window == body.as_bytes()),
        "text parameter round-tripped through a returned toasted row"
    );
    assert!(
        response
            .windows(expected_payload.len())
            .any(|window| window == expected_payload.as_bytes()),
        "binary bytea parameter round-tripped through a returned toasted row"
    );

    let mut seq = parse_bytes("", "select body, payload from docs where id = $1", &[20]);
    seq.extend(bind_bytes("", "", &[0], &[Some(b"1")], &[0, 0]));
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response
            .windows(body.len())
            .any(|window| window == body.as_bytes()),
        "text parameter stayed readable after storage"
    );
    assert!(
        response
            .windows(expected_payload.len())
            .any(|window| window == expected_payload.as_bytes()),
        "bytea parameter stayed readable after storage"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

/// A JDBC-style client that declares an `int4` parameter (OID 23) and binds it in
/// binary (4 bytes): the `Parse` is accepted (previously rejected as an
/// "unsupported parameter type OID"), `ParameterDescription` echoes int4, and the
/// 4-byte binary bind decodes correctly.
#[tokio::test]
async fn extended_protocol_accepts_int4_parameter_oid_and_echoes_it() {
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
    ] {
        client.write_all(&query_bytes(sql)).await.unwrap();
        read_until_ready(&mut client).await;
    }

    // Parse declaring $1 as int4 (OID 23), then Describe the statement.
    let mut seq = parse_bytes("", "select name from users where id = $1", &[23]);
    seq.extend(describe_statement_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(5).any(|w| w == [b'1', 0, 0, 0, 4]),
        "ParseComplete (int4 OID accepted)"
    );
    // ParameterDescription ('t') echoes the declared int4 OID: count 1, then OID 23.
    assert!(
        response.windows(6).any(|w| w == [0, 1, 0, 0, 0, 23]),
        "ParameterDescription echoes int4 OID 23"
    );

    // Bind $1 as a 4-byte binary int4 value and execute.
    let id = 1i32.to_be_bytes();
    let mut seq = bind_bytes("", "", &[1], &[Some(&id[..])], &[0]);
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(response.windows(3).any(|w| w == b"Ada"), "row value");

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

/// Catalog probes commonly declare parameters as `oid` (OID 26). The binder
/// treats it as SaguaroDB's integer storage type, but the protocol must still
/// accept and echo the OID wire identity and decode binary OIDs as unsigned
/// 32-bit values.
#[tokio::test]
async fn extended_protocol_accepts_oid_parameter_oid_and_binary_value() {
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

    let mut seq = parse_bytes("", "select $1", &[26]);
    seq.extend(describe_statement_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(5).any(|w| w == [b'1', 0, 0, 0, 4]),
        "ParseComplete (oid OID accepted)"
    );
    assert!(
        response.windows(6).any(|w| w == [0, 1, 0, 0, 0, 26]),
        "ParameterDescription echoes oid OID 26"
    );
    assert!(
        response
            .windows(12)
            .any(|w| w == [0, 0, 0, 26, 0, 4, 255, 255, 255, 255, 0, 0]),
        "RowDescription reports selected oid parameter as oid"
    );

    let oid = 4_000_000_000u32.to_be_bytes();
    let mut seq = bind_bytes("", "", &[1], &[Some(&oid[..])], &[1]);
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response
            .windows(8)
            .any(|w| w == [0, 0, 0, 4, 0xee, 0x6b, 0x28, 0x00]),
        "binary oid result encodes as unsigned 32-bit value"
    );

    let mut seq = parse_bytes("", "select pg_table_is_visible($1)", &[0]);
    seq.extend(describe_statement_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(6).any(|w| w == [0, 1, 0, 0, 0, 26]),
        "ParameterDescription infers oid OID 26 for catalog function argument"
    );

    let oid = 4_000_000_000u32.to_be_bytes();
    let mut seq = bind_bytes("", "", &[1], &[Some(&oid[..])], &[0]);
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(7).any(|w| w == [0, 1, 0, 0, 0, 1, b'f']),
        "inferred oid parameter decodes binary unsigned value and returns false"
    );

    client.write_all(&terminate_bytes()).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn extended_protocol_accepts_catalog_vector_parameter_oids_as_text() {
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

    let mut seq = parse_bytes("", "select oidvectortypes($1)", &[30]);
    seq.extend(describe_statement_bytes(""));
    seq.extend(bind_bytes("", "", &[0], &[Some(b"23 26")], &[0]));
    seq.extend(execute_bytes(""));
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(6).any(|w| w == [0, 1, 0, 0, 0, 30]),
        "ParameterDescription echoes oidvector OID 30"
    );
    assert!(
        response.windows(12).any(|w| w == b"integer, oid"),
        "oidvector parameter is decoded as text-backed catalog value"
    );

    let mut seq = bind_bytes("", "", &[1], &[Some(b"23 26")], &[0]);
    seq.extend(sync_bytes());
    client.write_all(&seq).await.unwrap();
    let response = read_until_ready(&mut client).await;
    assert!(
        response.windows(21).any(|w| w == b"binary catalog vector"),
        "binary catalog vector parameter is rejected"
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
    read_until_ready_with_timeout(client, Duration::from_secs(2)).await
}

async fn read_until_ready_with_timeout<S: AsyncRead + Unpin>(
    client: &mut S,
    timeout: Duration,
) -> Vec<u8> {
    let mut response = Vec::new();
    let mut buf = [0; 1024];
    tokio::time::timeout(timeout, async {
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

async fn read_until_message<S: AsyncRead + Unpin>(client: &mut S, needle: &[u8]) -> Vec<u8> {
    let mut response = Vec::new();
    let mut buf = [0; 1024];
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let read = client.read(&mut buf).await.unwrap();
            assert_ne!(read, 0, "connection closed before expected message");
            response.extend_from_slice(&buf[..read]);
            if response
                .windows(needle.len())
                .any(|window| window == needle)
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    response
}

fn message_tag_count(response: &[u8], tag: u8) -> usize {
    let mut count = 0;
    let mut offset = 0;
    while offset + 5 <= response.len() {
        if response[offset] == tag {
            count += 1;
        }
        let len = i32::from_be_bytes(
            response[offset + 1..offset + 5]
                .try_into()
                .expect("message length bytes"),
        );
        if len < 4 {
            break;
        }
        offset += 1 + len as usize;
    }
    count
}

fn assert_ready_status(response: &[u8], status: u8) {
    assert!(
        response
            .windows(6)
            .any(|window| window == [b'Z', 0, 0, 0, 5, status]),
        "ReadyForQuery status {} not found in response",
        status as char
    );
}

async fn assert_cursor_missing_in_new_transaction(client: &mut TcpStream, name: &str) {
    client.write_all(&query_bytes("begin")).await.unwrap();
    read_until_ready_any(client).await;
    client
        .write_all(&query_bytes(&format!("fetch from {name}")))
        .await
        .unwrap();
    let response = read_until_ready_any(client).await;
    assert!(response.windows(5).any(|w| w == b"34000"));
    client.write_all(&query_bytes("rollback")).await.unwrap();
    read_until_ready(client).await;
}

fn contains_binary_int4_field(response: &[u8], value: i32) -> bool {
    let mut expected = 4i32.to_be_bytes().to_vec();
    expected.extend_from_slice(&value.to_be_bytes());
    response
        .windows(expected.len())
        .any(|window| window == expected)
}

async fn wait_for_advertised_snapshot(app: &AppState) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if app.components.active_txns.oldest_xmin().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cursor worker did not advertise a snapshot");
}

async fn wait_for_no_advertised_snapshot(app: &AppState) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if app.components.active_txns.oldest_xmin().is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cursor worker did not release its snapshot pin");
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
