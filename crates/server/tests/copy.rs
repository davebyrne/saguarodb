mod support;

use support::{Connection, TestServer};

async fn server_with_table() -> (TestServer, Connection) {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, name text)")
        .await;
    (server, conn)
}

#[tokio::test]
async fn copy_from_stdin_text_inserts_rows() {
    let (_server, mut conn) = server_with_table().await;

    let copy = conn
        .copy_from("copy t from stdin", &[b"1\tann\n2\t\\N\n"])
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 2"));
    assert_eq!(copy.status, b'I', "autocommit COPY returns to idle");

    let rows = conn.ok("select id, name from t order by id").await.rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("ann".to_string())],
            vec![Some("2".to_string()), None],
        ]
    );
}

#[tokio::test]
async fn copy_from_stdin_csv_skips_header_and_splits_chunks() {
    let (_server, mut conn) = server_with_table().await;

    // The single data row is split across two CopyData frames; the header is skipped.
    let copy = conn
        .copy_from(
            "copy t (id, name) from stdin with (format csv, header true)",
            &[b"id,name\n7,da", b"ve\n"],
        )
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 1"));

    let rows = conn.ok("select name from t where id = 7").await.rows();
    assert_eq!(rows, vec![vec![Some("dave".to_string())]]);
}

#[tokio::test]
async fn copy_to_stdout_text_and_csv() {
    let (_server, mut conn) = server_with_table().await;
    conn.ok("insert into t (id, name) values (1, 'ann'), (2, 'bob')")
        .await;

    let (data, copy) = conn.copy_to("copy t to stdout").await.unwrap();
    assert_eq!(data, b"1\tann\n2\tbob\n");
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 2"));
    assert_eq!(copy.status, b'I');

    let (data, _) = conn
        .copy_to("copy t to stdout with (format csv, header true)")
        .await
        .unwrap();
    assert_eq!(data, b"id,name\n1,ann\n2,bob\n");

    // Column subset + reorder.
    let (data, _) = conn
        .copy_to("copy t (name, id) to stdout with (format csv)")
        .await
        .unwrap();
    assert_eq!(data, b"ann,1\nbob,2\n");
}

#[tokio::test]
async fn copy_from_round_trips_through_copy_to() {
    let (_server, mut conn) = server_with_table().await;
    let payload: &[u8] = b"10\thello world\n11\ttab\\there\n12\t\\N\n";
    conn.copy_from("copy t from stdin", &[payload])
        .await
        .unwrap();

    let (data, _) = conn.copy_to("copy t to stdout").await.unwrap();
    assert_eq!(data, payload);
}

#[tokio::test]
async fn copy_from_bad_value_aborts_whole_copy() {
    let (_server, mut conn) = server_with_table().await;

    let copy = conn
        .copy_from("copy t from stdin", &[b"1\tann\nnope\tbob\n"])
        .await
        .unwrap();
    assert_eq!(
        copy.error_code.as_deref(),
        Some("22P02"),
        "bad integer is InvalidTextRepresentation"
    );

    // All-or-nothing: the first (valid) row was rolled back too.
    let rows = conn.ok("select id from t").await.rows();
    assert!(rows.is_empty(), "aborted COPY left no rows, got {rows:?}");
}

#[tokio::test]
async fn copy_from_enforces_varchar_length() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, name varchar(3))")
        .await;

    // An in-limit value imports fine.
    let ok = conn
        .copy_from("copy t from stdin", &[b"1\tabc\n"])
        .await
        .unwrap();
    assert_eq!(ok.command_tag.as_deref(), Some("COPY 1"));

    // An over-limit value aborts the COPY with 22001 and persists nothing from it.
    let bad = conn
        .copy_from("copy t from stdin", &[b"2\tabcd\n"])
        .await
        .unwrap();
    assert_eq!(
        bad.error_code.as_deref(),
        Some("22001"),
        "over-length VARCHAR in COPY is string_data_right_truncation"
    );

    let rows = conn.ok("select id from t order by id").await.rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

#[tokio::test]
async fn copy_from_wrong_column_count_is_bad_format() {
    let (_server, mut conn) = server_with_table().await;
    let copy = conn
        .copy_from("copy t from stdin", &[b"1\tann\textra\n"])
        .await
        .unwrap();
    assert_eq!(copy.error_code.as_deref(), Some("22P04"));
}

#[tokio::test]
async fn copy_fail_from_client_aborts() {
    let (_server, mut conn) = server_with_table().await;

    let copy = conn
        .copy_fail("copy t from stdin", &[b"1\tann\n"], "client gave up")
        .await
        .unwrap();
    assert!(copy.error_code.is_some(), "CopyFail surfaces an error");
    assert_eq!(copy.status, b'I');

    let rows = conn.ok("select id from t").await.rows();
    assert!(rows.is_empty(), "CopyFail rolled back the rows");
}

#[tokio::test]
async fn copy_from_inside_transaction_commits_and_rolls_back() {
    let (_server, mut conn) = server_with_table().await;

    // COPY inside a BEGIN block folds into the open transaction.
    assert_eq!(conn.ok("begin").await.status, b'T');
    let copy = conn
        .copy_from("copy t from stdin", &[b"5\teve\n"])
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 1"));
    assert_eq!(copy.status, b'T', "still inside the transaction block");

    // The transaction sees its own copied row.
    let rows = conn.ok("select name from t where id = 5").await.rows();
    assert_eq!(rows, vec![vec![Some("eve".to_string())]]);

    conn.ok("rollback").await;
    let rows = conn.ok("select id from t where id = 5").await.rows();
    assert!(rows.is_empty(), "rolled-back COPY is not durable");

    // And a committed in-transaction COPY persists.
    conn.ok("begin").await;
    conn.copy_from("copy t from stdin", &[b"6\tfrank\n"])
        .await
        .unwrap();
    conn.ok("commit").await;
    let rows = conn.ok("select name from t where id = 6").await.rows();
    assert_eq!(rows, vec![vec![Some("frank".to_string())]]);
}

#[tokio::test]
async fn copy_empty_input_reports_zero() {
    let (_server, mut conn) = server_with_table().await;
    let copy = conn.copy_from("copy t from stdin", &[b""]).await.unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 0"));
}

#[tokio::test]
async fn unsupported_copy_forms_are_rejected() {
    let (_server, mut conn) = server_with_table().await;

    for sql in [
        "copy t from '/tmp/data.csv'",
        "copy t to '/tmp/data.csv'",
        "copy (select id from t) to stdout",
        "copy t from stdin with (format binary)",
    ] {
        let outcome = conn.query(sql).await.unwrap();
        assert!(
            outcome.result.is_err(),
            "expected `{sql}` to be rejected, got {:?}",
            outcome.result.map(|r| r.unwrap_rows())
        );
        // The connection stays usable after a rejected COPY.
        assert_eq!(outcome.status, b'I');
    }
}

#[tokio::test]
async fn copy_in_extended_protocol_is_rejected() {
    let (_server, mut conn) = server_with_table().await;
    let outcome = conn.extended_execute("copy t from stdin").await.unwrap();
    assert!(
        outcome.result.is_err(),
        "COPY must be rejected in the extended query protocol"
    );
}

#[tokio::test]
async fn copy_into_missing_table_errors() {
    let (_server, mut conn) = server_with_table().await;
    let outcome = conn.query("copy missing from stdin").await.unwrap();
    assert!(outcome.result.is_err());
}

// Multi-thread runtime so the 5s safety `timeout` below can fire even if a
// regression makes `run_graceful_shutdown` reach the blocking `run_checkpoint`
// (which would monopolize a single-threaded runtime and defeat the timer),
// matching `shutdown.rs`'s `graceful_shutdown_timeout_does_not_block_on_statement_guard`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_times_out_on_open_copy_from() {
    use saguarodb_server::config::Config;
    use saguarodb_server::shutdown::run_graceful_shutdown;
    use std::time::Duration;

    let server = TestServer::start_with_config(Config {
        shutdown_timeout_ms: 100,
        ..Config::default()
    })
    .await
    .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, name text)")
        .await;
    // Leave a COPY FROM open: the server holds the shared writer guard for it.
    conn.begin_copy_from("copy t from stdin").await.unwrap();

    // The open COPY must count as in-flight, so `wait_for_idle` times out and the
    // (untimed) shutdown checkpoint is skipped rather than blocking forever behind
    // the COPY's writer guard.
    let app = server.app().clone();
    let outcome = tokio::time::timeout(Duration::from_secs(5), run_graceful_shutdown(app))
        .await
        .expect("graceful shutdown hung on an open COPY FROM");
    assert!(
        outcome.unwrap_err().message.contains("timed out waiting"),
        "expected a shutdown timeout while a COPY FROM was open"
    );
}
