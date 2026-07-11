mod support;

use std::time::Duration;

use common::SqlState;

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
async fn statement_timeout_drains_copy_from_and_reports_one_terminal_error() {
    let (_server, mut conn) = server_with_table().await;
    conn.ok("set statement_timeout = '100 ms'").await.rows();
    conn.begin_copy_from("copy t from stdin").await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    let copy = conn.finish_copy_from(&[b"1\ttoo late\n"]).await.unwrap();
    assert_eq!(copy.error_code.as_deref(), Some("57014"));
    assert_eq!(copy.error_count, 1);
    assert_eq!(copy.command_tag, None);
    assert_eq!(copy.status, b'I');

    conn.ok("reset statement_timeout").await.rows();
    assert_eq!(
        conn.ok("select count(*) from t").await.rows(),
        vec![vec![Some("0".to_string())]],
        "timed-out COPY must not commit buffered rows"
    );
}

#[tokio::test]
async fn cancel_request_wakes_idle_copy_from_without_more_copy_data() {
    let (server, mut conn) = server_with_table().await;
    conn.begin_copy_from("copy t from stdin").await.unwrap();
    let (process_id, secret_key) = conn.backend_key();

    server.send_cancel(process_id, secret_key).await.unwrap();
    let err = tokio::time::timeout(Duration::from_secs(2), conn.wait_for_copy_error())
        .await
        .expect("CancelRequest should wake an idle COPY connection")
        .unwrap();
    assert_eq!(err.code, SqlState::QueryCanceled);
    assert!(err.message.contains("user request"));

    server
        .app()
        .components
        .shutdown
        .wait_for_idle(Duration::from_millis(100))
        .await
        .expect("canceled COPY drain state must not remain in flight");

    // ErrorResponse is immediate, but ReadyForQuery remains correctly deferred
    // until COPY's terminator restores framing synchronization.
    let completion = conn.finish_copy_from(&[]).await.unwrap();
    assert_eq!(completion.error_count, 0);
    assert_eq!(completion.status, b'I');
    assert_eq!(
        conn.ok("select count(*) from t").await.rows(),
        vec![vec![Some("0".to_string())]]
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

/// `COPY FROM` enforces the table's `CHECK` constraints per row, aborting the
/// whole COPY (one transaction) with `CheckViolation` (`23514`) on a violating row.
#[tokio::test]
async fn copy_from_enforces_check_constraint() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, n integer check (n > 0))")
        .await;

    // The second row violates `n > 0`; the autocommit COPY aborts and rolls back.
    let copy = conn
        .copy_from("copy t from stdin", &[b"1\t5\n2\t0\n"])
        .await
        .unwrap();
    assert_eq!(
        copy.error_code.as_deref(),
        Some("23514"),
        "expected CheckViolation"
    );
    let rows = conn.ok("select count(*) from t").await.rows();
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "a failed COPY commits no rows"
    );

    // A fully conforming COPY succeeds.
    let copy = conn
        .copy_from("copy t from stdin", &[b"1\t5\n3\t7\n"])
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 2"));
    let rows = conn.ok("select id from t order by id").await.rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("3".to_string())]]
    );
}

/// `COPY FROM` evaluates a non-constant expression `DEFAULT` for each omitted
/// column, per row — the same as INSERT (previously rejected as unsupported).
#[tokio::test]
async fn copy_from_applies_expression_default() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok(
        "create table t (id integer primary key, n integer default 2 * 3, \
         s text not null default upper('hi'))",
    )
    .await;

    // Only `id` is supplied; `n` and `s` take their expression defaults each row.
    let copy = conn
        .copy_from("copy t (id) from stdin", &[b"1\n2\n"])
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 2"));

    let rows = conn.ok("select id, n, s from t order by id").await.rows();
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("6".to_string()),
                Some("HI".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("6".to_string()),
                Some("HI".to_string()),
            ],
        ]
    );
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
async fn repeatable_read_copy_to_newer_catalog_table_is_empty() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table snapshot_anchor (id integer primary key)")
        .await;

    let mut reader = Connection::connect(&server).await.unwrap();
    reader.ok("begin isolation level repeatable read").await;
    assert_eq!(
        reader
            .ok("select count(*) from snapshot_anchor")
            .await
            .rows(),
        vec![vec![Some("0".to_string())]]
    );

    setup
        .ok("create table copied_later (id integer primary key, name text)")
        .await;
    setup
        .ok("insert into copied_later (id, name) values (1, 'ann')")
        .await;

    let (data, copy) = reader.copy_to("copy copied_later to stdout").await.unwrap();
    assert!(data.is_empty());
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 0"));
    reader.ok("rollback").await;
}

#[tokio::test]
async fn repeatable_read_copy_from_newer_catalog_table_is_rejected_before_copy_mode() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table snapshot_anchor (id integer primary key)")
        .await;

    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("begin isolation level repeatable read").await;
    assert_eq!(
        writer
            .ok("select count(*) from snapshot_anchor")
            .await
            .rows(),
        vec![vec![Some("0".to_string())]]
    );

    setup
        .ok("create table copied_later (id integer primary key)")
        .await;

    let outcome = writer.query("copy copied_later from stdin").await.unwrap();
    let err = match outcome.result {
        Ok(_) => panic!("COPY FROM should be rejected before CopyInResponse"),
        Err(err) => err,
    };
    assert_eq!(err.code, SqlState::SerializationFailure);
    assert_eq!(outcome.status, b'E');
    writer.ok("rollback").await;
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
async fn copy_from_toasted_text_and_bytea_round_trips_through_copy_to() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok(
        "create table docs (id integer primary key, body text, payload bytea) \
         with (toast = aggressive, toast_tuple_target = 512, \
               toast_min_value_size = 128, toast_compression = none)",
    )
    .await;

    let body = "copy-toast-body-".repeat(170);
    let payload = format!("\\\\x{}", "ef".repeat(1300));
    let copy_data = format!("1\t{body}\t{payload}\n");

    let copy = conn
        .copy_from("copy docs from stdin", &[copy_data.as_bytes()])
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 1"));

    let (data, copy) = conn
        .copy_to("copy docs to stdout")
        .await
        .expect("COPY TO should materialize toasted values");
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 1"));
    assert_eq!(data, copy_data.as_bytes());
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
async fn disconnect_mid_transaction_copy_from_aborts_transaction() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, name text)")
        .await;

    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("begin").await;
    conn.begin_copy_from("copy t (id, name) from stdin")
        .await
        .unwrap();
    assert_eq!(
        server.active_txn_count(),
        1,
        "transaction should be active while COPY is open"
    );

    conn.close().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if server.active_txn_count() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnect should abort the transaction owned by the COPY task");

    assert!(
        setup.ok("select id from t").await.rows().is_empty(),
        "aborted COPY transaction should not leave rows visible"
    );
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
