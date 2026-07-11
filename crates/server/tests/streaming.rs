//! End-to-end tests for the SELECT streaming bridge (`docs/specs/streaming.md`).
//! The wire output is identical to the old materializing path, so these focus on
//! streaming-specific risks: many batches, backpressure without deadlock, empty
//! results, and the in-transaction read path.

mod support;

use std::time::Duration;

use common::SqlState;
use support::{Connection, TestServer};

/// Rows buffered before the producer blocks: capacity 64 batches × 64 rows/batch.
/// A result larger than this forces the producer into its retrying backpressure loop.
const CHANNEL_ROW_CAPACITY: i64 = 64 * 64;

/// Insert `id` values `1..=n` into a single-column table, in chunks to keep each
/// statement's SQL a reasonable size.
async fn insert_sequential_ids(conn: &mut Connection, n: i64) {
    let mut next = 1;
    while next <= n {
        let end = (next + 999).min(n);
        let values = (next..=end)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.ok(&format!("insert into t (id) values {values}"))
            .await;
        next = end + 1;
    }
}

async fn insert_large_payloads(conn: &mut Connection, n: i64, payload_bytes: usize) {
    let payload = "x".repeat(payload_bytes);
    let mut next = 1;
    while next <= n {
        let end = (next + 19).min(n);
        let values = (next..=end)
            .map(|id| format!("({id},'{payload}')"))
            .collect::<Vec<_>>()
            .join(",");
        conn.ok(&format!(
            "insert into slow_rows (id, payload) values {values}"
        ))
        .await;
        next = end + 1;
    }
}

fn has_complete_frame(bytes: &[u8], expected_tag: u8) -> bool {
    complete_frame_body(bytes, expected_tag).is_some()
}

fn complete_frame_body(bytes: &[u8], expected_tag: u8) -> Option<&[u8]> {
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let tag = bytes[offset];
        let len = i32::from_be_bytes(bytes[offset + 1..offset + 5].try_into().unwrap()) as usize;
        if len < 4 || offset + 1 + len > bytes.len() {
            return None;
        }
        if tag == expected_tag {
            return Some(&bytes[offset + 5..offset + 1 + len]);
        }
        offset += 1 + len;
    }
    None
}

fn assert_statement_timeout(outcome: &support::QueryOutcome) {
    let err = match &outcome.result {
        Err(err) => err,
        Ok(_) => panic!("long-running query should time out"),
    };
    assert_eq!(err.code, SqlState::QueryCanceled);
    assert!(err.message.contains("statement timeout"));
}

/// Assert `rows` is exactly `[[1], [2], … [n]]` as text.
fn assert_ids_in_order(rows: &[Vec<Option<String>>], n: i64) {
    assert_eq!(rows.len(), n as usize, "every streamed row must arrive");
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.as_slice(), [Some((index as i64 + 1).to_string())]);
    }
}

/// A result set far larger than the channel can buffer must come back complete
/// and in order. 5000 rows exceeds the buffer, so the producer applies
/// backpressure while the consumer drains — checking multi-batch ordering and
/// guarding against a regression that awaited the producer task before draining
/// (which would deadlock the moment the channel filled, tripping the timeout).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_select_streams_all_rows_in_order() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    let n = CHANNEL_ROW_CAPACITY + 904; // 5000
    insert_sequential_ids(&mut conn, n).await;

    let outcome = conn.ok("select id from t order by id").await;
    assert_eq!(outcome.status, b'I');
    assert_ids_in_order(&outcome.rows(), n);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_timeout_stops_streamed_select_and_connection_is_reusable() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    insert_sequential_ids(&mut conn, 300).await;
    conn.ok("set statement_timeout = '50 ms'").await.rows();

    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        conn.ok("select a.id from t a cross join t b cross join t c"),
    )
    .await
    .expect("streamed SELECT should observe statement timeout");
    assert_statement_timeout(&outcome);
    assert_eq!(outcome.status, b'I');

    conn.ok("reset statement_timeout").await.rows();
    assert_eq!(
        conn.ok("select count(*) from t").await.rows(),
        vec![vec![Some("300".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_timeout_poisons_transaction_until_rollback() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    insert_sequential_ids(&mut conn, 300).await;
    conn.ok("set statement_timeout = '50 ms'").await.rows();
    assert_eq!(conn.ok("begin").await.status, b'T');

    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        conn.ok("select a.id from t a cross join t b cross join t c"),
    )
    .await
    .expect("in-transaction SELECT should observe statement timeout");
    assert_statement_timeout(&outcome);
    assert_eq!(outcome.status, b'E');
    let rejected = conn.ok("select count(*) from t").await;
    assert!(rejected.result.is_err());
    assert_eq!(rejected.status, b'E');

    assert_eq!(conn.ok("rollback").await.status, b'I');
    conn.ok("reset statement_timeout").await.rows();
    assert_eq!(
        conn.ok("select count(*) from t").await.rows(),
        vec![vec![Some("300".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_statement_timeout_stops_execute_and_sync_recovers() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    insert_sequential_ids(&mut conn, 300).await;
    conn.ok("set statement_timeout = '50 ms'").await.rows();

    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        conn.extended_execute("select a.id from t a cross join t b cross join t c"),
    )
    .await
    .expect("extended Execute should observe statement timeout")
    .unwrap();
    assert_statement_timeout(&outcome);
    assert_eq!(outcome.status, b'I', "Sync restores the extended cycle");

    conn.ok("reset statement_timeout").await.rows();
    assert_eq!(
        conn.extended_execute("select count(*) from t")
            .await
            .unwrap()
            .rows(),
        vec![vec![Some("300".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_timeout_interrupts_active_stream_under_client_backpressure() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table slow_rows (id integer primary key, payload text)")
        .await;
    // Nearly five MiB of protocol payload exceeds the loopback socket's send
    // buffer once the client pauses after its first row.
    insert_large_payloads(&mut conn, 600, 8 * 1024).await;
    conn.ok("set statement_timeout = '200 ms'").await.rows();

    let prefix = conn
        .begin_query_until_data_row("select id, payload from slow_rows")
        .await
        .unwrap();
    assert!(has_complete_frame(&prefix, b'T'), "RowDescription arrived");
    assert!(
        has_complete_frame(&prefix, b'D'),
        "at least one DataRow arrived"
    );

    // Stop draining long enough for server-side channel/socket backpressure and
    // the statement timer to overlap, then accept either a framed timeout reply or
    // the safe connection-close path if cancellation interrupted write_all.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (tail, closed) = conn
        .read_until_ready_or_close(Duration::from_secs(3))
        .await
        .unwrap();
    if !closed {
        let error = complete_frame_body(&tail, b'E')
            .expect("active stream must return a timeout error or close safely");
        assert!(error.windows(5).any(|window| window == b"57014"));
        assert!(
            error
                .windows("statement timeout".len())
                .any(|window| window == b"statement timeout")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_portal_timeout_under_backpressure_keeps_frames_valid_or_closes() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table portal_rows (id integer primary key, payload text)")
        .await;
    let payload = "x".repeat(8 * 1024);
    for start in (1..=600).step_by(20) {
        let end = (start + 19).min(600);
        let values = (start..=end)
            .map(|id| format!("({id},'{payload}')"))
            .collect::<Vec<_>>()
            .join(",");
        conn.ok(&format!(
            "insert into portal_rows (id, payload) values {values}"
        ))
        .await;
    }
    conn.ok("set statement_timeout = '200 ms'").await.rows();

    let prefix = conn
        .begin_extended_until_data_row("select id, payload from portal_rows", 600)
        .await
        .unwrap();
    assert!(has_complete_frame(&prefix, b'D'));

    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = conn.send_extended_sync().await;
    let (tail, closed) = conn
        .read_until_ready_or_close(Duration::from_secs(3))
        .await
        .unwrap();
    if !closed {
        let error = complete_frame_body(&tail, b'E')
            .expect("portal timeout must return a complete error frame or close safely");
        assert!(error.windows(5).any(|window| window == b"57014"));
    }
}

/// An empty SELECT still streams a schema (RowDescription) and a `SELECT 0` tag,
/// and the connection stays usable for the next query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_select_streams_zero_rows_and_leaves_connection_usable() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    conn.ok("insert into t (id) values (1)").await;

    let empty = conn.ok("select id from t where id = 42").await;
    assert_eq!(empty.status, b'I');
    assert!(empty.rows().is_empty(), "no rows match");

    // The connection is intact: a following query streams normally.
    let rows = conn.ok("select id from t order by id").await.rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

/// A multi-row SELECT inside an explicit transaction streams through the
/// in-transaction read path, keeps the block open ('T'), and COMMIT settles it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streamed_select_inside_transaction_preserves_block_status() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    conn.ok("insert into t (id) values (1),(2),(3)").await;

    assert_eq!(conn.ok("begin").await.status, b'T');
    let outcome = conn.ok("select id from t order by id").await;
    assert_eq!(
        outcome.status, b'T',
        "the block stays open after a streamed read"
    );
    assert_eq!(
        outcome.rows(),
        vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("3".to_string())],
        ]
    );
    assert_eq!(conn.ok("commit").await.status, b'I');
}

/// A mid-stream error (division by zero on the second row) aborts the streamed
/// read; in autocommit the connection returns to idle and stays usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streamed_select_error_autocommit_recovers() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, v integer)")
        .await;
    conn.ok("insert into t (id, v) values (1, 5), (2, 0)").await;

    // Row 1 (10/5) streams; row 2 (10/0) fails mid-stream after a DataRow.
    let outcome = conn.ok("select 10 / v from t order by id").await;
    assert!(outcome.result.is_err(), "the streamed read fails");
    assert_eq!(outcome.status, b'I', "autocommit returns to idle");

    // The connection is intact for the next query.
    let rows = conn.ok("select id from t order by id").await.rows();
    assert_eq!(rows.len(), 2);
}

/// The same failing streamed read inside an explicit transaction poisons the
/// block ('E'); only ROLLBACK/COMMIT are then accepted, and ROLLBACK recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streamed_select_error_inside_transaction_poisons_block() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, v integer)")
        .await;
    conn.ok("insert into t (id, v) values (1, 5), (2, 0)").await;

    assert_eq!(conn.ok("begin").await.status, b'T');
    let outcome = conn.ok("select 10 / v from t order by id").await;
    assert!(outcome.result.is_err(), "the streamed read fails");
    assert_eq!(outcome.status, b'E', "the block is poisoned");

    // A further statement is rejected until the block ends.
    let rejected = conn.ok("select id from t").await;
    assert!(
        rejected.result.is_err(),
        "commands are ignored while failed"
    );
    assert_eq!(rejected.status, b'E');

    assert_eq!(conn.ok("rollback").await.status, b'I');
}

/// The extended-protocol `Execute` streams a SELECT too: a result larger than the
/// channel buffer comes back complete and in order over Parse/Bind/Execute/Sync,
/// exercising the same backpressure/no-deadlock path as the simple protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_execute_streams_large_select() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    let n = CHANNEL_ROW_CAPACITY + 904; // 5000
    insert_sequential_ids(&mut conn, n).await;

    let outcome = conn
        .extended_execute("select id from t order by id")
        .await
        .unwrap();
    assert_eq!(outcome.status, b'I');
    assert_ids_in_order(&outcome.rows(), n);
}

/// An extended-protocol SELECT inside an explicit transaction streams through the
/// in-transaction path and keeps the block open ('T') until COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_execute_streams_select_in_transaction() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    conn.ok("insert into t (id) values (1),(2),(3)").await;

    assert_eq!(conn.extended_execute("begin").await.unwrap().status, b'T');
    let outcome = conn
        .extended_execute("select id from t order by id")
        .await
        .unwrap();
    assert_eq!(
        outcome.status, b'T',
        "block stays open after a streamed read"
    );
    assert_ids_in_order(&outcome.rows(), 3);
    assert_eq!(conn.extended_execute("commit").await.unwrap().status, b'I');
}
