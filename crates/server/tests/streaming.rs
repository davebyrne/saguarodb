//! End-to-end tests for the SELECT streaming bridge (`docs/specs/streaming.md`).
//! The wire output is identical to the old materializing path, so these focus on
//! streaming-specific risks: many batches, backpressure without deadlock, empty
//! results, and the in-transaction read path.

mod support;

use support::{Connection, TestServer};

/// A result set far larger than the channel can buffer must come back complete
/// and in order. With a bounded channel of 64 batches × 64 rows, the producer can
/// buffer at most ~4096 rows before it blocks on `blocking_send`; 5000 rows forces
/// that block, so this both checks multi-batch ordering and guards against a
/// regression that awaited the producer task before draining (which would deadlock
/// the moment the channel filled, tripping the ReadyForQuery timeout).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_select_streams_all_rows_in_order() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    const N: i64 = 5000;
    let mut next = 1;
    while next <= N {
        let end = (next + 999).min(N);
        let values = (next..=end)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.ok(&format!("insert into t (id) values {values}"))
            .await;
        next = end + 1;
    }

    let outcome = conn.ok("select id from t order by id").await;
    assert_eq!(outcome.status, b'I');
    let rows = outcome.rows();
    assert_eq!(rows.len(), N as usize, "every streamed row must arrive");
    // Order is preserved across every batch boundary.
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.as_slice(), [Some((index as i64 + 1).to_string())]);
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
