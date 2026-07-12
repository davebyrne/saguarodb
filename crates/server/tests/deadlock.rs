mod support;

use std::time::Duration;

use saguarodb_server::config::Config;
use support::{Connection, TestServer};

/// A second writer that hits a row lock held by an in-progress transaction BLOCKS
/// (it does not fail fast); when the holder ROLLS BACK, the blocked writer proceeds.
/// (`docs/specs/deadlock.md`)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_blocks_then_proceeds_when_holder_rolls_back() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;

    // A holds the row lock on id=1 (open transaction, uncommitted update).
    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("begin").await;
    a.ok("update t set v = 20 where id = 1").await;

    // B's conflicting update blocks on A. Spawn it so the test can make progress.
    let mut b = Connection::connect(&server).await.unwrap();
    b.ok("begin").await;
    let b_task =
        tokio::spawn(async move { (b.query("update t set v = 30 where id = 1").await, b) });

    // Give B time to reach the conflict and park; it must NOT have completed.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !b_task.is_finished(),
        "B must block on A's in-progress row lock, not fail fast"
    );

    // A rolls back ⇒ its lock evaporates ⇒ B unblocks and proceeds.
    a.ok("rollback").await;
    let (b_result, mut b) = b_task.await.unwrap();
    assert!(
        b_result.unwrap().result.is_ok(),
        "B proceeds once A's lock is released by rollback"
    );
    b.ok("commit").await;

    // B's update is the one that committed.
    assert_eq!(
        setup.ok("select v from t where id = 1").await.rows(),
        vec![vec![Some("30".to_string())]]
    );
}

/// A blocked writer that waits on a holder which COMMITS gets `40001` (the row
/// changed since its snapshot) — the only remaining serialization-failure case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_blocks_then_serialization_failure_when_holder_commits() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;

    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("begin").await;
    a.ok("update t set v = 20 where id = 1").await;

    let mut b = Connection::connect(&server).await.unwrap();
    b.ok("begin").await;
    let b_task =
        tokio::spawn(async move { (b.query("update t set v = 30 where id = 1").await, b) });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!b_task.is_finished(), "B must block on A");

    // A commits ⇒ the row changed under B's snapshot ⇒ B aborts with 40001.
    a.ok("commit").await;
    let (b_result, mut b) = b_task.await.unwrap();
    let err = b_result.unwrap().result.err().expect("B must conflict");
    assert!(err.message.contains("C=40001"), "message: {}", err.message);
    b.ok("rollback").await;

    // A's update is the one that committed.
    assert_eq!(
        setup.ok("select v from t where id = 1").await.rows(),
        vec![vec![Some("20".to_string())]]
    );
}

/// Two transactions that lock one row each and then cross-update form a wait-for
/// cycle; the timeout-based detector physically aborts exactly one victim before
/// returning `40P01`, so the survivor proceeds without waiting for a client-issued
/// ROLLBACK. (`docs/specs/deadlock.md`)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_aborts_one_victim_with_40p01() {
    // A short deadlock timeout keeps the test fast (detection after ~200ms).
    let server = TestServer::start_with_config(Config {
        deadlock_timeout_ms: 200,
        ..Config::default()
    })
    .await
    .unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;
    setup.ok("insert into t (id, v) values (2, 20)").await;

    // A locks row 1; B locks row 2 (distinct rows — no conflict yet).
    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("begin").await;
    a.ok("update t set v = 11 where id = 1").await;
    let mut b = Connection::connect(&server).await.unwrap();
    b.ok("begin").await;
    b.ok("update t set v = 21 where id = 2").await;

    // A now wants row 2 (held by B), B wants row 1 (held by A) ⇒ a cycle.
    let a_task =
        tokio::spawn(async move { (a.query("update t set v = 12 where id = 2").await, a) });
    let b_task =
        tokio::spawn(async move { (b.query("update t set v = 22 where id = 1").await, b) });

    // The victim releases its locks before returning, so either response may reach
    // the client first. Both must finish without a client-issued ROLLBACK.
    let (a_joined, b_joined) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(a_task, b_task)
    })
    .await
    .expect("deadlock participants did not finish after victim selection");
    let (a_outcome, a_conn) = a_joined.unwrap();
    let (b_outcome, b_conn) = b_joined.unwrap();
    let a_error = a_outcome.unwrap().result.err();
    let b_error = b_outcome.unwrap().result.err();
    let (victim_err, mut victim_conn, mut survivor_conn) = match (a_error, b_error) {
        (Some(error), None) => (error, a_conn, b_conn),
        (None, Some(error)) => (error, b_conn, a_conn),
        _ => panic!("exactly one deadlock participant must be aborted"),
    };
    assert!(
        victim_err.message.contains("C=40P01"),
        "victim must get 40P01, got: {}",
        victim_err.message
    );

    survivor_conn.ok("commit").await;
    victim_conn.ok("rollback").await;
    assert_eq!(server.active_txn_count(), 0);
}

/// A `CancelRequest` interrupts a writer blocked on a row lock: it aborts with
/// `QueryCanceled` (57014) rather than waiting for the holder (`docs/specs/deadlock.md` §5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_interrupts_a_blocked_writer() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;

    // A holds the row lock on id=1 and never finishes during the test.
    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("begin").await;
    a.ok("update t set v = 20 where id = 1").await;

    let mut b = Connection::connect(&server).await.unwrap();
    b.ok("begin").await;
    let (b_pid, b_secret) = b.backend_key();
    let b_task =
        tokio::spawn(async move { (b.query("update t set v = 30 where id = 1").await, b) });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!b_task.is_finished(), "B must be blocked on A");

    // Cancel B's blocked statement; it wakes within a poll tick and aborts 57014.
    server.send_cancel(b_pid, b_secret).await.unwrap();
    let (b_result, mut b) = b_task.await.unwrap();
    let err = b_result.unwrap().result.err().expect("B must be canceled");
    assert!(err.message.contains("C=57014"), "message: {}", err.message);

    // B's transaction is still open (failed); end it. A is untouched.
    b.ok("rollback").await;
    a.ok("rollback").await;
    assert_eq!(server.active_txn_count(), 0);
}
