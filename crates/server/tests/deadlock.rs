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

/// A Read Committed writer that waits on a holder which commits follows the
/// successor, rechecks it, and evaluates assignments over the latest row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_writer_updates_the_latest_committed_successor() {
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
        tokio::spawn(async move { (b.query("update t set v = v + 1 where id = 1").await, b) });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!b_task.is_finished(), "B must block on A");

    // A commits. B follows A's successor and applies `v + 1` to 20, not the
    // scan-time value 10.
    a.ok("commit").await;
    let (b_result, mut b) = b_task.await.unwrap();
    assert!(
        b_result.unwrap().result.is_ok(),
        "B must complete through EPQ"
    );
    b.ok("commit").await;

    assert_eq!(
        setup.ok("select v from t where id = 1").await.rows(),
        vec![vec![Some("21".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_writer_skips_successor_that_fails_epq_recheck() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;

    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("update t set v = 20 where id = 1").await;

    let mut waiter = Connection::connect(&server).await.unwrap();
    waiter.ok("begin").await;
    let waiter_task = tokio::spawn(async move {
        let result = waiter
            .query("update t set v = 30 where id = 1 and v = 10 returning v")
            .await;
        (result, waiter)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiter_task.is_finished(),
        "waiter must reach the held tuple"
    );

    holder.ok("commit").await;
    let (result, mut waiter) = waiter_task.await.unwrap();
    assert!(
        result.unwrap().result.unwrap().unwrap_rows().is_empty(),
        "the successor no longer satisfies v = 10"
    );
    waiter.ok("commit").await;
    assert_eq!(
        setup.ok("select v from t where id = 1").await.rows(),
        vec![vec![Some("20".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_delete_follows_the_latest_committed_successor() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t values (1, 10)").await;

    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("update t set v = 20 where id = 1").await;

    let mut deleter = Connection::connect(&server).await.unwrap();
    deleter.ok("begin").await;
    let delete_task = tokio::spawn(async move {
        let result = deleter
            .query("delete from t where id = 1 returning v")
            .await;
        (result, deleter)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !delete_task.is_finished(),
        "deleter must wait for the holder"
    );

    holder.ok("commit").await;
    let (result, mut deleter) = delete_task.await.unwrap();
    assert_eq!(
        result.unwrap().rows(),
        vec![vec![Some("20".to_string())]],
        "DELETE RETURNING must see the latest locked row"
    );
    deleter.ok("commit").await;
    assert!(setup.ok("select * from t").await.rows().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_update_from_rechecks_the_joined_source_plan() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup
        .ok("create table src (id integer primary key, new_v integer)")
        .await;
    setup.ok("insert into t values (1, 10)").await;
    setup.ok("insert into src values (1, 99)").await;

    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("update t set v = 20 where id = 1").await;

    let mut waiter = Connection::connect(&server).await.unwrap();
    waiter.ok("begin").await;
    let waiter_task = tokio::spawn(async move {
        let result = waiter
            .query(
                "update t set v = src.new_v from src \
                 where t.id = src.id and t.v = 10 returning v",
            )
            .await;
        (result, waiter)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiter_task.is_finished(),
        "joined UPDATE must wait for the tuple"
    );

    holder.ok("commit").await;
    let (result, mut waiter) = waiter_task.await.unwrap();
    assert!(
        result.unwrap().rows().is_empty(),
        "the latest target must be rechecked against the joined WHERE clause"
    );
    waiter.ok("commit").await;
    assert_eq!(
        setup.ok("select v from t").await.rows(),
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
