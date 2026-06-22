mod support;

use std::time::Duration;

use support::{Connection, TestServer};

/// `BEGIN; INSERT; SELECT (sees own insert); COMMIT;` then a new transaction
/// `SELECT` sees the committed row. Status bytes track `I -> T -> T -> T -> I`.
#[tokio::test]
async fn begin_insert_select_commit_is_visible_afterward() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key, name text)")
        .await;

    let begin = conn.ok("begin").await;
    assert_eq!(begin.status, b'T', "BEGIN moves the session to 'T'");

    let insert = conn
        .ok("insert into users (id, name) values (1, 'Ada')")
        .await;
    assert_eq!(insert.status, b'T');

    // The open transaction sees its own uncommitted insert.
    let select = conn.ok("select id from users").await;
    assert_eq!(select.status, b'T');
    assert_eq!(select.rows(), vec![vec![Some("1".to_string())]]);

    let commit = conn.ok("commit").await;
    assert_eq!(commit.status, b'I', "COMMIT returns to 'I'");

    // A new transaction (fresh connection) sees the committed row.
    let mut other = Connection::connect(&server).await.unwrap();
    let rows = other.ok("select id from users").await.rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    assert_eq!(server.active_txn_count(), 0);
}

/// `BEGIN; INSERT; ROLLBACK;` then `SELECT` does NOT see the row.
#[tokio::test]
async fn begin_insert_rollback_is_invisible() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key, name text)")
        .await;
    conn.ok("begin").await;
    conn.ok("insert into users (id, name) values (1, 'Ada')")
        .await;
    let rollback = conn.ok("rollback").await;
    assert_eq!(rollback.status, b'I');

    let rows = conn.ok("select id from users").await.rows();
    assert!(rows.is_empty(), "rolled-back insert is invisible");
    assert_eq!(server.active_txn_count(), 0);
}

/// Failed state: `BEGIN; <bad statement → error>; SELECT (rejected 25P02);
/// ROLLBACK;` walks the status bytes `T -> E -> E -> I`.
#[tokio::test]
async fn failed_statement_gates_with_25p02_until_rollback() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    let begin = conn.ok("begin").await;
    assert_eq!(begin.status, b'T');

    // A statement against a missing table errors and poisons the block to 'E'.
    let bad = conn.query("select id from ghosts").await.unwrap();
    assert!(bad.result.is_err());
    assert_eq!(bad.status, b'E', "an error inside the block enters 'E'");

    // While 'E', a normal statement is rejected with 25P02 and stays 'E'.
    let rejected = conn.query("select id from users").await.unwrap();
    let err = rejected.result.err().expect("rejected while aborted");
    assert!(
        err.message.contains("current transaction is aborted"),
        "message was: {}",
        err.message
    );
    assert_eq!(rejected.status, b'E');

    // ROLLBACK ends the block and returns to 'I'.
    let rollback = conn.ok("rollback").await;
    assert_eq!(rollback.status, b'I');

    // The connection is usable again afterward.
    let ok = conn.ok("select id from users").await;
    assert_eq!(ok.status, b'I');
    assert!(ok.rows().is_empty());
    assert_eq!(server.active_txn_count(), 0);
}

/// A syntax error inside a transaction block poisons it to 'E' (Postgres).
#[tokio::test]
async fn syntax_error_inside_block_enters_e_state() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("begin").await;
    let bad = conn.query("not valid sql at all").await.unwrap();
    assert!(bad.result.is_err());
    assert_eq!(bad.status, b'E');
    let rollback = conn.ok("rollback").await;
    assert_eq!(rollback.status, b'I');
}

/// COMMIT of a failed (aborted) transaction issues ROLLBACK and returns to 'I'.
#[tokio::test]
async fn commit_of_aborted_block_rolls_back() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    conn.ok("begin").await;
    conn.ok("insert into users (id) values (1)").await;
    let bad = conn.query("select id from ghosts").await.unwrap();
    assert!(bad.result.is_err());
    assert_eq!(bad.status, b'E');

    // COMMIT of the aborted block rolls it back.
    let commit = conn.ok("commit").await;
    assert_eq!(commit.status, b'I');

    // The insert never committed.
    let rows = conn.ok("select id from users").await.rows();
    assert!(rows.is_empty());
}

/// DDL inside an explicit transaction is rejected (DDL is non-transactional).
#[tokio::test]
async fn ddl_inside_transaction_is_rejected() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("begin").await;
    let ddl = conn
        .query("create table users (id integer primary key)")
        .await
        .unwrap();
    let err = ddl.result.err().expect("DDL in a txn is rejected");
    assert!(
        err.message.to_lowercase().contains("ddl"),
        "message was: {}",
        err.message
    );
    conn.ok("rollback").await;
}

/// COMMIT/ROLLBACK with no open transaction are no-ops that stay 'I'.
#[tokio::test]
async fn commit_or_rollback_without_transaction_is_noop() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let commit = conn.ok("commit").await;
    assert_eq!(commit.status, b'I');
    let rollback = conn.ok("rollback").await;
    assert_eq!(rollback.status, b'I');
}

/// A reader on connection B is NOT blocked by an in-flight writer on connection
/// A, and does not see A's uncommitted row (Read Committed). After A commits, a
/// fresh B read sees it.
#[tokio::test]
async fn reader_is_not_blocked_by_in_flight_writer() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key, name text)")
        .await;

    // Connection A opens a transaction, inserts a row, and holds the write guard
    // open (no COMMIT yet).
    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("begin").await;
    a.ok("insert into users (id, name) values (1, 'Ada')").await;
    assert_eq!(server.active_txn_count(), 1, "A's writer is in-flight");

    // Connection B reads while A's writer holds the guard. The read takes no write
    // guard, so it completes promptly (well within the timeout) and does not see
    // A's uncommitted row.
    let mut b = Connection::connect(&server).await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), b.query("select id from users"))
        .await
        .expect("a lock-free reader must not block behind the in-flight writer")
        .unwrap();
    assert!(
        read.result.unwrap().unwrap_rows().is_empty(),
        "B must not see A's uncommitted row (Read Committed)"
    );

    // A commits; a fresh B read now sees the row.
    a.ok("commit").await;
    let rows = b.ok("select id from users").await.rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    assert_eq!(server.active_txn_count(), 0);
}

/// Read Committed: within one open transaction, a second read sees a row another
/// transaction committed between the two reads (a fresh per-statement snapshot).
#[tokio::test]
async fn read_committed_sees_concurrent_commit_between_statements() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;

    let mut reader = Connection::connect(&server).await.unwrap();
    reader.ok("begin").await;
    // First read: empty.
    assert!(reader.ok("select id from users").await.rows().is_empty());

    // A concurrent autocommit writer commits a row.
    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("insert into users (id) values (1)").await;

    // Second read in the same Read Committed transaction sees the new row.
    let rows = reader.ok("select id from users").await.rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())]],
        "Read Committed captures a fresh snapshot per statement"
    );
    reader.ok("commit").await;
}

/// Two write transactions on DIFFERENT rows run CONCURRENTLY (E2b: writers share
/// the writer guard). Writer B is NOT blocked behind A's open write transaction —
/// it inserts a different key and commits WHILE A is still open. Neither corrupts
/// the other, and a read runs concurrently with both open writers.
#[tokio::test]
async fn write_transactions_run_concurrently_and_a_read_runs_concurrently() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table accounts (id integer primary key, balance integer)")
        .await;

    // Writer A opens a transaction and inserts; it holds the SHARED writer guard.
    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("begin").await;
    a.ok("insert into accounts (id, balance) values (1, 100)")
        .await;
    assert_eq!(server.active_txn_count(), 1, "A's writer is in-flight");

    // Writer B (its own connection) inserts a DIFFERENT key in its own transaction
    // WHILE A is still open. Under E2b the shared writer guard does not block it, so
    // B's whole BEGIN/INSERT/COMMIT must complete promptly even though A is open.
    let mut b = Connection::connect(&server).await.unwrap();
    let b_done = tokio::time::timeout(Duration::from_secs(5), async {
        b.ok("begin").await;
        b.ok("insert into accounts (id, balance) values (2, 200)")
            .await;
        b.ok("commit").await;
    })
    .await;
    assert!(
        b_done.is_ok(),
        "writer B must run concurrently with the still-open writer A (not block on it)"
    );

    // A read on a third connection completes concurrently with the still-open A and
    // sees only B's committed row (Read Committed), not A's uncommitted insert.
    let mut reader = Connection::connect(&server).await.unwrap();
    let read = tokio::time::timeout(
        Duration::from_secs(2),
        reader.query("select id from accounts"),
    )
    .await
    .expect("a read runs concurrently with an open writer")
    .unwrap();
    assert_eq!(
        read.result.unwrap().unwrap_rows(),
        vec![vec![Some("2".to_string())]],
        "the read sees B's committed row but not A's uncommitted one"
    );

    // A commits; both rows are present and uncorrupted.
    a.ok("commit").await;
    let rows = setup
        .ok("select id, balance from accounts order by id")
        .await
        .rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("100".to_string())],
            vec![Some("2".to_string()), Some("200".to_string())],
        ]
    );
    assert_eq!(server.active_txn_count(), 0);
}

/// A client disconnecting mid-transaction aborts cleanly: the row is not visible,
/// the registry entry is gone, and a later writer can acquire the guard (proving
/// it was not leaked).
#[tokio::test]
async fn disconnect_mid_transaction_aborts_cleanly() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;

    {
        let mut a = Connection::connect(&server).await.unwrap();
        a.ok("begin").await;
        a.ok("insert into users (id) values (1)").await;
        assert_eq!(server.active_txn_count(), 1);
        // Abruptly close the connection mid-transaction.
        a.close().await;
    }

    // Wait for the server to finish aborting the dropped session's transaction.
    // The abort runs in the connection task's drop; poll the registry until empty.
    wait_until(Duration::from_secs(2), || server.active_txn_count() == 0).await;

    // The uncommitted row is invisible, and a fresh writer (which must acquire the
    // exclusive guard) succeeds — proving the guard was released, not leaked.
    let mut b = Connection::connect(&server).await.unwrap();
    assert!(b.ok("select id from users").await.rows().is_empty());
    b.ok("insert into users (id) values (2)").await;
    let rows = b.ok("select id from users").await.rows();
    assert_eq!(rows, vec![vec![Some("2".to_string())]]);
    assert_eq!(server.active_txn_count(), 0);
}

/// Regression: a connection that opens a transaction via a simple-query
/// `BEGIN; INSERT` and then issues an extended-protocol `Execute` of a WRITE on
/// the SAME connection must NOT self-deadlock. Before the fix the extended path
/// ran as an independent autocommit unit and re-acquired the single exclusive
/// write guard the open transaction already held, wedging this connection and
/// every future writer process-wide. The extended write must instead participate
/// in the open transaction (reusing the held guard), be visible to a same-txn
/// read, persist on COMMIT, and leave the guard free for a fresh writer.
#[tokio::test]
async fn extended_write_joins_open_simple_query_transaction_without_deadlock() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key, name text)")
        .await;

    // Open a transaction and take the write guard via the simple-query path.
    let begin = conn.ok("begin").await;
    assert_eq!(begin.status, b'T');
    let insert = conn
        .ok("insert into users (id, name) values (1, 'Ada')")
        .await;
    assert_eq!(insert.status, b'T');
    assert_eq!(server.active_txn_count(), 1, "the writer is in-flight");

    // The extended-protocol INSERT on the SAME connection must complete promptly
    // (no hang) — it joins the open transaction instead of re-acquiring the guard.
    let extended = tokio::time::timeout(
        Duration::from_secs(5),
        conn.extended_execute("insert into users (id, name) values (2, 'Bo')"),
    )
    .await
    .expect("the extended write must not self-deadlock behind the held write guard")
    .unwrap();
    assert!(
        extended.result.is_ok(),
        "extended insert failed: {:?}",
        extended.result.err()
    );
    assert_eq!(
        extended.status, b'T',
        "the session is still inside the open transaction after the extended write"
    );
    // Still exactly one in-flight transaction: the extended write joined it rather
    // than starting a second one.
    assert_eq!(server.active_txn_count(), 1);

    // A read in the same transaction sees BOTH rows (its own inserts).
    let select = conn.ok("select id from users order by id").await;
    assert_eq!(
        select.rows(),
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
        "the open transaction sees both its simple and extended inserts"
    );

    // COMMIT persists both rows and releases the guard.
    let commit = conn.ok("commit").await;
    assert_eq!(commit.status, b'I');
    assert_eq!(server.active_txn_count(), 0);

    // A fresh connection's writer is NOT wedged: it acquires the guard and commits.
    let mut other = Connection::connect(&server).await.unwrap();
    let rows = other.ok("select id from users order by id").await.rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
        "both rows committed durably"
    );
    let fresh_write = tokio::time::timeout(
        Duration::from_secs(5),
        other.query("insert into users (id, name) values (3, 'Cy')"),
    )
    .await
    .expect("a fresh writer must not be wedged by the prior connection")
    .unwrap();
    assert!(fresh_write.result.is_ok());
    let rows = other.ok("select id from users order by id").await.rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("3".to_string())],
        ]
    );
    assert_eq!(server.active_txn_count(), 0);
}

/// An extended-protocol `Execute` of BEGIN/COMMIT routes through the session's
/// transaction lifecycle (not as an independent autocommit unit): the BEGIN opens
/// the session transaction, an extended INSERT joins it, and the COMMIT persists
/// the row and returns the session to idle.
#[tokio::test]
async fn extended_transaction_control_drives_the_session_transaction() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;

    // BEGIN via the extended protocol opens the session transaction ('T').
    let begin = conn.extended_execute("begin").await.unwrap();
    assert!(
        begin.result.is_ok(),
        "extended BEGIN: {:?}",
        begin.result.err()
    );
    assert_eq!(
        begin.status, b'T',
        "extended BEGIN moves the session to 'T'"
    );
    assert_eq!(server.active_txn_count(), 1);

    // An extended INSERT joins the open transaction.
    let insert = conn
        .extended_execute("insert into users (id) values (1)")
        .await
        .unwrap();
    assert!(insert.result.is_ok());
    assert_eq!(insert.status, b'T');
    assert_eq!(
        server.active_txn_count(),
        1,
        "no second transaction was started"
    );

    // COMMIT via the extended protocol persists the row and returns to idle.
    let commit = conn.extended_execute("commit").await.unwrap();
    assert!(commit.result.is_ok());
    assert_eq!(commit.status, b'I');
    assert_eq!(server.active_txn_count(), 0);

    let rows = conn.ok("select id from users").await.rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
}

/// An extended `Execute` while the open transaction is in the failed ('E') state
/// is rejected with `25P02`, exactly like the simple-query path; ROLLBACK (also
/// over the extended protocol) ends the block and returns to idle.
#[tokio::test]
async fn extended_execute_is_gated_by_failed_transaction_state() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    conn.ok("begin").await;

    // Poison the block to 'E' with a statement against a missing table.
    let bad = conn.query("select id from ghosts").await.unwrap();
    assert!(bad.result.is_err());
    assert_eq!(bad.status, b'E');

    // An extended write while 'E' is rejected with 25P02 and stays 'E'.
    let rejected = conn
        .extended_execute("insert into users (id) values (1)")
        .await
        .unwrap();
    let err = rejected.result.err().expect("rejected while aborted");
    assert!(
        err.message.contains("current transaction is aborted"),
        "message was: {}",
        err.message
    );
    assert_eq!(rejected.status, b'E');

    // ROLLBACK over the extended protocol ends the block.
    let rollback = conn.extended_execute("rollback").await.unwrap();
    assert!(rollback.result.is_ok());
    assert_eq!(rollback.status, b'I');
    assert_eq!(server.active_txn_count(), 0);

    // The poisoned insert never committed.
    let rows = conn.ok("select id from users").await.rows();
    assert!(rows.is_empty());
}

/// End-to-end lost-update / `40001` (E2b): two connections each `BEGIN; UPDATE` the
/// SAME row under their own in-flight transaction. First-updater-wins: exactly one
/// UPDATE commits; the other surfaces `40001` (`SerializationFailure`) over the
/// protocol and is poisoned to 'E' (must ROLLBACK). The surviving committed value is
/// the winner's. The two UPDATEs are aligned with a barrier (no sleeps); the test
/// asserts the OUTCOME (one winner, one 40001), not which connection wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_update_same_row_yields_one_winner_and_one_40001() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let server = Arc::new(TestServer::start().await.unwrap());
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table accounts (id integer primary key, balance integer)")
        .await;
    setup
        .ok("insert into accounts (id, balance) values (1, 100)")
        .await;

    // Two writers each open a transaction, then align on the barrier before issuing
    // the conflicting UPDATE so both transactions are in-flight when they race.
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for (conn_idx, new_balance) in [(0u8, 200), (1u8, 300)] {
        let server = server.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = Connection::connect(&server).await.unwrap();
            conn.ok("begin").await;
            barrier.wait().await;
            let outcome = conn
                .ok(&format!(
                    "update accounts set balance = {new_balance} where id = 1"
                ))
                .await;
            match outcome.result {
                Ok(_) => {
                    // The winner commits its update durably.
                    conn.ok("commit").await;
                    (conn_idx, Ok(new_balance))
                }
                Err(err) => {
                    // The loser is poisoned to 'E' and must roll back. Its error must
                    // be the 40001 serialization failure (wire SQLSTATE 40001).
                    assert_eq!(outcome.status, b'E', "the losing updater enters 'E'");
                    conn.ok("rollback").await;
                    (conn_idx, Err(err.message))
                }
            }
        }));
    }

    let mut winners = 0;
    let mut conflicts = 0;
    let mut winning_balance = 0;
    for handle in handles {
        let (_idx, result) = handle.await.expect("updater task finished");
        match result {
            Ok(balance) => {
                winners += 1;
                winning_balance = balance;
            }
            Err(message) => {
                conflicts += 1;
                assert!(
                    message.contains("40001"),
                    "the loser must surface SQLSTATE 40001, got: {message}"
                );
            }
        }
    }
    assert_eq!(winners, 1, "exactly one updater commits");
    assert_eq!(conflicts, 1, "exactly one updater gets 40001");

    // The surviving committed balance is the winner's.
    let rows = setup
        .ok("select balance from accounts where id = 1")
        .await
        .rows();
    assert_eq!(rows, vec![vec![Some(winning_balance.to_string())]]);
    assert_eq!(server.active_txn_count(), 0);
}

/// A read-only transaction stays non-blocking while a writer is open (E2b: readers
/// are lock-free, writers share the guard): connection R opens a transaction and
/// reads repeatedly while connection W holds an open write transaction; none of R's
/// reads block, and R does not see W's uncommitted row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_transaction_is_non_blocking_while_a_writer_is_open() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;

    // Writer W opens a transaction and inserts, holding the shared writer guard.
    let mut w = Connection::connect(&server).await.unwrap();
    w.ok("begin").await;
    w.ok("insert into users (id) values (1)").await;
    assert_eq!(server.active_txn_count(), 1);

    // Reader R, in its own (read-only) transaction, reads several times while W is
    // open. Each read must complete promptly (no write guard taken) and never see
    // W's uncommitted row.
    let mut r = Connection::connect(&server).await.unwrap();
    r.ok("begin").await;
    for _ in 0..5 {
        let read = tokio::time::timeout(Duration::from_secs(2), r.query("select id from users"))
            .await
            .expect("a read-only transaction must stay non-blocking while a writer is open")
            .unwrap();
        assert!(
            read.result.unwrap().unwrap_rows().is_empty(),
            "the read-only transaction must not see the writer's uncommitted row"
        );
    }
    r.ok("commit").await;

    // W commits; a fresh read now sees the row.
    w.ok("commit").await;
    let rows = setup.ok("select id from users").await.rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    assert_eq!(server.active_txn_count(), 0);
}

/// Checkpoint-vs-writer (E2b): a forced checkpoint runs WHILE several writer
/// connections hammer the database. The checkpoint takes the EXCLUSIVE guard, so it
/// drains all in-flight writers and runs alone — it must complete with no
/// "unflushable dirty page" error, and afterward every committed row is intact. This
/// exercises the preserved Milestone-D "no in-flight writer at checkpoint" invariant
/// under concurrent writers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_drains_concurrent_writers_and_stays_consistent() {
    use std::sync::Arc;

    let server = Arc::new(TestServer::start().await.unwrap());
    let mut setup = Connection::connect(&server).await.unwrap();
    setup.ok("create table t (id integer primary key)").await;

    // Several writer tasks insert disjoint key ranges (autocommit per statement) so
    // many short write transactions are in flight while the checkpoint fires.
    const WRITERS: i64 = 4;
    const PER_WRITER: i64 = 60;
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let server = server.clone();
        writers.push(tokio::spawn(async move {
            let mut conn = Connection::connect(&server).await.unwrap();
            let base = w * PER_WRITER;
            for i in 0..PER_WRITER {
                let id = base + i + 1;
                conn.ok(&format!("insert into t (id) values ({id})")).await;
            }
        }));
    }

    // Fire a checkpoint concurrently with the writers. It must complete cleanly (its
    // `flush_dirty_pages` would Err on an unflushable page); it drains writers via
    // the exclusive guard. Run a couple to overlap more of the writers' lifetimes.
    for _ in 0..2 {
        server.force_checkpoint().await.expect(
            "checkpoint must complete cleanly while writers run (drained, no unflushable page)",
        );
    }

    for handle in writers {
        handle.await.expect("writer task finished");
    }
    // A final checkpoint after all writers have drained.
    server.force_checkpoint().await.unwrap();

    // Every committed row survived the concurrent checkpointing.
    let rows = setup.ok("select id from t order by id").await.rows();
    let expected: Vec<Vec<Option<String>>> = (1..=(WRITERS * PER_WRITER))
        .map(|id| vec![Some(id.to_string())])
        .collect();
    assert_eq!(
        rows.len(),
        expected.len(),
        "no committed row lost across concurrent checkpointing"
    );
    assert_eq!(rows, expected);
    assert_eq!(server.active_txn_count(), 0);
}

/// Poll `condition` until it holds or `timeout` elapses; panics on timeout.
async fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("condition not met within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
