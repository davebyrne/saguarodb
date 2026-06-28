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

/// The payoff of Milestone G: Read Committed vs Repeatable Read end-to-end. Under
/// the default Read Committed a transaction's second SELECT sees a row another
/// connection committed between the two reads; under `BEGIN ISOLATION LEVEL
/// REPEATABLE READ` the transaction holds one snapshot for its whole life, so the
/// second SELECT does NOT see the concurrently-committed row.
#[tokio::test]
async fn repeatable_read_holds_a_stable_snapshot_unlike_read_committed() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;
    setup.ok("insert into users (id) values (1)").await;

    // Baseline (default Read Committed): the second read sees the concurrent commit.
    let mut rc = Connection::connect(&server).await.unwrap();
    rc.ok("begin").await;
    assert_eq!(
        rc.ok("select id from users order by id").await.rows(),
        vec![vec![Some("1".to_string())]],
        "first read sees the initial row"
    );
    let mut writer_rc = Connection::connect(&server).await.unwrap();
    writer_rc.ok("insert into users (id) values (2)").await;
    assert_eq!(
        rc.ok("select id from users order by id").await.rows(),
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
        "Read Committed's second read sees the concurrently-committed row"
    );
    rc.ok("commit").await;

    // Repeatable Read: the second read does NOT see a row committed after the
    // transaction's first statement captured its snapshot.
    let mut rr = Connection::connect(&server).await.unwrap();
    let begin = rr.ok("begin isolation level repeatable read").await;
    assert_eq!(begin.status, b'T');
    let first = rr.ok("select id from users order by id").await.rows();
    assert_eq!(
        first,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
        "the RR snapshot is captured at the first statement"
    );

    // A concurrent connection inserts and commits a brand-new row.
    let mut writer_rr = Connection::connect(&server).await.unwrap();
    writer_rr.ok("insert into users (id) values (3)").await;

    // Back in the RR transaction: the second read is identical to the first — the
    // new row (3) is invisible because the snapshot is frozen.
    let second = rr.ok("select id from users order by id").await.rows();
    assert_eq!(
        second, first,
        "Repeatable Read's second read does NOT see the concurrently-committed row"
    );
    rr.ok("commit").await;

    // After commit, a fresh transaction sees all three rows.
    assert_eq!(
        setup.ok("select id from users order by id").await.rows(),
        vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("3".to_string())],
        ]
    );
    assert_eq!(server.active_txn_count(), 0);
}

/// The payoff of Milestone G2: a per-connection default isolation set by
/// `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ`.
/// After the SET, a plain `BEGIN` (no explicit level) inherits Repeatable Read, so
/// its second SELECT does NOT see a row another connection committed in between.
/// Override precedence (`BEGIN ISOLATION LEVEL READ COMMITTED` is RC even with the
/// RR default) and per-connection reset (a fresh connection defaults to RC) are
/// checked too.
#[tokio::test]
async fn session_characteristics_sets_a_per_connection_default_isolation() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;
    setup.ok("insert into users (id) values (1)").await;

    // Connection A raises its default to Repeatable Read. The SET completes with a
    // `SET` command tag and stays idle (it opens no transaction).
    let mut a = Connection::connect(&server).await.unwrap();
    let set = a
        .ok("set session characteristics as transaction isolation level repeatable read")
        .await;
    assert_eq!(set.status, b'I', "SET SESSION CHARACTERISTICS stays idle");

    // A plain BEGIN on connection A inherits Repeatable Read: its snapshot is frozen
    // at the first statement, so a row committed afterward by another connection is
    // invisible to its second read.
    a.ok("begin").await;
    let first = a.ok("select id from users order by id").await.rows();
    assert_eq!(first, vec![vec![Some("1".to_string())]]);
    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("insert into users (id) values (2)").await;
    assert_eq!(
        a.ok("select id from users order by id").await.rows(),
        first,
        "the inherited Repeatable Read default freezes the snapshot"
    );
    a.ok("commit").await;

    // Override precedence: an explicit BEGIN level beats the session default. On the
    // SAME connection A (default still RR), `BEGIN ISOLATION LEVEL READ COMMITTED`
    // behaves as Read Committed — its second read sees a concurrent commit.
    a.ok("begin isolation level read committed").await;
    let before = a.ok("select id from users order by id").await.rows();
    writer.ok("insert into users (id) values (3)").await;
    let after = a.ok("select id from users order by id").await.rows();
    assert_eq!(
        after.len(),
        before.len() + 1,
        "explicit READ COMMITTED overrides the RR session default"
    );
    a.ok("commit").await;

    // Per-connection reset: a brand-new connection defaults to Read Committed,
    // regardless of connection A's session setting.
    let mut b = Connection::connect(&server).await.unwrap();
    b.ok("begin").await;
    let b_first = b.ok("select id from users order by id").await.rows();
    writer.ok("insert into users (id) values (4)").await;
    let b_second = b.ok("select id from users order by id").await.rows();
    assert_eq!(
        b_second.len(),
        b_first.len() + 1,
        "a fresh connection resets to Read Committed and sees the concurrent commit"
    );
    b.ok("commit").await;

    assert_eq!(server.active_txn_count(), 0);
}

/// `SET SESSION CHARACTERISTICS` is allowed inside a transaction block but does NOT
/// change the CURRENT transaction's isolation — it only sets the default for FUTURE
/// transactions. An open Read Committed transaction stays RC after the SET (its next
/// read sees a concurrent commit), while the NEXT transaction inherits RR.
#[tokio::test]
async fn session_characteristics_does_not_change_the_open_transaction() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;
    setup.ok("insert into users (id) values (1)").await;

    let mut conn = Connection::connect(&server).await.unwrap();
    // Open an explicit Read Committed transaction and fix its first snapshot.
    conn.ok("begin isolation level read committed").await;
    let first = conn.ok("select id from users order by id").await.rows();
    assert_eq!(first, vec![vec![Some("1".to_string())]]);

    // SET SESSION CHARACTERISTICS ... REPEATABLE READ inside the open RC block: it
    // succeeds and leaves the block open ('T'), but does not raise THIS txn to RR.
    let set = conn
        .ok("set session characteristics as transaction isolation level repeatable read")
        .await;
    assert_eq!(
        set.status, b'T',
        "SET SESSION CHARACTERISTICS is allowed inside a block and keeps it open"
    );

    // A concurrent commit IS visible to this still-Read-Committed transaction.
    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("insert into users (id) values (2)").await;
    assert_eq!(
        conn.ok("select id from users order by id").await.rows(),
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
        "the open transaction stayed Read Committed; SET SESSION CHARACTERISTICS did not change it"
    );
    conn.ok("commit").await;

    // The NEXT transaction on the same connection inherits the updated RR default.
    conn.ok("begin").await;
    let next_first = conn.ok("select id from users order by id").await.rows();
    writer.ok("insert into users (id) values (3)").await;
    assert_eq!(
        conn.ok("select id from users order by id").await.rows(),
        next_first,
        "the next transaction inherited Repeatable Read and froze its snapshot"
    );
    conn.ok("commit").await;
    assert_eq!(server.active_txn_count(), 0);
}

/// A `SERIALIZABLE` transaction shares Repeatable Read's stable per-transaction
/// snapshot (SSI layers rw-conflict tracking on top; `docs/specs/ssi.md`). A
/// read-only serializable transaction simply sees a stable snapshot across a
/// concurrent commit and commits cleanly.
#[tokio::test]
async fn serializable_holds_a_stable_snapshot_like_repeatable_read() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;
    setup.ok("insert into users (id) values (1)").await;

    let mut txn = Connection::connect(&server).await.unwrap();
    txn.ok("start transaction isolation level serializable")
        .await;
    let first = txn.ok("select id from users order by id").await.rows();
    assert_eq!(first, vec![vec![Some("1".to_string())]]);

    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("insert into users (id) values (2)").await;

    // SERIALIZABLE builds on snapshot isolation: the second read is unchanged.
    assert_eq!(
        txn.ok("select id from users order by id").await.rows(),
        first,
        "a SERIALIZABLE transaction gets a stable per-transaction snapshot"
    );
    txn.ok("commit").await;
    assert_eq!(server.active_txn_count(), 0);
}

/// Repeatable Read write-write conflict: an RR transaction reads a row, another
/// transaction updates+commits it, and the RR transaction's UPDATE of that row
/// surfaces `40001` (`SerializationFailure`) — the first-updater-wins machinery
/// also enforces RR's "cannot write a row changed after my snapshot".
#[tokio::test]
async fn repeatable_read_update_of_a_concurrently_changed_row_is_40001() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table accounts (id integer primary key, balance integer)")
        .await;
    setup
        .ok("insert into accounts (id, balance) values (1, 100)")
        .await;

    // The RR transaction reads the row, fixing its snapshot.
    let mut rr = Connection::connect(&server).await.unwrap();
    rr.ok("begin isolation level repeatable read").await;
    assert_eq!(
        rr.ok("select balance from accounts where id = 1")
            .await
            .rows(),
        vec![vec![Some("100".to_string())]]
    );

    // Another connection updates and commits the same row (autocommit).
    let mut other = Connection::connect(&server).await.unwrap();
    other
        .ok("update accounts set balance = 200 where id = 1")
        .await;

    // The RR transaction's UPDATE of that row now conflicts: the row was changed
    // and committed after the RR snapshot, so the write fails with 40001 and the
    // transaction is poisoned to 'E'.
    let conflict = rr
        .ok("update accounts set balance = 300 where id = 1")
        .await;
    assert_eq!(conflict.status, b'E', "the RR transaction enters 'E'");
    let message = conflict
        .result
        .err()
        .expect("the RR update of a concurrently-changed row must fail")
        .message;
    assert!(
        message.contains("40001"),
        "the RR write conflict surfaces SQLSTATE 40001, got: {message}"
    );
    rr.ok("rollback").await;

    // The winner's committed value survives.
    assert_eq!(
        setup
            .ok("select balance from accounts where id = 1")
            .await
            .rows(),
        vec![vec![Some("200".to_string())]]
    );
    assert_eq!(server.active_txn_count(), 0);
}

/// `SET TRANSACTION ISOLATION LEVEL` guards: it is honored before the transaction's
/// first query (and then RR holds a stable snapshot), and rejected after a query has
/// run (poisoning the block to 'E', matching Postgres' "must be called before any
/// query"). A bare `SET TRANSACTION` in autocommit is a no-op success.
#[tokio::test]
async fn set_transaction_isolation_level_is_guarded_by_the_first_query() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;
    setup.ok("insert into users (id) values (1)").await;

    // Before the first query: SET TRANSACTION sets the level, and the transaction
    // then behaves as Repeatable Read (stable snapshot).
    let mut ok = Connection::connect(&server).await.unwrap();
    ok.ok("begin").await;
    let set = ok
        .ok("set transaction isolation level repeatable read")
        .await;
    assert_eq!(
        set.status, b'T',
        "SET TRANSACTION before any query succeeds"
    );
    let first = ok.ok("select id from users order by id").await.rows();
    assert_eq!(first, vec![vec![Some("1".to_string())]]);
    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("insert into users (id) values (2)").await;
    assert_eq!(
        ok.ok("select id from users order by id").await.rows(),
        first,
        "after SET TRANSACTION REPEATABLE READ the snapshot is stable"
    );
    ok.ok("commit").await;

    // After a query has run: SET TRANSACTION is rejected and poisons the block.
    let mut late = Connection::connect(&server).await.unwrap();
    late.ok("begin").await;
    late.ok("select id from users").await;
    let rejected = late
        .ok("set transaction isolation level repeatable read")
        .await;
    assert_eq!(
        rejected.status, b'E',
        "the rejection poisons the block to 'E'"
    );
    let message = rejected
        .result
        .err()
        .expect("SET TRANSACTION after a query must error")
        .message;
    assert!(
        message.to_ascii_lowercase().contains("before any query"),
        "the error explains the before-first-query rule, got: {message}"
    );
    late.ok("rollback").await;

    // In autocommit (no open transaction): SET TRANSACTION is a no-op success.
    let no_txn = setup
        .ok("set transaction isolation level repeatable read")
        .await;
    assert_eq!(
        no_txn.status, b'I',
        "SET TRANSACTION with no open transaction is a no-op and stays Idle"
    );
    assert!(no_txn.result.is_ok());

    // Inside a failed ('E') block: SET TRANSACTION is rejected with 25P02 like any
    // non-COMMIT/ROLLBACK statement, and stays 'E'.
    let mut aborted = Connection::connect(&server).await.unwrap();
    aborted.ok("begin").await;
    let bad = aborted.query("select id from ghosts").await.unwrap();
    assert!(bad.result.is_err(), "the bad statement poisons the block");
    assert_eq!(bad.status, b'E');
    let in_failed = aborted
        .ok("set transaction isolation level repeatable read")
        .await;
    assert_eq!(in_failed.status, b'E', "SET TRANSACTION stays in 'E'");
    let message = in_failed
        .result
        .err()
        .expect("SET TRANSACTION in a failed block is rejected")
        .message;
    assert!(
        message.contains("current transaction is aborted"),
        "SET TRANSACTION in a failed block surfaces 25P02, got: {message}"
    );
    aborted.ok("rollback").await;

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

// --- savepoints (docs/specs/savepoints.md) ---

/// `ROLLBACK TO SAVEPOINT` undoes work done since the savepoint but keeps the
/// transaction (and the savepoint) open, and earlier work survives the commit.
#[tokio::test]
async fn rollback_to_savepoint_undoes_inner_work() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    conn.ok("begin").await;
    conn.ok("insert into t (id) values (1)").await;
    let sp = conn.ok("savepoint s").await;
    assert_eq!(sp.status, b'T');
    conn.ok("insert into t (id) values (2)").await;
    // The transaction sees both its own inserts before the rollback.
    assert_eq!(
        conn.ok("select id from t order by id").await.rows(),
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );

    let rb = conn.ok("rollback to savepoint s").await;
    assert_eq!(rb.status, b'T', "ROLLBACK TO keeps the block open");
    // The work since the savepoint is gone; the earlier insert remains.
    assert_eq!(
        conn.ok("select id from t order by id").await.rows(),
        vec![vec![Some("1".to_string())]]
    );

    conn.ok("commit").await;
    // After commit, only the kept row is visible; the family is fully settled.
    assert_eq!(
        conn.ok("select id from t").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(
        server.active_txn_count(),
        0,
        "the whole family is deregistered"
    );
}

/// `RELEASE SAVEPOINT` keeps the subtransaction's work; it commits with the parent
/// and is visible to a later transaction.
#[tokio::test]
async fn release_savepoint_keeps_work_after_commit() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    conn.ok("begin").await;
    conn.ok("savepoint s").await;
    conn.ok("insert into t (id) values (1)").await;
    let rel = conn.ok("release savepoint s").await;
    assert_eq!(rel.status, b'T');
    conn.ok("commit").await;

    assert_eq!(
        conn.ok("select id from t").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(server.active_txn_count(), 0);
}

/// Nested savepoints: `ROLLBACK TO` the outer level discards both the outer and
/// inner work done after it, while work before the outer savepoint survives.
#[tokio::test]
async fn nested_savepoint_rollback_to_outer_discards_inner() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    conn.ok("begin").await;
    conn.ok("insert into t (id) values (1)").await;
    conn.ok("savepoint outer").await;
    conn.ok("insert into t (id) values (2)").await;
    conn.ok("savepoint inner").await;
    conn.ok("insert into t (id) values (3)").await;
    conn.ok("rollback to savepoint outer").await;
    // Both the inner (3) and post-outer (2) inserts are gone; 1 remains.
    assert_eq!(
        conn.ok("select id from t order by id").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    conn.ok("commit").await;
    assert_eq!(
        conn.ok("select id from t").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
}

/// `ROLLBACK TO SAVEPOINT` recovers a transaction that entered the failed ('E')
/// state after the savepoint was established: the block becomes usable again and
/// commits the pre-savepoint work plus work done after recovery.
#[tokio::test]
async fn rollback_to_savepoint_recovers_failed_block() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    conn.ok("begin").await;
    conn.ok("insert into t (id) values (1)").await;
    conn.ok("savepoint s").await;
    // A bad statement poisons the block to 'E'.
    let bad = conn.query("select id from ghosts").await.unwrap();
    assert!(bad.result.is_err());
    assert_eq!(bad.status, b'E');
    // While 'E', a normal statement is still rejected with 25P02.
    let rejected = conn.query("insert into t (id) values (9)").await.unwrap();
    let err = rejected.result.err().unwrap();
    assert!(err.message.contains("C=25P02"), "message: {}", err.message);
    assert_eq!(rejected.status, b'E');

    // ROLLBACK TO the savepoint recovers the block to 'T'.
    let rb = conn.ok("rollback to savepoint s").await;
    assert_eq!(rb.status, b'T', "ROLLBACK TO clears the failed state");
    conn.ok("insert into t (id) values (2)").await;
    conn.ok("commit").await;

    assert_eq!(
        conn.ok("select id from t order by id").await.rows(),
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );
}

/// Same-name re-establishment: a second `SAVEPOINT s` shadows the first; `ROLLBACK
/// TO s` targets the most recent, and the older `s` is reachable again afterward.
#[tokio::test]
async fn same_name_savepoint_targets_most_recent() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    conn.ok("begin").await;
    conn.ok("savepoint s").await;
    conn.ok("insert into t (id) values (1)").await;
    conn.ok("savepoint s").await;
    conn.ok("insert into t (id) values (2)").await;
    // ROLLBACK TO s hits the most recent s, discarding only the second insert; the
    // inner s remains established (re-established with a fresh subxid).
    conn.ok("rollback to savepoint s").await;
    assert_eq!(
        conn.ok("select id from t order by id").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    // Releasing the inner s exposes the older (outer) s again; rolling back to it
    // now discards the first insert too.
    conn.ok("release savepoint s").await;
    conn.ok("rollback to savepoint s").await;
    assert!(conn.ok("select id from t").await.rows().is_empty());
    conn.ok("commit").await;
}

/// Error paths: savepoint commands outside a block (`25P01`), and unknown savepoint
/// names (`3B001`) — which abort the block to 'E' like any statement error, so each
/// is exercised in its own fresh transaction.
#[tokio::test]
async fn savepoint_error_paths() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    // Outside a transaction block: 25P01, no block to poison. (The harness encodes
    // the SQLSTATE into the decoded message as `C=<code>`.)
    let outside = conn.query("savepoint s").await.unwrap();
    let err = outside.result.err().unwrap();
    assert!(err.message.contains("C=25P01"), "message: {}", err.message);
    assert_eq!(outside.status, b'I');

    // Unknown ROLLBACK TO: 3B001, and the block is poisoned to 'E'.
    conn.ok("begin").await;
    let rb = conn.query("rollback to savepoint nope").await.unwrap();
    let rb_err = rb.result.err().unwrap();
    assert!(
        rb_err.message.contains("C=3B001"),
        "message: {}",
        rb_err.message
    );
    assert_eq!(rb.status, b'E', "unknown ROLLBACK TO aborts the block");
    conn.ok("rollback").await;

    // Unknown RELEASE: 3B001, and the block is poisoned to 'E'.
    conn.ok("begin").await;
    let rel = conn.query("release savepoint nope").await.unwrap();
    let rel_err = rel.result.err().unwrap();
    assert!(
        rel_err.message.contains("C=3B001"),
        "message: {}",
        rel_err.message
    );
    assert_eq!(rel.status, b'E', "unknown RELEASE aborts the block");
    conn.ok("rollback").await;
}

/// Regression (review of the savepoint lifecycle): a subxid `RELEASE`d into a
/// nested level must still be rolled back when `ROLLBACK TO` targets an enclosing
/// savepoint — its rows must NOT survive the commit. A by-stack-position selection
/// would miss the released subxid; selection is by subxid value.
#[tokio::test]
async fn rollback_to_outer_discards_released_nested_subxid() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    conn.ok("begin").await;
    conn.ok("savepoint a").await;
    conn.ok("insert into t (id) values (1)").await; // under a
    conn.ok("savepoint b").await;
    conn.ok("insert into t (id) values (2)").await; // under b
    conn.ok("release savepoint b").await; // b popped, its subxid stays live
    conn.ok("rollback to savepoint a").await; // must discard BOTH 1 and 2
    assert!(
        conn.ok("select id from t").await.rows().is_empty(),
        "rolling back to a discards work under a and the released-into-b subxid"
    );
    conn.ok("commit").await;
    assert!(conn.ok("select id from t").await.rows().is_empty());
}

/// Savepoints are rejected over the extended query protocol (simple-query only).
#[tokio::test]
async fn savepoint_rejected_in_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("begin").await;
    let err = conn
        .extended_execute("savepoint s")
        .await
        .unwrap()
        .result
        .err()
        .expect("savepoint must be rejected via the extended protocol");
    assert!(err.message.contains("C=0A000"), "message: {}", err.message);
    conn.ok("rollback").await;
}

// --- savepoints: cross-transaction visibility ---

/// A released subtransaction's row stays invisible to a concurrent transaction
/// until the top commits (no dirty read — the released subxid is still
/// registered/in-progress), then becomes visible.
#[tokio::test]
async fn released_subxid_row_is_invisible_until_top_commits() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup.ok("create table t (id integer primary key)").await;

    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("begin").await;
    a.ok("savepoint s").await;
    a.ok("insert into t (id) values (1)").await;
    a.ok("release savepoint s").await; // released, but the top has NOT committed

    // A concurrent reader must not see the released-but-uncommitted row.
    let mut b = Connection::connect(&server).await.unwrap();
    assert!(
        b.ok("select id from t").await.rows().is_empty(),
        "a released subxid's row must be invisible before the top commits"
    );

    a.ok("commit").await;
    // Once the top commits, a fresh read sees the released row.
    assert_eq!(
        b.ok("select id from t").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
}

/// After a transaction with savepoints commits, another transaction sees exactly
/// its released rows and never its rolled-back ones.
#[tokio::test]
async fn committed_savepoint_txn_exposes_released_hides_rolled_back() {
    let server = TestServer::start().await.unwrap();
    let mut a = Connection::connect(&server).await.unwrap();
    a.ok("create table t (id integer primary key)").await;

    a.ok("begin").await;
    a.ok("savepoint keep").await;
    a.ok("insert into t (id) values (1)").await;
    a.ok("release savepoint keep").await; // 1 is kept
    a.ok("savepoint drop_it").await;
    a.ok("insert into t (id) values (2)").await;
    a.ok("rollback to savepoint drop_it").await; // 2 is rolled back
    a.ok("commit").await;

    let mut b = Connection::connect(&server).await.unwrap();
    assert_eq!(
        b.ok("select id from t").await.rows(),
        vec![vec![Some("1".to_string())]],
        "the released row is visible; the rolled-back row is not"
    );
}
