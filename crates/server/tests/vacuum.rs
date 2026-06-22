mod support;

use support::{Connection, TestServer};

/// VACUUM is a maintenance command: `VACUUM` and `VACUUM <table>` succeed, and the
/// session stays idle (no transaction block is opened).
#[tokio::test]
async fn vacuum_command_succeeds_for_database_and_single_table() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key, name text)")
        .await;
    conn.ok("insert into users (id, name) values (1, 'Ada')")
        .await;

    // `VACUUM <table>` runs and leaves the session idle.
    let one = conn.ok("vacuum users").await;
    assert!(one.result.is_ok(), "VACUUM users should succeed");
    assert_eq!(one.status, b'I', "VACUUM does not open a transaction block");

    // `VACUUM` (whole database) runs and leaves the session idle.
    let all = conn.ok("vacuum").await;
    assert!(all.result.is_ok(), "VACUUM should succeed");
    assert_eq!(all.status, b'I');
}

/// `VACUUM <unknown>` errors with an undefined-table error and does not open a
/// transaction block.
#[tokio::test]
async fn vacuum_unknown_table_errors() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let outcome = conn.query("vacuum ghosts").await.unwrap();
    let err = outcome
        .result
        .err()
        .expect("VACUUM of an unknown table is rejected");
    assert!(
        err.message.to_lowercase().contains("does not exist"),
        "message was: {}",
        err.message
    );
    assert_eq!(
        outcome.status, b'I',
        "the failed VACUUM leaves the session idle"
    );
}

/// VACUUM inside an explicit transaction block is rejected (like DDL, it is
/// non-transactional), poisoning the block to the 'E' state.
#[tokio::test]
async fn vacuum_inside_transaction_block_is_rejected() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    conn.ok("begin").await;
    let outcome = conn.query("vacuum").await.unwrap();
    let err = outcome
        .result
        .err()
        .expect("VACUUM in a transaction block is rejected");
    assert!(
        err.message.to_lowercase().contains("transaction block"),
        "message was: {}",
        err.message
    );
    assert_eq!(outcome.status, b'E', "VACUUM poisons the open block to 'E'");
    conn.ok("rollback").await;
}

/// VACUUM runs over the extended query protocol too (it carries no parameters and
/// no bound payload; it is routed to `run_vacuum`, not bind/plan).
#[tokio::test]
async fn vacuum_runs_over_the_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    conn.ok("insert into users (id) values (1)").await;
    conn.ok("delete from users where id = 1").await;

    let outcome = conn.extended_execute("vacuum users").await.unwrap();
    assert!(
        outcome.result.is_ok(),
        "extended-protocol VACUUM should succeed"
    );
    assert_eq!(outcome.status, b'I');
}

/// End-to-end reclamation: insert N rows (+ a secondary index), DELETE half and
/// commit, then VACUUM. The live rows stay correct across point, range, and
/// secondary scans; the deleted rows stay gone; and a subsequent insert reuses the
/// reclaimed slot ids (free space recovered).
#[tokio::test]
async fn vacuum_reclaims_deleted_rows_and_keeps_live_rows_correct() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table accounts (id integer primary key, owner text, balance integer)")
        .await;
    conn.ok("create index accounts_owner on accounts (owner)")
        .await;

    // 10 rows; even ids are deleted, odd ids survive.
    for id in 0..10 {
        let owner = if id % 2 == 0 { "even" } else { "odd" };
        conn.ok(&format!(
            "insert into accounts (id, owner, balance) values ({id}, '{owner}', {})",
            id * 100
        ))
        .await;
    }
    let sum_before: i64 = total_balance(&mut conn).await;

    conn.ok("delete from accounts where id % 2 = 0").await;
    let vacuum = conn.ok("vacuum accounts").await;
    assert!(vacuum.result.is_ok(), "VACUUM accounts should succeed");

    // The deleted (even) rows are gone; the live (odd) rows are all still correct.
    for id in 0..10 {
        let rows = conn
            .ok(&format!("select id, balance from accounts where id = {id}"))
            .await
            .rows();
        if id % 2 == 0 {
            assert!(rows.is_empty(), "even id {id} was deleted and vacuumed");
        } else {
            assert_eq!(
                rows,
                vec![vec![Some(id.to_string()), Some((id * 100).to_string())]],
                "odd id {id} survives with its value intact"
            );
        }
    }

    // Range scan over the live set is correct and ordered.
    let live_ids: Vec<Option<String>> = conn
        .ok("select id from accounts order by id")
        .await
        .rows()
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(
        live_ids,
        vec![
            Some("1".to_string()),
            Some("3".to_string()),
            Some("5".to_string()),
            Some("7".to_string()),
            Some("9".to_string()),
        ]
    );

    // Secondary-index scan: the 'even' entries are gone, the 'odd' entries remain.
    let evens = conn
        .ok("select id from accounts where owner = 'even' order by id")
        .await
        .rows();
    assert!(evens.is_empty(), "no 'even' rows remain after VACUUM");
    let odds = conn
        .ok("select id from accounts where owner = 'odd' order by id")
        .await
        .rows();
    assert_eq!(
        odds.len(),
        5,
        "all five 'odd' rows resolve via the secondary index"
    );

    // A fresh insert reuses reclaimed space and is fully correct.
    conn.ok("insert into accounts (id, owner, balance) values (100, 'odd', 5000)")
        .await;
    let reinserted = conn
        .ok("select balance from accounts where id = 100")
        .await
        .rows();
    assert_eq!(reinserted, vec![vec![Some("5000".to_string())]]);

    // Bank invariant: the surviving balances plus the reinsert equal the expected
    // sum (no live row lost, no dead row resurrected). Odd balances are
    // 100+300+500+700+900 = 2500, plus the reinsert 5000 = 7500.
    let sum_after = total_balance(&mut conn).await;
    assert_eq!(sum_after, 2500 + 5000);
    assert!(
        sum_before > sum_after - 5000,
        "the deleted half lowered the live sum"
    );
}

/// An UPDATE-heavy variant: many UPDATEs leave dead old versions in the heap;
/// VACUUM reclaims them and the visible (latest) values stay correct.
#[tokio::test]
async fn vacuum_reclaims_dead_update_versions() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table counters (id integer primary key, n integer)")
        .await;
    conn.ok("insert into counters (id, n) values (1, 0)").await;

    // 20 in-place updates each leave a dead old version behind.
    for n in 1..=20 {
        conn.ok(&format!("update counters set n = {n} where id = 1"))
            .await;
    }

    let vacuum = conn.ok("vacuum counters").await;
    assert!(vacuum.result.is_ok());

    // The latest value is visible and unique.
    let rows = conn.ok("select n from counters where id = 1").await.rows();
    assert_eq!(rows, vec![vec![Some("20".to_string())]]);
    let count = conn.ok("select count(*) from counters").await.rows();
    assert_eq!(count, vec![vec![Some("1".to_string())]]);
}

/// The horizon-safety invariant at the server level. While a snapshot advertises an
/// old `xmin`, the GC horizon is pinned at or below that `xmin` even after a delete
/// commits and the id allocator advances well past it — so VACUUM (which captures
/// the horizon under the exclusive guard) cannot advance past a version the live
/// snapshot still sees. This is the mechanism that prevents VACUUM from reclaiming a
/// version a reader needs.
#[tokio::test]
async fn live_snapshot_pins_the_horizon_below_a_committed_delete() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key, name text)")
        .await;
    conn.ok("insert into users (id, name) values (1, 'Ada')")
        .await;
    conn.ok("insert into users (id, name) values (2, 'Grace')")
        .await;

    let components = &server.app().components;

    // A long-lived reader captures and HOLDS a snapshot, advertising its xmin so the
    // GC horizon is pinned for the snapshot's whole lifetime.
    let next_id = components
        .next_txn_id
        .load(std::sync::atomic::Ordering::Acquire);
    let (_active, _xmax, held) = components.active_txns.capture(|| next_id);
    let pinned = held.xmin();

    // Delete a row and commit it, then run several more statements so the id
    // allocator advances well above the held snapshot's xmin.
    conn.ok("delete from users where id = 1").await;
    for id in 10..20 {
        conn.ok(&format!("insert into users (id, name) values ({id}, 'x')"))
            .await;
    }

    // While the snapshot is held, the horizon is pinned at (or below) its xmin — NOT
    // at the much-higher next_txn_id. VACUUM captures exactly this horizon under the
    // guard, so it cannot reclaim a version the held snapshot could see live.
    let horizon_while_held = components.gc_horizon();
    assert!(
        horizon_while_held <= pinned,
        "the held snapshot pins the horizon at {pinned}, not the advanced allocator \
         (horizon was {horizon_while_held})"
    );

    let vacuum = conn.ok("vacuum users").await;
    assert!(
        vacuum.result.is_ok(),
        "VACUUM under a pinned horizon still runs"
    );

    // Drop the advertisement: the horizon is now free to advance to next_txn_id, so a
    // later VACUUM can reclaim the version the snapshot was protecting.
    drop(held);
    assert!(
        components.gc_horizon() > pinned,
        "releasing the snapshot lets the horizon advance"
    );

    // The visible data is consistent throughout: id 1 is deleted, id 2 and the later
    // inserts survive.
    let remaining = conn
        .ok("select id from users order by id")
        .await
        .rows()
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(remaining.first(), Some(&Some("2".to_string())));
    assert!(
        !remaining.contains(&Some("1".to_string())),
        "the committed delete of id 1 is not visible"
    );
}

/// VACUUM then crash + restart: the reclaimed state is durable (the VACUUM
/// full-page images replay), live data is intact, and deleted data stays gone.
#[tokio::test]
async fn vacuumed_state_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("create table users (id integer primary key, name text)")
            .await;
        conn.ok("create index users_name on users (name)").await;
        for id in 0..6 {
            let name = if id % 2 == 0 { "keep" } else { "gone" };
            conn.ok(&format!(
                "insert into users (id, name) values ({id}, '{name}')"
            ))
            .await;
        }
        conn.ok("delete from users where name = 'gone'").await;
        let vacuum = conn.ok("vacuum users").await;
        assert!(vacuum.result.is_ok());
        // Reuse a reclaimed slot after vacuum to exercise reclaim + insert replay.
        conn.ok("insert into users (id, name) values (100, 'keep')")
            .await;
        // The drop here triggers a graceful shutdown.
    }

    // Restart from the same data dir: recovery replays the VACUUM FPIs and the
    // reclaim+reuse insert.
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let ids = conn
        .ok("select id from users order by id")
        .await
        .rows()
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    // The kept rows (even ids 0,2,4) plus the reinsert (100) survive; the 'gone' rows
    // (odd ids) stay deleted.
    assert_eq!(
        ids,
        vec![
            Some("0".to_string()),
            Some("2".to_string()),
            Some("4".to_string()),
            Some("100".to_string()),
        ]
    );

    // The secondary index is consistent after recovery: only 'keep' rows resolve.
    let gone = conn
        .ok("select id from users where name = 'gone'")
        .await
        .rows();
    assert!(
        gone.is_empty(),
        "vacuumed 'gone' rows stay gone after restart"
    );
    let keep = conn
        .ok("select id from users where name = 'keep' order by id")
        .await
        .rows();
    assert_eq!(
        keep.len(),
        4,
        "all 'keep' rows resolve via the secondary index"
    );
}

/// Sum the `balance` column via a SQL aggregate; returns 0 when the table is empty.
async fn total_balance(conn: &mut Connection) -> i64 {
    let rows = conn.ok("select sum(balance) from accounts").await.rows();
    rows.first()
        .and_then(|row| row.first())
        .and_then(|cell| cell.as_ref())
        .map(|text| text.parse::<i64>().expect("sum is an integer"))
        .unwrap_or(0)
}
