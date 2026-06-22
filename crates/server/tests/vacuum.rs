mod support;

use saguarodb_server::config::Config;
use support::{Connection, TestServer};

/// A config with a known checkpoint cadence and auto-vacuum threshold for the F4b
/// tests. Checkpoints are NOT fired automatically by commits (cadence is huge); the
/// tests drive them explicitly via `force_checkpoint`, so the gating is deterministic.
fn auto_vacuum_config(threshold: u64) -> Config {
    Config {
        buffer_pool_frames: 64,
        checkpoint_every_n_commits: 1_000_000,
        checkpoint_wal_bytes: 1 << 40,
        auto_vacuum_dead_rows: threshold,
        shutdown_timeout_ms: 1_000,
        ..Config::default()
    }
}

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

// ---------------------------------------------------------------------------
// Milestone F4b — auto-prune dead versions at checkpoint behind a threshold.
// ---------------------------------------------------------------------------

/// Threshold gating. With churn BELOW the threshold, a checkpoint does NOT auto-prune
/// (the dead-rows accumulator is left untouched); once enough churn crosses the
/// threshold, the next checkpoint auto-prunes and resets the accumulator to zero.
#[tokio::test]
async fn checkpoint_auto_prunes_only_above_the_threshold() {
    // Threshold of 5 committed dead versions.
    let server = TestServer::start_with_config(auto_vacuum_config(5))
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table t (id integer primary key, n integer)")
        .await;
    for id in 0..10 {
        conn.ok(&format!("insert into t (id, n) values ({id}, 0)"))
            .await;
    }

    // Three committed deletes ⇒ 3 dead versions, BELOW the threshold of 5.
    for id in 0..3 {
        conn.ok(&format!("delete from t where id = {id}")).await;
    }
    assert_eq!(server.dead_rows_since_vacuum(), 3, "three dead versions");

    // A checkpoint with the count below the threshold does NOT auto-prune: the
    // accumulator is untouched (a prune would have reset it to 0).
    server.force_checkpoint().await.unwrap();
    assert_eq!(
        server.dead_rows_since_vacuum(),
        3,
        "below-threshold checkpoint leaves the accumulator unchanged (no auto-prune)"
    );

    // More deletes push the count to 5 (>= threshold).
    for id in 3..5 {
        conn.ok(&format!("delete from t where id = {id}")).await;
    }
    assert_eq!(server.dead_rows_since_vacuum(), 5);

    // Now a checkpoint auto-prunes and resets the accumulator to 0.
    server.force_checkpoint().await.unwrap();
    assert_eq!(
        server.dead_rows_since_vacuum(),
        0,
        "crossing the threshold makes the checkpoint auto-prune and reset the counter"
    );

    // The surviving rows are exactly the non-deleted ids; the deleted ones stay gone.
    let ids = conn
        .ok("select id from t order by id")
        .await
        .rows()
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        (5..10)
            .map(|id: i32| Some(id.to_string()))
            .collect::<Vec<_>>(),
    );
}

/// A threshold of 0 disables auto-prune: no checkpoint ever auto-prunes, regardless
/// of how much churn accumulates.
#[tokio::test]
async fn auto_prune_disabled_when_threshold_is_zero() {
    let server = TestServer::start_with_config(auto_vacuum_config(0))
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table t (id integer primary key)").await;
    for id in 0..20 {
        conn.ok(&format!("insert into t (id) values ({id})")).await;
    }
    for id in 0..20 {
        conn.ok(&format!("delete from t where id = {id}")).await;
    }
    let dead_before = server.dead_rows_since_vacuum();
    assert_eq!(dead_before, 20);

    server.force_checkpoint().await.unwrap();
    assert_eq!(
        server.dead_rows_since_vacuum(),
        dead_before,
        "threshold 0 disables auto-prune; the accumulator is never reset by a checkpoint"
    );
}

/// Space stays bounded under sustained DELETE+INSERT churn across many checkpoints,
/// with NO operator `VACUUM`. After a warmup that establishes the heap's working-set
/// size, a long churn loop (each iteration deletes a row, inserts a fresh one, and
/// periodically checkpoints) must not let the heap grow unboundedly: the auto-prune
/// reclaims dead versions and `insert_row` reuses the freed slots, so the heap page
/// count stabilizes.
#[tokio::test]
async fn sustained_churn_keeps_heap_bounded_without_operator_vacuum() {
    // Low threshold so every churn batch triggers an auto-prune at the next checkpoint.
    let server = TestServer::start_with_config(auto_vacuum_config(10))
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table churn (id integer primary key, payload text)")
        .await;

    // Warmup: a steady working set of 40 live rows (ids 0..40).
    for id in 0..40 {
        conn.ok(&format!(
            "insert into churn (id, payload) values ({id}, 'row{id}')"
        ))
        .await;
    }
    server.force_checkpoint().await.unwrap();
    let baseline_pages = server.heap_page_count("churn");

    // Sustained churn: 600 delete+insert pairs (15x the working set), checkpointing
    // every 20 iterations so auto-prune runs repeatedly. The live-row count stays 40
    // the whole time; only the id rolls forward (`next_id = 40 + i`), so without
    // reclamation the heap would grow without bound.
    for i in 0..600 {
        let next_id = 40 + i;
        let victim = next_id - 40; // the oldest live id
        conn.ok(&format!("delete from churn where id = {victim}"))
            .await;
        conn.ok(&format!(
            "insert into churn (id, payload) values ({next_id}, 'row{next_id}')"
        ))
        .await;
        if i % 20 == 19 {
            server.force_checkpoint().await.unwrap();
        }
    }
    server.force_checkpoint().await.unwrap();

    // The live set is still exactly 40 rows.
    let live = conn.ok("select count(*) from churn").await.rows();
    assert_eq!(live, vec![vec![Some("40".to_string())]]);

    // Space is bounded: after 600 churn pairs the heap is no larger than a small
    // constant over its warmed-up baseline (reclaimed slots are reused). Without
    // auto-prune the heap would have grown by ~600 tuples' worth of pages.
    let final_pages = server.heap_page_count("churn");
    assert!(
        final_pages <= baseline_pages + 2,
        "heap stayed bounded under churn: baseline={baseline_pages}, final={final_pages} \
         (a growing heap would mean dead versions were never reclaimed)"
    );
}

/// Auto-prune does not change query results: the visible row set and the bank-SUM
/// invariant are identical whether auto-prune fires or is disabled. Two servers run
/// the same DELETE/UPDATE+INSERT workload — one with auto-prune ON (low threshold,
/// frequent checkpoints), one with it OFF (threshold 0) — and must agree exactly.
#[tokio::test]
async fn auto_prune_does_not_change_visible_results() {
    async fn run_workload(threshold: u64) -> (Vec<Vec<Option<String>>>, i64) {
        let server = TestServer::start_with_config(auto_vacuum_config(threshold))
            .await
            .unwrap();
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("create table accounts (id integer primary key, owner text, balance integer)")
            .await;
        conn.ok("create index accounts_owner on accounts (owner)")
            .await;

        // Open 20 accounts of 100 each (total 2000), checkpointing midway.
        for id in 0..20 {
            let owner = if id % 2 == 0 { "even" } else { "odd" };
            conn.ok(&format!(
                "insert into accounts (id, owner, balance) values ({id}, '{owner}', 100)"
            ))
            .await;
        }
        server.force_checkpoint().await.unwrap();

        // Transfers (UPDATE pairs) and churn (DELETE+re-INSERT keeping the total) —
        // each producing dead versions — interleaved with checkpoints.
        for round in 0..15 {
            let a = round % 20;
            let b = (round + 1) % 20;
            conn.ok(&format!(
                "update accounts set balance = balance - 10 where id = {a}"
            ))
            .await;
            conn.ok(&format!(
                "update accounts set balance = balance + 10 where id = {b}"
            ))
            .await;
            // Delete then re-insert the same id with the same balance (no net change).
            conn.ok("delete from accounts where id = 19").await;
            conn.ok("insert into accounts (id, owner, balance) values (19, 'odd', 100)")
                .await;
            server.force_checkpoint().await.unwrap();
        }

        let rows = conn
            .ok("select id, owner, balance from accounts order by id")
            .await
            .rows();
        let sum = total_balance(&mut conn).await;
        (rows, sum)
    }

    let (rows_on, sum_on) = run_workload(5).await; // auto-prune ON
    let (rows_off, sum_off) = run_workload(0).await; // auto-prune OFF

    assert_eq!(
        rows_on, rows_off,
        "the visible row set is identical with auto-prune on vs off"
    );
    assert_eq!(sum_on, sum_off, "the bank-SUM invariant matches");
    assert_eq!(
        sum_on, 2000,
        "the bank invariant holds across auto-pruning checkpoints (transfers + no-net churn)"
    );
}

/// Safety via the checkpoint trigger (mirrors the on-demand horizon-pin test, F4a).
/// A live snapshot advertises an old `xmin`, pinning the GC horizon below a committed
/// delete. An auto-pruning checkpoint captures the horizon UNDER its guard, so it must
/// NOT reclaim a version that snapshot still sees — exactly like on-demand VACUUM.
#[tokio::test]
async fn auto_prune_checkpoint_respects_a_live_snapshot_horizon() {
    let server = TestServer::start_with_config(auto_vacuum_config(1))
        .await
        .unwrap();
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

    // Commit a delete (1 dead version, >= threshold 1) and advance the allocator with
    // more commits, so the active-id min / next_txn_id is well above the held xmin.
    conn.ok("delete from users where id = 1").await;
    for id in 10..20 {
        conn.ok(&format!("insert into users (id, name) values ({id}, 'x')"))
            .await;
    }

    // While the snapshot is held, the horizon is pinned at (or below) its xmin. An
    // auto-pruning checkpoint captures exactly this horizon under the guard.
    assert!(
        components.gc_horizon() <= pinned,
        "the held snapshot pins the horizon below the advanced allocator"
    );
    server.force_checkpoint().await.unwrap();

    // Dropping the advertisement lets the horizon advance; the deferred reclamation can
    // now happen at the next auto-pruning checkpoint.
    drop(held);
    assert!(
        components.gc_horizon() > pinned,
        "releasing the snapshot lets the horizon advance"
    );

    // The visible data is consistent throughout: id 1 is deleted (never resurrected by
    // the auto-prune), id 2 and the later inserts survive.
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
        "the committed delete of id 1 stays invisible across the auto-pruning checkpoint"
    );
}

/// An auto-pruning checkpoint, then crash + restart: the reclaimed state is durable
/// (the vacuum FPIs replay from this checkpoint), live data is intact, and deleted
/// data stays gone — with NO operator `VACUUM`, only the auto-prune at checkpoint.
#[tokio::test]
async fn auto_pruned_state_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = auto_vacuum_config(3);
    config.data_dir = dir.path().to_path_buf();

    {
        let server = TestServer::start_with_config(config.clone()).await.unwrap();
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
        // Delete the three 'gone' rows (>= threshold 3), then force a checkpoint that
        // auto-prunes them and flushes the vacuum FPIs durably.
        conn.ok("delete from users where name = 'gone'").await;
        assert_eq!(server.dead_rows_since_vacuum(), 3);
        server.force_checkpoint().await.unwrap();
        assert_eq!(
            server.dead_rows_since_vacuum(),
            0,
            "the checkpoint auto-pruned the deleted rows"
        );
        // Reuse a reclaimed slot after the auto-prune to exercise reclaim + insert
        // replay through recovery.
        conn.ok("insert into users (id, name) values (100, 'keep')")
            .await;
        // The drop here triggers a graceful shutdown.
    }

    // Restart from the same data dir: recovery replays the auto-prune FPIs and the
    // reclaim+reuse insert.
    let server = TestServer::start_with_config(config).await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let ids = conn
        .ok("select id from users order by id")
        .await
        .rows()
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            Some("0".to_string()),
            Some("2".to_string()),
            Some("4".to_string()),
            Some("100".to_string()),
        ],
        "kept rows + the reinsert survive; 'gone' rows stay reclaimed after restart"
    );

    let gone = conn
        .ok("select id from users where name = 'gone'")
        .await
        .rows();
    assert!(
        gone.is_empty(),
        "auto-pruned 'gone' rows stay gone after restart"
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
