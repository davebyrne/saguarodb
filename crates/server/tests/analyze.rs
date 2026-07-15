//! End-to-end `ANALYZE` behavior over the wire (`docs/specs/statistics.md`
//! §5, §7): statement forms, command tags, locking, the
//! `default_statistics_target` GUC, and durability across restart.

mod support;

use std::time::Duration;

use common::{NDistinct, SqlState, TableStatistics};
use saguarodb_server::config::Config;
use support::{Connection, TestServer, command_tags};

fn table_statistics(server: &TestServer, table: &str) -> Option<TableStatistics> {
    let catalog = &server.app().components.catalog;
    let schema = catalog
        .get_table_by_name(table)
        .unwrap()
        .unwrap_or_else(|| panic!("table {table} should exist"));
    catalog.get_table_statistics(schema.id).unwrap()
}

async fn create_skewed_users(conn: &mut Connection) {
    conn.ok("create table users (id integer primary key, name text)")
        .await;
    let mut insert = String::from("insert into users (id, name) values ");
    for id in 0..100 {
        if id > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({id}, 'name{}')", id % 4));
    }
    conn.ok(&insert).await;
}

#[tokio::test]
async fn analyze_collects_statistics_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        let mut conn = Connection::connect(&server).await.unwrap();
        create_skewed_users(&mut conn).await;
        assert_eq!(table_statistics(&server, "users"), None);

        let response = conn.query_raw("analyze users").await.unwrap();
        assert_eq!(command_tags(&response).unwrap(), vec!["ANALYZE"]);

        let stats = table_statistics(&server, "users").expect("statistics after ANALYZE");
        assert_eq!(stats.row_count, 100);
        assert!(stats.page_count >= 1);
        assert_eq!(stats.columns[&1].n_distinct, NDistinct::Count(4));
        assert_eq!(stats.columns[&1].most_common.len(), 4);
    }

    // Statistics are durable: the committed WAL record (or a later manifest)
    // restores them on reopen.
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let stats = table_statistics(&server, "users").expect("statistics after restart");
    assert_eq!(stats.row_count, 100);
    assert_eq!(stats.columns[&1].n_distinct, NDistinct::Count(4));
}

#[tokio::test]
async fn concurrent_analyze_commits_form_a_recoverable_before_chain() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        let mut setup = Connection::connect(&server).await.unwrap();
        create_skewed_users(&mut setup).await;
        let mut first = Connection::connect(&server).await.unwrap();
        let mut second = Connection::connect(&server).await.unwrap();

        let (first_result, second_result) = tokio::join!(
            first.query_raw("analyze users"),
            second.query_raw("analyze users")
        );
        assert_eq!(
            command_tags(&first_result.unwrap()).unwrap(),
            vec!["ANALYZE"]
        );
        assert_eq!(
            command_tags(&second_result.unwrap()).unwrap(),
            vec!["ANALYZE"]
        );
    }

    let reopened = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let stats = table_statistics(&reopened, "users").expect("statistics after concurrent ANALYZE");
    assert_eq!(stats.row_count, 100);
}

#[tokio::test]
async fn analyze_without_a_table_covers_every_user_table() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table a (id integer primary key)").await;
    conn.ok("create table b (id integer primary key)").await;
    conn.ok("insert into a (id) values (1), (2)").await;

    let response = conn.query_raw("analyze").await.unwrap();
    assert_eq!(command_tags(&response).unwrap(), vec!["ANALYZE"]);

    let stats_a = table_statistics(&server, "a").expect("statistics for a");
    assert_eq!(stats_a.row_count, 2);
    let stats_b = table_statistics(&server, "b").expect("statistics for b");
    assert_eq!(stats_b.row_count, 0);
}

#[tokio::test]
async fn vacuum_analyze_runs_the_statistics_pass_with_the_vacuum_tag() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    create_skewed_users(&mut conn).await;

    let response = conn.query_raw("vacuum analyze users").await.unwrap();
    assert_eq!(command_tags(&response).unwrap(), vec!["VACUUM"]);
    let stats = table_statistics(&server, "users").expect("statistics after VACUUM ANALYZE");
    assert_eq!(stats.row_count, 100);

    // Plain VACUUM must not refresh statistics.
    conn.ok("insert into users (id, name) values (100, 'name0')")
        .await;
    conn.ok("vacuum users").await;
    let stats = table_statistics(&server, "users").unwrap();
    assert_eq!(stats.row_count, 100, "plain VACUUM leaves statistics alone");
}

#[tokio::test]
async fn analyze_is_rejected_inside_a_transaction_block_and_on_unknown_tables() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    conn.ok("begin").await;
    let outcome = conn.query("analyze t").await.unwrap();
    let err = outcome
        .result
        .err()
        .expect("ANALYZE inside a transaction block is rejected");
    assert_eq!(err.code, SqlState::FeatureNotSupported);
    assert!(err.message.contains("transaction block"), "{}", err.message);
    conn.ok("rollback").await;

    let outcome = conn.query("analyze missing").await.unwrap();
    let err = outcome
        .result
        .err()
        .expect("ANALYZE of an unknown table is rejected");
    assert_eq!(err.code, SqlState::UndefinedTable);
    assert_eq!(outcome.status, b'I', "no transaction block is left open");
}

#[tokio::test]
async fn statistics_target_guc_bounds_the_histogram_and_validates() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table wide (id integer primary key)").await;
    let mut insert = String::from("insert into wide (id) values ");
    for id in 0..200 {
        if id > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({id})"));
    }
    conn.ok(&insert).await;

    let shown = conn.ok("show default_statistics_target").await;
    assert_eq!(shown.rows(), vec![vec![Some("100".to_string())]]);

    conn.ok("set default_statistics_target = 2").await;
    conn.ok("analyze wide").await;
    let stats = table_statistics(&server, "wide").expect("statistics");
    assert!(
        stats.columns[&0].histogram_bounds.len() <= 3,
        "target 2 allows at most 3 histogram bounds, got {}",
        stats.columns[&0].histogram_bounds.len()
    );

    for bad in [
        "set default_statistics_target = 0",
        "set default_statistics_target = 1001",
        "set default_statistics_target = 'lots'",
    ] {
        let outcome = conn.query(bad).await.unwrap();
        let err = outcome.result.err().expect("out-of-range target rejected");
        assert_eq!(err.code, SqlState::InvalidParameterValue, "for `{bad}`");
    }

    conn.ok("reset default_statistics_target").await;
    let shown = conn.ok("show default_statistics_target").await;
    assert_eq!(shown.rows(), vec![vec![Some("100".to_string())]]);
}

#[tokio::test]
async fn analyze_does_not_block_behind_an_open_writer() {
    // AccessShare on the target: a writer holding row locks in an open
    // transaction must not block ANALYZE (unlike VACUUM's Share lock).
    let server = TestServer::start().await.unwrap();
    let mut writer = Connection::connect(&server).await.unwrap();
    let mut maintenance = Connection::connect(&server).await.unwrap();
    writer
        .ok("create table busy (id integer primary key, value integer)")
        .await;
    writer
        .ok("insert into busy (id, value) values (1, 10), (2, 20)")
        .await;
    writer.ok("begin").await;
    writer.ok("update busy set value = 11 where id = 1").await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), maintenance.ok("analyze busy"))
        .await
        .expect("ANALYZE must not block behind an open writer");
    assert!(outcome.result.is_ok(), "ANALYZE should succeed");
    let stats = table_statistics(&server, "busy").expect("statistics");
    // The writer's uncommitted update is invisible; both committed rows count.
    assert_eq!(stats.row_count, 2);

    writer.ok("rollback").await;
}

#[tokio::test]
async fn pg_class_and_pg_stats_expose_statistics() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    create_skewed_users(&mut conn).await;

    // Never analyzed: PostgreSQL's "unknown" convention and no pg_stats rows.
    let rows = conn
        .ok("select relpages, reltuples from pg_class where relname = 'users'")
        .await
        .rows();
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string()), Some("-1".to_string())]]
    );
    let rows = conn
        .ok("select count(*) from pg_stats where tablename = 'users'")
        .await
        .rows();
    assert_eq!(rows, vec![vec![Some("0".to_string())]]);

    conn.ok("analyze users").await;

    let rows = conn
        .ok("select relpages, reltuples from pg_class where relname = 'users'")
        .await
        .rows();
    assert_eq!(rows.len(), 1);
    assert!(rows[0][0].as_deref().unwrap().parse::<i64>().unwrap() >= 1);
    assert_eq!(rows[0][1].as_deref(), Some("100"));

    let rows = conn
        .ok(
            "select attname, null_frac, avg_width, n_distinct, most_common_vals, \
             most_common_freqs, histogram_bounds, correlation \
             from pg_stats where tablename = 'users' order by attname",
        )
        .await
        .rows();
    assert_eq!(rows.len(), 2);
    // `id` is unique: negative-fraction n_distinct, histogram only.
    assert_eq!(rows[0][0].as_deref(), Some("id"));
    assert_eq!(rows[0][1].as_deref(), Some("0"));
    assert_eq!(rows[0][3].as_deref(), Some("-1"));
    assert_eq!(rows[0][4], None, "unique column has no MCVs");
    assert!(rows[0][6].as_deref().unwrap().starts_with("{0,"));
    assert_eq!(rows[0][7], None, "correlation is NULL in v1");
    // `name` has four heavy values: MCVs cover everything, no histogram.
    assert_eq!(rows[1][0].as_deref(), Some("name"));
    assert_eq!(rows[1][3].as_deref(), Some("4"));
    assert_eq!(
        rows[1][4].as_deref(),
        Some("{name0,name1,name2,name3}"),
        "ties break by value order"
    );
    assert_eq!(rows[1][5].as_deref(), Some("{0.25,0.25,0.25,0.25}"));
    assert_eq!(rows[1][6], None, "MCVs cover every sampled value");
}

#[tokio::test]
async fn pg_stats_quotes_array_elements_that_need_it() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table people (id integer primary key, name text)")
        .await;
    // A repeated value containing a space and a comma must come back quoted
    // in the PostgreSQL array-output form.
    conn.ok("insert into people (id, name) values (1, 'Smith, Jo'), (2, 'Smith, Jo'), (3, 'x')")
        .await;
    conn.ok("analyze people").await;

    let rows = conn
        .ok("select most_common_vals from pg_stats where tablename = 'people' and attname = 'name'")
        .await
        .rows();
    assert_eq!(rows, vec![vec![Some(r#"{"Smith, Jo"}"#.to_string())]]);
}

#[tokio::test]
async fn analyzed_plans_swap_the_hash_build_side_and_still_join_correctly() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table small (id integer primary key)").await;
    conn.ok("insert into small (id) values (1), (2), (3)").await;
    conn.ok("create table big (id integer primary key)").await;
    let mut insert = String::from("insert into big (id) values ");
    for id in 0..200 {
        if id > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({id})"));
    }
    conn.ok(&insert).await;

    // Un-analyzed: historical build-right shape.
    let plan = conn
        .ok("explain select small.id from small join big on small.id = big.id")
        .await
        .rows();
    let plan_text = plan[0][0].as_deref().unwrap().to_string();
    assert!(plan_text.contains("build=right"), "{plan_text}");

    conn.ok("analyze").await;

    let plan = conn
        .ok("explain select small.id from small join big on small.id = big.id")
        .await
        .rows();
    let plan_text = plan
        .iter()
        .map(|row| row[0].as_deref().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("build=left"),
        "the 3-row side should become the build side:\n{plan_text}"
    );

    // The swapped join still returns the right rows in left ++ right order.
    let rows = conn
        .ok("select small.id, big.id from small join big on small.id = big.id order by small.id")
        .await
        .rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("1".to_string())],
            vec![Some("2".to_string()), Some("2".to_string())],
            vec![Some("3".to_string()), Some("3".to_string())],
        ]
    );
}

#[tokio::test]
async fn swapped_hash_join_handles_duplicate_and_null_keys() {
    // The build=left assembly path with the realistic shape: a small build
    // side with duplicate join keys and a NULL key, joined N:M into a larger
    // probe side with several matches per key and its own NULL key.
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table small (id integer primary key, k integer, tag text)")
        .await;
    conn.ok("insert into small (id, k, tag) values (1, 10, 'a'), (2, 10, 'b'), (3, null, 'c')")
        .await;
    conn.ok("create table big (id integer primary key, k integer, val text)")
        .await;
    let mut insert = String::from("insert into big (id, k, val) values ");
    for id in 0..100 {
        // Filler keys that never match small's.
        insert.push_str(&format!("({id}, {}, 'f{id}'),", id + 1000));
    }
    insert.push_str("(101, 10, 'x1'), (102, 10, 'x2'), (103, 10, 'x3'), (104, null, 'xnull')");
    conn.ok(&insert).await;
    conn.ok("analyze").await;

    let plan = conn
        .ok("explain select small.tag from small join big on small.k = big.k")
        .await
        .rows();
    let plan_text = plan
        .iter()
        .map(|row| row[0].as_deref().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan_text.contains("build=left"), "{plan_text}");

    let rows = conn
        .ok(
            "select small.tag, big.val from small join big on small.k = big.k \
             order by small.tag, big.val",
        )
        .await
        .rows();
    let expected: Vec<Vec<Option<String>>> = [
        ("a", "x1"),
        ("a", "x2"),
        ("a", "x3"),
        ("b", "x1"),
        ("b", "x2"),
        ("b", "x3"),
    ]
    .iter()
    .map(|(tag, val)| vec![Some(tag.to_string()), Some(val.to_string())])
    .collect();
    assert_eq!(
        rows, expected,
        "duplicate keys join N:M with distinct side values; NULL keys never match"
    );
}

#[tokio::test]
async fn analyze_runs_through_the_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table ext (id integer primary key)").await;
    conn.ok("insert into ext (id) values (1)").await;

    let outcome = conn.extended_execute("analyze ext").await.unwrap();
    assert!(outcome.result.is_ok(), "extended ANALYZE should succeed");
    let stats = table_statistics(&server, "ext").expect("statistics");
    assert_eq!(stats.row_count, 1);
}

/// A config that never checkpoints on its own; tests drive checkpoints
/// explicitly so the auto-analyze gating is deterministic (mirroring the
/// auto-prune tests in `vacuum.rs`).
fn auto_analyze_config(threshold: u64) -> Config {
    Config {
        buffer_pool_frames: 64,
        checkpoint_every_n_commits: 1_000_000,
        checkpoint_wal_bytes: 1 << 40,
        auto_vacuum_dead_rows: 0,
        auto_analyze_changed_rows: threshold,
        shutdown_timeout_ms: 1_000,
        ..Config::default()
    }
}

#[tokio::test]
async fn checkpoint_auto_analyze_fires_at_the_changed_rows_threshold() {
    let server = TestServer::start_with_config(auto_analyze_config(50))
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;

    let mut insert = String::from("insert into t (id) values ");
    for id in 0..30 {
        if id > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({id})"));
    }
    conn.ok(&insert).await;
    server.force_checkpoint().await.unwrap();
    assert_eq!(
        table_statistics(&server, "t"),
        None,
        "30 changed rows stay under the 50-row threshold"
    );

    let mut insert = String::from("insert into t (id) values ");
    for id in 30..60 {
        if id > 30 {
            insert.push(',');
        }
        insert.push_str(&format!("({id})"));
    }
    conn.ok(&insert).await;
    server.force_checkpoint().await.unwrap();
    let stats = table_statistics(&server, "t").expect("threshold crossed: statistics collected");
    assert_eq!(stats.row_count, 60);
    assert_eq!(
        server
            .app()
            .components
            .rows_changed_since_analyze
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "the accumulator resets after the pass"
    );

    // Under-threshold churn leaves the statistics alone.
    conn.ok("insert into t (id) values (100)").await;
    server.force_checkpoint().await.unwrap();
    let stats = table_statistics(&server, "t").unwrap();
    assert_eq!(stats.row_count, 60, "no refresh below the threshold");
}

#[tokio::test]
async fn auto_analyze_zero_threshold_disables_and_manual_full_analyze_resets() {
    let server = TestServer::start_with_config(auto_analyze_config(0))
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key)").await;
    let mut insert = String::from("insert into t (id) values ");
    for id in 0..100 {
        if id > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({id})"));
    }
    conn.ok(&insert).await;
    server.force_checkpoint().await.unwrap();
    assert_eq!(
        table_statistics(&server, "t"),
        None,
        "0 disables auto-analyze"
    );

    // The changed-rows accumulator still counts, and a manual full ANALYZE
    // resets it (docs/specs/statistics.md §10).
    let counter = &server.app().components.rows_changed_since_analyze;
    assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), 100);
    conn.ok("analyze t").await;
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Acquire),
        100,
        "a single-table ANALYZE leaves the accumulator alone"
    );
    conn.ok("analyze").await;
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Acquire),
        0,
        "a full ANALYZE resets the accumulator"
    );
}

#[tokio::test]
async fn copy_from_rows_count_toward_auto_analyze() {
    let server = TestServer::start_with_config(auto_analyze_config(0))
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table t (id integer primary key, name text)")
        .await;
    let counter = &server.app().components.rows_changed_since_analyze;

    // Autocommit COPY FROM: counted on its durable commit.
    let copy = conn
        .copy_from("copy t from stdin", &[b"1\tann\n2\tbob\n"])
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 2"));
    assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), 2);

    // COPY inside an explicit transaction: folded only on COMMIT.
    conn.ok("begin").await;
    let copy = conn
        .copy_from("copy t from stdin", &[b"3\tcarol\n"])
        .await
        .unwrap();
    assert_eq!(copy.command_tag.as_deref(), Some("COPY 1"));
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Acquire),
        2,
        "in-transaction COPY counts only at COMMIT"
    );
    conn.ok("commit").await;
    assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), 3);
}

#[tokio::test]
async fn analyze_excludes_non_finite_arrays_and_survives_restart() {
    // Regression for the SQL-arrays integration: a repeated ARRAY[1e400]
    // (containing Infinity) must be excluded from MCVs/histograms — a
    // non-finite value inside an array would otherwise serialize as JSON
    // `null` in the manifest/WAL statistics payloads and brick the next
    // startup, exactly like the scalar case.
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("create table arr (id integer primary key, vals double precision[])")
            .await;
        conn.ok(
            "insert into arr values (1, ARRAY[1e400]), (2, ARRAY[1e400]), (3, ARRAY[1.5, 2.5])",
        )
        .await;
        let response = conn.query_raw("analyze arr").await.unwrap();
        assert_eq!(command_tags(&response).unwrap(), vec!["ANALYZE"]);

        let stats = table_statistics(&server, "arr").expect("statistics");
        assert_eq!(stats.row_count, 3);
        let vals = &stats.columns[&1];
        assert!(
            vals.most_common.is_empty(),
            "the only repeated value contains Infinity and must be excluded"
        );
        server.force_checkpoint().await.unwrap();
    }

    // The manifest carries the statistics and the database still opens.
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let stats = table_statistics(&server, "arr").expect("statistics after restart");
    assert_eq!(stats.row_count, 3);
}
