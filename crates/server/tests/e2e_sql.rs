mod support;

use support::{TestServer, WorkspaceGraph};

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn e2e_create_insert_select_update_delete_explain() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text, active boolean)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name, active) values (1, 'Ada', true)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name, active) values (2, 'Grace', false)")
        .await
        .unwrap();

    let rows = server
        .simple_query("select name from users where id = 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("Ada".to_string())]]);

    server
        .simple_query("update users set active = true where id = 2")
        .await
        .unwrap();
    server
        .simple_query("delete from users where id = 1")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id, active from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string()), Some("t".to_string())]]
    );

    let explain = server
        .simple_query("explain select name from users where id = 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(explain[0][0].as_ref().unwrap().contains("IndexScan"));
}

#[tokio::test]
async fn e2e_delete_then_reinsert_same_key_succeeds() {
    // MVCC DELETE stamps xmax in place (no tombstone) and retains index entries, so
    // re-inserting the deleted primary key now succeeds: the committed-deleted
    // version no longer blocks it.
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada')")
        .await
        .unwrap();
    server
        .simple_query("delete from users where id = 1")
        .await
        .unwrap();

    // The deleted row is hidden.
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    // Re-inserting the same key now succeeds (previously a tombstone-then-reinsert;
    // now a committed-delete + insert).
    server
        .simple_query("insert into users (id, name) values (1, 'Bea')")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("Bea".to_string())]]
    );
}

#[tokio::test]
async fn e2e_update_new_version_is_visible_via_seq_and_index_scans() {
    // MVCC UPDATE writes a new heap version and inserts a per-version entry into
    // *every* index (the changed-column index and the unchanged-column index), so
    // after a committed UPDATE a SELECT sees the new value via a sequential scan,
    // an index scan on the changed column, AND a scan on an unchanged secondary
    // value (the anti-HOT-bug check: the unchanged-column index got an entry too).
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text, city text)")
        .await
        .unwrap();
    // city is changed by the update; name is the unchanged secondary value.
    server
        .simple_query("create index users_name on users (name)")
        .await
        .unwrap();
    server
        .simple_query("create index users_city on users (city)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name, city) values (1, 'Ada', 'paris')")
        .await
        .unwrap();

    server
        .simple_query("update users set city = 'london' where id = 1")
        .await
        .unwrap();

    // Sequential scan sees the new value.
    let rows = server
        .simple_query("select id, name, city from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("1".to_string()),
            Some("Ada".to_string()),
            Some("london".to_string()),
        ]]
    );

    // Index scan on the CHANGED column (city) returns the new version; the old
    // value resolves nothing.
    let rows = server
        .simple_query("select id from users where city = 'london'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    let rows = server
        .simple_query("select id from users where city = 'paris'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());

    // Index scan on the UNCHANGED column (name) STILL returns the row: the new
    // version got an entry in the unchanged-column index too.
    let rows = server
        .simple_query("select id, city from users where name = 'Ada'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("london".to_string())]]
    );
}

#[tokio::test]
async fn e2e_create_index_and_unique_constraint() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (1, 'Ada')")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (2, 'Grace')")
        .await
        .unwrap();

    // CREATE INDEX over the real wire protocol.
    server
        .simple_query("create index users_name on users (name)")
        .await
        .unwrap();

    // Queries still return the right rows after the index is built.
    let rows = server
        .simple_query("select id from users where name = 'Ada'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);

    // A unique index rejects a duplicate value through the protocol.
    server
        .simple_query("create unique index uq_name on users (name)")
        .await
        .unwrap();
    let err = server
        .simple_query("insert into users (id, name) values (3, 'Ada')")
        .await
        .err()
        .expect("duplicate value should violate the unique index");
    assert!(err.message.to_lowercase().contains("unique"));

    // DROP INDEX over the protocol.
    server.simple_query("drop index uq_name").await.unwrap();
    server.simple_query("drop index users_name").await.unwrap();
}

#[tokio::test]
async fn e2e_order_by_ordinal_sorts_by_output_column() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table nums (id integer primary key, label text)")
        .await
        .unwrap();
    for (id, label) in [(3, "c"), (1, "a"), (2, "b")] {
        server
            .simple_query(&format!(
                "insert into nums (id, label) values ({id}, '{label}')"
            ))
            .await
            .unwrap();
    }

    // ORDER BY 2 sorts by the second output column (id), ascending.
    let rows = server
        .simple_query("select label, id from nums order by 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("a".to_string()), Some("1".to_string())],
            vec![Some("b".to_string()), Some("2".to_string())],
            vec![Some("c".to_string()), Some("3".to_string())],
        ]
    );

    // ORDER BY 1 DESC sorts by the first output column (id), descending.
    let rows = server
        .simple_query("select id from nums order by 1 desc")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("3".to_string())],
            vec![Some("2".to_string())],
            vec![Some("1".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_hash_join_returns_matching_rows() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("create table accounts (id integer primary key, owner text)")
        .await
        .unwrap();
    for (id, name) in [(1, "Ada"), (2, "Grace")] {
        server
            .simple_query(&format!(
                "insert into users (id, name) values ({id}, '{name}')"
            ))
            .await
            .unwrap();
    }
    for (id, owner) in [(10, "Ada"), (20, "Linus")] {
        server
            .simple_query(&format!(
                "insert into accounts (id, owner) values ({id}, '{owner}')"
            ))
            .await
            .unwrap();
    }

    // Inner equi-join on name; only Ada matches.
    let rows = server
        .simple_query(
            "select users.id, accounts.id from users join accounts \
             on users.name = accounts.owner order by users.id",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("10".to_string())]]
    );

    let explain = server
        .simple_query(
            "explain select users.id from users join accounts on users.name = accounts.owner",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert!(explain[0][0].as_ref().unwrap().contains("HashJoin"));
}

#[tokio::test]
async fn e2e_insert_select_from_target_table_sees_only_preexisting_rows() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    for (id, name) in [(1, "Ada"), (2, "Grace")] {
        server
            .simple_query(&format!(
                "insert into users (id, name) values ({id}, '{name}')"
            ))
            .await
            .unwrap();
    }

    // Halloween problem: reading the target table must observe only the two
    // pre-insert rows, so exactly two rows are appended (against the real
    // on-disk B-tree scan).
    server
        .simple_query("insert into users select id + 10, name from users")
        .await
        .unwrap();

    let rows = server
        .simple_query("select id from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![Some("11".to_string())],
            vec![Some("12".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_scalar_functions_evaluate() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    server
        .simple_query("insert into users (id, name) values (-5, '  Ada  ')")
        .await
        .unwrap();

    let rows = server
        .simple_query(
            "select upper(name), length(trim(name)), abs(id), substring(name, 3, 3) from users",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("  ADA  ".to_string()),
            Some("3".to_string()),
            Some("5".to_string()),
            Some("Ada".to_string()),
        ]]
    );
}

#[tokio::test]
async fn protocol_decode_error_sends_error_and_closes_connection() {
    let server = TestServer::start().await.unwrap();
    let mut stream = server.connect_raw().await.unwrap();

    stream.write_all(b"!").await.unwrap();

    let mut response = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("server did not close connection after protocol error")
        .unwrap();

    assert!(read > 0);
    assert_eq!(response[0], b'E');
}

#[test]
fn crate_dependency_graph_has_no_forbidden_edges() {
    let graph = WorkspaceGraph::load_from_manifest_dir(env!("CARGO_MANIFEST_DIR")).unwrap();

    assert!(!graph.depends_on("saguarodb-parser", "saguarodb-catalog"));
    assert!(!graph.depends_on("saguarodb-planner", "saguarodb-storage"));
    assert!(!graph.depends_on("saguarodb-storage", "saguarodb-planner"));
    assert!(!graph.any_library_depends_on("saguarodb-server"));
}

#[test]
fn dependency_graph_detects_table_style_dependency_edges() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("Cargo.toml"),
        r#"
[workspace]
members = [
  "crates/parser",
  "crates/tool",
]
"#,
    )
    .unwrap();

    let parser_dir = dir.path().join("crates/parser");
    std::fs::create_dir_all(parser_dir.join("src")).unwrap();
    std::fs::write(parser_dir.join("src/lib.rs"), "").unwrap();
    std::fs::write(
        parser_dir.join("Cargo.toml"),
        r#"
[package]
name = "saguarodb-parser"
version = "0.1.0"
edition = "2024"

[dependencies.catalog]
package = "saguarodb-catalog"
path = "../catalog"
"#,
    )
    .unwrap();

    let tool_dir = dir.path().join("crates/tool");
    std::fs::create_dir_all(tool_dir.join("src")).unwrap();
    std::fs::write(tool_dir.join("src/lib.rs"), "").unwrap();
    std::fs::write(tool_dir.join("src/main.rs"), "fn main() {}\n").unwrap();
    std::fs::write(
        tool_dir.join("Cargo.toml"),
        r#"
[package]
name = "saguarodb-tool"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "saguarodb-tool"
path = "src/main.rs"

[dependencies.server]
package = "saguarodb-server"
path = "../server"
"#,
    )
    .unwrap();

    let graph = WorkspaceGraph::load_from_manifest_dir(parser_dir.to_str().unwrap()).unwrap();

    assert!(graph.depends_on("saguarodb-parser", "saguarodb-catalog"));
    assert!(graph.any_library_depends_on("saguarodb-server"));
}

#[tokio::test]
async fn read_until_ready_times_out_when_connection_stays_open() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"R").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let result = support::read_until_ready_with_timeout(&mut stream, Duration::from_millis(10))
        .await
        .unwrap_err();

    assert!(
        result
            .message
            .contains("timed out waiting for ReadyForQuery")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn e2e_aggregate_distinct_deduplicates_arguments() {
    let server = TestServer::start().await.unwrap();

    server
        .simple_query("create table sales (id integer primary key, region text, amount integer)")
        .await
        .unwrap();
    for (id, region, amount) in [
        (1, "west", "10"),
        (2, "west", "10"),
        (3, "west", "20"),
        (4, "east", "30"),
        (5, "east", "30"),
        (6, "east", "null"),
    ] {
        server
            .simple_query(&format!(
                "insert into sales (id, region, amount) values ({id}, '{region}', {amount})"
            ))
            .await
            .unwrap();
    }

    // count(distinct amount) dedups {10,20,30} and ignores the NULL => 3.
    let rows = server
        .simple_query("select count(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("3".to_string())]]);

    // sum(distinct amount) = 10 + 20 + 30 = 60.
    let rows = server
        .simple_query("select sum(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("60".to_string())]]);

    // avg(distinct amount) = 60 / 3 = 20.
    let rows = server
        .simple_query("select avg(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("20".to_string())]]);

    // min/max are unaffected by DISTINCT but must still be accepted.
    let rows = server
        .simple_query("select min(distinct amount), max(distinct amount) from sales")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("10".to_string()), Some("30".to_string())]]
    );

    // DISTINCT applies per group.
    let rows = server
        .simple_query(
            "select region, count(distinct amount) from sales group by region order by region",
        )
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("east".to_string()), Some("1".to_string())],
            vec![Some("west".to_string()), Some("2".to_string())],
        ]
    );
}

#[tokio::test]
async fn e2e_count_distinct_wildcard_is_rejected() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table sales (id integer primary key, amount integer)")
        .await
        .unwrap();

    // COUNT(DISTINCT *) is not valid SQL; DISTINCT requires an explicit argument.
    let err = server
        .simple_query("select count(distinct *) from sales")
        .await
        .err()
        .expect("expected count(distinct *) to be rejected");
    assert!(
        err.message.contains("42601"),
        "expected a syntax error, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_select_distinct_deduplicates_rows() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, region text, tier integer)")
        .await
        .unwrap();
    for (id, region, tier) in [
        (1, "west", "1"),
        (2, "west", "1"),
        (3, "west", "2"),
        (4, "east", "1"),
        (5, "east", "1"),
    ] {
        server
            .simple_query(&format!(
                "insert into t (id, region, tier) values ({id}, '{region}', {tier})"
            ))
            .await
            .unwrap();
    }

    // DISTINCT over (region, tier) collapses the duplicate (west,1) and (east,1).
    let rows = server
        .simple_query("select distinct region, tier from t order by region, tier")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("east".to_string()), Some("1".to_string())],
            vec![Some("west".to_string()), Some("1".to_string())],
            vec![Some("west".to_string()), Some("2".to_string())],
        ]
    );

    // DISTINCT over a single column.
    let rows = server
        .simple_query("select distinct region from t order by region")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("east".to_string())],
            vec![Some("west".to_string())]
        ]
    );

    // LIMIT applies to the distinct rows, not the pre-dedup rows.
    let rows = server
        .simple_query("select distinct region from t order by region limit 1")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("east".to_string())]]);
}

#[tokio::test]
async fn e2e_select_distinct_over_grouped_aggregate() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer)")
        .await
        .unwrap();
    // Groups a=1 -> 2 rows, a=2 -> 2 rows, a=3 -> 1 row, so the per-group counts
    // are {2, 2, 1}. This exercises DISTINCT over rewritten aggregate LocalRefs:
    // the Distinct node sits above Aggregate/Sort and dedups the count outputs.
    for (id, a) in [(1, "1"), (2, "1"), (3, "2"), (4, "2"), (5, "3")] {
        server
            .simple_query(&format!("insert into t (id, a) values ({id}, {a})"))
            .await
            .unwrap();
    }

    let rows = server
        .simple_query("select distinct count(*) from t group by a order by count(*)")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );
}

#[tokio::test]
async fn e2e_explain_select_distinct_shows_distinct_node() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, region text)")
        .await
        .unwrap();

    let explain = server
        .simple_query("explain select distinct region from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("Distinct"),
        "EXPLAIN output missing Distinct node: {:?}",
        explain[0][0]
    );
}

#[tokio::test]
async fn e2e_select_distinct_collapses_nulls() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table n (id integer primary key, v integer)")
        .await
        .unwrap();
    for (id, v) in [(1, "null"), (2, "null"), (3, "5"), (4, "5")] {
        server
            .simple_query(&format!("insert into n (id, v) values ({id}, {v})"))
            .await
            .unwrap();
    }

    // Two NULLs are not distinct from each other: {NULL, 5}.
    let mut rows = server
        .simple_query("select distinct v from n")
        .await
        .unwrap()
        .unwrap_rows();
    rows.sort();
    assert_eq!(rows, vec![vec![None], vec![Some("5".to_string())]]);
}

#[tokio::test]
async fn e2e_select_distinct_rejects_order_by_outside_select_list() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, a integer, b integer)")
        .await
        .unwrap();

    // For SELECT DISTINCT, ORDER BY must reference the select list. `b` is not
    // projected, so this is an invalid_column_reference (42P10).
    let err = server
        .simple_query("select distinct a from t order by b")
        .await
        .err()
        .expect("expected ORDER BY outside select list to be rejected");
    assert!(
        err.message.contains("42P10"),
        "expected invalid_column_reference, got: {}",
        err.message
    );
}

#[tokio::test]
async fn e2e_plain_and_distinct_aggregate_coexist_in_one_select() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key, v integer)")
        .await
        .unwrap();
    for (id, v) in [(1, "10"), (2, "10"), (3, "20")] {
        server
            .simple_query(&format!("insert into t (id, v) values ({id}, {v})"))
            .await
            .unwrap();
    }

    // count(v) and count(distinct v) over the same argument must not collapse
    // into one aggregate: 3 non-null values, 2 distinct values.
    let rows = server
        .simple_query("select count(v), count(distinct v) from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("3".to_string()), Some("2".to_string())]]
    );
}
