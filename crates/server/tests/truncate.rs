mod support;

use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::Duration;

use support::{Connection, TestServer, command_tags};

fn count_files(path: &Path) -> usize {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| if path.is_dir() { count_files(&path) } else { 1 })
        .sum()
}

#[tokio::test]
async fn truncate_table_removes_rows_and_returns_command_tag() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key, name text)")
        .await;
    conn.ok("insert into users (id, name) values (1, 'Ada'), (2, 'Grace')")
        .await;

    let response = conn.query_raw("truncate table users").await.unwrap();
    assert_eq!(command_tags(&response).unwrap(), vec!["TRUNCATE TABLE"]);
    assert!(conn.ok("select id from users").await.rows().is_empty());

    conn.ok("insert into users (id, name) values (1, 'Ada again')")
        .await;
    assert_eq!(
        conn.ok("select name from users where id = 1").await.rows(),
        vec![vec![Some("Ada again".to_string())]]
    );
}

#[tokio::test]
async fn truncate_multiple_tables_swaps_every_generation_together() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table accounts (id integer primary key, balance integer)")
        .await;
    conn.ok("create table history (id integer primary key, note text)")
        .await;
    conn.ok("create index history_note on history (note)").await;
    conn.ok("insert into accounts values (1, 100)").await;
    conn.ok("insert into history values (1, 'before')").await;

    let response = conn
        .query_raw("truncate table accounts, history")
        .await
        .unwrap();
    assert_eq!(command_tags(&response).unwrap(), vec!["TRUNCATE TABLE"]);
    assert!(conn.ok("select id from accounts").await.rows().is_empty());
    assert!(
        conn.ok("select id from history where note = 'before'")
            .await
            .rows()
            .is_empty()
    );

    conn.ok("insert into accounts values (2, 200)").await;
    conn.ok("insert into history values (2, 'after')").await;
    assert_eq!(
        conn.ok("select id from history where note = 'after'")
            .await
            .rows(),
        vec![vec![Some("2".to_string())]]
    );
}

#[tokio::test]
async fn truncate_validates_all_targets_before_allocating_transaction_or_storage_ids() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table kept (id integer primary key)").await;
    conn.ok("insert into kept values (1)").await;

    let next_txn_id = server.app().components.next_txn_id.load(Ordering::Acquire);
    let next_storage_id = server
        .app()
        .components
        .catalog
        .snapshot()
        .unwrap()
        .next_storage_id;

    let outcome = conn
        .query("truncate table kept, missing_after")
        .await
        .unwrap();
    let err = outcome
        .result
        .err()
        .expect("late missing target should reject the complete statement");
    assert!(err.message.contains("does not exist"), "message was: {err}");
    assert_eq!(
        conn.ok("select id from kept").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(
        server.app().components.next_txn_id.load(Ordering::Acquire),
        next_txn_id
    );
    assert_eq!(
        server
            .app()
            .components
            .catalog
            .snapshot()
            .unwrap()
            .next_storage_id,
        next_storage_id
    );
    assert_eq!(server.active_txn_count(), 0);
}

#[tokio::test]
async fn truncate_duplicate_targets_fail_before_commit_and_leave_server_usable() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table kept (id integer primary key)").await;
    conn.ok("insert into kept values (1)").await;

    let next_txn_id = server.app().components.next_txn_id.load(Ordering::Acquire);
    let next_storage_id = server
        .app()
        .components
        .catalog
        .snapshot()
        .unwrap()
        .next_storage_id;

    let outcome = conn.query("truncate kept, kept").await.unwrap();
    assert!(
        outcome.result.is_err(),
        "duplicate target should be rejected"
    );
    assert_eq!(
        conn.ok("select id from kept").await.rows(),
        vec![vec![Some("1".to_string())]],
        "the failed statement must not truncate or kill the connection"
    );
    assert_eq!(
        server.app().components.next_txn_id.load(Ordering::Acquire),
        next_txn_id
    );
    assert_eq!(
        server
            .app()
            .components
            .catalog
            .snapshot()
            .unwrap()
            .next_storage_id,
        next_storage_id
    );
    assert_eq!(server.active_txn_count(), 0);
}

#[tokio::test]
async fn truncate_wrong_object_target_fails_before_allocating_ids() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table kept (id integer primary key)").await;
    conn.ok("insert into kept values (1)").await;
    conn.ok("create view not_a_table as select id from kept")
        .await;

    let next_txn_id = server.app().components.next_txn_id.load(Ordering::Acquire);
    let next_storage_id = server
        .app()
        .components
        .catalog
        .snapshot()
        .unwrap()
        .next_storage_id;
    let outcome = conn
        .query("truncate table kept, not_a_table")
        .await
        .unwrap();
    let err = outcome
        .result
        .err()
        .expect("view target should reject TRUNCATE");
    assert!(err.message.contains("42809"), "message was: {err}");
    assert_eq!(
        conn.ok("select id from kept").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(
        server.app().components.next_txn_id.load(Ordering::Acquire),
        next_txn_id
    );
    assert_eq!(
        server
            .app()
            .components
            .catalog
            .snapshot()
            .unwrap()
            .next_storage_id,
        next_storage_id
    );
}

#[tokio::test]
async fn truncate_unknown_table_errors_without_opening_transaction() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let outcome = conn.query("truncate table ghosts").await.unwrap();
    let err = outcome.result.err().expect("missing table should error");
    assert!(
        err.message.to_lowercase().contains("does not exist"),
        "message was: {}",
        err.message
    );
    assert_eq!(outcome.status, b'I');
}

#[tokio::test]
async fn truncate_inside_transaction_commits_and_rolls_back_generation() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    conn.ok("insert into users values (1)").await;
    conn.ok("begin").await;
    conn.ok("truncate table users").await.rows();
    assert!(conn.ok("select id from users").await.rows().is_empty());
    conn.ok("insert into users values (2)").await.rows();
    conn.ok("rollback").await;
    assert_eq!(
        conn.ok("select id from users").await.rows(),
        vec![vec![Some("1".to_string())]],
    );

    conn.ok("begin").await;
    conn.ok("truncate table users").await.rows();
    conn.ok("insert into users values (3)").await.rows();
    conn.ok("commit").await;
    assert_eq!(
        conn.ok("select id from users").await.rows(),
        vec![vec![Some("3".to_string())]],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactional_truncate_blocks_other_sessions_until_commit() {
    let server = TestServer::start().await.unwrap();
    let mut owner = Connection::connect(&server).await.unwrap();
    owner
        .ok("create table locked_truncate (id integer primary key)")
        .await
        .rows();
    owner
        .ok("insert into locked_truncate values (1)")
        .await
        .rows();
    owner.ok("begin").await.rows();
    owner.ok("truncate locked_truncate").await.rows();

    let mut reader = Connection::connect(&server).await.unwrap();
    let read_task =
        tokio::spawn(async move { reader.query("select * from locked_truncate").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !read_task.is_finished(),
        "reader must wait for transactional TRUNCATE"
    );

    owner.ok("commit").await.rows();
    assert!(
        read_task
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .unwrap_rows()
            .is_empty()
    );
}

#[tokio::test]
async fn repeated_transactional_truncate_uses_final_generation_and_rolls_back_original() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table repeated (id integer primary key, body text)")
        .await
        .rows();
    conn.ok("create index repeated_body on repeated (body)")
        .await
        .rows();
    conn.ok("insert into repeated values (1, 'original')")
        .await
        .rows();

    conn.ok("begin").await.rows();
    conn.ok("truncate repeated").await.rows();
    conn.ok("insert into repeated values (2, 'middle')")
        .await
        .rows();
    conn.ok("truncate repeated").await.rows();
    conn.ok("insert into repeated values (3, 'final')")
        .await
        .rows();
    conn.ok("rollback").await.rows();
    assert_eq!(
        conn.ok("select id, body from repeated").await.rows(),
        vec![vec![Some("1".to_string()), Some("original".to_string())]],
    );

    conn.ok("begin").await.rows();
    conn.ok("truncate repeated").await.rows();
    conn.ok("insert into repeated values (4, 'middle2')")
        .await
        .rows();
    conn.ok("truncate repeated").await.rows();
    conn.ok("insert into repeated values (5, 'committed')")
        .await
        .rows();
    conn.ok("commit").await.rows();
    assert_eq!(
        conn.ok("select id from repeated where body = 'committed'")
            .await
            .rows(),
        vec![vec![Some("5".to_string())]],
    );
}

#[tokio::test]
async fn rolled_back_transactional_truncate_removes_replacement_files() {
    let data_dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with_data_dir(data_dir.path())
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok(
        "create table cleanup_probe (id integer primary key, body text) \
             with (toast = aggressive)",
    )
    .await;
    conn.ok("create index cleanup_probe_body_idx on cleanup_probe (body)")
        .await;
    conn.ok("insert into cleanup_probe values (1, 'before')")
        .await;
    let baseline = count_files(data_dir.path());

    for _ in 0..5 {
        conn.ok("begin").await;
        conn.ok("truncate cleanup_probe").await;
        conn.ok("rollback").await;
    }

    assert_eq!(count_files(data_dir.path()), baseline);
    assert_eq!(
        conn.ok("select body from cleanup_probe where id = 1")
            .await
            .rows(),
        vec![vec![Some("before".to_string())]]
    );
}

#[tokio::test]
async fn transactional_truncate_overlay_sees_unrelated_live_catalog_changes() {
    let server = TestServer::start().await.unwrap();
    let mut owner = Connection::connect(&server).await.unwrap();
    let mut ddl = Connection::connect(&server).await.unwrap();

    owner.ok("create table truncate_target (id integer)").await;
    owner.ok("begin").await;
    owner.ok("truncate truncate_target").await;

    ddl.ok("create table created_later (id integer)").await;
    ddl.ok("insert into created_later values (7)").await;

    assert_eq!(
        owner.ok("select id from created_later").await.rows(),
        vec![vec![Some("7".to_string())]]
    );
    owner.ok("rollback").await;
}

#[tokio::test]
async fn copy_from_after_transactional_truncate_uses_replacement_generation() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table truncate_copy (id integer primary key, body text)")
        .await
        .rows();
    conn.ok("insert into truncate_copy values (1, 'old')")
        .await
        .rows();

    conn.ok("begin").await.rows();
    conn.ok("truncate truncate_copy").await.rows();
    let completion = conn
        .copy_from("copy truncate_copy from stdin", &[b"2\tnew\n"])
        .await
        .unwrap();
    assert_eq!(completion.command_tag.as_deref(), Some("COPY 1"));
    assert_eq!(
        conn.ok("select id, body from truncate_copy").await.rows(),
        vec![vec![Some("2".to_string()), Some("new".to_string())]],
    );
    conn.ok("commit").await.rows();
    assert_eq!(
        conn.ok("select id from truncate_copy").await.rows(),
        vec![vec![Some("2".to_string())]],
    );
}

#[tokio::test]
async fn repeatable_read_owner_sees_transactional_truncate_replacement() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table truncate_rr (id integer primary key)")
        .await
        .rows();
    conn.ok("insert into truncate_rr values (1)").await.rows();
    conn.ok("begin isolation level repeatable read")
        .await
        .rows();
    assert_eq!(
        conn.ok("select id from truncate_rr").await.rows(),
        vec![vec![Some("1".to_string())]],
    );
    conn.ok("truncate truncate_rr").await.rows();
    assert!(
        conn.ok("select id from truncate_rr")
            .await
            .rows()
            .is_empty()
    );
    conn.ok("rollback").await.rows();
}

#[tokio::test]
async fn rollback_to_savepoint_discards_transactional_truncate() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table savepoint_truncate (id integer primary key)")
        .await
        .rows();
    conn.ok("insert into savepoint_truncate values (1)")
        .await
        .rows();
    conn.ok("begin").await.rows();
    conn.ok("savepoint s").await.rows();
    conn.ok("truncate savepoint_truncate").await.rows();
    assert!(
        conn.ok("select id from savepoint_truncate")
            .await
            .rows()
            .is_empty()
    );
    conn.ok("rollback to savepoint s").await.rows();
    assert_eq!(
        conn.ok("select id from savepoint_truncate").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    conn.ok("rollback").await.rows();
}

#[tokio::test]
async fn transactional_truncate_rejects_overlapping_parked_queries_only() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table truncate_parked (id integer primary key)")
        .await
        .rows();
    conn.ok("create table truncate_unrelated (id integer primary key)")
        .await
        .rows();
    conn.ok("insert into truncate_parked values (1), (2)")
        .await
        .rows();

    conn.ok("begin").await.rows();
    conn.ok("declare c cursor for select * from truncate_parked")
        .await
        .rows();
    let outcome = conn.query("truncate truncate_parked").await.unwrap();
    assert_eq!(
        outcome.result.err().expect("TRUNCATE must fail").code,
        common::SqlState::ObjectInUse
    );
    conn.ok("rollback").await.rows();

    conn.ok("begin").await.rows();
    conn.ok("declare c cursor for select * from truncate_unrelated")
        .await
        .rows();
    conn.ok("truncate truncate_parked").await.rows();
    conn.ok("rollback").await.rows();

    conn.ok("begin").await.rows();
    conn.begin_suspended_execute("select * from truncate_parked order by id", 1)
        .await
        .unwrap();
    let outcome = conn.query("truncate truncate_parked").await.unwrap();
    assert_eq!(
        outcome.result.err().expect("TRUNCATE must fail").code,
        common::SqlState::ObjectInUse
    );
    conn.ok("rollback").await.rows();

    conn.ok("begin").await.rows();
    conn.ok("declare c cursor for select * from truncate_parked")
        .await
        .rows();
    let failed = conn
        .query("select * from missing_parked_table")
        .await
        .unwrap();
    assert!(failed.result.is_err());
    let outcome = conn.query("truncate truncate_parked").await.unwrap();
    assert_eq!(
        outcome.result.err().expect("failed block must reject").code,
        common::SqlState::InFailedSqlTransaction,
    );
    conn.ok("rollback").await.rows();
}

#[tokio::test]
async fn transactional_truncate_rejects_pipelined_autocommit_suspended_portal() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table pipeline_parked (id integer primary key)")
        .await
        .rows();
    conn.ok("insert into pipeline_parked values (1), (2)")
        .await
        .rows();
    conn.begin_named_suspended_execute(
        "parked_select",
        "parked_portal",
        "select * from pipeline_parked order by id",
        1,
    )
    .await
    .unwrap();

    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        conn.pipelined_begin_then_execute("truncate pipeline_parked"),
    )
    .await
    .expect("TRUNCATE must not self-block on the connection's parked portal")
    .unwrap();
    assert_eq!(
        outcome.result.err().expect("TRUNCATE must fail").code,
        common::SqlState::ObjectInUse,
    );
    assert_eq!(outcome.status, b'E');
    conn.ok("rollback").await.rows();
}

#[tokio::test]
async fn prepared_selects_revalidate_against_transactional_truncate_overlay() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table prepared_generation (id integer primary key)")
        .await
        .rows();
    conn.prepare("read_generation", "select * from prepared_generation")
        .await
        .unwrap();

    conn.ok("begin").await.rows();
    conn.ok("truncate prepared_generation").await.rows();
    let normal = conn.execute_prepared("read_generation").await.unwrap();
    let err = normal.result.err().expect("cached generation must fail");
    assert_eq!(err.code, common::SqlState::FeatureNotSupported);
    assert!(err.message.contains("cached plan must be reprepared"));
    conn.ok("rollback").await.rows();

    conn.ok("begin").await.rows();
    conn.ok("truncate prepared_generation").await.rows();
    let limited = conn
        .execute_prepared_limited("read_generation", 1)
        .await
        .unwrap();
    let err = limited.result.err().expect("cached generation must fail");
    assert_eq!(err.code, common::SqlState::FeatureNotSupported);
    assert!(err.message.contains("cached plan must be reprepared"));
    conn.ok("rollback").await.rows();

    conn.ok("begin").await.rows();
    conn.ok("truncate prepared_generation").await.rows();
    conn.prepare(
        "read_replacement_generation",
        "select * from prepared_generation",
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        conn.execute_prepared("read_replacement_generation")
            .await
            .unwrap()
            .result
            .unwrap()
            .unwrap_rows()
            .is_empty()
    );
    conn.ok("rollback").await.rows();
}

#[tokio::test]
async fn truncate_rebuilds_secondary_indexes_and_toast_generation() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    let body = "large truncate toast value ".repeat(300);

    conn.ok(
        "create table docs (id integer primary key, name text, body text) \
         with (toast_min_value_size = 128, toast_compression = none)",
    )
    .await;
    conn.ok("create index docs_name on docs (name)").await;
    conn.ok(&format!(
        "insert into docs (id, name, body) values (1, 'Ada', '{body}')"
    ))
    .await;
    assert_eq!(
        conn.ok("select id from docs where name = 'Ada'")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]]
    );

    conn.ok("truncate docs").await;
    assert!(
        conn.ok("select id from docs where name = 'Ada'")
            .await
            .rows()
            .is_empty(),
        "secondary index scan should see the empty replacement generation"
    );

    conn.ok(&format!(
        "insert into docs (id, name, body) values (1, 'Ada', '{body}')"
    ))
    .await;
    assert_eq!(
        conn.ok("select body from docs where name = 'Ada'")
            .await
            .rows(),
        vec![vec![Some(body)]],
        "secondary index and TOAST reads should work after truncate"
    );
}

#[tokio::test]
async fn prepared_truncate_runs_over_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    conn.ok("insert into users (id) values (1)").await;
    conn.prepare("trunc_users", "truncate table users")
        .await
        .unwrap()
        .unwrap();
    let outcome = conn.execute_prepared("trunc_users").await.unwrap();
    assert!(outcome.result.is_ok());
    assert_eq!(outcome.status, b'I');
    assert!(conn.ok("select id from users").await.rows().is_empty());

    conn.ok("insert into users values (2)").await.rows();
    conn.ok("begin").await.rows();
    let outcome = conn.execute_prepared("trunc_users").await.unwrap();
    assert!(outcome.result.is_ok());
    assert_eq!(outcome.status, b'T');
    assert!(conn.ok("select id from users").await.rows().is_empty());
    conn.ok("rollback").await.rows();
    assert_eq!(
        conn.ok("select id from users").await.rows(),
        vec![vec![Some("2".to_string())]],
    );
}
