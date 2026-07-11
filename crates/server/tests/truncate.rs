mod support;

use std::sync::atomic::Ordering;

use support::{Connection, TestServer, command_tags};

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
async fn truncate_inside_transaction_block_is_rejected() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table users (id integer primary key)").await;
    conn.ok("begin").await;
    let outcome = conn.query("truncate table users").await.unwrap();
    let err = outcome
        .result
        .err()
        .expect("TRUNCATE in a transaction block is rejected");
    assert!(
        err.message.to_lowercase().contains("transaction block"),
        "message was: {}",
        err.message
    );
    assert_eq!(
        outcome.status, b'E',
        "TRUNCATE poisons the open block to 'E'"
    );
    conn.ok("rollback").await;
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
}
