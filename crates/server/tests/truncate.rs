mod support;

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
