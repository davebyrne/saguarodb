mod support;

use support::{Connection, QueryOutcome, TestServer};

fn error_message(outcome: &QueryOutcome) -> String {
    match &outcome.result {
        Ok(_) => panic!("expected query error"),
        Err(err) => err.message.clone(),
    }
}

fn has_frame_containing(bytes: &[u8], tag: u8, needle: &[u8]) -> bool {
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let frame_tag = bytes[offset];
        let len = i32::from_be_bytes(bytes[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let end = offset + 1 + len;
        if len < 4 || end > bytes.len() {
            return false;
        }
        let body = &bytes[offset + 5..end];
        if frame_tag == tag && body.windows(needle.len()).any(|window| window == needle) {
            return true;
        }
        offset = end;
    }
    false
}

fn frame_count_containing(bytes: &[u8], tag: u8, needle: &[u8]) -> usize {
    let mut offset = 0;
    let mut count = 0;
    while offset + 5 <= bytes.len() {
        let frame_tag = bytes[offset];
        let len = i32::from_be_bytes(bytes[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let end = offset + 1 + len;
        if len < 4 || end > bytes.len() {
            return count;
        }
        let body = &bytes[offset + 5..end];
        if frame_tag == tag && body.windows(needle.len()).any(|window| window == needle) {
            count += 1;
        }
        offset = end;
    }
    count
}

#[tokio::test]
async fn set_show_reset_and_accept_all_gucs_are_session_local() {
    let server = TestServer::start().await.unwrap();
    let mut conn_a = Connection::connect(&server).await.unwrap();
    let mut conn_b = Connection::connect(&server).await.unwrap();

    assert_eq!(
        conn_a.ok("SHOW extra_float_digits").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    conn_a.ok("SET extra_float_digits = 3").await.rows();
    assert_eq!(
        conn_a.ok("SHOW extra_float_digits").await.rows(),
        vec![vec![Some("3".to_string())]]
    );
    assert_eq!(
        conn_b.ok("SHOW extra_float_digits").await.rows(),
        vec![vec![Some("1".to_string())]]
    );

    conn_a.ok("SET datestyle TO 'ISO'").await.rows();
    assert_eq!(
        conn_a.ok("SHOW datestyle").await.rows(),
        vec![vec![Some("ISO".to_string())]]
    );

    conn_a.ok("SET my_app.batch_size TO '250'").await.rows();
    assert_eq!(
        conn_a.ok("SHOW my_app.batch_size").await.rows(),
        vec![vec![Some("250".to_string())]]
    );

    conn_a.ok("RESET extra_float_digits").await.rows();
    assert_eq!(
        conn_a.ok("SHOW extra_float_digits").await.rows(),
        vec![vec![Some("1".to_string())]]
    );

    let message = error_message(&conn_a.ok("SHOW no_such_parameter").await);
    assert!(message.contains("C=42704"), "got {message}");
}

#[tokio::test]
async fn changing_application_name_sends_parameter_status() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let response = conn
        .query_raw("SET application_name = 'jdbc-app'")
        .await
        .unwrap();
    assert!(
        has_frame_containing(&response, b'S', b"application_name\0jdbc-app\0"),
        "expected a ParameterStatus frame for application_name"
    );

    let response = conn
        .query_raw("SET application_name = 'jdbc-app'")
        .await
        .unwrap();
    assert!(!has_frame_containing(
        &response,
        b'S',
        b"application_name\0"
    ));

    let response = conn.query_raw("DISCARD ALL").await.unwrap();
    assert!(has_frame_containing(
        &response,
        b'S',
        b"application_name\0\0"
    ));
}

#[tokio::test]
async fn changing_application_name_over_extended_protocol_sends_parameter_status() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let response = conn
        .extended_execute_raw("SET application_name = 'ext-app'")
        .await
        .unwrap();
    assert!(
        has_frame_containing(&response, b'S', b"application_name\0ext-app\0"),
        "expected extended Sync to report application_name"
    );

    let response = conn
        .extended_execute_raw("SET application_name = 'ext-app'")
        .await
        .unwrap();
    assert!(!has_frame_containing(
        &response,
        b'S',
        b"application_name\0"
    ));
}

#[tokio::test]
async fn extended_application_name_reports_each_execute_before_sync() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let mut bytes = Connection::extended_parse("SET application_name = 'first'");
    bytes.extend(Connection::extended_bind());
    bytes.extend(Connection::extended_execute_portal());
    bytes.extend(Connection::extended_parse(
        "SET application_name = 'second'",
    ));
    bytes.extend(Connection::extended_bind());
    bytes.extend(Connection::extended_execute_portal());
    bytes.extend(Connection::extended_sync());

    let response = conn.extended_raw(bytes).await.unwrap();
    assert_eq!(
        frame_count_containing(&response, b'S', b"application_name\0"),
        2,
        "each successful Execute that changes application_name should report"
    );
    assert!(has_frame_containing(
        &response,
        b'S',
        b"application_name\0first\0"
    ));
    assert!(has_frame_containing(
        &response,
        b'S',
        b"application_name\0second\0"
    ));
}

#[tokio::test]
async fn transaction_isolation_guc_matches_set_transaction_rules() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;
    setup.ok("insert into users (id) values (1)").await;

    let mut conn = Connection::connect(&server).await.unwrap();
    assert_eq!(
        conn.ok("SHOW transaction_isolation").await.rows(),
        vec![vec![Some("read committed".to_string())]]
    );

    let outside = conn
        .ok("SET transaction_isolation TO 'repeatable read'")
        .await;
    assert!(outside.result.is_ok());
    assert_eq!(outside.status, b'I');
    assert_eq!(
        conn.ok("SHOW transaction_isolation").await.rows(),
        vec![vec![Some("read committed".to_string())]],
        "outside a transaction, SET transaction_isolation is a SET TRANSACTION no-op"
    );

    conn.ok("BEGIN").await.rows();
    let set = conn.ok("SET transaction_isolation TO SERIALIZABLE").await;
    assert!(set.result.is_ok());
    assert_eq!(set.status, b'T');
    assert_eq!(
        conn.ok("SHOW transaction_isolation").await.rows(),
        vec![vec![Some("serializable".to_string())]]
    );
    conn.ok("ROLLBACK").await.rows();

    conn.ok("BEGIN").await.rows();
    conn.ok("SELECT id FROM users").await.rows();
    let late = conn
        .ok("SET transaction_isolation TO 'repeatable read'")
        .await;
    assert!(error_message(&late).contains("0A000"));
    assert_eq!(late.status, b'E');
    conn.ok("ROLLBACK").await.rows();
}

#[tokio::test]
async fn default_transaction_isolation_guc_controls_future_transactions() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table users (id integer primary key)")
        .await;
    setup.ok("insert into users (id) values (1)").await;

    let mut conn = Connection::connect(&server).await.unwrap();
    let mut writer = Connection::connect(&server).await.unwrap();

    conn.ok("SET default_transaction_isolation TO 'repeatable read'")
        .await
        .rows();
    assert_eq!(
        conn.ok("SHOW default_transaction_isolation").await.rows(),
        vec![vec![Some("repeatable read".to_string())]]
    );
    assert_eq!(
        conn.ok("SHOW transaction_isolation").await.rows(),
        vec![vec![Some("repeatable read".to_string())]],
        "outside a block transaction_isolation reflects the session default"
    );

    conn.ok("BEGIN").await.rows();
    let first = conn.ok("SELECT id FROM users ORDER BY id").await.rows();
    writer.ok("insert into users (id) values (2)").await.rows();
    assert_eq!(
        conn.ok("SELECT id FROM users ORDER BY id").await.rows(),
        first,
        "a BEGIN without explicit level inherited repeatable read"
    );
    conn.ok("COMMIT").await.rows();

    conn.ok("RESET default_transaction_isolation").await.rows();
    assert_eq!(
        conn.ok("SHOW default_transaction_isolation").await.rows(),
        vec![vec![Some("read committed".to_string())]]
    );
}

#[tokio::test]
async fn session_config_works_over_the_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    let out = conn
        .extended_execute("SET application_name = 'ext-test'")
        .await
        .unwrap();
    assert!(out.result.is_ok());
    assert_eq!(out.status, b'I');
    assert_eq!(
        conn.extended_execute("SHOW application_name")
            .await
            .unwrap()
            .rows(),
        vec![vec![Some("ext-test".to_string())]]
    );
    assert_eq!(
        conn.ok("SHOW application_name").await.rows(),
        vec![vec![Some("ext-test".to_string())]],
        "extended SET uses the same per-session GUC store as simple SHOW"
    );

    conn.extended_execute("SET default_transaction_isolation TO 'repeatable read'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        conn.extended_execute("SHOW default_transaction_isolation")
            .await
            .unwrap()
            .rows(),
        vec![vec![Some("repeatable read".to_string())]]
    );
    assert_eq!(
        conn.ok("SHOW transaction_isolation").await.rows(),
        vec![vec![Some("repeatable read".to_string())]],
        "outside a transaction, transaction_isolation reflects the session default"
    );

    let outside = conn
        .extended_execute("SET transaction_isolation TO SERIALIZABLE")
        .await
        .unwrap();
    assert!(outside.result.is_ok());
    assert_eq!(outside.status, b'I');
    assert_eq!(
        conn.ok("SHOW transaction_isolation").await.rows(),
        vec![vec![Some("repeatable read".to_string())]],
        "outside a transaction, SET transaction_isolation is a SET TRANSACTION no-op"
    );

    conn.ok("BEGIN").await.rows();
    let in_txn = conn
        .extended_execute("SET transaction_isolation TO SERIALIZABLE")
        .await
        .unwrap();
    assert!(in_txn.result.is_ok());
    assert_eq!(in_txn.status, b'T');
    assert_eq!(
        conn.ok("SHOW transaction_isolation").await.rows(),
        vec![vec![Some("serializable".to_string())]]
    );
    conn.ok("ROLLBACK").await.rows();
}

#[tokio::test]
async fn extended_discard_all_deallocates_prepared_statements() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.prepare("stale", "select 1").await.unwrap().unwrap();
    assert_eq!(
        conn.execute_prepared("stale").await.unwrap().rows(),
        vec![vec![Some("1".to_string())]]
    );

    let out = conn.extended_execute("DISCARD ALL").await.unwrap();
    assert!(out.result.is_ok());
    assert_eq!(out.status, b'I');

    let stale = conn.execute_prepared("stale").await.unwrap();
    assert!(error_message(&stale).contains("prepared statement \"stale\" does not exist"));
}

#[tokio::test]
async fn discard_all_resets_gucs_default_isolation_and_sequence_currval() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create sequence user_ids").await.rows();
    conn.ok("create table users (id integer primary key)").await;
    conn.ok("insert into users (id) values (1)").await;

    conn.ok("SET extra_float_digits = 3").await.rows();
    conn.ok("SET default_transaction_isolation TO SERIALIZABLE")
        .await
        .rows();
    conn.prepare("stale", "select 1").await.unwrap().unwrap();
    assert_eq!(
        conn.execute_prepared("stale").await.unwrap().rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(
        conn.ok("select nextval('user_ids') from users")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(
        conn.ok("select currval('user_ids') from users")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]]
    );
    conn.ok("RESET ALL").await.rows();
    assert_eq!(
        conn.ok("select currval('user_ids') from users")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]],
        "RESET ALL resets configuration, not sequence currval memory"
    );

    let out = conn.ok("DISCARD ALL").await;
    assert!(out.result.is_ok());
    assert_eq!(out.status, b'I');
    assert_eq!(
        conn.ok("SHOW extra_float_digits").await.rows(),
        vec![vec![Some("1".to_string())]]
    );
    assert_eq!(
        conn.ok("SHOW default_transaction_isolation").await.rows(),
        vec![vec![Some("read committed".to_string())]]
    );
    assert!(
        conn.ok("select currval('user_ids') from users")
            .await
            .result
            .is_err()
    );
    let stale = conn.execute_prepared("stale").await.unwrap();
    assert!(error_message(&stale).contains("prepared statement \"stale\" does not exist"));
}

#[tokio::test]
async fn session_config_respects_failed_blocks_and_discard_poisoning() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("BEGIN").await.rows();
    let set = conn.ok("SET extra_float_digits = 2").await;
    assert!(set.result.is_ok());
    assert_eq!(set.status, b'T');

    let _ = conn.ok("select no_such_column").await;
    let show = conn.ok("SHOW extra_float_digits").await;
    assert!(error_message(&show).contains("25P02"));
    assert_eq!(show.status, b'E');
    conn.ok("ROLLBACK").await.rows();

    conn.ok("BEGIN").await.rows();
    let discard = conn.ok("DISCARD ALL").await;
    assert!(error_message(&discard).contains("0A000"));
    assert_eq!(discard.status, b'E');
    conn.ok("ROLLBACK").await.rows();
}
