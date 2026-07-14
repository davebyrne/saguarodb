mod support;

use std::time::Duration;

use support::{Connection, TestServer};

#[tokio::test]
async fn alter_add_and_drop_foreign_key_enforces_existing_and_future_rows() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table child (id integer primary key, parent_id integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into parent values (1)")
        .await
        .unwrap();
    server
        .simple_query("insert into child values (1, 1)")
        .await
        .unwrap();
    server
        .simple_query(
            "alter table child add constraint child_parent foreign key (parent_id) references parent",
        )
        .await
        .unwrap();

    let err = server
        .simple_query("insert into child values (2, 99)")
        .await
        .err()
        .expect("new foreign key must be enforced");
    assert!(err.message.contains("23503"), "{}", err.message);

    server
        .simple_query("alter table child drop constraint child_parent restrict")
        .await
        .unwrap();
    server
        .simple_query("insert into child values (2, 99)")
        .await
        .unwrap();
}

#[tokio::test]
async fn alter_add_rejects_invalid_existing_rows_without_publishing() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table child (id integer primary key, parent_id integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into child values (1, 99)")
        .await
        .unwrap();
    let err = server
        .simple_query("alter table child add foreign key (parent_id) references parent")
        .await
        .err()
        .expect("invalid existing row must reject ALTER");
    assert!(err.message.contains("23503"), "{}", err.message);

    server
        .simple_query("insert into child values (2, 98)")
        .await
        .unwrap();
}

#[tokio::test]
async fn alter_foreign_key_is_rejected_inside_transaction_and_if_exists_noops() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table child (id integer primary key, parent_id integer)")
        .await
        .unwrap();
    server
        .simple_query("alter table child drop constraint if exists missing")
        .await
        .unwrap();

    let mut connection = Connection::connect(&server).await.unwrap();
    connection.query("begin").await.unwrap().unwrap();
    let err = connection
        .query("alter table child add foreign key (parent_id) references parent")
        .await
        .unwrap()
        .result
        .err()
        .expect("FK ALTER in a transaction must fail");
    assert!(err.message.contains("0A000"), "{}", err.message);
    connection.query("rollback").await.unwrap().unwrap();
}

#[tokio::test]
async fn generic_drop_constraint_routes_primary_key_names() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table items (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("alter table items drop constraint items_pkey")
        .await
        .unwrap();
    server
        .simple_query("insert into items values (1), (1)")
        .await
        .unwrap();

    server
        .simple_query(
            "create table unique_items (id integer primary key, code integer, unique (code))",
        )
        .await
        .unwrap();
    for sql in [
        "alter table unique_items drop constraint unique_items_code_key",
        "alter table unique_items drop constraint if exists unique_items_code_key",
    ] {
        let err = server
            .simple_query(sql)
            .await
            .err()
            .expect("an existing unsupported UNIQUE constraint must not be hidden");
        assert!(err.message.contains("0A000"), "{}", err.message);
    }
}

#[tokio::test]
async fn alter_foreign_keys_support_composites_generated_names_and_self_reference() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table parent (id integer primary key, tenant integer, code text, unique (tenant, code))",
        )
        .await
        .unwrap();
    server
        .simple_query("create table child (id integer primary key, tenant integer, code text)")
        .await
        .unwrap();
    server
        .simple_query("insert into parent values (1, 7, 'x')")
        .await
        .unwrap();
    server
        .simple_query("insert into child values (1, 7, 'x')")
        .await
        .unwrap();
    server
        .simple_query(
            "alter table child add foreign key (tenant, code) references parent(tenant, code)",
        )
        .await
        .unwrap();
    let err = server
        .simple_query("insert into child values (2, 7, 'missing')")
        .await
        .err()
        .expect("generated composite FK must be enforced");
    assert!(
        err.message.contains("child_tenant_code_fkey"),
        "{}",
        err.message
    );

    server
        .simple_query("create table nodes (id integer primary key, parent_id integer)")
        .await
        .unwrap();
    server
        .simple_query("insert into nodes values (1, 1)")
        .await
        .unwrap();
    server
        .simple_query("alter table nodes add foreign key (parent_id) references nodes")
        .await
        .unwrap();
}

#[tokio::test]
async fn committed_alter_foreign_key_add_and_drop_replay_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table parent (id integer primary key)")
            .await
            .unwrap();
        server
            .simple_query("create table child (id integer primary key, parent_id integer)")
            .await
            .unwrap();
        server
            .simple_query("alter table child add constraint keep_fk foreign key (parent_id) references parent")
            .await
            .unwrap();
        assert_eq!(server.checkpoint_count(), 0);
    }
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        let checkpoints_after_add_recovery = server.checkpoint_count();
        assert_eq!(checkpoints_after_add_recovery, 1);
        let err = server
            .simple_query("insert into child values (1, 99)")
            .await
            .err()
            .expect("recovered ADD must be enforced");
        assert!(err.message.contains("23503"), "{}", err.message);
        server
            .simple_query("alter table child drop constraint keep_fk")
            .await
            .unwrap();
        assert_eq!(server.checkpoint_count(), checkpoints_after_add_recovery);
    }
    let server = TestServer::start_with_data_dir(&path).await.unwrap();
    server
        .simple_query("insert into child values (1, 99)")
        .await
        .unwrap();
}

#[tokio::test]
async fn alter_foreign_key_runs_over_extended_protocol() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table child (id integer primary key, parent_id integer)")
        .await
        .unwrap();
    let mut connection = Connection::connect(&server).await.unwrap();
    connection
        .extended_execute(
            "alter table child add constraint child_parent foreign key (parent_id) references parent",
        )
        .await
        .unwrap();
    connection
        .extended_execute("alter table child drop constraint child_parent")
        .await
        .unwrap();
}

#[tokio::test]
async fn prepared_alter_foreign_key_tracks_child_and_parent_versions() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table child (id integer primary key, parent_id integer)")
        .await
        .unwrap();
    let mut connection = Connection::connect(&server).await.unwrap();
    connection
        .prepare(
            "add_fk",
            "alter table child add constraint child_parent foreign key (parent_id) references parent",
        )
        .await
        .unwrap()
        .result
        .unwrap();
    server
        .simple_query("alter table parent rename column id to parent_key")
        .await
        .unwrap();
    let err = connection
        .execute_prepared("add_fk")
        .await
        .unwrap()
        .result
        .err()
        .expect("changed parent must invalidate prepared ADD");
    assert!(err.message.contains("0A000"), "{}", err.message);

    server
        .simple_query("alter table parent rename column parent_key to id")
        .await
        .unwrap();
    server
        .simple_query(
            "alter table child add constraint child_parent foreign key (parent_id) references parent",
        )
        .await
        .unwrap();
    connection
        .prepare("drop_fk", "alter table child drop constraint child_parent")
        .await
        .unwrap()
        .result
        .unwrap();
    server
        .simple_query("alter table parent rename column id to parent_key")
        .await
        .unwrap();
    let err = connection
        .execute_prepared("drop_fk")
        .await
        .unwrap()
        .result
        .err()
        .expect("changed parent must invalidate prepared DROP");
    assert!(err.message.contains("0A000"), "{}", err.message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_interrupts_alter_foreign_key_lock_wait() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table parent (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table child (id integer primary key, parent_id integer)")
        .await
        .unwrap();

    let mut writer = Connection::connect(&server).await.unwrap();
    writer.query("begin").await.unwrap().result.unwrap();
    writer
        .query("insert into child values (1, 99)")
        .await
        .unwrap()
        .result
        .unwrap();

    let mut alter = Connection::connect(&server).await.unwrap();
    let (pid, secret) = alter.backend_key();
    let task = tokio::spawn(async move {
        alter
            .query("alter table child add foreign key (parent_id) references parent")
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !task.is_finished(),
        "ALTER should be waiting for the child lock"
    );
    server.send_cancel(pid, secret).await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("canceled ALTER wait must finish")
        .unwrap()
        .unwrap();
    let err = outcome.result.err().expect("ALTER must be canceled");
    assert!(err.message.contains("57014"), "{}", err.message);
    writer.query("rollback").await.unwrap().result.unwrap();

    server
        .simple_query("insert into child values (2, 98)")
        .await
        .unwrap();
}
