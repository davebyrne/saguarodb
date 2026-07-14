mod support;

use support::{Connection, TestServer};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_table_rejects_external_fk_and_allows_self_and_complete_sets() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table drop_parent_fk (id integer primary key)")
        .await;
    conn.ok("create table drop_child_fk (id integer primary key, parent_id integer)")
        .await;
    server
        .attach_foreign_key(
            "drop_child_parent_fkey",
            "drop_child_fk",
            &["parent_id"],
            "drop_parent_fk",
            &["id"],
        )
        .unwrap();

    let error = conn
        .query("drop table drop_parent_fk")
        .await
        .unwrap()
        .result
        .err()
        .expect("an external child must block DROP TABLE");
    assert!(error.message.contains("C=2BP01"), "{error}");
    conn.ok("drop table drop_parent_fk, drop_child_fk").await;

    conn.ok("create table drop_self_fk (id integer primary key, parent_id integer)")
        .await;
    server
        .attach_foreign_key(
            "drop_self_parent_fkey",
            "drop_self_fk",
            &["parent_id"],
            "drop_self_fk",
            &["id"],
        )
        .unwrap();
    conn.ok("drop table drop_self_fk").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cyclic_batch_drop_replays_in_wal_record_order() {
    let data_dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with_data_dir(data_dir.path())
        .await
        .unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table cycle_a_fk (id integer primary key)")
        .await;
    conn.ok("create table cycle_b_fk (id integer primary key)")
        .await;
    server
        .attach_foreign_key(
            "cycle_a_b_fkey",
            "cycle_a_fk",
            &["id"],
            "cycle_b_fk",
            &["id"],
        )
        .unwrap();
    server
        .attach_foreign_key(
            "cycle_b_a_fkey",
            "cycle_b_fk",
            &["id"],
            "cycle_a_fk",
            &["id"],
        )
        .unwrap();
    server.force_checkpoint().await.unwrap();
    conn.ok("drop table cycle_a_fk, cycle_b_fk").await;
    conn.close().await;
    drop(server);

    let restarted = TestServer::start_with_data_dir(data_dir.path())
        .await
        .unwrap();
    assert!(
        restarted
            .app()
            .components
            .catalog
            .get_table_by_name("cycle_a_fk")
            .unwrap()
            .is_none()
    );
    assert!(
        restarted
            .app()
            .components
            .catalog
            .get_table_by_name("cycle_b_fk")
            .unwrap()
            .is_none()
    );
}
