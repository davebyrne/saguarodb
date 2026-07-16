mod support;

use std::time::Duration;

use support::{Connection, TestServer};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_copy_from_retains_target_lock_until_protocol_completion() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table copy_held (id integer primary key)")
        .await
        .rows();

    let mut copy = Connection::connect(&server).await.unwrap();
    copy.begin_copy_from("copy copy_held from stdin")
        .await
        .unwrap();
    let mut ddl = Connection::connect(&server).await.unwrap();
    let drop_task = tokio::spawn(async move { ddl.query("drop table copy_held").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !drop_task.is_finished(),
        "DROP TABLE must wait through the open COPY protocol lifetime"
    );

    copy.finish_copy_from(&[b"1\n"]).await.unwrap();
    drop_task.await.unwrap().unwrap().result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_portal_retains_target_lock_until_resume() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table portal_held (id integer primary key)")
        .await
        .rows();
    setup
        .ok("insert into portal_held (id) values (1), (2), (3)")
        .await
        .rows();

    let mut reader = Connection::connect(&server).await.unwrap();
    reader
        .begin_suspended_execute("select id from portal_held order by id", 1)
        .await
        .unwrap();
    let mut ddl = Connection::connect(&server).await.unwrap();
    let drop_task = tokio::spawn(async move { ddl.query("drop table portal_held").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !drop_task.is_finished(),
        "DROP TABLE must wait while the extended portal is suspended"
    );

    reader.finish_suspended_execute().await.unwrap();
    drop_task.await.unwrap().unwrap().result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relation_ddl_waits_for_its_target_while_unrelated_ddl_proceeds() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup.ok("create table held (id integer primary key)").await;
    setup
        .ok("create table unrelated (id integer primary key)")
        .await;

    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("select * from held").await;

    let mut dropper = Connection::connect(&server).await.unwrap();
    let drop_task = tokio::spawn(async move { dropper.query("drop table held").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !drop_task.is_finished(),
        "DROP TABLE must wait for the target's retained AccessShare lock"
    );

    let mut unrelated = Connection::connect(&server).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        unrelated.query("alter table unrelated rename column id to key"),
    )
    .await
    .expect("unrelated DDL must not wait behind target-scoped DROP")
    .unwrap()
    .result
    .unwrap();

    holder.ok("commit").await;
    drop_task.await.unwrap().unwrap().result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_waits_for_target_writer_but_allows_readers() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table items (id integer primary key, value integer)")
        .await;

    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("begin").await;
    writer
        .ok("insert into items (id, value) values (1, 10)")
        .await;

    let mut indexer = Connection::connect(&server).await.unwrap();
    let index_task = tokio::spawn(async move {
        indexer
            .query("create index items_value_idx on items (value)")
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !index_task.is_finished(),
        "CREATE INDEX Share must wait for a target RowExclusive holder"
    );

    let mut reader = Connection::connect(&server).await.unwrap();
    let rows = tokio::time::timeout(
        Duration::from_secs(2),
        reader.query("select count(*) from items"),
    )
    .await
    .expect("AccessShare reader should remain compatible with queued Share")
    .unwrap()
    .result
    .unwrap()
    .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("0".to_string())]]);

    writer.ok("commit").await;
    index_task.await.unwrap().unwrap().result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequence_drop_waits_for_transaction_owned_sequence_access() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup.ok("create sequence ids").await;

    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("select nextval('ids')").await;

    let mut dropper = Connection::connect(&server).await.unwrap();
    let drop_task = tokio::spawn(async move { dropper.query("drop sequence ids").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !drop_task.is_finished(),
        "DROP SEQUENCE must wait for transaction-owned SequenceAccess"
    );

    holder.ok("commit").await;
    drop_task.await.unwrap().unwrap().result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_serial_column_waits_for_transaction_owned_sequence_access() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table serial_holder (kept integer, doomed serial)")
        .await;

    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder
        .ok("select nextval('serial_holder_doomed_seq')")
        .await;

    let mut dropper = Connection::connect(&server).await.unwrap();
    let drop_task = tokio::spawn(async move {
        dropper
            .query("alter table serial_holder drop column doomed")
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !drop_task.is_finished(),
        "DROP COLUMN must wait for transaction-owned SequenceAccess"
    );

    holder.ok("commit").await;
    drop_task.await.unwrap().unwrap().result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vacuum_waits_for_target_writer_while_unrelated_vacuum_proceeds() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup.ok("create table held (id integer primary key)").await;
    setup
        .ok("create table unrelated (id integer primary key)")
        .await;

    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("begin").await;
    writer.ok("insert into held (id) values (1)").await;

    let mut vacuum = Connection::connect(&server).await.unwrap();
    let vacuum_task = tokio::spawn(async move { vacuum.query("vacuum held").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !vacuum_task.is_finished(),
        "VACUUM Share must wait for a target RowExclusive holder"
    );

    let mut unrelated = Connection::connect(&server).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), unrelated.query("vacuum unrelated"))
        .await
        .expect("unrelated VACUUM must not wait behind the target writer")
        .unwrap()
        .result
        .unwrap();

    writer.ok("commit").await;
    vacuum_task.await.unwrap().unwrap().result.unwrap();
}

#[tokio::test]
async fn maintenance_alter_validation_error_releases_xid_and_guards() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table keyed (id integer primary key)").await;

    let err = conn
        .ok("alter table keyed add primary key (id)")
        .await
        .result
        .err()
        .expect("adding a second primary key must fail");
    assert!(err.message.contains("C=55000"), "message: {}", err.message);
    assert_eq!(
        server.active_txn_count(),
        0,
        "failed maintenance ALTER must settle its preallocated xid"
    );
    tokio::time::timeout(Duration::from_secs(2), server.force_checkpoint())
        .await
        .expect("failed maintenance ALTER must not leak a checkpoint participant")
        .unwrap();
}

#[tokio::test]
async fn schema_rewrite_default_catalog_lookup_does_not_reenter_publication_gate() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table referenced (id integer primary key)")
        .await;
    conn.ok("create table rows (id integer primary key)").await;
    conn.ok("insert into rows (id) values (1)").await;

    tokio::time::timeout(
        Duration::from_secs(2),
        conn.query(
            "alter table rows add column target_oid bigint not null \
             default to_regclass('referenced')",
        ),
    )
    .await
    .expect("catalog introspection during rewrite must not self-deadlock")
    .unwrap()
    .result
    .unwrap();
    assert_eq!(
        conn.ok("select target_oid is not null from rows")
            .await
            .rows(),
        vec![vec![Some("t".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_interrupts_maintenance_relation_lock_wait() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup.ok("create table held (id integer primary key)").await;

    let mut writer = Connection::connect(&server).await.unwrap();
    writer.ok("begin").await;
    writer.ok("insert into held (id) values (1)").await;

    let mut vacuum = Connection::connect(&server).await.unwrap();
    let (pid, secret) = vacuum.backend_key();
    let vacuum_task = tokio::spawn(async move { vacuum.query("vacuum held").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!vacuum_task.is_finished(), "VACUUM should be waiting");

    server.send_cancel(pid, secret).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), vacuum_task)
        .await
        .expect("canceled maintenance wait must finish")
        .unwrap()
        .unwrap();
    let err = result.result.err().expect("VACUUM wait should be canceled");
    assert!(err.message.contains("C=57014"), "message: {}", err.message);
    writer.ok("rollback").await;
}
