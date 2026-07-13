mod support;

use std::time::Duration;

use support::{Connection, TestServer};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn locking_select_holds_tuple_lock_until_transaction_end() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t values (1, 10)").await;

    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    assert_eq!(
        holder
            .ok("select v from t where id = 1 for update")
            .await
            .rows(),
        vec![vec![Some("10".to_string())]]
    );

    let mut observer = Connection::connect(&server).await.unwrap();
    let error = observer
        .query("select * from t where id = 1 for update nowait")
        .await
        .unwrap()
        .result
        .err()
        .expect("NOWAIT must report the conflicting tuple lock");
    assert!(
        error.message.contains("C=55P03"),
        "message: {}",
        error.message
    );
    assert!(
        observer
            .ok("select * from t where id = 1 for update skip locked")
            .await
            .rows()
            .is_empty()
    );

    let mut writer = Connection::connect(&server).await.unwrap();
    let writer_task = tokio::spawn(async move {
        let result = writer.query("update t set v = 20 where id = 1").await;
        (result, writer)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !writer_task.is_finished(),
        "writer must wait for FOR UPDATE"
    );

    holder.ok("commit").await;
    let (result, _writer) = writer_task.await.unwrap();
    assert!(result.unwrap().result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_share_is_compatible_with_no_key_update_but_not_update() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t values (1, 10)").await;

    let mut key_reader = Connection::connect(&server).await.unwrap();
    key_reader.ok("begin").await;
    key_reader
        .ok("select * from t where id = 1 for key share")
        .await;

    let mut compatible = Connection::connect(&server).await.unwrap();
    compatible.ok("begin").await;
    compatible
        .ok("select * from t where id = 1 for no key update nowait")
        .await;

    let mut conflicting = Connection::connect(&server).await.unwrap();
    let error = conflicting
        .query("select * from t where id = 1 for update nowait")
        .await
        .unwrap()
        .result
        .err()
        .expect("FOR UPDATE must conflict with KEY SHARE");
    assert!(
        error.message.contains("C=55P03"),
        "message: {}",
        error.message
    );

    compatible.ok("rollback").await;
    key_reader.ok("rollback").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limit_locks_only_rows_returned_after_skipping_locked_candidates() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t values (1, 10), (2, 20)").await;

    let mut first = Connection::connect(&server).await.unwrap();
    first.ok("begin").await;
    assert_eq!(
        first
            .ok("select id from t order by id limit 1 for update")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]]
    );

    let mut second = Connection::connect(&server).await.unwrap();
    second.ok("begin").await;
    assert_eq!(
        second
            .ok("select id from t order by id limit 1 for update skip locked")
            .await
            .rows(),
        vec![vec![Some("2".to_string())]]
    );
    second
        .ok("select * from t where id = 2 for update nowait")
        .await;

    second.ok("rollback").await;
    first.ok("rollback").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_analyze_locking_select_uses_the_locking_lifecycle() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t values (1, 10)").await;

    let mut analyzer = Connection::connect(&server).await.unwrap();
    analyzer.ok("begin").await;
    analyzer
        .ok("explain analyze select * from t where id = 1 for update")
        .await;

    let mut contender = Connection::connect(&server).await.unwrap();
    let error = contender
        .query("select * from t where id = 1 for update nowait")
        .await
        .unwrap()
        .result
        .err()
        .expect("analyzed locking SELECT must retain its tuple lock");
    assert!(
        error.message.contains("C=55P03"),
        "message: {}",
        error.message
    );

    analyzer.ok("rollback").await;
    contender
        .ok("select * from t where id = 1 for update nowait")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeatable_read_locking_select_rejects_a_post_snapshot_successor() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t values (1, 10)").await;

    let mut retained = Connection::connect(&server).await.unwrap();
    retained.ok("begin isolation level repeatable read").await;
    retained.ok("select * from t").await;

    let mut updater = Connection::connect(&server).await.unwrap();
    updater.ok("update t set v = 20 where id = 1").await;

    let error = retained
        .query("select * from t where id = 1 for update")
        .await
        .unwrap()
        .result
        .err()
        .expect("retained snapshot must reject the post-snapshot version");
    assert!(
        error.message.contains("C=40001"),
        "message: {}",
        error.message
    );
    retained.ok("rollback").await;
}
