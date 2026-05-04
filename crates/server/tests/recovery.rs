mod support;

use support::{TestServer, write_uncommitted_insert_record_for_test};

#[tokio::test]
async fn committed_data_survives_restart_with_snapshot_and_wal() {
    let dir = tempfile::tempdir().unwrap();

    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table users (id integer primary key, name text)")
            .await
            .unwrap();
        server
            .simple_query("insert into users (id, name) values (1, 'Ada')")
            .await
            .unwrap();
        server.force_checkpoint().await.unwrap();
        server
            .simple_query("insert into users (id, name) values (2, 'Grace')")
            .await
            .unwrap();
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id, name from users order by id")
        .await
        .unwrap()
        .unwrap_rows();

    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("Ada".to_string())],
            vec![Some("2".to_string()), Some("Grace".to_string())],
        ]
    );
}

#[tokio::test]
async fn uncommitted_wal_record_is_ignored_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    write_uncommitted_insert_record_for_test(dir.path(), 1, "Ada").unwrap();

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    server
        .simple_query("create table users (id integer primary key, name text)")
        .await
        .unwrap();
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();

    assert!(rows.is_empty());
}
