mod support;

use std::path::Path;

use support::{TestServer, write_uncommitted_record_for_test};

#[tokio::test]
async fn committed_data_survives_restart_with_checkpoint_and_wal() {
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
    write_uncommitted_record_for_test(dir.path()).unwrap();

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

#[tokio::test]
async fn committed_data_survives_crash_without_checkpoint() {
    // No checkpoint before the crash: recovery must redo the committed records
    // from the start of the WAL onto freshly created heap pages.
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
async fn redo_replays_across_repeated_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        // Crash with row 1 uncheckpointed.
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table t (id integer primary key)")
            .await
            .unwrap();
        server
            .simple_query("insert into t (id) values (1)")
            .await
            .unwrap();
    }
    {
        // Cycle 1 recovery redoes row 1 and checkpoints; then write row 2
        // (uncheckpointed) and crash again.
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("insert into t (id) values (2)")
            .await
            .unwrap();
    }

    // Cycle 2 recovery redoes row 2 onto the heap recovered in cycle 1. Each
    // recovery replays the records written since the previous checkpoint.
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id from t order by id")
        .await
        .unwrap()
        .unwrap_rows();

    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );
}

#[tokio::test]
async fn torn_heap_page_is_repaired_by_full_page_image() {
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
        // The checkpoint flushes the page to the heap; the next write is the first
        // modification of that page since the checkpoint, so it logs a full-page
        // image covering both rows.
        server.force_checkpoint().await.unwrap();
        server
            .simple_query("insert into users (id, name) values (2, 'Grace')")
            .await
            .unwrap();
    }

    // Simulate a torn heap write by corrupting the on-disk page bytes. Redo must
    // reinstall the full-page image and recover both rows.
    corrupt_heap_pages(dir.path());

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
async fn recovery_fails_loudly_when_buffer_too_small() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table big (id integer primary key, payload text)")
            .await
            .unwrap();
        // Two rows each large enough to occupy a separate page.
        let payload = "x".repeat(7000);
        server
            .simple_query(&format!(
                "insert into big (id, payload) values (1, '{payload}')"
            ))
            .await
            .unwrap();
        server
            .simple_query(&format!(
                "insert into big (id, payload) values (2, '{payload}')"
            ))
            .await
            .unwrap();
        server.force_checkpoint().await.unwrap();
    }

    // Reopen with a one-frame buffer pool: the recovery working set no longer
    // fits, so the directory rebuild would be partial. Recovery must error.
    let config = saguarodb_server::config::Config {
        data_dir: dir.path().to_path_buf(),
        port: 0,
        buffer_pool_frames: 1,
        checkpoint_every_n_commits: 1_000,
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        shutdown_timeout_ms: 1_000,
    };
    let err = match saguarodb_server::recovery::open_app(config) {
        Ok(_) => panic!("expected recovery to fail with a one-frame buffer pool"),
        Err(err) => err,
    };
    assert!(err.message.contains("buffer pool is too small"));
}

#[tokio::test]
async fn committed_pages_spill_to_heap_under_buffer_pressure() {
    let dir = tempfile::tempdir().unwrap();
    // A small pool with checkpoints effectively disabled: the committed working
    // set must exceed the pool, so eviction-flush-on-steal spills pages to the
    // heap during normal operation rather than erroring out of frames.
    let config = saguarodb_server::config::Config {
        data_dir: dir.path().to_path_buf(),
        port: 0,
        buffer_pool_frames: 4,
        checkpoint_every_n_commits: 1_000_000,
        checkpoint_wal_bytes: 1 << 30,
        shutdown_timeout_ms: 1_000,
    };
    let app = saguarodb_server::recovery::open_app(config).unwrap();
    app.query_service
        .execute_sql("create table big (id integer primary key, payload text)")
        .unwrap();

    // Each row is large enough to fill its own page, so ten rows need far more
    // than four frames.
    let payload = "x".repeat(7000);
    for id in 1..=10 {
        app.query_service
            .execute_sql(&format!(
                "insert into big (id, payload) values ({id}, '{payload}')"
            ))
            .unwrap();
    }

    let result = app
        .query_service
        .execute_sql("select id from big order by id")
        .unwrap();
    assert_eq!(result.row_count(), 10);
}

/// Overwrite the first page of every heap file with garbage, simulating a torn
/// write that leaves the on-disk page corrupt.
fn corrupt_heap_pages(data_dir: &Path) {
    use std::io::Write;
    let heap_dir = data_dir.join("heap");
    for entry in std::fs::read_dir(&heap_dir).unwrap() {
        let path = entry.unwrap().path();
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all(&[0xFF; 8192]).unwrap();
        file.sync_all().unwrap();
    }
}
