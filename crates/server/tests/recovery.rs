mod support;

use std::path::Path;

use support::{Connection, TestServer, write_uncommitted_record_for_test};

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
async fn committed_multi_statement_transaction_survives_restart() {
    // A committed explicit transaction's statements all share one txn_id with a
    // single durable Commit, so redo-committed-only replays them together: every
    // row of `BEGIN; INSERT; INSERT; COMMIT` is visible after restart.
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("create table users (id integer primary key, name text)")
            .await;
        conn.ok("begin").await;
        conn.ok("insert into users (id, name) values (1, 'Ada')")
            .await;
        conn.ok("insert into users (id, name) values (2, 'Grace')")
            .await;
        let commit = conn.ok("commit").await;
        assert_eq!(commit.status, b'I');
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
async fn in_flight_transaction_rows_are_not_visible_after_restart() {
    // A transaction that never commits before the "crash" leaves uncommitted
    // HeapInsert records with no durable Commit. Under redo-committed-only those
    // records are not replayed and the flush gate never wrote their pages, so the
    // rows are absent after restart. (Full redo-all is Milestone D2; this stays
    // within what redo-committed-only supports.)
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        // Commit the table create so the table exists after restart.
        server
            .simple_query("create table users (id integer primary key, name text)")
            .await
            .unwrap();
        // Open a transaction, insert rows, and never commit. Dropping the
        // connection and server ends the in-flight transaction without a Commit.
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("begin").await;
        conn.ok("insert into users (id, name) values (1, 'Ada')")
            .await;
        conn.ok("insert into users (id, name) values (2, 'Grace')")
            .await;
        // No COMMIT: the connection drops here, then the server drops.
        conn.close().await;
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(
        rows.is_empty(),
        "an uncommitted transaction's rows are not visible after restart"
    );
}

#[tokio::test]
async fn committed_delete_stays_hidden_after_restart() {
    // A committed autocommit DELETE stamps xmax via HeapUpdateHeader; recovery must
    // replay that redo so the deleted row stays hidden by visibility after restart.
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
        server
            .simple_query("delete from users where id = 1")
            .await
            .unwrap();
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id, name from users order by id")
        .await
        .unwrap()
        .unwrap_rows();

    // Only Grace survives the restart; the committed delete of Ada is replayed.
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string()), Some("Grace".to_string())]]
    );
}

#[tokio::test]
async fn delete_then_reinsert_survives_restart() {
    // delete-then-reinsert of the same primary key now succeeds (the committed
    // deleted version no longer blocks it); recovery replays the delete and the
    // re-insert, leaving the re-inserted row visible.
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
            .simple_query("delete from users where id = 1")
            .await
            .unwrap();
        server
            .simple_query("insert into users (id, name) values (1, 'Bea')")
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
        vec![vec![Some("1".to_string()), Some("Bea".to_string())]]
    );
}

#[tokio::test]
async fn committed_update_new_version_survives_restart() {
    // A committed autocommit UPDATE writes a new heap version, stamps the old
    // version's xmax + t_ctid->new via HeapUpdateHeader, and inserts new index
    // entries. Recovery replays all of those records, so after restart a SELECT
    // (seq scan and index scan) sees the NEW value and not the old one.
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table users (id integer primary key, name text)")
            .await
            .unwrap();
        server
            .simple_query("create index users_name on users (name)")
            .await
            .unwrap();
        server
            .simple_query("insert into users (id, name) values (1, 'Ada')")
            .await
            .unwrap();
        server
            .simple_query("update users set name = 'Bea' where id = 1")
            .await
            .unwrap();
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    // Sequential scan sees the new value after restart.
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("Bea".to_string())]]
    );
    // Index scan on the new value resolves the new version; the old value is gone.
    let rows = server
        .simple_query("select id from users where name = 'Bea'")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    let rows = server
        .simple_query("select id from users where name = 'Ada'")
        .await
        .unwrap()
        .unwrap_rows();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn aborted_update_leaves_old_value_after_restart() {
    // A UPDATE that violates a unique secondary constraint errors; the autocommit
    // transaction aborts (before-image undo restores the page, and the Abort
    // record marks the txn aborted). After restart no orphan new version is
    // visible and the old value survives.
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table users (id integer primary key, name text)")
            .await
            .unwrap();
        server
            .simple_query("create unique index uq_name on users (name)")
            .await
            .unwrap();
        server
            .simple_query("insert into users (id, name) values (1, 'Ada')")
            .await
            .unwrap();
        server
            .simple_query("insert into users (id, name) values (2, 'Bea')")
            .await
            .unwrap();

        // Updating row 1's name to 'Bea' collides with the live row 2 ⇒ the
        // statement errors and the autocommit transaction aborts.
        let err = server
            .simple_query("update users set name = 'Bea' where id = 1")
            .await
            .err()
            .expect("unique violation aborts the update");
        assert!(err.message.to_lowercase().contains("unique"));
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    // The aborted update left both original rows; no orphan new 'Bea'-named version
    // of id 1 is visible.
    let rows = server
        .simple_query("select id, name from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("Ada".to_string())],
            vec![Some("2".to_string()), Some("Bea".to_string())],
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
async fn recovery_succeeds_with_buffer_smaller_than_working_set() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table big (id integer primary key, payload text)")
            .await
            .unwrap();
        let payload = "x".repeat(7000);
        for id in 1..=4 {
            server
                .simple_query(&format!(
                    "insert into big (id, payload) values ({id}, '{payload}')"
                ))
                .await
                .unwrap();
        }
        server.force_checkpoint().await.unwrap();
    }

    // Reopen with a one-frame buffer pool. The durable on-disk index means
    // recovery rebuilds nothing in memory, so it no longer needs the working set
    // to fit — it spills, and queries still read every row.
    let config = saguarodb_server::config::Config {
        data_dir: dir.path().to_path_buf(),
        port: 0,
        buffer_pool_frames: 1,
        checkpoint_every_n_commits: 1_000,
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        shutdown_timeout_ms: 1_000,
        ..Default::default()
    };
    let app = saguarodb_server::recovery::open_app(config).unwrap();
    let result = app
        .query_service
        .execute_sql("select id from big order by id")
        .unwrap();
    assert_eq!(result.row_count(), 4);
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
        ..Default::default()
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

#[tokio::test]
async fn insert_after_checkpoint_and_restart_does_not_reuse_pages() {
    let dir = tempfile::tempdir().unwrap();
    let payload = "x".repeat(7000);
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table t (id integer primary key, payload text)")
            .await
            .unwrap();
        // Four big rows occupy four heap pages, then a checkpoint makes them
        // durable (and truncates their redo).
        for id in 1..=4 {
            server
                .simple_query(&format!(
                    "insert into t (id, payload) values ({id}, '{payload}')"
                ))
                .await
                .unwrap();
        }
        server.force_checkpoint().await.unwrap();
    }

    // Reopen (recovery replays nothing) and insert a fifth row needing a new heap
    // page. The page allocator must be seeded from the on-disk extent so the new
    // page does not reuse page 0 and overwrite id=1.
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query(&format!(
                "insert into t (id, payload) values (5, '{payload}')"
            ))
            .await
            .unwrap();
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        (1..=5)
            .map(|id| vec![Some(id.to_string())])
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn split_index_survives_restart_and_post_restart_growth() {
    let dir = tempfile::tempdir().unwrap();
    // In-process (no TCP) so the thousands of inserts that force index splits stay
    // fast. A fresh config per phase reopens the same data dir.
    let config = || saguarodb_server::config::Config {
        data_dir: dir.path().to_path_buf(),
        port: 0,
        buffer_pool_frames: 64,
        checkpoint_every_n_commits: 100,
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        shutdown_timeout_ms: 1_000,
        ..Default::default()
    };

    // Build an index that splits into a root plus several leaves.
    {
        let app = saguarodb_server::recovery::open_app(config()).unwrap();
        app.query_service
            .execute_sql("create table t (id integer primary key)")
            .unwrap();
        for id in 0..400 {
            app.query_service
                .execute_sql(&format!("insert into t (id) values ({id})"))
                .unwrap();
        }
    }

    // Reopen and keep inserting ascending keys. These fill the rightmost leaf and
    // split it, allocating fresh index pages *after* recovery — the allocator must
    // be seeded from the .idx extent or a new node would reuse an existing index
    // page and corrupt the tree.
    {
        let app = saguarodb_server::recovery::open_app(config()).unwrap();
        for id in 400..800 {
            app.query_service
                .execute_sql(&format!("insert into t (id) values ({id})"))
                .unwrap();
        }
    }

    // Reopen once more and confirm every key is present and ordered.
    let app = saguarodb_server::recovery::open_app(config()).unwrap();
    let result = app
        .query_service
        .execute_sql("select id from t order by id")
        .unwrap();
    assert_eq!(result.row_count(), 800);
}

/// Overwrite the first page of every heap (`.heap`) file with garbage, simulating
/// a torn heap write. Index (`.idx`) files are left intact: this exercises
/// torn-page repair of a heap page, and the metapage of an index is not rewritten
/// post-checkpoint, so it relies on the checkpoint's durable write rather than redo.
fn corrupt_heap_pages(data_dir: &Path) {
    use std::io::Write;
    let heap_dir = data_dir.join("heap");
    for entry in std::fs::read_dir(&heap_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("heap") {
            continue;
        }
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all(&[0xFF; 8192]).unwrap();
        file.sync_all().unwrap();
    }
}
