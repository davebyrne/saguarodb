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
    // single durable Commit. Redo-all replays every record and the CLOG marks the
    // txn committed, so every row of `BEGIN; INSERT; INSERT; COMMIT` is visible
    // after restart.
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
    // A transaction that never commits before the "crash" leaves heap records with
    // no durable Commit. Redo-all replays those records (Milestone D2), but the
    // txn has no `Commit`, so recovery records it Aborted (in-flight-at-crash =
    // aborted) and the visibility predicate hides its rows after restart.
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
    // An UPDATE that violates a unique secondary constraint errors; the autocommit
    // transaction aborts. Abort is status-based (Milestone D1): the new version
    // stays in the heap but the `Abort` record marks the txn aborted, so redo-all
    // replays it yet the CLOG hides it. After restart no orphan new version is
    // visible and the old value (created by the committed insert) survives.
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
async fn uncommitted_wal_record_is_invisible_after_restart() {
    // Redo-all (`docs/specs/mvcc.md` §8, Milestone D2) REPLAYS an uncommitted
    // transaction's flushed heap records (reconstructing the page), rather than
    // ignoring them — but with no durable `Commit` the txn is recovered as aborted,
    // so its tuple is invisible. The synthetic record is on a standalone file id,
    // so it does not collide with the table created after recovery. (Before D2,
    // redo-committed-only simply skipped the record.)
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

#[tokio::test]
async fn committed_then_truncated_transaction_stays_visible_via_floor() {
    // A committed transaction whose `Commit` record is later truncated by a
    // checkpoint must stay visible after restart, via the implicit-committed floor
    // (`docs/specs/mvcc.md` §5.4). Sequence: commit a row, checkpoint (truncates
    // that txn's records), commit a second row, checkpoint, then crash. After
    // restart the first row — whose Commit is long gone — is still visible.
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
        server.force_checkpoint().await.unwrap();
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
async fn committed_row_survives_back_to_back_checkpoints_with_no_write_between() {
    // Regression for the Checkpoint-marker/floor interaction (`docs/specs/mvcc.md`
    // §5.4): two checkpoints with NO committed write between them. The second
    // checkpoint's truncation boundary lands on the FIRST checkpoint's `Checkpoint`
    // marker (the highest retained LSN), dropping the last committed transaction's
    // real `Commit` record. The marker carries that transaction's id as its
    // high-water `txn_id`, but the marker is metadata, not a transaction needing
    // protection — so the recovery floor scan must EXCLUDE it. If it counted the
    // marker as a "retained non-committed transaction", the floor would clamp at
    // that id and the committed row would read in-progress (invisible) after
    // restart — silent committed-data loss. (This is the idle-then-shutdown-
    // checkpoint sequence in production.)
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
        // Two checkpoints back-to-back, no write in between.
        server.force_checkpoint().await.unwrap();
        server.force_checkpoint().await.unwrap();
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id, name from users")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("Ada".to_string())]],
        "the committed row must survive two checkpoints with no write between them"
    );
}

#[tokio::test]
async fn aborted_transaction_stays_invisible_across_checkpoint_and_restart() {
    // THE CRITICAL Milestone-D correctness test (change 4). An explicit transaction
    // writes rows, ROLLBACKs (status-based abort: no undo), its pages are flushed to
    // the heap by a checkpoint, and a LATER committed row pushes the aborted txn
    // below the next checkpoint's truncation boundary. The conservative-truncation
    // guard (`docs/specs/mvcc.md` §5.4, §8) must keep the aborted txn's `Abort`
    // record (and the floor below it) so its on-disk rows stay invisible after a
    // crash. WITHOUT the guard, truncation would drop the `Abort` and the floor
    // would float above the aborted txn, marking it implicitly committed and making
    // its rows wrongly appear — corruption. (Manually verified: replacing the
    // conservative `effective_lsn` clamp in `WalManager::truncate_before` with the
    // raw `lsn`, and the conservative recovery floor with the allocation boundary,
    // makes this test fail with 'Ghost' visible.)
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table users (id integer primary key, name text)")
            .await
            .unwrap();
        // A committed base row.
        server
            .simple_query("insert into users (id, name) values (1, 'Ada')")
            .await
            .unwrap();

        // An explicit transaction inserts a row, then ROLLBACKs. Its heap+index
        // pages stay dirty (no before-image undo); the Abort record is appended.
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("begin").await;
        conn.ok("insert into users (id, name) values (2, 'Ghost')")
            .await;
        let rolled_back = conn.ok("rollback").await;
        assert_eq!(rolled_back.status, b'I');
        conn.close().await;

        // Checkpoint: flushes the aborted txn's dirty pages to the heap (relaxed
        // flush gate) and truncates committed WAL history — but must PIN the aborted
        // txn so its Abort survives.
        server.force_checkpoint().await.unwrap();

        // A later committed row, then another checkpoint, so the aborted txn sits
        // below the truncation boundary (the scenario the guard must survive).
        server
            .simple_query("insert into users (id, name) values (3, 'Bea')")
            .await
            .unwrap();
        server.force_checkpoint().await.unwrap();
    }

    // Crash + restart: redo-all replays the aborted txn's flushed rows, but the
    // retained Abort (and the conservative floor) keep them invisible.
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
            vec![Some("3".to_string()), Some("Bea".to_string())],
        ],
        "the rolled-back 'Ghost' row must never be visible after restart"
    );
}

#[tokio::test]
async fn vacuumed_aborted_txn_is_truncated_past_with_no_resurrection_after_restart() {
    // THE critical Milestone-F4c test (`docs/specs/mvcc.md` §5.4, §9). This is the
    // relaxation of the conservative-truncation guard, and the place where a mistake
    // reintroduces the aborted-data-visible-after-crash hole. Sequence:
    //   1. A committed base row 'Ada'.
    //   2. An explicit transaction inserts 'Ghost', then ROLLBACKs (status-based abort,
    //      no undo); its heap+index pages stay dirty.
    //   3. A checkpoint FLUSHES the aborted txn's pages to the heap and PINS its WAL
    //      records (Abort retained) — the conservative guard, still in force here.
    //   4. A FULL `VACUUM` reclaims the aborted-creator 'Ghost' tuple (heap + index;
    //      aborted-creator reclaim has NO age requirement) and advances the vacuum
    //      floor past the aborted txn.
    //   5. A later committed row 'Bea', then another checkpoint: now the aborted txn is
    //      BELOW the vacuum floor, so truncation DROPS its `Abort` and floats the
    //      implicit-committed floor past it — safe, because the VACUUM already made the
    //      'Ghost' reclamation durable (the checkpoint flush+fsync the VACUUM's pages
    //      before this checkpoint's `truncate_before` consults the floor).
    //   6. Crash + restart: 'Ghost' must be ABSENT — no committed ghost. There is no
    //      surviving on-disk version to resurrect, so flooring past the aborted txn is
    //      vacuously correct.
    // Without the F4c gating, blindly flooring past a NON-vacuumed abort (the
    // counter-test `aborted_transaction_stays_invisible_across_checkpoint_and_restart`)
    // WOULD resurrect 'Ghost'; here the relaxation is licensed only because VACUUM
    // reclaimed its tuples first.
    let dir = tempfile::tempdir().unwrap();
    let wal_bytes_before_truncation;
    let wal_bytes_after_truncation;
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

        // Abort a transaction that inserted 'Ghost'.
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("begin").await;
        conn.ok("insert into users (id, name) values (2, 'Ghost')")
            .await;
        assert_eq!(conn.ok("rollback").await.status, b'I');
        conn.close().await;

        // Checkpoint: flush the aborted txn's pages; its Abort is still PINNED here
        // (no VACUUM yet), so truncation retains it. Record the retained WAL size.
        server.force_checkpoint().await.unwrap();
        wal_bytes_before_truncation = server.app().components.wal.bytes_after(0).unwrap();

        // FULL VACUUM: reclaim the aborted-creator 'Ghost' tuple (heap + index) and
        // advance the vacuum floor past the aborted txn. `VACUUM` with no table is a
        // full pass over every user table.
        let vacuum = server.simple_query("vacuum").await;
        assert!(vacuum.is_ok(), "full VACUUM should succeed");

        // A later committed row, then a checkpoint: the aborted txn is now below the
        // vacuum floor, so truncation drops its Abort and floats the floor past it. The
        // VACUUM's reclamation is flushed+fsynced by THIS checkpoint before its
        // truncate_before consults the floor (the durability-ordering invariant).
        server
            .simple_query("insert into users (id, name) values (3, 'Bea')")
            .await
            .unwrap();
        server.force_checkpoint().await.unwrap();
        wal_bytes_after_truncation = server.app().components.wal.bytes_after(0).unwrap();
    }

    // The relaxation had EFFECT: after VACUUM, the second checkpoint truncated past the
    // previously-pinning aborted txn, so the retained WAL did not just grow by the new
    // committed row — it dropped the pinned prefix. (A weaker but robust check that the
    // truncation reached past the abort: the retained size did not balloon.)
    assert!(
        wal_bytes_after_truncation <= wal_bytes_before_truncation,
        "after VACUUM the checkpoint truncated past the reclaimed aborted txn \
         (before={wal_bytes_before_truncation}, after={wal_bytes_after_truncation})"
    );

    // Crash + restart: 'Ghost' is gone — no committed-ghost resurrection.
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
            vec![Some("3".to_string()), Some("Bea".to_string())],
        ],
        "the VACUUM-reclaimed aborted 'Ghost' row must NOT resurrect after restart"
    );
}

#[tokio::test]
async fn single_table_vacuum_does_not_relax_truncation_for_other_tables() {
    // A single-table `VACUUM t` must NOT advance the vacuum floor (`docs/specs/mvcc.md`
    // §9, F4c): it leaves OTHER tables' aborted-creator tuples on disk, so flooring
    // past those aborts would resurrect them. Here an aborted txn writes to `other`,
    // then `VACUUM users` (a DIFFERENT table) runs — which must NOT reclaim `other`'s
    // ghost nor advance the floor — and after a checkpoint + restart the ghost stays
    // invisible (the abort still pins, exactly as in the no-VACUUM counter-test).
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table users (id integer primary key, name text)")
            .await
            .unwrap();
        server
            .simple_query("create table other (id integer primary key, name text)")
            .await
            .unwrap();
        server
            .simple_query("insert into other (id, name) values (1, 'Keep')")
            .await
            .unwrap();

        // Abort a transaction that inserted a ghost into `other`.
        let mut conn = Connection::connect(&server).await.unwrap();
        conn.ok("begin").await;
        conn.ok("insert into other (id, name) values (2, 'Ghost')")
            .await;
        assert_eq!(conn.ok("rollback").await.status, b'I');
        conn.close().await;

        server.force_checkpoint().await.unwrap();

        // VACUUM a DIFFERENT table: does not reclaim `other`'s ghost and does not
        // advance the vacuum floor, so `other`'s aborted txn must still pin truncation.
        assert!(server.simple_query("vacuum users").await.is_ok());

        server
            .simple_query("insert into other (id, name) values (3, 'Also')")
            .await
            .unwrap();
        server.force_checkpoint().await.unwrap();
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id, name from other order by id")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("Keep".to_string())],
            vec![Some("3".to_string()), Some("Also".to_string())],
        ],
        "a single-table VACUUM must not relax truncation for another table's abort"
    );
}

#[tokio::test]
async fn uncommitted_pages_evicted_under_pressure_then_committed_are_visible() {
    // With a small buffer pool, a large transaction's uncommitted pages are stolen
    // (flushed to the heap) under buffer pressure — the relaxed flush gate
    // (Milestone D1) admits them. After COMMIT and restart, redo-all + the committed
    // CLOG make every row visible.
    let dir = tempfile::tempdir().unwrap();
    let payload = "x".repeat(7000);
    {
        let app = saguarodb_server::recovery::open_app(small_pool_config(dir.path())).unwrap();
        app.query_service
            .execute_sql("create table big (id integer primary key, payload text)")
            .unwrap();
        // One big transaction far larger than the 4-frame pool: its uncommitted
        // pages must spill to the heap mid-transaction. The autocommit `execute_sql`
        // cannot hold a transaction across calls, so drive the explicit transaction
        // through the session-carrying simple path.
        let cancel = std::sync::atomic::AtomicBool::new(false);
        // The session default isolation is irrelevant here (these are plain explicit
        // transactions); thread the built-in default and ignore the returned one.
        let iso = common::IsolationLevel::default();
        let (mut slot, _iso, res) = app
            .query_service
            .execute_simple("begin", None, iso, &cancel);
        res.unwrap();
        for id in 1..=10 {
            let (next, _iso, res) = app.query_service.execute_simple(
                &format!("insert into big (id, payload) values ({id}, '{payload}')"),
                slot,
                iso,
                &cancel,
            );
            res.unwrap();
            slot = next;
        }
        let (slot, _iso, res) = app
            .query_service
            .execute_simple("commit", slot, iso, &cancel);
        res.unwrap();
        assert!(slot.is_none());
    }

    let app = saguarodb_server::recovery::open_app(small_pool_config(dir.path())).unwrap();
    let result = app
        .query_service
        .execute_sql("select id from big order by id")
        .unwrap();
    assert_eq!(result.row_count(), 10);
}

#[tokio::test]
async fn uncommitted_pages_evicted_under_pressure_then_aborted_are_invisible() {
    // The mirror of the previous test: a large transaction's uncommitted pages are
    // stolen to the heap under buffer pressure, then the transaction ROLLBACKs
    // (status-based abort) and a checkpoint makes everything durable. After restart,
    // redo-all replays the flushed pages but the CLOG (Aborted) hides every row.
    let dir = tempfile::tempdir().unwrap();
    let payload = "x".repeat(7000);
    {
        let app = saguarodb_server::recovery::open_app(small_pool_config(dir.path())).unwrap();
        app.query_service
            .execute_sql("create table big (id integer primary key, payload text)")
            .unwrap();
        let cancel = std::sync::atomic::AtomicBool::new(false);
        // The session default isolation is irrelevant here (these are plain explicit
        // transactions); thread the built-in default and ignore the returned one.
        let iso = common::IsolationLevel::default();
        let (mut slot, _iso, res) = app
            .query_service
            .execute_simple("begin", None, iso, &cancel);
        res.unwrap();
        for id in 1..=10 {
            let (next, _iso, res) = app.query_service.execute_simple(
                &format!("insert into big (id, payload) values ({id}, '{payload}')"),
                slot,
                iso,
                &cancel,
            );
            res.unwrap();
            slot = next;
        }
        let (slot, _iso, res) = app
            .query_service
            .execute_simple("rollback", slot, iso, &cancel);
        res.unwrap();
        assert!(slot.is_none());
        // Make the flushed-then-aborted pages durable, exercising the conservative
        // truncation guard for the eviction path too.
        saguarodb_server::checkpoint::run_checkpoint(&app.components).unwrap();
    }

    let app = saguarodb_server::recovery::open_app(small_pool_config(dir.path())).unwrap();
    let result = app.query_service.execute_sql("select id from big").unwrap();
    assert_eq!(
        result.row_count(),
        0,
        "a rolled-back transaction's evicted rows are invisible after restart"
    );
}

#[tokio::test]
async fn aborted_autocommit_statement_stays_invisible_after_restart() {
    // An autocommit write that errors mid-statement aborts (status-based). Its
    // partial heap writes may have been flushed; after restart they are invisible.
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table users (id integer primary key, val integer)")
            .await
            .unwrap();
        server
            .simple_query("insert into users (id, val) values (1, 1)")
            .await
            .unwrap();
        server
            .simple_query("insert into users (id, val) values (2, 9223372036854775807)")
            .await
            .unwrap();
        // An UPDATE that overflows on the second row aborts the whole statement
        // after mutating the first row's version.
        let err = server
            .simple_query("update users set val = val + 1")
            .await
            .err()
            .expect("overflow aborts the update");
        assert!(err.message.to_lowercase().contains("range"));
        server.force_checkpoint().await.unwrap();
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id, val from users order by id")
        .await
        .unwrap()
        .unwrap_rows();
    // Both original values survive; the aborted UPDATE's new versions are invisible.
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("1".to_string())],
            vec![
                Some("2".to_string()),
                Some("9223372036854775807".to_string()),
            ],
        ]
    );
}

/// Checkpoint-vs-writer under concurrent writers, then crash + recover (E2b). While
/// several writer connections insert committed rows, a checkpoint fires concurrently:
/// it takes the EXCLUSIVE guard, drains every in-flight (shared) writer, and runs
/// alone — so it must complete with no "unflushable dirty page" error (the preserved
/// "no in-flight writer at checkpoint" invariant). One extra transaction is left
/// uncommitted at the "crash" (the process drops without COMMIT). After restart the
/// committed rows all survive and the uncommitted one is invisible (in-flight-at-
/// crash = aborted) — confirming the inverted lock keeps the Milestone-D guarantees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_concurrent_with_writers_then_crash_recovers_consistently() {
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    const WRITERS: i64 = 4;
    const PER_WRITER: i64 = 40;
    {
        let server = Arc::new(TestServer::start_with_data_dir(dir.path()).await.unwrap());
        {
            let mut setup = Connection::connect(&server).await.unwrap();
            setup.ok("create table t (id integer primary key)").await;
        }

        // Writer tasks insert disjoint committed key ranges (autocommit per row), so
        // many short write transactions are in flight while the checkpoint fires.
        let mut writers = Vec::new();
        for w in 0..WRITERS {
            let server = server.clone();
            writers.push(tokio::spawn(async move {
                let mut conn = Connection::connect(&server).await.unwrap();
                let base = w * PER_WRITER;
                for i in 0..PER_WRITER {
                    let id = base + i + 1;
                    conn.ok(&format!("insert into t (id) values ({id})")).await;
                }
            }));
        }

        // Fire checkpoints concurrently with the writers. Each must complete cleanly
        // (a drained, no-in-flight-writer body); `force_checkpoint` propagates any
        // "unflushable dirty page" error, so a panic here would fail the test.
        for _ in 0..3 {
            server
                .force_checkpoint()
                .await
                .expect("checkpoint drains writers and completes cleanly under concurrency");
        }

        for handle in writers {
            handle.await.expect("writer task finished");
        }

        // Leave one transaction UNCOMMITTED at the crash: open it and insert a
        // sentinel row (id 100000), then "crash" without committing. Note we do NOT
        // checkpoint while this writer is open: a checkpoint takes the EXCLUSIVE guard
        // and would (correctly) block waiting for this in-flight writer's SHARED guard
        // to drain. Recovery replays the in-flight insert's WAL records under redo-all
        // and the CLOG hides them (in-flight-at-crash = aborted), so the sentinel is
        // invisible after restart regardless of whether its page reached the heap.
        let mut dangling = Connection::connect(&server).await.unwrap();
        dangling.ok("begin").await;
        dangling.ok("insert into t (id) values (100000)").await;
        // "Crash": drop the connection and the server without committing `dangling`.
        dangling.close().await;
        // The server (and its Arc) is dropped at the end of this scope.
    }

    // Recover and assert consistency: every committed row survives; the uncommitted
    // sentinel (id 100000) is invisible (in-flight-at-crash = aborted).
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select id from t order by id")
        .await
        .unwrap()
        .unwrap_rows();
    let expected: Vec<Vec<Option<String>>> = (1..=(WRITERS * PER_WRITER))
        .map(|id| vec![Some(id.to_string())])
        .collect();
    assert_eq!(
        rows.len(),
        expected.len(),
        "every committed row survives the concurrent checkpoint + crash"
    );
    assert_eq!(rows, expected);
    assert!(
        !rows.iter().any(|r| r[0] == Some("100000".to_string())),
        "the uncommitted sentinel row is invisible after recovery"
    );
}

/// A 4-frame pool with checkpoints effectively disabled, so a transaction's
/// working set must exceed the pool and spill (steal) to the heap mid-flight.
fn small_pool_config(dir: &Path) -> saguarodb_server::config::Config {
    saguarodb_server::config::Config {
        data_dir: dir.to_path_buf(),
        port: 0,
        buffer_pool_frames: 4,
        checkpoint_every_n_commits: 1_000_000,
        checkpoint_wal_bytes: 1 << 30,
        shutdown_timeout_ms: 1_000,
        ..Default::default()
    }
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
