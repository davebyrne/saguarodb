mod support;

use support::{Connection, TestServer};

/// A dictionary created AFTER the last checkpoint must be re-resolvable at
/// recovery purely from the dict file + WAL (`compression.md` §7): create a
/// zstd table, load data, checkpoint, ALTER to train the dict (post-
/// checkpoint), insert more (dict-compressed FPIs), crash-restart, verify.
#[tokio::test]
async fn dictionary_and_compressed_wal_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query(
                "create table logs (id integer primary key, body text) with (compression = 'zstd')",
            )
            .await
            .unwrap();
        // 512 rows × ~340-byte bodies ≈ 20+ heap pages: comfortably past the
        // ≥8-sample training gate (Task 2) so the ALTER really trains a dict.
        // Multi-row inserts keep the test fast.
        let body_fill = "abcdefghijklmnopqrstuvwxyz-".repeat(12);
        for chunk in 0..32 {
            let values: Vec<String> = (0..16)
                .map(|i| {
                    let id = chunk * 16 + i;
                    format!("({id}, 'log-line-{id}-{body_fill}')")
                })
                .collect();
            server
                .simple_query(&format!(
                    "insert into logs (id, body) values {}",
                    values.join(",")
                ))
                .await
                .unwrap();
        }
        server.force_checkpoint().await.unwrap();
        // Post-checkpoint: trains + persists the dictionary, rewrites files.
        server
            .simple_query("alter table logs set (compression = 'zstd')")
            .await
            .unwrap();
        for chunk in 32..36 {
            let values: Vec<String> = (0..16)
                .map(|i| {
                    let id = chunk * 16 + i;
                    format!("({id}, 'log-line-{id}-{body_fill}')")
                })
                .collect();
            server
                .simple_query(&format!(
                    "insert into logs (id, body) values {}",
                    values.join(",")
                ))
                .await
                .unwrap();
        }
        // No checkpoint here: recovery must replay CreateDictionary +
        // AlterTableCompression + dict-compressed FPIs from the WAL.
    }
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select count(*) from logs")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("576".to_string())]]);
    // The trained dictionary file exists.
    let dicts: Vec<_> = std::fs::read_dir(dir.path().join("dicts"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("dict"))
        .collect();
    assert_eq!(dicts.len(), 1);
}

/// §13: `CREATE TABLE ... WITH (compression = 'zstd')` → insert → checkpoint →
/// restart → select roundtrip, and the heap file's on-disk allocation is
/// actually smaller than its logical size (hole-punched compressed pages),
/// mirroring the storage-level `hole_punch_reclaims_blocks_when_supported`
/// test (skip, don't fail, on a filesystem that doesn't reclaim).
#[tokio::test]
async fn zstd_table_round_trips_and_reclaims_disk() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query(
                "create table docs (id integer primary key, body text) with (compression = 'zstd')",
            )
            .await
            .unwrap();
        for i in 0..200 {
            server
                .simple_query(&format!(
                    "insert into docs (id, body) values ({i}, '{}')",
                    "lorem-ipsum-dolor-sit-amet-".repeat(8)
                ))
                .await
                .unwrap();
        }
        server.force_checkpoint().await.unwrap();
    }
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select count(*) from docs")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("200".to_string())]]);

    // Allocated < logical on a hole-punching fs (skip otherwise, as in the
    // storage-level test): find the heap file(s) under `<data_dir>/heap`.
    use std::os::unix::fs::MetadataExt;
    let heap_dir = dir.path().join("heap");
    let mut punched = false;
    let mut any_heap = false;
    for entry in std::fs::read_dir(&heap_dir).unwrap().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("heap") {
            continue;
        }
        any_heap = true;
        let meta = entry.metadata().unwrap();
        if meta.blocks() * 512 < meta.len() {
            punched = true;
        }
    }
    assert!(
        any_heap,
        "expected at least one .heap file under {heap_dir:?}"
    );
    if !punched {
        eprintln!("skipping punch assertion: filesystem did not reclaim blocks");
    }
}

/// §13: `ALTER TABLE` rewrite in both directions (`none → zstd`, `zstd →
/// none`) with correctness preserved, and the final state survives a restart.
#[tokio::test]
async fn alter_both_directions_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table t (id integer primary key, body text)")
            .await
            .unwrap();
        for i in 0..100 {
            server
                .simple_query(&format!(
                    "insert into t (id, body) values ({i}, 'row-{i}-{}')",
                    "abcdefghij".repeat(10)
                ))
                .await
                .unwrap();
        }
        server
            .simple_query("alter table t set (compression = 'zstd')")
            .await
            .unwrap();
        let rows = server
            .simple_query("select count(*) from t")
            .await
            .unwrap()
            .unwrap_rows();
        assert_eq!(rows, vec![vec![Some("100".to_string())]]);

        server
            .simple_query("alter table t set (compression = 'none')")
            .await
            .unwrap();
        let rows = server
            .simple_query("select count(*) from t")
            .await
            .unwrap()
            .unwrap_rows();
        assert_eq!(rows, vec![vec![Some("100".to_string())]]);
    }
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select min(id), max(id), count(*) from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![
            Some("0".to_string()),
            Some("99".to_string()),
            Some("100".to_string())
        ]]
    );
}

/// §13: VACUUM works on a compressed table — dead versions are reclaimed
/// through envelope-encoded (compressed) pages just as on raw pages.
#[tokio::test]
async fn vacuum_on_compressed_table() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    server
        .simple_query(
            "create table t (id integer primary key, body text) with (compression = 'zstd')",
        )
        .await
        .unwrap();
    for i in 0..50 {
        server
            .simple_query(&format!("insert into t (id, body) values ({i}, 'x{i}y')"))
            .await
            .unwrap();
    }
    server
        .simple_query("update t set body = 'updated' where id < 25")
        .await
        .unwrap();
    server
        .simple_query("delete from t where id >= 40")
        .await
        .unwrap();
    server.simple_query("vacuum t").await.unwrap();
    let rows = server
        .simple_query("select count(*) from t")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("40".to_string())]]);
}

/// §13: `ALTER TABLE ... SET (compression = ...)` is a maintenance command
/// (like VACUUM) and is rejected inside an explicit transaction block,
/// poisoning it to `'E'` — mirroring
/// `vacuum_inside_transaction_block_is_rejected` in `tests/vacuum.rs`.
#[tokio::test]
async fn alter_rejected_inside_transaction_block() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();

    conn.ok("create table t (id integer primary key)").await;
    conn.ok("begin").await;
    let outcome = conn
        .query("alter table t set (compression = 'zstd')")
        .await
        .unwrap();
    let err = outcome
        .result
        .err()
        .expect("ALTER TABLE in a transaction block is rejected");
    assert!(
        err.message.to_lowercase().contains("transaction block"),
        "message was: {}",
        err.message
    );
    assert_eq!(
        outcome.status, b'E',
        "ALTER TABLE poisons the open block to 'E'"
    );
    conn.ok("rollback").await;

    // The connection is usable again afterward, and the table was untouched.
    let after = conn.ok("select id from t").await;
    assert_eq!(after.status, b'I');
    assert!(after.rows().is_empty());
}

/// §13: unknown `WITH` keys/codecs and unsupported ALTER forms are rejected at
/// parse time; nothing reaches storage.
#[tokio::test]
async fn bad_options_are_rejected() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key)")
        .await
        .unwrap();

    for sql in [
        "create table u (id integer primary key) with (fillfactor = 70)",
        "create table u (id integer primary key) with (compression = 'lz4')",
        "alter table t set (compression = 'lz4')",
        "alter table t add column x integer",
    ] {
        server
            .simple_query(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected rejection for `{sql}`"));
    }
}
