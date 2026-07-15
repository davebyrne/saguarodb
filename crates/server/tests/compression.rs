mod support;

use common::{
    ColumnDef, CompressionSetting, DataType, KeyRange, PgType, RelationKind, StatementContext,
    TableSchema, ToastCompression, ToastMode, ToastOptions,
};
use saguarodb_server::config::Config;
use storage::SchemaOperations;
use support::{Connection, TestServer};

/// Independently probes whether this filesystem's `fallocate`
/// `FALLOC_FL_PUNCH_HOLE` actually reclaims disk blocks, using a throwaway
/// scratch file under `dir` (never a table's heap file) — mirroring the
/// storage-level `hole_punch_reclaims_blocks_when_supported` probe
/// (`crates/storage/src/heap.rs`). Some filesystems accept the fallocate call
/// but don't shrink the allocated block count, so only a measured shrink in
/// `st_blocks` counts as "supports punch". Deciding the test's expectation
/// this way (rather than from whether the docs heap happened to shrink) means
/// a real compression/punch regression cannot masquerade as "unsupported
/// filesystem".
#[cfg(target_os = "linux")]
fn filesystem_supports_hole_punch(dir: &std::path::Path) -> bool {
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;

    let path = dir.join("hole-punch-probe.tmp");
    let mut file = std::fs::File::create(&path).unwrap();
    let block: u64 = 4096;
    file.write_all(&vec![0xABu8; (block * 2) as usize]).unwrap();
    file.sync_all().unwrap();
    let before = file.metadata().unwrap().blocks() * 512;

    let punched = rustix::fs::fallocate(
        &file,
        rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
        0,
        block,
    )
    .is_ok();
    let reclaimed = punched && {
        let after = file.metadata().unwrap().blocks() * 512;
        after < before
    };
    drop(file);
    let _ = std::fs::remove_file(&path);
    reclaimed
}

#[cfg(not(target_os = "linux"))]
fn filesystem_supports_hole_punch(_dir: &std::path::Path) -> bool {
    false
}

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
        // CatalogChange + dict-compressed FPIs from the WAL.
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
/// restart → select roundtrip, and the `docs` table's OWN heap file's on-disk
/// allocation is actually smaller than its logical size (hole-punched
/// compressed pages), mirroring the storage-level
/// `hole_punch_reclaims_blocks_when_supported` test. Whether the reclaim
/// assertion applies is decided by an INDEPENDENT probe
/// ([`filesystem_supports_hole_punch`]) against a scratch file on this same
/// filesystem — not by whether the docs heap happened to shrink — so a real
/// compression/punch regression (e.g. every page stored raw) FAILS the test
/// on a hole-punching filesystem instead of silently passing as "skip".
#[tokio::test]
async fn zstd_table_round_trips_and_reclaims_disk() {
    let dir = tempfile::tempdir().unwrap();
    let fs_reclaims_holes = filesystem_supports_hole_punch(dir.path());

    let table_id;
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
        table_id = server
            .app()
            .components
            .catalog
            .get_table_by_name("docs")
            .unwrap()
            .expect("docs table exists in the catalog")
            .id;
    }
    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let rows = server
        .simple_query("select count(*) from docs")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(rows, vec![vec![Some("200".to_string())]]);

    // Target the docs table's OWN heap file (`<table_id>.heap`), not "any .heap
    // file" under the data dir — a catalog heap could otherwise be mistaken for
    // it. The 200-row docs table is the only thing that should live here.
    use std::os::unix::fs::MetadataExt;
    let heap_path = dir.path().join("heap").join(format!("{table_id}.heap"));
    let meta = std::fs::metadata(&heap_path)
        .unwrap_or_else(|err| panic!("expected docs heap file at {heap_path:?}: {err}"));
    let allocated = meta.blocks() * 512;
    let logical = meta.len();
    let punched = allocated < logical;

    if fs_reclaims_holes {
        assert!(
            punched,
            "zstd heap file did not reclaim disk: allocated {allocated} >= logical {logical}"
        );
    } else {
        eprintln!(
            "skipping punch assertion: independent probe found this filesystem does not \
             reclaim hole-punched blocks"
        );
    }
}

/// The deterministic `body` text inserted for a given `id` in
/// `alter_both_directions_survives_restart`, so the test can assert the exact
/// payload — not just row counts and pk aggregates — survives each rewrite.
fn expected_body(id: i32) -> String {
    format!("row-{id}-{}", "abcdefghij".repeat(10))
}

/// §13: `ALTER TABLE` rewrite in both directions (`none → zstd`, `zstd →
/// none`) with correctness preserved, and the final state survives a restart.
/// Correctness is checked both structurally (row count, pk min/max) AND at the
/// payload level: a probe row's `body` must decode back to the exact original
/// text after compression, after decompression, and after a crash-restart — a
/// rewrite that corrupted `body` while preserving ids/row-count would pass the
/// weaker checks alone.
#[tokio::test]
async fn alter_both_directions_survives_restart() {
    const PROBE_ID: i32 = 42;
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
                    "insert into t (id, body) values ({i}, '{}')",
                    expected_body(i)
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
        let body_rows = server
            .simple_query(&format!("select body from t where id = {PROBE_ID}"))
            .await
            .unwrap()
            .unwrap_rows();
        assert_eq!(
            body_rows,
            vec![vec![Some(expected_body(PROBE_ID))]],
            "body must round-trip through the none -> zstd rewrite intact"
        );

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
        let body_rows = server
            .simple_query(&format!("select body from t where id = {PROBE_ID}"))
            .await
            .unwrap()
            .unwrap_rows();
        assert_eq!(
            body_rows,
            vec![vec![Some(expected_body(PROBE_ID))]],
            "body must round-trip through the zstd -> none rewrite intact"
        );
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
    let body_rows = server
        .simple_query(&format!("select body from t where id = {PROBE_ID}"))
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        body_rows,
        vec![vec![Some(expected_body(PROBE_ID))]],
        "body must still decode correctly after a crash-restart"
    );
}

/// §13: VACUUM works on a compressed table — dead versions are reclaimed
/// through envelope-encoded (compressed) pages just as on raw pages.
///
/// `count(*) == 40` alone is a *visibility* check: MVCC hides dead versions
/// from a snapshot whether or not VACUUM ever physically reclaimed them, so on
/// its own it can't tell "VACUUM worked" from "VACUUM never ran". Two things
/// give this test real teeth for the compressed path specifically:
/// - `force_checkpoint()` runs BEFORE `vacuum t`, flushing the table's pages
///   compressed to disk first, so VACUUM's heap-prune phase must decode them
///   back through the envelope path (not find them already resident,
///   uncompressed, in the buffer pool).
/// - `dead_rows_since_vacuum()` — the per-commit dead-version accumulator
///   (`docs/specs/mvcc.md` §9) — is asserted to be exactly 35 (25 from the
///   UPDATE + 10 from the DELETE) right before VACUUM runs: a real,
///   deterministic physical signal that there is genuine dead-version debt
///   here for VACUUM to reclaim, not just a `count(*)` coincidence.
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
    assert_eq!(
        server.dead_rows_since_vacuum(),
        35,
        "25 updated + 10 deleted rows should have produced 35 committed dead \
         versions for VACUUM to reclaim"
    );

    // Flush the compressed pages to disk BEFORE vacuuming, so VACUUM's
    // heap-prune phase reads them back through the envelope decode path
    // instead of finding them still resident (uncompressed) in the buffer
    // pool.
    server.force_checkpoint().await.unwrap();

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

/// Unknown `WITH`/`SET` keys and unsupported codecs are rejected at parse
/// time. PostgreSQL's `fillfactor` is accepted on `CREATE TABLE` as a
/// validated compatibility no-op, but remains unsupported by `ALTER TABLE`.
#[tokio::test]
async fn bad_options_are_rejected() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table t (id integer primary key)")
        .await
        .unwrap();
    server
        .simple_query("create table fillfactor_ok (id integer primary key) with (fillfactor = 70)")
        .await
        .unwrap();

    for sql in [
        "create table u (id integer primary key) with (compression = 'lz4')",
        "alter table t set (compression = 'lz4')",
        "alter table t set (fillfactor = 70)",
    ] {
        server
            .simple_query(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected rejection for `{sql}`"));
    }
}

/// Review fix (points 1+2): `run_alter_table_compression` previously never
/// called `record_commit_and_maybe_checkpoint_after_durable_commit`, so its
/// rewrite's (potentially large) FullPageImage bytes were not counted toward
/// the WAL-bytes checkpoint threshold until an unrelated later commit
/// happened to notice them. This proves the ALTER's own WAL activity is now
/// counted: with `checkpoint_every_n_commits` disabled (so only WAL bytes can
/// trip a checkpoint) and a `checkpoint_wal_bytes` threshold sized well below
/// what a many-page rewrite produces (but comfortably above a single small
/// commit record), running the ALTER on a table with real data must itself
/// trigger a checkpoint.
#[tokio::test]
async fn alter_table_rewrite_trips_checkpoint_wal_bytes_threshold() {
    let config = Config {
        checkpoint_every_n_commits: u64::MAX,
        checkpoint_wal_bytes: 8 * 1024,
        ..Config::default()
    };
    let server = TestServer::start_with_config(config).await.unwrap();
    server
        .simple_query("create table logs (id integer primary key, body text)")
        .await
        .unwrap();

    // 512 rows of varied, only moderately compressible text spread across
    // 20+ heap pages, so the rewrite's FullPageImage records comfortably
    // exceed the 8 KiB threshold regardless of the exact zstd ratio achieved.
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

    // Reset the WAL-bytes accounting baseline so only the ALTER's own WAL
    // activity below (dict training record + CatalogChange + Commit +
    // one FullPageImage per rewritten page) is measured against the threshold.
    server.force_checkpoint().await.unwrap();
    let before = server.checkpoint_count();

    server
        .simple_query("alter table logs set (compression = 'zstd')")
        .await
        .unwrap();

    assert!(
        server.checkpoint_count() > before,
        "ALTER TABLE's rewrite must count toward the checkpoint WAL-bytes \
         threshold and trigger a checkpoint here (checkpoint-accounting fix)"
    );
}

/// Review fix (point 3): a table whose catalog `active_dict_id` names a
/// dictionary must fail recovery loudly and immediately if that dictionary's
/// durable file is missing, rather than silently falling back to dict-less
/// decoding at write time and surfacing a confusing decode error much later,
/// on first read of a dict-compressed page.
#[tokio::test]
async fn recovery_fails_fast_on_missing_referenced_dictionary() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query("create table logs (id integer primary key, body text)")
            .await
            .unwrap();
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
        // Trains + persists a dictionary and makes `logs` reference it as its
        // active dict.
        server
            .simple_query("alter table logs set (compression = 'zstd')")
            .await
            .unwrap();
        // Checkpoint so the WAL's `CreateDictionary`/`CatalogChange`
        // records are truncated away: otherwise recovery's redo replay would
        // just re-save the deleted dict file below from the WAL record's
        // embedded bytes, masking the missing-file case this test targets.
        server.force_checkpoint().await.unwrap();
    }

    // Delete the trained dictionary file: the catalog still references it as
    // `logs`'s active dict, but the durable dict file is now gone.
    let dicts_dir = dir.path().join("dicts");
    let mut deleted = 0;
    for entry in std::fs::read_dir(&dicts_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|x| x.to_str()) == Some("dict") {
            std::fs::remove_file(entry.path()).unwrap();
            deleted += 1;
        }
    }
    assert_eq!(deleted, 1, "expected exactly one trained dictionary file");

    let err = TestServer::start_with_data_dir(dir.path())
        .await
        .err()
        .expect("recovery must fail when a catalog-referenced dictionary file is missing");
    assert!(
        err.message.contains("logs"),
        "error should name the affected table: {}",
        err.message
    );
    assert!(
        err.message.to_lowercase().contains("dictionary"),
        "error should mention the missing dictionary: {}",
        err.message
    );
}

fn hidden_toast_chunk_count(server: &TestServer, table: &str) -> usize {
    let catalog = &server.app().components.catalog;
    let base = catalog
        .get_table_by_name(table)
        .unwrap()
        .unwrap_or_else(|| panic!("{table} table exists"));
    let toast_table_id = base.toast_table_id.expect("hidden TOAST relation id");
    let ctx = StatementContext::new(0);
    let mut iter = server
        .app()
        .components
        .storage
        .scan_range(&ctx, toast_table_id, &KeyRange::All)
        .unwrap();
    let mut chunks = 0;
    while iter.next().unwrap().is_some() {
        chunks += 1;
    }
    chunks
}

fn legacy_schema_without_toast() -> TableSchema {
    TableSchema {
        id: 50,
        schema_id: common::PUBLIC_SCHEMA_ID,
        storage_id: 50,
        name: "legacy_docs".to_string(),
        columns: vec![
            ColumnDef {
                id: 0,
                object_id: 1,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: Some(PgType::Int8),
            },
            ColumnDef {
                id: 1,
                object_id: 2,
                name: "body".to_string(),
                data_type: DataType::Text,
                nullable: true,
                max_length: None,
                default: None,
                pg_type: Some(PgType::Text),
            },
        ],
        primary_key: Vec::new(),
        schema_version: common::INITIAL_SCHEMA_VERSION,
        compression: CompressionSetting::None,
        active_dict_id: None,
        toast: ToastOptions::legacy_catalog_default(),
        toast_table_id: None,
        relation_kind: RelationKind::User,
        checks: Vec::new(),
        foreign_keys: Vec::new(),
        next_foreign_key_id: 0,
        next_column_object_id: u32::MAX,
    }
}

#[tokio::test]
async fn alter_toast_options_apply_to_future_writes_and_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let large_body = "large-toast-value-".repeat(350);
    {
        let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
        server
            .simple_query(
                "create table docs (id integer primary key, body text) \
                 with (toast = off, toast_compression = none)",
            )
            .await
            .unwrap();
        server
            .simple_query("insert into docs (id, body) values (1, 'inline-before-alter')")
            .await
            .unwrap();
        assert_eq!(hidden_toast_chunk_count(&server, "docs"), 0);

        server
            .simple_query(
                "alter table docs set \
                 (toast = aggressive, toast_tuple_target = 512, \
                  toast_min_value_size = 128, toast_compression = none)",
            )
            .await
            .unwrap();

        let docs = server
            .app()
            .components
            .catalog
            .get_table_by_name("docs")
            .unwrap()
            .expect("docs table exists");
        assert_eq!(docs.toast.mode, ToastMode::Aggressive);
        assert_eq!(docs.toast.tuple_target, 512);
        assert_eq!(docs.toast.min_value_size, 128);
        assert_eq!(docs.toast.compression, ToastCompression::None);
        assert_eq!(docs.toast.active_dict_id, None);

        server
            .simple_query(&format!(
                "insert into docs (id, body) values (2, '{large_body}')"
            ))
            .await
            .unwrap();
        assert!(
            hidden_toast_chunk_count(&server, "docs") > 0,
            "future writes after TOAST ALTER should use the hidden TOAST relation"
        );
        assert_eq!(
            server
                .simple_query("select body from docs where id = 2")
                .await
                .unwrap()
                .unwrap_rows(),
            vec![vec![Some(large_body.clone())]]
        );
    }

    let server = TestServer::start_with_data_dir(dir.path()).await.unwrap();
    let docs = server
        .app()
        .components
        .catalog
        .get_table_by_name("docs")
        .unwrap()
        .expect("docs table exists after restart");
    assert_eq!(docs.toast.mode, ToastMode::Aggressive);
    assert_eq!(docs.toast.tuple_target, 512);
    assert_eq!(docs.toast.min_value_size, 128);
    assert_eq!(docs.toast.compression, ToastCompression::None);
    assert!(
        docs.toast_table_id.is_some(),
        "hidden TOAST relation should survive WAL replay"
    );
    assert_eq!(
        server
            .simple_query("select id, body from docs order by id")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![
            vec![
                Some("1".to_string()),
                Some("inline-before-alter".to_string())
            ],
            vec![Some("2".to_string()), Some(large_body)],
        ]
    );
}

#[tokio::test]
async fn alter_toast_zstd_dict_trains_value_dictionary_when_corpus_suffices() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query(
            "create table logs (id integer primary key, body text) \
             with (toast = off, toast_compression = none)",
        )
        .await
        .unwrap();
    let body_fill = "alpha-beta-gamma-delta-epsilon-".repeat(35);
    for chunk in 0..8 {
        let values: Vec<String> = (0..8)
            .map(|i| {
                let id = chunk * 8 + i;
                format!("({id}, 'sample-{id}-{body_fill}')")
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

    server
        .simple_query(
            "alter table logs set \
             (toast = aggressive, toast_min_value_size = 128, \
              toast_compression = zstd_dict)",
        )
        .await
        .unwrap();

    let logs = server
        .app()
        .components
        .catalog
        .get_table_by_name("logs")
        .unwrap()
        .expect("logs table exists");
    assert_eq!(logs.toast.compression, ToastCompression::ZstdDict);
    let dict_id = logs
        .toast
        .active_dict_id
        .expect("large repetitive corpus should train a TOAST dictionary");
    assert!(
        server.app().components.compression.has_dictionary(dict_id),
        "trained TOAST dictionary should be registered for future writes"
    );
}

#[tokio::test]
async fn alter_toast_zstd_dict_allows_tiny_corpus_without_dictionary() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table notes (id integer primary key, body text)")
        .await
        .unwrap();
    server
        .simple_query("insert into notes (id, body) values (1, 'short'), (2, 'small')")
        .await
        .unwrap();

    server
        .simple_query("alter table notes set (toast_compression = zstd_dict)")
        .await
        .unwrap();

    let notes = server
        .app()
        .components
        .catalog
        .get_table_by_name("notes")
        .unwrap()
        .expect("notes table exists");
    assert_eq!(notes.toast.compression, ToastCompression::ZstdDict);
    assert_eq!(notes.toast.active_dict_id, None);
}

#[tokio::test]
async fn alter_toast_options_create_hidden_relation_for_legacy_catalog_table() {
    let server = TestServer::start().await.unwrap();
    let legacy = legacy_schema_without_toast();
    server
        .app()
        .components
        .catalog
        .apply_create_table(legacy.clone())
        .unwrap();
    server
        .app()
        .components
        .storage
        .create_table(&StatementContext::new(0), &legacy)
        .unwrap();
    server
        .simple_query("create index legacy_docs_body on legacy_docs (body)")
        .await
        .unwrap();
    let body_index = server
        .app()
        .components
        .catalog
        .get_index_by_name("legacy_docs_body")
        .unwrap()
        .expect("legacy_docs_body index exists");

    server
        .simple_query("alter table legacy_docs set (toast = off)")
        .await
        .unwrap();

    let legacy_docs = server
        .app()
        .components
        .catalog
        .get_table_by_name("legacy_docs")
        .unwrap()
        .expect("legacy_docs table exists");
    let toast_id = legacy_docs
        .toast_table_id
        .expect("ALTER should allocate hidden TOAST relation for legacy text table");
    let hidden = server
        .app()
        .components
        .catalog
        .get_table(toast_id)
        .unwrap()
        .expect("hidden TOAST relation exists");
    assert_eq!(
        hidden.relation_kind,
        RelationKind::Toast {
            base_table: legacy_docs.id
        }
    );
    assert_eq!(hidden.compression, CompressionSetting::None);
    assert_ne!(
        hidden.storage_id, body_index.storage_id,
        "late TOAST relation must allocate a fresh storage id, not reuse a secondary index generation"
    );
    assert!(
        hidden.storage_id > body_index.storage_id,
        "late TOAST relation should come after the pre-existing secondary index in storage allocation order"
    );
}

#[tokio::test]
async fn alter_rejects_mixed_page_compression_and_toast_options() {
    let server = TestServer::start().await.unwrap();
    server
        .simple_query("create table docs (id integer primary key, body text)")
        .await
        .unwrap();

    let result = server
        .simple_query("alter table docs set (compression = zstd, toast = auto)")
        .await;
    let Err(err) = result else {
        panic!("mixed page compression and TOAST ALTER should be rejected");
    };
    assert!(
        err.message
            .contains("cannot combine page compression and TOAST options"),
        "unexpected error: {}",
        err.message
    );
}
