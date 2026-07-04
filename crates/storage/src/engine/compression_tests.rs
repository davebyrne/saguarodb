use std::sync::Arc;

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use common::{
    ColumnDef, CompressionSetting, DataType, PageFlushInfo, Row, Snapshot, StatementContext,
    TableSchema, Value,
};
use compress::CompressionRegistry;
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

use crate::engine::{PageBackedStorageEngine, StorageMode};
use crate::heap::HeapPageStore;
use crate::traits::{SchemaOperations, StorageEngine};

const TABLE_ID: u32 = 1;

struct AlwaysFlush;
impl common::FlushPolicy for AlwaysFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        true
    }
}

/// A Zstd-compressed users table: mirrors `vacuum_tests::users_schema()` with
/// the two compression fields set.
fn users_schema_zstd() -> TableSchema {
    TableSchema {
        id: TABLE_ID,
        name: "users".to_string(),
        columns: vec![
            ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            },
            ColumnDef {
                id: 1,
                name: "note".to_string(),
                data_type: DataType::Text,
                nullable: true,
                max_length: None,
                default: None,
                pg_type: None,
            },
        ],
        primary_key: vec![0],
        compression: CompressionSetting::Zstd,
        active_dict_id: None,
    }
}

/// A row whose ~200-byte repetitive text column compresses far below its
/// on-page size, so every heap page it lands on compresses well under
/// `buffer::PAGE_SIZE`.
fn wide_row(i: i64) -> Row {
    Row {
        values: vec![Value::Integer(i), Value::Text("x".repeat(200))],
    }
}

fn ctx(txn_id: u64) -> StatementContext {
    StatementContext::with_snapshot(
        txn_id,
        Arc::new(Snapshot {
            xmin: 1,
            xmax: txn_id + 1,
            xip: vec![],
        }),
    )
}

fn commit(wal: &FileWalManager, txn_id: u64) {
    wal.append(WalRecord {
        lsn: 0,
        txn_id,
        kind: WalRecordKind::Commit,
    })
    .unwrap();
    wal.flush().unwrap();
}

struct Fixture {
    engine: PageBackedStorageEngine,
    wal: Arc<FileWalManager>,
    buffer: Arc<MemoryBufferPool>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(CompressionRegistry::new());
        let store: Arc<dyn PageStore> = Arc::new(
            HeapPageStore::open_with_compression(dir.path().join("data"), registry.clone())
                .unwrap(),
        );
        let buffer = Arc::new(MemoryBufferPool::new(256, Box::new(AlwaysFlush), store));
        buffer.enable_stealing();
        let wal = Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let engine = PageBackedStorageEngine::open_with_compression(
            buffer.clone(),
            wal.clone(),
            StorageMode::Normal,
            registry,
        )
        .unwrap();
        let fixture = Self {
            engine,
            wal,
            buffer,
            _dir: dir,
        };
        fixture
            .engine
            .create_table(&ctx(100), &users_schema_zstd())
            .unwrap();
        commit(&fixture.wal, 100);
        fixture
    }

    /// Insert `count` committed rows starting at txn id `start_txn`.
    fn insert_rows(&self, start_txn: u64, count: i64) {
        for i in 0..count {
            let txn_id = start_txn + i as u64;
            self.engine
                .insert(&ctx(txn_id), TABLE_ID, wide_row(i))
                .unwrap();
            commit(&self.wal, txn_id);
        }
    }
}

#[test]
fn dml_on_zstd_table_logs_compressed_fpis() {
    let fixture = Fixture::new();
    fixture.insert_rows(101, 20);

    // Every FPI for this table's files must be the compressed variant (the
    // repetitive rows compress far below 8 KiB): B-tree node images AND the
    // heap first-touch image.
    let mut compressed = 0;
    let mut raw = 0;
    for record in fixture.wal.replay_from(0).unwrap() {
        match record.unwrap().kind {
            WalRecordKind::FullPageImageCompressed { .. } => compressed += 1,
            WalRecordKind::FullPageImage { .. } => raw += 1,
            _ => {}
        }
    }
    assert!(compressed > 0, "expected compressed FPIs, found none");
    assert_eq!(raw, 0, "raw FPIs slipped through on compressible pages");
}

#[test]
fn rewrite_table_pages_dirties_every_initialized_page() {
    let fixture = Fixture::new();
    fixture.insert_rows(101, 20);
    // Checkpoint-style flush, then mark everything clean so the following
    // assertion is about pages `rewrite_table_pages` newly dirtied.
    fixture.buffer.flush_dirty_pages().unwrap();
    fixture.buffer.mark_all_clean().unwrap();

    // Task 12's ALTER rewrite path depends on `rewrite_table_pages` being a
    // pure "mark dirty" no-op: it must not append WAL records, and it must
    // not change any page's logical bytes (only the caller's own follow-up
    // flush/WAL emission does that). Capture both witnesses before the call.
    let heap_before: [u8; buffer::PAGE_SIZE] = {
        let guard = fixture.buffer.read_page(TABLE_ID, 0).unwrap();
        *guard.data()
    };
    let wal_len_before = fixture.wal.replay_from(0).unwrap().count();

    let touched = fixture
        .engine
        .rewrite_table_pages(&users_schema_zstd())
        .unwrap();
    assert!(touched >= 2, "heap page + at least the index metapage/root");

    assert_eq!(
        fixture.wal.replay_from(0).unwrap().count(),
        wal_len_before,
        "rewrite_table_pages must append no WAL records of its own"
    );
    let heap_after: [u8; buffer::PAGE_SIZE] = {
        let guard = fixture.buffer.read_page(TABLE_ID, 0).unwrap();
        *guard.data()
    };
    assert_eq!(
        heap_before, heap_after,
        "rewrite_table_pages must dirty pages without mutating their bytes"
    );

    // Pages are dirty again: flush succeeds and the store re-encodes.
    fixture.buffer.flush_dirty_pages().unwrap();
}

#[test]
fn sample_heap_pages_returns_page_images_capped() {
    let fixture = Fixture::new();
    // Enough rows to span several heap pages.
    fixture.insert_rows(101, 200);

    let samples = fixture
        .engine
        .sample_heap_pages(&users_schema_zstd(), 4)
        .unwrap();
    assert!(samples.len() <= 4);
    assert!(samples.iter().all(|s| s.len() == buffer::PAGE_SIZE));
}
