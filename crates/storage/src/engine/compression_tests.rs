use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use common::{
    CancelReason, ColumnDef, CompressionSetting, DataType, IndexSchema, PageFlushInfo, QueryCancel,
    RelationKind, Row, Snapshot, SqlState, StatementContext, TableSchema, ToastOptions, Value,
};
use compress::CompressionRegistry;
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

use crate::engine::{PageBackedStorageEngine, StorageMode};
use crate::heap::{HeapPageStore, primary_index_file_id, secondary_index_file_id};
use crate::page;
use crate::traits::SchemaOperations;

const TABLE_ID: u32 = 1;
const NOTE_INDEX_ID: u32 = 7;

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
        schema_id: common::PUBLIC_SCHEMA_ID,
        storage_id: TABLE_ID,
        name: "users".to_string(),
        columns: vec![
            ColumnDef {
                id: 0,
                object_id: 1,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            },
            ColumnDef {
                id: 1,
                object_id: 2,
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
        toast: ToastOptions::disabled(),
        toast_table_id: None,
        relation_kind: RelationKind::User,
        schema_version: common::INITIAL_SCHEMA_VERSION,
        next_column_object_id: u32::MAX,
    }
}

/// A non-unique secondary index on the `note` column, mirroring
/// `vacuum_tests::name_index` — gives `rewrite_table_pages` a live
/// secondary-index file to exercise (it otherwise only ever sees the heap
/// and primary-key files).
fn note_index() -> IndexSchema {
    IndexSchema {
        id: NOTE_INDEX_ID,
        schema_id: common::PUBLIC_SCHEMA_ID,
        storage_id: 101,
        table: TABLE_ID,
        name: "users_note".to_string(),
        columns: vec![1],
        unique: false,
        constraint: None,
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
    crate::with_test_tuple_locks(StatementContext::with_snapshot(
        txn_id,
        Arc::new(Snapshot {
            xmin: 1,
            xmax: txn_id + 1,
            xip: vec![],
        }),
    ))
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
            .engine
            .create_index(&ctx(101), &note_index(), 0)
            .unwrap();
        commit(&fixture.wal, 101);
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
    fixture.insert_rows(102, 20);

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

/// Normalize a `FullPageImageCompressed` record to the raw `FullPageImage`
/// shape `apply_physical_redo` accepts (decompressing via a fresh dict-less
/// registry — every FPI in this fixture is dict-less, since no test here
/// registers a dictionary). Every other record kind passes through
/// unchanged. Mirrors `vacuum_tests::resolve_to_raw_fpi`.
fn resolve_to_raw_fpi(kind: WalRecordKind) -> WalRecordKind {
    match kind {
        WalRecordKind::FullPageImageCompressed {
            file_id,
            page_num,
            codec,
            dict_id,
            payload,
        } => {
            let registry = CompressionRegistry::new();
            let image = registry
                .decompress_fpi(codec, dict_id, &payload, buffer::PAGE_SIZE)
                .unwrap();
            WalRecordKind::FullPageImage {
                file_id,
                page_num,
                image,
            }
        }
        other => other,
    }
}

#[test]
fn rewrite_table_pages_logs_fpi_and_repairs_torn_pages() {
    let fixture = Fixture::new();
    fixture.insert_rows(102, 20);
    // Checkpoint-style flush, then mark everything clean so the following
    // assertions are about exactly what `rewrite_table_pages` itself does.
    fixture.buffer.flush_dirty_pages().unwrap();
    fixture.buffer.mark_all_clean().unwrap();

    let pk_file_id = primary_index_file_id(TABLE_ID);
    let secondary_file_id = secondary_index_file_id(note_index().storage_id);
    let files = [TABLE_ID, pk_file_id, secondary_file_id];

    // Snapshot every initialized page's image before the rewrite.
    let mut before: HashMap<(u32, u32), [u8; buffer::PAGE_SIZE]> = HashMap::new();
    for &file_id in &files {
        let page_count = fixture.buffer.page_count(file_id).unwrap();
        for page_num in 0..page_count {
            if fixture.buffer.is_page_abandoned(file_id, page_num) {
                continue;
            }
            let guard = fixture.buffer.read_page(file_id, page_num).unwrap();
            if page::is_any_page_initialized(guard.data()) {
                before.insert((file_id, page_num), *guard.data());
            }
        }
    }
    assert!(
        before.len() >= 3,
        "heap page + PK index root + secondary index root"
    );
    assert!(
        before
            .keys()
            .any(|&(file_id, _)| file_id == secondary_file_id),
        "the secondary-index file must have at least one initialized page \
         for this test to exercise rewrite_table_pages' secondary-index branch"
    );

    let wal_len_before = fixture.wal.replay_from(0).unwrap().count();

    let rewrite = fixture
        .engine
        .rewrite_table_pages(&users_schema_zstd())
        .unwrap();
    assert_eq!(
        rewrite.pages_touched,
        before.len(),
        "one page touched per initialized page"
    );
    let mut expected_files = before
        .keys()
        .map(|(file_id, _)| *file_id)
        .collect::<Vec<_>>();
    expected_files.sort_unstable();
    expected_files.dedup();
    assert_eq!(rewrite.file_ids, expected_files);

    // The rewrite must append exactly one FullPageImage[Compressed] per
    // touched page and nothing else.
    let mut fpi_by_page: HashMap<(u32, u32), (u64, WalRecordKind)> = HashMap::new();
    let mut new_record_count = 0usize;
    for (i, record) in fixture.wal.replay_from(0).unwrap().enumerate() {
        if i < wal_len_before {
            continue;
        }
        let record = record.unwrap();
        new_record_count += 1;
        match &record.kind {
            WalRecordKind::FullPageImage {
                file_id, page_num, ..
            }
            | WalRecordKind::FullPageImageCompressed {
                file_id, page_num, ..
            } => {
                let key = (*file_id, *page_num);
                assert!(
                    fpi_by_page
                        .insert(key, (record.lsn, record.kind.clone()))
                        .is_none(),
                    "duplicate FPI logged for {key:?}"
                );
            }
            other => panic!("rewrite_table_pages must log only FPIs, found {other:?}"),
        }
    }
    assert_eq!(
        new_record_count,
        before.len(),
        "rewrite_table_pages must append exactly one WAL record per touched page"
    );
    assert_eq!(
        fpi_by_page.keys().copied().collect::<HashSet<_>>(),
        before.keys().copied().collect::<HashSet<_>>(),
        "an FPI must be logged for exactly the touched pages"
    );

    // Logical content is unchanged; only the PageLSN (and its checksum)
    // advances. For every touched page, also prove a torn write during the
    // page flush is repaired: replaying the page's FPI onto a zeroed frame
    // (what recovery sees for a torn page) reconstructs the post-rewrite
    // image byte-for-byte.
    for (&(file_id, page_num), pre_image) in &before {
        let post_image: [u8; buffer::PAGE_SIZE] = {
            let guard = fixture.buffer.read_page(file_id, page_num).unwrap();
            *guard.data()
        };
        assert_ne!(
            page::page_lsn(&post_image),
            page::page_lsn(pre_image),
            "{file_id}/{page_num}: PageLSN must advance"
        );
        let mut pre_norm = *pre_image;
        let mut post_norm = post_image;
        page::set_page_lsn(&mut pre_norm, 0);
        page::set_page_lsn(&mut post_norm, 0);
        assert_eq!(
            pre_norm, post_norm,
            "{file_id}/{page_num}: rewrite must change nothing but the PageLSN"
        );

        let (lsn, kind) = fpi_by_page.get(&(file_id, page_num)).unwrap();
        let raw_kind = resolve_to_raw_fpi(kind.clone());
        let mut recovered = [0u8; buffer::PAGE_SIZE];
        crate::redo::apply_physical_redo(&mut recovered, *lsn, &raw_kind).unwrap();
        assert_eq!(
            recovered, post_image,
            "{file_id}/{page_num}: FPI redo onto a zeroed (torn-write) frame must \
             reconstruct the rewritten page byte-for-byte"
        );
    }

    // Confirms only that the rewritten pages flush cleanly under this
    // fixture's `AlwaysFlush` policy, which ignores PageLSN — it does NOT
    // exercise write-ahead ordering. The real guarantee (the rewrite's FPIs
    // must be durable before `flush_dirty_pages` runs, since that call does
    // not gate on PageLSN itself) is enforced by the caller's `wal.flush()`
    // in `run_alter_table_compression`, not by this call.
    fixture.buffer.flush_dirty_pages().unwrap();
}

#[test]
fn sample_heap_pages_returns_page_images_capped() {
    let fixture = Fixture::new();
    // Enough rows to span several heap pages.
    fixture.insert_rows(102, 200);

    let samples = fixture
        .engine
        .sample_heap_pages(&users_schema_zstd(), 4)
        .unwrap();
    assert!(samples.len() <= 4);
    assert!(samples.iter().all(|s| s.len() == buffer::PAGE_SIZE));
}

#[test]
fn sample_heap_pages_cancelable_observes_statement_timeout() {
    let fixture = Fixture::new();
    fixture.insert_rows(102, 10);
    let cancel = QueryCancel::new();
    cancel.request(CancelReason::StatementTimeout);

    let err = fixture
        .engine
        .sample_heap_pages_cancelable(&users_schema_zstd(), 4, &cancel)
        .unwrap_err();
    assert_eq!(err.code, SqlState::QueryCanceled);
}
