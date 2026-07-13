use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use common::{
    ColumnDef, CompressionSetting, DataType, FileId, IndexConstraintKind, IndexSchema, Key,
    KeyRange, Lsn, PageFlushInfo, RelationKind, Row, Snapshot, SqlState, StatementContext,
    TableSchema, ToastCompression, ToastOptions, TruncateCatalogUpdate, TruncateTablePlan, TxnId,
    TxnStatus, TxnStatusView, Value, toast_schema,
};
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

use super::{PageBackedStorageEngine, StorageMode};
use crate::HeapPageStore;
use crate::heap::{heap_file_id, primary_index_file_id, secondary_index_file_id};
use crate::traits::{RelationSnapshot, SchemaOperations, StorageEngine};

const TABLE_ID: u32 = 1;
const TOAST_TABLE_ID: u32 = 2;
const NAME_INDEX_ID: u32 = 3;
const NOTE_INDEX_ID: u32 = 4;
const OTHER_TABLE_ID: u32 = 5;

const BASE_STORAGE_ID: FileId = 10;
const TOAST_STORAGE_ID: FileId = 20;
const NAME_INDEX_STORAGE_ID: FileId = 30;
const NOTE_INDEX_STORAGE_ID: FileId = 40;
const OTHER_STORAGE_ID: FileId = 50;

const NEW_BASE_STORAGE_ID: FileId = 110;
const NEW_TOAST_STORAGE_ID: FileId = 120;
const NEW_NAME_INDEX_STORAGE_ID: FileId = 130;
const NEW_OTHER_STORAGE_ID: FileId = 140;

struct AlwaysFlush;

impl common::FlushPolicy for AlwaysFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        true
    }
}

#[derive(Default)]
struct BlockingFpiWal {
    next_lsn: AtomicU64,
    flushed_lsn: AtomicU64,
    block_file: AtomicU32,
    gate: Mutex<Option<FpiGate>>,
}

struct FpiGate {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl BlockingFpiWal {
    fn block_next_fpi_for(&self, file_id: FileId) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        self.block_file.store(file_id, Ordering::Release);
        *self.gate.lock().unwrap() = Some(FpiGate {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }
}

impl WalManager for BlockingFpiWal {
    fn append(&self, record: WalRecord) -> common::Result<Lsn> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::AcqRel) + 1;
        let fpi_file = match &record.kind {
            WalRecordKind::FullPageImage { file_id, .. }
            | WalRecordKind::FullPageImageCompressed { file_id, .. } => Some(*file_id),
            _ => None,
        };
        if fpi_file == Some(self.block_file.load(Ordering::Acquire))
            && let Some(gate) = self.gate.lock().unwrap().take()
        {
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
            self.block_file.store(0, Ordering::Release);
        }
        Ok(lsn)
    }

    fn flush(&self) -> common::Result<Lsn> {
        let lsn = self.next_lsn.load(Ordering::Acquire);
        self.flushed_lsn.store(lsn, Ordering::Release);
        Ok(lsn)
    }

    fn replay_from(
        &self,
        _lsn: Lsn,
    ) -> common::Result<Box<dyn Iterator<Item = common::Result<WalRecord>>>> {
        Ok(Box::new(std::iter::empty()))
    }

    fn truncate_before(&self, _lsn: Lsn) -> common::Result<()> {
        Ok(())
    }

    fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn.load(Ordering::Acquire)
    }

    fn bytes_after(&self, _lsn: Lsn) -> common::Result<u64> {
        Ok(0)
    }

    fn persist_clog(&self, _clog_lsn: Lsn) -> common::Result<()> {
        Ok(())
    }

    fn set_vacuum_floor(&self, _boundary: TxnId) -> common::Result<()> {
        Ok(())
    }

    fn establish_recovery_committed_floor(&self, _allocation_boundary: u64) -> common::Result<()> {
        Ok(())
    }

    fn resolve_in_flight_as_aborted(
        &self,
        _writer_xids: &std::collections::HashSet<u64>,
    ) -> common::Result<()> {
        Ok(())
    }
}

impl TxnStatusView for BlockingFpiWal {
    fn status(&self, _txn_id: TxnId) -> TxnStatus {
        TxnStatus::Committed
    }
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
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("data")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(256, Box::new(AlwaysFlush), store));
        buffer.enable_stealing();
        let wal = Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let engine =
            PageBackedStorageEngine::open(buffer.clone(), wal.clone(), StorageMode::Normal)
                .unwrap();

        let fixture = Self {
            engine,
            wal,
            buffer,
            _dir: dir,
        };
        fixture
            .engine
            .create_table(&ctx(100), &users_schema(BASE_STORAGE_ID))
            .unwrap();
        commit(&fixture.wal, 100);
        fixture
            .engine
            .create_table(&ctx(101), &users_toast_schema(TOAST_STORAGE_ID))
            .unwrap();
        commit(&fixture.wal, 101);
        fixture
            .engine
            .create_index(&ctx(102), &name_index(NAME_INDEX_STORAGE_ID), 0)
            .unwrap();
        commit(&fixture.wal, 102);
        fixture
    }

    fn insert_committed(&self, txn_id: u64, row: Row) {
        self.engine.insert(&ctx(txn_id), TABLE_ID, row).unwrap();
        commit(&self.wal, txn_id);
    }

    fn capture_relations(&self) -> Arc<dyn RelationSnapshot> {
        self.engine.capture_relation_snapshot().unwrap()
    }

    fn scan_rows(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot) -> Vec<Row> {
        let mut rows = Vec::new();
        let mut iter = <PageBackedStorageEngine as StorageEngine>::scan(
            &self.engine,
            ctx,
            relations,
            TABLE_ID,
        )
        .unwrap();
        while let Some(stored) = iter.next().unwrap() {
            rows.push(stored.row);
        }
        rows
    }

    fn index_rows(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        name: &str,
    ) -> Vec<Row> {
        let mut rows = Vec::new();
        let range = KeyRange::Exact(Key(vec![Value::Text(name.to_string())]));
        let mut iter = <PageBackedStorageEngine as StorageEngine>::index_scan(
            &self.engine,
            ctx,
            relations,
            TABLE_ID,
            NAME_INDEX_ID,
            &range,
        )
        .unwrap();
        while let Some(stored) = iter.next().unwrap() {
            rows.push(stored.row);
        }
        rows
    }

    fn truncate_committed(&self, txn_id: u64) {
        let plan = truncate_plan();
        let update = truncate_update();
        self.engine
            .prepare_truncate_table(&ctx(txn_id), &plan, &update)
            .unwrap();
        commit(&self.wal, txn_id);
        self.engine.publish_truncate_table(update).unwrap();
        <PageBackedStorageEngine as StorageEngine>::commit_txn(&self.engine, txn_id).unwrap();
    }

    fn file_ids(&self) -> BTreeSet<FileId> {
        self.buffer.list_file_ids().unwrap().into_iter().collect()
    }

    fn flush_dirty_pages(&self) {
        self.buffer.flush_dirty_pages().unwrap();
    }

    fn records_for_txn(&self, txn_id: u64) -> Vec<WalRecordKind> {
        self.wal
            .replay_from(0)
            .unwrap()
            .map(|record| record.unwrap())
            .filter(|record| record.txn_id == txn_id)
            .map(|record| record.kind)
            .collect()
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

fn users_schema(storage_id: FileId) -> TableSchema {
    let mut toast = ToastOptions::default_new_table();
    toast.tuple_target = ToastOptions::MIN_TOAST_TUPLE_TARGET;
    toast.min_value_size = ToastOptions::MIN_TOAST_MIN_VALUE_SIZE;
    toast.compression = ToastCompression::None;

    TableSchema {
        id: TABLE_ID,
        schema_id: common::PUBLIC_SCHEMA_ID,
        storage_id,
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
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            },
            ColumnDef {
                id: 2,
                name: "note".to_string(),
                data_type: DataType::Text,
                nullable: true,
                max_length: None,
                default: None,
                pg_type: None,
            },
        ],
        primary_key: vec![0],
        compression: CompressionSetting::None,
        active_dict_id: None,
        toast,
        schema_version: common::INITIAL_SCHEMA_VERSION,
        checks: Vec::new(),
        toast_table_id: Some(TOAST_TABLE_ID),
        relation_kind: RelationKind::User,
    }
}

fn users_toast_schema(storage_id: FileId) -> TableSchema {
    let mut schema = toast_schema(&users_schema(BASE_STORAGE_ID), TOAST_TABLE_ID);
    schema.storage_id = storage_id;
    schema
}

fn name_index(storage_id: FileId) -> IndexSchema {
    IndexSchema {
        id: NAME_INDEX_ID,
        schema_id: common::PUBLIC_SCHEMA_ID,
        storage_id,
        table: TABLE_ID,
        name: "users_name_key".to_string(),
        columns: vec![1],
        unique: true,
        constraint: IndexConstraintKind::Unique,
    }
}

fn note_index(storage_id: FileId) -> IndexSchema {
    IndexSchema {
        id: NOTE_INDEX_ID,
        schema_id: common::PUBLIC_SCHEMA_ID,
        storage_id,
        table: TABLE_ID,
        name: "users_note_idx".to_string(),
        columns: vec![2],
        unique: false,
        constraint: IndexConstraintKind::None,
    }
}

fn truncate_plan() -> TruncateTablePlan {
    TruncateTablePlan {
        table_id: TABLE_ID,
        new_table_storage_id: NEW_BASE_STORAGE_ID,
        new_toast_storage_id: Some((TOAST_TABLE_ID, NEW_TOAST_STORAGE_ID)),
        new_index_storage_ids: vec![(NAME_INDEX_ID, NEW_NAME_INDEX_STORAGE_ID)],
    }
}

fn truncate_update() -> TruncateCatalogUpdate {
    TruncateCatalogUpdate {
        table: users_schema(NEW_BASE_STORAGE_ID),
        toast_table: Some(users_toast_schema(NEW_TOAST_STORAGE_ID)),
        indexes: vec![name_index(NEW_NAME_INDEX_STORAGE_ID)],
    }
}

fn other_schema(storage_id: FileId) -> TableSchema {
    let mut schema = users_schema(storage_id);
    schema.id = OTHER_TABLE_ID;
    schema.name = "other".to_string();
    schema.toast = ToastOptions::legacy_catalog_default();
    schema.toast_table_id = None;
    schema
}

fn other_truncate_update(storage_id: FileId) -> TruncateCatalogUpdate {
    TruncateCatalogUpdate {
        table: other_schema(storage_id),
        toast_table: None,
        indexes: Vec::new(),
    }
}

fn user_row(id: i64, name: &str, note: &str) -> Row {
    Row {
        values: vec![
            Value::Integer(id),
            Value::Text(name.to_string()),
            Value::Text(note.to_string()),
        ],
    }
}

fn large_note() -> String {
    "large external toast value ".repeat(250)
}

fn old_generation_files() -> [FileId; 5] {
    [
        heap_file_id(BASE_STORAGE_ID),
        primary_index_file_id(BASE_STORAGE_ID),
        heap_file_id(TOAST_STORAGE_ID),
        primary_index_file_id(TOAST_STORAGE_ID),
        secondary_index_file_id(NAME_INDEX_STORAGE_ID),
    ]
}

#[test]
fn prepare_truncate_logs_logical_record_before_physical_pages() {
    let fixture = Fixture::new();
    let plan = truncate_plan();
    let update = truncate_update();

    fixture
        .engine
        .prepare_truncate_table(&ctx(400), &plan, &update)
        .unwrap();

    let records = fixture.records_for_txn(400);
    assert!(
        matches!(
            records.first(),
            Some(WalRecordKind::TruncateTable {
                table_id: TABLE_ID,
                new_table_storage_id: NEW_BASE_STORAGE_ID,
                new_toast_storage_id: Some((TOAST_TABLE_ID, NEW_TOAST_STORAGE_ID)),
                ..
            })
        ),
        "first truncate WAL record was {records:?}"
    );
    assert!(
        records.iter().skip(1).any(|kind| matches!(
            kind,
            WalRecordKind::FullPageImage { .. } | WalRecordKind::FullPageImageCompressed { .. }
        )),
        "truncate prepare did not initialize any B-tree pages: {records:?}"
    );
}

#[test]
fn schema_rewrite_logs_logical_record_before_physical_pages() {
    let fixture = Fixture::new();
    let mut schema = users_schema(NEW_BASE_STORAGE_ID);
    schema.schema_version += 1;
    let index = name_index(NEW_NAME_INDEX_STORAGE_ID);

    fixture
        .engine
        .update_table_schema(&ctx(401), &schema, std::slice::from_ref(&index))
        .unwrap();

    let records = fixture.records_for_txn(401);
    assert!(
        matches!(
            records.first(),
            Some(WalRecordKind::UpdateTableSchema {
                schema: logged_schema,
                indexes,
            }) if logged_schema.storage_id == NEW_BASE_STORAGE_ID
                && indexes.len() == 1
                && indexes[0].storage_id == NEW_NAME_INDEX_STORAGE_ID
        ),
        "first schema rewrite WAL record was {records:?}"
    );
    assert!(
        records.iter().skip(1).any(|kind| matches!(
            kind,
            WalRecordKind::FullPageImage { .. } | WalRecordKind::FullPageImageCompressed { .. }
        )),
        "schema rewrite did not initialize any B-tree pages: {records:?}"
    );
}

#[test]
fn old_relation_snapshot_reads_old_generation_and_new_snapshot_is_empty() {
    let fixture = Fixture::new();
    let note = large_note();
    let old_rows = vec![
        user_row(1, "alice", &note),
        user_row(2, "bob", "small inline note"),
    ];
    fixture.insert_committed(200, old_rows[0].clone());
    fixture.insert_committed(201, old_rows[1].clone());

    let old_ctx = ctx(300);
    let old_relations = fixture.capture_relations();
    fixture.truncate_committed(400);
    let new_relations = fixture.capture_relations();

    assert_eq!(
        fixture.scan_rows(&old_ctx, old_relations.as_ref()),
        old_rows
    );
    assert!(
        fixture
            .scan_rows(&ctx(301), new_relations.as_ref())
            .is_empty()
    );
}

#[test]
fn old_secondary_snapshot_detoasts_after_truncate_and_unique_reinsert_succeeds() {
    let fixture = Fixture::new();
    let note = large_note();
    let old_row = user_row(1, "alice", &note);
    fixture.insert_committed(200, old_row.clone());

    let old_ctx = ctx(300);
    let old_relations = fixture.capture_relations();
    fixture.truncate_committed(400);
    let new_relations = fixture.capture_relations();

    assert_eq!(
        fixture.index_rows(&old_ctx, old_relations.as_ref(), "alice"),
        vec![old_row.clone()]
    );
    assert!(
        fixture
            .index_rows(&ctx(301), new_relations.as_ref(), "alice")
            .is_empty()
    );

    let replacement = user_row(1, "alice", "new generation row");
    fixture.insert_committed(500, replacement.clone());
    let latest_relations = fixture.capture_relations();
    assert_eq!(
        fixture.index_rows(&ctx(501), latest_relations.as_ref(), "alice"),
        vec![replacement]
    );
}

#[test]
fn failed_publish_truncate_does_not_partially_swap_generations() {
    let fixture = Fixture::new();
    let row = user_row(1, "alice", "still live");
    fixture.insert_committed(200, row.clone());

    let mut malformed = truncate_update();
    malformed.toast_table = None;
    let err = fixture
        .engine
        .publish_truncate_table(malformed)
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::InternalError);

    let current_relations = fixture.capture_relations();
    assert_eq!(
        fixture.scan_rows(&ctx(300), current_relations.as_ref()),
        vec![row.clone()]
    );
    assert_eq!(
        fixture.index_rows(&ctx(301), current_relations.as_ref(), "alice"),
        vec![row]
    );
}

#[test]
fn batch_publish_rejects_collisions_without_swapping_any_generation() {
    let fixture = Fixture::new();
    fixture
        .engine
        .create_table(&ctx(150), &other_schema(OTHER_STORAGE_ID))
        .unwrap();
    commit(&fixture.wal, 150);
    let mut colliding = other_truncate_update(NEW_OTHER_STORAGE_ID);
    colliding.table.storage_id = NEW_BASE_STORAGE_ID;

    let err = fixture
        .engine
        .publish_truncate_tables(vec![truncate_update(), colliding])
        .unwrap_err();
    assert_eq!(err.code, SqlState::InternalError);

    let state = fixture.engine.lock_state().unwrap();
    assert_eq!(
        state.tables.get(&TABLE_ID).unwrap().schema.storage_id,
        BASE_STORAGE_ID
    );
    assert_eq!(
        state.tables.get(&OTHER_TABLE_ID).unwrap().schema.storage_id,
        OTHER_STORAGE_ID
    );
    drop(state);

    let update = truncate_update();
    let err = fixture
        .engine
        .publish_truncate_tables(vec![update.clone(), update])
        .unwrap_err();
    assert_eq!(err.code, SqlState::InternalError);
    assert_eq!(
        fixture
            .engine
            .lock_state()
            .unwrap()
            .tables
            .get(&TABLE_ID)
            .unwrap()
            .schema
            .storage_id,
        BASE_STORAGE_ID
    );
}

#[test]
fn create_index_is_not_visible_until_physical_build_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn PageStore> = Arc::new(HeapPageStore::open(dir.path().join("data")).unwrap());
    let buffer = Arc::new(MemoryBufferPool::new(256, Box::new(AlwaysFlush), store));
    buffer.enable_stealing();
    let wal = Arc::new(BlockingFpiWal::default());
    let engine =
        Arc::new(PageBackedStorageEngine::open(buffer, wal.clone(), StorageMode::Normal).unwrap());
    let mut schema = users_schema(BASE_STORAGE_ID);
    schema.toast = ToastOptions::legacy_catalog_default();
    schema.toast_table_id = None;
    engine.create_table(&ctx(100), &schema).unwrap();
    engine
        .insert(&ctx(101), TABLE_ID, user_row(1, "alice", "small note"))
        .unwrap();

    let index = name_index(NAME_INDEX_STORAGE_ID);
    let blocked_file = secondary_index_file_id(index.storage_id);
    let (entered_build, release_build) = wal.block_next_fpi_for(blocked_file);
    let build_engine = engine.clone();
    let build = std::thread::spawn(move || build_engine.create_index(&ctx(102), &index, 0));

    entered_build
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("CREATE INDEX did not reach secondary B-tree initialization");

    let relations = engine.capture_relation_snapshot().unwrap();
    let err = match <PageBackedStorageEngine as StorageEngine>::index_scan(
        &engine,
        &ctx(103),
        relations.as_ref(),
        TABLE_ID,
        NAME_INDEX_ID,
        &KeyRange::Exact(Key(vec![Value::Text("alice".to_string())])),
    ) {
        Ok(_) => panic!("in-progress index unexpectedly became visible"),
        Err(err) => err,
    };
    assert_eq!(
        err.code,
        common::SqlState::UndefinedTable,
        "in-progress index should be unavailable to relation snapshots: {err:?}"
    );

    release_build.send(()).unwrap();
    build.join().unwrap().unwrap();

    let relations = engine.capture_relation_snapshot().unwrap();
    let mut rows = Vec::new();
    let mut iter = <PageBackedStorageEngine as StorageEngine>::index_scan(
        &engine,
        &ctx(104),
        relations.as_ref(),
        TABLE_ID,
        NAME_INDEX_ID,
        &KeyRange::Exact(Key(vec![Value::Text("alice".to_string())])),
    )
    .unwrap();
    while let Some(stored) = iter.next().unwrap() {
        rows.push(stored.row);
    }
    assert_eq!(rows, vec![user_row(1, "alice", "small note")]);
}

#[test]
fn published_truncate_files_are_not_removed_by_later_rollback_cleanup() {
    let fixture = Fixture::new();
    let old_row = user_row(1, "alice", "old generation");
    fixture.insert_committed(200, old_row);

    let txn_id = 400;
    let plan = truncate_plan();
    let update = truncate_update();
    fixture
        .engine
        .prepare_truncate_table(&ctx(txn_id), &plan, &update)
        .unwrap();
    fixture
        .engine
        .publish_truncate_table(update.clone())
        .unwrap();

    <PageBackedStorageEngine as StorageEngine>::rollback_txn(&fixture.engine, txn_id).unwrap();
    fixture.flush_dirty_pages();

    let files = fixture.file_ids();
    for file_id in truncate_created_files_for_test(&update) {
        assert!(
            files.contains(&file_id),
            "published truncate file {file_id} was removed by rollback cleanup"
        );
    }
    let new_relations = fixture.capture_relations();
    assert!(
        fixture
            .scan_rows(&ctx(401), new_relations.as_ref())
            .is_empty(),
        "published empty generation must remain readable after rollback cleanup"
    );
    let replacement = user_row(2, "bob", "new generation");
    fixture.insert_committed(401, replacement.clone());
    let latest_relations = fixture.capture_relations();
    assert_eq!(
        fixture.scan_rows(&ctx(402), latest_relations.as_ref()),
        vec![replacement],
        "published generation must remain writable after rollback cleanup"
    );
}

#[test]
fn transactional_truncate_publish_restores_original_on_rollback() {
    let fixture = Fixture::new();
    let old_row = user_row(1, "alice", "old generation");
    fixture.insert_committed(200, old_row.clone());

    let txn_id = 400;
    let plan = truncate_plan();
    let update = truncate_update();
    fixture
        .engine
        .prepare_truncate_table(&ctx(txn_id), &plan, &update)
        .unwrap();
    fixture
        .engine
        .publish_truncate_tables_transactional(txn_id, vec![update])
        .unwrap();
    assert!(
        fixture
            .scan_rows(&ctx(txn_id), fixture.capture_relations().as_ref())
            .is_empty()
    );

    <PageBackedStorageEngine as StorageEngine>::rollback_txn(&fixture.engine, txn_id).unwrap();
    assert_eq!(
        fixture.scan_rows(&ctx(401), fixture.capture_relations().as_ref()),
        vec![old_row]
    );
}

#[test]
fn rollback_to_savepoint_restores_pre_truncate_generation() {
    let fixture = Fixture::new();
    let old_row = user_row(1, "alice", "old generation");
    fixture.insert_committed(200, old_row.clone());

    let txn_id = 400;
    let savepoint = fixture.engine.savepoint(txn_id).unwrap();
    let plan = truncate_plan();
    let update = truncate_update();
    fixture
        .engine
        .prepare_truncate_table(&ctx(txn_id), &plan, &update)
        .unwrap();
    fixture
        .engine
        .publish_truncate_tables_transactional(txn_id, vec![update])
        .unwrap();
    fixture
        .engine
        .rollback_to_savepoint(txn_id, &savepoint)
        .unwrap();

    assert_eq!(
        fixture.scan_rows(&ctx(txn_id), fixture.capture_relations().as_ref()),
        vec![old_row]
    );
    assert!(fixture.engine.try_cleanup_retired_generations().unwrap() >= 1);
}

#[test]
fn table_handle_fallback_pins_current_generation() {
    let fixture = Fixture::new();
    let mut old_relations = fixture
        .engine
        .capture_pagebacked_relation_snapshot()
        .unwrap();
    old_relations.tables.remove(&TABLE_ID);
    old_relations.allow_current_fallback = true;

    let handle = fixture
        .engine
        .table_handle(&old_relations, TABLE_ID)
        .unwrap();

    assert!(
        Arc::strong_count(&handle._generation) >= 2,
        "fallback table handle must pin the current generation while files are used"
    );
}

#[test]
fn index_handle_fallback_pins_current_generation() {
    let fixture = Fixture::new();
    let mut old_relations = fixture
        .engine
        .capture_pagebacked_relation_snapshot()
        .unwrap();
    old_relations.indexes.remove(&NAME_INDEX_ID);
    old_relations.allow_current_fallback = true;

    let handle = fixture
        .engine
        .index_handle(&old_relations, TABLE_ID, NAME_INDEX_ID)
        .unwrap();

    assert!(
        Arc::strong_count(&handle._generation) >= 2,
        "fallback index handle must pin the current generation while files are used"
    );
}

#[test]
fn captured_relation_snapshot_missing_table_is_strict() {
    let fixture = Fixture::new();
    let mut relations = fixture
        .engine
        .capture_pagebacked_relation_snapshot()
        .unwrap();
    relations.tables.remove(&TABLE_ID);

    let err = match fixture.engine.table_handle(&relations, TABLE_ID) {
        Ok(_) => panic!("missing table in captured relation snapshot should be strict"),
        Err(err) => err,
    };
    assert_eq!(err.code, SqlState::UndefinedTable);
}

#[test]
fn captured_relation_snapshot_missing_table_does_not_fallback_to_index() {
    let fixture = Fixture::new();
    let mut relations = fixture
        .engine
        .capture_pagebacked_relation_snapshot()
        .unwrap();
    relations.tables.remove(&TABLE_ID);
    relations.indexes.remove(&NAME_INDEX_ID);

    let err = match fixture
        .engine
        .index_handle(&relations, TABLE_ID, NAME_INDEX_ID)
    {
        Ok(_) => panic!("missing index in captured relation snapshot should be strict"),
        Err(err) => err,
    };
    assert_eq!(err.code, SqlState::UndefinedTable);
}

#[test]
fn rollback_of_published_create_index_retires_snapshot_held_file() {
    let fixture = Fixture::new();
    let row = user_row(1, "alice", "snapshot note");
    fixture.insert_committed(200, row.clone());

    let txn_id = 400;
    let index = note_index(NOTE_INDEX_STORAGE_ID);
    fixture
        .engine
        .create_index(&ctx(txn_id), &index, 0)
        .unwrap();
    fixture.flush_dirty_pages();

    let relation_with_index = fixture.capture_relations();
    let file_id = secondary_index_file_id(index.storage_id);
    assert!(fixture.file_ids().contains(&file_id));

    <PageBackedStorageEngine as StorageEngine>::rollback_txn(&fixture.engine, txn_id).unwrap();
    assert_eq!(fixture.engine.cleanup_orphan_files().unwrap(), 0);
    assert!(
        fixture.file_ids().contains(&file_id),
        "published index file was removed while a relation snapshot still held it"
    );

    let mut iter = <PageBackedStorageEngine as StorageEngine>::index_scan(
        &fixture.engine,
        &ctx(401),
        relation_with_index.as_ref(),
        TABLE_ID,
        NOTE_INDEX_ID,
        &KeyRange::Exact(Key(vec![Value::Text("snapshot note".to_string())])),
    )
    .unwrap();
    let stored = iter.next().unwrap().expect("held snapshot lost index row");
    assert_eq!(stored.row, row);
    assert!(iter.next().unwrap().is_none());

    drop(relation_with_index);
    assert_eq!(fixture.engine.try_cleanup_retired_generations().unwrap(), 1);
    assert!(
        !fixture.file_ids().contains(&file_id),
        "rolled-back published index file was not removed after snapshots drained"
    );
}

#[test]
fn committed_drop_files_wait_for_snapshot_then_cleanup() {
    let fixture = Fixture::new();
    let note = large_note();
    fixture.insert_committed(200, user_row(1, "alice", &note));
    fixture.flush_dirty_pages();

    let old_relations = fixture.capture_relations();
    let old_files = old_generation_files();
    for file_id in old_files {
        assert!(fixture.file_ids().contains(&file_id));
    }

    let txn_id = 400;
    fixture.engine.drop_table(&ctx(txn_id), TABLE_ID).unwrap();
    commit(&fixture.wal, txn_id);
    <PageBackedStorageEngine as StorageEngine>::commit_txn(&fixture.engine, txn_id).unwrap();

    assert_eq!(fixture.engine.cleanup_orphan_files().unwrap(), 0);
    assert_eq!(fixture.engine.try_cleanup_retired_generations().unwrap(), 0);
    for file_id in old_files {
        assert!(
            fixture.file_ids().contains(&file_id),
            "drop cleanup removed file {file_id} while an old relation snapshot was alive"
        );
    }

    drop(old_relations);
    assert_eq!(fixture.engine.try_cleanup_retired_generations().unwrap(), 1);
    for file_id in old_files {
        assert!(
            !fixture.file_ids().contains(&file_id),
            "dropped relation file {file_id} survived after snapshots drained"
        );
    }
}

#[test]
fn retired_generation_cleanup_waits_for_old_snapshot_then_removes_files() {
    let fixture = Fixture::new();
    let old_relations = {
        let note = large_note();
        fixture.insert_committed(200, user_row(1, "alice", &note));
        fixture.capture_relations()
    };
    fixture.flush_dirty_pages();
    let old_files = old_generation_files();
    let files_before = fixture.file_ids();
    for file_id in old_files {
        assert!(
            files_before.contains(&file_id),
            "missing old file {file_id}"
        );
    }

    fixture.truncate_committed(400);
    fixture.flush_dirty_pages();
    assert_eq!(fixture.engine.cleanup_orphan_files().unwrap(), 0);
    assert_eq!(fixture.engine.try_cleanup_retired_generations().unwrap(), 0);
    let files_while_snapshot_lives = fixture.file_ids();
    for file_id in old_files {
        assert!(
            files_while_snapshot_lives.contains(&file_id),
            "cleaned file {file_id} while an old relation snapshot was alive"
        );
    }

    drop(old_relations);
    assert_eq!(fixture.engine.try_cleanup_retired_generations().unwrap(), 1);
    let files_after = fixture.file_ids();
    for file_id in old_files {
        assert!(
            !files_after.contains(&file_id),
            "retired file {file_id} survived after its relation snapshot dropped"
        );
    }
    assert!(files_after.contains(&secondary_index_file_id(NEW_NAME_INDEX_STORAGE_ID)));
}

fn truncate_created_files_for_test(update: &TruncateCatalogUpdate) -> Vec<FileId> {
    let mut files = vec![primary_index_file_id(update.table.storage_id)];
    if let Some(toast) = &update.toast_table {
        files.push(primary_index_file_id(toast.storage_id));
    }
    files.extend(
        update
            .indexes
            .iter()
            .map(|index| secondary_index_file_id(index.storage_id)),
    );
    files
}
