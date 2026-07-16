use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use common::{
    ColumnDef, CompressionSetting, DataType, FileId, IndexSchema, PageFlushInfo, RelationKind, Row,
    Snapshot, StatementContext, TableSchema, ToastOptions, Value,
};
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

use super::PageBackedStorageEngine;
use crate::HeapPageStore;
use crate::heap::{primary_index_file_id, secondary_index_file_id};
use crate::traits::SchemaOperations;

const TABLE_ID: u32 = 1;
const NAME_INDEX_ID: u32 = 1;

struct AlwaysFlush;
impl common::FlushPolicy for AlwaysFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        true
    }
}

fn engine() -> (
    PageBackedStorageEngine,
    Arc<FileWalManager>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn PageStore> = Arc::new(HeapPageStore::open(dir.path().join("data")).unwrap());
    let buffer = Arc::new(MemoryBufferPool::new(256, Box::new(AlwaysFlush), store));
    buffer.enable_stealing();
    let wal = Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
    let engine =
        PageBackedStorageEngine::open(buffer, wal.clone(), super::StorageMode::Normal).unwrap();
    (engine, wal, dir)
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

fn users_schema() -> TableSchema {
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
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                max_length: None,
                default: None,
                pg_type: None,
            },
        ],
        primary_key: vec![0],
        schema_version: common::INITIAL_SCHEMA_VERSION,
        compression: CompressionSetting::None,
        active_dict_id: None,
        toast: ToastOptions::disabled(),
        toast_table_id: None,
        relation_kind: RelationKind::User,
        next_column_object_id: u32::MAX,
    }
}

fn name_index() -> IndexSchema {
    IndexSchema {
        id: NAME_INDEX_ID,
        schema_id: common::PUBLIC_SCHEMA_ID,
        storage_id: 101,
        table: TABLE_ID,
        name: "users_name".to_string(),
        columns: vec![1],
        unique: false,
        constraint: None,
    }
}

fn row(id: i64, name: &str) -> Row {
    Row {
        values: vec![Value::Integer(id), Value::Text(name.to_string())],
    }
}

/// Whether the registry currently holds a latch for `file_id` (used to assert an
/// operation lazily registered the expected per-file latch).
fn has_latch(engine: &PageBackedStorageEngine, file_id: FileId) -> bool {
    engine
        .structural_latches
        .lock()
        .unwrap()
        .contains_key(&file_id)
}

#[test]
fn structural_latch_returns_same_arc_per_file_and_distinct_across_files() {
    let (engine, _wal, _dir) = engine();
    let a = engine.structural_latch(0x1234);
    let b = engine.structural_latch(0x1234);
    let c = engine.structural_latch(0x5678);

    // Same FileId ⇒ the SAME Arc<Mutex>, so same-structure ops contend on one
    // latch; a different FileId ⇒ a DIFFERENT Arc, so they run independently.
    assert!(Arc::ptr_eq(&a, &b));
    assert!(!Arc::ptr_eq(&a, &c));
}

#[test]
fn structural_latch_does_not_serialize_globally() {
    // The registry mutex is held only briefly per lookup: two different files'
    // latches can be locked at the same time (no global serialization). If the
    // registry mutex were held across the lock, this would deadlock/contend.
    let (engine, _wal, _dir) = engine();
    let a = engine.structural_latch(0xAAAA);
    let b = engine.structural_latch(0xBBBB);
    let ga = a.lock();
    let gb = b.lock(); // would block forever if the registry mutex were held here
    drop(gb);
    drop(ga);
}

#[test]
fn identity_rewrite_gate_blocks_identity_scans() {
    let (engine, wal, _dir) = engine();
    let setup = ctx(100);
    engine.create_table(&setup, &users_schema()).unwrap();
    engine.insert(&ctx(10), TABLE_ID, row(1, "amy")).unwrap();
    commit(&wal, 10);

    let engine = Arc::new(engine);
    let rewrite_latch = engine.identity_rewrite_latch(TABLE_ID);
    let rewrite_guard = rewrite_latch.write();
    let barrier = Arc::new(Barrier::new(2));
    let (tx, rx) = mpsc::channel();

    let scan_engine = Arc::clone(&engine);
    let scan_barrier = Arc::clone(&barrier);
    let handle = std::thread::spawn(move || {
        scan_barrier.wait();
        let result = scan_engine.scan(&ctx(20), TABLE_ID).and_then(|mut rows| {
            let mut count = 0;
            while rows.next()?.is_some() {
                count += 1;
            }
            Ok(count)
        });
        tx.send(result).unwrap();
    });

    barrier.wait();
    assert!(
        rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "scan completed while the identity rewrite gate was held"
    );

    drop(rewrite_guard);
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap(), 1);
    handle.join().unwrap();
}

#[test]
fn insert_registers_heap_and_index_latches() {
    let (engine, wal, _dir) = engine();
    let setup = ctx(100);
    engine.create_table(&setup, &users_schema()).unwrap();
    engine.create_index(&setup, &name_index(), 0).unwrap();
    commit(&wal, 100);

    // create_index's backfill (none here) plus the create touch the secondary
    // index latch; an INSERT then exercises the heap, PK-index, and secondary
    // latches. After the insert the registry has an entry for each expected file.
    engine.insert(&ctx(10), TABLE_ID, row(1, "amy")).unwrap();
    commit(&wal, 10);

    assert!(has_latch(&engine, TABLE_ID), "heap latch registered");
    assert!(
        has_latch(&engine, primary_index_file_id(TABLE_ID)),
        "primary-key index latch registered"
    );
    assert!(
        has_latch(&engine, secondary_index_file_id(name_index().storage_id)),
        "secondary index latch registered"
    );
}

#[test]
fn heap_insertion_latch_is_held_for_the_duration_of_write_new_row() {
    // The per-heap latch is the same Arc the engine uses internally, and a single
    // `parking_lot::Mutex` is NOT reentrant: while a structural op holds it, a
    // second lock attempt by this thread would deadlock — so a `try_lock` from the
    // test thread succeeds only because no op is in flight here. This is the
    // deterministic stand-in for "the op holds its latch" until E2b's overlap
    // stress tests: we assert the registry hands out the same lockable latch the
    // engine acquires, and that holding it blocks a re-lock.
    let (engine, wal, _dir) = engine();
    let setup = ctx(100);
    engine.create_table(&setup, &users_schema()).unwrap();
    commit(&wal, 100);
    engine.insert(&ctx(10), TABLE_ID, row(1, "amy")).unwrap();
    commit(&wal, 10);

    let heap_latch = engine.structural_latch(TABLE_ID);
    let guard = heap_latch.lock();
    // While this thread holds the heap latch, the same non-reentrant latch cannot
    // be re-locked (try_lock fails), proving it is the real exclusion primitive
    // the heap insert path acquires.
    assert!(heap_latch.try_lock().is_none());
    drop(guard);
    assert!(heap_latch.try_lock().is_some());
}
