use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use common::{
    ColumnDef, DataType, IndexSchema, Key, PageFlushInfo, Row, Snapshot, SqlState,
    StatementContext, TableSchema, Value,
};
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

use super::PageBackedStorageEngine;
use crate::HeapPageStore;
use crate::traits::{SchemaOperations, StorageEngine};

const TABLE_ID: u32 = 1;
const NAME_INDEX_ID: u32 = 1;

struct AlwaysFlush;
impl common::FlushPolicy for AlwaysFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        true
    }
}

/// A shared engine plus its WAL, built so several threads can drive it at once
/// (`Arc<PageBackedStorageEngine>`), mirroring the server's shared writer model.
/// `frames` sets the buffer-pool size so a test can force eviction/steal (and
/// hence on-disk file extension) to overlap with concurrent allocation.
struct SharedEngine {
    engine: Arc<PageBackedStorageEngine>,
    wal: Arc<FileWalManager>,
    _dir: tempfile::TempDir,
}

impl SharedEngine {
    fn with_frames(frames: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("data")).unwrap());
        let buffer = Arc::new(MemoryBufferPool::new(frames, Box::new(AlwaysFlush), store));
        buffer.enable_stealing();
        let wal = Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let engine = Arc::new(
            PageBackedStorageEngine::open(buffer, wal.clone(), super::StorageMode::Normal).unwrap(),
        );
        Self {
            engine,
            wal,
            _dir: dir,
        }
    }

    fn new() -> Self {
        Self::with_frames(1024)
    }

    fn commit(&self, txn_id: u64) {
        self.wal
            .append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        self.wal.flush().unwrap();
    }
}

/// A degenerate snapshot for an autocommit-style statement under `txn_id`: empty
/// `xip`, `xmax` past every allocated id, so it sees all committed rows plus its
/// own writes (via `current_txn`).
fn ctx(txn_id: u64, xmax: u64) -> StatementContext {
    StatementContext::with_snapshot(
        txn_id,
        Arc::new(Snapshot {
            xmin: 1,
            xmax,
            xip: vec![],
        }),
    )
}

fn users_schema() -> TableSchema {
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
            },
            ColumnDef {
                id: 1,
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                max_length: None,
            },
        ],
        primary_key: vec![0],
    }
}

fn name_index() -> IndexSchema {
    IndexSchema {
        id: NAME_INDEX_ID,
        table: TABLE_ID,
        name: "users_name".to_string(),
        columns: vec![1],
        unique: false,
    }
}

fn row(id: i64, name: &str) -> Row {
    Row {
        values: vec![Value::Integer(id), Value::Text(name.to_string())],
    }
}

/// Drain a sequential scan into the `id` column of every visible row, sorted.
fn scan_ids(shared: &SharedEngine, reader_xmax: u64) -> Vec<i64> {
    let mut iter = shared.engine.scan(&ctx(0, reader_xmax), TABLE_ID).unwrap();
    let mut ids = Vec::new();
    while let Some(stored) = iter.next().unwrap() {
        if let Value::Integer(id) = stored.row.values[0] {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    ids
}

/// N threads insert DISTINCT keys into ONE table whose single PK index is forced
/// to split many times. The per-index latch must make concurrent splits safe: a
/// full scan afterward returns EXACTLY the inserted key multiset — no lost, no
/// duplicated, no corrupted entries.
#[test]
fn concurrent_splits_one_index_preserve_every_key() {
    let shared = SharedEngine::new();
    let setup = ctx(100, 101);
    shared.engine.create_table(&setup, &users_schema()).unwrap();
    shared.commit(100);

    const THREADS: usize = 6;
    const PER_THREAD: i64 = 400; // 2400 keys ⇒ many B-tree splits
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = shared.engine.clone();
        let wal = shared.wal.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            // Disjoint key range per thread (vary work by index, not by sleep).
            let base = (t as i64) * PER_THREAD;
            let txn_id = 1000 + t as u64;
            barrier.wait();
            for i in 0..PER_THREAD {
                let id = base + i + 1;
                engine
                    .insert(&ctx(txn_id, 10_000), TABLE_ID, row(id, "x"))
                    .expect("insert of a distinct key under the per-index latch");
            }
            // Commit this writer's txn so its rows are visible to the final scan.
            wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
            wal.flush().unwrap();
        }));
    }
    for handle in handles {
        handle.join().expect("inserter thread finished");
    }

    let ids = scan_ids(&shared, 10_000);
    let expected: Vec<i64> = (1..=(THREADS as i64 * PER_THREAD)).collect();
    assert_eq!(
        ids.len(),
        expected.len(),
        "no rows lost or duplicated across concurrent splits"
    );
    assert_eq!(ids, expected, "exactly the inserted key multiset survives");
}

/// N threads insert rows into ONE table heap, sized so many share a page,
/// forcing the per-heap latch to serialize free-space search + allocate +
/// insert. All rows must be present with no slot overwrite and no panic.
#[test]
fn concurrent_heap_inserts_one_table_keep_every_row() {
    let shared = SharedEngine::new();
    let setup = ctx(100, 101);
    shared.engine.create_table(&setup, &users_schema()).unwrap();
    shared.commit(100);

    const THREADS: usize = 8;
    const PER_THREAD: i64 = 150;
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = shared.engine.clone();
        let wal = shared.wal.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let base = (t as i64) * PER_THREAD;
            let txn_id = 2000 + t as u64;
            barrier.wait();
            for i in 0..PER_THREAD {
                let id = base + i + 1;
                // Small payloads so many tuples share a heap page (stresses the
                // free-space search + slot allocation under the per-heap latch).
                engine
                    .insert(&ctx(txn_id, 10_000), TABLE_ID, row(id, "r"))
                    .expect("heap insert under the per-heap latch");
            }
            wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
            wal.flush().unwrap();
        }));
    }
    for handle in handles {
        handle.join().expect("heap inserter thread finished");
    }

    let ids = scan_ids(&shared, 10_000);
    let expected: Vec<i64> = (1..=(THREADS as i64 * PER_THREAD)).collect();
    assert_eq!(ids, expected, "every heap row present, no slot overwrite");
}

/// Two writers on DIFFERENT tables run truly concurrently and both complete
/// correctly (a smoke test that cross-table writers do not serialize/corrupt).
#[test]
fn cross_table_writers_are_concurrent_and_correct() {
    // Two heaps: TABLE_ID and a second table id 2.
    const TABLE_B: u32 = 2;
    let shared = SharedEngine::new();
    let setup = ctx(100, 101);
    shared.engine.create_table(&setup, &users_schema()).unwrap();
    let mut schema_b = users_schema();
    schema_b.id = TABLE_B;
    schema_b.name = "other".to_string();
    shared.engine.create_table(&setup, &schema_b).unwrap();
    shared.commit(100);

    const PER_THREAD: i64 = 300;
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for (table, txn_id) in [(TABLE_ID, 3001u64), (TABLE_B, 3002u64)] {
        let engine = shared.engine.clone();
        let wal = shared.wal.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for id in 1..=PER_THREAD {
                engine
                    .insert(&ctx(txn_id, 10_000), table, row(id, "c"))
                    .expect("cross-table insert");
            }
            wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
            wal.flush().unwrap();
        }));
    }
    for handle in handles {
        handle.join().expect("cross-table thread finished");
    }

    // Each table independently holds all its rows.
    let a: Vec<i64> = {
        let mut iter = shared.engine.scan(&ctx(0, 10_000), TABLE_ID).unwrap();
        let mut v = Vec::new();
        while let Some(s) = iter.next().unwrap() {
            if let Value::Integer(id) = s.row.values[0] {
                v.push(id);
            }
        }
        v.sort_unstable();
        v
    };
    let b: Vec<i64> = {
        let mut iter = shared.engine.scan(&ctx(0, 10_000), TABLE_B).unwrap();
        let mut v = Vec::new();
        while let Some(s) = iter.next().unwrap() {
            if let Value::Integer(id) = s.row.values[0] {
                v.push(id);
            }
        }
        v.sort_unstable();
        v
    };
    let expected: Vec<i64> = (1..=PER_THREAD).collect();
    assert_eq!(a, expected);
    assert_eq!(b, expected);
}

/// N writers each UPDATE the SAME committed key under their OWN in-flight txn.
/// First-updater-wins: exactly one stamps `xmax` and succeeds; every other sees
/// the winner's `xmax` (a committed-or-in-progress deleter) and aborts with
/// `40001`. The surviving committed value is the winner's.
#[test]
fn concurrent_update_same_key_one_winner_others_40001() {
    let shared = SharedEngine::new();
    let setup = ctx(100, 101);
    shared.engine.create_table(&setup, &users_schema()).unwrap();
    shared.commit(100);
    // The single committed row every updater targets.
    shared
        .engine
        .insert(&ctx(10, 11), TABLE_ID, row(1, "original"))
        .unwrap();
    shared.commit(10);

    const THREADS: usize = 5;
    let key = Key(vec![Value::Integer(1)]);
    let barrier = Arc::new(Barrier::new(THREADS));
    let winners = Arc::new(AtomicUsize::new(0));
    let conflicts = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = shared.engine.clone();
        let barrier = barrier.clone();
        let winners = winners.clone();
        let conflicts = conflicts.clone();
        let key = key.clone();
        handles.push(thread::spawn(move || {
            let txn_id = 5000 + t as u64;
            // Each updater's snapshot sees the original committed row (txn 10) and
            // excludes the other in-flight updaters (degenerate xip is fine: the
            // conflict is decided by the physical `xmax`, not the snapshot).
            let new_name = format!("by-{txn_id}");
            barrier.wait();
            match engine.update(&ctx(txn_id, 10_000), TABLE_ID, &key, row(1, &new_name)) {
                Ok(true) => {
                    winners.fetch_add(1, Ordering::AcqRel);
                    txn_id // the winner's txn id (commit it below)
                }
                Ok(false) => panic!("update located no visible row"),
                Err(err) => {
                    assert_eq!(
                        err.code,
                        SqlState::SerializationFailure,
                        "a losing concurrent updater must get 40001, got: {err:?}"
                    );
                    conflicts.fetch_add(1, Ordering::AcqRel);
                    0
                }
            }
        }));
    }
    let mut winner_txn = 0u64;
    for handle in handles {
        let result = handle.join().expect("updater thread finished");
        if result != 0 {
            winner_txn = result;
        }
    }
    assert_eq!(
        winners.load(Ordering::Acquire),
        1,
        "exactly one updater wins the first-updater-wins race"
    );
    assert_eq!(
        conflicts.load(Ordering::Acquire),
        THREADS - 1,
        "every other updater aborts with 40001"
    );

    // Commit the winner; the surviving visible value is the winner's.
    shared.commit(winner_txn);
    let mut iter = shared.engine.scan(&ctx(0, 10_000), TABLE_ID).unwrap();
    let mut names = Vec::new();
    while let Some(stored) = iter.next().unwrap() {
        names.push(stored.row.values[1].clone());
    }
    assert_eq!(names.len(), 1, "exactly one visible version of the row");
    assert_eq!(
        names[0],
        Value::Text(format!("by-{winner_txn}")),
        "the surviving value is the winning updater's"
    );
}

/// N writers each INSERT the SAME primary key under their own in-flight txn.
/// The per-index latch makes uniqueness-check-and-insert atomic: exactly one
/// succeeds; every other sees the winner's entry and aborts — `40001` while the
/// winner is in-flight (the loser cannot tell the winner will commit), or `23505`
/// if the winner already committed. After committing the winner, one row remains.
#[test]
fn concurrent_insert_same_key_one_winner_others_conflict() {
    let shared = SharedEngine::new();
    let setup = ctx(100, 101);
    shared.engine.create_table(&setup, &users_schema()).unwrap();
    shared.commit(100);

    const THREADS: usize = 6;
    let barrier = Arc::new(Barrier::new(THREADS));
    let winners = Arc::new(AtomicUsize::new(0));
    let conflicts = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = shared.engine.clone();
        let barrier = barrier.clone();
        let winners = winners.clone();
        let conflicts = conflicts.clone();
        handles.push(thread::spawn(move || {
            let txn_id = 6000 + t as u64;
            barrier.wait();
            match engine.insert(&ctx(txn_id, 10_000), TABLE_ID, row(7, "dup")) {
                Ok(_) => {
                    winners.fetch_add(1, Ordering::AcqRel);
                    txn_id
                }
                Err(err) => {
                    assert!(
                        err.code == SqlState::SerializationFailure
                            || err.code == SqlState::UniqueViolation,
                        "a losing concurrent inserter must get 40001 or 23505, got: {err:?}"
                    );
                    conflicts.fetch_add(1, Ordering::AcqRel);
                    0
                }
            }
        }));
    }
    let mut winner_txn = 0u64;
    for handle in handles {
        let result = handle.join().expect("inserter thread finished");
        if result != 0 {
            winner_txn = result;
        }
    }
    assert_eq!(
        winners.load(Ordering::Acquire),
        1,
        "exactly one inserter claims the unique key"
    );
    assert_eq!(conflicts.load(Ordering::Acquire), THREADS - 1);

    shared.commit(winner_txn);
    let ids = scan_ids(&shared, 10_000);
    assert_eq!(ids, vec![7], "exactly one committed row for the key");
}

/// Deadlock guard: N threads insert into a table with TWO indexes (PK +
/// secondary) in a tight loop. Each statement takes the heap latch, then the PK
/// latch, then the secondary latch — always released before the next (rule 1:
/// never two structural latches at once), so there is no lock-ordering cycle. The
/// whole run must COMPLETE within a bounded wall-clock budget; a hang would mean a
/// latch-ordering deadlock.
#[test]
fn multi_index_inserts_do_not_deadlock_within_bounded_time() {
    let shared = SharedEngine::new();
    let setup = ctx(100, 101);
    shared.engine.create_table(&setup, &users_schema()).unwrap();
    shared
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    shared.commit(100);

    const THREADS: usize = 6;
    const PER_THREAD: i64 = 250;
    let barrier = Arc::new(Barrier::new(THREADS));
    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = shared.engine.clone();
        let wal = shared.wal.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let base = (t as i64) * PER_THREAD;
            let txn_id = 7000 + t as u64;
            barrier.wait();
            for i in 0..PER_THREAD {
                let id = base + i + 1;
                // Distinct secondary values too, so secondary inserts also split.
                let name = format!("n{id}");
                engine
                    .insert(&ctx(txn_id, 100_000), TABLE_ID, row(id, &name))
                    .expect("two-index insert");
            }
            wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
            wal.flush().unwrap();
        }));
    }
    for handle in handles {
        handle.join().expect("two-index inserter thread finished");
    }
    // Generous ceiling: the run is small; exceeding this means a hang, not slow.
    assert!(
        start.elapsed() < Duration::from_secs(60),
        "multi-index concurrent inserts must complete without deadlock"
    );

    let ids = scan_ids(&shared, 100_000);
    let expected: Vec<i64> = (1..=(THREADS as i64 * PER_THREAD)).collect();
    assert_eq!(
        ids, expected,
        "every row present after the deadlock-guard run"
    );
}

/// Concurrent allocation through a TINY buffer pool forces steal-eviction (which
/// writes stolen pages to disk, extending the heap file) to overlap with fresh
/// `new_page` allocation. The per-heap latch + the lock-held extent seed must keep
/// page-number allocation correct under that overlap: every inserted row survives,
/// none overwritten by a reused page number.
#[test]
fn concurrent_allocation_with_eviction_does_not_lose_rows() {
    // A very small pool so most pages are stolen out to disk (extending the heap
    // file) while other threads allocate fresh pages — the steal-vs-write race
    // window the `evicting`-flag guard closes (Milestone E2b). Aggressive params
    // make this a sharp regression guard.
    let shared = SharedEngine::with_frames(6);
    let setup = ctx(100, 101);
    shared.engine.create_table(&setup, &users_schema()).unwrap();
    shared.commit(100);

    const THREADS: usize = 6;
    const PER_THREAD: i64 = 250;
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = shared.engine.clone();
        let wal = shared.wal.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let base = (t as i64) * PER_THREAD;
            let txn_id = 8000 + t as u64;
            barrier.wait();
            for i in 0..PER_THREAD {
                let id = base + i + 1;
                engine
                    .insert(&ctx(txn_id, 100_000), TABLE_ID, row(id, "e"))
                    .expect("insert under eviction pressure");
            }
            wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
            wal.flush().unwrap();
        }));
    }
    for handle in handles {
        handle.join().expect("eviction-pressure thread finished");
    }

    let ids = scan_ids(&shared, 100_000);
    let expected: Vec<i64> = (1..=(THREADS as i64 * PER_THREAD)).collect();
    assert_eq!(
        ids, expected,
        "no row lost to a reused page number under concurrent steal-eviction"
    );
}
