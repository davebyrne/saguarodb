use std::sync::Arc;

use std::collections::HashSet;

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use common::{
    ColumnDef, CompressionSetting, DataType, IndexSchema, KeyRange, PageFlushInfo, Row, Snapshot,
    StatementContext, TableSchema, Value,
};
use compress::CompressionRegistry;
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

use super::{PageBackedStorageEngine, RowLocation, VACUUM_TXN};
use crate::HeapPageStore;
use crate::heap::index_file_id;
use crate::traits::{SchemaOperations, StorageEngine};

const TABLE_ID: u32 = 1;
const NAME_INDEX_ID: u32 = 7;

struct AlwaysFlush;
impl common::FlushPolicy for AlwaysFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        true
    }
}

struct Fixture {
    engine: PageBackedStorageEngine,
    wal: Arc<FileWalManager>,
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
            PageBackedStorageEngine::open(buffer, wal.clone(), super::StorageMode::Normal).unwrap();
        let fixture = Self {
            engine,
            wal,
            _dir: dir,
        };
        // DDL under a committed setup transaction, then create the heap.
        fixture
            .engine
            .create_table(&ctx(100), &users_schema())
            .unwrap();
        fixture.commit(100);
        fixture
    }

    /// Append a `Commit` for `txn_id` and flush so the CLOG records it Committed
    /// (a commit only settles once durable).
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

    /// Append an `Abort` for `txn_id` so the CLOG records it Aborted (abort is not
    /// fsync-gated).
    fn abort(&self, txn_id: u64) {
        self.wal
            .append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Abort,
            })
            .unwrap();
    }

    /// Insert a committed row, returning its heap TID.
    fn insert_committed(&self, txn_id: u64, row: Row) -> RowLocation {
        let rid = self.engine.insert(&ctx(txn_id), TABLE_ID, row).unwrap();
        self.commit(txn_id);
        RowLocation {
            file_id: TABLE_ID,
            page_num: rid.page_num,
            slot_num: rid.slot_num,
        }
    }

    /// Delete the row keyed by `id` under `deleter` (stamps xmax). The caller then
    /// decides whether to commit/abort/leave-in-flight the deleter.
    fn delete(&self, deleter: u64, id: i64) {
        assert!(
            self.engine
                .delete(&ctx(deleter), TABLE_ID, &key(id))
                .unwrap(),
            "delete of id {id} should have matched a visible row"
        );
    }

    /// Whether the physical line pointer at `location` is still NORMAL (decodes a
    /// live tuple), reading past visibility.
    fn is_normal(&self, location: RowLocation) -> bool {
        let readable = self
            .engine
            .buffer_pool
            .read_page(location.file_id, location.page_num)
            .unwrap();
        crate::page::read_row(readable.data(), location.slot_num)
            .unwrap()
            .is_some()
    }

    /// The physical row bytes at `location`, or `None` if the slot is not NORMAL.
    fn physical_bytes(&self, location: RowLocation) -> Option<Vec<u8>> {
        let readable = self
            .engine
            .buffer_pool
            .read_page(location.file_id, location.page_num)
            .unwrap();
        crate::page::read_row(readable.data(), location.slot_num).unwrap()
    }

    /// Free bytes on the heap page (slot-array start minus free_start), used to
    /// assert a prune reclaimed space.
    fn free_bytes(&self, page_num: u32) -> usize {
        let readable = self
            .engine
            .buffer_pool
            .read_page(TABLE_ID, page_num)
            .unwrap();
        let free_start =
            crate::page::read_u16(readable.data(), crate::page::FREE_SPACE_OFFSET) as usize;
        // The first slot lives at the top of the page growing down; with `n` slots
        // the slot array occupies `n * SLOT_LEN` bytes from the page end. Free space
        // is everything between free_start and that slot array.
        let num_slots =
            crate::page::read_u16(readable.data(), crate::page::NUM_SLOTS_OFFSET) as usize;
        let slot_array = num_slots * crate::page::SLOT_LEN;
        buffer::PAGE_SIZE - slot_array - free_start
    }

    /// Every full-page-image record (raw or compressed) in the WAL for this
    /// table's heap file, decompressed back to `(page_num, image)` pairs.
    /// Compression (Task 7) attempts zstd on every FPI unconditionally, even
    /// under the fixture's plain (no file config, no dictionary) registry, so
    /// a compressible page now logs `FullPageImageCompressed`; a fresh
    /// dict-less registry decompresses it identically.
    fn full_page_images(&self) -> Vec<(u32, Vec<u8>)> {
        let registry = CompressionRegistry::new();
        self.wal
            .replay_from(0)
            .unwrap()
            .filter_map(|record| match record.unwrap().kind {
                WalRecordKind::FullPageImage {
                    file_id,
                    page_num,
                    image,
                } if file_id == TABLE_ID => Some((page_num, image)),
                WalRecordKind::FullPageImageCompressed {
                    file_id,
                    page_num,
                    codec,
                    dict_id,
                    payload,
                } if file_id == TABLE_ID => {
                    let image = registry
                        .decompress_fpi(codec, dict_id, &payload, buffer::PAGE_SIZE)
                        .unwrap();
                    Some((page_num, image))
                }
                _ => None,
            })
            .collect()
    }
}

fn ctx(txn_id: u64) -> StatementContext {
    // A snapshot that sees every committed id below the next id, with no in-flight
    // exclusions — DML under it reads the latest committed state.
    StatementContext::with_snapshot(
        txn_id,
        Arc::new(Snapshot {
            xmin: 1,
            xmax: txn_id + 1,
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
                default: None,
                pg_type: None,
            },
            ColumnDef {
                id: 1,
                name: "name".to_string(),
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
    }
}

fn row(id: i64, name: &str) -> Row {
    Row {
        values: vec![Value::Integer(id), Value::Text(name.to_string())],
    }
}

fn key(id: i64) -> common::Key {
    common::Key(vec![Value::Integer(id)])
}

/// A non-unique secondary index on the `name` column.
fn name_index() -> IndexSchema {
    IndexSchema {
        id: NAME_INDEX_ID,
        table: TABLE_ID,
        name: "users_name".to_string(),
        columns: vec![1],
        unique: false,
    }
}

/// Every TID stored in the primary-key index, in `(key, tid)` order.
fn pk_index_tids(engine: &PageBackedStorageEngine) -> Vec<RowLocation> {
    engine
        .btree(index_file_id(TABLE_ID))
        .range(&KeyRange::All)
        .unwrap()
        .into_iter()
        .map(|(_, tid)| tid)
        .collect()
}

/// Every TID stored in the `name` secondary index, in `(key, tid)` order.
fn name_index_tids(engine: &PageBackedStorageEngine) -> Vec<RowLocation> {
    engine
        .secondary_btree(NAME_INDEX_ID)
        .range(&KeyRange::All)
        .unwrap()
        .into_iter()
        .map(|(_, tid)| tid)
        .collect()
}

/// Normalize a `FullPageImageCompressed` record to the raw `FullPageImage`
/// shape `apply_physical_redo` accepts (decompressing via a fresh dict-less
/// registry — every FPI in this file's fixtures is dict-less, since no test
/// here registers a dictionary). Every other record kind passes through
/// unchanged. Mirrors the resolve-to-raw step real recovery replay performs
/// (Task 11) before physiological redo.
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
fn vacuum_indexes_removes_dangling_entries_from_pk_and_secondary() {
    let fixture = Fixture::new();
    fixture
        .engine
        .create_index(&ctx(101), &name_index(), 0)
        .unwrap();
    fixture.commit(101);

    let keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    let also_gone = fixture.insert_committed(12, row(3, "gone-too"));

    // Two rows are deleted-and-committed below the horizon; one survives. Prune the
    // heap so their TIDs are DEAD (their index entries now dangle).
    fixture.delete(20, 2);
    fixture.commit(20);
    fixture.delete(21, 3);
    fixture.commit(21);
    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 30).unwrap();
    let dead: HashSet<RowLocation> = reclaimed.iter().copied().collect();
    assert_eq!(dead, HashSet::from([gone, also_gone]));

    // Before index vacuum the dangling entries still resolve to the dead TIDs.
    assert!(pk_index_tids(&fixture.engine).contains(&gone));
    assert!(name_index_tids(&fixture.engine).contains(&gone));

    fixture
        .engine
        .vacuum_indexes(&users_schema(), &dead)
        .unwrap();

    // No PK or secondary entry resolves to a dead TID anymore.
    let pk = pk_index_tids(&fixture.engine);
    let secondary = name_index_tids(&fixture.engine);
    for tid in pk.iter().chain(secondary.iter()) {
        assert!(!dead.contains(tid), "{tid:?} should have been vacuumed");
    }
    // The live row's entry survives in both indexes and still resolves correctly.
    assert_eq!(pk, vec![keep]);
    assert_eq!(secondary, vec![keep]);
}

#[test]
fn vacuum_indexes_handles_multiple_leaves_and_duplicate_keys() {
    let fixture = Fixture::new();
    fixture
        .engine
        .create_index(&ctx(101), &name_index(), 0)
        .unwrap();
    fixture.commit(101);

    // Many rows; half will be deleted. Use a small set of repeated names so the
    // secondary index has dup-key runs (many TIDs share one indexed value).
    let n = 300i64;
    let names = ["alpha", "beta", "gamma", "delta"];
    let mut live: Vec<RowLocation> = Vec::new();
    let mut dead: HashSet<RowLocation> = HashSet::new();
    for id in 0..n {
        let txn = 1000 + id as u64;
        let loc = fixture.insert_committed(txn, row(id, names[(id % 4) as usize]));
        if id % 2 == 0 {
            let deleter = 5000 + id as u64;
            fixture.delete(deleter, id);
            fixture.commit(deleter);
            dead.insert(loc);
        } else {
            live.push(loc);
        }
    }

    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 9000).unwrap();
    assert_eq!(
        reclaimed.iter().copied().collect::<HashSet<_>>(),
        dead,
        "heap prune reclaims exactly the deleted TIDs"
    );

    fixture
        .engine
        .vacuum_indexes(&users_schema(), &dead)
        .unwrap();

    // Every surviving entry in both indexes is a live TID; each live TID appears
    // exactly once per index; no dead TID remains.
    let mut pk = pk_index_tids(&fixture.engine);
    let mut secondary = name_index_tids(&fixture.engine);
    pk.sort_by_key(|l| (l.page_num, l.slot_num));
    secondary.sort_by_key(|l| (l.page_num, l.slot_num));
    let mut expected = live.clone();
    expected.sort_by_key(|l| (l.page_num, l.slot_num));
    assert_eq!(pk, expected, "PK index holds exactly the live TIDs");
    assert_eq!(
        secondary, expected,
        "secondary index holds exactly the live TIDs"
    );
}

#[test]
fn vacuum_indexes_empty_set_changes_nothing_and_logs_no_wal() {
    let fixture = Fixture::new();
    fixture
        .engine
        .create_index(&ctx(101), &name_index(), 0)
        .unwrap();
    fixture.commit(101);
    let keep = fixture.insert_committed(10, row(1, "keep"));

    let pk_before = pk_index_tids(&fixture.engine);
    let secondary_before = name_index_tids(&fixture.engine);
    let wal_len_before = fixture.wal.replay_from(0).unwrap().count();

    fixture
        .engine
        .vacuum_indexes(&users_schema(), &HashSet::new())
        .unwrap();

    assert_eq!(pk_index_tids(&fixture.engine), pk_before);
    assert_eq!(name_index_tids(&fixture.engine), secondary_before);
    assert_eq!(pk_before, vec![keep]);
    assert_eq!(
        fixture.wal.replay_from(0).unwrap().count(),
        wal_len_before,
        "an empty dead set appends no WAL"
    );
}

#[test]
fn vacuumed_index_page_survives_recovery_replay() {
    let fixture = Fixture::new();
    let keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    fixture.delete(20, 2);
    fixture.commit(20);

    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
    let dead: HashSet<RowLocation> = reclaimed.iter().copied().collect();
    assert_eq!(dead, HashSet::from([gone]));

    let pk_file_id = index_file_id(TABLE_ID);
    fixture
        .engine
        .vacuum_indexes(&users_schema(), &dead)
        .unwrap();

    // The runtime PK leaf page after index vacuum, captured from the buffer pool.
    // The single leaf is page 1 of the index file (page 0 is the metapage).
    let leaf_page = 1u32;
    let vacuumed = {
        let readable = fixture
            .engine
            .buffer_pool
            .read_page(pk_file_id, leaf_page)
            .unwrap();
        *readable.data()
    };
    assert_eq!(
        pk_index_tids(&fixture.engine),
        vec![keep],
        "the vacuumed PK index holds only the live entry"
    );

    // Replaying the index file's FullPageImages onto a fresh page under PageLSN
    // gating reinstalls the vacuumed leaf byte-for-byte (the crash-safety
    // guarantee — FPI redo regardless of txn id). `apply_physical_redo` only
    // accepts the raw variant, so resolve a compressed FPI first.
    let mut recovered = [0u8; buffer::PAGE_SIZE];
    for record in fixture.wal.replay_from(0).unwrap() {
        let record = record.unwrap();
        let kind = resolve_to_raw_fpi(record.kind);
        if let WalRecordKind::FullPageImage {
            file_id, page_num, ..
        } = &kind
            && *file_id == pk_file_id
            && *page_num == leaf_page
        {
            crate::redo::apply_physical_redo(&mut recovered, record.lsn, &kind).unwrap();
        }
    }
    assert_eq!(
        recovered, vacuumed,
        "the FullPageImage reinstalls the vacuumed leaf byte-for-byte"
    );
}

#[test]
fn vacuum_indexes_is_b_link_safe_against_a_concurrent_scanner() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Barrier, Mutex as StdMutex};

    // Many distinct keys, half deleted, spread across many index leaves so the
    // scanner and the vacuum genuinely overlap on the leaf chain.
    let fixture = Arc::new(Fixture::new());
    let n = 800i64;
    let mut live: HashSet<RowLocation> = HashSet::new();
    let mut dead: HashSet<RowLocation> = HashSet::new();
    for id in 0..n {
        let txn = 1000 + id as u64;
        let loc = fixture.insert_committed(txn, row(id, "x"));
        if id % 2 == 0 {
            let deleter = 6000 + id as u64;
            fixture.delete(deleter, id);
            fixture.commit(deleter);
            dead.insert(loc);
        } else {
            live.insert(loc);
        }
    }
    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 9000).unwrap();
    assert_eq!(reclaimed.iter().copied().collect::<HashSet<_>>(), dead);

    let pk_file_id = index_file_id(TABLE_ID);
    let live = Arc::new(live);
    let dead = Arc::new(dead);
    let barrier = Arc::new(Barrier::new(2));
    let stop = Arc::new(AtomicBool::new(false));
    let failure: Arc<StdMutex<Option<String>>> = Arc::new(StdMutex::new(None));

    // Reader thread: lock-free range scans in a loop (no structural latch). Each
    // pass must see every LIVE entry exactly once and never panic. A dead entry
    // may or may not be present depending on timing (it is being removed), so the
    // invariant is: no live entry missing and no entry duplicated.
    let reader = {
        let fixture = Arc::clone(&fixture);
        let live = Arc::clone(&live);
        let barrier = Arc::clone(&barrier);
        let stop = Arc::clone(&stop);
        let failure = Arc::clone(&failure);
        std::thread::spawn(move || {
            barrier.wait();
            let mut passes = 0u32;
            while !stop.load(Ordering::Relaxed) || passes < 2 {
                let scanned: Vec<RowLocation> = fixture
                    .engine
                    .btree(pk_file_id)
                    .range(&KeyRange::All)
                    .unwrap()
                    .into_iter()
                    .map(|(_, tid)| tid)
                    .collect();
                let mut seen: HashSet<RowLocation> = HashSet::new();
                for tid in &scanned {
                    if !seen.insert(*tid) {
                        *failure.lock().unwrap() =
                            Some(format!("scanner saw duplicate entry {tid:?}"));
                        return;
                    }
                }
                for tid in live.iter() {
                    if !seen.contains(tid) {
                        *failure.lock().unwrap() =
                            Some(format!("scanner missed live entry {tid:?}"));
                        return;
                    }
                }
                passes += 1;
                if stop.load(Ordering::Relaxed) && passes >= 2 {
                    break;
                }
            }
        })
    };

    let writer = {
        let fixture = Arc::clone(&fixture);
        let dead = Arc::clone(&dead);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            fixture
                .engine
                .vacuum_indexes(&users_schema(), &dead)
                .unwrap();
        })
    };

    writer.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();

    if let Some(message) = failure.lock().unwrap().take() {
        panic!("{message}");
    }
    // After the dust settles, exactly the live entries remain.
    let mut pk = pk_index_tids(&fixture.engine);
    pk.sort_by_key(|l| (l.page_num, l.slot_num));
    let mut expected: Vec<RowLocation> = live.iter().copied().collect();
    expected.sort_by_key(|l| (l.page_num, l.slot_num));
    assert_eq!(pk, expected, "only live entries remain after index vacuum");
}

#[test]
fn reclaims_committed_deleted_below_horizon() {
    let fixture = Fixture::new();
    let keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));

    // The deleter (txn 20) commits; choose a horizon above it so the committed
    // delete is universally effective.
    fixture.delete(20, 2);
    fixture.commit(20);

    let keep_bytes = fixture.physical_bytes(keep).expect("survivor is NORMAL");
    let free_before = fixture.free_bytes(keep.page_num);

    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();

    // The deleted slot is the only reclaimed TID; its line pointer is now DEAD
    // (read_row -> None) while the survivor stays NORMAL and byte-identical.
    assert_eq!(reclaimed, vec![gone]);
    assert!(fixture.physical_bytes(gone).is_none());
    assert_eq!(
        fixture.physical_bytes(keep),
        Some(keep_bytes),
        "the survivor's bytes are unchanged at its stable slot id"
    );
    assert!(
        fixture.free_bytes(keep.page_num) > free_before,
        "pruning the dead tuple reclaimed page free space"
    );
}

#[test]
fn leaves_non_dead_versions_untouched_but_resets_an_aborted_deleter() {
    let fixture = Fixture::new();
    // A live committed row (xmax == INVALID): never reclaimable, never reset.
    let live = fixture.insert_committed(10, row(1, "live"));
    // A committed delete AT the horizon (xmax == horizon): not yet reclaimable
    // (a snapshot at the boundary may still see the row live), not reset.
    let at_horizon = fixture.insert_committed(11, row(2, "at_horizon"));
    // An aborted-deleter row: the delete rolled back, the row is still live —
    // VACUUM's abort-cleanup (F4c root-cause) RESETS its stamped xmax in place.
    let aborted_delete = fixture.insert_committed(12, row(3, "aborted_delete"));
    // An in-flight deleter row: the deleter never committed/aborted, so its xmax
    // is NOT definitively settled and must NOT be reset.
    let in_flight_delete = fixture.insert_committed(13, row(4, "in_flight_delete"));

    // Stamp the deletes. xmax = horizon (40) for the boundary row; an aborted
    // deleter (41) and an in-flight deleter (42).
    fixture.delete(40, 2);
    fixture.commit(40);
    fixture.delete(41, 3);
    fixture.abort(41);
    fixture.delete(42, 4); // txn 42 left in-flight (no commit, no abort)

    // The aborted-deleter row carries xmax = 41 before VACUUM.
    let aborted_before =
        crate::codec::decode_mvcc_header(&fixture.physical_bytes(aborted_delete).unwrap()).unwrap();
    assert_eq!(aborted_before.1, 41, "aborted-deleter xmax is stamped");

    let untouched_before: Vec<_> = [live, at_horizon, in_flight_delete]
        .iter()
        .map(|&loc| fixture.physical_bytes(loc))
        .collect();

    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 40).unwrap();

    // Nothing is reclaimed: the only candidate at horizon 40 would be a committed
    // delete strictly below 40, and there is none.
    assert!(
        reclaimed.is_empty(),
        "no version is dead-to-all at horizon 40: {reclaimed:?}"
    );

    // The live, at-horizon, and in-flight-deleter rows are byte-untouched.
    for (loc, was) in [live, at_horizon, in_flight_delete]
        .iter()
        .zip(untouched_before)
    {
        assert!(fixture.is_normal(*loc), "{loc:?} must stay NORMAL");
        assert_eq!(
            fixture.physical_bytes(*loc),
            was,
            "{loc:?} bytes must be untouched"
        );
    }

    // The aborted-deleter row stays NORMAL but its xmax was reset to INVALID (the
    // rolled-back delete did not happen; the row is live again with no dangling
    // deleter), leaving NO on-disk reference to the aborted txn 41.
    assert!(fixture.is_normal(aborted_delete), "the row stays live");
    let aborted_after =
        crate::codec::decode_mvcc_header(&fixture.physical_bytes(aborted_delete).unwrap()).unwrap();
    assert_eq!(
        aborted_after.1,
        common::INVALID_XID,
        "the aborted deleter's xmax is reset to INVALID"
    );
    assert_eq!(
        aborted_after.2,
        crate::codec::INVALID_TID,
        "t_ctid is reset to the no-successor sentinel"
    );
    assert_eq!(
        aborted_after.3 & (crate::codec::HOT_UPDATED | common::XMAX_ABORTED),
        0,
        "HOT_UPDATED and the settled XMAX hint are cleared"
    );
    // xmin is preserved (the creator is unchanged).
    assert_eq!(aborted_after.0, aborted_before.0, "xmin is preserved");
}

#[test]
fn no_dead_tuples_is_a_noop() {
    let fixture = Fixture::new();
    let a = fixture.insert_committed(10, row(1, "a"));
    let b = fixture.insert_committed(11, row(2, "b"));
    let fpis_before = fixture.full_page_images().len();
    let bytes_a = fixture.physical_bytes(a);
    let bytes_b = fixture.physical_bytes(b);

    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 100).unwrap();

    assert!(reclaimed.is_empty(), "no reclaimable tuples");
    assert_eq!(
        fixture.full_page_images().len(),
        fpis_before,
        "a no-dead VACUUM appends no FullPageImage"
    );
    assert_eq!(fixture.physical_bytes(a), bytes_a, "page A is unmutated");
    assert_eq!(fixture.physical_bytes(b), bytes_b, "page B is unmutated");
}

#[test]
fn pruned_page_survives_recovery_replay() {
    let fixture = Fixture::new();
    let _keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    fixture.delete(20, 2);
    fixture.commit(20);

    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
    assert_eq!(reclaimed, vec![gone]);

    // The runtime page after pruning, captured from the buffer pool.
    let pruned = {
        let readable = fixture
            .engine
            .buffer_pool
            .read_page(TABLE_ID, gone.page_num)
            .unwrap();
        *readable.data()
    };

    // VACUUM logged exactly one FullPageImage for the pruned page; replaying it
    // onto a fresh (zeroed) page under PageLSN gating reinstalls the compacted
    // page byte-for-byte — the crash-safety guarantee (no torn page).
    let fpis: Vec<_> = fixture
        .full_page_images()
        .into_iter()
        .filter(|(page_num, _)| *page_num == gone.page_num)
        .collect();
    assert_eq!(
        fpis.len(),
        1,
        "exactly one FullPageImage per pruned page (unconditional)"
    );

    let mut recovered = [0u8; buffer::PAGE_SIZE];
    for record in fixture.wal.replay_from(0).unwrap() {
        let record = record.unwrap();
        let kind = resolve_to_raw_fpi(record.kind);
        if let WalRecordKind::FullPageImage {
            file_id, page_num, ..
        } = &kind
            && *file_id == TABLE_ID
            && *page_num == gone.page_num
        {
            crate::redo::apply_physical_redo(&mut recovered, record.lsn, &kind).unwrap();
        }
    }
    assert_eq!(
        recovered, pruned,
        "the FullPageImage reinstalls the compacted page byte-for-byte"
    );
}

#[test]
fn finds_dead_tuples_across_multiple_pages() {
    let fixture = Fixture::new();
    // Wide rows (~4 KiB) so at most two fit per 8 KiB page, forcing the dead
    // tuples onto distinct heap pages and exercising the full-extent scan.
    let wide = "x".repeat(4000);
    let mut dead: Vec<RowLocation> = Vec::new();
    let mut survivors: Vec<RowLocation> = Vec::new();
    for id in 0..6i64 {
        let txn = 10 + id as u64;
        let loc = fixture.insert_committed(txn, row(id, &wide));
        if id % 2 == 0 {
            dead.push(loc);
        } else {
            survivors.push(loc);
        }
    }

    // The dead rows span more than one heap page (the precondition the test wants
    // to prove the scan covers).
    let dead_pages: std::collections::BTreeSet<u32> = dead.iter().map(|loc| loc.page_num).collect();
    assert!(
        dead_pages.len() >= 2,
        "test setup must spread dead tuples across >=2 pages, got {dead_pages:?}"
    );

    // Delete the even-id rows (ids 0, 2, 4) under committed deleters below the
    // horizon.
    for (i, _loc) in dead.iter().enumerate() {
        let deleter = 100 + i as u64;
        let id = i as i64 * 2;
        fixture.delete(deleter, id);
        fixture.commit(deleter);
    }

    let (mut reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 200).unwrap();
    reclaimed.sort_by_key(|loc| (loc.page_num, loc.slot_num));
    let mut expected = dead.clone();
    expected.sort_by_key(|loc| (loc.page_num, loc.slot_num));

    assert_eq!(
        reclaimed, expected,
        "every dead tuple across all heap pages is reclaimed"
    );
    for loc in &dead {
        assert!(
            fixture.physical_bytes(*loc).is_none(),
            "{loc:?} is pruned to DEAD"
        );
    }
    for loc in &survivors {
        assert!(
            fixture.is_normal(*loc),
            "{loc:?} survives untouched and NORMAL"
        );
    }
}

// --- F3b: reclaim_line_pointers (DEAD -> UNUSED) + insert reuses UNUSED ---

impl Fixture {
    /// The number of slots in the heap page (the slot-array length).
    fn num_slots(&self, page_num: u32) -> u16 {
        let readable = self
            .engine
            .buffer_pool
            .read_page(TABLE_ID, page_num)
            .unwrap();
        crate::page::read_u16(readable.data(), crate::page::NUM_SLOTS_OFFSET)
    }

    /// Run the full F2b → F3a → F3b VACUUM sequence at `horizon` and return the
    /// reclaimed (now `UNUSED`) TIDs — the canonical ordering for slot reuse.
    fn vacuum_full(&self, horizon: u64) -> HashSet<RowLocation> {
        let (reclaimed, _freed) = self.engine.vacuum_heap(&users_schema(), horizon).unwrap();
        let dead: HashSet<RowLocation> = reclaimed.iter().copied().collect();
        self.engine.vacuum_indexes(&users_schema(), &dead).unwrap();
        self.engine
            .reclaim_line_pointers(&users_schema(), &dead)
            .unwrap();
        dead
    }
}

#[test]
fn reclaim_line_pointers_flips_dead_to_unused_and_logs_per_page() {
    let fixture = Fixture::new();
    let _keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    fixture.delete(20, 2);
    fixture.commit(20);

    // F2b: prune to DEAD; F3a: strip index entries; F3b: reclaim DEAD -> UNUSED.
    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
    let dead: HashSet<RowLocation> = reclaimed.iter().copied().collect();
    fixture
        .engine
        .vacuum_indexes(&users_schema(), &dead)
        .unwrap();
    let fpis_before = fixture.full_page_images().len();

    fixture
        .engine
        .reclaim_line_pointers(&users_schema(), &dead)
        .unwrap();

    // The reclaimed slot reads as absent and the page validates; F3b logs exactly
    // one FullPageImage for the single touched page.
    assert!(fixture.physical_bytes(gone).is_none());
    {
        let readable = fixture
            .engine
            .buffer_pool
            .read_page(TABLE_ID, gone.page_num)
            .unwrap();
        crate::page::validate(readable.data()).unwrap();
    }
    assert_eq!(
        fixture.full_page_images().len(),
        fpis_before + 1,
        "F3b logs one FullPageImage per reclaimed page"
    );
}

#[test]
fn reclaim_line_pointers_rejects_a_normal_slot() {
    // Calling F3b on a slot that was never pruned (still NORMAL) is a misuse:
    // `page::reclaim_line_pointers` requires DEAD and errors otherwise. This is
    // the cheap guard against gross misordering (reclaiming a never-pruned slot).
    let fixture = Fixture::new();
    let live = fixture.insert_committed(10, row(1, "live"));
    let err = fixture
        .engine
        .reclaim_line_pointers(&users_schema(), &HashSet::from([live]))
        .unwrap_err();
    assert!(
        err.message.contains("not DEAD"),
        "reclaiming a NORMAL slot must error: {}",
        err.message
    );
    assert!(fixture.is_normal(live), "the live slot is untouched");
}

#[test]
fn reclaim_line_pointers_empty_set_is_a_noop() {
    let fixture = Fixture::new();
    let _a = fixture.insert_committed(10, row(1, "a"));
    let fpis_before = fixture.full_page_images().len();
    fixture
        .engine
        .reclaim_line_pointers(&users_schema(), &HashSet::new())
        .unwrap();
    assert_eq!(
        fixture.full_page_images().len(),
        fpis_before,
        "an empty F3b set logs no WAL"
    );
}

#[test]
fn insert_reuses_a_reclaimed_unused_slot_without_growing_the_array() {
    let fixture = Fixture::new();
    let keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    // `keep` and `gone` share a page (small rows); record the slot count there.
    assert_eq!(keep.page_num, gone.page_num);
    let slots_before = fixture.num_slots(gone.page_num);

    fixture.delete(20, 2);
    fixture.commit(20);
    let dead = fixture.vacuum_full(21);
    assert!(dead.contains(&gone));

    // A new row inserted after the full VACUUM recycles the freed slot id `gone`
    // rather than appending: the slot array does not grow.
    let rid = fixture
        .engine
        .insert(&ctx(30), TABLE_ID, row(3, "new"))
        .unwrap();
    fixture.commit(30);
    assert_eq!(
        (rid.page_num, rid.slot_num),
        (gone.page_num, gone.slot_num),
        "the new row reused the freed UNUSED slot id"
    );
    assert_eq!(
        fixture.num_slots(gone.page_num),
        slots_before,
        "reusing a slot did not grow the slot array"
    );
    // The new row is readable at the reused slot, and `keep` is intact.
    assert_eq!(
        fixture.engine.get(&ctx(31), TABLE_ID, &key(3)).unwrap(),
        Some(row(3, "new"))
    );
    assert_eq!(
        fixture.engine.get(&ctx(31), TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "keep"))
    );
}

#[test]
fn insert_does_not_reuse_a_dead_slot() {
    // A DEAD slot (F2b ran, but F3a/F3b did NOT) must never be reused: it may
    // still carry an index entry. With no UNUSED slot, insert appends instead.
    let fixture = Fixture::new();
    let _keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    let slots_before = fixture.num_slots(gone.page_num);

    fixture.delete(20, 2);
    fixture.commit(20);
    // ONLY the heap prune: the slot is DEAD, not yet UNUSED.
    let (reclaimed, _freed) = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
    assert_eq!(reclaimed, vec![gone]);
    assert!(fixture.physical_bytes(gone).is_none());

    let rid = fixture
        .engine
        .insert(&ctx(30), TABLE_ID, row(3, "new"))
        .unwrap();
    fixture.commit(30);
    assert_ne!(
        (rid.page_num, rid.slot_num),
        (gone.page_num, gone.slot_num),
        "a DEAD slot must NEVER be reused by insert"
    );
    assert_eq!(
        fixture.num_slots(gone.page_num),
        slots_before + 1,
        "with no UNUSED slot, insert appended a fresh slot id"
    );
}

#[test]
fn no_stale_index_resolution_after_reclaim_and_reuse() {
    let fixture = Fixture::new();
    fixture
        .engine
        .create_index(&ctx(101), &name_index(), 0)
        .unwrap();
    fixture.commit(101);

    // Three rows; delete two and commit, then run the full VACUUM cycle.
    let keep = fixture.insert_committed(10, row(1, "keep"));
    let gone_a = fixture.insert_committed(11, row(2, "del-a"));
    let gone_b = fixture.insert_committed(12, row(3, "del-b"));
    fixture.delete(20, 2);
    fixture.commit(20);
    fixture.delete(21, 3);
    fixture.commit(21);
    let dead = fixture.vacuum_full(30);
    assert_eq!(dead, HashSet::from([gone_a, gone_b]));

    // After F3a there is NO leftover index entry for a dead TID, so no stale
    // resolution is even possible: every PK/secondary entry resolves to a live row.
    for tid in pk_index_tids(&fixture.engine)
        .iter()
        .chain(name_index_tids(&fixture.engine).iter())
    {
        assert!(!dead.contains(tid), "{tid:?} still indexed after F3a");
    }

    // Insert a new row that reuses a freed slot id; its PK and secondary entries
    // are brand new (the reclaimed slot had none).
    let rid = fixture
        .engine
        .insert(&ctx(40), TABLE_ID, row(4, "fresh"))
        .unwrap();
    fixture.commit(40);
    let reused = RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };
    assert!(
        reused == gone_a || reused == gone_b,
        "the new row reused one of the freed UNUSED slot ids: {reused:?}"
    );

    // A full PK scan returns exactly the live set {keep, fresh}: no dead key, and
    // the reused slot resolves only to the NEW row, never a stale one.
    let mut live: Vec<Row> = fixture
        .engine
        .btree(index_file_id(TABLE_ID))
        .range(&KeyRange::All)
        .unwrap()
        .into_iter()
        .filter_map(|(_, loc)| {
            fixture
                .physical_bytes(loc)
                .map(|b| crate::codec::decode_row(&users_schema(), &b).unwrap().row)
        })
        .collect();
    live.sort_by_key(|r| match &r.values[0] {
        Value::Integer(i) => *i,
        _ => unreachable!(),
    });
    assert_eq!(live, vec![row(1, "keep"), row(4, "fresh")]);

    // A point lookup on the deleted keys finds nothing; on the live keys finds the
    // right rows; the secondary index resolves "fresh" to the reused slot's row.
    assert_eq!(
        fixture.engine.get(&ctx(41), TABLE_ID, &key(2)).unwrap(),
        None
    );
    assert_eq!(
        fixture.engine.get(&ctx(41), TABLE_ID, &key(3)).unwrap(),
        None
    );
    assert_eq!(
        fixture.engine.get(&ctx(41), TABLE_ID, &key(4)).unwrap(),
        Some(row(4, "fresh"))
    );
    let _ = keep;
}

#[test]
fn reclaim_then_reuse_survives_recovery_replay() {
    let fixture = Fixture::new();
    let _keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    fixture.delete(20, 2);
    fixture.commit(20);
    let dead = fixture.vacuum_full(21);
    assert!(dead.contains(&gone));

    // Insert a new row that reuses the freed slot id (logged as a HeapInsert or a
    // FullPageImage), then capture the runtime page as the recovery target.
    let rid = fixture
        .engine
        .insert(&ctx(30), TABLE_ID, row(3, "new"))
        .unwrap();
    fixture.commit(30);
    assert_eq!(
        (rid.page_num, rid.slot_num),
        (gone.page_num, gone.slot_num),
        "the new row reused the freed slot id"
    );
    let final_page = {
        let readable = fixture
            .engine
            .buffer_pool
            .read_page(TABLE_ID, gone.page_num)
            .unwrap();
        *readable.data()
    };

    // Replay every physiological redo record for this heap page in LSN order onto
    // a fresh zeroed buffer: the reclaim (FPI: slot -> UNUSED) followed by the
    // insert-into-reused-slot (HeapInsert/FPI) must converge to the final state.
    let mut recovered = [0u8; buffer::PAGE_SIZE];
    for record in fixture.wal.replay_from(0).unwrap() {
        let record = record.unwrap();
        // `apply_physical_redo` only accepts the raw FPI variant; resolve a
        // compressed one first (a compressible reclaimed/reused page now logs
        // `FullPageImageCompressed`).
        let kind = resolve_to_raw_fpi(record.kind);
        let target = match &kind {
            WalRecordKind::HeapInit {
                file_id, page_num, ..
            }
            | WalRecordKind::HeapInsert {
                file_id, page_num, ..
            }
            | WalRecordKind::HeapUpdateHeader {
                file_id, page_num, ..
            }
            | WalRecordKind::FullPageImage {
                file_id, page_num, ..
            } => Some((*file_id, *page_num)),
            _ => None,
        };
        if target == Some((TABLE_ID, gone.page_num)) {
            crate::redo::apply_physical_redo(&mut recovered, record.lsn, &kind).unwrap();
        }
    }
    assert_eq!(
        recovered, final_page,
        "reclaim + insert-into-reused-slot replays to the final state"
    );
    // And the recovered page resolves the reused slot to the NEW row.
    let bytes = crate::page::read_row(&recovered, gone.slot_num)
        .unwrap()
        .expect("reused slot is NORMAL after replay");
    assert_eq!(
        crate::codec::decode_row(&users_schema(), &bytes)
            .unwrap()
            .row,
        row(3, "new")
    );
}

#[test]
fn vacuum_txn_is_the_recovery_maintenance_id() {
    // VACUUM stamps its pages under txn 0 (the recovery/maintenance convention),
    // never a user txn id: its reclamation must not be undone by an abort.
    assert_eq!(VACUUM_TXN, 0);
}

// --- F4a: the `engine.vacuum` orchestration (F2b -> F3a -> F3b in one call) ---

#[test]
fn vacuum_orchestrates_heap_index_and_line_pointers_in_order() {
    let fixture = Fixture::new();
    fixture
        .engine
        .create_index(&ctx(101), &name_index(), 0)
        .unwrap();
    fixture.commit(101);

    let keep = fixture.insert_committed(10, row(1, "keep"));
    let gone = fixture.insert_committed(11, row(2, "gone"));
    fixture.delete(20, 2);
    fixture.commit(20);

    // Before the deleted entry still dangles in both indexes.
    assert!(pk_index_tids(&fixture.engine).contains(&gone));
    assert!(name_index_tids(&fixture.engine).contains(&gone));

    // One `vacuum` call runs F2b -> F3a -> F3b: prune the heap, strip index
    // entries, reclaim the line pointer. It reports one reclaimed TID.
    let reclaimed = fixture.engine.vacuum(&users_schema(), 30).unwrap();
    assert_eq!(reclaimed, 1, "exactly the deleted TID is reclaimed");

    // Heap slot is reclaimed (reads as absent); both index entries are gone; the
    // live row's entries survive in both indexes.
    assert!(
        fixture.physical_bytes(gone).is_none(),
        "dead slot reclaimed"
    );
    assert_eq!(pk_index_tids(&fixture.engine), vec![keep]);
    assert_eq!(name_index_tids(&fixture.engine), vec![keep]);
    assert!(fixture.is_normal(keep), "the live row survives untouched");

    // The reclaimed slot id is now UNUSED and a new insert reuses it — proof F3b
    // ran (a still-DEAD slot would not be recycled).
    let rid = fixture
        .engine
        .insert(&ctx(40), TABLE_ID, row(3, "new"))
        .unwrap();
    fixture.commit(40);
    assert_eq!(
        (rid.page_num, rid.slot_num),
        (gone.page_num, gone.slot_num),
        "the reclaimed slot id is reused by a later insert"
    );

    // The live row and the new row both resolve; the resurrected-dead row does not.
    let reader = ctx(50);
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "keep"))
    );
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(3)).unwrap(),
        Some(row(3, "new"))
    );
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(2)).unwrap(),
        None,
        "the vacuumed row stays gone"
    );
}

#[test]
fn vacuum_with_nothing_dead_reclaims_zero_and_logs_no_wal() {
    let fixture = Fixture::new();
    let live = fixture.insert_committed(10, row(1, "live"));
    let fpis_before = fixture.full_page_images().len();

    // No committed-deleted version below the horizon: F2b finds nothing, so F3a/F3b
    // are skipped — zero reclaimed, no FullPageImage logged.
    let reclaimed = fixture.engine.vacuum(&users_schema(), 30).unwrap();
    assert_eq!(reclaimed, 0);
    assert_eq!(
        fixture.full_page_images().len(),
        fpis_before,
        "a no-dead VACUUM logs no WAL"
    );
    assert!(fixture.is_normal(live), "the live row is untouched");
}

#[test]
fn vacuum_retains_a_version_a_horizon_below_the_delete_still_protects() {
    // The horizon-safety invariant at the engine level: a committed DELETE at
    // xmax = 50 is reclaimable ONLY when the horizon is above 50. With a horizon of
    // 50 (a live snapshot froze its xmin at 50 and can still see the row live), the
    // version is NOT below the horizon, so VACUUM must retain it — no data loss.
    let fixture = Fixture::new();
    let row_loc = fixture.insert_committed(10, row(1, "protected"));
    fixture.delete(50, 1);
    fixture.commit(50);

    // Horizon = 50: 50 < 50 is false, so the version is NOT dead-to-all. VACUUM
    // reclaims nothing and the row is still physically present (a snapshot with
    // xmin = 50 that sees the delete in-flight would still resolve it).
    let reclaimed = fixture.engine.vacuum(&users_schema(), 50).unwrap();
    assert_eq!(
        reclaimed, 0,
        "a version the horizon protects is NOT reclaimed"
    );
    assert!(
        fixture.is_normal(row_loc),
        "the protected version is retained in the heap"
    );
    assert!(
        pk_index_tids(&fixture.engine).contains(&row_loc),
        "its index entry is retained too"
    );

    // Once the horizon advances past the deleter (51 > 50), the version becomes
    // reclaimable and VACUUM frees it.
    let reclaimed = fixture.engine.vacuum(&users_schema(), 51).unwrap();
    assert_eq!(reclaimed, 1, "above the deleter the version is reclaimed");
    assert!(fixture.physical_bytes(row_loc).is_none());
    assert!(!pk_index_tids(&fixture.engine).contains(&row_loc));
}
