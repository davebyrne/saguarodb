use std::sync::Arc;

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use common::{
    ColumnDef, CompressionSetting, DataType, INVALID_XID, IndexSchema, Key, KeyRange,
    PageFlushInfo, RelationKind, Row, RowId, Snapshot, SqlState, StatementContext, TableSchema,
    ToastOptions, Value,
};
use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

use super::PageBackedStorageEngine;
use super::conflict_wait_test_support::{aborting_blocker, committing_blocker};
use crate::HeapPageStore;
use crate::traits::{SchemaOperations, StorageEngine};

struct AlwaysFlush;
impl common::FlushPolicy for AlwaysFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        true
    }
}

/// A storage engine over an in-memory buffer pool and a real (file-backed) WAL,
/// whose CLOG the tests drive via `Commit`/`Abort` records to control which
/// `xmin`/`xmax` are committed/aborted/in-progress.
struct Fixture {
    engine: PageBackedStorageEngine,
    wal: Arc<FileWalManager>,
    _dir: tempfile::TempDir,
}

const TABLE_ID: u32 = 1;

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
        Self {
            engine,
            wal,
            _dir: dir,
        }
    }

    /// Append a `Commit` for `txn_id` and flush so the CLOG records it
    /// `Committed` (flush is what settles a commit).
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

    /// Append an `Abort` for `txn_id` so the CLOG records it `Aborted`.
    fn abort(&self, txn_id: u64) {
        self.wal
            .append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Abort,
            })
            .unwrap();
    }

    /// Stamp a deleter (`xmax`) on the heap tuple at `(page_num, slot)` of the
    /// users table, simulating an in-place DELETE before versioning writes (B4)
    /// are wired. Mirrors the eventual engine path: append a `HeapUpdateHeader`
    /// record for a real LSN, then mutate the header in place. `t_ctid` stays
    /// the no-successor sentinel; `infomask` is the caller's hint bits.
    fn stamp_xmax(&self, page_num: u32, slot: u16, xmax: u64, infomask: u16) {
        let lsn = self
            .wal
            .append(WalRecord {
                lsn: 0,
                txn_id: xmax,
                kind: WalRecordKind::HeapUpdateHeader {
                    file_id: TABLE_ID,
                    page_num,
                    slot,
                    xmax,
                    t_ctid: crate::codec::INVALID_TID,
                    infomask,
                },
            })
            .unwrap();
        let mut guard = self
            .engine
            .buffer_pool
            .write_page(TABLE_ID, page_num, xmax)
            .unwrap();
        crate::page::set_tuple_header(
            guard.data_mut(),
            slot,
            xmax,
            crate::codec::INVALID_TID,
            infomask,
            lsn,
        )
        .unwrap();
    }

    /// The heap TIDs the primary-key index carries for `key`, read straight
    /// from the B-tree (no visibility filtering), so a test can assert that a
    /// deleted version's index entry is *retained* rather than removed.
    fn pk_index_tids(&self, key: &Key) -> Vec<super::RowLocation> {
        self.engine
            .btree(crate::heap::index_file_id(TABLE_ID))
            .scan_key(key)
            .unwrap()
    }

    /// The heap TIDs secondary index `index_id` carries for a textual `name`
    /// value, read straight from the B-tree (no visibility filtering), so an
    /// UPDATE test can assert that *both* the old and new versions hold a
    /// per-version entry (one entry per version) under the same value.
    fn secondary_index_tids(&self, index_id: u32, name: &str) -> Vec<super::RowLocation> {
        self.engine
            .secondary_btree(index_id)
            .scan_key(&Key(vec![Value::Text(name.to_string())]))
            .unwrap()
    }

    /// Decode the *physical* tuple header at `location` (ignoring snapshot
    /// visibility). Returns `None` when the line pointer is not NORMAL/live
    /// (DEAD/UNUSED), so a caller can assert both "the slot is still NORMAL"
    /// and "xmax was stamped".
    fn decode_physical(&self, location: super::RowLocation) -> Option<crate::codec::DecodedRow> {
        let readable = self
            .engine
            .buffer_pool
            .read_page(location.file_id, location.page_num)
            .unwrap();
        let bytes = crate::page::read_row(readable.data(), location.slot_num).unwrap()?;
        Some(crate::codec::decode_row(&users_schema(), &bytes).unwrap())
    }

    // --- H1 HOT-chain synthesis helpers (no H2/H3 production path yet) ---

    /// Append a raw tuple for `row` (creator `xmin`) directly onto an existing
    /// heap `page_num`, stamping `infomask` (e.g. `HEAP_ONLY`) and an `xmax`
    /// in place, and return its new slot. Used to build a synthetic heap-only
    /// successor that — by HOT design — has NO index entry of its own, so it is
    /// reachable only by walking `t_ctid` from its root.
    fn append_raw_tuple(
        &self,
        page_num: u32,
        row: &Row,
        xmin: u64,
        xmax: u64,
        infomask: u16,
    ) -> u16 {
        let bytes = crate::codec::encode_row(&users_schema(), row, xmin).unwrap();
        let mut guard = self
            .engine
            .buffer_pool
            .write_page(TABLE_ID, page_num, xmin)
            .unwrap();
        let slot = crate::page::insert_row(guard.data_mut(), &bytes).unwrap();
        // Stamp xmax/infomask on the freshly inserted NORMAL slot (its t_ctid
        // stays the no-successor sentinel until a caller chains it).
        let lsn = crate::page::page_lsn(guard.data());
        crate::page::set_tuple_header(
            guard.data_mut(),
            slot,
            xmax,
            crate::codec::INVALID_TID,
            infomask,
            lsn,
        )
        .unwrap();
        slot
    }

    /// Chain the tuple at `(page_num, slot)` forward to `successor` on the same
    /// page: stamp `xmax`, `t_ctid -> successor`, and `infomask` (e.g.
    /// `HOT_UPDATED`) — the root side of a HOT update. The slot must be NORMAL.
    fn chain_to(&self, page_num: u32, slot: u16, successor: u16, xmax: u64, infomask: u16) {
        let mut guard = self
            .engine
            .buffer_pool
            .write_page(TABLE_ID, page_num, xmax)
            .unwrap();
        let lsn = crate::page::page_lsn(guard.data());
        crate::page::set_tuple_header(
            guard.data_mut(),
            slot,
            xmax,
            (page_num, successor),
            infomask,
            lsn,
        )
        .unwrap();
    }

    /// Overwrite the line pointer at `(page_num, slot)` with a `REDIRECT` to
    /// `target` on the same page (the H3 pruning result, synthesized here).
    fn make_redirect(&self, page_num: u32, slot: u16, target: u16) {
        let mut guard = self
            .engine
            .buffer_pool
            .write_page(TABLE_ID, page_num, 0)
            .unwrap();
        crate::page::set_redirect(guard.data_mut(), slot, target).unwrap();
    }

    /// Resolve `key` to the visible version's `(RowLocation, infomask)` via the
    /// engine's HOT-aware `locate_visible_version` (REDIRECT + bounded chain),
    /// the path UPDATE/DELETE use to target the live version.
    fn locate(
        &self,
        key: &Key,
        snapshot: Snapshot,
        current_txn: u64,
    ) -> Option<(super::RowLocation, u16)> {
        let btree = self.engine.btree(crate::heap::index_file_id(TABLE_ID));
        self.engine
            .locate_visible_version(&btree, key, &snapshot, &[current_txn])
            .unwrap()
    }

    /// Like [`Self::locate`] but for the 3-column HOT schema (`hot_schema`).
    fn locate_hot(
        &self,
        key: &Key,
        snapshot: Snapshot,
        current_txn: u64,
    ) -> Option<(super::RowLocation, u16)> {
        let btree = self.engine.btree(crate::heap::index_file_id(TABLE_ID));
        self.engine
            .locate_visible_version(&btree, key, &snapshot, &[current_txn])
            .unwrap()
    }

    /// The line-pointer state of `(page_num, slot)` (NORMAL/DEAD/UNUSED/REDIRECT),
    /// so an H3 collapse test can assert the root became a REDIRECT and dead
    /// members became UNUSED.
    fn slot_state_hot(&self, page_num: u32, slot: u16) -> crate::page::LinePointer {
        let readable = self
            .engine
            .buffer_pool
            .read_page(TABLE_ID, page_num)
            .unwrap();
        crate::page::slot_state(readable.data(), slot).unwrap()
    }

    /// The page's current free byte count, to assert HOT-collapse reclaimed space.
    fn free_bytes_hot(&self, page_num: u32) -> usize {
        let readable = self
            .engine
            .buffer_pool
            .read_page(TABLE_ID, page_num)
            .unwrap();
        let data = readable.data();
        let num_slots = crate::page::read_u16(data, crate::page::NUM_SLOTS_OFFSET);
        let free_start = crate::page::read_u16(data, crate::page::FREE_SPACE_OFFSET) as usize;
        let slot_array = buffer::PAGE_SIZE - (num_slots as usize) * crate::page::SLOT_LEN;
        slot_array.saturating_sub(free_start)
    }
}

fn ctx(txn_id: u64, snapshot: Snapshot) -> StatementContext {
    StatementContext::with_snapshot(txn_id, std::sync::Arc::new(snapshot))
}

/// Like [`ctx`] but carries an explicit GC horizon (for the H3 update-path prune).
fn ctx_h(txn_id: u64, snapshot: Snapshot, gc_horizon: u64) -> StatementContext {
    StatementContext::with_snapshot(txn_id, std::sync::Arc::new(snapshot))
        .with_gc_horizon(gc_horizon)
}

/// A snapshot that sees every settled (committed) id below `xmax` except the
/// listed in-progress ids, none of which are own writes.
fn snapshot(xmax: u64, xip: Vec<u64>) -> Snapshot {
    Snapshot { xmin: 1, xmax, xip }
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
        toast: ToastOptions::legacy_catalog_default(),
        toast_table_id: None,
        relation_kind: RelationKind::User,
    }
}

fn name_index() -> IndexSchema {
    IndexSchema {
        id: 1,
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

fn key(id: i64) -> Key {
    Key(vec![Value::Integer(id)])
}

/// Insert three rows whose creating transactions are, respectively, committed,
/// in-progress, and aborted; settle the CLOG accordingly. Returns the fixture
/// with the table created. The reader uses `READER`/its snapshot to scan.
fn fixture_with_mixed_visibility() -> Fixture {
    let fixture = Fixture::new();
    // DDL under a committed setup transaction.
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture.commit(100);

    // Committed creator (txn 10): visible.
    fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            row(1, "committed"),
        )
        .unwrap();
    fixture.commit(10);

    // In-progress creator (txn 20): never settled ⇒ hidden.
    fixture
        .engine
        .insert(
            &ctx(20, snapshot(21, vec![])),
            TABLE_ID,
            row(2, "in_progress"),
        )
        .unwrap();

    // Aborted creator (txn 30): hidden.
    fixture
        .engine
        .insert(&ctx(30, snapshot(31, vec![])), TABLE_ID, row(3, "aborted"))
        .unwrap();
    fixture.abort(30);

    fixture
}

/// The reader's snapshot: the future starts at 40 (so 10/20/30 are in the
/// past), txn 20 is in-progress (in `xip`), and the reader is not its own txn
/// (current_txn 0), so visibility is settled purely by the CLOG.
fn reader_snapshot() -> Snapshot {
    snapshot(40, vec![20])
}

#[test]
fn seq_scan_skips_invisible_versions() {
    let fixture = fixture_with_mixed_visibility();
    let mut iter = fixture
        .engine
        .scan_range(&ctx(0, reader_snapshot()), TABLE_ID, &KeyRange::All)
        .unwrap();

    let mut names = Vec::new();
    while let Some(stored) = iter.next().unwrap() {
        names.push(stored.row.values[1].clone());
    }
    // Only the committed row survives; the in-progress and aborted creators are
    // hidden by the visibility predicate.
    assert_eq!(names, vec![Value::Text("committed".to_string())]);
}

#[test]
fn point_lookup_hides_invisible_and_shows_committed() {
    let fixture = fixture_with_mixed_visibility();
    let reader = ctx(0, reader_snapshot());

    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "committed"))
    );
    // In-progress creator: hidden, not an error.
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(2)).unwrap(),
        None
    );
    // Aborted creator: hidden, not an error.
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(3)).unwrap(),
        None
    );
}

#[test]
fn index_scan_skips_invisible_versions_without_erroring() {
    let fixture = fixture_with_mixed_visibility();
    // Build the secondary index after the rows exist, under a committed txn.
    // Backfill reads not-dead-to-all physical rows (not snapshot-filtered), so
    // the in-progress row gets an index entry even though it is invisible to
    // this reader. The scan must then skip invisible entries at the heap, not
    // error.
    let builder = ctx(101, snapshot(102, vec![]));
    fixture
        .engine
        .create_index(&builder, &name_index(), 0)
        .unwrap();
    fixture.commit(101);

    let mut iter = fixture
        .engine
        .index_scan(
            &ctx(0, reader_snapshot()),
            TABLE_ID,
            name_index().id,
            &KeyRange::All,
        )
        .unwrap();

    let mut names = Vec::new();
    while let Some(stored) = iter.next().unwrap() {
        names.push(stored.row.values[1].clone());
    }
    // Only the committed row is visible; any invisible index entries are skipped
    // rather than returned or erroring.
    assert_eq!(names, vec![Value::Text("committed".to_string())]);
}

#[test]
fn degenerate_snapshot_shows_all_committed_and_own_writes() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture.commit(100);

    // Insert a committed row (txn 10) and an own-write row under the reader's
    // own txn (txn 50, never committed) — both must be visible to txn 50 under
    // the degenerate snapshot (empty xip, sees all committed + own writes).
    fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            row(1, "committed"),
        )
        .unwrap();
    fixture.commit(10);
    fixture
        .engine
        .insert(
            &ctx(50, snapshot(51, vec![])),
            TABLE_ID,
            row(2, "own_write"),
        )
        .unwrap();

    // The degenerate autocommit snapshot for txn 50: empty xip, xmax past every
    // allocated id. Own write (txn 50) is seen via current_txn; committed rows
    // are seen via the CLOG.
    let mut iter = fixture
        .engine
        .scan_range(&ctx(50, snapshot(60, vec![])), TABLE_ID, &KeyRange::All)
        .unwrap();
    let mut names = Vec::new();
    while let Some(stored) = iter.next().unwrap() {
        names.push(stored.row.values[1].clone());
    }
    assert_eq!(
        names,
        vec![
            Value::Text("committed".to_string()),
            Value::Text("own_write".to_string()),
        ]
    );
}

// --- MVCC-aware uniqueness (Milestone B commit 7) ---

/// A committed, live version holding a primary key blocks a re-insert of that
/// key with `UniqueViolation`. This is the single-version baseline preserved by
/// the visibility-aware check.
#[test]
fn unique_live_committed_pk_conflicts() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture.commit(100);

    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "alive"))
        .unwrap();
    fixture.commit(10);

    let err = fixture
        .engine
        .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(1, "dup"))
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::UniqueViolation);
}

/// A primary key whose only existing version had an **aborted creator** is dead;
/// re-inserting that key succeeds (no conflict). The version is planted by
/// inserting under a creator txn and then aborting it.
#[test]
fn unique_aborted_creator_pk_does_not_conflict() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture.commit(100);

    // Creator txn 10 inserts key 1, then aborts ⇒ the version is dead.
    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "aborted"))
        .unwrap();
    fixture.abort(10);

    // A fresh committed txn re-inserts key 1: the dead version must not block it.
    fixture
        .engine
        .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(1, "reinsert"))
        .unwrap();
    fixture.commit(11);

    // The live version is the one that survives.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(20, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "reinsert"))
    );
}

/// A primary key whose only existing version is **committed-deleted** (its
/// `xmax` committed) is dead; re-inserting that key succeeds. The deletion is
/// planted by stamping `xmax` in place (versioning DELETE is not wired yet) and
/// committing the deleter.
#[test]
fn unique_committed_deleted_pk_does_not_conflict() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture.commit(100);

    // Creator txn 10 inserts key 1 (committed-live).
    let rid = fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "deleted"))
        .unwrap();
    fixture.commit(10);

    // Deleter txn 20 stamps xmax in place and commits ⇒ the version is gone.
    fixture.stamp_xmax(rid.page_num, rid.slot_num, 20, common::XMAX_COMMITTED);
    fixture.commit(20);

    // Re-insert key 1: the committed-deleted version must not block it.
    fixture
        .engine
        .insert(&ctx(21, snapshot(22, vec![])), TABLE_ID, row(1, "reinsert"))
        .unwrap();
    fixture.commit(21);

    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(30, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "reinsert"))
    );
}

/// A **committed-but-aborted-delete** version is still alive and conflicts: a
/// version with a committed creator and an *aborted* `xmax` blocks a re-insert.
/// Guards against treating any non-INVALID `xmax` as "deleted".
#[test]
fn unique_aborted_delete_pk_still_conflicts() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture.commit(100);

    let rid = fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "alive"))
        .unwrap();
    fixture.commit(10);

    // Deleter txn 20 stamps xmax but aborts ⇒ the delete never happened.
    fixture.stamp_xmax(rid.page_num, rid.slot_num, 20, common::XMAX_ABORTED);
    fixture.abort(20);

    let err = fixture
        .engine
        .insert(&ctx(21, snapshot(22, vec![])), TABLE_ID, row(1, "dup"))
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::UniqueViolation);
}

/// The same liveness rule governs unique **secondary** indexes: an aborted
/// creator's secondary entry does not block a duplicate non-NULL value.
#[test]
fn unique_secondary_aborted_creator_does_not_conflict() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    let unique_name = IndexSchema {
        id: 1,
        table: TABLE_ID,
        name: "users_name_unique".to_string(),
        columns: vec![1],
        unique: true,
    };
    fixture
        .engine
        .create_index(&setup, &unique_name, 0)
        .unwrap();
    fixture.commit(100);

    // Creator txn 10 inserts (id 1, name "amy"), then aborts ⇒ dead version.
    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "amy"))
        .unwrap();
    fixture.abort(10);

    // A different row with the SAME unique name must be accepted: the dead
    // version does not occupy the unique key.
    fixture
        .engine
        .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(2, "amy"))
        .unwrap();
    fixture.commit(11);

    // A committed-live duplicate name is still rejected.
    let err = fixture
        .engine
        .insert(&ctx(12, snapshot(13, vec![])), TABLE_ID, row(3, "amy"))
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::UniqueViolation);
}

// --- Concurrent-inserter unique conflicts: wait, then 23505 or success ---
//
// A key held by another transaction's still-uncommitted insert is undecidable:
// the inserter cannot tell whether it is a true duplicate (that txn may yet
// abort), so the racer blocks on that transaction, then re-checks: commit becomes
// a definite `UniqueViolation` (23505), abort frees the key. These are planted
// with the CLOG-driving fixture: insert under a creator txn and leave it
// in-progress (no Commit/Abort) to model the concurrent uncommitted inserter, then
// commit or abort it from the waiter to settle the outcome.

/// A committed table with a (non-unique by default) `users_name` secondary index.
fn fixture_with_table_and_name_index() -> Fixture {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    fixture.commit(100);
    fixture
}

/// INSERT racing an **in-progress** other inserter of the same primary key BLOCKS
/// on that inserter (no fail-fast). When the holder commits during the wait, the
/// racer re-checks and gets a definite `UniqueViolation` (23505)
/// (`docs/specs/deadlock.md`).
#[test]
fn insert_pk_blocks_on_in_flight_holder_then_unique_violation() {
    let fixture = fixture_with_table_and_name_index();

    // Creator txn 10 inserts key 1 and is left in-progress (no commit/abort).
    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "inflight"))
        .unwrap();

    // Txn 11 races: it blocks on txn 10; the waiter commits 10 during the wait, so
    // the retry sees a committed duplicate.
    let err = fixture
        .engine
        .insert(
            &committing_blocker(ctx(11, snapshot(12, vec![])), fixture.wal.clone()),
            TABLE_ID,
            row(1, "racer"),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::UniqueViolation);
}

/// If the in-flight holder **aborts** during the wait instead, its version is dead,
/// so the blocked racer's re-check finds the key free and the INSERT proceeds.
#[test]
fn insert_pk_blocks_on_in_flight_holder_then_succeeds_when_holder_aborts() {
    let fixture = fixture_with_table_and_name_index();

    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "inflight"))
        .unwrap();

    // Txn 11 blocks on txn 10; the waiter aborts 10, so the retry finds the key free.
    fixture
        .engine
        .insert(
            &aborting_blocker(ctx(11, snapshot(12, vec![])), fixture.wal.clone()),
            TABLE_ID,
            row(1, "racer"),
        )
        .unwrap();
    fixture.commit(11);
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(20, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "racer"))
    );
}

/// If the in-flight creator **aborts** instead, its version is dead, so a later
/// INSERT of that key succeeds (no conflict).
#[test]
fn insert_pk_in_flight_then_aborted_succeeds() {
    let fixture = fixture_with_table_and_name_index();

    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "inflight"))
        .unwrap();
    fixture.abort(10);

    // The aborted version does not occupy the key ⇒ the re-insert succeeds.
    fixture
        .engine
        .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(1, "winner"))
        .unwrap();
    fixture.commit(11);

    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(20, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "winner"))
    );
}

/// A committed table with a UNIQUE `users_name` secondary index.
fn fixture_with_unique_name_index() -> Fixture {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    let unique_name = IndexSchema {
        id: 1,
        table: TABLE_ID,
        name: "users_name_unique".to_string(),
        columns: vec![1],
        unique: true,
    };
    fixture
        .engine
        .create_index(&setup, &unique_name, 0)
        .unwrap();
    fixture.commit(100);
    fixture
}

/// A duplicate unique SECONDARY-index name held by an in-progress inserter BLOCKS
/// the racer (no fail-fast); when the holder commits during the wait, the racer
/// gets a definite `UniqueViolation` (23505) on the secondary index
/// (`docs/specs/deadlock.md`).
#[test]
fn insert_unique_secondary_blocks_on_in_flight_then_violation() {
    let fixture = fixture_with_unique_name_index();

    // Creator txn 10 inserts (id 1, name "amy") and is left in-progress.
    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "amy"))
        .unwrap();

    // A DIFFERENT pk with the same unique name ⇒ the conflict is on the secondary
    // index, not the PK. Txn 11 blocks on txn 10; the waiter commits 10, so the
    // retry sees a committed duplicate unique name.
    let err = fixture
        .engine
        .insert(
            &committing_blocker(ctx(11, snapshot(12, vec![])), fixture.wal.clone()),
            TABLE_ID,
            row(2, "amy"),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::UniqueViolation);
}

/// Unique-secondary in-flight holder that **aborts** ⇒ a later insert of the same
/// unique name succeeds.
#[test]
fn insert_unique_secondary_in_flight_then_aborted_succeeds() {
    let fixture = fixture_with_unique_name_index();

    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "amy"))
        .unwrap();
    fixture.abort(10);

    fixture
        .engine
        .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(2, "amy"))
        .unwrap();
    fixture.commit(11);

    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(20, vec![])), TABLE_ID, &key(2))
            .unwrap(),
        Some(row(2, "amy"))
    );
}

/// Multiple NULL indexed values under a UNIQUE secondary index still coexist:
/// the NULL-secondary skip is preserved (SQL treats NULLs as distinct), so an
/// in-flight NULL holder never yields 40001 either.
#[test]
fn insert_unique_secondary_multiple_nulls_allowed_with_in_flight_holder() {
    let fixture = fixture_with_unique_name_index();

    // Creator txn 10 inserts a NULL-name row and is left in-progress.
    fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            Row {
                values: vec![Value::Integer(1), Value::Null],
            },
        )
        .unwrap();

    // A second NULL-name row (different pk) is accepted despite the in-flight
    // holder: the unique check is skipped for NULL ⇒ no 40001 and no 23505.
    fixture
        .engine
        .insert(
            &ctx(11, snapshot(12, vec![])),
            TABLE_ID,
            Row {
                values: vec![Value::Integer(2), Value::Null],
            },
        )
        .unwrap();
    fixture.commit(11);
}

// --- MVCC DELETE: stamp xmax in place, retain entries (Milestone B commit 8) ---

/// A committed table with one committed-live row and a `users_name` secondary
/// index, ready for the DELETE tests below.
fn fixture_with_one_row_and_index() -> (Fixture, RowId) {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    fixture
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    fixture.commit(100);

    let rid = fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "alive"))
        .unwrap();
    fixture.commit(10);
    (fixture, rid)
}

/// A committed DELETE hides the row from a *later* snapshot through both a
/// sequential scan and a secondary index scan — external behavior is unchanged.
#[test]
fn committed_delete_hides_row_from_seq_and_index_scans() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    // Deleter txn 20 (degenerate own snapshot) removes the row, then commits.
    assert!(
        fixture
            .engine
            .delete(&ctx(20, snapshot(21, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(20);

    // A reader whose snapshot is after the deleter sees no row, via either scan.
    let reader = ctx(0, snapshot(30, vec![]));

    let mut seq = fixture
        .engine
        .scan_range(&reader, TABLE_ID, &KeyRange::All)
        .unwrap();
    assert!(seq.next().unwrap().is_none());

    let mut idx = fixture
        .engine
        .index_scan(&reader, TABLE_ID, name_index().id, &KeyRange::All)
        .unwrap();
    assert!(idx.next().unwrap().is_none());

    // And a point get is hidden too.
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        None
    );
}

/// MVCC DELETE stamps `xmax` on a *NORMAL* line pointer in place and **retains**
/// the index entries: the tuple lingers physically (no tombstone) and the
/// primary-key index still points at it (VACUUM reclaims both later).
#[test]
fn delete_keeps_slot_normal_stamps_xmax_and_retains_index_entry() {
    let (fixture, rid) = fixture_with_one_row_and_index();
    let location = super::RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };

    // Before: the PK index has one entry and the slot is NORMAL (decodes, no xmax).
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![location]);
    let before = fixture.decode_physical(location).expect("slot is NORMAL");
    assert_eq!(before.xmax, common::INVALID_XID);

    assert!(
        fixture
            .engine
            .delete(&ctx(20, snapshot(21, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(20);

    // After: the line pointer is still NORMAL (decode succeeds, not DEAD) and
    // carries xmax = the deleter; the index entry is unchanged (retained).
    let after = fixture
        .decode_physical(location)
        .expect("slot stays NORMAL after an MVCC delete");
    assert_eq!(after.xmax, 20);
    assert_eq!(after.t_ctid, crate::codec::INVALID_TID);
    assert_eq!(after.row, row(1, "alive"));
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![location]);
}

/// DELETE then re-INSERT of the same primary key now SUCCEEDS: the
/// committed-deleted version no longer blocks the re-insert (the new capability
/// this commit unlocks). The live version is the re-inserted one.
#[test]
fn delete_then_reinsert_same_pk_succeeds() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    assert!(
        fixture
            .engine
            .delete(&ctx(20, snapshot(21, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(20);

    // Re-insert the same key: the committed-deleted version does not conflict.
    fixture
        .engine
        .insert(
            &ctx(21, snapshot(22, vec![])),
            TABLE_ID,
            row(1, "reinserted"),
        )
        .unwrap();
    fixture.commit(21);

    // The live version is the re-inserted row, visible to a later snapshot.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(30, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "reinserted"))
    );
    // Internally both versions' PK entries linger (the old deleted one and the
    // new live one), pending VACUUM.
    assert_eq!(fixture.pk_index_tids(&key(1)).len(), 2);
}

/// Deleting a key with no visible version is a no-op (`Ok(false)`), matching the
/// missing-row semantics: a second DELETE of an already-deleted key affects no
/// row.
#[test]
fn delete_of_already_deleted_key_is_a_no_op() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    assert!(
        fixture
            .engine
            .delete(&ctx(20, snapshot(21, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(20);

    // The row is already committed-deleted; a later deleter sees nothing to
    // delete.
    assert!(
        !fixture
            .engine
            .delete(&ctx(21, snapshot(22, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
}

/// An *aborted* DELETE leaves the row visible: the stamped `xmax` belongs to an
/// aborted deleter, so the delete never took effect.
#[test]
fn aborted_delete_leaves_row_visible() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    assert!(
        fixture
            .engine
            .delete(&ctx(20, snapshot(21, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.abort(20);

    // The deleter aborted, so a later reader still sees the row.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(30, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "alive"))
    );
}

// --- MVCC UPDATE: write a new version, chain the old, all-index entries
//     (Milestone B commit 9) ---

/// A committed UPDATE is seen by a *later* snapshot through a sequential scan, an
/// index scan on the **changed** column value, AND an index scan on an
/// **unchanged** secondary value — the last proves the new version got an entry
/// in the unchanged-column index too (the anti-HOT-bug check: every index gets a
/// per-version entry, not only changed-column indexes).
#[test]
fn committed_update_is_visible_via_seq_and_both_secondary_scans() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    // Two secondary indexes: one on `name` (changed by the update), one on `id`
    // (an unchanged column). The unchanged-column index must still gain a new
    // entry for the new version.
    let name_idx = name_index();
    let id_idx = IndexSchema {
        id: 2,
        table: TABLE_ID,
        name: "users_id".to_string(),
        columns: vec![0],
        unique: false,
    };
    fixture.engine.create_index(&setup, &name_idx, 0).unwrap();
    fixture.engine.create_index(&setup, &id_idx, 0).unwrap();
    fixture.commit(100);

    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "old"))
        .unwrap();
    fixture.commit(10);

    // Update the name "old" -> "new" (id unchanged) under txn 20, then commit.
    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                row(1, "new")
            )
            .unwrap()
    );
    fixture.commit(20);

    let reader = ctx(0, snapshot(30, vec![]));

    // Sequential scan sees the new value.
    let mut seq = fixture
        .engine
        .scan_range(&reader, TABLE_ID, &KeyRange::All)
        .unwrap();
    let stored = seq.next().unwrap().unwrap();
    assert_eq!(stored.row, row(1, "new"));
    assert!(seq.next().unwrap().is_none());

    // Index scan on the CHANGED column (name = "new") returns the new version;
    // the old value "old" returns nothing (the old version is superseded).
    let by_new_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_idx.id, &name_eq("new"))
            .unwrap(),
    );
    assert_eq!(by_new_name, vec![row(1, "new")]);
    let by_old_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_idx.id, &name_eq("old"))
            .unwrap(),
    );
    assert!(by_old_name.is_empty());

    // Index scan on the UNCHANGED column (id = 1) ALSO returns the new version:
    // the new tuple got its own entry in the unchanged-column index. Were the
    // engine to skip unchanged-column indexes (the HOT optimization), the id
    // index's only entry would point at the now-superseded old version and this
    // scan would wrongly return the old row — or, with visibility filtering,
    // nothing.
    let by_id = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, id_idx.id, &KeyRange::Exact(key(1)))
            .unwrap(),
    );
    assert_eq!(by_id, vec![row(1, "new")]);
}

/// Internally both versions coexist after an UPDATE: the old version is stamped
/// `xmax = txn` with `t_ctid` pointing at the new version (the forward chain),
/// and the new version is live (`xmax = INVALID`, `t_ctid = INVALID`). Asserted
/// via physical header decode. Both PK index entries linger (one per version).
#[test]
fn update_chains_old_to_new_and_keeps_both_versions() {
    let (fixture, rid) = fixture_with_one_row_and_index();
    let old_location = super::RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };

    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                row(1, "updated"),
            )
            .unwrap()
    );
    fixture.commit(20);

    // Two PK entries now: the old (superseded) one and the new (live) one.
    let tids = fixture.pk_index_tids(&key(1));
    assert_eq!(tids.len(), 2);
    let new_location = *tids.iter().find(|loc| **loc != old_location).unwrap();

    // The old version is stamped xmax = 20 and chained forward to the new TID,
    // and its slot stays NORMAL (decodes).
    let old = fixture
        .decode_physical(old_location)
        .expect("old slot stays NORMAL");
    assert_eq!(old.xmax, 20);
    assert_eq!(old.t_ctid, (new_location.page_num, new_location.slot_num));
    assert_eq!(old.row, row(1, "alive"));

    // The new version is live: xmin = 20, no deleter, no successor.
    let new = fixture
        .decode_physical(new_location)
        .expect("new slot is NORMAL");
    assert_eq!(new.xmin, 20);
    assert_eq!(new.xmax, common::INVALID_XID);
    assert_eq!(new.t_ctid, crate::codec::INVALID_TID);
    assert_eq!(new.row, row(1, "updated"));

    // Both versions also hold a secondary `name` entry (one entry per version).
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "alive").len(),
        1
    );
    assert_eq!(
        fixture
            .secondary_index_tids(name_index().id, "updated")
            .len(),
        1
    );
}

/// An older snapshot that predates the UPDATE still resolves the OLD version
/// through a secondary scan on the OLD value — the retained old entry + the old
/// version being visible to the old snapshot. This is the MVCC point: the
/// pre-update reader is unaffected by the update.
#[test]
fn old_snapshot_resolves_old_version_via_retained_secondary_entry() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    // Capture an OLD snapshot before the update: the future starts at 15, so the
    // updater (txn 20) is in the future and invisible to this snapshot. The
    // creator (txn 10) is committed and below xmax ⇒ visible.
    let old_snapshot = ctx(0, snapshot(15, vec![]));

    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                row(1, "updated"),
            )
            .unwrap()
    );
    fixture.commit(20);

    // The pre-update reader, scanning the OLD name value, still resolves the OLD
    // version: its entry was retained and the old version is visible to a
    // snapshot in which the deleter (txn 20) is in the future.
    let by_old_name = collect_names(
        fixture
            .engine
            .index_scan(&old_snapshot, TABLE_ID, name_index().id, &name_eq("alive"))
            .unwrap(),
    );
    assert_eq!(by_old_name, vec![row(1, "alive")]);

    // A reader after the update sees the new value, and the old value is gone.
    let after = ctx(0, snapshot(30, vec![]));
    assert_eq!(
        fixture.engine.get(&after, TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "updated"))
    );
}

/// Changing a UNIQUE secondary value to a *different live row's* value raises
/// `UniqueViolation`; changing it to a brand-new value succeeds; "updating" the
/// unique value to its own current value succeeds (no false self-conflict,
/// because the superseded old version is treated as own-deleted).
#[test]
fn update_unique_secondary_conflicts_only_with_other_live_rows() {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture
        .engine
        .create_table(&setup, &users_schema())
        .unwrap();
    let unique_name = IndexSchema {
        id: 1,
        table: TABLE_ID,
        name: "users_name_unique".to_string(),
        columns: vec![1],
        unique: true,
    };
    fixture
        .engine
        .create_index(&setup, &unique_name, 0)
        .unwrap();
    fixture.commit(100);

    // Two committed-live rows with distinct unique names.
    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "amy"))
        .unwrap();
    fixture
        .engine
        .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(2, "bob"))
        .unwrap();
    fixture.commit(10);
    fixture.commit(11);

    // Updating row 1's name to "bob" (another live row's value) ⇒ UniqueViolation.
    let err = fixture
        .engine
        .update(
            &ctx(20, snapshot(21, vec![])),
            TABLE_ID,
            &key(1),
            row(1, "bob"),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::UniqueViolation);
    // A statement error aborts the transaction (mvcc.md Decision 3): the partial
    // new version txn 20 wrote (and its index entries) become CLOG-aborted ⇒
    // invisible and non-conflicting, exactly as the server's abort path arranges.
    fixture.abort(20);

    // Updating row 1's name to a brand-new value ⇒ OK.
    assert!(
        fixture
            .engine
            .update(
                &ctx(21, snapshot(22, vec![])),
                TABLE_ID,
                &key(1),
                row(1, "cleo")
            )
            .unwrap()
    );
    fixture.commit(21);

    // "Updating" row 1 to its own current unique value ("cleo") ⇒ OK: the old
    // version it supersedes is own-deleted, so it does not self-conflict.
    assert!(
        fixture
            .engine
            .update(
                &ctx(22, snapshot(23, vec![])),
                TABLE_ID,
                &key(1),
                row(1, "cleo")
            )
            .unwrap()
    );
    fixture.commit(22);

    // The live row reads back as "cleo".
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(40, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "cleo"))
    );
}

/// Changing the primary key is rejected (existing behavior preserved); the row
/// is unchanged.
#[test]
fn update_rejects_primary_key_change() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    let err = fixture
        .engine
        .update(
            &ctx(20, snapshot(21, vec![])),
            TABLE_ID,
            &key(1),
            row(2, "alive"),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::DatatypeMismatch);

    // The original row is untouched.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(30, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "alive"))
    );
}

/// After a delete-then-reinsert (two PK entries for the key — a committed-deleted
/// version and a live one), an UPDATE targets the VISIBLE version (the live
/// re-inserted one), not an arbitrary `search(key)` entry. This is the
/// multi-version landmine fix.
#[test]
fn update_targets_the_visible_version_after_delete_then_reinsert() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    // Delete the original (committed), then re-insert the same key (committed):
    // now two PK entries exist for key 1 — the dead one and the live one.
    assert!(
        fixture
            .engine
            .delete(&ctx(20, snapshot(21, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(20);
    fixture
        .engine
        .insert(
            &ctx(21, snapshot(22, vec![])),
            TABLE_ID,
            row(1, "reinserted"),
        )
        .unwrap();
    fixture.commit(21);
    assert_eq!(fixture.pk_index_tids(&key(1)).len(), 2);

    // Update key 1: it must update the live (re-inserted) version, not the dead
    // one — the visible-version targeting.
    assert!(
        fixture
            .engine
            .update(
                &ctx(22, snapshot(23, vec![])),
                TABLE_ID,
                &key(1),
                row(1, "updated")
            )
            .unwrap()
    );
    fixture.commit(22);

    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(40, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "updated"))
    );
}

// --- E1b: write-write conflict detection on UPDATE/DELETE (mvcc.md §7.3) ---
//
// Each test plants a conflicting `xmax = DELETER` on the target version BEFORE
// the operation, under a writer snapshot in which that deleter is NOT visible (in
// `xip`, so its delete looks in-progress to the writer) — so the row stays
// VISIBLE, `locate_visible_version` returns it, and the stamp-time check fires
// against the deleter's *actual* CLOG status. `xmax` is planted with `infomask =
// 0` so `write_conflict` probes the CLOG rather than short-circuiting on a hint.
// The writer is txn `WRITER` (`> DELETER`), its snapshot's future starting just
// above `WRITER`.

const DELETER: u64 = 50;
const WRITER: u64 = 60;

/// A committed table with one committed-live row (creator txn 10), plus a planted
/// `xmax = DELETER` (no hint bits) on that row's tuple. The deleter's CLOG status
/// is left for the caller to settle (commit/abort/leave-in-progress). Returns the
/// fixture and the row's TID.
fn fixture_with_planted_deleter() -> (Fixture, RowId) {
    let (fixture, rid) = fixture_with_one_row_and_index();
    // Plant a deleter's lock on the row, no settled-status hint bits, so the
    // stamp-time check resolves the deleter via the CLOG.
    fixture.stamp_xmax(rid.page_num, rid.slot_num, DELETER, 0);
    (fixture, rid)
}

/// The writer's snapshot: the future starts just above `WRITER`, and `DELETER` is
/// in-progress at capture (in `xip`) so the planted delete does not hide the row
/// from the writer — `locate_visible_version` returns it and the conflict check
/// fires on the deleter's actual status.
fn writer_snapshot() -> Snapshot {
    Snapshot {
        xmin: 1,
        xmax: WRITER + 1,
        xip: vec![DELETER],
    }
}

/// DELETE conflicts with a **committed-after-snapshot** deleter: the planted
/// `xmax = DELETER` belongs to a txn that committed but is invisible to the
/// writer's snapshot (in `xip`), so the row is still visible to the writer; the
/// atomic stamp-time check sees `DELETER` committed in the CLOG ⇒ `40001`.
#[test]
fn delete_conflicts_with_committed_deleter() {
    let (fixture, _rid) = fixture_with_planted_deleter();
    fixture.commit(DELETER);

    let err = fixture
        .engine
        .delete(&ctx(WRITER, writer_snapshot()), TABLE_ID, &key(1))
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::SerializationFailure);
}

/// UPDATE conflicts with a **committed-after-snapshot** deleter, same setup as the
/// DELETE case (both stamp `xmax` through `stamp_xmax_logged`).
#[test]
fn update_conflicts_with_committed_deleter() {
    let (fixture, _rid) = fixture_with_planted_deleter();
    fixture.commit(DELETER);

    let err = fixture
        .engine
        .update(
            &ctx(WRITER, writer_snapshot()),
            TABLE_ID,
            &key(1),
            row(1, "new"),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::SerializationFailure);
}

/// DELETE BLOCKS on an **in-progress** deleter (`xmax = DELETER`, no Commit/Abort);
/// when that deleter commits during the wait, the writer re-checks, sees the row
/// committed-deleted, and gets `40001` (`docs/specs/deadlock.md`).
#[test]
fn delete_blocks_on_in_progress_deleter_then_conflicts() {
    let (fixture, _rid) = fixture_with_planted_deleter();
    // DELETER is in-progress; the writer blocks on it. The waiter commits DELETER,
    // so the retry sees a committed deleter ⇒ conflict.
    let err = fixture
        .engine
        .delete(
            &committing_blocker(ctx(WRITER, writer_snapshot()), fixture.wal.clone()),
            TABLE_ID,
            &key(1),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::SerializationFailure);
}

/// UPDATE BLOCKS on an **in-progress** deleter (same path); a committed deleter on
/// re-check ⇒ `40001`.
#[test]
fn update_blocks_on_in_progress_deleter_then_conflicts() {
    let (fixture, _rid) = fixture_with_planted_deleter();

    let err = fixture
        .engine
        .update(
            &committing_blocker(ctx(WRITER, writer_snapshot()), fixture.wal.clone()),
            TABLE_ID,
            &key(1),
            row(1, "new"),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::SerializationFailure);
}

/// DELETE does **not** conflict with an **aborted** deleter: the planted lock
/// evaporated (its delete never happened), so the writer proceeds and the DELETE
/// applies — a later reader sees no row.
#[test]
fn delete_proceeds_when_deleter_aborted() {
    let (fixture, _rid) = fixture_with_planted_deleter();
    fixture.abort(DELETER);

    assert!(
        fixture
            .engine
            .delete(&ctx(WRITER, writer_snapshot()), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(WRITER);

    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(WRITER + 2, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        None
    );
}

/// UPDATE does **not** conflict with an **aborted** deleter: the writer proceeds
/// and the new value applies — a later reader sees the updated row.
#[test]
fn update_proceeds_when_deleter_aborted() {
    let (fixture, _rid) = fixture_with_planted_deleter();
    fixture.abort(DELETER);

    assert!(
        fixture
            .engine
            .update(
                &ctx(WRITER, writer_snapshot()),
                TABLE_ID,
                &key(1),
                row(1, "updated"),
            )
            .unwrap()
    );
    fixture.commit(WRITER);

    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(WRITER + 2, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "updated"))
    );
}

/// Plain DELETE/UPDATE of a row whose `xmax = INVALID` (no prior lock) proceeds
/// normally — the conflict check returns `Proceed`.
#[test]
fn delete_and_update_of_unlocked_row_proceed() {
    let (fixture, _rid) = fixture_with_one_row_and_index();

    // UPDATE an unlocked row.
    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                row(1, "updated"),
            )
            .unwrap()
    );
    fixture.commit(20);
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(30, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(row(1, "updated"))
    );

    // DELETE the (still unlocked) live version.
    assert!(
        fixture
            .engine
            .delete(&ctx(21, snapshot(22, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(21);
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(40, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        None
    );
}

fn name_eq(name: &str) -> KeyRange {
    KeyRange::Exact(Key(vec![Value::Text(name.to_string())]))
}

/// Drain an index/sequential-scan iterator into the rows it yields.
fn collect_names(mut iter: Box<dyn crate::traits::RowIterator>) -> Vec<Row> {
    let mut rows = Vec::new();
    while let Some(stored) = iter.next().unwrap() {
        rows.push(stored.row);
    }
    rows
}

// ----------------------------------------------------------------------
// H1 — HOT read-side resolution: REDIRECT + bounded HOT-chain walk.
//
// These synthesize HOT chains / REDIRECTs directly on the heap page (the H2
// HOT-update and H3 pruning production paths do not exist yet), then assert
// the index-lookup read paths resolve them correctly: REDIRECT → bounded
// `t_ctid` walk → visibility, never crossing into an independently-indexed
// successor (no double-return), and corruption → structured error not a loop.
// ----------------------------------------------------------------------

/// A fixture with `users` created (committed) and a single committed root row
/// (id `1`, "root", creator txn 10) inserted via the normal path, so the root
/// carries a real primary-key index entry. Returns the fixture and the root's
/// heap `RowLocation`.
fn fixture_with_root() -> (Fixture, super::RowLocation) {
    let fixture = Fixture::new();
    fixture
        .engine
        .create_table(&ctx(100, snapshot(101, vec![])), &users_schema())
        .unwrap();
    fixture.commit(100);
    fixture
        .engine
        .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "root"))
        .unwrap();
    fixture.commit(10);
    let location = fixture.pk_index_tids(&key(1))[0];
    (fixture, location)
}

#[test]
fn redirect_resolves_to_its_normal_target() {
    // A HOT root whose original tuple was pruned to a REDIRECT (H3) still
    // resolves through the index: the index entry's stable root slot is a
    // REDIRECT to the surviving NORMAL version on the same page.
    let (fixture, root) = fixture_with_root();
    // Build the surviving target version on the same page (creator txn 10,
    // committed) and point the indexed root slot at it.
    let target = fixture.append_raw_tuple(root.page_num, &row(1, "redirected"), 10, INVALID_XID, 0);
    fixture.make_redirect(root.page_num, root.slot_num, target);

    let reader = ctx(0, snapshot(40, vec![]));
    // Point lookup, sequential scan, and the UPDATE/DELETE locate path all
    // follow the REDIRECT to the NORMAL target.
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "redirected"))
    );
    assert_eq!(
        collect_names(
            fixture
                .engine
                .scan_range(&reader, TABLE_ID, &KeyRange::All)
                .unwrap()
        ),
        vec![row(1, "redirected")]
    );
    let (located, _infomask) = fixture.locate(&key(1), snapshot(40, vec![]), 0).unwrap();
    assert_eq!(located.slot_num, target, "locate resolved through redirect");
}

#[test]
fn redirect_to_redirect_is_a_structured_error_not_a_loop() {
    // A REDIRECT must point at a NORMAL slot; a redirect-to-redirect is
    // corruption and must surface as a structured error, never loop.
    let (fixture, root) = fixture_with_root();
    // Two extra NORMAL slots so both redirect ids are in-bounds.
    let mid = fixture.append_raw_tuple(root.page_num, &row(1, "mid"), 10, INVALID_XID, 0);
    let _end = fixture.append_raw_tuple(root.page_num, &row(1, "end"), 10, INVALID_XID, 0);
    // root → mid, but mid is itself a REDIRECT (→ end): redirect-to-redirect.
    fixture.make_redirect(root.page_num, mid, _end);
    fixture.make_redirect(root.page_num, root.slot_num, mid);

    let err = fixture
        .engine
        .get(&ctx(0, snapshot(40, vec![])), TABLE_ID, &key(1))
        .unwrap_err();
    assert_eq!(err.code, SqlState::InternalError);
    assert!(err.message.contains("redirect"), "{}", err.message);
}

#[test]
fn redirect_to_dead_is_a_structured_error() {
    // A REDIRECT to a DEAD (reclaimed-tuple) slot is corruption.
    let (fixture, root) = fixture_with_root();
    let dead = fixture.append_raw_tuple(root.page_num, &row(1, "dead"), 10, INVALID_XID, 0);
    // Tombstone the target to DEAD via the page primitive.
    {
        let mut guard = fixture
            .engine
            .buffer_pool
            .write_page(TABLE_ID, root.page_num, 0)
            .unwrap();
        crate::page::delete_row(guard.data_mut(), dead).unwrap();
    }
    fixture.make_redirect(root.page_num, root.slot_num, dead);

    let err = fixture
        .engine
        .get(&ctx(0, snapshot(40, vec![])), TABLE_ID, &key(1))
        .unwrap_err();
    assert_eq!(err.code, SqlState::InternalError);
}

#[test]
fn hot_chain_returns_visible_heap_only_successor_when_root_invisible() {
    // Root (creator 10) HOT-updated by txn 20 to a HEAP_ONLY successor on the
    // same page: root has xmax = 20 + HOT_UPDATED + t_ctid → successor; the
    // successor (xmin = 20, HEAP_ONLY) has NO index entry. A reader that sees
    // both 10 and 20 committed sees the root as deleted and must return the
    // heap-only successor by walking the chain.
    let (fixture, root) = fixture_with_root();
    let succ = fixture.append_raw_tuple(
        root.page_num,
        &row(1, "hot_new"),
        20,
        INVALID_XID,
        crate::codec::HEAP_ONLY,
    );
    fixture.chain_to(
        root.page_num,
        root.slot_num,
        succ,
        20,
        crate::codec::HOT_UPDATED,
    );
    fixture.commit(20);

    let reader = ctx(0, snapshot(40, vec![]));
    // The walk reaches the heap-only successor; the (now-deleted) root is hidden.
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "hot_new"))
    );
    // Exactly one row is yielded by a scan (no double-count) and it is the new
    // version, even though the heap holds two physical tuples for the key.
    assert_eq!(
        collect_names(
            fixture
                .engine
                .scan_range(&reader, TABLE_ID, &KeyRange::All)
                .unwrap()
        ),
        vec![row(1, "hot_new")]
    );
    // UPDATE/DELETE target the live heap-only successor, not the pruned root.
    let (located, _infomask) = fixture.locate(&key(1), snapshot(40, vec![]), 0).unwrap();
    assert_eq!(located.slot_num, succ);
}

#[test]
fn hot_chain_returns_root_when_it_is_the_visible_version() {
    // Same chain, but a reader whose snapshot has txn 20 in-progress (the
    // HOT-update has not committed for it): the root is still live/visible and
    // must be returned; the in-flight successor is not.
    let (fixture, root) = fixture_with_root();
    let succ = fixture.append_raw_tuple(
        root.page_num,
        &row(1, "hot_new"),
        20,
        INVALID_XID,
        crate::codec::HEAP_ONLY,
    );
    fixture.chain_to(
        root.page_num,
        root.slot_num,
        succ,
        20,
        crate::codec::HOT_UPDATED,
    );
    // txn 20 left in-progress (no commit/abort).

    // Reader sees 10 committed, 20 in-progress ⇒ the root's delete by 20 is not
    // effective ⇒ root is visible.
    let reader = ctx(0, snapshot(40, vec![20]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "root"))
    );
}

#[test]
fn walk_stops_at_a_non_heap_only_successor_no_double_return() {
    // THE double-count guard: a root HOT_UPDATED whose `t_ctid` successor is an
    // INDEPENDENTLY-INDEXED version (NOT HEAP_ONLY) must NOT be crossed — that
    // successor is reachable via its own index entry. With the root invisible
    // (deleted by committed txn 20) and the successor NOT heap-only, the walk
    // stops at the root and returns None (the successor is found via its index
    // entry, not this chain).
    let (fixture, root) = fixture_with_root();
    // Successor lacks HEAP_ONLY ⇒ it is "independently indexed".
    let succ = fixture.append_raw_tuple(root.page_num, &row(1, "indexed_new"), 20, INVALID_XID, 0);
    fixture.chain_to(
        root.page_num,
        root.slot_num,
        succ,
        20,
        crate::codec::HOT_UPDATED,
    );
    fixture.commit(20);

    // The chain walk from the root's index entry stops at the invisible root and
    // does NOT descend into the non-heap-only successor, so the point lookup via
    // the root entry yields nothing here (no double-return of `succ`).
    let reader = ctx(0, snapshot(40, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        None
    );
    // Confirm the walk parameters: root is HOT_UPDATED, successor is NOT
    // heap-only, so the stop rule (not the visibility) is what ends the walk.
    let root_dec = fixture.decode_physical(root).unwrap();
    assert_ne!(root_dec.infomask & crate::codec::HOT_UPDATED, 0);
    let succ_loc = super::RowLocation {
        file_id: TABLE_ID,
        page_num: root.page_num,
        slot_num: succ,
    };
    assert_eq!(
        fixture.decode_physical(succ_loc).unwrap().infomask & crate::codec::HEAP_ONLY,
        0
    );
}

#[test]
fn cyclic_hot_chain_is_a_structured_error_not_an_infinite_loop() {
    // A corrupt cycle among HEAP_ONLY members: root → a → b → a. `a` and `b` are
    // both HEAP_ONLY + HOT_UPDATED (so the walk keeps following them), and `b`
    // points back at `a`, closing the cycle. The bounded walk's visited-set
    // guard must turn this into a structured error, never spin. (A back-edge to
    // the non-heap-only root would instead stop cleanly, which is the
    // `walk_stops_at_a_non_heap_only_successor` case — so the cycle is built
    // strictly inside the heap-only segment.) All are invisible to the reader.
    let (fixture, root) = fixture_with_root();
    let a = fixture.append_raw_tuple(
        root.page_num,
        &row(1, "a"),
        20,
        20,
        crate::codec::HEAP_ONLY | crate::codec::HOT_UPDATED,
    );
    let b = fixture.append_raw_tuple(
        root.page_num,
        &row(1, "b"),
        20,
        20,
        crate::codec::HEAP_ONLY | crate::codec::HOT_UPDATED,
    );
    // root → a (root is HOT_UPDATED but not heap-only — the indexed root).
    fixture.chain_to(
        root.page_num,
        root.slot_num,
        a,
        20,
        crate::codec::HOT_UPDATED,
    );
    // a → b, b → a: the heap-only cycle.
    fixture.chain_to(
        root.page_num,
        a,
        b,
        20,
        crate::codec::HEAP_ONLY | crate::codec::HOT_UPDATED,
    );
    fixture.chain_to(
        root.page_num,
        b,
        a,
        20,
        crate::codec::HEAP_ONLY | crate::codec::HOT_UPDATED,
    );
    fixture.commit(20);

    let err = fixture
        .engine
        .get(&ctx(0, snapshot(40, vec![])), TABLE_ID, &key(1))
        .unwrap_err();
    assert_eq!(err.code, SqlState::InternalError);
    assert!(err.message.contains("cyclic"), "{}", err.message);
}

#[test]
fn non_hot_data_resolves_unchanged() {
    // Regression: with no HOT machinery active (a plain NORMAL root, no
    // HOT_UPDATED, no REDIRECT), resolution is the prior single-tuple check.
    let (fixture, _root) = fixture_with_root();
    let reader = ctx(0, snapshot(40, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(row(1, "root"))
    );
    assert_eq!(
        collect_names(
            fixture
                .engine
                .scan_range(&reader, TABLE_ID, &KeyRange::All)
                .unwrap()
        ),
        vec![row(1, "root")]
    );
}

// ----------------------------------------------------------------------
// H2 — HOT-update fast path + its two safety guards (CREATE INDEX
// broken-chain fail-fast, VACUUM skip of HOT-chain tuples).
// ----------------------------------------------------------------------

/// A HOT update (only the non-indexed `id`... no — `name` IS indexed; here we add
/// a NON-indexed column). The fixture's `name` is indexed, so to exercise HOT we
/// need a table whose updated column is not indexed. Build a 3-column table.
fn hot_schema() -> TableSchema {
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
        toast: ToastOptions::legacy_catalog_default(),
        toast_table_id: None,
        relation_kind: RelationKind::User,
    }
}

fn hot_row(id: i64, name: &str, note: &str) -> Row {
    Row {
        values: vec![
            Value::Integer(id),
            Value::Text(name.to_string()),
            Value::Text(note.to_string()),
        ],
    }
}

/// A `users(id pk, name, note)` table with a secondary index on `name` (NOT on
/// `note`), one committed row, all under txn 100/10. Returns the fixture and the
/// row's heap location (the chain root).
fn hot_fixture() -> (Fixture, super::RowLocation) {
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture.engine.create_table(&setup, &hot_schema()).unwrap();
    fixture
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    fixture.commit(100);
    let rid = fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            hot_row(1, "Ada", "v1"),
        )
        .unwrap();
    fixture.commit(10);
    let root = super::RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };
    (fixture, root)
}

fn decode_hot(fixture: &Fixture, loc: super::RowLocation) -> crate::codec::DecodedRow {
    let readable = fixture
        .engine
        .buffer_pool
        .read_page(loc.file_id, loc.page_num)
        .unwrap();
    let bytes = crate::page::read_row(readable.data(), loc.slot_num)
        .unwrap()
        .expect("slot is NORMAL");
    crate::codec::decode_row(&hot_schema(), &bytes).unwrap()
}

#[test]
fn hot_update_same_page_no_new_index_entry_and_reads_once() {
    // Updating only the NON-indexed `note` column is a HOT update: the new
    // version lands on the SAME page with HEAP_ONLY, the root gets HOT_UPDATED +
    // t_ctid -> it, and NO new index entry is created. Reads (PK and secondary)
    // see the updated row exactly once.
    let (fixture, root) = hot_fixture();

    // Index-entry counts BEFORE the update: one PK entry, one secondary entry.
    assert_eq!(fixture.pk_index_tids(&key(1)).len(), 1);
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada").len(),
        1
    );

    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", "v2"),
            )
            .unwrap()
    );
    fixture.commit(20);

    // The root was HOT-updated: xmax = 20, HOT_UPDATED set, t_ctid -> a slot on
    // the SAME page.
    let root_dec = decode_hot(&fixture, root);
    assert_eq!(root_dec.xmax, 20);
    assert_ne!(root_dec.infomask & crate::codec::HOT_UPDATED, 0);
    let (succ_page, succ_slot) = root_dec.t_ctid;
    assert_eq!(
        succ_page, root.page_num,
        "HOT successor is on the same page"
    );

    // The successor is a live HEAP_ONLY tuple carrying the new note.
    let succ_loc = super::RowLocation {
        file_id: TABLE_ID,
        page_num: succ_page,
        slot_num: succ_slot,
    };
    let succ = decode_hot(&fixture, succ_loc);
    assert_eq!(succ.xmin, 20);
    assert_eq!(succ.xmax, common::INVALID_XID);
    assert_ne!(succ.infomask & crate::codec::HEAP_ONLY, 0);
    assert_eq!(succ.row, hot_row(1, "Ada", "v2"));

    // NO new index entries: still exactly one PK entry (the root) and one
    // secondary entry — both pointing at the ROOT, not the heap-only successor.
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada"),
        vec![root]
    );

    // Reads see the updated row exactly once: PK get, sequential scan, and the
    // secondary index scan all resolve the chain to the heap-only successor.
    let reader = ctx(0, snapshot(30, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(hot_row(1, "Ada", "v2"))
    );
    let seq: Vec<Row> = collect_names(
        fixture
            .engine
            .scan_range(&reader, TABLE_ID, &KeyRange::All)
            .unwrap(),
    );
    assert_eq!(seq, vec![hot_row(1, "Ada", "v2")]);
    let by_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_index().id, &name_eq("Ada"))
            .unwrap(),
    );
    assert_eq!(by_name, vec![hot_row(1, "Ada", "v2")]);
}

#[test]
fn indexed_column_change_falls_back_to_a_normal_update() {
    // Changing the INDEXED `name` is NOT HOT: a fresh fully-indexed version is
    // written (new PK + secondary entries appear) and the new version is NOT
    // HEAP_ONLY.
    let (fixture, root) = hot_fixture();

    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Bea", "v2"),
            )
            .unwrap()
    );
    fixture.commit(20);

    // Two PK entries now (one per version): a fully-indexed (non-HOT) update.
    let pk = fixture.pk_index_tids(&key(1));
    assert_eq!(pk.len(), 2, "a new fully-indexed version was inserted");
    let new_loc = *pk.iter().find(|loc| **loc != root).unwrap();
    // The new version is NOT heap-only.
    let new_dec = decode_hot(&fixture, new_loc);
    assert_eq!(new_dec.infomask & crate::codec::HEAP_ONLY, 0);
    // The root is chained but NOT HOT_UPDATED (a normal MVCC update).
    let root_dec = decode_hot(&fixture, root);
    assert_eq!(root_dec.infomask & crate::codec::HOT_UPDATED, 0);

    // Both indexes find the new version by the NEW name; the old name is gone.
    let reader = ctx(0, snapshot(30, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(hot_row(1, "Bea", "v2"))
    );
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Bea").len(),
        1
    );
    let by_new = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_index().id, &name_eq("Bea"))
            .unwrap(),
    );
    assert_eq!(by_new, vec![hot_row(1, "Bea", "v2")]);
}

#[test]
fn same_page_full_falls_back_to_a_normal_update() {
    // When the predecessor's page has no room for the new tuple, the HOT path is
    // ineligible and we fall back to a normal fully-indexed update (a new tuple on
    // ANOTHER page + a new index entry).
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture.engine.create_table(&setup, &hot_schema()).unwrap();
    fixture
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    fixture.commit(100);

    // Fill the first heap page nearly full with one big-note row plus filler rows,
    // so a subsequent same-size HOT update of row 1 cannot also fit on it.
    let big = "x".repeat(3000);
    let rid = fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            hot_row(1, "Ada", &big),
        )
        .unwrap();
    let root = super::RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };
    // Pad the same page with one more ~3000-byte note row (write_new_row fills a
    // page before extending), so ~6000 of the page's 8192 bytes are used and the
    // free space is below one more big-note tuple.
    fixture
        .engine
        .insert(
            &ctx(12, snapshot(13, vec![])),
            TABLE_ID,
            hot_row(2, "filler", &big),
        )
        .unwrap();
    fixture.commit(12);
    fixture.commit(10);

    // The filler shares row 1's page, so that page is now too full for another
    // big-note tuple (the HOT update below).
    assert_eq!(
        fixture.pk_index_tids(&key(2))[0].page_num,
        root.page_num,
        "filler row must share row 1's page",
    );

    // HOT-update row 1's NON-indexed note with another big value: no same-page
    // room ⇒ fall back to a normal update (new tuple on a fresh page, new PK
    // entry).
    assert!(
        fixture
            .engine
            .update(
                &ctx(40, snapshot(41, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", &"y".repeat(3000)),
            )
            .unwrap()
    );
    fixture.commit(40);

    let pk = fixture.pk_index_tids(&key(1));
    assert_eq!(pk.len(), 2, "fell back to a fully-indexed update");
    let new_loc = *pk.iter().find(|loc| **loc != root).unwrap();
    assert_ne!(
        new_loc.page_num, root.page_num,
        "new version is on another page"
    );
    let new_dec = decode_hot(&fixture, new_loc);
    assert_eq!(
        new_dec.infomask & crate::codec::HEAP_ONLY,
        0,
        "not heap-only"
    );
    // The root is a normal (non-HOT) update.
    let root_dec = decode_hot(&fixture, root);
    assert_eq!(root_dec.infomask & crate::codec::HOT_UPDATED, 0);
    // The updated row reads back.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(50, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(hot_row(1, "Ada", &"y".repeat(3000)))
    );
}

#[test]
fn concurrent_hot_update_first_updater_wins_40001() {
    // Two writers HOT-update the same row. The first stamps the predecessor's
    // xmax; the second observes the committed xmax and aborts with 40001. The
    // orphaned heap-only tuple the loser wrote is harmless (invisible once its txn
    // aborts).
    let (fixture, _root) = hot_fixture();

    // Writer 30 HOT-updates and commits (the winner of the row lock).
    assert!(
        fixture
            .engine
            .update(
                &ctx(30, snapshot(31, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", "w30"),
            )
            .unwrap()
    );
    fixture.commit(30);

    // Writer 40 holds a snapshot in which 30 is still in-progress (in `xip`), so
    // the root's deleter (xmax = 30) is not visible and 40 sees the ORIGINAL v1 as
    // the live version and targets the root. The root's physical xmax is now 30
    // (committed in the CLOG), so the atomic first-updater-wins check fires `40001`
    // — the actual-status row-lock check ignores the snapshot.
    let err = fixture
        .engine
        .update(
            &ctx(40, snapshot(41, vec![30])),
            TABLE_ID,
            &key(1),
            hot_row(1, "Ada", "w40"),
        )
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::SerializationFailure);

    // The committed winner's value is what a later reader sees.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(50, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(hot_row(1, "Ada", "w30"))
    );
}

#[test]
fn vacuum_collapses_a_committed_dead_hot_chain_to_a_redirect() {
    // Build a multi-version HOT chain root -> v2 -> v3 -> v4, advance the horizon so
    // the dead prefix (root, v2, v3) is dead-to-all, run VACUUM: H3 COLLAPSES the
    // chain — the root slot becomes a REDIRECT to the live tail v4, the dead
    // heap-only members v2/v3 are freed to UNUSED, the index entry still resolves to
    // v4 (PK + secondary), and freed page space is reclaimed.
    let (fixture, root) = hot_fixture();

    // Three successive HOT updates of the non-indexed note (txns 20, 21, 22),
    // building root -> v2 -> v3 -> v4, all on the same page, no new index entries.
    for (txn, note) in [(20u64, "v2"), (21, "v3"), (22, "v4")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx(txn, snapshot(txn + 1, vec![])),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", note),
                )
                .unwrap()
        );
        fixture.commit(txn);
    }
    // Still exactly one PK + one secondary entry (HOT added none). Capture the live
    // tail (v4) slot and the page's free space before the collapse.
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada"),
        vec![root]
    );
    let v4 = fixture
        .locate_hot(&key(1), snapshot(120, vec![]), 0)
        .expect("v4 visible")
        .0;
    assert_ne!(
        v4.slot_num, root.slot_num,
        "v4 is a heap-only successor slot"
    );
    let free_before = fixture.free_bytes_hot(root.page_num);

    // Horizon 100: root/v2/v3 (xmax in {20,21,22}, all < 100 and committed) are
    // dead-to-all; v4 (xmax INVALID) is the live tail.
    let schema = hot_schema();
    let reclaimed = fixture.engine.vacuum(&schema, 100).unwrap();
    assert!(reclaimed >= 2, "v2 and v3 were freed: {reclaimed}");

    // The root slot is now a REDIRECT to the live tail v4.
    assert_eq!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(v4.slot_num),
        "the dead root collapsed to a REDIRECT pointing at the live tail",
    );

    // The chain's dead heap-only members are now UNUSED (freed directly, no index
    // entry — the key HOT win), and v4 stays NORMAL.
    assert_eq!(
        fixture.slot_state_hot(v4.page_num, v4.slot_num),
        crate::page::LinePointer::Normal
    );

    // The index entries still point at the (stable) root slot, which resolves via
    // the REDIRECT to v4 — exactly once, on BOTH index paths.
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada"),
        vec![root]
    );
    let reader = ctx(0, snapshot(120, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(hot_row(1, "Ada", "v4"))
    );
    let by_seq = collect_names(
        fixture
            .engine
            .scan_range(&reader, TABLE_ID, &KeyRange::All)
            .unwrap(),
    );
    assert_eq!(by_seq, vec![hot_row(1, "Ada", "v4")]);
    let by_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_index().id, &name_eq("Ada"))
            .unwrap(),
    );
    assert_eq!(by_name, vec![hot_row(1, "Ada", "v4")]);

    // Freed page space was reclaimed (the dead v2/v3/root tuples' bytes).
    assert!(
        fixture.free_bytes_hot(root.page_num) > free_before,
        "collapsing the dead prefix reclaimed page free space",
    );
}

#[test]
fn vacuum_reclaims_an_aborted_creator_hot_heap_only_tuple() {
    // An aborted HOT update leaves a HEAP_ONLY successor whose creator (xmin)
    // aborted: it is a dead-end orphan (no committed version chained onto it), so
    // VACUUM MUST reclaim it (the corrected H2 skip-guard). Leaving it would leak
    // space and — per F4c — keep a surviving on-disk reference to the aborted txn.
    // After reclaim, the root still reads its ORIGINAL value (the rolled-back HOT
    // successor is gone, no resurrection).
    let (fixture, root) = hot_fixture();

    // HOT-update the note v1 -> v2 under txn 20, then ABORT it (no undo): the
    // successor (xmin = 20, HEAP_ONLY) and the root's xmax = 20 + HOT_UPDATED both
    // belong to the aborted txn. The root stays live (the update rolled back).
    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", "v2"),
            )
            .unwrap()
    );
    fixture.abort(20);

    // The root currently points its t_ctid at the heap-only successor.
    let root_dec = decode_hot(&fixture, root);
    assert_ne!(root_dec.infomask & crate::codec::HOT_UPDATED, 0);
    let (succ_page, succ_slot) = root_dec.t_ctid;
    let succ_loc = super::RowLocation {
        file_id: TABLE_ID,
        page_num: succ_page,
        slot_num: succ_slot,
    };
    // Pre-VACUUM: the heap-only successor is a live NORMAL slot (aborted creator).
    let succ = decode_hot(&fixture, succ_loc);
    assert_eq!(succ.xmin, 20);
    assert_ne!(succ.infomask & crate::codec::HEAP_ONLY, 0);

    // VACUUM at any horizon reclaims the aborted-creator successor (aborted-creator
    // reclaim has NO age requirement). H3 frees the heap-only successor straight to
    // UNUSED (no index entry) and un-HOTs the surviving root (the chain-aware
    // abort-cleanup).
    let schema = hot_schema();
    let reclaimed = fixture.engine.vacuum(&schema, 100).unwrap();
    assert!(
        reclaimed >= 1,
        "the aborted-creator HOT heap-only successor must be reclaimed"
    );

    // The successor slot is now UNUSED (freed directly, no DEAD intermediary, since
    // a HEAP_ONLY tuple has no index entry).
    assert_eq!(
        fixture.slot_state_hot(succ_page, succ_slot),
        crate::page::LinePointer::Unused
    );
    // The root was un-HOTed in place: xmax cleared to INVALID, HOT_UPDATED dropped,
    // t_ctid reset — the exact live, never-updated header shape.
    let root_after = decode_hot(&fixture, root);
    assert_eq!(root_after.xmax, common::INVALID_XID);
    assert_eq!(root_after.infomask & crate::codec::HOT_UPDATED, 0);
    assert_eq!(root_after.t_ctid, crate::codec::INVALID_TID);
    assert_eq!(root_after.row, hot_row(1, "Ada", "v1"));

    // The root still reads its ORIGINAL value: the aborted update's successor is
    // gone (no resurrection), and the root's own xmax = 20 is an aborted deleter, so
    // the row stays visible.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(120, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(hot_row(1, "Ada", "v1"))
    );
    // The index still points at the (intact) root.
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
}

#[test]
fn vacuum_marks_a_fully_dead_hot_chain_root_dead_and_reclaims_it() {
    // Build a HOT chain, then DELETE the row and advance the horizon so the WHOLE
    // chain (root + every heap-only successor + the deleted tail) is dead-to-all.
    // H3 marks the root DEAD (F3a strips its index entry, F3b reclaims it
    // DEAD → UNUSED), frees the heap-only members to UNUSED, reads return nothing,
    // and the slot is reusable by a later insert.
    let (fixture, root) = hot_fixture();
    for (txn, note) in [(20u64, "v2"), (21, "v3")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx(txn, snapshot(txn + 1, vec![])),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", note),
                )
                .unwrap()
        );
        fixture.commit(txn);
    }
    // DELETE the row (stamps xmax on the live tail v3).
    assert!(
        fixture
            .engine
            .delete(&ctx(30, snapshot(31, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(30);

    // Horizon 100: root/v2/v3 all dead-to-all (committed deletes < 100). VACUUM
    // marks the root DEAD, strips its index entries, then reclaims it to UNUSED.
    let schema = hot_schema();
    let reclaimed = fixture.engine.vacuum(&schema, 100).unwrap();
    assert!(reclaimed >= 3, "the whole 3-version chain was reclaimed");

    // The root slot is now UNUSED (DEAD → index-vacuum → reclaimed UNUSED).
    assert_eq!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Unused
    );
    // The index entry was removed (no dangling PK or secondary entry).
    assert!(fixture.pk_index_tids(&key(1)).is_empty());
    assert!(
        fixture
            .secondary_index_tids(name_index().id, "Ada")
            .is_empty()
    );
    // Reads return nothing.
    let reader = ctx(0, snapshot(120, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        None
    );
    let seq = collect_names(
        fixture
            .engine
            .scan_range(&reader, TABLE_ID, &KeyRange::All)
            .unwrap(),
    );
    assert!(seq.is_empty());

    // The reclaimed UNUSED slot is reusable by a later insert (it may reuse the
    // root slot id on the same page).
    let rid = fixture
        .engine
        .insert(
            &ctx(40, snapshot(41, vec![])),
            TABLE_ID,
            hot_row(2, "Bea", "n"),
        )
        .unwrap();
    fixture.commit(40);
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(50, vec![])), TABLE_ID, &key(2))
            .unwrap(),
        Some(hot_row(2, "Bea", "n"))
    );
    let _ = rid;
}

#[test]
fn vacuum_collapse_then_further_hot_update_extends_a_multi_segment_chain() {
    // After a collapse leaves root = REDIRECT → L (a heap-only tail), a further HOT
    // update of the (now redirected) row must extend the chain: REDIRECT → L →
    // L2. Reads on BOTH index paths resolve to the latest version, proving the
    // resolver walks REDIRECT → heap-only → heap-only correctly.
    let (fixture, root) = hot_fixture();
    for (txn, note) in [(20u64, "v2"), (21, "v3")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx(txn, snapshot(txn + 1, vec![])),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", note),
                )
                .unwrap()
        );
        fixture.commit(txn);
    }
    // Collapse: root/v2 dead-to-all, v3 live ⇒ root REDIRECT → v3.
    let schema = hot_schema();
    fixture.engine.vacuum(&schema, 100).unwrap();
    let v3 = fixture
        .locate_hot(&key(1), snapshot(120, vec![]), 0)
        .expect("v3 visible")
        .0;
    assert_eq!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(v3.slot_num)
    );

    // A further HOT update of the now-redirected row (note v3 -> v4). It targets v3
    // (the live tail under the REDIRECT) and chains it to a new heap-only v4.
    assert!(
        fixture
            .engine
            .update(
                &ctx(120, snapshot(121, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", "v4"),
            )
            .unwrap()
    );
    fixture.commit(120);

    // The chain is now REDIRECT(root) → v3 (HOT_UPDATED) → v4 (heap-only tail). No
    // new index entry was added (still the single root entry).
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada"),
        vec![root]
    );
    // Both index paths resolve through REDIRECT → v3 → v4 to the latest version.
    let reader = ctx(0, snapshot(130, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(hot_row(1, "Ada", "v4"))
    );
    let by_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_index().id, &name_eq("Ada"))
            .unwrap(),
    );
    assert_eq!(by_name, vec![hot_row(1, "Ada", "v4")]);

    // A second collapse (horizon past v3's xmax = 120) re-points the REDIRECT at v4.
    fixture.engine.vacuum(&schema, 200).unwrap();
    let v4 = fixture
        .locate_hot(&key(1), snapshot(220, vec![]), 0)
        .expect("v4 visible")
        .0;
    assert_eq!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(v4.slot_num)
    );
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(220, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(hot_row(1, "Ada", "v4"))
    );
}

#[test]
fn vacuum_recollapses_a_redirect_rooted_chain_grown_by_two_versions() {
    // Regression for the H3 re-collapse corruption (`docs/specs/mvcc.md` §9/§10).
    // First VACUUM collapses the chain to `root = REDIRECT → L`. Then the chain
    // GROWS by TWO HOT versions from the redirect target (L → L1 → L2). A second
    // VACUUM (with L and L1 dead-to-all) must re-collapse the chain EXACTLY ONCE —
    // re-pointing the REDIRECT at the newest live tail L2 and freeing the now-dead
    // intermediates (incl. the old redirect target L) to UNUSED without error or
    // corruption. Before the fix, the redirect target was treated as an independent
    // root in the second pass, planning the chain twice → a slot freed twice →
    // `apply_prune_plan` errored mid-page → permanent `page checksum mismatch`.
    let (fixture, root) = hot_fixture();
    let schema = hot_schema();

    // root -> v2 -> v3, then collapse: root REDIRECT → v3 (the live tail L).
    for (txn, note) in [(20u64, "v2"), (21, "v3")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx(txn, snapshot(txn + 1, vec![])),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", note),
                )
                .unwrap()
        );
        fixture.commit(txn);
    }
    fixture.engine.vacuum(&schema, 100).unwrap();
    let l = fixture
        .locate_hot(&key(1), snapshot(120, vec![]), 0)
        .expect("L visible")
        .0;
    assert_eq!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(l.slot_num),
        "first collapse points the root at the live tail L",
    );

    // Grow the chain by TWO from the redirect target: L (v3) -> L1 (v4) -> L2 (v5).
    for (txn, note) in [(120u64, "v4"), (121, "v5")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx(txn, snapshot(txn + 1, vec![])),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", note),
                )
                .unwrap()
        );
        fixture.commit(txn);
    }
    let l2 = fixture
        .locate_hot(&key(1), snapshot(220, vec![]), 0)
        .expect("L2 visible")
        .0;
    assert_ne!(l2.slot_num, l.slot_num, "L2 is a later heap-only slot");

    // Second VACUUM (horizon 200): L (xmax 120) and L1 (xmax 121) are dead-to-all,
    // L2 is the live tail. Must NOT error and must NOT corrupt the page.
    let reclaimed = fixture
        .engine
        .vacuum(&schema, 200)
        .expect("re-collapse must not error");
    assert!(reclaimed >= 2, "L and L1 were freed: {reclaimed}");

    // The root is now a REDIRECT to the NEWEST live tail L2 (not the stale L).
    assert_eq!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(l2.slot_num),
        "re-collapse re-points the REDIRECT at the newest live version",
    );
    // The old redirect target L and the intermediate L1 are freed to UNUSED.
    assert_eq!(
        fixture.slot_state_hot(l.page_num, l.slot_num),
        crate::page::LinePointer::Unused,
        "the old redirect target is freed to UNUSED",
    );
    // L2 stays NORMAL.
    assert_eq!(
        fixture.slot_state_hot(l2.page_num, l2.slot_num),
        crate::page::LinePointer::Normal,
    );

    // The page validates (a corrupt checksum would make this read error) and the
    // live row reads back correctly on both index paths and a seq scan.
    let reader = ctx(0, snapshot(220, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(hot_row(1, "Ada", "v5")),
    );
    let by_seq = collect_names(
        fixture
            .engine
            .scan_range(&reader, TABLE_ID, &KeyRange::All)
            .unwrap(),
    );
    assert_eq!(by_seq, vec![hot_row(1, "Ada", "v5")]);
    let by_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_index().id, &name_eq("Ada"))
            .unwrap(),
    );
    assert_eq!(by_name, vec![hot_row(1, "Ada", "v5")]);
    // Index counts stayed stable: still the single root entry on both indexes.
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada"),
        vec![root],
    );
}

#[test]
fn update_path_prunes_a_redirect_rooted_chain_to_make_room() {
    // The update-path variant of the re-collapse: after a VACUUM has made the root a
    // REDIRECT, drive enough same-page HOT updates that a later update must run the
    // update-path prune OVER the REDIRECT-rooted chain to make room. It must collapse
    // the chain once (no corruption) and reads must resolve to the latest value.
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture.engine.create_table(&setup, &hot_schema()).unwrap();
    fixture
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    fixture.commit(100);

    // ~1900-byte notes: a few versions nearly fill the 8192B page.
    let big = |tag: &str| format!("{tag}{}", "x".repeat(1900));
    let rid = fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            hot_row(1, "Ada", &big("v1")),
        )
        .unwrap();
    fixture.commit(10);
    let root = super::RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };

    // root -> v2 -> v3, then VACUUM collapses to root REDIRECT → v3.
    for (txn, tag) in [(20u64, "v2"), (21, "v3")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx(txn, snapshot(txn + 1, vec![])),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", &big(tag)),
                )
                .unwrap()
        );
        fixture.commit(txn);
    }
    let schema = hot_schema();
    fixture.engine.vacuum(&schema, 100).unwrap();
    assert!(matches!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(_)
    ));

    // Grow the REDIRECT-rooted chain with more big HOT updates until the page is too
    // full for the next one, forcing the update-path prune over the redirect chain.
    // The horizon (200) makes the just-superseded versions prunable, so the prune
    // re-collapses the REDIRECT-rooted chain to reclaim room — staying on the HOT
    // path (no new index entry).
    for (txn, tag) in [(120u64, "v4"), (121, "v5"), (122, "v6"), (123, "v7")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx_h(txn, snapshot(txn + 1, vec![]), 200),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", &big(tag)),
                )
                .unwrap(),
            "HOT update {tag} must succeed without corruption",
        );
        fixture.commit(txn);
    }

    // The root is still a REDIRECT, no extra index entries appeared, and reads
    // resolve to the latest value through both index paths.
    assert!(matches!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(_)
    ));
    assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada").len(),
        1
    );
    let reader = ctx(0, snapshot(230, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(hot_row(1, "Ada", &big("v7"))),
    );
    let by_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_index().id, &name_eq("Ada"))
            .unwrap(),
    );
    assert_eq!(by_name, vec![hot_row(1, "Ada", &big("v7"))]);
}

#[test]
fn classify_marks_a_redirect_target_as_a_member_not_a_root() {
    // Unit-level assertion of the Part 1 fix: build a page whose root REDIRECTs to a
    // live tail that has itself been HOT-extended (REDIRECT(root) → L → L1 → L2 with
    // L,L1 dead-to-all). `classify_page_for_prune` must mark the redirect target L as
    // a chain MEMBER, so the chain is planned EXACTLY ONCE via the REDIRECT root: no
    // slot appears twice in `free_to_unused`, and no slot is both freed and
    // redirected.
    let (fixture, root) = hot_fixture();
    let schema = hot_schema();
    for (txn, note) in [(20u64, "v2"), (21, "v3")] {
        fixture
            .engine
            .update(
                &ctx(txn, snapshot(txn + 1, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", note),
            )
            .unwrap();
        fixture.commit(txn);
    }
    fixture.engine.vacuum(&schema, 100).unwrap();
    for (txn, note) in [(120u64, "v4"), (121, "v5")] {
        fixture
            .engine
            .update(
                &ctx(txn, snapshot(txn + 1, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", note),
            )
            .unwrap();
        fixture.commit(txn);
    }

    // Classify the page directly (horizon 200: L and L1 dead-to-all, L2 live).
    let data = {
        let readable = fixture
            .engine
            .buffer_pool
            .read_page(TABLE_ID, root.page_num)
            .unwrap();
        *readable.data()
    };
    let plan = fixture
        .engine
        .classify_page_for_prune(&data, 200, true)
        .expect("classify must not error");

    // No slot is freed to UNUSED more than once.
    let mut freed = plan.free_to_unused.clone();
    freed.sort_unstable();
    let mut deduped = freed.clone();
    deduped.dedup();
    assert_eq!(
        freed, deduped,
        "no slot may appear twice in free_to_unused: {:?}",
        plan.free_to_unused,
    );
    // No slot is both freed AND redirected.
    let freed_set: std::collections::HashSet<u16> = plan.free_to_unused.iter().copied().collect();
    for &(redir_root, _target) in &plan.redirect_roots {
        assert!(
            !freed_set.contains(&redir_root),
            "slot {redir_root} is both freed and used as a REDIRECT root",
        );
    }
    // Exactly one chain was planned: a single REDIRECT root (the indexed root), not
    // an extra one rooted at the redirect target.
    assert_eq!(
        plan.redirect_roots.len(),
        1,
        "the chain is planned once via the REDIRECT root: {:?}",
        plan.redirect_roots,
    );
    assert_eq!(plan.redirect_roots[0].0, root.slot_num);
}

#[test]
fn apply_prune_plan_leaves_the_page_intact_on_a_malformed_plan() {
    // Part 2 atomicity: a deliberately malformed plan (a slot listed TWICE in
    // free_to_unused) must return an Err AND leave the page byte-identical with a
    // valid checksum — never a half-mutated page with a stale checksum.
    let (fixture, root) = hot_fixture();
    // The freshly inserted root is a live NORMAL slot to (mis)free.
    let succ = root.slot_num;

    // Snapshot the exact page bytes before the malformed apply.
    let before = {
        let readable = fixture
            .engine
            .buffer_pool
            .read_page(TABLE_ID, root.page_num)
            .unwrap();
        *readable.data()
    };
    assert!(
        crate::page::validate(&before).is_ok(),
        "page is valid before the malformed apply",
    );

    // A plan that frees the SAME NORMAL slot twice — `free_slots_to_unused` rejects
    // the second free (the slot is already UNUSED on the scratch copy).
    let bad_plan = super::PagePrunePlan {
        redirect_roots: Vec::new(),
        dead_roots: Vec::new(),
        free_to_unused: vec![succ, succ],
        reset_slots: Vec::new(),
    };
    let mut guard = fixture
        .engine
        .buffer_pool
        .write_page(TABLE_ID, root.page_num, super::VACUUM_TXN)
        .unwrap();
    let result = fixture.engine.apply_prune_plan(
        &mut guard,
        &bad_plan,
        TABLE_ID,
        root.page_num,
        super::VACUUM_TXN,
    );
    assert!(result.is_err(), "a malformed plan must error");

    // The frame is byte-identical to its pre-apply image (no partial mutation) and
    // still passes checksum validation.
    assert_eq!(
        guard.data(),
        &before,
        "the frame is unchanged after a rejected plan",
    );
    assert!(
        crate::page::validate(guard.data()).is_ok(),
        "the frame still validates (no stale checksum)",
    );
    drop(guard);

    // And a fresh read of the page still succeeds (no permanent corruption).
    let reread = {
        let readable = fixture
            .engine
            .buffer_pool
            .read_page(TABLE_ID, root.page_num)
            .unwrap();
        *readable.data()
    };
    assert_eq!(reread, before);
}

#[test]
fn update_path_prunes_to_make_room_and_stays_on_the_hot_path() {
    // Pack a page with several big committed-dead HOT versions of one row, so a
    // further HOT update would have no same-page room. With the GC horizon advanced
    // past the dead prefix, the H3 update-path prune collapses it (REDIRECT to the
    // live tail, dead members freed) to make room, so the update STILL takes the HOT
    // path: no new PK/secondary index entries appear.
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture.engine.create_table(&setup, &hot_schema()).unwrap();
    fixture
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    fixture.commit(100);

    // ~1900-byte notes: 4 versions (~7600B) leave the 8192B page too full for a 5th.
    let big = |tag: &str| format!("{tag}{}", "x".repeat(1900));
    let rid = fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            hot_row(1, "Ada", &big("v1")),
        )
        .unwrap();
    fixture.commit(10);
    let root = super::RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };
    // HOT-update the non-indexed note three times (txns 20,21,22): root -> v2 -> v3
    // -> v4, all on the one page, no new index entries. (No horizon needed yet — the
    // page still has room for each.)
    for (txn, tag) in [(20u64, "v2"), (21, "v3"), (22, "v4")] {
        assert!(
            fixture
                .engine
                .update(
                    &ctx(txn, snapshot(txn + 1, vec![])),
                    TABLE_ID,
                    &key(1),
                    hot_row(1, "Ada", &big(tag)),
                )
                .unwrap()
        );
        fixture.commit(txn);
    }
    assert_eq!(
        fixture.pk_index_tids(&key(1)),
        vec![root],
        "still one PK entry"
    );
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada").len(),
        1
    );

    // A 5th HOT update with horizon 100 (root/v2/v3/v4 deleters all < 100 except v4
    // which is the live tail): with no same-page room, the update-path prune
    // collapses the dead prefix (root,v2,v3) to reclaim space, then the HOT insert
    // succeeds on the same page — NO new index entries.
    assert!(
        fixture
            .engine
            .update(
                &ctx_h(40, snapshot(41, vec![]), 100),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", &big("v5")),
            )
            .unwrap()
    );
    fixture.commit(40);

    // STILL exactly one PK + one secondary entry: the update stayed on the HOT path
    // (pruning made room), so no fully-indexed fallback version was written.
    assert_eq!(
        fixture.pk_index_tids(&key(1)),
        vec![root],
        "update stayed HOT — no new PK entry",
    );
    assert_eq!(
        fixture.secondary_index_tids(name_index().id, "Ada").len(),
        1,
        "no new secondary entry",
    );
    // The root collapsed to a REDIRECT (its original tuple was in the dead prefix).
    assert!(matches!(
        fixture.slot_state_hot(root.page_num, root.slot_num),
        crate::page::LinePointer::Redirect(_)
    ));
    // The latest value reads back through both index paths.
    let reader = ctx(0, snapshot(120, vec![]));
    assert_eq!(
        fixture.engine.get(&reader, TABLE_ID, &key(1)).unwrap(),
        Some(hot_row(1, "Ada", &big("v5")))
    );
    let by_name = collect_names(
        fixture
            .engine
            .index_scan(&reader, TABLE_ID, name_index().id, &name_eq("Ada"))
            .unwrap(),
    );
    assert_eq!(by_name, vec![hot_row(1, "Ada", &big("v5"))]);
}

#[test]
fn update_path_falls_back_when_pruning_cannot_free_enough() {
    // When the predecessor's page is full of LIVE (non-prunable) data, the
    // update-path prune frees nothing, so the HOT update falls back to a normal
    // fully-indexed update (a new tuple on another page + a new PK entry).
    let fixture = Fixture::new();
    let setup = ctx(100, snapshot(101, vec![]));
    fixture.engine.create_table(&setup, &hot_schema()).unwrap();
    fixture
        .engine
        .create_index(&setup, &name_index(), 0)
        .unwrap();
    fixture.commit(100);

    let big = "x".repeat(3000);
    // Row 1 (the update target) + a big LIVE filler row 2 share the page, leaving
    // < 3000 bytes free. Both creators commit, so neither is dead-to-all → pruning
    // reclaims nothing.
    let rid = fixture
        .engine
        .insert(
            &ctx(10, snapshot(11, vec![])),
            TABLE_ID,
            hot_row(1, "Ada", &big),
        )
        .unwrap();
    let root = super::RowLocation {
        file_id: TABLE_ID,
        page_num: rid.page_num,
        slot_num: rid.slot_num,
    };
    fixture
        .engine
        .insert(
            &ctx(12, snapshot(13, vec![])),
            TABLE_ID,
            hot_row(2, "filler", &big),
        )
        .unwrap();
    fixture.commit(12);
    fixture.commit(10);
    assert_eq!(
        fixture.pk_index_tids(&key(2))[0].page_num,
        root.page_num,
        "filler shares row 1's page",
    );

    // HOT-update row 1's note with another big value at a high horizon: pruning
    // finds nothing dead-to-all (everything live), so it cannot free room ⇒ fall
    // back to a normal update (new tuple on a fresh page, new PK entry).
    assert!(
        fixture
            .engine
            .update(
                &ctx_h(40, snapshot(41, vec![]), 100),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", &"y".repeat(3000)),
            )
            .unwrap()
    );
    fixture.commit(40);

    let pk = fixture.pk_index_tids(&key(1));
    assert_eq!(pk.len(), 2, "fell back to a fully-indexed update");
    let new_loc = *pk.iter().find(|loc| **loc != root).unwrap();
    assert_ne!(
        new_loc.page_num, root.page_num,
        "the fallback version is on another page",
    );
    let new_dec = decode_hot(&fixture, new_loc);
    assert_eq!(
        new_dec.infomask & crate::codec::HEAP_ONLY,
        0,
        "not heap-only"
    );
    // The updated row still reads back.
    assert_eq!(
        fixture
            .engine
            .get(&ctx(0, snapshot(120, vec![])), TABLE_ID, &key(1))
            .unwrap(),
        Some(hot_row(1, "Ada", &"y".repeat(3000)))
    );
}

#[test]
fn create_index_over_a_broken_live_hot_chain_aborts_retryable() {
    // A HOT chain whose versions differ on a NOT-yet-indexed column (`note`),
    // with an OLD version kept live by a low horizon, makes CREATE INDEX(note)
    // fail-fast with 40001; with the horizon advanced past those versions the
    // build succeeds.
    let (fixture, _root) = hot_fixture();

    // HOT-update the note v1 -> v2 (both versions present on the chain). The root
    // (note "v1", xmax = 20 committed) and the heap-only successor (note "v2").
    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", "v2"),
            )
            .unwrap()
    );
    fixture.commit(20);

    let note_index = IndexSchema {
        id: 2,
        table: TABLE_ID,
        name: "users_note".to_string(),
        columns: vec![2], // the `note` column
        unique: false,
    };

    // Horizon 15 (below the deleter xmax = 20): the root version (note "v1") is
    // NOT dead_to_all, and the heap-only successor (note "v2") is live too — two
    // live versions differing on `note` ⇒ broken chain ⇒ retryable 40001.
    let builder = ctx(101, snapshot(102, vec![]));
    let err = fixture
        .engine
        .create_index(&builder, &note_index, 15)
        .unwrap_err();
    assert_eq!(err.code, common::SqlState::SerializationFailure);
    assert!(err.message.contains("HOT chain"), "{}", err.message);

    // Horizon 21 (above xmax = 20): the root (committed-deleted below horizon) is
    // dead_to_all, so only the heap-only "v2" is live ⇒ NOT broken ⇒ build
    // succeeds and the new index finds the live row by its `note`.
    fixture
        .engine
        .create_index(&builder, &note_index, 21)
        .unwrap();
    fixture.commit(101);

    let by_note = collect_names(
        fixture
            .engine
            .index_scan(
                &ctx(0, snapshot(120, vec![])),
                TABLE_ID,
                note_index.id,
                &KeyRange::Exact(Key(vec![Value::Text("v2".to_string())])),
            )
            .unwrap(),
    );
    assert_eq!(by_note, vec![hot_row(1, "Ada", "v2")]);
}

#[test]
fn create_index_indexes_a_chain_live_to_an_older_reader_but_not_to_the_builder() {
    // A not-dead-to-all version that the BUILDER's own snapshot cannot see (it is
    // deleted in the builder's past, but the deleter is at/above the GC horizon so
    // an OLDER lock-free reader still sees it) MUST still get an index entry —
    // indexing is unconditional, not gated on the builder's snapshot. (Regression:
    // a build-visibility gate would skip it and lose that older reader's read.)
    let (fixture, root) = hot_fixture();

    // HOT-update note v1 -> v2 (txn 20), then DELETE the row (txn 80). The chain is
    // root("v1", xmax=20) -> heap-only("v2", xmin=20, xmax=80). Both deleters
    // commit.
    assert!(
        fixture
            .engine
            .update(
                &ctx(20, snapshot(21, vec![])),
                TABLE_ID,
                &key(1),
                hot_row(1, "Ada", "v2"),
            )
            .unwrap()
    );
    fixture.commit(20);
    assert!(
        fixture
            .engine
            .delete(&ctx(80, snapshot(81, vec![])), TABLE_ID, &key(1))
            .unwrap()
    );
    fixture.commit(80);

    let note_index = IndexSchema {
        id: 2,
        table: TABLE_ID,
        name: "users_note".to_string(),
        columns: vec![2],
        unique: false,
    };

    // Horizon 50: the root (xmax=20 < 50, committed) is dead_to_all, but the
    // heap-only "v2" (xmax=80 >= 50) is NOT — an older reader with xmin around 50
    // could still see it. The BUILDER's snapshot (xmax=120) sees the whole chain as
    // deleted. The single live key "v2" must still be indexed at the root.
    let builder = ctx(101, snapshot(120, vec![]));
    fixture
        .engine
        .create_index(&builder, &note_index, 50)
        .unwrap();
    fixture.commit(101);

    // The entry exists and points at the chain ROOT.
    let tids: Vec<_> = fixture
        .engine
        .secondary_btree(note_index.id)
        .scan_key(&Key(vec![Value::Text("v2".to_string())]))
        .unwrap();
    assert_eq!(tids, vec![root], "v2 is indexed at the chain root");

    // An older reader (snapshot where the deleter 80 is still in-progress) finds
    // the row via the new index — the read that the build-visibility gate would
    // have lost.
    let older = ctx(0, snapshot(90, vec![80]));
    let by_note = collect_names(
        fixture
            .engine
            .index_scan(
                &older,
                TABLE_ID,
                note_index.id,
                &KeyRange::Exact(Key(vec![Value::Text("v2".to_string())])),
            )
            .unwrap(),
    );
    assert_eq!(by_note, vec![hot_row(1, "Ada", "v2")]);
}
