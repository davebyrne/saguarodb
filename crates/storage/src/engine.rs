//! ## Structural write latches and lock ordering (Milestone E2a)
//!
//! Stage-2 concurrency (`docs/specs/mvcc.md` §7.1, §10 E2a) serializes structural
//! mutations **within** one index or one table heap while allowing concurrent
//! writers across *different* indexes/heaps and lock-free B-link readers. The
//! substrate is a per-[`FileId`] registry of `Arc<parking_lot::Mutex<()>>` latches
//! ([`PageBackedStorageEngine::structural_latch`]); the engine is shared via `Arc`,
//! so two transactions mutating the same index/heap contend on the same latch.
//!
//! The on-disk B-tree splits without latch coupling (it releases the latch between
//! levels and re-acquires the parent to propagate a split), and heap free-space
//! search reads a page, drops the read latch, then re-acquires write — both are
//! unsafe for concurrent structural writers (a fully-concurrent B-link tree and a
//! free-space map are deferred — `mvcc.md` §12). A per-index / per-heap structural
//! latch held across the *whole* operation closes both windows.
//!
//! **Lock-ordering contract (followed uniformly to prevent deadlock):**
//!
//! 1. **Never hold two structural latches simultaneously.** Each structural latch is
//!    acquired, the mutation runs, then it is released *before* the next structural
//!    latch is taken (the heap-insertion latch is released before the index latches;
//!    the PK-index latch is released before each secondary-index latch). Because no
//!    structural latch is ever held while acquiring another, there is no multi-latch
//!    deadlock regardless of index order — simpler and safer than a deterministic
//!    ordering scheme.
//! 2. **Never acquire a structural latch while holding a buffer-pool frame latch.**
//!    The order is always: structural latch → (inside the btree/heap op) frame latch
//!    → (inside a WAL append) WAL mutex. No path takes `read_page`/`write_page` and
//!    then acquires a structural latch (that inversion could deadlock). The E1b
//!    `stamp_xmax_logged` conflict check takes only a frame latch and **no**
//!    structural latch (an in-place `xmax` stamp allocates no free space and mutates
//!    a known slot), so it does not participate in this ordering.
//!
//! As of Milestone E2b (the shared-writer / exclusive-checkpoint lock inversion)
//! many writers run concurrently, so these structural latches are now **load-bearing
//! and genuinely contended**: two writers mutating the same index or heap serialize
//! on its latch, while writers on different indexes/heaps run in parallel. A
//! checkpoint takes the exclusive concurrency guard and so never overlaps a writer.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use buffer::{BufferPool, PageWriteGuard};
use common::{
    ColumnId, ColumnInfo, DbError, FileId, IndexId, IndexSchema, Key, KeyRange, Lsn, PageNum,
    Result, Row, RowId, Snapshot, SqlState, StatementContext, StoredRow, TableId, TableSchema,
    TxnStatusView, UniqueConflict, Value, WriteConflict, classify_unique_conflict, is_visible,
    write_conflict,
};
use parking_lot::Mutex as PlMutex;
use wal::{WalManager, WalRecord, WalRecordKind};

use crate::btree::BTree;
use crate::codec::{decode_row, encode_row};
use crate::heap::{index_file_id, secondary_index_file_id};
use crate::page;
use crate::traits::{RowIterator, SchemaOperations, StorageEngine};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageMode {
    Recovery,
    Normal,
}

/// The transaction id VACUUM stamps on the pages it prunes (`docs/specs/mvcc.md`
/// §9). It is `0` — the recovery/maintenance convention shared with
/// `fetch_for_redo` and the recovery DDL cascade ("txn 0 means no rollback
/// tracking") — because VACUUM is non-transactional maintenance: its reclamation
/// must never be undone by an abort and must not hinge on a user commit. A pruned
/// page is logged as an unconditional `FullPageImage`, which recovery reinstalls by
/// PageLSN gating alone, independent of this txn id.
const VACUUM_TXN: u64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowLocation {
    pub file_id: FileId,
    pub page_num: PageNum,
    pub slot_num: u16,
}

#[derive(Clone)]
struct TableState {
    schema: TableSchema,
    dropped: bool,
}

#[derive(Clone)]
struct IndexState {
    schema: IndexSchema,
    dropped: bool,
}

#[derive(Default)]
struct TxnRollback {
    tables: BTreeMap<TableId, Option<TableState>>,
    indexes: BTreeMap<IndexId, Option<IndexState>>,
}

struct StorageState {
    mode: StorageMode,
    tables: BTreeMap<TableId, TableState>,
    indexes: BTreeMap<IndexId, IndexState>,
    rollback: BTreeMap<u64, TxnRollback>,
}

pub struct PageBackedStorageEngine {
    pub(crate) buffer_pool: Arc<dyn BufferPool>,
    pub(crate) wal: Arc<dyn WalManager>,
    state: Mutex<StorageState>,
    /// Per-[`FileId`] structural write latches (Milestone E2a; see the module-level
    /// lock-ordering doc). Lazily populated: the registry `Mutex` is held only
    /// briefly to look up or insert a file's `Arc<Mutex>`, never across the
    /// structural operation itself (else all structural ops would serialize
    /// globally). Shared across all transactions because the engine is shared via
    /// `Arc`, so two txns mutating the same index/heap contend on the same latch.
    structural_latches: Mutex<HashMap<FileId, Arc<PlMutex<()>>>>,
}

impl PageBackedStorageEngine {
    pub fn open(
        buffer_pool: Arc<dyn BufferPool>,
        wal: Arc<dyn WalManager>,
        mode: StorageMode,
    ) -> Result<Self> {
        Ok(Self {
            buffer_pool,
            wal,
            state: Mutex::new(StorageState {
                mode,
                tables: BTreeMap::new(),
                indexes: BTreeMap::new(),
                rollback: BTreeMap::new(),
            }),
            structural_latches: Mutex::new(HashMap::new()),
        })
    }

    /// The structural write latch for `file_id` (a heap, primary-key index, or
    /// secondary-index file), serializing structural mutations *within* that file
    /// (Milestone E2a; `docs/specs/mvcc.md` §7.1, §10 E2a). The registry `Mutex` is
    /// locked only **briefly** — to look up the file's latch or lazily insert a fresh
    /// one — and dropped before the returned `Arc<Mutex>` is locked, so the registry
    /// never serializes the structural work; only same-file structural ops contend.
    ///
    /// Returns the SAME `Arc<Mutex>` for a given `FileId` across calls (so two writers
    /// on one index/heap share one latch) and a DIFFERENT one per file (so writers on
    /// different indexes/heaps run concurrently). As of E2b (concurrent writers) the
    /// latch is genuinely contended: same-file writers serialize on it.
    pub(crate) fn structural_latch(&self, file_id: FileId) -> Arc<PlMutex<()>> {
        let mut latches = self
            .structural_latches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(latches.entry(file_id).or_default())
    }

    pub fn install_schemas(&self, schemas: Vec<TableSchema>) -> Result<()> {
        let mut state = self.lock_state()?;
        state.tables.clear();
        for schema in schemas {
            state.tables.insert(
                schema.id,
                TableState {
                    schema,
                    dropped: false,
                },
            );
        }
        Ok(())
    }

    /// Install the live secondary-index schemas (from the catalog at startup), so
    /// DML maintains them. Replaces any previously installed index set.
    pub fn install_index_schemas(&self, schemas: Vec<IndexSchema>) -> Result<()> {
        let mut state = self.lock_state()?;
        state.indexes.clear();
        for schema in schemas {
            state.indexes.insert(
                schema.id,
                IndexState {
                    schema,
                    dropped: false,
                },
            );
        }
        Ok(())
    }

    pub fn set_mode(&self, mode: StorageMode) -> Result<()> {
        self.lock_state()?.mode = mode;
        Ok(())
    }

    /// The CLOG-backed [`TxnStatusView`] for the visibility predicate
    /// (`common::is_visible`, `docs/specs/mvcc.md` §6). The engine already holds an
    /// `Arc<dyn WalManager>`, and the WAL manager *is* a `TxnStatusView` (backed by
    /// its in-memory CLOG), so this hands out `&dyn TxnStatusView` with no extra
    /// handle — the "injection" of transaction status into the engine. The
    /// snapshot-aware read paths (`read_visible_row`, consumed by `get`/`scan_range`/
    /// `index_scan`) consult it to settle each candidate tuple's `xmin`/`xmax`.
    pub(crate) fn txn_status_view(&self) -> &dyn TxnStatusView {
        // Trait upcast: `dyn WalManager` has `TxnStatusView` as a supertrait.
        self.wal.as_ref()
    }

    pub(crate) fn apply_create_table_without_wal(&self, schema: TableSchema) -> Result<()> {
        // Recovery replays the index pages from their full-page-image redo
        // records, so this installs metadata only; it must not create the tree.
        let mut state = self.lock_state()?;
        state.tables.insert(
            schema.id,
            TableState {
                schema,
                dropped: false,
            },
        );
        Ok(())
    }

    pub(crate) fn apply_drop_table_without_wal(&self, table: TableId) -> Result<()> {
        let mut state = self.lock_state()?;
        if let Some(table_state) = state.tables.get_mut(&table) {
            table_state.dropped = true;
        }
        // Recovery replays a single DropTable record; cascade to the table's
        // indexes here, matching the catalog's apply_drop_table cascade. txn 0
        // means no rollback tracking.
        mark_table_indexes_dropped(&mut state, 0, table);
        Ok(())
    }

    pub(crate) fn apply_create_index_without_wal(&self, schema: IndexSchema) -> Result<()> {
        // Like apply_create_table_without_wal: the secondary tree's pages are
        // replayed from their full-page-image redo records, so this installs index
        // metadata only and must not build or backfill the tree.
        let mut state = self.lock_state()?;
        state.indexes.insert(
            schema.id,
            IndexState {
                schema,
                dropped: false,
            },
        );
        Ok(())
    }

    pub(crate) fn apply_drop_index_without_wal(&self, index: IndexId) -> Result<()> {
        let mut state = self.lock_state()?;
        if let Some(index_state) = state.indexes.get_mut(&index) {
            index_state.dropped = true;
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, StorageState>> {
        self.state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))
    }

    /// The schema and index file id of a live table, looked up under the lock so
    /// the heap and B-tree work can run without holding it.
    fn table_handle(&self, table: TableId) -> Result<(TableSchema, FileId)> {
        let state = self.lock_state()?;
        let table_state = live_table(&state, table)?;
        Ok((table_state.schema.clone(), index_file_id(table)))
    }

    /// Like `table_handle`, but a missing or dropped table yields `None` (callers
    /// that treat that as a no-op rather than an error).
    fn table_handle_opt(&self, table: TableId) -> Result<Option<(TableSchema, FileId)>> {
        let state = self.lock_state()?;
        match state.tables.get(&table) {
            Some(table_state) if !table_state.dropped => {
                Ok(Some((table_state.schema.clone(), index_file_id(table))))
            }
            _ => Ok(None),
        }
    }

    /// The live secondary indexes on a table, ordered by index id. DML consults
    /// this to keep every index in sync with the heap.
    fn table_indexes(&self, table: TableId) -> Result<Vec<IndexSchema>> {
        let state = self.lock_state()?;
        Ok(state
            .indexes
            .values()
            .filter(|index| !index.dropped && index.schema.table == table)
            .map(|index| index.schema.clone())
            .collect())
    }

    /// Check that an index is live and belongs to `table`, erroring otherwise (a
    /// dropped index keeps its pages as bloat and must not be scanned).
    fn ensure_index_live(&self, table: TableId, index: IndexId) -> Result<()> {
        let state = self.lock_state()?;
        match state.indexes.get(&index) {
            Some(index_state) if !index_state.dropped && index_state.schema.table == table => {
                Ok(())
            }
            _ => Err(undefined_index(index)),
        }
    }

    fn btree(&self, index_file_id: FileId) -> BTree<'_, RowLocation> {
        BTree::new(self.buffer_pool.as_ref(), self.wal.as_ref(), index_file_id)
    }

    /// The B-tree for a secondary index. Uniform with the primary-key index: keyed
    /// by the indexed columns and storing the heap `RowLocation` (TID) as its value,
    /// so duplicate indexed values are disambiguated by the `(key, tid)` ordering.
    fn secondary_btree(&self, index: IndexId) -> BTree<'_, RowLocation> {
        BTree::new(
            self.buffer_pool.as_ref(),
            self.wal.as_ref(),
            secondary_index_file_id(index),
        )
    }

    /// Insert `(entry_key, location)` into a secondary index, enforcing uniqueness
    /// for a unique index. The secondary key is the indexed column(s) alone (no pk
    /// tiebreaker); duplicate indexed values are disambiguated by the heap TID in
    /// `(key, tid)` order. A unique index rejects a duplicate non-NULL indexed value
    /// via the shared visibility-aware [`Self::unique_conflict_kind`] check (it
    /// conflicts only with an alive-or-potentially-alive version; dead/aborted
    /// versions are ignored). A committed-live duplicate raises
    /// [`SqlState::UniqueViolation`] (`23505`); a value held only by another
    /// in-progress inserter raises [`SqlState::SerializationFailure`] (`40001`,
    /// retry — §7.3). A NULL indexed value never participates in a unique constraint
    /// (SQL treats NULLs as distinct), so the check is skipped when `has_null`;
    /// distinct NULL rows coexist because their heap TIDs differ.
    fn insert_secondary_entry(
        &self,
        ctx: &StatementContext,
        table_schema: &TableSchema,
        index: &IndexSchema,
        entry_key: &Key,
        has_null: bool,
        location: &RowLocation,
    ) -> Result<()> {
        let secondary = self.secondary_btree(index.id);
        // Hold this secondary index's structural latch across the uniqueness check
        // AND the insert atomically (Milestone E2a). For a unique secondary the scan
        // (`unique_conflict_kind`) and the mutation (`insert`, including any split /
        // root split) must be under ONE latch hold, or two concurrent inserts of the
        // same value could both pass the check and both insert a duplicate. For a
        // non-unique secondary there is no check, but the latch still serializes the
        // split protocol against another structural writer on this same index. The
        // latch is released on return, before the caller takes any other structural
        // latch (rule 1: never two structural latches at once). Contended under E2b's
        // concurrent writers: same-secondary writers serialize here.
        let latch = self.structural_latch(secondary_index_file_id(index.id));
        let _index_guard = latch.lock();
        if index.unique && !has_null {
            match self.unique_conflict_kind(&secondary, entry_key, table_schema, ctx.txn_id)? {
                UniqueConflict::Violation => return Err(duplicate_unique_index(&index.name)),
                UniqueConflict::InFlight => return Err(unique_conflict_retry()),
                UniqueConflict::None => {}
            }
        }
        secondary.insert(ctx.txn_id, entry_key, location)
    }

    /// Append a WAL record (in `Normal` mode only) and return its assigned LSN.
    /// Returns `0` in recovery mode, where the record is not produced.
    fn append_wal(
        &self,
        state: &StorageState,
        ctx: &StatementContext,
        kind: WalRecordKind,
    ) -> Result<Lsn> {
        if state.mode == StorageMode::Normal {
            self.wal.append(WalRecord {
                lsn: 0,
                txn_id: ctx.txn_id,
                kind,
            })
        } else {
            Ok(0)
        }
    }

    fn write_new_row(&self, schema: &TableSchema, row: &Row, txn_id: u64) -> Result<RowLocation> {
        let row_bytes = encode_row(schema, row, txn_id)?;
        if row_bytes.len() + page_overhead() > buffer::PAGE_SIZE {
            return Err(DbError::storage(
                SqlState::InternalError,
                "row is too large for a data page",
            ));
        }

        let file_id = schema.id;
        // Hold the per-heap-file structural latch across the WHOLE free-space search
        // + allocate + insert (Milestone E2a). This makes "find space / extend /
        // insert / log" atomic against another inserter on the same table heap,
        // closing the TOCTOU where the read-check-drop-rewrite below would let two
        // concurrent inserters both target the same last slot. The latch wraps the
        // existing-page scan, the `new_page` extension, and `log_insert`; it is
        // dropped on return so a later index insert takes its own latch (rule 1: never
        // two structural latches at once). Contended under E2b's concurrent writers:
        // same-heap inserters serialize here. (Lock order: structural latch → frame
        // latch inside `read_page`/`write_page`/`new_page` → WAL mutex inside the
        // appends.)
        let latch = self.structural_latch(file_id);
        let _heap_guard = latch.lock();
        for page_num in self.table_page_nums(file_id)? {
            let readable = self.buffer_pool.read_page(file_id, page_num)?;
            let has_space = page::has_space_for(readable.data(), row_bytes.len())?;
            drop(readable);
            if has_space {
                let mut writable = self.buffer_pool.write_page(file_id, page_num, txn_id)?;
                let slot_num =
                    self.log_insert(&mut writable, txn_id, file_id, page_num, &row_bytes)?;
                return Ok(RowLocation {
                    file_id,
                    page_num,
                    slot_num,
                });
            }
        }

        // Allocate a fresh page. HeapInit is the page's own redo base, so a new
        // page never needs a separate full-page image.
        let mut writable = self.buffer_pool.new_page(file_id, txn_id)?;
        let page_num = writable.page_num();
        let init_lsn = self.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::HeapInit { file_id, page_num },
        })?;
        page::init_page(writable.data_mut(), page_num);
        page::set_page_lsn(writable.data_mut(), init_lsn);
        let slot_num = self.log_insert(&mut writable, txn_id, file_id, page_num, &row_bytes)?;
        Ok(RowLocation {
            file_id,
            page_num,
            slot_num,
        })
    }

    /// Insert a row into a pinned page and log its redo record: a full-page image
    /// on the first modification since the last checkpoint (torn-page protection),
    /// otherwise a `HeapInsert` delta. Stamps the page-LSN with the record's LSN.
    fn log_insert(
        &self,
        guard: &mut PageWriteGuard,
        txn_id: u64,
        file_id: FileId,
        page_num: PageNum,
        row_bytes: &[u8],
    ) -> Result<u16> {
        if guard.take_needs_fpi() {
            let slot_num = page::insert_row(guard.data_mut(), row_bytes)?;
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::FullPageImage {
                    file_id,
                    page_num,
                    image: guard.data().to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), lsn);
            Ok(slot_num)
        } else {
            // Insert into the buffer FIRST, then log the slot id it actually landed
            // in. `insert_row` recycles an UNUSED slot id before appending (F3b), so
            // the produced slot is no longer predictable as `next_slot`; logging the
            // real slot keeps the `HeapInsert` redo exact (its redo re-runs
            // `insert_row` and asserts the same slot id is reproduced). Mutating the
            // buffer before appending the record mirrors the FPI arm above and is
            // WAL-safe: the page-LSN is stamped with the record's LSN below, so the
            // dirty page cannot be flushed ahead of its WAL record.
            let slot_num = page::insert_row(guard.data_mut(), row_bytes)?;
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::HeapInsert {
                    file_id,
                    page_num,
                    slot: slot_num,
                    row_bytes: row_bytes.to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), lsn);
            Ok(slot_num)
        }
    }

    /// Write a HOT heap-only successor tuple onto **the predecessor's own page**
    /// (`page_num`), or return `Ok(None)` when the page has no room (so the caller
    /// falls back to a normal fully-indexed update). This is the placement half of
    /// the HOT-update fast path (`docs/specs/mvcc.md` §10 Milestone H2): unlike
    /// [`Self::write_new_row`] (which picks *any* page with space), HOT must keep the
    /// new version on the predecessor's page so the bounded `t_ctid` walk (H1) reaches
    /// it from the indexed root without a new index entry.
    ///
    /// The tuple is encoded with [`crate::codec::HEAP_ONLY`] set in its header
    /// (`xmin = txn_id`, `xmax = invalid`, `t_ctid = self`), so the bit is carried
    /// into the logged `HeapInsert` image and redone on recovery (the row bytes are
    /// the source of truth for `infomask`). It is logged exactly like
    /// [`Self::log_insert`] (a `FullPageImage` on first touch since the checkpoint,
    /// else a `HeapInsert` delta), so recovery reinstalls it identically.
    ///
    /// **Latching.** Takes the per-heap structural latch then the frame write latch
    /// for `page_num` (lock order structural → frame → WAL), both released on return.
    /// The space peek is done **before** consuming the page's first-touch FPI flag,
    /// so a no-room fall-back does not perturb the page's WAL state.
    fn try_hot_insert_on_page(
        &self,
        schema: &TableSchema,
        page_num: PageNum,
        row: &Row,
        txn_id: u64,
    ) -> Result<Option<RowLocation>> {
        let file_id = schema.id;
        let row_bytes =
            crate::codec::encode_row_with_infomask(schema, row, txn_id, crate::codec::HEAP_ONLY)?;

        let latch = self.structural_latch(file_id);
        let _heap_guard = latch.lock();
        let mut guard = self.buffer_pool.write_page(file_id, page_num, txn_id)?;

        // Peek whether the new tuple fits on THIS page before touching any WAL state
        // (so a fall-back leaves the page's first-touch FPI flag intact).
        if !page::has_space_for(guard.data(), row_bytes.len())? {
            return Ok(None);
        }

        let slot_num = self.log_insert(&mut guard, txn_id, file_id, page_num, &row_bytes)?;
        Ok(Some(RowLocation {
            file_id,
            page_num,
            slot_num,
        }))
    }

    /// Stamp `xmax = txn_id` and `t_ctid` on the version at `location` **in place**
    /// and log its redo record (a full-page image on first touch since the last
    /// checkpoint, else a `HeapUpdateHeader` delta). The line pointer stays
    /// `NORMAL`: the tuple is physically present and is hidden purely by visibility
    /// once the stamping transaction commits (`docs/specs/mvcc.md` §3.2 invariant
    /// 1). `infomask` is carried through unchanged (no hint bits set here — that is
    /// the optional commit 10).
    ///
    /// This is the shared "mark a version superseded" write for both MVCC writes:
    /// `DELETE` passes `t_ctid = INVALID_TID` (a delete has no successor version);
    /// `UPDATE` passes `t_ctid = new_tid`, the forward version-chain pointer to the
    /// new tuple (invariant 5). It never removes the tuple or its index entries
    /// (VACUUM reclaims them, Milestone F).
    ///
    /// **First-updater-wins conflict check (E1b, `docs/specs/mvcc.md` §7.3).**
    /// `xmax` doubles as the row lock. Under the `write_page` frame latch — and
    /// **before** appending any WAL record or mutating the page — this re-reads the
    /// target version's *current physical* header (`xmax`/`infomask`) and runs
    /// [`common::write_conflict`]. The read-classify-stamp sequence is atomic on the
    /// frame latch: two concurrent writers racing to claim this version serialize on
    /// the latch, so the loser observes the winner's just-stamped `xmax` and aborts
    /// with [`SqlState::SerializationFailure`] (`40001`) — no WAL is appended and the
    /// header is left untouched on conflict. Checking `xmax` earlier (e.g. at
    /// `locate_visible_version` time) and stamping later under a fresh latch would be
    /// a TOCTOU race that defeats first-updater-wins, so the check lives here, inside
    /// the latch, next to the stamp. As of E2b (concurrent writers) this is
    /// load-bearing: when two writers race to delete/update the same version, the
    /// loser observes the winner's `xmax` and aborts with `40001`.
    fn stamp_xmax_logged(
        &self,
        location: RowLocation,
        t_ctid: (PageNum, u16),
        infomask: u16,
        txn_id: u64,
    ) -> Result<()> {
        let mut guard = self
            .buffer_pool
            .write_page(location.file_id, location.page_num, txn_id)?;

        // Atomic first-updater-wins check: read the version's CURRENT physical
        // `xmax`/`infomask` under this frame latch and classify against the live
        // CLOG. A `Conflict` (the deleter committed-after-my-snapshot or is another
        // in-flight writer) fails fast — returning here appends NO WAL record and
        // leaves the header unstamped, so the winning writer's `xmax` stands.
        let current = page::read_row(guard.data(), location.slot_num)?
            .ok_or_else(|| storage_internal("cannot stamp xmax on a non-live slot"))?;
        let (_xmin, current_xmax, _t_ctid, current_infomask) =
            crate::codec::decode_mvcc_header(&current)?;
        if write_conflict(
            current_xmax,
            current_infomask,
            txn_id,
            self.txn_status_view(),
        ) == WriteConflict::Conflict
        {
            return Err(DbError::execute(
                SqlState::SerializationFailure,
                "could not serialize access due to concurrent update",
            ));
        }

        if guard.take_needs_fpi() {
            // Mutate the header first, then capture the page in a full-page image.
            // Keep the existing page-LSN on this in-place stamp; the FPI append
            // below assigns the record's LSN as the new page-LSN.
            let current_lsn = page::page_lsn(guard.data());
            page::set_tuple_header(
                guard.data_mut(),
                location.slot_num,
                txn_id,
                t_ctid,
                infomask,
                current_lsn,
            )?;
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::FullPageImage {
                    file_id: location.file_id,
                    page_num: location.page_num,
                    image: guard.data().to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), lsn);
        } else {
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::HeapUpdateHeader {
                    file_id: location.file_id,
                    page_num: location.page_num,
                    slot: location.slot_num,
                    xmax: txn_id,
                    t_ctid,
                    infomask,
                },
            })?;
            page::set_tuple_header(
                guard.data_mut(),
                location.slot_num,
                txn_id,
                t_ctid,
                infomask,
                lsn,
            )?;
        }
        Ok(())
    }

    /// Read the *current physical* row at `location`, ignoring snapshot
    /// visibility. Used by index-maintenance paths (delete/update/index backfill)
    /// that must see the live tuple to recompute its index keys, not the version a
    /// reader's snapshot would observe. User-facing reads use
    /// [`Self::read_visible_row`] instead. Returns `None` if the line pointer is
    /// absent (DEAD/UNUSED).
    fn read_location(&self, schema: &TableSchema, location: RowLocation) -> Result<Option<Row>> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
            return Ok(None);
        };
        Ok(Some(decode_row(schema, &bytes)?.row))
    }

    /// Resolve an index entry's TID — possibly a HOT root — to the heap slot of the
    /// single version **visible** to `snapshot` from `current_txn`, reading the
    /// `location` page once under a read latch (pure: no page mutation; pruning is
    /// the UPDATE/VACUUM path's job, `mvcc.md` §10 Milestone H). The two-step
    /// resolution (`mvcc.md` §5.2, §10 Milestone H1) is:
    ///
    /// 1. **REDIRECT resolution.** If `location.slot_num` is a `REDIRECT` line
    ///    pointer (a HOT root whose original tuple was pruned), follow it to its
    ///    same-page target. The target MUST be `NORMAL`: a redirect-to-redirect or
    ///    redirect-to-dead is corruption and returns a structured error rather than
    ///    looping. A `DEAD`/`UNUSED` root slot resolves to no version (`Ok(None)`).
    /// 2. **Bounded HOT-chain walk.** From the resolved root tuple, walk the forward
    ///    `t_ctid` chain, returning the first version [`is_visible`] accepts. THE
    ///    correctness invariant: the walk follows `t_ctid` into a successor **only
    ///    when the current tuple is `HOT_UPDATED` and the successor is `HEAP_ONLY`**
    ///    on the same page — i.e. it stays strictly within one HOT-chain segment. It
    ///    STOPS at any successor that is independently indexed (not `HEAP_ONLY`),
    ///    because that successor is reachable via its OWN index entry; following it
    ///    here would let one visible row be returned through two index entries
    ///    (double-count). Termination is guaranteed by a visited-slot set (so a
    ///    cyclic `t_ctid` from corruption errors instead of spinning).
    ///
    /// Returns the visible version's `(RowLocation, infomask)`; `None` when no
    /// version in the chain is visible (deleted/aborted/never-present) or the root
    /// slot is reclaimed. With no HOT tuples in the heap yet (H2/H3 unimplemented),
    /// every root is `NORMAL` with `t_ctid = INVALID_TID`, so this resolves the root
    /// slot itself and the walk is a single step — behavior-identical to the prior
    /// single-tuple visibility check.
    fn resolve_visible_in_chain(
        &self,
        schema: &TableSchema,
        location: RowLocation,
        snapshot: &Snapshot,
        current_txn: u64,
    ) -> Result<Option<(RowLocation, u16)>> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        let data = readable.data();
        let page_num = location.page_num;
        let file_id = location.file_id;

        // Step 1: resolve a REDIRECT root to its same-page NORMAL target.
        let mut current_slot = match page::slot_state(data, location.slot_num)? {
            page::LinePointer::Normal => location.slot_num,
            page::LinePointer::Redirect(target) => {
                // A REDIRECT always points within the same page at a NORMAL slot.
                match page::slot_state(data, target)? {
                    page::LinePointer::Normal => target,
                    _ => {
                        return Err(storage_internal(
                            "redirect line pointer target is not a NORMAL tuple",
                        ));
                    }
                }
            }
            // A reclaimed (DEAD/UNUSED) root slot resolves to no version.
            page::LinePointer::Dead | page::LinePointer::Unused => return Ok(None),
        };

        // Step 2: bounded HOT-chain walk from the resolved root. Termination is
        // guaranteed by `visited` (a cyclic `t_ctid` becomes a structured error, not
        // a spin); the slot count is only a capacity hint for that set.
        let slot_count = page::next_slot(data)?;
        let mut visited: HashSet<u16> = HashSet::with_capacity(slot_count as usize);
        loop {
            if !visited.insert(current_slot) {
                return Err(storage_internal("cyclic HOT chain detected"));
            }

            // The resolved root is NORMAL (step 1) and every followed successor is
            // validated NORMAL before we step onto it, so a missing tuple here is a
            // corrupt chain, not a skippable reclaimed slot.
            let Some(bytes) = page::read_row(data, current_slot)? else {
                return Err(storage_internal("HOT chain member is not a live tuple"));
            };
            let decoded = decode_row(schema, &bytes)?;
            if is_visible(
                decoded.xmin,
                decoded.xmax,
                decoded.infomask,
                snapshot,
                current_txn,
                self.txn_status_view(),
            ) {
                return Ok(Some((
                    RowLocation {
                        file_id,
                        page_num,
                        slot_num: current_slot,
                    },
                    decoded.infomask,
                )));
            }

            // Decide whether to follow `t_ctid` into a heap-only successor. Stop
            // unless: this tuple was HOT-updated, its successor is on THIS page, and
            // that successor is HEAP_ONLY (so it has no index entry of its own and is
            // reachable only here). Any other case — latest version, a non-HOT
            // successor, or an off-page successor — is independently indexed/absent,
            // so we must not cross into it (double-count guard).
            if decoded.infomask & crate::codec::HOT_UPDATED == 0 {
                return Ok(None);
            }
            let (succ_page, succ_slot) = decoded.t_ctid;
            if succ_page != page_num {
                return Ok(None);
            }
            // Peek the successor's header: only a HEAP_ONLY, NORMAL successor is part
            // of this HOT-chain segment. A non-HEAP_ONLY successor is independently
            // indexed (stop); a non-NORMAL successor under a HOT_UPDATED pointer is
            // corruption.
            match page::slot_state(data, succ_slot)? {
                page::LinePointer::Normal => {}
                _ => {
                    return Err(storage_internal(
                        "HOT_UPDATED successor slot is not a NORMAL tuple",
                    ));
                }
            }
            let Some(succ_bytes) = page::read_row(data, succ_slot)? else {
                return Err(storage_internal(
                    "HOT_UPDATED successor is not a live tuple",
                ));
            };
            let (_xmin, _xmax, _t_ctid, succ_infomask) =
                crate::codec::decode_mvcc_header(&succ_bytes)?;
            if succ_infomask & crate::codec::HEAP_ONLY == 0 {
                // The successor is independently indexed — it is reached via its own
                // index entry, so stop here (do not double-count it).
                return Ok(None);
            }
            current_slot = succ_slot;
        }
    }

    /// Collect the physically-present versions of the HOT chain rooted at `root`, in
    /// chain order: the resolved root tuple plus every heap-only successor reached by
    /// the bounded `t_ctid` walk (the same `HOT_UPDATED → HEAP_ONLY`, same-page,
    /// stop-at-independently-indexed rule as [`Self::resolve_visible_in_chain`], but
    /// gathering ALL members instead of returning the first visible one). Each element
    /// is `(RowLocation, DecodedRow)` for a `NORMAL` member.
    ///
    /// Used by `create_index`'s HOT broken-chain check (`docs/specs/mvcc.md` §10
    /// Milestone H2): a non-HOT root resolves to a one-element vec (so a plain
    /// single-version table is untouched); a HOT chain yields its root + heap-only
    /// members so the build can test whether two not-dead-to-all versions disagree on
    /// the new index's key. Runs under the exclusive guard (stable physical view), so
    /// the walk is a pure read with no concurrent mutation. A `DEAD`/`UNUSED` root
    /// resolves to no versions (`Ok(vec![])`); a corrupt chain (cycle, bad redirect,
    /// non-NORMAL HOT successor) is a structured error, never a spin.
    fn collect_chain_versions(
        &self,
        schema: &TableSchema,
        root: RowLocation,
    ) -> Result<Vec<(RowLocation, crate::codec::DecodedRow)>> {
        let readable = self.buffer_pool.read_page(root.file_id, root.page_num)?;
        let data = readable.data();
        let page_num = root.page_num;
        let file_id = root.file_id;

        // Step 1: resolve a REDIRECT root to its same-page NORMAL target (mirrors
        // `resolve_visible_in_chain`).
        let mut current_slot = match page::slot_state(data, root.slot_num)? {
            page::LinePointer::Normal => root.slot_num,
            page::LinePointer::Redirect(target) => match page::slot_state(data, target)? {
                page::LinePointer::Normal => target,
                _ => {
                    return Err(storage_internal(
                        "redirect line pointer target is not a NORMAL tuple",
                    ));
                }
            },
            page::LinePointer::Dead | page::LinePointer::Unused => return Ok(Vec::new()),
        };

        let slot_count = page::next_slot(data)?;
        let mut visited: HashSet<u16> = HashSet::with_capacity(slot_count as usize);
        let mut versions = Vec::new();
        loop {
            if !visited.insert(current_slot) {
                return Err(storage_internal("cyclic HOT chain detected"));
            }
            let Some(bytes) = page::read_row(data, current_slot)? else {
                return Err(storage_internal("HOT chain member is not a live tuple"));
            };
            let decoded = decode_row(schema, &bytes)?;
            let infomask = decoded.infomask;
            let t_ctid = decoded.t_ctid;
            versions.push((
                RowLocation {
                    file_id,
                    page_num,
                    slot_num: current_slot,
                },
                decoded,
            ));

            // Follow only a same-page HEAP_ONLY successor of a HOT_UPDATED tuple — the
            // bounded HOT-chain segment.
            if infomask & crate::codec::HOT_UPDATED == 0 {
                return Ok(versions);
            }
            let (succ_page, succ_slot) = t_ctid;
            if succ_page != page_num {
                return Ok(versions);
            }
            match page::slot_state(data, succ_slot)? {
                page::LinePointer::Normal => {}
                _ => {
                    return Err(storage_internal(
                        "HOT_UPDATED successor slot is not a NORMAL tuple",
                    ));
                }
            }
            let Some(succ_bytes) = page::read_row(data, succ_slot)? else {
                return Err(storage_internal(
                    "HOT_UPDATED successor is not a live tuple",
                ));
            };
            let (_xmin, _xmax, _t_ctid, succ_infomask) =
                crate::codec::decode_mvcc_header(&succ_bytes)?;
            if succ_infomask & crate::codec::HEAP_ONLY == 0 {
                // Independently indexed successor: stop (it is its own root).
                return Ok(versions);
            }
            current_slot = succ_slot;
        }
    }

    /// Resolve a (possibly HOT) index entry to its visible heap version and read it,
    /// returning the **resolved heap location** alongside the row so callers stamp
    /// the right `RowId` (the live chain member, not the pruned root). Routes through
    /// [`Self::resolve_visible_in_chain`] (REDIRECT + bounded `t_ctid` walk +
    /// [`is_visible`], `docs/specs/mvcc.md` §6, §10 Milestone H1): an invisible chain
    /// (or a reclaimed root slot) yields `None` and is skipped by the caller — never
    /// an error. Under the degenerate autocommit snapshot every committed row and own
    /// write is visible, so this filters nothing; with no HOT tuples in the heap yet
    /// (H2/H3 unimplemented), the resolution is the prior single-tuple check at the
    /// index TID itself.
    fn read_visible_row(
        &self,
        schema: &TableSchema,
        location: RowLocation,
        snapshot: &Snapshot,
        current_txn: u64,
    ) -> Result<Option<(RowLocation, Row)>> {
        let Some((resolved, _infomask)) =
            self.resolve_visible_in_chain(schema, location, snapshot, current_txn)?
        else {
            return Ok(None);
        };
        // The resolved slot is the NORMAL, visible chain member; read its bytes.
        let Some(row) = self.read_location(schema, resolved)? else {
            return Ok(None);
        };
        Ok(Some((resolved, row)))
    }

    /// Locate the single version of `key` visible to `snapshot` from `current_txn`
    /// and return its heap location together with the version's current `infomask`
    /// (`docs/specs/mvcc.md` §6). The primary-key index may carry an entry per
    /// version (B4); each candidate TID is decoded at its *physical* header and the
    /// visibility predicate ([`is_visible`]) settles which one this snapshot sees.
    /// Under snapshot isolation at most one version of a key is visible, so the
    /// first visible candidate is the row the executor matched. Returns `None` when
    /// no version is visible (already deleted, aborted, or never present) — the
    /// caller treats that as "no row" (a no-op delete). A DEAD/UNUSED line pointer
    /// (`read_row` ⇒ `None`) is a reclaimed slot and is skipped.
    fn locate_visible_version(
        &self,
        schema: &TableSchema,
        index_btree: &BTree<'_, RowLocation>,
        key: &Key,
        snapshot: &Snapshot,
        current_txn: u64,
    ) -> Result<Option<(RowLocation, u16)>> {
        for location in index_btree.scan_key(key)? {
            // Each index entry's TID is a (possibly HOT) root: resolve REDIRECT +
            // the bounded `t_ctid` chain to the version this snapshot sees. Returns
            // the heap location of the visible chain member (which UPDATE/DELETE then
            // stamp), not the index TID — so a HOT-updated row is stamped at the live
            // heap-only version, not its pruned root.
            if let Some(resolved) =
                self.resolve_visible_in_chain(schema, location, snapshot, current_txn)?
            {
                return Ok(Some(resolved));
            }
        }
        Ok(None)
    }

    /// Whether any existing version indexed under `key` in `index_btree` **conflicts**
    /// with a unique-constraint insert by `current_txn` — the shared,
    /// visibility-aware uniqueness check for the primary-key index and unique
    /// secondary indexes (`docs/specs/mvcc.md` §6/§7.3). It replaces the temporary
    /// presence-probes (B2 commits 3–4): "any entry for the key" became "the
    /// strongest [`UniqueConflict`] across the *alive-or-potentially-alive* versions
    /// for the key".
    ///
    /// This is a **liveness ("dirty") check, not a snapshot read**: it consults the
    /// CLOG (`TxnStatusView`) + the tuple's `infomask` hint bits — never a
    /// [`Snapshot`] — so it sees concurrently in-flight and already-committed state,
    /// not just what `current_txn`'s snapshot would observe. Each candidate TID from
    /// `scan_key` is read at the *physical* tuple header (NOT via
    /// [`Self::read_visible_row`], which would wrongly hide non-visible-but-alive
    /// versions); a DEAD/UNUSED line pointer (`read_row` ⇒ `None`) is a reclaimed
    /// slot and contributes no conflict. The per-candidate decision is
    /// [`common::classify_unique_conflict`]: a creator-aborted or committed-deleted
    /// (incl. deleted-by-me) version is [`UniqueConflict::None`] and ignored; a
    /// committed/own/frozen-live version is a definite [`UniqueConflict::Violation`]
    /// (`23505`); a version created by another still-running txn is
    /// [`UniqueConflict::InFlight`] (`40001`, "retry").
    ///
    /// **Precedence `Violation > InFlight > None`** (returns the strongest across
    /// candidates): a single committed-live duplicate is a definite `23505` even if
    /// another candidate is only in-flight; only when no candidate is a definite
    /// duplicate but at least one is in-flight do we return `InFlight`.
    ///
    /// While writers are serialized (Stage 1) no concurrent uncommitted inserter
    /// exists, so this never returns `InFlight` at runtime and every index entry is a
    /// committed, non-deleted tuple — it returns `Violation` exactly when the old
    /// presence-probe / boolean check did, so existing uniqueness behavior is
    /// unchanged. The `InFlight` arm becomes load-bearing once writers run
    /// concurrently (Milestone E2b).
    fn unique_conflict_kind(
        &self,
        index_btree: &BTree<'_, RowLocation>,
        key: &Key,
        schema: &TableSchema,
        current_txn: u64,
    ) -> Result<UniqueConflict> {
        let status = self.txn_status_view();
        let mut strongest = UniqueConflict::None;
        for location in index_btree.scan_key(key)? {
            let readable = self
                .buffer_pool
                .read_page(location.file_id, location.page_num)?;
            let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
                // DEAD/UNUSED line pointer: the slot was reclaimed; no conflict.
                continue;
            };
            let decoded = decode_row(schema, &bytes)?;
            match classify_unique_conflict(
                decoded.xmin,
                decoded.xmax,
                decoded.infomask,
                current_txn,
                status,
            ) {
                // A committed-live duplicate is definitive; nothing outranks it.
                UniqueConflict::Violation => return Ok(UniqueConflict::Violation),
                // An in-flight candidate is the strongest seen so far, but a later
                // candidate could still be a definite Violation, so keep scanning.
                UniqueConflict::InFlight => strongest = UniqueConflict::InFlight,
                UniqueConflict::None => {}
            }
        }
        Ok(strongest)
    }

    fn table_page_nums(&self, file_id: FileId) -> Result<Vec<PageNum>> {
        let mut pages: Vec<_> = self
            .buffer_pool
            .iter_pages()?
            .filter(|info| info.file_id == file_id && page::is_initialized(&info.data.0))
            .map(|info| info.page_num)
            .collect();
        pages.sort_unstable();
        Ok(pages)
    }

    /// The heap-prune VACUUM pass (`docs/specs/mvcc.md` §9, Milestone F2b): for every
    /// heap page of `schema`'s table, physically reclaim the tuples that are
    /// dead-to-everyone at `horizon` and return their TIDs. Reclaiming an aborted or
    /// committed-deleted version's space is what bounds heap bloat once the system has
    /// MVCC versions (`DELETE`/`UPDATE` only *tombstone* in milestones B–E).
    ///
    /// For each page, every `NORMAL` slot's tuple is classified with
    /// [`common::is_dead_to_all`] (its `xmin`/`xmax`/`infomask` from
    /// [`crate::codec::decode_mvcc_header`], settled against the live CLOG via
    /// [`Self::txn_status_view`]). Only dead-to-all slots are pruned: a live version
    /// (`xmax == INVALID_XID`), an in-flight deleter, and a committed delete at or above
    /// the horizon are all left `NORMAL` (the predicate's aborted-creator-any-age /
    /// committed-delete-below-horizon asymmetry — §9).
    ///
    /// **Abort-cleanup (F4c root-cause, `docs/specs/mvcc.md` §5.4 / §9 F4c).** A KEPT
    /// slot whose deleter is *definitively aborted* (`xmax != INVALID_XID` and the
    /// `XMAX_ABORTED` hint or `status(xmax) == Aborted`) is the surviving predecessor of
    /// an aborted UPDATE/DELETE — it stays live (the delete rolled back) and is NOT
    /// reclaimed, but its `xmax = T` is the only on-disk reference to the aborted `T` as a
    /// *deleter*. Its header is reset IN PLACE — `xmax → INVALID_XID`, `t_ctid → INVALID`,
    /// `HOT_UPDATED` + settled `XMAX_*` cleared (preserving `xmin`/`XMIN_*`/`HEAP_ONLY`) —
    /// so a full pass leaves no surviving reference to `T` (as deleter, mirroring the
    /// aborted-creator reclaim), licensing the F4c floor-advance for ALL aborted
    /// UPDATE/DELETE, not just inserts. VACUUM holds the exclusive guard, so `xmax`'s
    /// status is settled (never reset an in-progress xmax).
    ///
    /// A page that had any dead slot OR any abort-cleanup reset is rewritten — the resets
    /// applied FIRST, then [`page::prune_and_compact`] (dead slots → `DEAD`, survivors
    /// compacted, offsets/`free_start`/PageLSN/checksum rewritten) — and logged as a
    /// single **unconditional** `FullPageImage`: a prune+compact relocates survivors and
    /// is not expressible as a delta, so it is never gated on `take_needs_fpi` (mirrors
    /// `btree::log_full_page`); the in-place header resets fold into the same image. A
    /// page with neither is skipped entirely — no WAL record, no mutation. Survivors are
    /// byte-identical at their stable slot ids (`prune_and_compact`'s contract), and the
    /// resets keep the tuple at its slot id and length, so no index entry is touched (the
    /// line pointer stays addressable; `DEAD → UNUSED` reclaim and index vacuum are F3,
    /// not done here).
    ///
    /// **Full-extent scan.** Iterates `0..page_count` of the heap file via
    /// [`BufferPool::page_count`], faulting each page in (resident or from disk), rather
    /// than only the resident pages [`Self::table_page_nums`] reports — an evicted page
    /// holding dead tuples must still be vacuumed, else GC is incomplete.
    ///
    /// **Latching (lock order: structural → frame → WAL).** Per page, takes the
    /// per-heap structural latch then the frame write latch, releasing both before the
    /// next page (never held across pages). VACUUM runs under the exclusive
    /// concurrency guard today (no concurrent writers, §10 Milestone F), so these
    /// uncontended latches are forward-looking: a future concurrent VACUUM is then a
    /// guard change, not a rewrite of this method.
    ///
    /// **`vacuum_txn` = 0 (the recovery/maintenance convention).** Pages are dirtied
    /// and logged under txn id `0`, the same id recovery uses for non-transactional
    /// page work (`fetch_for_redo`; `apply_drop_table_without_wal`'s "txn 0 means no
    /// rollback tracking"). VACUUM is maintenance, not a user transaction: its
    /// reclamation must never be undone by an abort and must not depend on a user
    /// commit. A `FullPageImage` is unconditional torn-page repair — recovery's redo
    /// arm reinstalls it purely by PageLSN gating (`page_lsn(data) >= lsn` skips it,
    /// else `copy_from_slice` + force the record LSN), independent of the record's
    /// `txn_id` — so a crash mid-VACUUM leaves every pruned page either pre-prune or
    /// exactly the compacted image, never torn.
    pub(crate) fn vacuum_heap(
        &self,
        schema: &TableSchema,
        horizon: u64,
    ) -> Result<Vec<RowLocation>> {
        // A table's heap file id is its table id (no high bit; see `heap::index_file_id`).
        let file_id = schema.id;
        let page_count = self.buffer_pool.page_count(file_id)?;
        let latch = self.structural_latch(file_id);

        let mut reclaimed: Vec<RowLocation> = Vec::new();
        for page_num in 0..page_count {
            // Lock order: structural latch → frame write latch → (WAL mutex inside the
            // append). Both are released at the end of each iteration so no latch is
            // held across pages (rule 1: never two structural latches; forward-looking
            // for a concurrent VACUUM).
            let _heap_guard = latch.lock();
            let mut guard = self.buffer_pool.write_page(file_id, page_num, VACUUM_TXN)?;

            // An uninitialized frame (e.g. a never-written page in the extent) carries
            // no tuples to classify.
            if !page::is_initialized(guard.data()) {
                continue;
            }

            // Classify every NORMAL slot. `page::read_row` returns `Some` only for a
            // NORMAL line pointer (a DEAD/UNUSED slot reads as `None`), so the slot ids
            // it yields are exactly the live candidates; `next_slot` is the slot count.
            // A slot is either RECLAIMED (`dead_slots`, pruned to DEAD) or KEPT; a kept
            // slot whose deleter definitively ABORTED is additionally collected into
            // `reset_slots` for in-place abort-cleanup (F4c root-cause, below).
            let slot_count = page::next_slot(guard.data())?;
            let mut dead_slots: Vec<u16> = Vec::new();
            let mut reset_slots: Vec<u16> = Vec::new();
            for slot in 0..slot_count {
                let Some(tuple) = page::read_row(guard.data(), slot)? else {
                    continue;
                };
                let (xmin, xmax, _t_ctid, infomask) = crate::codec::decode_mvcc_header(&tuple)?;
                if common::is_dead_to_all(xmin, xmax, infomask, horizon, self.txn_status_view()) {
                    // The tuple is dead-to-all. HOT-chain safety (H2/H3,
                    // `docs/specs/mvcc.md` §10 Milestone H2/H3): a HOT-chain member
                    // (`HEAP_ONLY` successor or `HOT_UPDATED` root) is reclaimable ONLY
                    // when its creator aborted. An aborted-creator HOT tuple is a
                    // dead-end orphan — an aborted UPDATE never sits in the MIDDLE of a
                    // live chain (its xmin is the chain's youngest id and no later version
                    // was committed onto it), so reclaiming it cannot sever a still-live
                    // successor; leaving it would both leak space and (per F4c) keep a
                    // surviving on-disk reference to an aborted txn. A HOT tuple
                    // dead-to-all via a COMMITTED delete is the genuine "could be a live
                    // committed chain's middle" case — reclaiming it would sever the
                    // `t_ctid` walk to a still-live successor — so it is deferred to H3's
                    // chain-aware pruning (redirect the root, splice the chain). Non-HOT
                    // tuples are reclaimed unconditionally on dead-to-all.
                    let is_hot =
                        infomask & (crate::codec::HEAP_ONLY | crate::codec::HOT_UPDATED) != 0;
                    let creator_aborted = infomask & common::XMIN_ABORTED != 0
                        || self.txn_status_view().is_aborted(xmin);
                    if is_hot && !creator_aborted {
                        continue;
                    }
                    dead_slots.push(slot);
                    continue;
                }

                // KEPT slot. **Abort-cleanup (F4c root-cause, `docs/specs/mvcc.md` §5.4 /
                // §9 F4c).** An aborted UPDATE/DELETE stamps `xmax = T` (and, for a HOT
                // root, `HOT_UPDATED` + `t_ctid`) on its surviving predecessor, which
                // stays live because the delete/update rolled back. VACUUM does not
                // reclaim that live row and nothing else resets the stamp, so once the
                // vacuum floor floats past `T` and its `Abort` is truncated, recovery
                // rebuilds the CLOG and reads `xmax = T` as implicitly Committed —
                // wrongly DELETING the row after a crash. Reset the stamp in place so a
                // full pass leaves NO surviving on-disk reference to an aborted txn (as
                // deleter, mirroring the aborted-creator reclaim above): clear `xmax` to
                // INVALID, drop the dangling `t_ctid`, and un-HOT an aborted root. Only on
                // a DEFINITIVE abort — VACUUM holds the exclusive guard so no writer is in
                // flight and `xmax`'s status is settled; never reset an in-progress xmax.
                let deleter_aborted = xmax != common::INVALID_XID
                    && (infomask & common::XMAX_ABORTED != 0
                        || self.txn_status_view().is_aborted(xmax));
                if deleter_aborted {
                    reset_slots.push(slot);
                }
            }

            if dead_slots.is_empty() && reset_slots.is_empty() {
                continue;
            }

            // Apply the in-place abort-cleanup header resets FIRST (before the compaction
            // relocates survivors), then prune+compact the dead slots, then log the whole
            // result as a SINGLE unconditional FullPageImage. A header reset clears
            // `xmax → INVALID`, `t_ctid → INVALID`, and the `HOT_UPDATED` / settled-`XMAX_*`
            // hint bits (giving the tuple the exact live, never-deleted header shape),
            // preserving every other bit (e.g. `XMIN_COMMITTED`, `HEAP_ONLY`). The reset
            // keeps the tuple at its stable slot id and does not change its length, so no
            // index entry is touched.
            let provisional_lsn = page::page_lsn(guard.data());
            for &slot in &reset_slots {
                let cleared_bits =
                    crate::codec::HOT_UPDATED | common::XMAX_ABORTED | common::XMAX_COMMITTED;
                let tuple = page::read_row(guard.data(), slot)?
                    .ok_or_else(|| storage_internal("abort-cleanup slot is not live"))?;
                let (_xmin, _xmax, _t_ctid, infomask) = crate::codec::decode_mvcc_header(&tuple)?;
                page::set_tuple_header(
                    guard.data_mut(),
                    slot,
                    common::INVALID_XID,
                    crate::codec::INVALID_TID,
                    infomask & !cleared_bits,
                    provisional_lsn,
                )?;
            }

            // Prune + compact, then log the compacted page as a single unconditional
            // FullPageImage (a compaction relocates survivors and is not a delta; the
            // header resets above further mutate it in place), and stamp the FPI's LSN as
            // the new page-LSN — the `btree::log_full_page` pattern. `prune_and_compact`
            // restamps the provisional LSN; the FPI append below overwrites it with the
            // record's LSN so redo gating is exact. Recovery's redo arm reinstalls this
            // image purely by PageLSN gating, independent of txn id, so a crash mid-VACUUM
            // leaves the page either pre-pass or exactly this image — never torn — and the
            // abort-cleanup is durable before any later `truncate_before` consults the
            // floor (a checkpoint flushes+fsyncs every dirty page before that).
            page::prune_and_compact(guard.data_mut(), &dead_slots, provisional_lsn)?;
            let fpi_lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id: VACUUM_TXN,
                kind: WalRecordKind::FullPageImage {
                    file_id,
                    page_num,
                    image: guard.data().to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), fpi_lsn);

            for slot in dead_slots {
                reclaimed.push(RowLocation {
                    file_id,
                    page_num,
                    slot_num: slot,
                });
            }
        }

        Ok(reclaimed)
    }

    /// Index VACUUM (`docs/specs/mvcc.md` §9, Milestone F3a): remove every index
    /// entry — across the table's primary-key index and every live secondary index —
    /// whose value (the heap `RowLocation`/TID) is in `dead_tids`. `dead_tids` are the
    /// TIDs `vacuum_heap` pruned to `DEAD`; their index entries still dangle (pointing
    /// at a now-DEAD slot) and must be removed before the line pointers can be
    /// reclaimed `DEAD → UNUSED` (F3b).
    ///
    /// Entries are matched by **dead-TID membership, not by key**: after the heap
    /// prune compacted the page the dead tuple's key bytes are gone, so the key cannot
    /// be recomputed; the index leaf's stored value (the TID) is the only handle left.
    /// Each index is vacuumed in a single leaf-chain walk (`BTree::remove_values_in`),
    /// shifting matching entries out of each leaf under its frame write latch and
    /// logging a `FullPageImage` of every changed leaf — the `vacuum_heap` /
    /// `btree::log_full_page` crash-safety pattern, redone by PageLSN gating regardless
    /// of txn id. The pass runs under the maintenance txn id (`0`, [`VACUUM_TXN`]) so
    /// its removals are never undone by an abort and do not pin WAL truncation.
    ///
    /// **Latching.** Each index is vacuumed under *its own* per-index structural latch,
    /// acquired and released around that index's whole walk and never held while
    /// another index's latch is taken (rule 1: never two structural latches at once).
    /// The per-leaf write latch a removal takes inside `remove_values_in` is mutually
    /// exclusive with a concurrent lock-free scanner's per-leaf read latch on the same
    /// leaf, and no leaf is merged/freed and no right-sibling link is rewritten, so a
    /// concurrent scanner can neither miss nor duplicate a live entry (B-link safe).
    ///
    /// Called by [`vacuum`](Self::vacuum) as F4a's middle phase (F2b → **F3a** →
    /// F3b). It does **not** reclaim line pointers `DEAD → UNUSED` (F3b); the slots
    /// stay `DEAD` until that later step.
    pub(crate) fn vacuum_indexes(
        &self,
        schema: &TableSchema,
        dead_tids: &HashSet<RowLocation>,
    ) -> Result<()> {
        if dead_tids.is_empty() {
            return Ok(());
        }

        // Primary-key index, under its own structural latch (released before the next).
        let pk_file_id = index_file_id(schema.id);
        {
            let latch = self.structural_latch(pk_file_id);
            let _pk_guard = latch.lock();
            self.btree(pk_file_id)
                .remove_values_in(VACUUM_TXN, dead_tids)?;
        }

        // Every live secondary index, each under its own structural latch (one at a
        // time — rule 1: never two structural latches simultaneously).
        for index in self.table_indexes(schema.id)? {
            let secondary_file_id = secondary_index_file_id(index.id);
            let latch = self.structural_latch(secondary_file_id);
            let _index_guard = latch.lock();
            self.secondary_btree(index.id)
                .remove_values_in(VACUUM_TXN, dead_tids)?;
        }

        Ok(())
    }

    /// Line-pointer reclaim, the third VACUUM phase (`docs/specs/mvcc.md` §9,
    /// Milestone F3b): flip each `dead_tid`'s heap line pointer `DEAD → UNUSED`,
    /// freeing its slot id for reuse by a future `insert_row`. `dead_tids` are the
    /// TIDs `vacuum_heap` (F2b) pruned to `DEAD` and `vacuum_indexes` (F3a) has since
    /// stripped of every index entry; reclaiming them to `UNUSED` is what bounds the
    /// slot array under delete→vacuum→insert churn (a `DEAD` line pointer is dead
    /// weight `insert_row` will not recycle).
    ///
    /// **Ordering invariant — F2b → F3a → F3b.** This MUST run only after
    /// `vacuum_indexes` removed every index entry for these TIDs. The invariant is
    /// the safety hinge for slot reuse: `insert_row` recycles an `UNUSED` slot id,
    /// so an `UNUSED` slot must have *no* dangling index entry, or a stale entry
    /// would resolve to the new tuple written into the reclaimed slot (silent
    /// corruption). [`vacuum`](Self::vacuum) (F4a) enforces the F2b → F3a → F3b order
    /// by calling these three phases in sequence on one set of dead TIDs.
    /// `page::reclaim_line_pointers` debug-asserts each slot is currently `DEAD` (a
    /// `NORMAL`/`UNUSED`/out-of-bounds slot is a hard error), which catches the gross
    /// misordering of reclaiming a never-pruned slot, though it cannot by itself
    /// prove the *index* entries are gone — that is F4a's ordering responsibility.
    ///
    /// **Per page, lock order structural → frame → WAL.** TIDs are grouped by heap
    /// page; each page is reclaimed under the per-heap structural latch then the
    /// frame write latch (released before the next page, never held across pages —
    /// rule 1), and logged as a single unconditional `FullPageImage` under the
    /// maintenance txn id (`0`, [`VACUUM_TXN`]), the same crash-safety pattern as
    /// `vacuum_heap`/`vacuum_indexes`: recovery reinstalls the reclaimed page purely
    /// by PageLSN gating, independent of the record's `txn_id`. A reclaim
    /// (slot → `UNUSED`) followed by a later insert-into-reused-slot (`HeapInsert`)
    /// replay in LSN order to the final state (the new row at that slot), so a crash
    /// mid-reclaim leaves the page either pre-reclaim or exactly the reclaimed image,
    /// never torn.
    ///
    /// Called by [`vacuum`](Self::vacuum) as F4a's final phase (F2b → F3a → **F3b**).
    pub(crate) fn reclaim_line_pointers(
        &self,
        schema: &TableSchema,
        dead_tids: &HashSet<RowLocation>,
    ) -> Result<()> {
        if dead_tids.is_empty() {
            return Ok(());
        }

        // A table's heap file id is its table id (no high bit; see `heap::index_file_id`).
        let file_id = schema.id;
        let latch = self.structural_latch(file_id);

        // Group the dead slots by heap page so each page is rewritten once. A TID
        // from another file (an index TID) is a caller bug — these are heap TIDs that
        // `vacuum_heap` returned for this table's heap file.
        let mut by_page: BTreeMap<PageNum, Vec<u16>> = BTreeMap::new();
        for tid in dead_tids {
            debug_assert_eq!(
                tid.file_id, file_id,
                "reclaim_line_pointers expects heap TIDs for this table's heap file",
            );
            if tid.file_id == file_id {
                by_page.entry(tid.page_num).or_default().push(tid.slot_num);
            }
        }

        for (page_num, slots) in by_page {
            // Lock order: structural latch → frame write latch → (WAL mutex inside the
            // append). Both released at the end of each iteration so no latch is held
            // across pages (rule 1; forward-looking for a concurrent VACUUM).
            let _heap_guard = latch.lock();
            let mut guard = self.buffer_pool.write_page(file_id, page_num, VACUUM_TXN)?;

            // Flip DEAD → UNUSED, then log the reclaimed page as a single unconditional
            // FullPageImage and stamp the FPI's LSN as the new page-LSN (the
            // `vacuum_heap` / `btree::log_full_page` pattern). `reclaim_line_pointers`
            // stamps a provisional LSN; the FPI append overwrites it with the record's
            // LSN so redo gating is exact.
            let provisional_lsn = page::page_lsn(guard.data());
            page::reclaim_line_pointers(guard.data_mut(), &slots, provisional_lsn)?;
            let fpi_lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id: VACUUM_TXN,
                kind: WalRecordKind::FullPageImage {
                    file_id,
                    page_num,
                    image: guard.data().to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), fpi_lsn);
        }

        Ok(())
    }

    /// VACUUM one table (`docs/specs/mvcc.md` §9, §10 Milestone F4a): the live
    /// orchestration that ties the three reclamation phases together in their
    /// mandatory order — heap-prune (F2b) → index-vacuum (F3a) → line-pointer
    /// reclaim (F3b) — and returns the number of heap tuples reclaimed (for the
    /// `VACUUM` command tag / observability). `horizon` is the GC horizon
    /// (`ServerComponents::gc_horizon`), the minimum `xmin` advertised by any live
    /// snapshot; a version with `xmax < horizon` is dead to every current and
    /// future snapshot ([`common::is_dead_to_all`]).
    ///
    /// **The order is the safety invariant (F3b's hinge).** `vacuum_heap` returns the
    /// TIDs it pruned to `DEAD`; `vacuum_indexes` must strip every index entry for
    /// those TIDs **before** `reclaim_line_pointers` flips them `DEAD → UNUSED`,
    /// because `insert_row` recycles an `UNUSED` slot — a dangling index entry over a
    /// reclaimed-then-reused slot would resolve to the wrong (new) tuple (silent
    /// corruption). Running the three calls in this fixed sequence on one dead-TID
    /// set is exactly what discharges that precondition. When the heap prune finds
    /// nothing dead, the index and line-pointer phases are skipped (an empty set is a
    /// documented no-op for both, but skipping avoids even the empty-set call).
    ///
    /// **Safety against data loss (the horizon-under-the-guard argument).** The caller
    /// runs this under the EXCLUSIVE checkpoint guard, so NO writer executes during
    /// the pass: no committed-deleter can appear mid-pass, and `horizon` is captured
    /// once (after acquiring the guard) as the min advertised `xmin` over all live
    /// snapshots — INCLUDING lock-free readers, which advertise their `xmin`. So every
    /// version this reclaims has `xmax < horizon`, meaning its delete committed before
    /// any still-live snapshot's `xmin`; no current snapshot can see it live, and any
    /// reader that starts mid-pass freezes `xmin >= horizon` (the deleter is in its
    /// settled past). VACUUM therefore never reclaims a version a snapshot needs.
    pub fn vacuum(&self, schema: &TableSchema, horizon: u64) -> Result<usize> {
        // Phase F2b — heap-prune dead-to-all tuples to DEAD, collecting their TIDs.
        let dead = self.vacuum_heap(schema, horizon)?;
        let reclaimed = dead.len();
        if !dead.is_empty() {
            let dead: HashSet<RowLocation> = dead.into_iter().collect();
            // Phase F3a — strip every PK + secondary index entry for those TIDs.
            self.vacuum_indexes(schema, &dead)?;
            // Phase F3b — reclaim the now entry-free line pointers DEAD → UNUSED.
            // MUST follow F3a (above): see this method's ordering invariant.
            self.reclaim_line_pointers(schema, &dead)?;
        }
        Ok(reclaimed)
    }

    /// Attempt the HOT-update fast path (`docs/specs/mvcc.md` §10 Milestone H2) for
    /// an `UPDATE` whose visible predecessor is at `previous_location` (`infomask` its
    /// current header hints). Returns:
    ///
    /// - `Ok(Some(true))` — the HOT update was performed (the caller returns it).
    /// - `Ok(None)` — NOT eligible; the caller falls back to the normal fully-indexed
    ///   update path.
    ///
    /// Eligible iff BOTH:
    /// 1. **No indexed column changed.** The new row's key equals the predecessor's
    ///    for the primary key (already enforced by the caller — a PK change is
    ///    rejected) AND for every secondary index ([`secondary_index_key`]). If all
    ///    index keys match, only non-indexed columns differ.
    /// 2. **Same-page room.** The new heap-only tuple, encoded, fits in the free space
    ///    of the predecessor's own page ([`Self::try_hot_insert_on_page`] returns
    ///    `Some`). Reusing an `UNUSED` slot or appending both count; if it does not
    ///    fit, fall back (H3 will prune-to-make-room — not done here).
    ///
    /// When eligible: write the heap-only successor on the predecessor's page, then
    /// stamp the predecessor `xmax = txn`, `t_ctid → new`, and `HOT_UPDATED` via
    /// [`Self::stamp_xmax_logged`] (which keeps the atomic first-updater-wins check —
    /// a concurrent claimer yields `40001`). NO index entries are inserted: the index
    /// still points at the chain root, and the H1 bounded walk reaches the new version.
    ///
    /// **Orphan-on-conflict safety.** The heap-only tuple is placed BEFORE the
    /// stamp-with-conflict-check, mirroring the non-HOT path: on a `40001` the
    /// just-written heap-only tuple is left unreferenced (no predecessor `t_ctid`
    /// points at it, and it has no index entry), so its aborting `xmin` makes it
    /// invisible via CLOG ⇒ dead-to-all ⇒ reclaimable by VACUUM — harmless, exactly
    /// like the non-HOT orphan.
    fn try_hot_update(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        table: TableId,
        previous_location: RowLocation,
        infomask: u16,
        row: &Row,
    ) -> Result<Option<bool>> {
        // Eligibility (1): no indexed column changed. Read the predecessor's CURRENT
        // physical row (not a snapshot read — we need its actual indexed values) and
        // compare every secondary index's key against the new row's. The primary key
        // is already known unchanged (the caller rejects a PK change). A missing
        // predecessor here means it was reclaimed under us — not eligible.
        let Some(previous_row) = self.read_location(schema, previous_location)? else {
            return Ok(None);
        };
        for index in self.table_indexes(table)? {
            let (old_key, _) = secondary_index_key(schema, &index, &previous_row)?;
            let (new_key, _) = secondary_index_key(schema, &index, row)?;
            if old_key != new_key {
                // An indexed column changed ⇒ the new version needs its own index
                // entry ⇒ not a HOT update; fall back.
                return Ok(None);
            }
        }

        // Eligibility (2): the new heap-only tuple fits on the predecessor's page.
        // `try_hot_insert_on_page` returns `None` (no room) ⇒ fall back (H3 pruning is
        // out of scope for H2).
        let Some(new_location) =
            self.try_hot_insert_on_page(schema, previous_location.page_num, row, ctx.txn_id)?
        else {
            return Ok(None);
        };

        // Stamp the predecessor: xmax = txn, t_ctid → the new heap-only tuple, and
        // HOT_UPDATED set (preserving its other infomask hints). This keeps the atomic
        // first-updater-wins check; on a `40001` the heap-only tuple written above is a
        // harmless orphan (see this method's doc). The new tuple is on the SAME page as
        // the predecessor by construction, so the H1 walk's same-page `HOT_UPDATED →
        // HEAP_ONLY` step reaches it.
        let new_tid = (new_location.page_num, new_location.slot_num);
        self.stamp_xmax_logged(
            previous_location,
            new_tid,
            infomask | crate::codec::HOT_UPDATED,
            ctx.txn_id,
        )?;

        // No index entries: the index keeps pointing at the chain root; the new
        // heap-only version is reached only by the bounded `t_ctid` walk from it. This
        // is the whole point of HOT — the un-indexed in-place version.
        Ok(Some(true))
    }
}

impl StorageEngine for PageBackedStorageEngine {
    fn insert(&self, ctx: &StatementContext, table: TableId, row: Row) -> Result<RowId> {
        let (schema, index_fid) = self.table_handle(table)?;
        let key = key_for_row(&schema, &row)?;
        let btree = self.btree(index_fid);

        // Write the new heap tuple first (under its own per-heap latch inside
        // `write_new_row`, released on return), THEN do the primary-key uniqueness
        // check + index insert atomically under the PK index latch. Writing the heap
        // row before taking the PK latch keeps the two structural latches disjoint
        // (rule 1: never two at once). A transiently orphaned heap tuple (if the PK
        // check below fails) is invisible via CLOG once the txn aborts and reclaimed
        // by VACUUM — the same orphan-on-conflict handling `update` relies on.
        let location = self.write_new_row(&schema, &row, ctx.txn_id)?;

        // Visibility-aware primary-key uniqueness AND the index insert under ONE hold
        // of the PK index structural latch (Milestone E2a, the critical atomic
        // check-and-insert): the multi-entry tree no longer rejects duplicate keys
        // structurally, so reject only when an alive-or-potentially-alive version
        // already holds the key (dead/aborted versions do not block a re-insert). A
        // committed-live duplicate is a definite `UniqueViolation`; a key held only by
        // another in-progress inserter is undecidable ⇒ `SerializationFailure` (retry
        // — §7.3). Holding the latch across BOTH the scan and the insert (incl. any
        // leaf/parent/root split + `set_root`) is what stops two concurrent inserts of
        // the same key from both passing the check and both inserting. As of E2b
        // (concurrent writers) this is load-bearing: the loser of a same-key race sees
        // the winner's entry and gets `UniqueViolation` (committed) or
        // `SerializationFailure` (in-flight), never a silent double-insert.
        {
            let latch = self.structural_latch(index_fid);
            let _pk_guard = latch.lock();
            match self.unique_conflict_kind(&btree, &key, &schema, ctx.txn_id)? {
                UniqueConflict::Violation => return Err(duplicate_primary_key()),
                UniqueConflict::InFlight => return Err(unique_conflict_retry()),
                UniqueConflict::None => {}
            }
            btree.insert(ctx.txn_id, &key, &location)?;
        }

        for index in self.table_indexes(table)? {
            let (entry_key, has_null) = secondary_index_key(&schema, &index, &row)?;
            self.insert_secondary_entry(ctx, &schema, &index, &entry_key, has_null, &location)?;
        }

        Ok(RowId {
            page_num: location.page_num,
            slot_num: location.slot_num,
        })
    }

    fn get(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<Option<Row>> {
        let (schema, index_fid) = self.table_handle(table)?;
        // The primary-key index may carry entries for several versions of this key
        // once versioning lands (B4); collect every candidate TID and return the
        // single one visible to this snapshot. Today there is one entry per key.
        for location in self.btree(index_fid).scan_key(key)? {
            if let Some((_resolved, row)) =
                self.read_visible_row(&schema, location, &ctx.snapshot, ctx.txn_id)?
            {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn delete(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<bool> {
        let Some((schema, index_fid)) = self.table_handle_opt(table)? else {
            return Ok(false);
        };
        let btree = self.btree(index_fid);
        // Locate the single version this statement's snapshot sees (the row the
        // executor matched). If none is visible the key was already deleted or is
        // absent, so the delete affects no row — preserve the no-op semantics.
        let Some((location, infomask)) =
            self.locate_visible_version(&schema, &btree, key, &ctx.snapshot, ctx.txn_id)?
        else {
            return Ok(false);
        };

        // MVCC delete: stamp xmax on the still-NORMAL line pointer in place. The
        // tuple and *all* its index entries (PK and secondary) are retained — the
        // row is hidden by visibility (xmax committed ⇒ invisible to later
        // snapshots), and VACUUM (Milestone F) reclaims the dead version and its
        // entries. No tombstone, no index-entry removal.
        self.stamp_xmax_logged(location, crate::codec::INVALID_TID, infomask, ctx.txn_id)?;
        Ok(true)
    }

    fn update(&self, ctx: &StatementContext, table: TableId, key: &Key, row: Row) -> Result<bool> {
        let (schema, index_fid) = self.table_handle(table)?;
        let btree = self.btree(index_fid);
        // Locate the version this statement's snapshot sees (the row the executor
        // matched), NOT an arbitrary `search(key)` entry. The primary-key index may
        // carry an entry per version once versioning lands (and after a
        // delete-then-reinsert there are several entries for the key), so targeting
        // the *visible* version is what makes the right row the one updated. If none
        // is visible the key was already deleted or is absent, so the update affects
        // no row — preserve the no-op semantics.
        let Some((previous_location, infomask)) =
            self.locate_visible_version(&schema, &btree, key, &ctx.snapshot, ctx.txn_id)?
        else {
            return Ok(false);
        };
        let replacement_key = key_for_row(&schema, &row)?;
        if &replacement_key != key {
            return Err(DbError::execute(
                SqlState::DatatypeMismatch,
                "primary key updates are not supported",
            ));
        }

        // HOT-update fast path (`docs/specs/mvcc.md` §10 Milestone H2). When BOTH (a)
        // no indexed column changed and (b) the new tuple fits on the predecessor's
        // own page, write the new version as a heap-only tuple on that page, chain the
        // predecessor to it, and insert NO index entries — the index keeps pointing at
        // the chain root, and H1's bounded `t_ctid` walk reaches the new version via
        // the `HOT_UPDATED → HEAP_ONLY` segment. Falls through to the normal
        // fully-indexed path when ineligible. Pruning-to-make-room on a full page is
        // H3; here a full page simply falls back.
        if let Some(result) =
            self.try_hot_update(ctx, &schema, table, previous_location, infomask, &row)?
        {
            return Ok(result);
        }

        // MVCC UPDATE (Postgres-style, non-HOT): write the new tuple as a fresh heap
        // version (`xmin = txn`, `xmax = invalid`, `t_ctid = self`), then chain the
        // old version forward to it and insert per-version index entries for the new
        // version into *every* index. The old version and all old index entries are
        // retained; VACUUM (Milestone F) reclaims them. Reads do not walk `t_ctid`
        // (every version is independently indexed), so the new version needs its own
        // entry in *all* indexes — including indexes whose columns did not change —
        // or a scan on an unchanged secondary value would never find it (the
        // changed-index-only skip is a HOT optimization, Milestone H; applying it
        // here would be a correctness bug — `docs/specs/mvcc.md` Appendix A commit 9).
        let new_location = self.write_new_row(&schema, &row, ctx.txn_id)?;

        // Stamp the old version *before* the new version's uniqueness checks, so its
        // `xmax = ctx.txn_id` makes `unique_conflict_kind` treat it as own-deleted
        // (non-conflicting): the new version must not collide with the logical row it
        // supersedes, but must still collide with any *other* live row. The forward
        // `t_ctid` points at the new version (invariant 5).
        //
        // The atomic first-updater-wins check lives in `stamp_xmax_logged` (E1b,
        // §7.3): if another writer already claimed the old version's `xmax`, this
        // returns `40001` *after* the new version was written above (the index
        // inserts below have not run yet, so only the heap tuple is orphaned). That
        // transient new tuple is an **orphan-on-conflict** and needs no manual
        // cleanup: the `40001` error aborts the transaction, so the new version
        // (xmin = the aborting txn) becomes invisible via CLOG = Aborted and is
        // reclaimed by VACUUM (Milestone F) — the abort + visibility machinery
        // handles it. (A pre-write conflict check to avoid the transient orphan is a
        // deferred optimization; the authoritative check stays atomic at stamp time
        // to keep first-updater-wins race-free.)
        let new_tid = (new_location.page_num, new_location.slot_num);
        self.stamp_xmax_logged(previous_location, new_tid, infomask, ctx.txn_id)?;

        // Primary-key entry for the new version, under ONE hold of the PK index
        // structural latch across the uniqueness check AND the insert (Milestone E2a,
        // atomic check-and-insert). The key is unchanged (a PK change is rejected
        // above), so this adds a second `(key, new_tid)` entry alongside the retained
        // old one. The uniqueness check now sees the old version as own-deleted
        // (`xmax == ctx.txn_id` ⇒ `UniqueConflict::None`), so the unchanged PK does not
        // falsely self-conflict; a collision with a *different* committed-live row is a
        // `UniqueViolation`, and one with another in-progress inserter is a
        // `SerializationFailure` (retry — §7.3). The latch is taken AFTER the
        // `stamp_xmax_logged` above (which holds only a frame latch, no structural
        // latch) and wraps the whole `insert` incl. any split/root-split; it is
        // released before the secondary inserts each take their own latch (rule 1).
        {
            let latch = self.structural_latch(index_fid);
            let _pk_guard = latch.lock();
            match self.unique_conflict_kind(&btree, key, &schema, ctx.txn_id)? {
                UniqueConflict::Violation => return Err(duplicate_primary_key()),
                UniqueConflict::InFlight => return Err(unique_conflict_retry()),
                UniqueConflict::None => {}
            }
            btree.insert(ctx.txn_id, key, &new_location)?;
        }

        // A new per-version entry for the new tuple in *every* secondary index
        // (changed-column or not), pointing at `new_location`. Old entries are
        // retained. `insert_secondary_entry` enforces unique-secondary constraints
        // visibility-aware: an unchanged unique value does not self-conflict (the old
        // version is own-deleted), but a value colliding with a different live row
        // raises `UniqueViolation`.
        for index in self.table_indexes(table)? {
            let (new_key, has_null) = secondary_index_key(&schema, &index, &row)?;
            self.insert_secondary_entry(ctx, &schema, &index, &new_key, has_null, &new_location)?;
        }

        Ok(true)
    }

    fn scan(&self, ctx: &StatementContext, table: TableId) -> Result<Box<dyn RowIterator>> {
        self.scan_range(ctx, table, &KeyRange::All)
    }

    fn scan_range(
        &self,
        ctx: &StatementContext,
        table: TableId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let (schema, index_fid) = self.table_handle(table)?;
        let entries = self.btree(index_fid).range(range)?;

        let mut rows = Vec::with_capacity(entries.len());
        for (key, location) in entries {
            // Resolve the index entry's TID (a possibly-HOT root: REDIRECT + bounded
            // `t_ctid` chain) to the version this snapshot sees; an invisible chain
            // (or a reclaimed root slot) is skipped, not returned or errored. A
            // HEAP_ONLY successor has NO index entry of its own, so it is never
            // yielded directly here — only via its root's chain — which is exactly
            // why the bounded walk's stop-at-indexed-successor rule prevents a row
            // from being returned twice (`mvcc.md` §10 Milestone H1). The yielded
            // `RowId` is the resolved live version, not the index TID.
            let Some((resolved, row)) =
                self.read_visible_row(&schema, location, &ctx.snapshot, ctx.txn_id)?
            else {
                continue;
            };
            rows.push(StoredRow {
                row_id: RowId {
                    page_num: resolved.page_num,
                    slot_num: resolved.slot_num,
                },
                key,
                row,
            });
        }

        Ok(Box::new(PageRowIterator {
            schema: column_info(&schema),
            rows,
            index: 0,
        }))
    }

    fn index_scan(
        &self,
        ctx: &StatementContext,
        table: TableId,
        index: IndexId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let (schema, _pk_file_id) = self.table_handle(table)?;
        self.ensure_index_live(table, index)?;

        // The secondary index points directly at heap TIDs (uniform with the
        // primary-key index), so a scan collects candidate TIDs from the index and
        // resolves each at the heap. Each TID is a (possibly HOT) root: a non-HOT
        // version is independently indexed and resolves to itself; a HEAP_ONLY
        // successor has no index entry and is reached only via its root's bounded
        // `t_ctid` walk (REDIRECT + chain in `read_visible_row`; `mvcc.md` §5.2, §10
        // Milestone H1). Because the walk stops at any independently-indexed
        // successor, a row is never yielded via two index entries.
        let entries = self.secondary_btree(index).range(range)?;
        let mut rows = Vec::with_capacity(entries.len());
        for (_entry_key, location) in entries {
            // Resolve to the visible version; an invisible chain (or a DEAD/absent
            // root line pointer) is skipped, not an error.
            let Some((resolved, row)) =
                self.read_visible_row(&schema, location, &ctx.snapshot, ctx.txn_id)?
            else {
                continue;
            };
            // The row's primary key is recovered from the heap row, preserving the
            // `StoredRow.key` semantics callers relied on under secondary→PK. The
            // `RowId` is the resolved live version, not the index TID.
            let key = key_for_row(&schema, &row)?;
            rows.push(StoredRow {
                row_id: RowId {
                    page_num: resolved.page_num,
                    slot_num: resolved.slot_num,
                },
                key,
                row,
            });
        }

        Ok(Box::new(PageRowIterator {
            schema: column_info(&schema),
            rows,
            index: 0,
        }))
    }

    fn rollback_txn(&self, txn_id: u64) -> Result<()> {
        // Abort is status-based (`docs/specs/mvcc.md` §4 Decision 3, Milestone D1):
        // index and heap PAGE changes are NOT undone — an aborted transaction's
        // versions stay in the heap, hidden by the CLOG and reclaimed by VACUUM.
        // This restores only the engine's own DDL metadata (table/index schema
        // shadow state), so a failed in-unit CREATE/DROP leaves no phantom catalog
        // entry.
        let mut state = self.lock_state()?;
        let Some(rollback) = state.rollback.remove(&txn_id) else {
            return Ok(());
        };
        for (table_id, previous) in rollback.tables.into_iter().rev() {
            match previous {
                Some(table) => {
                    state.tables.insert(table_id, table);
                }
                None => {
                    state.tables.remove(&table_id);
                }
            }
        }
        for (index_id, previous) in rollback.indexes.into_iter().rev() {
            match previous {
                Some(index) => {
                    state.indexes.insert(index_id, index);
                }
                None => {
                    state.indexes.remove(&index_id);
                }
            }
        }
        Ok(())
    }

    fn commit_txn(&self, txn_id: u64) -> Result<()> {
        self.lock_state()?.rollback.remove(&txn_id);
        Ok(())
    }
}

impl SchemaOperations for PageBackedStorageEngine {
    fn create_table(&self, ctx: &StatementContext, schema: &TableSchema) -> Result<()> {
        {
            let mut state = self.lock_state()?;
            self.append_wal(
                &state,
                ctx,
                WalRecordKind::CreateTable {
                    schema: schema.clone(),
                },
            )?;
            record_table_before(&mut state, ctx.txn_id, schema.id);
            state.tables.insert(
                schema.id,
                TableState {
                    schema: schema.clone(),
                    dropped: false,
                },
            );
        }
        // Create the empty on-disk index (metapage + root leaf). Its redo is
        // logged as full-page images, so recovery re-establishes it.
        self.btree(index_file_id(schema.id)).create(ctx.txn_id)
    }

    fn drop_table(&self, ctx: &StatementContext, table: TableId) -> Result<()> {
        let mut state = self.lock_state()?;
        if !state
            .tables
            .get(&table)
            .map(|table| !table.dropped)
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.append_wal(&state, ctx, WalRecordKind::DropTable { table })?;
        record_table_before(&mut state, ctx.txn_id, table);
        let table_state = state
            .tables
            .get_mut(&table)
            .ok_or_else(|| undefined_table(table))?;
        // V1 leaves the heap and index pages in place (no physical reclaim).
        table_state.dropped = true;
        // Cascade to the table's secondary indexes, mirroring the catalog's
        // drop-table cascade so the two stay consistent.
        mark_table_indexes_dropped(&mut state, ctx.txn_id, table);
        Ok(())
    }

    fn create_index(
        &self,
        ctx: &StatementContext,
        schema: &IndexSchema,
        gc_horizon: u64,
    ) -> Result<()> {
        let (table_schema, pk_file_id) = self.table_handle(schema.table)?;
        {
            let mut state = self.lock_state()?;
            self.append_wal(
                &state,
                ctx,
                WalRecordKind::CreateIndex {
                    schema: schema.clone(),
                },
            )?;
            record_index_before(&mut state, ctx.txn_id, schema.id);
            state.indexes.insert(
                schema.id,
                IndexState {
                    schema: schema.clone(),
                    dropped: false,
                },
            );
        }
        // Build the empty secondary tree (its pages are full-page-image redo), then
        // backfill it from the live rows via the primary-key index. Each PK entry's
        // TID is a HOT-chain ROOT; the new secondary entry points at that ROOT
        // (uniform with how HOT chains are addressed — the H1 walk resolves a root to
        // the live version).
        //
        // **HOT broken-chain safety (fail-fast, H2 — `docs/specs/mvcc.md` §10).** The
        // caller runs CREATE INDEX under the EXCLUSIVE guard (no concurrent writer), so
        // the physical chain view is stable for the duration of the build. For each
        // chain we examine its physically-present versions; if TWO OR MORE are NOT
        // dead-to-all at `gc_horizon` and DIFFER on the new index's column(s), some
        // live snapshot may span the chain and a single root-pointed entry cannot
        // serve all snapshots (the planner consumes equality predicates into the index
        // range and does not re-check them), so we abort with a retryable `40001`.
        let secondary = self.secondary_btree(schema.id);
        secondary.create(ctx.txn_id)?;
        for (_pk, root) in self.btree(pk_file_id).range(&KeyRange::All)? {
            // The physically-present versions reachable from this chain root (the root
            // plus any heap-only HOT-chain members on its page), in chain order.
            let versions = self.collect_chain_versions(&table_schema, root)?;

            // A non-HOT root is its own one-element chain (no HOT successors). Index its
            // PHYSICAL row unconditionally, pointing at the root — exactly the pre-HOT
            // backfill behavior: every physically-present row (committed, in-flight, or
            // aborted) gets an entry, and the scan filters by visibility at read time.
            // The broken-chain hazard cannot arise for a single-version chain. Use the
            // version `collect_chain_versions` resolved (which already followed a
            // REDIRECT root to its NORMAL target) rather than re-reading `root` — a
            // REDIRECT slot reads no bytes directly. A reclaimed (DEAD/UNUSED) root
            // resolves to no versions; nothing to index.
            if versions.len() <= 1 {
                if let Some((_loc, decoded)) = versions.first() {
                    let (key, has_null) = secondary_index_key(&table_schema, schema, &decoded.row)?;
                    self.insert_secondary_entry(ctx, &table_schema, schema, &key, has_null, &root)?;
                }
                continue;
            }

            // A HOT chain (root HOT_UPDATED → heap-only successors) is reached via the
            // SINGLE root entry; H1's bounded walk resolves it per a reader's snapshot.
            // Collect the DISTINCT new-index keys across the chain's not-dead-to-all
            // versions — i.e. the versions some still-live snapshot may see. Aborted-
            // creator / committed-deleted-below-`gc_horizon` versions are dead to
            // everyone and cannot be spanned by any snapshot, so they are excluded.
            let mut live_entries: Vec<(Key, bool)> = Vec::new();
            for (_loc, decoded) in &versions {
                if common::is_dead_to_all(
                    decoded.xmin,
                    decoded.xmax,
                    decoded.infomask,
                    gc_horizon,
                    self.txn_status_view(),
                ) {
                    continue;
                }
                let (new_key, has_null) = secondary_index_key(&table_schema, schema, &decoded.row)?;
                if !live_entries.iter().any(|(k, _)| *k == new_key) {
                    live_entries.push((new_key, has_null));
                }
            }

            // Two or more distinct live keys ⇒ the chain is broken: a single
            // root-pointed entry cannot serve every snapshot's value. Abort with a
            // retryable `40001` (`docs/specs/mvcc.md` §10 Milestone H2).
            if live_entries.len() >= 2 {
                return Err(DbError::execute(
                    SqlState::SerializationFailure,
                    "cannot build index over a live HOT chain with differing key values; \
                     retry after the transaction ends or after VACUUM",
                ));
            }

            // Exactly one live key (all live versions agree): index it, pointing the
            // entry at the chain ROOT — UNCONDITIONALLY, not gated on the BUILDER's
            // snapshot. A version may be not-dead-to-all (visible to an older concurrent
            // lock-free reader) yet invisible to this builder's own newer snapshot;
            // indexing it anyway is what lets that older reader find the row via the new
            // index (the planner does not re-check the equality at the heap). Zero live
            // keys means every version is dead-to-all — no snapshot can see the chain —
            // so there is nothing to index.
            if let Some((key, has_null)) = live_entries.into_iter().next() {
                self.insert_secondary_entry(ctx, &table_schema, schema, &key, has_null, &root)?;
            }
        }
        Ok(())
    }

    fn drop_index(&self, ctx: &StatementContext, index: IndexId) -> Result<()> {
        let mut state = self.lock_state()?;
        if !state
            .indexes
            .get(&index)
            .map(|index| !index.dropped)
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.append_wal(&state, ctx, WalRecordKind::DropIndex { index })?;
        record_index_before(&mut state, ctx.txn_id, index);
        let index_state = state
            .indexes
            .get_mut(&index)
            .ok_or_else(|| undefined_index(index))?;
        // V1 leaves the index pages in place (no physical reclaim), like drop_table.
        index_state.dropped = true;
        Ok(())
    }
}

struct PageRowIterator {
    schema: Vec<ColumnInfo>,
    rows: Vec<StoredRow>,
    index: usize,
}

impl RowIterator for PageRowIterator {
    fn next(&mut self) -> Result<Option<StoredRow>> {
        let Some(row) = self.rows.get(self.index).cloned() else {
            return Ok(None);
        };
        self.index += 1;
        Ok(Some(row))
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }
}

pub(crate) fn key_for_row(schema: &TableSchema, row: &Row) -> Result<Key> {
    let mut values = Vec::with_capacity(schema.primary_key.len());
    for primary_key in &schema.primary_key {
        let value = column_value(schema, row, *primary_key)?;
        if matches!(value, Value::Null) {
            return Err(DbError::execute(
                SqlState::NotNullViolation,
                "primary key cannot be NULL",
            ));
        }
        values.push(value);
    }
    Ok(Key(values))
}

/// The value of column `column_id` in `row`, located by the schema's column
/// order. An unknown column or a too-short row is corrupt state.
fn column_value(schema: &TableSchema, row: &Row, column_id: ColumnId) -> Result<Value> {
    let slot = schema
        .columns
        .iter()
        .position(|column| column.id == column_id)
        .ok_or_else(|| storage_internal("column is missing from table schema"))?;
    row.values
        .get(slot)
        .cloned()
        .ok_or_else(|| storage_internal("row is missing a column value"))
}

/// The secondary-index B-tree key for `row`: just the encoded indexed column(s).
/// The primary key is no longer embedded — duplicate secondary keys are
/// disambiguated by the heap TID in the tree's `(key, tid)` ordering. Returns the
/// key together with whether any indexed value is NULL, so the unique-constraint
/// probe can skip NULL keys (SQL treats NULLs as distinct, so NULL never
/// participates in a unique constraint; distinct NULL rows coexist via their
/// differing TIDs).
fn secondary_index_key(table: &TableSchema, index: &IndexSchema, row: &Row) -> Result<(Key, bool)> {
    let mut values = Vec::with_capacity(index.columns.len());
    let mut has_null = false;
    for column_id in &index.columns {
        let value = column_value(table, row, *column_id)?;
        has_null |= matches!(value, Value::Null);
        values.push(value);
    }
    Ok((Key(values), has_null))
}

fn live_table(state: &StorageState, table: TableId) -> Result<&TableState> {
    let table_state = state
        .tables
        .get(&table)
        .ok_or_else(|| undefined_table(table))?;
    if table_state.dropped {
        return Err(undefined_table(table));
    }
    Ok(table_state)
}

fn record_table_before(state: &mut StorageState, txn_id: u64, table: TableId) {
    if txn_id == 0 {
        return;
    }
    let previous = state.tables.get(&table).cloned();
    state
        .rollback
        .entry(txn_id)
        .or_default()
        .tables
        .entry(table)
        .or_insert(previous);
}

fn record_index_before(state: &mut StorageState, txn_id: u64, index: IndexId) {
    if txn_id == 0 {
        return;
    }
    let previous = state.indexes.get(&index).cloned();
    state
        .rollback
        .entry(txn_id)
        .or_default()
        .indexes
        .entry(index)
        .or_insert(previous);
}

/// Mark every live secondary index on `table` dropped (with rollback tracking
/// under `txn_id`; `0` skips it for recovery). Dropping a table cascades to its
/// indexes, keeping storage's index set consistent with the catalog's.
fn mark_table_indexes_dropped(state: &mut StorageState, txn_id: u64, table: TableId) {
    let index_ids: Vec<IndexId> = state
        .indexes
        .iter()
        .filter(|(_, index)| !index.dropped && index.schema.table == table)
        .map(|(id, _)| *id)
        .collect();
    for index_id in index_ids {
        record_index_before(state, txn_id, index_id);
        if let Some(index) = state.indexes.get_mut(&index_id) {
            index.dropped = true;
        }
    }
}

fn column_info(schema: &TableSchema) -> Vec<ColumnInfo> {
    schema
        .columns
        .iter()
        .map(|column| ColumnInfo {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            table_id: Some(schema.id),
            column_id: Some(column.id),
        })
        .collect()
}

fn page_overhead() -> usize {
    page::HEADER_LEN + page::SLOT_LEN
}

fn undefined_table(table: TableId) -> DbError {
    DbError::storage(
        SqlState::UndefinedTable,
        format!("table id {table} does not exist"),
    )
}

fn undefined_index(index: IndexId) -> DbError {
    DbError::storage(
        SqlState::UndefinedTable,
        format!("index id {index} does not exist"),
    )
}

fn duplicate_unique_index(name: &str) -> DbError {
    DbError::storage(
        SqlState::UniqueViolation,
        format!("duplicate key value violates unique index {name}"),
    )
}

fn duplicate_primary_key() -> DbError {
    DbError::storage(SqlState::UniqueViolation, "duplicate primary key")
}

/// A concurrent inserter held the unique key with an as-yet-uncommitted version, so
/// uniqueness is undecidable. The fail-fast first-updater-wins policy (§7.3) returns
/// [`SqlState::SerializationFailure`] (`40001`) rather than blocking; the client may
/// retry, and if the other inserter aborts the retry succeeds.
fn unique_conflict_retry() -> DbError {
    DbError::storage(
        SqlState::SerializationFailure,
        "could not determine uniqueness: a concurrent transaction holds this key; retry",
    )
}

fn storage_internal(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod visibility_tests {
    use std::sync::Arc;

    use buffer::{BufferPool, MemoryBufferPool, PageStore};
    use common::{
        ColumnDef, DataType, INVALID_XID, IndexSchema, Key, KeyRange, PageFlushInfo, Row, RowId,
        Snapshot, SqlState, StatementContext, TableSchema, Value,
    };
    use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

    use super::PageBackedStorageEngine;
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
                PageBackedStorageEngine::open(buffer, wal.clone(), super::StorageMode::Normal)
                    .unwrap();
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
        fn decode_physical(
            &self,
            location: super::RowLocation,
        ) -> Option<crate::codec::DecodedRow> {
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
            let schema = users_schema();
            let btree = self.engine.btree(crate::heap::index_file_id(TABLE_ID));
            self.engine
                .locate_visible_version(&schema, &btree, key, &snapshot, current_txn)
                .unwrap()
        }
    }

    fn ctx(txn_id: u64, snapshot: Snapshot) -> StatementContext {
        StatementContext::with_snapshot(txn_id, std::sync::Arc::new(snapshot))
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
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
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
        // Backfill reads the live physical rows (not snapshot-filtered), so every
        // row — including the aborted/in-progress ones — gets an index entry. The
        // scan must then *skip* the invisible ones at the heap, not error.
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
        // The index has entries for all three rows, but only the committed one is
        // visible; the entries pointing at the aborted/in-progress tuples are
        // skipped rather than returned or erroring.
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

    // --- E1c: concurrent-inserter unique conflicts as 40001 vs 23505 (mvcc.md §7.3) ---
    //
    // A key held by another transaction's still-uncommitted insert is undecidable:
    // the inserter cannot tell whether it is a true duplicate (that txn may yet
    // abort), so it returns `SerializationFailure` (40001, retry) rather than the
    // definite `UniqueViolation` (23505). These are planted with the existing
    // CLOG-driving fixture: insert under a creator txn and leave it in-progress
    // (no Commit/Abort) to model the concurrent uncommitted inserter, then commit or
    // abort it to settle the outcome. Under serialized writers this case cannot
    // arise at runtime (E2b), so the engine tests plant it directly.

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

    /// INSERT racing an **in-progress** other inserter of the same primary key fails
    /// fast with `SerializationFailure` (40001), not `UniqueViolation`: the key's
    /// only version has an uncommitted creator that may yet abort, so uniqueness is
    /// undecidable.
    #[test]
    fn insert_pk_in_flight_other_inserter_is_serialization_failure() {
        let fixture = fixture_with_table_and_name_index();

        // Creator txn 10 inserts key 1 and is left in-progress (no commit/abort).
        fixture
            .engine
            .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "inflight"))
            .unwrap();

        // Txn 11 races to insert the same key: the in-flight version is undecidable.
        let err = fixture
            .engine
            .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(1, "racer"))
            .unwrap_err();
        assert_eq!(err.code, common::SqlState::SerializationFailure);
    }

    /// Sequencing the same race: once the in-flight creator **commits**, a later
    /// INSERT of that key is a definite duplicate ⇒ `UniqueViolation` (23505).
    #[test]
    fn insert_pk_in_flight_then_committed_becomes_unique_violation() {
        let fixture = fixture_with_table_and_name_index();

        fixture
            .engine
            .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "inflight"))
            .unwrap();

        // Phase 1 — still in-flight ⇒ 40001.
        let retry = fixture
            .engine
            .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(1, "racer"))
            .unwrap_err();
        assert_eq!(retry.code, common::SqlState::SerializationFailure);

        // Phase 2 — the creator commits ⇒ a later INSERT is a definite duplicate.
        fixture.commit(10);
        let dup = fixture
            .engine
            .insert(&ctx(12, snapshot(13, vec![])), TABLE_ID, row(1, "racer"))
            .unwrap_err();
        assert_eq!(dup.code, common::SqlState::UniqueViolation);
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

    /// The same in-flight→40001 / committed→23505 split for a UNIQUE SECONDARY index:
    /// a duplicate unique name held only by an uncommitted inserter is `40001`; once
    /// that inserter commits it becomes a definite `UniqueViolation`.
    #[test]
    fn insert_unique_secondary_in_flight_then_committed_split() {
        let fixture = fixture_with_unique_name_index();

        // Creator txn 10 inserts (id 1, name "amy") and is left in-progress.
        fixture
            .engine
            .insert(&ctx(10, snapshot(11, vec![])), TABLE_ID, row(1, "amy"))
            .unwrap();

        // Phase 1 — a different row with the same unique name, while the holder is
        // in-flight ⇒ undecidable ⇒ 40001 (note: a DIFFERENT pk, so the conflict is
        // on the secondary index, not the PK).
        let retry = fixture
            .engine
            .insert(&ctx(11, snapshot(12, vec![])), TABLE_ID, row(2, "amy"))
            .unwrap_err();
        assert_eq!(retry.code, common::SqlState::SerializationFailure);

        // Phase 2 — the holder commits ⇒ the duplicate unique name is definite ⇒ 23505.
        fixture.commit(10);
        let dup = fixture
            .engine
            .insert(&ctx(12, snapshot(13, vec![])), TABLE_ID, row(3, "amy"))
            .unwrap_err();
        assert_eq!(dup.code, common::SqlState::UniqueViolation);
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

    /// DELETE conflicts with an **in-progress** deleter: `xmax = DELETER` is planted
    /// with no Commit/Abort, so the CLOG reads it `InProgress`; the fail-fast policy
    /// treats a live lock holder as a hard conflict ⇒ `40001`.
    #[test]
    fn delete_conflicts_with_in_progress_deleter() {
        let (fixture, _rid) = fixture_with_planted_deleter();
        // DELETER neither committed nor aborted ⇒ in-progress.

        let err = fixture
            .engine
            .delete(&ctx(WRITER, writer_snapshot()), TABLE_ID, &key(1))
            .unwrap_err();
        assert_eq!(err.code, common::SqlState::SerializationFailure);
    }

    /// UPDATE conflicts with an **in-progress** deleter (same fail-fast policy).
    #[test]
    fn update_conflicts_with_in_progress_deleter() {
        let (fixture, _rid) = fixture_with_planted_deleter();

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

    /// The no-op-under-serialized-writers case: a plain DELETE/UPDATE of a row whose
    /// `xmax = INVALID` (no prior lock) proceeds normally — the conflict check returns
    /// `Proceed` and behavior is unchanged.
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
        let target =
            fixture.append_raw_tuple(root.page_num, &row(1, "redirected"), 10, INVALID_XID, 0);
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
        let succ =
            fixture.append_raw_tuple(root.page_num, &row(1, "indexed_new"), 20, INVALID_XID, 0);
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
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
                ColumnDef {
                    id: 2,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
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
    fn vacuum_does_not_sever_a_hot_chain() {
        // Build a multi-version HOT chain, advance the horizon so the middle/root
        // versions are dead_to_all, run VACUUM, then read: the chain is intact and the
        // latest version is still visible (the vacuum skip-guard, H2 part 3).
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
        // Still exactly one PK + one secondary entry (HOT added none).
        assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
        assert_eq!(
            fixture.secondary_index_tids(name_index().id, "Ada"),
            vec![root]
        );

        // Horizon 100: every superseded version (xmax in {20,21,22}, all < 100 and
        // committed) is dead_to_all — but they are HOT-chain members, so VACUUM must
        // SKIP them rather than sever the chain.
        let schema = hot_schema();
        let reclaimed = fixture.engine.vacuum(&schema, 100).unwrap();
        assert_eq!(reclaimed, 0, "HOT-chain tuples are not reclaimed in H2");

        // The chain survives: the latest version is still resolvable, and the entries
        // still point at the (intact) root.
        assert_eq!(fixture.pk_index_tids(&key(1)), vec![root]);
        assert_eq!(
            fixture
                .engine
                .get(&ctx(0, snapshot(120, vec![])), TABLE_ID, &key(1))
                .unwrap(),
            Some(hot_row(1, "Ada", "v4"))
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
        // reclaim has NO age requirement).
        let schema = hot_schema();
        let reclaimed = fixture.engine.vacuum(&schema, 100).unwrap();
        assert!(
            reclaimed >= 1,
            "the aborted-creator HOT heap-only successor must be reclaimed"
        );

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
}

/// Structural-write-latch registry tests (Milestone E2a). These assert the latch
/// *substrate* (registry identity and that operations register the expected
/// per-file latches), not contention/atomicity. Real concurrent stress tests that
/// drive overlapping writers live in `concurrent_writers_tests` below (E2b).
#[cfg(test)]
mod structural_latch_tests {
    use std::sync::Arc;

    use buffer::{BufferPool, MemoryBufferPool, PageStore};
    use common::{
        ColumnDef, DataType, FileId, IndexSchema, PageFlushInfo, Row, Snapshot, StatementContext,
        TableSchema, Value,
    };
    use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

    use super::PageBackedStorageEngine;
    use crate::HeapPageStore;
    use crate::heap::{index_file_id, secondary_index_file_id};
    use crate::traits::{SchemaOperations, StorageEngine};

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
        let store: Arc<dyn PageStore> =
            Arc::new(HeapPageStore::open(dir.path().join("data")).unwrap());
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
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
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
            has_latch(&engine, index_file_id(TABLE_ID)),
            "primary-key index latch registered"
        );
        assert!(
            has_latch(&engine, secondary_index_file_id(NAME_INDEX_ID)),
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
}

/// Concurrent-writer stress tests (Milestone E2b). With the lock inversion the
/// engine's per-index / per-heap structural latches (E2a) and per-row conflict
/// detection (E1) become load-bearing: many threads drive the *shared* engine
/// (`Arc<PageBackedStorageEngine>`) concurrently with no global writer lock above
/// them, exactly as the server now does under the shared writer guard.
///
/// Determinism: threads start together on a `std::sync::Barrier` (no warm-up sleep)
/// and vary their work by THREAD INDEX (disjoint key ranges), never by sleeping.
/// Each test joins all handles within the test body (a hang would fail CI via the
/// harness timeout, and the dedicated deadlock-guard test bounds its own wait), then
/// asserts the exact post-state. No assertion depends on thread interleaving timing.
#[cfg(test)]
mod concurrent_writers_tests {
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
                PageBackedStorageEngine::open(buffer, wal.clone(), super::StorageMode::Normal)
                    .unwrap(),
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
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
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
}

/// `vacuum_heap` (`docs/specs/mvcc.md` §9, Milestone F2b): the heap-prune VACUUM
/// pass classifies each NORMAL tuple with `is_dead_to_all(horizon)`, prunes+compacts
/// pages with dead tuples, logs each pruned page as an unconditional FullPageImage,
/// and returns the dead TIDs. These tests drive the CLOG via `Commit`/`Abort` records
/// (the same fixture style as `visibility_tests`) so they control exactly which
/// `xmin`/`xmax` are committed/aborted/in-flight at a chosen `horizon`.
#[cfg(test)]
mod vacuum_tests {
    use std::sync::Arc;

    use std::collections::HashSet;

    use buffer::{BufferPool, MemoryBufferPool, PageStore};
    use common::{
        ColumnDef, DataType, IndexSchema, KeyRange, PageFlushInfo, Row, Snapshot, StatementContext,
        TableSchema, Value,
    };
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
                PageBackedStorageEngine::open(buffer, wal.clone(), super::StorageMode::Normal)
                    .unwrap();
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

        /// Every `FullPageImage` record in the WAL, as `(page_num, image)` pairs.
        fn full_page_images(&self) -> Vec<(u32, Vec<u8>)> {
            self.wal
                .replay_from(0)
                .unwrap()
                .filter_map(|record| match record.unwrap().kind {
                    WalRecordKind::FullPageImage {
                        file_id,
                        page_num,
                        image,
                    } if file_id == TABLE_ID => Some((page_num, image)),
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
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
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
        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 30).unwrap();
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

        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 9000).unwrap();
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

        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
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
        // guarantee — FPI redo regardless of txn id).
        let mut recovered = [0u8; buffer::PAGE_SIZE];
        for record in fixture.wal.replay_from(0).unwrap() {
            let record = record.unwrap();
            if let WalRecordKind::FullPageImage {
                file_id, page_num, ..
            } = &record.kind
                && *file_id == pk_file_id
                && *page_num == leaf_page
            {
                crate::redo::apply_physical_redo(&mut recovered, record.lsn, &record.kind).unwrap();
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
        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 9000).unwrap();
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

        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();

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
            crate::codec::decode_mvcc_header(&fixture.physical_bytes(aborted_delete).unwrap())
                .unwrap();
        assert_eq!(aborted_before.1, 41, "aborted-deleter xmax is stamped");

        let untouched_before: Vec<_> = [live, at_horizon, in_flight_delete]
            .iter()
            .map(|&loc| fixture.physical_bytes(loc))
            .collect();

        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 40).unwrap();

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
            crate::codec::decode_mvcc_header(&fixture.physical_bytes(aborted_delete).unwrap())
                .unwrap();
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

        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 100).unwrap();

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

        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
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
            if let WalRecordKind::FullPageImage {
                file_id, page_num, ..
            } = &record.kind
                && *file_id == TABLE_ID
                && *page_num == gone.page_num
            {
                crate::redo::apply_physical_redo(&mut recovered, record.lsn, &record.kind).unwrap();
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
        let dead_pages: std::collections::BTreeSet<u32> =
            dead.iter().map(|loc| loc.page_num).collect();
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

        let mut reclaimed = fixture.engine.vacuum_heap(&users_schema(), 200).unwrap();
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
            let reclaimed = self.engine.vacuum_heap(&users_schema(), horizon).unwrap();
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
        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
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
        let reclaimed = fixture.engine.vacuum_heap(&users_schema(), 21).unwrap();
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
            let target = match &record.kind {
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
                crate::redo::apply_physical_redo(&mut recovered, record.lsn, &record.kind).unwrap();
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
}
