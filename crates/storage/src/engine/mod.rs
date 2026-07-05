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
    ColumnId, ColumnInfo, CompressionSetting, DbError, FileId, IndexId, IndexSchema, Key, KeyRange,
    Lsn, PageNum, Result, Row, RowId, SequenceId, SequenceManager, SequenceSchema, Snapshot,
    SqlState, StatementContext, StoredRow, TableId, TableSchema, TxnStatusView, UniqueConflict,
    Value, WriteConflict, classify_unique_conflict, is_visible, write_conflict,
};
use parking_lot::Mutex as PlMutex;
use wal::{WalManager, WalRecord, WalRecordKind};

use crate::btree::BTree;
use crate::codec::{decode_row, encode_row};
use crate::heap::{index_file_id, secondary_index_file_id};
use crate::page;
use crate::traits::{RowIterator, SchemaOperations, StorageEngine};

mod dml;
mod index;
mod recovery;
mod vacuum;
mod visibility;

use dml::StampOutcome;

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

/// One physical member of a HOT chain, gathered by the H3 prune walk
/// (`docs/specs/mvcc.md` §9/§10 H3): its slot id and the MVCC header fields the
/// collapse decision needs. The `t_ctid` itself is not retained — the walk already
/// followed it to the next member.
#[derive(Clone, Copy, Debug)]
struct ChainMember {
    slot: u16,
    xmin: u64,
    xmax: u64,
    infomask: u16,
}

/// The HOT prune plan for ONE heap page (`docs/specs/mvcc.md` §9/§10 H3): the
/// line-pointer rewrites and in-place header resets `vacuum_heap` applies under the
/// frame latch, computed by [`PageBackedStorageEngine::classify_page_for_prune`].
///
/// Application order on the page is: header resets (in place) → free heap-only
/// members to `UNUSED` → redirect collapsed roots → mark fully-dead roots `DEAD` →
/// `compact`. Only `dead_roots` carry index entries that index vacuum (F3a) must
/// strip and line-pointer reclaim (F3b) must free; REDIRECT roots keep a LIVE entry
/// (F3a skips them) and heap-only members freed to `UNUSED` never had an entry.
#[derive(Default, Debug)]
struct PagePrunePlan {
    /// `(root_slot, live_tail_slot)`: a HOT root whose dead head is replaced by a
    /// REDIRECT to the surviving live tail. Its index entry stays live.
    redirect_roots: Vec<(u16, u16)>,
    /// Index-referenced roots (NORMAL or already-REDIRECT) of a fully-dead chain →
    /// `DEAD` (then F3a strips the entry, F3b reclaims the slot).
    dead_roots: Vec<u16>,
    /// `HEAP_ONLY` chain members (no index entry of their own) → `UNUSED` directly.
    free_to_unused: Vec<u16>,
    /// Slots whose header is reset in place (abort-cleanup: un-HOT a HOT predecessor
    /// of an aborted successor, or clear a non-HOT aborted-deleter stamp — F4c).
    reset_slots: Vec<u16>,
}

impl PagePrunePlan {
    fn is_empty(&self) -> bool {
        self.redirect_roots.is_empty()
            && self.dead_roots.is_empty()
            && self.free_to_unused.is_empty()
            && self.reset_slots.is_empty()
    }
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

#[derive(Clone)]
struct SequenceState {
    schema: Arc<Mutex<SequenceSchema>>,
}

impl SequenceState {
    fn new(schema: SequenceSchema) -> Self {
        Self {
            schema: Arc::new(Mutex::new(schema)),
        }
    }

    fn lock_schema(&self) -> Result<MutexGuard<'_, SequenceSchema>> {
        self.schema
            .lock()
            .map_err(|_| DbError::internal("sequence lock poisoned"))
    }

    fn snapshot(&self) -> Result<Self> {
        Ok(Self::new(self.lock_schema()?.clone()))
    }
}

#[derive(Default)]
struct TxnRollback {
    tables: BTreeMap<TableId, Option<TableState>>,
    indexes: BTreeMap<IndexId, Option<IndexState>>,
    sequences: BTreeMap<SequenceId, Option<SequenceState>>,
}

struct StorageState {
    mode: StorageMode,
    tables: BTreeMap<TableId, TableState>,
    indexes: BTreeMap<IndexId, IndexState>,
    sequences: BTreeMap<SequenceId, SequenceState>,
    rollback: BTreeMap<u64, TxnRollback>,
}

pub struct PageBackedStorageEngine {
    pub(crate) buffer_pool: Arc<dyn BufferPool>,
    pub(crate) wal: Arc<dyn WalManager>,
    /// Shared file-compression config + dictionary resolver
    /// (`docs/specs/compression.md`): heap/index at-rest envelopes consult it via
    /// `HeapPageStore`, and every WAL full-page image compresses through it
    /// (`fpi_record_kind`) unconditionally, independent of any file's config.
    pub(crate) compression: Arc<compress::CompressionRegistry>,
    state: Mutex<StorageState>,
    /// Per-[`FileId`] structural write latches (Milestone E2a; see the module-level
    /// lock-ordering doc). Lazily populated: the registry `Mutex` is held only
    /// briefly to look up or insert a file's `Arc<Mutex>`, never across the
    /// structural operation itself (else all structural ops would serialize
    /// globally). Shared across all transactions because the engine is shared via
    /// `Arc`, so two txns mutating the same index/heap contend on the same latch.
    structural_latches: Mutex<HashMap<FileId, Arc<PlMutex<()>>>>,
}

impl std::fmt::Debug for PageBackedStorageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageBackedStorageEngine")
            .finish_non_exhaustive()
    }
}

impl PageBackedStorageEngine {
    /// Open with a fresh, default (all-raw) [`compress::CompressionRegistry`] —
    /// no file compresses at rest and every WAL FPI still compresses
    /// unconditionally through the default dict-less codec (`compress_fpi`).
    pub fn open(
        buffer_pool: Arc<dyn BufferPool>,
        wal: Arc<dyn WalManager>,
        mode: StorageMode,
    ) -> Result<Self> {
        Self::open_with_compression(
            buffer_pool,
            wal,
            mode,
            Arc::new(compress::CompressionRegistry::new()),
        )
    }

    /// Open sharing `compression` with the caller (the server injects the SAME
    /// registry instance into the `HeapPageStore` for at-rest envelopes, so file
    /// configs set here are consulted by both, `docs/specs/compression.md` §5a).
    pub fn open_with_compression(
        buffer_pool: Arc<dyn BufferPool>,
        wal: Arc<dyn WalManager>,
        mode: StorageMode,
        compression: Arc<compress::CompressionRegistry>,
    ) -> Result<Self> {
        Ok(Self {
            buffer_pool,
            wal,
            compression,
            state: Mutex::new(StorageState {
                mode,
                tables: BTreeMap::new(),
                indexes: BTreeMap::new(),
                sequences: BTreeMap::new(),
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
            // `register_table_compression` touches only `self.compression` (a
            // separate lock), so it is safe to call while `state` is held.
            self.register_table_compression(&schema);
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
        let mut configs = Vec::with_capacity(schemas.len());
        for schema in schemas {
            // A secondary index's file never uses the heap's trained dictionary,
            // so its config is derived from the OWNING table's compression
            // setting alone (`compression.md` §4). The table is looked up in the
            // ALREADY-HELD `state` (installed before its indexes at startup); a
            // miss is an internal inconsistency in the installed catalog.
            let table_compression = state
                .tables
                .get(&schema.table)
                .map(|table| table.schema.compression)
                .ok_or_else(|| {
                    storage_internal(format!(
                        "index {} references an unknown table {}",
                        schema.id, schema.table
                    ))
                })?;
            configs.push((
                secondary_index_file_id(schema.id),
                index_compression_for(table_compression),
            ));
            state.indexes.insert(
                schema.id,
                IndexState {
                    schema,
                    dropped: false,
                },
            );
        }
        drop(state);
        for (file_id, config) in configs {
            self.compression.set_file_config(file_id, config);
        }
        Ok(())
    }

    pub fn install_sequences(&self, schemas: Vec<SequenceSchema>) -> Result<()> {
        let mut state = self.lock_state()?;
        state.sequences.clear();
        for schema in schemas {
            state
                .sequences
                .insert(schema.id, SequenceState::new(schema));
        }
        Ok(())
    }

    pub fn sequence_schemas_for_checkpoint(&self) -> Result<Vec<SequenceSchema>> {
        let sequences = {
            let state = self.lock_state()?;
            state.sequences.values().cloned().collect::<Vec<_>>()
        };
        sequences
            .iter()
            .map(|sequence| Ok(sequence.lock_schema()?.clone()))
            .collect()
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
        BTree::new(
            self.buffer_pool.as_ref(),
            self.wal.as_ref(),
            index_file_id,
            self.compression.as_ref(),
        )
    }

    /// The B-tree for a secondary index. Uniform with the primary-key index: keyed
    /// by the indexed columns and storing the heap `RowLocation` (TID) as its value,
    /// so duplicate indexed values are disambiguated by the `(key, tid)` ordering.
    fn secondary_btree(&self, index: IndexId) -> BTree<'_, RowLocation> {
        BTree::new(
            self.buffer_pool.as_ref(),
            self.wal.as_ref(),
            secondary_index_file_id(index),
            self.compression.as_ref(),
        )
    }

    /// Install `schema`'s at-rest file configs into the shared registry: the
    /// heap file gets the table's codec + trained dictionary; the primary-key
    /// index file gets the SAME codec but never the heap's dictionary
    /// (`docs/specs/compression.md` §4). Called whenever a table's schema is
    /// installed or its compression setting changes.
    fn register_table_compression(&self, schema: &TableSchema) {
        use compress::FileCompression;
        let heap_config = match schema.compression {
            CompressionSetting::None => FileCompression::None,
            CompressionSetting::Zstd => FileCompression::Zstd {
                dict_id: schema.active_dict_id,
            },
        };
        // Index pages never use the heap-trained dictionary (`compression.md` §4).
        let index_config = match schema.compression {
            CompressionSetting::None => FileCompression::None,
            CompressionSetting::Zstd => FileCompression::Zstd { dict_id: None },
        };
        self.compression.set_file_config(schema.id, heap_config);
        self.compression
            .set_file_config(index_file_id(schema.id), index_config);
    }

    /// Install an ALTERed schema: swap the TableState schema and re-register
    /// file configs. No WAL — the caller (server ALTER / recovery replay) owns
    /// record emission and ordering (`compression.md` §8).
    pub fn set_table_compression(&self, schema: &TableSchema) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("state lock poisoned"))?;
        let table = state
            .tables
            .get_mut(&schema.id)
            .filter(|t| !t.dropped)
            .ok_or_else(|| DbError::internal(format!("table {} is not installed", schema.id)))?;
        table.schema = schema.clone();
        let secondary_ids: Vec<IndexId> = state
            .indexes
            .values()
            .filter(|i| !i.dropped && i.schema.table == schema.id)
            .map(|i| i.schema.id)
            .collect();
        drop(state);
        self.register_table_compression(schema);
        let index_config = index_compression_for(schema.compression);
        for index_id in secondary_ids {
            self.compression
                .set_file_config(secondary_index_file_id(index_id), index_config);
        }
        Ok(())
    }

    /// Evenly-sampled initialized heap page images for dictionary training.
    /// Caller holds the exclusive guard, so the images are stable.
    pub fn sample_heap_pages(&self, schema: &TableSchema, cap: usize) -> Result<Vec<Vec<u8>>> {
        let file_id = schema.id;
        let page_count = self.buffer_pool.page_count(file_id)?;
        if page_count == 0 || cap == 0 {
            return Ok(Vec::new());
        }
        let step = (page_count as usize).div_ceil(cap).max(1) as PageNum;
        let mut samples = Vec::new();
        let mut page_num = 0;
        while page_num < page_count {
            if !self.buffer_pool.is_page_abandoned(file_id, page_num) {
                let guard = self.buffer_pool.read_page(file_id, page_num)?;
                if page::is_initialized(guard.data()) {
                    samples.push(guard.data().to_vec());
                }
            }
            page_num += step;
        }
        Ok(samples)
    }

    /// Re-encode every initialized page of the table's heap, PK-index, and
    /// live secondary-index files at rest under the updated registry config.
    /// Each page is logged as a single unconditional `FullPageImage` (under
    /// the maintenance txn id, [`VACUUM_TXN`]) and the FPI's LSN is stamped
    /// as the page's new PageLSN — exactly the `vacuum_heap` /
    /// `reclaim_line_pointers` pattern — so a torn write during the
    /// following flush is repaired by redo replaying the FPI
    /// (`compression.md` §8 step 7). Logical content is unchanged; only the
    /// PageLSN header field advances.
    pub fn rewrite_table_pages(&self, schema: &TableSchema) -> Result<usize> {
        let mut files = vec![schema.id, index_file_id(schema.id)];
        {
            let state = self.lock_state()?;
            files.extend(
                state
                    .indexes
                    .values()
                    .filter(|i| !i.dropped && i.schema.table == schema.id)
                    .map(|i| secondary_index_file_id(i.schema.id)),
            );
        }
        let mut touched = 0usize;
        for file_id in files {
            let page_count = self.buffer_pool.page_count(file_id)?;
            let latch = self.structural_latch(file_id);
            for page_num in 0..page_count {
                if self.buffer_pool.is_page_abandoned(file_id, page_num) {
                    continue;
                }
                {
                    let guard = self.buffer_pool.read_page(file_id, page_num)?;
                    // Heap AND index files are walked here (unlike the
                    // heap-only `page::is_initialized`), so accept either
                    // page type.
                    if !page::is_any_page_initialized(guard.data()) {
                        continue;
                    }
                }
                let _structural = latch.lock();
                let mut guard = self.buffer_pool.write_page(file_id, page_num, VACUUM_TXN)?;
                let image = *guard.data();
                let fpi_lsn = self.wal.append(WalRecord {
                    lsn: 0,
                    txn_id: VACUUM_TXN,
                    kind: fpi_record_kind(&self.compression, file_id, page_num, &image),
                })?;
                page::set_page_lsn(guard.data_mut(), fpi_lsn);
                touched += 1;
            }
        }
        Ok(touched)
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

    fn append_and_flush_sequence_wal(
        &self,
        mode: StorageMode,
        txn_id: u64,
        kind: WalRecordKind,
    ) -> Result<()> {
        if mode == StorageMode::Normal {
            self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind,
            })?;
            self.wal.flush()?;
        }
        Ok(())
    }

    fn sequence_handle(&self, sequence: SequenceId) -> Result<(StorageMode, SequenceState)> {
        let state = self.lock_state()?;
        let sequence_state = state
            .sequences
            .get(&sequence)
            .cloned()
            .ok_or_else(|| undefined_sequence(sequence))?;
        Ok((state.mode, sequence_state))
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
        // another in-progress inserter is undecidable, so we drop the latch, wait, and
        // re-check. Holding the latch across BOTH the scan and the insert (incl. any
        // leaf/parent/root split + `set_root`) is what stops two concurrent inserts of
        // the same key from both passing the check and both inserting.
        {
            let latch = self.structural_latch(index_fid);
            loop {
                let guard = latch.lock();
                match self.unique_conflict_kind(&btree, &key, &schema, &ctx.live_txns)? {
                    UniqueConflict::Violation => return Err(duplicate_primary_key()),
                    UniqueConflict::None => {
                        btree.insert(ctx.txn_id, &key, &location)?;
                        break;
                    }
                    // A key held only by an in-progress inserter is undecidable: drop
                    // the structural latch BEFORE blocking (the holder may itself be
                    // waiting on this latch, which would deadlock — `docs/specs/deadlock.md`),
                    // then re-check under a fresh latch (committed ⇒ 23505; aborted ⇒ free).
                    UniqueConflict::WouldBlock(blocker) => {
                        drop(guard);
                        self.wait_for_conflict(ctx, blocker)?;
                    }
                }
            }
        }

        for index in self.table_indexes(table)? {
            let (entry_key, has_null) = secondary_index_key(&schema, &index, &row)?;
            self.insert_secondary_entry(ctx, &schema, &index, &entry_key, has_null, &location)?;
        }

        // SSI: this insert may complete an rw-antidependency with a concurrent
        // serializable reader of the table (or a point reader of this key) — the
        // phantom case (`docs/specs/ssi.md` §6). No-op for non-SERIALIZABLE writers; an
        // `Err` is the SSI `40001` victim, aborting this statement.
        ctx.ssi_tracker.note_write(ctx.txn_id, table, &key)?;

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
                self.read_visible_row(&schema, location, &ctx.snapshot, &ctx.live_txns)?
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
            self.locate_visible_version(&schema, &btree, key, &ctx.snapshot, &ctx.live_txns)?
        else {
            return Ok(false);
        };

        // MVCC delete: stamp xmax on the still-NORMAL line pointer in place. The
        // tuple and *all* its index entries (PK and secondary) are retained — the
        // row is hidden by visibility (xmax committed ⇒ invisible to later
        // snapshots), and VACUUM (Milestone F) reclaims the dead version and its
        // entries. No tombstone, no index-entry removal.
        while let StampOutcome::WouldBlock(blocker) = self.stamp_xmax_logged(
            location,
            crate::codec::INVALID_TID,
            infomask,
            ctx.txn_id,
            &ctx.live_txns,
        )? {
            // An in-progress writer holds this row's lock: wait, then re-check.
            self.wait_for_conflict(ctx, blocker)?;
        }
        // SSI: this delete overwrote the row a concurrent serializable reader may have
        // read (`docs/specs/ssi.md` §6).
        ctx.ssi_tracker.note_write(ctx.txn_id, table, key)?;
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
            self.locate_visible_version(&schema, &btree, key, &ctx.snapshot, &ctx.live_txns)?
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
        // fully-indexed path when ineligible. When the predecessor's page is full, the
        // H3 update-path prune (under the heap latch, `ctx.gc_horizon` threaded in)
        // tries to reclaim same-page room first; only if it still cannot fit does it
        // fall back.
        if let Some(result) =
            self.try_hot_update(ctx, &schema, table, previous_location, infomask, &row)?
        {
            // SSI: a successful HOT update overwrote the row a concurrent serializable
            // reader may have read (`docs/specs/ssi.md` §6).
            if result {
                ctx.ssi_tracker.note_write(ctx.txn_id, table, key)?;
            }
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
        while let StampOutcome::WouldBlock(blocker) = self.stamp_xmax_logged(
            previous_location,
            new_tid,
            infomask,
            ctx.txn_id,
            &ctx.live_txns,
        )? {
            // An in-progress writer holds the predecessor's lock: wait, then
            // re-attempt the stamp (the new version is already written).
            self.wait_for_conflict(ctx, blocker)?;
        }

        // Primary-key entry for the new version, under ONE hold of the PK index
        // structural latch across the uniqueness check AND the insert (Milestone E2a,
        // atomic check-and-insert). The key is unchanged (a PK change is rejected
        // above), so this adds a second `(key, new_tid)` entry alongside the retained
        // old one. The uniqueness check now sees the old version as own-deleted
        // (`xmax == ctx.txn_id` ⇒ `UniqueConflict::None`), so the unchanged PK does not
        // falsely self-conflict; a collision with a *different* committed-live row is a
        // `UniqueViolation`, and one with another in-progress inserter waits and
        // re-checks. The latch is taken AFTER the
        // `stamp_xmax_logged` above (which holds only a frame latch, no structural
        // latch) and wraps the whole `insert` incl. any split/root-split; it is
        // released before the secondary inserts each take their own latch (rule 1).
        {
            let latch = self.structural_latch(index_fid);
            loop {
                let guard = latch.lock();
                match self.unique_conflict_kind(&btree, key, &schema, &ctx.live_txns)? {
                    UniqueConflict::Violation => return Err(duplicate_primary_key()),
                    UniqueConflict::None => {
                        btree.insert(ctx.txn_id, key, &new_location)?;
                        break;
                    }
                    // Drop the structural latch before blocking on the in-progress
                    // holder, then re-check (`docs/specs/deadlock.md`).
                    UniqueConflict::WouldBlock(blocker) => {
                        drop(guard);
                        self.wait_for_conflict(ctx, blocker)?;
                    }
                }
            }
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

        // SSI: the non-HOT update overwrote the row a concurrent serializable reader
        // may have read (`docs/specs/ssi.md` §6).
        ctx.ssi_tracker.note_write(ctx.txn_id, table, key)?;
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
                self.read_visible_row(&schema, location, &ctx.snapshot, &ctx.live_txns)?
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
                self.read_visible_row(&schema, location, &ctx.snapshot, &ctx.live_txns)?
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
        for (sequence_id, previous) in rollback.sequences.into_iter().rev() {
            match previous {
                Some(sequence) => {
                    state.sequences.insert(sequence_id, sequence);
                }
                None => {
                    state.sequences.remove(&sequence_id);
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
        // Register the heap/PK-index file configs before the tree's own pages
        // are created, so even its first metapage/root are encoded at rest per
        // the declared setting.
        self.register_table_compression(schema);
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
        // The new secondary index's file config mirrors the OWNING table's
        // codec but never its dictionary (`compression.md` §4).
        self.compression.set_file_config(
            secondary_index_file_id(schema.id),
            index_compression_for(table_schema.compression),
        );
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

    fn create_sequence(
        &self,
        ctx: &StatementContext,
        schema: &common::SequenceSchema,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        self.append_wal(
            &state,
            ctx,
            WalRecordKind::CreateSequence {
                schema: schema.clone(),
            },
        )?;
        record_sequence_before(&mut state, ctx.txn_id, schema.id)?;
        state
            .sequences
            .insert(schema.id, SequenceState::new(schema.clone()));
        Ok(())
    }

    fn drop_sequence(&self, ctx: &StatementContext, sequence: common::SequenceId) -> Result<()> {
        let mut state = self.lock_state()?;
        if !state.sequences.contains_key(&sequence) {
            return Ok(());
        }
        self.append_wal(&state, ctx, WalRecordKind::DropSequence { sequence })?;
        record_sequence_before(&mut state, ctx.txn_id, sequence)?;
        state.sequences.remove(&sequence);
        Ok(())
    }
}

impl SequenceManager for PageBackedStorageEngine {
    fn sequence_exists(&self, sequence: SequenceId) -> Result<bool> {
        let state = self.lock_state()?;
        Ok(state.sequences.contains_key(&sequence))
    }

    fn nextval(&self, txn_id: u64, sequence: SequenceId) -> Result<i64> {
        let (mode, sequence_state) = self.sequence_handle(sequence)?;
        let mut schema = sequence_state.lock_schema()?;
        let next = next_sequence_value(&schema)?;
        self.append_and_flush_sequence_wal(
            mode,
            txn_id,
            WalRecordKind::SequenceAdvance {
                sequence,
                value: next,
            },
        )?;
        schema.last_value = next;
        schema.is_called = true;
        Ok(next)
    }

    fn setval(
        &self,
        txn_id: u64,
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<i64> {
        let (mode, sequence_state) = self.sequence_handle(sequence)?;
        let mut schema = sequence_state.lock_schema()?;
        validate_sequence_value(&schema, value)?;
        self.append_and_flush_sequence_wal(
            mode,
            txn_id,
            WalRecordKind::SetSequenceValue {
                sequence,
                value,
                is_called,
            },
        )?;
        schema.last_value = value;
        schema.is_called = is_called;
        Ok(value)
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

/// Build the WAL record kind for a full-page image: the compressed variant
/// when the registry shrinks it (unconditional policy, `compression.md` §6),
/// the raw image otherwise. Compression failure can never fail a write.
pub(crate) fn fpi_record_kind(
    compression: &compress::CompressionRegistry,
    file_id: FileId,
    page_num: PageNum,
    image: &[u8; buffer::PAGE_SIZE],
) -> WalRecordKind {
    match compression.compress_fpi(file_id, image) {
        Some((codec, dict_id, payload)) => WalRecordKind::FullPageImageCompressed {
            file_id,
            page_num,
            codec,
            dict_id,
            payload,
        },
        None => WalRecordKind::FullPageImage {
            file_id,
            page_num,
            image: image.to_vec(),
        },
    }
}

/// The at-rest file config for a secondary-index file, mirroring the owning
/// table's `compression` setting but never its trained dictionary
/// (`docs/specs/compression.md` §4): index pages are always dict-less zstd (or
/// none).
fn index_compression_for(compression: CompressionSetting) -> compress::FileCompression {
    match compression {
        CompressionSetting::None => compress::FileCompression::None,
        CompressionSetting::Zstd => compress::FileCompression::Zstd { dict_id: None },
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

fn next_sequence_value(schema: &SequenceSchema) -> Result<i64> {
    if !schema.is_called {
        return Ok(schema.last_value);
    }
    let Some(next) = schema.last_value.checked_add(schema.increment) else {
        return sequence_exhausted(schema);
    };
    if next >= schema.min_value && next <= schema.max_value {
        return Ok(next);
    }
    sequence_exhausted(schema)
}

fn sequence_exhausted(schema: &SequenceSchema) -> Result<i64> {
    if schema.cycle {
        if schema.increment > 0 {
            Ok(schema.min_value)
        } else {
            Ok(schema.max_value)
        }
    } else {
        Err(DbError::storage(
            SqlState::NumericValueOutOfRange,
            format!("sequence {} reached its limit", schema.name),
        ))
    }
}

fn validate_sequence_value(schema: &SequenceSchema, value: i64) -> Result<()> {
    if value < schema.min_value || value > schema.max_value {
        return Err(DbError::storage(
            SqlState::NumericValueOutOfRange,
            format!(
                "value {value} is out of bounds for sequence {}",
                schema.name
            ),
        ));
    }
    Ok(())
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

fn record_sequence_before(
    state: &mut StorageState,
    txn_id: u64,
    sequence: SequenceId,
) -> Result<()> {
    if txn_id == 0 {
        return Ok(());
    }
    let previous = state
        .sequences
        .get(&sequence)
        .map(SequenceState::snapshot)
        .transpose()?;
    state
        .rollback
        .entry(txn_id)
        .or_default()
        .sequences
        .entry(sequence)
        .or_insert(previous);
    Ok(())
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
            pg_type: None,
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

fn undefined_sequence(sequence: SequenceId) -> DbError {
    DbError::storage(
        SqlState::UndefinedTable,
        format!("sequence id {sequence} does not exist"),
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

fn storage_internal(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod conflict_wait_test_support;
#[cfg(test)]
mod visibility_tests;

/// Structural-write-latch registry tests (Milestone E2a). These assert the latch
/// *substrate* (registry identity and that operations register the expected
/// per-file latches), not contention/atomicity. Real concurrent stress tests that
/// drive overlapping writers live in `concurrent_writers_tests` below (E2b).
#[cfg(test)]
mod structural_latch_tests;

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
mod concurrent_writers_tests;

/// `vacuum_heap` (`docs/specs/mvcc.md` §9, Milestone F2b): the heap-prune VACUUM
/// pass classifies each NORMAL tuple with `is_dead_to_all(horizon)`, prunes+compacts
/// pages with dead tuples, logs each pruned page as an unconditional FullPageImage,
/// and returns the dead TIDs. These tests drive the CLOG via `Commit`/`Abort` records
/// (the same fixture style as `visibility_tests`) so they control exactly which
/// `xmin`/`xmax` are committed/aborted/in-flight at a chosen `horizon`.
#[cfg(test)]
mod vacuum_tests;

/// Registry-wiring tests (compression Task 7): the engine constructed via
/// `open_with_compression` compresses WAL full-page images unconditionally and
/// exposes the ALTER-support methods (`set_table_compression`,
/// `sample_heap_pages`, `rewrite_table_pages`).
#[cfg(test)]
mod compression_tests;
