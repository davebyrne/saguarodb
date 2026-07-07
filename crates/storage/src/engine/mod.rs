//! ## Structural write latches and lock ordering (Milestone E2a)
//!
//! Stage-2 concurrency (`docs/specs/mvcc.md` §7.1, §10 E2a) serializes structural
//! mutations **within** one index or one table heap while allowing concurrent
//! writers across *different* indexes/heaps and lock-free B-link readers. The
//! substrate is a per-[`FileId`] registry of `Arc<parking_lot::Mutex<()>>` latches
//! ([`PageBackedStorageEngine::structural_latch`]); the engine is shared via `Arc`,
//! so two transactions mutating the same index/heap contend on the same latch.
//! A separate per-table identity rewrite gate protects the destructive reset/rebuild
//! used by `ALTER TABLE ... ADD/DROP PRIMARY KEY`: ordinary identity reads and
//! writes take its shared side, while the rebuild takes its exclusive side.
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use buffer::{BufferPool, PageWriteGuard};
use common::{
    ColumnDef, ColumnId, ColumnInfo, CompressionSetting, DataType, DbError, FileId, IndexId,
    IndexSchema, Key, KeyRange, Lsn, PageNum, RelationKind, Result, Row, RowId, SequenceId,
    SequenceManager, SequenceSchema, Snapshot, SqlState, StatementContext, StoredRow, TableId,
    TableSchema, TruncateCatalogUpdate, TruncateTablePlan, TxnStatusView, UniqueConflict, Value,
    WriteConflict, classify_unique_conflict, is_visible, write_conflict,
};
use parking_lot::{Mutex as PlMutex, RwLock as PlRwLock};
use wal::{WalManager, WalRecord, WalRecordKind};

use crate::btree::BTree;
use crate::codec::{
    DecodedPhysicalValue, ToastPointer, decode_mvcc_header, decode_physical_row, decode_row,
};
use crate::heap::{heap_file_id, primary_index_file_id, secondary_index_file_id};
use crate::page;
use crate::traits::{RelationSnapshot, RowIterator, SchemaOperations, StorageEngine};

mod dml;
mod index;
mod recovery;
mod vacuum;
mod visibility;

use dml::{HotUpdateRequest, StampOutcome};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageMode {
    Recovery,
    Normal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebuildWalMode {
    Logged(u64),
    Unlogged,
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

struct TableGeneration {
    schema: TableSchema,
    dropped: bool,
}

struct IndexGeneration {
    schema: IndexSchema,
    dropped: bool,
}

struct TableHandle {
    _generation: Arc<TableGeneration>,
    schema: TableSchema,
    primary_index_file_id: FileId,
}

impl TableHandle {
    fn new(generation: Arc<TableGeneration>) -> Self {
        Self {
            schema: generation.schema.clone(),
            primary_index_file_id: primary_index_file_id(generation.schema.storage_id),
            _generation: generation,
        }
    }
}

struct IndexHandle {
    _generation: Arc<IndexGeneration>,
    schema: IndexSchema,
}

impl IndexHandle {
    fn new(generation: Arc<IndexGeneration>) -> Self {
        Self {
            schema: generation.schema.clone(),
            _generation: generation,
        }
    }
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
    tables: BTreeMap<TableId, Option<Arc<TableGeneration>>>,
    indexes: BTreeMap<IndexId, Option<Arc<IndexGeneration>>>,
    sequences: BTreeMap<SequenceId, Option<SequenceState>>,
    unpublished_files: Vec<FileId>,
}

#[derive(Clone)]
pub(crate) struct PageBackedRelationSnapshot {
    tables: BTreeMap<TableId, Arc<TableGeneration>>,
    indexes: BTreeMap<IndexId, Arc<IndexGeneration>>,
    relation_epoch: u64,
}

impl RelationSnapshot for PageBackedRelationSnapshot {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn relation_epoch(&self) -> u64 {
        self.relation_epoch
    }
}

struct RetiredGeneration {
    files: Vec<FileId>,
    table_refs: Vec<Weak<TableGeneration>>,
    index_refs: Vec<Weak<IndexGeneration>>,
}

struct StorageState {
    mode: StorageMode,
    tables: BTreeMap<TableId, Arc<TableGeneration>>,
    indexes: BTreeMap<IndexId, Arc<IndexGeneration>>,
    sequences: BTreeMap<SequenceId, SequenceState>,
    toast_next_value_ids: BTreeMap<TableId, u64>,
    rollback: BTreeMap<u64, TxnRollback>,
    retired_generations: VecDeque<RetiredGeneration>,
    relation_epoch: u64,
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
    /// Per-table gate for storage-identity tree rewrites. Normal B-link scans and
    /// inserts take the shared side; ALTER PRIMARY KEY takes the exclusive side
    /// before resetting and rebuilding the identity tree.
    identity_rewrite_latches: Mutex<HashMap<TableId, Arc<PlRwLock<()>>>>,
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
                toast_next_value_ids: BTreeMap::new(),
                rollback: BTreeMap::new(),
                retired_generations: VecDeque::new(),
                relation_epoch: 0,
            }),
            structural_latches: Mutex::new(HashMap::new()),
            identity_rewrite_latches: Mutex::new(HashMap::new()),
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

    fn identity_rewrite_latch(&self, table: TableId) -> Arc<PlRwLock<()>> {
        let mut latches = self
            .identity_rewrite_latches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(latches.entry(table).or_default())
    }

    pub fn install_schemas(&self, schemas: Vec<TableSchema>) -> Result<()> {
        let seed_toast_allocators = self.lock_state()?.mode == StorageMode::Normal;
        let mut tables = BTreeMap::new();
        let mut toast_next_value_ids = BTreeMap::new();
        for schema in schemas {
            self.register_table_compression(&schema);
            if seed_toast_allocators && matches!(schema.relation_kind, RelationKind::Toast { .. }) {
                let next_value_id = self.seed_toast_next_value_id(&schema)?;
                toast_next_value_ids.insert(schema.id, next_value_id);
            }
            tables.insert(
                schema.id,
                Arc::new(TableGeneration {
                    schema,
                    dropped: false,
                }),
            );
        }
        let mut state = self.lock_state()?;
        state.tables = tables;
        state.toast_next_value_ids = toast_next_value_ids;
        bump_relation_epoch(&mut state);
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
                secondary_index_file_id(schema.storage_id),
                index_compression_for(table_compression),
            ));
            state.indexes.insert(
                schema.id,
                Arc::new(IndexGeneration {
                    schema,
                    dropped: false,
                }),
            );
        }
        bump_relation_epoch(&mut state);
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
        let should_reseed_toast = {
            let state = self.lock_state()?;
            state.mode == StorageMode::Recovery && mode == StorageMode::Normal
        };
        if should_reseed_toast {
            self.reseed_toast_value_ids_for_recovery_completion()?;
        }
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

    pub(crate) fn capture_pagebacked_relation_snapshot(
        &self,
    ) -> Result<PageBackedRelationSnapshot> {
        let state = self.lock_state()?;
        Ok(PageBackedRelationSnapshot {
            tables: state.tables.clone(),
            indexes: state.indexes.clone(),
            relation_epoch: state.relation_epoch,
        })
    }

    pub fn relation_epoch(&self) -> Result<u64> {
        Ok(self.lock_state()?.relation_epoch)
    }

    fn current_relations(&self) -> Result<Arc<dyn RelationSnapshot>> {
        <Self as StorageEngine>::capture_relation_snapshot(self)
    }

    pub fn insert(&self, ctx: &StatementContext, table: TableId, row: Row) -> Result<RowId> {
        let relations = self.current_relations()?;
        <Self as StorageEngine>::insert(self, ctx, relations.as_ref(), table, row)
    }

    pub fn get(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<Option<Row>> {
        let relations = self.current_relations()?;
        <Self as StorageEngine>::get(self, ctx, relations.as_ref(), table, key)
    }

    pub fn delete(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<bool> {
        let relations = self.current_relations()?;
        <Self as StorageEngine>::delete(self, ctx, relations.as_ref(), table, key)
    }

    pub fn update(
        &self,
        ctx: &StatementContext,
        table: TableId,
        key: &Key,
        row: Row,
    ) -> Result<bool> {
        let relations = self.current_relations()?;
        <Self as StorageEngine>::update(self, ctx, relations.as_ref(), table, key, row)
    }

    pub fn scan(&self, ctx: &StatementContext, table: TableId) -> Result<Box<dyn RowIterator>> {
        let relations = self.current_relations()?;
        <Self as StorageEngine>::scan(self, ctx, relations.as_ref(), table)
    }

    pub fn scan_range(
        &self,
        ctx: &StatementContext,
        table: TableId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let relations = self.current_relations()?;
        <Self as StorageEngine>::scan_range(self, ctx, relations.as_ref(), table, range)
    }

    pub fn index_scan(
        &self,
        ctx: &StatementContext,
        table: TableId,
        index: IndexId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let relations = self.current_relations()?;
        <Self as StorageEngine>::index_scan(self, ctx, relations.as_ref(), table, index, range)
    }

    /// The schema and index file id of a live table, looked up under the lock so
    /// the heap and B-tree work can run without holding it. The returned handle
    /// pins the generation files for as long as the caller may touch them.
    fn table_handle(
        &self,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
    ) -> Result<TableHandle> {
        let table_state = match relations.tables.get(&table) {
            Some(table_state) if !table_state.dropped => table_state.clone(),
            Some(_) => return Err(undefined_table(table)),
            None => {
                let state = self.lock_state()?;
                state
                    .tables
                    .get(&table)
                    .filter(|table_state| !table_state.dropped)
                    .cloned()
                    .ok_or_else(|| undefined_table(table))?
            }
        };
        Ok(TableHandle::new(table_state))
    }

    /// Like `table_handle`, but a missing or dropped table yields `None` (callers
    /// that treat that as a no-op rather than an error).
    fn table_handle_opt(
        &self,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
    ) -> Result<Option<TableHandle>> {
        match relations.tables.get(&table) {
            Some(table_state) if !table_state.dropped => {
                Ok(Some(TableHandle::new(table_state.clone())))
            }
            Some(_) => Ok(None),
            None => {
                let state = self.lock_state()?;
                Ok(state
                    .tables
                    .get(&table)
                    .filter(|table_state| !table_state.dropped)
                    .cloned()
                    .map(TableHandle::new))
            }
        }
    }

    #[allow(
        dead_code,
        reason = "called by the storage-private TOAST write path added in a later phase"
    )]
    pub(crate) fn alloc_toast_value_id(&self, toast_table: TableId) -> Result<u64> {
        let schema = {
            let mut state = self.lock_state()?;
            let schema = {
                let table_state = live_table(&state.tables, toast_table)?;
                crate::toast::ensure_toast_relation(&table_state.schema)?;
                table_state.schema.clone()
            };
            if let Some(next_value_id) = state.toast_next_value_ids.get_mut(&toast_table) {
                return crate::toast::allocate_next_value_id(next_value_id);
            }
            schema
        };

        let seeded_next_value_id = self.seed_toast_next_value_id(&schema)?;
        let mut state = self.lock_state()?;
        {
            let table_state = live_table(&state.tables, toast_table)?;
            crate::toast::ensure_toast_relation(&table_state.schema)?;
        }
        let next_value_id = state
            .toast_next_value_ids
            .entry(toast_table)
            .or_insert(seeded_next_value_id);
        crate::toast::allocate_next_value_id(next_value_id)
    }

    #[allow(
        dead_code,
        reason = "called by the row TOAST preparation path added in a later phase"
    )]
    pub(crate) fn write_toast_stream(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        base: &TableSchema,
        raw_len: u32,
        codec: u8,
        stream: &[u8],
    ) -> Result<ToastPointer> {
        crate::toast::parse_external_stream(codec, stream)?;
        let toast_table_id = base.toast_table_id.ok_or_else(|| {
            storage_internal(format!(
                "table {} does not have a hidden TOAST relation",
                base.name
            ))
        })?;
        let stored_len = u32::try_from(stream.len()).map_err(|_| {
            DbError::storage(
                SqlState::ProgramLimitExceeded,
                "external TOAST stream exceeds the supported length",
            )
        })?;
        let value_id = self.alloc_toast_value_id(toast_table_id)?;
        let pointer = ToastPointer {
            value_id,
            raw_len,
            stored_len,
            codec,
        };
        pointer.encode()?;

        for (seq, chunk) in stream.chunks(crate::toast::TOAST_CHUNK_PAYLOAD).enumerate() {
            <Self as StorageEngine>::insert(
                self,
                ctx,
                relations,
                toast_table_id,
                crate::toast::chunk_row(value_id, seq, chunk)?,
            )?;
        }

        Ok(pointer)
    }

    #[allow(
        dead_code,
        reason = "called by the detoast read path added in a later phase"
    )]
    pub(crate) fn read_toast_stream(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        base: &TableSchema,
        pointer: &ToastPointer,
    ) -> Result<Vec<u8>> {
        pointer.encode()?;
        let toast_table_id = base.toast_table_id.ok_or_else(|| {
            storage_internal(format!(
                "table {} does not have a hidden TOAST relation",
                base.name
            ))
        })?;
        let mut iter = <Self as StorageEngine>::scan_range(
            self,
            ctx,
            relations,
            toast_table_id,
            &KeyRange::Exact(toast_value_key_prefix(pointer.value_id)?),
        )?;
        let mut chunks = BTreeMap::new();
        while let Some(stored) = iter.next()? {
            let (value_id, seq, data) = toast_chunk_parts(&stored.row)?;
            if value_id != pointer.value_id || stored.key != toast_chunk_key(value_id, seq)? {
                return Err(crate::toast::toast_corruption(format!(
                    "TOAST chunk key does not match row for value_id {} seq {seq}",
                    pointer.value_id
                )));
            }
            if chunks.insert(seq, data).is_some() {
                return Err(crate::toast::toast_corruption(format!(
                    "TOAST chunks for value_id {} contain duplicate seq {seq}",
                    pointer.value_id
                )));
            }
        }
        let mut stream = Vec::with_capacity(pointer.stored_len as usize);
        let mut expected_seq = 0i64;
        for (seq, data) in chunks {
            if seq != expected_seq {
                return Err(crate::toast::toast_corruption(format!(
                    "TOAST chunks for value_id {} are missing, duplicate, or out of order at seq {seq}",
                    pointer.value_id
                )));
            }
            stream.extend_from_slice(&data);
            expected_seq = expected_seq.checked_add(1).ok_or_else(|| {
                DbError::storage(
                    SqlState::ProgramLimitExceeded,
                    "TOAST chunk sequence exceeds i64::MAX",
                )
            })?;
        }
        if stream.len() != pointer.stored_len as usize {
            return Err(crate::toast::toast_corruption(format!(
                "TOAST stream for value_id {} has {} bytes, expected {}",
                pointer.value_id,
                stream.len(),
                pointer.stored_len
            )));
        }
        crate::toast::parse_external_stream(pointer.codec, &stream)?;
        Ok(stream)
    }

    fn seed_toast_next_value_id(&self, schema: &TableSchema) -> Result<u64> {
        crate::toast::ensure_toast_relation(schema)?;
        let file_id = heap_file_id(schema.storage_id);
        let page_count = self.buffer_pool.page_count(file_id)?;
        let mut max_value_id: Option<u64> = None;
        for page_num in 0..page_count {
            if self.buffer_pool.is_page_abandoned(file_id, page_num) {
                continue;
            }
            let guard = self.buffer_pool.read_page(file_id, page_num)?;
            if !page::is_initialized(guard.data()) {
                continue;
            }
            let slot_count = page::next_slot(guard.data())?;
            for slot in 0..slot_count {
                let Some(row_bytes) = page::read_row(guard.data(), slot)? else {
                    continue;
                };
                let row = decode_row(schema, &row_bytes)?.row;
                let value_id = crate::toast::value_id_from_chunk_row(schema, &row)?;
                max_value_id = Some(max_value_id.map_or(value_id, |max| max.max(value_id)));
            }
        }
        max_value_id
            .map(crate::toast::next_after_value_id)
            .unwrap_or(Ok(crate::toast::FIRST_TOAST_VALUE_ID))
    }

    fn reseed_toast_value_ids_for_recovery_completion(&self) -> Result<()> {
        let toast_schemas = {
            let state = self.lock_state()?;
            state
                .tables
                .values()
                .filter(|table_state| {
                    !table_state.dropped
                        && matches!(table_state.schema.relation_kind, RelationKind::Toast { .. })
                })
                .map(|table_state| table_state.schema.clone())
                .collect::<Vec<_>>()
        };
        let mut toast_next_value_ids = BTreeMap::new();
        for schema in toast_schemas {
            toast_next_value_ids.insert(schema.id, self.seed_toast_next_value_id(&schema)?);
        }

        let mut state = self.lock_state()?;
        if state.mode == StorageMode::Recovery {
            state.toast_next_value_ids = toast_next_value_ids;
        }
        Ok(())
    }

    /// The live secondary indexes on a table, ordered by index id. DML consults
    /// this to keep every index in sync with the heap.
    fn table_indexes(
        &self,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
    ) -> Result<Vec<IndexSchema>> {
        if relations.tables.contains_key(&table) {
            return Ok(relations
                .indexes
                .values()
                .filter(|index| !index.dropped && index.schema.table == table)
                .map(|index| index.schema.clone())
                .collect());
        }
        self.current_table_indexes(table)
    }

    fn current_table_indexes(&self, table: TableId) -> Result<Vec<IndexSchema>> {
        let state = self.lock_state()?;
        Ok(state
            .indexes
            .values()
            .filter(|index| !index.dropped && index.schema.table == table)
            .map(|index| index.schema.clone())
            .collect())
    }

    /// Check that an index belongs to `table`, erroring if it was never installed
    /// or belongs elsewhere. Dropped indexes are still physically scan-readable so
    /// an already-planned statement can finish against retained entries; DML
    /// maintenance uses `table_indexes`, which filters dropped indexes out.
    fn index_handle(
        &self,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
        index: IndexId,
    ) -> Result<IndexHandle> {
        if relations.tables.contains_key(&table) {
            match relations.indexes.get(&index) {
                Some(index_state) if index_state.schema.table == table => {
                    Ok(IndexHandle::new(index_state.clone()))
                }
                _ => {
                    let snapshot_table = relations
                        .tables
                        .get(&table)
                        .filter(|table_state| !table_state.dropped)
                        .ok_or_else(|| undefined_index(index))?;
                    let state = self.lock_state()?;
                    let current_table = state
                        .tables
                        .get(&table)
                        .filter(|table_state| !table_state.dropped)
                        .ok_or_else(|| undefined_index(index))?;
                    if !Arc::ptr_eq(snapshot_table, current_table) {
                        return Err(undefined_index(index));
                    }
                    state
                        .indexes
                        .get(&index)
                        .filter(|index_state| index_state.schema.table == table)
                        .cloned()
                        .map(IndexHandle::new)
                        .ok_or_else(|| undefined_index(index))
                }
            }
        } else {
            let state = self.lock_state()?;
            state
                .indexes
                .get(&index)
                .filter(|index_state| index_state.schema.table == table)
                .cloned()
                .map(IndexHandle::new)
                .ok_or_else(|| undefined_index(index))
        }
    }

    fn ensure_current_generation_for_write(
        &self,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
    ) -> Result<()> {
        let Some(snapshot_table) = relations.tables.get(&table) else {
            return Ok(());
        };
        if snapshot_table.dropped {
            return Err(undefined_table(table));
        }

        let state = self.lock_state()?;
        let current_table = state
            .tables
            .get(&table)
            .filter(|table_state| !table_state.dropped)
            .ok_or_else(|| undefined_table(table))?;
        if !Arc::ptr_eq(snapshot_table, current_table) {
            return Err(relation_generation_write_conflict(table));
        }

        let snapshot_indexes = relations
            .indexes
            .iter()
            .filter(|(_, index)| !index.dropped && index.schema.table == table)
            .collect::<BTreeMap<_, _>>();
        let current_indexes = state
            .indexes
            .iter()
            .filter(|(_, index)| !index.dropped && index.schema.table == table)
            .collect::<BTreeMap<_, _>>();
        if snapshot_indexes.len() != current_indexes.len() {
            return Err(relation_generation_write_conflict(table));
        }
        for (index_id, snapshot_index) in snapshot_indexes {
            let Some(current_index) = current_indexes.get(index_id) else {
                return Err(relation_generation_write_conflict(table));
            };
            if !Arc::ptr_eq(snapshot_index, current_index) {
                return Err(relation_generation_write_conflict(table));
            }
        }

        Ok(())
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
    fn secondary_btree(&self, index: &IndexSchema) -> BTree<'_, RowLocation> {
        BTree::new(
            self.buffer_pool.as_ref(),
            self.wal.as_ref(),
            secondary_index_file_id(index.storage_id),
            self.compression.as_ref(),
        )
    }

    fn insert_storage_identity_entry(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        index_file_id: FileId,
        key: &Key,
        location: &RowLocation,
    ) -> Result<()> {
        let btree = self.btree(index_file_id);
        let latch = self.structural_latch(index_file_id);
        loop {
            let rewrite_latch = self.identity_rewrite_latch(schema.id);
            let rewrite_guard = rewrite_latch.read();
            let guard = latch.lock();
            if !schema.primary_key.is_empty() {
                match self.unique_conflict_kind(&btree, key, schema, &ctx.live_txns)? {
                    UniqueConflict::Violation => return Err(duplicate_primary_key()),
                    UniqueConflict::WouldBlock(blocker) => {
                        drop(guard);
                        drop(rewrite_guard);
                        self.wait_for_conflict(ctx, blocker)?;
                        continue;
                    }
                    UniqueConflict::None => {}
                }
            }
            return btree.insert(ctx.txn_id, key, location);
        }
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
        self.compression
            .set_file_config(heap_file_id(schema.storage_id), heap_config);
        self.compression
            .set_file_config(primary_index_file_id(schema.storage_id), index_config);
    }

    /// Install an ALTERed schema: publish a new table generation and re-register
    /// file configs. No WAL — the caller (server ALTER / recovery replay) owns
    /// record emission and ordering (`compression.md` §8).
    pub fn set_table_compression(&self, schema: &TableSchema) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("state lock poisoned"))?;
        let dropped = state
            .tables
            .get(&schema.id)
            .filter(|t| !t.dropped)
            .ok_or_else(|| DbError::internal(format!("table {} is not installed", schema.id)))?
            .dropped;
        state.tables.insert(
            schema.id,
            Arc::new(TableGeneration {
                schema: schema.clone(),
                dropped,
            }),
        );
        bump_relation_epoch(&mut state);
        let secondary_storage_ids: Vec<FileId> = state
            .indexes
            .values()
            .filter(|i| !i.dropped && i.schema.table == schema.id)
            .map(|i| i.schema.storage_id)
            .collect();
        drop(state);
        self.register_table_compression(schema);
        let index_config = index_compression_for(schema.compression);
        for storage_id in secondary_storage_ids {
            self.compression
                .set_file_config(secondary_index_file_id(storage_id), index_config);
        }
        Ok(())
    }

    /// Install an ALTERed TOAST schema: publish a new table generation. No WAL — the
    /// caller (server ALTER / recovery replay) owns record emission and ordering.
    pub fn set_table_toast_metadata(&self, schema: &TableSchema) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("state lock poisoned"))?;
        let dropped = state
            .tables
            .get(&schema.id)
            .filter(|t| !t.dropped)
            .ok_or_else(|| DbError::internal(format!("table {} is not installed", schema.id)))?
            .dropped;
        state.tables.insert(
            schema.id,
            Arc::new(TableGeneration {
                schema: schema.clone(),
                dropped,
            }),
        );
        bump_relation_epoch(&mut state);
        Ok(())
    }

    /// Validate that the existing heap can be addressed by `schema`'s storage
    /// identity (logical primary key, or hidden heap identity when empty). This
    /// performs no page or metadata mutation and is used before the DDL commit.
    pub fn validate_table_primary_key_change(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        gc_horizon: u64,
    ) -> Result<()> {
        self.storage_identity_entries_for_schema(ctx, schema, gc_horizon)
            .map(|_| ())
    }

    /// Install a primary-key ALTER during recovery and rebuild the table identity
    /// B-tree from heap rows without appending WAL. The rebuild is derived from
    /// the committed logical DDL record after the WAL replay pass has reached the
    /// final heap state.
    pub fn set_table_primary_key(&self, schema: &TableSchema, gc_horizon: u64) -> Result<()> {
        self.set_table_primary_key_with_rebuild(schema, gc_horizon, RebuildWalMode::Unlogged)
    }

    /// Install a primary-key ALTER during normal execution and rebuild the table
    /// identity B-tree with physical full-page-image redo. The caller must flush
    /// WAL before any checkpoint may truncate the logical ALTER record.
    pub fn set_table_primary_key_logged(
        &self,
        schema: &TableSchema,
        gc_horizon: u64,
        txn_id: u64,
    ) -> Result<()> {
        self.set_table_primary_key_with_rebuild(schema, gc_horizon, RebuildWalMode::Logged(txn_id))
    }

    fn set_table_primary_key_with_rebuild(
        &self,
        schema: &TableSchema,
        gc_horizon: u64,
        wal_mode: RebuildWalMode,
    ) -> Result<()> {
        let ctx = StatementContext::new(0);
        let entries = self.storage_identity_entries_for_schema(&ctx, schema, gc_horizon)?;
        self.rebuild_storage_identity(schema, &entries, wal_mode)?;
        self.set_table_primary_key_metadata(schema)
    }

    pub(crate) fn set_table_primary_key_metadata(&self, schema: &TableSchema) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("state lock poisoned"))?;
        let dropped = state
            .tables
            .get(&schema.id)
            .filter(|t| !t.dropped)
            .ok_or_else(|| DbError::internal(format!("table {} is not installed", schema.id)))?
            .dropped;
        state.tables.insert(
            schema.id,
            Arc::new(TableGeneration {
                schema: schema.clone(),
                dropped,
            }),
        );
        bump_relation_epoch(&mut state);
        Ok(())
    }

    fn storage_identity_entries_for_schema(
        &self,
        ctx: &StatementContext,
        new_schema: &TableSchema,
        gc_horizon: u64,
    ) -> Result<Vec<(Key, RowLocation)>> {
        let old_schema = {
            let state = self.lock_state()?;
            live_table(&state.tables, new_schema.id)?.schema.clone()
        };
        let relations = self.capture_pagebacked_relation_snapshot()?;
        if old_schema.columns.len() != new_schema.columns.len()
            || old_schema
                .columns
                .iter()
                .zip(&new_schema.columns)
                .any(|(old, new)| old.id != new.id || old.data_type != new.data_type)
        {
            return Err(storage_internal(format!(
                "table {} primary-key ALTER changed the row layout",
                new_schema.name
            )));
        }

        let mut entries = Vec::new();
        let mut live_primary_key_rows = HashSet::new();
        for root in self.heap_identity_roots(&old_schema)? {
            let versions = self.collect_chain_versions(&old_schema, root)?;
            let mut live_entries = Vec::new();
            for (_loc, decoded) in &versions {
                if common::is_dead_to_all(
                    decoded.header.xmin,
                    decoded.header.xmax,
                    decoded.header.infomask,
                    gc_horizon,
                    self.txn_status_view(),
                ) {
                    continue;
                }
                let row =
                    self.materialize_physical_row(ctx, &relations, &old_schema, decoded.clone())?;
                let key = storage_identity_key_for_row(new_schema, &row, root)?;
                let conflict = classify_unique_conflict(
                    decoded.header.xmin,
                    decoded.header.xmax,
                    decoded.header.infomask,
                    &ctx.live_txns,
                    self.txn_status_view(),
                );
                if let Some((_existing_key, existing_conflict)) = live_entries
                    .iter_mut()
                    .find(|(existing_key, _)| *existing_key == key)
                {
                    *existing_conflict = strongest_unique_conflict(*existing_conflict, conflict);
                } else {
                    live_entries.push((key, conflict));
                }
            }

            if live_entries.len() >= 2 {
                return Err(DbError::execute(
                    SqlState::SerializationFailure,
                    "cannot alter primary key over a live HOT chain with differing key values; \
                     retry after the transaction ends or after VACUUM",
                ));
            }
            let Some((key, conflict)) = live_entries.into_iter().next() else {
                continue;
            };
            if !new_schema.primary_key.is_empty() {
                record_primary_key_candidate(&mut live_primary_key_rows, &key, conflict)?;
            }
            entries.push((key, root));
        }
        Ok(entries)
    }

    fn heap_identity_roots(&self, schema: &TableSchema) -> Result<Vec<RowLocation>> {
        let page_count = self.buffer_pool.page_count(schema.id)?;
        let mut roots = Vec::new();
        for page_num in 0..page_count {
            if self.buffer_pool.is_page_abandoned(schema.id, page_num) {
                continue;
            }
            let page_roots = {
                let guard = self.buffer_pool.read_page(schema.id, page_num)?;
                if !page::is_initialized(guard.data()) {
                    Vec::new()
                } else {
                    Self::heap_identity_roots_on_page(schema.id, page_num, guard.data())?
                }
            };
            roots.extend(page_roots);
        }
        Ok(roots)
    }

    fn heap_identity_roots_on_page(
        file_id: FileId,
        page_num: PageNum,
        data: &[u8; buffer::PAGE_SIZE],
    ) -> Result<Vec<RowLocation>> {
        let slot_count = page::next_slot(data)?;
        let mut is_member = HashSet::with_capacity(slot_count as usize);

        for slot in 0..slot_count {
            match page::slot_state(data, slot)? {
                page::LinePointer::Redirect(target) => {
                    is_member.insert(target);
                }
                page::LinePointer::Normal => {
                    let Some(bytes) = page::read_row(data, slot)? else {
                        return Err(storage_internal("normal line pointer has no tuple"));
                    };
                    let (_xmin, _xmax, t_ctid, infomask) = decode_mvcc_header(&bytes)?;
                    if infomask & crate::codec::HOT_UPDATED == 0 {
                        continue;
                    }
                    let (succ_page, succ_slot) = t_ctid;
                    if succ_page != page_num {
                        continue;
                    }
                    if let page::LinePointer::Normal = page::slot_state(data, succ_slot)? {
                        let Some(succ_bytes) = page::read_row(data, succ_slot)? else {
                            return Err(storage_internal("HOT successor is not a live tuple"));
                        };
                        let (_x, _xm, _t, succ_infomask) = decode_mvcc_header(&succ_bytes)?;
                        if succ_infomask & crate::codec::HEAP_ONLY != 0 {
                            is_member.insert(succ_slot);
                        }
                    }
                }
                page::LinePointer::Dead | page::LinePointer::Unused => {}
            }
        }

        let mut roots = Vec::new();
        for slot in 0..slot_count {
            match page::slot_state(data, slot)? {
                page::LinePointer::Normal => {
                    if is_member.contains(&slot) {
                        continue;
                    }
                    let Some(bytes) = page::read_row(data, slot)? else {
                        return Err(storage_internal("normal line pointer has no tuple"));
                    };
                    let (_xmin, _xmax, _t_ctid, infomask) = decode_mvcc_header(&bytes)?;
                    if infomask & crate::codec::HEAP_ONLY != 0 {
                        continue;
                    }
                    roots.push(RowLocation {
                        file_id,
                        page_num,
                        slot_num: slot,
                    });
                }
                page::LinePointer::Redirect(_) => roots.push(RowLocation {
                    file_id,
                    page_num,
                    slot_num: slot,
                }),
                page::LinePointer::Dead | page::LinePointer::Unused => {}
            }
        }
        Ok(roots)
    }

    fn rebuild_storage_identity(
        &self,
        schema: &TableSchema,
        entries: &[(Key, RowLocation)],
        wal_mode: RebuildWalMode,
    ) -> Result<()> {
        let rewrite_latch = self.identity_rewrite_latch(schema.id);
        let _rewrite_guard = rewrite_latch.write();
        let index_fid = primary_index_file_id(schema.storage_id);
        let latch = self.structural_latch(index_fid);
        let _guard = latch.lock();
        let btree = self.btree(index_fid);
        match wal_mode {
            RebuildWalMode::Logged(txn_id) => btree.reset_to_empty(txn_id)?,
            RebuildWalMode::Unlogged => btree.reset_to_empty_unlogged()?,
        }
        for (key, location) in entries {
            match wal_mode {
                RebuildWalMode::Logged(txn_id) => btree.insert(txn_id, key, location)?,
                RebuildWalMode::Unlogged => btree.insert_unlogged(key, location)?,
            }
        }
        Ok(())
    }

    /// Evenly-sampled initialized heap page images for dictionary training.
    /// Caller holds the exclusive guard, so the images are stable.
    pub fn sample_heap_pages(&self, schema: &TableSchema, cap: usize) -> Result<Vec<Vec<u8>>> {
        let file_id = heap_file_id(schema.storage_id);
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

    /// Bounded logical TEXT/BYTEA samples for TOAST dictionary training.
    ///
    /// Unlike `scan_range`, this walks heap pages directly so the caller's
    /// `max_samples`/`max_bytes` budget limits memory use on large tables. Rows
    /// are filtered with the normal MVCC visibility predicate and then routed
    /// through the same detoast materialization path used by user reads only when
    /// a compressed/external value's declared logical size fits the remaining byte
    /// budget; oversized values are skipped before reading hidden chunks or
    /// decompressing payloads.
    pub fn sample_toast_values(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        max_samples: usize,
        max_bytes: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let toastable_columns: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                matches!(column.data_type, DataType::Text | DataType::Bytea).then_some(index)
            })
            .collect();
        if max_samples == 0 || max_bytes == 0 || toastable_columns.is_empty() {
            return Ok(Vec::new());
        }

        let mut samples = Vec::new();
        let mut sampled_bytes = 0usize;
        let file_id = heap_file_id(schema.storage_id);
        let page_count = self.buffer_pool.page_count(file_id)?;
        let relations = self.capture_pagebacked_relation_snapshot()?;
        'pages: for page_num in 0..page_count {
            if self.buffer_pool.is_page_abandoned(file_id, page_num) {
                continue;
            }

            let page_rows = {
                let guard = self.buffer_pool.read_page(file_id, page_num)?;
                if !page::is_initialized(guard.data()) {
                    continue;
                }
                let slot_count = page::next_slot(guard.data())?;
                let mut rows = Vec::new();
                for slot in 0..slot_count {
                    if let Some(bytes) = page::read_row(guard.data(), slot)? {
                        rows.push(bytes);
                    }
                }
                rows
            };

            for bytes in page_rows {
                let (xmin, xmax, _t_ctid, infomask) = decode_mvcc_header(&bytes)?;
                if !is_visible(
                    xmin,
                    xmax,
                    infomask,
                    &ctx.snapshot,
                    ctx.live_txns.as_ref(),
                    self.txn_status_view(),
                ) {
                    continue;
                }
                let physical = decode_physical_row(schema, &bytes)?;
                for &column_index in &toastable_columns {
                    if samples.len() >= max_samples || sampled_bytes >= max_bytes {
                        break 'pages;
                    }
                    let remaining = max_bytes - sampled_bytes;
                    let physical_value = physical.values.get(column_index).ok_or_else(|| {
                        DbError::internal("decoded row is missing a toastable column")
                    })?;
                    if let Some(sample) = self.sample_toast_physical_value(
                        ctx,
                        &relations,
                        schema,
                        column_index,
                        physical_value,
                        remaining,
                    )? {
                        sampled_bytes += sample.len();
                        samples.push(sample);
                    }
                }
            }
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
        let mut files = vec![
            heap_file_id(schema.storage_id),
            primary_index_file_id(schema.storage_id),
        ];
        {
            let state = self.lock_state()?;
            files.extend(
                state
                    .indexes
                    .values()
                    .filter(|i| !i.dropped && i.schema.table == schema.id)
                    .map(|i| secondary_index_file_id(i.schema.storage_id)),
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

    pub fn prepare_truncate_table(
        &self,
        ctx: &StatementContext,
        plan: &TruncateTablePlan,
        update: &TruncateCatalogUpdate,
    ) -> Result<()> {
        validate_truncate_update_matches_plan(plan, update)?;
        let files = truncate_update_files(update);
        {
            let mut state = self.lock_state()?;
            validate_truncate_update_storage_ids_are_fresh(&state, update)?;
            self.append_wal(
                &state,
                ctx,
                WalRecordKind::TruncateTable {
                    table_id: plan.table_id,
                    new_table_storage_id: plan.new_table_storage_id,
                    new_toast_storage_id: plan.new_toast_storage_id,
                    new_index_storage_ids: plan.new_index_storage_ids.clone(),
                },
            )?;
            if ctx.txn_id != 0 {
                state
                    .rollback
                    .entry(ctx.txn_id)
                    .or_default()
                    .unpublished_files
                    .extend(files.iter().copied());
            }
        }

        self.register_table_compression(&update.table);
        self.btree(primary_index_file_id(update.table.storage_id))
            .create(ctx.txn_id)?;
        if let Some(toast) = &update.toast_table {
            self.register_table_compression(toast);
            self.btree(primary_index_file_id(toast.storage_id))
                .create(ctx.txn_id)?;
        }
        let index_config = index_compression_for(update.table.compression);
        for index in &update.indexes {
            self.compression
                .set_file_config(secondary_index_file_id(index.storage_id), index_config);
            self.secondary_btree(index).create(ctx.txn_id)?;
        }
        Ok(())
    }

    pub fn publish_truncate_table(&self, update: TruncateCatalogUpdate) -> Result<()> {
        self.publish_truncate_table_update(update)
    }

    pub(crate) fn apply_truncate_table_without_wal(
        &self,
        update: TruncateCatalogUpdate,
    ) -> Result<()> {
        self.publish_truncate_table_update(update)
    }

    fn publish_truncate_table_update(&self, update: TruncateCatalogUpdate) -> Result<()> {
        let mut state = self.lock_state()?;

        let old_table = state
            .tables
            .get(&update.table.id)
            .filter(|table| !table.dropped)
            .cloned()
            .ok_or_else(|| {
                storage_internal(format!(
                    "truncate target table {} is not installed",
                    update.table.id
                ))
            })?;
        if old_table.schema.relation_kind != RelationKind::User {
            return Err(storage_internal(format!(
                "truncate target table {} is not a user relation",
                update.table.id
            )));
        }
        if update.table.toast_table_id != old_table.schema.toast_table_id {
            return Err(storage_internal(format!(
                "truncate update for table {} does not preserve its TOAST relation link",
                update.table.id
            )));
        }
        validate_truncate_update_indexes_match_current(&state, &update)?;
        validate_truncate_update_storage_ids_are_fresh(&state, &update)?;

        let old_toast = match &update.toast_table {
            Some(toast) => {
                if old_table.schema.toast_table_id != Some(toast.id) {
                    return Err(storage_internal(format!(
                        "truncate update names TOAST table {} but target table {} references {:?}",
                        toast.id, update.table.id, old_table.schema.toast_table_id
                    )));
                }
                let old_toast = state
                    .tables
                    .get(&toast.id)
                    .filter(|table| !table.dropped)
                    .cloned()
                    .ok_or_else(|| {
                        storage_internal(format!(
                            "truncate TOAST table {} is not installed",
                            toast.id
                        ))
                    })?;
                if old_toast.schema.relation_kind
                    != (RelationKind::Toast {
                        base_table: update.table.id,
                    })
                {
                    return Err(storage_internal(format!(
                        "truncate TOAST table {} does not belong to target table {}",
                        toast.id, update.table.id
                    )));
                }
                Some(old_toast)
            }
            None => {
                if let Some(old_toast_id) = old_table.schema.toast_table_id {
                    return Err(storage_internal(format!(
                        "truncate update missing TOAST table {old_toast_id} for target table {}",
                        update.table.id
                    )));
                }
                None
            }
        };

        let mut old_indexes = Vec::with_capacity(update.indexes.len());
        for index in &update.indexes {
            let old_index = state
                .indexes
                .get(&index.id)
                .filter(|old| !old.dropped)
                .cloned()
                .ok_or_else(|| {
                    storage_internal(format!("truncate index {} is not installed", index.id))
                })?;
            if old_index.schema.table != update.table.id {
                return Err(storage_internal(format!(
                    "truncate index {} belongs to table {}, expected {}",
                    index.id, old_index.schema.table, update.table.id
                )));
            }
            old_indexes.push(old_index);
        }

        let mut retired = RetiredGeneration {
            files: Vec::new(),
            table_refs: Vec::new(),
            index_refs: Vec::new(),
        };
        retired.files.extend(table_files(&old_table.schema));
        retired.table_refs.push(Arc::downgrade(&old_table));
        if let Some(old_toast) = &old_toast {
            retired.files.extend(table_files(&old_toast.schema));
            retired.table_refs.push(Arc::downgrade(old_toast));
        }
        for old_index in &old_indexes {
            retired
                .files
                .push(secondary_index_file_id(old_index.schema.storage_id));
            retired.index_refs.push(Arc::downgrade(old_index));
        }

        self.register_table_compression(&update.table);
        if let Some(toast) = &update.toast_table {
            self.register_table_compression(toast);
        }
        let index_config = index_compression_for(update.table.compression);
        for index in &update.indexes {
            self.compression
                .set_file_config(secondary_index_file_id(index.storage_id), index_config);
        }

        state.tables.insert(
            update.table.id,
            Arc::new(TableGeneration {
                schema: update.table.clone(),
                dropped: false,
            }),
        );
        if let Some(toast) = &update.toast_table {
            state.tables.insert(
                toast.id,
                Arc::new(TableGeneration {
                    schema: toast.clone(),
                    dropped: false,
                }),
            );
            state
                .toast_next_value_ids
                .insert(toast.id, crate::toast::FIRST_TOAST_VALUE_ID);
        }
        for index in &update.indexes {
            state.indexes.insert(
                index.id,
                Arc::new(IndexGeneration {
                    schema: index.clone(),
                    dropped: false,
                }),
            );
        }
        let published_files: BTreeSet<FileId> =
            truncate_update_files(&update).into_iter().collect();
        for rollback in state.rollback.values_mut() {
            rollback
                .unpublished_files
                .retain(|file_id| !published_files.contains(file_id));
        }
        state.retired_generations.push_back(retired);
        bump_relation_epoch(&mut state);
        Ok(())
    }

    pub fn try_cleanup_retired_generations(&self) -> Result<usize> {
        let candidates = {
            let mut state = self.lock_state()?;
            let mut pending = VecDeque::new();
            let mut candidates = Vec::new();
            while let Some(retired) = state.retired_generations.pop_front() {
                if retired_generation_is_referenced(&retired) {
                    pending.push_back(retired);
                } else {
                    candidates.push(retired);
                }
            }
            state.retired_generations = pending;
            candidates
        };

        let mut cleaned = 0usize;
        let mut still_pending = VecDeque::new();
        for retired in candidates {
            let mut discardable = true;
            for file_id in &retired.files {
                if !self.buffer_pool.discard_file_if_unpinned(*file_id)? {
                    discardable = false;
                    break;
                }
            }
            if discardable {
                for file_id in &retired.files {
                    self.buffer_pool.remove_file(*file_id)?;
                }
                cleaned += 1;
            } else {
                still_pending.push_back(retired);
            }
        }
        if !still_pending.is_empty() {
            self.lock_state()?.retired_generations.extend(still_pending);
        }
        Ok(cleaned)
    }

    pub fn cleanup_orphan_files(&self) -> Result<usize> {
        let protected = self.protected_file_ids()?;
        let mut removed = 0usize;
        for file_id in self.buffer_pool.list_file_ids()? {
            if protected.contains(&file_id) {
                continue;
            }
            if self.buffer_pool.discard_file_if_unpinned(file_id)? {
                self.buffer_pool.remove_file(file_id)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn protected_file_ids(&self) -> Result<BTreeSet<FileId>> {
        let state = self.lock_state()?;
        let mut files = BTreeSet::new();
        for table in state.tables.values().filter(|table| !table.dropped) {
            files.extend(table_files(&table.schema));
        }
        for index in state.indexes.values().filter(|index| !index.dropped) {
            files.insert(secondary_index_file_id(index.schema.storage_id));
        }
        for retired in &state.retired_generations {
            files.extend(retired.files.iter().copied());
        }
        for rollback in state.rollback.values() {
            files.extend(rollback.unpublished_files.iter().copied());
            for table in rollback.tables.values().flatten() {
                files.extend(table_files(&table.schema));
            }
            for index in rollback.indexes.values().flatten() {
                files.insert(secondary_index_file_id(index.schema.storage_id));
            }
        }
        Ok(files)
    }

    fn remove_files(&self, files: Vec<FileId>) -> Result<()> {
        for file_id in files {
            if !self.buffer_pool.discard_file_if_unpinned(file_id)? {
                return Err(storage_internal(format!(
                    "cannot remove file {file_id} while it is pinned"
                )));
            }
            self.buffer_pool.remove_file(file_id)?;
        }
        Ok(())
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
    fn capture_relation_snapshot(&self) -> Result<Arc<dyn RelationSnapshot>> {
        Ok(Arc::new(self.capture_pagebacked_relation_snapshot()?))
    }

    fn insert(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        row: Row,
    ) -> Result<RowId> {
        let relations = pagebacked_relations(relations)?;
        self.ensure_current_generation_for_write(relations, table)?;
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let index_fid = table_handle.primary_index_file_id;

        // Write the new heap tuple first (under its own per-heap latch inside
        // `write_new_row`, released on return), THEN insert the table identity entry
        // atomically under its index latch. Writing the heap row before taking that
        // latch keeps the two structural latches disjoint (rule 1: never two at
        // once). A transiently orphaned heap tuple (if the uniqueness check below
        // fails) is invisible via CLOG once the txn aborts and reclaimed by VACUUM —
        // the same orphan-on-conflict handling `update` relies on.
        let row_bytes = self.prepare_row_for_storage(
            ctx,
            relations,
            &schema,
            &crate::codec::MvccHeader::fresh(ctx.txn_id, 0),
            &row,
        )?;
        let location = self.write_new_row_bytes(&schema, &row_bytes, ctx.txn_id)?;
        let key = storage_identity_key_for_row(&schema, &row, location)?;

        self.insert_storage_identity_entry(ctx, &schema, index_fid, &key, &location)?;

        for index in self.table_indexes(relations, table)? {
            let (entry_key, has_null) = secondary_index_key(&schema, &index, &row)?;
            self.insert_secondary_entry(ctx, &schema, &index, &entry_key, has_null, &location)?;
        }

        // SSI: this insert may complete an rw-antidependency with a concurrent
        // serializable reader of the table (or a point reader of this key) — the
        // phantom case (`docs/specs/ssi.md` §6). No-op for non-SERIALIZABLE writers; an
        // `Err` is the SSI `40001` victim, aborting this statement.
        let ssi_key = ssi_write_key_for_row(&schema, &row, &key)?;
        ctx.ssi_tracker.note_write(ctx.txn_id, table, &ssi_key)?;

        Ok(RowId {
            page_num: location.page_num,
            slot_num: location.slot_num,
        })
    }

    fn get(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<Option<Row>> {
        let relations = pagebacked_relations(relations)?;
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let index_fid = table_handle.primary_index_file_id;
        let locations = {
            let rewrite_latch = self.identity_rewrite_latch(table);
            let _rewrite_guard = rewrite_latch.read();
            self.btree(index_fid).scan_key(key)?
        };
        // The primary-key index may carry entries for several versions of this key
        // once versioning lands (B4); collect every candidate TID and return the
        // single one visible to this snapshot. Today there is one entry per key.
        for location in locations {
            if let Some((_resolved, row)) =
                self.read_visible_row(ctx, relations, &schema, location)?
            {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn delete(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<bool> {
        let relations = pagebacked_relations(relations)?;
        self.ensure_current_generation_for_write(relations, table)?;
        let Some(table_handle) = self.table_handle_opt(relations, table)? else {
            return Ok(false);
        };
        let schema = table_handle.schema.clone();
        let index_fid = table_handle.primary_index_file_id;
        let btree = self.btree(index_fid);
        // Locate the single version this statement's snapshot sees (the row the
        // executor matched). If none is visible the key was already deleted or is
        // absent, so the delete affects no row — preserve the no-op semantics.
        let visible = {
            let rewrite_latch = self.identity_rewrite_latch(table);
            let _rewrite_guard = rewrite_latch.read();
            self.locate_visible_version(&btree, key, &ctx.snapshot, &ctx.live_txns)?
        };
        let Some((location, infomask)) = visible else {
            return Ok(false);
        };
        let previous_row = self
            .read_visible_row(ctx, relations, &schema, location)?
            .map(|(_resolved, row)| row)
            .ok_or_else(|| storage_internal("visible row disappeared during delete"))?;
        let ssi_key = ssi_write_key_for_row(&schema, &previous_row, key)?;

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
        ctx.ssi_tracker.note_write(ctx.txn_id, table, &ssi_key)?;
        Ok(true)
    }

    fn update(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
    ) -> Result<bool> {
        let relations = pagebacked_relations(relations)?;
        self.ensure_current_generation_for_write(relations, table)?;
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let index_fid = table_handle.primary_index_file_id;
        let btree = self.btree(index_fid);
        // Locate the version this statement's snapshot sees (the row the executor
        // matched), NOT an arbitrary `search(key)` entry. The primary-key index may
        // carry an entry per version once versioning lands (and after a
        // delete-then-reinsert there are several entries for the key), so targeting
        // the *visible* version is what makes the right row the one updated. If none
        // is visible the key was already deleted or is absent, so the update affects
        // no row — preserve the no-op semantics.
        let visible = {
            let rewrite_latch = self.identity_rewrite_latch(table);
            let _rewrite_guard = rewrite_latch.read();
            self.locate_visible_version(&btree, key, &ctx.snapshot, &ctx.live_txns)?
        };
        let Some((previous_location, infomask)) = visible else {
            return Ok(false);
        };
        let previous_row = self
            .read_visible_row(ctx, relations, &schema, previous_location)?
            .map(|(_resolved, row)| row)
            .ok_or_else(|| storage_internal("visible row disappeared during update"))?;
        let ssi_keys = ssi_write_keys_for_update(&schema, &previous_row, &row, key)?;
        // HOT-update fast path (`docs/specs/mvcc.md` §10 Milestone H2). When BOTH (a)
        // no indexed column changed and (b) the new tuple fits on the predecessor's
        // own page, write the new version as a heap-only tuple on that page, chain the
        // predecessor to it, and insert NO index entries — the index keeps pointing at
        // the chain root, and H1's bounded `t_ctid` walk reaches the new version via
        // the `HOT_UPDATED → HEAP_ONLY` segment. TOAST-enabled tables use this path
        // only when the predecessor and successor both stay inline; external TOAST
        // pointers fall back to the fully-indexed path so chunk cleanup remains owned
        // by full VACUUM. When the predecessor's page is full, the H3 update-path
        // prune (under the heap latch, `ctx.gc_horizon` threaded in) tries to reclaim
        // same-page room first; only if it still cannot fit does it fall back.
        if let Some(result) = self.try_hot_update(HotUpdateRequest {
            ctx,
            relations,
            schema: &schema,
            table,
            previous_location,
            infomask,
            row: &row,
        })? {
            // SSI: a successful HOT update overwrote the row a concurrent serializable
            // reader may have read (`docs/specs/ssi.md` §6).
            if result {
                for ssi_key in &ssi_keys {
                    ctx.ssi_tracker.note_write(ctx.txn_id, table, ssi_key)?;
                }
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
        let row_bytes = self.prepare_row_for_storage(
            ctx,
            relations,
            &schema,
            &crate::codec::MvccHeader::fresh(ctx.txn_id, 0),
            &row,
        )?;
        let new_location = self.write_new_row_bytes(&schema, &row_bytes, ctx.txn_id)?;

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

        // New non-HOT versions get their own identity entry. For PK tables that is
        // the logical primary-key value; for no-PK tables it is the hidden heap key.
        // Old identity entries are retained until VACUUM, like every other MVCC
        // index entry.
        let replacement_key = storage_identity_key_for_row(&schema, &row, new_location)?;
        self.insert_storage_identity_entry(
            ctx,
            &schema,
            index_fid,
            &replacement_key,
            &new_location,
        )?;

        // A new per-version entry for the new tuple in *every* secondary index
        // (changed-column or not), pointing at `new_location`. Old entries are
        // retained. `insert_secondary_entry` enforces unique-secondary constraints
        // visibility-aware: an unchanged unique value does not self-conflict (the old
        // version is own-deleted), but a value colliding with a different live row
        // raises `UniqueViolation`.
        for index in self.table_indexes(relations, table)? {
            let (new_key, has_null) = secondary_index_key(&schema, &index, &row)?;
            self.insert_secondary_entry(ctx, &schema, &index, &new_key, has_null, &new_location)?;
        }

        // SSI: the non-HOT update overwrote the row a concurrent serializable reader
        // may have read (`docs/specs/ssi.md` §6).
        for ssi_key in &ssi_keys {
            ctx.ssi_tracker.note_write(ctx.txn_id, table, ssi_key)?;
        }
        Ok(true)
    }

    fn scan(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
    ) -> Result<Box<dyn RowIterator>> {
        <Self as StorageEngine>::scan_range(self, ctx, relations, table, &KeyRange::All)
    }

    fn scan_range(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let relations = pagebacked_relations(relations)?;
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let index_fid = table_handle.primary_index_file_id;
        let entries = {
            let rewrite_latch = self.identity_rewrite_latch(table);
            let _rewrite_guard = rewrite_latch.read();
            self.btree(index_fid).range(range)?
        };

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
            let Some((resolved, row)) = self.read_visible_row(ctx, relations, &schema, location)?
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
        relations: &dyn RelationSnapshot,
        table: TableId,
        index: IndexId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let relations = pagebacked_relations(relations)?;
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let index_handle = self.index_handle(relations, table, index)?;
        let index_schema = index_handle.schema.clone();

        // The secondary index points directly at heap TIDs (uniform with the
        // primary-key index), so a scan collects candidate TIDs from the index and
        // resolves each at the heap. Each TID is a (possibly HOT) root: a non-HOT
        // version is independently indexed and resolves to itself; a HEAP_ONLY
        // successor has no index entry and is reached only via its root's bounded
        // `t_ctid` walk (REDIRECT + chain in `read_visible_row`; `mvcc.md` §5.2, §10
        // Milestone H1). Because the walk stops at any independently-indexed
        // successor, a row is never yielded via two index entries.
        let entries = self.secondary_btree(&index_schema).range(range)?;
        let mut rows = Vec::with_capacity(entries.len());
        for (_entry_key, location) in entries {
            // Resolve to the visible version; an invisible chain (or a DEAD/absent
            // root line pointer) is skipped, not an error.
            let Some((resolved, row)) = self.read_visible_row(ctx, relations, &schema, location)?
            else {
                continue;
            };
            // Secondary entries point at the indexed root TID. For PK tables the
            // executor identity remains the logical primary key; for no-PK tables it is
            // the hidden heap key derived from that root TID. HOT chains preserve the
            // root identity; non-HOT versions have their own secondary entry and own
            // identity.
            let key = storage_identity_key_for_row(&schema, &row, location)?;
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
        let relation_metadata_changed = !rollback.tables.is_empty() || !rollback.indexes.is_empty();
        let mut unpublished_files = rollback.unpublished_files;
        let mut retired = RetiredGeneration {
            files: Vec::new(),
            table_refs: Vec::new(),
            index_refs: Vec::new(),
        };
        for (table_id, previous) in rollback.tables.into_iter().rev() {
            if let Some(current) = state.tables.get(&table_id).cloned() {
                retire_replaced_table_generation(&mut retired, &current, previous.as_ref());
            }
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
            if let Some(current) = state.indexes.get(&index_id).cloned() {
                retire_replaced_index_generation(&mut retired, &current, previous.as_ref());
            }
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
        if relation_metadata_changed {
            if !retired.files.is_empty() {
                let retired_files: BTreeSet<FileId> = retired.files.iter().copied().collect();
                unpublished_files.retain(|file_id| !retired_files.contains(file_id));
                state.retired_generations.push_back(retired);
            }
            bump_relation_epoch(&mut state);
        }
        drop(state);
        self.remove_files(unpublished_files)
    }

    fn commit_txn(&self, txn_id: u64) -> Result<()> {
        let mut state = self.lock_state()?;
        let Some(rollback) = state.rollback.remove(&txn_id) else {
            return Ok(());
        };
        let mut retired = RetiredGeneration {
            files: Vec::new(),
            table_refs: Vec::new(),
            index_refs: Vec::new(),
        };
        for (table_id, previous) in rollback.tables {
            let Some(previous) = previous else {
                continue;
            };
            if committed_change_retired_table(&state, table_id, &previous) {
                retired.files.extend(table_files(&previous.schema));
                retired.table_refs.push(Arc::downgrade(&previous));
            }
        }
        for (index_id, previous) in rollback.indexes {
            let Some(previous) = previous else {
                continue;
            };
            if committed_change_retired_index(&state, index_id, &previous) {
                retired
                    .files
                    .push(secondary_index_file_id(previous.schema.storage_id));
                retired.index_refs.push(Arc::downgrade(&previous));
            }
        }
        if !retired.files.is_empty() {
            state.retired_generations.push_back(retired);
        }
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
                Arc::new(TableGeneration {
                    schema: schema.clone(),
                    dropped: false,
                }),
            );
            if matches!(schema.relation_kind, RelationKind::Toast { .. }) {
                state
                    .toast_next_value_ids
                    .insert(schema.id, crate::toast::FIRST_TOAST_VALUE_ID);
            }
            bump_relation_epoch(&mut state);
        }
        // Register the heap/PK-index file configs before the tree's own pages
        // are created, so even its first metapage/root are encoded at rest per
        // the declared setting.
        self.register_table_compression(schema);
        // Create the empty on-disk index (metapage + root leaf). Its redo is
        // logged as full-page images, so recovery re-establishes it.
        self.btree(primary_index_file_id(schema.storage_id))
            .create(ctx.txn_id)
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
        let toast_table_id = live_toast_table_id(&state, table);
        self.append_wal(&state, ctx, WalRecordKind::DropTable { table })?;
        mark_table_dropped(&mut state, ctx.txn_id, table);
        if let Some(toast_table_id) = toast_table_id {
            mark_table_dropped(&mut state, ctx.txn_id, toast_table_id);
        }
        bump_relation_epoch(&mut state);
        Ok(())
    }

    fn create_index(
        &self,
        ctx: &StatementContext,
        schema: &IndexSchema,
        gc_horizon: u64,
    ) -> Result<()> {
        let relations = self.capture_pagebacked_relation_snapshot()?;
        let table_handle = self.table_handle(&relations, schema.table)?;
        let table_schema = table_handle.schema.clone();
        let pk_file_id = table_handle.primary_index_file_id;
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
            if ctx.txn_id != 0 {
                state
                    .rollback
                    .entry(ctx.txn_id)
                    .or_default()
                    .unpublished_files
                    .push(secondary_index_file_id(schema.storage_id));
            }
        }
        // The new secondary index's file config mirrors the OWNING table's
        // codec but never its dictionary (`compression.md` §4).
        self.compression.set_file_config(
            secondary_index_file_id(schema.storage_id),
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
        let secondary = self.secondary_btree(schema);
        secondary.create(ctx.txn_id)?;
        for (_pk, root) in self.btree(pk_file_id).range(&KeyRange::All)? {
            // The physically-present versions reachable from this chain root (the root
            // plus any heap-only HOT-chain members on its page), in chain order.
            let versions = self.collect_chain_versions(&table_schema, root)?;

            // A non-HOT root is its own one-element chain (no HOT successors). Index its
            // physical row only if it is not dead-to-all at the build horizon. Dead
            // rows cannot be visible to any snapshot and must be skipped before
            // detoasting, because aborted toasted parents may reference chunks that
            // are invisible through the hidden relation's snapshot-visible scan. The
            // broken-chain hazard cannot arise for a single-version chain. Use the
            // version `collect_chain_versions` resolved (which already followed a
            // REDIRECT root to its NORMAL target) rather than re-reading `root` — a
            // REDIRECT slot reads no bytes directly. A reclaimed (DEAD/UNUSED) root
            // resolves to no versions; nothing to index.
            if versions.len() <= 1 {
                if let Some((_loc, decoded)) = versions.first() {
                    if common::is_dead_to_all(
                        decoded.header.xmin,
                        decoded.header.xmax,
                        decoded.header.infomask,
                        gc_horizon,
                        self.txn_status_view(),
                    ) {
                        continue;
                    }
                    let row = self.materialize_physical_row(
                        ctx,
                        &relations,
                        &table_schema,
                        decoded.clone(),
                    )?;
                    let (key, has_null) = secondary_index_key(&table_schema, schema, &row)?;
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
                    decoded.header.xmin,
                    decoded.header.xmax,
                    decoded.header.infomask,
                    gc_horizon,
                    self.txn_status_view(),
                ) {
                    continue;
                }
                let row =
                    self.materialize_physical_row(ctx, &relations, &table_schema, decoded.clone())?;
                let (new_key, has_null) = secondary_index_key(&table_schema, schema, &row)?;
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
        let mut state = self.lock_state()?;
        state.indexes.insert(
            schema.id,
            Arc::new(IndexGeneration {
                schema: schema.clone(),
                dropped: false,
            }),
        );
        if let Some(rollback) = state.rollback.get_mut(&ctx.txn_id) {
            let file_id = secondary_index_file_id(schema.storage_id);
            rollback
                .unpublished_files
                .retain(|candidate| *candidate != file_id);
        }
        bump_relation_epoch(&mut state);
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
        let schema = state
            .indexes
            .get(&index)
            .ok_or_else(|| undefined_index(index))?
            .schema
            .clone();
        // V1 leaves the index pages in place (no physical reclaim), like drop_table.
        state.indexes.insert(
            index,
            Arc::new(IndexGeneration {
                schema,
                dropped: true,
            }),
        );
        bump_relation_epoch(&mut state);
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

fn table_files(schema: &TableSchema) -> [FileId; 2] {
    [
        heap_file_id(schema.storage_id),
        primary_index_file_id(schema.storage_id),
    ]
}

fn truncate_update_files(update: &TruncateCatalogUpdate) -> Vec<FileId> {
    let mut files = Vec::new();
    files.extend(table_files(&update.table));
    if let Some(toast) = &update.toast_table {
        files.extend(table_files(toast));
    }
    files.extend(
        update
            .indexes
            .iter()
            .map(|index| secondary_index_file_id(index.storage_id)),
    );
    files
}

fn validate_truncate_update_matches_plan(
    plan: &TruncateTablePlan,
    update: &TruncateCatalogUpdate,
) -> Result<()> {
    if plan.table_id != update.table.id {
        return Err(storage_internal(format!(
            "truncate plan targets table {} but update targets table {}",
            plan.table_id, update.table.id
        )));
    }
    if plan.new_table_storage_id != update.table.storage_id {
        return Err(storage_internal(format!(
            "truncate plan table storage id {} does not match update storage id {}",
            plan.new_table_storage_id, update.table.storage_id
        )));
    }

    let update_toast_storage = update
        .toast_table
        .as_ref()
        .map(|toast| (toast.id, toast.storage_id));
    if plan.new_toast_storage_id != update_toast_storage {
        return Err(storage_internal(format!(
            "truncate plan TOAST storage {:?} does not match update TOAST storage {:?}",
            plan.new_toast_storage_id, update_toast_storage
        )));
    }

    let mut planned_indexes = BTreeMap::new();
    for (index_id, storage_id) in &plan.new_index_storage_ids {
        if planned_indexes.insert(*index_id, *storage_id).is_some() {
            return Err(storage_internal(format!(
                "truncate plan repeats index {index_id}"
            )));
        }
    }
    let mut update_indexes = BTreeMap::new();
    for index in &update.indexes {
        if index.table != update.table.id {
            return Err(storage_internal(format!(
                "truncate update index {} belongs to table {}, expected {}",
                index.id, index.table, update.table.id
            )));
        }
        if update_indexes.insert(index.id, index.storage_id).is_some() {
            return Err(storage_internal(format!(
                "truncate update repeats index {}",
                index.id
            )));
        }
    }
    if planned_indexes != update_indexes {
        return Err(storage_internal(format!(
            "truncate plan indexes {:?} do not match update indexes {:?}",
            planned_indexes, update_indexes
        )));
    }

    Ok(())
}

fn validate_truncate_update_indexes_match_current(
    state: &StorageState,
    update: &TruncateCatalogUpdate,
) -> Result<()> {
    let mut update_ids = BTreeSet::new();
    for index in &update.indexes {
        if index.table != update.table.id {
            return Err(storage_internal(format!(
                "truncate update index {} belongs to table {}, expected {}",
                index.id, index.table, update.table.id
            )));
        }
        if !update_ids.insert(index.id) {
            return Err(storage_internal(format!(
                "truncate update repeats index {}",
                index.id
            )));
        }
    }

    let current_ids = state
        .indexes
        .values()
        .filter(|index| !index.dropped && index.schema.table == update.table.id)
        .map(|index| index.schema.id)
        .collect::<BTreeSet<_>>();
    if current_ids != update_ids {
        return Err(storage_internal(format!(
            "truncate update index set {:?} does not match current index set {:?} for table {}",
            update_ids, current_ids, update.table.id
        )));
    }

    Ok(())
}

fn validate_truncate_update_storage_ids_are_fresh(
    state: &StorageState,
    update: &TruncateCatalogUpdate,
) -> Result<()> {
    let update_ids = truncate_update_storage_ids(update);
    for table in state.tables.values().filter(|table| !table.dropped) {
        if update_ids.contains(&table.schema.storage_id) {
            return Err(storage_internal(format!(
                "truncate update storage id {} collides with table {}",
                table.schema.storage_id, table.schema.name
            )));
        }
    }
    for index in state.indexes.values().filter(|index| !index.dropped) {
        if update_ids.contains(&index.schema.storage_id) {
            return Err(storage_internal(format!(
                "truncate update storage id {} collides with index {}",
                index.schema.storage_id, index.schema.name
            )));
        }
    }
    Ok(())
}

fn truncate_update_storage_ids(update: &TruncateCatalogUpdate) -> BTreeSet<FileId> {
    let mut ids = BTreeSet::new();
    ids.insert(update.table.storage_id);
    if let Some(toast) = &update.toast_table {
        ids.insert(toast.storage_id);
    }
    ids.extend(update.indexes.iter().map(|index| index.storage_id));
    ids
}

fn retired_generation_is_referenced(retired: &RetiredGeneration) -> bool {
    retired
        .table_refs
        .iter()
        .any(|generation| generation.strong_count() > 0)
        || retired
            .index_refs
            .iter()
            .any(|generation| generation.strong_count() > 0)
}

fn retire_replaced_table_generation(
    retired: &mut RetiredGeneration,
    current: &Arc<TableGeneration>,
    replacement: Option<&Arc<TableGeneration>>,
) {
    if replacement.is_some_and(|previous| previous.schema.storage_id == current.schema.storage_id) {
        return;
    }
    retired.files.extend(table_files(&current.schema));
    retired.table_refs.push(Arc::downgrade(current));
}

fn retire_replaced_index_generation(
    retired: &mut RetiredGeneration,
    current: &Arc<IndexGeneration>,
    replacement: Option<&Arc<IndexGeneration>>,
) {
    if replacement.is_some_and(|previous| previous.schema.storage_id == current.schema.storage_id) {
        return;
    }
    retired
        .files
        .push(secondary_index_file_id(current.schema.storage_id));
    retired.index_refs.push(Arc::downgrade(current));
}

fn committed_change_retired_table(
    state: &StorageState,
    table_id: TableId,
    previous: &Arc<TableGeneration>,
) -> bool {
    match state.tables.get(&table_id) {
        Some(current) => current.dropped || current.schema.storage_id != previous.schema.storage_id,
        None => true,
    }
}

fn committed_change_retired_index(
    state: &StorageState,
    index_id: IndexId,
    previous: &Arc<IndexGeneration>,
) -> bool {
    match state.indexes.get(&index_id) {
        Some(current) => current.dropped || current.schema.storage_id != previous.schema.storage_id,
        None => true,
    }
}

fn storage_key_for_location(location: RowLocation) -> Key {
    let page = i64::from(location.page_num);
    let slot = i64::from(location.slot_num);
    Key(vec![Value::Integer((page << 16) | slot)])
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

fn row_key_for_columns(
    table: &TableSchema,
    columns: &[ColumnId],
    row: &Row,
) -> Result<(Key, bool)> {
    let mut values = Vec::with_capacity(columns.len());
    let mut has_null = false;
    for column_id in columns {
        let value = column_value(table, row, *column_id)?;
        has_null |= matches!(value, Value::Null);
        values.push(value);
    }
    Ok((Key(values), has_null))
}

fn storage_identity_key_for_row(
    schema: &TableSchema,
    row: &Row,
    location: RowLocation,
) -> Result<Key> {
    if schema.primary_key.is_empty() {
        return Ok(storage_key_for_location(location));
    }
    primary_key_for_row(schema, row)
}

fn primary_key_for_row(schema: &TableSchema, row: &Row) -> Result<Key> {
    let (key, has_null) = row_key_for_columns(schema, &schema.primary_key, row)?;
    if has_null {
        return Err(DbError::storage(
            SqlState::NotNullViolation,
            "primary key column cannot be NULL",
        ));
    }
    Ok(key)
}

fn ssi_write_key_for_row(schema: &TableSchema, row: &Row, fallback: &Key) -> Result<Key> {
    if schema.primary_key.is_empty() {
        Ok(fallback.clone())
    } else {
        primary_key_for_row(schema, row)
    }
}

fn ssi_write_keys_for_update(
    schema: &TableSchema,
    previous_row: &Row,
    new_row: &Row,
    fallback: &Key,
) -> Result<Vec<Key>> {
    if schema.primary_key.is_empty() {
        return Ok(vec![fallback.clone()]);
    }
    let previous_key = ssi_write_key_for_row(schema, previous_row, fallback)?;
    let new_key = ssi_write_key_for_row(schema, new_row, fallback)?;
    if previous_key == new_key {
        Ok(vec![previous_key])
    } else {
        Ok(vec![previous_key, new_key])
    }
}

/// The secondary-index B-tree key for `row`: just the encoded indexed column(s).
/// The primary key is no longer embedded — duplicate secondary keys are
/// disambiguated by the heap TID in the tree's `(key, tid)` ordering. Returns the
/// key together with whether any indexed value is NULL, so the unique-constraint
/// probe can skip NULL keys (SQL treats NULLs as distinct, so NULL never
/// participates in a unique constraint; distinct NULL rows coexist via their
/// differing TIDs).
fn secondary_index_key(table: &TableSchema, index: &IndexSchema, row: &Row) -> Result<(Key, bool)> {
    row_key_for_columns(table, &index.columns, row)
}

fn pagebacked_relations(relations: &dyn RelationSnapshot) -> Result<&PageBackedRelationSnapshot> {
    relations
        .as_any()
        .downcast_ref::<PageBackedRelationSnapshot>()
        .ok_or_else(|| storage_internal("relation snapshot belongs to a different storage engine"))
}

fn bump_relation_epoch(state: &mut StorageState) {
    state.relation_epoch = state.relation_epoch.wrapping_add(1);
}

fn live_table(
    tables: &BTreeMap<TableId, Arc<TableGeneration>>,
    table: TableId,
) -> Result<&TableGeneration> {
    let table_state = tables.get(&table).ok_or_else(|| undefined_table(table))?;
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

fn live_toast_table_id(state: &StorageState, table: TableId) -> Option<TableId> {
    let table_state = state.tables.get(&table)?;
    if table_state.dropped || table_state.schema.relation_kind != RelationKind::User {
        return None;
    }
    table_state.schema.toast_table_id
}

fn toast_chunk_parts(row: &Row) -> Result<(u64, i64, Vec<u8>)> {
    let value_id = match row.values.first() {
        Some(Value::Integer(value)) if *value > 0 => *value as u64,
        Some(Value::Integer(value)) => {
            return Err(crate::toast::toast_corruption(format!(
                "TOAST chunk has invalid value_id {value}"
            )));
        }
        Some(_) => {
            return Err(crate::toast::toast_corruption(
                "TOAST chunk value_id is not an integer",
            ));
        }
        None => {
            return Err(crate::toast::toast_corruption(
                "TOAST chunk is missing value_id",
            ));
        }
    };
    let seq = match row.values.get(1) {
        Some(Value::Integer(seq)) if *seq >= 0 => *seq,
        Some(Value::Integer(seq)) => {
            return Err(crate::toast::toast_corruption(format!(
                "TOAST chunk has negative seq {seq}"
            )));
        }
        Some(_) => {
            return Err(crate::toast::toast_corruption(
                "TOAST chunk seq is not an integer",
            ));
        }
        None => return Err(crate::toast::toast_corruption("TOAST chunk is missing seq")),
    };
    let data = match row.values.get(2) {
        Some(Value::Bytes(data)) => data.clone(),
        Some(_) => {
            return Err(crate::toast::toast_corruption(
                "TOAST chunk data is not BYTEA",
            ));
        }
        None => {
            return Err(crate::toast::toast_corruption(
                "TOAST chunk is missing data",
            ));
        }
    };
    Ok((value_id, seq, data))
}

fn toast_value_key_prefix(value_id: u64) -> Result<Key> {
    let value_id = i64::try_from(value_id).map_err(|_| {
        crate::toast::toast_corruption(format!("TOAST value_id {value_id} is invalid"))
    })?;
    if value_id <= 0 {
        return Err(crate::toast::toast_corruption(format!(
            "TOAST value_id {value_id} is invalid"
        )));
    }
    Ok(Key(vec![Value::Integer(value_id)]))
}

fn toast_chunk_key(value_id: u64, seq: i64) -> Result<Key> {
    let mut values = toast_value_key_prefix(value_id)?.0;
    values.push(Value::Integer(seq));
    Ok(Key(values))
}

fn mark_table_dropped(state: &mut StorageState, txn_id: u64, table: TableId) {
    record_table_before(state, txn_id, table);
    let is_toast_relation = state.tables.get(&table).is_some_and(|table_state| {
        matches!(table_state.schema.relation_kind, RelationKind::Toast { .. })
    });
    if is_toast_relation {
        state.toast_next_value_ids.remove(&table);
    }
    if let Some(schema) = state.tables.get(&table).map(|table| table.schema.clone()) {
        // V1 leaves heap and index pages in place (no physical reclaim).
        state.tables.insert(
            table,
            Arc::new(TableGeneration {
                schema,
                dropped: true,
            }),
        );
    }
    // Cascade to secondary indexes, mirroring the catalog's drop-table cascade so
    // the two metadata layers stay consistent.
    mark_table_indexes_dropped(state, txn_id, table);
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
        if let Some(schema) = state
            .indexes
            .get(&index_id)
            .map(|index| index.schema.clone())
        {
            state.indexes.insert(
                index_id,
                Arc::new(IndexGeneration {
                    schema,
                    dropped: true,
                }),
            );
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

fn strongest_unique_conflict(left: UniqueConflict, right: UniqueConflict) -> UniqueConflict {
    match (left, right) {
        (UniqueConflict::Violation, _) | (_, UniqueConflict::Violation) => {
            UniqueConflict::Violation
        }
        (UniqueConflict::WouldBlock(blocker), _) | (_, UniqueConflict::WouldBlock(blocker)) => {
            UniqueConflict::WouldBlock(blocker)
        }
        (UniqueConflict::None, UniqueConflict::None) => UniqueConflict::None,
    }
}

fn record_primary_key_candidate(
    live_primary_key_rows: &mut HashSet<Key>,
    key: &Key,
    conflict: UniqueConflict,
) -> Result<()> {
    match conflict {
        UniqueConflict::None => Ok(()),
        UniqueConflict::Violation => {
            if live_primary_key_rows.insert(key.clone()) {
                Ok(())
            } else {
                Err(duplicate_primary_key())
            }
        }
        UniqueConflict::WouldBlock(blocker) => Err(DbError::execute(
            SqlState::SerializationFailure,
            format!(
                "cannot alter primary key while transaction {blocker} may hold a conflicting key; retry after it ends"
            ),
        )),
    }
}

fn duplicate_primary_key() -> DbError {
    DbError::storage(
        SqlState::UniqueViolation,
        "duplicate key value violates primary key",
    )
}

fn relation_generation_write_conflict(table: TableId) -> DbError {
    DbError::storage(
        SqlState::SerializationFailure,
        format!(
            "could not serialize write to table {table} because its relation generation changed"
        ),
    )
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

#[cfg(test)]
mod truncate_tests;
