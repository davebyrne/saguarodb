use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use buffer::{BufferPool, PageWriteGuard};
use common::{
    ColumnId, ColumnInfo, DbError, FileId, IndexId, IndexSchema, Key, KeyRange, Lsn, PageNum,
    Result, Row, RowId, Snapshot, SqlState, StatementContext, StoredRow, TableId, TableSchema,
    TxnStatusView, Value, WriteConflict, is_visible, version_conflicts, write_conflict,
};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
        })
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
    /// via the shared visibility-aware [`Self::unique_conflict_exists`] check (it
    /// conflicts only with an alive-or-potentially-alive version; dead/aborted
    /// versions are ignored). A NULL indexed value never participates in a unique
    /// constraint (SQL treats NULLs as distinct), so the check is skipped when
    /// `has_null`; distinct NULL rows coexist because their heap TIDs differ.
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
        if index.unique
            && !has_null
            && self.unique_conflict_exists(&secondary, entry_key, table_schema, ctx.txn_id)?
        {
            return Err(duplicate_unique_index(&index.name));
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
            let slot_num = page::next_slot(guard.data())?;
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
            let produced = page::insert_row(guard.data_mut(), row_bytes)?;
            debug_assert_eq!(produced, slot_num);
            page::set_page_lsn(guard.data_mut(), lsn);
            Ok(produced)
        }
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
    /// the latch, next to the stamp. Under the current serialized-writer model the
    /// located version's `xmax` is `INVALID_XID` (or this txn's own), so the check is
    /// a runtime no-op; it becomes load-bearing once writers run concurrently (E2b).
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

    /// Read the row at `location` only if it is **visible** to `snapshot` from
    /// `current_txn` (`docs/specs/mvcc.md` §6). Decodes the v2 tuple header
    /// (`xmin`/`xmax`/`infomask`) and applies [`is_visible`] against the CLOG-backed
    /// status view; an invisible version yields `None` and is skipped by the caller.
    /// A missing line pointer (DEAD/UNUSED) likewise yields `None` — an index entry
    /// landing on an absent or invisible tuple is skipped, never an error (the
    /// forward-looking hook for B4's retained index entries). Under the degenerate
    /// autocommit snapshot every committed row and own write is visible, so this
    /// filters nothing.
    fn read_visible_row(
        &self,
        schema: &TableSchema,
        location: RowLocation,
        snapshot: &Snapshot,
        current_txn: u64,
    ) -> Result<Option<Row>> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
            return Ok(None);
        };
        let decoded = decode_row(schema, &bytes)?;
        if !is_visible(
            decoded.xmin,
            decoded.xmax,
            decoded.infomask,
            snapshot,
            current_txn,
            self.txn_status_view(),
        ) {
            return Ok(None);
        }
        Ok(Some(decoded.row))
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
            let readable = self
                .buffer_pool
                .read_page(location.file_id, location.page_num)?;
            let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
                continue;
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
                return Ok(Some((location, decoded.infomask)));
            }
        }
        Ok(None)
    }

    /// Whether any existing version indexed under `key` in `index_btree` **conflicts**
    /// with a unique-constraint insert by `current_txn` — the shared,
    /// visibility-aware uniqueness check for the primary-key index and unique
    /// secondary indexes (`docs/specs/mvcc.md` §6/§7.3). It replaces the temporary
    /// presence-probes (B2 commits 3–4): "any entry for the key" became "any
    /// *alive-or-potentially-alive* version for the key".
    ///
    /// This is a **liveness ("dirty") check, not a snapshot read**: it consults the
    /// CLOG (`TxnStatusView`) + the tuple's `infomask` hint bits — never a
    /// [`Snapshot`] — so it sees concurrently in-flight and already-committed state,
    /// not just what `current_txn`'s snapshot would observe. Each candidate TID from
    /// `scan_key` is read at the *physical* tuple header (NOT via
    /// [`Self::read_visible_row`], which would wrongly hide non-visible-but-alive
    /// versions); a DEAD/UNUSED line pointer (`read_row` ⇒ `None`) is a reclaimed
    /// slot and contributes no conflict. The per-candidate decision is
    /// [`common::version_conflicts`]: a creator-aborted or committed-deleted (incl.
    /// deleted-by-me) version is dead and ignored; anything else conflicts.
    ///
    /// While the engine is single-version every index entry is a committed,
    /// non-deleted tuple, so this returns `true` exactly when the old presence-probe
    /// did — existing uniqueness behavior is unchanged. It becomes load-bearing once
    /// versioning (B4 commits 8–9) starts stamping `xmax`/writing aborted versions.
    fn unique_conflict_exists(
        &self,
        index_btree: &BTree<'_, RowLocation>,
        key: &Key,
        schema: &TableSchema,
        current_txn: u64,
    ) -> Result<bool> {
        let status = self.txn_status_view();
        for location in index_btree.scan_key(key)? {
            let readable = self
                .buffer_pool
                .read_page(location.file_id, location.page_num)?;
            let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
                // DEAD/UNUSED line pointer: the slot was reclaimed; no conflict.
                continue;
            };
            let decoded = decode_row(schema, &bytes)?;
            if version_conflicts(
                decoded.xmin,
                decoded.xmax,
                decoded.infomask,
                current_txn,
                status,
            ) {
                return Ok(true);
            }
        }
        Ok(false)
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
}

impl StorageEngine for PageBackedStorageEngine {
    fn insert(&self, ctx: &StatementContext, table: TableId, row: Row) -> Result<RowId> {
        let (schema, index_fid) = self.table_handle(table)?;
        let key = key_for_row(&schema, &row)?;
        let btree = self.btree(index_fid);
        // Visibility-aware primary-key uniqueness: the multi-entry tree no longer
        // rejects duplicate keys structurally, so reject only when an
        // alive-or-potentially-alive version already holds the key (dead/aborted
        // versions do not block a re-insert). Single-version today ⇒ behaves like a
        // presence check; correct once versioning (B4) lands.
        if self.unique_conflict_exists(&btree, &key, &schema, ctx.txn_id)? {
            return Err(DbError::storage(
                SqlState::UniqueViolation,
                "duplicate primary key",
            ));
        }

        let location = self.write_new_row(&schema, &row, ctx.txn_id)?;
        btree.insert(ctx.txn_id, &key, &location)?;

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
            if let Some(row) =
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
        // `xmax = ctx.txn_id` makes `unique_conflict_exists` treat it as own-deleted
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

        // Primary-key entry for the new version. The key is unchanged (a PK change is
        // rejected above), so this adds a second `(key, new_tid)` entry alongside the
        // retained old one. The uniqueness check now sees the old version as
        // own-deleted, so the unchanged PK does not falsely self-conflict.
        if self.unique_conflict_exists(&btree, key, &schema, ctx.txn_id)? {
            return Err(DbError::storage(
                SqlState::UniqueViolation,
                "duplicate primary key",
            ));
        }
        btree.insert(ctx.txn_id, key, &new_location)?;

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
            // Visibility-check the candidate TID at the heap; an invisible version
            // (or an absent line pointer) is skipped, not returned or errored.
            let Some(row) = self.read_visible_row(&schema, location, &ctx.snapshot, ctx.txn_id)?
            else {
                continue;
            };
            rows.push(StoredRow {
                row_id: RowId {
                    page_num: location.page_num,
                    slot_num: location.slot_num,
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
        // reads the heap at each — no primary-key indirection, and no walking the
        // `t_ctid` chain (every version is independently indexed; `mvcc.md` §6,
        // Appendix A "Reads do not walk t_ctid").
        let entries = self.secondary_btree(index).range(range)?;
        let mut rows = Vec::with_capacity(entries.len());
        for (_entry_key, location) in entries {
            // Visibility-check the candidate TID at the heap. An index entry whose
            // tuple is invisible to this snapshot (or whose line pointer is
            // DEAD/absent) is skipped, not an error — the forward-looking hook for
            // B4's retained per-version index entries.
            let Some(row) = self.read_visible_row(&schema, location, &ctx.snapshot, ctx.txn_id)?
            else {
                continue;
            };
            // The row's primary key is recovered from the heap row, preserving the
            // `StoredRow.key` semantics callers relied on under secondary→PK.
            let key = key_for_row(&schema, &row)?;
            rows.push(StoredRow {
                row_id: RowId {
                    page_num: location.page_num,
                    slot_num: location.slot_num,
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

    fn create_index(&self, ctx: &StatementContext, schema: &IndexSchema) -> Result<()> {
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
        // backfill it from the live rows via the primary-key index. Each secondary
        // entry points directly at the heap TID (uniform with the primary key).
        let secondary = self.secondary_btree(schema.id);
        secondary.create(ctx.txn_id)?;
        for (_pk, location) in self.btree(pk_file_id).range(&KeyRange::All)? {
            let row = self
                .read_location(&table_schema, location)?
                .ok_or_else(|| {
                    storage_internal("primary-key index points to a dead row during index backfill")
                })?;
            let (key, has_null) = secondary_index_key(&table_schema, schema, &row)?;
            self.insert_secondary_entry(ctx, &table_schema, schema, &key, has_null, &location)?;
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

fn storage_internal(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod visibility_tests {
    use std::sync::Arc;

    use buffer::{BufferPool, MemoryBufferPool, PageStore};
    use common::{
        ColumnDef, DataType, IndexSchema, Key, KeyRange, PageFlushInfo, Row, RowId, Snapshot,
        StatementContext, TableSchema, Value,
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
            .create_index(&builder, &name_index())
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
        fixture.engine.create_index(&setup, &unique_name).unwrap();
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
        fixture.engine.create_index(&setup, &name_index()).unwrap();
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
        fixture.engine.create_index(&setup, &name_idx).unwrap();
        fixture.engine.create_index(&setup, &id_idx).unwrap();
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
        fixture.engine.create_index(&setup, &unique_name).unwrap();
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
}
