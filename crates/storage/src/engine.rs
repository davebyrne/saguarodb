use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use buffer::{BufferPool, PageWriteGuard};
use common::{
    ColumnId, ColumnInfo, DbError, FileId, IndexId, IndexSchema, Key, KeyRange, Lsn, PageNum,
    Result, Row, RowId, Snapshot, SqlState, StatementContext, StoredRow, TableId, TableSchema,
    TxnStatusView, Value, is_visible,
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
    /// `(key, tid)` order. A unique index presence-probes its key first so a
    /// duplicate non-NULL indexed value is rejected. A NULL indexed value never
    /// participates in a unique constraint (SQL treats NULLs as distinct), so the
    /// probe is skipped when `has_null`; distinct NULL rows coexist because their
    /// heap TIDs differ. This presence-probe is TEMPORARY: Milestone B commit 7
    /// replaces it with a visibility-aware uniqueness check.
    fn insert_secondary_entry(
        &self,
        ctx: &StatementContext,
        index: &IndexSchema,
        entry_key: &Key,
        has_null: bool,
        location: &RowLocation,
    ) -> Result<()> {
        let secondary = self.secondary_btree(index.id);
        if index.unique && !has_null && !secondary.scan_key(entry_key)?.is_empty() {
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

    /// Mark a row dead and log its redo record (full-page image on first touch
    /// since the last checkpoint, else a `HeapDelete` delta).
    fn delete_row_logged(&self, location: RowLocation, txn_id: u64) -> Result<bool> {
        let mut guard = self
            .buffer_pool
            .write_page(location.file_id, location.page_num, txn_id)?;
        if guard.take_needs_fpi() {
            let deleted = page::delete_row(guard.data_mut(), location.slot_num)?;
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
            Ok(deleted)
        } else {
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::HeapDelete {
                    file_id: location.file_id,
                    page_num: location.page_num,
                    slot: location.slot_num,
                },
            })?;
            let deleted = page::delete_row(guard.data_mut(), location.slot_num)?;
            page::set_page_lsn(guard.data_mut(), lsn);
            Ok(deleted)
        }
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
        // TEMPORARY presence-probe for primary-key uniqueness: the multi-entry
        // tree no longer rejects duplicate keys structurally, so the engine
        // checks for an existing entry first. Milestone B commit 7 replaces this
        // with a visibility-aware uniqueness check.
        if !btree.scan_key(&key)?.is_empty() {
            return Err(DbError::storage(
                SqlState::UniqueViolation,
                "duplicate primary key",
            ));
        }

        let location = self.write_new_row(&schema, &row, ctx.txn_id)?;
        btree.insert(ctx.txn_id, &key, &location)?;

        for index in self.table_indexes(table)? {
            let (entry_key, has_null) = secondary_index_key(&schema, &index, &row)?;
            self.insert_secondary_entry(ctx, &index, &entry_key, has_null, &location)?;
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
        let Some(location) = btree.search(key)? else {
            return Ok(false);
        };

        let indexes = self.table_indexes(table)?;
        if !indexes.is_empty() {
            let row = self.read_location(&schema, location)?.ok_or_else(|| {
                storage_internal("primary-key index points to a dead row during delete")
            })?;
            for index in &indexes {
                let (entry_key, _has_null) = secondary_index_key(&schema, index, &row)?;
                // The secondary value is the heap TID; remove the specific
                // (entry_key, location) entry.
                self.secondary_btree(index.id)
                    .remove(ctx.txn_id, &entry_key, &location)?;
            }
        }

        self.delete_row_logged(location, ctx.txn_id)?;
        btree.remove(ctx.txn_id, key, &location)?;
        Ok(true)
    }

    fn update(&self, ctx: &StatementContext, table: TableId, key: &Key, row: Row) -> Result<bool> {
        let (schema, index_fid) = self.table_handle(table)?;
        let btree = self.btree(index_fid);
        let Some(previous_location) = btree.search(key)? else {
            return Ok(false);
        };
        let replacement_key = key_for_row(&schema, &row)?;
        if &replacement_key != key {
            return Err(DbError::execute(
                SqlState::DatatypeMismatch,
                "primary key updates are not supported",
            ));
        }

        let indexes = self.table_indexes(table)?;
        let previous_row = if indexes.is_empty() {
            None
        } else {
            Some(
                self.read_location(&schema, previous_location)?
                    .ok_or_else(|| {
                        storage_internal("primary-key index points to a dead row during update")
                    })?,
            )
        };

        self.delete_row_logged(previous_location, ctx.txn_id)?;
        let new_location = self.write_new_row(&schema, &row, ctx.txn_id)?;
        // The primary key is unchanged (rejected above otherwise), so move its
        // single index entry to the new heap location: remove the old (key, loc)
        // and insert (key, new_loc). Versioning UPDATE arrives in Milestone B
        // commit 9; for now there is still one tid per key.
        btree.remove(ctx.txn_id, key, &previous_location)?;
        btree.insert(ctx.txn_id, key, &new_location)?;

        if let Some(previous_row) = previous_row {
            // Remove every old entry before inserting the new ones, so a unique
            // index whose value is unchanged does not see a false duplicate. The
            // old entries point at `previous_location`; the new ones at
            // `new_location` (the row relocated within the heap).
            for index in &indexes {
                let (old_key, _has_null) = secondary_index_key(&schema, index, &previous_row)?;
                self.secondary_btree(index.id)
                    .remove(ctx.txn_id, &old_key, &previous_location)?;
            }
            for index in &indexes {
                let (new_key, has_null) = secondary_index_key(&schema, index, &row)?;
                self.insert_secondary_entry(ctx, index, &new_key, has_null, &new_location)?;
            }
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
        // Index and heap page changes are rolled back by the buffer pool's
        // before-images; storage only restores its own table metadata.
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
            self.insert_secondary_entry(ctx, schema, &key, has_null, &location)?;
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
        ColumnDef, DataType, IndexSchema, Key, KeyRange, PageFlushInfo, Row, Snapshot,
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
    }

    fn ctx(txn_id: u64, snapshot: Snapshot) -> StatementContext {
        StatementContext::with_snapshot(txn_id, snapshot)
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
}
