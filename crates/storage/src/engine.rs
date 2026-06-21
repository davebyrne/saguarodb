use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use buffer::{BufferPool, PageWriteGuard};
use common::{
    ColumnId, ColumnInfo, DbError, FileId, IndexId, IndexSchema, Key, KeyRange, Lsn, PageNum,
    Result, Row, RowId, SqlState, StatementContext, StoredRow, TableId, TableSchema, Value,
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

    /// The B-tree for a secondary index, keyed by the indexed columns and storing
    /// the row's primary key as its value.
    fn secondary_btree(&self, index: IndexId) -> BTree<'_, Key> {
        BTree::new(
            self.buffer_pool.as_ref(),
            self.wal.as_ref(),
            secondary_index_file_id(index),
        )
    }

    /// Insert `(entry_key, pk)` into a secondary index, enforcing uniqueness for a
    /// unique index. The multi-entry tree no longer rejects duplicate keys
    /// structurally, so a unique index presence-probes its key first; its
    /// `secondary_index_key` is `[indexed..]` alone (no pk tiebreaker), so a
    /// duplicate indexed value collides and is rejected. A non-unique index (and a
    /// unique index over a NULL value) embeds the pk in the key, so its keys are
    /// distinct and no probe is needed. This presence-probe is TEMPORARY: Milestone
    /// B commit 7 replaces it with a visibility-aware uniqueness check.
    fn insert_secondary_entry(
        &self,
        ctx: &StatementContext,
        index: &IndexSchema,
        entry_key: &Key,
        pk: &Key,
    ) -> Result<()> {
        let secondary = self.secondary_btree(index.id);
        if index.unique && !secondary.scan_key(entry_key)?.is_empty() {
            return Err(duplicate_unique_index(&index.name));
        }
        secondary.insert(ctx.txn_id, entry_key, pk)
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

    fn read_location(&self, schema: &TableSchema, location: RowLocation) -> Result<Option<Row>> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
            return Ok(None);
        };
        // Milestone A stamps the MVCC header but does not yet apply visibility, so
        // callers still consume only the column values (`DecodedRow::row`).
        Ok(Some(decode_row(schema, &bytes)?.row))
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
            let entry_key = secondary_index_key(&schema, &index, &row, &key)?;
            self.insert_secondary_entry(ctx, &index, &entry_key, &key)?;
        }

        Ok(RowId {
            page_num: location.page_num,
            slot_num: location.slot_num,
        })
    }

    fn get(&self, _ctx: &StatementContext, table: TableId, key: &Key) -> Result<Option<Row>> {
        let (schema, index_fid) = self.table_handle(table)?;
        let Some(location) = self.btree(index_fid).search(key)? else {
            return Ok(None);
        };
        self.read_location(&schema, location)
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
                let entry_key = secondary_index_key(&schema, index, &row, key)?;
                // Secondary value is the row's primary key (unchanged this
                // milestone); remove the specific (entry_key, pk) entry.
                self.secondary_btree(index.id)
                    .remove(ctx.txn_id, &entry_key, key)?;
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
            // index whose value is unchanged does not see a false duplicate.
            for index in &indexes {
                let old_key = secondary_index_key(&schema, index, &previous_row, key)?;
                self.secondary_btree(index.id)
                    .remove(ctx.txn_id, &old_key, key)?;
            }
            for index in &indexes {
                let new_key = secondary_index_key(&schema, index, &row, key)?;
                self.insert_secondary_entry(ctx, index, &new_key, key)?;
            }
        }

        Ok(true)
    }

    fn scan(&self, ctx: &StatementContext, table: TableId) -> Result<Box<dyn RowIterator>> {
        self.scan_range(ctx, table, &KeyRange::All)
    }

    fn scan_range(
        &self,
        _ctx: &StatementContext,
        table: TableId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let (schema, index_fid) = self.table_handle(table)?;
        let entries = self.btree(index_fid).range(range)?;

        let mut rows = Vec::with_capacity(entries.len());
        for (key, location) in entries {
            let row = self
                .read_location(&schema, location)?
                .ok_or_else(|| storage_internal("primary-key index points to dead row"))?;
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
        _ctx: &StatementContext,
        table: TableId,
        index: IndexId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let (schema, pk_file_id) = self.table_handle(table)?;
        self.ensure_index_live(table, index)?;

        // Walk the secondary index for the matching primary keys, then resolve
        // each through the primary-key index to its current heap row.
        let pk_btree = self.btree(pk_file_id);
        let entries = self.secondary_btree(index).range(range)?;
        let mut rows = Vec::with_capacity(entries.len());
        for (_entry_key, pk) in entries {
            let location = pk_btree.search(&pk)?.ok_or_else(|| {
                storage_internal("secondary index points to a missing primary key")
            })?;
            let row = self
                .read_location(&schema, location)?
                .ok_or_else(|| storage_internal("secondary index resolves to a dead row"))?;
            rows.push(StoredRow {
                row_id: RowId {
                    page_num: location.page_num,
                    slot_num: location.slot_num,
                },
                key: pk,
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
        // backfill it from the live rows via the primary-key index.
        let secondary = self.secondary_btree(schema.id);
        secondary.create(ctx.txn_id)?;
        for (pk, location) in self.btree(pk_file_id).range(&KeyRange::All)? {
            let row = self
                .read_location(&table_schema, location)?
                .ok_or_else(|| {
                    storage_internal("primary-key index points to a dead row during index backfill")
                })?;
            let key = secondary_index_key(&table_schema, schema, &row, &pk)?;
            self.insert_secondary_entry(ctx, schema, &key, &pk)?;
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

/// The secondary-index B-tree key for `row`. Non-unique indexes append the
/// primary key so every entry is distinct; a unique index keys on the indexed
/// values alone so the tree rejects duplicates — except when an indexed value is
/// NULL, where the primary key is appended too, because SQL treats NULLs as
/// distinct.
fn secondary_index_key(
    table: &TableSchema,
    index: &IndexSchema,
    row: &Row,
    pk: &Key,
) -> Result<Key> {
    let mut values = Vec::with_capacity(index.columns.len() + pk.0.len());
    let mut has_null = false;
    for column_id in &index.columns {
        let value = column_value(table, row, *column_id)?;
        has_null |= matches!(value, Value::Null);
        values.push(value);
    }
    if !index.unique || has_null {
        values.extend(pk.0.iter().cloned());
    }
    Ok(Key(values))
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
