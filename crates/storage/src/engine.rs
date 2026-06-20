use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};

use buffer::BufferPool;
use common::{
    ColumnInfo, DbError, FileId, Key, KeyRange, Lsn, PageNum, Result, Row, RowId, SqlState,
    StatementContext, StoredRow, TableId, TableSchema, Value,
};
use wal::{WalManager, WalRecord, WalRecordKind};

use crate::codec::{decode_row, encode_row};
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
    directory: BTreeMap<Key, RowLocation>,
    dropped: bool,
}

#[derive(Default)]
struct TxnRollback {
    directories: BTreeMap<TableId, BTreeMap<Key, Option<RowLocation>>>,
    tables: BTreeMap<TableId, Option<TableState>>,
}

struct StorageState {
    mode: StorageMode,
    tables: BTreeMap<TableId, TableState>,
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
                    directory: BTreeMap::new(),
                    dropped: false,
                },
            );
        }
        Ok(())
    }

    pub fn rebuild_directories(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        for table in state.tables.values_mut() {
            table.directory.clear();
        }

        let pages: Vec<_> = self.buffer_pool.iter_pages()?.collect();
        for info in pages {
            let Some(table) = state.tables.get_mut(&info.file_id) else {
                continue;
            };
            if table.dropped || !page::is_initialized(&info.data.0) {
                continue;
            }
            let page_id = page::page_id(&info.data.0)?;
            if page_id != info.page_num {
                return Err(storage_internal(
                    "page id does not match buffer page number",
                ));
            }
            for (slot_num, bytes) in page::live_rows(&info.data.0)? {
                let row = decode_row(&table.schema, &bytes)?;
                let key = key_for_row(&table.schema, &row)?;
                let previous = table.directory.insert(
                    key,
                    RowLocation {
                        file_id: info.file_id,
                        page_num: info.page_num,
                        slot_num,
                    },
                );
                if previous.is_some() {
                    return Err(DbError::storage(
                        SqlState::UniqueViolation,
                        "duplicate primary key while rebuilding storage directory",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn set_mode(&self, mode: StorageMode) -> Result<()> {
        self.lock_state()?.mode = mode;
        Ok(())
    }

    pub(crate) fn apply_insert_without_wal(
        &self,
        table: TableId,
        key: Key,
        row: Row,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let schema = live_table(&state, table)?.schema.clone();
        let row_key = key_for_row(&schema, &row)?;
        if row_key != key {
            return Err(storage_internal("WAL insert key does not match row"));
        }
        if live_table(&state, table)?.directory.contains_key(&key) {
            return Err(DbError::storage(
                SqlState::UniqueViolation,
                "duplicate primary key",
            ));
        }
        let location = self.write_new_row(&schema, &row, 0, 0)?;
        live_table_mut(&mut state, table)?
            .directory
            .insert(key, location);
        Ok(())
    }

    pub(crate) fn apply_update_without_wal(
        &self,
        table: TableId,
        key: Key,
        row: Row,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let table_state = live_table(&state, table)?;
        let schema = table_state.schema.clone();
        let Some(previous_location) = table_state.directory.get(&key).copied() else {
            return Ok(());
        };
        let replacement_key = key_for_row(&schema, &row)?;
        if replacement_key != key {
            return Err(storage_internal("WAL update key does not match row"));
        }
        self.mark_dead(previous_location, 0, 0)?;
        let new_location = self.write_new_row(&schema, &row, 0, 0)?;
        live_table_mut(&mut state, table)?
            .directory
            .insert(key, new_location);
        Ok(())
    }

    pub(crate) fn apply_delete_without_wal(&self, table: TableId, key: Key) -> Result<()> {
        let mut state = self.lock_state()?;
        let Some(location) = live_table_mut(&mut state, table)?.directory.remove(&key) else {
            return Ok(());
        };
        self.mark_dead(location, 0, 0)?;
        Ok(())
    }

    pub(crate) fn apply_create_table_without_wal(&self, schema: TableSchema) -> Result<()> {
        let mut state = self.lock_state()?;
        state.tables.insert(
            schema.id,
            TableState {
                schema,
                directory: BTreeMap::new(),
                dropped: false,
            },
        );
        Ok(())
    }

    pub(crate) fn apply_drop_table_without_wal(&self, table: TableId) -> Result<()> {
        let mut state = self.lock_state()?;
        if let Some(table_state) = state.tables.get_mut(&table) {
            table_state.dropped = true;
            table_state.directory.clear();
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, StorageState>> {
        self.state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))
    }

    /// Append a WAL record (in `Normal` mode only) and return its assigned LSN,
    /// which the caller stamps into the modified page. Returns `0` in recovery
    /// mode, where the page-LSN of replayed rows is irrelevant.
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

    fn write_new_row(
        &self,
        schema: &TableSchema,
        row: &Row,
        txn_id: u64,
        lsn: Lsn,
    ) -> Result<RowLocation> {
        let row_bytes = encode_row(schema, row)?;
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
                let slot_num = page::insert_row(writable.data_mut(), &row_bytes)?;
                page::set_page_lsn(writable.data_mut(), lsn);
                return Ok(RowLocation {
                    file_id,
                    page_num,
                    slot_num,
                });
            }
        }

        let mut writable = self.buffer_pool.new_page(file_id, txn_id)?;
        let page_num = writable.page_num();
        page::init_page(writable.data_mut(), page_num);
        let slot_num = page::insert_row(writable.data_mut(), &row_bytes)?;
        page::set_page_lsn(writable.data_mut(), lsn);
        Ok(RowLocation {
            file_id,
            page_num,
            slot_num,
        })
    }

    fn mark_dead(&self, location: RowLocation, txn_id: u64, lsn: Lsn) -> Result<bool> {
        let mut writable =
            self.buffer_pool
                .write_page(location.file_id, location.page_num, txn_id)?;
        let deleted = page::delete_row(writable.data_mut(), location.slot_num)?;
        page::set_page_lsn(writable.data_mut(), lsn);
        Ok(deleted)
    }

    fn read_location(&self, schema: &TableSchema, location: RowLocation) -> Result<Option<Row>> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
            return Ok(None);
        };
        Ok(Some(decode_row(schema, &bytes)?))
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
        let mut state = self.lock_state()?;
        let schema = live_table(&state, table)?.schema.clone();
        let key = key_for_row(&schema, &row)?;
        if live_table(&state, table)?.directory.contains_key(&key) {
            return Err(DbError::storage(
                SqlState::UniqueViolation,
                "duplicate primary key",
            ));
        }

        let lsn = self.append_wal(
            &state,
            ctx,
            WalRecordKind::Insert {
                table,
                key: key.clone(),
                row: row.clone(),
            },
        )?;
        record_directory_before(&mut state, ctx.txn_id, table, &key)?;
        let location = self.write_new_row(&schema, &row, ctx.txn_id, lsn)?;
        live_table_mut(&mut state, table)?
            .directory
            .insert(key, location);
        Ok(RowId {
            page_num: location.page_num,
            slot_num: location.slot_num,
        })
    }

    fn get(&self, _ctx: &StatementContext, table: TableId, key: &Key) -> Result<Option<Row>> {
        let (schema, location) = {
            let state = self.lock_state()?;
            let table = live_table(&state, table)?;
            let Some(location) = table.directory.get(key).copied() else {
                return Ok(None);
            };
            (table.schema.clone(), location)
        };
        self.read_location(&schema, location)
    }

    fn delete(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<bool> {
        let mut state = self.lock_state()?;
        if !state
            .tables
            .get(&table)
            .map(|table| !table.dropped)
            .unwrap_or(false)
        {
            return Ok(false);
        }
        let Some(location) = live_table(&state, table)?.directory.get(key).copied() else {
            return Ok(false);
        };

        let lsn = self.append_wal(
            &state,
            ctx,
            WalRecordKind::Delete {
                table,
                key: key.clone(),
            },
        )?;
        record_directory_before(&mut state, ctx.txn_id, table, key)?;
        self.mark_dead(location, ctx.txn_id, lsn)?;
        live_table_mut(&mut state, table)?.directory.remove(key);
        Ok(true)
    }

    fn update(&self, ctx: &StatementContext, table: TableId, key: &Key, row: Row) -> Result<bool> {
        let mut state = self.lock_state()?;
        let table_state = live_table(&state, table)?;
        let schema = table_state.schema.clone();
        let Some(previous_location) = table_state.directory.get(key).copied() else {
            return Ok(false);
        };
        let replacement_key = key_for_row(&schema, &row)?;
        if &replacement_key != key {
            return Err(DbError::execute(
                SqlState::DatatypeMismatch,
                "primary key updates are not supported",
            ));
        }

        let lsn = self.append_wal(
            &state,
            ctx,
            WalRecordKind::Update {
                table,
                key: key.clone(),
                row: row.clone(),
            },
        )?;
        record_directory_before(&mut state, ctx.txn_id, table, key)?;
        self.mark_dead(previous_location, ctx.txn_id, lsn)?;
        let new_location = self.write_new_row(&schema, &row, ctx.txn_id, lsn)?;
        live_table_mut(&mut state, table)?
            .directory
            .insert(key.clone(), new_location);
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
        let (schema, entries) = {
            let state = self.lock_state()?;
            let table_state = live_table(&state, table)?;
            let entries = table_state
                .directory
                .iter()
                .filter(|(key, _)| key_in_range(key, range))
                .map(|(key, location)| (key.clone(), *location))
                .collect::<Vec<_>>();
            (table_state.schema.clone(), entries)
        };

        let mut rows = Vec::with_capacity(entries.len());
        for (key, location) in entries {
            let row = self
                .read_location(&schema, location)?
                .ok_or_else(|| storage_internal("primary-key directory points to dead row"))?;
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

        for (table_id, entries) in rollback.directories {
            if let Some(table) = state.tables.get_mut(&table_id) {
                for (key, previous) in entries {
                    match previous {
                        Some(location) => {
                            table.directory.insert(key, location);
                        }
                        None => {
                            table.directory.remove(&key);
                        }
                    }
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
                directory: BTreeMap::new(),
                dropped: false,
            },
        );
        Ok(())
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
        table_state.dropped = true;
        table_state.directory.clear();
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
        let slot = schema
            .columns
            .iter()
            .position(|column| column.id == *primary_key)
            .ok_or_else(|| storage_internal("primary key column is missing"))?;
        let value = row
            .values
            .get(slot)
            .cloned()
            .ok_or_else(|| storage_internal("row is missing primary key slot"))?;
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

fn live_table_mut(state: &mut StorageState, table: TableId) -> Result<&mut TableState> {
    let table_state = state
        .tables
        .get_mut(&table)
        .ok_or_else(|| undefined_table(table))?;
    if table_state.dropped {
        return Err(undefined_table(table));
    }
    Ok(table_state)
}

fn record_directory_before(
    state: &mut StorageState,
    txn_id: u64,
    table: TableId,
    key: &Key,
) -> Result<()> {
    if txn_id == 0 {
        return Ok(());
    }
    let previous = live_table(state, table)?.directory.get(key).copied();
    state
        .rollback
        .entry(txn_id)
        .or_default()
        .directories
        .entry(table)
        .or_default()
        .entry(key.clone())
        .or_insert(previous);
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

fn key_in_range(key: &Key, range: &KeyRange) -> bool {
    match range {
        KeyRange::All => true,
        KeyRange::Exact(exact) => key == exact,
        KeyRange::Range { start, end } => {
            bound_contains_start(start, key) && bound_contains_end(end, key)
        }
    }
}

fn bound_contains_start(bound: &Bound<Key>, key: &Key) -> bool {
    match bound {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
    }
}

fn bound_contains_end(bound: &Bound<Key>, key: &Key) -> bool {
    match bound {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
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

fn storage_internal(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}
