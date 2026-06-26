use catalog::{CatalogManager, MemoryCatalog};
use common::{
    ColumnId, ColumnInfo, CopyOptions, DataType, DbError, IndexId, IndexSchema, Key, KeyRange,
    ParsedColumnDef, Result, Row, RowId, SqlState, StatementContext, StoredRow, TableId,
    TableSchema, Value,
};
use planner::{PhysicalPlan, bind, logical_plan, physical_plan};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use storage::{RowIterator, SchemaOperations, StorageEngine};

use crate::{CopyIn, CopyOut, ExecutionContext, ExecutionResult, QueryEngine};

pub struct ExecutorHarness {
    catalog: MemoryCatalog,
    storage: MemoryStorage,
    engine: QueryEngine,
}

impl ExecutorHarness {
    pub fn with_users() -> Self {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                    },
                ],
                vec!["id".to_string()],
            )
            .unwrap();
        let storage = MemoryStorage::empty();
        storage
            .create_table(&StatementContext::new(0), &schema)
            .unwrap();
        Self {
            catalog,
            storage,
            engine: QueryEngine,
        }
    }

    pub fn execute(&self, sql: &str) -> Result<ExecutionResult> {
        self.execute_with_cancel(sql, &AtomicBool::new(false))
    }

    pub fn execute_with_cancel(&self, sql: &str, cancel: &AtomicBool) -> Result<ExecutionResult> {
        let statement = parser::parse(sql)?;
        let bound = bind(&statement, &self.catalog)?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, &self.catalog)?;
        let is_read = is_read_plan(&physical);
        let statement = StatementContext::new(if is_read { 0 } else { 1 });
        let txn_id = statement.txn_id;
        let ctx = ExecutionContext {
            statement,
            catalog: &self.catalog,
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel,
        };
        let result = self.engine.execute(&ctx, &physical);
        if is_read {
            return result;
        }

        match result {
            Ok(result) => {
                self.storage.commit_txn(txn_id)?;
                Ok(result)
            }
            Err(err) => {
                let _ = self.storage.rollback_txn(txn_id);
                Err(err)
            }
        }
    }

    pub fn select_rows(&self, sql: &str) -> Result<Vec<Row>> {
        match self.execute(sql)? {
            ExecutionResult::Query { rows, .. } => Ok(rows),
            _ => Err(common::DbError::internal("expected query result")),
        }
    }

    fn resolve_columns(&self, table: &str, columns: &[&str]) -> (TableId, Vec<ColumnId>) {
        let schema = self.catalog.get_table_by_name(table).unwrap().unwrap();
        let ids = if columns.is_empty() {
            schema.columns.iter().map(|c| c.id).collect()
        } else {
            columns
                .iter()
                .map(|name| schema.columns.iter().find(|c| c.name == *name).unwrap().id)
                .collect()
        };
        (schema.id, ids)
    }

    /// Run a `COPY FROM` over the given chunks in one (committed) transaction,
    /// returning the rows inserted. `columns` empty means all columns.
    pub fn copy_in(
        &self,
        table: &str,
        columns: &[&str],
        options: CopyOptions,
        chunks: &[&[u8]],
    ) -> Result<u64> {
        let (table_id, column_ids) = self.resolve_columns(table, columns);
        let cancel = AtomicBool::new(false);
        let statement = StatementContext::new(1);
        let txn_id = statement.txn_id;
        let ctx = ExecutionContext {
            statement,
            catalog: &self.catalog,
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
        };
        let result = (|| {
            let mut copy_in = CopyIn::new(&ctx, table_id, column_ids, options)?;
            for chunk in chunks {
                copy_in.push_chunk(chunk)?;
            }
            copy_in.finish()
        })();
        match result {
            Ok(count) => {
                self.storage.commit_txn(txn_id)?;
                Ok(count)
            }
            Err(err) => {
                let _ = self.storage.rollback_txn(txn_id);
                Err(err)
            }
        }
    }

    /// Run a `COPY TO`, returning the full wire byte stream (header + rows).
    pub fn copy_out(&self, table: &str, columns: &[&str], options: CopyOptions) -> Result<Vec<u8>> {
        let (table_id, column_ids) = self.resolve_columns(table, columns);
        let cancel = AtomicBool::new(false);
        let ctx = ExecutionContext {
            statement: StatementContext::new(0),
            catalog: &self.catalog,
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
        };
        let mut out = CopyOut::new(&ctx, table_id, &column_ids, options)?;
        let mut bytes = Vec::new();
        if let Some(header) = out.header_line() {
            bytes.extend(header);
        }
        while let Some(row) = out.next_row()? {
            bytes.extend(row);
        }
        Ok(bytes)
    }
}

fn is_read_plan(plan: &PhysicalPlan) -> bool {
    !matches!(
        plan,
        PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. }
            | PhysicalPlan::Insert { .. }
            | PhysicalPlan::Update { .. }
            | PhysicalPlan::Delete { .. }
    )
}

#[derive(Default)]
pub struct MemoryStorage {
    state: Mutex<MemoryStorageState>,
}

#[derive(Default)]
struct MemoryStorageState {
    schemas: BTreeMap<TableId, TableSchema>,
    indexes: BTreeMap<IndexId, IndexSchema>,
    rows: BTreeMap<TableId, BTreeMap<Key, Row>>,
    savepoints: BTreeMap<u64, MemoryStorageSnapshot>,
}

#[derive(Clone)]
struct MemoryStorageSnapshot {
    schemas: BTreeMap<TableId, TableSchema>,
    indexes: BTreeMap<IndexId, IndexSchema>,
    rows: BTreeMap<TableId, BTreeMap<Key, Row>>,
}

impl MemoryStorage {
    pub fn empty() -> Self {
        Self::default()
    }
}

impl StorageEngine for MemoryStorage {
    fn insert(&self, ctx: &StatementContext, table: TableId, row: Row) -> Result<RowId> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        let schema = state
            .schemas
            .get(&table)
            .cloned()
            .ok_or_else(|| undefined_table(table))?;
        let key = key_for_row(&schema, &row)?;
        let rows = state.rows.entry(table).or_default();
        if rows.contains_key(&key) {
            return Err(DbError::storage(
                SqlState::UniqueViolation,
                "duplicate primary key",
            ));
        }
        let row_id = row_id_for_len(rows.len())?;
        rows.insert(key, row);
        Ok(row_id)
    }

    fn get(&self, _ctx: &StatementContext, table: TableId, key: &Key) -> Result<Option<Row>> {
        let state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        Ok(state
            .rows
            .get(&table)
            .and_then(|rows| rows.get(key))
            .cloned())
    }

    fn delete(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<bool> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        Ok(state
            .rows
            .get_mut(&table)
            .map(|rows| rows.remove(key).is_some())
            .unwrap_or(false))
    }

    fn update(&self, ctx: &StatementContext, table: TableId, key: &Key, row: Row) -> Result<bool> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        let schema = state
            .schemas
            .get(&table)
            .cloned()
            .ok_or_else(|| undefined_table(table))?;
        let replacement_key = key_for_row(&schema, &row)?;
        if &replacement_key != key {
            return Err(DbError::execute(
                SqlState::DatatypeMismatch,
                "primary key updates are not supported",
            ));
        }
        let Some(rows) = state.rows.get_mut(&table) else {
            return Ok(false);
        };
        if !rows.contains_key(key) {
            return Ok(false);
        }
        rows.insert(key.clone(), row);
        Ok(true)
    }

    fn scan(&self, _ctx: &StatementContext, table: TableId) -> Result<Box<dyn RowIterator>> {
        let state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        let schema = state
            .schemas
            .get(&table)
            .cloned()
            .ok_or_else(|| undefined_table(table))?;
        let rows = state.rows.get(&table).map(stored_rows).unwrap_or_default();
        Ok(Box::new(MemoryRowIterator {
            schema: column_info(&schema),
            rows,
            index: 0,
        }))
    }

    fn scan_range(
        &self,
        _ctx: &StatementContext,
        table: TableId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>> {
        let state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        let schema = state
            .schemas
            .get(&table)
            .cloned()
            .ok_or_else(|| undefined_table(table))?;
        let rows = state
            .rows
            .get(&table)
            .map(|rows| {
                rows.iter()
                    .enumerate()
                    .filter(|(_, (key, _))| key_in_range(key, range))
                    .map(|(index, (key, row))| stored_row(index, key, row))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Box::new(MemoryRowIterator {
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
        let state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        let schema = state
            .schemas
            .get(&table)
            .cloned()
            .ok_or_else(|| undefined_table(table))?;
        let index_schema = state
            .indexes
            .get(&index)
            .filter(|index| index.table == table)
            .cloned()
            .ok_or_else(|| undefined_index(index))?;

        // The mock holds rows directly, so it keys on the indexed columns alone
        // (no trailing primary key) and orders by (indexed values, primary key).
        let mut matched: Vec<(Key, Key, Row)> = Vec::new();
        if let Some(rows) = state.rows.get(&table) {
            for (pk, row) in rows {
                let indexed = index_key(&schema, &index_schema, row)?;
                if key_in_range(&indexed, range) {
                    matched.push((indexed, pk.clone(), row.clone()));
                }
            }
        }
        matched.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let rows = matched
            .into_iter()
            .enumerate()
            .map(|(index, (_, pk, row))| StoredRow {
                row_id: row_id_for_len(index).unwrap_or(RowId {
                    page_num: u32::MAX,
                    slot_num: u16::MAX,
                }),
                key: pk,
                row,
            })
            .collect();
        Ok(Box::new(MemoryRowIterator {
            schema: column_info(&schema),
            rows,
            index: 0,
        }))
    }

    fn rollback_txn(&self, txn_id: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        if let Some(snapshot) = state.savepoints.remove(&txn_id) {
            state.schemas = snapshot.schemas;
            state.indexes = snapshot.indexes;
            state.rows = snapshot.rows;
        }
        Ok(())
    }

    fn commit_txn(&self, txn_id: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        state.savepoints.remove(&txn_id);
        Ok(())
    }
}

impl SchemaOperations for MemoryStorage {
    fn create_table(&self, ctx: &StatementContext, schema: &TableSchema) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        state.schemas.insert(schema.id, schema.clone());
        state.rows.entry(schema.id).or_default();
        Ok(())
    }

    fn drop_table(&self, ctx: &StatementContext, table: TableId) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        state.schemas.remove(&table);
        state.indexes.retain(|_, index| index.table != table);
        state.rows.remove(&table);
        Ok(())
    }

    fn create_index(
        &self,
        ctx: &StatementContext,
        schema: &IndexSchema,
        _gc_horizon: u64,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        state.indexes.insert(schema.id, schema.clone());
        Ok(())
    }

    fn drop_index(&self, ctx: &StatementContext, index: IndexId) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        state.indexes.remove(&index);
        Ok(())
    }
}

fn begin_txn(state: &mut MemoryStorageState, txn_id: u64) {
    if txn_id == 0 || state.savepoints.contains_key(&txn_id) {
        return;
    }
    state.savepoints.insert(
        txn_id,
        MemoryStorageSnapshot {
            schemas: state.schemas.clone(),
            indexes: state.indexes.clone(),
            rows: state.rows.clone(),
        },
    );
}

struct MemoryRowIterator {
    schema: Vec<ColumnInfo>,
    rows: Vec<StoredRow>,
    index: usize,
}

impl RowIterator for MemoryRowIterator {
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

fn key_for_row(schema: &TableSchema, row: &Row) -> Result<Key> {
    let primary_key = schema
        .primary_key
        .first()
        .ok_or_else(|| DbError::internal("table has no primary key"))?;
    let slot = schema
        .columns
        .iter()
        .position(|column| column.id == *primary_key)
        .ok_or_else(|| DbError::internal("primary key column is missing"))?;
    let value = row
        .values
        .get(slot)
        .cloned()
        .ok_or_else(|| DbError::internal("row is missing primary key slot"))?;
    if matches!(value, Value::Null) {
        return Err(DbError::execute(
            SqlState::NotNullViolation,
            "primary key cannot be NULL",
        ));
    }
    Ok(Key(vec![value]))
}

fn stored_rows(rows: &BTreeMap<Key, Row>) -> Vec<StoredRow> {
    rows.iter()
        .enumerate()
        .map(|(index, (key, row))| stored_row(index, key, row))
        .collect()
}

fn stored_row(index: usize, key: &Key, row: &Row) -> StoredRow {
    StoredRow {
        row_id: row_id_for_len(index).unwrap_or(RowId {
            page_num: u32::MAX,
            slot_num: u16::MAX,
        }),
        key: key.clone(),
        row: row.clone(),
    }
}

fn row_id_for_len(len: usize) -> Result<RowId> {
    let slot_num = u16::try_from(len).map_err(|_| DbError::internal("too many test rows"))?;
    Ok(RowId {
        page_num: 0,
        slot_num,
    })
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

fn index_key(schema: &TableSchema, index: &IndexSchema, row: &Row) -> Result<Key> {
    let mut values = Vec::with_capacity(index.columns.len());
    for column_id in &index.columns {
        let slot = schema
            .columns
            .iter()
            .position(|column| column.id == *column_id)
            .ok_or_else(|| DbError::internal("index column is missing from table"))?;
        let value = row
            .values
            .get(slot)
            .cloned()
            .ok_or_else(|| DbError::internal("row is missing an index column"))?;
        values.push(value);
    }
    Ok(Key(values))
}
