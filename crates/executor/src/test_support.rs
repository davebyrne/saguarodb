use catalog::{CatalogManager, MemoryCatalog};
use common::{
    ColumnId, ColumnInfo, CopyOptions, DataType, DbError, IndexConstraintKind, IndexId,
    IndexSchema, Key, KeyRange, ParsedColumnDef, Result, Row, RowId, SqlState, StatementContext,
    StoredRow, TableId, TableSchema, Value,
};
use planner::{PhysicalPlan, bind, format_explain, logical_plan, physical_plan};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use storage::{RelationSnapshot, RowIterator, SchemaOperations, StorageEngine};

use crate::{CopyIn, CopyOut, ExecutionContext, ExecutionResult, QueryEngine, RowSink};

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
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        let storage = MemoryStorage::empty();
        storage
            .create_table(&StatementContext::new(0), &schema)
            .unwrap();
        let primary_key = catalog
            .create_index_with_constraint(
                "users_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();
        storage
            .create_index(&StatementContext::new(0), &primary_key, 0)
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
            relations: self.storage.capture_relation_snapshot()?,
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

    pub fn explain_plan(&self, sql: &str) -> Result<String> {
        let statement = parser::parse(sql)?;
        let bound = bind(&statement, &self.catalog)?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, &self.catalog)?;
        Ok(format_explain(&physical))
    }

    /// Stream a read query through `execute_query_streamed`, driving the provided
    /// `sink` in batches of `batch_size`. Mirrors the read path of
    /// `execute_with_cancel`; returns the number of rows streamed.
    pub fn stream_read_plan(
        &self,
        sql: &str,
        sink: &mut dyn RowSink,
        batch_size: usize,
    ) -> Result<u64> {
        let statement = parser::parse(sql)?;
        let bound = bind(&statement, &self.catalog)?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, &self.catalog)?;
        let cancel = AtomicBool::new(false);
        let ctx = ExecutionContext {
            statement: StatementContext::new(0),
            relations: self.storage.capture_relation_snapshot()?,
            catalog: &self.catalog,
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
        };
        self.engine
            .execute_query_streamed(&ctx, &physical, sink, batch_size)
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
            relations: self.storage.capture_relation_snapshot()?,
            catalog: &self.catalog,
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
        };
        let result = (|| {
            let mut copy_in =
                CopyIn::new(&ctx, table_id, column_ids, options, Vec::new(), Vec::new())?;
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
            relations: self.storage.capture_relation_snapshot()?,
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

    pub fn storage_keys(&self, table: &str) -> Result<Vec<Key>> {
        let schema = self
            .catalog
            .get_table_by_name(table)?
            .ok_or_else(|| undefined_table_by_name(table))?;
        self.storage.keys_for_table(schema.id)
    }
}

fn is_read_plan(plan: &PhysicalPlan) -> bool {
    !matches!(
        plan,
        PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. }
            | PhysicalPlan::CreateSequence { .. }
            | PhysicalPlan::DropSequence { .. }
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
    next_key: i64,
    savepoints: BTreeMap<u64, MemoryStorageSnapshot>,
}

#[derive(Clone)]
struct MemoryStorageSnapshot {
    schemas: BTreeMap<TableId, TableSchema>,
    indexes: BTreeMap<IndexId, IndexSchema>,
    rows: BTreeMap<TableId, BTreeMap<Key, Row>>,
    next_key: i64,
}

impl MemoryStorage {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn keys_for_table(&self, table: TableId) -> Result<Vec<Key>> {
        let state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        Ok(state
            .rows
            .get(&table)
            .map(|rows| rows.keys().cloned().collect())
            .unwrap_or_default())
    }
}

struct MemoryRelationSnapshot;

impl RelationSnapshot for MemoryRelationSnapshot {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn relation_epoch(&self) -> u64 {
        0
    }
}

impl StorageEngine for MemoryStorage {
    fn capture_relation_snapshot(&self) -> Result<Arc<dyn RelationSnapshot>> {
        Ok(Arc::new(MemoryRelationSnapshot))
    }

    fn insert(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        row: Row,
    ) -> Result<RowId> {
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
        let key = storage_identity_key_for_insert(&mut state, &schema, &row)?;
        validate_unique_indexes(&state, &schema, None, &row)?;
        let rows = state.rows.entry(table).or_default();
        if rows.contains_key(&key) {
            return Err(duplicate_storage_identity_error(&schema));
        }
        let row_id = row_id_for_len(rows.len())?;
        rows.insert(key, row);
        Ok(row_id)
    }

    fn get(
        &self,
        _ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<Option<Row>> {
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

    fn delete(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<bool> {
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

    fn update(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
    ) -> Result<bool> {
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
        let replacement_key = storage_identity_key_for_update(&schema, key, &row)?;
        validate_unique_indexes(&state, &schema, Some(key), &row)?;
        let Some(rows) = state.rows.get_mut(&table) else {
            return Ok(false);
        };
        if !rows.contains_key(key) {
            return Ok(false);
        }
        if &replacement_key != key && rows.contains_key(&replacement_key) {
            return Err(duplicate_storage_identity_error(&schema));
        }
        rows.remove(key)
            .ok_or_else(|| DbError::internal("row disappeared during test update"))?;
        rows.insert(replacement_key, row);
        Ok(true)
    }

    fn scan(
        &self,
        _ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
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
        _relations: &dyn RelationSnapshot,
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
        _relations: &dyn RelationSnapshot,
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
            state.next_key = snapshot.next_key;
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

    fn create_sequence(
        &self,
        _ctx: &StatementContext,
        _schema: &common::SequenceSchema,
    ) -> Result<()> {
        Ok(())
    }

    fn drop_sequence(&self, _ctx: &StatementContext, _sequence: common::SequenceId) -> Result<()> {
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
            next_key: state.next_key,
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

fn stored_rows(rows: &BTreeMap<Key, Row>) -> Vec<StoredRow> {
    rows.iter()
        .enumerate()
        .map(|(index, (key, row))| stored_row(index, key, row))
        .collect()
}

fn allocate_storage_key(state: &mut MemoryStorageState) -> Result<Key> {
    state.next_key = state
        .next_key
        .checked_add(1)
        .ok_or_else(|| DbError::internal("test storage key overflow"))?;
    Ok(Key(vec![Value::Integer(state.next_key)]))
}

fn storage_identity_key_for_insert(
    state: &mut MemoryStorageState,
    schema: &TableSchema,
    row: &Row,
) -> Result<Key> {
    if schema.primary_key.is_empty() {
        return allocate_storage_key(state);
    }
    primary_key_for_row(schema, row)
}

fn storage_identity_key_for_update(schema: &TableSchema, current: &Key, row: &Row) -> Result<Key> {
    if schema.primary_key.is_empty() {
        return Ok(current.clone());
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

fn validate_unique_indexes(
    state: &MemoryStorageState,
    schema: &TableSchema,
    current_key: Option<&Key>,
    candidate: &Row,
) -> Result<()> {
    for index in state
        .indexes
        .values()
        .filter(|index| index.table == schema.id && index.unique)
    {
        let (candidate_key, has_null) = row_key_for_columns(schema, &index.columns, candidate)?;
        if has_null {
            continue;
        }
        let Some(rows) = state.rows.get(&schema.id) else {
            continue;
        };
        for (key, row) in rows {
            if current_key == Some(key) {
                continue;
            }
            if index_key(schema, index, row)? == candidate_key {
                return Err(DbError::storage(
                    SqlState::UniqueViolation,
                    if index.constraint == IndexConstraintKind::PrimaryKey {
                        "duplicate primary key"
                    } else {
                        "duplicate key value violates unique index"
                    },
                ));
            }
        }
    }
    Ok(())
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
            pg_type: None,
        })
        .collect()
}

fn key_in_range(key: &Key, range: &KeyRange) -> bool {
    let prefix_len = comparison_prefix_len(range);
    let prefix = prefix_of(key, prefix_len);
    let compared = prefix.as_ref().unwrap_or(key);
    key_prefix_in_range(compared, range)
}

fn comparison_prefix_len(range: &KeyRange) -> usize {
    let bound_len = |bound: &Bound<Key>| match bound {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.0.len()),
        Bound::Unbounded => None,
    };
    match range {
        KeyRange::All => 0,
        KeyRange::Exact(key) => key.0.len(),
        KeyRange::Range { start, end } => bound_len(start).or_else(|| bound_len(end)).unwrap_or(0),
    }
}

fn prefix_of(key: &Key, len: usize) -> Option<Key> {
    (len < key.0.len()).then(|| Key(key.0[..len].to_vec()))
}

fn key_prefix_in_range(key: &Key, range: &KeyRange) -> bool {
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

fn undefined_table_by_name(table: &str) -> DbError {
    DbError::storage(
        SqlState::UndefinedTable,
        format!("table {table} does not exist"),
    )
}

fn undefined_index(index: IndexId) -> DbError {
    DbError::storage(
        SqlState::UndefinedTable,
        format!("index id {index} does not exist"),
    )
}

fn index_key(schema: &TableSchema, index: &IndexSchema, row: &Row) -> Result<Key> {
    let (key, _has_null) = row_key_for_columns(schema, &index.columns, row)?;
    Ok(key)
}

fn row_key_for_columns(
    schema: &TableSchema,
    columns: &[ColumnId],
    row: &Row,
) -> Result<(Key, bool)> {
    let mut values = Vec::with_capacity(columns.len());
    let mut has_null = false;
    for column_id in columns {
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
        has_null |= matches!(value, Value::Null);
        values.push(value);
    }
    Ok((Key(values), has_null))
}

fn duplicate_storage_identity_error(schema: &TableSchema) -> DbError {
    DbError::storage(
        SqlState::UniqueViolation,
        if schema.primary_key.is_empty() {
            "duplicate storage identity"
        } else {
            "duplicate primary key"
        },
    )
}
