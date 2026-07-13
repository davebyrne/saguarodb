use catalog::{CatalogManager, MemoryCatalog};
use common::{
    ColumnId, ColumnInfo, CopyOptions, DataType, DbError, IndexConstraintKind, IndexId,
    IndexSchema, Key, KeyRange, ParsedColumnDef, QueryCancel, Result, Row, RowId, RowIdentity,
    SqlState, StatementContext, StoredRow, TableId, TableSchema, TupleLockAcquire,
    TupleLockManager, TupleLockMode, TupleLockTag, TupleLockWaitPolicy, Value,
};
use planner::{ExplainAnalysis, PhysicalPlan, bind, format_explain, logical_plan, physical_plan};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use storage::{
    LockRowResult, LockedRow, RelationSnapshot, RowIterator, SchemaOperations, StorageEngine,
};

use crate::{CopyIn, CopyOut, ExecutionContext, ExecutionResult, QueryEngine, RowSink};

#[derive(Debug)]
struct TestSequenceRuntime;

impl common::SequenceManager for TestSequenceRuntime {
    fn sequence_exists(&self, _sequence: u32) -> Result<bool> {
        Ok(true)
    }

    fn nextval(&self, _txn_id: u64, _sequence: u32) -> Result<i64> {
        Ok(1)
    }

    fn setval(&self, _txn_id: u64, _sequence: u32, value: i64, _is_called: bool) -> Result<i64> {
        Ok(value)
    }
}

pub struct ExecutorHarness {
    catalog: Arc<MemoryCatalog>,
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
            .create_table(&memory_statement_context(0), &schema)
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
            .create_index(&memory_statement_context(0), &primary_key, 0)
            .unwrap();
        Self {
            catalog: Arc::new(catalog),
            storage,
            engine: QueryEngine,
        }
    }

    pub fn execute(&self, sql: &str) -> Result<ExecutionResult> {
        self.execute_with_cancel(sql, &QueryCancel::new())
    }

    pub fn execute_with_cancel(&self, sql: &str, cancel: &QueryCancel) -> Result<ExecutionResult> {
        self.execute_with_spill(sql, cancel, spill::SpillConfig::default())
    }

    pub fn execute_with_spill(
        &self,
        sql: &str,
        cancel: &QueryCancel,
        spill: spill::SpillConfig,
    ) -> Result<ExecutionResult> {
        let statement = parser::parse(sql)?;
        let bound = bind(&statement, self.catalog.as_ref())?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.catalog.as_ref())?;
        let is_read = is_read_plan(&physical);
        let statement = memory_statement_context(if is_read { 0 } else { 1 });
        let txn_id = statement.txn_id;
        let ctx = ExecutionContext {
            statement,
            relations: self.storage.capture_relation_snapshot()?,
            catalog: self.catalog.clone(),
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel,
            spill,
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

    pub fn storage_scan_count(&self) -> usize {
        self.storage.scan_count()
    }

    pub fn select_rows(&self, sql: &str) -> Result<Vec<Row>> {
        match self.execute(sql)? {
            ExecutionResult::Query { rows, .. } => Ok(rows),
            _ => Err(common::DbError::internal("expected query result")),
        }
    }

    pub fn explain_plan(&self, sql: &str) -> Result<String> {
        let statement = parser::parse(sql)?;
        let bound = bind(&statement, self.catalog.as_ref())?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.catalog.as_ref())?;
        format_explain(&physical, self.catalog.as_ref())
    }

    pub fn analyze_query(&self, sql: &str) -> Result<ExplainAnalysis> {
        let statement = parser::parse(sql)?;
        let bound = bind(&statement, self.catalog.as_ref())?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.catalog.as_ref())?;
        let cancel = QueryCancel::new();
        let ctx = ExecutionContext {
            statement: memory_statement_context(1)
                .with_sequence_manager(Arc::new(TestSequenceRuntime)),
            relations: self.storage.capture_relation_snapshot()?,
            catalog: self.catalog.clone(),
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
            spill: spill::SpillConfig::default(),
        };
        self.engine.analyze_query(&ctx, &physical)
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
        let bound = bind(&statement, self.catalog.as_ref())?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.catalog.as_ref())?;
        let cancel = QueryCancel::new();
        let ctx = ExecutionContext {
            statement: memory_statement_context(0),
            relations: self.storage.capture_relation_snapshot()?,
            catalog: self.catalog.clone(),
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
            spill: spill::SpillConfig::default(),
        };
        self.engine
            .execute_query_streamed(&ctx, &physical, sink, batch_size)
    }

    fn resolve_columns(&self, table: &str, columns: &[&str]) -> (TableSchema, Vec<ColumnId>) {
        let schema = self.catalog.get_table_by_name(table).unwrap().unwrap();
        let ids = if columns.is_empty() {
            schema.columns.iter().map(|c| c.id).collect()
        } else {
            columns
                .iter()
                .map(|name| schema.columns.iter().find(|c| c.name == *name).unwrap().id)
                .collect()
        };
        (schema, ids)
    }

    pub fn table_schema(&self, table: &str) -> TableSchema {
        self.catalog.get_table_by_name(table).unwrap().unwrap()
    }

    pub fn rename_catalog_column(&self, table: &str, old_name: &str, new_name: &str) -> Result<()> {
        let schema = self
            .catalog
            .get_table_by_name(table)?
            .ok_or_else(|| undefined_table_by_name(table))?;
        self.catalog
            .rename_table_column(schema.id, old_name, new_name.to_string())?;
        Ok(())
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
        let (schema, column_ids) = self.resolve_columns(table, columns);
        let cancel = QueryCancel::new();
        let statement = memory_statement_context(1);
        let txn_id = statement.txn_id;
        let ctx = ExecutionContext {
            statement,
            relations: self.storage.capture_relation_snapshot()?,
            catalog: self.catalog.clone(),
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
            spill: spill::SpillConfig::default(),
        };
        let result = (|| {
            let mut copy_in =
                CopyIn::new(&ctx, schema, column_ids, options, Vec::new(), Vec::new())?;
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
        let (schema, column_ids) = self.resolve_columns(table, columns);
        self.copy_out_with_schema(schema, &column_ids, options)
    }

    pub fn copy_out_with_schema(
        &self,
        schema: TableSchema,
        columns: &[ColumnId],
        options: CopyOptions,
    ) -> Result<Vec<u8>> {
        let cancel = QueryCancel::new();
        let ctx = ExecutionContext {
            statement: memory_statement_context(0),
            relations: self.storage.capture_relation_snapshot()?,
            catalog: self.catalog.clone(),
            storage: &self.storage,
            schema_ops: &self.storage,
            gc_horizon: common::FIRST_NORMAL_XID,
            cancel: &cancel,
            spill: spill::SpillConfig::default(),
        };
        let mut out = CopyOut::new(&ctx, schema, columns, options)?;
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
    matches!(
        plan,
        PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::SystemScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::MergeJoin { .. }
            | PhysicalPlan::Filter { .. }
            | PhysicalPlan::Projection { .. }
            | PhysicalPlan::Sort { .. }
            | PhysicalPlan::Distinct { .. }
            | PhysicalPlan::Limit { .. }
            | PhysicalPlan::Aggregate { .. }
            | PhysicalPlan::Values { .. }
            | PhysicalPlan::SetOp { .. }
    )
}

#[derive(Debug, Default)]
struct PermissiveMemoryTupleLocks;

impl TupleLockManager for PermissiveMemoryTupleLocks {
    fn acquire_tuple(
        &self,
        _xid: u64,
        _tag: &TupleLockTag,
        _mode: TupleLockMode,
        _wait_policy: TupleLockWaitPolicy,
        _cancel: &QueryCancel,
    ) -> Result<TupleLockAcquire> {
        Ok(TupleLockAcquire::Acquired(
            common::TupleLockGrantChange::manager_receipt(()),
        ))
    }

    fn restore_tuple_grants(
        &self,
        _xid: u64,
        _changes: Vec<common::TupleLockGrantChange>,
    ) -> Result<()> {
        Ok(())
    }

    fn holds_tuple(&self, _xid: u64, _tag: &TupleLockTag, _mode: TupleLockMode) -> bool {
        true
    }
}

fn memory_statement_context(txn_id: u64) -> StatementContext {
    StatementContext::new(txn_id).with_tuple_lock_manager(Arc::new(PermissiveMemoryTupleLocks))
}

#[derive(Default)]
pub struct MemoryStorage {
    state: Mutex<MemoryStorageState>,
    scan_calls: AtomicUsize,
}

#[derive(Default)]
struct MemoryStorageState {
    schemas: BTreeMap<TableId, TableSchema>,
    indexes: BTreeMap<IndexId, IndexSchema>,
    rows: BTreeMap<TableId, BTreeMap<Key, MemoryStoredRow>>,
    next_key: i64,
    next_row_id: u64,
    savepoints: BTreeMap<u64, MemoryStorageSnapshot>,
}

#[derive(Clone)]
struct MemoryStorageSnapshot {
    schemas: BTreeMap<TableId, TableSchema>,
    indexes: BTreeMap<IndexId, IndexSchema>,
    rows: BTreeMap<TableId, BTreeMap<Key, MemoryStoredRow>>,
    next_key: i64,
}

#[derive(Clone)]
struct MemoryStoredRow {
    row_id: RowId,
    xmin: u64,
    row: Row,
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

    pub fn scan_count(&self) -> usize {
        self.scan_calls.load(Ordering::SeqCst)
    }

    fn mutate_locked(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        target: &LockedRow,
        replacement: Option<Row>,
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
        let required_mode = match replacement.as_ref() {
            None => TupleLockMode::Update,
            Some(row)
                if schema.primary_key.is_empty()
                    || storage_identity_key_for_update(&schema, &target.identity().key, row)?
                        == target.identity().key =>
            {
                TupleLockMode::NoKeyUpdate
            }
            Some(_) => TupleLockMode::Update,
        };
        if target.table() != table || target.owner() != ctx.txn_id {
            return Err(DbError::internal(
                "locked row capability belongs to a different table or transaction",
            ));
        }
        let tag = TupleLockTag {
            table,
            key: target.identity().key.clone(),
        };
        if !ctx.tuple_locks.holds_tuple(ctx.txn_id, &tag, required_mode) {
            return Err(DbError::internal(
                "locked row capability has no matching live tuple-lock grant",
            ));
        }
        if target.mode() < required_mode {
            return Err(DbError::internal(format!(
                "tuple lock mode {:?} is insufficient; {:?} is required",
                target.mode(),
                required_mode
            )));
        }
        let is_current = state
            .rows
            .get(&table)
            .and_then(|rows| rows.get(&target.identity().key))
            .is_some_and(|stored| {
                stored.row_id == target.identity().row_id
                    && stored.xmin == target.identity().xmin
                    && stored.row == *target.row()
            });
        if !is_current {
            return Err(DbError::internal(
                "locked row target is no longer the current test-storage row",
            ));
        }

        let Some(row) = replacement else {
            state
                .rows
                .get_mut(&table)
                .expect("validated table rows")
                .remove(&target.identity().key);
            return Ok(true);
        };
        let replacement_key =
            storage_identity_key_for_update(&schema, &target.identity().key, &row)?;
        validate_unique_indexes(&state, &schema, Some(&target.identity().key), &row)?;
        let replacement_row_id = allocate_memory_row_id(&mut state)?;
        let rows = state.rows.get_mut(&table).expect("validated table rows");
        if replacement_key != target.identity().key && rows.contains_key(&replacement_key) {
            return Err(duplicate_storage_identity_error(&schema));
        }
        rows.remove(&target.identity().key)
            .expect("validated locked row");
        rows.insert(
            replacement_key,
            MemoryStoredRow {
                row_id: replacement_row_id,
                xmin: ctx.txn_id,
                row,
            },
        );
        Ok(true)
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
        if state
            .rows
            .get(&table)
            .is_some_and(|rows| rows.contains_key(&key))
        {
            return Err(duplicate_storage_identity_error(&schema));
        }
        let row_id = allocate_memory_row_id(&mut state)?;
        let rows = state.rows.entry(table).or_default();
        rows.insert(
            key,
            MemoryStoredRow {
                row_id,
                xmin: ctx.txn_id,
                row,
            },
        );
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
            .map(|stored| stored.row.clone()))
    }

    fn lock_row(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        identity: &RowIdentity,
        mode: TupleLockMode,
        wait_policy: TupleLockWaitPolicy,
    ) -> Result<LockRowResult> {
        let change = match ctx.tuple_locks.acquire_tuple(
            ctx.txn_id,
            &TupleLockTag {
                table,
                key: identity.key.clone(),
            },
            mode,
            wait_policy,
            ctx.cancel.as_ref(),
        )? {
            TupleLockAcquire::Acquired(change) => change,
            TupleLockAcquire::Skipped => return Ok(LockRowResult::Skipped),
        };
        let lookup = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))
            .map(|state| {
                state
                    .rows
                    .get(&table)
                    .and_then(|rows| rows.get(&identity.key))
                    .filter(|stored| {
                        stored.row_id == identity.row_id && stored.xmin == identity.xmin
                    })
                    .map(|stored| stored.row.clone())
            });
        match lookup {
            Ok(Some(row)) => Ok(LockRowResult::Locked(LockedRow::from_lock_grant(
                table,
                ctx.txn_id,
                identity.clone(),
                row,
                mode,
            ))),
            Ok(None) => {
                ctx.tuple_locks
                    .restore_tuple_grants(ctx.txn_id, vec![change])?;
                Ok(LockRowResult::Deleted)
            }
            Err(err) => match ctx
                .tuple_locks
                .restore_tuple_grants(ctx.txn_id, vec![change])
            {
                Ok(()) => Err(err),
                Err(restore) => Err(DbError::internal(format!(
                    "row lookup failed ({err}); restoring its tuple-lock grant also failed ({restore})"
                ))),
            },
        }
    }

    fn update_locked(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        target: &LockedRow,
        row: Row,
    ) -> Result<bool> {
        self.mutate_locked(ctx, relations, table, target, Some(row))
    }

    fn delete_locked(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        target: &LockedRow,
    ) -> Result<bool> {
        self.mutate_locked(ctx, relations, table, target, None)
    }

    fn delete(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<bool> {
        let identity = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?
            .rows
            .get(&table)
            .and_then(|rows| rows.get(key))
            .map(|stored| RowIdentity {
                row_id: stored.row_id,
                xmin: stored.xmin,
                key: key.clone(),
            });
        let Some(identity) = identity else {
            return Ok(false);
        };
        match self.lock_row(
            ctx,
            relations,
            table,
            &identity,
            TupleLockMode::Update,
            TupleLockWaitPolicy::Block,
        )? {
            LockRowResult::Locked(target) => self.delete_locked(ctx, relations, table, &target),
            LockRowResult::Deleted => Err(memory_concurrent_update_error()),
            LockRowResult::Skipped => {
                unreachable!("blocking tuple-lock acquisition cannot skip a row")
            }
        }
    }

    fn update(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
    ) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        let schema = state
            .schemas
            .get(&table)
            .cloned()
            .ok_or_else(|| undefined_table(table))?;
        let replacement_key = storage_identity_key_for_update(&schema, key, &row)?;
        let Some(stored) = state.rows.get(&table).and_then(|rows| rows.get(key)) else {
            return Ok(false);
        };
        let identity = RowIdentity {
            row_id: stored.row_id,
            xmin: stored.xmin,
            key: key.clone(),
        };
        drop(state);
        let mode = if replacement_key == *key {
            TupleLockMode::NoKeyUpdate
        } else {
            TupleLockMode::Update
        };
        match self.lock_row(
            ctx,
            relations,
            table,
            &identity,
            mode,
            TupleLockWaitPolicy::Block,
        )? {
            LockRowResult::Locked(target) => {
                self.update_locked(ctx, relations, table, &target, row)
            }
            LockRowResult::Deleted => Err(memory_concurrent_update_error()),
            LockRowResult::Skipped => {
                unreachable!("blocking tuple-lock acquisition cannot skip a row")
            }
        }
    }

    fn scan(
        &self,
        _ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
    ) -> Result<Box<dyn RowIterator>> {
        self.scan_calls.fetch_add(1, Ordering::SeqCst);
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
                    .filter(|(key, _)| key_in_range(key, range))
                    .map(|(key, row)| stored_row(key, row))
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
        let mut matched: Vec<(Key, Key, MemoryStoredRow)> = Vec::new();
        if let Some(rows) = state.rows.get(&table) {
            for (pk, stored) in rows {
                let indexed = index_key(&schema, &index_schema, &stored.row)?;
                if key_in_range(&indexed, range) {
                    matched.push((indexed, pk.clone(), stored.clone()));
                }
            }
        }
        matched.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let rows = matched
            .into_iter()
            .map(|(_, pk, stored)| StoredRow {
                row_id: stored.row_id,
                xmin: stored.xmin,
                key: pk,
                row: stored.row,
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
    fn create_schema(
        &self,
        _ctx: &StatementContext,
        _schema: &common::NamespaceSchema,
    ) -> Result<()> {
        Ok(())
    }

    fn drop_schema(&self, _ctx: &StatementContext, _schema: common::SchemaId) -> Result<()> {
        Ok(())
    }

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

    fn update_table_schema(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        indexes: &[IndexSchema],
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        begin_txn(&mut state, ctx.txn_id);
        state.schemas.insert(schema.id, schema.clone());
        for index in indexes {
            state.indexes.insert(index.id, index.clone());
        }
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

    fn create_view(&self, _ctx: &StatementContext, _schema: &common::ViewSchema) -> Result<()> {
        Ok(())
    }

    fn replace_view(&self, _ctx: &StatementContext, _schema: &common::ViewSchema) -> Result<()> {
        Ok(())
    }

    fn drop_view(&self, _ctx: &StatementContext, _view: TableId) -> Result<()> {
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

fn stored_rows(rows: &BTreeMap<Key, MemoryStoredRow>) -> Vec<StoredRow> {
    rows.iter().map(|(key, row)| stored_row(key, row)).collect()
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
        for (key, stored) in rows {
            if current_key == Some(key) {
                continue;
            }
            if index_key(schema, index, &stored.row)? == candidate_key {
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

fn stored_row(key: &Key, stored: &MemoryStoredRow) -> StoredRow {
    StoredRow {
        row_id: stored.row_id,
        xmin: stored.xmin,
        key: key.clone(),
        row: stored.row.clone(),
    }
}

fn row_id_for_sequence(sequence: u64) -> Result<RowId> {
    let slots_per_page = u64::from(u16::MAX) + 1;
    let page_num = u32::try_from(sequence / slots_per_page)
        .map_err(|_| DbError::internal("test row id space exhausted"))?;
    let slot_num =
        u16::try_from(sequence % slots_per_page).expect("remainder is always representable as u16");
    Ok(RowId { page_num, slot_num })
}

fn allocate_memory_row_id(state: &mut MemoryStorageState) -> Result<RowId> {
    let row_id = row_id_for_sequence(state.next_row_id)?;
    state.next_row_id = state
        .next_row_id
        .checked_add(1)
        .ok_or_else(|| DbError::internal("test row id overflow"))?;
    Ok(row_id)
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

fn memory_concurrent_update_error() -> DbError {
    DbError::execute(
        SqlState::SerializationFailure,
        "could not serialize access due to concurrent update",
    )
}

#[cfg(test)]
mod memory_storage_identity_tests {
    use super::*;
    use common::{CompressionSetting, QueryCancel, TupleLockGrantChange, TupleLockManager};

    #[derive(Debug, Default)]
    struct TestTupleLocks;

    impl TupleLockManager for TestTupleLocks {
        fn acquire_tuple(
            &self,
            _xid: u64,
            _tag: &TupleLockTag,
            _mode: TupleLockMode,
            _wait_policy: TupleLockWaitPolicy,
            _cancel: &QueryCancel,
        ) -> Result<TupleLockAcquire> {
            Ok(TupleLockAcquire::Acquired(
                TupleLockGrantChange::manager_receipt(()),
            ))
        }

        fn restore_tuple_grants(
            &self,
            _xid: u64,
            _changes: Vec<TupleLockGrantChange>,
        ) -> Result<()> {
            Ok(())
        }

        fn holds_tuple(&self, _xid: u64, _tag: &TupleLockTag, _mode: TupleLockMode) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct RejectTupleLocks;

    impl TupleLockManager for RejectTupleLocks {
        fn acquire_tuple(
            &self,
            _xid: u64,
            _tag: &TupleLockTag,
            _mode: TupleLockMode,
            _wait_policy: TupleLockWaitPolicy,
            _cancel: &QueryCancel,
        ) -> Result<TupleLockAcquire> {
            Err(DbError::execute(
                SqlState::LockNotAvailable,
                "injected tuple-lock conflict",
            ))
        }

        fn restore_tuple_grants(
            &self,
            _xid: u64,
            _changes: Vec<TupleLockGrantChange>,
        ) -> Result<()> {
            Ok(())
        }

        fn holds_tuple(&self, _xid: u64, _tag: &TupleLockTag, _mode: TupleLockMode) -> bool {
            false
        }
    }

    enum InterveningMutation {
        Delete,
        Update(Row),
    }

    struct InterveningTupleLocks {
        storage: Arc<MemoryStorage>,
        table: TableId,
        mutation: Mutex<Option<InterveningMutation>>,
    }

    impl std::fmt::Debug for InterveningTupleLocks {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("InterveningTupleLocks")
                .field("table", &self.table)
                .finish_non_exhaustive()
        }
    }

    impl TupleLockManager for InterveningTupleLocks {
        fn acquire_tuple(
            &self,
            _xid: u64,
            tag: &TupleLockTag,
            _mode: TupleLockMode,
            _wait_policy: TupleLockWaitPolicy,
            _cancel: &QueryCancel,
        ) -> Result<TupleLockAcquire> {
            if let Some(mutation) = self.mutation.lock().unwrap().take() {
                let relations = self.storage.capture_relation_snapshot()?;
                let ctx = memory_statement_context(99);
                match mutation {
                    InterveningMutation::Delete => {
                        self.storage
                            .delete(&ctx, relations.as_ref(), self.table, &tag.key)?;
                    }
                    InterveningMutation::Update(row) => {
                        self.storage
                            .update(&ctx, relations.as_ref(), self.table, &tag.key, row)?;
                    }
                }
            }
            Ok(TupleLockAcquire::Acquired(
                TupleLockGrantChange::manager_receipt(()),
            ))
        }

        fn restore_tuple_grants(
            &self,
            _xid: u64,
            _changes: Vec<TupleLockGrantChange>,
        ) -> Result<()> {
            Ok(())
        }

        fn holds_tuple(&self, _xid: u64, _tag: &TupleLockTag, _mode: TupleLockMode) -> bool {
            true
        }
    }

    fn storage_with_users() -> (MemoryStorage, TableSchema, Arc<dyn RelationSnapshot>) {
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
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        let storage = MemoryStorage::empty();
        storage
            .create_table(&memory_statement_context(0), &schema)
            .unwrap();
        let relations = storage.capture_relation_snapshot().unwrap();
        (storage, schema, relations)
    }

    fn user(id: i64, name: &str) -> Row {
        Row {
            values: vec![Value::Integer(id), Value::Text(name.to_string())],
        }
    }

    #[test]
    fn stale_identity_does_not_lock_or_mutate_reinserted_key() {
        let (storage, schema, relations) = storage_with_users();
        let key = Key(vec![Value::Integer(1)]);
        let old_row_id = storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                user(1, "old"),
            )
            .unwrap();
        let stale = LockedRow::from_lock_grant(
            schema.id,
            4,
            RowIdentity {
                row_id: old_row_id,
                xmin: 1,
                key: key.clone(),
            },
            user(1, "old"),
            TupleLockMode::Update,
        );
        storage
            .delete(
                &memory_statement_context(2),
                relations.as_ref(),
                schema.id,
                &key,
            )
            .unwrap();
        let replacement_row_id = storage
            .insert(
                &memory_statement_context(3),
                relations.as_ref(),
                schema.id,
                user(1, "replacement"),
            )
            .unwrap();
        assert_ne!(old_row_id, replacement_row_id);

        let ctx = memory_statement_context(4).with_tuple_lock_manager(Arc::new(TestTupleLocks));
        assert_eq!(
            storage
                .lock_row(
                    &ctx,
                    relations.as_ref(),
                    schema.id,
                    stale.identity(),
                    TupleLockMode::Update,
                    TupleLockWaitPolicy::Block,
                )
                .unwrap(),
            LockRowResult::Deleted
        );
        let err = storage
            .update_locked(
                &ctx,
                relations.as_ref(),
                schema.id,
                &stale,
                user(1, "wrong"),
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(
            storage
                .get(&ctx, relations.as_ref(), schema.id, &key)
                .unwrap(),
            Some(user(1, "replacement"))
        );
    }

    #[test]
    fn rollback_does_not_reuse_a_stale_row_identity() {
        let (storage, schema, relations) = storage_with_users();
        let key = Key(vec![Value::Integer(1)]);
        let old_row_id = storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                user(1, "rolled back"),
            )
            .unwrap();
        storage.rollback_txn(1).unwrap();
        let replacement_row_id = storage
            .insert(
                &memory_statement_context(2),
                relations.as_ref(),
                schema.id,
                user(1, "replacement"),
            )
            .unwrap();
        assert_ne!(old_row_id, replacement_row_id);

        let ctx = memory_statement_context(3).with_tuple_lock_manager(Arc::new(TestTupleLocks));
        assert_eq!(
            storage
                .lock_row(
                    &ctx,
                    relations.as_ref(),
                    schema.id,
                    &RowIdentity {
                        row_id: old_row_id,
                        xmin: 1,
                        key,
                    },
                    TupleLockMode::Update,
                    TupleLockWaitPolicy::Block,
                )
                .unwrap(),
            LockRowResult::Deleted
        );
    }

    #[test]
    fn intervening_same_key_update_invalidates_locked_identity() {
        let (storage, schema, relations) = storage_with_users();
        let key = Key(vec![Value::Integer(1)]);
        let old_row_id = storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                user(1, "old"),
            )
            .unwrap();
        let stale = LockedRow::from_lock_grant(
            schema.id,
            3,
            RowIdentity {
                row_id: old_row_id,
                xmin: 1,
                key: key.clone(),
            },
            user(1, "old"),
            TupleLockMode::Update,
        );
        storage
            .update(
                &memory_statement_context(2),
                relations.as_ref(),
                schema.id,
                &key,
                user(1, "intervening"),
            )
            .unwrap();

        let ctx = memory_statement_context(3).with_tuple_lock_manager(Arc::new(TestTupleLocks));
        let err = storage
            .update_locked(
                &ctx,
                relations.as_ref(),
                schema.id,
                &stale,
                user(1, "wrong"),
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(
            storage
                .get(&ctx, relations.as_ref(), schema.id, &key)
                .unwrap(),
            Some(user(1, "intervening"))
        );
    }

    #[test]
    fn ordinary_update_and_delete_participate_in_tuple_locking() {
        let (storage, schema, relations) = storage_with_users();
        let setup = memory_statement_context(1);
        storage
            .insert(&setup, relations.as_ref(), schema.id, user(1, "first"))
            .unwrap();
        storage
            .insert(&setup, relations.as_ref(), schema.id, user(2, "second"))
            .unwrap();
        let rejecting =
            memory_statement_context(2).with_tuple_lock_manager(Arc::new(RejectTupleLocks));

        let update_err = storage
            .update(
                &rejecting,
                relations.as_ref(),
                schema.id,
                &Key(vec![Value::Integer(1)]),
                user(1, "wrong"),
            )
            .unwrap_err();
        assert_eq!(update_err.code, SqlState::LockNotAvailable);
        let delete_err = storage
            .delete(
                &rejecting,
                relations.as_ref(),
                schema.id,
                &Key(vec![Value::Integer(2)]),
            )
            .unwrap_err();
        assert_eq!(delete_err.code, SqlState::LockNotAvailable);
        assert_eq!(
            storage
                .get(
                    &setup,
                    relations.as_ref(),
                    schema.id,
                    &Key(vec![Value::Integer(1)]),
                )
                .unwrap(),
            Some(user(1, "first"))
        );
        assert_eq!(
            storage
                .get(
                    &setup,
                    relations.as_ref(),
                    schema.id,
                    &Key(vec![Value::Integer(2)]),
                )
                .unwrap(),
            Some(user(2, "second"))
        );
    }

    #[test]
    fn ordinary_mutation_reports_an_intervening_replacement_as_serialization_failure() {
        let (storage, schema, relations) = storage_with_users();
        let storage = Arc::new(storage);
        let key = Key(vec![Value::Integer(1)]);
        storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                user(1, "original"),
            )
            .unwrap();
        let deleting =
            memory_statement_context(2).with_tuple_lock_manager(Arc::new(InterveningTupleLocks {
                storage: Arc::clone(&storage),
                table: schema.id,
                mutation: Mutex::new(Some(InterveningMutation::Delete)),
            }));
        let update_err = storage
            .update(
                &deleting,
                relations.as_ref(),
                schema.id,
                &key,
                user(1, "outer update"),
            )
            .unwrap_err();
        assert_eq!(update_err.code, SqlState::SerializationFailure);
        assert_eq!(
            storage
                .get(&deleting, relations.as_ref(), schema.id, &key)
                .unwrap(),
            None
        );

        storage
            .insert(
                &memory_statement_context(3),
                relations.as_ref(),
                schema.id,
                user(1, "replacement target"),
            )
            .unwrap();
        let updating =
            memory_statement_context(4).with_tuple_lock_manager(Arc::new(InterveningTupleLocks {
                storage: Arc::clone(&storage),
                table: schema.id,
                mutation: Mutex::new(Some(InterveningMutation::Update(user(
                    1,
                    "intervening update",
                )))),
            }));
        let delete_err = storage
            .delete(&updating, relations.as_ref(), schema.id, &key)
            .unwrap_err();
        assert_eq!(delete_err.code, SqlState::SerializationFailure);
        assert_eq!(
            storage
                .get(&updating, relations.as_ref(), schema.id, &key)
                .unwrap(),
            Some(user(1, "intervening update"))
        );
    }

    #[test]
    fn row_identity_sequence_uses_page_and_slot_components() {
        let (storage, schema, relations) = storage_with_users();
        storage.state.lock().unwrap().next_row_id = u64::from(u16::MAX);
        let last_slot = storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                user(1, "last slot"),
            )
            .unwrap();
        let next_page = storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                user(2, "next page"),
            )
            .unwrap();
        assert_eq!(
            last_slot,
            RowId {
                page_num: 0,
                slot_num: u16::MAX
            }
        );
        assert_eq!(
            next_page,
            RowId {
                page_num: 1,
                slot_num: 0
            }
        );
    }

    #[test]
    fn deleting_an_earlier_key_does_not_change_scan_row_identity() {
        let (storage, schema, relations) = storage_with_users();
        let setup = memory_statement_context(1);
        storage
            .insert(&setup, relations.as_ref(), schema.id, user(1, "first"))
            .unwrap();
        storage
            .insert(&setup, relations.as_ref(), schema.id, user(2, "second"))
            .unwrap();
        let mut before = storage.scan(&setup, relations.as_ref(), schema.id).unwrap();
        before.next().unwrap().unwrap();
        let second_before = before.next().unwrap().unwrap();

        storage
            .delete(
                &memory_statement_context(2),
                relations.as_ref(),
                schema.id,
                &Key(vec![Value::Integer(1)]),
            )
            .unwrap();
        let mut after = storage
            .scan(&memory_statement_context(3), relations.as_ref(), schema.id)
            .unwrap();
        let second_after = after.next().unwrap().unwrap();
        assert_eq!(second_after.row_id, second_before.row_id);
        assert_eq!(second_after.key, second_before.key);
    }
}
