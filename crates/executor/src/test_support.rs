use catalog::{CatalogManager, MemoryCatalog, ResolvedForeignKey};
use common::{
    ColumnId, ColumnInfo, ConflictWaiter, CopyOptions, DataType, DbError, ForeignKeyAction,
    IndexId, IndexSchema, Key, KeyRange, ParsedColumnDef, QueryCancel, Result, Row, RowId,
    RowIdentity, SqlState, SsiTracker, StatementContext, StoredRow, TableId, TableSchema,
    TupleLockAcquire, TupleLockManager, TupleLockMode, TupleLockTag, TupleLockWaitPolicy,
    TxnStatus, Value,
};
use planner::{ExplainAnalysis, PhysicalPlan, bind, format_explain, logical_plan, physical_plan};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use storage::{
    DependentRowProbe, LockRowResult, LockedRow, RelationSnapshot, RowIterator, SchemaOperations,
    StorageEngine,
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

struct CommittingMemoryWaiter {
    storage: Arc<MemoryStorage>,
    blocker: u64,
}

impl std::fmt::Debug for CommittingMemoryWaiter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittingMemoryWaiter")
            .field("blocker", &self.blocker)
            .finish_non_exhaustive()
    }
}

impl ConflictWaiter for CommittingMemoryWaiter {
    fn wait_for(&self, _waiter: u64, blocker: u64, cancel: &QueryCancel) -> Result<()> {
        cancel.check()?;
        if blocker != self.blocker {
            return Err(DbError::internal("unexpected executor-harness blocker"));
        }
        self.storage.commit_txn(blocker)
    }
}

pub struct ExecutorHarness {
    catalog: Arc<MemoryCatalog>,
    storage: Arc<MemoryStorage>,
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
        let storage = Arc::new(MemoryStorage::empty());
        storage
            .create_table(&memory_statement_context(0), &schema)
            .unwrap();
        let primary_key = catalog
            .create_primary_key_index(
                common::PUBLIC_SCHEMA_ID,
                "users_pkey".to_string(),
                schema.id,
                &["id".to_string()],
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

    pub fn add_table(
        &self,
        name: &str,
        columns: &[(&str, DataType, bool)],
        primary_key: &[&str],
    ) -> TableSchema {
        let schema = self
            .catalog
            .create_table(
                name.to_string(),
                columns
                    .iter()
                    .map(|(name, data_type, nullable)| ParsedColumnDef {
                        name: (*name).to_string(),
                        data_type: data_type.clone(),
                        nullable: *nullable,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    })
                    .collect(),
                primary_key.iter().map(|name| (*name).to_string()).collect(),
                common::CompressionSetting::None,
            )
            .unwrap();
        self.storage
            .create_table(&memory_statement_context(0), &schema)
            .unwrap();
        if !primary_key.is_empty() {
            let index = self
                .catalog
                .create_primary_key_index(
                    common::PUBLIC_SCHEMA_ID,
                    format!("{name}_pkey"),
                    schema.id,
                    &primary_key
                        .iter()
                        .map(|column| (*column).to_string())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            self.storage
                .create_index(&memory_statement_context(0), &index, 0)
                .unwrap();
        }
        schema
    }

    pub fn add_unique_constraint(&self, table: &str, columns: &[&str]) -> IndexSchema {
        let table_id = self.catalog.get_table_by_name(table).unwrap().unwrap().id;
        let index = self
            .catalog
            .create_unique_constraint_index(
                common::PUBLIC_SCHEMA_ID,
                format!("{}_{}_key", table, columns.join("_")),
                table_id,
                &columns
                    .iter()
                    .map(|column| (*column).to_string())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        self.storage
            .create_index(&memory_statement_context(0), &index, 0)
            .unwrap();
        index
    }

    pub fn add_foreign_key(
        &self,
        name: &str,
        child: &str,
        columns: &[&str],
        parent: &str,
        referenced_columns: &[&str],
    ) {
        self.add_foreign_key_with_actions(
            name,
            child,
            columns,
            parent,
            referenced_columns,
            ForeignKeyAction::NoAction,
            ForeignKeyAction::NoAction,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_foreign_key_with_actions(
        &self,
        name: &str,
        child: &str,
        columns: &[&str],
        parent: &str,
        referenced_columns: &[&str],
        on_update: ForeignKeyAction,
        on_delete: ForeignKeyAction,
    ) {
        let child_schema = self.catalog.get_table_by_name(child).unwrap().unwrap();
        let parent_schema = self.catalog.get_table_by_name(parent).unwrap().unwrap();
        let resolve = |schema: &TableSchema, names: &[&str]| {
            names
                .iter()
                .map(|name| {
                    schema
                        .columns
                        .iter()
                        .find(|column| column.name == *name)
                        .unwrap()
                        .id
                })
                .collect::<Vec<_>>()
        };
        self.catalog
            .attach_foreign_keys(
                child_schema.id,
                vec![ResolvedForeignKey {
                    name: Some(name.to_string()),
                    columns: resolve(&child_schema, columns),
                    referenced_table: parent_schema.id,
                    referenced_columns: resolve(&parent_schema, referenced_columns),
                    on_update,
                    on_delete,
                }],
            )
            .unwrap();
    }

    pub fn insert_uncommitted(&self, table: &str, xid: u64, values: Vec<Value>) -> Result<()> {
        let schema = self
            .catalog
            .get_table_by_name(table)?
            .ok_or_else(|| undefined_table_by_name(table))?;
        let relations = self.storage.capture_relation_snapshot()?;
        self.storage.insert(
            &memory_statement_context(xid),
            relations.as_ref(),
            schema.id,
            Row { values },
        )?;
        Ok(())
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
        self.execute_with_runtime(sql, cancel, spill, None, None)
    }

    pub fn execute_with_ssi_tracker(
        &self,
        sql: &str,
        tracker: Arc<dyn SsiTracker>,
    ) -> Result<ExecutionResult> {
        self.execute_with_runtime(
            sql,
            &QueryCancel::new(),
            spill::SpillConfig::default(),
            Some(tracker),
            None,
        )
    }

    pub fn execute_after_committing_blocker(
        &self,
        sql: &str,
        blocker: u64,
    ) -> Result<ExecutionResult> {
        self.execute_with_runtime(
            sql,
            &QueryCancel::new(),
            spill::SpillConfig::default(),
            None,
            Some(blocker),
        )
    }

    fn execute_with_runtime(
        &self,
        sql: &str,
        cancel: &QueryCancel,
        spill: spill::SpillConfig,
        ssi_tracker: Option<Arc<dyn SsiTracker>>,
        committing_blocker: Option<u64>,
    ) -> Result<ExecutionResult> {
        let statement = parser::parse(sql)?;
        let bound = bind(&statement, self.catalog.as_ref())?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.catalog.as_ref())?;
        let is_read = is_read_plan(&physical);
        let mut statement = memory_statement_context(if is_read { 0 } else { 1 });
        if let Some(ssi_tracker) = ssi_tracker {
            statement = statement.with_ssi_tracker(ssi_tracker);
        }
        if let Some(blocker) = committing_blocker {
            statement = statement.with_conflict_waiter(
                Arc::new(CommittingMemoryWaiter {
                    storage: Arc::clone(&self.storage),
                    blocker,
                }),
                Arc::new(QueryCancel::new()),
            );
        }
        let txn_id = statement.txn_id;
        let ctx = ExecutionContext {
            statement,
            relations: self.storage.capture_relation_snapshot()?,
            catalog: self.catalog.clone(),
            allocator_catalog: None,
            storage: self.storage.as_ref(),
            schema_ops: self.storage.as_ref(),
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
            allocator_catalog: None,
            storage: self.storage.as_ref(),
            schema_ops: self.storage.as_ref(),
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
            allocator_catalog: None,
            storage: self.storage.as_ref(),
            schema_ops: self.storage.as_ref(),
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
            allocator_catalog: None,
            storage: self.storage.as_ref(),
            schema_ops: self.storage.as_ref(),
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
            allocator_catalog: None,
            storage: self.storage.as_ref(),
            schema_ops: self.storage.as_ref(),
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

pub(crate) fn memory_statement_context(txn_id: u64) -> StatementContext {
    StatementContext::new(txn_id).with_tuple_lock_manager(Arc::new(PermissiveMemoryTupleLocks))
}

fn restore_memory_lock_change(
    ctx: &StatementContext,
    change: Option<common::TupleLockGrantChange>,
) -> Result<()> {
    match change {
        Some(change) => ctx
            .tuple_locks
            .restore_tuple_grants(ctx.txn_id, vec![change]),
        None => Ok(()),
    }
}

fn restore_memory_lock_change_after_error<T>(
    ctx: &StatementContext,
    change: Option<common::TupleLockGrantChange>,
    original: DbError,
) -> Result<T> {
    match restore_memory_lock_change(ctx, change) {
        Ok(()) => Err(original),
        Err(restore) => Err(DbError::internal(format!(
            "foreign-key probe failed ({original}); restoring its tuple-lock grant also failed ({restore})"
        ))),
    }
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
    probe_txn_statuses: BTreeMap<u64, TxnStatus>,
    probe_deleters: BTreeMap<(TableId, Key, RowId, u64), u64>,
    probe_pending_rows: BTreeMap<(TableId, Key), Vec<MemoryStoredRow>>,
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
    predecessors: Vec<(RowId, u64)>,
}

enum MemoryProbeCandidate<T> {
    Missing,
    Wait(u64),
    Found(T),
}

enum MemoryProbeRowState {
    Dead,
    Wait(u64),
    Live,
}

fn memory_probe_creator_state(
    state: &MemoryStorageState,
    ctx: &StatementContext,
    xmin: u64,
) -> TxnStatus {
    if ctx.live_txns.contains(&xmin) {
        TxnStatus::Committed
    } else {
        state
            .probe_txn_statuses
            .get(&xmin)
            .copied()
            .unwrap_or(TxnStatus::Committed)
    }
}

fn memory_probe_row_state(
    state: &MemoryStorageState,
    ctx: &StatementContext,
    table: TableId,
    identity_key: &Key,
    row_id: RowId,
    xmin: u64,
) -> MemoryProbeRowState {
    match memory_probe_creator_state(state, ctx, xmin) {
        TxnStatus::Aborted => return MemoryProbeRowState::Dead,
        TxnStatus::InProgress => return MemoryProbeRowState::Wait(xmin),
        TxnStatus::Committed => {}
    }
    let Some(deleter) = state
        .probe_deleters
        .get(&(table, identity_key.clone(), row_id, xmin))
    else {
        return MemoryProbeRowState::Live;
    };
    if ctx.live_txns.contains(deleter) {
        return MemoryProbeRowState::Dead;
    }
    match state
        .probe_txn_statuses
        .get(deleter)
        .copied()
        .unwrap_or(TxnStatus::Committed)
    {
        TxnStatus::Aborted => MemoryProbeRowState::Live,
        TxnStatus::InProgress => MemoryProbeRowState::Wait(*deleter),
        TxnStatus::Committed => MemoryProbeRowState::Dead,
    }
}

fn memory_probe_rows(state: &MemoryStorageState, table: TableId) -> Vec<(Key, MemoryStoredRow)> {
    let mut rows = state
        .rows
        .get(&table)
        .map(|rows| {
            rows.iter()
                .map(|(key, row)| (key.clone(), row.clone()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    rows.extend(
        state
            .probe_pending_rows
            .iter()
            .filter(|((pending_table, _), _)| *pending_table == table)
            .flat_map(|((_, key), rows)| rows.iter().cloned().map(|row| (key.clone(), row))),
    );
    rows
}

fn ensure_memory_current_visible(ctx: &StatementContext, identity: &RowIdentity) -> Result<()> {
    if ctx.isolation == common::IsolationLevel::ReadCommitted
        || ctx.live_txns.contains(&identity.xmin)
        || (identity.xmin < ctx.snapshot.xmax && !ctx.snapshot.xip.contains(&identity.xmin))
    {
        return Ok(());
    }
    Err(DbError::execute(
        SqlState::SerializationFailure,
        "could not serialize access due to concurrent foreign key change",
    ))
}

impl MemoryStorage {
    fn lock_row_with_change(
        &self,
        ctx: &StatementContext,
        table: TableId,
        identity: &RowIdentity,
        mode: TupleLockMode,
        wait_policy: TupleLockWaitPolicy,
    ) -> Result<(LockRowResult, Option<common::TupleLockGrantChange>)> {
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
            TupleLockAcquire::Skipped => return Ok((LockRowResult::Skipped, None)),
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
                        (stored.row_id == identity.row_id && stored.xmin == identity.xmin)
                            || stored
                                .predecessors
                                .contains(&(identity.row_id, identity.xmin))
                    })
                    .map(|stored| {
                        (
                            RowIdentity {
                                row_id: stored.row_id,
                                xmin: stored.xmin,
                                key: identity.key.clone(),
                            },
                            stored.row.clone(),
                        )
                    })
            });
        match lookup {
            Ok(Some((current_identity, row))) => Ok((
                LockRowResult::Locked(LockedRow::from_lock_grant(
                    table,
                    ctx.txn_id,
                    current_identity,
                    row,
                    mode,
                )),
                Some(change),
            )),
            Ok(None) => {
                ctx.tuple_locks
                    .restore_tuple_grants(ctx.txn_id, vec![change])?;
                Ok((LockRowResult::Deleted, None))
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
            let previous = state
                .rows
                .get_mut(&table)
                .expect("validated table rows")
                .remove(&target.identity().key)
                .ok_or_else(|| DbError::internal("validated row disappeared"))?;
            let pending_key = (table, target.identity().key.clone());
            let deleter_key = (
                table,
                target.identity().key.clone(),
                previous.row_id,
                previous.xmin,
            );
            state
                .probe_pending_rows
                .entry(pending_key.clone())
                .or_default()
                .push(previous);
            state.probe_deleters.insert(deleter_key, ctx.txn_id);
            return Ok(true);
        };
        let replacement_key =
            storage_identity_key_for_update(&schema, &target.identity().key, &row)?;
        validate_unique_indexes(&state, &schema, Some(&target.identity().key), &row)?;
        let replacement_row_id = allocate_memory_row_id(&mut state)?;
        let previous = {
            let rows = state.rows.get_mut(&table).expect("validated table rows");
            if replacement_key != target.identity().key && rows.contains_key(&replacement_key) {
                return Err(duplicate_storage_identity_error(&schema));
            }
            rows.remove(&target.identity().key)
                .expect("validated locked row")
        };
        let pending_key = (table, target.identity().key.clone());
        let deleter_key = (
            table,
            target.identity().key.clone(),
            previous.row_id,
            previous.xmin,
        );
        state
            .probe_pending_rows
            .entry(pending_key.clone())
            .or_default()
            .push(previous.clone());
        state.probe_deleters.insert(deleter_key, ctx.txn_id);
        let mut predecessors = previous.predecessors;
        predecessors.push((previous.row_id, previous.xmin));
        state
            .rows
            .get_mut(&table)
            .expect("validated table rows")
            .insert(
                replacement_key,
                MemoryStoredRow {
                    row_id: replacement_row_id,
                    xmin: ctx.txn_id,
                    row,
                    predecessors,
                },
            );
        Ok(true)
    }

    fn update_with_lock_mode(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
        mode: TupleLockMode,
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
            mode,
            TupleLockWaitPolicy::Block,
        )? {
            LockRowResult::Locked(target) if target.identity() == &identity => {
                self.update_locked(ctx, relations, table, &target, row)
            }
            LockRowResult::Locked(_) | LockRowResult::Deleted => {
                Err(memory_concurrent_update_error())
            }
            LockRowResult::Skipped => Err(DbError::internal(
                "blocking test-storage update skipped a row",
            )),
        }
    }

    fn probe_memory_referenced_key_locked(
        &self,
        ctx: &StatementContext,
        table: TableId,
        access_index: IndexId,
        key: &Key,
        mode: TupleLockMode,
    ) -> Result<Option<LockedRow>> {
        loop {
            let candidate = {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| DbError::internal("storage lock poisoned"))?;
                let schema = state
                    .schemas
                    .get(&table)
                    .ok_or_else(|| undefined_table(table))?;
                let columns = if access_index == common::PRIMARY_KEY_INDEX_ID {
                    if schema.primary_key.is_empty() {
                        return Err(DbError::internal(
                            "foreign-key probe selected a missing test-storage primary key",
                        ));
                    }
                    schema.primary_key.clone()
                } else {
                    let index = state
                        .indexes
                        .get(&access_index)
                        .filter(|index| index.table == table && index.constraint.is_some())
                        .ok_or_else(|| undefined_index(access_index))?;
                    index.columns.clone()
                };
                let mut candidate = MemoryProbeCandidate::Missing;
                for (identity_key, stored) in memory_probe_rows(&state, table) {
                    ctx.cancel.check()?;
                    if key_for_columns(schema, &columns, &stored.row)? == *key {
                        match memory_probe_row_state(
                            &state,
                            ctx,
                            table,
                            &identity_key,
                            stored.row_id,
                            stored.xmin,
                        ) {
                            MemoryProbeRowState::Dead => continue,
                            MemoryProbeRowState::Wait(blocker) => {
                                candidate = MemoryProbeCandidate::Wait(blocker);
                                break;
                            }
                            MemoryProbeRowState::Live => {
                                candidate = MemoryProbeCandidate::Found((
                                    RowIdentity {
                                        row_id: stored.row_id,
                                        xmin: stored.xmin,
                                        key: identity_key,
                                    },
                                    columns.clone(),
                                    schema.clone(),
                                ));
                                break;
                            }
                        }
                    }
                }
                candidate
            };
            let (identity, columns, schema) = match candidate {
                MemoryProbeCandidate::Missing => return Ok(None),
                MemoryProbeCandidate::Wait(blocker) => {
                    ctx.conflict_waiter
                        .wait_for(ctx.txn_id, blocker, ctx.cancel.as_ref())?;
                    continue;
                }
                MemoryProbeCandidate::Found(candidate) => candidate,
            };
            let (lock_result, change) =
                self.lock_row_with_change(ctx, table, &identity, mode, TupleLockWaitPolicy::Block)?;
            match lock_result {
                LockRowResult::Locked(locked) => {
                    let current_key = match key_for_columns(&schema, &columns, locked.row()) {
                        Ok(current_key) => current_key,
                        Err(err) => {
                            return restore_memory_lock_change_after_error(ctx, change, err);
                        }
                    };
                    if current_key == *key {
                        if let Err(err) = ensure_memory_current_visible(ctx, locked.identity()) {
                            return restore_memory_lock_change_after_error(ctx, change, err);
                        }
                        return Ok(Some(locked));
                    }
                    restore_memory_lock_change(ctx, change)?;
                }
                LockRowResult::Deleted => {}
                LockRowResult::Skipped => {
                    return Err(DbError::internal(
                        "blocking foreign-key test-storage probe skipped a row",
                    ));
                }
            }
        }
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
        if !schema.primary_key.is_empty() {
            drop(state);
            let _retained_reservation = storage::reserve_unique_key(ctx, table, &key)?;
            state = self
                .state
                .lock()
                .map_err(|_| DbError::internal("storage lock poisoned"))?;
        }
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
                predecessors: Vec::new(),
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

    fn referenced_key_exists(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        access_index: IndexId,
        key: &Key,
    ) -> Result<bool> {
        Ok(self
            .probe_memory_referenced_key_locked(
                ctx,
                table,
                access_index,
                key,
                TupleLockMode::KeyShare,
            )?
            .is_some())
    }

    fn lock_unique_conflict(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        mode: TupleLockMode,
    ) -> Result<Option<LockedRow>> {
        loop {
            let reservation = storage::reserve_unique_key(ctx, table, key)?;
            if self
                .probe_memory_referenced_key_locked(
                    ctx,
                    table,
                    common::PRIMARY_KEY_INDEX_ID,
                    key,
                    TupleLockMode::NoKeyUpdate,
                )?
                .is_none()
            {
                return Ok(None);
            }
            ctx.tuple_locks
                .restore_tuple_grants(ctx.txn_id, vec![reservation])?;
            if let Some(locked) = self.probe_memory_referenced_key_locked(
                ctx,
                table,
                common::PRIMARY_KEY_INDEX_ID,
                key,
                mode,
            )? {
                return Ok(Some(locked));
            }
        }
    }

    fn dependent_row_exists(
        &self,
        ctx: &StatementContext,
        _relations: &dyn RelationSnapshot,
        probe: DependentRowProbe<'_>,
    ) -> Result<bool> {
        let DependentRowProbe {
            table,
            columns,
            key,
            supporting_index,
            excluded,
        } = probe;
        loop {
            let candidate = {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| DbError::internal("storage lock poisoned"))?;
                let schema = state
                    .schemas
                    .get(&table)
                    .ok_or_else(|| undefined_table(table))?;
                if let Some(index_id) = supporting_index {
                    if index_id == common::PRIMARY_KEY_INDEX_ID {
                        if schema.primary_key != columns {
                            return Err(DbError::internal(
                                "foreign-key test-storage primary index does not match child columns",
                            ));
                        }
                    } else if !state
                        .indexes
                        .get(&index_id)
                        .is_some_and(|index| index.table == table && index.columns == columns)
                    {
                        return Err(DbError::internal(
                            "foreign-key test-storage child index metadata is inconsistent",
                        ));
                    }
                }
                let mut candidate = MemoryProbeCandidate::Missing;
                for (identity_key, stored) in memory_probe_rows(&state, table) {
                    ctx.cancel.check()?;
                    let identity = RowIdentity {
                        row_id: stored.row_id,
                        xmin: stored.xmin,
                        key: identity_key.clone(),
                    };
                    if excluded.is_some_and(|excluded| excluded == &identity)
                        || key_for_columns(schema, columns, &stored.row)? != *key
                    {
                        continue;
                    }
                    match memory_probe_row_state(
                        &state,
                        ctx,
                        table,
                        &identity_key,
                        stored.row_id,
                        stored.xmin,
                    ) {
                        MemoryProbeRowState::Dead => {}
                        MemoryProbeRowState::Wait(blocker) => {
                            candidate = MemoryProbeCandidate::Wait(blocker);
                            break;
                        }
                        MemoryProbeRowState::Live => {
                            candidate = MemoryProbeCandidate::Found(identity);
                            break;
                        }
                    }
                }
                candidate
            };
            match candidate {
                MemoryProbeCandidate::Missing => return Ok(false),
                MemoryProbeCandidate::Wait(blocker) => {
                    ctx.conflict_waiter
                        .wait_for(ctx.txn_id, blocker, ctx.cancel.as_ref())?;
                }
                MemoryProbeCandidate::Found(identity) => {
                    ensure_memory_current_visible(ctx, &identity)?;
                    return Ok(true);
                }
            }
        }
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
        let (result, _retained_change) =
            self.lock_row_with_change(ctx, table, identity, mode, wait_policy)?;
        Ok(result)
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
            LockRowResult::Locked(target) if target.identity() == &identity => {
                self.delete_locked(ctx, relations, table, &target)
            }
            LockRowResult::Locked(_) => Err(memory_concurrent_update_error()),
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
        drop(state);
        let mode = if replacement_key == *key {
            TupleLockMode::NoKeyUpdate
        } else {
            TupleLockMode::Update
        };
        self.update_with_lock_mode(ctx, relations, table, key, row, mode)
    }

    fn update_requiring_update_lock(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
    ) -> Result<bool> {
        self.update_with_lock_mode(ctx, relations, table, key, row, TupleLockMode::Update)
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
        state.probe_txn_statuses.insert(txn_id, TxnStatus::Aborted);
        clear_memory_probe_deletions(&mut state, txn_id);
        Ok(())
    }

    fn commit_txn(&self, txn_id: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("storage lock poisoned"))?;
        state.savepoints.remove(&txn_id);
        state
            .probe_txn_statuses
            .insert(txn_id, TxnStatus::Committed);
        clear_memory_probe_deletions(&mut state, txn_id);
        Ok(())
    }
}

impl SchemaOperations for MemoryStorage {
    fn apply_catalog_change(
        &self,
        _ctx: &StatementContext,
        _change_set: &common::CatalogChangeSet,
    ) -> Result<()> {
        Ok(())
    }

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
    state
        .probe_txn_statuses
        .insert(txn_id, TxnStatus::InProgress);
}

fn clear_memory_probe_deletions(state: &mut MemoryStorageState, txn_id: u64) {
    let keys = state
        .probe_deleters
        .iter()
        .filter(|(_, deleter)| **deleter == txn_id)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in keys {
        state.probe_deleters.remove(&key);
        state.probe_pending_rows.remove(&(key.0, key.1));
    }
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
                    if index.columns == schema.primary_key {
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

fn key_for_columns(schema: &TableSchema, columns: &[ColumnId], row: &Row) -> Result<Key> {
    row_key_for_columns(schema, columns, row).map(|(key, _has_null)| key)
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

    #[derive(Debug, Default)]
    struct ExclusiveReservationLocks {
        holder: Mutex<Option<(u64, TupleLockTag)>>,
    }

    impl TupleLockManager for ExclusiveReservationLocks {
        fn acquire_tuple(
            &self,
            xid: u64,
            tag: &TupleLockTag,
            _mode: TupleLockMode,
            _wait_policy: TupleLockWaitPolicy,
            cancel: &QueryCancel,
        ) -> Result<TupleLockAcquire> {
            cancel.check()?;
            let mut holder = self
                .holder
                .lock()
                .map_err(|_| DbError::internal("test reservation lock poisoned"))?;
            match holder.as_ref() {
                Some((owner, held_tag)) if *owner != xid && held_tag == tag => {
                    Err(DbError::execute(
                        SqlState::LockNotAvailable,
                        "test unique-key reservation is held",
                    ))
                }
                Some(_) => Ok(TupleLockAcquire::Acquired(
                    TupleLockGrantChange::manager_receipt(()),
                )),
                None => {
                    *holder = Some((xid, tag.clone()));
                    Ok(TupleLockAcquire::Acquired(
                        TupleLockGrantChange::manager_receipt(()),
                    ))
                }
            }
        }

        fn restore_tuple_grants(
            &self,
            _xid: u64,
            _changes: Vec<TupleLockGrantChange>,
        ) -> Result<()> {
            Ok(())
        }

        fn holds_tuple(&self, xid: u64, tag: &TupleLockTag, _mode: TupleLockMode) -> bool {
            self.holder
                .lock()
                .map(|holder| {
                    holder
                        .as_ref()
                        .is_some_and(|(owner, held_tag)| *owner == xid && held_tag == tag)
                })
                .unwrap_or(false)
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
        restored: AtomicUsize,
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
                self.storage.commit_txn(99)?;
            }
            Ok(TupleLockAcquire::Acquired(
                TupleLockGrantChange::manager_receipt(()),
            ))
        }

        fn restore_tuple_grants(
            &self,
            _xid: u64,
            changes: Vec<TupleLockGrantChange>,
        ) -> Result<()> {
            self.restored.fetch_add(changes.len(), Ordering::SeqCst);
            Ok(())
        }

        fn holds_tuple(&self, _xid: u64, _tag: &TupleLockTag, _mode: TupleLockMode) -> bool {
            true
        }
    }

    struct SettlingProbeWaiter {
        storage: Arc<MemoryStorage>,
        blocker: u64,
        final_status: TxnStatus,
        waits: AtomicUsize,
    }

    impl std::fmt::Debug for SettlingProbeWaiter {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("SettlingProbeWaiter")
                .field("blocker", &self.blocker)
                .finish_non_exhaustive()
        }
    }

    impl common::ConflictWaiter for SettlingProbeWaiter {
        fn wait_for(&self, _waiter: u64, blocker: u64, cancel: &QueryCancel) -> Result<()> {
            cancel.check()?;
            if blocker != self.blocker {
                return Err(DbError::internal("unexpected memory probe blocker"));
            }
            self.storage
                .state
                .lock()
                .map_err(|_| DbError::internal("storage lock poisoned"))?
                .probe_txn_statuses
                .insert(blocker, self.final_status);
            self.waits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum ProbeFinalization {
        Commit,
        Rollback,
    }

    struct FinalizingProbeWaiter {
        storage: Arc<MemoryStorage>,
        blocker: u64,
        finalization: ProbeFinalization,
    }

    impl std::fmt::Debug for FinalizingProbeWaiter {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("FinalizingProbeWaiter")
                .field("blocker", &self.blocker)
                .finish_non_exhaustive()
        }
    }

    impl common::ConflictWaiter for FinalizingProbeWaiter {
        fn wait_for(&self, _waiter: u64, blocker: u64, cancel: &QueryCancel) -> Result<()> {
            cancel.check()?;
            if blocker != self.blocker {
                return Err(DbError::internal("unexpected memory probe blocker"));
            }
            match self.finalization {
                ProbeFinalization::Commit => self.storage.commit_txn(blocker),
                ProbeFinalization::Rollback => self.storage.rollback_txn(blocker),
            }
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
    fn on_conflict_missing_probe_reserves_key_against_a_late_insert() {
        let (storage, schema, relations) = storage_with_users();
        let locks = Arc::new(ExclusiveReservationLocks::default());
        let key = Key(vec![Value::Integer(1)]);
        let arbiter = memory_statement_context(10).with_tuple_lock_manager(locks.clone());
        assert!(
            storage
                .lock_unique_conflict(
                    &arbiter,
                    relations.as_ref(),
                    schema.id,
                    &key,
                    TupleLockMode::KeyShare,
                )
                .unwrap()
                .is_none()
        );

        let competing = memory_statement_context(20).with_tuple_lock_manager(locks);
        let err = storage
            .insert(&competing, relations.as_ref(), schema.id, user(1, "late"))
            .unwrap_err();
        assert_eq!(err.code, SqlState::LockNotAvailable);
        assert_eq!(
            storage
                .get(&arbiter, relations.as_ref(), schema.id, &key)
                .unwrap(),
            None
        );
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
                restored: AtomicUsize::new(0),
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
                restored: AtomicUsize::new(0),
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
    fn referenced_probe_restores_rejected_intervening_update_lock() {
        let (storage, schema, relations) = storage_with_users();
        let storage = Arc::new(storage);
        let index = IndexSchema {
            id: 1,
            schema_id: schema.schema_id,
            storage_id: 101,
            table: schema.id,
            name: "users_name_key".to_string(),
            columns: vec![1],
            unique: true,
            constraint: Some(1),
        };
        storage
            .create_index(&memory_statement_context(0), &index, 0)
            .unwrap();
        storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                user(1, "old"),
            )
            .unwrap();
        storage.commit_txn(1).unwrap();
        let locks = Arc::new(InterveningTupleLocks {
            storage: Arc::clone(&storage),
            table: schema.id,
            mutation: Mutex::new(Some(InterveningMutation::Update(user(1, "new")))),
            restored: AtomicUsize::new(0),
        });
        let probe = memory_statement_context(2).with_tuple_lock_manager(locks.clone());
        assert!(
            !storage
                .referenced_key_exists(
                    &probe,
                    relations.as_ref(),
                    schema.id,
                    index.id,
                    &Key(vec![Value::Text("old".to_string())]),
                )
                .unwrap()
        );
        assert_eq!(locks.restored.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn foreign_key_probe_double_waits_cancels_and_enforces_retained_snapshot() {
        let (storage, schema, relations) = storage_with_users();
        let storage = Arc::new(storage);
        storage
            .insert(
                &memory_statement_context(10),
                relations.as_ref(),
                schema.id,
                user(1, "parent"),
            )
            .unwrap();
        storage
            .state
            .lock()
            .unwrap()
            .probe_txn_statuses
            .insert(10, TxnStatus::InProgress);
        let waiter = Arc::new(SettlingProbeWaiter {
            storage: Arc::clone(&storage),
            blocker: 10,
            final_status: TxnStatus::Committed,
            waits: AtomicUsize::new(0),
        });
        let current = StatementContext::with_snapshot(
            20,
            Arc::new(common::Snapshot {
                xmin: 10,
                xmax: 21,
                xip: vec![10],
            }),
        )
        .with_tuple_lock_manager(Arc::new(TestTupleLocks))
        .with_conflict_waiter(waiter.clone(), Arc::new(QueryCancel::new()));
        assert!(
            storage
                .referenced_key_exists(
                    &current,
                    relations.as_ref(),
                    schema.id,
                    common::PRIMARY_KEY_INDEX_ID,
                    &Key(vec![Value::Integer(1)]),
                )
                .unwrap()
        );
        assert_eq!(waiter.waits.load(Ordering::SeqCst), 1);

        storage
            .state
            .lock()
            .unwrap()
            .probe_txn_statuses
            .insert(10, TxnStatus::InProgress);
        let waiter = Arc::new(SettlingProbeWaiter {
            storage: Arc::clone(&storage),
            blocker: 10,
            final_status: TxnStatus::Committed,
            waits: AtomicUsize::new(0),
        });
        let current = StatementContext::with_snapshot(
            20,
            Arc::new(common::Snapshot {
                xmin: 10,
                xmax: 21,
                xip: vec![10],
            }),
        )
        .with_tuple_lock_manager(Arc::new(TestTupleLocks))
        .with_conflict_waiter(waiter.clone(), Arc::new(QueryCancel::new()));
        assert!(
            storage
                .dependent_row_exists(
                    &current,
                    relations.as_ref(),
                    DependentRowProbe {
                        table: schema.id,
                        columns: &[0],
                        key: &Key(vec![Value::Integer(1)]),
                        supporting_index: Some(common::PRIMARY_KEY_INDEX_ID),
                        excluded: None,
                    },
                )
                .unwrap()
        );
        assert_eq!(waiter.waits.load(Ordering::SeqCst), 1);

        let identity_key = Key(vec![Value::Integer(1)]);
        {
            let mut state = storage.state.lock().unwrap();
            let stored = state
                .rows
                .get(&schema.id)
                .and_then(|rows| rows.get(&identity_key))
                .cloned()
                .unwrap();
            state.probe_deleters.insert(
                (schema.id, identity_key.clone(), stored.row_id, stored.xmin),
                50,
            );
            state.probe_txn_statuses.insert(50, TxnStatus::InProgress);
        }
        let waiter = Arc::new(SettlingProbeWaiter {
            storage: Arc::clone(&storage),
            blocker: 50,
            final_status: TxnStatus::Committed,
            waits: AtomicUsize::new(0),
        });
        let deleting = memory_statement_context(60)
            .with_conflict_waiter(waiter.clone(), Arc::new(QueryCancel::new()));
        assert!(
            !storage
                .referenced_key_exists(
                    &deleting,
                    relations.as_ref(),
                    schema.id,
                    common::PRIMARY_KEY_INDEX_ID,
                    &identity_key,
                )
                .unwrap()
        );
        assert_eq!(waiter.waits.load(Ordering::SeqCst), 1);

        storage
            .state
            .lock()
            .unwrap()
            .probe_txn_statuses
            .insert(50, TxnStatus::InProgress);
        let waiter = Arc::new(SettlingProbeWaiter {
            storage: Arc::clone(&storage),
            blocker: 50,
            final_status: TxnStatus::Aborted,
            waits: AtomicUsize::new(0),
        });
        let deleting = memory_statement_context(61)
            .with_conflict_waiter(waiter.clone(), Arc::new(QueryCancel::new()));
        assert!(
            storage
                .referenced_key_exists(
                    &deleting,
                    relations.as_ref(),
                    schema.id,
                    common::PRIMARY_KEY_INDEX_ID,
                    &identity_key,
                )
                .unwrap()
        );
        assert_eq!(waiter.waits.load(Ordering::SeqCst), 1);

        storage
            .state
            .lock()
            .unwrap()
            .probe_txn_statuses
            .insert(50, TxnStatus::InProgress);
        let waiter = Arc::new(SettlingProbeWaiter {
            storage: Arc::clone(&storage),
            blocker: 50,
            final_status: TxnStatus::Committed,
            waits: AtomicUsize::new(0),
        });
        let deleting = memory_statement_context(62)
            .with_conflict_waiter(waiter.clone(), Arc::new(QueryCancel::new()));
        assert!(
            !storage
                .dependent_row_exists(
                    &deleting,
                    relations.as_ref(),
                    DependentRowProbe {
                        table: schema.id,
                        columns: &[0],
                        key: &identity_key,
                        supporting_index: None,
                        excluded: None,
                    },
                )
                .unwrap()
        );
        assert_eq!(waiter.waits.load(Ordering::SeqCst), 1);

        storage
            .state
            .lock()
            .unwrap()
            .probe_txn_statuses
            .insert(50, TxnStatus::InProgress);
        let waiter = Arc::new(SettlingProbeWaiter {
            storage: Arc::clone(&storage),
            blocker: 50,
            final_status: TxnStatus::Aborted,
            waits: AtomicUsize::new(0),
        });
        let deleting = memory_statement_context(63)
            .with_conflict_waiter(waiter.clone(), Arc::new(QueryCancel::new()));
        assert!(
            storage
                .dependent_row_exists(
                    &deleting,
                    relations.as_ref(),
                    DependentRowProbe {
                        table: schema.id,
                        columns: &[0],
                        key: &identity_key,
                        supporting_index: None,
                        excluded: None,
                    },
                )
                .unwrap()
        );
        assert_eq!(waiter.waits.load(Ordering::SeqCst), 1);

        let cancel = Arc::new(QueryCancel::new());
        cancel.request(common::CancelReason::UserRequest);
        let canceled = memory_statement_context(30).with_conflict_waiter(waiter, cancel);
        let err = storage
            .dependent_row_exists(
                &canceled,
                relations.as_ref(),
                DependentRowProbe {
                    table: schema.id,
                    columns: &[0],
                    key: &Key(vec![Value::Integer(1)]),
                    supporting_index: None,
                    excluded: None,
                },
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::QueryCanceled);

        let retained = StatementContext::with_snapshot_and_isolation(
            40,
            Arc::new(common::Snapshot {
                xmin: 1,
                xmax: 10,
                xip: Vec::new(),
            }),
            common::IsolationLevel::RepeatableRead,
        )
        .with_tuple_lock_manager(Arc::new(TestTupleLocks));
        let err = storage
            .referenced_key_exists(
                &retained,
                relations.as_ref(),
                schema.id,
                common::PRIMARY_KEY_INDEX_ID,
                &Key(vec![Value::Integer(1)]),
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::SerializationFailure);
        let err = storage
            .dependent_row_exists(
                &retained,
                relations.as_ref(),
                DependentRowProbe {
                    table: schema.id,
                    columns: &[0],
                    key: &Key(vec![Value::Integer(1)]),
                    supporting_index: None,
                    excluded: None,
                },
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::SerializationFailure);
    }

    #[test]
    fn foreign_key_probe_double_tracks_normal_dml_creator_and_deleter_lifecycle() {
        let (storage, schema, relations) = storage_with_users();
        let storage = Arc::new(storage);
        storage
            .insert(
                &memory_statement_context(10),
                relations.as_ref(),
                schema.id,
                user(1, "committing creator"),
            )
            .unwrap();
        let probe = memory_statement_context(20).with_conflict_waiter(
            Arc::new(FinalizingProbeWaiter {
                storage: Arc::clone(&storage),
                blocker: 10,
                finalization: ProbeFinalization::Commit,
            }),
            Arc::new(QueryCancel::new()),
        );
        assert!(
            storage
                .referenced_key_exists(
                    &probe,
                    relations.as_ref(),
                    schema.id,
                    common::PRIMARY_KEY_INDEX_ID,
                    &Key(vec![Value::Integer(1)]),
                )
                .unwrap()
        );

        storage
            .insert(
                &memory_statement_context(11),
                relations.as_ref(),
                schema.id,
                user(2, "aborting creator"),
            )
            .unwrap();
        let probe = memory_statement_context(21).with_conflict_waiter(
            Arc::new(FinalizingProbeWaiter {
                storage: Arc::clone(&storage),
                blocker: 11,
                finalization: ProbeFinalization::Rollback,
            }),
            Arc::new(QueryCancel::new()),
        );
        assert!(
            !storage
                .dependent_row_exists(
                    &probe,
                    relations.as_ref(),
                    DependentRowProbe {
                        table: schema.id,
                        columns: &[0],
                        key: &Key(vec![Value::Integer(2)]),
                        supporting_index: Some(common::PRIMARY_KEY_INDEX_ID),
                        excluded: None,
                    },
                )
                .unwrap()
        );

        let delete = memory_statement_context(30);
        assert!(
            storage
                .delete(
                    &delete,
                    relations.as_ref(),
                    schema.id,
                    &Key(vec![Value::Integer(1)]),
                )
                .unwrap()
        );
        let probe = memory_statement_context(31).with_conflict_waiter(
            Arc::new(FinalizingProbeWaiter {
                storage: Arc::clone(&storage),
                blocker: 30,
                finalization: ProbeFinalization::Rollback,
            }),
            Arc::new(QueryCancel::new()),
        );
        assert!(
            storage
                .referenced_key_exists(
                    &probe,
                    relations.as_ref(),
                    schema.id,
                    common::PRIMARY_KEY_INDEX_ID,
                    &Key(vec![Value::Integer(1)]),
                )
                .unwrap()
        );

        let delete = memory_statement_context(32);
        assert!(
            storage
                .delete(
                    &delete,
                    relations.as_ref(),
                    schema.id,
                    &Key(vec![Value::Integer(1)]),
                )
                .unwrap()
        );
        let probe = memory_statement_context(33).with_conflict_waiter(
            Arc::new(FinalizingProbeWaiter {
                storage: Arc::clone(&storage),
                blocker: 32,
                finalization: ProbeFinalization::Commit,
            }),
            Arc::new(QueryCancel::new()),
        );
        assert!(
            !storage
                .dependent_row_exists(
                    &probe,
                    relations.as_ref(),
                    DependentRowProbe {
                        table: schema.id,
                        columns: &[0],
                        key: &Key(vec![Value::Integer(1)]),
                        supporting_index: None,
                        excluded: None,
                    },
                )
                .unwrap()
        );
    }

    #[test]
    fn foreign_key_probe_double_retains_transaction_start_image_across_repeated_updates() {
        let (storage, schema, relations) = storage_with_users();
        let storage = Arc::new(storage);
        let index = IndexSchema {
            id: 1,
            schema_id: schema.schema_id,
            storage_id: 101,
            table: schema.id,
            name: "users_name_key".to_string(),
            columns: vec![1],
            unique: true,
            constraint: Some(1),
        };
        storage
            .create_index(&memory_statement_context(0), &index, 0)
            .unwrap();
        storage
            .insert(
                &memory_statement_context(10),
                relations.as_ref(),
                schema.id,
                user(1, "original"),
            )
            .unwrap();
        storage.commit_txn(10).unwrap();
        let update = memory_statement_context(30);
        for name in ["middle", "final"] {
            assert!(
                storage
                    .update(
                        &update,
                        relations.as_ref(),
                        schema.id,
                        &Key(vec![Value::Integer(1)]),
                        user(1, name),
                    )
                    .unwrap()
            );
        }
        assert!(
            storage
                .dependent_row_exists(
                    &update,
                    relations.as_ref(),
                    DependentRowProbe {
                        table: schema.id,
                        columns: &[1],
                        key: &Key(vec![Value::Text("final".to_string())]),
                        supporting_index: Some(index.id),
                        excluded: None,
                    },
                )
                .unwrap()
        );
        let probe = memory_statement_context(40).with_conflict_waiter(
            Arc::new(FinalizingProbeWaiter {
                storage: Arc::clone(&storage),
                blocker: 30,
                finalization: ProbeFinalization::Rollback,
            }),
            Arc::new(QueryCancel::new()),
        );
        let original = Key(vec![Value::Text("original".to_string())]);
        assert!(
            storage
                .referenced_key_exists(&probe, relations.as_ref(), schema.id, index.id, &original,)
                .unwrap()
        );

        let update = memory_statement_context(31);
        for name in ["middle", "final"] {
            assert!(
                storage
                    .update(
                        &update,
                        relations.as_ref(),
                        schema.id,
                        &Key(vec![Value::Integer(1)]),
                        user(1, name),
                    )
                    .unwrap()
            );
        }
        let probe = memory_statement_context(41).with_conflict_waiter(
            Arc::new(FinalizingProbeWaiter {
                storage: Arc::clone(&storage),
                blocker: 31,
                finalization: ProbeFinalization::Commit,
            }),
            Arc::new(QueryCancel::new()),
        );
        assert!(
            !storage
                .dependent_row_exists(
                    &probe,
                    relations.as_ref(),
                    DependentRowProbe {
                        table: schema.id,
                        columns: &[1],
                        key: &original,
                        supporting_index: Some(index.id),
                        excluded: None,
                    },
                )
                .unwrap()
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
