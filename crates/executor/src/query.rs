use std::sync::atomic::{AtomicBool, Ordering};

use catalog::CatalogManager;
use common::{
    ColumnDefault, ColumnId, ColumnInfo, CopyOptions, DataType, DbError, ExecRow, IndexId, Key,
    KeyRange, ParsedColumnDef, Result, Row, SqlState, StatementContext, TableId, TableSchema,
    Value,
};
use planner::{BoundExpr, BoundOnConflict, BoundReturning, PhysicalPlan};
use storage::{RowIterator, SchemaOperations, StorageEngine};

use crate::ExecutionResult;
use crate::copy::{CopyParser, format_header, format_row};
use crate::eval_expr_with_context;
use crate::ops::{
    AggregateOp, DistinctOp, FilterOp, HashJoinOp, IndexScanOp, LimitOp, NestedLoopJoinOp,
    ProjectionOp, SeqScanOp, SortOp, ValuesOp,
};

pub struct ExecutionContext<'a> {
    pub statement: StatementContext,
    pub catalog: &'a dyn CatalogManager,
    pub storage: &'a dyn StorageEngine,
    pub schema_ops: &'a dyn SchemaOperations,
    /// The GC horizon (minimum advertised snapshot `xmin`) captured by the server,
    /// threaded into `CREATE INDEX` for its HOT broken-chain safety check
    /// (`docs/specs/mvcc.md` §10 Milestone H2). For non-DDL statements it is unused;
    /// the server sets it under the exclusive guard for DDL and to any value
    /// otherwise.
    pub gc_horizon: u64,
    /// Set from another connection's `CancelRequest`; the engine polls it
    /// between rows and aborts with `QueryCanceled` when it becomes true.
    pub cancel: &'a AtomicBool,
}

/// Abort with `QueryCanceled` if a cancellation has been requested. Called
/// between rows in the row-producing and write loops.
fn check_canceled(ctx: &ExecutionContext<'_>) -> Result<()> {
    if ctx.cancel.load(Ordering::Relaxed) {
        return Err(DbError::execute(
            SqlState::QueryCanceled,
            "canceling statement due to user request",
        ));
    }
    Ok(())
}

pub trait PlanExecutor {
    fn output_schema(&self) -> &[ColumnInfo];
    fn open(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<ExecRow>>;
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<ExecRow>> {
        let mut rows = Vec::new();
        for _ in 0..max_rows {
            let Some(row) = self.next()? else {
                break;
            };
            rows.push(row);
        }
        Ok(rows)
    }
    fn close(&mut self) -> Result<()>;
}

pub struct QueryEngine;

impl QueryEngine {
    pub fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult> {
        // Resolve uncorrelated subqueries to constants once, up front, so the
        // operators below evaluate only ordinary expressions.
        let resolved = crate::subquery::resolve_plan_subqueries(ctx, plan)?;
        let plan = &resolved;
        match plan {
            PhysicalPlan::CreateTable {
                name,
                columns,
                primary_key,
                unique,
            } => execute_create_table(ctx, name, columns, primary_key, unique),
            PhysicalPlan::DropTable { table } => execute_drop_table(ctx, *table),
            PhysicalPlan::CreateIndex {
                name,
                table,
                columns,
                unique,
            } => execute_create_index(ctx, name, table, columns, *unique),
            PhysicalPlan::DropIndex { index } => execute_drop_index(ctx, *index),
            PhysicalPlan::CreateSequence { name, options } => {
                execute_create_sequence(ctx, name, options)
            }
            PhysicalPlan::DropSequence { name, if_exists } => {
                execute_drop_sequence(ctx, name, *if_exists)
            }
            PhysicalPlan::Insert {
                table,
                columns,
                source,
                on_conflict,
                returning,
            } => execute_insert(
                ctx,
                *table,
                columns,
                source,
                on_conflict.as_ref(),
                returning.as_ref(),
            ),
            PhysicalPlan::Update {
                table,
                assignments,
                source,
                returning,
            } => execute_update(ctx, *table, assignments, source, returning.as_ref()),
            PhysicalPlan::Delete {
                table,
                source,
                returning,
            } => execute_delete(ctx, *table, source, returning.as_ref()),
            _ => execute_query(ctx, plan),
        }
    }
}

pub(crate) fn build_executor<'a>(
    ctx: &'a ExecutionContext<'_>,
    plan: &PhysicalPlan,
) -> Result<Box<dyn PlanExecutor + 'a>> {
    match plan {
        PhysicalPlan::SeqScan { table, filter, .. } => {
            // SSI: a sequential scan reads the whole relation (`docs/specs/ssi.md` §5).
            // No-op unless this is a SERIALIZABLE statement (NoSsiTracker otherwise).
            ctx.statement
                .ssi_tracker
                .record_relation_read(ctx.statement.txn_id, *table);
            Ok(Box::new(SeqScanOp::new(
                ctx.statement.clone(),
                ctx.storage,
                *table,
                filter.clone(),
                table_output_schema(ctx.catalog, *table)?,
            )))
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            range,
            filter,
            ..
        } => {
            // SSI: an exact-key lookup reads one tuple (recorded even when no row
            // matches, so a later insert of that key is caught as a phantom); a range
            // scan reads the whole relation (`docs/specs/ssi.md` §5).
            match range {
                KeyRange::Exact(key) => {
                    ctx.statement
                        .ssi_tracker
                        .record_tuple_read(ctx.statement.txn_id, *table, key)
                }
                KeyRange::Range { .. } | KeyRange::All => ctx
                    .statement
                    .ssi_tracker
                    .record_relation_read(ctx.statement.txn_id, *table),
            }
            Ok(Box::new(IndexScanOp::new(
                ctx.statement.clone(),
                ctx.storage,
                *table,
                *index,
                range.clone(),
                filter.clone(),
                table_output_schema(ctx.catalog, *table)?,
            )))
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
        } => {
            let left = build_executor(ctx, left)?;
            let right = build_executor(ctx, right)?;
            Ok(Box::new(NestedLoopJoinOp::new(
                ctx.statement.clone(),
                left,
                right,
                condition.clone(),
                *join_type,
            )))
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            right_keys,
        } => {
            let left = build_executor(ctx, left)?;
            let right = build_executor(ctx, right)?;
            Ok(Box::new(HashJoinOp::new(
                left,
                right,
                left_keys.clone(),
                right_keys.clone(),
            )))
        }
        PhysicalPlan::Filter { source, predicate } => Ok(Box::new(FilterOp::new(
            ctx.statement.clone(),
            build_executor(ctx, source)?,
            predicate.clone(),
        ))),
        PhysicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => Ok(Box::new(ProjectionOp::new(
            ctx.statement.clone(),
            build_executor(ctx, source)?,
            expressions.clone(),
            output_schema.clone(),
        ))),
        PhysicalPlan::Distinct { source, on_keys } => Ok(Box::new(DistinctOp::new(
            ctx.statement.clone(),
            build_executor(ctx, source)?,
            on_keys.clone(),
        ))),
        PhysicalPlan::Sort { source, order_by } => Ok(Box::new(SortOp::new(
            ctx.statement.clone(),
            build_executor(ctx, source)?,
            order_by.clone(),
        ))),
        PhysicalPlan::Limit {
            source,
            count,
            offset,
        } => Ok(Box::new(LimitOp::new(
            build_executor(ctx, source)?,
            *count,
            offset.unwrap_or(0),
        ))),
        PhysicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => Ok(Box::new(AggregateOp::new(
            ctx.statement.clone(),
            build_executor(ctx, source)?,
            group_by.clone(),
            aggregates.clone(),
            output_schema.clone(),
        ))),
        PhysicalPlan::Values {
            rows,
            output_schema,
        } => Ok(Box::new(ValuesOp::new(
            ctx.statement.clone(),
            rows.clone(),
            output_schema.clone(),
        ))),
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::DropIndex { .. }
        | PhysicalPlan::CreateSequence { .. }
        | PhysicalPlan::DropSequence { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. } => Err(DbError::internal(
            "DML and DDL plans are not valid executor sources",
        )),
    }
}

fn execute_query(ctx: &ExecutionContext<'_>, plan: &PhysicalPlan) -> Result<ExecutionResult> {
    let mut executor = build_executor(ctx, plan)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let columns = executor.output_schema().to_vec();
        let mut rows = Vec::new();
        while let Some(row) = executor.next()? {
            check_canceled(ctx)?;
            rows.push(row.row);
        }
        Ok(ExecutionResult::Query { columns, rows })
    })();
    close_after(executor.as_mut(), result)
}

fn execute_insert(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    columns: &[ColumnId],
    source: &PhysicalPlan,
    on_conflict: Option<&BoundOnConflict>,
    returning: Option<&BoundReturning>,
) -> Result<ExecutionResult> {
    let schema = require_table(ctx.catalog, table)?;
    let mut executor = build_executor(ctx, source)?;
    // Materialize the source fully before inserting. For `INSERT ... SELECT`
    // that reads the target table, this makes the query observe only the
    // pre-insert rows (the Halloween problem) regardless of how the storage
    // engine iterates.
    let source_rows = collect_all(executor.as_mut())?;

    let mut count = 0;
    let mut returned = Vec::new();
    for source_row in source_rows {
        check_canceled(ctx)?;
        if source_row.row.values.len() != columns.len() {
            return Err(DbError::execute(
                SqlState::DatatypeMismatch,
                "INSERT source produced the wrong number of values",
            ));
        }
        let row = build_insert_row(&ctx.statement, &schema, columns, source_row.row.values)?;

        // ON CONFLICT: the arbiter is the primary key. Probe the visible row at the
        // proposed primary key; on a conflict, take the action (skip for DO NOTHING,
        // update the existing row for DO UPDATE) instead of inserting. The probe uses
        // snapshot visibility (including this statement's own earlier inserts), so a
        // duplicate key within the same statement is also caught.
        if let Some(on_conflict) = on_conflict {
            let key = primary_key_for_row(&schema, &row.values)?;
            // SSI: the ON CONFLICT arbiter probe is a tuple read of `key` — record a
            // SIREAD lock (even when no row matches, so a later insert of `key` is
            // caught as a phantom, `docs/specs/ssi.md` §5.1). The IndexScan exact-key
            // arm records this for ordinary point reads; this probe bypasses
            // `build_executor`, so it must record here. No-op for non-SERIALIZABLE.
            ctx.statement
                .ssi_tracker
                .record_tuple_read(ctx.statement.txn_id, table, &key);
            if let Some(existing) = ctx.storage.get(&ctx.statement, table, &key)? {
                if let BoundOnConflict::DoUpdate {
                    assignments,
                    filter,
                } = on_conflict
                    && let Some(updated) = apply_conflict_update(
                        ctx,
                        table,
                        &schema,
                        &key,
                        &existing,
                        &row,
                        assignments,
                        filter.as_ref(),
                    )?
                {
                    count += 1;
                    if let Some(returning) = returning {
                        returned.push(eval_returning(ctx, returning, &updated)?);
                    }
                }
                // DO NOTHING (or a DO UPDATE skipped by its WHERE) inserts no row.
                continue;
            }
        }

        if let Some(returning) = returning {
            returned.push(eval_returning(ctx, returning, &row.values)?);
        }
        ctx.storage.insert(&ctx.statement, table, row)?;
        count += 1;
    }

    Ok(modified_result("INSERT", count, returning, returned))
}

/// Build the primary-key [`Key`] for a full table row (catalog slot order),
/// matching the storage engine's `key_for_row` ordering (primary-key column
/// order). Used by `INSERT ... ON CONFLICT` to probe the arbiter (the PK).
fn primary_key_for_row(schema: &TableSchema, values: &[Value]) -> Result<Key> {
    let mut key = Vec::with_capacity(schema.primary_key.len());
    for column in &schema.primary_key {
        let slot = column_slot(schema, *column)?;
        key.push(values[slot].clone());
    }
    Ok(Key(key))
}

/// Apply an `ON CONFLICT ... DO UPDATE` to the conflicting `existing` row. The
/// assignment values and optional `WHERE` evaluate over the combined
/// `existing ++ proposed` row (so bare columns are the existing row and
/// `excluded.<col>` is the proposed insert). Returns the new full row when the
/// row was updated, or `None` when the `WHERE` excluded it (or the visible row
/// vanished before the update).
#[allow(clippy::too_many_arguments)]
fn apply_conflict_update(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    schema: &TableSchema,
    key: &Key,
    existing: &Row,
    proposed: &Row,
    assignments: &[(ColumnId, BoundExpr)],
    filter: Option<&BoundExpr>,
) -> Result<Option<Vec<Value>>> {
    let mut combined = existing.values.clone();
    combined.extend(proposed.values.iter().cloned());
    let combined_row = ExecRow {
        row: Row { values: combined },
        identity: None,
    };

    if let Some(filter) = filter
        && !matches!(
            eval_expr_with_context(&ctx.statement, filter, &combined_row)?,
            Value::Boolean(true)
        )
    {
        // The DO UPDATE WHERE did not pass (false or NULL): no insert, no update.
        return Ok(None);
    }

    let mut new_values = existing.values.clone();
    for (column, expr) in assignments {
        let slot = column_slot(schema, *column)?;
        new_values[slot] = eval_expr_with_context(&ctx.statement, expr, &combined_row)?;
    }
    coerce_numeric_columns(schema, &mut new_values)?;
    validate_row_constraints(schema, &new_values)?;
    let updated = new_values.clone();
    if ctx
        .storage
        .update(&ctx.statement, table, key, Row { values: new_values })?
    {
        Ok(Some(updated))
    } else {
        Ok(None)
    }
}

/// Map a row's `columns`-ordered values onto a full table row (NULL for omitted
/// columns), validate types and NOT NULL, and round/validate NUMERIC columns.
/// Shared by INSERT, INSERT ... ON CONFLICT, and COPY FROM. Callers guarantee
/// `values.len() == columns.len()`.
pub(crate) fn build_insert_row(
    statement: &StatementContext,
    schema: &TableSchema,
    columns: &[ColumnId],
    values: Vec<Value>,
) -> Result<Row> {
    debug_assert_eq!(values.len(), columns.len());
    let mut full = vec![Value::Null; schema.columns.len()];
    for (column, value) in columns.iter().zip(values) {
        let slot = column_slot(schema, *column)?;
        validate_value_type(&schema.columns[slot], &value)?;
        full[slot] = value;
    }
    for (slot, column) in schema.columns.iter().enumerate() {
        if !columns.contains(&column.id) {
            full[slot] = evaluate_column_default(statement, column)?;
        }
    }
    coerce_numeric_columns(schema, &mut full)?;
    validate_row_constraints(schema, &full)?;
    Ok(Row { values: full })
}

fn evaluate_column_default(
    statement: &StatementContext,
    column: &common::ColumnDef,
) -> Result<Value> {
    match &column.default {
        Some(ColumnDefault::Const(value)) => Ok(value.clone()),
        Some(ColumnDefault::Nextval(sequence)) => {
            let value = statement
                .sequence_manager
                .nextval(statement.txn_id, *sequence)?;
            statement
                .session_sequences
                .record_currval(*sequence, value)?;
            Ok(Value::Integer(value))
        }
        None => Ok(Value::Null),
    }
}

/// Map a row's `columns`-ordered values onto a full table row and insert it.
/// Shared by INSERT and the COPY FROM path.
pub(crate) fn map_and_insert_row(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    schema: &TableSchema,
    columns: &[ColumnId],
    values: Vec<Value>,
) -> Result<()> {
    let row = build_insert_row(&ctx.statement, schema, columns, values)?;
    ctx.storage.insert(&ctx.statement, table, row)?;
    Ok(())
}

/// Evaluate a `RETURNING` projection over an affected full table row (in catalog
/// slot order). The expressions reference table columns by slot, so the
/// constructed `ExecRow` carries the row's values with no storage identity.
fn eval_returning(
    ctx: &ExecutionContext<'_>,
    returning: &BoundReturning,
    full_row: &[Value],
) -> Result<Row> {
    let exec_row = ExecRow {
        row: Row {
            values: full_row.to_vec(),
        },
        identity: None,
    };
    let values = returning
        .exprs
        .iter()
        .map(|expr| eval_expr_with_context(&ctx.statement, expr, &exec_row))
        .collect::<Result<Vec<_>>>()?;
    Ok(Row { values })
}

/// Wrap a DML statement's affected-row `count` (and any collected `RETURNING`
/// rows) into the right `ExecutionResult`: `ModifiedReturning` when the statement
/// has a `RETURNING` clause, otherwise a plain `Modified` count.
fn modified_result(
    command: &str,
    count: u64,
    returning: Option<&BoundReturning>,
    rows: Vec<Row>,
) -> ExecutionResult {
    match returning {
        Some(returning) => ExecutionResult::ModifiedReturning {
            command: command.to_string(),
            count,
            columns: returning.output_schema.clone(),
            rows,
        },
        None => ExecutionResult::Modified {
            command: command.to_string(),
            count,
        },
    }
}

/// Drives `COPY <table> [(cols)] FROM STDIN`: parses streamed bytes into rows and
/// inserts them through the shared insert path. The server feeds chunks as
/// `CopyData` arrives and calls [`CopyIn::finish`] on `CopyDone`; the whole COPY
/// runs in one transaction (the server owns the txn/guard and commit).
pub struct CopyIn<'a> {
    ctx: &'a ExecutionContext<'a>,
    table: TableId,
    schema: TableSchema,
    columns: Vec<ColumnId>,
    parser: CopyParser,
    count: u64,
}

impl<'a> CopyIn<'a> {
    pub fn new(
        ctx: &'a ExecutionContext<'a>,
        table: TableId,
        columns: Vec<ColumnId>,
        options: CopyOptions,
    ) -> Result<Self> {
        let schema = require_table(ctx.catalog, table)?;
        let column_types = columns
            .iter()
            .map(|column| {
                Ok(schema.columns[column_slot(&schema, *column)?]
                    .data_type
                    .clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            ctx,
            table,
            schema,
            columns,
            parser: CopyParser::new(column_types, options),
            count: 0,
        })
    }

    /// Parse and insert every row completed by `chunk`.
    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        check_canceled(self.ctx)?;
        for row in self.parser.push(chunk)? {
            map_and_insert_row(self.ctx, self.table, &self.schema, &self.columns, row)?;
            self.count += 1;
        }
        Ok(())
    }

    /// Flush the trailing record (if any) and return the total rows inserted.
    pub fn finish(mut self) -> Result<u64> {
        for row in self.parser.finish()? {
            map_and_insert_row(self.ctx, self.table, &self.schema, &self.columns, row)?;
            self.count += 1;
        }
        Ok(self.count)
    }
}

/// Drives `COPY <table> [(cols)] TO STDOUT`: scans the table and projects the COPY
/// columns, rendering each row to wire bytes. Owns its scan iterator, so the
/// server can move it into the producer task; the server batches the rows into
/// `CopyData` frames.
pub struct CopyOut {
    iter: Box<dyn RowIterator>,
    slots: Vec<usize>,
    options: CopyOptions,
    column_names: Vec<String>,
}

impl CopyOut {
    pub fn new(
        ctx: &ExecutionContext<'_>,
        table: TableId,
        columns: &[ColumnId],
        options: CopyOptions,
    ) -> Result<Self> {
        let schema = require_table(ctx.catalog, table)?;
        let mut slots = Vec::with_capacity(columns.len());
        let mut column_names = Vec::with_capacity(columns.len());
        for column in columns {
            let slot = column_slot(&schema, *column)?;
            slots.push(slot);
            column_names.push(schema.columns[slot].name.clone());
        }
        // SSI: COPY ... TO scans the whole relation, so it records a relation SIREAD
        // lock like a SeqScan (`docs/specs/ssi.md` §5.1). This scan path bypasses
        // `build_executor`, so the lock must be recorded here. No-op for RC/RR via
        // NoSsiTracker (autocommit COPY TO is Read Committed and records nothing).
        ctx.statement
            .ssi_tracker
            .record_relation_read(ctx.statement.txn_id, table);
        let iter = ctx.storage.scan(&ctx.statement, table)?;
        Ok(Self {
            iter,
            slots,
            options,
            column_names,
        })
    }

    /// The `HEADER` line, or `None` when `HEADER` is off.
    pub fn header_line(&self) -> Option<Vec<u8>> {
        if !self.options.header {
            return None;
        }
        let names: Vec<&str> = self.column_names.iter().map(String::as_str).collect();
        Some(format_header(&names, &self.options))
    }

    /// Render the next row's wire bytes, or `None` at end of scan.
    pub fn next_row(&mut self) -> Result<Option<Vec<u8>>> {
        match self.iter.next()? {
            Some(stored) => {
                let values: Vec<Value> = self
                    .slots
                    .iter()
                    .map(|&slot| stored.row.values[slot].clone())
                    .collect();
                Ok(Some(format_row(&values, &self.options)))
            }
            None => Ok(None),
        }
    }
}

fn execute_update(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    assignments: &[(ColumnId, planner::BoundExpr)],
    source: &PhysicalPlan,
    returning: Option<&BoundReturning>,
) -> Result<ExecutionResult> {
    let schema = require_table(ctx.catalog, table)?;
    let mut executor = build_executor(ctx, source)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let mut count = 0;
        let mut returned = Vec::new();
        while let Some(source_row) = executor.next()? {
            check_canceled(ctx)?;
            let identity = source_row.identity.clone().ok_or_else(|| {
                DbError::internal("UPDATE source row did not include storage identity")
            })?;
            let mut values = source_row.row.values.clone();
            if values.len() != schema.columns.len() {
                return Err(DbError::internal(
                    "UPDATE source row shape does not match table schema",
                ));
            }
            for (column, expr) in assignments {
                let slot = column_slot(&schema, *column)?;
                values[slot] = eval_expr_with_context(&ctx.statement, expr, &source_row)?;
            }
            coerce_numeric_columns(&schema, &mut values)?;
            validate_row_constraints(&schema, &values)?;
            // Evaluate RETURNING over the NEW row before it is moved into storage;
            // only keep it if the update actually affected a row.
            let returned_row = returning
                .map(|returning| eval_returning(ctx, returning, &values))
                .transpose()?;
            if ctx
                .storage
                .update(&ctx.statement, table, &identity.key, Row { values })?
            {
                count += 1;
                if let Some(row) = returned_row {
                    returned.push(row);
                }
            }
        }

        Ok(modified_result("UPDATE", count, returning, returned))
    })();
    close_after(executor.as_mut(), result)
}

fn execute_delete(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    source: &PhysicalPlan,
    returning: Option<&BoundReturning>,
) -> Result<ExecutionResult> {
    let mut executor = build_executor(ctx, source)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let mut count = 0;
        let mut returned = Vec::new();
        while let Some(source_row) = executor.next()? {
            check_canceled(ctx)?;
            // RETURNING on DELETE projects the OLD (deleted) row; evaluate it
            // before consuming the row's identity for the delete.
            let returned_row = returning
                .map(|returning| eval_returning(ctx, returning, &source_row.row.values))
                .transpose()?;
            let identity = source_row.identity.ok_or_else(|| {
                DbError::internal("DELETE source row did not include storage identity")
            })?;
            if ctx.storage.delete(&ctx.statement, table, &identity.key)? {
                count += 1;
                if let Some(row) = returned_row {
                    returned.push(row);
                }
            }
        }

        Ok(modified_result("DELETE", count, returning, returned))
    })();
    close_after(executor.as_mut(), result)
}

fn execute_create_table(
    ctx: &ExecutionContext<'_>,
    name: &str,
    columns: &[ParsedColumnDef],
    primary_key: &[String],
    unique: &[Vec<String>],
) -> Result<ExecutionResult> {
    let schema =
        ctx.catalog
            .create_table(name.to_string(), columns.to_vec(), primary_key.to_vec())?;
    if let Err(err) = ctx.schema_ops.create_table(&ctx.statement, &schema) {
        let _ = ctx.catalog.drop_table(schema.id);
        return Err(err);
    }
    // Each UNIQUE constraint becomes a unique index built on the just-created
    // (empty) table, in declared order. On any failure, drop the table — which
    // cascades to every index created so far in the catalog — and return; the
    // autocommit unit also rolls back the storage-side DDL state.
    for columns in unique {
        if let Err(err) = create_unique_constraint_index(ctx, &schema, columns) {
            let _ = ctx.catalog.drop_table(schema.id);
            return Err(err);
        }
    }
    Ok(ExecutionResult::Modified {
        command: "CREATE TABLE".to_string(),
        count: 0,
    })
}

/// Create one `UNIQUE` constraint's backing index on a freshly created table. The
/// index name follows PostgreSQL's `<table>_<col...>_key` convention.
fn create_unique_constraint_index(
    ctx: &ExecutionContext<'_>,
    schema: &TableSchema,
    columns: &[String],
) -> Result<()> {
    let name = format!("{}_{}_key", schema.name, columns.join("_"));
    let index = ctx
        .catalog
        .create_index(name, &schema.name, columns, true)?;
    if let Err(err) = ctx
        .schema_ops
        .create_index(&ctx.statement, &index, ctx.gc_horizon)
    {
        let _ = ctx.catalog.drop_index(index.id);
        return Err(err);
    }
    Ok(())
}

fn execute_drop_table(ctx: &ExecutionContext<'_>, table: TableId) -> Result<ExecutionResult> {
    ctx.schema_ops.drop_table(&ctx.statement, table)?;
    ctx.catalog.drop_table(table)?;
    Ok(ExecutionResult::Modified {
        command: "DROP TABLE".to_string(),
        count: 0,
    })
}

fn execute_create_index(
    ctx: &ExecutionContext<'_>,
    name: &str,
    table: &str,
    columns: &[String],
    unique: bool,
) -> Result<ExecutionResult> {
    let schema = ctx
        .catalog
        .create_index(name.to_string(), table, columns, unique)?;
    if let Err(err) = ctx
        .schema_ops
        .create_index(&ctx.statement, &schema, ctx.gc_horizon)
    {
        let _ = ctx.catalog.drop_index(schema.id);
        return Err(err);
    }
    Ok(ExecutionResult::Modified {
        command: "CREATE INDEX".to_string(),
        count: 0,
    })
}

fn execute_drop_index(ctx: &ExecutionContext<'_>, index: IndexId) -> Result<ExecutionResult> {
    ctx.schema_ops.drop_index(&ctx.statement, index)?;
    ctx.catalog.drop_index(index)?;
    Ok(ExecutionResult::Modified {
        command: "DROP INDEX".to_string(),
        count: 0,
    })
}

fn execute_create_sequence(
    ctx: &ExecutionContext<'_>,
    name: &str,
    options: &common::SequenceOptions,
) -> Result<ExecutionResult> {
    let schema = ctx
        .catalog
        .create_sequence(name.to_string(), options.clone(), false)?;
    if let Err(err) = ctx.schema_ops.create_sequence(&ctx.statement, &schema) {
        let _ = ctx.catalog.drop_sequence(schema.id);
        return Err(err);
    }
    Ok(ExecutionResult::Modified {
        command: "CREATE SEQUENCE".to_string(),
        count: 0,
    })
}

fn execute_drop_sequence(
    ctx: &ExecutionContext<'_>,
    name: &str,
    if_exists: bool,
) -> Result<ExecutionResult> {
    let sequence = ctx.catalog.get_sequence_by_name(name)?;
    let Some(sequence) = sequence else {
        if if_exists {
            return Ok(ExecutionResult::Modified {
                command: "DROP SEQUENCE".to_string(),
                count: 0,
            });
        }
        return Err(DbError::plan(
            SqlState::UndefinedTable,
            format!("sequence {name} does not exist"),
        ));
    };
    ctx.catalog.drop_sequence(sequence.id)?;
    if let Err(err) = ctx.schema_ops.drop_sequence(&ctx.statement, sequence.id) {
        let _ = ctx.catalog.apply_create_sequence(sequence);
        return Err(err);
    }
    Ok(ExecutionResult::Modified {
        command: "DROP SEQUENCE".to_string(),
        count: 0,
    })
}

fn table_output_schema(catalog: &dyn CatalogManager, table: TableId) -> Result<Vec<ColumnInfo>> {
    Ok(require_table(catalog, table)?
        .columns
        .into_iter()
        .map(|column| ColumnInfo {
            name: column.name,
            data_type: column.data_type,
            table_id: Some(table),
            column_id: Some(column.id),
        })
        .collect())
}

fn require_table(catalog: &dyn CatalogManager, table: TableId) -> Result<TableSchema> {
    catalog.get_table(table)?.ok_or_else(|| {
        DbError::execute(
            SqlState::UndefinedTable,
            format!("table id {table} does not exist"),
        )
    })
}

fn column_slot(schema: &TableSchema, column: ColumnId) -> Result<usize> {
    schema
        .columns
        .iter()
        .position(|candidate| candidate.id == column)
        .ok_or_else(|| {
            DbError::execute(
                SqlState::UndefinedColumn,
                format!("column id {column} does not exist"),
            )
        })
}

/// Enforce per-column runtime constraints on a full row before it is written:
/// NOT NULL, and the bounded character-type length (`VARCHAR(n)` / `CHAR(n)`).
/// Shared by INSERT, `COPY ... FROM`, and UPDATE.
/// Round each `NUMERIC(p, s)` column's value to its scale and reject precision
/// overflow before the row is validated and stored. Unconstrained `NUMERIC` and
/// non-numeric columns are left unchanged.
fn coerce_numeric_columns(schema: &TableSchema, values: &mut [Value]) -> Result<()> {
    for (column, value) in schema.columns.iter().zip(values.iter_mut()) {
        if let DataType::Numeric { precision, scale } = column.data_type
            && let Value::Numeric(d) = value
        {
            let coerced = common::numeric::apply_typmod(*d, precision, scale).ok_or_else(|| {
                DbError::execute(
                    SqlState::NumericValueOutOfRange,
                    format!("numeric field overflow for column {}", column.name),
                )
            })?;
            *value = Value::Numeric(coerced);
        }
    }
    Ok(())
}

fn validate_row_constraints(schema: &TableSchema, values: &[Value]) -> Result<()> {
    for (column, value) in schema.columns.iter().zip(values) {
        match (value, column.max_length) {
            (Value::Null, _) if !column.nullable => {
                return Err(DbError::execute(
                    SqlState::NotNullViolation,
                    format!("column {} cannot be NULL", column.name),
                ));
            }
            (Value::Text(text), Some(max)) if text.chars().count() > max as usize => {
                return Err(DbError::execute(
                    SqlState::StringDataRightTruncation,
                    format!(
                        "value too long for column {} (maximum {max} characters)",
                        column.name
                    ),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_value_type(column: &common::ColumnDef, value: &Value) -> Result<()> {
    if matches!(value, Value::Null) {
        return Ok(());
    }
    let matches_type = matches!(
        (&column.data_type, value),
        (DataType::Integer, Value::Integer(_))
            | (DataType::Double, Value::Float(_))
            | (DataType::Real, Value::Real(_))
            | (DataType::Numeric { .. }, Value::Numeric(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Boolean, Value::Boolean(_))
            | (DataType::Date, Value::Date(_))
            | (DataType::Timestamp, Value::Timestamp(_))
            | (DataType::Time, Value::Time(_))
            | (DataType::TimestampTz, Value::TimestampTz(_))
            | (DataType::Interval, Value::Interval(_))
            | (DataType::Bytea, Value::Bytes(_))
            | (DataType::Uuid, Value::Uuid(_))
    );
    if matches_type {
        return Ok(());
    }
    Err(DbError::execute(
        SqlState::DatatypeMismatch,
        format!(
            "expected column {} to receive {:?}, got {:?}",
            column.name, column.data_type, value
        ),
    ))
}

pub(crate) fn collect_all(source: &mut dyn PlanExecutor) -> Result<Vec<ExecRow>> {
    open_executor(source)?;
    let result = (|| {
        let mut rows = Vec::new();
        while let Some(row) = source.next()? {
            rows.push(row);
        }
        Ok(rows)
    })();
    close_after(source, result)
}

fn open_executor(executor: &mut dyn PlanExecutor) -> Result<()> {
    if let Err(err) = executor.open() {
        let _ = executor.close();
        return Err(err);
    }
    Ok(())
}

fn close_after<T>(executor: &mut dyn PlanExecutor, result: Result<T>) -> Result<T> {
    let close_result = executor.close();
    match (result, close_result) {
        (Err(err), _) => Err(err),
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
    }
}

#[allow(dead_code)]
fn _type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Integer => "INTEGER",
        DataType::Text => "TEXT",
        DataType::Boolean => "BOOLEAN",
        DataType::Date => "DATE",
        DataType::Timestamp => "TIMESTAMP",
        DataType::Time => "TIME",
        DataType::TimestampTz => "TIMESTAMP WITH TIME ZONE",
        DataType::Interval => "INTERVAL",
        DataType::Bytea => "BYTEA",
        DataType::Uuid => "UUID",
        DataType::Double => "DOUBLE PRECISION",
        DataType::Numeric { .. } => "NUMERIC",
        DataType::Real => "REAL",
    }
}
