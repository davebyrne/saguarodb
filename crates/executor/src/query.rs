use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use catalog::{CatalogManager, TableColumnAlteration};
use common::{
    ColumnDefault, ColumnId, ColumnInfo, CompressionSetting, CopyOptions, DataType, DbError,
    ExecRow, IndexConstraintKind, IndexId, Key, KeyRange, ParsedColumnDef, ParsedDefault,
    QueryCancel, Result, Row, SequenceOptions, SequenceSchema, SqlState, StatementContext,
    StoredRow, TableId, TableSchema, ToastOptions, TruncateCatalogUpdate, Value, ViewColumn,
};
use planner::{
    BoundExpr, BoundOnConflict, BoundReturning, DropTableTarget, PhysicalPlan, bind_default_expr,
};
use storage::{RelationSnapshot, RowIterator, SchemaOperations, StorageEngine};

use crate::ExecutionResult;
use crate::copy::{CopyParser, format_header, format_row};
use crate::eval_expr;
use crate::ops::SystemScanOp;
use crate::ops::{
    AggregateOp, DistinctOp, FilterOp, HashJoinOp, IndexScanInput, IndexScanOp, LimitOp,
    NestedLoopJoinOp, ProjectionOp, SeqScanOp, SetOpOp, SortOp, TableFunctionOp, ValuesOp,
};

pub struct ExecutionContext<'a> {
    pub statement: StatementContext,
    pub relations: Arc<dyn RelationSnapshot>,
    pub catalog: Arc<dyn CatalogManager>,
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
    pub cancel: &'a QueryCancel,
}

/// Abort with `QueryCanceled` if a cancellation has been requested. Called
/// between rows in the row-producing and write loops.
pub(crate) fn check_canceled(ctx: &ExecutionContext<'_>) -> Result<()> {
    check_canceled_flag(ctx.cancel)
}

/// The cancellation check on the bare flag, so the streaming drive can poll it
/// without threading the whole `ExecutionContext`.
fn check_canceled_flag(cancel: &QueryCancel) -> Result<()> {
    cancel.check()
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

/// A consumer of streamed query output. [`QueryEngine::execute_query_streamed`]
/// calls [`RowSink::start`] once with the output schema, then [`RowSink::push`]
/// with row batches until the plan is exhausted or the sink asks to stop.
///
/// This is the seam that lets the server stream `SELECT` results through a
/// bounded channel without the `executor` crate depending on the channel type
/// (`docs/specs/streaming.md` §3).
pub trait RowSink {
    /// Called once, before any rows, with the query's output columns.
    fn start(&mut self, columns: &[ColumnInfo]) -> Result<()>;

    /// Push a batch of rows. Returning [`ControlFlow::Break`] stops the scan
    /// early (e.g. the downstream consumer is gone); the engine then closes the
    /// executor and returns the count streamed so far.
    fn push(&mut self, rows: Vec<Row>) -> Result<ControlFlow<()>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchStatus {
    Exhausted { count: u64 },
    Suspended { count: u64 },
}

impl FetchStatus {
    pub fn count(self) -> u64 {
        match self {
            FetchStatus::Exhausted { count } | FetchStatus::Suspended { count } => count,
        }
    }
}

pub struct OpenQuery<'a> {
    columns: Vec<ColumnInfo>,
    executor: Option<Box<dyn PlanExecutor + 'a>>,
    cancel: &'a QueryCancel,
    pending: Option<ExecRow>,
    exhausted: bool,
    closed: bool,
}

impl<'a> OpenQuery<'a> {
    fn from_executor(
        cancel: &'a QueryCancel,
        mut executor: Box<dyn PlanExecutor + 'a>,
    ) -> Result<Self> {
        let columns = executor.output_schema().to_vec();
        open_executor(executor.as_mut())?;
        Ok(Self {
            columns,
            executor: Some(executor),
            cancel,
            pending: None,
            exhausted: false,
            closed: false,
        })
    }

    pub fn output_schema(&self) -> &[ColumnInfo] {
        &self.columns
    }

    pub fn fetch(
        &mut self,
        max_rows: Option<u64>,
        sink: &mut dyn RowSink,
        batch_size: usize,
    ) -> Result<FetchStatus> {
        let result = self.fetch_inner(max_rows, sink, batch_size.max(1));
        if result.is_err() {
            let _ = self.close();
        }
        result
    }

    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.pending = None;
        match self.executor.take() {
            Some(mut executor) => executor.close(),
            None => Ok(()),
        }
    }

    fn fetch_inner(
        &mut self,
        max_rows: Option<u64>,
        sink: &mut dyn RowSink,
        batch_size: usize,
    ) -> Result<FetchStatus> {
        sink.start(&self.columns)?;
        if self.exhausted {
            return Ok(FetchStatus::Exhausted { count: 0 });
        }
        if self.closed {
            return Err(DbError::internal("cannot fetch from a closed query"));
        }

        let mut count = 0;
        let mut batch = Vec::with_capacity(batch_size);
        loop {
            if max_rows.is_some_and(|limit| count >= limit) {
                return self.finish_limited_fetch(count, sink, batch);
            }

            let next = match self.pending.take() {
                Some(row) => Some(row),
                None => self.executor_mut()?.next()?,
            };
            let Some(row) = next else {
                return self.finish_exhausted_fetch(count, sink, batch);
            };

            check_canceled_flag(self.cancel)?;
            batch.push(row.row);
            count += 1;

            if batch.len() >= batch_size {
                let full = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                if sink.push(full)?.is_break() {
                    return self.lookahead_status(count);
                }
            }
        }
    }

    fn finish_limited_fetch(
        &mut self,
        count: u64,
        sink: &mut dyn RowSink,
        batch: Vec<Row>,
    ) -> Result<FetchStatus> {
        if !batch.is_empty() && sink.push(batch)?.is_break() {
            return self.lookahead_status(count);
        }

        self.lookahead_status(count)
    }

    fn lookahead_status(&mut self, count: u64) -> Result<FetchStatus> {
        if self.pending.is_some() {
            return Ok(FetchStatus::Suspended { count });
        }
        let next = self.executor_mut()?.next()?;
        match next {
            Some(row) => {
                check_canceled_flag(self.cancel)?;
                self.pending = Some(row);
                Ok(FetchStatus::Suspended { count })
            }
            None => {
                self.exhausted = true;
                self.close()?;
                Ok(FetchStatus::Exhausted { count })
            }
        }
    }

    fn finish_exhausted_fetch(
        &mut self,
        count: u64,
        sink: &mut dyn RowSink,
        batch: Vec<Row>,
    ) -> Result<FetchStatus> {
        if !batch.is_empty() {
            let _ = sink.push(batch)?;
        }
        self.exhausted = true;
        self.close()?;
        Ok(FetchStatus::Exhausted { count })
    }

    fn executor_mut(&mut self) -> Result<&mut Box<dyn PlanExecutor + 'a>> {
        self.executor
            .as_mut()
            .ok_or_else(|| DbError::internal("open query executor is closed"))
    }
}

impl Drop for OpenQuery<'_> {
    fn drop(&mut self) {
        let _ = self.close();
    }
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
                if_not_exists,
                columns,
                primary_key,
                unique,
                compression,
                toast,
                checks,
            } => execute_create_table(
                ctx,
                name,
                *if_not_exists,
                columns,
                primary_key,
                unique,
                *compression,
                toast.clone(),
                checks,
            ),
            PhysicalPlan::DropTable { targets, if_exists } => {
                execute_drop_tables(ctx, targets, *if_exists)
            }
            PhysicalPlan::AlterTableAddColumn {
                table,
                table_name: _,
                if_not_exists,
                column,
            } => execute_alter_table_add_column(ctx, *table, *if_not_exists, column),
            PhysicalPlan::AlterTableDropColumn {
                table,
                table_name: _,
                if_exists,
                column,
            } => execute_alter_table_drop_column(ctx, *table, *if_exists, column),
            PhysicalPlan::AlterTableRenameColumn {
                table,
                table_name: _,
                old_name,
                new_name,
            } => execute_alter_table_rename_column(ctx, *table, old_name, new_name),
            PhysicalPlan::AlterTableRenameTable {
                table,
                table_name: _,
                new_name,
            } => execute_alter_table_rename_table(ctx, *table, new_name),
            PhysicalPlan::CreateView {
                name,
                or_replace,
                columns,
                query,
                definition,
                dependencies,
            } => execute_create_view(
                ctx,
                name,
                *or_replace,
                columns,
                query,
                definition,
                dependencies,
            ),
            PhysicalPlan::DropView { name, if_exists } => execute_drop_view(ctx, name, *if_exists),
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
                default_exprs,
                check_exprs,
            } => execute_insert(
                ctx,
                *table,
                columns,
                source,
                on_conflict.as_ref(),
                returning.as_ref(),
                default_exprs,
                check_exprs,
            ),
            PhysicalPlan::Update {
                table,
                assignments,
                source,
                joined_source,
                returning,
                check_exprs,
            } => execute_update(
                ctx,
                *table,
                assignments,
                source,
                *joined_source,
                returning.as_ref(),
                check_exprs,
            ),
            PhysicalPlan::Delete {
                table,
                source,
                joined_source,
                returning,
            } => execute_delete(ctx, *table, source, *joined_source, returning.as_ref()),
            _ => execute_query(ctx, plan),
        }
    }

    /// Drive a query `plan`, streaming its rows into `sink` in batches of at most
    /// `batch_size` rows rather than materializing them into an
    /// [`ExecutionResult::Query`]. Returns the number of rows streamed.
    ///
    /// This is the streaming counterpart of the `SELECT` arm of [`Self::execute`]
    /// and shares the same [`OpenQuery`] fetch path as materialized results, so
    /// streamed and materialized results cannot diverge (`docs/specs/streaming.md`
    /// §3). The caller must hold the snapshot's GC-horizon advertisement and any
    /// transaction guard for the full duration of the call, exactly as the
    /// materializing path does.
    pub fn execute_query_streamed(
        &self,
        ctx: &ExecutionContext<'_>,
        plan: &PhysicalPlan,
        sink: &mut dyn RowSink,
        batch_size: usize,
    ) -> Result<u64> {
        let mut query = self.open_query(ctx, plan)?;
        let result = query.fetch(None, sink, batch_size).map(FetchStatus::count);
        let close_result = query.close();
        match (result, close_result) {
            (Err(err), _) => Err(err),
            (Ok(count), Ok(())) => Ok(count),
            (Ok(_), Err(err)) => Err(err),
        }
    }

    pub fn open_query<'a>(
        &self,
        ctx: &'a ExecutionContext<'_>,
        plan: &PhysicalPlan,
    ) -> Result<OpenQuery<'a>> {
        let resolved = crate::subquery::resolve_plan_subqueries(ctx, plan)?;
        open_resolved_query(ctx, &resolved)
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
                ctx.relations.clone(),
                ctx.storage,
                *table,
                filter.clone(),
                table_output_schema(ctx.catalog.as_ref(), *table)?,
            )))
        }
        PhysicalPlan::SystemScan {
            view,
            output_schema,
            filter,
        } => Ok(Box::new(SystemScanOp::new(
            ctx.statement.clone(),
            crate::system::rows_for(*view, ctx.catalog.as_ref(), &ctx.statement)?,
            output_schema.clone(),
            filter.clone(),
        ))),
        PhysicalPlan::IndexScan {
            table,
            index,
            range,
            full_filter,
            filter,
            ..
        } => {
            // SSI: a full declared-primary-key point lookup reads one tuple
            // (recorded even when no row matches, so a later insert of that key
            // is caught as a phantom). A composite-key prefix scan, a range scan,
            // or any catalog-index scan reads the relation conservatively
            // (`docs/specs/ssi.md` §5). Catalog index scans stay relation reads
            // because an old relation snapshot may fall back to a full scan if the
            // current-catalog index is unavailable for that generation.
            let full_primary_key_exact_read = if *index == common::PRIMARY_KEY_INDEX_ID
                && let KeyRange::Exact(key) = range
            {
                key.0.len()
                    == require_table(ctx.catalog.as_ref(), *table)?
                        .primary_key
                        .len()
            } else {
                false
            };
            if full_primary_key_exact_read {
                let KeyRange::Exact(key) = range else {
                    unreachable!("full_primary_key_exact_read requires an exact range");
                };
                ctx.statement
                    .ssi_tracker
                    .record_tuple_read(ctx.statement.txn_id, *table, key);
            } else {
                ctx.statement
                    .ssi_tracker
                    .record_relation_read(ctx.statement.txn_id, *table);
            }
            Ok(Box::new(IndexScanOp::new(IndexScanInput {
                ctx: ctx.statement.clone(),
                relations: ctx.relations.clone(),
                storage: ctx.storage,
                table: *table,
                index: *index,
                range: range.clone(),
                full_filter: full_filter.clone(),
                filter: filter.clone(),
                output_schema: table_output_schema(ctx.catalog.as_ref(), *table)?,
            })))
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
            identity_from,
        } => {
            let left = build_executor(ctx, left)?;
            let right = build_executor(ctx, right)?;
            Ok(Box::new(NestedLoopJoinOp::new(
                ctx.statement.clone(),
                left,
                right,
                condition.clone(),
                *join_type,
                *identity_from,
            )))
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            identity_from,
        } => {
            let left = build_executor(ctx, left)?;
            let right = build_executor(ctx, right)?;
            Ok(Box::new(HashJoinOp::new(
                ctx.statement.clone(),
                left,
                right,
                left_keys.clone(),
                right_keys.clone(),
                *join_type,
                *identity_from,
            )))
        }
        PhysicalPlan::Apply {
            input,
            subplan,
            correlations,
            kind,
        } => {
            // Resolve the template's uncorrelated nested subqueries once at
            // construction (docs/specs/subqueries.md section 5.2); the
            // statement-level pre-pass deliberately did not descend into it.
            let subplan = crate::subquery::resolve_plan_subqueries(ctx, subplan)?;
            if let planner::ApplyKind::Lateral {
                left_join,
                condition,
                output_schema,
            } = kind
            {
                // The ON condition's own uncorrelated subqueries were already
                // resolved by whichever pre-pass walked this Apply node (the
                // rewriter's Apply arm covers the Lateral condition).
                return Ok(Box::new(crate::ops::LateralApplyOp::new(
                    ctx,
                    build_executor(ctx, input)?,
                    subplan,
                    correlations.clone(),
                    *left_join,
                    condition.as_deref().cloned(),
                    output_schema.clone(),
                )));
            }
            Ok(Box::new(crate::ops::ApplyOp::new(
                ctx,
                build_executor(ctx, input)?,
                subplan,
                correlations.clone(),
                kind.clone(),
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
            ctx.statement.clone(),
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
        PhysicalPlan::TableFunction {
            name,
            args,
            output_schema,
        } => Ok(Box::new(TableFunctionOp::new(
            ctx.statement.clone(),
            name.clone(),
            args.clone(),
            output_schema.clone(),
        ))),
        PhysicalPlan::SetOp {
            op,
            all,
            left,
            right,
        } => Ok(Box::new(SetOpOp::new(
            ctx.statement.clone(),
            *op,
            *all,
            build_executor(ctx, left)?,
            build_executor(ctx, right)?,
        ))),
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTableAddColumn { .. }
        | PhysicalPlan::AlterTableDropColumn { .. }
        | PhysicalPlan::AlterTableRenameColumn { .. }
        | PhysicalPlan::AlterTableRenameTable { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::DropIndex { .. }
        | PhysicalPlan::CreateSequence { .. }
        | PhysicalPlan::DropSequence { .. }
        | PhysicalPlan::CreateView { .. }
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. } => Err(DbError::internal(
            "DML and DDL plans are not valid executor sources",
        )),
    }
}

/// Batch size used to materialize a `SELECT` into an [`ExecutionResult::Query`].
/// It only bounds temporary-batch churn on the non-streaming path (the rows are
/// re-collected into one `Vec` regardless), so a large value minimizes overhead.
const MATERIALIZE_BATCH_ROWS: usize = 1024;

fn execute_query(ctx: &ExecutionContext<'_>, plan: &PhysicalPlan) -> Result<ExecutionResult> {
    // The plan reaching here has already had its subqueries resolved by
    // `QueryEngine::execute`; open it as an `OpenQuery` so the materialized,
    // streamed, and cursor-facing paths share one fetch loop.
    let mut sink = VecRowSink::default();
    let mut query = open_resolved_query(ctx, plan)?;
    let result = query.fetch(None, &mut sink, MATERIALIZE_BATCH_ROWS);
    let close_result = query.close();
    match (result, close_result) {
        (Err(err), _) => return Err(err),
        (Ok(_), Ok(())) => {}
        (Ok(_), Err(err)) => return Err(err),
    }
    Ok(ExecutionResult::Query {
        columns: sink.columns,
        rows: sink.rows,
    })
}

fn open_resolved_query<'a>(
    ctx: &'a ExecutionContext<'_>,
    plan: &PhysicalPlan,
) -> Result<OpenQuery<'a>> {
    OpenQuery::from_executor(ctx.cancel, build_executor(ctx, plan)?)
}

/// A [`RowSink`] that materializes all rows into memory — the collecting sink
/// behind the non-streaming [`execute_query`].
#[derive(Default)]
struct VecRowSink {
    columns: Vec<ColumnInfo>,
    rows: Vec<Row>,
}

impl RowSink for VecRowSink {
    fn start(&mut self, columns: &[ColumnInfo]) -> Result<()> {
        self.columns = columns.to_vec();
        Ok(())
    }

    fn push(&mut self, rows: Vec<Row>) -> Result<ControlFlow<()>> {
        self.rows.extend(rows);
        Ok(ControlFlow::Continue(()))
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_insert(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    columns: &[ColumnId],
    source: &PhysicalPlan,
    on_conflict: Option<&BoundOnConflict>,
    returning: Option<&BoundReturning>,
    default_exprs: &[(ColumnId, BoundExpr)],
    check_exprs: &[BoundExpr],
) -> Result<ExecutionResult> {
    let schema = require_table(ctx.catalog.as_ref(), table)?;
    let has_conflict_arbiter = if let Some(on_conflict) = on_conflict {
        validate_on_conflict_arbiter(on_conflict, &schema)?
    } else {
        false
    };
    let mut executor = build_executor(ctx, source)?;
    // Materialize the source fully before inserting. For `INSERT ... SELECT`
    // that reads the target table, this makes the query observe only the
    // pre-insert rows (the Halloween problem) regardless of how the storage
    // engine iterates.
    let source_rows = collect_all_cancelable(executor.as_mut(), ctx.cancel)?;

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
        let row = build_insert_row(
            &ctx.statement,
            &schema,
            columns,
            source_row.row.values,
            default_exprs,
        )?;
        // CHECK constraints are evaluated on the proposed row before conflict
        // arbitration, matching PostgreSQL (a DO NOTHING that conflicts still
        // raises a check violation on the proposed row).
        validate_check_constraints(&ctx.statement, &schema, check_exprs, &row.values)?;

        // ON CONFLICT: the bound arbiter is the declared primary-key constraint, if
        // one existed at bind/prepare time. Probe that primary-key index; on a
        // conflict, take the action (skip for DO NOTHING, update the existing row for
        // DO UPDATE) instead of inserting. The probe uses snapshot visibility
        // (including this statement's own earlier inserts), so a duplicate key within
        // the same statement is also caught. A targetless statement bound with no
        // primary key has no arbiter, so there is simply nothing to probe.
        if let Some(on_conflict) = on_conflict
            && has_conflict_arbiter
        {
            let key = primary_key_for_row(&schema, &row.values)?;
            // SSI: this probe bypasses `build_executor`, but it is still a point read
            // of the proposed primary-key value.
            ctx.statement
                .ssi_tracker
                .record_tuple_read(ctx.statement.txn_id, table, &key);
            if let Some(existing) =
                ctx.storage
                    .get(&ctx.statement, ctx.relations.as_ref(), table, &key)?
            {
                if let BoundOnConflict::DoUpdate {
                    assignments,
                    filter,
                    ..
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
                        check_exprs,
                    )?
                {
                    if let Some(returning) = returning {
                        returned.push(eval_returning(ctx, returning, &updated)?);
                    }
                    count += 1;
                }
                // DO NOTHING (or a DO UPDATE skipped by its WHERE) inserts no row.
                continue;
            }
        }

        let returning_values = returning.map(|_| row.values.clone());
        ctx.storage
            .insert(&ctx.statement, ctx.relations.as_ref(), table, row)?;
        if let (Some(returning), Some(values)) = (returning, returning_values) {
            returned.push(eval_returning(ctx, returning, &values)?);
        }
        count += 1;
    }

    Ok(modified_result("INSERT", count, returning, returned))
}

fn validate_on_conflict_arbiter(
    on_conflict: &BoundOnConflict,
    schema: &TableSchema,
) -> Result<bool> {
    let Some(target) = on_conflict_target(on_conflict) else {
        return Ok(false);
    };
    let mut target = target.to_vec();
    target.sort_unstable();
    let mut primary_key = schema.primary_key.clone();
    primary_key.sort_unstable();
    if target == primary_key && !primary_key.is_empty() {
        return Ok(true);
    }
    Err(DbError::execute(
        SqlState::FeatureNotSupported,
        "ON CONFLICT arbiter must be the primary key; only the primary key is supported",
    ))
}

fn on_conflict_target(on_conflict: &BoundOnConflict) -> Option<&[ColumnId]> {
    match on_conflict {
        BoundOnConflict::DoNothing { target } => target.as_deref(),
        BoundOnConflict::DoUpdate { target, .. } => Some(target),
    }
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
    check_exprs: &[BoundExpr],
) -> Result<Option<Vec<Value>>> {
    let mut combined = existing.values.clone();
    combined.extend(proposed.values.iter().cloned());
    let combined_row = ExecRow {
        row: Row { values: combined },
        identity: None,
    };

    if let Some(filter) = filter
        && !matches!(
            eval_expr(&ctx.statement, filter, &combined_row)?,
            Value::Boolean(true)
        )
    {
        // The DO UPDATE WHERE did not pass (false or NULL): no insert, no update.
        return Ok(None);
    }

    let mut new_values = existing.values.clone();
    for (column, expr) in assignments {
        let slot = column_slot(schema, *column)?;
        new_values[slot] = eval_expr(&ctx.statement, expr, &combined_row)?;
    }
    coerce_numeric_columns(schema, &mut new_values)?;
    validate_row_constraints(schema, &new_values)?;
    validate_check_constraints(&ctx.statement, schema, check_exprs, &new_values)?;
    let updated = new_values.clone();
    if ctx.storage.update(
        &ctx.statement,
        ctx.relations.as_ref(),
        table,
        key,
        Row { values: new_values },
    )? {
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
    default_exprs: &[(ColumnId, BoundExpr)],
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
            full[slot] = evaluate_column_default(statement, column, default_exprs)?;
        }
    }
    coerce_numeric_columns(schema, &mut full)?;
    validate_row_constraints(schema, &full)?;
    Ok(Row { values: full })
}

fn evaluate_column_default(
    statement: &StatementContext,
    column: &common::ColumnDef,
    default_exprs: &[(ColumnId, BoundExpr)],
) -> Result<Value> {
    match &column.default {
        Some(ColumnDefault::Const(value)) => Ok(value.clone()),
        Some(ColumnDefault::Nextval(sequence)) => Ok(Value::Integer(
            statement.nextval_recording_currval(*sequence)?,
        )),
        Some(ColumnDefault::Expr(_)) => {
            // The binder bound this column's default expression against an empty
            // scope; evaluate it over an empty row (it cannot reference columns).
            let bound = default_exprs
                .iter()
                .find(|(id, _)| *id == column.id)
                .map(|(_, expr)| expr)
                .ok_or_else(|| {
                    DbError::execute(
                        SqlState::FeatureNotSupported,
                        format!(
                            "expression DEFAULT for column {} is not supported here",
                            column.name
                        ),
                    )
                })?;
            let empty = ExecRow {
                row: Row { values: Vec::new() },
                identity: None,
            };
            eval_expr(statement, bound, &empty)
        }
        None => Ok(Value::Null),
    }
}

/// Map a row's `columns`-ordered values onto a full table row, enforce the table's
/// constraints, and insert it. The COPY FROM insert path: `default_exprs` supplies
/// omitted columns' bound expression defaults (evaluated per row) and `check_exprs`
/// the table's bound `CHECK` constraints (enforced per row), so COPY matches INSERT.
pub(crate) fn map_and_insert_row(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    schema: &TableSchema,
    columns: &[ColumnId],
    values: Vec<Value>,
    default_exprs: &[(ColumnId, BoundExpr)],
    check_exprs: &[BoundExpr],
) -> Result<()> {
    let row = build_insert_row(&ctx.statement, schema, columns, values, default_exprs)?;
    validate_check_constraints(&ctx.statement, schema, check_exprs, &row.values)?;
    ctx.storage
        .insert(&ctx.statement, ctx.relations.as_ref(), table, row)?;
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
        .map(|expr| eval_expr(&ctx.statement, expr, &exec_row))
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
    schema: TableSchema,
    columns: Vec<ColumnId>,
    /// Bound expression defaults for omitted columns and the table's bound `CHECK`
    /// constraints, so COPY FROM applies defaults and enforces checks like INSERT.
    default_exprs: Vec<(ColumnId, BoundExpr)>,
    check_exprs: Vec<BoundExpr>,
    parser: CopyParser,
    count: u64,
}

impl<'a> CopyIn<'a> {
    pub fn new(
        ctx: &'a ExecutionContext<'a>,
        schema: TableSchema,
        columns: Vec<ColumnId>,
        options: CopyOptions,
        default_exprs: Vec<(ColumnId, BoundExpr)>,
        check_exprs: Vec<BoundExpr>,
    ) -> Result<Self> {
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
            schema,
            columns,
            default_exprs,
            check_exprs,
            parser: CopyParser::new(column_types, options),
            count: 0,
        })
    }

    /// Parse and insert every row completed by `chunk`.
    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        check_canceled(self.ctx)?;
        for row in self.parser.push(chunk)? {
            map_and_insert_row(
                self.ctx,
                self.schema.id,
                &self.schema,
                &self.columns,
                row,
                &self.default_exprs,
                &self.check_exprs,
            )?;
            self.count += 1;
        }
        Ok(())
    }

    /// Flush the trailing record (if any) and return the total rows inserted.
    pub fn finish(mut self) -> Result<u64> {
        for row in self.parser.finish()? {
            map_and_insert_row(
                self.ctx,
                self.schema.id,
                &self.schema,
                &self.columns,
                row,
                &self.default_exprs,
                &self.check_exprs,
            )?;
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
        schema: TableSchema,
        columns: &[ColumnId],
        options: CopyOptions,
    ) -> Result<Self> {
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
            .record_relation_read(ctx.statement.txn_id, schema.id);
        let iter = match ctx
            .storage
            .scan(&ctx.statement, ctx.relations.as_ref(), schema.id)
        {
            Ok(iter) => iter,
            Err(err)
                if err.code == SqlState::UndefinedTable
                    && ctx.relations.missing_tables_are_empty() =>
            {
                Box::new(EmptyRowIterator {
                    schema: output_schema_for_table(&schema),
                })
            }
            Err(err) => return Err(err),
        };
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

struct EmptyRowIterator {
    schema: Vec<ColumnInfo>,
}

impl RowIterator for EmptyRowIterator {
    fn next(&mut self) -> Result<Option<StoredRow>> {
        Ok(None)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_update(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    assignments: &[(ColumnId, planner::BoundExpr)],
    source: &PhysicalPlan,
    joined_source: bool,
    returning: Option<&BoundReturning>,
    check_exprs: &[BoundExpr],
) -> Result<ExecutionResult> {
    let schema = require_table(ctx.catalog.as_ref(), table)?;
    let mut executor = build_executor(ctx, source)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let mut count = 0;
        let mut returned = Vec::new();
        // A joined source (UPDATE ... FROM) produces the combined
        // (target ++ FROM) row: the target prefix is the row to update, and a
        // target row matched by multiple source rows is updated once — first
        // match in scan order (docs/specs/subqueries.md section 8.2).
        let mut seen_targets = std::collections::HashSet::new();
        while let Some(source_row) = executor.next()? {
            check_canceled(ctx)?;
            let identity = source_row.identity.clone().ok_or_else(|| {
                DbError::internal("UPDATE source row did not include storage identity")
            })?;
            let mut values = source_row.row.values.clone();
            if values.len() < schema.columns.len() {
                return Err(DbError::internal(
                    "UPDATE source row shape does not match table schema",
                ));
            }
            // Width cannot stand in for `joined_source`: a zero-column FROM
            // item keeps the combined width equal to the table's.
            if joined_source
                && !seen_targets.insert((identity.row_id.page_num, identity.row_id.slot_num))
            {
                continue;
            }
            for (column, expr) in assignments {
                let slot = column_slot(&schema, *column)?;
                values[slot] = eval_expr(&ctx.statement, expr, &source_row)?;
            }
            values.truncate(schema.columns.len());
            coerce_numeric_columns(&schema, &mut values)?;
            validate_row_constraints(&schema, &values)?;
            validate_check_constraints(&ctx.statement, &schema, check_exprs, &values)?;
            let returning_values = returning.map(|_| values.clone());
            if ctx.storage.update(
                &ctx.statement,
                ctx.relations.as_ref(),
                table,
                &identity.key,
                Row { values },
            )? {
                if let (Some(returning), Some(values)) = (returning, returning_values) {
                    returned.push(eval_returning(ctx, returning, &values)?);
                }
                count += 1;
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
    joined_source: bool,
    returning: Option<&BoundReturning>,
) -> Result<ExecutionResult> {
    let mut executor = build_executor(ctx, source)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let schema = require_table(ctx.catalog.as_ref(), table)?;
        let mut count = 0;
        let mut returned = Vec::new();
        // DELETE ... USING: combined source rows, deleted once per target —
        // first match in scan order (docs/specs/subqueries.md section 8.2).
        let mut seen_targets = std::collections::HashSet::new();
        while let Some(source_row) = executor.next()? {
            check_canceled(ctx)?;
            if source_row.row.values.len() < schema.columns.len() {
                return Err(DbError::internal(
                    "DELETE source row shape does not match table schema",
                ));
            }
            let identity = source_row.identity.clone().ok_or_else(|| {
                DbError::internal("DELETE source row did not include storage identity")
            })?;
            if joined_source
                && !seen_targets.insert((identity.row_id.page_num, identity.row_id.slot_num))
            {
                continue;
            }
            let returning_values = returning.map(|_| {
                let mut values = source_row.row.values.clone();
                // RETURNING sees the deleted (old) row: the target prefix.
                values.truncate(schema.columns.len());
                values
            });
            if ctx
                .storage
                .delete(&ctx.statement, ctx.relations.as_ref(), table, &identity.key)?
            {
                if let (Some(returning), Some(values)) = (returning, returning_values) {
                    returned.push(eval_returning(ctx, returning, &values)?);
                }
                count += 1;
            }
        }

        Ok(modified_result("DELETE", count, returning, returned))
    })();
    close_after(executor.as_mut(), result)
}

#[allow(clippy::too_many_arguments)]
fn execute_create_table(
    ctx: &ExecutionContext<'_>,
    name: &str,
    if_not_exists: bool,
    columns: &[ParsedColumnDef],
    primary_key: &[String],
    unique: &[Vec<String>],
    compression: CompressionSetting,
    toast: ToastOptions,
    checks: &[String],
) -> Result<ExecutionResult> {
    if if_not_exists {
        catalog::validate_create_table_definition(name, columns, primary_key, unique)?;
        if ctx.catalog.get_table_by_name(name)?.is_some() {
            return Ok(ExecutionResult::Modified {
                command: "CREATE TABLE".to_string(),
                count: 0,
            });
        }
        if ctx.catalog.get_view_by_name(name)?.is_some() {
            return Err(DbError::plan(
                SqlState::DuplicateTable,
                format!("relation {name} already exists"),
            ));
        }
    }

    let serial = resolve_serial_columns(ctx.catalog.as_ref(), name, columns)?;
    let mut created_sequences = Vec::new();
    for serial_column in &serial {
        match create_owned_serial_sequence(ctx, &serial_column.sequence) {
            Ok(sequence) => created_sequences.push(sequence),
            Err(err) => {
                cleanup_serial_sequences(ctx, &created_sequences);
                return Err(err);
            }
        }
    }

    let columns = columns_with_serial_defaults(columns, &serial)?;
    let schema = match ctx.catalog.create_table_with_options(
        name.to_string(),
        columns,
        primary_key.to_vec(),
        compression,
        toast,
        checks.to_vec(),
    ) {
        Ok(schema) => schema,
        Err(err) if if_not_exists && err.code == SqlState::DuplicateTable => {
            cleanup_serial_sequences(ctx, &created_sequences);
            return Ok(ExecutionResult::Modified {
                command: "CREATE TABLE".to_string(),
                count: 0,
            });
        }
        Err(err) => {
            cleanup_serial_sequences(ctx, &created_sequences);
            return Err(err);
        }
    };
    let toast_schema = match schema.toast_table_id {
        Some(toast_table_id) => Some(
            ctx.catalog
                .get_table(toast_table_id)?
                .ok_or_else(|| DbError::internal("created table is missing its TOAST relation"))?,
        ),
        None => None,
    };
    if let Err(err) = ctx.schema_ops.create_table(&ctx.statement, &schema) {
        cleanup_created_table(ctx, schema.id, &created_sequences);
        return Err(err);
    }
    if let Some(toast_schema) = &toast_schema
        && let Err(err) = ctx.schema_ops.create_table(&ctx.statement, toast_schema)
    {
        cleanup_created_table(ctx, schema.id, &created_sequences);
        return Err(err);
    }
    if !primary_key.is_empty()
        && let Err(err) = create_primary_key_constraint_index(ctx, &schema, primary_key)
    {
        cleanup_created_table(ctx, schema.id, &created_sequences);
        return Err(err);
    }
    // Each UNIQUE constraint becomes a unique index built on the just-created
    // (empty) table, in declared order. On any failure, drop the table — which
    // cascades to every index created so far in the catalog — and return; the
    // autocommit unit also rolls back the storage-side DDL state.
    for columns in unique {
        if let Err(err) = create_unique_constraint_index(ctx, &schema, columns) {
            cleanup_created_table(ctx, schema.id, &created_sequences);
            return Err(err);
        }
    }
    Ok(ExecutionResult::Modified {
        command: "CREATE TABLE".to_string(),
        count: 0,
    })
}

#[derive(Clone, Debug)]
struct ResolvedSerialColumn {
    index: usize,
    sequence: String,
}

/// Derive the `SERIAL` columns straight from the parsed column list (the single
/// source of truth — each carries `ParsedDefault::Serial`), choosing a generated
/// owned-sequence name for each. No parallel list is threaded through the plan.
fn resolve_serial_columns(
    catalog: &dyn CatalogManager,
    table: &str,
    columns: &[ParsedColumnDef],
) -> Result<Vec<ResolvedSerialColumn>> {
    let mut generated = HashSet::new();
    let mut resolved = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        if !matches!(column.default, Some(ParsedDefault::Serial)) {
            continue;
        }
        let sequence = choose_serial_sequence_name(catalog, &mut generated, table, &column.name)?;
        resolved.push(ResolvedSerialColumn { index, sequence });
    }
    Ok(resolved)
}

fn choose_serial_sequence_name(
    catalog: &dyn CatalogManager,
    generated: &mut HashSet<String>,
    table: &str,
    column: &str,
) -> Result<String> {
    let base = format!("{table}_{column}_seq");
    let mut suffix = 0_u64;
    loop {
        let candidate = if suffix == 0 {
            base.clone()
        } else {
            format!("{base}{suffix}")
        };
        if !generated.contains(&candidate) && catalog.get_sequence_by_name(&candidate)?.is_none() {
            generated.insert(candidate.clone());
            return Ok(candidate);
        }
        suffix = suffix
            .checked_add(1)
            .ok_or_else(|| DbError::internal("serial sequence suffix overflow"))?;
    }
}

fn create_owned_serial_sequence(ctx: &ExecutionContext<'_>, name: &str) -> Result<SequenceSchema> {
    let sequence =
        ctx.catalog
            .create_sequence(name.to_string(), SequenceOptions::default(), true)?;
    if let Err(err) = ctx.schema_ops.create_sequence(&ctx.statement, &sequence) {
        let _ = ctx.catalog.apply_drop_sequence(sequence.id);
        return Err(err);
    }
    Ok(sequence)
}

fn columns_with_serial_defaults(
    columns: &[ParsedColumnDef],
    serial: &[ResolvedSerialColumn],
) -> Result<Vec<ParsedColumnDef>> {
    let mut columns = columns.to_vec();
    for serial_column in serial {
        // `index` was derived from this same column list (each `SERIAL` column carries
        // `ParsedDefault::Serial`), so it always points at that column — guard
        // defensively rather than index-panic.
        let column = columns.get_mut(serial_column.index).ok_or_else(|| {
            DbError::internal(format!(
                "serial column index {} out of range in CREATE TABLE columns",
                serial_column.index
            ))
        })?;
        column.default = Some(ParsedDefault::OwnedNextval(serial_column.sequence.clone()));
    }
    Ok(columns)
}

fn cleanup_created_table(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    serial_sequences: &[SequenceSchema],
) {
    let _ = ctx.catalog.drop_table(table);
    cleanup_serial_sequences(ctx, serial_sequences);
}

fn cleanup_serial_sequences(ctx: &ExecutionContext<'_>, sequences: &[SequenceSchema]) {
    for sequence in sequences.iter().rev() {
        let _ = ctx.schema_ops.drop_sequence(&ctx.statement, sequence.id);
        let _ = ctx.catalog.apply_drop_sequence(sequence.id);
    }
}

/// Create one `UNIQUE` constraint's backing index on a freshly created table. The
/// index name follows PostgreSQL's `<table>_<col...>_key` convention.
fn create_unique_constraint_index(
    ctx: &ExecutionContext<'_>,
    schema: &TableSchema,
    columns: &[String],
) -> Result<()> {
    let name = format!("{}_{}_key", schema.name, columns.join("_"));
    let index = ctx.catalog.create_index_with_constraint(
        name,
        &schema.name,
        columns,
        true,
        IndexConstraintKind::Unique,
    )?;
    if let Err(err) = ctx
        .schema_ops
        .create_index(&ctx.statement, &index, ctx.gc_horizon)
    {
        let _ = ctx.catalog.drop_index(index.id);
        return Err(err);
    }
    Ok(())
}

fn create_primary_key_constraint_index(
    ctx: &ExecutionContext<'_>,
    schema: &TableSchema,
    columns: &[String],
) -> Result<()> {
    let name = format!("{}_pkey", schema.name);
    let index = ctx.catalog.create_index_with_constraint(
        name,
        &schema.name,
        columns,
        true,
        IndexConstraintKind::PrimaryKey,
    )?;
    if let Err(err) = ctx
        .schema_ops
        .create_index(&ctx.statement, &index, ctx.gc_horizon)
    {
        let _ = ctx.catalog.apply_drop_index(index.id);
        return Err(err);
    }
    Ok(())
}

fn execute_drop_tables(
    ctx: &ExecutionContext<'_>,
    targets: &[DropTableTarget],
    if_exists: bool,
) -> Result<ExecutionResult> {
    let mut resolved = Vec::with_capacity(targets.len());
    for target in targets {
        let table = match target.table {
            Some(table) => table,
            None if if_exists => match ctx.catalog.get_table_by_name(&target.name)? {
                Some(table) => table.id,
                None if ctx.catalog.get_view_by_name(&target.name)?.is_some() => {
                    return Err(DbError::plan(
                        SqlState::WrongObjectType,
                        format!("relation {} is a view, not a table", target.name),
                    ));
                }
                None => continue,
            },
            None => {
                return Err(DbError::plan(
                    SqlState::UndefinedTable,
                    format!("table {} does not exist", target.name),
                ));
            }
        };
        resolved.push((table, owned_sequences_for_table(ctx, table)?));
    }

    for (table, owned_sequences) in resolved {
        ctx.schema_ops.drop_table(&ctx.statement, table)?;
        for sequence in &owned_sequences {
            ctx.schema_ops.drop_sequence(&ctx.statement, sequence.id)?;
        }
        ctx.catalog.drop_table(table)?;
        for sequence in owned_sequences {
            ctx.catalog.apply_drop_sequence(sequence.id)?;
        }
    }
    Ok(ExecutionResult::Modified {
        command: "DROP TABLE".to_string(),
        count: 0,
    })
}

fn apply_rewrite_storage_ids(
    ctx: &ExecutionContext<'_>,
    table: TableId,
) -> Result<TruncateCatalogUpdate> {
    let plan = ctx.catalog.prepare_truncate_table(table)?;
    ctx.catalog.apply_truncate_table(&plan)
}

fn execute_alter_table_add_column(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    if_not_exists: bool,
    column: &ParsedColumnDef,
) -> Result<ExecutionResult> {
    let old_schema = require_table(ctx.catalog.as_ref(), table)?;
    if matches!(
        ctx.catalog
            .preflight_add_table_column(old_schema.id, if_not_exists, column)?,
        TableColumnAlteration::Noop
    ) {
        return Ok(alter_table_result());
    }

    let old_relations = ctx.relations.clone();
    let added = ctx
        .catalog
        .add_table_column(old_schema.id, column.clone())?;
    let rewrite = apply_rewrite_storage_ids(ctx, added.id)?;
    let schema = rewrite.table;
    if old_schema.toast_table_id != schema.toast_table_id
        && let Some(toast_schema) = rewrite.toast_table.as_ref()
    {
        ctx.schema_ops.create_table(&ctx.statement, toast_schema)?;
    } else if let Some(toast_schema) = rewrite.toast_table.as_ref() {
        ctx.schema_ops
            .update_table_schema(&ctx.statement, toast_schema, &[])?;
    }
    let indexes = rewrite.indexes;
    let added_column = schema
        .columns
        .last()
        .ok_or_else(|| DbError::internal("ALTER TABLE ADD COLUMN produced no column"))?;
    let default_exprs = expression_default_for_column(ctx, added_column)?;
    ctx.schema_ops
        .update_table_schema(&ctx.statement, &schema, &indexes)?;
    let rewrite_relations = ctx.storage.capture_relation_snapshot()?;

    ctx.storage.for_each_visible_row(
        &ctx.statement,
        old_relations.as_ref(),
        old_schema.id,
        &mut |mut stored| {
            check_canceled(ctx)?;
            let row = &mut stored.row;
            row.values.push(evaluate_column_default(
                &ctx.statement,
                added_column,
                &default_exprs,
            )?);
            coerce_numeric_columns(&schema, &mut row.values)?;
            validate_row_constraints(&schema, &row.values)?;
            ctx.storage.insert(
                &ctx.statement,
                rewrite_relations.as_ref(),
                schema.id,
                stored.row,
            )?;
            Ok(())
        },
    )?;

    Ok(alter_table_result())
}

fn execute_alter_table_drop_column(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    if_exists: bool,
    column: &str,
) -> Result<ExecutionResult> {
    let old_schema = require_table(ctx.catalog.as_ref(), table)?;
    if matches!(
        ctx.catalog
            .preflight_drop_table_column(old_schema.id, if_exists, column)?,
        TableColumnAlteration::Noop
    ) {
        return Ok(alter_table_result());
    }
    let position = old_schema
        .columns
        .iter()
        .position(|existing| existing.name == column)
        .ok_or_else(|| DbError::internal("preflight accepted a missing dropped column"))?;

    let old_relations = ctx.relations.clone();
    let dropped = ctx.catalog.drop_table_column(old_schema.id, column)?;
    let rewrite = apply_rewrite_storage_ids(ctx, dropped.id)?;
    let schema = rewrite.table;
    if let Some(toast_schema) = rewrite.toast_table.as_ref() {
        ctx.schema_ops
            .update_table_schema(&ctx.statement, toast_schema, &[])?;
    }
    let indexes = rewrite.indexes;
    ctx.schema_ops
        .update_table_schema(&ctx.statement, &schema, &indexes)?;
    let rewrite_relations = ctx.storage.capture_relation_snapshot()?;

    ctx.storage.for_each_visible_row(
        &ctx.statement,
        old_relations.as_ref(),
        old_schema.id,
        &mut |stored| {
            check_canceled(ctx)?;
            let mut values = stored.row.values;
            if position >= values.len() {
                return Err(DbError::internal(
                    "stored row is missing the dropped column position",
                ));
            }
            values.remove(position);
            coerce_numeric_columns(&schema, &mut values)?;
            validate_row_constraints(&schema, &values)?;
            ctx.storage.insert(
                &ctx.statement,
                rewrite_relations.as_ref(),
                schema.id,
                Row { values },
            )?;
            Ok(())
        },
    )?;

    Ok(alter_table_result())
}

fn execute_alter_table_rename_column(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    old_name: &str,
    new_name: &str,
) -> Result<ExecutionResult> {
    let old_schema = require_table(ctx.catalog.as_ref(), table)?;
    let schema = ctx
        .catalog
        .rename_table_column(old_schema.id, old_name, new_name.to_string())?;
    let indexes = ctx.catalog.list_indexes_for_table(schema.id)?;
    ctx.schema_ops
        .update_table_schema(&ctx.statement, &schema, &indexes)?;
    Ok(alter_table_result())
}

fn execute_alter_table_rename_table(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    new_name: &str,
) -> Result<ExecutionResult> {
    let old_schema = require_table(ctx.catalog.as_ref(), table)?;
    let schema = ctx
        .catalog
        .rename_table(old_schema.id, new_name.to_string())?;
    if schema != old_schema {
        let indexes = ctx.catalog.list_indexes_for_table(schema.id)?;
        ctx.schema_ops
            .update_table_schema(&ctx.statement, &schema, &indexes)?;
    }
    Ok(alter_table_result())
}

fn alter_table_result() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "ALTER TABLE".to_string(),
        count: 0,
    }
}

fn execute_create_view(
    ctx: &ExecutionContext<'_>,
    name: &str,
    or_replace: bool,
    columns: &[String],
    query: &planner::BoundQuery,
    definition: &str,
    dependencies: &[common::ViewDependency],
) -> Result<ExecutionResult> {
    let output = create_view_output_columns(columns, query);
    let schema = match ctx.catalog.get_view_by_name(name)? {
        Some(existing) if or_replace => {
            let replaced = ctx.catalog.replace_view(
                existing.id,
                output,
                definition.to_string(),
                dependencies.to_vec(),
            )?;
            if let Err(err) = ctx.schema_ops.replace_view(&ctx.statement, &replaced) {
                let _ = ctx.catalog.apply_replace_view(existing);
                return Err(err);
            }
            replaced
        }
        Some(_) => {
            return Err(DbError::plan(
                SqlState::DuplicateTable,
                format!("relation {name} already exists"),
            ));
        }
        None => {
            let schema = ctx.catalog.create_view(
                name.to_string(),
                output,
                definition.to_string(),
                dependencies.to_vec(),
            )?;
            if let Err(err) = ctx.schema_ops.create_view(&ctx.statement, &schema) {
                let _ = ctx.catalog.apply_drop_view(schema.id);
                return Err(err);
            }
            schema
        }
    };
    debug_assert_eq!(schema.name, name);
    Ok(ExecutionResult::Modified {
        command: "CREATE VIEW".to_string(),
        count: 0,
    })
}

fn create_view_output_columns(columns: &[String], query: &planner::BoundQuery) -> Vec<ViewColumn> {
    query
        .output_columns()
        .into_iter()
        .zip(query.output_schema())
        .enumerate()
        .map(|(index, (output, info))| ViewColumn {
            name: columns
                .get(index)
                .cloned()
                .unwrap_or_else(|| output.name.clone()),
            data_type: output.data_type,
            nullable: output.nullable,
            pg_type: info.pg_type.clone(),
        })
        .collect()
}

fn execute_drop_view(
    ctx: &ExecutionContext<'_>,
    name: &str,
    if_exists: bool,
) -> Result<ExecutionResult> {
    let view = match ctx.catalog.get_view_by_name(name)? {
        Some(view) => view,
        None if ctx.catalog.get_table_by_name(name)?.is_some() => {
            return Err(DbError::plan(
                SqlState::WrongObjectType,
                format!("relation {name} is a table, not a view"),
            ));
        }
        None if if_exists => {
            return Ok(ExecutionResult::Modified {
                command: "DROP VIEW".to_string(),
                count: 0,
            });
        }
        None => {
            return Err(DbError::plan(
                SqlState::UndefinedTable,
                format!("view {name} does not exist"),
            ));
        }
    };
    ctx.schema_ops.drop_view(&ctx.statement, view.id)?;
    ctx.catalog.drop_view(view.id)?;
    Ok(ExecutionResult::Modified {
        command: "DROP VIEW".to_string(),
        count: 0,
    })
}

fn expression_default_for_column(
    ctx: &ExecutionContext<'_>,
    column: &common::ColumnDef,
) -> Result<Vec<(ColumnId, BoundExpr)>> {
    match &column.default {
        Some(ColumnDefault::Expr(text)) => Ok(vec![(
            column.id,
            bind_default_expr(ctx.catalog.as_ref(), text)?,
        )]),
        _ => Ok(Vec::new()),
    }
}

fn owned_sequences_for_table(
    ctx: &ExecutionContext<'_>,
    table: TableId,
) -> Result<Vec<SequenceSchema>> {
    let Some(schema) = ctx.catalog.get_table(table)? else {
        return Ok(Vec::new());
    };
    let mut sequences = Vec::new();
    for column in &schema.columns {
        let Some(ColumnDefault::Nextval(sequence_id)) = column.default else {
            continue;
        };
        let Some(sequence) = ctx.catalog.get_sequence(sequence_id)? else {
            continue;
        };
        if sequence.owned {
            sequences.push(sequence);
        }
    }
    Ok(sequences)
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
    if ctx
        .catalog
        .get_index(index)?
        .is_some_and(|index| index.constraint == IndexConstraintKind::PrimaryKey)
    {
        return Err(DbError::plan(
            common::SqlState::DependentObjectsStillExist,
            "cannot drop index backing a primary key constraint",
        ));
    }
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
    Ok(output_schema_for_table(&require_table(catalog, table)?))
}

fn output_schema_for_table(schema: &TableSchema) -> Vec<ColumnInfo> {
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

/// Evaluate a table's `CHECK` constraints over a full row (catalog slot order).
/// A constraint that evaluates to `false` violates; `true` or `NULL` (unknown)
/// passes, matching PostgreSQL's three-valued semantics.
///
/// Enforcement is driven by `check_exprs` (the binder's bound form of the table's
/// checks, in the same order as `schema.checks`), so no check can be skipped by a
/// length mismatch; `schema.checks` supplies the constraint's text for the error
/// message when the two arrays line up, with a generic fallback otherwise.
fn validate_check_constraints(
    statement: &StatementContext,
    schema: &TableSchema,
    check_exprs: &[BoundExpr],
    values: &[Value],
) -> Result<()> {
    if check_exprs.is_empty() {
        return Ok(());
    }
    let row = ExecRow {
        row: Row {
            values: values.to_vec(),
        },
        identity: None,
    };
    for (index, expr) in check_exprs.iter().enumerate() {
        if matches!(eval_expr(statement, expr, &row)?, Value::Boolean(false)) {
            let text = schema
                .checks
                .get(index)
                .map(String::as_str)
                .unwrap_or("check constraint");
            return Err(DbError::execute(
                SqlState::CheckViolation,
                format!(
                    "new row for relation \"{}\" violates check constraint ({text})",
                    schema.name
                ),
            ));
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
            (Value::Array(array), Some(max))
                if matches!(array.element_type(), DataType::Text)
                    && array.elements().iter().any(|value| {
                        matches!(value, Value::Text(text) if text.chars().count() > max as usize)
                    }) => {
                return Err(DbError::execute(
                    SqlState::StringDataRightTruncation,
                    format!(
                        "array element too long for column {} (maximum {max} characters)",
                        column.name
                    ),
                ));
            }
            _ => {}
        }
        // A narrowed integer column (int2/int4) reports a distinct wire OID, so
        // the value must fit its width even though storage is a single 64-bit int.
        if let Value::Integer(int) = value
            && let Some(type_name) = column.wire_type().narrow_int_overflow(*int)
        {
            return Err(DbError::execute(
                SqlState::NumericValueOutOfRange,
                format!("{type_name} out of range for column {}", column.name),
            ));
        }
    }
    Ok(())
}

fn validate_value_type(column: &common::ColumnDef, value: &Value) -> Result<()> {
    if common::value_matches_type(value, &column.data_type) {
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

pub(crate) fn collect_all_cancelable(
    source: &mut dyn PlanExecutor,
    cancel: &QueryCancel,
) -> Result<Vec<ExecRow>> {
    collect_all_inner(source, cancel)
}

fn collect_all_inner(source: &mut dyn PlanExecutor, cancel: &QueryCancel) -> Result<Vec<ExecRow>> {
    cancel.check()?;
    open_executor(source)?;
    let result = (|| {
        let mut rows = Vec::new();
        loop {
            cancel.check()?;
            let Some(row) = source.next()? else {
                break;
            };
            rows.push(row);
        }
        cancel.check()?;
        Ok(rows)
    })();
    close_after(source, result)
}

pub(crate) fn open_executor(executor: &mut dyn PlanExecutor) -> Result<()> {
    if let Err(err) = executor.open() {
        let _ = executor.close();
        return Err(err);
    }
    Ok(())
}

pub(crate) fn close_after<T>(executor: &mut dyn PlanExecutor, result: Result<T>) -> Result<T> {
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
        DataType::Array(_) => "ARRAY",
    }
}

#[cfg(test)]
mod drive_tests {
    use super::*;
    use common::{CancelReason, QueryCancel};
    use common::{ColumnInfo, ExecRow, Row, Value};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Default)]
    struct MockCounts {
        opened: usize,
        closed: usize,
        yielded: usize,
    }

    /// A `PlanExecutor` stub for exercising the drive/close plumbing directly:
    /// it yields a fixed row sequence (optionally failing on `open` or on the
    /// nth `next`), and counts `open`/`close`/`next` calls so tests can assert
    /// the close invariant on every path.
    struct MockExecutor {
        schema: Vec<ColumnInfo>,
        rows: std::vec::IntoIter<Row>,
        fail_open: bool,
        fail_next_after: Option<usize>,
        opened: usize,
        closed: usize,
        yielded: usize,
        shared: Option<Rc<RefCell<MockCounts>>>,
    }

    impl MockExecutor {
        fn with_rows(count: usize) -> Self {
            Self::with_rows_and_shared(count, None)
        }

        fn with_shared_rows(count: usize) -> (Self, Rc<RefCell<MockCounts>>) {
            let shared = Rc::new(RefCell::new(MockCounts::default()));
            (
                Self::with_rows_and_shared(count, Some(shared.clone())),
                shared,
            )
        }

        fn with_rows_and_shared(count: usize, shared: Option<Rc<RefCell<MockCounts>>>) -> Self {
            let rows: Vec<Row> = (0..count)
                .map(|i| Row {
                    values: vec![Value::Integer(i as i64)],
                })
                .collect();
            Self {
                schema: vec![ColumnInfo {
                    name: "n".to_string(),
                    data_type: DataType::Integer,
                    table_id: None,
                    column_id: None,
                    pg_type: None,
                }],
                rows: rows.into_iter(),
                fail_open: false,
                fail_next_after: None,
                opened: 0,
                closed: 0,
                yielded: 0,
                shared,
            }
        }
    }

    impl PlanExecutor for MockExecutor {
        fn output_schema(&self) -> &[ColumnInfo] {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            self.opened += 1;
            if let Some(shared) = &self.shared {
                shared.borrow_mut().opened += 1;
            }
            if self.fail_open {
                return Err(DbError::internal("open failed"));
            }
            Ok(())
        }

        fn next(&mut self) -> Result<Option<ExecRow>> {
            if self.fail_next_after == Some(self.yielded) {
                return Err(DbError::internal("next failed"));
            }
            match self.rows.next() {
                Some(row) => {
                    self.yielded += 1;
                    if let Some(shared) = &self.shared {
                        shared.borrow_mut().yielded += 1;
                    }
                    Ok(Some(ExecRow {
                        row,
                        identity: None,
                    }))
                }
                None => Ok(None),
            }
        }

        fn close(&mut self) -> Result<()> {
            self.closed += 1;
            if let Some(shared) = &self.shared {
                shared.borrow_mut().closed += 1;
            }
            Ok(())
        }
    }

    /// A sink that optionally stops the scan once it has collected `break_at`
    /// rows.
    struct TestSink {
        break_at: Option<usize>,
        rows: usize,
    }

    impl TestSink {
        fn new() -> Self {
            Self {
                break_at: None,
                rows: 0,
            }
        }

        fn breaking_at(rows: usize) -> Self {
            Self {
                break_at: Some(rows),
                rows: 0,
            }
        }
    }

    impl RowSink for TestSink {
        fn start(&mut self, _columns: &[ColumnInfo]) -> Result<()> {
            Ok(())
        }

        fn push(&mut self, rows: Vec<Row>) -> Result<ControlFlow<()>> {
            self.rows += rows.len();
            match self.break_at {
                Some(limit) if self.rows >= limit => Ok(ControlFlow::Break(())),
                _ => Ok(ControlFlow::Continue(())),
            }
        }
    }

    struct FailingSink;

    impl RowSink for FailingSink {
        fn start(&mut self, _columns: &[ColumnInfo]) -> Result<()> {
            Ok(())
        }

        fn push(&mut self, _rows: Vec<Row>) -> Result<ControlFlow<()>> {
            Err(DbError::internal("sink failed"))
        }
    }

    #[test]
    fn open_query_closes_executor_after_normal_completion() {
        let cancel = QueryCancel::new();
        let (executor, counts) = MockExecutor::with_shared_rows(3);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut sink = TestSink::new();

        let status = query.fetch(None, &mut sink, 2).unwrap();

        assert_eq!(status, FetchStatus::Exhausted { count: 3 });
        assert_eq!(counts.borrow().opened, 1);
        assert_eq!(counts.borrow().closed, 1);
    }

    #[test]
    fn open_query_sink_break_looks_ahead_before_suspending() {
        let cancel = QueryCancel::new();
        let (executor, counts) = MockExecutor::with_shared_rows(5);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        // One row per batch; break once two rows have been pushed.
        let mut sink = TestSink::breaking_at(2);

        let status = query.fetch(None, &mut sink, 1).unwrap();

        assert_eq!(status, FetchStatus::Suspended { count: 2 });
        assert_eq!(
            counts.borrow().yielded,
            3,
            "one extra row is buffered as lookahead"
        );
        assert_eq!(counts.borrow().closed, 0, "suspended query remains open");

        let mut second = TestSink::new();
        let status = query.fetch(None, &mut second, 2).unwrap();
        assert_eq!(status, FetchStatus::Exhausted { count: 3 });
        assert_eq!(second.rows, 3);
    }

    #[test]
    fn open_query_sink_break_at_end_reports_exhausted() {
        let cancel = QueryCancel::new();
        let (executor, counts) = MockExecutor::with_shared_rows(2);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut sink = TestSink::breaking_at(2);

        let status = query.fetch(None, &mut sink, 1).unwrap();

        assert_eq!(status, FetchStatus::Exhausted { count: 2 });
        assert_eq!(counts.borrow().closed, 1);
    }

    #[test]
    fn open_query_closes_executor_on_next_error() {
        let cancel = QueryCancel::new();
        let (mut executor, counts) = MockExecutor::with_shared_rows(5);
        executor.fail_next_after = Some(2);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut sink = TestSink::new();

        let err = query.fetch(None, &mut sink, 2).unwrap_err();

        assert!(err.to_string().contains("next failed"));
        assert_eq!(
            counts.borrow().closed,
            1,
            "close runs after a mid-drive error"
        );
    }

    #[test]
    fn open_query_closes_executor_on_open_failure() {
        let cancel = QueryCancel::new();
        let (mut executor, counts) = MockExecutor::with_shared_rows(3);
        executor.fail_open = true;

        let err = match OpenQuery::from_executor(&cancel, Box::new(executor)) {
            Ok(_) => panic!("open should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("open failed"));
        assert_eq!(counts.borrow().opened, 1);
        assert_eq!(
            counts.borrow().closed,
            1,
            "open failure still closes the executor"
        );
        assert_eq!(
            counts.borrow().yielded,
            0,
            "next is never called after open fails"
        );
    }

    #[test]
    fn open_query_cancellation_aborts_and_closes() {
        let cancel = QueryCancel::new();
        cancel.request(CancelReason::UserRequest);
        let (executor, counts) = MockExecutor::with_shared_rows(3);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut sink = TestSink::new();

        let err = query.fetch(None, &mut sink, 2).unwrap_err();

        assert_eq!(err.code, SqlState::QueryCanceled);
        assert_eq!(
            counts.borrow().closed,
            1,
            "cancellation still closes the executor"
        );
    }

    #[test]
    fn open_query_sink_error_closes_executor() {
        let cancel = QueryCancel::new();
        let (executor, counts) = MockExecutor::with_shared_rows(3);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut sink = FailingSink;

        let err = query.fetch(None, &mut sink, 1).unwrap_err();

        assert!(err.to_string().contains("sink failed"));
        assert_eq!(
            counts.borrow().closed,
            1,
            "sink errors still close the executor"
        );
    }

    #[test]
    fn open_query_fetch_suspends_and_resumes() {
        let cancel = QueryCancel::new();
        let executor = MockExecutor::with_rows(5);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();

        let mut first = TestSink::new();
        let status = query.fetch(Some(2), &mut first, 1).unwrap();
        assert_eq!(status, FetchStatus::Suspended { count: 2 });
        assert_eq!(first.rows, 2);

        let mut second = TestSink::new();
        let status = query.fetch(Some(10), &mut second, 2).unwrap();
        assert_eq!(status, FetchStatus::Exhausted { count: 3 });
        assert_eq!(second.rows, 3);
    }

    #[test]
    fn open_query_exact_limit_reports_exhausted() {
        let cancel = QueryCancel::new();
        let executor = MockExecutor::with_rows(3);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut sink = TestSink::new();

        let status = query.fetch(Some(3), &mut sink, 2).unwrap();

        assert_eq!(status, FetchStatus::Exhausted { count: 3 });
        assert_eq!(sink.rows, 3);
    }

    #[test]
    fn open_query_zero_row_fetch_buffers_next_row() {
        let cancel = QueryCancel::new();
        let executor = MockExecutor::with_rows(2);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut first = TestSink::new();

        let status = query.fetch(Some(0), &mut first, 2).unwrap();
        assert_eq!(status, FetchStatus::Suspended { count: 0 });
        assert_eq!(first.rows, 0);

        let mut second = TestSink::new();
        let status = query.fetch(Some(1), &mut second, 2).unwrap();
        assert_eq!(status, FetchStatus::Suspended { count: 1 });
        assert_eq!(second.rows, 1);
    }

    #[test]
    fn open_query_zero_row_fetch_preserves_pending_row() {
        let cancel = QueryCancel::new();
        let executor = MockExecutor::with_rows(2);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();

        let mut first = TestSink::new();
        assert_eq!(
            query.fetch(Some(1), &mut first, 2).unwrap(),
            FetchStatus::Suspended { count: 1 }
        );

        let mut zero = TestSink::new();
        assert_eq!(
            query.fetch(Some(0), &mut zero, 2).unwrap(),
            FetchStatus::Suspended { count: 0 }
        );
        assert_eq!(zero.rows, 0);

        let mut second = TestSink::new();
        assert_eq!(
            query.fetch(Some(1), &mut second, 2).unwrap(),
            FetchStatus::Exhausted { count: 1 }
        );
        assert_eq!(
            second.rows, 1,
            "the row buffered by the first fetch was not skipped"
        );
    }

    #[test]
    fn open_query_closes_on_exhaustion_and_close_is_idempotent() {
        let cancel = QueryCancel::new();
        let (executor, counts) = MockExecutor::with_shared_rows(1);
        let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
        let mut sink = TestSink::new();

        let status = query.fetch(None, &mut sink, 2).unwrap();
        assert_eq!(status, FetchStatus::Exhausted { count: 1 });
        assert_eq!(counts.borrow().closed, 1);

        query.close().unwrap();
        assert_eq!(counts.borrow().closed, 1);
    }

    #[test]
    fn open_query_drop_closes_suspended_executor() {
        let cancel = QueryCancel::new();
        let (executor, counts) = MockExecutor::with_shared_rows(3);
        {
            let mut query = OpenQuery::from_executor(&cancel, Box::new(executor)).unwrap();
            let mut sink = TestSink::new();
            let status = query.fetch(Some(1), &mut sink, 1).unwrap();
            assert_eq!(status, FetchStatus::Suspended { count: 1 });
            assert_eq!(counts.borrow().closed, 0);
        }
        assert_eq!(counts.borrow().closed, 1);
    }
}
