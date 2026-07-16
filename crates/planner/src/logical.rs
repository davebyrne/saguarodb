use catalog::SystemView;
use common::{
    ColumnId, ColumnInfo, CompressionSetting, DbError, IndexId, ParsedColumnDef, Result, SchemaId,
    SequenceOptions, StoredQueryV1, TableId, ToastOptions,
};

use crate::{
    AggregateExpr, ApplyKind, BoundDistinct, BoundExpr, BoundForeignKey, BoundFrom,
    BoundInsertSource, BoundOnConflict, BoundOrderByItem, BoundQuery, BoundQueryBody,
    BoundReturning, BoundSelect, BoundStatement, JoinSide, JoinType, SetOp,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalPlan {
    CreateSchema {
        name: String,
        if_not_exists: bool,
    },
    DropSchema {
        name: String,
        if_exists: bool,
    },
    CreateTable {
        schema: SchemaId,
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        compression: CompressionSetting,
        toast: ToastOptions,
        /// `CHECK` constraint texts, persisted with the schema (see
        /// `BoundStatement::CreateTable`).
        checks: Vec<common::StoredExpression>,
        foreign_keys: Vec<BoundForeignKey>,
    },
    DropTable {
        targets: Vec<crate::DropTableTarget>,
        if_exists: bool,
    },
    AlterTableAddColumn {
        table: TableId,
        table_name: String,
        if_not_exists: bool,
        column: ParsedColumnDef,
    },
    AlterTableDropColumn {
        table: TableId,
        table_name: String,
        if_exists: bool,
        column: String,
    },
    AlterTableRenameColumn {
        table: TableId,
        table_name: String,
        old_name: String,
        new_name: String,
    },
    AlterTableRenameTable {
        table: TableId,
        table_name: String,
        new_name: String,
    },
    AlterTableAlterColumnType {
        table: TableId,
        table_name: String,
        column: String,
        data_type: common::DataType,
        pg_type: common::PgType,
    },
    CreateIndex {
        schema: SchemaId,
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
        index: IndexId,
    },
    CreateSequence {
        schema: SchemaId,
        name: String,
        options: SequenceOptions,
    },
    DropSequence {
        name: String,
        search_path: Vec<SchemaId>,
        sequence: Option<common::SequenceId>,
        if_exists: bool,
    },
    CreateView {
        schema: SchemaId,
        name: String,
        or_replace: bool,
        columns: Vec<String>,
        query: BoundQuery,
        definition: String,
        stored_query: StoredQueryV1,
        definition_search_path: Vec<SchemaId>,
    },
    DropView {
        name: String,
        search_path: Vec<SchemaId>,
        view: Option<TableId>,
        if_exists: bool,
    },
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: Box<LogicalPlan>,
        on_conflict: Option<BoundOnConflict>,
        returning: Option<BoundReturning>,
        /// Bound expression `DEFAULT`s for omitted columns (see
        /// `BoundStatement::Insert`), carried through to execution.
        default_exprs: Vec<(ColumnId, BoundExpr)>,
        /// Bound `CHECK` expressions enforced per inserted row (see
        /// `BoundStatement::Insert`).
        check_exprs: Vec<BoundExpr>,
    },
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: Box<LogicalPlan>,
        /// `UPDATE ... FROM`: source rows are the combined (target ++ FROM)
        /// row; hoisting must not narrow them (`docs/specs/subqueries.md` §8).
        joined_source: bool,
        returning: Option<BoundReturning>,
        /// Bound `CHECK` expressions enforced per updated row (see
        /// `BoundStatement::Update`).
        check_exprs: Vec<BoundExpr>,
    },
    Delete {
        table: TableId,
        source: Box<LogicalPlan>,
        /// `DELETE ... USING` (`docs/specs/subqueries.md` §8).
        joined_source: bool,
        returning: Option<BoundReturning>,
    },
    LockRows {
        source: Box<LogicalPlan>,
        table: TableId,
        mode: common::TupleLockMode,
        wait_policy: common::TupleLockWaitPolicy,
        recheck: Option<BoundExpr>,
        expressions: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Scan {
        table: TableId,
        filter: Option<BoundExpr>,
    },
    /// Dependent join (`docs/specs/subqueries.md` §5): per `input` row, the
    /// correlated `subplan` template is re-executed with each `OuterRef { slot }`
    /// replaced by the value of `correlations[slot]` evaluated against that
    /// row, and one column (per `kind`) is appended after the input columns.
    /// Row identity passes through from the input side.
    Apply {
        input: Box<LogicalPlan>,
        subplan: Box<LogicalPlan>,
        correlations: Vec<BoundExpr>,
        kind: ApplyKind,
    },
    SystemScan {
        view: SystemView,
        filter: Option<BoundExpr>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Option<BoundExpr>,
        join_type: JoinType,
        /// `Some(Left)` on a DML-source join spine: combined rows carry the
        /// left side's row identity (`docs/specs/subqueries.md` §8.1).
        identity_from: Option<JoinSide>,
    },
    Filter {
        source: Box<LogicalPlan>,
        predicate: BoundExpr,
    },
    Projection {
        source: Box<LogicalPlan>,
        expressions: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Sort {
        source: Box<LogicalPlan>,
        order_by: Vec<BoundOrderByItem>,
    },
    /// De-duplicate rows by `on_keys`, keeping the first row of each distinct
    /// key in input order. For plain `SELECT DISTINCT`, `on_keys` are the output
    /// (projection) expressions, so whole rows are de-duplicated.
    Distinct {
        source: Box<LogicalPlan>,
        on_keys: Vec<BoundExpr>,
    },
    Limit {
        source: Box<LogicalPlan>,
        count: u64,
        offset: Option<u64>,
    },
    Aggregate {
        source: Box<LogicalPlan>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Values {
        rows: Vec<Vec<BoundExpr>>,
        output_schema: Vec<ColumnInfo>,
    },
    TableFunction {
        name: String,
        args: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    /// A set operation over two sub-plans. `all` keeps duplicates; otherwise the
    /// combined result is de-duplicated. Both sides produce identically-typed rows
    /// (the binder reconciled them), so the output schema is the left side's.
    SetOp {
        op: SetOp,
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
}

pub fn logical_plan(bound: &BoundStatement) -> Result<LogicalPlan> {
    let plan = build_logical_plan(bound)?;
    Ok(crate::simplify::simplify_logical(plan))
}

fn build_logical_plan(bound: &BoundStatement) -> Result<LogicalPlan> {
    match bound {
        BoundStatement::CreateSchema {
            name,
            if_not_exists,
        } => Ok(LogicalPlan::CreateSchema {
            name: name.clone(),
            if_not_exists: *if_not_exists,
        }),
        BoundStatement::DropSchema { name, if_exists } => Ok(LogicalPlan::DropSchema {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        BoundStatement::CreateTable {
            schema,
            name,
            if_not_exists,
            columns,
            primary_key,
            unique,
            compression,
            toast,
            checks,
            foreign_keys,
        } => Ok(LogicalPlan::CreateTable {
            schema: *schema,
            name: name.clone(),
            if_not_exists: *if_not_exists,
            columns: columns.clone(),
            primary_key: primary_key.clone(),
            unique: unique.clone(),
            compression: *compression,
            toast: toast.clone(),
            checks: checks.clone(),
            foreign_keys: foreign_keys.clone(),
        }),
        BoundStatement::DropTable { targets, if_exists } => Ok(LogicalPlan::DropTable {
            targets: targets.clone(),
            if_exists: *if_exists,
        }),
        BoundStatement::AlterTableAddColumn {
            table,
            table_name,
            if_not_exists,
            column,
        } => Ok(LogicalPlan::AlterTableAddColumn {
            table: *table,
            table_name: table_name.clone(),
            if_not_exists: *if_not_exists,
            column: column.clone(),
        }),
        BoundStatement::AlterTableDropColumn {
            table,
            table_name,
            if_exists,
            column,
        } => Ok(LogicalPlan::AlterTableDropColumn {
            table: *table,
            table_name: table_name.clone(),
            if_exists: *if_exists,
            column: column.clone(),
        }),
        BoundStatement::AlterTableRenameColumn {
            table,
            table_name,
            old_name,
            new_name,
        } => Ok(LogicalPlan::AlterTableRenameColumn {
            table: *table,
            table_name: table_name.clone(),
            old_name: old_name.clone(),
            new_name: new_name.clone(),
        }),
        BoundStatement::AlterTableRenameTable {
            table,
            table_name,
            new_name,
        } => Ok(LogicalPlan::AlterTableRenameTable {
            table: *table,
            table_name: table_name.clone(),
            new_name: new_name.clone(),
        }),
        BoundStatement::AlterTableAlterColumnType {
            table,
            table_name,
            column,
            data_type,
            pg_type,
        } => Ok(LogicalPlan::AlterTableAlterColumnType {
            table: *table,
            table_name: table_name.clone(),
            column: column.clone(),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
        }),
        BoundStatement::CreateIndex {
            schema,
            name,
            table,
            columns,
            unique,
        } => Ok(LogicalPlan::CreateIndex {
            schema: *schema,
            name: name.clone(),
            table: table.clone(),
            columns: columns.clone(),
            unique: *unique,
        }),
        BoundStatement::DropIndex { index } => Ok(LogicalPlan::DropIndex { index: *index }),
        BoundStatement::CreateSequence {
            schema,
            name,
            options,
        } => Ok(LogicalPlan::CreateSequence {
            schema: *schema,
            name: name.clone(),
            options: options.clone(),
        }),
        BoundStatement::DropSequence {
            name,
            search_path,
            sequence,
            if_exists,
        } => Ok(LogicalPlan::DropSequence {
            name: name.clone(),
            search_path: search_path.clone(),
            sequence: *sequence,
            if_exists: *if_exists,
        }),
        BoundStatement::CreateView {
            schema,
            name,
            or_replace,
            columns,
            query,
            definition,
            stored_query,
            definition_search_path,
        } => Ok(LogicalPlan::CreateView {
            schema: *schema,
            name: name.clone(),
            or_replace: *or_replace,
            columns: columns.clone(),
            query: query.clone(),
            definition: definition.clone(),
            stored_query: stored_query.clone(),
            definition_search_path: definition_search_path.clone(),
        }),
        BoundStatement::DropView {
            name,
            search_path,
            view,
            if_exists,
        } => Ok(LogicalPlan::DropView {
            name: name.clone(),
            search_path: search_path.clone(),
            view: *view,
            if_exists: *if_exists,
        }),
        BoundStatement::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            default_exprs,
            check_exprs,
        } => {
            let source = match source {
                BoundInsertSource::Values {
                    rows,
                    output_schema,
                } => LogicalPlan::Values {
                    rows: rows.clone(),
                    output_schema: output_schema.clone(),
                },
                BoundInsertSource::Query(query) => plan_query(query)?,
            };
            Ok(LogicalPlan::Insert {
                table: *table,
                columns: columns.clone(),
                source: Box::new(source),
                on_conflict: on_conflict.clone(),
                returning: returning.clone(),
                default_exprs: default_exprs.clone(),
                check_exprs: check_exprs.clone(),
            })
        }
        BoundStatement::Query(query) => plan_query(query),
        BoundStatement::Update {
            table,
            assignments,
            source,
            joined_source,
            returning,
            check_exprs,
        } => {
            let mut source = plan_select_source(source)?;
            if *joined_source {
                mark_dml_spine_identity(&mut source);
            }
            Ok(LogicalPlan::Update {
                table: *table,
                assignments: assignments.clone(),
                source: Box::new(source),
                joined_source: *joined_source,
                returning: returning.clone(),
                check_exprs: check_exprs.clone(),
            })
        }
        BoundStatement::Delete {
            table,
            source,
            joined_source,
            returning,
        } => {
            let mut source = plan_select_source(source)?;
            if *joined_source {
                mark_dml_spine_identity(&mut source);
            }
            Ok(LogicalPlan::Delete {
                table: *table,
                source: Box::new(source),
                joined_source: *joined_source,
                returning: returning.clone(),
            })
        }
        BoundStatement::Explain { .. } => Err(DbError::plan(
            common::SqlState::SyntaxError,
            "logical_plan does not accept EXPLAIN; plan the inner statement",
        )),
        // COPY is not lowered to a logical plan; the server drives it over the
        // COPY sub-protocol, reusing the storage insert/scan paths. Reaching here
        // is a routing bug.
        BoundStatement::Copy { .. } => Err(DbError::internal(
            "logical_plan does not accept COPY; the server drives it directly",
        )),
    }
}

/// Lower a bound query: lower its body, then apply the query-level
/// `ORDER BY`/`LIMIT`/`OFFSET`. A set-operation body adds an arm here that combines
/// its arms' plans before those modifiers.
pub(crate) fn plan_query(query: &BoundQuery) -> Result<LogicalPlan> {
    match &query.body {
        BoundQueryBody::Select(select) => plan_select_body(
            select,
            &query.order_by,
            query.limit,
            query.offset,
            query.row_lock.as_ref(),
        ),
        // A VALUES body is a literal row set. It lowers directly to the existing
        // `Values` node; the query-level `ORDER BY` (bound to output positions) and
        // `LIMIT`/`OFFSET` stack above.
        BoundQueryBody::Values(values) => {
            let plan = LogicalPlan::Values {
                rows: values.rows.clone(),
                output_schema: values.output_schema.clone(),
            };
            Ok(apply_order_and_limit(
                plan,
                &query.order_by,
                query.limit,
                query.offset,
            ))
        }
        // A set operation lowers each arm and combines them; the query-level
        // `ORDER BY` (bound to output positions) and `LIMIT`/`OFFSET` stack above.
        BoundQueryBody::SetOp(set_op) => {
            let plan = LogicalPlan::SetOp {
                op: set_op.op,
                all: set_op.all,
                left: Box::new(plan_query(&set_op.left)?),
                right: Box::new(plan_query(&set_op.right)?),
            };
            Ok(apply_order_and_limit(
                plan,
                &query.order_by,
                query.limit,
                query.offset,
            ))
        }
    }
}

/// Stack a `Sort` for the query-level `ORDER BY` (already bound to output-position
/// keys) and then the `LIMIT`/`OFFSET`, above a body plan that carries no ordering
/// of its own (a VALUES or set-operation body). Empty `order_by` skips the `Sort`.
fn apply_order_and_limit(
    plan: LogicalPlan,
    order_by: &[BoundOrderByItem],
    limit: Option<u64>,
    offset: Option<u64>,
) -> LogicalPlan {
    let plan = if order_by.is_empty() {
        plan
    } else {
        LogicalPlan::Sort {
            source: Box::new(plan),
            order_by: order_by.to_vec(),
        }
    };
    apply_limit(plan, limit, offset)
}

/// Stack a `Limit` node for the query-level `LIMIT`/`OFFSET`. A bare `OFFSET` with
/// no `LIMIT` is `count = u64::MAX`; no `LIMIT`/`OFFSET` leaves the plan unchanged.
fn apply_limit(plan: LogicalPlan, limit: Option<u64>, offset: Option<u64>) -> LogicalPlan {
    if let Some(limit) = limit {
        LogicalPlan::Limit {
            source: Box::new(plan),
            count: limit,
            offset,
        }
    } else if let Some(offset) = offset {
        LogicalPlan::Limit {
            source: Box::new(plan),
            count: u64::MAX,
            offset: Some(offset),
        }
    } else {
        plan
    }
}

/// Lower a `SELECT` block with the enclosing query's `ORDER BY`/`LIMIT`/`OFFSET`.
/// The modifiers are passed in (rather than read from the block) because they live
/// on the [`BoundQuery`] wrapper; the aggregate-context `ORDER BY` rewrite stays
/// here because it depends on this block's `group_by`/aggregates.
fn plan_select_body(
    select: &BoundSelect,
    order_by: &[BoundOrderByItem],
    limit: Option<u64>,
    offset: Option<u64>,
    row_lock: Option<&crate::BoundRowLock>,
) -> Result<LogicalPlan> {
    let mut plan = plan_select_source(select)?;

    let aggregate_context = !select.group_by.is_empty()
        || select
            .columns
            .iter()
            .any(|item| contains_aggregate(&item.expr))
        || select.having.is_some()
        || order_by.iter().any(|item| contains_aggregate(&item.expr));

    if aggregate_context {
        let mut aggregates = Vec::new();
        for item in &select.columns {
            collect_aggregates(&item.expr, &mut aggregates);
        }
        if let Some(having) = &select.having {
            collect_aggregates(having, &mut aggregates);
        }
        for item in order_by {
            collect_aggregates(&item.expr, &mut aggregates);
        }

        let output_schema = aggregate_output_schema(&select.group_by, &aggregates);
        plan = LogicalPlan::Aggregate {
            source: Box::new(plan),
            group_by: select.group_by.clone(),
            aggregates: aggregates.clone(),
            output_schema,
        };

        if let Some(having) = &select.having {
            plan = LogicalPlan::Filter {
                source: Box::new(plan),
                predicate: rewrite_aggregate_expr(having, &select.group_by, &aggregates)?,
            };
        }

        if !order_by.is_empty() {
            plan = LogicalPlan::Sort {
                source: Box::new(plan),
                order_by: order_by
                    .iter()
                    .map(|item| {
                        Ok(BoundOrderByItem {
                            expr: rewrite_aggregate_expr(
                                &item.expr,
                                &select.group_by,
                                &aggregates,
                            )?,
                            ascending: item.ascending,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            };
        }

        let expressions = select
            .columns
            .iter()
            .map(|item| rewrite_aggregate_expr(&item.expr, &select.group_by, &aggregates))
            .collect::<Result<Vec<_>>>()?;
        // DISTINCT ON keys may reference grouped columns (the binder rejects
        // aggregates in them), so they get the same grouped-expression rewrite
        // as the projection expressions.
        let distinct_keys = match &select.distinct {
            None => None,
            Some(BoundDistinct::All) => Some(expressions.clone()),
            Some(BoundDistinct::On(on)) => Some(
                on.iter()
                    .map(|expr| rewrite_aggregate_expr(expr, &select.group_by, &aggregates))
                    .collect::<Result<Vec<_>>>()?,
            ),
        };
        plan = apply_distinct_and_projection(plan, select, distinct_keys, expressions);
    } else {
        if !order_by.is_empty() {
            plan = LogicalPlan::Sort {
                source: Box::new(plan),
                order_by: order_by.to_vec(),
            };
        }

        let expressions: Vec<BoundExpr> = select
            .columns
            .iter()
            .map(|item| item.expr.clone())
            .collect();
        let distinct_keys = match &select.distinct {
            None => None,
            Some(BoundDistinct::All) => Some(expressions.clone()),
            Some(BoundDistinct::On(on)) => Some(on.clone()),
        };
        plan = if let Some(lock) = row_lock {
            if distinct_keys.is_some() {
                return Err(DbError::internal(
                    "locking SELECT reached planning with DISTINCT",
                ));
            }
            LogicalPlan::LockRows {
                source: Box::new(plan),
                table: lock.table,
                mode: lock.mode,
                wait_policy: lock.wait_policy,
                recheck: select.filter.clone(),
                expressions,
                output_schema: select.output_schema.clone(),
            }
        } else {
            apply_distinct_and_projection(plan, select, distinct_keys, expressions)
        };
    }

    Ok(apply_limit(plan, limit, offset))
}

/// Stack the optional `Distinct` node below the `Projection`. `Distinct` sits
/// between any `Sort` and the `Projection`, so that after sorting, keeping the
/// first row per distinct key yields correctly ordered distinct output. For
/// plain `SELECT DISTINCT` the dedup keys are the projection expressions (whole
/// output rows); for `DISTINCT ON` they are the `ON` key expressions.
fn apply_distinct_and_projection(
    mut plan: LogicalPlan,
    select: &BoundSelect,
    distinct_keys: Option<Vec<BoundExpr>>,
    expressions: Vec<BoundExpr>,
) -> LogicalPlan {
    if let Some(on_keys) = distinct_keys {
        plan = LogicalPlan::Distinct {
            source: Box::new(plan),
            on_keys,
        };
    }
    LogicalPlan::Projection {
        source: Box::new(plan),
        expressions,
        output_schema: select.output_schema.clone(),
    }
}

fn plan_select_source(select: &BoundSelect) -> Result<LogicalPlan> {
    match &select.from {
        Some(from) => plan_from(from, select.filter.clone()),
        // A FROM-less SELECT (`SELECT 1`) evaluates its projection over a single
        // unit row: a one-row, zero-column `Values` node (already supported by the
        // physical planner and executor). A `WHERE`, if present, filters that row.
        None => {
            let unit = LogicalPlan::Values {
                rows: vec![vec![]],
                output_schema: Vec::new(),
            };
            Ok(match select.filter.clone() {
                Some(predicate) => LogicalPlan::Filter {
                    source: Box::new(unit),
                    predicate,
                },
                None => unit,
            })
        }
    }
}

fn plan_from(from: &BoundFrom, filter: Option<BoundExpr>) -> Result<LogicalPlan> {
    plan_from_at(from, 0, filter)
}

/// The number of columns a FROM subtree contributes to the joined row.
fn bound_from_width(from: &BoundFrom) -> usize {
    match from {
        BoundFrom::Table { schema, .. }
        | BoundFrom::System { schema, .. }
        | BoundFrom::Derived { schema, .. }
        | BoundFrom::View { schema, .. }
        | BoundFrom::TableFunction { schema, .. } => schema.len(),
        BoundFrom::Join { left, right, .. } => bound_from_width(left) + bound_from_width(right),
    }
}

/// Rebase FROM-scope slot references onto a join subtree's local row: the
/// binder assigns slots across the WHOLE FROM list, but a join operator (and
/// a lateral Apply's input) sees only its subtree's columns, starting at
/// `offset`. The binder's ON-scope visibility rule guarantees no reference
/// points below `offset`.
fn rebase_from_scope_expr(expr: &BoundExpr, offset: usize) -> Result<BoundExpr> {
    if offset == 0 {
        return Ok(expr.clone());
    }
    crate::rewrite::rewrite_expr(expr, &mut |node| match node {
        BoundExpr::InputRef {
            input,
            column,
            slot,
            data_type,
            nullable,
        } => {
            let slot = slot.checked_sub(offset).ok_or_else(|| {
                DbError::internal("join-scope expression references a column outside its subtree")
            })?;
            Ok(Some(BoundExpr::InputRef {
                input: *input,
                column: *column,
                slot,
                data_type: data_type.clone(),
                nullable: *nullable,
            }))
        }
        _ => Ok(None),
    })
}

/// Lower a FROM subtree whose leftmost column sits at global slot `offset`.
/// Join conditions and lateral correlations bind with global FROM-scope
/// slots and are rebased here to the subtree's local row.
fn plan_from_at(from: &BoundFrom, offset: usize, filter: Option<BoundExpr>) -> Result<LogicalPlan> {
    match from {
        BoundFrom::Table { table, .. } => Ok(LogicalPlan::Scan {
            table: *table,
            filter,
        }),
        BoundFrom::System { view, .. } => Ok(LogicalPlan::SystemScan {
            view: *view,
            filter,
        }),
        BoundFrom::TableFunction {
            name, args, schema, ..
        } => {
            let plan = LogicalPlan::TableFunction {
                name: name.clone(),
                args: args.clone(),
                output_schema: schema
                    .iter()
                    .map(|column| ColumnInfo {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        table_id: None,
                        column_id: None,
                        pg_type: column.pg_type.clone(),
                    })
                    .collect(),
            };
            Ok(match filter {
                Some(predicate) => LogicalPlan::Filter {
                    source: Box::new(plan),
                    predicate,
                },
                None => plan,
            })
        }
        // A derived table lowers to its inner query's plan. Its columns already
        // sit at the derived binding's slots, so an outer WHERE (the standalone
        // case) is applied as a Filter above it — it cannot be pushed into the
        // inner scan. A correlated LATERAL in this position (standalone, or
        // the left item of a join) has no siblings, only chained
        // enclosing-scope entries; it still lowers to an Apply — over a unit
        // Values row — so its correlation list is carried on the plan node
        // and substituted by slot like every other Apply, instead of the
        // body's OuterRefs being embedded against a possibly divergent
        // enclosing index space.
        BoundFrom::Derived { query, .. } | BoundFrom::View { query, .. } => {
            let plan = if let BoundFrom::Derived {
                lateral: true,
                schema,
                ..
            } = from
                && !query.correlations.is_empty()
            {
                LogicalPlan::Apply {
                    input: Box::new(LogicalPlan::Values {
                        rows: vec![vec![]],
                        output_schema: Vec::new(),
                    }),
                    subplan: Box::new(plan_query(query)?),
                    correlations: query
                        .correlations
                        .iter()
                        .map(|correlation| correlation.outer.clone())
                        .collect(),
                    kind: ApplyKind::Lateral {
                        left_join: false,
                        condition: None,
                        output_schema: schema
                            .iter()
                            .map(|column| ColumnInfo {
                                name: column.name.clone(),
                                data_type: column.data_type.clone(),
                                table_id: None,
                                column_id: None,
                                pg_type: column.pg_type.clone(),
                            })
                            .collect(),
                    },
                }
            } else {
                plan_query(query)?
            };
            Ok(match filter {
                Some(predicate) => LogicalPlan::Filter {
                    source: Box::new(plan),
                    predicate,
                },
                None => plan,
            })
        }
        BoundFrom::Join {
            left,
            right,
            condition,
            join_type,
        } => {
            // A LATERAL derived table on the right lowers to an Apply: its
            // body re-executes per left row with the sibling references
            // substituted, appending the derived columns
            // (`docs/specs/subqueries.md` §7). The ON condition binds in the
            // full FROM scope, whose slots already match the combined
            // (left ++ derived) row.
            let mut plan = if let BoundFrom::TableFunction {
                name, args, schema, ..
            } = &**right
            {
                let output_schema: Vec<ColumnInfo> = schema
                    .iter()
                    .map(|column| ColumnInfo {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        table_id: None,
                        column_id: None,
                        pg_type: column.pg_type.clone(),
                    })
                    .collect();
                let subplan_args = args
                    .iter()
                    .enumerate()
                    .map(|(slot, arg)| BoundExpr::OuterRef {
                        slot,
                        data_type: arg.data_type(),
                        nullable: arg.nullable(),
                    })
                    .collect();
                LogicalPlan::Apply {
                    input: Box::new(plan_from_at(left, offset, None)?),
                    subplan: Box::new(LogicalPlan::TableFunction {
                        name: name.clone(),
                        args: subplan_args,
                        output_schema: output_schema.clone(),
                    }),
                    correlations: args
                        .iter()
                        .map(|arg| rebase_from_scope_expr(arg, offset))
                        .collect::<Result<Vec<_>>>()?,
                    kind: ApplyKind::Lateral {
                        left_join: matches!(join_type, JoinType::Left),
                        condition: condition
                            .as_ref()
                            .map(|condition| {
                                rebase_from_scope_expr(condition, offset).map(Box::new)
                            })
                            .transpose()?,
                        output_schema,
                    },
                }
            } else if let BoundFrom::Derived {
                query,
                schema,
                lateral: true,
                ..
            } = &**right
            {
                LogicalPlan::Apply {
                    input: Box::new(plan_from_at(left, offset, None)?),
                    subplan: Box::new(plan_query(query)?),
                    correlations: query
                        .correlations
                        .iter()
                        .map(|correlation| rebase_from_scope_expr(&correlation.outer, offset))
                        .collect::<Result<Vec<_>>>()?,
                    kind: ApplyKind::Lateral {
                        left_join: matches!(join_type, JoinType::Left),
                        condition: condition
                            .as_ref()
                            .map(|condition| {
                                rebase_from_scope_expr(condition, offset).map(Box::new)
                            })
                            .transpose()?,
                        output_schema: schema
                            .iter()
                            .map(|column| ColumnInfo {
                                name: column.name.clone(),
                                data_type: column.data_type.clone(),
                                table_id: None,
                                column_id: None,
                                pg_type: column.pg_type.clone(),
                            })
                            .collect(),
                    },
                }
            } else {
                LogicalPlan::Join {
                    left: Box::new(plan_from_at(left, offset, None)?),
                    right: Box::new(plan_from_at(right, offset + bound_from_width(left), None)?),
                    condition: condition
                        .as_ref()
                        .map(|condition| rebase_from_scope_expr(condition, offset))
                        .transpose()?,
                    join_type: *join_type,
                    identity_from: None,
                }
            };
            if let Some(filter) = filter {
                plan = LogicalPlan::Filter {
                    source: Box::new(plan),
                    predicate: filter,
                };
            }
            Ok(plan)
        }
    }
}

fn aggregate_output_schema(
    group_by: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> Vec<ColumnInfo> {
    let mut output = Vec::with_capacity(group_by.len() + aggregates.len());
    for (index, expr) in group_by.iter().enumerate() {
        output.push(ColumnInfo {
            name: format!("group_{index}"),
            data_type: expr.data_type(),
            table_id: None,
            column_id: None,
            pg_type: None,
        });
    }
    for (index, aggregate) in aggregates.iter().enumerate() {
        output.push(ColumnInfo {
            name: format!("aggregate_{index}"),
            data_type: aggregate.data_type.clone(),
            table_id: None,
            column_id: None,
            pg_type: None,
        });
    }
    output
}

fn collect_aggregates(expr: &BoundExpr, output: &mut Vec<AggregateExpr>) {
    match expr {
        BoundExpr::AggregateCall {
            func,
            arg,
            distinct,
            data_type,
            nullable,
        } => {
            let aggregate = AggregateExpr {
                func: *func,
                arg: arg.as_deref().cloned(),
                distinct: *distinct,
                data_type: data_type.clone(),
                nullable: *nullable,
            };
            if !output.iter().any(|existing| existing == &aggregate) {
                output.push(aggregate);
            }
        }
        BoundExpr::BinaryOp { left, right, .. } => {
            collect_aggregates(left, output);
            collect_aggregates(right, output);
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. }
        | BoundExpr::RuntimeInSet { expr, .. } => collect_aggregates(expr, output),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                collect_aggregates(arg, output);
            }
        }
        BoundExpr::Array { elements, .. } => {
            for element in elements {
                collect_aggregates(element, output);
            }
        }
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            collect_aggregates(array, output);
            for subscript in subscripts {
                collect_aggregates(subscript, output);
            }
        }
        BoundExpr::Any { left, array, .. } => {
            collect_aggregates(left, output);
            collect_aggregates(array, output);
        }
        BoundExpr::Setval {
            value, is_called, ..
        } => {
            collect_aggregates(value, output);
            if let Some(is_called) = is_called {
                collect_aggregates(is_called, output);
            }
        }
        BoundExpr::InList { expr, list, .. } => {
            collect_aggregates(expr, output);
            for item in list {
                collect_aggregates(item, output);
            }
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            collect_aggregates(expr, output);
            collect_aggregates(low, output);
            collect_aggregates(high, output);
        }
        BoundExpr::Like { expr, pattern, .. } => {
            collect_aggregates(expr, output);
            collect_aggregates(pattern, output);
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                collect_aggregates(operand, output);
            }
            for (when, then) in when_clauses {
                collect_aggregates(when, output);
                collect_aggregates(then, output);
            }
            if let Some(else_clause) = else_clause {
                collect_aggregates(else_clause, output);
            }
        }
        // A subquery body is its own aggregation scope; only `InSubquery`'s
        // left operand belongs to the outer query and may carry an aggregate.
        // Correlation entries are bare column references (`InputRef`/`OuterRef`),
        // never aggregates.
        BoundExpr::InSubquery { expr, .. } => collect_aggregates(expr, output),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => {}
    }
}

fn rewrite_aggregate_expr(
    expr: &BoundExpr,
    group_by: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> Result<BoundExpr> {
    if let Some(index) = group_by.iter().position(|group| group == expr) {
        return Ok(BoundExpr::LocalRef {
            slot: index,
            data_type: expr.data_type(),
            nullable: expr.nullable(),
        });
    }

    if let BoundExpr::AggregateCall {
        func,
        arg,
        distinct,
        data_type,
        nullable,
    } = expr
    {
        let aggregate = AggregateExpr {
            func: *func,
            arg: arg.as_deref().cloned(),
            distinct: *distinct,
            data_type: data_type.clone(),
            nullable: *nullable,
        };
        let index = aggregates
            .iter()
            .position(|existing| existing == &aggregate)
            .ok_or_else(|| DbError::internal("aggregate expression was not extracted"))?;
        return Ok(BoundExpr::LocalRef {
            slot: group_by.len() + index,
            data_type: data_type.clone(),
            nullable: *nullable,
        });
    }

    match expr {
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            nullable,
        } => Ok(BoundExpr::BinaryOp {
            left: Box::new(rewrite_aggregate_expr(left, group_by, aggregates)?),
            op: *op,
            right: Box::new(rewrite_aggregate_expr(right, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::UnaryOp {
            op,
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Function {
            name,
            args,
            data_type,
            pg_type,
            nullable,
        } => Ok(BoundExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| rewrite_aggregate_expr(arg, group_by, aggregates))
                .collect::<Result<Vec<_>>>()?,
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => Ok(BoundExpr::Array {
            elements: elements
                .iter()
                .map(|e| rewrite_aggregate_expr(e, group_by, aggregates))
                .collect::<Result<Vec<_>>>()?,
            dimensions: dimensions.clone(),
            element_type: element_type.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => Ok(BoundExpr::ArraySubscript {
            array: Box::new(rewrite_aggregate_expr(array, group_by, aggregates)?),
            subscripts: subscripts
                .iter()
                .map(|e| rewrite_aggregate_expr(e, group_by, aggregates))
                .collect::<Result<Vec<_>>>()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => Ok(BoundExpr::Any {
            left: Box::new(rewrite_aggregate_expr(left, group_by, aggregates)?),
            op: *op,
            array: Box::new(rewrite_aggregate_expr(array, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Setval {
            sequence,
            value,
            is_called,
            data_type,
            nullable,
        } => Ok(BoundExpr::Setval {
            sequence: *sequence,
            value: Box::new(rewrite_aggregate_expr(value, group_by, aggregates)?),
            is_called: is_called
                .as_deref()
                .map(|expr| rewrite_aggregate_expr(expr, group_by, aggregates).map(Box::new))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::IsNull {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::IsNotNull {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::InList {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            list: list
                .iter()
                .map(|item| rewrite_aggregate_expr(item, group_by, aggregates))
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::RuntimeInSet {
            expr,
            set,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::RuntimeInSet {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            set: *set,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::Between {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            low: Box::new(rewrite_aggregate_expr(low, group_by, aggregates)?),
            high: Box::new(rewrite_aggregate_expr(high, group_by, aggregates)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape,
            data_type,
            nullable,
        } => Ok(BoundExpr::Like {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            pattern: Box::new(rewrite_aggregate_expr(pattern, group_by, aggregates)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape: *escape,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable,
        } => Ok(BoundExpr::Case {
            operand: operand
                .as_deref()
                .map(|expr| rewrite_aggregate_expr(expr, group_by, aggregates).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| {
                    Ok((
                        rewrite_aggregate_expr(when, group_by, aggregates)?,
                        rewrite_aggregate_expr(then, group_by, aggregates)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_deref()
                .map(|expr| rewrite_aggregate_expr(expr, group_by, aggregates).map(Box::new))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => Ok(BoundExpr::Cast {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        }),
        // A subquery body is its own scope and needs no grouped-expression
        // rewrite, but its correlation entries are THIS query's expressions —
        // evaluated against the post-aggregate row — and are rewritten like
        // any other outer expression. `InSubquery`'s left operand too.
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::InSubquery {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            query: Box::new(rewrite_query_correlations(query, group_by, aggregates)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::ScalarSubquery {
            query,
            data_type,
            nullable,
        } => Ok(BoundExpr::ScalarSubquery {
            query: Box::new(rewrite_query_correlations(query, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Exists {
            query,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::Exists {
            query: Box::new(rewrite_query_correlations(query, group_by, aggregates)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. } => Ok(expr.clone()),
        BoundExpr::AggregateCall { .. } => Err(DbError::internal(
            "nested aggregate survived binder validation",
        )),
    }
}

/// Mark the join spine of a joined DML source so combined rows carry the
/// TARGET table's physical row identity: the target is the leftmost input,
/// and every inner/cross join on the left spine forwards its left side's
/// identity (`docs/specs/subqueries.md` §8.1). Filters pass rows through
/// whole; the walk stops at the target scan.
fn mark_dml_spine_identity(plan: &mut LogicalPlan) {
    match plan {
        LogicalPlan::Filter { source, .. } | LogicalPlan::Projection { source, .. } => {
            mark_dml_spine_identity(source)
        }
        // A LATERAL FROM item lowers to an Apply on the spine; ApplyOp passes
        // its input rows' identity through, so the walk continues below it.
        LogicalPlan::Apply { input, .. } => mark_dml_spine_identity(input),
        LogicalPlan::Join {
            left,
            join_type: JoinType::Inner | JoinType::Cross,
            identity_from,
            ..
        } => {
            *identity_from = Some(JoinSide::Left);
            mark_dml_spine_identity(left);
        }
        _ => {}
    }
}

/// Rewrite a correlated subquery's correlation entries for an aggregate
/// query: each `outer` expression is evaluated against the post-aggregate row
/// and must be mapped to its `LocalRef` slot like any grouped expression. The
/// body itself is untouched. Binder validation already rejected ungrouped
/// correlation entries, so the rewrite always finds a mapping when one is
/// required.
fn rewrite_query_correlations(
    query: &BoundQuery,
    group_by: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> Result<BoundQuery> {
    if query.correlations.is_empty() {
        return Ok(query.clone());
    }
    let mut query = query.clone();
    for correlation in &mut query.correlations {
        correlation.outer = rewrite_aggregate_expr(&correlation.outer, group_by, aggregates)?;
    }
    Ok(query)
}

pub(crate) fn contains_aggregate(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::AggregateCall { .. } => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. }
        | BoundExpr::RuntimeInSet { expr, .. } => contains_aggregate(expr),
        BoundExpr::Function { args, .. } => args.iter().any(contains_aggregate),
        BoundExpr::Array { elements, .. } => elements.iter().any(contains_aggregate),
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => contains_aggregate(array) || subscripts.iter().any(contains_aggregate),
        BoundExpr::Any { left, array, .. } => contains_aggregate(left) || contains_aggregate(array),
        BoundExpr::Setval {
            value, is_called, ..
        } => contains_aggregate(value) || is_called.as_deref().is_some_and(contains_aggregate),
        BoundExpr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        BoundExpr::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || when_clauses
                    .iter()
                    .any(|(when, then)| contains_aggregate(when) || contains_aggregate(then))
                || else_clause.as_deref().is_some_and(contains_aggregate)
        }
        BoundExpr::InSubquery { expr, .. } => contains_aggregate(expr),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => false,
    }
}
