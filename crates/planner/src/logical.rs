use catalog::SystemView;
use common::{
    ColumnId, ColumnInfo, CompressionSetting, DbError, IndexId, ParsedColumnDef, Result,
    SequenceOptions, TableId, ToastOptions,
};

use crate::{
    AggregateExpr, BoundDistinct, BoundExpr, BoundFrom, BoundInsertSource, BoundOnConflict,
    BoundOrderByItem, BoundQuery, BoundQueryBody, BoundReturning, BoundSelect, BoundStatement,
    JoinType, SetOp,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalPlan {
    CreateTable {
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        compression: CompressionSetting,
        toast: ToastOptions,
    },
    DropTable {
        table: TableId,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
        index: IndexId,
    },
    CreateSequence {
        name: String,
        options: SequenceOptions,
    },
    DropSequence {
        name: String,
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
    },
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: Box<LogicalPlan>,
        returning: Option<BoundReturning>,
    },
    Delete {
        table: TableId,
        source: Box<LogicalPlan>,
        returning: Option<BoundReturning>,
    },
    Scan {
        table: TableId,
        filter: Option<BoundExpr>,
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
        BoundStatement::CreateTable {
            name,
            columns,
            primary_key,
            unique,
            compression,
            toast,
        } => Ok(LogicalPlan::CreateTable {
            name: name.clone(),
            columns: columns.clone(),
            primary_key: primary_key.clone(),
            unique: unique.clone(),
            compression: *compression,
            toast: toast.clone(),
        }),
        BoundStatement::DropTable { table } => Ok(LogicalPlan::DropTable { table: *table }),
        BoundStatement::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => Ok(LogicalPlan::CreateIndex {
            name: name.clone(),
            table: table.clone(),
            columns: columns.clone(),
            unique: *unique,
        }),
        BoundStatement::DropIndex { index } => Ok(LogicalPlan::DropIndex { index: *index }),
        BoundStatement::CreateSequence { name, options } => Ok(LogicalPlan::CreateSequence {
            name: name.clone(),
            options: options.clone(),
        }),
        BoundStatement::DropSequence { name, if_exists } => Ok(LogicalPlan::DropSequence {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        BoundStatement::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            default_exprs,
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
            })
        }
        BoundStatement::Query(query) => plan_query(query),
        BoundStatement::Update {
            table,
            assignments,
            source,
            returning,
        } => Ok(LogicalPlan::Update {
            table: *table,
            assignments: assignments.clone(),
            source: Box::new(plan_select_source(source)?),
            returning: returning.clone(),
        }),
        BoundStatement::Delete {
            table,
            source,
            returning,
        } => Ok(LogicalPlan::Delete {
            table: *table,
            source: Box::new(plan_select_source(source)?),
            returning: returning.clone(),
        }),
        BoundStatement::Explain(_) => Err(DbError::plan(
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
fn plan_query(query: &BoundQuery) -> Result<LogicalPlan> {
    match &query.body {
        BoundQueryBody::Select(select) => {
            plan_select_body(select, &query.order_by, query.limit, query.offset)
        }
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
        plan = apply_distinct_and_projection(plan, select, distinct_keys, expressions);
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
    match from {
        BoundFrom::Table { table, .. } => Ok(LogicalPlan::Scan {
            table: *table,
            filter,
        }),
        BoundFrom::System { view, .. } => Ok(LogicalPlan::SystemScan {
            view: *view,
            filter,
        }),
        // A derived table lowers to its inner query's plan. Its columns already
        // sit at the derived binding's slots, so an outer WHERE (the standalone
        // case) is applied as a Filter above it — it cannot be pushed into the
        // inner scan.
        BoundFrom::Derived { query, .. } => {
            let plan = plan_query(query)?;
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
            let mut plan = LogicalPlan::Join {
                left: Box::new(plan_from(left, None)?),
                right: Box::new(plan_from(right, None)?),
                condition: condition.clone(),
                join_type: *join_type,
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
        | BoundExpr::Cast { expr, .. } => collect_aggregates(expr, output),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                collect_aggregates(arg, output);
            }
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
        // A subquery body is its own (uncorrelated) scope; only `InSubquery`'s
        // left operand belongs to the outer query and may carry an aggregate.
        BoundExpr::InSubquery { expr, .. } => collect_aggregates(expr, output),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
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
            nullable,
        } => Ok(BoundExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| rewrite_aggregate_expr(arg, group_by, aggregates))
                .collect::<Result<Vec<_>>>()?,
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
        // The subquery body is uncorrelated (its own scope), so it needs no
        // grouped-expression rewrite; only `InSubquery`'s left operand does.
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::InSubquery {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            query: query.clone(),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => Ok(expr.clone()),
        BoundExpr::AggregateCall { .. } => Err(DbError::internal(
            "nested aggregate survived binder validation",
        )),
    }
}

fn contains_aggregate(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::AggregateCall { .. } => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => contains_aggregate(expr),
        BoundExpr::Function { args, .. } => args.iter().any(contains_aggregate),
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
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => false,
    }
}
