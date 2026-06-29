//! Subquery resolution.
//!
//! Uncorrelated subqueries are resolved to constants before the main plan runs:
//! a one-time pre-pass over the physical plan executes each subquery's sub-plan
//! and rewrites the expression in place.
//!
//! - A scalar subquery `(SELECT ...)` becomes a literal (`NULL` when empty; more
//!   than one row is a `CardinalityViolation`).
//! - `[NOT] EXISTS (...)` becomes a boolean literal.
//! - `expr [NOT] IN (...)` becomes an `InList` of literals, so the existing
//!   three-valued-logic `IN` evaluation applies unchanged.
//!
//! Because the subqueries are uncorrelated, executing them once under the
//! statement's snapshot is sufficient; correlation is not yet supported.

use common::{DataType, DbError, Result, Row, SqlState, Value};
use planner::{
    AggregateExpr, BoundExpr, BoundOrderByItem, BoundSelect, BoundStatement, PhysicalPlan,
    logical_plan, physical_plan,
};

use crate::query::{ExecutionContext, build_executor, collect_all};

/// Rewrite every subquery expression in `plan` to a constant by executing it.
/// Recurses into child plans so nested subqueries are handled bottom-up.
pub(crate) fn resolve_plan_subqueries(
    ctx: &ExecutionContext<'_>,
    plan: &PhysicalPlan,
) -> Result<PhysicalPlan> {
    Ok(match plan {
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::DropIndex { .. }
        | PhysicalPlan::CreateSequence { .. }
        | PhysicalPlan::DropSequence { .. } => plan.clone(),
        PhysicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
        } => PhysicalPlan::Insert {
            table: *table,
            columns: columns.clone(),
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            on_conflict: on_conflict.clone(),
            returning: returning.clone(),
        },
        PhysicalPlan::Update {
            table,
            assignments,
            source,
            returning,
        } => PhysicalPlan::Update {
            table: *table,
            assignments: assignments
                .iter()
                .map(|(column, expr)| Ok((*column, resolve_expr(ctx, expr)?)))
                .collect::<Result<Vec<_>>>()?,
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            returning: returning.clone(),
        },
        PhysicalPlan::Delete {
            table,
            source,
            returning,
        } => PhysicalPlan::Delete {
            table: *table,
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            returning: returning.clone(),
        },
        PhysicalPlan::SeqScan {
            table,
            table_name,
            filter,
        } => PhysicalPlan::SeqScan {
            table: *table,
            table_name: table_name.clone(),
            filter: resolve_opt(ctx, filter)?,
        },
        PhysicalPlan::IndexScan {
            table,
            table_name,
            index,
            range,
            filter,
        } => PhysicalPlan::IndexScan {
            table: *table,
            table_name: table_name.clone(),
            index: *index,
            range: range.clone(),
            filter: resolve_opt(ctx, filter)?,
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(resolve_plan_subqueries(ctx, left)?),
            right: Box::new(resolve_plan_subqueries(ctx, right)?),
            condition: resolve_opt(ctx, condition)?,
            join_type: *join_type,
        },
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            right_keys,
        } => PhysicalPlan::HashJoin {
            left: Box::new(resolve_plan_subqueries(ctx, left)?),
            right: Box::new(resolve_plan_subqueries(ctx, right)?),
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
        },
        PhysicalPlan::Filter { source, predicate } => PhysicalPlan::Filter {
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            predicate: resolve_expr(ctx, predicate)?,
        },
        PhysicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => PhysicalPlan::Projection {
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            expressions: resolve_vec(ctx, expressions)?,
            output_schema: output_schema.clone(),
        },
        PhysicalPlan::Sort { source, order_by } => PhysicalPlan::Sort {
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            order_by: order_by
                .iter()
                .map(|item| {
                    Ok(BoundOrderByItem {
                        expr: resolve_expr(ctx, &item.expr)?,
                        ascending: item.ascending,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        },
        PhysicalPlan::Distinct { source, on_keys } => PhysicalPlan::Distinct {
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            on_keys: resolve_vec(ctx, on_keys)?,
        },
        PhysicalPlan::Limit {
            source,
            count,
            offset,
        } => PhysicalPlan::Limit {
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            count: *count,
            offset: *offset,
        },
        PhysicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => PhysicalPlan::Aggregate {
            source: Box::new(resolve_plan_subqueries(ctx, source)?),
            group_by: resolve_vec(ctx, group_by)?,
            aggregates: aggregates
                .iter()
                .map(|aggregate| {
                    Ok(AggregateExpr {
                        func: aggregate.func,
                        arg: aggregate
                            .arg
                            .as_ref()
                            .map(|arg| resolve_expr(ctx, arg))
                            .transpose()?,
                        distinct: aggregate.distinct,
                        data_type: aggregate.data_type.clone(),
                        nullable: aggregate.nullable,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            output_schema: output_schema.clone(),
        },
        PhysicalPlan::Values {
            rows,
            output_schema,
        } => PhysicalPlan::Values {
            rows: rows
                .iter()
                .map(|row| resolve_vec(ctx, row))
                .collect::<Result<Vec<_>>>()?,
            output_schema: output_schema.clone(),
        },
    })
}

fn resolve_opt(ctx: &ExecutionContext<'_>, expr: &Option<BoundExpr>) -> Result<Option<BoundExpr>> {
    expr.as_ref()
        .map(|expr| resolve_expr(ctx, expr))
        .transpose()
}

fn resolve_vec(ctx: &ExecutionContext<'_>, exprs: &[BoundExpr]) -> Result<Vec<BoundExpr>> {
    exprs.iter().map(|expr| resolve_expr(ctx, expr)).collect()
}

fn resolve_boxed(ctx: &ExecutionContext<'_>, expr: &BoundExpr) -> Result<Box<BoundExpr>> {
    Ok(Box::new(resolve_expr(ctx, expr)?))
}

/// Rewrite the subquery expressions inside `expr`, leaving everything else
/// structurally identical.
fn resolve_expr(ctx: &ExecutionContext<'_>, expr: &BoundExpr) -> Result<BoundExpr> {
    match expr {
        BoundExpr::ScalarSubquery {
            select,
            data_type,
            nullable,
        } => Ok(BoundExpr::Literal {
            value: run_scalar_subquery(ctx, select)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Exists {
            select,
            negated,
            data_type,
            nullable,
        } => {
            let exists = !materialize_subquery(ctx, select)?.is_empty();
            Ok(BoundExpr::Literal {
                value: Value::Boolean(exists ^ *negated),
                data_type: data_type.clone(),
                nullable: *nullable,
            })
        }
        BoundExpr::InSubquery {
            expr: operand,
            select,
            negated,
            data_type,
            nullable,
        } => {
            let operand = resolve_boxed(ctx, operand)?;
            let column_type = subquery_column_type(select)?;
            let rows = materialize_subquery(ctx, select)?;
            let list = rows
                .into_iter()
                .map(|row| {
                    Ok(BoundExpr::Literal {
                        value: single_value(row)?,
                        data_type: column_type.clone(),
                        nullable: true,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(BoundExpr::InList {
                expr: operand,
                list,
                negated: *negated,
                data_type: data_type.clone(),
                nullable: *nullable,
            })
        }
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. } => Ok(expr.clone()),
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            nullable,
        } => Ok(BoundExpr::BinaryOp {
            left: resolve_boxed(ctx, left)?,
            op: *op,
            right: resolve_boxed(ctx, right)?,
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
            expr: resolve_boxed(ctx, expr)?,
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
            args: resolve_vec(ctx, args)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::AggregateCall {
            func,
            arg,
            distinct,
            data_type,
            nullable,
        } => Ok(BoundExpr::AggregateCall {
            func: *func,
            arg: arg
                .as_ref()
                .map(|arg| resolve_boxed(ctx, arg))
                .transpose()?,
            distinct: *distinct,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::IsNull {
            expr: resolve_boxed(ctx, expr)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::IsNotNull {
            expr: resolve_boxed(ctx, expr)?,
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
            expr: resolve_boxed(ctx, expr)?,
            list: resolve_vec(ctx, list)?,
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
            expr: resolve_boxed(ctx, expr)?,
            low: resolve_boxed(ctx, low)?,
            high: resolve_boxed(ctx, high)?,
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
            expr: resolve_boxed(ctx, expr)?,
            pattern: resolve_boxed(ctx, pattern)?,
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
                .as_ref()
                .map(|operand| resolve_boxed(ctx, operand))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| Ok((resolve_expr(ctx, when)?, resolve_expr(ctx, then)?)))
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_ref()
                .map(|else_clause| resolve_boxed(ctx, else_clause))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Cast {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::Cast {
            expr: resolve_boxed(ctx, expr)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
    }
}

/// Execute a scalar subquery: at most one row (else a `CardinalityViolation`),
/// returning its single column value, or `NULL` when the result is empty.
fn run_scalar_subquery(ctx: &ExecutionContext<'_>, select: &BoundSelect) -> Result<Value> {
    let mut rows = materialize_subquery(ctx, select)?;
    if rows.len() > 1 {
        return Err(DbError::execute(
            SqlState::CardinalityViolation,
            "more than one row returned by a subquery used as an expression",
        ));
    }
    match rows.pop() {
        Some(row) => single_value(row),
        None => Ok(Value::Null),
    }
}

/// Plan and run a subquery's bound SELECT, returning its materialized rows.
fn materialize_subquery(ctx: &ExecutionContext<'_>, select: &BoundSelect) -> Result<Vec<Row>> {
    let statement = BoundStatement::Select(select.clone());
    let logical = logical_plan(&statement)?;
    let physical = physical_plan(&logical, ctx.catalog)?;
    let resolved = resolve_plan_subqueries(ctx, &physical)?;
    let mut executor = build_executor(ctx, &resolved)?;
    let rows = collect_all(executor.as_mut())?;
    Ok(rows.into_iter().map(|row| row.row).collect())
}

/// The single column's type of a single-column subquery (validated by the
/// binder; re-checked here so a malformed plan fails loudly).
fn subquery_column_type(select: &BoundSelect) -> Result<DataType> {
    match select.output_schema.as_slice() {
        [column] => Ok(column.data_type.clone()),
        _ => Err(DbError::internal(
            "subquery used as a value did not have exactly one output column",
        )),
    }
}

/// Extract the single value from a one-column subquery row.
fn single_value(row: Row) -> Result<Value> {
    let mut values = row.values;
    if values.len() != 1 {
        return Err(DbError::internal(
            "subquery used as a value produced a row with the wrong number of columns",
        ));
    }
    Ok(values.pop().unwrap())
}
