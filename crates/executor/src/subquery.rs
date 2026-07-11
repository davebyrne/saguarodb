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
//! Correlated subqueries in supported positions were hoisted into `Apply`
//! nodes by the planner and never appear here as expressions; one that does
//! appear sits in an unsupported position and is rejected
//! (`docs/specs/subqueries.md` §5).

use common::{DataType, DbError, Result, Row, SqlState, Value};
use planner::{BoundExpr, BoundQuery, BoundStatement, PhysicalPlan, logical_plan, physical_plan};

use planner::rewrite_plan_exprs;

use crate::query::{ExecutionContext, build_executor, collect_all};

/// Rewrite every subquery expression in `plan` to a constant by executing it,
/// via the shared structural rewriter (`docs/specs/subqueries.md` §5.3).
/// Recursion into child plans is the rewriter's; nested subqueries inside a
/// body are handled when the body is materialized (which re-enters this
/// pre-pass on the inner plan).
pub(crate) fn resolve_plan_subqueries(
    ctx: &ExecutionContext<'_>,
    plan: &PhysicalPlan,
) -> Result<PhysicalPlan> {
    rewrite_plan_exprs(plan, &mut |expr| resolve_subquery_expr(ctx, expr))
}

/// The pre-pass callback: resolve a subquery expression to its constant form,
/// leave every other node to the rewriter's structural walk. The rewriter
/// continues into a replacement's children, so `IN`'s left operand — carried
/// raw into the `InList` — still gets its own subqueries resolved.
fn resolve_subquery_expr(
    ctx: &ExecutionContext<'_>,
    expr: &BoundExpr,
) -> Result<Option<BoundExpr>> {
    match expr {
        BoundExpr::ScalarSubquery {
            query,
            data_type,
            nullable,
        } => {
            reject_correlated(query)?;
            Ok(Some(BoundExpr::Literal {
                value: run_scalar_subquery(ctx, query)?,
                data_type: data_type.clone(),
                nullable: *nullable,
            }))
        }
        BoundExpr::Exists {
            query,
            negated,
            data_type,
            nullable,
        } => {
            reject_correlated(query)?;
            let exists = !materialize_subquery(ctx, query)?.is_empty();
            Ok(Some(BoundExpr::Literal {
                value: Value::Boolean(exists ^ *negated),
                data_type: data_type.clone(),
                nullable: *nullable,
            }))
        }
        BoundExpr::InSubquery {
            expr: operand,
            query,
            negated,
            data_type,
            nullable,
        } => {
            reject_correlated(query)?;
            let column_type = subquery_column_type(query)?;
            let rows = materialize_subquery(ctx, query)?;
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
            Ok(Some(BoundExpr::InList {
                expr: operand.clone(),
                list,
                negated: *negated,
                data_type: data_type.clone(),
                nullable: *nullable,
            }))
        }
        // `OuterRef`s are left in place: inside an Apply template they are
        // the substitution points the Apply operator owns; a stray one
        // anywhere else fails loudly in expression evaluation.
        _ => Ok(None),
    }
}

/// The unsupported-position guard for correlated subqueries
/// (`docs/specs/subqueries.md` §5, §10): the hoisting pass lifted every
/// correlated subquery in a supported position into an `Apply` node, so a
/// correlated subquery expression reaching this pre-pass sits in a position
/// the planner does not hoist (join `ON`, `ORDER BY`, DML assignments,
/// RETURNING, ON CONFLICT, ...). It is rejected rather than resolved to a
/// wrong constant. The guard runs recursively — `materialize_subquery`
/// re-enters this pre-pass for the inner plan — so a correlated subquery
/// nested anywhere is caught at its boundary.
fn reject_correlated(query: &BoundQuery) -> Result<()> {
    if query.correlations.is_empty() {
        return Ok(());
    }
    Err(DbError::execute(
        SqlState::FeatureNotSupported,
        "correlated subqueries are not supported in this position",
    ))
}

/// Execute a scalar subquery: at most one row (else a `CardinalityViolation`),
/// returning its single column value, or `NULL` when the result is empty.
fn run_scalar_subquery(ctx: &ExecutionContext<'_>, query: &BoundQuery) -> Result<Value> {
    let mut rows = materialize_subquery(ctx, query)?;
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

/// Plan and run a subquery's bound query, returning its materialized rows.
fn materialize_subquery(ctx: &ExecutionContext<'_>, query: &BoundQuery) -> Result<Vec<Row>> {
    let statement = BoundStatement::Query(query.clone());
    let logical = logical_plan(&statement)?;
    let physical = physical_plan(&logical, ctx.catalog)?;
    let resolved = resolve_plan_subqueries(ctx, &physical)?;
    let mut executor = build_executor(ctx, &resolved)?;
    let rows = collect_all(executor.as_mut())?;
    Ok(rows.into_iter().map(|row| row.row).collect())
}

/// The single column's type of a single-column subquery (validated by the
/// binder; re-checked here so a malformed plan fails loudly).
fn subquery_column_type(query: &BoundQuery) -> Result<DataType> {
    match query.output_schema() {
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
