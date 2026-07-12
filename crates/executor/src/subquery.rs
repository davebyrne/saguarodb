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

use std::fmt;
use std::sync::{Arc, Mutex};

use common::{DataType, DbError, Result, Row, RuntimeValueSet, SqlState, Value};
use planner::{BoundExpr, BoundQuery, BoundStatement, PhysicalPlan, logical_plan, physical_plan};
use spill::{SpillTape, SpillTapeReader};

use planner::rewrite_plan_exprs;

use crate::query::{ExecutionContext, build_executor};

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
            let exists = run_exists_subquery(ctx, query)?;
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
            subquery_column_type(query)?;
            let mut executor = build_subquery_executor(ctx, query)?;
            crate::query::open_executor(executor.as_mut())?;
            let spill_ctx = ctx.spill.for_operator(ctx.statement.cancel.clone());
            let mut tape = SpillTape::new(spill_ctx);
            let result = (|| {
                while let Some(row) = executor.next()? {
                    tape.push(single_value(row.row)?)?;
                }
                tape.finish()?;
                Ok(tape)
            })();
            let tape = crate::query::close_after(executor.as_mut(), result)?;
            let set = ctx
                .statement
                .runtime_value_sets
                .register(Arc::new(SpillValueSet {
                    tape: Mutex::new(tape),
                }))?;
            Ok(Some(BoundExpr::RuntimeInSet {
                expr: operand.clone(),
                set,
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
    let mut executor = build_subquery_executor(ctx, query)?;
    crate::query::open_executor(executor.as_mut())?;
    let result = (|| {
        let first = executor.next()?;
        if executor.next()?.is_some() {
            return Err(DbError::execute(
                SqlState::CardinalityViolation,
                "more than one row returned by a subquery used as an expression",
            ));
        }
        first.map_or(Ok(Value::Null), |row| single_value(row.row))
    })();
    crate::query::close_after(executor.as_mut(), result)
}

fn run_exists_subquery(ctx: &ExecutionContext<'_>, query: &BoundQuery) -> Result<bool> {
    let mut executor = build_subquery_executor(ctx, query)?;
    crate::query::open_executor(executor.as_mut())?;
    let result = executor.next().map(|row| row.is_some());
    crate::query::close_after(executor.as_mut(), result)
}

fn build_subquery_executor<'a>(
    ctx: &'a ExecutionContext<'a>,
    query: &BoundQuery,
) -> Result<Box<dyn crate::query::PlanExecutor + 'a>> {
    let statement = BoundStatement::Query(query.clone());
    let logical = logical_plan(&statement)?;
    let physical = physical_plan(&logical, ctx.catalog.as_ref())?;
    let resolved = resolve_plan_subqueries(ctx, &physical)?;
    build_executor(ctx, &resolved)
}

struct SpillValueSet {
    tape: Mutex<SpillTape<Value>>,
}

impl fmt::Debug for SpillValueSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillValueSet").finish_non_exhaustive()
    }
}

impl RuntimeValueSet for SpillValueSet {
    fn evaluate(&self, operand: &Value, negated: bool) -> Result<Value> {
        let mut tape = self
            .tape
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut reader: SpillTapeReader<Value> = tape.reader()?;
        let mut saw_null = matches!(operand, Value::Null);
        while let Some(value) = reader.next_record()? {
            if matches!(value, Value::Null) {
                saw_null = true;
            } else if !matches!(operand, Value::Null) && value == *operand {
                return Ok(Value::Boolean(!negated));
            }
        }
        if saw_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Boolean(negated))
        }
    }
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
