mod aggregate;
mod distinct;
mod dml;
mod filter;
mod join;
mod limit;
mod projection;
mod scan;
mod sort;
mod values;

pub use aggregate::AggregateOp;
pub use distinct::DistinctOp;
pub use filter::FilterOp;
pub use join::HashJoinOp;
pub use join::NestedLoopJoinOp;
pub use join::join_rows;
pub use limit::LimitOp;
pub use projection::ProjectionOp;
pub use projection::project_row;
pub use scan::{IndexScanOp, SeqScanOp};
pub use sort::SortOp;
pub use values::ValuesOp;

use common::{Result, StatementContext, Value};
use planner::BoundExpr;

use crate::eval_expr_with_context;

pub(crate) fn predicate_matches(
    ctx: &StatementContext,
    expr: &BoundExpr,
    row: &common::ExecRow,
) -> Result<bool> {
    Ok(matches!(
        eval_expr_with_context(ctx, expr, row)?,
        Value::Boolean(true)
    ))
}
