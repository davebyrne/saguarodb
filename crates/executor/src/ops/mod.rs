mod aggregate;
mod apply;
mod distinct;
mod dml;
mod filter;
mod join;
mod limit;
mod projection;
mod scan;
mod set_op;
mod sort;
mod system_scan;
mod values;

pub use aggregate::AggregateOp;
pub use apply::ApplyOp;
pub use distinct::DistinctOp;
pub use filter::FilterOp;
pub use join::HashJoinOp;
pub use join::NestedLoopJoinOp;
pub use join::join_rows;
pub use limit::LimitOp;
pub use projection::ProjectionOp;
pub use projection::project_row;
pub(crate) use scan::IndexScanInput;
pub use scan::{IndexScanOp, SeqScanOp};
pub use set_op::SetOpOp;
pub use sort::SortOp;
pub use system_scan::SystemScanOp;
pub use values::ValuesOp;

use common::{Result, StatementContext, Value};
use planner::BoundExpr;

use crate::eval_expr;

pub(crate) fn predicate_matches(
    ctx: &StatementContext,
    expr: &BoundExpr,
    row: &common::ExecRow,
) -> Result<bool> {
    Ok(matches!(eval_expr(ctx, expr, row)?, Value::Boolean(true)))
}
