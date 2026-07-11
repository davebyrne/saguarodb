use std::collections::BTreeSet;

use common::{ColumnInfo, ExecRow, Result, Row, StatementContext, Value};
use planner::BoundExpr;

use crate::eval_expr;
use crate::query::PlanExecutor;

/// Streaming de-duplication for `SELECT DISTINCT`. Emits the first row of each
/// distinct `on_keys` tuple in input order and drops the rest. Because the
/// input is already sorted when an `ORDER BY` is present, keeping the first
/// occurrence preserves the requested ordering. NULL keys collapse together
/// (two NULLs are not distinct from each other), matching SQL `DISTINCT`.
pub struct DistinctOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    on_keys: Vec<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    seen: BTreeSet<Vec<Value>>,
}

impl<'a> DistinctOp<'a> {
    pub fn new(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        on_keys: Vec<BoundExpr>,
    ) -> Self {
        let output_schema = source.output_schema().to_vec();
        Self {
            ctx,
            source,
            on_keys,
            output_schema,
            seen: BTreeSet::new(),
        }
    }
}

impl PlanExecutor for DistinctOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.seen.clear();
        self.source.open()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        while let Some(row) = self.source.next()? {
            self.ctx.cancel.check()?;
            let key = self
                .on_keys
                .iter()
                .map(|expr| eval_expr(&self.ctx, expr, &row))
                .collect::<Result<Vec<_>>>()?;
            if self.seen.insert(key) {
                // De-duplication collapses several source rows into one, so the
                // surviving row no longer maps to a single heap tuple; clear its
                // identity like `AggregateOp` does.
                return Ok(Some(ExecRow {
                    row: Row {
                        values: row.row.values,
                    },
                    identity: None,
                }));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.seen.clear();
        self.source.close()
    }
}
