use common::{ColumnInfo, ExecRow, Result, Row, StatementContext};
use planner::BoundExpr;

use crate::eval_expr_with_context;
use crate::query::PlanExecutor;

pub struct ValuesOp {
    ctx: StatementContext,
    rows: Vec<Vec<BoundExpr>>,
    output_schema: Vec<ColumnInfo>,
    index: usize,
}

impl ValuesOp {
    pub fn new(
        ctx: StatementContext,
        rows: Vec<Vec<BoundExpr>>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            ctx,
            rows,
            output_schema,
            index: 0,
        }
    }
}

impl PlanExecutor for ValuesOp {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.index = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let Some(expressions) = self.rows.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        let empty = ExecRow {
            row: Row { values: vec![] },
            identity: None,
        };
        let values = expressions
            .iter()
            .map(|expr| eval_expr_with_context(&self.ctx, expr, &empty))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(ExecRow {
            row: Row { values },
            identity: None,
        }))
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
