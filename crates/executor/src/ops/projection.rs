use common::{ColumnInfo, ExecRow, Result, Row};
use planner::BoundExpr;

use crate::eval_expr;
use crate::query::PlanExecutor;

pub struct ProjectionOp<'a> {
    source: Box<dyn PlanExecutor + 'a>,
    expressions: Vec<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
}

impl<'a> ProjectionOp<'a> {
    pub fn new(
        source: Box<dyn PlanExecutor + 'a>,
        expressions: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            source,
            expressions,
            output_schema,
        }
    }
}

impl PlanExecutor for ProjectionOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.source.open()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        self.source
            .next()?
            .map(|row| project_row(row, &self.expressions))
            .transpose()
    }

    fn close(&mut self) -> Result<()> {
        self.source.close()
    }
}

pub fn project_row(input: ExecRow, expressions: &[BoundExpr]) -> Result<ExecRow> {
    let values = expressions
        .iter()
        .map(|expr| eval_expr(expr, &input))
        .collect::<Result<Vec<_>>>()?;
    Ok(ExecRow {
        row: Row { values },
        identity: input.identity,
    })
}
