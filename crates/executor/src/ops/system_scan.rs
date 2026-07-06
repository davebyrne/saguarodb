use common::{ColumnInfo, ExecRow, Result, Row, StatementContext};
use planner::BoundExpr;

use crate::ops::predicate_matches;
use crate::query::PlanExecutor;

pub struct SystemScanOp {
    ctx: StatementContext,
    rows: Vec<Row>,
    output_schema: Vec<ColumnInfo>,
    filter: Option<BoundExpr>,
    index: usize,
    opened: bool,
}

impl SystemScanOp {
    pub fn new(
        ctx: StatementContext,
        rows: Vec<Row>,
        output_schema: Vec<ColumnInfo>,
        filter: Option<BoundExpr>,
    ) -> Self {
        Self {
            ctx,
            rows,
            output_schema,
            filter,
            index: 0,
            opened: false,
        }
    }
}

impl PlanExecutor for SystemScanOp {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.index = 0;
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        if !self.opened {
            return Err(common::DbError::internal("SystemScanOp was not opened"));
        }
        while let Some(row) = self.rows.get(self.index) {
            self.index += 1;
            let row = ExecRow {
                row: row.clone(),
                identity: None,
            };
            if self
                .filter
                .as_ref()
                .map(|filter| predicate_matches(&self.ctx, filter, &row))
                .transpose()?
                .unwrap_or(true)
            {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.opened = false;
        Ok(())
    }
}
