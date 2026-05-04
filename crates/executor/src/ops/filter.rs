use common::{ColumnInfo, ExecRow, Result};
use planner::BoundExpr;

use crate::ops::predicate_matches;
use crate::query::PlanExecutor;

pub struct FilterOp<'a> {
    source: Box<dyn PlanExecutor + 'a>,
    predicate: BoundExpr,
    output_schema: Vec<ColumnInfo>,
}

impl<'a> FilterOp<'a> {
    pub fn new(source: Box<dyn PlanExecutor + 'a>, predicate: BoundExpr) -> Self {
        let output_schema = source.output_schema().to_vec();
        Self {
            source,
            predicate,
            output_schema,
        }
    }
}

impl PlanExecutor for FilterOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.source.open()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        while let Some(row) = self.source.next()? {
            if predicate_matches(&self.predicate, &row)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.source.close()
    }
}
