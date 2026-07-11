use common::{ColumnInfo, ExecRow, Result, StatementContext};

use crate::query::PlanExecutor;

pub struct LimitOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    count: u64,
    offset: u64,
    emitted: u64,
    output_schema: Vec<ColumnInfo>,
}

impl<'a> LimitOp<'a> {
    pub fn new(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        count: u64,
        offset: u64,
    ) -> Self {
        let output_schema = source.output_schema().to_vec();
        Self {
            ctx,
            source,
            count,
            offset,
            emitted: 0,
            output_schema,
        }
    }
}

impl PlanExecutor for LimitOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.emitted = 0;
        self.source.open()?;
        for _ in 0..self.offset {
            self.ctx.cancel.check()?;
            if self.source.next()?.is_none() {
                break;
            }
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        self.ctx.cancel.check()?;
        if self.emitted >= self.count {
            return Ok(None);
        }
        let row = self.source.next()?;
        if row.is_some() {
            self.emitted += 1;
        }
        Ok(row)
    }

    fn close(&mut self) -> Result<()> {
        self.source.close()
    }
}
