use std::cmp::Ordering;

use common::{ColumnInfo, ExecRow, Result, StatementContext, Value};
use planner::BoundOrderByItem;
use spill::{ExternalSorter, SortedStream, SpillConfig};

use crate::eval_expr;
use crate::ops::spill_row::SpillRow;
use crate::query::{PlanExecutor, close_after, open_executor};

pub struct SortOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    order_by: Vec<BoundOrderByItem>,
    output_schema: Vec<ColumnInfo>,
    spill: SpillConfig,
    stream: Option<SortedStream<SpillRow>>,
}

impl<'a> SortOp<'a> {
    pub fn new(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        order_by: Vec<BoundOrderByItem>,
        spill: SpillConfig,
    ) -> Self {
        let output_schema = source.output_schema().to_vec();
        Self {
            ctx,
            source,
            order_by,
            output_schema,
            spill,
            stream: None,
        }
    }
}

impl PlanExecutor for SortOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.stream = None;
        let order_by = self.order_by.clone();
        let mut sorter = ExternalSorter::new(
            self.spill.for_operator(self.ctx.cancel.clone()),
            move |left: &SpillRow, right: &SpillRow| {
                compare_keys(&left.keys, &right.keys, &order_by)
            },
        );
        open_executor(self.source.as_mut())?;
        let result = (|| {
            let mut ordinal = 0u64;
            while let Some(row) = self.source.next()? {
                self.ctx.cancel.check()?;
                let keys = self
                    .order_by
                    .iter()
                    .map(|item| eval_expr(&self.ctx, &item.expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                sorter.push(SpillRow {
                    row,
                    keys,
                    ordinal,
                    source: 0,
                })?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| common::DbError::internal("sort input ordinal overflow"))?;
            }
            sorter.finish()
        })();
        let stream = close_after(self.source.as_mut(), result)?;
        self.stream = Some(stream);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        self.stream
            .as_mut()
            .ok_or_else(|| common::DbError::internal("sort is not open"))?
            .next_record()
            .map(|row| row.map(|row| row.row))
    }

    fn close(&mut self) -> Result<()> {
        self.stream = None;
        Ok(())
    }
}

pub(crate) fn compare_keys(
    left: &[Value],
    right: &[Value],
    order_by: &[BoundOrderByItem],
) -> Ordering {
    for ((left, right), item) in left.iter().zip(right).zip(order_by) {
        let ordering = compare_key_value(left, right, item);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(crate) fn compare_key_value(left: &Value, right: &Value, item: &BoundOrderByItem) -> Ordering {
    let nulls_first = item.nulls_first.unwrap_or(!item.ascending);
    match (matches!(left, Value::Null), matches!(right, Value::Null)) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {}
    }

    let ordering = left.cmp(right);
    if item.ascending {
        ordering
    } else {
        ordering.reverse()
    }
}
