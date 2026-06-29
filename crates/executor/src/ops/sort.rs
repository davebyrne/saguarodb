use std::cmp::Ordering;

use common::{ColumnInfo, ExecRow, Result, StatementContext, Value};
use planner::BoundOrderByItem;

use crate::eval_expr_with_context;
use crate::query::{PlanExecutor, collect_all};

pub struct SortOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    order_by: Vec<BoundOrderByItem>,
    output_schema: Vec<ColumnInfo>,
    rows: Vec<ExecRow>,
    index: usize,
}

impl<'a> SortOp<'a> {
    pub fn new(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        order_by: Vec<BoundOrderByItem>,
    ) -> Self {
        let output_schema = source.output_schema().to_vec();
        Self {
            ctx,
            source,
            order_by,
            output_schema,
            rows: Vec::new(),
            index: 0,
        }
    }
}

impl PlanExecutor for SortOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.index = 0;
        self.rows.clear();
        let mut keyed = Vec::new();
        for row in collect_all(self.source.as_mut())? {
            let keys = self
                .order_by
                .iter()
                .map(|item| eval_expr_with_context(&self.ctx, &item.expr, &row))
                .collect::<Result<Vec<_>>>()?;
            keyed.push((row, keys));
        }
        keyed.sort_by(|left, right| compare_keys(&left.1, &right.1, &self.order_by));
        self.rows = keyed.into_iter().map(|(row, _)| row).collect();
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let Some(row) = self.rows.get(self.index).cloned() else {
            return Ok(None);
        };
        self.index += 1;
        Ok(Some(row))
    }

    fn close(&mut self) -> Result<()> {
        self.rows.clear();
        self.index = 0;
        Ok(())
    }
}

fn compare_keys(left: &[Value], right: &[Value], order_by: &[BoundOrderByItem]) -> Ordering {
    for ((left, right), item) in left.iter().zip(right).zip(order_by) {
        let ordering = compare_key_value(left, right, item);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_key_value(left: &Value, right: &Value, item: &BoundOrderByItem) -> Ordering {
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
