use std::cmp::Ordering;

use common::{ColumnInfo, ExecRow, QueryCancel, Result, StatementContext, Value};
use planner::BoundOrderByItem;

use crate::eval_expr;
use crate::query::{PlanExecutor, collect_all_cancelable};

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
        for row in collect_all_cancelable(self.source.as_mut(), self.ctx.cancel.as_ref())? {
            self.ctx.cancel.check()?;
            let keys = self
                .order_by
                .iter()
                .map(|item| eval_expr(&self.ctx, &item.expr, &row))
                .collect::<Result<Vec<_>>>()?;
            keyed.push((row, keys));
        }
        self.ctx.cancel.check()?;
        sort_cancelable(&mut keyed, self.ctx.cancel.as_ref(), |left, right| {
            compare_keys(&left.1, &right.1, &self.order_by)
        })?;
        self.ctx.cancel.check()?;
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

/// Stable in-memory sort with bounded cancellation latency. Small runs use the
/// standard stable sort with one fixed comparator; a bottom-up merge checks the
/// token between runs and periodically while combining them.
fn sort_cancelable<T, F>(values: &mut Vec<T>, cancel: &QueryCancel, compare: F) -> Result<()>
where
    F: Fn(&T, &T) -> Ordering,
{
    const RUN_LEN: usize = 256;
    const CANCEL_CHECK_INTERVAL: usize = 256;

    for run in values.chunks_mut(RUN_LEN) {
        cancel.check()?;
        run.sort_by(&compare);
    }

    let len = values.len();
    let mut width = RUN_LEN;
    while width < len {
        cancel.check()?;
        let mut source: Vec<Option<T>> = std::mem::take(values).into_iter().map(Some).collect();
        let mut merged = Vec::with_capacity(len);
        let mut moved = 0usize;
        let mut start = 0usize;
        while start < len {
            cancel.check()?;
            let middle = start.saturating_add(width).min(len);
            let end = middle.saturating_add(width).min(len);
            let (mut left, mut right) = (start, middle);
            while left < middle || right < end {
                if moved.is_multiple_of(CANCEL_CHECK_INTERVAL) {
                    cancel.check()?;
                }
                let take_left = right >= end
                    || (left < middle
                        && compare(
                            source[left].as_ref().expect("unmoved left sort item"),
                            source[right].as_ref().expect("unmoved right sort item"),
                        ) != Ordering::Greater);
                let index = if take_left {
                    let index = left;
                    left += 1;
                    index
                } else {
                    let index = right;
                    right += 1;
                    index
                };
                merged.push(source[index].take().expect("sort item moved once"));
                moved += 1;
            }
            start = end;
        }
        *values = merged;
        width = width.saturating_mul(2);
    }
    cancel.check()
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use common::{CancelReason, QueryCancel, SqlState};

    use super::sort_cancelable;

    #[test]
    fn cancelable_sort_preserves_stable_order() {
        let cancel = QueryCancel::new();
        let mut values: Vec<_> = (0..1_025)
            .map(|ordinal| (ordinal * 37 % 11, ordinal))
            .collect();
        let mut expected = values.clone();
        expected.sort_by_key(|value| value.0);

        sort_cancelable(&mut values, &cancel, |left, right| left.0.cmp(&right.0)).unwrap();

        assert_eq!(values, expected);
    }

    #[test]
    fn cancelable_sort_observes_cancellation_during_merge() {
        let cancel = QueryCancel::new();
        let merge_started = Cell::new(false);
        let mut values: Vec<_> = (0..512).step_by(2).chain((1..512).step_by(2)).collect();

        let err = sort_cancelable(&mut values, &cancel, |left, right| {
            // Each initial 256-item run contains only one parity. Comparing an
            // even value to an odd value can therefore happen only in the merge.
            if left % 2 != right % 2 {
                merge_started.set(true);
                cancel.request(CancelReason::StatementTimeout);
            }
            left.cmp(right)
        })
        .unwrap_err();

        assert_eq!(err.code, SqlState::QueryCanceled);
        assert!(merge_started.get());
    }
}
