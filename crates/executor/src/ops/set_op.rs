use std::cmp::Ordering;

use common::{ColumnInfo, ExecRow, Result, StatementContext, Value};
use planner::SetOp;
use spill::{ExternalSorter, SortedStream, SpillConfig};

use crate::ops::spill_row::SpillRow;
use crate::query::{PlanExecutor, close_after, open_executor};

/// Work-memory-bounded UNION/INTERSECT/EXCEPT execution. The first external
/// sort makes equal rows adjacent, with right rows before left rows so counts
/// are known before left occurrences are visited. The second sort restores the
/// selected rows to the engine's established input order.
pub struct SetOpOp<'a> {
    ctx: StatementContext,
    op: SetOp,
    all: bool,
    left: Box<dyn PlanExecutor + 'a>,
    right: Box<dyn PlanExecutor + 'a>,
    output_schema: Vec<ColumnInfo>,
    spill: SpillConfig,
    stream: Option<SortedStream<SpillRow>>,
}

impl<'a> SetOpOp<'a> {
    pub fn new(
        ctx: StatementContext,
        op: SetOp,
        all: bool,
        left: Box<dyn PlanExecutor + 'a>,
        right: Box<dyn PlanExecutor + 'a>,
        spill: SpillConfig,
    ) -> Self {
        let output_schema = left.output_schema().to_vec();
        Self {
            ctx,
            op,
            all,
            left,
            right,
            output_schema,
            spill,
            stream: None,
        }
    }
}

impl PlanExecutor for SetOpOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.stream = None;
        let spill_ctx = self.spill.for_operator(self.ctx.cancel.clone());
        let mut by_key =
            ExternalSorter::new(spill_ctx.clone(), |left: &SpillRow, right: &SpillRow| {
                left.keys
                    .cmp(&right.keys)
                    .then_with(|| right.source.cmp(&left.source))
            });
        let mut ordinal = 0u64;
        drain_arm(self.left.as_mut(), 0, &mut ordinal, &mut by_key, &self.ctx)?;
        drain_arm(self.right.as_mut(), 1, &mut ordinal, &mut by_key, &self.ctx)?;

        let mut input = by_key.finish()?;
        let mut output = ExternalSorter::new(spill_ctx, |left: &SpillRow, right: &SpillRow| {
            left.ordinal.cmp(&right.ordinal)
        });
        let mut current_key: Option<Vec<Value>> = None;
        let mut right_remaining = 0usize;
        let mut emitted = false;
        let mut union_first: Option<SpillRow> = None;
        while let Some(mut record) = input.next_record()? {
            self.ctx.cancel.check()?;
            if current_key.as_ref() != Some(&record.keys) {
                if let Some(record) = union_first.take() {
                    output.push(record)?;
                }
                current_key = Some(record.keys.clone());
                right_remaining = 0;
                emitted = false;
            }
            record.row.identity = None;
            match (self.op, self.all, record.source) {
                (SetOp::Union, true, _) => output.push(record)?,
                (SetOp::Union, false, _)
                    if union_first
                        .as_ref()
                        .is_none_or(|first| record.ordinal < first.ordinal) =>
                {
                    union_first = Some(record);
                }
                (SetOp::Intersect | SetOp::Except, _, 1) => {
                    right_remaining = right_remaining.checked_add(1).ok_or_else(|| {
                        common::DbError::internal("set operation occurrence count overflow")
                    })?;
                }
                (SetOp::Intersect, true, 0) if right_remaining > 0 => {
                    right_remaining -= 1;
                    output.push(record)?;
                }
                (SetOp::Intersect, false, 0) if right_remaining > 0 && !emitted => {
                    emitted = true;
                    output.push(record)?;
                }
                (SetOp::Except, true, 0) if right_remaining > 0 => right_remaining -= 1,
                (SetOp::Except, true, 0) => output.push(record)?,
                (SetOp::Except, false, 0) if right_remaining == 0 && !emitted => {
                    emitted = true;
                    output.push(record)?;
                }
                _ => {}
            }
        }
        if let Some(record) = union_first {
            output.push(record)?;
        }
        self.stream = Some(output.finish()?);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        self.stream
            .as_mut()
            .ok_or_else(|| common::DbError::internal("set operation is not open"))?
            .next_record()
            .map(|row| row.map(|row| row.row))
    }

    fn close(&mut self) -> Result<()> {
        self.stream = None;
        Ok(())
    }
}

fn drain_arm<C>(
    arm: &mut dyn PlanExecutor,
    source: u8,
    ordinal: &mut u64,
    sorter: &mut ExternalSorter<SpillRow, C>,
    ctx: &StatementContext,
) -> Result<()>
where
    C: Fn(&SpillRow, &SpillRow) -> Ordering,
{
    open_executor(arm)?;
    let result = (|| {
        while let Some(row) = arm.next()? {
            ctx.cancel.check()?;
            sorter.push(SpillRow {
                keys: row.row.values.clone(),
                row,
                ordinal: *ordinal,
                source,
            })?;
            *ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| common::DbError::internal("set operation input ordinal overflow"))?;
        }
        Ok(())
    })();
    close_after(arm, result)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common::{CancelReason, DbError, Key, QueryCancel, Row, RowId, RowIdentity, SqlState};

    use super::*;

    struct RowsOp {
        rows: VecDeque<ExecRow>,
        closes: Arc<AtomicUsize>,
        cancel_on_eof: Option<Arc<QueryCancel>>,
        fail_next: bool,
        fail_close: bool,
    }

    impl PlanExecutor for RowsOp {
        fn output_schema(&self) -> &[ColumnInfo] {
            &[]
        }

        fn open(&mut self) -> Result<()> {
            Ok(())
        }

        fn next(&mut self) -> Result<Option<ExecRow>> {
            if self.fail_next {
                self.fail_next = false;
                return Err(DbError::internal("test next failure"));
            }
            let row = self.rows.pop_front();
            if row.is_none()
                && let Some(cancel) = self.cancel_on_eof.take()
            {
                cancel.request(CancelReason::StatementTimeout);
            }
            Ok(row)
        }

        fn close(&mut self) -> Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                Err(DbError::internal("test close failure"))
            } else {
                Ok(())
            }
        }
    }

    fn input(values: Vec<Value>, closes: Arc<AtomicUsize>) -> Box<dyn PlanExecutor> {
        Box::new(RowsOp {
            rows: values
                .into_iter()
                .enumerate()
                .map(|(ordinal, value)| ExecRow {
                    row: Row {
                        values: vec![value],
                    },
                    identity: Some(RowIdentity {
                        row_id: RowId {
                            page_num: 1,
                            slot_num: ordinal as u16,
                        },
                        xmin: 1,
                        key: Key(vec![Value::Integer(ordinal as i64)]),
                    }),
                })
                .collect(),
            closes,
            cancel_on_eof: None,
            fail_next: false,
            fail_close: false,
        })
    }

    fn value(label: &str) -> Value {
        Value::Text(format!("{label}{}", "x".repeat(1_000)))
    }

    fn execute(op: SetOp, all: bool) -> (Vec<Value>, usize, usize, u64) {
        let left_closes = Arc::new(AtomicUsize::new(0));
        let right_closes = Arc::new(AtomicUsize::new(0));
        let cancel = Arc::new(QueryCancel::new());
        let mut ctx = StatementContext::new(0);
        ctx.cancel = cancel;
        let spill = SpillConfig::new(spill::MIN_WORK_MEM_BYTES, std::env::temp_dir());
        let stats = spill.stats.clone();
        let mut op = SetOpOp::new(
            ctx,
            op,
            all,
            input(
                vec![value("one"), value("one"), value("two"), Value::Null],
                left_closes.clone(),
            ),
            input(
                vec![
                    value("one"),
                    value("two"),
                    value("two"),
                    Value::Null,
                    value("three"),
                ],
                right_closes.clone(),
            ),
            spill,
        );
        op.open().unwrap();
        let mut values = Vec::new();
        while let Some(row) = op.next().unwrap() {
            assert!(row.identity.is_none());
            values.push(row.row.values.into_iter().next().unwrap());
        }
        op.close().unwrap();
        (
            values,
            left_closes.load(Ordering::SeqCst),
            right_closes.load(Ordering::SeqCst),
            stats.files_created(),
        )
    }

    #[test]
    fn external_set_operations_preserve_set_multiset_null_and_order_semantics() {
        assert_eq!(
            execute(SetOp::Union, true).0,
            vec![
                value("one"),
                value("one"),
                value("two"),
                Value::Null,
                value("one"),
                value("two"),
                value("two"),
                Value::Null,
                value("three"),
            ]
        );
        assert_eq!(
            execute(SetOp::Union, false).0,
            vec![value("one"), value("two"), Value::Null, value("three")]
        );
        assert_eq!(
            execute(SetOp::Intersect, true).0,
            vec![value("one"), value("two"), Value::Null]
        );
        assert_eq!(
            execute(SetOp::Intersect, false).0,
            vec![value("one"), value("two"), Value::Null]
        );
        assert_eq!(execute(SetOp::Except, true).0, vec![value("one")]);
        assert!(execute(SetOp::Except, false).0.is_empty());
        for (op, all) in [
            (SetOp::Union, true),
            (SetOp::Union, false),
            (SetOp::Intersect, true),
            (SetOp::Intersect, false),
            (SetOp::Except, true),
            (SetOp::Except, false),
        ] {
            assert!(execute(op, all).3 > 0, "{op:?} all={all} did not spill");
        }
        let (_, left_closes, right_closes, _) = execute(SetOp::Union, false);
        assert_eq!((left_closes, right_closes), (1, 1));
    }

    #[test]
    fn set_operation_closes_children_on_next_close_and_cancellation_errors() {
        let cancel = Arc::new(QueryCancel::new());
        let left_closes = Arc::new(AtomicUsize::new(0));
        let right_closes = Arc::new(AtomicUsize::new(0));
        let left = RowsOp {
            rows: VecDeque::new(),
            closes: left_closes.clone(),
            cancel_on_eof: Some(cancel.clone()),
            fail_next: false,
            fail_close: false,
        };
        let right = RowsOp {
            rows: VecDeque::from([ExecRow {
                row: Row {
                    values: vec![Value::Integer(1)],
                },
                identity: None,
            }]),
            closes: right_closes.clone(),
            cancel_on_eof: None,
            fail_next: false,
            fail_close: false,
        };
        let mut ctx = StatementContext::new(0);
        ctx.cancel = cancel;
        let mut op = SetOpOp::new(
            ctx,
            SetOp::Union,
            false,
            Box::new(left),
            Box::new(right),
            SpillConfig::default(),
        );
        assert_eq!(op.open().unwrap_err().code, SqlState::QueryCanceled);
        assert_eq!(
            (
                left_closes.load(Ordering::SeqCst),
                right_closes.load(Ordering::SeqCst)
            ),
            (1, 1)
        );

        for (fail_next, fail_close) in [(true, false), (false, true)] {
            let closes = Arc::new(AtomicUsize::new(0));
            let failing = RowsOp {
                rows: VecDeque::new(),
                closes: closes.clone(),
                cancel_on_eof: None,
                fail_next,
                fail_close,
            };
            let mut op = SetOpOp::new(
                StatementContext::new(0),
                SetOp::Union,
                false,
                Box::new(failing),
                input(Vec::new(), Arc::new(AtomicUsize::new(0))),
                SpillConfig::default(),
            );
            assert!(op.open().is_err());
            assert_eq!(closes.load(Ordering::SeqCst), 1);
        }
    }
}
