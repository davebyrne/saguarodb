use common::{ColumnInfo, ExecRow, Result, Row, StatementContext};
use planner::BoundExpr;
use spill::{ExternalSorter, SortedStream, SpillConfig};

use crate::eval_expr;
use crate::ops::spill_row::SpillRow;
use crate::query::{PlanExecutor, close_after, open_executor};

/// Work-memory-bounded de-duplication for `SELECT DISTINCT`. A key sort makes
/// duplicates adjacent; a second ordinal sort restores the first occurrences
/// to input order. NULL keys collapse together, matching SQL `DISTINCT`.
pub struct DistinctOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    on_keys: Vec<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    spill: SpillConfig,
    stream: Option<SortedStream<SpillRow>>,
}

impl<'a> DistinctOp<'a> {
    pub fn new(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        on_keys: Vec<BoundExpr>,
        spill: SpillConfig,
    ) -> Self {
        let output_schema = source.output_schema().to_vec();
        Self {
            ctx,
            source,
            on_keys,
            output_schema,
            spill,
            stream: None,
        }
    }
}

impl PlanExecutor for DistinctOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.stream = None;
        let spill_ctx = self.spill.for_operator(self.ctx.cancel.clone());
        let mut by_key =
            ExternalSorter::new(spill_ctx.clone(), |left: &SpillRow, right: &SpillRow| {
                left.keys.cmp(&right.keys)
            });
        open_executor(self.source.as_mut())?;
        let result = (|| {
            let mut ordinal = 0u64;
            while let Some(row) = self.source.next()? {
                self.ctx.cancel.check()?;
                let keys = self
                    .on_keys
                    .iter()
                    .map(|expr| eval_expr(&self.ctx, expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                by_key.push(SpillRow {
                    row,
                    keys,
                    ordinal,
                    source: 0,
                })?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| common::DbError::internal("distinct input ordinal overflow"))?;
            }
            by_key.finish()
        })();
        let mut keyed = close_after(self.source.as_mut(), result)?;
        let mut by_ordinal = ExternalSorter::new(spill_ctx, |left: &SpillRow, right: &SpillRow| {
            left.ordinal.cmp(&right.ordinal)
        });
        let mut previous = None;
        while let Some(mut row) = keyed.next_record()? {
            if previous.as_ref() == Some(&row.keys) {
                continue;
            }
            previous = Some(row.keys.clone());
            row.row = ExecRow {
                row: Row {
                    values: row.row.row.values,
                },
                identity: None,
            };
            by_ordinal.push(row)?;
        }
        self.stream = Some(by_ordinal.finish()?);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        self.stream
            .as_mut()
            .ok_or_else(|| common::DbError::internal("distinct is not open"))?
            .next_record()
            .map(|row| row.map(|row| row.row))
    }

    fn close(&mut self) -> Result<()> {
        self.stream = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common::{DataType, DbError, Key, Row, RowId, RowIdentity, Value};
    use planner::BoundExpr;

    use super::*;

    struct RowsOp {
        rows: VecDeque<ExecRow>,
        closes: Arc<AtomicUsize>,
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
            Ok(self.rows.pop_front())
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

    fn child(closes: Arc<AtomicUsize>, fail_next: bool, fail_close: bool) -> RowsOp {
        RowsOp {
            rows: VecDeque::from([ExecRow {
                row: Row {
                    values: vec![Value::Integer(1)],
                },
                identity: None,
            }]),
            closes,
            fail_next,
            fail_close,
        }
    }

    #[test]
    fn distinct_closes_its_drained_child_exactly_once_in_success_and_error_paths() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut op = DistinctOp::new(
            StatementContext::new(0),
            Box::new(child(closes.clone(), false, false)),
            Vec::new(),
            SpillConfig::default(),
        );
        op.open().unwrap();
        op.close().unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);

        for (fail_next, fail_close) in [(true, false), (false, true)] {
            let closes = Arc::new(AtomicUsize::new(0));
            let mut op = DistinctOp::new(
                StatementContext::new(0),
                Box::new(child(closes.clone(), fail_next, fail_close)),
                Vec::new(),
                SpillConfig::default(),
            );
            assert!(op.open().is_err());
            assert_eq!(closes.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn distinct_forced_spill_preserves_first_null_and_duplicate_order_and_clears_identity() {
        let a = Value::Text(format!("a{}", "x".repeat(1_000)));
        let b = Value::Text(format!("b{}", "x".repeat(1_000)));
        let values = [
            a.clone(),
            Value::Null,
            b.clone(),
            a.clone(),
            Value::Null,
            b.clone(),
            a.clone(),
            b.clone(),
        ];
        let rows = values
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
            .collect();
        let closes = Arc::new(AtomicUsize::new(0));
        let source = RowsOp {
            rows,
            closes,
            fail_next: false,
            fail_close: false,
        };
        let spill = SpillConfig::new(spill::MIN_WORK_MEM_BYTES, std::env::temp_dir());
        let stats = spill.stats.clone();
        let mut op = DistinctOp::new(
            StatementContext::new(0),
            Box::new(source),
            vec![BoundExpr::InputRef {
                input: 0,
                column: 0,
                slot: 0,
                data_type: DataType::Text,
                nullable: true,
            }],
            spill,
        );

        op.open().unwrap();
        let mut output = Vec::new();
        while let Some(row) = op.next().unwrap() {
            assert!(row.identity.is_none());
            output.push(row.row.values.into_iter().next().unwrap());
        }

        assert_eq!(output, vec![a, Value::Null, b]);
        assert!(stats.files_created() > 0);
    }
}
