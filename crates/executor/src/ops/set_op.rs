use std::collections::BTreeSet;

use common::{ColumnInfo, ExecRow, Result, Row, Value};
use planner::SetOp;

use crate::query::{PlanExecutor, collect_all};

/// Executes a set operation (`UNION`/`INTERSECT`/`EXCEPT`) over two sub-plans.
///
/// Both arms are materialized on `open` and the result is computed up front, then
/// drained by `next`. Materialization is required because de-duplication and
/// membership tests need to see whole inputs; it matches how the engine already
/// materializes query results. Row equality is structural over the full row with
/// `NULL == NULL` (a `BTreeSet<Vec<Value>>`, as `DistinctOp` uses), matching SQL
/// set semantics. Output rows carry no heap identity (a set operation's rows do
/// not map to single source tuples), like `DistinctOp`/`AggregateOp`.
///
/// `all` (only `UNION ALL`; `INTERSECT ALL`/`EXCEPT ALL` are rejected by the
/// binder) keeps duplicates; otherwise the result is de-duplicated. Both arms
/// produce identically-typed rows (reconciled by the binder), so the output schema
/// is the left arm's.
pub struct SetOpOp<'a> {
    op: SetOp,
    all: bool,
    left: Box<dyn PlanExecutor + 'a>,
    right: Box<dyn PlanExecutor + 'a>,
    output_schema: Vec<ColumnInfo>,
    result: Vec<Row>,
    index: usize,
}

impl<'a> SetOpOp<'a> {
    pub fn new(
        op: SetOp,
        all: bool,
        left: Box<dyn PlanExecutor + 'a>,
        right: Box<dyn PlanExecutor + 'a>,
    ) -> Self {
        let output_schema = left.output_schema().to_vec();
        Self {
            op,
            all,
            left,
            right,
            output_schema,
            result: Vec::new(),
            index: 0,
        }
    }
}

impl PlanExecutor for SetOpOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        // `collect_all` opens, drains, and closes each child.
        let left = collect_all(self.left.as_mut())?;
        let right = collect_all(self.right.as_mut())?;
        self.result = combine(self.op, self.all, left, right);
        self.index = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let Some(row) = self.result.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        Ok(Some(ExecRow {
            row: row.clone(),
            identity: None,
        }))
    }

    fn close(&mut self) -> Result<()> {
        self.result = Vec::new();
        self.index = 0;
        Ok(())
    }
}

/// Combine the materialized arms per the operator. `UNION ALL` concatenates;
/// `UNION` concatenates and de-duplicates; `INTERSECT`/`EXCEPT` return the distinct
/// left rows that are (respectively, are not) present in the right arm.
fn combine(op: SetOp, all: bool, left: Vec<ExecRow>, right: Vec<ExecRow>) -> Vec<Row> {
    match op {
        SetOp::Union if all => left
            .into_iter()
            .chain(right)
            .map(|exec_row| exec_row.row)
            .collect(),
        SetOp::Union => {
            let mut seen = BTreeSet::new();
            left.into_iter()
                .chain(right)
                .filter_map(|exec_row| keep_first(&mut seen, exec_row.row))
                .collect()
        }
        SetOp::Intersect => {
            let right_rows: BTreeSet<Vec<Value>> = right
                .into_iter()
                .map(|exec_row| exec_row.row.values)
                .collect();
            let mut emitted = BTreeSet::new();
            left.into_iter()
                .filter(|exec_row| right_rows.contains(&exec_row.row.values))
                .filter_map(|exec_row| keep_first(&mut emitted, exec_row.row))
                .collect()
        }
        SetOp::Except => {
            let right_rows: BTreeSet<Vec<Value>> = right
                .into_iter()
                .map(|exec_row| exec_row.row.values)
                .collect();
            let mut emitted = BTreeSet::new();
            left.into_iter()
                .filter(|exec_row| !right_rows.contains(&exec_row.row.values))
                .filter_map(|exec_row| keep_first(&mut emitted, exec_row.row))
                .collect()
        }
    }
}

/// Return `row` only the first time its values are seen (recording them in `seen`).
fn keep_first(seen: &mut BTreeSet<Vec<Value>>, row: Row) -> Option<Row> {
    seen.insert(row.values.clone()).then_some(row)
}
