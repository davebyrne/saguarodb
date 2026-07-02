use std::collections::{BTreeMap, BTreeSet};

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
/// `all` selects multiset semantics (`UNION ALL` keeps duplicates; `INTERSECT ALL`
/// and `EXCEPT ALL` use per-row occurrence counts); otherwise the result is
/// de-duplicated. Both arms produce identically-typed rows (reconciled by the
/// binder), so the output schema is the left arm's.
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

/// Combine the materialized arms per the operator. The `ALL` (multiset) forms use
/// per-row occurrence counts; the plain (distinct) forms de-duplicate:
/// - `UNION ALL` concatenates; `UNION` concatenates and de-duplicates.
/// - `INTERSECT ALL` emits `min(count_left, count_right)` copies of each row (in
///   left order); `INTERSECT` emits the distinct left rows present in the right.
/// - `EXCEPT ALL` emits `max(0, count_left - count_right)` copies of each row (in
///   left order); `EXCEPT` emits the distinct left rows absent from the right.
///
/// All forms use structural whole-row equality with `NULL == NULL`.
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
        // INTERSECT ALL: emit a left row while the right arm still has an unmatched
        // copy of it, consuming one right occurrence per emitted row (so the count
        // emitted is min(left, right)).
        SetOp::Intersect if all => {
            let mut remaining = occurrence_counts(right);
            left.into_iter()
                .filter_map(|exec_row| {
                    consume_one(&mut remaining, &exec_row.row.values).then_some(exec_row.row)
                })
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
        // EXCEPT ALL: drop a left row while the right arm still has an unmatched
        // copy of it (cancelling one right occurrence); emit the surplus (so the
        // count emitted is max(0, left - right)).
        SetOp::Except if all => {
            let mut remaining = occurrence_counts(right);
            left.into_iter()
                .filter_map(|exec_row| {
                    (!consume_one(&mut remaining, &exec_row.row.values)).then_some(exec_row.row)
                })
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

/// Count how many times each distinct row occurs.
fn occurrence_counts(rows: Vec<ExecRow>) -> BTreeMap<Vec<Value>, usize> {
    let mut counts = BTreeMap::new();
    for exec_row in rows {
        *counts.entry(exec_row.row.values).or_insert(0) += 1;
    }
    counts
}

/// Consume one occurrence of `values` from `counts` if present; returns whether a
/// count was consumed. `INTERSECT ALL` emits on `true`, `EXCEPT ALL` on `false`.
fn consume_one(counts: &mut BTreeMap<Vec<Value>, usize>, values: &[Value]) -> bool {
    match counts.get_mut(values) {
        Some(count) if *count > 0 => {
            *count -= 1;
            true
        }
        _ => false,
    }
}
