use std::collections::HashMap;

use common::{ColumnInfo, DbError, ExecRow, Result, Row, StatementContext, Value};
use planner::{BoundExpr, JoinSide, JoinType};

use crate::ops::predicate_matches;
use crate::query::{PlanExecutor, collect_all_cancelable};

pub struct NestedLoopJoinOp<'a> {
    ctx: StatementContext,
    left: Box<dyn PlanExecutor + 'a>,
    right: Box<dyn PlanExecutor + 'a>,
    condition: Option<BoundExpr>,
    join_type: JoinType,
    /// `Some(Left)` on a DML-source spine: combined rows carry the left
    /// side's row identity (`docs/specs/subqueries.md` §8.1). The identity
    /// side is never null-padded — DML spines are inner/cross joins.
    identity_from: Option<JoinSide>,
    output_schema: Vec<ColumnInfo>,
    rows: Vec<ExecRow>,
    index: usize,
    left_width: usize,
    right_width: usize,
}

impl<'a> NestedLoopJoinOp<'a> {
    pub fn new(
        ctx: StatementContext,
        left: Box<dyn PlanExecutor + 'a>,
        right: Box<dyn PlanExecutor + 'a>,
        condition: Option<BoundExpr>,
        join_type: JoinType,
        identity_from: Option<JoinSide>,
    ) -> Self {
        let left_width = left.output_schema().len();
        let right_width = right.output_schema().len();
        let mut output_schema = left.output_schema().to_vec();
        if !join_type.is_semi_or_anti() {
            output_schema.extend_from_slice(right.output_schema());
        }
        Self {
            ctx,
            left,
            right,
            condition,
            join_type,
            identity_from,
            output_schema,
            rows: Vec::new(),
            index: 0,
            left_width,
            right_width,
        }
    }
}

impl PlanExecutor for NestedLoopJoinOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.rows.clear();
        self.index = 0;

        let left_rows = collect_all_cancelable(self.left.as_mut(), self.ctx.cancel.as_ref())?;
        let right_rows = collect_all_cancelable(self.right.as_mut(), self.ctx.cancel.as_ref())?;

        // Semi/anti joins emit the left ExecRow itself (row identity intact,
        // no right columns) at most once per left row.
        if self.join_type.is_semi_or_anti() {
            for left in left_rows {
                self.ctx.cancel.check()?;
                let mut matched = false;
                for (right_index, right) in right_rows.iter().enumerate() {
                    if right_index % 256 == 0 {
                        self.ctx.cancel.check()?;
                    }
                    let joined = join_row_refs(&left, right);
                    if join_condition_matches(&self.ctx, &self.condition, &joined)? {
                        matched = true;
                        break;
                    }
                }
                if matched == (self.join_type == JoinType::Semi) {
                    self.rows.push(left);
                }
            }
            return Ok(());
        }

        let mut matched_right = vec![false; right_rows.len()];
        for left in &left_rows {
            self.ctx.cancel.check()?;
            let mut matched_left = false;
            for (right_index, right) in right_rows.iter().enumerate() {
                if right_index % 256 == 0 {
                    self.ctx.cancel.check()?;
                }
                let mut joined = join_row_refs(left, right);
                if self.join_type == JoinType::Cross
                    || join_condition_matches(&self.ctx, &self.condition, &joined)?
                {
                    matched_left = true;
                    matched_right[right_index] = true;
                    if self.identity_from == Some(JoinSide::Left) {
                        joined.identity = left.identity.clone();
                    }
                    self.rows.push(joined);
                }
            }

            if !matched_left && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                self.rows.push(join_with_null_right(left, self.right_width));
            }
        }

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            for (right, matched) in right_rows.iter().zip(matched_right) {
                self.ctx.cancel.check()?;
                if !matched {
                    self.rows.push(join_with_null_left(self.left_width, right));
                }
            }
        }

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

pub fn join_rows(left: ExecRow, right: ExecRow) -> ExecRow {
    let mut values = left.row.values;
    values.extend(right.row.values);
    ExecRow {
        row: Row { values },
        identity: None,
    }
}

fn join_condition_matches(
    ctx: &StatementContext,
    condition: &Option<BoundExpr>,
    row: &ExecRow,
) -> Result<bool> {
    match condition {
        Some(condition) => predicate_matches(ctx, condition, row),
        None => Ok(true),
    }
}

fn join_row_refs(left: &ExecRow, right: &ExecRow) -> ExecRow {
    let mut values = left.row.values.clone();
    values.extend(right.row.values.clone());
    ExecRow {
        row: Row { values },
        identity: None,
    }
}

fn join_with_null_right(left: &ExecRow, right_width: usize) -> ExecRow {
    let mut values = left.row.values.clone();
    values.extend(std::iter::repeat_n(Value::Null, right_width));
    ExecRow {
        row: Row { values },
        identity: None,
    }
}

fn join_with_null_left(left_width: usize, right: &ExecRow) -> ExecRow {
    let mut values = vec![Value::Null; left_width];
    values.extend(right.row.values.clone());
    ExecRow {
        row: Row { values },
        identity: None,
    }
}

/// Inner equi-join. Builds an in-memory hash table over ONE input (the right
/// by default; the left when the planner chose `build_left` — the smaller
/// estimated side, `docs/specs/statistics.md` §9.2) and STREAMS the other,
/// probing one row at a time, so only the build side and the current probe
/// row's matches are resident. Output column order is left ++ right either
/// way. `left_keys`/`right_keys` are paired column slots into the left and
/// right child rows.
pub struct HashJoinOp<'a> {
    ctx: StatementContext,
    left: Box<dyn PlanExecutor + 'a>,
    right: Box<dyn PlanExecutor + 'a>,
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    /// `Inner`, `Semi`, or `Anti`. Outer joins never take the hash path.
    join_type: JoinType,
    /// `Some(Left)` on a DML-source spine (`docs/specs/subqueries.md` §8.1).
    identity_from: Option<JoinSide>,
    /// Build over the left input, stream the right. The planner sets this
    /// only for plain inner joins; semi/anti always build right and stream
    /// (probe with) left.
    build_left: bool,
    output_schema: Vec<ColumnInfo>,
    table: HashMap<Vec<Value>, Vec<ExecRow>>,
    /// Remaining joined rows for the current probe row, next-first.
    pending: Vec<ExecRow>,
}

impl<'a> HashJoinOp<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: StatementContext,
        left: Box<dyn PlanExecutor + 'a>,
        right: Box<dyn PlanExecutor + 'a>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: JoinType,
        identity_from: Option<JoinSide>,
        build_left: bool,
    ) -> Self {
        // The planner sets build_left only for plain inner joins; a semi/anti
        // probe would otherwise read right rows with left key slots.
        debug_assert!(
            !build_left || join_type == JoinType::Inner,
            "build_left is only valid for inner hash joins"
        );
        let mut output_schema = left.output_schema().to_vec();
        if !join_type.is_semi_or_anti() {
            output_schema.extend_from_slice(right.output_schema());
        }
        Self {
            ctx,
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            identity_from,
            build_left,
            output_schema,
            table: HashMap::new(),
            pending: Vec::new(),
        }
    }

    fn probe_input(&mut self) -> &mut (dyn PlanExecutor + 'a) {
        if self.build_left {
            self.right.as_mut()
        } else {
            self.left.as_mut()
        }
    }
}

impl PlanExecutor for HashJoinOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.table.clear();
        self.pending.clear();

        let (build_input, build_keys) = if self.build_left {
            (self.left.as_mut(), &self.left_keys)
        } else {
            (self.right.as_mut(), &self.right_keys)
        };
        let build_rows = collect_all_cancelable(build_input, self.ctx.cancel.as_ref())?;
        for row in build_rows {
            self.ctx.cancel.check()?;
            if let Some(key) = join_key(&row.row.values, build_keys)? {
                self.table.entry(key).or_default().push(row);
            }
        }

        self.probe_input().open()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        loop {
            if let Some(row) = self.pending.pop() {
                return Ok(Some(row));
            }
            self.ctx.cancel.check()?;
            let Some(probe) = self.probe_input().next()? else {
                return Ok(None);
            };

            if self.join_type.is_semi_or_anti() {
                // Semi/anti probe with the left input (the planner never sets
                // build_left for them) and emit the left ExecRow itself
                // (identity intact) at most once. A NULL in a left key never
                // equals anything, so it is a non-match: dropped for semi,
                // emitted for anti — exactly the [NOT] EXISTS equality
                // semantics decorrelation relies on.
                let matched = match join_key(&probe.row.values, &self.left_keys)? {
                    Some(key) => self.table.contains_key(&key),
                    None => false,
                };
                if matched == (self.join_type == JoinType::Semi) {
                    return Ok(Some(probe));
                }
                continue;
            }

            let probe_keys = if self.build_left {
                &self.right_keys
            } else {
                &self.left_keys
            };
            let Some(key) = join_key(&probe.row.values, probe_keys)? else {
                continue;
            };
            if let Some(matches) = self.table.get(&key) {
                // Emit in build order: `pending` pops from the back.
                for build_row in matches.iter().rev() {
                    self.ctx.cancel.check()?;
                    let (left, right) = if self.build_left {
                        (build_row, &probe)
                    } else {
                        (&probe, build_row)
                    };
                    let mut joined = join_row_refs(left, right);
                    if self.identity_from == Some(JoinSide::Left) {
                        joined.identity = left.identity.clone();
                    }
                    self.pending.push(joined);
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.table.clear();
        self.pending.clear();
        self.probe_input().close()
    }
}

/// Collects the key values at `key_slots`. Returns `None` when any key column is
/// NULL, since SQL equality never matches NULL, so such rows cannot join.
fn join_key(values: &[Value], key_slots: &[usize]) -> Result<Option<Vec<Value>>> {
    let mut key = Vec::with_capacity(key_slots.len());
    for &slot in key_slots {
        let value = values
            .get(slot)
            .ok_or_else(|| DbError::internal(format!("join key slot {slot} is out of bounds")))?;
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        key.push(value.clone());
    }
    Ok(Some(key))
}
