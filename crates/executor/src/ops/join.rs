use std::collections::HashMap;

use common::{ColumnInfo, DbError, ExecRow, Result, Row, StatementContext, Value};
use planner::{BoundExpr, JoinSide, JoinType};

use crate::ops::predicate_matches;
use crate::query::{PlanExecutor, collect_all};

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

        let left_rows = collect_all(self.left.as_mut())?;
        let right_rows = collect_all(self.right.as_mut())?;

        // Semi/anti joins emit the left ExecRow itself (row identity intact,
        // no right columns) at most once per left row.
        if self.join_type.is_semi_or_anti() {
            for left in left_rows {
                let mut matched = false;
                for right in &right_rows {
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
            let mut matched_left = false;
            for (right_index, right) in right_rows.iter().enumerate() {
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

/// Inner equi-join. Builds a probe table over the right input keyed by its join
/// columns, then probes it with each left row. `left_keys`/`right_keys` are
/// paired column slots into the left and right child rows.
pub struct HashJoinOp<'a> {
    left: Box<dyn PlanExecutor + 'a>,
    right: Box<dyn PlanExecutor + 'a>,
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    /// `Inner`, `Semi`, or `Anti`. Outer joins never take the hash path.
    join_type: JoinType,
    /// `Some(Left)` on a DML-source spine (`docs/specs/subqueries.md` §8.1).
    identity_from: Option<JoinSide>,
    output_schema: Vec<ColumnInfo>,
    rows: Vec<ExecRow>,
    index: usize,
}

impl<'a> HashJoinOp<'a> {
    pub fn new(
        left: Box<dyn PlanExecutor + 'a>,
        right: Box<dyn PlanExecutor + 'a>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: JoinType,
        identity_from: Option<JoinSide>,
    ) -> Self {
        let mut output_schema = left.output_schema().to_vec();
        if !join_type.is_semi_or_anti() {
            output_schema.extend_from_slice(right.output_schema());
        }
        Self {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            identity_from,
            output_schema,
            rows: Vec::new(),
            index: 0,
        }
    }
}

impl PlanExecutor for HashJoinOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.rows.clear();
        self.index = 0;

        let left_rows = collect_all(self.left.as_mut())?;
        let right_rows = collect_all(self.right.as_mut())?;

        let mut table: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
        for (right_index, right) in right_rows.iter().enumerate() {
            if let Some(key) = join_key(&right.row.values, &self.right_keys)? {
                table.entry(key).or_default().push(right_index);
            }
        }

        if self.join_type.is_semi_or_anti() {
            // Semi/anti probes emit the left ExecRow itself (identity intact)
            // at most once. A NULL in a left key never equals anything, so it
            // is a non-match: dropped for semi, emitted for anti — exactly
            // the [NOT] EXISTS equality semantics decorrelation relies on.
            for left in left_rows {
                let matched = match join_key(&left.row.values, &self.left_keys)? {
                    Some(key) => table.contains_key(&key),
                    None => false,
                };
                if matched == (self.join_type == JoinType::Semi) {
                    self.rows.push(left);
                }
            }
            return Ok(());
        }

        for left in &left_rows {
            let Some(key) = join_key(&left.row.values, &self.left_keys)? else {
                continue;
            };
            if let Some(matches) = table.get(&key) {
                for &right_index in matches {
                    let mut joined = join_row_refs(left, &right_rows[right_index]);
                    if self.identity_from == Some(JoinSide::Left) {
                        joined.identity = left.identity.clone();
                    }
                    self.rows.push(joined);
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
