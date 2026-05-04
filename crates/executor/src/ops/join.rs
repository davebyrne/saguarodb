use common::{ColumnInfo, ExecRow, Result, Row, Value};
use planner::{BoundExpr, JoinType};

use crate::ops::predicate_matches;
use crate::query::{PlanExecutor, collect_all};

pub struct NestedLoopJoinOp<'a> {
    left: Box<dyn PlanExecutor + 'a>,
    right: Box<dyn PlanExecutor + 'a>,
    condition: Option<BoundExpr>,
    join_type: JoinType,
    output_schema: Vec<ColumnInfo>,
    rows: Vec<ExecRow>,
    index: usize,
    left_width: usize,
    right_width: usize,
}

impl<'a> NestedLoopJoinOp<'a> {
    pub fn new(
        left: Box<dyn PlanExecutor + 'a>,
        right: Box<dyn PlanExecutor + 'a>,
        condition: Option<BoundExpr>,
        join_type: JoinType,
    ) -> Self {
        let left_width = left.output_schema().len();
        let right_width = right.output_schema().len();
        let mut output_schema = left.output_schema().to_vec();
        output_schema.extend_from_slice(right.output_schema());
        Self {
            left,
            right,
            condition,
            join_type,
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

        let mut matched_right = vec![false; right_rows.len()];
        for left in &left_rows {
            let mut matched_left = false;
            for (right_index, right) in right_rows.iter().enumerate() {
                let joined = join_row_refs(left, right);
                if self.join_type == JoinType::Cross
                    || join_condition_matches(&self.condition, &joined)?
                {
                    matched_left = true;
                    matched_right[right_index] = true;
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

fn join_condition_matches(condition: &Option<BoundExpr>, row: &ExecRow) -> Result<bool> {
    match condition {
        Some(condition) => predicate_matches(condition, row),
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
