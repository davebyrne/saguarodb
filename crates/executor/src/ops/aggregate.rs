use std::collections::{BTreeMap, BTreeSet};

use common::{ColumnInfo, DbError, ExecRow, Result, Row, SqlState, Value};
use planner::{AggregateExpr, AggregateFunc, BoundExpr};

use crate::eval_expr;
use crate::expr::integer_overflow;
use crate::query::{PlanExecutor, collect_all};

pub struct AggregateOp<'a> {
    source: Box<dyn PlanExecutor + 'a>,
    group_by: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    output_schema: Vec<ColumnInfo>,
    rows: Vec<ExecRow>,
    index: usize,
}

impl<'a> AggregateOp<'a> {
    pub fn new(
        source: Box<dyn PlanExecutor + 'a>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            source,
            group_by,
            aggregates,
            output_schema,
            rows: Vec::new(),
            index: 0,
        }
    }
}

impl PlanExecutor for AggregateOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.rows.clear();
        self.index = 0;
        let input = collect_all(self.source.as_mut())?;
        let groups = build_groups(&self.group_by, input)?;
        for (group_key, rows) in groups {
            let mut values = group_key;
            for aggregate in &self.aggregates {
                values.push(evaluate_aggregate(aggregate, &rows)?);
            }
            self.rows.push(ExecRow {
                row: Row { values },
                identity: None,
            });
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

fn build_groups(
    group_by: &[BoundExpr],
    input: Vec<ExecRow>,
) -> Result<Vec<(Vec<Value>, Vec<ExecRow>)>> {
    if group_by.is_empty() {
        return Ok(vec![(Vec::new(), input)]);
    }

    let mut groups: BTreeMap<Vec<Value>, Vec<ExecRow>> = BTreeMap::new();
    for row in input {
        let key = group_by
            .iter()
            .map(|expr| eval_expr(expr, &row))
            .collect::<Result<Vec<_>>>()?;
        groups.entry(key).or_default().push(row);
    }
    Ok(groups.into_iter().collect())
}

fn evaluate_aggregate(aggregate: &AggregateExpr, rows: &[ExecRow]) -> Result<Value> {
    match aggregate.func {
        AggregateFunc::Count => count_aggregate(aggregate, rows),
        AggregateFunc::Sum => integer_fold_aggregate(aggregate, rows, IntegerAggregate::Sum),
        AggregateFunc::Avg => integer_fold_aggregate(aggregate, rows, IntegerAggregate::Avg),
        AggregateFunc::Min => min_max_aggregate(aggregate, rows, true),
        AggregateFunc::Max => min_max_aggregate(aggregate, rows, false),
    }
}

fn count_aggregate(aggregate: &AggregateExpr, rows: &[ExecRow]) -> Result<Value> {
    if aggregate.arg.is_none() {
        return Ok(Value::Integer(rows.len() as i64));
    }

    let values = aggregate_values(aggregate, rows)?;
    Ok(Value::Integer(
        values
            .into_iter()
            .filter(|value| !matches!(value, Value::Null))
            .count() as i64,
    ))
}

#[derive(Clone, Copy)]
enum IntegerAggregate {
    Sum,
    Avg,
}

fn integer_fold_aggregate(
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    kind: IntegerAggregate,
) -> Result<Value> {
    let values = aggregate_values(aggregate, rows)?;
    let mut sum = 0_i64;
    let mut count = 0_i64;
    for value in values {
        match value {
            Value::Null => {}
            Value::Integer(value) => {
                sum = sum.checked_add(value).ok_or_else(integer_overflow)?;
                count = count.checked_add(1).ok_or_else(integer_overflow)?;
            }
            _ => {
                return Err(DbError::execute(
                    SqlState::DatatypeMismatch,
                    "SUM and AVG require integer input",
                ));
            }
        }
    }

    if count == 0 {
        return Ok(Value::Null);
    }

    match kind {
        IntegerAggregate::Sum => Ok(Value::Integer(sum)),
        IntegerAggregate::Avg => Ok(Value::Integer(sum / count)),
    }
}

fn min_max_aggregate(aggregate: &AggregateExpr, rows: &[ExecRow], min: bool) -> Result<Value> {
    let values = aggregate_values(aggregate, rows)?;
    let mut best: Option<Value> = None;
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        match &best {
            Some(current) if (min && value >= *current) || (!min && value <= *current) => {}
            _ => best = Some(value),
        }
    }
    Ok(best.unwrap_or(Value::Null))
}

fn aggregate_values(aggregate: &AggregateExpr, rows: &[ExecRow]) -> Result<Vec<Value>> {
    let Some(arg) = &aggregate.arg else {
        return Ok(Vec::new());
    };

    let mut values = Vec::with_capacity(rows.len());
    let mut distinct = BTreeSet::new();
    for row in rows {
        let value = eval_expr(arg, row)?;
        if aggregate.distinct && !distinct.insert(value.clone()) {
            continue;
        }
        values.push(value);
    }
    Ok(values)
}
