use std::collections::{BTreeMap, BTreeSet};

use common::{
    ColumnInfo, DataType, DbError, Decimal, ExecRow, Result, Row, SqlState, StatementContext, Value,
};
use planner::{AggregateExpr, AggregateFunc, BoundExpr};

use crate::eval_expr_with_context;
use crate::expr::integer_overflow;
use crate::query::{PlanExecutor, collect_all};

pub struct AggregateOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    group_by: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    output_schema: Vec<ColumnInfo>,
    rows: Vec<ExecRow>,
    index: usize,
}

impl<'a> AggregateOp<'a> {
    pub fn new(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            ctx,
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
        let groups = build_groups(&self.ctx, &self.group_by, input)?;
        for (group_key, rows) in groups {
            let mut values = group_key;
            for aggregate in &self.aggregates {
                values.push(evaluate_aggregate(&self.ctx, aggregate, &rows)?);
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
    ctx: &StatementContext,
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
            .map(|expr| eval_expr_with_context(ctx, expr, &row))
            .collect::<Result<Vec<_>>>()?;
        groups.entry(key).or_default().push(row);
    }
    Ok(groups.into_iter().collect())
}

fn evaluate_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
) -> Result<Value> {
    match aggregate.func {
        AggregateFunc::Count => count_aggregate(ctx, aggregate, rows),
        AggregateFunc::Sum => fold_aggregate(ctx, aggregate, rows, FoldKind::Sum),
        AggregateFunc::Avg => fold_aggregate(ctx, aggregate, rows, FoldKind::Avg),
        AggregateFunc::Min => min_max_aggregate(ctx, aggregate, rows, true),
        AggregateFunc::Max => min_max_aggregate(ctx, aggregate, rows, false),
        AggregateFunc::StddevSamp => variance_aggregate(ctx, aggregate, rows, Spread::Sample, true),
        AggregateFunc::StddevPop => {
            variance_aggregate(ctx, aggregate, rows, Spread::Population, true)
        }
        AggregateFunc::VarSamp => variance_aggregate(ctx, aggregate, rows, Spread::Sample, false),
        AggregateFunc::VarPop => {
            variance_aggregate(ctx, aggregate, rows, Spread::Population, false)
        }
        AggregateFunc::BoolAnd => bool_aggregate(ctx, aggregate, rows, true),
        AggregateFunc::BoolOr => bool_aggregate(ctx, aggregate, rows, false),
    }
}

/// Whether a variance/stddev divides by `n - 1` (sample) or `n` (population).
#[derive(Clone, Copy)]
enum Spread {
    Sample,
    Population,
}

/// `STDDEV_*` / `VAR_*`: the population/sample variance of the non-NULL numeric
/// inputs (or its square root for stddev). Returns NULL when there are too few
/// values (no rows for population; fewer than two for sample).
fn variance_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    spread: Spread,
    stddev: bool,
) -> Result<Value> {
    let mut data = Vec::new();
    for value in aggregate_values(ctx, aggregate, rows)? {
        match value {
            Value::Null => {}
            Value::Integer(value) => data.push(value as f64),
            Value::Float(value) => data.push(value.0),
            _ => {
                return Err(DbError::execute(
                    SqlState::DatatypeMismatch,
                    "STDDEV and VARIANCE require numeric input",
                ));
            }
        }
    }

    let n = data.len();
    let divisor = match spread {
        Spread::Sample if n < 2 => return Ok(Value::Null),
        Spread::Sample => (n - 1) as f64,
        Spread::Population if n == 0 => return Ok(Value::Null),
        Spread::Population => n as f64,
    };

    let mean = data.iter().sum::<f64>() / n as f64;
    let sum_squares = data.iter().map(|value| (value - mean).powi(2)).sum::<f64>();
    let variance = sum_squares / divisor;
    let result = if stddev { variance.sqrt() } else { variance };
    Ok(Value::Float(result.into()))
}

/// `BOOL_AND` (all) / `BOOL_OR` (any) over the non-NULL boolean inputs; NULL when
/// there are no non-NULL inputs.
fn bool_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    all: bool,
) -> Result<Value> {
    let mut seen = false;
    let mut acc = all;
    for value in aggregate_values(ctx, aggregate, rows)? {
        match value {
            Value::Null => {}
            Value::Boolean(value) => {
                seen = true;
                acc = if all { acc && value } else { acc || value };
            }
            _ => {
                return Err(DbError::execute(
                    SqlState::DatatypeMismatch,
                    "BOOL_AND and BOOL_OR require boolean input",
                ));
            }
        }
    }
    if seen {
        Ok(Value::Boolean(acc))
    } else {
        Ok(Value::Null)
    }
}

fn count_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
) -> Result<Value> {
    if aggregate.arg.is_none() {
        return Ok(Value::Integer(rows.len() as i64));
    }

    let values = aggregate_values(ctx, aggregate, rows)?;
    Ok(Value::Integer(
        values
            .into_iter()
            .filter(|value| !matches!(value, Value::Null))
            .count() as i64,
    ))
}

#[derive(Clone, Copy)]
enum FoldKind {
    Sum,
    Avg,
}

/// Dispatch SUM/AVG to the integer, real, double, or numeric fold based on the bound
/// result type.
fn fold_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    kind: FoldKind,
) -> Result<Value> {
    match aggregate.data_type {
        DataType::Double => float_fold_aggregate(ctx, aggregate, rows, kind),
        DataType::Real => real_fold_aggregate(ctx, aggregate, rows, kind),
        DataType::Numeric { .. } => numeric_fold_aggregate(ctx, aggregate, rows, kind),
        _ => integer_fold_aggregate(ctx, aggregate, rows, kind),
    }
}

fn integer_fold_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    kind: FoldKind,
) -> Result<Value> {
    let values = aggregate_values(ctx, aggregate, rows)?;
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
        FoldKind::Sum => Ok(Value::Integer(sum)),
        FoldKind::Avg => Ok(Value::Integer(sum / count)),
    }
}

fn float_fold_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    kind: FoldKind,
) -> Result<Value> {
    let values = aggregate_values(ctx, aggregate, rows)?;
    let mut sum = 0.0_f64;
    let mut count = 0_i64;
    for value in values {
        match value {
            Value::Null => {}
            Value::Float(value) => {
                sum += value.0;
                count += 1;
            }
            _ => {
                return Err(DbError::execute(
                    SqlState::DatatypeMismatch,
                    "SUM and AVG require numeric input",
                ));
            }
        }
    }

    if count == 0 {
        return Ok(Value::Null);
    }

    match kind {
        FoldKind::Sum => Ok(Value::Float(sum.into())),
        FoldKind::Avg => Ok(Value::Float((sum / count as f64).into())),
    }
}

fn real_fold_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    kind: FoldKind,
) -> Result<Value> {
    let values = aggregate_values(ctx, aggregate, rows)?;
    let mut sum = 0.0_f32;
    let mut count = 0_i64;
    for value in values {
        match value {
            Value::Null => {}
            Value::Real(value) => {
                sum += value.0;
                count += 1;
            }
            _ => {
                return Err(DbError::execute(
                    SqlState::DatatypeMismatch,
                    "SUM and AVG require numeric input",
                ));
            }
        }
    }

    if count == 0 {
        return Ok(Value::Null);
    }

    match kind {
        FoldKind::Sum => Ok(Value::Real(sum.into())),
        FoldKind::Avg => Ok(Value::Real((sum / count as f32).into())),
    }
}

fn numeric_fold_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    kind: FoldKind,
) -> Result<Value> {
    let values = aggregate_values(ctx, aggregate, rows)?;
    let mut sum = Decimal::ZERO;
    let mut count = 0_i64;
    for value in values {
        match value {
            Value::Null => {}
            Value::Numeric(value) => {
                sum = sum.checked_add(value).ok_or_else(numeric_overflow)?;
                count += 1;
            }
            _ => {
                return Err(DbError::execute(
                    SqlState::DatatypeMismatch,
                    "SUM and AVG require numeric input",
                ));
            }
        }
    }

    if count == 0 {
        return Ok(Value::Null);
    }

    match kind {
        FoldKind::Sum => Ok(Value::Numeric(sum)),
        // AVG divides by the row count; the divisor is non-zero here.
        FoldKind::Avg => sum
            .checked_div(Decimal::from(count))
            .map(Value::Numeric)
            .ok_or_else(numeric_overflow),
    }
}

fn numeric_overflow() -> DbError {
    DbError::execute(SqlState::NumericValueOutOfRange, "numeric field overflow")
}

fn min_max_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
    min: bool,
) -> Result<Value> {
    let values = aggregate_values(ctx, aggregate, rows)?;
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

fn aggregate_values(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    rows: &[ExecRow],
) -> Result<Vec<Value>> {
    let Some(arg) = &aggregate.arg else {
        return Ok(Vec::new());
    };

    let mut values = Vec::with_capacity(rows.len());
    let mut distinct = BTreeSet::new();
    for row in rows {
        let value = eval_expr_with_context(ctx, arg, row)?;
        if aggregate.distinct && !distinct.insert(value.clone()) {
            continue;
        }
        values.push(value);
    }
    Ok(values)
}
