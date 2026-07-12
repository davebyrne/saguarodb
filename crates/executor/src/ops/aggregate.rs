use std::cmp::Ordering;

use common::{
    ArrayDimension, ColumnInfo, DataType, DbError, Decimal, ExecRow, Result, Row, SqlArray,
    SqlState, StatementContext, Value,
};
use planner::{AggregateExpr, AggregateFunc, BoundExpr};
use spill::{ExternalSorter, SortedStream, SpillConfig, SpillContext, SpillTape};

use crate::eval_expr;
use crate::expr::integer_overflow;
use crate::ops::spill_row::SpillRow;
use crate::query::{PlanExecutor, close_after, open_executor};

type ValueSorter = ExternalSorter<SpillRow, Box<dyn Fn(&SpillRow, &SpillRow) -> Ordering>>;

pub struct AggregateOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    group_by: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    output_schema: Vec<ColumnInfo>,
    spill: SpillConfig,
    spill_ctx: Option<SpillContext>,
    stream: Option<SortedStream<SpillRow>>,
    pending: Option<SpillRow>,
    global_result: Option<ExecRow>,
}

impl<'a> AggregateOp<'a> {
    pub fn new(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
        spill: SpillConfig,
    ) -> Self {
        Self {
            ctx,
            source,
            group_by,
            aggregates,
            output_schema,
            spill,
            spill_ctx: None,
            stream: None,
            pending: None,
            global_result: None,
        }
    }

    fn process_row(
        &self,
        record: SpillRow,
        states: &mut [AggregateState],
        distinct: &mut [Option<ValueSorter>],
        ordinal: &mut u64,
    ) -> Result<()> {
        for ((aggregate, state), sorter) in self.aggregates.iter().zip(states).zip(distinct) {
            self.ctx.cancel.check()?;
            let value = match &aggregate.arg {
                Some(arg) => Some(eval_expr(&self.ctx, arg, &record.row)?),
                None => None,
            };
            if let Some(sorter) = sorter {
                let value = value.unwrap_or(Value::Null);
                sorter.push(SpillRow {
                    row: ExecRow {
                        row: Row {
                            values: vec![value.clone()],
                        },
                        identity: None,
                    },
                    keys: vec![value],
                    ordinal: *ordinal,
                    source: 0,
                })?;
            } else {
                state.step(value.as_ref())?;
            }
        }
        *ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| DbError::internal("aggregate group ordinal overflow"))?;
        Ok(())
    }

    fn finish_distinct(
        states: &mut [AggregateState],
        distinct: Vec<Option<ValueSorter>>,
        spill_ctx: &SpillContext,
    ) -> Result<()> {
        for (state, sorter) in states.iter_mut().zip(distinct) {
            let Some(sorter) = sorter else { continue };
            let mut values = sorter.finish()?;
            let mut first_occurrences = ExternalSorter::new(
                spill_ctx.clone(),
                Box::new(|left: &SpillRow, right: &SpillRow| left.ordinal.cmp(&right.ordinal))
                    as Box<dyn Fn(&SpillRow, &SpillRow) -> Ordering>,
            );
            let mut previous = None;
            while let Some(record) = values.next_record()? {
                let value = record
                    .row
                    .row
                    .values
                    .first()
                    .ok_or_else(|| DbError::internal("empty distinct aggregate record"))?;
                if previous.as_ref() == Some(value) {
                    continue;
                }
                previous = Some(value.clone());
                first_occurrences.push(record)?;
            }
            let mut first_occurrences = first_occurrences.finish()?;
            while let Some(record) = first_occurrences.next_record()? {
                let value = record
                    .row
                    .row
                    .values
                    .first()
                    .ok_or_else(|| DbError::internal("empty distinct aggregate record"))?;
                state.step(Some(value))?;
            }
        }
        Ok(())
    }
}

impl PlanExecutor for AggregateOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.stream = None;
        self.pending = None;
        self.global_result = None;
        let order_ctx = self.spill.for_operator(self.ctx.cancel.clone());
        self.spill_ctx = Some(order_ctx.clone());
        if self.group_by.is_empty() {
            let mut states = self
                .aggregates
                .iter()
                .map(|aggregate| AggregateState::new(aggregate, order_ctx.clone()))
                .collect::<Result<Vec<_>>>()?;
            let mut distinct = self
                .aggregates
                .iter()
                .map(|aggregate| {
                    aggregate.distinct.then(|| {
                        ExternalSorter::new(
                            order_ctx.clone(),
                            Box::new(|left: &SpillRow, right: &SpillRow| {
                                left.keys
                                    .cmp(&right.keys)
                                    .then_with(|| left.ordinal.cmp(&right.ordinal))
                            })
                                as Box<dyn Fn(&SpillRow, &SpillRow) -> Ordering>,
                        )
                    })
                })
                .collect::<Vec<_>>();
            open_executor(self.source.as_mut())?;
            let result = (|| {
                let mut ordinal = 0u64;
                while let Some(row) = self.source.next()? {
                    self.process_row(
                        SpillRow {
                            row,
                            keys: Vec::new(),
                            ordinal,
                            source: 0,
                        },
                        &mut states,
                        &mut distinct,
                        &mut ordinal,
                    )?;
                }
                Self::finish_distinct(&mut states, distinct, &order_ctx)?;
                let values = states
                    .into_iter()
                    .map(AggregateState::finish)
                    .collect::<Result<Vec<_>>>()?;
                Ok(ExecRow {
                    row: Row { values },
                    identity: None,
                })
            })();
            self.global_result = Some(close_after(self.source.as_mut(), result)?);
            return Ok(());
        }
        let mut sorter = ExternalSorter::new(order_ctx, |left: &SpillRow, right: &SpillRow| {
            left.keys
                .cmp(&right.keys)
                .then_with(|| left.ordinal.cmp(&right.ordinal))
        });
        open_executor(self.source.as_mut())?;
        let result = (|| {
            let mut ordinal = 0u64;
            while let Some(row) = self.source.next()? {
                self.ctx.cancel.check()?;
                let keys = self
                    .group_by
                    .iter()
                    .map(|expr| eval_expr(&self.ctx, expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                sorter.push(SpillRow {
                    row,
                    keys,
                    ordinal,
                    source: 0,
                })?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| DbError::internal("aggregate input ordinal overflow"))?;
            }
            sorter.finish()
        })();
        self.stream = Some(close_after(self.source.as_mut(), result)?);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        if self.group_by.is_empty() {
            return Ok(self.global_result.take());
        }
        let first = match self.pending.take() {
            Some(record) => Some(record),
            None => self
                .stream
                .as_mut()
                .ok_or_else(|| DbError::internal("aggregate is not open"))?
                .next_record()?,
        };
        let Some(first) = first else { return Ok(None) };

        let group_key = first.keys.clone();
        let spill_ctx = self
            .spill_ctx
            .as_ref()
            .expect("open aggregate spill context")
            .clone();
        let mut states = self
            .aggregates
            .iter()
            .map(|aggregate| AggregateState::new(aggregate, spill_ctx.clone()))
            .collect::<Result<Vec<_>>>()?;
        let mut distinct = self
            .aggregates
            .iter()
            .map(|aggregate| {
                aggregate.distinct.then(|| {
                    ExternalSorter::new(
                        spill_ctx.clone(),
                        Box::new(|left: &SpillRow, right: &SpillRow| {
                            left.keys
                                .cmp(&right.keys)
                                .then_with(|| left.ordinal.cmp(&right.ordinal))
                        }) as Box<dyn Fn(&SpillRow, &SpillRow) -> Ordering>,
                    )
                })
            })
            .collect::<Vec<_>>();
        let mut ordinal = 0u64;
        self.process_row(first, &mut states, &mut distinct, &mut ordinal)?;
        loop {
            let record = self
                .stream
                .as_mut()
                .expect("open aggregate stream")
                .next_record()?;
            match record {
                Some(record) if record.keys == group_key => {
                    self.process_row(record, &mut states, &mut distinct, &mut ordinal)?;
                }
                Some(record) => {
                    self.pending = Some(record);
                    break;
                }
                None => break,
            }
        }

        Self::finish_distinct(&mut states, distinct, &spill_ctx)?;
        let mut values = group_key;
        for state in states {
            values.push(state.finish()?);
        }
        Ok(Some(ExecRow {
            row: Row { values },
            identity: None,
        }))
    }

    fn close(&mut self) -> Result<()> {
        self.stream = None;
        self.spill_ctx = None;
        self.pending = None;
        self.global_result = None;
        Ok(())
    }
}

enum AggregateState {
    Count(i64),
    Integer {
        sum: i64,
        count: i64,
        avg: bool,
    },
    Float {
        sum: f64,
        count: i64,
        avg: bool,
    },
    Real {
        sum: f32,
        count: i64,
        avg: bool,
    },
    Numeric {
        sum: Decimal,
        count: i64,
        avg: bool,
    },
    MinMax {
        value: Option<Value>,
        min: bool,
    },
    Variance {
        count: u64,
        mean: f64,
        m2: f64,
        sample: bool,
        stddev: bool,
    },
    Bool {
        seen: bool,
        value: bool,
        all: bool,
    },
    Collect {
        tape: SpillTape<ExecRow>,
        kind: CollectKind,
    },
}

enum CollectKind {
    Array(DataType),
    String,
}

impl AggregateState {
    fn new(aggregate: &AggregateExpr, spill_ctx: SpillContext) -> Result<Self> {
        Ok(match aggregate.func {
            AggregateFunc::Count => Self::Count(0),
            AggregateFunc::Sum | AggregateFunc::Avg => {
                let avg = aggregate.func == AggregateFunc::Avg;
                match aggregate.data_type {
                    DataType::Double => Self::Float {
                        sum: 0.0,
                        count: 0,
                        avg,
                    },
                    DataType::Real => Self::Real {
                        sum: 0.0,
                        count: 0,
                        avg,
                    },
                    DataType::Numeric { .. } => Self::Numeric {
                        sum: Decimal::ZERO,
                        count: 0,
                        avg,
                    },
                    _ => Self::Integer {
                        sum: 0,
                        count: 0,
                        avg,
                    },
                }
            }
            AggregateFunc::Min => Self::MinMax {
                value: None,
                min: true,
            },
            AggregateFunc::Max => Self::MinMax {
                value: None,
                min: false,
            },
            AggregateFunc::StddevSamp => Self::Variance {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                sample: true,
                stddev: true,
            },
            AggregateFunc::StddevPop => Self::Variance {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                sample: false,
                stddev: true,
            },
            AggregateFunc::VarSamp => Self::Variance {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                sample: true,
                stddev: false,
            },
            AggregateFunc::VarPop => Self::Variance {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                sample: false,
                stddev: false,
            },
            AggregateFunc::BoolAnd => Self::Bool {
                seen: false,
                value: true,
                all: true,
            },
            AggregateFunc::BoolOr => Self::Bool {
                seen: false,
                value: false,
                all: false,
            },
            AggregateFunc::ArrayAgg => {
                let DataType::Array(array_type) = &aggregate.data_type else {
                    return Err(DbError::internal("ARRAY_AGG result type is not an array"));
                };
                Self::Collect {
                    tape: SpillTape::new(spill_ctx),
                    kind: CollectKind::Array(array_type.element_type().clone()),
                }
            }
            AggregateFunc::StringAgg => Self::Collect {
                tape: SpillTape::new(spill_ctx),
                kind: CollectKind::String,
            },
        })
    }

    fn step(&mut self, value: Option<&Value>) -> Result<()> {
        match self {
            Self::Count(count) => {
                if value.is_none_or(|value| !matches!(value, Value::Null)) {
                    *count = count.checked_add(1).ok_or_else(integer_overflow)?;
                }
            }
            Self::Integer { sum, count, .. } => match value {
                None | Some(Value::Null) => {}
                Some(Value::Integer(value)) => {
                    *sum = sum.checked_add(*value).ok_or_else(integer_overflow)?;
                    *count = count.checked_add(1).ok_or_else(integer_overflow)?;
                }
                _ => return Err(type_error("SUM and AVG require integer input")),
            },
            Self::Float { sum, count, .. } => match value {
                None | Some(Value::Null) => {}
                Some(Value::Float(value)) => {
                    *sum += value.0;
                    *count += 1;
                }
                _ => return Err(type_error("SUM and AVG require numeric input")),
            },
            Self::Real { sum, count, .. } => match value {
                None | Some(Value::Null) => {}
                Some(Value::Real(value)) => {
                    *sum += value.0;
                    *count += 1;
                }
                _ => return Err(type_error("SUM and AVG require numeric input")),
            },
            Self::Numeric { sum, count, .. } => match value {
                None | Some(Value::Null) => {}
                Some(Value::Numeric(value)) => {
                    *sum = sum.checked_add(*value).ok_or_else(numeric_overflow)?;
                    *count += 1;
                }
                _ => return Err(type_error("SUM and AVG require numeric input")),
            },
            Self::MinMax { value: best, min } => {
                let Some(value) = value.filter(|value| !matches!(value, Value::Null)) else {
                    return Ok(());
                };
                if best
                    .as_ref()
                    .is_none_or(|current| (*min && value < current) || (!*min && value > current))
                {
                    *best = Some(value.clone());
                }
            }
            Self::Variance {
                count, mean, m2, ..
            } => {
                let value = match value {
                    None | Some(Value::Null) => return Ok(()),
                    Some(Value::Integer(value)) => *value as f64,
                    Some(Value::Float(value)) => value.0,
                    Some(Value::Real(value)) => value.0 as f64,
                    Some(Value::Numeric(value)) => {
                        common::numeric::to_f64(value).ok_or_else(numeric_overflow)?
                    }
                    _ => return Err(type_error("STDDEV and VARIANCE require numeric input")),
                };
                *count = count.checked_add(1).ok_or_else(integer_overflow)?;
                let delta = value - *mean;
                *mean += delta / *count as f64;
                *m2 += delta * (value - *mean);
            }
            Self::Bool {
                seen,
                value: result,
                all,
            } => match value {
                None | Some(Value::Null) => {}
                Some(Value::Boolean(value)) => {
                    *seen = true;
                    *result = if *all {
                        *result && *value
                    } else {
                        *result || *value
                    };
                }
                _ => return Err(type_error("BOOL_AND and BOOL_OR require boolean input")),
            },
            Self::Collect { tape, .. } => {
                let value = value
                    .cloned()
                    .ok_or_else(|| DbError::internal("collect aggregate has no argument"))?;
                tape.push(ExecRow {
                    row: Row {
                        values: vec![value],
                    },
                    identity: None,
                })?;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<Value> {
        let state = match self {
            Self::Collect { tape, kind } => return finish_collection(tape, kind),
            state => state,
        };
        Ok(match state {
            Self::Count(count) => Value::Integer(count),
            Self::Integer { count: 0, .. } => Value::Null,
            Self::Integer { sum, count, avg } => {
                Value::Integer(if avg { sum / count } else { sum })
            }
            Self::Float { count: 0, .. } => Value::Null,
            Self::Float { sum, count, avg } => {
                Value::Float((if avg { sum / count as f64 } else { sum }).into())
            }
            Self::Real { count: 0, .. } => Value::Null,
            Self::Real { sum, count, avg } => {
                Value::Real((if avg { sum / count as f32 } else { sum }).into())
            }
            Self::Numeric { count: 0, .. } => Value::Null,
            Self::Numeric { sum, count, avg } => {
                if avg {
                    Value::Numeric(
                        sum.checked_div(Decimal::from(count))
                            .ok_or_else(numeric_overflow)?,
                    )
                } else {
                    Value::Numeric(sum)
                }
            }
            Self::MinMax { value, .. } => value.unwrap_or(Value::Null),
            Self::Variance {
                count,
                m2,
                sample,
                stddev,
                ..
            } => {
                if (sample && count < 2) || (!sample && count == 0) {
                    Value::Null
                } else {
                    let divisor = if sample {
                        (count - 1) as f64
                    } else {
                        count as f64
                    };
                    let variance = m2 / divisor;
                    Value::Float((if stddev { variance.sqrt() } else { variance }).into())
                }
            }
            Self::Bool { seen: false, .. } => Value::Null,
            Self::Bool { value, .. } => Value::Boolean(value),
            Self::Collect { .. } => unreachable!("collect aggregate handled above"),
        })
    }
}

fn finish_collection(mut tape: SpillTape<ExecRow>, kind: CollectKind) -> Result<Value> {
    tape.finish()?;
    let mut reader = tape.reader()?;
    match kind {
        CollectKind::Array(element_type) => {
            let mut values = Vec::new();
            while let Some(row) = reader.next_record()? {
                let [value] = row.row.values.as_slice() else {
                    return Err(DbError::internal("invalid ARRAY_AGG spill record"));
                };
                values.push(value.clone());
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let len = u32::try_from(values.len()).map_err(|_| {
                DbError::execute(SqlState::ProgramLimitExceeded, "array is too large")
            })?;
            Ok(Value::Array(SqlArray::new(
                element_type,
                vec![ArrayDimension::new(len, 1)],
                values,
            )?))
        }
        CollectKind::String => {
            let mut output = String::new();
            let mut seen = false;
            while let Some(row) = reader.next_record()? {
                let [Value::Array(pair)] = row.row.values.as_slice() else {
                    return Err(DbError::internal("STRING_AGG arguments are not a pair"));
                };
                let [value, delimiter] = pair.elements() else {
                    return Err(DbError::internal("STRING_AGG arguments are not a pair"));
                };
                let Value::Text(value) = value else {
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    return Err(DbError::internal("STRING_AGG value is not text"));
                };
                if seen && let Value::Text(delimiter) = delimiter {
                    output.push_str(delimiter);
                }
                output.push_str(value);
                seen = true;
            }
            Ok(if seen {
                Value::Text(output)
            } else {
                Value::Null
            })
        }
    }
}

fn type_error(message: &str) -> DbError {
    DbError::execute(SqlState::DatatypeMismatch, message)
}

fn numeric_overflow() -> DbError {
    DbError::execute(SqlState::NumericValueOutOfRange, "numeric field overflow")
}
