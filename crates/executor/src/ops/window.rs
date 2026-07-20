use std::cmp::Ordering;

use common::{
    ColumnInfo, DbError, Decimal, ExecRow, Result, Row, SqlState, StatementContext, Value,
};
use planner::{
    AggregateExpr, AggregateFunc, BoundExpr, BoundFrameBound, BoundOrderByItem, BoundWindowSpec,
    WindowFrameUnits, WindowFunc, WindowFuncExpr,
};
use spill::{ExternalSorter, SortedStream, SpillConfig, SpillContext, SpillTape, SpillTapeReader};

use crate::eval_expr;
use crate::ops::aggregate::AggregateState;
use crate::ops::sort::compare_keys;
use crate::ops::spill_row::SpillRow;
use crate::query::{PlanExecutor, close_after, open_executor};

pub struct WindowOp<'a> {
    ctx: StatementContext,
    source: Box<dyn PlanExecutor + 'a>,
    spec: BoundWindowSpec,
    functions: Vec<RuntimeFunction>,
    offset_keys: Vec<OffsetKey>,
    output_schema: Vec<ColumnInfo>,
    input_width: usize,
    spill: SpillConfig,
    spill_ctx: Option<SpillContext>,
    stream: Option<SortedStream<SpillRow>>,
    pending: Option<SpillRow>,
    partition: Option<PartitionState>,
}

#[derive(Clone)]
enum RuntimeFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile {
        arg: BoundExpr,
    },
    Offset {
        value: BoundExpr,
        default: Option<BoundExpr>,
        cursor: Option<usize>,
    },
    FirstValue {
        value: BoundExpr,
    },
    LastValue {
        value: BoundExpr,
    },
    NthValue {
        value: BoundExpr,
        n: BoundExpr,
    },
    Aggregate {
        aggregate: AggregateExpr,
        mode: AggregateMode,
        collect: bool,
        cache_peers: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateMode {
    WholePartition,
    Growing,
    Sliding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OffsetDirection {
    Lag,
    Lead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OffsetKey {
    direction: OffsetDirection,
    amount: u64,
}

#[derive(Clone)]
struct TapeCursor {
    reader: SpillTapeReader<SpillRow>,
    next_index: u64,
    lookahead: Option<(u64, SpillRow)>,
}

impl TapeCursor {
    fn new(reader: SpillTapeReader<SpillRow>) -> Self {
        Self {
            reader,
            next_index: 0,
            lookahead: None,
        }
    }

    fn advance_to(&mut self, target: u64) -> Result<Option<SpillRow>> {
        if let Some((index, row)) = &self.lookahead {
            if *index == target {
                return Ok(Some(row.clone()));
            }
            if *index > target {
                return Err(DbError::internal("window tape cursor moved backwards"));
            }
        }
        loop {
            let Some(row) = self.reader.next_record()? else {
                self.lookahead = None;
                return Ok(None);
            };
            let index = self.next_index;
            self.next_index = self
                .next_index
                .checked_add(1)
                .ok_or_else(|| DbError::internal("window tape cursor index overflow"))?;
            self.lookahead = Some((index, row));
            if index == target {
                return Ok(self.lookahead.as_ref().map(|(_, row)| row.clone()));
            }
            if index > target {
                return Err(DbError::internal("window tape cursor skipped its target"));
            }
        }
    }
}

struct PartitionState {
    _tape: SpillTape<SpillRow>,
    current: TapeCursor,
    peer_probe: TapeCursor,
    offsets: Vec<(OffsetKey, TapeCursor)>,
    frame_start: TapeCursor,
    frame_end: TapeCursor,
    range_start_probe: TapeCursor,
    range_end_probe: TapeCursor,
    frame_start_index: u64,
    frame_end_index: u64,
    count: u64,
    emit_index: u64,
    peer_start: u64,
    peer_end: u64,
    dense_rank: u64,
    ntile: Vec<Option<Value>>,
    aggregates: Vec<Option<AggregateRuntime>>,
}

struct AggregateRuntime {
    state: Option<AggregateState>,
    feed: TapeCursor,
    fed: u64,
    frame_start: u64,
    cached_peer_end: Option<u64>,
    cached_value: Option<Value>,
}

impl WindowOp<'_> {
    pub fn new<'a>(
        ctx: StatementContext,
        source: Box<dyn PlanExecutor + 'a>,
        spec: BoundWindowSpec,
        functions: Vec<WindowFuncExpr>,
        spill: SpillConfig,
    ) -> Result<WindowOp<'a>> {
        let input_width = source.output_schema().len();
        let mut output_schema = source.output_schema().to_vec();
        let mut offset_keys = Vec::new();
        let mut runtime = Vec::with_capacity(functions.len());
        for function in &functions {
            output_schema.push(ColumnInfo {
                name: function_name(function.func).to_string(),
                data_type: function.data_type.clone(),
                table_id: None,
                column_id: None,
                pg_type: None,
            });
            runtime.push(runtime_function(function, &spec, &mut offset_keys)?);
        }

        Ok(WindowOp {
            ctx,
            source,
            spec,
            functions: runtime,
            offset_keys,
            output_schema,
            input_width,
            spill,
            spill_ctx: None,
            stream: None,
            pending: None,
            partition: None,
        })
    }

    fn load_partition(&mut self) -> Result<bool> {
        let first = match self.pending.take() {
            Some(row) => row,
            None => {
                let stream = self
                    .stream
                    .as_mut()
                    .ok_or_else(|| DbError::internal("window is not open"))?;
                let Some(row) = stream.next_record()? else {
                    return Ok(false);
                };
                row
            }
        };
        validate_row_width(&first.row, self.input_width)?;
        let partition_key_count = self.spec.partition_by.len();
        let first_partition_keys = key_prefix(&first.keys, partition_key_count)?.to_vec();
        let first_row = first.row.clone();
        let spill_ctx = self
            .spill_ctx
            .as_ref()
            .ok_or_else(|| DbError::internal("window spill context is missing"))?
            .clone();
        let mut tape = SpillTape::new(spill_ctx);
        tape.push(first)?;
        let mut count = 1u64;
        loop {
            self.ctx.cancel.check()?;
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| DbError::internal("window is not open"))?;
            let Some(row) = stream.next_record()? else {
                break;
            };
            validate_row_width(&row.row, self.input_width)?;
            if key_prefix(&row.keys, partition_key_count)? != first_partition_keys.as_slice() {
                self.pending = Some(row);
                break;
            }
            tape.push(row)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| DbError::internal("window partition row count overflow"))?;
        }
        tape.finish()?;

        let current = TapeCursor::new(tape.reader()?);
        let peer_probe = TapeCursor::new(tape.reader()?);
        let frame_start = TapeCursor::new(tape.reader()?);
        let frame_end = TapeCursor::new(tape.reader()?);
        let range_start_probe = TapeCursor::new(tape.reader()?);
        let range_end_probe = TapeCursor::new(tape.reader()?);
        let mut offsets = Vec::new();
        for key in &self.offset_keys {
            offsets.push((*key, TapeCursor::new(tape.reader()?)));
        }
        let mut ntile = vec![None; self.functions.len()];
        let mut aggregates = Vec::with_capacity(self.functions.len());
        let aggregate_spill_ctx = self
            .spill_ctx
            .as_ref()
            .ok_or_else(|| DbError::internal("window spill context is missing"))?
            .clone();
        for (index, function) in self.functions.iter().enumerate() {
            if let RuntimeFunction::Ntile { arg } = function {
                let value = eval_expr(&self.ctx, arg, &first_row)?;
                ntile[index] = Some(validate_ntile(value)?);
            }
            let runtime = match function {
                RuntimeFunction::Aggregate {
                    aggregate,
                    mode,
                    collect,
                    ..
                } => Some(AggregateRuntime {
                    state: if *collect || matches!(mode, AggregateMode::Sliding) {
                        None
                    } else {
                        Some(AggregateState::new(aggregate, aggregate_spill_ctx.clone())?)
                    },
                    feed: TapeCursor::new(tape.reader()?),
                    fed: 0,
                    frame_start: 0,
                    cached_peer_end: None,
                    cached_value: None,
                }),
                _ => None,
            };
            aggregates.push(runtime);
        }
        self.partition = Some(PartitionState {
            _tape: tape,
            current,
            peer_probe,
            offsets,
            frame_start,
            frame_end,
            range_start_probe,
            range_end_probe,
            frame_start_index: 0,
            frame_end_index: 0,
            count,
            emit_index: 0,
            peer_start: 0,
            peer_end: 0,
            dense_rank: 0,
            ntile,
            aggregates,
        });
        Ok(true)
    }
}

impl PlanExecutor for WindowOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.stream = None;
        self.pending = None;
        self.partition = None;
        validate_frame_offsets(&self.spec)?;
        let spill_ctx = self.spill.for_operator(self.ctx.cancel.clone());
        self.spill_ctx = Some(spill_ctx.clone());

        let mut sort_order = self
            .spec
            .partition_by
            .iter()
            .cloned()
            .map(|expr| BoundOrderByItem {
                expr,
                ascending: true,
                nulls_first: Some(false),
            })
            .collect::<Vec<_>>();
        sort_order.extend(self.spec.order_by.clone());
        let compare_order = sort_order.clone();
        let mut sorter =
            ExternalSorter::new(spill_ctx, move |left: &SpillRow, right: &SpillRow| {
                compare_keys(&left.keys, &right.keys, &compare_order)
            });
        open_executor(self.source.as_mut())?;
        let result = (|| {
            let mut ordinal = 0u64;
            while let Some(row) = self.source.next()? {
                self.ctx.cancel.check()?;
                validate_row_width(&row, self.input_width)?;
                let keys = sort_order
                    .iter()
                    .map(|item| eval_expr(&self.ctx, &item.expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                sorter.push(SpillRow {
                    row,
                    keys,
                    ordinal,
                    source: 0,
                })?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| DbError::internal("window input ordinal overflow"))?;
            }
            sorter.finish()
        })();
        self.stream = Some(close_after(self.source.as_mut(), result)?);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        loop {
            self.ctx.cancel.check()?;
            if self.partition.is_none() && !self.load_partition()? {
                return Ok(None);
            }
            let partition = self
                .partition
                .as_mut()
                .ok_or_else(|| DbError::internal("window partition is missing"))?;
            if partition.emit_index >= partition.count {
                self.partition = None;
                continue;
            }
            let index = partition.emit_index;
            let record = partition
                .current
                .advance_to(index)?
                .ok_or_else(|| DbError::internal("window partition tape ended early"))?;
            update_peer_group(
                partition,
                &record,
                self.spec.partition_by.len(),
                self.spec.order_by.len(),
            )?;
            let (frame_start, frame_end) = frame_bounds(
                &self.ctx,
                &self.spec,
                partition,
                &record,
                self.spec.partition_by.len(),
            )?;
            if frame_start < partition.count {
                partition
                    .frame_start
                    .advance_to(frame_start)?
                    .ok_or_else(|| {
                        DbError::internal("window frame-start cursor ended inside partition")
                    })?;
            }
            let mut values = record.row.row.values.clone();
            values
                .try_reserve(self.functions.len())
                .map_err(|_| DbError::internal("failed to reserve window output row capacity"))?;
            for (function_index, function) in self.functions.iter().enumerate() {
                self.ctx.cancel.check()?;
                values.push(evaluate_function(
                    &self.ctx,
                    function,
                    function_index,
                    partition,
                    &record.row,
                    frame_start,
                    frame_end,
                    self.spill_ctx
                        .as_ref()
                        .ok_or_else(|| DbError::internal("window spill context is missing"))?,
                )?);
            }
            partition.emit_index = partition
                .emit_index
                .checked_add(1)
                .ok_or_else(|| DbError::internal("window emission index overflow"))?;
            return Ok(Some(ExecRow {
                row: Row { values },
                identity: record.row.identity,
            }));
        }
    }

    fn close(&mut self) -> Result<()> {
        self.partition = None;
        self.pending = None;
        self.stream = None;
        self.spill_ctx = None;
        Ok(())
    }
}

fn runtime_function(
    function: &WindowFuncExpr,
    spec: &BoundWindowSpec,
    offset_keys: &mut Vec<OffsetKey>,
) -> Result<RuntimeFunction> {
    Ok(match function.func {
        WindowFunc::RowNumber => RuntimeFunction::RowNumber,
        WindowFunc::Rank => RuntimeFunction::Rank,
        WindowFunc::DenseRank => RuntimeFunction::DenseRank,
        WindowFunc::PercentRank => RuntimeFunction::PercentRank,
        WindowFunc::CumeDist => RuntimeFunction::CumeDist,
        WindowFunc::Ntile => RuntimeFunction::Ntile {
            arg: required_arg(&function.args, 0, "ntile")?.clone(),
        },
        WindowFunc::Lag | WindowFunc::Lead => {
            let value = required_arg(&function.args, 0, function_name(function.func))?.clone();
            let default = function.args.get(2).cloned();
            let direction = if matches!(function.func, WindowFunc::Lag) {
                OffsetDirection::Lag
            } else {
                OffsetDirection::Lead
            };
            let offset = match function.args.get(1) {
                None => Some((direction, 1)),
                Some(BoundExpr::Literal {
                    value: Value::Null, ..
                }) => None,
                Some(BoundExpr::Literal {
                    value: Value::Integer(value),
                    ..
                }) => {
                    let normalized_direction = if *value < 0 {
                        opposite(direction)
                    } else {
                        direction
                    };
                    Some((normalized_direction, value.unsigned_abs()))
                }
                Some(_) => {
                    return Err(DbError::internal(
                        "lag/lead offset is not a bind-time integer constant",
                    ));
                }
            };
            let cursor = offset.map(|(direction, amount)| {
                let key = OffsetKey { direction, amount };
                match offset_keys.iter().position(|candidate| *candidate == key) {
                    Some(index) => index,
                    None => {
                        let index = offset_keys.len();
                        offset_keys.push(key);
                        index
                    }
                }
            });
            RuntimeFunction::Offset {
                value,
                default,
                cursor,
            }
        }
        WindowFunc::FirstValue => RuntimeFunction::FirstValue {
            value: required_arg(&function.args, 0, "first_value")?.clone(),
        },
        WindowFunc::LastValue => RuntimeFunction::LastValue {
            value: required_arg(&function.args, 0, "last_value")?.clone(),
        },
        WindowFunc::NthValue => RuntimeFunction::NthValue {
            value: required_arg(&function.args, 0, "nth_value")?.clone(),
            n: required_arg(&function.args, 1, "nth_value")?.clone(),
        },
        WindowFunc::Aggregate(func) => {
            let aggregate = AggregateExpr {
                func,
                arg: function.args.first().cloned(),
                distinct: false,
                data_type: function.data_type.clone(),
                nullable: function.nullable,
            };
            let collect = matches!(func, AggregateFunc::ArrayAgg | AggregateFunc::StringAgg);
            let mode = if is_whole_partition_frame(spec) {
                AggregateMode::WholePartition
            } else if matches!(spec.frame.start, BoundFrameBound::UnboundedPreceding) {
                AggregateMode::Growing
            } else {
                AggregateMode::Sliding
            };
            RuntimeFunction::Aggregate {
                aggregate,
                mode,
                collect,
                cache_peers: matches!(spec.frame.units, WindowFrameUnits::Range)
                    && matches!(spec.frame.end, BoundFrameBound::CurrentRow),
            }
        }
    })
}

fn required_arg<'a>(args: &'a [BoundExpr], index: usize, name: &str) -> Result<&'a BoundExpr> {
    args.get(index)
        .ok_or_else(|| DbError::internal(format!("validated {name} arity changed")))
}

fn opposite(direction: OffsetDirection) -> OffsetDirection {
    match direction {
        OffsetDirection::Lag => OffsetDirection::Lead,
        OffsetDirection::Lead => OffsetDirection::Lag,
    }
}

fn validate_ntile(value: Value) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(value) if value > 0 => Ok(Value::Integer(value)),
        Value::Integer(_) => Err(DbError::execute(
            SqlState::InvalidArgumentForNtile,
            "argument of ntile must be greater than zero",
        )),
        _ => Err(DbError::internal("ntile argument is not an integer")),
    }
}

fn is_whole_partition_frame(spec: &BoundWindowSpec) -> bool {
    matches!(spec.frame.start, BoundFrameBound::UnboundedPreceding)
        && matches!(spec.frame.end, BoundFrameBound::UnboundedFollowing)
}

fn validate_frame_offsets(spec: &BoundWindowSpec) -> Result<()> {
    for bound in [&spec.frame.start, &spec.frame.end] {
        let value = match bound {
            BoundFrameBound::PrecedingRange(value) | BoundFrameBound::FollowingRange(value) => {
                value
            }
            _ => continue,
        };
        if matches!(value, Value::Null) {
            return Err(DbError::execute(
                SqlState::NullValueNotAllowed,
                "frame offset must not be null",
            ));
        }
        let negative = match value {
            Value::Integer(value) => *value < 0,
            Value::Numeric(value) => *value < Decimal::ZERO,
            Value::Float(value) => value.0 < 0.0,
            Value::Real(value) => value.0 < 0.0,
            Value::Interval(value) => *value < common::Interval::ZERO,
            _ => {
                return Err(DbError::internal(
                    "RANGE frame offset has an unsupported runtime type",
                ));
            }
        };
        if negative {
            return Err(DbError::execute(
                SqlState::InvalidPrecedingOrFollowingSize,
                "frame starting offset must not be negative",
            ));
        }
    }
    Ok(())
}

fn frame_bounds(
    ctx: &StatementContext,
    spec: &BoundWindowSpec,
    partition: &mut PartitionState,
    current: &SpillRow,
    partition_key_count: usize,
) -> Result<(u64, u64)> {
    match spec.frame.units {
        WindowFrameUnits::Rows => Ok((
            rows_bound(
                &spec.frame.start,
                partition.emit_index,
                partition.count,
                false,
            )?,
            rows_bound(&spec.frame.end, partition.emit_index, partition.count, true)?,
        )),
        WindowFrameUnits::Range => {
            if spec.order_by.is_empty() {
                let boundary = |bound: &BoundFrameBound, end: bool| match bound {
                    BoundFrameBound::UnboundedPreceding => Ok(0),
                    BoundFrameBound::CurrentRow => Ok(if end { partition.count } else { 0 }),
                    BoundFrameBound::UnboundedFollowing => Ok(partition.count),
                    _ => Err(DbError::internal(
                        "offset RANGE frame unexpectedly has no order key",
                    )),
                };
                return Ok((
                    boundary(&spec.frame.start, false)?,
                    boundary(&spec.frame.end, true)?,
                ));
            }
            let order_key = current
                .keys
                .get(partition_key_count)
                .ok_or_else(|| DbError::internal("RANGE frame order key is missing"))?;
            let start = range_bound(
                ctx,
                &spec.frame.start,
                false,
                spec,
                order_key,
                partition_key_count,
                partition.peer_start,
                partition.peer_end,
                partition.count,
                &mut partition.range_start_probe,
                &mut partition.frame_start_index,
            )?;
            let end = range_bound(
                ctx,
                &spec.frame.end,
                true,
                spec,
                order_key,
                partition_key_count,
                partition.peer_start,
                partition.peer_end,
                partition.count,
                &mut partition.range_end_probe,
                &mut partition.frame_end_index,
            )?;
            Ok((start, end))
        }
    }
}

fn rows_bound(bound: &BoundFrameBound, index: u64, count: u64, end: bool) -> Result<u64> {
    let inclusive = |target: u64| -> Result<u64> {
        if end {
            target
                .checked_add(1)
                .map(|value| value.min(count))
                .ok_or_else(|| DbError::internal("ROWS frame boundary overflow"))
        } else {
            Ok(target.min(count))
        }
    };
    match bound {
        BoundFrameBound::UnboundedPreceding => Ok(0),
        BoundFrameBound::PrecedingRows(offset) => match index.checked_sub(*offset) {
            Some(target) => inclusive(target),
            None => Ok(0),
        },
        BoundFrameBound::CurrentRow => inclusive(index),
        BoundFrameBound::FollowingRows(offset) => match index.checked_add(*offset) {
            Some(target) => inclusive(target),
            None => Ok(count),
        },
        BoundFrameBound::UnboundedFollowing => Ok(count),
        BoundFrameBound::PrecedingRange(_) | BoundFrameBound::FollowingRange(_) => {
            Err(DbError::internal("RANGE offset found in ROWS frame"))
        }
    }
}

#[derive(Clone)]
enum RangeThreshold {
    BelowAll,
    Value(Value),
    AboveAll,
}

#[allow(clippy::too_many_arguments)]
fn range_bound(
    ctx: &StatementContext,
    bound: &BoundFrameBound,
    end: bool,
    spec: &BoundWindowSpec,
    current_key: &Value,
    key_index: usize,
    peer_start: u64,
    peer_end: u64,
    count: u64,
    cursor: &mut TapeCursor,
    probe_index: &mut u64,
) -> Result<u64> {
    match bound {
        BoundFrameBound::UnboundedPreceding => Ok(0),
        BoundFrameBound::UnboundedFollowing => Ok(count),
        BoundFrameBound::CurrentRow => Ok(if end { peer_end } else { peer_start }),
        BoundFrameBound::PrecedingRange(offset) | BoundFrameBound::FollowingRange(offset) => {
            if matches!(current_key, Value::Null) {
                return Ok(if end { peer_end } else { peer_start });
            }
            let order = spec
                .order_by
                .first()
                .ok_or_else(|| DbError::internal("offset RANGE frame has no order key"))?;
            let preceding = matches!(bound, BoundFrameBound::PrecedingRange(_));
            let add = preceding != order.ascending;
            let threshold = range_threshold(current_key, offset, add)?;
            while *probe_index < count {
                ctx.cancel.check()?;
                let row = cursor
                    .advance_to(*probe_index)?
                    .ok_or_else(|| DbError::internal("RANGE frame probe ended inside partition"))?;
                let candidate = row
                    .keys
                    .get(key_index)
                    .ok_or_else(|| DbError::internal("RANGE frame probe order key is missing"))?;
                if matches!(candidate, Value::Null) {
                    if order.nulls_first.unwrap_or(!order.ascending) {
                        *probe_index = probe_index
                            .checked_add(1)
                            .ok_or_else(|| DbError::internal("RANGE frame probe index overflow"))?;
                        continue;
                    }
                    return Ok(*probe_index);
                }
                let comparison = compare_range_candidate(candidate, &threshold, order)?;
                if comparison == Ordering::Greater || (!end && comparison == Ordering::Equal) {
                    return Ok(*probe_index);
                }
                *probe_index = probe_index
                    .checked_add(1)
                    .ok_or_else(|| DbError::internal("RANGE frame probe index overflow"))?;
            }
            Ok(count)
        }
        BoundFrameBound::PrecedingRows(_) | BoundFrameBound::FollowingRows(_) => {
            Err(DbError::internal("ROWS offset found in RANGE frame"))
        }
    }
}

fn range_threshold(key: &Value, offset: &Value, add: bool) -> Result<RangeThreshold> {
    let overflow = || {
        if add {
            RangeThreshold::AboveAll
        } else {
            RangeThreshold::BelowAll
        }
    };
    let value = match (key, offset) {
        (Value::Integer(key), Value::Integer(offset)) => {
            let value = if add {
                key.checked_add(*offset)
            } else {
                key.checked_sub(*offset)
            };
            return Ok(value.map_or_else(overflow, |value| {
                RangeThreshold::Value(Value::Integer(value))
            }));
        }
        (Value::Numeric(key), Value::Numeric(offset)) => {
            let value = if add {
                key.checked_add(*offset)
            } else {
                key.checked_sub(*offset)
            };
            return Ok(value.map_or_else(overflow, |value| {
                RangeThreshold::Value(Value::Numeric(value))
            }));
        }
        (Value::Float(key), Value::Float(offset)) => {
            let raw = if add {
                key.0 + offset.0
            } else {
                key.0 - offset.0
            };
            if raw.is_infinite() {
                return Ok(overflow());
            }
            Value::Float(raw.into())
        }
        (Value::Real(key), Value::Real(offset)) => {
            let raw = if add {
                key.0 + offset.0
            } else {
                key.0 - offset.0
            };
            if raw.is_infinite() {
                return Ok(overflow());
            }
            Value::Real(raw.into())
        }
        (Value::Timestamp(key), Value::Interval(offset)) => {
            let Some(value) = shift_range_timestamp(*key, *offset, add) else {
                return Ok(overflow());
            };
            Value::Timestamp(value)
        }
        (Value::TimestampTz(key), Value::Interval(offset)) => {
            let Some(value) = shift_range_timestamp(*key, *offset, add) else {
                return Ok(overflow());
            };
            Value::TimestampTz(value)
        }
        (Value::Date(days), Value::Interval(offset)) => {
            const MICROS_PER_DAY: i64 = 86_400_000_000;
            let Some(micros) = days.checked_mul(MICROS_PER_DAY) else {
                return Ok(overflow());
            };
            let Some(value) = shift_range_timestamp(micros, *offset, add) else {
                return Ok(overflow());
            };
            Value::Timestamp(value)
        }
        _ => {
            return Err(DbError::internal(
                "RANGE frame key and offset runtime types do not match",
            ));
        }
    };
    Ok(RangeThreshold::Value(value))
}

fn shift_range_timestamp(micros: i64, offset: common::Interval, add: bool) -> Option<i64> {
    let offset = if add {
        Some(offset)
    } else {
        offset.checked_neg()
    }?;
    common::datetime::add_interval_to_timestamp(micros, &offset)
}

fn compare_range_candidate(
    candidate: &Value,
    threshold: &RangeThreshold,
    order: &BoundOrderByItem,
) -> Result<Ordering> {
    let natural = match threshold {
        RangeThreshold::BelowAll => Ordering::Greater,
        RangeThreshold::AboveAll => Ordering::Less,
        RangeThreshold::Value(Value::Timestamp(threshold))
            if matches!(candidate, Value::Date(_)) =>
        {
            const MICROS_PER_DAY: i64 = 86_400_000_000;
            let Value::Date(days) = candidate else {
                return Err(DbError::internal("date RANGE candidate changed type"));
            };
            let micros = days
                .checked_mul(MICROS_PER_DAY)
                .ok_or_else(|| DbError::internal("date RANGE candidate overflow"))?;
            micros.cmp(threshold)
        }
        RangeThreshold::Value(threshold) => {
            if std::mem::discriminant(candidate) != std::mem::discriminant(threshold) {
                return Err(DbError::internal(
                    "RANGE frame comparison would cross value variants",
                ));
            }
            candidate.cmp(threshold)
        }
    };
    Ok(if order.ascending {
        natural
    } else {
        natural.reverse()
    })
}

fn update_peer_group(
    partition: &mut PartitionState,
    current: &SpillRow,
    partition_key_count: usize,
    order_key_count: usize,
) -> Result<()> {
    if partition.emit_index < partition.peer_end {
        return Ok(());
    }
    partition.peer_start = partition.emit_index;
    partition.dense_rank = partition
        .dense_rank
        .checked_add(1)
        .ok_or_else(|| DbError::internal("window dense rank overflow"))?;
    let current_order = key_suffix(&current.keys, partition_key_count, order_key_count)?;
    let mut probe_index = partition.emit_index;
    loop {
        let Some(probe) = partition.peer_probe.advance_to(probe_index)? else {
            partition.peer_end = partition.count;
            return Ok(());
        };
        if key_suffix(&probe.keys, partition_key_count, order_key_count)? != current_order {
            partition.peer_end = probe_index;
            return Ok(());
        }
        probe_index = probe_index
            .checked_add(1)
            .ok_or_else(|| DbError::internal("window peer probe index overflow"))?;
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_function(
    ctx: &StatementContext,
    function: &RuntimeFunction,
    function_index: usize,
    partition: &mut PartitionState,
    current: &ExecRow,
    frame_start: u64,
    frame_end: u64,
    spill_ctx: &SpillContext,
) -> Result<Value> {
    match function {
        RuntimeFunction::RowNumber => integer_value(
            partition
                .emit_index
                .checked_add(1)
                .ok_or_else(|| DbError::internal("row_number overflow"))?,
            "row_number",
        ),
        RuntimeFunction::Rank => integer_value(
            partition
                .peer_start
                .checked_add(1)
                .ok_or_else(|| DbError::internal("rank overflow"))?,
            "rank",
        ),
        RuntimeFunction::DenseRank => integer_value(partition.dense_rank, "dense_rank"),
        RuntimeFunction::PercentRank => {
            let value = if partition.count == 1 {
                0.0
            } else {
                let denominator = partition
                    .count
                    .checked_sub(1)
                    .ok_or_else(|| DbError::internal("percent_rank denominator underflow"))?;
                partition.peer_start as f64 / denominator as f64
            };
            Ok(Value::Float(value.into()))
        }
        RuntimeFunction::CumeDist => Ok(Value::Float(
            (partition.peer_end as f64 / partition.count as f64).into(),
        )),
        RuntimeFunction::Ntile { .. } => {
            let value = partition
                .ntile
                .get(function_index)
                .and_then(Clone::clone)
                .ok_or_else(|| DbError::internal("ntile partition value is missing"))?;
            let Value::Integer(tiles) = value else {
                return Ok(Value::Null);
            };
            ntile_value(partition.emit_index, partition.count, tiles)
        }
        RuntimeFunction::Offset {
            value,
            default,
            cursor,
        } => {
            let Some(cursor_index) = cursor else {
                return Ok(Value::Null);
            };
            let (key, cursor) = partition
                .offsets
                .get_mut(*cursor_index)
                .ok_or_else(|| DbError::internal("window offset cursor is missing"))?;
            let target = match key.direction {
                OffsetDirection::Lag => partition.emit_index.checked_sub(key.amount),
                OffsetDirection::Lead => partition.emit_index.checked_add(key.amount),
            };
            let target = target.filter(|target| *target < partition.count);
            match target {
                Some(target) => {
                    let row = cursor.advance_to(target)?.ok_or_else(|| {
                        DbError::internal("window offset cursor ended inside partition")
                    })?;
                    eval_expr(ctx, value, &row.row)
                }
                None => default
                    .as_ref()
                    .map_or(Ok(Value::Null), |default| eval_expr(ctx, default, current)),
            }
        }
        RuntimeFunction::FirstValue { value } => evaluate_frame_value(
            ctx,
            &mut partition.frame_start,
            value,
            frame_start,
            frame_end,
        ),
        RuntimeFunction::LastValue { value } => {
            if frame_start >= frame_end {
                return Ok(Value::Null);
            }
            let target = frame_end
                .checked_sub(1)
                .ok_or_else(|| DbError::internal("window frame end underflow"))?;
            let row = partition
                .frame_end
                .advance_to(target)?
                .ok_or_else(|| DbError::internal("window frame-last cursor ended early"))?;
            eval_expr(ctx, value, &row.row)
        }
        RuntimeFunction::NthValue { value, n } => {
            let n = eval_expr(ctx, n, current)?;
            let Value::Integer(n) = n else {
                if matches!(n, Value::Null) {
                    return Ok(Value::Null);
                }
                return Err(DbError::internal("nth_value argument is not an integer"));
            };
            if n < 1 {
                return Err(DbError::execute(
                    SqlState::InvalidArgumentForNthValue,
                    "argument of nth_value must be greater than zero",
                ));
            }
            let offset = u64::try_from(n)
                .map_err(|_| DbError::internal("positive nth_value argument conversion failed"))?
                .checked_sub(1)
                .ok_or_else(|| DbError::internal("nth_value offset underflow"))?;
            let Some(target) = frame_start.checked_add(offset) else {
                return Ok(Value::Null);
            };
            if target >= frame_end {
                return Ok(Value::Null);
            }
            let mut lookup = partition.frame_start.clone();
            let row = lookup
                .advance_to(target)?
                .ok_or_else(|| DbError::internal("nth_value cursor ended inside frame"))?;
            eval_expr(ctx, value, &row.row)
        }
        RuntimeFunction::Aggregate {
            aggregate,
            mode,
            collect,
            cache_peers,
        } => {
            let frame_cursor = partition.frame_start.clone();
            let runtime = partition
                .aggregates
                .get_mut(function_index)
                .and_then(Option::as_mut)
                .ok_or_else(|| DbError::internal("window aggregate runtime is missing"))?;
            evaluate_window_aggregate(
                ctx,
                aggregate,
                *mode,
                *collect,
                *cache_peers,
                runtime,
                frame_cursor,
                frame_start,
                frame_end,
                partition.peer_end,
                spill_ctx,
            )
        }
    }
}

fn evaluate_frame_value(
    ctx: &StatementContext,
    cursor: &mut TapeCursor,
    value: &BoundExpr,
    frame_start: u64,
    frame_end: u64,
) -> Result<Value> {
    if frame_start >= frame_end {
        return Ok(Value::Null);
    }
    let row = cursor
        .advance_to(frame_start)?
        .ok_or_else(|| DbError::internal("window frame-value cursor ended early"))?;
    eval_expr(ctx, value, &row.row)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_window_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    mode: AggregateMode,
    collect: bool,
    cache_peers: bool,
    runtime: &mut AggregateRuntime,
    frame_cursor: TapeCursor,
    frame_start: u64,
    frame_end: u64,
    peer_end: u64,
    spill_ctx: &SpillContext,
) -> Result<Value> {
    if collect {
        return recompute_aggregate(
            ctx,
            aggregate,
            frame_cursor,
            frame_start,
            frame_end,
            spill_ctx,
        );
    }
    match mode {
        AggregateMode::WholePartition => {
            if let Some(value) = &runtime.cached_value {
                return Ok(value.clone());
            }
            feed_state_range(
                ctx,
                aggregate,
                runtime
                    .state
                    .as_mut()
                    .ok_or_else(|| DbError::internal("whole-partition state is missing"))?,
                &mut runtime.feed,
                runtime.fed,
                frame_end,
            )?;
            runtime.fed = frame_end;
            let value = runtime
                .state
                .as_ref()
                .ok_or_else(|| DbError::internal("whole-partition state is missing"))?
                .snapshot()?;
            runtime.cached_value = Some(value.clone());
            Ok(value)
        }
        AggregateMode::Growing => {
            if cache_peers
                && runtime.cached_peer_end == Some(peer_end)
                && let Some(value) = &runtime.cached_value
            {
                return Ok(value.clone());
            }
            feed_state_range(
                ctx,
                aggregate,
                runtime
                    .state
                    .as_mut()
                    .ok_or_else(|| DbError::internal("growing aggregate state is missing"))?,
                &mut runtime.feed,
                runtime.fed,
                frame_end,
            )?;
            runtime.fed = frame_end;
            let value = runtime
                .state
                .as_ref()
                .ok_or_else(|| DbError::internal("growing aggregate state is missing"))?
                .snapshot()?;
            runtime.cached_peer_end = Some(peer_end);
            runtime.cached_value = Some(value.clone());
            Ok(value)
        }
        AggregateMode::Sliding => {
            if runtime.state.is_none() || runtime.frame_start != frame_start {
                runtime.state = Some(AggregateState::new(aggregate, spill_ctx.clone())?);
                runtime.feed = frame_cursor;
                runtime.fed = frame_start;
                runtime.frame_start = frame_start;
            }
            feed_state_range(
                ctx,
                aggregate,
                runtime
                    .state
                    .as_mut()
                    .ok_or_else(|| DbError::internal("sliding aggregate state is missing"))?,
                &mut runtime.feed,
                runtime.fed,
                frame_end,
            )?;
            runtime.fed = frame_end;
            runtime
                .state
                .as_ref()
                .ok_or_else(|| DbError::internal("sliding aggregate state is missing"))?
                .snapshot()
        }
    }
}

fn recompute_aggregate(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    mut cursor: TapeCursor,
    frame_start: u64,
    frame_end: u64,
    spill_ctx: &SpillContext,
) -> Result<Value> {
    let mut state = AggregateState::new(aggregate, spill_ctx.clone())?;
    feed_state_range(
        ctx,
        aggregate,
        &mut state,
        &mut cursor,
        frame_start,
        frame_end,
    )?;
    state.finish()
}

fn feed_state_range(
    ctx: &StatementContext,
    aggregate: &AggregateExpr,
    state: &mut AggregateState,
    cursor: &mut TapeCursor,
    mut index: u64,
    end: u64,
) -> Result<()> {
    while index < end {
        ctx.cancel.check()?;
        let row = cursor
            .advance_to(index)?
            .ok_or_else(|| DbError::internal("window aggregate cursor ended inside frame"))?;
        let value = aggregate
            .arg
            .as_ref()
            .map(|arg| eval_expr(ctx, arg, &row.row))
            .transpose()?;
        state.step(value.as_ref())?;
        index = index
            .checked_add(1)
            .ok_or_else(|| DbError::internal("window aggregate feed index overflow"))?;
    }
    Ok(())
}

fn ntile_value(index: u64, count: u64, tiles: i64) -> Result<Value> {
    let tiles = u64::try_from(tiles)
        .map_err(|_| DbError::internal("validated ntile argument became negative"))?;
    let base = count / tiles;
    let larger_tiles = count % tiles;
    let larger_size = base
        .checked_add(1)
        .ok_or_else(|| DbError::internal("ntile bucket size overflow"))?;
    let larger_rows = larger_size
        .checked_mul(larger_tiles)
        .ok_or_else(|| DbError::internal("ntile row boundary overflow"))?;
    let tile = if base == 0 {
        index
    } else if index < larger_rows {
        index / larger_size
    } else {
        let tail = index
            .checked_sub(larger_rows)
            .ok_or_else(|| DbError::internal("ntile tail underflow"))?;
        larger_tiles
            .checked_add(tail / base)
            .ok_or_else(|| DbError::internal("ntile bucket overflow"))?
    };
    integer_value(
        tile.checked_add(1)
            .ok_or_else(|| DbError::internal("ntile result overflow"))?,
        "ntile",
    )
}

fn integer_value(value: u64, name: &str) -> Result<Value> {
    i64::try_from(value)
        .map(Value::Integer)
        .map_err(|_| DbError::internal(format!("{name} result exceeds INTEGER range")))
}

fn key_prefix(keys: &[Value], count: usize) -> Result<&[Value]> {
    keys.get(..count)
        .ok_or_else(|| DbError::internal("window sort row has too few partition keys"))
}

fn key_suffix(keys: &[Value], start: usize, count: usize) -> Result<&[Value]> {
    let end = start
        .checked_add(count)
        .ok_or_else(|| DbError::internal("window key width overflow"))?;
    keys.get(start..end)
        .ok_or_else(|| DbError::internal("window sort row has too few order keys"))
}

fn validate_row_width(row: &ExecRow, expected: usize) -> Result<()> {
    if row.row.values.len() == expected {
        Ok(())
    } else {
        Err(DbError::internal(format!(
            "window source row width mismatch: expected {expected}, got {}",
            row.row.values.len()
        )))
    }
}

fn function_name(function: WindowFunc) -> &'static str {
    match function {
        WindowFunc::RowNumber => "row_number",
        WindowFunc::Rank => "rank",
        WindowFunc::DenseRank => "dense_rank",
        WindowFunc::Ntile => "ntile",
        WindowFunc::PercentRank => "percent_rank",
        WindowFunc::CumeDist => "cume_dist",
        WindowFunc::Lag => "lag",
        WindowFunc::Lead => "lead",
        WindowFunc::FirstValue => "first_value",
        WindowFunc::LastValue => "last_value",
        WindowFunc::NthValue => "nth_value",
        WindowFunc::Aggregate(_) => "aggregate",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common::{CancelReason, DataType, Key, RowId, RowIdentity};
    use planner::{BoundFrameBound, BoundWindowFrame, WindowFrameUnits};

    use super::*;

    struct RowsOp {
        rows: VecDeque<ExecRow>,
        schema: Vec<ColumnInfo>,
        closes: Arc<AtomicUsize>,
        fail_open: bool,
        fail_next: bool,
        fail_close: bool,
    }

    impl PlanExecutor for RowsOp {
        fn output_schema(&self) -> &[ColumnInfo] {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            if self.fail_open {
                Err(DbError::internal("test open failure"))
            } else {
                Ok(())
            }
        }

        fn next(&mut self) -> Result<Option<ExecRow>> {
            if self.fail_next {
                self.fail_next = false;
                Err(DbError::internal("test next failure"))
            } else {
                Ok(self.rows.pop_front())
            }
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

    fn column(name: &str, data_type: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            table_id: None,
            column_id: None,
            pg_type: None,
        }
    }

    fn input(slot: usize, data_type: DataType) -> BoundExpr {
        BoundExpr::LocalRef {
            slot,
            data_type,
            nullable: true,
        }
    }

    fn literal(value: Value, data_type: DataType) -> BoundExpr {
        let nullable = matches!(value, Value::Null);
        BoundExpr::Literal {
            value,
            data_type,
            nullable,
        }
    }

    fn order(slot: usize) -> BoundOrderByItem {
        BoundOrderByItem {
            expr: input(slot, DataType::Integer),
            ascending: true,
            nulls_first: Some(false),
        }
    }

    fn spec(partition: Vec<BoundExpr>, order_by: Vec<BoundOrderByItem>) -> BoundWindowSpec {
        BoundWindowSpec {
            partition_by: partition,
            order_by,
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Range,
                start: BoundFrameBound::UnboundedPreceding,
                end: BoundFrameBound::CurrentRow,
            },
        }
    }

    fn function(
        func: WindowFunc,
        args: Vec<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    ) -> WindowFuncExpr {
        WindowFuncExpr {
            func,
            args,
            data_type,
            nullable,
        }
    }

    fn rows(values: Vec<Vec<Value>>) -> VecDeque<ExecRow> {
        values
            .into_iter()
            .enumerate()
            .map(|(index, values)| ExecRow {
                row: Row { values },
                identity: Some(RowIdentity {
                    row_id: RowId {
                        page_num: 1,
                        slot_num: u16::try_from(index).unwrap(),
                    },
                    xmin: 1,
                    key: Key(vec![Value::Integer(i64::try_from(index).unwrap())]),
                }),
            })
            .collect()
    }

    fn source(values: Vec<Vec<Value>>, closes: Arc<AtomicUsize>) -> RowsOp {
        RowsOp {
            rows: rows(values),
            schema: vec![
                column("p", DataType::Integer),
                column("k", DataType::Integer),
                column("v", DataType::Text),
            ],
            closes,
            fail_open: false,
            fail_next: false,
            fail_close: false,
        }
    }

    fn run(
        values: Vec<Vec<Value>>,
        spec: BoundWindowSpec,
        functions: Vec<WindowFuncExpr>,
        spill: SpillConfig,
    ) -> (Vec<ExecRow>, u64) {
        let closes = Arc::new(AtomicUsize::new(0));
        let stats = spill.stats.clone();
        let mut op = WindowOp::new(
            StatementContext::new(1),
            Box::new(source(values, closes.clone())),
            spec,
            functions,
            spill,
        )
        .unwrap();
        op.open().unwrap();
        let mut output = Vec::new();
        while let Some(row) = op.next().unwrap() {
            output.push(row);
        }
        op.close().unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        (output, stats.files_created())
    }

    fn output_values(rows: &[ExecRow]) -> Vec<Vec<Value>> {
        rows.iter().map(|row| row.row.values.clone()).collect()
    }

    #[test]
    fn ranking_ties_null_peers_and_partition_resets_preserve_identity_and_order() {
        let functions = vec![
            function(WindowFunc::RowNumber, vec![], DataType::Integer, false),
            function(WindowFunc::Rank, vec![], DataType::Integer, false),
            function(WindowFunc::DenseRank, vec![], DataType::Integer, false),
        ];
        let input_rows = vec![
            vec![Value::Integer(2), Value::Null, Value::Text("e".into())],
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Text("c".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("a".into()),
            ],
            vec![
                Value::Integer(2),
                Value::Integer(1),
                Value::Text("d".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("b".into()),
            ],
            vec![Value::Integer(2), Value::Null, Value::Text("f".into())],
        ];
        let (output, _) = run(
            input_rows,
            spec(vec![input(0, DataType::Integer)], vec![order(1)]),
            functions,
            SpillConfig::default(),
        );
        assert!(output.iter().all(|row| row.identity.is_some()));
        let values = output_values(&output);
        assert_eq!(
            values.iter().map(|row| &row[2]).collect::<Vec<_>>(),
            vec![
                &Value::Text("a".into()),
                &Value::Text("b".into()),
                &Value::Text("c".into()),
                &Value::Text("d".into()),
                &Value::Text("e".into()),
                &Value::Text("f".into()),
            ]
        );
        assert_eq!(
            values
                .iter()
                .map(|row| row[3..].to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Integer(1), Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(3), Value::Integer(3), Value::Integer(2)],
                vec![Value::Integer(1), Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(2), Value::Integer(2)],
                vec![Value::Integer(3), Value::Integer(2), Value::Integer(2)],
            ]
        );
    }

    #[test]
    fn distribution_handles_single_row_and_no_order_by_peer_group() {
        let functions = vec![
            function(WindowFunc::PercentRank, vec![], DataType::Double, false),
            function(WindowFunc::CumeDist, vec![], DataType::Double, false),
        ];
        let (single, _) = run(
            vec![vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("x".into()),
            ]],
            spec(vec![], vec![order(1)]),
            functions.clone(),
            SpillConfig::default(),
        );
        assert_eq!(single[0].row.values[3], Value::Float(0.0.into()));
        assert_eq!(single[0].row.values[4], Value::Float(1.0.into()));

        let (peers, _) = run(
            vec![
                vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Text("a".into()),
                ],
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("b".into()),
                ],
            ],
            spec(vec![], vec![]),
            functions,
            SpillConfig::default(),
        );
        assert!(peers.iter().all(|row| {
            row.row.values[3] == Value::Float(0.0.into())
                && row.row.values[4] == Value::Float(1.0.into())
        }));
    }

    #[test]
    fn ntile_covers_uneven_more_tiles_null_and_nonpositive() {
        let values = (0..10)
            .map(|value| {
                vec![
                    Value::Integer(1),
                    Value::Integer(value),
                    Value::Text("x".into()),
                ]
            })
            .collect();
        let ntile = |value| {
            function(
                WindowFunc::Ntile,
                vec![literal(value, DataType::Integer)],
                DataType::Integer,
                false,
            )
        };
        let (output, _) = run(
            values,
            spec(vec![], vec![order(1)]),
            vec![ntile(Value::Integer(3))],
            SpillConfig::default(),
        );
        assert_eq!(
            output
                .iter()
                .map(|row| row.row.values[3].clone())
                .collect::<Vec<_>>(),
            vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 3]
                .into_iter()
                .map(Value::Integer)
                .collect::<Vec<_>>()
        );
        let (more, _) = run(
            vec![
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("a".into()),
                ],
                vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Text("b".into()),
                ],
            ],
            spec(vec![], vec![order(1)]),
            vec![ntile(Value::Integer(5)), ntile(Value::Null)],
            SpillConfig::default(),
        );
        assert_eq!(more[0].row.values[3..], [Value::Integer(1), Value::Null]);
        assert_eq!(more[1].row.values[3..], [Value::Integer(2), Value::Null]);

        let closes = Arc::new(AtomicUsize::new(0));
        let mut op = WindowOp::new(
            StatementContext::new(1),
            Box::new(source(
                vec![vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("a".into()),
                ]],
                closes,
            )),
            spec(vec![], vec![]),
            vec![ntile(Value::Integer(0))],
            SpillConfig::default(),
        )
        .unwrap();
        op.open().unwrap();
        let error = op.next().unwrap_err();
        assert_eq!(error.code, SqlState::InvalidArgumentForNtile);
        assert_eq!(error.message, "argument of ntile must be greater than zero");
    }

    #[test]
    fn lag_lead_offsets_defaults_null_negative_and_partition_edges() {
        let value = input(1, DataType::Integer);
        let default_current = input(1, DataType::Integer);
        let offset = |func, amount: Option<Value>, default: Option<BoundExpr>| {
            let mut args = vec![value.clone()];
            if let Some(amount) = amount {
                args.push(literal(amount, DataType::Integer));
            }
            if let Some(default) = default {
                args.push(default);
            }
            function(func, args, DataType::Integer, true)
        };
        let functions = vec![
            offset(WindowFunc::Lag, None, None),
            offset(WindowFunc::Lead, Some(Value::Integer(2)), None),
            offset(
                WindowFunc::Lag,
                Some(Value::Integer(3)),
                Some(default_current),
            ),
            offset(WindowFunc::Lead, Some(Value::Null), None),
            offset(WindowFunc::Lag, Some(Value::Integer(-1)), None),
            offset(WindowFunc::Lead, Some(Value::Integer(-2)), None),
        ];
        let (output, _) = run(
            (1..=4)
                .map(|value| {
                    vec![
                        Value::Integer(1),
                        Value::Integer(value),
                        Value::Text("x".into()),
                    ]
                })
                .collect(),
            spec(vec![], vec![order(1)]),
            functions,
            SpillConfig::default(),
        );
        assert_eq!(
            output
                .iter()
                .map(|row| row.row.values[3..].to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    Value::Null,
                    Value::Integer(3),
                    Value::Integer(1),
                    Value::Null,
                    Value::Integer(2),
                    Value::Null
                ],
                vec![
                    Value::Integer(1),
                    Value::Integer(4),
                    Value::Integer(2),
                    Value::Null,
                    Value::Integer(3),
                    Value::Null
                ],
                vec![
                    Value::Integer(2),
                    Value::Null,
                    Value::Integer(3),
                    Value::Null,
                    Value::Integer(4),
                    Value::Integer(1)
                ],
                vec![
                    Value::Integer(3),
                    Value::Null,
                    Value::Integer(1),
                    Value::Null,
                    Value::Null,
                    Value::Integer(2)
                ],
            ]
        );
    }

    #[test]
    fn forced_spill_matches_large_memory_and_creates_files() {
        let padded = "x".repeat(2_000);
        let values = (0..40)
            .map(|value| {
                vec![
                    Value::Integer(value % 2),
                    Value::Integer(value),
                    Value::Text(format!("{value}{padded}")),
                ]
            })
            .collect::<Vec<_>>();
        let functions = vec![
            function(WindowFunc::Rank, vec![], DataType::Integer, false),
            function(
                WindowFunc::Lag,
                vec![input(2, DataType::Text)],
                DataType::Text,
                true,
            ),
            function(
                WindowFunc::Aggregate(AggregateFunc::Sum),
                vec![input(1, DataType::Integer)],
                DataType::Integer,
                true,
            ),
        ];
        let window_spec = BoundWindowSpec {
            partition_by: vec![input(0, DataType::Integer)],
            order_by: vec![order(1)],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Rows,
                start: BoundFrameBound::PrecedingRows(2),
                end: BoundFrameBound::FollowingRows(2),
            },
        };
        let (large, _) = run(
            values.clone(),
            window_spec.clone(),
            functions.clone(),
            SpillConfig::default(),
        );
        let (spilled, files) = run(
            values,
            window_spec,
            functions,
            SpillConfig::new(spill::MIN_WORK_MEM_BYTES, std::env::temp_dir()),
        );
        assert_eq!(output_values(&spilled), output_values(&large));
        assert!(files > 0);
    }

    #[test]
    fn child_failures_close_once_and_cancellation_is_polled_in_both_phases() {
        for (fail_open, fail_next, fail_close) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let closes = Arc::new(AtomicUsize::new(0));
            let mut child = source(
                vec![vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("a".into()),
                ]],
                closes.clone(),
            );
            child.fail_open = fail_open;
            child.fail_next = fail_next;
            child.fail_close = fail_close;
            let mut op = WindowOp::new(
                StatementContext::new(1),
                Box::new(child),
                spec(vec![], vec![]),
                vec![function(
                    WindowFunc::RowNumber,
                    vec![],
                    DataType::Integer,
                    false,
                )],
                SpillConfig::default(),
            )
            .unwrap();
            assert!(op.open().is_err());
            assert_eq!(closes.load(Ordering::SeqCst), 1);
        }

        let phase_a = StatementContext::new(1);
        phase_a.cancel.request(CancelReason::UserRequest);
        let closes = Arc::new(AtomicUsize::new(0));
        let mut op = WindowOp::new(
            phase_a,
            Box::new(source(
                vec![vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("a".into()),
                ]],
                closes.clone(),
            )),
            spec(vec![], vec![]),
            vec![function(
                WindowFunc::RowNumber,
                vec![],
                DataType::Integer,
                false,
            )],
            SpillConfig::default(),
        )
        .unwrap();
        assert_eq!(op.open().unwrap_err().code, SqlState::QueryCanceled);
        assert_eq!(closes.load(Ordering::SeqCst), 1);

        let phase_b = StatementContext::new(1);
        let cancel = phase_b.cancel.clone();
        let mut op = WindowOp::new(
            phase_b,
            Box::new(source(
                vec![vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("a".into()),
                ]],
                Arc::new(AtomicUsize::new(0)),
            )),
            spec(vec![], vec![]),
            vec![function(
                WindowFunc::RowNumber,
                vec![],
                DataType::Integer,
                false,
            )],
            SpillConfig::default(),
        )
        .unwrap();
        op.open().unwrap();
        cancel.request(CancelReason::UserRequest);
        assert_eq!(op.next().unwrap_err().code, SqlState::QueryCanceled);
    }

    #[test]
    fn malformed_source_width_is_an_internal_error_and_closes_child_once() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut malformed = source(vec![], closes.clone());
        malformed.rows = rows(vec![vec![Value::Integer(1), Value::Integer(2)]]);
        let mut op = WindowOp::new(
            StatementContext::new(1),
            Box::new(malformed),
            spec(vec![], vec![]),
            vec![function(
                WindowFunc::RowNumber,
                vec![],
                DataType::Integer,
                false,
            )],
            SpillConfig::default(),
        )
        .unwrap();
        let error = op.open().unwrap_err();
        assert_eq!(error.code, SqlState::InternalError);
        assert!(error.message.contains("window source row width mismatch"));
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn default_range_frame_includes_peers_for_values_and_running_sum() {
        let input_rows = vec![
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("a".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("b".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(3),
                Value::Text("c".into()),
            ],
        ];
        let functions = vec![
            function(
                WindowFunc::FirstValue,
                vec![input(2, DataType::Text)],
                DataType::Text,
                true,
            ),
            function(
                WindowFunc::LastValue,
                vec![input(2, DataType::Text)],
                DataType::Text,
                true,
            ),
            function(
                WindowFunc::NthValue,
                vec![
                    input(2, DataType::Text),
                    literal(Value::Integer(2), DataType::Integer),
                ],
                DataType::Text,
                true,
            ),
            function(
                WindowFunc::Aggregate(AggregateFunc::Sum),
                vec![input(1, DataType::Integer)],
                DataType::Integer,
                true,
            ),
        ];
        let (output, _) = run(
            input_rows,
            spec(vec![], vec![order(1)]),
            functions,
            SpillConfig::default(),
        );
        assert_eq!(
            output
                .iter()
                .map(|row| row.row.values[3..].to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Text("b".into()),
                    Value::Text("b".into()),
                    Value::Integer(2),
                ],
                vec![
                    Value::Text("a".into()),
                    Value::Text("b".into()),
                    Value::Text("b".into()),
                    Value::Integer(2),
                ],
                vec![
                    Value::Text("a".into()),
                    Value::Text("c".into()),
                    Value::Text("b".into()),
                    Value::Integer(5),
                ],
            ]
        );
    }

    #[test]
    fn default_range_frame_null_key_includes_partition_prefix() {
        let string_agg_args = BoundExpr::Array {
            elements: vec![
                input(2, DataType::Text),
                literal(Value::Text(",".into()), DataType::Text),
            ],
            dimensions: vec![2],
            element_type: DataType::Text,
            data_type: DataType::Array(common::ArrayType::new(DataType::Text).unwrap()),
            nullable: false,
        };
        let (output, _) = run(
            vec![
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("1".into()),
                ],
                vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Text("2".into()),
                ],
                vec![Value::Integer(1), Value::Null, Value::Null],
            ],
            spec(vec![], vec![order(1)]),
            vec![
                function(
                    WindowFunc::FirstValue,
                    vec![input(1, DataType::Integer)],
                    DataType::Integer,
                    true,
                ),
                function(
                    WindowFunc::Aggregate(AggregateFunc::StringAgg),
                    vec![string_agg_args],
                    DataType::Text,
                    true,
                ),
            ],
            SpillConfig::default(),
        );
        assert_eq!(
            output
                .iter()
                .map(|row| row.row.values[3..].to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Integer(1), Value::Text("1".into())],
                vec![Value::Integer(1), Value::Text("1,2".into())],
                vec![Value::Integer(1), Value::Text("1,2".into())],
            ]
        );
    }

    #[test]
    fn whole_range_frame_null_key_uses_unbounded_bounds() {
        let window_spec = BoundWindowSpec {
            partition_by: vec![],
            order_by: vec![BoundOrderByItem {
                expr: input(1, DataType::Integer),
                ascending: false,
                nulls_first: Some(true),
            }],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Range,
                start: BoundFrameBound::UnboundedPreceding,
                end: BoundFrameBound::UnboundedFollowing,
            },
        };
        let (output, _) = run(
            vec![
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("one".into()),
                ],
                vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Text("two".into()),
                ],
                vec![Value::Integer(1), Value::Null, Value::Text("null".into())],
            ],
            window_spec,
            vec![
                function(
                    WindowFunc::Aggregate(AggregateFunc::Sum),
                    vec![input(1, DataType::Integer)],
                    DataType::Integer,
                    true,
                ),
                function(
                    WindowFunc::LastValue,
                    vec![input(2, DataType::Text)],
                    DataType::Text,
                    true,
                ),
            ],
            SpillConfig::default(),
        );
        assert_eq!(
            output
                .iter()
                .map(|row| row.row.values[3..].to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Integer(3), Value::Text("one".into())],
                vec![Value::Integer(3), Value::Text("one".into())],
                vec![Value::Integer(3), Value::Text("one".into())],
            ]
        );
    }

    #[test]
    fn rows_moving_and_empty_frames_and_nth_error() {
        let moving_spec = BoundWindowSpec {
            partition_by: vec![],
            order_by: vec![order(1)],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Rows,
                start: BoundFrameBound::PrecedingRows(1),
                end: BoundFrameBound::FollowingRows(1),
            },
        };
        let sum = function(
            WindowFunc::Aggregate(AggregateFunc::Sum),
            vec![input(1, DataType::Integer)],
            DataType::Integer,
            true,
        );
        let values = (1..=3)
            .map(|value| {
                vec![
                    Value::Integer(1),
                    Value::Integer(value),
                    Value::Text("x".into()),
                ]
            })
            .collect();
        let (moving, _) = run(
            values,
            moving_spec,
            vec![sum.clone()],
            SpillConfig::default(),
        );
        assert_eq!(
            moving
                .iter()
                .map(|row| row.row.values[3].clone())
                .collect::<Vec<_>>(),
            vec![Value::Integer(3), Value::Integer(6), Value::Integer(5)]
        );

        let empty_spec = BoundWindowSpec {
            partition_by: vec![],
            order_by: vec![order(1)],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Rows,
                start: BoundFrameBound::FollowingRows(2),
                end: BoundFrameBound::FollowingRows(3),
            },
        };
        let (empty, _) = run(
            vec![vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("x".into()),
            ]],
            empty_spec,
            vec![
                sum,
                function(
                    WindowFunc::Aggregate(AggregateFunc::Count),
                    vec![],
                    DataType::Integer,
                    false,
                ),
            ],
            SpillConfig::default(),
        );
        assert_eq!(empty[0].row.values[3..], [Value::Null, Value::Integer(0)]);

        let closes = Arc::new(AtomicUsize::new(0));
        let mut op = WindowOp::new(
            StatementContext::new(1),
            Box::new(source(
                vec![vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("x".into()),
                ]],
                closes,
            )),
            spec(vec![], vec![order(1)]),
            vec![function(
                WindowFunc::NthValue,
                vec![
                    input(2, DataType::Text),
                    literal(Value::Integer(0), DataType::Integer),
                ],
                DataType::Text,
                true,
            )],
            SpillConfig::default(),
        )
        .unwrap();
        op.open().unwrap();
        assert_eq!(
            op.next().unwrap_err().code,
            SqlState::InvalidArgumentForNthValue
        );
    }

    #[test]
    fn range_integer_offsets_follow_sort_direction_null_peers_and_overflow_clamps() {
        let range_spec = |ascending| BoundWindowSpec {
            partition_by: vec![],
            order_by: vec![BoundOrderByItem {
                expr: input(1, DataType::Integer),
                ascending,
                nulls_first: Some(false),
            }],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Range,
                start: BoundFrameBound::PrecedingRange(Value::Integer(1)),
                end: BoundFrameBound::CurrentRow,
            },
        };
        let sum = function(
            WindowFunc::Aggregate(AggregateFunc::Sum),
            vec![input(1, DataType::Integer)],
            DataType::Integer,
            true,
        );
        let values = vec![
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("a".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Text("b".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(4),
                Value::Text("c".into()),
            ],
            vec![Value::Integer(1), Value::Null, Value::Text("d".into())],
        ];
        let (ascending, _) = run(
            values.clone(),
            range_spec(true),
            vec![sum.clone()],
            SpillConfig::default(),
        );
        assert_eq!(
            ascending
                .iter()
                .map(|row| row.row.values[3].clone())
                .collect::<Vec<_>>(),
            vec![
                Value::Integer(1),
                Value::Integer(3),
                Value::Integer(4),
                Value::Null
            ]
        );
        let (descending, _) = run(values, range_spec(false), vec![sum], SpillConfig::default());
        assert_eq!(
            descending
                .iter()
                .map(|row| row.row.values[3].clone())
                .collect::<Vec<_>>(),
            vec![
                Value::Integer(4),
                Value::Integer(2),
                Value::Integer(3),
                Value::Null
            ]
        );

        let overflow_spec = BoundWindowSpec {
            partition_by: vec![],
            order_by: vec![order(1)],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Range,
                start: BoundFrameBound::CurrentRow,
                end: BoundFrameBound::FollowingRange(Value::Integer(1)),
            },
        };
        let (overflow, _) = run(
            vec![vec![
                Value::Integer(1),
                Value::Integer(i64::MAX),
                Value::Text("x".into()),
            ]],
            overflow_spec,
            vec![function(
                WindowFunc::Aggregate(AggregateFunc::Count),
                vec![],
                DataType::Integer,
                false,
            )],
            SpillConfig::default(),
        );
        assert_eq!(overflow[0].row.values[3], Value::Integer(1));
    }

    #[test]
    fn sliding_min_max_recompute_values_that_leave_the_frame() {
        let moving_spec = BoundWindowSpec {
            partition_by: vec![],
            order_by: vec![order(1)],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Rows,
                start: BoundFrameBound::PrecedingRows(1),
                end: BoundFrameBound::CurrentRow,
            },
        };
        let values = vec![
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("c".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Text("a".into()),
            ],
            vec![
                Value::Integer(1),
                Value::Integer(3),
                Value::Text("b".into()),
            ],
        ];
        let (output, _) = run(
            values,
            moving_spec,
            vec![
                function(
                    WindowFunc::Aggregate(AggregateFunc::Min),
                    vec![input(2, DataType::Text)],
                    DataType::Text,
                    true,
                ),
                function(
                    WindowFunc::Aggregate(AggregateFunc::Max),
                    vec![input(2, DataType::Text)],
                    DataType::Text,
                    true,
                ),
            ],
            SpillConfig::default(),
        );
        assert_eq!(
            output
                .iter()
                .map(|row| row.row.values[3..].to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Text("c".into()), Value::Text("c".into())],
                vec![Value::Text("a".into()), Value::Text("c".into())],
                vec![Value::Text("a".into()), Value::Text("b".into())],
            ]
        );
    }

    #[test]
    fn frame_boundary_helpers_cover_zero_huge_offsets_and_typed_range_thresholds() {
        assert_eq!(
            rows_bound(&BoundFrameBound::PrecedingRows(0), 2, 4, false).unwrap(),
            2
        );
        assert_eq!(
            rows_bound(&BoundFrameBound::PrecedingRows(u64::MAX), 2, 4, false).unwrap(),
            0
        );
        assert_eq!(
            rows_bound(&BoundFrameBound::FollowingRows(u64::MAX), 2, 4, true).unwrap(),
            4
        );

        let numeric = Decimal::new(25, 1);
        let offset = Decimal::new(5, 1);
        assert!(matches!(
            range_threshold(&Value::Numeric(numeric), &Value::Numeric(offset), false).unwrap(),
            RangeThreshold::Value(Value::Numeric(value)) if value == Decimal::new(20, 1)
        ));
        let interval = common::Interval::new(0, 1, 0);
        assert!(matches!(
            range_threshold(
                &Value::Timestamp(172_800_000_000),
                &Value::Interval(interval),
                false,
            )
            .unwrap(),
            RangeThreshold::Value(Value::Timestamp(86_400_000_000))
        ));
        let date_threshold = range_threshold(
            &Value::Date(2),
            &Value::Interval(common::Interval::new(0, 1, 0)),
            false,
        )
        .unwrap();
        assert!(matches!(
            date_threshold,
            RangeThreshold::Value(Value::Timestamp(86_400_000_000))
        ));
    }

    #[test]
    fn range_nulls_first_are_excluded_and_null_row_uses_its_peer_group() {
        let window_spec = BoundWindowSpec {
            partition_by: vec![],
            order_by: vec![BoundOrderByItem {
                expr: input(1, DataType::Integer),
                ascending: true,
                nulls_first: Some(true),
            }],
            frame: BoundWindowFrame {
                units: WindowFrameUnits::Range,
                start: BoundFrameBound::PrecedingRange(Value::Integer(1)),
                end: BoundFrameBound::CurrentRow,
            },
        };
        let (output, _) = run(
            vec![
                vec![Value::Integer(1), Value::Null, Value::Text("a".into())],
                vec![Value::Integer(1), Value::Null, Value::Text("b".into())],
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Text("c".into()),
                ],
                vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Text("d".into()),
                ],
            ],
            window_spec,
            vec![function(
                WindowFunc::Aggregate(AggregateFunc::Count),
                vec![],
                DataType::Integer,
                false,
            )],
            SpillConfig::default(),
        );
        assert_eq!(
            output
                .iter()
                .map(|row| row.row.values[3].clone())
                .collect::<Vec<_>>(),
            vec![
                Value::Integer(2),
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(2),
            ]
        );
    }
}
