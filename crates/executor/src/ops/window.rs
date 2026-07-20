use common::{ColumnInfo, DbError, ExecRow, Result, Row, SqlState, StatementContext, Value};
use planner::{BoundExpr, BoundOrderByItem, BoundWindowSpec, WindowFunc, WindowFuncExpr};
use spill::{ExternalSorter, SortedStream, SpillConfig, SpillContext, SpillTape, SpillTapeReader};

use crate::eval_expr;
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
    count: u64,
    emit_index: u64,
    peer_start: u64,
    peer_end: u64,
    dense_rank: u64,
    ntile: Vec<Option<Value>>,
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
            runtime.push(runtime_function(function, &mut offset_keys)?);
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
        let mut offsets = Vec::new();
        for key in &self.offset_keys {
            offsets.push((*key, TapeCursor::new(tape.reader()?)));
        }
        let mut ntile = vec![None; self.functions.len()];
        for (index, function) in self.functions.iter().enumerate() {
            if let RuntimeFunction::Ntile { arg } = function {
                let value = eval_expr(&self.ctx, arg, &first_row)?;
                ntile[index] = Some(validate_ntile(value)?);
            }
        }
        self.partition = Some(PartitionState {
            _tape: tape,
            current,
            peer_probe,
            offsets,
            count,
            emit_index: 0,
            peer_start: 0,
            peer_end: 0,
            dense_rank: 0,
            ntile,
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
        WindowFunc::FirstValue
        | WindowFunc::LastValue
        | WindowFunc::NthValue
        | WindowFunc::Aggregate(_) => {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "window frames are not yet implemented",
            ));
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

fn evaluate_function(
    ctx: &StatementContext,
    function: &RuntimeFunction,
    function_index: usize,
    partition: &mut PartitionState,
    current: &ExecRow,
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
    }
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
        ];
        let window_spec = spec(vec![input(0, DataType::Integer)], vec![order(1)]);
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
    fn frame_respecting_functions_are_staged() {
        let error = WindowOp::new(
            StatementContext::new(1),
            Box::new(source(vec![], Arc::new(AtomicUsize::new(0)))),
            spec(vec![], vec![]),
            vec![function(
                WindowFunc::FirstValue,
                vec![input(1, DataType::Integer)],
                DataType::Integer,
                true,
            )],
            SpillConfig::default(),
        )
        .err()
        .expect("first_value is staged until M5");
        assert_eq!(error.code, SqlState::FeatureNotSupported);
        assert_eq!(error.message, "window frames are not yet implemented");
    }
}
