use std::cmp::Ordering;

use common::{ColumnInfo, DbError, ExecRow, Result, Row, StatementContext, Value};
use planner::{BoundExpr, JoinSide, JoinType};
use spill::{
    ExternalSorter, Reservation, RetainedSize, SortedStream, SpillConfig, SpillTape,
    SpillTapeReader,
};

use crate::ops::predicate_matches;
use crate::ops::spill_row::SpillRow;
use crate::query::{PlanExecutor, close_after, open_executor};

type OrdinalSorter = ExternalSorter<SpillRow, Box<dyn Fn(&SpillRow, &SpillRow) -> Ordering>>;

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
    spill: SpillConfig,
    left_tape: Option<SpillTape<ExecRow>>,
    right_tape: Option<SpillTape<ExecRow>>,
    left_reader: Option<SpillTapeReader<ExecRow>>,
    right_reader: Option<SpillTapeReader<ExecRow>>,
    current_left: Option<ExecRow>,
    matched: bool,
    right_position: u64,
    unmatched_position: u64,
    matched_sorter: Option<OrdinalSorter>,
    matched_stream: Option<SortedStream<SpillRow>>,
    matched_next: Option<u64>,
    phase: NestedPhase,
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
        spill: SpillConfig,
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
            spill,
            left_tape: None,
            right_tape: None,
            left_reader: None,
            right_reader: None,
            current_left: None,
            matched: false,
            right_position: 0,
            unmatched_position: 0,
            matched_sorter: None,
            matched_stream: None,
            matched_next: None,
            phase: NestedPhase::Done,
            left_width,
            right_width,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NestedPhase {
    Main,
    UnmatchedRight,
    Done,
}

impl PlanExecutor for NestedLoopJoinOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.reset();
        let spill_ctx = self.spill.for_operator(self.ctx.cancel.clone());
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.matched_sorter = Some(ExternalSorter::new(
                spill_ctx.clone(),
                Box::new(|left: &SpillRow, right: &SpillRow| left.keys.cmp(&right.keys))
                    as Box<dyn Fn(&SpillRow, &SpillRow) -> Ordering>,
            ));
        }
        let mut left_tape = SpillTape::new(spill_ctx.clone());
        drain_to_tape(self.left.as_mut(), &mut left_tape, &self.ctx)?;
        let mut right_tape = SpillTape::new(spill_ctx);
        drain_to_tape(self.right.as_mut(), &mut right_tape, &self.ctx)?;
        self.left_reader = Some(left_tape.reader()?);
        self.right_reader = Some(right_tape.reader()?);
        self.left_tape = Some(left_tape);
        self.right_tape = Some(right_tape);
        self.phase = NestedPhase::Main;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        loop {
            self.ctx.cancel.check()?;
            match self.phase {
                NestedPhase::Main => {
                    if self.current_left.is_none() {
                        self.current_left = self
                            .left_reader
                            .as_mut()
                            .expect("open nested-loop left reader")
                            .next_record()?;
                        let Some(_) = self.current_left else {
                            if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                                self.phase = NestedPhase::UnmatchedRight;
                                self.unmatched_position = 0;
                                self.matched_stream = Some(
                                    self.matched_sorter
                                        .take()
                                        .expect("right/full matched sorter")
                                        .finish()?,
                                );
                                self.matched_next = None;
                                self.right_reader = Some(
                                    self.right_tape
                                        .as_mut()
                                        .expect("open nested-loop right tape")
                                        .reader()?,
                                );
                                continue;
                            }
                            self.phase = NestedPhase::Done;
                            continue;
                        };
                        self.matched = false;
                        self.right_position = 0;
                        self.right_reader = Some(
                            self.right_tape
                                .as_mut()
                                .expect("open nested-loop right tape")
                                .reader()?,
                        );
                    }
                    let left = self.current_left.as_ref().expect("current left row");
                    while let Some(right) = self
                        .right_reader
                        .as_mut()
                        .expect("open nested-loop right reader")
                        .next_record()?
                    {
                        self.ctx.cancel.check()?;
                        let right_position = self.right_position;
                        self.right_position =
                            self.right_position.checked_add(1).ok_or_else(|| {
                                DbError::internal("nested-loop right ordinal overflow")
                            })?;
                        let mut joined = join_row_refs(left, &right);
                        if self.join_type == JoinType::Cross
                            || join_condition_matches(&self.ctx, &self.condition, &joined)?
                        {
                            self.matched = true;
                            if let Some(sorter) = &mut self.matched_sorter {
                                sorter.push(ordinal_record(right_position))?;
                            }
                            if self.join_type == JoinType::Semi {
                                return Ok(self.current_left.take());
                            }
                            if self.join_type == JoinType::Anti {
                                self.current_left = None;
                                break;
                            }
                            if self.identity_from == Some(JoinSide::Left) {
                                joined.identity = left.identity.clone();
                            }
                            return Ok(Some(joined));
                        }
                    }
                    if self.current_left.is_none() {
                        continue;
                    }
                    let left = self.current_left.take().expect("finished left row");
                    if self.join_type == JoinType::Anti && !self.matched {
                        return Ok(Some(left));
                    }
                    if !self.matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                        return Ok(Some(join_with_null_right(&left, self.right_width)));
                    }
                }
                NestedPhase::UnmatchedRight => {
                    let right = self
                        .right_reader
                        .as_mut()
                        .expect("open nested-loop right reader")
                        .next_record()?;
                    let Some(right) = right else {
                        self.phase = NestedPhase::Done;
                        continue;
                    };
                    let ordinal = self.unmatched_position;
                    self.unmatched_position = self
                        .unmatched_position
                        .checked_add(1)
                        .ok_or_else(|| DbError::internal("unmatched-right ordinal overflow"))?;
                    loop {
                        if self.matched_next.is_none() {
                            self.matched_next = self
                                .matched_stream
                                .as_mut()
                                .expect("right/full matched stream")
                                .next_record()?
                                .map(record_ordinal);
                        }
                        match self.matched_next {
                            Some(matched) if matched < ordinal => self.matched_next = None,
                            _ => break,
                        }
                    }
                    if self.matched_next == Some(ordinal) {
                        continue;
                    }
                    return Ok(Some(join_with_null_left(self.left_width, &right)));
                }
                NestedPhase::Done => return Ok(None),
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.reset();
        Ok(())
    }
}

impl NestedLoopJoinOp<'_> {
    fn reset(&mut self) {
        self.left_reader = None;
        self.right_reader = None;
        self.left_tape = None;
        self.right_tape = None;
        self.current_left = None;
        self.right_position = 0;
        self.unmatched_position = 0;
        self.matched_sorter = None;
        self.matched_stream = None;
        self.matched_next = None;
        self.phase = NestedPhase::Done;
        self.matched = false;
    }
}

fn drain_to_tape(
    source: &mut dyn PlanExecutor,
    tape: &mut SpillTape<ExecRow>,
    ctx: &StatementContext,
) -> Result<()> {
    open_executor(source)?;
    let result = (|| {
        while let Some(row) = source.next()? {
            ctx.cancel.check()?;
            tape.push(row)?;
        }
        tape.finish()
    })();
    close_after(source, result)
}

fn ordinal_record(ordinal: u64) -> SpillRow {
    let value = Value::Numeric(ordinal.into());
    SpillRow {
        row: ExecRow {
            row: Row { values: Vec::new() },
            identity: None,
        },
        keys: vec![value],
        ordinal,
        source: 0,
    }
}

fn record_ordinal(record: SpillRow) -> u64 {
    record.ordinal
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
    ctx: StatementContext,
    left: Box<dyn PlanExecutor + 'a>,
    right: Box<dyn PlanExecutor + 'a>,
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    /// `Inner`, `Semi`, or `Anti`. Outer joins never take the hash path.
    join_type: JoinType,
    /// `Some(Left)` on a DML-source spine (`docs/specs/subqueries.md` §8.1).
    identity_from: Option<JoinSide>,
    /// Build over the logical left input when the cost-based planner estimates it
    /// is smaller. Only valid for plain inner joins; semi/anti always build right.
    build_left: bool,
    output_schema: Vec<ColumnInfo>,
    spill: SpillConfig,
    right_tape: Option<SpillTape<ExecRow>>,
    right_reader: Option<SpillTapeReader<ExecRow>>,
    table: Option<Vec<HashBuildRow>>,
    reservation: Option<Reservation>,
    current_left: Option<ExecRow>,
    match_key: Option<Vec<Value>>,
    match_index: usize,
    left_open: bool,
}

struct HashBuildRow {
    key: Vec<Value>,
    row: ExecRow,
    ordinal: u64,
}

pub struct HashJoinInput<'a> {
    pub ctx: StatementContext,
    pub left: Box<dyn PlanExecutor + 'a>,
    pub right: Box<dyn PlanExecutor + 'a>,
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
    pub join_type: JoinType,
    pub identity_from: Option<JoinSide>,
    pub build_left: bool,
    pub spill: SpillConfig,
}

impl<'a> HashJoinOp<'a> {
    pub fn new(input: HashJoinInput<'a>) -> Self {
        let HashJoinInput {
            ctx,
            mut left,
            mut right,
            mut left_keys,
            mut right_keys,
            join_type,
            identity_from,
            build_left,
            spill,
        } = input;
        debug_assert!(
            !build_left || join_type == JoinType::Inner,
            "build_left is only valid for inner hash joins"
        );
        let mut output_schema = left.output_schema().to_vec();
        if !join_type.is_semi_or_anti() {
            output_schema.extend_from_slice(right.output_schema());
        }
        if build_left {
            std::mem::swap(&mut left, &mut right);
            std::mem::swap(&mut left_keys, &mut right_keys);
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
            spill,
            right_tape: None,
            right_reader: None,
            table: None,
            reservation: None,
            current_left: None,
            match_key: None,
            match_index: 0,
            left_open: false,
        }
    }
}

impl PlanExecutor for HashJoinOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.reset_hash()?;
        let spill_ctx = self.spill.for_operator(self.ctx.cancel.clone());
        self.reservation = spill_ctx.reserve(0);
        let mut tape: Option<SpillTape<ExecRow>> = None;
        let mut build: Option<Vec<HashBuildRow>> = Some(Vec::new());
        let mut ordinal = 0u64;
        open_executor(self.right.as_mut())?;
        let result = (|| {
            while let Some(right) = self.right.next()? {
                self.ctx.cancel.check()?;
                if build.is_none() {
                    if join_key_heap_size(&right.row.values, &self.right_keys)?.is_some() {
                        tape.as_mut().expect("hash fallback tape").push(right)?;
                    }
                    continue;
                }
                let Some(estimated_key_heap) =
                    join_key_heap_size(&right.row.values, &self.right_keys)?
                else {
                    continue;
                };
                let rows = build.as_mut().expect("hash build rows");
                let old_capacity = rows.capacity();
                let growing = rows.len() == old_capacity;
                let requested_capacity = if growing {
                    old_capacity.saturating_mul(2).max(4)
                } else {
                    old_capacity
                };
                let capacity_charge = if growing {
                    requested_capacity.saturating_mul(std::mem::size_of::<HashBuildRow>()) as u64
                } else {
                    0
                };
                let heap_charge = right
                    .retained_size()
                    .saturating_sub(std::mem::size_of::<ExecRow>() as u64)
                    .saturating_add(estimated_key_heap);
                let precharge = capacity_charge.saturating_add(heap_charge);
                let mut fits = self
                    .reservation
                    .as_mut()
                    .is_some_and(|reservation| reservation.try_grow(precharge));
                let mut candidate = None;
                if fits && growing {
                    let mut replacement = Vec::new();
                    replacement
                        .try_reserve_exact(requested_capacity)
                        .map_err(|error| {
                            DbError::internal(format!("cannot reserve hash build row: {error}"))
                        })?;
                    let capacity_extra = replacement
                        .capacity()
                        .saturating_sub(requested_capacity)
                        .saturating_mul(std::mem::size_of::<HashBuildRow>())
                        as u64;
                    fits = self
                        .reservation
                        .as_mut()
                        .is_some_and(|reservation| reservation.try_grow(capacity_extra));
                    candidate = Some(replacement);
                }
                let key = if fits {
                    join_key(&right.row.values, &self.right_keys)?
                        .expect("nonnull preflight hash key")
                } else {
                    Vec::new()
                };
                let key_extra = key
                    .retained_size()
                    .saturating_sub(std::mem::size_of::<Vec<Value>>() as u64)
                    .saturating_sub(estimated_key_heap);
                fits = fits
                    && self
                        .reservation
                        .as_mut()
                        .is_some_and(|reservation| reservation.try_grow(key_extra));
                if fits {
                    if let Some(mut replacement) = candidate {
                        replacement.append(rows);
                        *rows = replacement;
                        self.reservation
                            .as_mut()
                            .expect("hash build reservation")
                            .shrink(
                                old_capacity.saturating_mul(std::mem::size_of::<HashBuildRow>())
                                    as u64,
                            );
                    }
                    rows.push(HashBuildRow {
                        key,
                        row: right,
                        ordinal,
                    });
                    ordinal = ordinal
                        .checked_add(1)
                        .ok_or_else(|| DbError::internal("hash build input ordinal overflow"))?;
                } else {
                    drop(candidate);
                    let prior = build.take().expect("hash build before fallback");
                    let mut fallback = SpillTape::disk_only(spill_ctx.clone())?;
                    for row in prior {
                        fallback.push(row.row)?;
                    }
                    self.reservation = None;
                    fallback.push(right)?;
                    tape = Some(fallback);
                }
            }
            if let Some(tape) = &mut tape {
                tape.finish()?;
            }
            Ok(())
        })();
        close_after(self.right.as_mut(), result)?;
        if let Some(rows) = &mut build {
            sort_hash_build_cancelable(&self.ctx, rows)?;
        }
        if build.is_none() {
            let tape = tape.as_mut().expect("finished hash fallback tape");
            self.right_reader = Some(tape.reader()?);
        }
        self.right_tape = tape;
        self.table = build;
        open_executor(self.left.as_mut())?;
        self.left_open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        loop {
            self.ctx.cancel.check()?;
            if self.current_left.is_none() {
                self.current_left = self.left.next()?;
                if self.current_left.is_none() {
                    self.finish_left()?;
                    return Ok(None);
                }
                self.match_key = join_key(
                    &self
                        .current_left
                        .as_ref()
                        .expect("current hash left")
                        .row
                        .values,
                    &self.left_keys,
                )?;
                self.match_index = 0;
                if self.table.is_none() {
                    self.right_reader = Some(
                        self.right_tape
                            .as_mut()
                            .expect("open hash right tape")
                            .reader()?,
                    );
                }
            }

            if let Some(table) = &self.table {
                let range = self.match_key.as_ref().map(|key| {
                    let start = table.partition_point(|row| row.key < *key);
                    let end = table.partition_point(|row| row.key <= *key);
                    start..end
                });
                if self.join_type.is_semi_or_anti() {
                    let matched = range.as_ref().is_some_and(|range| !range.is_empty());
                    let left = self.current_left.take().expect("hash semi/anti left");
                    if matched == (self.join_type == JoinType::Semi) {
                        return Ok(Some(left));
                    }
                    continue;
                }
                if let Some(right) = range
                    .and_then(|range| table.get(range.start.saturating_add(self.match_index)))
                    .filter(|row| self.match_key.as_ref() == Some(&row.key))
                {
                    self.match_index += 1;
                    let probe = self.current_left.as_ref().expect("hash matched probe");
                    let (left, logical_right) = if self.build_left {
                        (&right.row, probe)
                    } else {
                        (probe, &right.row)
                    };
                    let mut joined = join_row_refs(left, logical_right);
                    joined.identity = match self.identity_from {
                        Some(JoinSide::Left) => left.identity.clone(),
                        None => None,
                    };
                    return Ok(Some(joined));
                }
                self.current_left = None;
                continue;
            }

            let probe = self.current_left.as_ref().expect("fallback hash probe");
            let mut matched = false;
            while let Some(right) = self
                .right_reader
                .as_mut()
                .expect("fallback hash reader")
                .next_record()?
            {
                let right_key = join_key(&right.row.values, &self.right_keys)?;
                if self.match_key.is_some() && self.match_key == right_key {
                    matched = true;
                    if self.join_type == JoinType::Semi {
                        return Ok(self.current_left.take());
                    }
                    if self.join_type == JoinType::Anti {
                        self.current_left = None;
                        break;
                    }
                    let (left, logical_right) = if self.build_left {
                        (&right, probe)
                    } else {
                        (probe, &right)
                    };
                    let mut joined = join_row_refs(left, logical_right);
                    joined.identity = match self.identity_from {
                        Some(JoinSide::Left) => left.identity.clone(),
                        None => None,
                    };
                    return Ok(Some(joined));
                }
            }
            if self.current_left.is_none() {
                continue;
            }
            let left = self.current_left.take().expect("finished fallback left");
            if self.join_type == JoinType::Anti && !matched {
                return Ok(Some(left));
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.reset_hash()
    }
}

impl HashJoinOp<'_> {
    fn finish_left(&mut self) -> Result<()> {
        if self.left_open {
            self.left_open = false;
            self.left.close()
        } else {
            Ok(())
        }
    }

    fn reset_hash(&mut self) -> Result<()> {
        let close = self.finish_left();
        self.right_reader = None;
        self.right_tape = None;
        self.table = None;
        self.reservation = None;
        self.current_left = None;
        self.match_key = None;
        self.match_index = 0;
        close
    }
}

fn sort_hash_build_cancelable(ctx: &StatementContext, rows: &mut [HashBuildRow]) -> Result<()> {
    use std::cell::Cell;

    const POLL_EVERY_COMPARISONS: usize = 256;
    let comparisons = Cell::new(0usize);
    let canceled = Cell::new(false);
    rows.sort_unstable_by(|left, right| {
        if canceled.get() {
            return Ordering::Equal;
        }
        let count = comparisons.get();
        comparisons.set(count.wrapping_add(1));
        if count.is_multiple_of(POLL_EVERY_COMPARISONS) && ctx.cancel.check().is_err() {
            canceled.set(true);
            return Ordering::Equal;
        }
        left.key
            .cmp(&right.key)
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    ctx.cancel.check()
}

/// Collects the key values at `key_slots`. Returns `None` when any key column is
/// NULL, since SQL equality never matches NULL, so such rows cannot join.
fn join_key_heap_size(values: &[Value], key_slots: &[usize]) -> Result<Option<u64>> {
    let mut size = key_slots.len().saturating_mul(std::mem::size_of::<Value>()) as u64;
    for &slot in key_slots {
        let value = values
            .get(slot)
            .ok_or_else(|| DbError::internal(format!("join key slot {slot} is out of bounds")))?;
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        size = size.saturating_add(
            value
                .retained_size()
                .saturating_sub(std::mem::size_of::<Value>() as u64),
        );
    }
    Ok(Some(size))
}

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
