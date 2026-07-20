//! The Apply (dependent join) operator: per-outer-row execution of a
//! correlated subquery template (`docs/specs/subqueries.md` §5.2).

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use common::{ColumnInfo, DataType, DbError, ExecRow, Result, Row, SqlState, Value};
use planner::{ApplyKind, BoundExpr, PhysicalPlan, rewrite_plan_exprs};
use spill::{Reservation, RetainedSize, SpillContext, SpillTape, SpillTapeReader};

use crate::expr::{compare_values, eval_expr};
use crate::instrumentation::DynamicProfile;
use crate::query::{
    ExecutionContext, PlanExecutor, build_executor, build_executor_with_validated_analysis_profile,
    build_executor_with_validated_profile, check_canceled, close_after, open_executor,
};

/// One memoized subplan result, keyed by the correlation-value tuple. The
/// `In` kind memoizes the materialized column — not the membership verdict —
/// because the operand is evaluated per outer row independently of the key.
#[derive(Clone)]
enum MemoPayload {
    Scalar(Value),
    Column(Arc<Mutex<SpillTape<Value>>>),
    Rows(Arc<Mutex<SpillTape<Row>>>),
}

struct MemoEntry {
    payload: MemoPayload,
    last_used: u64,
    metadata_charge: u64,
}

struct ApplyMemo {
    entries: HashMap<Vec<Value>, MemoEntry>,
    reservation: Reservation,
    access: u64,
}

impl ApplyMemo {
    fn new(ctx: &SpillContext) -> Option<Self> {
        Some(Self {
            entries: HashMap::new(),
            reservation: ctx.reserve(0)?,
            access: 0,
        })
    }

    fn tick(&mut self) -> u64 {
        if let Some(next) = self.access.checked_add(1) {
            self.access = next;
            return next;
        }
        let mut order = self
            .entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_used))
            .collect::<Vec<_>>();
        order.sort_unstable_by_key(|(_, used)| *used);
        for (index, (key, _)) in order.into_iter().enumerate() {
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.last_used = index as u64 + 1;
            }
        }
        self.access = self.entries.len() as u64 + 1;
        self.access
    }

    fn get(&mut self, key: &[Value]) -> Option<MemoPayload> {
        let used = self.tick();
        let entry = self.entries.get_mut(key)?;
        entry.last_used = used;
        Some(entry.payload.clone())
    }

    fn insert(&mut self, key: Vec<Value>, payload: MemoPayload) -> bool {
        let scalar_heap = match &payload {
            MemoPayload::Scalar(value) => value
                .retained_size()
                .saturating_sub(size_of::<Value>() as u64),
            _ => 0,
        };
        let charge = key
            .retained_size()
            .saturating_add(size_of::<MemoEntry>() as u64)
            .saturating_add(scalar_heap)
            .saturating_add(1);
        while !self.reservation.try_grow(charge) {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                return false;
            };
            if let Some(entry) = self.entries.remove(&victim) {
                self.reservation.shrink(entry.metadata_charge);
            }
        }
        if self.entries.len() == self.entries.capacity() {
            let old = self.entries.capacity();
            let unit = size_of::<(Vec<Value>, MemoEntry)>() as u64 + 1;
            let conservative_capacity = self
                .entries
                .len()
                .saturating_add(1)
                .next_power_of_two()
                .saturating_mul(2);
            let map_charge = conservative_capacity.saturating_sub(old) as u64 * unit;
            let mut growth_charged = false;
            while self.entries.len() == self.entries.capacity() {
                if self.reservation.try_grow(map_charge) {
                    growth_charged = true;
                    break;
                }
                let Some(victim) = self
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(key, _)| key.clone())
                else {
                    self.reservation.shrink(charge);
                    return false;
                };
                if let Some(entry) = self.entries.remove(&victim) {
                    self.reservation.shrink(entry.metadata_charge);
                }
            }
            if growth_charged && self.entries.try_reserve(1).is_err() {
                self.reservation.shrink(charge + map_charge);
                return false;
            }
            if growth_charged {
                let actual_charge = self.entries.capacity().saturating_sub(old) as u64 * unit;
                self.reservation
                    .shrink(map_charge.saturating_sub(actual_charge));
            }
        }
        let used = self.tick();
        self.entries.insert(
            key,
            MemoEntry {
                payload,
                last_used: used,
                metadata_charge: charge,
            },
        );
        true
    }
}

pub struct ApplyOp<'a> {
    ctx: &'a ExecutionContext<'a>,
    input: Box<dyn PlanExecutor + 'a>,
    /// The subquery template: the construction-time pre-pass has already
    /// resolved its uncorrelated nested subqueries to constants, so per-row
    /// work is `OuterRef` substitution only.
    subplan: PhysicalPlan,
    /// Per `OuterRef` slot: the outer column expression, evaluated against
    /// each input row (`docs/specs/subqueries.md` §4.2).
    correlations: Vec<BoundExpr>,
    kind: ApplyKind,
    output_schema: Vec<ColumnInfo>,
    /// `None` when the template contains a sequence function — such a subplan
    /// re-executes for every outer row (`docs/specs/subqueries.md` §2).
    memo: Option<ApplyMemo>,
    spill_ctx: SpillContext,
    profile: Option<DynamicProfile>,
}

impl<'a> ApplyOp<'a> {
    pub(crate) fn new(
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn PlanExecutor + 'a>,
        subplan: PhysicalPlan,
        correlations: Vec<BoundExpr>,
        kind: ApplyKind,
        profile: Option<DynamicProfile>,
    ) -> Result<Self> {
        let appended = ColumnInfo {
            name: "?column?".to_string(),
            data_type: match &kind {
                ApplyKind::Scalar { data_type } => data_type.clone(),
                ApplyKind::Exists { .. } | ApplyKind::In { .. } => DataType::Boolean,
                ApplyKind::Lateral { .. } => {
                    return Err(DbError::internal(
                        "lateral apply was routed to the scalar apply executor",
                    ));
                }
            },
            table_id: None,
            column_id: None,
            pg_type: None,
        };
        let mut output_schema = input.output_schema().to_vec();
        output_schema.push(appended);
        let spill_ctx = ctx.spill.for_operator(ctx.statement.cancel.clone());
        let memo = if plan_has_volatile_exprs(&subplan) {
            None
        } else {
            ApplyMemo::new(&spill_ctx)
        };
        Ok(Self {
            ctx,
            input,
            subplan,
            correlations,
            kind,
            output_schema,
            memo,
            spill_ctx,
            profile,
        })
    }

    /// Substitute this outer row's correlation values into the template and
    /// build the inner executor.
    fn build_inner(&self, key: &[Value]) -> Result<Box<dyn PlanExecutor + 'a>> {
        let substituted = substitute_template(&self.subplan, key)?;
        match &self.profile {
            Some(profile) => match &profile.analysis {
                Some(analysis) => build_executor_with_validated_analysis_profile(
                    self.ctx,
                    &substituted,
                    &profile.layout,
                    &profile.collector,
                    analysis,
                    profile.init_parent,
                ),
                None => build_executor_with_validated_profile(
                    self.ctx,
                    &substituted,
                    &profile.layout,
                    &profile.collector,
                ),
            },
            None => build_executor(self.ctx, &substituted),
        }
    }

    /// Compute (or recall) the subplan result for one correlation-value tuple.
    fn subplan_result(&mut self, key: Vec<Value>) -> Result<MemoPayload> {
        if let Some(payload) = self.memo.as_mut().and_then(|memo| memo.get(&key)) {
            return Ok(payload);
        }

        let registry = &self.ctx.statement.runtime_value_sets;
        let watermark = registry.watermark();
        let mut inner = match self.build_inner(&key) {
            Ok(inner) => inner,
            Err(err) => {
                registry.remove_since(watermark);
                return Err(err);
            }
        };
        if let Err(err) = open_executor(inner.as_mut()) {
            registry.remove_since(watermark);
            return Err(err);
        }
        let kind = &self.kind;
        let result = (|| {
            Ok(match kind {
                ApplyKind::Scalar { .. } => {
                    let value = match inner.next()? {
                        None => Value::Null,
                        Some(row) => {
                            if inner.next()?.is_some() {
                                return Err(DbError::execute(
                                    SqlState::CardinalityViolation,
                                    "more than one row returned by a subquery used as an expression",
                                ));
                            }
                            single_value(row.row)?
                        }
                    };
                    MemoPayload::Scalar(value)
                }
                ApplyKind::Exists { negated } => {
                    let exists = inner.next()?.is_some();
                    MemoPayload::Scalar(Value::Boolean(exists ^ *negated))
                }
                ApplyKind::In { .. } => {
                    let mut column = SpillTape::new(self.spill_ctx.clone());
                    while let Some(row) = inner.next()? {
                        check_canceled(self.ctx)?;
                        column.push(single_value(row.row)?)?;
                    }
                    column.finish()?;
                    MemoPayload::Column(Arc::new(Mutex::new(column)))
                }
                ApplyKind::Lateral { .. } => {
                    return Err(DbError::internal(
                        "lateral apply was routed to the scalar apply executor",
                    ));
                }
            })
        })();
        let entry = close_after(inner.as_mut(), result);
        registry.remove_since(watermark);
        let entry = entry?;

        if let Some(memo) = &mut self.memo {
            memo.insert(key, entry.clone());
        }
        Ok(entry)
    }
}

impl PlanExecutor for ApplyOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        // The memo (when enabled) survives a re-open: results are a pure
        // function of the correlation key under the statement's snapshot.
        self.input.open()
    }

    fn close(&mut self) -> Result<()> {
        // Inner executors are opened, drained, and closed within a single
        // `next` call; only the input child stays open across calls.
        self.input.close()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let Some(outer) = self.input.next()? else {
            return Ok(None);
        };
        check_canceled(self.ctx)?;
        let statement = &self.ctx.statement;
        let key = self
            .correlations
            .iter()
            .map(|expr| eval_expr(statement, expr, &outer))
            .collect::<Result<Vec<_>>>()?;
        let entry = self.subplan_result(key)?;

        let appended = match (&self.kind, entry) {
            (ApplyKind::Scalar { .. } | ApplyKind::Exists { .. }, MemoPayload::Scalar(value)) => {
                value
            }
            (ApplyKind::In { operand, negated }, MemoPayload::Column(column)) => {
                let operand = eval_expr(statement, operand, &outer)?;
                let mut tape = column
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let mut reader = tape.reader()?;
                in_membership(&operand, &mut reader, *negated)?
            }
            _ => return Err(DbError::internal("apply result shape mismatch")),
        };

        let mut values = outer.row.values;
        values.push(appended);
        Ok(Some(ExecRow {
            row: Row { values },
            // Physical row identity passes through from the input side, so an
            // Apply inside a DML source keeps UPDATE/DELETE targetable.
            identity: outer.identity,
        }))
    }
}

/// Substitute one outer row's correlation values (`key`, indexed by `OuterRef`
/// slot) into a subquery template.
fn substitute_template(subplan: &PhysicalPlan, key: &[Value]) -> Result<PhysicalPlan> {
    rewrite_plan_exprs(subplan, &mut |expr| match expr {
        BoundExpr::OuterRef {
            slot,
            data_type,
            nullable,
        } => {
            let value = key.get(*slot).cloned().ok_or_else(|| {
                DbError::internal(format!("correlation slot {slot} out of bounds"))
            })?;
            Ok(Some(BoundExpr::Literal {
                value,
                data_type: data_type.clone(),
                nullable: *nullable,
            }))
        }
        _ => Ok(None),
    })
}

/// The LATERAL Apply operator (`docs/specs/subqueries.md` §7): per outer row
/// the derived-table template re-executes with sibling references
/// substituted, and every matching inner row is appended after the outer
/// columns (one output row per match); `left_join` emits one null-padded row
/// when nothing matches. The memo stores the UNFILTERED inner rows per
/// correlation key — the ON condition may reference outer columns, so it is
/// evaluated per outer row against each combined row.
pub struct LateralApplyOp<'a> {
    ctx: &'a ExecutionContext<'a>,
    input: Box<dyn PlanExecutor + 'a>,
    subplan: PhysicalPlan,
    correlations: Vec<BoundExpr>,
    left_join: bool,
    condition: Option<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    inner_width: usize,
    memo: Option<ApplyMemo>,
    spill_ctx: SpillContext,
    current_outer: Option<ExecRow>,
    current_reader: Option<SpillTapeReader<Row>>,
    current_owned_tape: Option<Arc<Mutex<SpillTape<Row>>>>,
    current_matched: bool,
    profile: Option<DynamicProfile>,
}

impl<'a> LateralApplyOp<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn PlanExecutor + 'a>,
        subplan: PhysicalPlan,
        correlations: Vec<BoundExpr>,
        left_join: bool,
        condition: Option<BoundExpr>,
        inner_schema: Vec<ColumnInfo>,
        profile: Option<DynamicProfile>,
    ) -> Self {
        let mut output_schema = input.output_schema().to_vec();
        let inner_width = inner_schema.len();
        output_schema.extend(inner_schema);
        let spill_ctx = ctx.spill.for_operator(ctx.statement.cancel.clone());
        let memo = if plan_has_volatile_exprs(&subplan) {
            None
        } else {
            ApplyMemo::new(&spill_ctx)
        };
        Self {
            ctx,
            input,
            subplan,
            correlations,
            left_join,
            condition,
            output_schema,
            inner_width,
            memo,
            spill_ctx,
            current_outer: None,
            current_reader: None,
            current_owned_tape: None,
            current_matched: false,
            profile,
        }
    }

    /// The template's materialized rows for one correlation-value tuple.
    fn inner_rows(&mut self, key: Vec<Value>) -> Result<Arc<Mutex<SpillTape<Row>>>> {
        if let Some(MemoPayload::Rows(rows)) = self.memo.as_mut().and_then(|memo| memo.get(&key)) {
            return Ok(rows);
        }
        let registry = &self.ctx.statement.runtime_value_sets;
        let watermark = registry.watermark();
        let substituted = substitute_template(&self.subplan, &key)?;
        let inner = match &self.profile {
            Some(profile) => match &profile.analysis {
                Some(analysis) => build_executor_with_validated_analysis_profile(
                    self.ctx,
                    &substituted,
                    &profile.layout,
                    &profile.collector,
                    analysis,
                    profile.init_parent,
                ),
                None => build_executor_with_validated_profile(
                    self.ctx,
                    &substituted,
                    &profile.layout,
                    &profile.collector,
                ),
            },
            None => build_executor(self.ctx, &substituted),
        };
        let mut inner = match inner {
            Ok(inner) => inner,
            Err(err) => {
                registry.remove_since(watermark);
                return Err(err);
            }
        };
        if let Err(err) = open_executor(inner.as_mut()) {
            registry.remove_since(watermark);
            return Err(err);
        }
        let result = (|| {
            let mut rows = SpillTape::new(self.spill_ctx.clone());
            while let Some(row) = inner.next()? {
                check_canceled(self.ctx)?;
                rows.push(row.row)?;
            }
            rows.finish()?;
            Ok(rows)
        })();
        let rows = close_after(inner.as_mut(), result);
        registry.remove_since(watermark);
        let rows = Arc::new(Mutex::new(rows?));
        if let Some(memo) = &mut self.memo {
            memo.insert(key, MemoPayload::Rows(Arc::clone(&rows)));
        }
        Ok(rows)
    }
}

impl PlanExecutor for LateralApplyOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.current_reader = None;
        self.current_owned_tape = None;
        self.current_outer = None;
        self.input.open()
    }

    fn close(&mut self) -> Result<()> {
        self.current_reader = None;
        self.current_owned_tape = None;
        self.current_outer = None;
        self.input.close()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        loop {
            check_canceled(self.ctx)?;
            if let Some(reader) = &mut self.current_reader {
                while let Some(inner) = reader.next_record()? {
                    check_canceled(self.ctx)?;
                    let outer = self.current_outer.as_ref().ok_or_else(|| {
                        DbError::internal("lateral Apply reader has no outer row")
                    })?;
                    let mut values = outer.row.values.clone();
                    values.extend(inner.values);
                    let combined = ExecRow {
                        row: Row { values },
                        identity: outer.identity.clone(),
                    };
                    let matches = self.condition.as_ref().map_or(Ok(true), |condition| {
                        eval_expr(&self.ctx.statement, condition, &combined)
                            .map(|value| matches!(value, Value::Boolean(true)))
                    })?;
                    if matches {
                        self.current_matched = true;
                        return Ok(Some(combined));
                    }
                }
                self.current_reader = None;
                self.current_owned_tape = None;
                let outer = self
                    .current_outer
                    .take()
                    .ok_or_else(|| DbError::internal("lateral Apply reader lost its outer row"))?;
                if self.left_join && !self.current_matched {
                    let mut values = outer.row.values;
                    values.extend(std::iter::repeat_n(Value::Null, self.inner_width));
                    return Ok(Some(ExecRow {
                        row: Row { values },
                        identity: outer.identity,
                    }));
                }
            }
            let Some(outer) = self.input.next()? else {
                return Ok(None);
            };
            check_canceled(self.ctx)?;
            let statement = &self.ctx.statement;
            let key = self
                .correlations
                .iter()
                .map(|expr| eval_expr(statement, expr, &outer))
                .collect::<Result<Vec<_>>>()?;
            let rows = self.inner_rows(key)?;
            let reader = rows
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .reader()?;
            self.current_outer = Some(outer);
            self.current_reader = Some(reader);
            self.current_owned_tape = Some(rows);
            self.current_matched = false;
        }
    }
}

/// SQL `IN` three-valued logic over a materialized subquery column, matching
/// the uncorrelated path's `InList` evaluation: a match is `true`; otherwise
/// a `NULL` operand or a `NULL` element makes the result `NULL`, else
/// `false`. `NOT IN` negates through three-valued `NOT` (`NULL` stays
/// `NULL`).
fn in_membership(
    operand: &Value,
    column: &mut SpillTapeReader<Value>,
    negated: bool,
) -> Result<Value> {
    if matches!(operand, Value::Null) {
        return Ok(Value::Null);
    }
    let mut saw_null = false;
    let mut found = false;
    while let Some(value) = column.next_record()? {
        if matches!(value, Value::Null) {
            saw_null = true;
            continue;
        }
        if matches!(
            compare_values(operand, planner::BinOp::Eq, &value)?,
            Value::Boolean(true)
        ) {
            found = true;
            break;
        }
    }
    Ok(match (found, saw_null) {
        (true, _) => Value::Boolean(!negated),
        (false, true) => Value::Null,
        (false, false) => Value::Boolean(negated),
    })
}

/// Extract the single value of a one-column subquery row (the binder
/// guarantees the shape; re-checked so a malformed plan fails loudly).
fn single_value(row: Row) -> Result<Value> {
    let mut values = row.values;
    if values.len() != 1 {
        return Err(DbError::internal(
            "subquery used as a value produced a row with the wrong number of columns",
        ));
    }
    values
        .pop()
        .ok_or_else(|| DbError::internal("single-value subquery row is empty"))
}

/// Whether any expression in the template (including nested Apply templates,
/// which the shared rewriter deliberately does not descend into) is a
/// sequence function. Such templates are never memoized.
fn plan_has_volatile_exprs(plan: &PhysicalPlan) -> bool {
    let mut found = false;
    let _ = rewrite_plan_exprs(plan, &mut |expr| {
        match expr {
            BoundExpr::Nextval { .. } | BoundExpr::Setval { .. } | BoundExpr::Currval { .. } => {
                found = true;
            }
            // A not-yet-resolved subquery expression (resolved lazily at a
            // nested ApplyOp's construction, i.e. per outer memo miss) hides
            // its body from the plan walk; probe the bound body directly.
            BoundExpr::ScalarSubquery { query, .. }
            | BoundExpr::Exists { query, .. }
            | BoundExpr::InSubquery { query, .. }
                if planner::query_contains_sequence_exprs(query) =>
            {
                found = true;
            }
            _ => {}
        }
        Ok(None)
    });
    if found {
        return true;
    }
    nested_apply_subplans(plan)
        .into_iter()
        .any(plan_has_volatile_exprs)
}

/// The subplan templates of every `Apply` node in `plan`, found without
/// entering them (each returned subplan is probed on its own).
fn nested_apply_subplans(plan: &PhysicalPlan) -> Vec<&PhysicalPlan> {
    let mut subplans = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        match node {
            PhysicalPlan::Apply { input, subplan, .. } => {
                subplans.push(subplan.as_ref());
                stack.push(input);
            }
            PhysicalPlan::Insert { source, .. }
            | PhysicalPlan::Update { source, .. }
            | PhysicalPlan::Delete { source, .. }
            | PhysicalPlan::Filter { source, .. }
            | PhysicalPlan::Projection { source, .. }
            | PhysicalPlan::LockRows { source, .. }
            | PhysicalPlan::Sort { source, .. }
            | PhysicalPlan::Distinct { source, .. }
            | PhysicalPlan::Limit { source, .. }
            | PhysicalPlan::Aggregate { source, .. }
            | PhysicalPlan::Window { source, .. } => stack.push(source),
            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::MergeJoin { left, right, .. }
            | PhysicalPlan::SetOp { left, right, .. } => {
                stack.push(left);
                stack.push(right);
            }
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::SystemScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::Values { .. }
            | PhysicalPlan::TableFunction { .. }
            | PhysicalPlan::CreateSchema { .. }
            | PhysicalPlan::DropSchema { .. }
            | PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::AlterTableAddColumn { .. }
            | PhysicalPlan::AlterTableDropColumn { .. }
            | PhysicalPlan::AlterTableRenameColumn { .. }
            | PhysicalPlan::AlterTableRenameTable { .. }
            | PhysicalPlan::AlterTableAlterColumnType { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. }
            | PhysicalPlan::CreateSequence { .. }
            | PhysicalPlan::DropSequence { .. }
            | PhysicalPlan::CreateView { .. }
            | PhysicalPlan::DropView { .. } => {}
        }
    }
    subplans
}
