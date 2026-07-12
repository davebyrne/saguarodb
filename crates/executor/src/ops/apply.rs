//! The Apply (dependent join) operator: per-outer-row execution of a
//! correlated subquery template (`docs/specs/subqueries.md` §5.2).

use std::collections::HashMap;
use std::rc::Rc;

use common::{ColumnInfo, DataType, DbError, ExecRow, Result, Row, SqlState, Value};
use planner::{ApplyKind, BoundExpr, PhysicalPlan, rewrite_plan_exprs};

use crate::expr::{compare_values, eval_expr};
use crate::query::{
    ExecutionContext, PlanExecutor, build_executor, check_canceled, close_after, open_executor,
};

/// One memoized subplan result, keyed by the correlation-value tuple. The
/// `In` kind memoizes the materialized column — not the membership verdict —
/// because the operand is evaluated per outer row independently of the key.
enum MemoEntry {
    Value(Value),
    Column(Rc<Vec<Value>>),
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
    memo: Option<HashMap<Vec<Value>, MemoEntry>>,
}

impl<'a> ApplyOp<'a> {
    pub(crate) fn new(
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn PlanExecutor + 'a>,
        subplan: PhysicalPlan,
        correlations: Vec<BoundExpr>,
        kind: ApplyKind,
    ) -> Self {
        let appended = ColumnInfo {
            name: "?column?".to_string(),
            data_type: match &kind {
                ApplyKind::Scalar { data_type } => data_type.clone(),
                ApplyKind::Exists { .. } | ApplyKind::In { .. } => DataType::Boolean,
                ApplyKind::Lateral { .. } => {
                    unreachable!("Lateral applies are built as LateralApplyOp")
                }
            },
            table_id: None,
            column_id: None,
            pg_type: None,
        };
        let mut output_schema = input.output_schema().to_vec();
        output_schema.push(appended);
        let memo = if plan_has_volatile_exprs(&subplan) {
            None
        } else {
            Some(HashMap::new())
        };
        Self {
            ctx,
            input,
            subplan,
            correlations,
            kind,
            output_schema,
            memo,
        }
    }

    /// Substitute this outer row's correlation values into the template and
    /// build the inner executor.
    fn build_inner(&self, key: &[Value]) -> Result<Box<dyn PlanExecutor + 'a>> {
        let substituted = substitute_template(&self.subplan, key)?;
        build_executor(self.ctx, &substituted)
    }

    /// Compute (or recall) the subplan result for one correlation-value tuple.
    fn subplan_result(&mut self, key: Vec<Value>) -> Result<MemoEntry> {
        if let Some(memo) = &self.memo
            && let Some(entry) = memo.get(&key)
        {
            return Ok(match entry {
                MemoEntry::Value(value) => MemoEntry::Value(value.clone()),
                MemoEntry::Column(column) => MemoEntry::Column(Rc::clone(column)),
            });
        }

        let mut inner = self.build_inner(&key)?;
        open_executor(inner.as_mut())?;
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
                    MemoEntry::Value(value)
                }
                ApplyKind::Exists { negated } => {
                    let exists = inner.next()?.is_some();
                    MemoEntry::Value(Value::Boolean(exists ^ *negated))
                }
                ApplyKind::In { .. } => {
                    let mut column = Vec::new();
                    while let Some(row) = inner.next()? {
                        check_canceled(self.ctx)?;
                        column.push(single_value(row.row)?);
                    }
                    MemoEntry::Column(Rc::new(column))
                }
                ApplyKind::Lateral { .. } => {
                    unreachable!("Lateral applies are built as LateralApplyOp")
                }
            })
        })();
        let entry = close_after(inner.as_mut(), result)?;

        if let Some(memo) = &mut self.memo {
            memo.insert(
                key,
                match &entry {
                    MemoEntry::Value(value) => MemoEntry::Value(value.clone()),
                    MemoEntry::Column(column) => MemoEntry::Column(Rc::clone(column)),
                },
            );
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
            (ApplyKind::Scalar { .. } | ApplyKind::Exists { .. }, MemoEntry::Value(value)) => value,
            (ApplyKind::In { operand, negated }, MemoEntry::Column(column)) => {
                let operand = eval_expr(statement, operand, &outer)?;
                in_membership(&operand, &column, *negated)?
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
    memo: Option<HashMap<Vec<Value>, Rc<Vec<Row>>>>,
    /// Combined rows for the current outer row, emitted one per `next` call.
    pending: std::collections::VecDeque<ExecRow>,
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
    ) -> Self {
        let mut output_schema = input.output_schema().to_vec();
        let inner_width = inner_schema.len();
        output_schema.extend(inner_schema);
        let memo = if plan_has_volatile_exprs(&subplan) {
            None
        } else {
            Some(HashMap::new())
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
            pending: std::collections::VecDeque::new(),
        }
    }

    /// The template's materialized rows for one correlation-value tuple.
    fn inner_rows(&mut self, key: Vec<Value>) -> Result<Rc<Vec<Row>>> {
        if let Some(memo) = &self.memo
            && let Some(rows) = memo.get(&key)
        {
            return Ok(Rc::clone(rows));
        }
        let substituted = substitute_template(&self.subplan, &key)?;
        let mut inner = build_executor(self.ctx, &substituted)?;
        open_executor(inner.as_mut())?;
        let result = (|| {
            let mut rows = Vec::new();
            while let Some(row) = inner.next()? {
                check_canceled(self.ctx)?;
                rows.push(row.row);
            }
            Ok(rows)
        })();
        let rows = Rc::new(close_after(inner.as_mut(), result)?);
        if let Some(memo) = &mut self.memo {
            memo.insert(key, Rc::clone(&rows));
        }
        Ok(rows)
    }
}

impl PlanExecutor for LateralApplyOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.pending.clear();
        self.input.open()
    }

    fn close(&mut self) -> Result<()> {
        self.pending.clear();
        self.input.close()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                return Ok(Some(row));
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

            for inner in rows.iter() {
                let mut values = outer.row.values.clone();
                values.extend(inner.values.iter().cloned());
                let combined = ExecRow {
                    row: Row { values },
                    // Physical row identity passes through from the outer
                    // side, as for every Apply.
                    identity: outer.identity.clone(),
                };
                let matches = match &self.condition {
                    Some(condition) => {
                        matches!(
                            eval_expr(statement, condition, &combined)?,
                            Value::Boolean(true)
                        )
                    }
                    None => true,
                };
                if matches {
                    self.pending.push_back(combined);
                }
            }
            if self.pending.is_empty() && self.left_join {
                let mut values = outer.row.values;
                values.extend(std::iter::repeat_n(Value::Null, self.inner_width));
                self.pending.push_back(ExecRow {
                    row: Row { values },
                    identity: outer.identity,
                });
            }
        }
    }
}

/// SQL `IN` three-valued logic over a materialized subquery column, matching
/// the uncorrelated path's `InList` evaluation: a match is `true`; otherwise
/// a `NULL` operand or a `NULL` element makes the result `NULL`, else
/// `false`. `NOT IN` negates through three-valued `NOT` (`NULL` stays
/// `NULL`).
fn in_membership(operand: &Value, column: &[Value], negated: bool) -> Result<Value> {
    if matches!(operand, Value::Null) {
        return Ok(Value::Null);
    }
    let mut saw_null = false;
    let mut found = false;
    for value in column {
        if matches!(value, Value::Null) {
            saw_null = true;
            continue;
        }
        if matches!(
            compare_values(operand, planner::BinOp::Eq, value)?,
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
    Ok(values.pop().expect("length checked"))
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
            | PhysicalPlan::Sort { source, .. }
            | PhysicalPlan::Distinct { source, .. }
            | PhysicalPlan::Limit { source, .. }
            | PhysicalPlan::Aggregate { source, .. } => stack.push(source),
            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. }
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
