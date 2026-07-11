use std::ops::Bound;

use catalog::SystemView;
use common::{
    ColumnId, ColumnInfo, CompressionSetting, DataType, IndexConstraintKind, IndexId, Key,
    KeyRange, PRIMARY_KEY_INDEX_ID, ParsedColumnDef, Result, SequenceOptions, TableId,
    ToastOptions, Value, ViewDependency,
};

use crate::{
    AggregateExpr, ApplyKind, BinOp, BoundExpr, BoundOnConflict, BoundOrderByItem, BoundReturning,
    JoinType, LogicalPlan, SetOp,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhysicalPlan {
    CreateTable {
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        compression: CompressionSetting,
        toast: ToastOptions,
        /// `CHECK` constraint texts, persisted with the schema (see
        /// `BoundStatement::CreateTable`).
        checks: Vec<String>,
    },
    DropTable {
        name: String,
        if_exists: bool,
        table: Option<TableId>,
    },
    AlterTableAddColumn {
        table: TableId,
        table_name: String,
        if_not_exists: bool,
        column: ParsedColumnDef,
    },
    AlterTableDropColumn {
        table: TableId,
        table_name: String,
        if_exists: bool,
        column: String,
    },
    AlterTableRenameColumn {
        table: TableId,
        table_name: String,
        old_name: String,
        new_name: String,
    },
    AlterTableRenameTable {
        table: TableId,
        table_name: String,
        new_name: String,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
        index: IndexId,
    },
    CreateSequence {
        name: String,
        options: SequenceOptions,
    },
    DropSequence {
        name: String,
        if_exists: bool,
    },
    CreateView {
        name: String,
        or_replace: bool,
        columns: Vec<String>,
        query: crate::BoundQuery,
        definition: String,
        dependencies: Vec<ViewDependency>,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: Box<PhysicalPlan>,
        on_conflict: Option<BoundOnConflict>,
        returning: Option<BoundReturning>,
        /// Bound expression `DEFAULT`s for omitted columns (see
        /// `BoundStatement::Insert`), evaluated per row by the executor.
        default_exprs: Vec<(ColumnId, BoundExpr)>,
        /// Bound `CHECK` expressions enforced per inserted row (see
        /// `BoundStatement::Insert`).
        check_exprs: Vec<BoundExpr>,
    },
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: Box<PhysicalPlan>,
        returning: Option<BoundReturning>,
        /// Bound `CHECK` expressions enforced per updated row (see
        /// `BoundStatement::Update`).
        check_exprs: Vec<BoundExpr>,
    },
    Delete {
        table: TableId,
        source: Box<PhysicalPlan>,
        returning: Option<BoundReturning>,
    },
    SeqScan {
        table: TableId,
        table_name: String,
        filter: Option<BoundExpr>,
    },
    /// Dependent join (`docs/specs/subqueries.md` section 5): per `input` row,
    /// `subplan` is re-executed with each `OuterRef { slot }` replaced by the
    /// value of `correlations[slot]` evaluated against that row, and one
    /// column (per `kind`) is appended after the input columns. Row identity
    /// passes through from the input side. The statement-level subquery
    /// pre-pass does not descend into `subplan`; the Apply operator owns it.
    Apply {
        input: Box<PhysicalPlan>,
        subplan: Box<PhysicalPlan>,
        correlations: Vec<BoundExpr>,
        kind: ApplyKind,
    },
    SystemScan {
        view: SystemView,
        output_schema: Vec<ColumnInfo>,
        filter: Option<BoundExpr>,
    },
    IndexScan {
        table: TableId,
        table_name: String,
        index: IndexId,
        range: KeyRange,
        full_filter: Option<BoundExpr>,
        filter: Option<BoundExpr>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Option<BoundExpr>,
        join_type: JoinType,
    },
    /// Inner equi-join. `left_keys`/`right_keys` are paired column slots,
    /// relative to the left and right child rows respectively, that must be
    /// equal for two rows to join.
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        /// `Inner`, `Semi`, or `Anti`; outer joins never take the hash path.
        /// Semi/anti output the left side only.
        join_type: JoinType,
    },
    Filter {
        source: Box<PhysicalPlan>,
        predicate: BoundExpr,
    },
    Projection {
        source: Box<PhysicalPlan>,
        expressions: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Sort {
        source: Box<PhysicalPlan>,
        order_by: Vec<BoundOrderByItem>,
    },
    /// De-duplicate rows by `on_keys`, keeping the first row of each distinct
    /// key in input order.
    Distinct {
        source: Box<PhysicalPlan>,
        on_keys: Vec<BoundExpr>,
    },
    Limit {
        source: Box<PhysicalPlan>,
        count: u64,
        offset: Option<u64>,
    },
    Aggregate {
        source: Box<PhysicalPlan>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Values {
        rows: Vec<Vec<BoundExpr>>,
        output_schema: Vec<ColumnInfo>,
    },
    SetOp {
        op: SetOp,
        all: bool,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
}

pub fn physical_plan(
    logical: &LogicalPlan,
    catalog: &dyn catalog::CatalogManager,
) -> Result<PhysicalPlan> {
    // Hoist correlated subqueries into Apply nodes first
    // (docs/specs/subqueries.md section 5.1); physical planning below maps the
    // hoisted tree. Apply subplans are planned by the Apply arm directly, so
    // the hoist runs once per statement.
    let hoisted = crate::hoist::hoist_correlated_subqueries(logical.clone(), catalog)?;
    physical_plan_inner(&hoisted, catalog)
}

fn physical_plan_inner(
    logical: &LogicalPlan,
    catalog: &dyn catalog::CatalogManager,
) -> Result<PhysicalPlan> {
    match logical {
        LogicalPlan::CreateTable {
            name,
            if_not_exists,
            columns,
            primary_key,
            unique,
            compression,
            toast,
            checks,
        } => Ok(PhysicalPlan::CreateTable {
            name: name.clone(),
            if_not_exists: *if_not_exists,
            columns: columns.clone(),
            primary_key: primary_key.clone(),
            unique: unique.clone(),
            compression: *compression,
            toast: toast.clone(),
            checks: checks.clone(),
        }),
        LogicalPlan::DropTable {
            name,
            if_exists,
            table,
        } => Ok(PhysicalPlan::DropTable {
            name: name.clone(),
            if_exists: *if_exists,
            table: *table,
        }),
        LogicalPlan::AlterTableAddColumn {
            table,
            table_name,
            if_not_exists,
            column,
        } => Ok(PhysicalPlan::AlterTableAddColumn {
            table: *table,
            table_name: table_name.clone(),
            if_not_exists: *if_not_exists,
            column: column.clone(),
        }),
        LogicalPlan::AlterTableDropColumn {
            table,
            table_name,
            if_exists,
            column,
        } => Ok(PhysicalPlan::AlterTableDropColumn {
            table: *table,
            table_name: table_name.clone(),
            if_exists: *if_exists,
            column: column.clone(),
        }),
        LogicalPlan::AlterTableRenameColumn {
            table,
            table_name,
            old_name,
            new_name,
        } => Ok(PhysicalPlan::AlterTableRenameColumn {
            table: *table,
            table_name: table_name.clone(),
            old_name: old_name.clone(),
            new_name: new_name.clone(),
        }),
        LogicalPlan::AlterTableRenameTable {
            table,
            table_name,
            new_name,
        } => Ok(PhysicalPlan::AlterTableRenameTable {
            table: *table,
            table_name: table_name.clone(),
            new_name: new_name.clone(),
        }),
        LogicalPlan::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => Ok(PhysicalPlan::CreateIndex {
            name: name.clone(),
            table: table.clone(),
            columns: columns.clone(),
            unique: *unique,
        }),
        LogicalPlan::DropIndex { index } => Ok(PhysicalPlan::DropIndex { index: *index }),
        LogicalPlan::CreateSequence { name, options } => Ok(PhysicalPlan::CreateSequence {
            name: name.clone(),
            options: options.clone(),
        }),
        LogicalPlan::DropSequence { name, if_exists } => Ok(PhysicalPlan::DropSequence {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        LogicalPlan::CreateView {
            name,
            or_replace,
            columns,
            query,
            definition,
            dependencies,
        } => Ok(PhysicalPlan::CreateView {
            name: name.clone(),
            or_replace: *or_replace,
            columns: columns.clone(),
            query: query.clone(),
            definition: definition.clone(),
            dependencies: dependencies.clone(),
        }),
        LogicalPlan::DropView { name, if_exists } => Ok(PhysicalPlan::DropView {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            default_exprs,
            check_exprs,
        } => Ok(PhysicalPlan::Insert {
            table: *table,
            columns: columns.clone(),
            source: Box::new(physical_plan_inner(source, catalog)?),
            on_conflict: on_conflict.clone(),
            returning: returning.clone(),
            default_exprs: default_exprs.clone(),
            check_exprs: check_exprs.clone(),
        }),
        LogicalPlan::Update {
            table,
            assignments,
            source,
            returning,
            check_exprs,
        } => Ok(PhysicalPlan::Update {
            table: *table,
            assignments: assignments.clone(),
            source: Box::new(physical_plan_inner(source, catalog)?),
            returning: returning.clone(),
            check_exprs: check_exprs.clone(),
        }),
        LogicalPlan::Delete {
            table,
            source,
            returning,
        } => Ok(PhysicalPlan::Delete {
            table: *table,
            source: Box::new(physical_plan_inner(source, catalog)?),
            returning: returning.clone(),
        }),
        LogicalPlan::Scan { table, filter } => plan_scan(*table, filter.clone(), catalog),
        LogicalPlan::SystemScan { view, filter } => Ok(PhysicalPlan::SystemScan {
            view: *view,
            output_schema: system_output_schema(*view),
            filter: filter.clone(),
        }),
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => plan_join(left, right, condition, *join_type, catalog),
        LogicalPlan::Apply {
            input,
            subplan,
            correlations,
            kind,
        } => Ok(PhysicalPlan::Apply {
            input: Box::new(physical_plan_inner(input, catalog)?),
            // The subplan was already hoisted; plan it directly. OuterRef
            // expressions inside it are not index-usable, so its scans plan
            // as full scans plus filters until substitution.
            subplan: Box::new(physical_plan_inner(subplan, catalog)?),
            correlations: correlations.clone(),
            kind: kind.clone(),
        }),
        LogicalPlan::Filter { source, predicate } => Ok(PhysicalPlan::Filter {
            source: Box::new(physical_plan_inner(source, catalog)?),
            predicate: predicate.clone(),
        }),
        LogicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => Ok(PhysicalPlan::Projection {
            source: Box::new(physical_plan_inner(source, catalog)?),
            expressions: expressions.clone(),
            output_schema: output_schema.clone(),
        }),
        LogicalPlan::Sort { source, order_by } => Ok(PhysicalPlan::Sort {
            source: Box::new(physical_plan_inner(source, catalog)?),
            order_by: order_by.clone(),
        }),
        LogicalPlan::Distinct { source, on_keys } => Ok(PhysicalPlan::Distinct {
            source: Box::new(physical_plan_inner(source, catalog)?),
            on_keys: on_keys.clone(),
        }),
        LogicalPlan::Limit {
            source,
            count,
            offset,
        } => Ok(PhysicalPlan::Limit {
            source: Box::new(physical_plan_inner(source, catalog)?),
            count: *count,
            offset: *offset,
        }),
        LogicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => Ok(PhysicalPlan::Aggregate {
            source: Box::new(physical_plan_inner(source, catalog)?),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            output_schema: output_schema.clone(),
        }),
        LogicalPlan::Values {
            rows,
            output_schema,
        } => Ok(PhysicalPlan::Values {
            rows: rows.clone(),
            output_schema: output_schema.clone(),
        }),
        LogicalPlan::SetOp {
            op,
            all,
            left,
            right,
        } => Ok(PhysicalPlan::SetOp {
            op: *op,
            all: *all,
            left: Box::new(physical_plan_inner(left, catalog)?),
            right: Box::new(physical_plan_inner(right, catalog)?),
        }),
    }
}

fn plan_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    condition: &Option<BoundExpr>,
    join_type: JoinType,
    catalog: &dyn catalog::CatalogManager,
) -> Result<PhysicalPlan> {
    let left_plan = physical_plan_inner(left, catalog)?;
    let right_plan = physical_plan_inner(right, catalog)?;

    // An inner join whose ON predicate has at least one `left_col = right_col`
    // equality conjunct can run as a hash join on those equality pairs. Any
    // remaining (non-equi or expression) conjuncts are re-checked in a Filter
    // above the hash join. A semi/anti join (produced by decorrelation) takes
    // the hash path only when the WHOLE condition is equality pairs — its
    // output has no right columns, so a residual could not be re-checked
    // above it. Anything else falls back to the nested-loop join.
    if matches!(join_type, JoinType::Inner | JoinType::Semi | JoinType::Anti)
        && let Some(condition) = condition
    {
        let left_width = output_width(&left_plan, catalog)?;
        let split = split_equi_keys(condition, left_width);
        let hashable = !split.left_keys.is_empty()
            && (join_type == JoinType::Inner || split.residual.is_none());
        if hashable {
            let hash = PhysicalPlan::HashJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                left_keys: split.left_keys,
                right_keys: split.right_keys,
                join_type,
            };
            return Ok(match split.residual {
                Some(predicate) => PhysicalPlan::Filter {
                    source: Box::new(hash),
                    predicate,
                },
                None => hash,
            });
        }
    }

    Ok(PhysicalPlan::NestedLoopJoin {
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        condition: condition.clone(),
        join_type,
    })
}

struct EquiSplit {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    residual: Option<BoundExpr>,
}

/// Partition an inner-join predicate into `left_col = right_col` equality pairs
/// (the hash keys) and the remaining conjuncts (a residual re-checked in a
/// `Filter` above the join). Left key slots are as-is; right key slots are
/// rebased by `left_width`. Residual conjuncts keep their global slots, which
/// already index the joined (left ++ right) row.
fn split_equi_keys(condition: &BoundExpr, left_width: usize) -> EquiSplit {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut residuals = Vec::new();
    collect_split(
        condition,
        left_width,
        &mut left_keys,
        &mut right_keys,
        &mut residuals,
    );
    let residual = residuals
        .into_iter()
        .reduce(|acc, next| BoundExpr::BinaryOp {
            left: Box::new(acc),
            op: BinOp::And,
            right: Box::new(next),
            data_type: DataType::Boolean,
            nullable: true,
        });
    EquiSplit {
        left_keys,
        right_keys,
        residual,
    }
}

fn collect_split(
    expr: &BoundExpr,
    left_width: usize,
    left_keys: &mut Vec<usize>,
    right_keys: &mut Vec<usize>,
    residuals: &mut Vec<BoundExpr>,
) {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } => {
            collect_split(left, left_width, left_keys, right_keys, residuals);
            collect_split(right, left_width, left_keys, right_keys, residuals);
        }
        BoundExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
            ..
        } => match equi_key_pair(left, right, left_width) {
            Some((left_slot, right_slot)) => {
                left_keys.push(left_slot);
                right_keys.push(right_slot);
            }
            None => residuals.push(expr.clone()),
        },
        _ => residuals.push(expr.clone()),
    }
}

/// Returns `(left_slot, right_slot)` if `a` and `b` are column references on
/// opposite sides of the join, where `right_slot` is rebased onto the right
/// child row. Same-side or non-column operands return `None`.
fn equi_key_pair(a: &BoundExpr, b: &BoundExpr, left_width: usize) -> Option<(usize, usize)> {
    let a = input_ref_slot(a)?;
    let b = input_ref_slot(b)?;
    match (a < left_width, b < left_width) {
        (true, false) => Some((a, b - left_width)),
        (false, true) => Some((b, a - left_width)),
        _ => None,
    }
}

fn input_ref_slot(expr: &BoundExpr) -> Option<usize> {
    match expr {
        // Both reference kinds index the operator's input row by slot;
        // `LocalRef` appears in post-aggregate positions (e.g. a HAVING
        // decorrelation's join condition).
        BoundExpr::InputRef { slot, .. } | BoundExpr::LocalRef { slot, .. } => Some(*slot),
        _ => None,
    }
}

/// Number of output columns produced by a query sub-plan, used to map join
/// predicate slots onto the right child row.
fn output_width(plan: &PhysicalPlan, catalog: &dyn catalog::CatalogManager) -> Result<usize> {
    match plan {
        PhysicalPlan::SeqScan { table, .. } | PhysicalPlan::IndexScan { table, .. } => {
            table_column_count(*table, catalog)
        }
        PhysicalPlan::SystemScan { output_schema, .. } => Ok(output_schema.len()),
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            ..
        }
        | PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            ..
        } => {
            if join_type.is_semi_or_anti() {
                output_width(left, catalog)
            } else {
                Ok(output_width(left, catalog)? + output_width(right, catalog)?)
            }
        }
        PhysicalPlan::Filter { source, .. }
        | PhysicalPlan::Sort { source, .. }
        | PhysicalPlan::Distinct { source, .. }
        | PhysicalPlan::Limit { source, .. } => output_width(source, catalog),
        // Both arms have equal width (the binder reconciled them); use the left.
        PhysicalPlan::SetOp { left, .. } => output_width(left, catalog),
        PhysicalPlan::Apply { input, kind, .. } => {
            let appended = match kind {
                ApplyKind::Lateral { output_schema, .. } => output_schema.len(),
                _ => 1,
            };
            Ok(output_width(input, catalog)? + appended)
        }
        PhysicalPlan::Projection { output_schema, .. }
        | PhysicalPlan::Aggregate { output_schema, .. }
        | PhysicalPlan::Values { output_schema, .. } => Ok(output_schema.len()),
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTableAddColumn { .. }
        | PhysicalPlan::AlterTableDropColumn { .. }
        | PhysicalPlan::AlterTableRenameColumn { .. }
        | PhysicalPlan::AlterTableRenameTable { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::DropIndex { .. }
        | PhysicalPlan::CreateSequence { .. }
        | PhysicalPlan::DropSequence { .. }
        | PhysicalPlan::CreateView { .. }
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. } => Err(common::DbError::internal(
            "DML and DDL plans have no row output width",
        )),
    }
}

fn system_output_schema(view: SystemView) -> Vec<ColumnInfo> {
    view.columns()
        .into_iter()
        .map(|column| ColumnInfo {
            name: column.name,
            data_type: column.data_type,
            table_id: None,
            column_id: Some(column.id),
            pg_type: column.pg_type,
        })
        .collect()
}

fn table_column_count(table: TableId, catalog: &dyn catalog::CatalogManager) -> Result<usize> {
    let schema = catalog.get_table(table)?.ok_or_else(|| {
        common::DbError::plan(
            common::SqlState::UndefinedTable,
            format!("table id {table} does not exist"),
        )
    })?;
    Ok(schema.columns.len())
}

fn plan_scan(
    table: TableId,
    filter: Option<BoundExpr>,
    catalog: &dyn catalog::CatalogManager,
) -> Result<PhysicalPlan> {
    let schema = catalog.get_table(table)?.ok_or_else(|| {
        common::DbError::plan(
            common::SqlState::UndefinedTable,
            format!("table id {table} does not exist"),
        )
    })?;
    let table_name = schema.name.clone();

    let Some(filter_expr) = filter else {
        return Ok(PhysicalPlan::SeqScan {
            table,
            table_name,
            filter: None,
        });
    };

    if let Some((index, candidate)) = best_index_scan(&schema, table, &filter_expr, catalog)? {
        let full_filter = Some(filter_expr.clone());
        let residual = residual_filter(filter_expr, &candidate.consumed);
        return Ok(PhysicalPlan::IndexScan {
            table,
            table_name,
            index,
            range: candidate.range,
            full_filter,
            filter: residual,
        });
    }

    Ok(PhysicalPlan::SeqScan {
        table,
        table_name,
        filter: Some(filter_expr),
    })
}

/// Pick the index whose leading column the filter constrains best: an equality
/// match beats a range, a declared primary-key constraint index wins remaining
/// semantic ties, and a lower index id breaks final ties. Returns the chosen index
/// id and its key candidate, or `None` to fall back to a sequential scan.
fn best_index_scan(
    schema: &common::TableSchema,
    table: TableId,
    filter: &BoundExpr,
    catalog: &dyn catalog::CatalogManager,
) -> Result<Option<(IndexId, KeyCandidate)>> {
    let mut candidates: Vec<(IndexId, IndexConstraintKind, KeyCandidate)> = Vec::new();

    if let Some(leading) = schema.primary_key.first().copied()
        && let Some(candidate) = best_key_candidate(filter, leading)
    {
        candidates.push((
            PRIMARY_KEY_INDEX_ID,
            IndexConstraintKind::PrimaryKey,
            candidate,
        ));
    }

    for index in catalog.list_indexes_for_table(table)? {
        if let Some(leading) = index.columns.first().copied()
            && let Some(candidate) = best_key_candidate(filter, leading)
        {
            candidates.push((index.id, index.constraint, candidate));
        }
    }

    Ok(candidates
        .into_iter()
        .max_by(|(a_id, a_constraint, a), (b_id, b_constraint, b)| {
            a.exact
                .cmp(&b.exact)
                .then_with(|| {
                    (*a_constraint == IndexConstraintKind::PrimaryKey)
                        .cmp(&(*b_constraint == IndexConstraintKind::PrimaryKey))
                })
                .then_with(|| b_id.cmp(a_id))
        })
        .map(|(id, _constraint, candidate)| (id, candidate)))
}

#[derive(Clone)]
struct KeyCandidate {
    range: KeyRange,
    consumed: Vec<BoundExpr>,
    exact: bool,
}

fn best_key_candidate(expr: &BoundExpr, key_column: ColumnId) -> Option<KeyCandidate> {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } => match (
            best_key_candidate(left, key_column),
            best_key_candidate(right, key_column),
        ) {
            (Some(left), Some(right)) => Some(fuse_candidates(left, right)),
            (Some(left), None) => Some(left),
            (None, right) => right,
        },
        _ => key_candidate_from_comparison(expr, key_column),
    }
}

/// Combine two candidates over the same index column: an exact match wins; a
/// lower bound and an upper bound fuse into a two-sided range consuming both;
/// otherwise keep the left candidate.
fn fuse_candidates(left: KeyCandidate, right: KeyCandidate) -> KeyCandidate {
    if left.exact {
        return left;
    }
    if right.exact {
        return right;
    }
    combine_ranges(&left, &right).unwrap_or(left)
}

fn combine_ranges(left: &KeyCandidate, right: &KeyCandidate) -> Option<KeyCandidate> {
    let (
        KeyRange::Range {
            start: left_start,
            end: left_end,
        },
        KeyRange::Range {
            start: right_start,
            end: right_end,
        },
    ) = (&left.range, &right.range)
    else {
        return None;
    };

    // One candidate must be a pure lower bound (end unbounded) and the other a
    // pure upper bound (start unbounded).
    let (start, end) =
        if matches!(left_end, Bound::Unbounded) && matches!(right_start, Bound::Unbounded) {
            (left_start.clone(), right_end.clone())
        } else if matches!(left_start, Bound::Unbounded) && matches!(right_end, Bound::Unbounded) {
            (right_start.clone(), left_end.clone())
        } else {
            return None;
        };

    if matches!(start, Bound::Unbounded) || matches!(end, Bound::Unbounded) {
        return None;
    }

    let mut consumed = left.consumed.clone();
    consumed.extend(right.consumed.clone());
    Some(KeyCandidate {
        range: KeyRange::Range { start, end },
        consumed,
        exact: false,
    })
}

fn key_candidate_from_comparison(expr: &BoundExpr, primary_key: ColumnId) -> Option<KeyCandidate> {
    let BoundExpr::BinaryOp {
        left, op, right, ..
    } = expr
    else {
        return None;
    };

    let (op, value) = match (key_input_column(left, primary_key), literal_key(right)) {
        (true, Some(value)) => (*op, value),
        _ => match (literal_key(left), key_input_column(right, primary_key)) {
            (Some(value), true) => (reverse_comparison(*op)?, value),
            _ => return None,
        },
    };

    let key = Key(vec![value]);
    let range = match op {
        BinOp::Eq => KeyRange::Exact(key),
        BinOp::Gt => KeyRange::Range {
            start: Bound::Excluded(key),
            end: Bound::Unbounded,
        },
        BinOp::GtEq => KeyRange::Range {
            start: Bound::Included(key),
            end: Bound::Unbounded,
        },
        BinOp::Lt => KeyRange::Range {
            start: Bound::Unbounded,
            end: Bound::Excluded(key),
        },
        BinOp::LtEq => KeyRange::Range {
            start: Bound::Unbounded,
            end: Bound::Included(key),
        },
        _ => return None,
    };

    Some(KeyCandidate {
        exact: matches!(range, KeyRange::Exact(_)),
        range,
        consumed: vec![expr.clone()],
    })
}

fn residual_filter(expr: BoundExpr, consumed: &[BoundExpr]) -> Option<BoundExpr> {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
            data_type,
            nullable,
        } => match (
            residual_filter(*left, consumed),
            residual_filter(*right, consumed),
        ) {
            (Some(left), Some(right)) => Some(BoundExpr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
                data_type,
                nullable,
            }),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        },
        other => {
            if consumed.contains(&other) {
                None
            } else {
                Some(other)
            }
        }
    }
}

fn key_input_column(expr: &BoundExpr, primary_key: ColumnId) -> bool {
    matches!(
        expr,
        BoundExpr::InputRef {
            column,
            ..
        } if *column == primary_key
    )
}

fn literal_key(expr: &BoundExpr) -> Option<Value> {
    match expr {
        BoundExpr::Literal {
            value:
                Value::Integer(_)
                | Value::Float(_)
                | Value::Real(_)
                | Value::Numeric(_)
                | Value::Text(_)
                | Value::Boolean(_)
                | Value::Date(_)
                | Value::Timestamp(_)
                | Value::Time(_)
                | Value::TimestampTz(_)
                | Value::Interval(_)
                | Value::Bytes(_)
                | Value::Uuid(_),
            ..
        } => {
            let BoundExpr::Literal { value, .. } = expr else {
                unreachable!();
            };
            Some(value.clone())
        }
        _ => None,
    }
}

fn reverse_comparison(op: BinOp) -> Option<BinOp> {
    match op {
        BinOp::Eq => Some(BinOp::Eq),
        BinOp::Lt => Some(BinOp::Gt),
        BinOp::LtEq => Some(BinOp::GtEq),
        BinOp::Gt => Some(BinOp::Lt),
        BinOp::GtEq => Some(BinOp::LtEq),
        _ => None,
    }
}
