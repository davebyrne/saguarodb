use std::ops::Bound;

use catalog::SystemView;
use common::{
    ColumnId, ColumnInfo, CompressionSetting, DataType, IndexId, Key, KeyRange,
    PRIMARY_KEY_INDEX_ID, ParsedColumnDef, Result, SchemaId, SequenceOptions, StoredQueryV1,
    TableId, ToastOptions, Value,
};

use crate::{
    AggregateExpr, ApplyKind, BinOp, BoundExpr, BoundForeignKey, BoundOnConflict, BoundOrderByItem,
    BoundReturning, BoundWindowSpec, JoinSide, JoinType, LogicalPlan, SetOp, WindowFuncExpr,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhysicalPlan {
    CreateSchema {
        name: String,
        if_not_exists: bool,
    },
    DropSchema {
        name: String,
        if_exists: bool,
    },
    CreateTable {
        schema: SchemaId,
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        compression: CompressionSetting,
        toast: ToastOptions,
        /// `CHECK` constraint texts, persisted with the schema (see
        /// `BoundStatement::CreateTable`).
        checks: Vec<common::StoredExpression>,
        foreign_keys: Vec<BoundForeignKey>,
    },
    DropTable {
        targets: Vec<crate::DropTableTarget>,
        if_exists: bool,
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
    AlterTableAlterColumnType {
        table: TableId,
        table_name: String,
        column: String,
        data_type: DataType,
        pg_type: common::PgType,
    },
    CreateIndex {
        schema: SchemaId,
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
        index: IndexId,
    },
    CreateSequence {
        schema: SchemaId,
        name: String,
        options: SequenceOptions,
    },
    DropSequence {
        name: String,
        search_path: Vec<SchemaId>,
        sequence: Option<common::SequenceId>,
        if_exists: bool,
    },
    CreateView {
        schema: SchemaId,
        name: String,
        or_replace: bool,
        columns: Vec<String>,
        query: crate::BoundQuery,
        definition: String,
        stored_query: StoredQueryV1,
        definition_search_path: Vec<SchemaId>,
    },
    DropView {
        name: String,
        search_path: Vec<SchemaId>,
        view: Option<TableId>,
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
        /// `UPDATE ... FROM`: source rows are the combined (target ++ FROM)
        /// row; the executor takes the target prefix and de-duplicates by row
        /// identity (`docs/specs/subqueries.md` §8.2). Width cannot stand in
        /// for this flag — a zero-column FROM item keeps the combined width
        /// equal to the table's.
        joined_source: bool,
        returning: Option<BoundReturning>,
        /// Bound `CHECK` expressions enforced per updated row (see
        /// `BoundStatement::Update`).
        check_exprs: Vec<BoundExpr>,
    },
    Delete {
        table: TableId,
        source: Box<PhysicalPlan>,
        /// `DELETE ... USING` (§8.2); see `Update::joined_source`.
        joined_source: bool,
        returning: Option<BoundReturning>,
    },
    LockRows {
        source: Box<PhysicalPlan>,
        table: TableId,
        mode: common::TupleLockMode,
        wait_policy: common::TupleLockWaitPolicy,
        recheck: Option<BoundExpr>,
        expressions: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
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
        /// `Some(Left)` on a DML-source join spine: combined rows carry the
        /// left side's row identity (`docs/specs/subqueries.md` §8.1).
        identity_from: Option<JoinSide>,
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
        /// `Some(Left)` on a DML-source join spine (§8.1).
        identity_from: Option<JoinSide>,
        /// Build the in-memory hash table over the LEFT input and stream the
        /// right (`docs/specs/statistics.md` §9.2). Chosen only for a plain
        /// inner join whose inputs are both fully analyzed and whose left
        /// side estimates smaller; the executor's output column order is
        /// left ++ right either way. `false` is the historical
        /// build-right/stream-left behavior.
        build_left: bool,
    },
    MergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        residual: Option<BoundExpr>,
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
    Window {
        source: Box<PhysicalPlan>,
        spec: BoundWindowSpec,
        functions: Vec<WindowFuncExpr>,
    },
    Values {
        rows: Vec<Vec<BoundExpr>>,
        output_schema: Vec<ColumnInfo>,
    },
    TableFunction {
        name: String,
        args: Vec<BoundExpr>,
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
        LogicalPlan::CreateSchema {
            name,
            if_not_exists,
        } => Ok(PhysicalPlan::CreateSchema {
            name: name.clone(),
            if_not_exists: *if_not_exists,
        }),
        LogicalPlan::DropSchema { name, if_exists } => Ok(PhysicalPlan::DropSchema {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        LogicalPlan::CreateTable {
            schema,
            name,
            if_not_exists,
            columns,
            primary_key,
            unique,
            compression,
            toast,
            checks,
            foreign_keys,
        } => Ok(PhysicalPlan::CreateTable {
            schema: *schema,
            name: name.clone(),
            if_not_exists: *if_not_exists,
            columns: columns.clone(),
            primary_key: primary_key.clone(),
            unique: unique.clone(),
            compression: *compression,
            toast: toast.clone(),
            checks: checks.clone(),
            foreign_keys: foreign_keys.clone(),
        }),
        LogicalPlan::DropTable { targets, if_exists } => Ok(PhysicalPlan::DropTable {
            targets: targets.clone(),
            if_exists: *if_exists,
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
        LogicalPlan::AlterTableAlterColumnType {
            table,
            table_name,
            column,
            data_type,
            pg_type,
        } => Ok(PhysicalPlan::AlterTableAlterColumnType {
            table: *table,
            table_name: table_name.clone(),
            column: column.clone(),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
        }),
        LogicalPlan::CreateIndex {
            schema,
            name,
            table,
            columns,
            unique,
        } => Ok(PhysicalPlan::CreateIndex {
            schema: *schema,
            name: name.clone(),
            table: table.clone(),
            columns: columns.clone(),
            unique: *unique,
        }),
        LogicalPlan::DropIndex { index } => Ok(PhysicalPlan::DropIndex { index: *index }),
        LogicalPlan::CreateSequence {
            schema,
            name,
            options,
        } => Ok(PhysicalPlan::CreateSequence {
            schema: *schema,
            name: name.clone(),
            options: options.clone(),
        }),
        LogicalPlan::DropSequence {
            name,
            search_path,
            sequence,
            if_exists,
        } => Ok(PhysicalPlan::DropSequence {
            name: name.clone(),
            search_path: search_path.clone(),
            sequence: *sequence,
            if_exists: *if_exists,
        }),
        LogicalPlan::CreateView {
            schema,
            name,
            or_replace,
            columns,
            query,
            definition,
            stored_query,
            definition_search_path,
        } => Ok(PhysicalPlan::CreateView {
            schema: *schema,
            name: name.clone(),
            or_replace: *or_replace,
            columns: columns.clone(),
            query: query.clone(),
            definition: definition.clone(),
            stored_query: stored_query.clone(),
            definition_search_path: definition_search_path.clone(),
        }),
        LogicalPlan::DropView {
            name,
            search_path,
            view,
            if_exists,
        } => Ok(PhysicalPlan::DropView {
            name: name.clone(),
            search_path: search_path.clone(),
            view: *view,
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
            joined_source,
            returning,
            check_exprs,
        } => Ok(PhysicalPlan::Update {
            table: *table,
            assignments: assignments.clone(),
            source: Box::new(physical_plan_inner(source, catalog)?),
            joined_source: *joined_source,
            returning: returning.clone(),
            check_exprs: check_exprs.clone(),
        }),
        LogicalPlan::Delete {
            table,
            source,
            joined_source,
            returning,
        } => Ok(PhysicalPlan::Delete {
            table: *table,
            source: Box::new(physical_plan_inner(source, catalog)?),
            joined_source: *joined_source,
            returning: returning.clone(),
        }),
        LogicalPlan::LockRows {
            source,
            table,
            mode,
            wait_policy,
            recheck,
            expressions,
            output_schema,
        } => Ok(PhysicalPlan::LockRows {
            source: Box::new(physical_plan_inner(source, catalog)?),
            table: *table,
            mode: *mode,
            wait_policy: *wait_policy,
            recheck: recheck.clone(),
            expressions: expressions.clone(),
            output_schema: output_schema.clone(),
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
            identity_from,
        } => plan_join(left, right, condition, *join_type, *identity_from, catalog),
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
        LogicalPlan::Window {
            source,
            spec,
            functions,
        } => Ok(PhysicalPlan::Window {
            source: Box::new(physical_plan_inner(source, catalog)?),
            spec: spec.clone(),
            functions: functions.clone(),
        }),
        LogicalPlan::Values {
            rows,
            output_schema,
        } => Ok(PhysicalPlan::Values {
            rows: rows.clone(),
            output_schema: output_schema.clone(),
        }),
        LogicalPlan::TableFunction {
            name,
            args,
            output_schema,
        } => Ok(PhysicalPlan::TableFunction {
            name: name.clone(),
            args: args.clone(),
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
    identity_from: Option<JoinSide>,
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
    if let Some(condition) = condition {
        let left_width = output_width(&left_plan, catalog)?;
        let split = split_equi_keys(condition, left_width);
        let hashable = matches!(join_type, JoinType::Inner | JoinType::Semi | JoinType::Anti)
            && !split.left_keys.is_empty()
            && (join_type == JoinType::Inner || split.residual.is_none());
        if hashable {
            // First cost-based decision (docs/specs/statistics.md §9.2):
            // build the hash table over the smaller estimated input. Only for
            // a plain inner join (semi/anti stream the left side by
            // construction), only outside a DML spine (conservative: which
            // duplicate identity row wins dedup stays order-stable), and only
            // when BOTH inputs are fully analyzed — un-analyzed plans keep
            // the historical build-right shape exactly.
            let build_left = join_type == JoinType::Inner
                && identity_from.is_none()
                && crate::estimate::plan_fully_analyzed(&left_plan, catalog)
                && crate::estimate::plan_fully_analyzed(&right_plan, catalog)
                && crate::estimate::estimated_rows(&left_plan, catalog)
                    < crate::estimate::estimated_rows(&right_plan, catalog);
            let hash = PhysicalPlan::HashJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                left_keys: split.left_keys,
                right_keys: split.right_keys,
                join_type,
                identity_from,
                build_left,
            };
            return Ok(match split.residual {
                Some(predicate) => PhysicalPlan::Filter {
                    source: Box::new(hash),
                    predicate,
                },
                None => hash,
            });
        }
        if matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full)
            && identity_from.is_none()
            && !split.left_keys.is_empty()
        {
            return Ok(PhysicalPlan::MergeJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                left_keys: split.left_keys,
                right_keys: split.right_keys,
                residual: split.residual,
                join_type,
            });
        }
    }

    Ok(PhysicalPlan::NestedLoopJoin {
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        condition: condition.clone(),
        join_type,
        identity_from,
    })
}

struct EquiSplit {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    residual: Option<BoundExpr>,
}

/// Partition an inner-join predicate into `left_col = right_col` equality pairs
/// (the hash keys) and the remaining conjuncts (a residual re-checked in a
/// `Filter` above the join). The condition's slots are subtree-local (the
/// lowering rebased them): left key slots are as-is, right key slots are
/// rebased by `left_width`, and residual conjuncts keep their slots, which
/// index the joined (left ++ right) row.
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
        }
        | PhysicalPlan::MergeJoin {
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
        PhysicalPlan::Window {
            source, functions, ..
        } => Ok(output_width(source, catalog)? + functions.len()),
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
        | PhysicalPlan::LockRows { output_schema, .. }
        | PhysicalPlan::Aggregate { output_schema, .. }
        | PhysicalPlan::Values { output_schema, .. }
        | PhysicalPlan::TableFunction { output_schema, .. } => Ok(output_schema.len()),
        PhysicalPlan::CreateSchema { .. }
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
        let residual = residual_filter(filter_expr.clone(), &candidate.consumed);
        let index_scan = PhysicalPlan::IndexScan {
            table,
            table_name: table_name.clone(),
            index,
            range: candidate.range,
            full_filter,
            filter: residual,
        };
        // Second cost-based decision (docs/specs/statistics.md §9.2): with
        // statistics, a high-selectivity index scan (one random heap fetch
        // per match) can lose to one sequential pass. Without statistics the
        // historical always-index rule is preserved exactly.
        if let Some(statistics) = catalog.get_table_statistics(table)? {
            let matches = crate::estimate::estimated_rows(&index_scan, catalog);
            let seq = crate::estimate::seq_scan_cost(statistics.page_count, statistics.row_count);
            let index_cost = crate::estimate::index_scan_cost(matches, statistics.row_count);
            if seq < index_cost {
                return Ok(PhysicalPlan::SeqScan {
                    table,
                    table_name,
                    filter: Some(filter_expr),
                });
            }
        }
        return Ok(index_scan);
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
    let mut candidates: Vec<(IndexId, bool, KeyCandidate)> = Vec::new();

    if let Some(leading) = schema.primary_key.first().copied()
        && let Some(candidate) = best_key_candidate(filter, leading)
    {
        candidates.push((PRIMARY_KEY_INDEX_ID, true, candidate));
    }

    for index in catalog.list_indexes_for_table(table)? {
        if let Some(leading) = index.columns.first().copied()
            && let Some(candidate) = best_key_candidate(filter, leading)
        {
            candidates.push((
                index.id,
                index.constraint.is_some() && index.columns == schema.primary_key,
                candidate,
            ));
        }
    }

    Ok(candidates
        .into_iter()
        .max_by(|(a_id, a_constraint, a), (b_id, b_constraint, b)| {
            a.exact
                .cmp(&b.exact)
                .then_with(|| a_constraint.cmp(b_constraint))
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
        } => match expr {
            BoundExpr::Literal { value, .. } => Some(value.clone()),
            _ => None,
        },
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
