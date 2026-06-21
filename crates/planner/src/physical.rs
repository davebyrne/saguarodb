use std::ops::Bound;

use common::{
    ColumnId, ColumnInfo, IndexId, Key, KeyRange, PRIMARY_KEY_INDEX_ID, ParsedColumnDef, Result,
    TableId, Value,
};

use crate::{AggregateExpr, BinOp, BoundExpr, BoundOrderByItem, JoinType, LogicalPlan};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhysicalPlan {
    CreateTable {
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    },
    DropTable {
        table: TableId,
    },
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: Box<PhysicalPlan>,
    },
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: Box<PhysicalPlan>,
    },
    Delete {
        table: TableId,
        source: Box<PhysicalPlan>,
    },
    SeqScan {
        table: TableId,
        table_name: String,
        filter: Option<BoundExpr>,
    },
    IndexScan {
        table: TableId,
        table_name: String,
        index: IndexId,
        range: KeyRange,
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
}

pub fn physical_plan(
    logical: &LogicalPlan,
    catalog: &dyn catalog::CatalogManager,
) -> Result<PhysicalPlan> {
    match logical {
        LogicalPlan::CreateTable {
            name,
            columns,
            primary_key,
        } => Ok(PhysicalPlan::CreateTable {
            name: name.clone(),
            columns: columns.clone(),
            primary_key: primary_key.clone(),
        }),
        LogicalPlan::DropTable { table } => Ok(PhysicalPlan::DropTable { table: *table }),
        LogicalPlan::Insert {
            table,
            columns,
            source,
        } => Ok(PhysicalPlan::Insert {
            table: *table,
            columns: columns.clone(),
            source: Box::new(physical_plan(source, catalog)?),
        }),
        LogicalPlan::Update {
            table,
            assignments,
            source,
        } => Ok(PhysicalPlan::Update {
            table: *table,
            assignments: assignments.clone(),
            source: Box::new(physical_plan(source, catalog)?),
        }),
        LogicalPlan::Delete { table, source } => Ok(PhysicalPlan::Delete {
            table: *table,
            source: Box::new(physical_plan(source, catalog)?),
        }),
        LogicalPlan::Scan { table, filter } => plan_scan(*table, filter.clone(), catalog),
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => plan_join(left, right, condition, *join_type, catalog),
        LogicalPlan::Filter { source, predicate } => Ok(PhysicalPlan::Filter {
            source: Box::new(physical_plan(source, catalog)?),
            predicate: predicate.clone(),
        }),
        LogicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => Ok(PhysicalPlan::Projection {
            source: Box::new(physical_plan(source, catalog)?),
            expressions: expressions.clone(),
            output_schema: output_schema.clone(),
        }),
        LogicalPlan::Sort { source, order_by } => Ok(PhysicalPlan::Sort {
            source: Box::new(physical_plan(source, catalog)?),
            order_by: order_by.clone(),
        }),
        LogicalPlan::Limit {
            source,
            count,
            offset,
        } => Ok(PhysicalPlan::Limit {
            source: Box::new(physical_plan(source, catalog)?),
            count: *count,
            offset: *offset,
        }),
        LogicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => Ok(PhysicalPlan::Aggregate {
            source: Box::new(physical_plan(source, catalog)?),
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
    }
}

fn plan_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    condition: &Option<BoundExpr>,
    join_type: JoinType,
    catalog: &dyn catalog::CatalogManager,
) -> Result<PhysicalPlan> {
    let left_plan = physical_plan(left, catalog)?;
    let right_plan = physical_plan(right, catalog)?;

    // An inner join whose ON predicate is a conjunction of column equalities,
    // each comparing a left column to a right column, can run as a hash join.
    // Anything else (outer/cross joins, non-equi or expression predicates)
    // falls back to the nested-loop join.
    if join_type == JoinType::Inner
        && let Some(condition) = condition
    {
        let left_width = output_width(&left_plan, catalog)?;
        if let Some((left_keys, right_keys)) = equi_join_keys(condition, left_width) {
            return Ok(PhysicalPlan::HashJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                left_keys,
                right_keys,
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

/// Extracts paired join-key slots from an inner-join predicate, returning
/// `None` if any conjunct is not a `left_column = right_column` equality.
/// Returned slots are relative to each child row: left slots as-is, right
/// slots rebased by `left_width` (join children are left-deep, so a child
/// row's column positions match its global slots).
fn equi_join_keys(condition: &BoundExpr, left_width: usize) -> Option<(Vec<usize>, Vec<usize>)> {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    if collect_equi_keys(condition, left_width, &mut left_keys, &mut right_keys) {
        Some((left_keys, right_keys))
    } else {
        None
    }
}

fn collect_equi_keys(
    expr: &BoundExpr,
    left_width: usize,
    left_keys: &mut Vec<usize>,
    right_keys: &mut Vec<usize>,
) -> bool {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } => {
            collect_equi_keys(left, left_width, left_keys, right_keys)
                && collect_equi_keys(right, left_width, left_keys, right_keys)
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
                true
            }
            None => false,
        },
        _ => false,
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
        BoundExpr::InputRef { slot, .. } => Some(*slot),
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
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. } => {
            Ok(output_width(left, catalog)? + output_width(right, catalog)?)
        }
        PhysicalPlan::Filter { source, .. }
        | PhysicalPlan::Sort { source, .. }
        | PhysicalPlan::Limit { source, .. } => output_width(source, catalog),
        PhysicalPlan::Projection { output_schema, .. }
        | PhysicalPlan::Aggregate { output_schema, .. }
        | PhysicalPlan::Values { output_schema, .. } => Ok(output_schema.len()),
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. } => Err(common::DbError::internal(
            "DML and DDL plans have no row output width",
        )),
    }
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

    let Some(primary_key) = schema.primary_key.first().copied() else {
        return Ok(PhysicalPlan::SeqScan {
            table,
            table_name,
            filter,
        });
    };

    if let Some(filter_expr) = filter {
        if let Some(candidate) = best_key_candidate(&filter_expr, primary_key) {
            let residual = residual_filter(filter_expr, &candidate.consumed);
            return Ok(PhysicalPlan::IndexScan {
                table,
                table_name: table_name.clone(),
                index: PRIMARY_KEY_INDEX_ID,
                range: candidate.range,
                filter: residual,
            });
        }
        Ok(PhysicalPlan::SeqScan {
            table,
            table_name,
            filter: Some(filter_expr),
        })
    } else {
        Ok(PhysicalPlan::SeqScan {
            table,
            table_name,
            filter: None,
        })
    }
}

#[derive(Clone)]
struct KeyCandidate {
    range: KeyRange,
    consumed: BoundExpr,
    exact: bool,
}

fn best_key_candidate(expr: &BoundExpr, primary_key: ColumnId) -> Option<KeyCandidate> {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } => match (
            best_key_candidate(left, primary_key),
            best_key_candidate(right, primary_key),
        ) {
            (Some(left), Some(right)) if right.exact && !left.exact => Some(right),
            (Some(left), _) => Some(left),
            (None, right) => right,
        },
        _ => key_candidate_from_comparison(expr, primary_key),
    }
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
        consumed: expr.clone(),
    })
}

fn residual_filter(expr: BoundExpr, consumed: &BoundExpr) -> Option<BoundExpr> {
    if &expr == consumed {
        return None;
    }
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
        other => Some(other),
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
            value: Value::Integer(_) | Value::Text(_) | Value::Boolean(_),
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
