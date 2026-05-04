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
        } => Ok(PhysicalPlan::NestedLoopJoin {
            left: Box::new(physical_plan(left, catalog)?),
            right: Box::new(physical_plan(right, catalog)?),
            condition: condition.clone(),
            join_type: *join_type,
        }),
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
