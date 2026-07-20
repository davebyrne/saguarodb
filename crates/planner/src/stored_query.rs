use std::collections::BTreeMap;

use catalog::{CatalogManager, SystemView};
use common::{
    BindingId, ColumnDef, ColumnInfo, DbError, FunctionId, Result, STORED_QUERY_VERSION, SqlState,
    StoredColumnReference, StoredCorrelatedColumn, StoredDistinct, StoredFrom, StoredJoinType,
    StoredOrderBy, StoredQueryBinOp, StoredQueryBody, StoredQueryColumn, StoredQueryExpr,
    StoredQueryUnaryOp, StoredQueryV1, StoredRangeColumn, StoredRowLock, StoredSelect,
    StoredSelectItem, StoredSetOp, StoredSetOperator, StoredTupleLockMode,
    StoredTupleLockWaitPolicy, StoredValues,
};
use parser::SetOp;

use crate::{
    AggregateFunc, BinOp, BoundDistinct, BoundExpr, BoundFrom, BoundOrderByItem, BoundQuery,
    BoundQueryBody, BoundRowLock, BoundSelect, BoundSelectItem, BoundSetOp, BoundValues,
    CorrelatedColumn, JoinType, UnaryOp,
};

#[derive(Clone)]
enum StoredRangeKind {
    Catalog(BTreeMap<u16, u32>),
    Position,
}

#[derive(Clone)]
struct StoredRangeInfo {
    kind: StoredRangeKind,
}

pub fn store_bound_query(query: &BoundQuery) -> Result<StoredQueryV1> {
    store_query(query, None)
}

fn store_query(
    query: &BoundQuery,
    parent: Option<&BTreeMap<BindingId, StoredRangeInfo>>,
) -> Result<StoredQueryV1> {
    let mut ranges = BTreeMap::new();
    if let BoundQueryBody::Select(select) = &query.body
        && let Some(from) = &select.from
    {
        collect_stored_ranges(from, &mut ranges)?;
    }
    let body = match &query.body {
        BoundQueryBody::Select(select) => {
            StoredQueryBody::Select(Box::new(store_select(select, &ranges)?))
        }
        BoundQueryBody::Values(values) => StoredQueryBody::Values(store_values(values, &ranges)?),
        BoundQueryBody::SetOp(set_op) => StoredQueryBody::SetOp(store_set_op(set_op, parent)?),
    };
    Ok(StoredQueryV1 {
        version: STORED_QUERY_VERSION,
        body,
        order_by: query
            .order_by
            .iter()
            .map(|item| store_order_by(item, &ranges))
            .collect::<Result<_>>()?,
        limit: query.limit,
        offset: query.offset,
        row_lock: query.row_lock.as_ref().map(store_row_lock),
        correlations: query
            .correlations
            .iter()
            .map(|column| store_correlation(column, parent.unwrap_or(&ranges)))
            .collect::<Result<_>>()?,
    })
}

fn collect_stored_ranges(
    from: &BoundFrom,
    ranges: &mut BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<()> {
    match from {
        BoundFrom::Table {
            binding, schema, ..
        } => {
            let columns = schema
                .iter()
                .map(|column| (column.id, column.object_id))
                .collect();
            insert_range(ranges, *binding, StoredRangeKind::Catalog(columns))?;
        }
        BoundFrom::System { binding, .. }
        | BoundFrom::Derived { binding, .. }
        | BoundFrom::View { binding, .. }
        | BoundFrom::TableFunction { binding, .. } => {
            insert_range(ranges, *binding, StoredRangeKind::Position)?;
        }
        BoundFrom::Join { left, right, .. } => {
            collect_stored_ranges(left, ranges)?;
            collect_stored_ranges(right, ranges)?;
        }
    }
    Ok(())
}

fn insert_range(
    ranges: &mut BTreeMap<BindingId, StoredRangeInfo>,
    range: BindingId,
    kind: StoredRangeKind,
) -> Result<()> {
    if ranges.insert(range, StoredRangeInfo { kind }).is_some() {
        return Err(DbError::internal(
            "duplicate binding in resolved view query",
        ));
    }
    Ok(())
}

fn store_select(
    select: &BoundSelect,
    ranges: &BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<StoredSelect> {
    Ok(StoredSelect {
        distinct: select
            .distinct
            .as_ref()
            .map(|distinct| match distinct {
                BoundDistinct::All => Ok(StoredDistinct::All),
                BoundDistinct::On(exprs) => Ok(StoredDistinct::On(store_exprs(exprs, ranges)?)),
            })
            .transpose()?,
        columns: select
            .columns
            .iter()
            .map(|item| {
                Ok(StoredSelectItem {
                    expr: store_expr(&item.expr, ranges)?,
                    alias: item.alias.clone(),
                })
            })
            .collect::<Result<_>>()?,
        from: select
            .from
            .as_ref()
            .map(|from| store_from(from, ranges))
            .transpose()?,
        filter: select
            .filter
            .as_ref()
            .map(|expr| store_expr(expr, ranges))
            .transpose()?,
        group_by: store_exprs(&select.group_by, ranges)?,
        having: select
            .having
            .as_ref()
            .map(|expr| store_expr(expr, ranges))
            .transpose()?,
        output_schema: store_output_schema(&select.output_schema),
    })
}

fn store_values(
    values: &BoundValues,
    ranges: &BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<StoredValues> {
    Ok(StoredValues {
        rows: values
            .rows
            .iter()
            .map(|row| store_exprs(row, ranges))
            .collect::<Result<_>>()?,
        output_schema: store_output_schema(&values.output_schema),
    })
}

fn store_set_op(
    set_op: &BoundSetOp,
    parent: Option<&BTreeMap<BindingId, StoredRangeInfo>>,
) -> Result<StoredSetOp> {
    Ok(StoredSetOp {
        op: match set_op.op {
            SetOp::Union => StoredSetOperator::Union,
            SetOp::Intersect => StoredSetOperator::Intersect,
            SetOp::Except => StoredSetOperator::Except,
        },
        all: set_op.all,
        left: Box::new(store_query(&set_op.left, parent)?),
        right: Box::new(store_query(&set_op.right, parent)?),
        output_schema: store_output_schema(&set_op.output_schema),
    })
}

fn store_from(
    from: &BoundFrom,
    ranges: &BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<StoredFrom> {
    match from {
        BoundFrom::Table {
            table,
            binding,
            alias,
            ..
        } => Ok(StoredFrom::Table {
            table: *table,
            range: *binding,
            alias: alias.clone(),
        }),
        BoundFrom::System {
            view,
            binding,
            alias,
            schema,
        } => Ok(StoredFrom::System {
            relation_oid: view.relation_oid(),
            range: *binding,
            alias: alias.clone(),
            schema: store_range_schema(schema),
        }),
        BoundFrom::Derived {
            query,
            binding,
            alias,
            schema,
            lateral,
        } => {
            let query = store_query(query, Some(ranges))?;
            let schema = store_derived_range_schema(schema, &query)?;
            Ok(StoredFrom::Derived {
                query: Box::new(query),
                range: *binding,
                alias: alias.clone(),
                schema,
                lateral: *lateral,
            })
        }
        BoundFrom::View {
            query,
            binding,
            alias,
            schema,
            ..
        } => {
            let query = store_query(query, Some(ranges))?;
            let schema = store_derived_range_schema(schema, &query)?;
            Ok(StoredFrom::Derived {
                query: Box::new(query),
                range: *binding,
                alias: alias.clone(),
                schema,
                lateral: false,
            })
        }
        BoundFrom::TableFunction {
            name,
            args,
            binding,
            alias,
            schema,
        } => Ok(StoredFrom::TableFunction {
            function: table_function_id(name)?,
            args: store_exprs(args, ranges)?,
            range: *binding,
            alias: alias.clone(),
            schema: store_range_schema(schema),
        }),
        BoundFrom::Join {
            left,
            right,
            join_type,
            condition,
        } => Ok(StoredFrom::Join {
            left: Box::new(store_from(left, ranges)?),
            right: Box::new(store_from(right, ranges)?),
            join_type: store_join_type(*join_type)?,
            condition: condition
                .as_ref()
                .map(|expr| store_expr(expr, ranges))
                .transpose()?,
        }),
    }
}

fn store_output_schema(columns: &[ColumnInfo]) -> Vec<StoredQueryColumn> {
    columns
        .iter()
        .map(|column| StoredQueryColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            pg_type: column.wire_type(),
        })
        .collect()
}

fn store_range_schema(columns: &[ColumnDef]) -> Vec<StoredRangeColumn> {
    columns
        .iter()
        .map(|column| StoredRangeColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            pg_type: column.wire_type(),
            nullable: column.nullable,
        })
        .collect()
}

fn store_derived_range_schema(
    columns: &[ColumnDef],
    query: &StoredQueryV1,
) -> Result<Vec<StoredRangeColumn>> {
    let nullability = query.output_nullability()?;
    if columns.len() != nullability.len() || columns.len() != query.output_schema().len() {
        return Err(DbError::internal(
            "bound derived range width does not match its query output",
        ));
    }
    Ok(columns
        .iter()
        .zip(query.output_schema().iter().zip(nullability))
        .map(|(column, (output, nullable))| StoredRangeColumn {
            name: column.name.clone(),
            data_type: output.data_type.clone(),
            pg_type: output.pg_type.clone(),
            nullable,
        })
        .collect())
}

fn store_order_by(
    item: &BoundOrderByItem,
    ranges: &BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<StoredOrderBy> {
    Ok(StoredOrderBy {
        expr: store_expr(&item.expr, ranges)?,
        ascending: item.ascending,
        nulls_first: item.nulls_first,
    })
}

fn store_correlation(
    column: &CorrelatedColumn,
    ranges: &BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<StoredCorrelatedColumn> {
    Ok(StoredCorrelatedColumn {
        outer: store_expr(&column.outer, ranges)?,
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    })
}

fn store_row_lock(lock: &BoundRowLock) -> StoredRowLock {
    StoredRowLock {
        table: lock.table,
        mode: match lock.mode {
            common::TupleLockMode::KeyShare => StoredTupleLockMode::KeyShare,
            common::TupleLockMode::Share => StoredTupleLockMode::Share,
            common::TupleLockMode::NoKeyUpdate => StoredTupleLockMode::NoKeyUpdate,
            common::TupleLockMode::Update => StoredTupleLockMode::Update,
        },
        wait_policy: match lock.wait_policy {
            common::TupleLockWaitPolicy::Block => StoredTupleLockWaitPolicy::Block,
            common::TupleLockWaitPolicy::NoWait => StoredTupleLockWaitPolicy::NoWait,
            common::TupleLockWaitPolicy::SkipLocked => StoredTupleLockWaitPolicy::SkipLocked,
        },
    }
}

fn store_join_type(join_type: JoinType) -> Result<StoredJoinType> {
    match join_type {
        JoinType::Inner => Ok(StoredJoinType::Inner),
        JoinType::Left => Ok(StoredJoinType::Left),
        JoinType::Right => Ok(StoredJoinType::Right),
        JoinType::Full => Ok(StoredJoinType::Full),
        JoinType::Cross => Ok(StoredJoinType::Cross),
        JoinType::Semi | JoinType::Anti => Err(DbError::internal(
            "planner-only join in resolved view query",
        )),
    }
}

fn store_exprs(
    exprs: &[BoundExpr],
    ranges: &BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<Vec<StoredQueryExpr>> {
    exprs.iter().map(|expr| store_expr(expr, ranges)).collect()
}

fn store_expr(
    expr: &BoundExpr,
    ranges: &BTreeMap<BindingId, StoredRangeInfo>,
) -> Result<StoredQueryExpr> {
    let stored = match expr {
        BoundExpr::Literal {
            value,
            data_type,
            nullable,
        } => StoredQueryExpr::Literal {
            value: value.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Parameter { .. } => {
            return Err(DbError::internal("parameter in resolved view query"));
        }
        BoundExpr::WindowCall { .. } => {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "window functions are not supported in CREATE VIEW",
            ));
        }
        BoundExpr::InputRef {
            input,
            column,
            data_type,
            nullable,
            ..
        } => {
            let range = ranges.get(input).ok_or_else(|| {
                DbError::internal("view expression references an unknown binding")
            })?;
            let column = match &range.kind {
                StoredRangeKind::Catalog(columns) => {
                    StoredColumnReference::Catalog(*columns.get(column).ok_or_else(|| {
                        DbError::internal("view expression references an unknown table column")
                    })?)
                }
                StoredRangeKind::Position => StoredColumnReference::Position(u32::from(*column)),
            };
            StoredQueryExpr::InputRef {
                range: *input,
                column,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            nullable,
        } => StoredQueryExpr::Binary {
            left: Box::new(store_expr(left, ranges)?),
            op: store_binop(*op),
            right: Box::new(store_expr(right, ranges)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::UnaryOp {
            op,
            expr,
            data_type,
            nullable,
        } => StoredQueryExpr::Unary {
            op: store_unary(*op),
            expr: Box::new(store_expr(expr, ranges)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Function {
            name,
            args,
            data_type,
            pg_type,
            nullable,
        } => StoredQueryExpr::Function {
            function: common::scalar_function_id(
                name,
                &args.iter().map(BoundExpr::data_type).collect::<Vec<_>>(),
                data_type,
            )
            .ok_or_else(|| {
                DbError::internal(format!("view uses unregistered scalar function {name}"))
            })?,
            args: store_exprs(args, ranges)?,
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => StoredQueryExpr::Array {
            elements: store_exprs(elements, ranges)?,
            dimensions: dimensions.clone(),
            element_type: element_type.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => StoredQueryExpr::ArraySubscript {
            array: Box::new(store_expr(array, ranges)?),
            subscripts: store_exprs(subscripts, ranges)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => StoredQueryExpr::Any {
            left: Box::new(store_expr(left, ranges)?),
            op: store_binop(*op),
            array: Box::new(store_expr(array, ranges)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Nextval {
            sequence,
            data_type,
            nullable,
        } => StoredQueryExpr::Nextval {
            sequence: *sequence,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Currval {
            sequence,
            data_type,
            nullable,
        } => StoredQueryExpr::Currval {
            sequence: *sequence,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Setval {
            sequence,
            value,
            is_called,
            data_type,
            nullable,
        } => StoredQueryExpr::Setval {
            sequence: *sequence,
            value: Box::new(store_expr(value, ranges)?),
            is_called: is_called
                .as_ref()
                .map(|expr| store_expr(expr, ranges).map(Box::new))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::AggregateCall {
            func,
            arg,
            distinct,
            data_type,
            nullable,
        } => StoredQueryExpr::Aggregate {
            function: aggregate_function_id(*func),
            arg: arg
                .as_ref()
                .map(|expr| store_expr(expr, ranges).map(Box::new))
                .transpose()?,
            distinct: *distinct,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::LocalRef {
            slot,
            data_type,
            nullable,
        } => StoredQueryExpr::LocalRef {
            output: u32::try_from(*slot)
                .map_err(|_| DbError::internal("view local reference exceeds durable range"))?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::OuterRef {
            slot,
            data_type,
            nullable,
        } => StoredQueryExpr::OuterRef {
            correlation: u32::try_from(*slot)
                .map_err(|_| DbError::internal("view outer reference exceeds durable range"))?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => StoredQueryExpr::IsNull {
            expr: Box::new(store_expr(expr, ranges)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => StoredQueryExpr::IsNotNull {
            expr: Box::new(store_expr(expr, ranges)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => StoredQueryExpr::InList {
            expr: Box::new(store_expr(expr, ranges)?),
            list: store_exprs(list, ranges)?,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::RuntimeInSet { .. } => {
            return Err(DbError::internal("runtime set in resolved view query"));
        }
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => StoredQueryExpr::Between {
            expr: Box::new(store_expr(expr, ranges)?),
            low: Box::new(store_expr(low, ranges)?),
            high: Box::new(store_expr(high, ranges)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape,
            data_type,
            nullable,
        } => StoredQueryExpr::Like {
            expr: Box::new(store_expr(expr, ranges)?),
            pattern: Box::new(store_expr(pattern, ranges)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape: *escape,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable,
        } => StoredQueryExpr::Case {
            operand: operand
                .as_ref()
                .map(|expr| store_expr(expr, ranges).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| Ok((store_expr(when, ranges)?, store_expr(then, ranges)?)))
                .collect::<Result<_>>()?,
            else_clause: else_clause
                .as_ref()
                .map(|expr| store_expr(expr, ranges).map(Box::new))
                .transpose()?,
            flow_sensitive_nullable: when_clauses.is_empty()
                || *nullable
                    != (else_clause.is_none()
                        || when_clauses.iter().any(|(_, then)| then.nullable())
                        || else_clause.as_deref().is_some_and(BoundExpr::nullable)),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => StoredQueryExpr::Cast {
            expr: Box::new(store_expr(expr, ranges)?),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::ScalarSubquery {
            query,
            data_type,
            nullable,
        } => StoredQueryExpr::ScalarSubquery {
            query: Box::new(store_query(query, Some(ranges))?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Exists {
            query,
            negated,
            data_type,
            nullable,
        } => StoredQueryExpr::Exists {
            query: Box::new(store_query(query, Some(ranges))?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
            data_type,
            nullable,
        } => StoredQueryExpr::InSubquery {
            expr: Box::new(store_expr(expr, ranges)?),
            query: Box::new(store_query(query, Some(ranges))?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
    };
    Ok(stored)
}

fn store_binop(op: BinOp) -> StoredQueryBinOp {
    match op {
        BinOp::Add => StoredQueryBinOp::Add,
        BinOp::Sub => StoredQueryBinOp::Sub,
        BinOp::Mul => StoredQueryBinOp::Mul,
        BinOp::Div => StoredQueryBinOp::Div,
        BinOp::Mod => StoredQueryBinOp::Mod,
        BinOp::Eq => StoredQueryBinOp::Eq,
        BinOp::Neq => StoredQueryBinOp::Neq,
        BinOp::Lt => StoredQueryBinOp::Lt,
        BinOp::LtEq => StoredQueryBinOp::LtEq,
        BinOp::Gt => StoredQueryBinOp::Gt,
        BinOp::GtEq => StoredQueryBinOp::GtEq,
        BinOp::And => StoredQueryBinOp::And,
        BinOp::Or => StoredQueryBinOp::Or,
        BinOp::Concat => StoredQueryBinOp::Concat,
        BinOp::IsDistinctFrom => StoredQueryBinOp::IsDistinctFrom,
        BinOp::IsNotDistinctFrom => StoredQueryBinOp::IsNotDistinctFrom,
    }
}

fn store_unary(op: UnaryOp) -> StoredQueryUnaryOp {
    match op {
        UnaryOp::Neg => StoredQueryUnaryOp::Neg,
        UnaryOp::Not => StoredQueryUnaryOp::Not,
    }
}

fn table_function_id(name: &str) -> Result<FunctionId> {
    match name {
        "unnest" => Ok(common::UNNEST_FUNCTION_ID),
        "generate_series" => Ok(common::GENERATE_SERIES_FUNCTION_ID),
        _ => Err(DbError::internal(format!(
            "unsupported table function {name} in view"
        ))),
    }
}

fn aggregate_function_id(func: AggregateFunc) -> FunctionId {
    match func {
        AggregateFunc::Count => 2_147,
        AggregateFunc::Sum => 2_107,
        AggregateFunc::Avg => 2_100,
        AggregateFunc::Min => 2_131,
        AggregateFunc::Max => 2_115,
        AggregateFunc::StddevSamp => 2_713,
        AggregateFunc::StddevPop => 2_712,
        AggregateFunc::VarSamp => 2_644,
        AggregateFunc::VarPop => 2_643,
        AggregateFunc::BoolAnd => 2_517,
        AggregateFunc::BoolOr => 2_518,
        AggregateFunc::ArrayAgg => 2_335,
        AggregateFunc::StringAgg => 3_538,
    }
}

fn aggregate_function(id: FunctionId) -> Option<AggregateFunc> {
    Some(match id {
        2_147 => AggregateFunc::Count,
        2_107 => AggregateFunc::Sum,
        2_100 => AggregateFunc::Avg,
        2_131 => AggregateFunc::Min,
        2_115 => AggregateFunc::Max,
        2_713 => AggregateFunc::StddevSamp,
        2_712 => AggregateFunc::StddevPop,
        2_644 => AggregateFunc::VarSamp,
        2_643 => AggregateFunc::VarPop,
        2_517 => AggregateFunc::BoolAnd,
        2_518 => AggregateFunc::BoolOr,
        2_335 => AggregateFunc::ArrayAgg,
        3_538 => AggregateFunc::StringAgg,
        _ => return None,
    })
}

#[derive(Clone)]
struct LowerRangeInfo {
    slot_start: usize,
    columns: Vec<ColumnDef>,
    catalog_columns: bool,
    null_extended: bool,
}

pub fn lower_stored_query(
    catalog: &dyn CatalogManager,
    query: &StoredQueryV1,
) -> Result<BoundQuery> {
    lower_query(catalog, query, None, 0)
}

fn lower_query(
    catalog: &dyn CatalogManager,
    query: &StoredQueryV1,
    parent: Option<&BTreeMap<BindingId, LowerRangeInfo>>,
    depth: usize,
) -> Result<BoundQuery> {
    if query.version != STORED_QUERY_VERSION {
        return Err(DbError::internal(format!(
            "unsupported stored query version {}",
            query.version
        )));
    }
    if depth >= common::MAX_STORED_QUERY_DEPTH {
        return Err(DbError::internal("stored query exceeds nesting limit"));
    }
    let mut ranges = BTreeMap::new();
    let body = match &query.body {
        StoredQueryBody::Select(select) => {
            let mut source_width = 0;
            let from = select
                .from
                .as_ref()
                .map(|from| lower_from(catalog, from, &mut ranges, &mut source_width, depth + 1))
                .transpose()?;
            BoundQueryBody::Select(Box::new(BoundSelect {
                source_width,
                distinct: select
                    .distinct
                    .as_ref()
                    .map(|distinct| match distinct {
                        StoredDistinct::All => Ok(BoundDistinct::All),
                        StoredDistinct::On(exprs) => Ok(BoundDistinct::On(lower_exprs(
                            catalog,
                            exprs,
                            &ranges,
                            depth + 1,
                        )?)),
                    })
                    .transpose()?,
                columns: select
                    .columns
                    .iter()
                    .map(|item| {
                        Ok(BoundSelectItem {
                            expr: lower_expr(catalog, &item.expr, &ranges, depth + 1)?,
                            alias: item.alias.clone(),
                        })
                    })
                    .collect::<Result<_>>()?,
                from,
                filter: select
                    .filter
                    .as_ref()
                    .map(|expr| lower_expr(catalog, expr, &ranges, depth + 1))
                    .transpose()?,
                group_by: lower_exprs(catalog, &select.group_by, &ranges, depth + 1)?,
                having: select
                    .having
                    .as_ref()
                    .map(|expr| lower_expr(catalog, expr, &ranges, depth + 1))
                    .transpose()?,
                output_schema: lower_output_schema(&select.output_schema),
            }))
        }
        StoredQueryBody::Values(values) => BoundQueryBody::Values(BoundValues {
            rows: values
                .rows
                .iter()
                .map(|row| lower_exprs(catalog, row, &ranges, depth + 1))
                .collect::<Result<_>>()?,
            output_schema: lower_output_schema(&values.output_schema),
        }),
        StoredQueryBody::SetOp(set_op) => BoundQueryBody::SetOp(BoundSetOp {
            op: match set_op.op {
                StoredSetOperator::Union => SetOp::Union,
                StoredSetOperator::Intersect => SetOp::Intersect,
                StoredSetOperator::Except => SetOp::Except,
            },
            all: set_op.all,
            left: Box::new(lower_query(catalog, &set_op.left, parent, depth + 1)?),
            right: Box::new(lower_query(catalog, &set_op.right, parent, depth + 1)?),
            output_schema: lower_output_schema(&set_op.output_schema),
        }),
    };
    let correlation_ranges = parent.unwrap_or(&ranges);
    Ok(BoundQuery {
        body,
        order_by: query
            .order_by
            .iter()
            .map(|item| {
                Ok(BoundOrderByItem {
                    expr: lower_expr(catalog, &item.expr, &ranges, depth + 1)?,
                    ascending: item.ascending,
                    nulls_first: item.nulls_first,
                })
            })
            .collect::<Result<_>>()?,
        limit: query.limit,
        offset: query.offset,
        row_lock: query.row_lock.as_ref().map(lower_row_lock),
        correlations: query
            .correlations
            .iter()
            .map(|column| {
                Ok(CorrelatedColumn {
                    outer: lower_expr(catalog, &column.outer, correlation_ranges, depth + 1)?,
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
            })
            .collect::<Result<_>>()?,
    })
}

fn lower_from(
    catalog: &dyn CatalogManager,
    from: &StoredFrom,
    ranges: &mut BTreeMap<BindingId, LowerRangeInfo>,
    next_slot: &mut usize,
    depth: usize,
) -> Result<BoundFrom> {
    match from {
        StoredFrom::Table {
            table,
            range,
            alias,
        } => {
            let schema = catalog.get_table(*table)?.ok_or_else(|| {
                DbError::internal(format!("stored view references unknown table {table}"))
            })?;
            add_lower_range(ranges, *range, *next_slot, schema.columns.clone(), true)?;
            *next_slot = next_slot
                .checked_add(schema.columns.len())
                .ok_or_else(|| DbError::internal("stored view slot count overflow"))?;
            Ok(BoundFrom::Table {
                table: *table,
                binding: *range,
                name: schema.name,
                alias: alias.clone(),
                schema: schema.columns,
            })
        }
        StoredFrom::System {
            relation_oid,
            range,
            alias,
            schema: stored_schema,
        } => {
            let view = SystemView::from_relation_oid(*relation_oid).ok_or_else(|| {
                DbError::internal(format!(
                    "stored view references unknown system relation {relation_oid}"
                ))
            })?;
            let schema = view.columns();
            if store_range_schema(&schema) != *stored_schema {
                return Err(DbError::internal(format!(
                    "stored view system relation {relation_oid} schema does not match catalog"
                )));
            }
            add_lower_range(ranges, *range, *next_slot, schema.clone(), false)?;
            *next_slot = next_slot
                .checked_add(schema.len())
                .ok_or_else(|| DbError::internal("stored view slot count overflow"))?;
            Ok(BoundFrom::System {
                view,
                binding: *range,
                alias: alias.clone(),
                schema,
            })
        }
        StoredFrom::Derived {
            query,
            range,
            alias,
            schema,
            lateral,
        } => {
            let columns = lower_range_schema(schema)?;
            let query = lower_query(catalog, query, Some(ranges), depth)?;
            add_lower_range(ranges, *range, *next_slot, columns.clone(), false)?;
            *next_slot = next_slot
                .checked_add(columns.len())
                .ok_or_else(|| DbError::internal("stored view slot count overflow"))?;
            Ok(BoundFrom::Derived {
                query: Box::new(query),
                binding: *range,
                alias: alias.clone(),
                schema: columns,
                lateral: *lateral,
            })
        }
        StoredFrom::TableFunction {
            function,
            args,
            range,
            alias,
            schema,
        } => {
            let name = table_function_name(*function)?;
            let columns = lower_range_schema(schema)?;
            let args = lower_exprs(catalog, args, ranges, depth)?;
            add_lower_range(ranges, *range, *next_slot, columns.clone(), false)?;
            *next_slot = next_slot
                .checked_add(columns.len())
                .ok_or_else(|| DbError::internal("stored view slot count overflow"))?;
            Ok(BoundFrom::TableFunction {
                name: name.to_string(),
                args,
                binding: *range,
                alias: alias.clone(),
                schema: columns,
            })
        }
        StoredFrom::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let bound_left = lower_from(catalog, left, ranges, next_slot, depth)?;
            let bound_right = lower_from(catalog, right, ranges, next_slot, depth)?;
            let condition = condition
                .as_ref()
                .map(|expr| lower_expr(catalog, expr, ranges, depth))
                .transpose()?;
            match join_type {
                StoredJoinType::Left => mark_lower_ranges_null_extended(right, ranges)?,
                StoredJoinType::Right => mark_lower_ranges_null_extended(left, ranges)?,
                StoredJoinType::Full => {
                    mark_lower_ranges_null_extended(left, ranges)?;
                    mark_lower_ranges_null_extended(right, ranges)?;
                }
                StoredJoinType::Inner | StoredJoinType::Cross => {}
            }
            Ok(BoundFrom::Join {
                left: Box::new(bound_left),
                right: Box::new(bound_right),
                join_type: lower_join_type(*join_type),
                condition,
            })
        }
    }
}

fn add_lower_range(
    ranges: &mut BTreeMap<BindingId, LowerRangeInfo>,
    range: BindingId,
    slot_start: usize,
    columns: Vec<ColumnDef>,
    catalog_columns: bool,
) -> Result<()> {
    if ranges
        .insert(
            range,
            LowerRangeInfo {
                slot_start,
                columns,
                catalog_columns,
                null_extended: false,
            },
        )
        .is_some()
    {
        return Err(DbError::internal("duplicate range in stored view"));
    }
    Ok(())
}

fn mark_lower_ranges_null_extended(
    from: &StoredFrom,
    ranges: &mut BTreeMap<BindingId, LowerRangeInfo>,
) -> Result<()> {
    match from {
        StoredFrom::Table { range, .. }
        | StoredFrom::System { range, .. }
        | StoredFrom::Derived { range, .. }
        | StoredFrom::TableFunction { range, .. } => {
            ranges
                .get_mut(range)
                .ok_or_else(|| DbError::internal("stored view join references unknown range"))?
                .null_extended = true;
        }
        StoredFrom::Join { left, right, .. } => {
            mark_lower_ranges_null_extended(left, ranges)?;
            mark_lower_ranges_null_extended(right, ranges)?;
        }
    }
    Ok(())
}

fn lower_range_schema(columns: &[StoredRangeColumn]) -> Result<Vec<ColumnDef>> {
    columns
        .iter()
        .enumerate()
        .map(|(position, column)| {
            Ok(ColumnDef {
                id: u16::try_from(position)
                    .map_err(|_| DbError::internal("stored view range is too wide"))?,
                object_id: 0,
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                max_length: None,
                default: None,
                pg_type: Some(column.pg_type.clone()),
            })
        })
        .collect()
}

fn lower_output_schema(columns: &[StoredQueryColumn]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .map(|column| ColumnInfo {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            table_id: None,
            column_id: None,
            pg_type: Some(column.pg_type.clone()),
        })
        .collect()
}

fn lower_row_lock(lock: &StoredRowLock) -> BoundRowLock {
    BoundRowLock {
        table: lock.table,
        mode: match lock.mode {
            StoredTupleLockMode::KeyShare => common::TupleLockMode::KeyShare,
            StoredTupleLockMode::Share => common::TupleLockMode::Share,
            StoredTupleLockMode::NoKeyUpdate => common::TupleLockMode::NoKeyUpdate,
            StoredTupleLockMode::Update => common::TupleLockMode::Update,
        },
        wait_policy: match lock.wait_policy {
            StoredTupleLockWaitPolicy::Block => common::TupleLockWaitPolicy::Block,
            StoredTupleLockWaitPolicy::NoWait => common::TupleLockWaitPolicy::NoWait,
            StoredTupleLockWaitPolicy::SkipLocked => common::TupleLockWaitPolicy::SkipLocked,
        },
    }
}

fn lower_join_type(join_type: StoredJoinType) -> JoinType {
    match join_type {
        StoredJoinType::Inner => JoinType::Inner,
        StoredJoinType::Left => JoinType::Left,
        StoredJoinType::Right => JoinType::Right,
        StoredJoinType::Full => JoinType::Full,
        StoredJoinType::Cross => JoinType::Cross,
    }
}

fn table_function_name(id: FunctionId) -> Result<&'static str> {
    match id {
        common::UNNEST_FUNCTION_ID => Ok("unnest"),
        common::GENERATE_SERIES_FUNCTION_ID => Ok("generate_series"),
        _ => Err(DbError::internal(format!(
            "stored view references unknown table function {id}"
        ))),
    }
}

fn lower_exprs(
    catalog: &dyn CatalogManager,
    exprs: &[StoredQueryExpr],
    ranges: &BTreeMap<BindingId, LowerRangeInfo>,
    depth: usize,
) -> Result<Vec<BoundExpr>> {
    exprs
        .iter()
        .map(|expr| lower_expr(catalog, expr, ranges, depth))
        .collect()
}

fn lower_expr(
    catalog: &dyn CatalogManager,
    expr: &StoredQueryExpr,
    ranges: &BTreeMap<BindingId, LowerRangeInfo>,
    depth: usize,
) -> Result<BoundExpr> {
    if depth >= common::MAX_STORED_QUERY_DEPTH {
        return Err(DbError::internal(
            "stored view expression exceeds nesting limit",
        ));
    }
    let bound = match expr {
        StoredQueryExpr::Literal {
            value,
            data_type,
            nullable,
        } => BoundExpr::Literal {
            value: value.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::InputRef {
            range,
            column,
            data_type,
            nullable,
        } => {
            let info = ranges.get(range).ok_or_else(|| {
                DbError::internal(format!("stored view references unknown range {range}"))
            })?;
            let dense = match column {
                StoredColumnReference::Catalog(object_id) if info.catalog_columns => info
                    .columns
                    .iter()
                    .position(|candidate| candidate.object_id == *object_id)
                    .ok_or_else(|| {
                        DbError::internal(format!(
                            "stored view references unknown column {object_id}"
                        ))
                    })?,
                StoredColumnReference::Position(position) if !info.catalog_columns => {
                    usize::try_from(*position).map_err(|_| {
                        DbError::internal("stored view column position exceeds platform range")
                    })?
                }
                _ => {
                    return Err(DbError::internal(
                        "stored view column reference kind does not match its range",
                    ));
                }
            };
            let column_def = info
                .columns
                .get(dense)
                .ok_or_else(|| DbError::internal("stored view column position is out of range"))?;
            if column_def.data_type != *data_type
                || *nullable != (column_def.nullable || info.null_extended)
            {
                return Err(DbError::internal(
                    "stored view column metadata does not match catalog",
                ));
            }
            BoundExpr::InputRef {
                input: *range,
                column: column_def.id,
                slot: info
                    .slot_start
                    .checked_add(dense)
                    .ok_or_else(|| DbError::internal("stored view slot overflow"))?,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredQueryExpr::Binary {
            left,
            op,
            right,
            data_type,
            nullable,
        } => BoundExpr::BinaryOp {
            left: Box::new(lower_expr(catalog, left, ranges, depth + 1)?),
            op: lower_binop(*op),
            right: Box::new(lower_expr(catalog, right, ranges, depth + 1)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Unary {
            op,
            expr,
            data_type,
            nullable,
        } => BoundExpr::UnaryOp {
            op: lower_unary(*op),
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Function {
            function,
            args,
            data_type,
            pg_type,
            nullable,
        } => {
            let (registered, _) =
                common::lookup_scalar_function_by_id(*function).ok_or_else(|| {
                    DbError::internal(format!(
                        "stored view references unknown scalar function {function}"
                    ))
                })?;
            BoundExpr::Function {
                name: registered.name.to_string(),
                args: lower_exprs(catalog, args, ranges, depth + 1)?,
                data_type: data_type.clone(),
                pg_type: pg_type.clone(),
                nullable: *nullable,
            }
        }
        StoredQueryExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => BoundExpr::Array {
            elements: lower_exprs(catalog, elements, ranges, depth + 1)?,
            dimensions: dimensions.clone(),
            element_type: element_type.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => BoundExpr::ArraySubscript {
            array: Box::new(lower_expr(catalog, array, ranges, depth + 1)?),
            subscripts: lower_exprs(catalog, subscripts, ranges, depth + 1)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => BoundExpr::Any {
            left: Box::new(lower_expr(catalog, left, ranges, depth + 1)?),
            op: lower_binop(*op),
            array: Box::new(lower_expr(catalog, array, ranges, depth + 1)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Nextval {
            sequence,
            data_type,
            nullable,
        } => {
            require_sequence(catalog, *sequence)?;
            BoundExpr::Nextval {
                sequence: *sequence,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredQueryExpr::Currval {
            sequence,
            data_type,
            nullable,
        } => {
            require_sequence(catalog, *sequence)?;
            BoundExpr::Currval {
                sequence: *sequence,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredQueryExpr::Setval {
            sequence,
            value,
            is_called,
            data_type,
            nullable,
        } => {
            require_sequence(catalog, *sequence)?;
            BoundExpr::Setval {
                sequence: *sequence,
                value: Box::new(lower_expr(catalog, value, ranges, depth + 1)?),
                is_called: is_called
                    .as_ref()
                    .map(|expr| lower_expr(catalog, expr, ranges, depth + 1).map(Box::new))
                    .transpose()?,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredQueryExpr::Aggregate {
            function,
            arg,
            distinct,
            data_type,
            nullable,
        } => BoundExpr::AggregateCall {
            func: aggregate_function(*function).ok_or_else(|| {
                DbError::internal(format!(
                    "stored view references unknown aggregate function {function}"
                ))
            })?,
            arg: arg
                .as_ref()
                .map(|expr| lower_expr(catalog, expr, ranges, depth + 1).map(Box::new))
                .transpose()?,
            distinct: *distinct,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::LocalRef {
            output,
            data_type,
            nullable,
        } => BoundExpr::LocalRef {
            slot: usize::try_from(*output).map_err(|_| {
                DbError::internal("stored view local reference exceeds platform range")
            })?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::OuterRef {
            correlation,
            data_type,
            nullable,
        } => BoundExpr::OuterRef {
            slot: usize::try_from(*correlation).map_err(|_| {
                DbError::internal("stored view outer reference exceeds platform range")
            })?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNull {
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNotNull {
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => BoundExpr::InList {
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            list: lower_exprs(catalog, list, ranges, depth + 1)?,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => BoundExpr::Between {
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            low: Box::new(lower_expr(catalog, low, ranges, depth + 1)?),
            high: Box::new(lower_expr(catalog, high, ranges, depth + 1)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape,
            data_type,
            nullable,
        } => BoundExpr::Like {
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            pattern: Box::new(lower_expr(catalog, pattern, ranges, depth + 1)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape: *escape,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Case {
            operand,
            when_clauses,
            else_clause,
            flow_sensitive_nullable: _,
            data_type,
            nullable,
        } => BoundExpr::Case {
            operand: operand
                .as_ref()
                .map(|expr| lower_expr(catalog, expr, ranges, depth + 1).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| {
                    Ok((
                        lower_expr(catalog, when, ranges, depth + 1)?,
                        lower_expr(catalog, then, ranges, depth + 1)?,
                    ))
                })
                .collect::<Result<_>>()?,
            else_clause: else_clause
                .as_ref()
                .map(|expr| lower_expr(catalog, expr, ranges, depth + 1).map(Box::new))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => BoundExpr::Cast {
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::ScalarSubquery {
            query,
            data_type,
            nullable,
        } => BoundExpr::ScalarSubquery {
            query: Box::new(lower_query(catalog, query, Some(ranges), depth + 1)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::Exists {
            query,
            negated,
            data_type,
            nullable,
        } => BoundExpr::Exists {
            query: Box::new(lower_query(catalog, query, Some(ranges), depth + 1)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredQueryExpr::InSubquery {
            expr,
            query,
            negated,
            data_type,
            nullable,
        } => BoundExpr::InSubquery {
            expr: Box::new(lower_expr(catalog, expr, ranges, depth + 1)?),
            query: Box::new(lower_query(catalog, query, Some(ranges), depth + 1)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
    };
    Ok(bound)
}

fn require_sequence(catalog: &dyn CatalogManager, sequence: u32) -> Result<()> {
    if catalog.get_sequence(sequence)?.is_none() {
        return Err(DbError::internal(format!(
            "stored view references unknown sequence {sequence}"
        )));
    }
    Ok(())
}

fn lower_binop(op: StoredQueryBinOp) -> BinOp {
    match op {
        StoredQueryBinOp::Add => BinOp::Add,
        StoredQueryBinOp::Sub => BinOp::Sub,
        StoredQueryBinOp::Mul => BinOp::Mul,
        StoredQueryBinOp::Div => BinOp::Div,
        StoredQueryBinOp::Mod => BinOp::Mod,
        StoredQueryBinOp::Eq => BinOp::Eq,
        StoredQueryBinOp::Neq => BinOp::Neq,
        StoredQueryBinOp::Lt => BinOp::Lt,
        StoredQueryBinOp::LtEq => BinOp::LtEq,
        StoredQueryBinOp::Gt => BinOp::Gt,
        StoredQueryBinOp::GtEq => BinOp::GtEq,
        StoredQueryBinOp::And => BinOp::And,
        StoredQueryBinOp::Or => BinOp::Or,
        StoredQueryBinOp::Concat => BinOp::Concat,
        StoredQueryBinOp::IsDistinctFrom => BinOp::IsDistinctFrom,
        StoredQueryBinOp::IsNotDistinctFrom => BinOp::IsNotDistinctFrom,
    }
}

fn lower_unary(op: StoredQueryUnaryOp) -> UnaryOp {
    match op {
        StoredQueryUnaryOp::Neg => UnaryOp::Neg,
        StoredQueryUnaryOp::Not => UnaryOp::Not,
    }
}
