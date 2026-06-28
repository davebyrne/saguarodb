use catalog::CatalogManager;
use common::{ColumnId, ColumnInfo, DataType, Result, SqlState, TableId, TableSchema, Value};
use parser::{Distinct, Expr, FromItem, OrderByItem, SelectItem, SelectStatement};

use crate::{
    BoundDistinct, BoundExpr, BoundFrom, BoundOrderByItem, BoundSelect, BoundSelectItem, JoinType,
};

use super::expr::{bind_boolean_expr, bind_expr};
use super::{
    BindContext, Binding, contains_aggregate, input_ref, plan_error, reject_aggregate,
    require_table,
};

pub(super) fn bind_select(
    catalog: &dyn CatalogManager,
    select: &SelectStatement,
    declared: &[Option<DataType>],
) -> Result<BoundSelect> {
    if select.from.is_empty() {
        return Err(plan_error(
            SqlState::UndefinedTable,
            "SELECT requires FROM in v1",
        ));
    }

    let mut ctx = BindContext::new(catalog, declared);
    let from = bind_from_items(catalog, &mut ctx, &select.from)?;
    let filter = select
        .filter
        .as_ref()
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &filter {
        reject_aggregate(filter)?;
    }

    let group_by = select
        .group_by
        .iter()
        .map(|expr| {
            let bound = bind_expr(&mut ctx, expr, None)?;
            reject_aggregate(&bound)?;
            Ok(bound)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut columns = Vec::new();
    for item in &select.columns {
        bind_select_item(&mut ctx, item, &mut columns)?;
    }

    let having = select
        .having
        .as_ref()
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    let order_by = bind_order_by(&mut ctx, &select.order_by, &columns)?;
    let distinct = bind_distinct(&mut ctx, select.distinct.as_ref(), &columns, &order_by)?;

    let distinct_on_keys = match &distinct {
        Some(BoundDistinct::On(keys)) => keys.as_slice(),
        _ => &[],
    };
    validate_aggregate_usage(
        &columns,
        &group_by,
        having.as_ref(),
        &order_by,
        distinct_on_keys,
    )?;

    let output_schema = columns
        .iter()
        .map(|item| ColumnInfo {
            name: item.alias.clone(),
            data_type: item.expr.data_type(),
            table_id: output_table_id(&ctx, &item.expr),
            column_id: output_column_id(&item.expr),
        })
        .collect();

    Ok(BoundSelect {
        distinct,
        columns,
        from,
        filter,
        group_by,
        having,
        order_by,
        limit: select.limit,
        offset: select.offset,
        output_schema,
    })
}

fn bind_from_items(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    items: &[FromItem],
) -> Result<BoundFrom> {
    let mut bound = bind_from_item(catalog, ctx, &items[0])?;
    for item in &items[1..] {
        let right = bind_from_item(catalog, ctx, item)?;
        bound = BoundFrom::Join {
            left: Box::new(bound),
            right: Box::new(right),
            join_type: JoinType::Cross,
            condition: None,
        };
    }
    Ok(bound)
}

fn bind_from_item(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    item: &FromItem,
) -> Result<BoundFrom> {
    match item {
        FromItem::Table { name, alias } => {
            let table = require_table(catalog, name)?;
            Ok(bind_table_from_schema(ctx, table, alias.clone()))
        }
        FromItem::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let left = bind_from_item(catalog, ctx, left)?;
            let right = bind_from_item(catalog, ctx, right)?;
            let join_type = convert_join_type(join_type.clone());
            let condition = match (join_type, condition) {
                (JoinType::Cross, None) => None,
                (JoinType::Cross, Some(_)) => {
                    return Err(plan_error(
                        SqlState::SyntaxError,
                        "CROSS JOIN cannot have an ON predicate in v1",
                    ));
                }
                (_, Some(expr)) => Some(bind_boolean_expr(ctx, expr)?),
                (_, None) => {
                    return Err(plan_error(
                        SqlState::SyntaxError,
                        "non-CROSS joins require an ON predicate",
                    ));
                }
            };
            if let Some(condition) = &condition {
                reject_aggregate(condition)?;
            }
            Ok(BoundFrom::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
            })
        }
    }
}

pub(super) fn bind_table_from_schema(
    ctx: &mut BindContext,
    table: TableSchema,
    alias: Option<String>,
) -> BoundFrom {
    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let slot_start = ctx.next_slot;
    ctx.next_slot += table.columns.len();
    ctx.bindings.push(Binding {
        id: binding,
        table_id: table.id,
        table_name: table.name.clone(),
        visible_name: alias.clone().unwrap_or_else(|| table.name.clone()),
        columns: table.columns.clone(),
        slot_start,
    });
    BoundFrom::Table {
        table: table.id,
        binding,
        alias,
        schema: table.columns,
    }
}

fn bind_select_item(
    ctx: &mut BindContext,
    item: &SelectItem,
    output: &mut Vec<BoundSelectItem>,
) -> Result<()> {
    match item {
        SelectItem::Wildcard => {
            for binding in &ctx.bindings {
                for column in &binding.columns {
                    output.push(BoundSelectItem {
                        expr: input_ref(binding, column),
                        alias: column.name.clone(),
                    });
                }
            }
        }
        SelectItem::QualifiedWildcard(qualifier) => {
            let binding = resolve_binding(ctx, qualifier)?;
            for column in &binding.columns {
                output.push(BoundSelectItem {
                    expr: input_ref(binding, column),
                    alias: column.name.clone(),
                });
            }
        }
        SelectItem::Expression { expr, alias } => {
            let bound = bind_expr(ctx, expr, None)?;
            let alias = alias.clone().unwrap_or_else(|| derive_alias(expr));
            output.push(BoundSelectItem { expr: bound, alias });
        }
    }
    Ok(())
}

fn bind_order_by(
    ctx: &mut BindContext,
    order_by: &[OrderByItem],
    columns: &[BoundSelectItem],
) -> Result<Vec<BoundOrderByItem>> {
    order_by
        .iter()
        .map(|item| {
            let expr = match &item.expr {
                // `ORDER BY <n>`: a bare positive integer literal selects the
                // nth output column (1-based), matching PostgreSQL.
                Expr::Literal(Value::Integer(position)) => {
                    let index = order_by_position_index(*position, columns.len())?;
                    columns[index].expr.clone()
                }
                Expr::ColumnRef {
                    table: None,
                    column,
                } => columns
                    .iter()
                    .find(|select_item| select_item.alias == *column)
                    .map(|select_item| select_item.expr.clone())
                    .map(Ok)
                    .unwrap_or_else(|| bind_expr(ctx, &item.expr, None))?,
                _ => bind_expr(ctx, &item.expr, None)?,
            };
            Ok(BoundOrderByItem {
                expr,
                ascending: item.ascending,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

/// Resolve and validate the `DISTINCT` modifier. Plain `SELECT DISTINCT`
/// requires each `ORDER BY` expression to be in the select list; `DISTINCT ON`
/// binds its key expressions and requires them to match the leading `ORDER BY`
/// expressions.
fn bind_distinct(
    ctx: &mut BindContext,
    distinct: Option<&Distinct>,
    columns: &[BoundSelectItem],
    order_by: &[BoundOrderByItem],
) -> Result<Option<BoundDistinct>> {
    match distinct {
        None => Ok(None),
        Some(Distinct::All) => {
            // Every ORDER BY expression must also appear in the select list;
            // otherwise the sort key is not part of the de-duplicated output.
            for item in order_by {
                if !columns.iter().any(|column| column.expr == item.expr) {
                    return Err(plan_error(
                        SqlState::InvalidColumnReference,
                        "for SELECT DISTINCT, ORDER BY expressions must appear in the select list",
                    ));
                }
            }
            Ok(Some(BoundDistinct::All))
        }
        Some(Distinct::On(exprs)) => {
            let on = exprs
                .iter()
                .map(|expr| {
                    let bound = bind_expr(ctx, expr, None)?;
                    reject_aggregate(&bound)?;
                    Ok(bound)
                })
                .collect::<Result<Vec<_>>>()?;
            validate_distinct_on_order_by(&on, order_by)?;
            Ok(Some(BoundDistinct::On(on)))
        }
    }
}

/// PostgreSQL requires every `ORDER BY` expression that precedes a non-key sort
/// expression to be a `DISTINCT ON` key, so the ordering does not split a key's
/// group before de-duplication. Scanning `ORDER BY` left to right, once a sort
/// expression is not a `DISTINCT ON` key, all *distinct* keys must already have
/// been seen. Keys absent from `ORDER BY` are allowed (their intra-key order is
/// then unspecified, as in PostgreSQL). PostgreSQL de-duplicates both the key
/// list and the sort list first, so a repeated `DISTINCT ON` key or `ORDER BY`
/// column counts once. With no `ORDER BY` the kept row per key is unspecified,
/// so no constraint applies.
fn validate_distinct_on_order_by(on: &[BoundExpr], order_by: &[BoundOrderByItem]) -> Result<()> {
    let mut distinct_keys: Vec<&BoundExpr> = Vec::new();
    for key in on {
        if !distinct_keys.contains(&key) {
            distinct_keys.push(key);
        }
    }

    let mut matched_keys: Vec<&BoundExpr> = Vec::new();
    for item in order_by {
        match on.iter().find(|key| **key == item.expr) {
            Some(key) => {
                if !matched_keys.contains(&key) {
                    matched_keys.push(key);
                }
            }
            None => {
                if matched_keys.len() < distinct_keys.len() {
                    return Err(plan_error(
                        SqlState::InvalidColumnReference,
                        "SELECT DISTINCT ON expressions must match the leading ORDER BY expressions",
                    ));
                }
                break;
            }
        }
    }
    Ok(())
}

/// Resolve a 1-based `ORDER BY` position into a zero-based output-column index.
fn order_by_position_index(position: i64, column_count: usize) -> Result<usize> {
    let in_range = position >= 1 && usize::try_from(position).is_ok_and(|p| p <= column_count);
    if !in_range {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("ORDER BY position {position} is out of range (1..{column_count})"),
        ));
    }
    Ok(position as usize - 1)
}

fn validate_aggregate_usage(
    columns: &[BoundSelectItem],
    group_by: &[BoundExpr],
    having: Option<&BoundExpr>,
    order_by: &[BoundOrderByItem],
    distinct_on: &[BoundExpr],
) -> Result<()> {
    // `DISTINCT ON` keys do not themselves induce aggregation (matching
    // PostgreSQL), but in an aggregate query they are subject to the same
    // grouped-expression rule as the select list and ORDER BY.
    let aggregate_context = !group_by.is_empty()
        || columns.iter().any(|item| contains_aggregate(&item.expr))
        || having.is_some()
        || order_by.iter().any(|item| contains_aggregate(&item.expr));

    if !aggregate_context {
        return Ok(());
    }

    for item in columns {
        validate_grouped_expr(&item.expr, group_by)?;
    }
    if let Some(having) = having {
        validate_grouped_expr(having, group_by)?;
    }
    for item in order_by {
        validate_grouped_expr(&item.expr, group_by)?;
    }
    for expr in distinct_on {
        validate_grouped_expr(expr, group_by)?;
    }
    Ok(())
}

fn validate_grouped_expr(expr: &BoundExpr, group_by: &[BoundExpr]) -> Result<()> {
    if matches!(expr, BoundExpr::AggregateCall { .. }) {
        return Ok(());
    }
    if !contains_aggregate(expr) {
        if !references_input(expr) || group_by.iter().any(|group| group == expr) {
            return Ok(());
        }
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "non-aggregate expression must appear exactly in GROUP BY",
        ));
    }

    match expr {
        BoundExpr::BinaryOp { left, right, .. } => {
            validate_grouped_expr(left, group_by)?;
            validate_grouped_expr(right, group_by)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => validate_grouped_expr(expr, group_by),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                validate_grouped_expr(arg, group_by)?;
            }
            Ok(())
        }
        BoundExpr::InList { expr, list, .. } => {
            validate_grouped_expr(expr, group_by)?;
            for item in list {
                validate_grouped_expr(item, group_by)?;
            }
            Ok(())
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            validate_grouped_expr(expr, group_by)?;
            validate_grouped_expr(low, group_by)?;
            validate_grouped_expr(high, group_by)
        }
        BoundExpr::Like { expr, pattern, .. } => {
            validate_grouped_expr(expr, group_by)?;
            validate_grouped_expr(pattern, group_by)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                validate_grouped_expr(operand, group_by)?;
            }
            for (when, then) in when_clauses {
                validate_grouped_expr(when, group_by)?;
                validate_grouped_expr(then, group_by)?;
            }
            if let Some(else_clause) = else_clause {
                validate_grouped_expr(else_clause, group_by)?;
            }
            Ok(())
        }
        // `InSubquery`'s left operand is an outer-scope expression; the subquery
        // body is its own (uncorrelated) scope and is treated as a constant.
        BoundExpr::InSubquery { expr, .. } => validate_grouped_expr(expr, group_by),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => validate_grouped_expr(expr, group_by),
        BoundExpr::AggregateCall { .. } => Ok(()),
    }
}

fn references_input(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::InputRef { .. } => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            references_input(left) || references_input(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => references_input(expr),
        BoundExpr::Function { args, .. } => args.iter().any(references_input),
        BoundExpr::AggregateCall { arg, .. } => arg.as_deref().is_some_and(references_input),
        BoundExpr::InList { expr, list, .. } => {
            references_input(expr) || list.iter().any(references_input)
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => references_input(expr) || references_input(low) || references_input(high),
        BoundExpr::Like { expr, pattern, .. } => {
            references_input(expr) || references_input(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand.as_deref().is_some_and(references_input)
                || when_clauses
                    .iter()
                    .any(|(when, then)| references_input(when) || references_input(then))
                || else_clause.as_deref().is_some_and(references_input)
        }
        // The left operand of `IN (subquery)` is an outer-scope expression; the
        // subquery body itself is uncorrelated and never references outer input.
        BoundExpr::InSubquery { expr, .. } => references_input(expr),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => false,
    }
}

fn resolve_binding<'a>(ctx: &'a BindContext, qualifier: &str) -> Result<&'a Binding> {
    let matches = ctx
        .bindings
        .iter()
        .filter(|binding| binding.visible_name == qualifier)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [binding] => Ok(binding),
        [] => Err(plan_error(
            SqlState::UndefinedTable,
            format!("table binding {qualifier} does not exist"),
        )),
        _ => Err(plan_error(
            SqlState::UndefinedTable,
            format!("table binding {qualifier} is ambiguous"),
        )),
    }
}

fn output_table_id(ctx: &BindContext, expr: &BoundExpr) -> Option<TableId> {
    match expr {
        BoundExpr::InputRef { input, .. } => ctx
            .bindings
            .iter()
            .find(|binding| binding.id == *input)
            .map(|binding| binding.table_id),
        _ => None,
    }
}

fn output_column_id(expr: &BoundExpr) -> Option<ColumnId> {
    match expr {
        BoundExpr::InputRef { column, .. } => Some(*column),
        _ => None,
    }
}

fn derive_alias(expr: &Expr) -> String {
    match expr {
        Expr::ColumnRef { column, .. } => column.clone(),
        Expr::Function { name, .. } => name.clone(),
        _ => "?column?".to_string(),
    }
}

fn convert_join_type(join_type: parser::JoinType) -> JoinType {
    match join_type {
        parser::JoinType::Inner => JoinType::Inner,
        parser::JoinType::Left => JoinType::Left,
        parser::JoinType::Right => JoinType::Right,
        parser::JoinType::Full => JoinType::Full,
        parser::JoinType::Cross => JoinType::Cross,
    }
}
