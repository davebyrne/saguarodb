use common::Result;
use sqlparser::ast as sql;

use crate::{
    Assignment, Distinct, Expr, FromItem, JoinType, OrderByItem, SelectItem, SelectStatement,
};

use super::expr::convert_expr;
use super::{ident_name, object_name, parse_error, unsupported};

pub(super) fn convert_query_to_select(query: sql::Query) -> Result<SelectStatement> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return unsupported("unsupported SELECT query form");
    }

    let (limit, offset) = convert_limit_clause(query.limit_clause)?;
    let order_by = query
        .order_by
        .map(convert_order_by)
        .transpose()?
        .unwrap_or_default();

    let sql::SetExpr::Select(select) = *query.body else {
        return unsupported("unsupported SELECT body");
    };
    convert_select(*select, order_by, limit, offset)
}

/// Convert a subquery body (`Box<SetExpr>`, as carried by `IN (subquery)`) into a
/// `SelectStatement`. A bare `SELECT` has no ORDER BY / LIMIT of its own; a
/// parenthesized query may, so it is routed through the full query converter.
pub(super) fn convert_set_expr_to_select(set_expr: sql::SetExpr) -> Result<SelectStatement> {
    match set_expr {
        sql::SetExpr::Select(select) => convert_select(*select, Vec::new(), None, None),
        sql::SetExpr::Query(query) => convert_query_to_select(*query),
        _ => unsupported("unsupported subquery body"),
    }
}

fn convert_select(
    select: sql::Select,
    order_by: Vec<OrderByItem>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<SelectStatement> {
    if select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return unsupported("unsupported SELECT form");
    }

    let group_by = match select.group_by {
        sql::GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            exprs.iter().map(convert_expr).collect::<Result<Vec<_>>>()?
        }
        sql::GroupByExpr::Expressions(_, _) | sql::GroupByExpr::All(_) => {
            return unsupported("unsupported GROUP BY form");
        }
    };

    let distinct = match &select.distinct {
        None => None,
        Some(sql::Distinct::Distinct) => Some(Distinct::All),
        Some(sql::Distinct::On(exprs)) => Some(Distinct::On(
            exprs.iter().map(convert_expr).collect::<Result<Vec<_>>>()?,
        )),
    };

    Ok(SelectStatement {
        distinct,
        columns: select
            .projection
            .iter()
            .map(convert_select_item)
            .collect::<Result<Vec<_>>>()?,
        from: select
            .from
            .iter()
            .map(convert_table_with_joins)
            .collect::<Result<Vec<_>>>()?,
        filter: select.selection.as_ref().map(convert_expr).transpose()?,
        group_by,
        having: select.having.as_ref().map(convert_expr).transpose()?,
        order_by,
        limit,
        offset,
    })
}

fn convert_select_item(item: &sql::SelectItem) -> Result<SelectItem> {
    match item {
        sql::SelectItem::Wildcard(options) => {
            reject_wildcard_options(options)?;
            Ok(SelectItem::Wildcard)
        }
        sql::SelectItem::QualifiedWildcard(kind, options) => {
            reject_wildcard_options(options)?;
            let sql::SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                return unsupported("unsupported qualified wildcard");
            };
            Ok(SelectItem::QualifiedWildcard(object_name(name)?))
        }
        sql::SelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expression {
            expr: convert_expr(expr)?,
            alias: None,
        }),
        sql::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expression {
            expr: convert_expr(expr)?,
            alias: Some(ident_name(alias)?),
        }),
    }
}

fn convert_table_with_joins(table: &sql::TableWithJoins) -> Result<FromItem> {
    let mut item = convert_table_factor(&table.relation)?;
    for join in &table.joins {
        item = convert_join(item, join)?;
    }
    Ok(item)
}

fn convert_join(left: FromItem, join: &sql::Join) -> Result<FromItem> {
    let right = convert_table_factor(&join.relation)?;
    let (join_type, condition) = match &join.join_operator {
        sql::JoinOperator::Inner(constraint) | sql::JoinOperator::Join(constraint) => {
            (JoinType::Inner, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::LeftOuter(constraint) | sql::JoinOperator::Left(constraint) => {
            (JoinType::Left, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::RightOuter(constraint) | sql::JoinOperator::Right(constraint) => {
            (JoinType::Right, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::FullOuter(constraint) => {
            (JoinType::Full, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::CrossJoin => (JoinType::Cross, None),
        sql::JoinOperator::CrossApply
        | sql::JoinOperator::OuterApply
        | sql::JoinOperator::AsOf { .. }
        | sql::JoinOperator::Semi(_)
        | sql::JoinOperator::LeftSemi(_)
        | sql::JoinOperator::RightSemi(_)
        | sql::JoinOperator::Anti(_)
        | sql::JoinOperator::LeftAnti(_)
        | sql::JoinOperator::RightAnti(_)
        | sql::JoinOperator::StraightJoin(_) => return unsupported("unsupported JOIN form"),
    };

    Ok(FromItem::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition,
    })
}

fn required_on_constraint(constraint: &sql::JoinConstraint) -> Result<Option<Expr>> {
    match constraint {
        sql::JoinConstraint::On(expr) => Ok(Some(convert_expr(expr)?)),
        sql::JoinConstraint::Using(_) => unsupported("USING joins are not supported"),
        sql::JoinConstraint::Natural => unsupported("NATURAL joins are not supported"),
        sql::JoinConstraint::None => unsupported("non-CROSS joins require an ON predicate"),
    }
}

fn convert_table_factor(table: &sql::TableFactor) -> Result<FromItem> {
    match table {
        sql::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return unsupported("unsupported table factor");
            }
            let alias = alias.as_ref().map(table_alias_name).transpose()?;
            Ok(FromItem::Table {
                name: object_name(name)?,
                alias,
            })
        }
        sql::TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if *lateral {
                return unsupported("LATERAL derived tables are not supported");
            }
            let Some(alias) = alias else {
                return unsupported("a subquery in FROM must have an alias");
            };
            let column_aliases = alias
                .columns
                .iter()
                .map(table_alias_column_name)
                .collect::<Result<Vec<_>>>()?;
            Ok(FromItem::Derived {
                subquery: Box::new(convert_query_to_select((**subquery).clone())?),
                alias: ident_name(&alias.name)?,
                column_aliases,
            })
        }
        _ => unsupported("unsupported table factor"),
    }
}

/// A column alias in `AS alias(col, ...)`. Type-annotated aliases (used by some
/// table-valued functions) are rejected.
fn table_alias_column_name(column: &sql::TableAliasColumnDef) -> Result<String> {
    if column.data_type.is_some() {
        return unsupported("typed column aliases are not supported");
    }
    ident_name(&column.name)
}

pub(super) fn table_name_from_table_with_joins(table: &sql::TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return unsupported("joins are not supported here");
    }
    let FromItem::Table { name, alias: None } = convert_table_factor(&table.relation)? else {
        return unsupported("expected table name");
    };
    Ok(name)
}

fn convert_order_by(order_by: sql::OrderBy) -> Result<Vec<OrderByItem>> {
    if order_by.interpolate.is_some() {
        return unsupported("unsupported ORDER BY form");
    }

    let sql::OrderByKind::Expressions(expressions) = order_by.kind else {
        return unsupported("unsupported ORDER BY form");
    };

    expressions
        .iter()
        .map(|item| {
            if item.with_fill.is_some() {
                return unsupported("unsupported ORDER BY form");
            }
            Ok(OrderByItem {
                expr: convert_expr(&item.expr)?,
                ascending: item.options.asc.unwrap_or(true),
                nulls_first: item.options.nulls_first,
            })
        })
        .collect()
}

fn convert_limit_clause(
    limit_clause: Option<sql::LimitClause>,
) -> Result<(Option<u64>, Option<u64>)> {
    match limit_clause {
        None => Ok((None, None)),
        Some(sql::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return unsupported("unsupported LIMIT form");
            }
            Ok((
                limit.as_ref().map(convert_u64_expr).transpose()?,
                offset
                    .as_ref()
                    .map(|offset| convert_u64_expr(&offset.value))
                    .transpose()?,
            ))
        }
        Some(sql::LimitClause::OffsetCommaLimit { .. }) => unsupported("unsupported LIMIT form"),
    }
}

pub(super) fn convert_assignment(assignment: sql::Assignment) -> Result<Assignment> {
    let sql::AssignmentTarget::ColumnName(column) = assignment.target else {
        return unsupported("unsupported assignment target");
    };

    Ok(Assignment {
        column: object_name(&column)?,
        value: convert_expr(&assignment.value)?,
    })
}

fn convert_u64_expr(expr: &sql::Expr) -> Result<u64> {
    let sql::Expr::Value(value) = expr else {
        return unsupported("LIMIT/OFFSET must be integer literals");
    };
    let sql::Value::Number(value, _) = &value.value else {
        return unsupported("LIMIT/OFFSET must be integer literals");
    };
    value
        .parse::<u64>()
        .map_err(|_| parse_error("LIMIT/OFFSET must be non-negative integer literals"))
}

fn table_alias_name(alias: &sql::TableAlias) -> Result<String> {
    if !alias.columns.is_empty() {
        return unsupported("table column aliases are not supported");
    }
    ident_name(&alias.name)
}

fn reject_wildcard_options(options: &sql::WildcardAdditionalOptions) -> Result<()> {
    if options.opt_ilike.is_some()
        || options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_replace.is_some()
        || options.opt_rename.is_some()
    {
        return unsupported("unsupported wildcard options");
    }
    Ok(())
}

pub(super) fn query_has_modifiers(query: &sql::Query) -> bool {
    query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
}
