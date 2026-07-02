use catalog::CatalogManager;
use common::{
    ColumnDef, ColumnId, ColumnInfo, DataType, PgType, Result, SqlState, TableId, TableSchema,
    Value,
};
use parser::{Distinct, Expr, FromItem, OrderByItem, Query, QueryBody, Select, SelectItem, SetOp};

use crate::{
    BoundDistinct, BoundExpr, BoundFrom, BoundOrderByItem, BoundQuery, BoundQueryBody, BoundSelect,
    BoundSelectItem, BoundSetOp, BoundValues, JoinType, OutputColumn,
};

use super::expr::{bind_boolean_expr, bind_expr};
use super::{
    BindContext, Binding, contains_aggregate, input_ref, plan_error, reject_aggregate,
    require_table, require_type,
};

/// Bind a query expression: bind the body, then attach the query-level
/// `ORDER BY`/`LIMIT`/`OFFSET`. Set operations add an arm here without disturbing
/// the callers (top-level statement, derived table, `INSERT ... SELECT`, subquery
/// expression).
pub(super) fn bind_query(
    catalog: &dyn CatalogManager,
    query: &Query,
    declared: &[Option<DataType>],
) -> Result<BoundQuery> {
    match &query.body {
        QueryBody::Select(select) => {
            let (bound_select, order_by) = bind_select(catalog, select, &query.order_by, declared)?;
            Ok(BoundQuery {
                body: BoundQueryBody::Select(Box::new(bound_select)),
                order_by,
                limit: query.limit,
                offset: query.offset,
            })
        }
        QueryBody::Values(rows) => {
            // `ORDER BY` over a bare VALUES needs output-position ordering, which
            // arrives with set operations; until then it is rejected. `LIMIT`/
            // `OFFSET` need no binding and pass through to the wrapper.
            if !query.order_by.is_empty() {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "ORDER BY over VALUES is not supported yet",
                ));
            }
            let values = bind_values(catalog, rows, declared)?;
            Ok(BoundQuery {
                body: BoundQueryBody::Values(values),
                order_by: Vec::new(),
                limit: query.limit,
                offset: query.offset,
            })
        }
        QueryBody::SetOp {
            op,
            all,
            left,
            right,
        } => {
            if *all && matches!(op, SetOp::Intersect | SetOp::Except) {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "INTERSECT ALL / EXCEPT ALL are not supported",
                ));
            }
            let left = bind_query(catalog, left, declared)?;
            let right = bind_query(catalog, right, declared)?;
            let (output_schema, output_columns) = reconcile_set_op(&left, &right)?;
            // `ORDER BY` over a set operation resolves against the combined output
            // by position or name only (there is no single input scope).
            let order_by = bind_set_op_order_by(&query.order_by, &output_columns)?;
            Ok(BoundQuery {
                body: BoundQueryBody::SetOp(BoundSetOp {
                    op: *op,
                    all: *all,
                    left: Box::new(left),
                    right: Box::new(right),
                    output_schema,
                }),
                order_by,
                limit: query.limit,
                offset: query.offset,
            })
        }
    }
}

/// Reconcile the two arms of a set operation. They must have the same number of
/// columns and — under the strict no-implicit-cast rule — identical column types.
/// Returns the result `RowDescription` (the left arm's column names, the shared
/// types) and the output columns (with nullability = either arm nullable), the
/// latter used to bind the query-level `ORDER BY`.
fn reconcile_set_op(
    left: &BoundQuery,
    right: &BoundQuery,
) -> Result<(Vec<ColumnInfo>, Vec<OutputColumn>)> {
    let left_columns = left.output_columns();
    let right_columns = right.output_columns();
    if left_columns.len() != right_columns.len() {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!(
                "each query of a set operation must have the same number of columns ({} vs {})",
                left_columns.len(),
                right_columns.len()
            ),
        ));
    }

    let mut output_schema = Vec::with_capacity(left_columns.len());
    let mut output_columns = Vec::with_capacity(left_columns.len());
    for (index, (left, right)) in left_columns.iter().zip(&right_columns).enumerate() {
        if left.data_type != right.data_type {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!(
                    "set operation column {} has mismatched types ({:?} vs {:?})",
                    index + 1,
                    left.data_type,
                    right.data_type
                ),
            ));
        }
        output_schema.push(ColumnInfo {
            name: left.name.clone(),
            data_type: left.data_type.clone(),
            table_id: None,
            column_id: None,
            // A set-operation result column is synthetic — it has no single
            // declared wire type, so it reports the collapsed default for its
            // reconciled `data_type` (matching PostgreSQL's base-OID/typmod -1).
            pg_type: None,
        });
        output_columns.push(OutputColumn {
            name: left.name.clone(),
            data_type: left.data_type.clone(),
            nullable: left.nullable || right.nullable,
        });
    }
    Ok((output_schema, output_columns))
}

/// Bind the query-level `ORDER BY` of a set operation. Each item must reference an
/// output column by 1-based position or by name (no arbitrary expressions — a set
/// operation has no single input scope); it becomes a `LocalRef` to that output
/// slot, which the `Sort` above the set operation evaluates over the result rows.
fn bind_set_op_order_by(
    order_by: &[OrderByItem],
    output_columns: &[OutputColumn],
) -> Result<Vec<BoundOrderByItem>> {
    order_by
        .iter()
        .map(|item| {
            let slot = match &item.expr {
                Expr::Literal(Value::Integer(position)) => {
                    order_by_position_index(*position, output_columns.len())?
                }
                Expr::ColumnRef {
                    table: None,
                    column,
                } => output_columns
                    .iter()
                    .position(|output| &output.name == column)
                    .ok_or_else(|| {
                        plan_error(
                            SqlState::InvalidColumnReference,
                            format!("column {column} does not exist in the set operation output"),
                        )
                    })?,
                _ => {
                    return Err(plan_error(
                        SqlState::FeatureNotSupported,
                        "ORDER BY over a set operation must reference an output column by position or name",
                    ));
                }
            };
            Ok(BoundOrderByItem {
                expr: BoundExpr::LocalRef {
                    slot,
                    data_type: output_columns[slot].data_type.clone(),
                    nullable: output_columns[slot].nullable,
                },
                ascending: item.ascending,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

/// Bind a `VALUES` body. Every row must have the same width. Each column's type is
/// the common type of its entries under the strict no-implicit-cast rule: a bare
/// `NULL` takes the inferred column type, and every non-`NULL` entry must match it
/// exactly (else `DatatypeMismatch`); an all-`NULL` column has no inferable type
/// and is rejected. VALUES is a leaf (no bindings), so column references inside it
/// cannot resolve. Output columns are named `column1`, `column2`, ...
fn bind_values(
    catalog: &dyn CatalogManager,
    rows: &[Vec<Expr>],
    declared: &[Option<DataType>],
) -> Result<BoundValues> {
    let width = rows[0].len();
    for row in rows {
        if row.len() != width {
            return Err(plan_error(
                SqlState::SyntaxError,
                "VALUES lists must all be the same length",
            ));
        }
    }

    // Pass 1: infer each column's type from its first non-NULL entry. An all-NULL
    // column has no inferable type and is rejected (the strict no-implicit-cast
    // rule gives no default type).
    let mut output_schema = Vec::with_capacity(width);
    for column in 0..width {
        let mut data_type = None;
        for row in rows {
            if matches!(row[column], Expr::Literal(Value::Null)) {
                continue;
            }
            let mut ctx = BindContext::new(catalog, declared);
            data_type = Some(bind_expr(&mut ctx, &row[column], None)?.data_type());
            break;
        }
        let Some(data_type) = data_type else {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!(
                    "could not determine data type of VALUES column {}",
                    column + 1
                ),
            ));
        };
        output_schema.push(ColumnInfo {
            name: format!("column{}", column + 1),
            data_type,
            table_id: None,
            column_id: None,
            // A VALUES column is computed from its rows, with no single declared
            // wire type, so it reports the collapsed default for `data_type`.
            pg_type: None,
        });
    }

    // Pass 2: bind every entry against its column type — bare NULLs adopt it, and
    // every other entry must match it exactly.
    let mut bound_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut bound_row = Vec::with_capacity(width);
        for (column, expr) in row.iter().enumerate() {
            let data_type = output_schema[column].data_type.clone();
            let mut ctx = BindContext::new(catalog, declared);
            let bound = bind_expr(&mut ctx, expr, Some(data_type.clone()))?;
            reject_aggregate(&bound)?;
            require_type(&bound, data_type)?;
            bound_row.push(bound);
        }
        bound_rows.push(bound_row);
    }

    Ok(BoundValues {
        rows: bound_rows,
        output_schema,
    })
}

/// Bind a single `SELECT` block together with the query-level `order_by` (bound
/// against this block's output columns). Returns the bound block and the bound
/// `ORDER BY`; `LIMIT`/`OFFSET` are copied onto the wrapper by [`bind_query`].
/// `ORDER BY` and `DISTINCT` are bound here because their validation is coupled
/// (`SELECT DISTINCT` requires each `ORDER BY` expression to be in the select
/// list, and `DISTINCT ON` keys must match the leading `ORDER BY`).
fn bind_select(
    catalog: &dyn CatalogManager,
    select: &Select,
    order_by: &[OrderByItem],
    declared: &[Option<DataType>],
) -> Result<(BoundSelect, Vec<BoundOrderByItem>)> {
    let mut ctx = BindContext::new(catalog, declared);
    // A FROM-less SELECT (`SELECT 1`) has no source relation: no bindings are
    // registered, so any column reference correctly fails to resolve.
    let from = if select.from.is_empty() {
        None
    } else {
        Some(bind_from_items(catalog, &mut ctx, &select.from)?)
    };
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
    let order_by = bind_order_by(&mut ctx, order_by, &columns)?;
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

    let output_schema = select_output_schema(&ctx, &columns);

    Ok((
        BoundSelect {
            distinct,
            columns,
            from,
            filter,
            group_by,
            having,
            output_schema,
        },
        order_by,
    ))
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
        FromItem::Derived {
            subquery,
            alias,
            column_aliases,
        } => bind_derived_table(catalog, ctx, subquery, alias, column_aliases),
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
        table_id: Some(table.id),
        table_name: table.name.clone(),
        visible_name: alias.clone().unwrap_or_else(|| table.name.clone()),
        columns: table.columns.clone(),
        slot_start,
        qualified_only: false,
    });
    BoundFrom::Table {
        table: table.id,
        binding,
        alias,
        schema: table.columns,
    }
}

/// Bind a derived table `(SELECT ...) AS alias [(cols)]`: bind the inner query in
/// its own scope, derive the visible columns (optionally renamed by the column
/// alias list), and register a binding that projects them into the outer scope.
fn bind_derived_table(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    subquery: &Query,
    alias: &str,
    column_aliases: &[String],
) -> Result<BoundFrom> {
    let query = bind_query(catalog, subquery, &ctx.declared_params)?;
    let output = query.output_columns();
    if column_aliases.len() > output.len() {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!(
                "table \"{alias}\" has {} columns available but {} column aliases specified",
                output.len(),
                column_aliases.len()
            ),
        ));
    }

    let columns: Vec<ColumnDef> = output
        .iter()
        .enumerate()
        .map(|(index, column)| ColumnDef {
            id: index as ColumnId,
            name: column_aliases
                .get(index)
                .cloned()
                .unwrap_or_else(|| column.name.clone()),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            max_length: None,
            default: None,
            pg_type: None,
        })
        .collect();

    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let slot_start = ctx.next_slot;
    ctx.next_slot += columns.len();
    ctx.bindings.push(Binding {
        id: binding,
        table_id: None,
        table_name: alias.to_string(),
        visible_name: alias.to_string(),
        columns: columns.clone(),
        slot_start,
        qualified_only: false,
    });

    Ok(BoundFrom::Derived {
        query: Box::new(query),
        binding,
        alias: alias.to_string(),
        schema: columns,
    })
}

/// Register the `excluded` pseudo-table binding for `INSERT ... ON CONFLICT DO
/// UPDATE`: the row proposed for insertion, addressed as `excluded.<col>`. Its
/// columns are the target table's, in the same slot order, but offset by the
/// current `next_slot` so they sit after the target row. It is `qualified_only`,
/// so a bare column resolves to the target row, not `excluded`. Returns the
/// binding's `slot_start` (the offset of the proposed row in the combined tuple).
pub(super) fn bind_excluded_binding(ctx: &mut BindContext, table: &TableSchema) -> usize {
    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let slot_start = ctx.next_slot;
    ctx.next_slot += table.columns.len();
    ctx.bindings.push(Binding {
        id: binding,
        table_id: Some(table.id),
        table_name: "excluded".to_string(),
        visible_name: "excluded".to_string(),
        columns: table.columns.clone(),
        slot_start,
        qualified_only: true,
    });
    slot_start
}

/// Build the result-set column metadata for a bound projection list, deriving
/// each column's name from its alias and its table/column ids from an underlying
/// `InputRef` (so the wire `RowDescription` can carry them). Shared by `SELECT`
/// and `RETURNING`.
pub(super) fn select_output_schema(
    ctx: &BindContext,
    columns: &[BoundSelectItem],
) -> Vec<ColumnInfo> {
    columns
        .iter()
        .map(|item| ColumnInfo {
            name: item.alias.clone(),
            data_type: item.expr.data_type(),
            table_id: output_table_id(ctx, &item.expr),
            column_id: output_column_id(&item.expr),
            pg_type: Some(output_pg_type(ctx, &item.expr)),
        })
        .collect()
}

pub(super) fn bind_select_item(
    ctx: &mut BindContext,
    item: &SelectItem,
    output: &mut Vec<BoundSelectItem>,
) -> Result<()> {
    match item {
        SelectItem::Wildcard => {
            // `SELECT *` needs a FROM clause: with no bindings there is nothing to
            // expand to, and expanding to zero columns would be a degenerate
            // result. PostgreSQL rejects this (`SELECT * with no tables ...`).
            // (`RETURNING *` always binds over the target table, so it is
            // unaffected.)
            if ctx.bindings.is_empty() {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "SELECT * with no tables specified is not valid",
                ));
            }
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
        BoundExpr::Setval {
            value, is_called, ..
        } => {
            validate_grouped_expr(value, group_by)?;
            if let Some(is_called) = is_called {
                validate_grouped_expr(is_called, group_by)?;
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
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
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
        BoundExpr::Setval {
            value, is_called, ..
        } => references_input(value) || is_called.as_deref().is_some_and(references_input),
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
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
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
            .and_then(|binding| binding.table_id),
        _ => None,
    }
}

fn output_column_id(expr: &BoundExpr) -> Option<ColumnId> {
    match expr {
        BoundExpr::InputRef { column, .. } => Some(*column),
        _ => None,
    }
}

/// The PostgreSQL wire type of an output column ("columns + casts" propagation):
/// a bare column reference reports its source column's declared wire type, an
/// explicit `CAST` reports its target wire type, and every other expression
/// reports the natural wire type collapsed from its result `DataType`.
fn output_pg_type(ctx: &BindContext, expr: &BoundExpr) -> PgType {
    match expr {
        // `column` is the dense index into the binding's columns (same value
        // `output_column_id` returns), so resolve the source `ColumnDef` directly.
        BoundExpr::InputRef {
            input,
            column,
            data_type,
            ..
        } => ctx
            .bindings
            .iter()
            .find(|binding| binding.id == *input)
            .and_then(|binding| binding.columns.get(usize::from(*column)))
            .map(ColumnDef::wire_type)
            .unwrap_or_else(|| PgType::from(data_type)),
        BoundExpr::Cast { pg_type, .. } => pg_type.clone(),
        other => PgType::from(&other.data_type()),
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
