use catalog::{CatalogManager, SystemView, is_system_schema, resolve_system_view};
use common::{
    BindingId, ColumnDef, ColumnId, ColumnInfo, DataType, PgType, Result, SqlState, TableId,
    TableSchema, Value, ViewSchema,
};
use parser::{
    Cte, Distinct, Expr, FromItem, OrderByItem, Query, QueryBody, Select, SelectItem, Statement,
};

use crate::{
    BoundDistinct, BoundExpr, BoundFrom, BoundOrderByItem, BoundQuery, BoundQueryBody, BoundSelect,
    BoundSelectItem, BoundSetOp, BoundValues, JoinType, OutputColumn,
};

use super::expr::{bind_boolean_expr, bind_expr};
use super::{
    BindContext, Binding, CteBinding, CteScope, contains_aggregate, input_ref, plan_error,
    reject_aggregate, require_type,
};

/// Bind a query expression: bind any `WITH` CTEs into a child scope, then bind the
/// body and attach the query-level `ORDER BY`/`LIMIT`/`OFFSET`. `ctes` is the CTE
/// scope inherited from an enclosing query (empty at the statement level), so a
/// subquery or derived table sees the outer query's CTEs. `expected` supplies a
/// target type per output column, used only to type a bare `NULL` output column
/// (from the sibling arm of an enclosing set operation); `None` when there is no
/// such context.
pub(super) fn bind_query(
    catalog: &dyn CatalogManager,
    query: &Query,
    declared: &[Option<DataType>],
    ctes: &CteScope,
    expected: Option<&[DataType]>,
) -> Result<BoundQuery> {
    let scope = bind_ctes(catalog, &query.with, ctes, declared)?;
    match &query.body {
        QueryBody::Select(select) => {
            let (bound_select, order_by) =
                bind_select(catalog, select, &query.order_by, declared, &scope, expected)?;
            Ok(BoundQuery {
                body: BoundQueryBody::Select(Box::new(bound_select)),
                order_by,
                limit: query.limit,
                offset: query.offset,
            })
        }
        QueryBody::Values(rows) => {
            // `LIMIT`/`OFFSET` need no binding. `ORDER BY` resolves against the
            // VALUES output columns by position or name (like a set operation). The
            // CTE scope is threaded in because a subquery inside a VALUES row can
            // reference an enclosing CTE, even though VALUES itself has no FROM.
            let values = bind_values(catalog, rows, declared, &scope, expected)?;
            let mut bound = BoundQuery {
                body: BoundQueryBody::Values(values),
                order_by: Vec::new(),
                limit: query.limit,
                offset: query.offset,
            };
            bound.order_by = bind_output_order_by(&query.order_by, &bound.output_columns())?;
            Ok(bound)
        }
        QueryBody::SetOp {
            op,
            all,
            left,
            right,
        } => {
            let (left, right) = bind_set_op_arms(catalog, left, right, declared, &scope, expected)?;
            let (output_schema, output_columns) = reconcile_set_op(&left, &right)?;
            // `ORDER BY` over a set operation resolves against the combined output
            // by position or name only (there is no single input scope).
            let order_by = bind_output_order_by(&query.order_by, &output_columns)?;
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

/// Bind the two arms of a set operation, resolving a bare-`NULL` output column in
/// one arm to the sibling arm's type.
///
/// When the output types are already known — an enclosing set operation supplied
/// `expected` — both arms bind directly against them: a bare `NULL` in either arm
/// adopts the known type, so no sibling-derived retry is needed. Binding a subtree
/// with concrete `expected` types is a single pass (this function does not retry),
/// so nested set operations do not re-bind their subtrees per level.
///
/// Only at a set operation with no external type context does an arm need types
/// from its sibling: one arm is bound to discover its column types, then the other
/// is bound with those types as `expected` (so its `NULL` columns adopt them). If
/// the left arm cannot bind on its own — typically a bare-`NULL` column needing the
/// right arm's type — the right arm is bound first and the left is re-bound with
/// its types (a genuine error in the left arm re-surfaces on that second attempt,
/// since `expected` types only bare `NULL`s). Because the re-bind carries concrete
/// `expected` types it is single-pass, so total work stays polynomial rather than
/// doubling per nesting level. A column that is a bare `NULL` in *both* arms (or
/// split across the arms so each needs the other) stays unresolved and is rejected;
/// an explicit cast is required.
fn bind_set_op_arms(
    catalog: &dyn CatalogManager,
    left: &Query,
    right: &Query,
    declared: &[Option<DataType>],
    ctes: &CteScope,
    expected: Option<&[DataType]>,
) -> Result<(BoundQuery, BoundQuery)> {
    if let Some(expected) = expected {
        // Output types already known: bind both arms directly, no retry.
        let left = bind_query(catalog, left, declared, ctes, Some(expected))?;
        let right = bind_query(catalog, right, declared, ctes, Some(expected))?;
        return Ok((left, right));
    }
    match bind_query(catalog, left, declared, ctes, None) {
        Ok(left) => {
            let types = output_column_types(&left);
            let right = bind_query(catalog, right, declared, ctes, Some(&types))?;
            Ok((left, right))
        }
        Err(left_err) => {
            let Ok(right) = bind_query(catalog, right, declared, ctes, None) else {
                return Err(left_err);
            };
            let types = output_column_types(&right);
            let left = bind_query(catalog, left, declared, ctes, Some(&types))?;
            Ok((left, right))
        }
    }
}

/// The output column types of a bound query, used as the `expected` types when
/// binding a sibling set-operation arm.
fn output_column_types(query: &BoundQuery) -> Vec<DataType> {
    query
        .output_columns()
        .into_iter()
        .map(|column| column.data_type)
        .collect()
}

/// Extend the enclosing CTE scope with this query's `WITH` CTEs. Each CTE is bound
/// once (inlined at each reference) and sees the scope so far — the enclosing CTEs
/// and its earlier siblings, but not itself (non-recursive) or later siblings. A
/// duplicate name within one `WITH` is rejected.
fn bind_ctes(
    catalog: &dyn CatalogManager,
    with: &[Cte],
    enclosing: &CteScope,
    declared: &[Option<DataType>],
) -> Result<CteScope> {
    let mut scope = enclosing.clone();
    let base = scope.ctes.len();
    for cte in with {
        if scope.ctes[base..]
            .iter()
            .any(|bound| bound.name == cte.name)
        {
            return Err(plan_error(
                SqlState::SyntaxError,
                format!("WITH query name \"{}\" specified more than once", cte.name),
            ));
        }
        let bound = bind_cte(catalog, cte, &scope, declared)?;
        scope.ctes.push(bound);
    }
    Ok(scope)
}

/// Bind one CTE and derive its output columns (renamed by its column-alias list).
fn bind_cte(
    catalog: &dyn CatalogManager,
    cte: &Cte,
    scope: &CteScope,
    declared: &[Option<DataType>],
) -> Result<CteBinding> {
    let query = bind_query(catalog, &cte.query, declared, scope, None)?;
    let columns = derive_alias_columns(&query.output_columns(), &cte.column_aliases, || {
        format!("CTE \"{}\"", cte.name)
    })?;
    Ok(CteBinding {
        name: cte.name.clone(),
        query,
        columns,
    })
}

/// Build the column metadata a derived relation exposes — a subquery in `FROM`, or
/// an inlined CTE reference — from its output columns: one `ColumnDef` per column,
/// renamed left to right by the optional column-alias list. More aliases than
/// columns is a `SyntaxError`; `describe` names the relation for that message
/// (e.g. `table "d"`, `CTE "x"`) and is only evaluated on error.
pub(super) fn derive_alias_columns(
    output: &[OutputColumn],
    column_aliases: &[String],
    describe: impl FnOnce() -> String,
) -> Result<Vec<ColumnDef>> {
    if column_aliases.len() > output.len() {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!(
                "{} has {} columns available but {} column aliases specified",
                describe(),
                output.len(),
                column_aliases.len()
            ),
        ));
    }
    Ok(output
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
        .collect())
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

/// Bind the query-level `ORDER BY` of a body that has no single input scope (a set
/// operation or a `VALUES`). Each item must reference an output column by 1-based
/// position or by name (no arbitrary expressions); it becomes a `LocalRef` to that
/// output slot, which the `Sort` above the body evaluates over the result rows.
fn bind_output_order_by(
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
                            format!("column {column} does not exist in the query output"),
                        )
                    })?,
                _ => {
                    return Err(plan_error(
                        SqlState::FeatureNotSupported,
                        "ORDER BY over this query must reference an output column by position or name",
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
/// exactly (else `DatatypeMismatch`); an all-`NULL` column takes the type
/// `expected` at its position (from a sibling set-operation arm) if any, else has
/// no inferable type and is rejected. Each entry binds in a fresh scope with no
/// table bindings (so a bare column reference cannot resolve) but with the
/// enclosing CTEs visible (a subquery in a row may reference one). Output columns
/// are named `column1`, ...
fn bind_values(
    catalog: &dyn CatalogManager,
    rows: &[Vec<Expr>],
    declared: &[Option<DataType>],
    ctes: &CteScope,
    expected: Option<&[DataType]>,
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

    // Pass 1: infer each column's type from its first non-NULL entry, falling back
    // to the `expected` type at that position (a sibling set-operation arm). An
    // all-NULL column with no expected type is rejected (the strict no-implicit-cast
    // rule gives no default type).
    let mut output_schema = Vec::with_capacity(width);
    for column in 0..width {
        let mut data_type = None;
        for row in rows {
            if matches!(row[column], Expr::Literal(Value::Null)) {
                continue;
            }
            let mut ctx = BindContext::new(catalog, declared);
            ctx.cte_scope = ctes.clone();
            data_type = Some(bind_expr(&mut ctx, &row[column], None)?.data_type());
            break;
        }
        let data_type = data_type.or_else(|| expected.and_then(|types| types.get(column).cloned()));
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
            ctx.cte_scope = ctes.clone();
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
    ctes: &CteScope,
    expected: Option<&[DataType]>,
) -> Result<(BoundSelect, Vec<BoundOrderByItem>)> {
    let mut ctx = BindContext::new(catalog, declared);
    ctx.cte_scope = ctes.clone();
    // A FROM-less SELECT (`SELECT 1`) has no source relation: no bindings are
    // registered, so any column reference correctly fails to resolve.
    let from = if select.from.is_empty() {
        None
    } else {
        let from = bind_from_items(catalog, &mut ctx, &select.from)?;
        apply_outer_join_nullability(&mut ctx, &from);
        Some(from)
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
        // A bare-`NULL` output column adopts the type expected at its output
        // position (supplied by a sibling set-operation arm), if any; every other
        // projection expression types itself.
        let expected_type = match item {
            SelectItem::Expression {
                expr: Expr::Literal(Value::Null),
                ..
            } => expected.and_then(|types| types.get(columns.len()).cloned()),
            _ => None,
        };
        bind_select_item(&mut ctx, item, expected_type, &mut columns)?;
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

fn apply_outer_join_nullability(ctx: &mut BindContext, from: &BoundFrom) {
    match from {
        BoundFrom::Join {
            left,
            right,
            join_type,
            ..
        } => {
            apply_outer_join_nullability(ctx, left);
            apply_outer_join_nullability(ctx, right);
            match join_type {
                JoinType::Left => mark_from_bindings_nullable(ctx, right),
                JoinType::Right => mark_from_bindings_nullable(ctx, left),
                JoinType::Full => {
                    mark_from_bindings_nullable(ctx, left);
                    mark_from_bindings_nullable(ctx, right);
                }
                JoinType::Inner | JoinType::Cross => {}
            }
        }
        BoundFrom::Table { .. }
        | BoundFrom::System { .. }
        | BoundFrom::Derived { .. }
        | BoundFrom::View { .. } => {}
    }
}

fn mark_from_bindings_nullable(ctx: &mut BindContext, from: &BoundFrom) {
    let mut bindings = Vec::new();
    collect_from_binding_ids(from, &mut bindings);
    for binding in &mut ctx.bindings {
        if bindings.contains(&binding.id) {
            for column in &mut binding.columns {
                column.nullable = true;
            }
        }
    }
}

fn collect_from_binding_ids(from: &BoundFrom, output: &mut Vec<BindingId>) {
    match from {
        BoundFrom::Table { binding, .. }
        | BoundFrom::System { binding, .. }
        | BoundFrom::Derived { binding, .. }
        | BoundFrom::View { binding, .. } => output.push(*binding),
        BoundFrom::Join { left, right, .. } => {
            collect_from_binding_ids(left, output);
            collect_from_binding_ids(right, output);
        }
    }
}

fn bind_from_item(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    item: &FromItem,
) -> Result<BoundFrom> {
    match item {
        FromItem::Table {
            schema,
            name,
            alias,
        } => bind_table_or_schema_qualified_name(catalog, ctx, schema.as_deref(), name, alias),
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

fn bind_table_or_schema_qualified_name(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    schema: Option<&str>,
    name: &str,
    alias: &Option<String>,
) -> Result<BoundFrom> {
    match schema {
        None => {
            // A bare CTE name shadows a catalog table (matching PostgreSQL). Taking
            // the binding out of the scope by value (the one unavoidable clone of
            // the inlined plan) also ends the `ctx.cte_scope` borrow before `ctx`
            // is mutated.
            if let Some(cte) = ctx.cte_scope.lookup(name).cloned() {
                Ok(bind_cte_reference(ctx, cte, alias.clone()))
            } else if let Some(table) = catalog.get_table_by_name(name)? {
                Ok(bind_table_from_schema(ctx, table, alias.clone()))
            } else if let Some(view) = catalog.get_view_by_name(name)? {
                bind_view_from_schema(catalog, ctx, view, alias.clone())
            } else if let Some(view) = resolve_system_view(None, name) {
                Ok(bind_system_view(ctx, view, alias.clone()))
            } else {
                Err(plan_error(
                    SqlState::UndefinedTable,
                    format!("table {name} does not exist"),
                ))
            }
        }
        Some("public") => {
            if let Some(table) = catalog.get_table_by_name(name)? {
                return Ok(bind_table_from_schema(ctx, table, alias.clone()));
            }
            if let Some(view) = catalog.get_view_by_name(name)? {
                return bind_view_from_schema(catalog, ctx, view, alias.clone());
            }
            Err(plan_error(
                SqlState::UndefinedTable,
                format!("table public.{name} does not exist"),
            ))
        }
        Some(schema) if is_system_schema(schema) => match resolve_system_view(Some(schema), name) {
            Some(view) => Ok(bind_system_view(ctx, view, alias.clone())),
            None => Err(plan_error(
                SqlState::UndefinedTable,
                format!("table {schema}.{name} does not exist"),
            )),
        },
        Some(schema) => Err(plan_error(
            SqlState::InvalidSchemaName,
            format!("schema \"{schema}\" does not exist"),
        )),
    }
}

fn parse_view_query(view: &ViewSchema) -> Result<Query> {
    match parser::parse(&view.definition)? {
        Statement::Query(query) => Ok(query),
        _ => Err(plan_error(
            SqlState::SyntaxError,
            format!("view {} definition is not a SELECT query", view.name),
        )),
    }
}

fn bind_view_from_schema(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    view: ViewSchema,
    alias: Option<String>,
) -> Result<BoundFrom> {
    let query_ast = parse_view_query(&view)?;
    // A stored view definition binds in its own scope. Caller CTEs must not
    // change what base relations the persisted SQL resolves to.
    let query = bind_query(
        catalog,
        &query_ast,
        &ctx.declared_params,
        &CteScope::default(),
        None,
    )?;
    let output_len = query.output_schema().len();
    if output_len != view.columns.len() {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            format!(
                "view {} definition returns {output_len} columns but catalog has {} columns",
                view.name,
                view.columns.len()
            ),
        ));
    }
    let visible_name = alias.unwrap_or_else(|| view.name.clone());
    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let slot_start = ctx.next_slot;
    ctx.next_slot += view.columns.len();
    ctx.bindings.push(Binding {
        id: binding,
        table_id: None,
        table_name: view.name.clone(),
        visible_name: visible_name.clone(),
        columns: view.columns.clone(),
        slot_start,
        qualified_only: false,
    });
    Ok(BoundFrom::View {
        view: view.id,
        schema_version: view.schema_version,
        query: Box::new(query),
        binding,
        alias: visible_name,
        schema: view.columns,
    })
}

fn bind_system_view(ctx: &mut BindContext, view: SystemView, alias: Option<String>) -> BoundFrom {
    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let columns = view.columns();
    let slot_start = ctx.next_slot;
    ctx.next_slot += columns.len();
    ctx.bindings.push(Binding {
        id: binding,
        table_id: None,
        table_name: view.qualified_name(),
        visible_name: alias.clone().unwrap_or_else(|| view.name().to_string()),
        columns: columns.clone(),
        slot_start,
        qualified_only: false,
    });
    BoundFrom::System {
        view,
        binding,
        alias,
        schema: columns,
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
        name: table.name,
        alias,
        schema: table.columns,
    }
}

/// Bind a reference to a CTE (`FROM cte [AS alias]`): inline the CTE's already-bound
/// query as a derived table, exposing its columns under `alias` (or the CTE name).
/// The CTE is bound once; each reference gets a clone of its plan, exactly like a
/// derived table, so no new plan node or executor operator is needed. Takes the
/// binding by value so the (already-cloned) plan is moved in, not cloned again.
fn bind_cte_reference(ctx: &mut BindContext, cte: CteBinding, alias: Option<String>) -> BoundFrom {
    let CteBinding {
        name,
        query,
        columns,
    } = cte;
    let visible_name = alias.unwrap_or_else(|| name.clone());
    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let slot_start = ctx.next_slot;
    ctx.next_slot += columns.len();
    ctx.bindings.push(Binding {
        id: binding,
        table_id: None,
        table_name: name,
        visible_name: visible_name.clone(),
        columns: columns.clone(),
        slot_start,
        qualified_only: false,
    });
    BoundFrom::Derived {
        query: Box::new(query),
        binding,
        alias: visible_name,
        schema: columns,
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
    // The derived subquery is bound in its own binding scope but still sees the
    // enclosing query's CTEs.
    let query = bind_query(
        catalog,
        subquery,
        &ctx.declared_params,
        &ctx.cte_scope,
        None,
    )?;
    let columns = derive_alias_columns(&query.output_columns(), column_aliases, || {
        format!("table \"{alias}\"")
    })?;

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
    expected: Option<DataType>,
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
                        wildcard_source: binding.table_id,
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
                    wildcard_source: binding.table_id,
                });
            }
        }
        SelectItem::Expression { expr, alias } => {
            let bound = bind_expr(ctx, expr, expected)?;
            let alias = alias.clone().unwrap_or_else(|| derive_alias(expr));
            output.push(BoundSelectItem {
                expr: bound,
                alias,
                wildcard_source: None,
            });
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
