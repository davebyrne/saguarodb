use std::collections::{BTreeMap, BTreeSet};

use catalog::{CatalogManager, SystemView, is_system_schema, resolve_system_view};
use common::{
    BindingId, ColumnDef, ColumnId, ColumnInfo, DataType, PgType, Result, SqlState, TableId,
    TableSchema, Value, ViewSchema,
};
use parser::{
    Cte, Distinct, Expr, FromItem, FunctionArg, OrderByItem, Query, QueryBody, Select, SelectItem,
    Statement,
};

use crate::{
    BoundDistinct, BoundExpr, BoundFrom, BoundOrderByItem, BoundQuery, BoundQueryBody, BoundSelect,
    BoundSelectItem, BoundSetOp, BoundValues, JoinType, OutputColumn,
};

use super::expr::{bind_boolean_expr, bind_expr};
use super::{
    BindContext, Binding, CteBinding, CteScope, OuterLink, PendingCorrelation, contains_aggregate,
    input_ref, plan_error, reject_aggregate, require_type,
};

/// Bind a query expression: bind any `WITH` CTEs into a child scope, then bind the
/// body and attach the query-level `ORDER BY`/`LIMIT`/`OFFSET`. `ctes` is the CTE
/// scope inherited from an enclosing query (empty at the statement level), so a
/// subquery or derived table sees the outer query's CTEs. `expected` supplies a
/// target type per output column, used only to type a bare `NULL` output column
/// (from the sibling arm of an enclosing set operation); `None` when there is no
/// such context.
#[allow(clippy::too_many_arguments)]
pub(super) fn bind_query<'a>(
    catalog: &'a dyn CatalogManager,
    query: &Query,
    declared: &[Option<PgType>],
    search_path: &[common::SchemaId],
    ctes: &CteScope,
    expected: Option<&[DataType]>,
    outer: &[OuterLink<'a>],
    pending: &mut Vec<PendingCorrelation>,
) -> Result<BoundQuery> {
    let scope = bind_ctes(catalog, &query.with, ctes, declared, search_path)?;
    match &query.body {
        QueryBody::Select(select) => {
            let (bound_select, order_by) = bind_select(
                catalog,
                select,
                &query.order_by,
                declared,
                search_path,
                &scope,
                expected,
                outer,
                pending,
            )?;
            Ok(BoundQuery {
                body: BoundQueryBody::Select(Box::new(bound_select)),
                order_by,
                limit: query.limit,
                offset: query.offset,
                correlations: Vec::new(),
            })
        }
        QueryBody::Values(rows) => {
            // `LIMIT`/`OFFSET` need no binding. `ORDER BY` resolves against the
            // VALUES output columns by position or name (like a set operation). The
            // CTE scope is threaded in because a subquery inside a VALUES row can
            // reference an enclosing CTE, even though VALUES itself has no FROM.
            let values = bind_values(
                catalog,
                rows,
                declared,
                search_path,
                &scope,
                expected,
                outer,
            )?;
            let mut bound = BoundQuery {
                body: BoundQueryBody::Values(values),
                order_by: Vec::new(),
                limit: query.limit,
                offset: query.offset,
                correlations: Vec::new(),
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
            let (left, right) = bind_set_op_arms(
                catalog,
                left,
                right,
                declared,
                search_path,
                &scope,
                expected,
                outer,
            )?;
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
                correlations: Vec::new(),
            })
        }
    }
}

/// Bind a child query whose body may reference `ctx`'s scope and the scopes
/// beyond it (a subquery expression body, or a LATERAL derived table's body).
/// Correlated references recorded during the bind are translated into the
/// returned query's `correlations` list: entries that resolved past the
/// immediately enclosing scope are re-interned into `ctx`'s own accumulator
/// and chained through an `OuterRef` (`docs/specs/subqueries.md` §4.2).
pub(super) fn bind_correlated_child_query(
    ctx: &mut BindContext,
    subquery: &Query,
) -> Result<BoundQuery> {
    let mut pending = Vec::new();
    let mut query = {
        let mut chain = Vec::with_capacity(1 + ctx.outer.len());
        chain.push(OuterLink { ctx, reject: None });
        chain.extend(ctx.outer.iter().copied());
        bind_query(
            ctx.catalog,
            subquery,
            &ctx.declared_params,
            &ctx.search_path,
            &ctx.cte_scope,
            None,
            &chain,
            &mut pending,
        )?
    };
    query.correlations = pending
        .into_iter()
        .map(|entry| {
            if entry.depth == 1 {
                entry.column
            } else {
                // Resolved past the immediate parent: intern one level up and
                // chain. `bind_column_ref` already rejected any reference that
                // crossed a reject-marked link, so this intern is always legal.
                let data_type = entry.column.data_type.clone();
                let nullable = entry.column.nullable;
                let slot = ctx.intern_correlation(entry.depth - 1, entry.column);
                crate::CorrelatedColumn {
                    outer: BoundExpr::OuterRef {
                        slot,
                        data_type: data_type.clone(),
                        nullable,
                    },
                    data_type,
                    nullable,
                }
            }
        })
        .collect();
    Ok(query)
}

/// Mark every link of an outer chain as rejected: the scopes stay walkable so
/// an outer reference produces a `FeatureNotSupported` error naming
/// `construct`, but no correlation can be recorded through them
/// (`docs/specs/subqueries.md` §1.1). An inner construct's marker overrides an
/// outer one — the reference crosses the inner boundary first.
fn rejected_links<'a>(outer: &[OuterLink<'a>], construct: &'static str) -> Vec<OuterLink<'a>> {
    outer
        .iter()
        .map(|link| OuterLink {
            ctx: link.ctx,
            reject: Some(construct),
        })
        .collect()
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
#[allow(clippy::too_many_arguments)]
fn bind_set_op_arms<'a>(
    catalog: &'a dyn CatalogManager,
    left: &Query,
    right: &Query,
    declared: &[Option<PgType>],
    search_path: &[common::SchemaId],
    ctes: &CteScope,
    expected: Option<&[DataType]>,
    outer: &[OuterLink<'a>],
) -> Result<(BoundQuery, BoundQuery)> {
    // An outer reference from a set-operation arm is rejected
    // (`docs/specs/subqueries.md` §1.1): the arms bind against a chain whose
    // links all reject, so `arm_pending` can never gain an entry.
    let arm_outer = rejected_links(outer, "a set-operation arm");
    let arm_pending = &mut Vec::new();
    if let Some(expected) = expected {
        // Output types already known: bind both arms directly, no retry.
        let left = bind_query(
            catalog,
            left,
            declared,
            search_path,
            ctes,
            Some(expected),
            &arm_outer,
            arm_pending,
        )?;
        let right = bind_query(
            catalog,
            right,
            declared,
            search_path,
            ctes,
            Some(expected),
            &arm_outer,
            arm_pending,
        )?;
        return Ok((left, right));
    }
    match bind_query(
        catalog,
        left,
        declared,
        search_path,
        ctes,
        None,
        &arm_outer,
        arm_pending,
    ) {
        Ok(left) => {
            let types = output_column_types(&left);
            let right = bind_query(
                catalog,
                right,
                declared,
                search_path,
                ctes,
                Some(&types),
                &arm_outer,
                arm_pending,
            )?;
            Ok((left, right))
        }
        Err(left_err) => {
            let Ok(right) = bind_query(
                catalog,
                right,
                declared,
                search_path,
                ctes,
                None,
                &arm_outer,
                arm_pending,
            ) else {
                return Err(left_err);
            };
            let types = output_column_types(&right);
            let left = bind_query(
                catalog,
                left,
                declared,
                search_path,
                ctes,
                Some(&types),
                &arm_outer,
                arm_pending,
            )?;
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
    declared: &[Option<PgType>],
    search_path: &[common::SchemaId],
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
        let bound = bind_cte(catalog, cte, &scope, declared, search_path)?;
        scope.ctes.push(bound);
    }
    Ok(scope)
}

/// Bind one CTE and derive its output columns (renamed by its column-alias list).
fn bind_cte(
    catalog: &dyn CatalogManager,
    cte: &Cte,
    scope: &CteScope,
    declared: &[Option<PgType>],
    search_path: &[common::SchemaId],
) -> Result<CteBinding> {
    // A CTE body is an isolated scope: it sees no enclosing bindings, so an
    // outer reference fails name resolution (matching PostgreSQL).
    let query = bind_query(
        catalog,
        &cte.query,
        declared,
        search_path,
        scope,
        None,
        &[],
        &mut Vec::new(),
    )?;
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
fn bind_values<'a>(
    catalog: &'a dyn CatalogManager,
    rows: &[Vec<Expr>],
    declared: &[Option<PgType>],
    search_path: &[common::SchemaId],
    ctes: &CteScope,
    expected: Option<&[DataType]>,
    outer: &[OuterLink<'a>],
) -> Result<BoundValues> {
    // Every VALUES entry binds in its own throwaway context, so there is no
    // single accumulator for `OuterRef` slots: outer references from a VALUES
    // body are rejected (`docs/specs/subqueries.md` §1.1).
    let outer = rejected_links(outer, "a VALUES list");
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
            let mut ctx = BindContext::with_outer(catalog, declared, search_path, outer.clone());
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
            let mut ctx = BindContext::with_outer(catalog, declared, search_path, outer.clone());
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
#[allow(clippy::too_many_arguments)]
fn bind_select<'a>(
    catalog: &'a dyn CatalogManager,
    select: &Select,
    order_by: &[OrderByItem],
    declared: &[Option<PgType>],
    search_path: &[common::SchemaId],
    ctes: &CteScope,
    expected: Option<&[DataType]>,
    outer: &[OuterLink<'a>],
    pending: &mut Vec<PendingCorrelation>,
) -> Result<(BoundSelect, Vec<BoundOrderByItem>)> {
    let mut ctx = BindContext::with_outer(catalog, declared, search_path, outer.to_vec());
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

    // Hand the correlations recorded against this scope's subquery boundary to
    // the caller; `bind_subquery` translates them into
    // `BoundQuery::correlations` when the boundary unwinds.
    pending.append(&mut ctx.correlations);

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
                // Semi/anti joins are planner-made (decorrelation); the
                // binder never produces them in a FROM clause.
                JoinType::Semi | JoinType::Anti => {
                    unreachable!("semi/anti joins do not appear in bound FROM clauses")
                }
            }
        }
        BoundFrom::Table { .. }
        | BoundFrom::System { .. }
        | BoundFrom::Derived { .. }
        | BoundFrom::View { .. }
        | BoundFrom::TableFunction { .. } => {}
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
        | BoundFrom::View { binding, .. }
        | BoundFrom::TableFunction { binding, .. } => output.push(*binding),
        BoundFrom::Join { left, right, .. } => {
            collect_from_binding_ids(left, output);
            collect_from_binding_ids(right, output);
        }
    }
}

pub(super) fn bind_from_item(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    item: &FromItem,
) -> Result<BoundFrom> {
    match item {
        FromItem::Table { name, alias } => bind_table_or_schema_qualified_name(
            catalog,
            ctx,
            name.schema.as_deref(),
            &name.name,
            alias,
        ),
        FromItem::Derived {
            subquery,
            alias,
            column_aliases,
            lateral,
        } => bind_derived_table(catalog, ctx, subquery, alias, column_aliases, *lateral),
        FromItem::TableFunction {
            name,
            args,
            alias,
            column_aliases,
            with_ordinality,
            ..
        } => bind_table_function(
            ctx,
            name,
            args,
            alias.as_deref(),
            column_aliases,
            *with_ordinality,
        ),
        FromItem::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let join_type = convert_join_type(join_type.clone());
            if matches!(join_type, JoinType::Right | JoinType::Full)
                && matches!(
                    **right,
                    FromItem::Derived { lateral: true, .. } | FromItem::TableFunction { .. }
                )
            {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "LATERAL is not supported on the nullable side of a RIGHT or FULL join",
                ));
            }
            let slots_before = ctx.next_slot;
            let join_scope_start = ctx.bindings.len();
            let left = bind_from_item(catalog, ctx, left)?;
            if let BoundFrom::TableFunction { args, .. } = &left
                && args.iter().any(references_input)
            {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "a table function referencing siblings must be the right side of its join",
                ));
            }
            // A LATERAL body referencing its siblings must be the RIGHT side
            // of its join: the plan lowers it to an Apply whose input is the
            // join's left subtree, which cannot supply columns from OUTSIDE
            // that subtree. Sibling-free (purely chained) laterals are fine
            // anywhere — the enclosing Apply substitutes them.
            if let BoundFrom::Derived {
                lateral: true,
                query,
                ..
            } = &left
                && query
                    .correlations
                    .iter()
                    .any(|correlation| !matches!(correlation.outer, BoundExpr::OuterRef { .. }))
            {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "a LATERAL derived table referencing its siblings must be \
                     the right side of its join",
                ));
            }
            let right = bind_from_item(catalog, ctx, right)?;
            if let BoundFrom::TableFunction { args, .. } = &right
                && args
                    .iter()
                    .any(|arg| references_input_slot_before(arg, slots_before))
            {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "a table function in an explicit join cannot reference FROM items outside that join",
                ));
            }
            // A sibling-referencing LATERAL lowers to an Apply whose input is
            // THIS join's left subtree; correlation slots are rebased to that
            // subtree during lowering, so references INTO the subtree are
            // fine — but a reference crossing the join boundary (an earlier
            // comma sibling) cannot be supplied by the Apply's input.
            if let BoundFrom::Derived {
                lateral: true,
                query,
                ..
            } = &right
                && query.correlations.iter().any(|correlation| {
                    matches!(
                        correlation.outer,
                        BoundExpr::InputRef { slot, .. } if slot < slots_before
                    )
                })
            {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "a LATERAL derived table in an explicit join cannot reference \
                     FROM items outside that join",
                ));
            }
            let saved_on_scope = ctx.on_scope_start;
            ctx.on_scope_start = Some(join_scope_start);
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
            ctx.on_scope_start = saved_on_scope;
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

fn bind_table_function(
    ctx: &mut BindContext,
    name: &str,
    args: &[Expr],
    alias: Option<&str>,
    column_aliases: &[String],
    with_ordinality: bool,
) -> Result<BoundFrom> {
    if with_ordinality {
        return Err(plan_error(
            SqlState::FeatureNotSupported,
            "WITH ORDINALITY is not supported",
        ));
    }
    if column_aliases.len() > 1 {
        return Err(plan_error(
            SqlState::SyntaxError,
            "table function has only one output column",
        ));
    }
    let (bound_args, data_type, nullable) = match name {
        "unnest" => {
            let [arg] = args else {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "UNNEST requires one array argument",
                ));
            };
            let arg = super::expr::bind_expr(ctx, arg, None)?;
            let DataType::Array(array_type) = arg.data_type() else {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    "UNNEST requires an array argument",
                ));
            };
            (vec![arg], array_type.element_type().clone(), true)
        }
        "generate_series" => {
            if !(2..=3).contains(&args.len()) {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "GENERATE_SERIES requires two or three arguments",
                ));
            }
            let args = args
                .iter()
                .map(|arg| super::expr::bind_expr(ctx, arg, Some(DataType::Integer)))
                .collect::<Result<Vec<_>>>()?;
            for arg in &args {
                require_type(arg, DataType::Integer)?;
            }
            (args, DataType::Integer, false)
        }
        _ => {
            return Err(plan_error(
                SqlState::SyntaxError,
                format!("table function {name} is not supported"),
            ));
        }
    };
    if bound_args.iter().any(contains_subquery_expr) {
        return Err(plan_error(
            SqlState::FeatureNotSupported,
            "subqueries in table-function arguments are not supported",
        ));
    }
    for arg in &bound_args {
        reject_aggregate(arg)?;
    }
    let visible_name = alias.unwrap_or(name).to_string();
    let column_name = column_aliases
        .first()
        .cloned()
        .unwrap_or_else(|| name.to_string());
    let columns = vec![ColumnDef {
        id: 0,
        name: column_name,
        data_type: data_type.clone(),
        nullable,
        max_length: None,
        default: None,
        pg_type: Some(PgType::from(&data_type)),
    }];
    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let slot_start = ctx.next_slot;
    ctx.next_slot += 1;
    ctx.bindings.push(Binding {
        id: binding,
        table_id: None,
        table_name: name.to_string(),
        visible_name: visible_name.clone(),
        columns: columns.clone(),
        slot_start,
        qualified_only: false,
    });
    Ok(BoundFrom::TableFunction {
        name: name.to_string(),
        args: bound_args,
        binding,
        alias: visible_name,
        schema: columns,
    })
}

fn contains_subquery_expr(expr: &BoundExpr) -> bool {
    if matches!(
        expr,
        BoundExpr::ScalarSubquery { .. } | BoundExpr::Exists { .. } | BoundExpr::InSubquery { .. }
    ) {
        return true;
    }
    let mut found = false;
    let _ = crate::params::for_each_child(expr, &mut |child| {
        found |= contains_subquery_expr(child);
        Ok(())
    });
    found
}

fn references_input_slot_before(expr: &BoundExpr, boundary: usize) -> bool {
    if matches!(expr, BoundExpr::InputRef { slot, .. } if *slot < boundary) {
        return true;
    }
    let mut found = false;
    let _ = crate::params::for_each_child(expr, &mut |child| {
        found |= references_input_slot_before(child, boundary);
        Ok(())
    });
    found
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
            } else {
                for schema in ctx.search_path.clone() {
                    if let Some(table) = catalog.get_table_in_schema(schema, name)? {
                        return Ok(bind_table_from_schema(ctx, table, alias.clone()));
                    }
                    if let Some(view) = catalog.get_view_in_schema(schema, name)? {
                        return bind_view_from_schema(catalog, ctx, view, alias.clone());
                    }
                    if catalog.get_index_in_schema(schema, name)?.is_some()
                        || catalog.get_sequence_in_schema(schema, name)?.is_some()
                    {
                        return Err(plan_error(
                            SqlState::WrongObjectType,
                            format!("relation {name} is not a table or view"),
                        ));
                    }
                }
                if let Some(view) = resolve_system_view(None, name) {
                    Ok(bind_system_view(ctx, view, alias.clone()))
                } else {
                    Err(plan_error(
                        SqlState::UndefinedTable,
                        format!("table {name} does not exist"),
                    ))
                }
            }
        }
        Some(schema) if is_system_schema(schema) => match resolve_system_view(Some(schema), name) {
            Some(view) => Ok(bind_system_view(ctx, view, alias.clone())),
            None => Err(plan_error(
                SqlState::UndefinedTable,
                format!("table {schema}.{name} does not exist"),
            )),
        },
        Some(schema) => {
            let namespace = catalog.get_schema_by_name(schema)?.ok_or_else(|| {
                plan_error(
                    SqlState::InvalidSchemaName,
                    format!("schema \"{schema}\" does not exist"),
                )
            })?;
            if let Some(table) = catalog.get_table_in_schema(namespace.id, name)? {
                return Ok(bind_table_from_schema(ctx, table, alias.clone()));
            }
            if let Some(view) = catalog.get_view_in_schema(namespace.id, name)? {
                return bind_view_from_schema(catalog, ctx, view, alias.clone());
            }
            Err(plan_error(
                SqlState::UndefinedTable,
                format!("table {schema}.{name} does not exist"),
            ))
        }
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
    let mut query_ast = parse_view_query(&view)?;
    stabilize_view_relation_names(catalog, &view, &mut query_ast)?;
    // A stored view definition binds in its own isolated scope. Caller CTEs
    // must not change what base relations the persisted SQL resolves to, and
    // it sees no enclosing bindings (an outer reference in a persisted view
    // definition is impossible: CREATE VIEW binds with no outer scope).
    let query = bind_query(
        catalog,
        &query_ast,
        &ctx.declared_params,
        &view.definition_search_path,
        &CteScope::default(),
        None,
        &[],
        &mut Vec::new(),
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

fn stabilize_view_relation_names(
    catalog: &dyn CatalogManager,
    view: &ViewSchema,
    query: &mut Query,
) -> Result<()> {
    let mut relations = BTreeMap::new();
    for schema_id in &view.definition_search_path {
        for dependency in &view.dependencies {
            let relation = if let Some(table) = catalog.get_table(dependency.relation)? {
                Some((table.schema_id, table.name))
            } else {
                catalog
                    .get_view(dependency.relation)?
                    .map(|view| (view.schema_id, view.name))
            };
            let Some((relation_schema, relation_name)) = relation else {
                continue;
            };
            if relation_schema == *schema_id {
                let Some(schema) = catalog.get_schema(relation_schema)? else {
                    continue;
                };
                relations
                    .entry(relation_name.clone())
                    .or_insert((schema.name, relation_name));
            }
        }
    }
    stabilize_query_relations(query, &relations, &BTreeSet::new());
    Ok(())
}

fn stabilize_query_relations(
    query: &mut Query,
    relations: &BTreeMap<String, (String, String)>,
    inherited_ctes: &BTreeSet<String>,
) {
    let mut ctes = inherited_ctes.clone();
    for cte in &mut query.with {
        stabilize_query_relations(&mut cte.query, relations, &ctes);
        ctes.insert(cte.name.clone());
    }
    match &mut query.body {
        QueryBody::Select(select) => {
            for item in &mut select.from {
                stabilize_from_relations(item, relations, &ctes);
            }
            for item in &mut select.columns {
                if let SelectItem::Expression { expr, .. } = item {
                    stabilize_expr_relations(expr, relations, &ctes);
                }
            }
            if let Some(filter) = &mut select.filter {
                stabilize_expr_relations(filter, relations, &ctes);
            }
            for expr in &mut select.group_by {
                stabilize_expr_relations(expr, relations, &ctes);
            }
            if let Some(having) = &mut select.having {
                stabilize_expr_relations(having, relations, &ctes);
            }
        }
        QueryBody::Values(rows) => {
            for expr in rows.iter_mut().flatten() {
                stabilize_expr_relations(expr, relations, &ctes);
            }
        }
        QueryBody::SetOp { left, right, .. } => {
            stabilize_query_relations(left, relations, &ctes);
            stabilize_query_relations(right, relations, &ctes);
        }
    }
    for order_by in &mut query.order_by {
        stabilize_expr_relations(&mut order_by.expr, relations, &ctes);
    }
}

fn stabilize_from_relations(
    item: &mut FromItem,
    relations: &BTreeMap<String, (String, String)>,
    ctes: &BTreeSet<String>,
) {
    match item {
        FromItem::Table { name, .. } if name.schema.is_none() && !ctes.contains(&name.name) => {
            if let Some((schema, relation)) = relations.get(&name.name) {
                name.schema = Some(schema.clone());
                name.name = relation.clone();
            }
        }
        FromItem::Table { .. } => {}
        FromItem::Derived { subquery, .. } => stabilize_query_relations(subquery, relations, ctes),
        FromItem::Join {
            left,
            right,
            condition,
            ..
        } => {
            stabilize_from_relations(left, relations, ctes);
            stabilize_from_relations(right, relations, ctes);
            if let Some(condition) = condition {
                stabilize_expr_relations(condition, relations, ctes);
            }
        }
    }
}

fn stabilize_expr_relations(
    expr: &mut Expr,
    relations: &BTreeMap<String, (String, String)>,
    ctes: &BTreeSet<String>,
) {
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            stabilize_query_relations(query, relations, ctes);
        }
        Expr::InSubquery { expr, subquery, .. } => {
            stabilize_expr_relations(expr, relations, ctes);
            stabilize_query_relations(subquery, relations, ctes);
        }
        Expr::BinaryOp { left, right, .. } => {
            stabilize_expr_relations(left, relations, ctes);
            stabilize_expr_relations(right, relations, ctes);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => stabilize_expr_relations(expr, relations, ctes),
        Expr::Function { args, .. } => {
            for arg in args {
                if let FunctionArg::Expr(expr) = arg {
                    stabilize_expr_relations(expr, relations, ctes);
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            stabilize_expr_relations(expr, relations, ctes);
            for item in list {
                stabilize_expr_relations(item, relations, ctes);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            stabilize_expr_relations(expr, relations, ctes);
            stabilize_expr_relations(low, relations, ctes);
            stabilize_expr_relations(high, relations, ctes);
        }
        Expr::Like { expr, pattern, .. } => {
            stabilize_expr_relations(expr, relations, ctes);
            stabilize_expr_relations(pattern, relations, ctes);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                stabilize_expr_relations(operand, relations, ctes);
            }
            for (when, then) in when_clauses {
                stabilize_expr_relations(when, relations, ctes);
                stabilize_expr_relations(then, relations, ctes);
            }
            if let Some(else_clause) = else_clause {
                stabilize_expr_relations(else_clause, relations, ctes);
            }
        }
        Expr::Literal(_) | Expr::Placeholder(_) | Expr::ColumnRef { .. } => {}
    }
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
        // An inlined CTE reference is never LATERAL.
        lateral: false,
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
    lateral: bool,
) -> Result<BoundFrom> {
    // The derived subquery is bound in its own binding scope but still sees
    // the enclosing query's CTEs. A LATERAL body additionally sees the FROM
    // bindings registered in `ctx` so far (its left siblings) plus the
    // enclosing scopes, recorded as correlations. A non-LATERAL body sees no
    // sibling bindings, and outer references past the derived boundary are
    // rejected rather than recorded — the enclosing chain is threaded
    // reject-marked purely so the error names the construct
    // (`docs/specs/subqueries.md` §1.1, §7).
    let query = if lateral {
        bind_correlated_child_query(ctx, subquery)?
    } else {
        let derived_outer = rejected_links(&ctx.outer, "a derived table");
        bind_query(
            catalog,
            subquery,
            &ctx.declared_params,
            &ctx.search_path,
            &ctx.cte_scope,
            None,
            &derived_outer,
            &mut Vec::new(),
        )?
    };
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
        lateral,
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
    // A subquery's body is its own scope; only its correlation entries (and
    // `InSubquery`'s left operand) are this query's expressions, so they —
    // not the whole subquery — must obey the grouped rule.
    match expr {
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            return validate_grouped_correlations(query, group_by);
        }
        BoundExpr::InSubquery {
            expr: operand,
            query,
            ..
        } => {
            validate_grouped_expr(operand, group_by)?;
            return validate_grouped_correlations(query, group_by);
        }
        _ => {}
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
        BoundExpr::Array { elements, .. } => {
            for element in elements {
                validate_grouped_expr(element, group_by)?;
            }
            Ok(())
        }
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            validate_grouped_expr(array, group_by)?;
            for subscript in subscripts {
                validate_grouped_expr(subscript, group_by)?;
            }
            Ok(())
        }
        BoundExpr::Any { left, array, .. } => {
            validate_grouped_expr(left, group_by)?;
            validate_grouped_expr(array, group_by)
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
        // Subquery variants returned early above; the remaining leaves cannot
        // contain an aggregate, so this arm is unreachable in practice.
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. }
        | BoundExpr::InSubquery { .. } => Ok(()),
        BoundExpr::AggregateCall { .. } => Ok(()),
    }
}

/// Validate a correlated subquery's correlation entries against the grouped
/// rule: each `outer` expression is evaluated against this query's rows, so
/// in an aggregate query it must be grouped (or reference no input).
fn validate_grouped_correlations(query: &BoundQuery, group_by: &[BoundExpr]) -> Result<()> {
    for correlation in &query.correlations {
        validate_grouped_expr(&correlation.outer, group_by)?;
    }
    Ok(())
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
        BoundExpr::Array { elements, .. } => elements.iter().any(references_input),
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => references_input(array) || subscripts.iter().any(references_input),
        BoundExpr::Any { left, array, .. } => references_input(left) || references_input(array),
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
        // A subquery references this scope's input only through its correlation
        // entries (`OuterRef`s inside the body point here via them); the body
        // itself is a separate scope. An `OuterRef` at THIS level references
        // the next scope out, not this one's input.
        BoundExpr::InSubquery { expr, query, .. } => {
            references_input(expr) || correlations_reference_input(query)
        }
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            correlations_reference_input(query)
        }
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. } => false,
    }
}

fn correlations_reference_input(query: &BoundQuery) -> bool {
    query
        .correlations
        .iter()
        .any(|correlation| references_input(&correlation.outer))
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
        BoundExpr::Parameter {
            pg_type: Some(pg_type),
            ..
        }
        | BoundExpr::Function {
            pg_type: Some(pg_type),
            ..
        } => pg_type.clone(),
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
