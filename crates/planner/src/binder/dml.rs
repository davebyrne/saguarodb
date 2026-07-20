use std::collections::HashSet;

use catalog::CatalogManager;
use common::{
    ColumnDef, ColumnDefault, ColumnId, ColumnInfo, CopyDirection, CopyOptions, DataType, DbError,
    PgType, QualifiedName, Result, SqlState, TableSchema,
};
use parser::{
    Assignment, ConflictAction, ConflictTarget, Expr, InsertSource, OnConflict, Query, SelectItem,
};

use crate::{
    BoundExpr, BoundFrom, BoundInsertSource, BoundOnConflict, BoundReturning, BoundSelect,
    BoundSelectItem, BoundStatement,
};

use super::expr::{bind_boolean_expr, bind_expr};
use super::query::{
    bind_excluded_binding, bind_query, bind_select_item, bind_table_from_schema,
    select_output_schema,
};
use super::{BindContext, CteScope, plan_error, reject_aggregate, reject_window, require_table};

/// Bind `COPY <table> [(cols)] FROM STDIN | TO STDOUT`: resolve the table and the
/// (possibly defaulted) column list to ids, reusing the INSERT column resolver.
/// COPY is driven by the server, so this performs only name resolution; the
/// executor's COPY routines reuse the storage insert/scan paths. Unlike INSERT,
/// COPY FROM does not reject an omitted NOT NULL column up front — that surfaces
/// per row (matching PostgreSQL) when the row's NULL fails `validate_not_null`.
pub(super) fn bind_copy(
    catalog: &dyn CatalogManager,
    table_name: &QualifiedName,
    search_path: &[common::SchemaId],
    column_names: &[String],
    direction: CopyDirection,
    options: &CopyOptions,
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name, search_path)?;
    let columns = insert_columns(&table, column_names)?;
    // COPY FROM inserts rows, so — like INSERT — it needs the omitted columns'
    // expression defaults evaluated per row and the table's CHECK constraints
    // enforced per row. COPY TO only reads, so it binds neither.
    let (default_exprs, check_exprs) = match direction {
        CopyDirection::From => (
            bind_omitted_expr_defaults(catalog, &table, &columns)?,
            super::bind_table_checks(catalog, &table)?,
        ),
        CopyDirection::To => (Vec::new(), Vec::new()),
    };
    Ok(BoundStatement::Copy {
        table: table.id,
        table_schema: table,
        columns,
        direction,
        options: options.clone(),
        default_exprs,
        check_exprs,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn bind_insert(
    catalog: &dyn CatalogManager,
    table_name: &QualifiedName,
    search_path: &[common::SchemaId],
    column_names: &[String],
    source: &InsertSource,
    on_conflict: Option<&OnConflict>,
    returning: Option<&[SelectItem]>,
    declared: &[Option<PgType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name, search_path)?;
    let columns = insert_columns(&table, column_names)?;
    validate_insert_omissions(&table, &columns)?;

    let source = match source {
        InsertSource::Values(rows) => {
            bind_insert_values(catalog, &table, &columns, rows, declared)?
        }
        InsertSource::Query(select) => {
            bind_insert_query(catalog, &table, &columns, select, declared, search_path)?
        }
    };

    let on_conflict = bind_on_conflict(catalog, &table, on_conflict, declared)?;
    let returning = bind_returning(catalog, &table, returning, declared)?;
    let default_exprs = bind_omitted_expr_defaults(catalog, &table, &columns)?;
    let check_exprs = super::bind_table_checks(catalog, &table)?;

    Ok(BoundStatement::Insert {
        table: table.id,
        columns,
        source,
        on_conflict,
        returning,
        default_exprs,
        check_exprs,
    })
}

/// Bind the expression `DEFAULT`s of columns omitted by this INSERT, so the
/// executor can evaluate them per row. Constant and sequence defaults need no
/// bound expression (the executor reads them from the schema); only
/// `ColumnDefault::Expr` columns appear here.
fn bind_omitted_expr_defaults(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    columns: &[ColumnId],
) -> Result<Vec<(ColumnId, BoundExpr)>> {
    let provided: HashSet<_> = columns.iter().copied().collect();
    let mut defaults = Vec::new();
    for column in &table.columns {
        if provided.contains(&column.id) {
            continue;
        }
        if let Some(ColumnDefault::Expr(expression)) = &column.default {
            defaults.push((
                column.id,
                crate::lower_stored_expression(catalog, expression, &[])?,
            ));
        }
    }
    Ok(defaults)
}

/// Bind an `ON CONFLICT` clause. The arbiter is always the primary key: an
/// explicit conflict target must name exactly the primary-key column(s), and
/// `DO UPDATE` requires a target (PostgreSQL does too). `DO UPDATE` assignments
/// and `WHERE` bind over `target ++ excluded` (see [`bind_do_update`]).
fn bind_on_conflict(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    on_conflict: Option<&OnConflict>,
    declared: &[Option<PgType>],
) -> Result<Option<BoundOnConflict>> {
    let Some(on_conflict) = on_conflict else {
        return Ok(None);
    };
    let requires_target = matches!(on_conflict.action, ConflictAction::DoUpdate { .. });
    let target = validate_conflict_target(table, on_conflict.target.as_ref(), requires_target)?;
    let target = match target {
        Some(target) => Some(target),
        None if !table.primary_key.is_empty() => Some(table.primary_key.clone()),
        None => None,
    };

    let action = match &on_conflict.action {
        ConflictAction::DoNothing => BoundOnConflict::DoNothing { target },
        ConflictAction::DoUpdate {
            assignments,
            filter,
        } => bind_do_update(
            catalog,
            table,
            target.clone().ok_or_else(|| {
                plan_error(
                    SqlState::FeatureNotSupported,
                    "ON CONFLICT DO UPDATE requires a conflict target (the primary key)",
                )
            })?,
            assignments,
            filter.as_ref(),
            declared,
        )?,
    };
    Ok(Some(action))
}

/// Validate the `ON CONFLICT` arbiter. SaguaroDB arbitrates only on the primary
/// key, so an explicit target must name exactly the primary-key column(s); any
/// other column list (a secondary unique index) is rejected with
/// `FeatureNotSupported`. A missing target is allowed for `DO NOTHING` but
/// rejected for `DO UPDATE`.
fn validate_conflict_target(
    table: &TableSchema,
    target: Option<&ConflictTarget>,
    requires_target: bool,
) -> Result<Option<Vec<ColumnId>>> {
    let Some(ConflictTarget::Columns(columns)) = target else {
        if requires_target {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "ON CONFLICT DO UPDATE requires a conflict target (the primary key)",
            ));
        }
        return Ok(None);
    };

    // Each named column must exist; then the set must equal the primary key.
    let mut named = Vec::with_capacity(columns.len());
    for name in columns {
        named.push(column_by_name(table, name)?.id);
    }
    named.sort_unstable();
    let mut pk = table.primary_key.clone();
    pk.sort_unstable();
    if named != pk {
        return Err(plan_error(
            SqlState::FeatureNotSupported,
            "ON CONFLICT arbiter must be the primary key; only the primary key is supported",
        ));
    }
    Ok(Some(named))
}

/// Bind an `ON CONFLICT ... DO UPDATE SET ... [WHERE ...]` action. Assignment
/// value expressions and the optional `WHERE` are bound over two bindings: the
/// existing target row (bare columns) and the proposed `excluded` row
/// (`excluded.<col>`). Duplicate assignments are rejected — same as `UPDATE`;
/// primary-key assignments are allowed and enforced by the PK constraint index
/// and storage rekeying.
fn bind_do_update(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    target: Vec<ColumnId>,
    assignments: &[Assignment],
    filter: Option<&Expr>,
    declared: &[Option<PgType>],
) -> Result<BoundOnConflict> {
    let mut ctx = BindContext::new(catalog, declared);
    // Target row first (slots 0..n; bare columns resolve here), then the
    // qualified-only `excluded` row (slots n..2n).
    bind_table_from_schema(&mut ctx, table.clone(), None);
    bind_excluded_binding(&mut ctx, table);

    let mut seen = HashSet::new();
    let mut bound_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let column = column_by_name(table, &assignment.column)?;
        if !seen.insert(column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("duplicate assignment for column {}", column.name),
            ));
        }
        let value = bind_expr(&mut ctx, &assignment.value, Some(column.data_type.clone()))?;
        let value = coerce_assignment_expr(value, column);
        reject_window(
            &value,
            "window functions are not allowed in ON CONFLICT SET",
        )?;
        reject_aggregate(&value)?;
        validate_assignable(&value, column)?;
        bound_assignments.push((column.id, value));
    }

    let filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &filter {
        reject_window(
            filter,
            "window functions are not allowed in ON CONFLICT WHERE",
        )?;
        reject_aggregate(filter)?;
    }

    Ok(BoundOnConflict::DoUpdate {
        target,
        assignments: bound_assignments,
        filter,
    })
}

/// Bind a `RETURNING` projection list over the target table. The expressions
/// reference the table's columns as a single binding in catalog (slot) order, so
/// at execution they evaluate over the affected full row (the inserted/updated
/// NEW row or the deleted OLD row). Aggregates are rejected (PostgreSQL does not
/// allow them in `RETURNING`).
fn bind_returning(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    items: Option<&[SelectItem]>,
    declared: &[Option<PgType>],
) -> Result<Option<BoundReturning>> {
    let Some(items) = items else {
        return Ok(None);
    };
    let mut ctx = BindContext::new(catalog, declared);
    bind_table_from_schema(&mut ctx, table.clone(), None);
    let mut bound_items = Vec::new();
    for item in items {
        bind_select_item(&mut ctx, item, None, &mut bound_items)?;
    }
    for item in &bound_items {
        reject_window(&item.expr, "window functions are not allowed in RETURNING")?;
        reject_aggregate(&item.expr)?;
    }
    let output_schema = select_output_schema(&ctx, &bound_items);
    let exprs = bound_items.into_iter().map(|item| item.expr).collect();
    Ok(Some(BoundReturning {
        exprs,
        output_schema,
    }))
}

fn bind_insert_values(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    columns: &[ColumnId],
    rows: &[Vec<Expr>],
    declared: &[Option<PgType>],
) -> Result<BoundInsertSource> {
    let mut bound_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != columns.len() {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                "INSERT row has wrong number of values",
            ));
        }

        let mut bound_row = Vec::with_capacity(row.len());
        for (expr, column_id) in row.iter().zip(columns) {
            let column = column_by_id(table, *column_id)?;
            let bound = bind_expr(
                &mut BindContext::new(catalog, declared),
                expr,
                Some(column.data_type.clone()),
            )?;
            let bound = coerce_assignment_expr(bound, column);
            reject_window(&bound, "window functions are not allowed in VALUES")?;
            reject_aggregate(&bound)?;
            validate_assignable(&bound, column)?;
            bound_row.push(bound);
        }
        bound_rows.push(bound_row);
    }

    let output_schema = columns
        .iter()
        .map(|column_id| {
            let column = column_by_id(table, *column_id)?;
            Ok(column_info_for_column(table, column))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(BoundInsertSource::Values {
        rows: bound_rows,
        output_schema,
    })
}

fn bind_insert_query(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    columns: &[ColumnId],
    subquery: &Query,
    declared: &[Option<PgType>],
    search_path: &[common::SchemaId],
) -> Result<BoundInsertSource> {
    // The INSERT source is a top-level query; it carries its own `WITH` (if any),
    // has no enclosing CTE scope, and gets no external `expected` types.
    let query = bind_query(
        catalog,
        subquery,
        declared,
        search_path,
        &CteScope::default(),
        None,
        &[],
        &mut Vec::new(),
    )?;
    let source_columns = query.output_columns();
    if source_columns.len() != columns.len() {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "INSERT ... SELECT query produces a different number of columns than the target",
        ));
    }
    for (source, column_id) in source_columns.iter().zip(columns) {
        let column = column_by_id(table, *column_id)?;
        validate_assignable_from(&source.data_type, source.nullable, column)?;
    }
    Ok(BoundInsertSource::Query(Box::new(query)))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn bind_update(
    catalog: &dyn CatalogManager,
    table_name: &QualifiedName,
    search_path: &[common::SchemaId],
    assignments: &[Assignment],
    from_items: &[parser::FromItem],
    filter: Option<&Expr>,
    returning: Option<&[SelectItem]>,
    declared: &[Option<PgType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name, search_path)?;
    // RETURNING binds against the target table only: it is evaluated over the
    // updated (new) row, which carries no FROM columns
    // (docs/specs/subqueries.md section 8 — a documented divergence from
    // PostgreSQL, which also exposes the matched FROM row).
    let returning = bind_returning(catalog, &table, returning, declared)?;
    let mut ctx = BindContext::new(catalog, declared);
    let from = bind_table_from_schema(&mut ctx, table.clone(), None);
    // UPDATE ... FROM: the extra relations join the target with the target as
    // the leftmost input, so assignments and WHERE see the combined scope.
    let from = join_dml_from_items(catalog, &mut ctx, from, from_items)?;
    let joined_source = !from_items.is_empty();
    let source_filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &source_filter {
        reject_window(filter, "window functions are not allowed in WHERE")?;
        reject_aggregate(filter)?;
    }

    let mut seen = HashSet::new();
    let mut bound_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let column = column_by_name(&table, &assignment.column)?;
        if !seen.insert(column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("duplicate assignment for column {}", column.name),
            ));
        }
        let value = bind_expr(&mut ctx, &assignment.value, Some(column.data_type.clone()))?;
        let value = coerce_assignment_expr(value, column);
        reject_window(&value, "window functions are not allowed in UPDATE SET")?;
        reject_aggregate(&value)?;
        validate_assignable(&value, column)?;
        bound_assignments.push((column.id, value));
    }

    let check_exprs = super::bind_table_checks(catalog, &table)?;

    Ok(BoundStatement::Update {
        table: table.id,
        assignments: bound_assignments,
        source: BoundSelect {
            source_width: ctx.next_slot,
            distinct: None,
            columns: bindings_select_items(&ctx),
            from: Some(from),
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            output_schema: bindings_output_schema(&ctx),
        },
        joined_source,
        returning,
        check_exprs,
    })
}

pub(super) fn bind_delete(
    catalog: &dyn CatalogManager,
    table_name: &QualifiedName,
    search_path: &[common::SchemaId],
    using: &[parser::FromItem],
    filter: Option<&Expr>,
    returning: Option<&[SelectItem]>,
    declared: &[Option<PgType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name, search_path)?;
    let returning = bind_returning(catalog, &table, returning, declared)?;
    let mut ctx = BindContext::new(catalog, declared);
    let from = bind_table_from_schema(&mut ctx, table.clone(), None);
    // DELETE ... USING joins the extra relations exactly like UPDATE ... FROM.
    let from = join_dml_from_items(catalog, &mut ctx, from, using)?;
    let joined_source = !using.is_empty();
    let source_filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &source_filter {
        reject_window(filter, "window functions are not allowed in WHERE")?;
        reject_aggregate(filter)?;
    }

    Ok(BoundStatement::Delete {
        table: table.id,
        source: BoundSelect {
            source_width: ctx.next_slot,
            distinct: None,
            columns: bindings_select_items(&ctx),
            from: Some(from),
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            output_schema: bindings_output_schema(&ctx),
        },
        joined_source,
        returning,
    })
}

/// Fold `UPDATE ... FROM` / `DELETE ... USING` items onto the target's
/// binding, target leftmost, mirroring a comma FROM list. Each item may
/// itself be an explicit join or a (possibly LATERAL) derived table.
fn join_dml_from_items(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    target: BoundFrom,
    items: &[parser::FromItem],
) -> Result<BoundFrom> {
    let mut bound = target;
    for item in items {
        let right = super::query::bind_from_item(catalog, ctx, item)?;
        bound = BoundFrom::Join {
            left: Box::new(bound),
            right: Box::new(right),
            join_type: crate::JoinType::Cross,
            condition: None,
        };
    }
    Ok(bound)
}

/// One select item per registered binding column, in slot order — the
/// combined (target ++ FROM) row a joined DML source produces.
fn bindings_select_items(ctx: &BindContext) -> Vec<BoundSelectItem> {
    ctx.bindings
        .iter()
        .flat_map(|binding| {
            binding.columns.iter().map(|column| BoundSelectItem {
                expr: super::input_ref(binding, column),
                alias: column.name.clone(),
            })
        })
        .collect()
}

fn bindings_output_schema(ctx: &BindContext) -> Vec<ColumnInfo> {
    ctx.bindings
        .iter()
        .flat_map(|binding| {
            binding.columns.iter().map(|column| ColumnInfo {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                table_id: binding.table_id,
                column_id: Some(column.id),
                pg_type: column.pg_type.clone(),
            })
        })
        .collect()
}

fn insert_columns(table: &TableSchema, column_names: &[String]) -> Result<Vec<ColumnId>> {
    if column_names.is_empty() {
        return Ok(table.columns.iter().map(|column| column.id).collect());
    }
    let mut seen = HashSet::new();
    column_names
        .iter()
        .map(|name| {
            let column = column_by_name(table, name)?;
            if !seen.insert(column.id) {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!("duplicate insert column {}", column.name),
                ));
            }
            Ok(column.id)
        })
        .collect()
}

fn validate_insert_omissions(table: &TableSchema, columns: &[ColumnId]) -> Result<()> {
    let provided: HashSet<_> = columns.iter().copied().collect();
    for column in &table.columns {
        // A NOT NULL column may be omitted only when it supplies a DEFAULT that
        // can produce a non-NULL value. A constant NULL default cannot; a sequence
        // default never does; an expression default might, so it is allowed here
        // and a NULL result is caught per row at insert time (matching PostgreSQL).
        let has_usable_default = matches!(
            &column.default,
            Some(ColumnDefault::Const(value)) if !matches!(value, common::Value::Null)
        ) || matches!(
            &column.default,
            Some(ColumnDefault::Nextval(_)) | Some(ColumnDefault::Expr(_))
        );
        if !column.nullable && !has_usable_default && !provided.contains(&column.id) {
            return Err(plan_error(
                SqlState::NotNullViolation,
                format!("column {} cannot be omitted", column.name),
            ));
        }
    }
    Ok(())
}

fn column_by_name<'a>(table: &'a TableSchema, name: &str) -> Result<&'a ColumnDef> {
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| {
            plan_error(
                SqlState::UndefinedColumn,
                format!("column {name} does not exist"),
            )
        })
}

fn column_by_id(table: &TableSchema, id: ColumnId) -> Result<&ColumnDef> {
    table
        .columns
        .iter()
        .find(|column| column.id == id)
        .ok_or_else(|| {
            DbError::internal(format!(
                "catalog table {} is missing column id {id}",
                table.name
            ))
        })
}

fn validate_assignable(expr: &BoundExpr, column: &ColumnDef) -> Result<()> {
    validate_assignable_from(&expr.data_type(), expr.nullable(), column)
}

/// DML assignment expressions are otherwise strict, but PostgreSQL accepts
/// assigning `TIMESTAMPTZ` expressions to `TIMESTAMP` columns. Keep the exception
/// explicit and local to expression assignments; `INSERT ... SELECT` remains a
/// strict output-column check.
fn coerce_assignment_expr(expr: BoundExpr, column: &ColumnDef) -> BoundExpr {
    if expr.data_type() == DataType::TimestampTz && column.data_type == DataType::Timestamp {
        let nullable = expr.nullable();
        return BoundExpr::Cast {
            expr: Box::new(expr),
            data_type: DataType::Timestamp,
            pg_type: PgType::Timestamp,
            nullable,
        };
    }
    expr
}

/// Whether a source column of `(data_type, nullable)` may feed `column`: the types
/// must match (a NUMERIC value is assignable to any NUMERIC column regardless of
/// its declared precision/scale — rounded and range-checked at store time), and a
/// nullable source cannot feed a `NOT NULL` column. Used both for a single bound
/// expression and for the output columns of an `INSERT ... <query>` source.
fn validate_assignable_from(
    data_type: &DataType,
    nullable: bool,
    column: &ColumnDef,
) -> Result<()> {
    let numeric_compatible = matches!(
        (data_type, &column.data_type),
        (DataType::Numeric { .. }, DataType::Numeric { .. })
    );
    if !numeric_compatible && *data_type != column.data_type {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            format!(
                "expected expression type {:?}, got {:?}",
                column.data_type, data_type
            ),
        ));
    }
    if !column.nullable && nullable {
        return Err(plan_error(
            SqlState::NotNullViolation,
            format!("column {} cannot be NULL", column.name),
        ));
    }
    Ok(())
}

fn column_info_for_column(table: &TableSchema, column: &ColumnDef) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        table_id: Some(table.id),
        column_id: Some(column.id),
        // A base-table column reports its declared wire type.
        pg_type: Some(column.wire_type()),
    }
}
