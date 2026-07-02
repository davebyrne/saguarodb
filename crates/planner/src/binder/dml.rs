use std::collections::HashSet;

use catalog::CatalogManager;
use common::{
    ColumnDef, ColumnDefault, ColumnId, ColumnInfo, CopyDirection, CopyOptions, DataType, DbError,
    Result, SqlState, TableSchema,
};
use parser::{
    Assignment, ConflictAction, ConflictTarget, Expr, InsertSource, OnConflict, Query, SelectItem,
};

use crate::{
    BoundExpr, BoundInsertSource, BoundOnConflict, BoundQueryBody, BoundReturning, BoundSelect,
    BoundSelectItem, BoundStatement,
};

use super::expr::{bind_boolean_expr, bind_expr};
use super::query::{
    bind_excluded_binding, bind_query, bind_select_item, bind_table_from_schema,
    select_output_schema,
};
use super::{
    BindContext, Binding, input_ref, plan_error, reject_aggregate, require_table, require_type,
};

/// Bind `COPY <table> [(cols)] FROM STDIN | TO STDOUT`: resolve the table and the
/// (possibly defaulted) column list to ids, reusing the INSERT column resolver.
/// COPY is driven by the server, so this performs only name resolution; the
/// executor's COPY routines reuse the storage insert/scan paths. Unlike INSERT,
/// COPY FROM does not reject an omitted NOT NULL column up front — that surfaces
/// per row (matching PostgreSQL) when the row's NULL fails `validate_not_null`.
pub(super) fn bind_copy(
    catalog: &dyn CatalogManager,
    table_name: &str,
    column_names: &[String],
    direction: CopyDirection,
    options: &CopyOptions,
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
    let columns = insert_columns(&table, column_names)?;
    Ok(BoundStatement::Copy {
        table: table.id,
        columns,
        direction,
        options: options.clone(),
    })
}

pub(super) fn bind_insert(
    catalog: &dyn CatalogManager,
    table_name: &str,
    column_names: &[String],
    source: &InsertSource,
    on_conflict: Option<&OnConflict>,
    returning: Option<&[SelectItem]>,
    declared: &[Option<DataType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
    let columns = insert_columns(&table, column_names)?;
    validate_insert_omissions(&table, &columns)?;

    let source = match source {
        InsertSource::Values(rows) => {
            bind_insert_values(catalog, &table, &columns, rows, declared)?
        }
        InsertSource::Query(select) => {
            bind_insert_query(catalog, &table, &columns, select, declared)?
        }
    };

    let on_conflict = bind_on_conflict(catalog, &table, on_conflict, declared)?;
    let returning = bind_returning(catalog, &table, returning, declared)?;

    Ok(BoundStatement::Insert {
        table: table.id,
        columns,
        source,
        on_conflict,
        returning,
    })
}

/// Bind an `ON CONFLICT` clause. The arbiter is always the primary key: an
/// explicit conflict target must name exactly the primary-key column(s), and
/// `DO UPDATE` requires a target (PostgreSQL does too). `DO UPDATE` assignments
/// and `WHERE` bind over `target ++ excluded` (see [`bind_do_update`]).
fn bind_on_conflict(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    on_conflict: Option<&OnConflict>,
    declared: &[Option<DataType>],
) -> Result<Option<BoundOnConflict>> {
    let Some(on_conflict) = on_conflict else {
        return Ok(None);
    };
    let requires_target = matches!(on_conflict.action, ConflictAction::DoUpdate { .. });
    validate_conflict_target(table, on_conflict.target.as_ref(), requires_target)?;

    let action = match &on_conflict.action {
        ConflictAction::DoNothing => BoundOnConflict::DoNothing,
        ConflictAction::DoUpdate {
            assignments,
            filter,
        } => bind_do_update(catalog, table, assignments, filter.as_ref(), declared)?,
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
) -> Result<()> {
    let Some(ConflictTarget::Columns(columns)) = target else {
        if requires_target {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "ON CONFLICT DO UPDATE requires a conflict target (the primary key)",
            ));
        }
        return Ok(());
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
    Ok(())
}

/// Bind an `ON CONFLICT ... DO UPDATE SET ... [WHERE ...]` action. Assignment
/// value expressions and the optional `WHERE` are bound over two bindings: the
/// existing target row (bare columns) and the proposed `excluded` row
/// (`excluded.<col>`). The primary key cannot be assigned, and duplicate
/// assignments are rejected — same as `UPDATE`.
fn bind_do_update(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    assignments: &[Assignment],
    filter: Option<&Expr>,
    declared: &[Option<DataType>],
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
        if table.primary_key.contains(&column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("cannot update primary key column {}", column.name),
            ));
        }
        if !seen.insert(column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("duplicate assignment for column {}", column.name),
            ));
        }
        let value = bind_expr(&mut ctx, &assignment.value, Some(column.data_type.clone()))?;
        reject_aggregate(&value)?;
        validate_assignable(&value, column)?;
        bound_assignments.push((column.id, value));
    }

    let filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &filter {
        reject_aggregate(filter)?;
    }

    Ok(BoundOnConflict::DoUpdate {
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
    declared: &[Option<DataType>],
) -> Result<Option<BoundReturning>> {
    let Some(items) = items else {
        return Ok(None);
    };
    let mut ctx = BindContext::new(catalog, declared);
    bind_table_from_schema(&mut ctx, table.clone(), None);
    let mut bound_items = Vec::new();
    for item in items {
        bind_select_item(&mut ctx, item, &mut bound_items)?;
    }
    for item in &bound_items {
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
    declared: &[Option<DataType>],
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
    declared: &[Option<DataType>],
) -> Result<BoundInsertSource> {
    let query = bind_query(catalog, subquery, declared)?;
    let BoundQueryBody::Select(select) = &query.body;
    let source_columns = &select.columns;
    if source_columns.len() != columns.len() {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "INSERT ... SELECT query produces a different number of columns than the target",
        ));
    }
    for (item, column_id) in source_columns.iter().zip(columns) {
        let column = column_by_id(table, *column_id)?;
        validate_assignable(&item.expr, column)?;
    }
    Ok(BoundInsertSource::Query(Box::new(query)))
}

pub(super) fn bind_update(
    catalog: &dyn CatalogManager,
    table_name: &str,
    assignments: &[Assignment],
    filter: Option<&Expr>,
    returning: Option<&[SelectItem]>,
    declared: &[Option<DataType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
    let returning = bind_returning(catalog, &table, returning, declared)?;
    let mut ctx = BindContext::new(catalog, declared);
    let from = bind_table_from_schema(&mut ctx, table.clone(), None);
    let source_filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &source_filter {
        reject_aggregate(filter)?;
    }

    let mut seen = HashSet::new();
    let mut bound_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let column = column_by_name(&table, &assignment.column)?;
        if table.primary_key.contains(&column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("cannot update primary key column {}", column.name),
            ));
        }
        if !seen.insert(column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("duplicate assignment for column {}", column.name),
            ));
        }
        let value = bind_expr(&mut ctx, &assignment.value, Some(column.data_type.clone()))?;
        reject_aggregate(&value)?;
        validate_assignable(&value, column)?;
        bound_assignments.push((column.id, value));
    }

    Ok(BoundStatement::Update {
        table: table.id,
        assignments: bound_assignments,
        source: BoundSelect {
            distinct: None,
            columns: table_select_items(&table, &ctx.bindings[0]),
            from: Some(from),
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            output_schema: table_output_schema(&table),
        },
        returning,
    })
}

pub(super) fn bind_delete(
    catalog: &dyn CatalogManager,
    table_name: &str,
    filter: Option<&Expr>,
    returning: Option<&[SelectItem]>,
    declared: &[Option<DataType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
    let returning = bind_returning(catalog, &table, returning, declared)?;
    let mut ctx = BindContext::new(catalog, declared);
    let from = bind_table_from_schema(&mut ctx, table.clone(), None);
    let source_filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &source_filter {
        reject_aggregate(filter)?;
    }

    Ok(BoundStatement::Delete {
        table: table.id,
        source: BoundSelect {
            distinct: None,
            columns: table_select_items(&table, &ctx.bindings[0]),
            from: Some(from),
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            output_schema: table_output_schema(&table),
        },
        returning,
    })
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
        // A NOT NULL column may be omitted only when it supplies a non-NULL
        // DEFAULT; otherwise the omitted value would be NULL.
        let has_usable_default = matches!(
            &column.default,
            Some(ColumnDefault::Const(value)) if !matches!(value, common::Value::Null)
        ) || matches!(&column.default, Some(ColumnDefault::Nextval(_)));
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
    // A NUMERIC value is assignable to any NUMERIC column regardless of the
    // declared (precision, scale): the value is rounded to the column's scale and
    // checked for precision overflow at store time, not by type identity.
    let numeric_compatible = matches!(
        (expr.data_type(), &column.data_type),
        (DataType::Numeric { .. }, DataType::Numeric { .. })
    );
    if !numeric_compatible {
        require_type(expr, column.data_type.clone())?;
    }
    if !column.nullable && expr.nullable() {
        return Err(plan_error(
            SqlState::NotNullViolation,
            format!("column {} cannot be NULL", column.name),
        ));
    }
    Ok(())
}

fn table_select_items(table: &TableSchema, binding: &Binding) -> Vec<BoundSelectItem> {
    table
        .columns
        .iter()
        .map(|column| BoundSelectItem {
            expr: input_ref(binding, column),
            alias: column.name.clone(),
        })
        .collect()
}

fn table_output_schema(table: &TableSchema) -> Vec<ColumnInfo> {
    table
        .columns
        .iter()
        .map(|column| column_info_for_column(table, column))
        .collect()
}

fn column_info_for_column(table: &TableSchema, column: &ColumnDef) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        table_id: Some(table.id),
        column_id: Some(column.id),
    }
}
