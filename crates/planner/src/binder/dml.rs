use std::collections::HashSet;

use catalog::CatalogManager;
use common::{
    ColumnDef, ColumnId, ColumnInfo, CopyDirection, CopyOptions, DataType, DbError, Result,
    SqlState, TableSchema,
};
use parser::{Assignment, Expr, InsertSource, SelectStatement};

use crate::{BoundExpr, BoundInsertSource, BoundSelect, BoundSelectItem, BoundStatement};

use super::expr::{bind_boolean_expr, bind_expr};
use super::query::{bind_select, bind_table_from_schema};
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

    Ok(BoundStatement::Insert {
        table: table.id,
        columns,
        source,
    })
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
    select: &SelectStatement,
    declared: &[Option<DataType>],
) -> Result<BoundInsertSource> {
    let bound = bind_select(catalog, select, declared)?;
    if bound.columns.len() != columns.len() {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "INSERT ... SELECT query produces a different number of columns than the target",
        ));
    }
    for (item, column_id) in bound.columns.iter().zip(columns) {
        let column = column_by_id(table, *column_id)?;
        validate_assignable(&item.expr, column)?;
    }
    Ok(BoundInsertSource::Query(Box::new(bound)))
}

pub(super) fn bind_update(
    catalog: &dyn CatalogManager,
    table_name: &str,
    assignments: &[Assignment],
    filter: Option<&Expr>,
    declared: &[Option<DataType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
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
            from,
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            output_schema: table_output_schema(&table),
        },
    })
}

pub(super) fn bind_delete(
    catalog: &dyn CatalogManager,
    table_name: &str,
    filter: Option<&Expr>,
    declared: &[Option<DataType>],
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
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
            from,
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            output_schema: table_output_schema(&table),
        },
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
        if !column.nullable && !provided.contains(&column.id) {
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
