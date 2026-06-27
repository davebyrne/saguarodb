use std::collections::HashSet;

use catalog::CatalogManager;
use common::{BindingId, ColumnDef, DataType, DbError, Result, SqlState, TableId, TableSchema};
use parser::Statement;

use crate::{BoundExpr, BoundStatement};

mod dml;
mod expr;
mod query;

use dml::{bind_copy, bind_delete, bind_insert, bind_update};
use query::bind_select;

#[derive(Clone, Debug)]
struct Binding {
    id: BindingId,
    table_id: TableId,
    table_name: String,
    visible_name: String,
    columns: Vec<ColumnDef>,
    slot_start: usize,
}

#[derive(Default)]
struct BindContext {
    bindings: Vec<Binding>,
    next_binding: BindingId,
    next_slot: usize,
    /// Parameter type OIDs declared by an extended-protocol `Parse` (mapped to
    /// `DataType`), 0-based and `None` when unspecified. Empty for simple queries.
    declared_params: Vec<Option<DataType>>,
}

impl BindContext {
    fn new(declared_params: &[Option<DataType>]) -> Self {
        Self {
            declared_params: declared_params.to_vec(),
            ..Self::default()
        }
    }

    fn declared_param(&self, index: usize) -> Option<DataType> {
        self.declared_params.get(index).cloned().flatten()
    }
}

/// Bind a statement from the simple query protocol. Query parameters are not
/// allowed here.
pub fn bind(statement: &Statement, catalog: &dyn CatalogManager) -> Result<BoundStatement> {
    let bound = bind_inner(statement, catalog, &[])?;
    if !crate::params::collect_param_types(&bound, &[])?.is_empty() {
        return Err(plan_error(
            SqlState::SyntaxError,
            "query parameters are not supported in the simple query protocol",
        ));
    }
    Ok(bound)
}

/// Bind a statement from the extended query protocol, resolving `$n` parameter
/// types (honoring the `Parse`-declared OIDs, otherwise inferring from context).
/// Returns the bound statement and the resolved parameter types by position.
pub fn bind_parameterized(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    declared_param_types: &[Option<DataType>],
) -> Result<(BoundStatement, Vec<DataType>)> {
    let bound = bind_inner(statement, catalog, declared_param_types)?;
    let params = crate::params::collect_param_types(&bound, declared_param_types)?;
    Ok((bound, params))
}

fn bind_inner(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    declared: &[Option<DataType>],
) -> Result<BoundStatement> {
    match statement {
        Statement::CreateTable {
            name,
            columns,
            primary_key,
        } => {
            let mut seen_primary_key_names = HashSet::new();
            for primary_key_name in primary_key {
                if !seen_primary_key_names.insert(primary_key_name) {
                    return Err(plan_error(
                        SqlState::SyntaxError,
                        format!("duplicate primary key column {primary_key_name}"),
                    ));
                }
            }
            if primary_key.len() != 1 {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    "v1 requires exactly one primary key column",
                ));
            }
            Ok(BoundStatement::CreateTable {
                name: name.clone(),
                columns: columns.clone(),
                primary_key: primary_key.clone(),
            })
        }
        Statement::DropTable { name } => {
            let table = require_table(catalog, name)?;
            Ok(BoundStatement::DropTable { table: table.id })
        }
        Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => Ok(BoundStatement::CreateIndex {
            name: name.clone(),
            table: table.clone(),
            columns: columns.clone(),
            unique: *unique,
        }),
        Statement::DropIndex { name } => {
            let index = require_index(catalog, name)?;
            Ok(BoundStatement::DropIndex { index: index.id })
        }
        Statement::Insert {
            table,
            columns,
            source,
        } => bind_insert(catalog, table, columns, source, declared),
        Statement::Select(select) => {
            bind_select(catalog, select, declared).map(BoundStatement::Select)
        }
        Statement::Update {
            table,
            assignments,
            filter,
        } => bind_update(catalog, table, assignments, filter.as_ref(), declared),
        Statement::Delete { table, filter } => {
            bind_delete(catalog, table, filter.as_ref(), declared)
        }
        Statement::Explain(inner) => Ok(BoundStatement::Explain(Box::new(bind_inner(
            inner, catalog, declared,
        )?))),
        // Transaction control is dispatched before binding (see `statement_class`
        // in the server), so the binder should not normally see these; this
        // defensive arm keeps the public `bind` API honest if called directly, and
        // never silently no-ops a BEGIN / SET TRANSACTION.
        Statement::Begin { .. }
        | Statement::Commit
        | Statement::Rollback
        | Statement::SetTransaction { .. }
        | Statement::SetSessionCharacteristics { .. } => Err(plan_error(
            SqlState::FeatureNotSupported,
            "transaction control statements do not bind",
        )),
        // VACUUM is a maintenance command dispatched to `run_vacuum` before binding
        // (it is not relational and never binds/plans). This defensive arm keeps the
        // public `bind` API total if called directly.
        Statement::Vacuum { .. } => Err(plan_error(
            SqlState::FeatureNotSupported,
            "VACUUM is a maintenance command and does not bind",
        )),
        Statement::Copy {
            table,
            columns,
            direction,
            options,
        } => bind_copy(catalog, table, columns, *direction, options),
    }
}

fn require_table(catalog: &dyn CatalogManager, name: &str) -> Result<TableSchema> {
    catalog.get_table_by_name(name)?.ok_or_else(|| {
        plan_error(
            SqlState::UndefinedTable,
            format!("table {name} does not exist"),
        )
    })
}

fn require_index(catalog: &dyn CatalogManager, name: &str) -> Result<common::IndexSchema> {
    catalog.get_index_by_name(name)?.ok_or_else(|| {
        plan_error(
            SqlState::UndefinedTable,
            format!("index {name} does not exist"),
        )
    })
}

fn input_ref(binding: &Binding, column: &ColumnDef) -> BoundExpr {
    BoundExpr::InputRef {
        input: binding.id,
        column: column.id,
        slot: binding.slot_start + usize::from(column.id),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn require_type(expr: &BoundExpr, expected: DataType) -> Result<()> {
    if expr.data_type() != expected {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            format!(
                "expected expression type {:?}, got {:?}",
                expected,
                expr.data_type()
            ),
        ));
    }
    Ok(())
}

fn reject_aggregate(expr: &BoundExpr) -> Result<()> {
    if contains_aggregate(expr) {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "aggregate calls are not allowed here",
        ));
    }
    Ok(())
}

fn contains_aggregate(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::AggregateCall { .. } => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => contains_aggregate(expr),
        BoundExpr::Function { args, .. } => args.iter().any(contains_aggregate),
        BoundExpr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        BoundExpr::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || when_clauses
                    .iter()
                    .any(|(when, then)| contains_aggregate(when) || contains_aggregate(then))
                || else_clause.as_deref().is_some_and(contains_aggregate)
        }
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. } => false,
    }
}

fn plan_error(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::plan(code, message)
}
