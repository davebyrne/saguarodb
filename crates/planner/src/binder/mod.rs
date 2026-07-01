use std::collections::HashSet;

use catalog::CatalogManager;
use common::{
    BindingId, ColumnDef, DataType, DbError, ParsedColumnDef, ParsedDefault, Result, SqlState,
    TableId, TableSchema, Value,
};
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
    /// The catalog table id, or `None` for a derived table (a subquery in FROM),
    /// which has no underlying table.
    table_id: Option<TableId>,
    table_name: String,
    visible_name: String,
    columns: Vec<ColumnDef>,
    slot_start: usize,
    /// When true, this binding participates in column resolution ONLY for a
    /// reference qualified with its `visible_name` (an unqualified column never
    /// resolves to it). Used for the `excluded` pseudo-table in `INSERT ... ON
    /// CONFLICT DO UPDATE`, so a bare column there resolves to the target row
    /// (matching PostgreSQL) instead of being ambiguous with `excluded`.
    qualified_only: bool,
}

struct BindContext<'a> {
    /// The catalog, carried so expression binding can resolve a subquery's tables
    /// (a subquery is bound in its own fresh scope — uncorrelated semantics).
    catalog: &'a dyn CatalogManager,
    bindings: Vec<Binding>,
    next_binding: BindingId,
    next_slot: usize,
    /// Parameter type OIDs declared by an extended-protocol `Parse` (mapped to
    /// `DataType`), 0-based and `None` when unspecified. Empty for simple queries.
    declared_params: Vec<Option<DataType>>,
}

impl<'a> BindContext<'a> {
    fn new(catalog: &'a dyn CatalogManager, declared_params: &[Option<DataType>]) -> Self {
        Self {
            catalog,
            bindings: Vec::new(),
            next_binding: 0,
            next_slot: 0,
            declared_params: declared_params.to_vec(),
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
            unique,
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
            if primary_key.is_empty() {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    "a table requires a primary key",
                ));
            }
            for column in columns {
                validate_default_value(catalog, column)?;
            }
            Ok(BoundStatement::CreateTable {
                name: name.clone(),
                columns: columns.clone(),
                primary_key: primary_key.clone(),
                unique: unique.clone(),
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
        Statement::CreateSequence { name, options } => Ok(BoundStatement::CreateSequence {
            name: name.clone(),
            options: options.clone(),
        }),
        Statement::DropSequence { name, if_exists } => Ok(BoundStatement::DropSequence {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
        } => bind_insert(
            catalog,
            table,
            columns,
            source,
            on_conflict.as_ref(),
            returning.as_deref(),
            declared,
        ),
        Statement::Select(select) => {
            bind_select(catalog, select, declared).map(BoundStatement::Select)
        }
        Statement::Update {
            table,
            assignments,
            filter,
            returning,
        } => bind_update(
            catalog,
            table,
            assignments,
            filter.as_ref(),
            returning.as_deref(),
            declared,
        ),
        Statement::Delete {
            table,
            filter,
            returning,
        } => bind_delete(
            catalog,
            table,
            filter.as_ref(),
            returning.as_deref(),
            declared,
        ),
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
        | Statement::SetSessionCharacteristics { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::RollbackToSavepoint { .. } => Err(plan_error(
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
        BoundExpr::Setval {
            value, is_called, ..
        } => contains_aggregate(value) || is_called.as_deref().is_some_and(contains_aggregate),
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
        // A subquery is its own (uncorrelated) scope: its inner select cannot
        // contain an aggregate of the OUTER query. `InSubquery`'s left operand,
        // however, is an outer-scope expression and may.
        BoundExpr::InSubquery { expr, .. } => contains_aggregate(expr),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => false,
    }
}

/// Validate a column's `DEFAULT` constant against its declared type. The default
/// is a constant folded by the parser; it must have the same type as the column
/// (no implicit casts), except `NULL` is accepted only when the column is
/// nullable (a `NULL` default on a `NOT NULL` column is rejected up front).
fn validate_default_value(catalog: &dyn CatalogManager, column: &ParsedColumnDef) -> Result<()> {
    let Some(default) = &column.default else {
        return Ok(());
    };
    let value = match default {
        ParsedDefault::Const(value) => value,
        ParsedDefault::Serial => {
            if column.data_type != DataType::Integer {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!(
                        "SERIAL column {} requires INTEGER, got {:?}",
                        column.name, column.data_type
                    ),
                ));
            }
            return Ok(());
        }
        ParsedDefault::Nextval(name) => {
            if column.data_type != DataType::Integer {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!(
                        "DEFAULT nextval for column {} requires INTEGER, got {:?}",
                        column.name, column.data_type
                    ),
                ));
            }
            // Confirm the sequence exists. Its SERIAL-ownership rule (a plain
            // `DEFAULT nextval` may not borrow a SERIAL-owned sequence) is validated
            // authoritatively by the catalog at CREATE TABLE
            // (`resolve_sequence_default`), so it is not duplicated here.
            if catalog.get_sequence_by_name(name)?.is_none() {
                return Err(plan_error(
                    SqlState::UndefinedTable,
                    format!("sequence {name} does not exist"),
                ));
            }
            return Ok(());
        }
        ParsedDefault::OwnedNextval(_) => {
            // `OwnedNextval` is produced by CREATE TABLE execution
            // (Serial -> OwnedNextval), never by the parser, so it never reaches
            // bind-time default validation.
            return Err(DbError::internal(
                "OwnedNextval default reached bind-time validation",
            ));
        }
    };
    if matches!(value, Value::Null) {
        if column.nullable {
            return Ok(());
        }
        return Err(plan_error(
            SqlState::NotNullViolation,
            format!("column {} is NOT NULL but its DEFAULT is NULL", column.name),
        ));
    }
    if default_value_matches(&column.data_type, value) {
        return Ok(());
    }
    Err(plan_error(
        SqlState::DatatypeMismatch,
        format!(
            "DEFAULT value for column {} does not match its type {:?}",
            column.name, column.data_type
        ),
    ))
}

/// Whether a non-NULL `DEFAULT` constant's value matches the column type. Numeric
/// values are compatible with any `NUMERIC(p, s)` column (rounded/range-checked at
/// store time), mirroring `INSERT` assignability.
fn default_value_matches(data_type: &DataType, value: &Value) -> bool {
    matches!(
        (data_type, value),
        (DataType::Integer, Value::Integer(_))
            | (DataType::Double, Value::Float(_))
            | (DataType::Numeric { .. }, Value::Numeric(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Boolean, Value::Boolean(_))
            | (DataType::Date, Value::Date(_))
            | (DataType::Timestamp, Value::Timestamp(_))
            | (DataType::Bytea, Value::Bytes(_))
            | (DataType::Uuid, Value::Uuid(_))
    )
}

fn plan_error(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::plan(code, message)
}
