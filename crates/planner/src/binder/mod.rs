use std::collections::HashSet;

use catalog::{CatalogManager, resolve_system_view};
use common::{
    BindingId, ColumnDef, ColumnId, DataType, DbError, ParsedColumnDef, ParsedDefault,
    RelationKind, Result, SqlState, TableId, TableSchema, Value,
};
use parser::{Expr, FromItem, FunctionArg, Query, QueryBody, SelectItem, Statement};

use crate::{BoundExpr, BoundQuery, BoundStatement};

mod dml;
mod expr;
mod query;

use dml::{bind_copy, bind_delete, bind_insert, bind_update};
use query::bind_query;

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

/// A common table expression, bound once and inlined at each reference. `columns`
/// is the CTE's output columns (renamed by its column-alias list); a reference
/// registers a derived-table binding over them and inlines a clone of `query`.
#[derive(Clone, Debug)]
struct CteBinding {
    name: String,
    query: BoundQuery,
    columns: Vec<ColumnDef>,
}

/// The CTEs visible at a point in binding, innermost last. A reference resolves to
/// the last binding of a name, so an inner `WITH` shadows an outer one and a CTE
/// shadows a catalog table of the same name.
#[derive(Clone, Debug, Default)]
struct CteScope {
    ctes: Vec<CteBinding>,
}

impl CteScope {
    fn lookup(&self, name: &str) -> Option<&CteBinding> {
        self.ctes.iter().rev().find(|cte| cte.name == name)
    }
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
    /// The CTEs (`WITH`) in scope for `FROM` resolution. Empty unless the query or
    /// an enclosing query has a `WITH` clause.
    cte_scope: CteScope,
}

impl<'a> BindContext<'a> {
    fn new(catalog: &'a dyn CatalogManager, declared_params: &[Option<DataType>]) -> Self {
        Self {
            catalog,
            bindings: Vec::new(),
            next_binding: 0,
            next_slot: 0,
            declared_params: declared_params.to_vec(),
            cte_scope: CteScope::default(),
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
            if_not_exists,
            columns,
            primary_key,
            unique,
            compression,
            toast,
            checks,
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
            for column in columns {
                validate_default_value(catalog, column)?;
            }
            // Validate each CHECK against the (not-yet-created) columns: it must
            // parse, bind, be boolean, and be constraint-safe. The bound form is
            // discarded; the text is stored and re-bound per statement at
            // INSERT/UPDATE (matching how expression DEFAULTs are handled).
            let check_columns = parsed_columns_as_column_defs(columns);
            for check in checks {
                bind_check_expr(catalog, name, &check_columns, check)?;
            }
            Ok(BoundStatement::CreateTable {
                name: name.clone(),
                if_not_exists: *if_not_exists,
                columns: columns.clone(),
                primary_key: primary_key.clone(),
                unique: unique.clone(),
                compression: compression.unwrap_or_default(),
                toast: common::ToastOptions::default_new_table().apply_patch(toast),
                checks: checks.clone(),
            })
        }
        Statement::DropTable { name, if_exists } => {
            let table = if *if_exists {
                None
            } else {
                Some(require_table(catalog, name)?.id)
            };
            Ok(BoundStatement::DropTable {
                name: name.clone(),
                if_exists: *if_exists,
                table,
            })
        }
        Statement::AlterTableAddColumn {
            table,
            if_not_exists,
            column,
        } => {
            validate_default_value(catalog, column)?;
            if matches!(column.default, Some(ParsedDefault::Serial)) {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "ALTER TABLE ADD COLUMN does not support SERIAL columns yet",
                ));
            }
            let table_schema = require_table(catalog, table)?;
            Ok(BoundStatement::AlterTableAddColumn {
                table: table_schema.id,
                table_name: table.clone(),
                if_not_exists: *if_not_exists,
                column: column.clone(),
            })
        }
        Statement::AlterTableDropColumn {
            table,
            if_exists,
            column,
        } => {
            let table_schema = require_table(catalog, table)?;
            Ok(BoundStatement::AlterTableDropColumn {
                table: table_schema.id,
                table_name: table.clone(),
                if_exists: *if_exists,
                column: column.clone(),
            })
        }
        Statement::AlterTableRenameColumn {
            table,
            old_name,
            new_name,
        } => {
            let table_schema = require_table(catalog, table)?;
            Ok(BoundStatement::AlterTableRenameColumn {
                table: table_schema.id,
                table_name: table.clone(),
                old_name: old_name.clone(),
                new_name: new_name.clone(),
            })
        }
        Statement::AlterTableRenameTable { table, new_name } => {
            let table_schema = require_table(catalog, table)?;
            Ok(BoundStatement::AlterTableRenameTable {
                table: table_schema.id,
                table_name: table.clone(),
                new_name: new_name.clone(),
            })
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
        Statement::CreateView {
            name,
            or_replace,
            columns,
            query,
            definition,
        } => {
            if query_has_placeholder(query) {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "CREATE VIEW does not support query parameters",
                ));
            }
            let query = bind_query(catalog, query, declared, &CteScope::default(), None)?;
            validate_create_view_columns(columns, &query)?;
            Ok(BoundStatement::CreateView {
                name: name.clone(),
                or_replace: *or_replace,
                columns: columns.clone(),
                query,
                definition: definition.clone(),
            })
        }
        Statement::DropView { name, if_exists } => Ok(BoundStatement::DropView {
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
        Statement::Query(query) => bind_query(catalog, query, declared, &CteScope::default(), None)
            .map(BoundStatement::Query),
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
        | Statement::SetVariable { .. }
        | Statement::ResetVariable { .. }
        | Statement::ShowVariable { .. }
        | Statement::DiscardAll
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::RollbackToSavepoint { .. } => Err(plan_error(
            SqlState::FeatureNotSupported,
            "session control statements do not bind",
        )),
        // VACUUM/TRUNCATE are maintenance commands dispatched before binding (they
        // are not relational and never bind/plan). These defensive arms keep the
        // public `bind` API total if called directly.
        Statement::Vacuum { .. } | Statement::Truncate { .. } => Err(plan_error(
            SqlState::FeatureNotSupported,
            "maintenance commands do not bind",
        )),
        // ALTER TABLE maintenance commands are dispatched before binding; this
        // arm keeps `bind` total while schema-evolution ALTER TABLE binds above.
        Statement::AlterTableSetCompression { .. }
        | Statement::AlterTableSetOptions { .. }
        | Statement::AlterTableAddPrimaryKey { .. }
        | Statement::AlterTableDropPrimaryKey { .. } => Err(plan_error(
            SqlState::FeatureNotSupported,
            "ALTER TABLE is a maintenance command and does not bind",
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
    let table = catalog.get_table_by_name(name)?.ok_or_else(|| {
        if resolve_system_view(None, name).is_some() {
            plan_error(
                SqlState::FeatureNotSupported,
                "cannot modify system catalog",
            )
        } else {
            plan_error(
                SqlState::UndefinedTable,
                format!("table {name} does not exist"),
            )
        }
    })?;
    if matches!(table.relation_kind, RelationKind::Toast { .. }) {
        return Err(plan_error(
            SqlState::FeatureNotSupported,
            "hidden TOAST relations are not queryable",
        ));
    }
    Ok(table)
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
        ParsedDefault::Expr(text) => {
            // A non-constant expression default: bind it in an empty column scope
            // (so it cannot reference table columns) and require its result type be
            // assignable to the column. It is bound again per row at INSERT time; a
            // NULL result is caught then by the NOT NULL check, so it is not
            // rejected here (matching PostgreSQL).
            let bound = bind_default_expr(catalog, text)?;
            let expr_type = bound.data_type();
            if !default_expr_type_matches(&column.data_type, &expr_type) {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!(
                        "DEFAULT expression for column {} has type {:?}, expected {:?}",
                        column.name, expr_type, column.data_type
                    ),
                ));
            }
            return Ok(());
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

/// Whether a `DEFAULT` expression's result type may feed a column of `column_type`,
/// under the same no-implicit-cast rule as `INSERT` assignability: the types must
/// match, except any `NUMERIC` value is assignable to any `NUMERIC` column
/// (rounded/range-checked at store time).
fn default_expr_type_matches(column_type: &DataType, expr_type: &DataType) -> bool {
    if matches!(
        (expr_type, column_type),
        (DataType::Numeric { .. }, DataType::Numeric { .. })
    ) {
        return true;
    }
    expr_type == column_type
}

/// Parse and bind a column `DEFAULT` expression's canonical text in an empty
/// column scope, so it cannot reference table columns (a column reference fails as
/// an unresolved column). Forms not valid in a constraint context — aggregates,
/// subqueries, and query parameters — are rejected.
pub fn bind_default_expr(catalog: &dyn CatalogManager, text: &str) -> Result<BoundExpr> {
    let parsed = parser::parse_expression(text)?;
    let mut ctx = BindContext::new(catalog, &[]);
    let bound = expr::bind_expr(&mut ctx, &parsed, None)?;
    reject_non_constraint_safe(&bound)?;
    Ok(bound)
}

/// Reject expression forms not permitted in a `CHECK` constraint or a column
/// `DEFAULT`: aggregates, subqueries, and query parameters. Column references are
/// allowed here — a `DEFAULT` is bound in an empty scope so it cannot produce one,
/// and a `CHECK` legitimately references the row's columns.
fn reject_non_constraint_safe(expr: &BoundExpr) -> Result<()> {
    match expr {
        BoundExpr::AggregateCall { .. } => {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "aggregate functions are not allowed in DEFAULT or CHECK expressions",
            ));
        }
        BoundExpr::Parameter { .. } => {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "parameters are not allowed in DEFAULT or CHECK expressions",
            ));
        }
        BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. }
        | BoundExpr::InSubquery { .. } => {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "subqueries are not allowed in DEFAULT or CHECK expressions",
            ));
        }
        _ => {}
    }
    crate::params::for_each_child(expr, &mut |child| reject_non_constraint_safe(child))
}

fn reject_qualified_check_column_refs(expr: &Expr) -> Result<()> {
    match expr {
        Expr::ColumnRef { table: Some(_), .. } => {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "table-qualified column references are not allowed in CHECK constraints",
            ));
        }
        Expr::ColumnRef { table: None, .. }
        | Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Subquery(_)
        | Expr::Exists { .. } => {}
        Expr::InSubquery { expr, .. }
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => reject_qualified_check_column_refs(expr)?,
        Expr::BinaryOp { left, right, .. } => {
            reject_qualified_check_column_refs(left)?;
            reject_qualified_check_column_refs(right)?;
        }
        Expr::Function { args, .. } => {
            for arg in args {
                if let FunctionArg::Expr(arg) = arg {
                    reject_qualified_check_column_refs(arg)?;
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            reject_qualified_check_column_refs(expr)?;
            for item in list {
                reject_qualified_check_column_refs(item)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            reject_qualified_check_column_refs(expr)?;
            reject_qualified_check_column_refs(low)?;
            reject_qualified_check_column_refs(high)?;
        }
        Expr::Like { expr, pattern, .. } => {
            reject_qualified_check_column_refs(expr)?;
            reject_qualified_check_column_refs(pattern)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                reject_qualified_check_column_refs(operand)?;
            }
            for (when, then) in when_clauses {
                reject_qualified_check_column_refs(when)?;
                reject_qualified_check_column_refs(then)?;
            }
            if let Some(else_clause) = else_clause {
                reject_qualified_check_column_refs(else_clause)?;
            }
        }
    }
    Ok(())
}

/// Bind a `CHECK` constraint's canonical text against a table's columns registered
/// as a single binding at slot 0 — the same full-row layout the executor validates,
/// so each `InputRef`'s slot equals its column position. The result must be boolean;
/// aggregates, subqueries, and parameters are rejected (`reject_non_constraint_safe`),
/// and a column reference resolves normally (unlike a `DEFAULT`, a `CHECK` may name
/// the row's columns).
fn bind_check_expr(
    catalog: &dyn CatalogManager,
    table_name: &str,
    columns: &[ColumnDef],
    text: &str,
) -> Result<BoundExpr> {
    let parsed = parser::parse_expression(text)?;
    reject_qualified_check_column_refs(&parsed)?;
    let mut ctx = BindContext::new(catalog, &[]);
    ctx.bindings.push(Binding {
        id: 0,
        table_id: None,
        table_name: table_name.to_string(),
        visible_name: table_name.to_string(),
        columns: columns.to_vec(),
        slot_start: 0,
        qualified_only: false,
    });
    ctx.next_binding = 1;
    ctx.next_slot = columns.len();
    let bound = expr::bind_expr(&mut ctx, &parsed, None)?;
    reject_non_constraint_safe(&bound)?;
    if bound.data_type() != DataType::Boolean {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            format!(
                "CHECK constraint must be a boolean expression, got {:?}",
                bound.data_type()
            ),
        ));
    }
    Ok(bound)
}

/// Bind all of a table's stored `CHECK` expressions against its columns, for the
/// executor to enforce per row at `INSERT`/`UPDATE`. Empty when the table has none.
pub(super) fn bind_table_checks(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
) -> Result<Vec<BoundExpr>> {
    table
        .checks
        .iter()
        .map(|text| bind_check_expr(catalog, &table.name, &table.columns, text))
        .collect()
}

/// View a `CREATE TABLE`'s not-yet-created columns as `ColumnDef`s (id = declaration
/// order) so a `CHECK` can be bound and validated before the table exists. Only the
/// name/type/nullability matter for binding; the default is irrelevant here.
fn parsed_columns_as_column_defs(columns: &[ParsedColumnDef]) -> Vec<ColumnDef> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| ColumnDef {
            id: index as ColumnId,
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            max_length: column.max_length,
            default: None,
            pg_type: column.pg_type.clone(),
        })
        .collect()
}

fn plan_error(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::plan(code, message)
}

fn validate_create_view_columns(columns: &[String], query: &BoundQuery) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    let output_len = query.output_schema().len();
    if columns.len() != output_len {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!(
                "CREATE VIEW specifies {} column names but query returns {} columns",
                columns.len(),
                output_len
            ),
        ));
    }
    let mut seen = HashSet::new();
    for column in columns {
        if !seen.insert(column) {
            return Err(plan_error(
                SqlState::SyntaxError,
                format!("duplicate view column {column}"),
            ));
        }
    }
    Ok(())
}

fn query_has_placeholder(query: &Query) -> bool {
    query
        .with
        .iter()
        .any(|cte| query_has_placeholder(&cte.query))
        || query_body_has_placeholder(&query.body)
        || query
            .order_by
            .iter()
            .any(|order_by| expr_has_placeholder(&order_by.expr))
}

fn query_body_has_placeholder(body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(select) => {
            select.columns.iter().any(select_item_has_placeholder)
                || select.from.iter().any(from_item_has_placeholder)
                || select.filter.as_ref().is_some_and(expr_has_placeholder)
                || select.group_by.iter().any(expr_has_placeholder)
                || select.having.as_ref().is_some_and(expr_has_placeholder)
                || select
                    .distinct
                    .as_ref()
                    .is_some_and(|distinct| match distinct {
                        parser::Distinct::All => false,
                        parser::Distinct::On(exprs) => exprs.iter().any(expr_has_placeholder),
                    })
        }
        QueryBody::Values(rows) => rows.iter().flatten().any(expr_has_placeholder),
        QueryBody::SetOp { left, right, .. } => {
            query_has_placeholder(left) || query_has_placeholder(right)
        }
    }
}

fn select_item_has_placeholder(item: &SelectItem) -> bool {
    match item {
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        SelectItem::Expression { expr, .. } => expr_has_placeholder(expr),
    }
}

fn from_item_has_placeholder(item: &FromItem) -> bool {
    match item {
        FromItem::Table { .. } => false,
        FromItem::Derived { subquery, .. } => query_has_placeholder(subquery),
        FromItem::Join {
            left,
            right,
            condition,
            ..
        } => {
            from_item_has_placeholder(left)
                || from_item_has_placeholder(right)
                || condition.as_ref().is_some_and(expr_has_placeholder)
        }
    }
}

fn expr_has_placeholder(expr: &Expr) -> bool {
    match expr {
        Expr::Placeholder(_) => true,
        Expr::Literal(_) | Expr::ColumnRef { .. } => false,
        Expr::Subquery(query) => query_has_placeholder(query),
        Expr::InSubquery { expr, subquery, .. } => {
            expr_has_placeholder(expr) || query_has_placeholder(subquery)
        }
        Expr::Exists { subquery, .. } => query_has_placeholder(subquery),
        Expr::BinaryOp { left, right, .. } => {
            expr_has_placeholder(left) || expr_has_placeholder(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => expr_has_placeholder(expr),
        Expr::Function { args, .. } => args.iter().any(function_arg_has_placeholder),
        Expr::InList { expr, list, .. } => {
            expr_has_placeholder(expr) || list.iter().any(expr_has_placeholder)
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_has_placeholder(expr) || expr_has_placeholder(low) || expr_has_placeholder(high),
        Expr::Like { expr, pattern, .. } => {
            expr_has_placeholder(expr) || expr_has_placeholder(pattern)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_has_placeholder(expr))
                || when_clauses
                    .iter()
                    .any(|(when, then)| expr_has_placeholder(when) || expr_has_placeholder(then))
                || else_clause
                    .as_ref()
                    .is_some_and(|expr| expr_has_placeholder(expr))
        }
    }
}

fn function_arg_has_placeholder(arg: &FunctionArg) -> bool {
    match arg {
        FunctionArg::Expr(expr) => expr_has_placeholder(expr),
        FunctionArg::Wildcard => false,
    }
}
