use std::collections::{BTreeMap, BTreeSet, HashSet};

use catalog::{CatalogManager, resolve_system_view};
use common::{
    BindingId, ColumnDef, ColumnId, DataType, DbError, ParsedColumnDef, ParsedDefault, PgType,
    RelationKind, Result, SqlState, TableId, TableSchema, Value, ViewDependency,
};
use parser::{Expr, FromItem, FunctionArg, Query, QueryBody, SelectItem, Statement};

use crate::{
    BoundDistinct, BoundExpr, BoundFrom, BoundOrderByItem, BoundQuery, BoundQueryBody, BoundSelect,
    BoundStatement, BoundValues, CorrelatedColumn,
};

mod dml;
mod expr;
mod query;

use dml::{bind_copy, bind_delete, bind_insert, bind_update};
use query::{bind_query, derive_alias_columns};

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

/// One enclosing scope visible from a subquery body, innermost first in a
/// chain. `reject` names the construct when a reference resolving to this
/// scope is rejected instead of recorded (`docs/specs/subqueries.md` §1.1):
/// the chain is still walked so the error names the construct rather than
/// claiming the column does not exist. The flag survives flattening into the
/// chains of deeper subqueries.
#[derive(Clone, Copy)]
struct OuterLink<'a> {
    ctx: &'a BindContext<'a>,
    reject: Option<&'static str>,
}

/// A correlated reference recorded while binding a subquery body, tagged with
/// the scope distance at which the name resolved (1 = the immediately
/// enclosing scope). When the subquery boundary unwinds, entries with
/// `depth > 1` are re-interned into the parent's accumulator and their
/// `outer` becomes an `OuterRef` into the parent's list
/// (`docs/specs/subqueries.md` §4.2).
struct PendingCorrelation {
    depth: usize,
    column: CorrelatedColumn,
}

struct BindContext<'a> {
    /// The catalog, carried so expression binding can resolve a subquery's tables.
    catalog: &'a dyn CatalogManager,
    bindings: Vec<Binding>,
    next_binding: BindingId,
    next_slot: usize,
    /// Parameter type OIDs declared by an extended-protocol `Parse`, 0-based and
    /// `None` when unspecified. Empty for simple queries. Binding uses the
    /// collapsed `DataType`, but output metadata for a selected parameter can
    /// preserve the declared `PgType` (for example PostgreSQL `oid`).
    declared_params: Vec<Option<PgType>>,
    /// The CTEs (`WITH`) in scope for `FROM` resolution. Empty unless the query or
    /// an enclosing query has a `WITH` clause.
    cte_scope: CteScope,
    /// The enclosing scopes a subquery body may reference, innermost first.
    /// Empty at the top level and for deliberately isolated scopes (CTE and
    /// view bodies). `docs/specs/subqueries.md` §4.1.
    outer: Vec<OuterLink<'a>>,
    /// The correlated references recorded against this scope's subquery
    /// boundary, in `OuterRef` slot order. Drained into
    /// `BoundQuery::correlations` when the boundary unwinds. Always empty for
    /// a scope with no `outer` chain.
    correlations: Vec<PendingCorrelation>,
    /// While binding a join's `ON` condition: the index of the join's first
    /// binding. Only bindings from that index on are visible — a reference to
    /// an earlier sibling FROM entry is rejected like PostgreSQL's "invalid
    /// reference to FROM-clause entry" (the join operator only sees its own
    /// subtree's row). `None` outside `ON` binding.
    on_scope_start: Option<usize>,
}

impl<'a> BindContext<'a> {
    fn new(catalog: &'a dyn CatalogManager, declared_params: &[Option<PgType>]) -> Self {
        Self::with_outer(catalog, declared_params, Vec::new())
    }

    fn with_outer(
        catalog: &'a dyn CatalogManager,
        declared_params: &[Option<PgType>],
        outer: Vec<OuterLink<'a>>,
    ) -> Self {
        Self {
            catalog,
            bindings: Vec::new(),
            next_binding: 0,
            next_slot: 0,
            declared_params: declared_params.to_vec(),
            cte_scope: CteScope::default(),
            outer,
            correlations: Vec::new(),
            on_scope_start: None,
        }
    }

    /// Record a correlated reference resolved at `depth`, re-using the slot of
    /// an identical existing entry. Returns the `OuterRef` slot.
    fn intern_correlation(&mut self, depth: usize, column: CorrelatedColumn) -> usize {
        if let Some(slot) = self
            .correlations
            .iter()
            .position(|pending| pending.depth == depth && pending.column == column)
        {
            return slot;
        }
        self.correlations.push(PendingCorrelation { depth, column });
        self.correlations.len() - 1
    }

    fn declared_param(&self, index: usize) -> Option<DataType> {
        self.declared_param_pg_type(index)
            .map(|pg_type| pg_type.data_type())
    }

    fn declared_param_pg_type(&self, index: usize) -> Option<PgType> {
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
    let declared_pg_types: Vec<_> = declared_param_types
        .iter()
        .map(|data_type| data_type.as_ref().map(PgType::from))
        .collect();
    bind_parameterized_with_pg_types(statement, catalog, &declared_pg_types)
}

pub fn bind_parameterized_with_pg_types(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    declared_param_types: &[Option<PgType>],
) -> Result<(BoundStatement, Vec<DataType>)> {
    let bound = bind_inner(statement, catalog, declared_param_types)?;
    let declared_data_types: Vec<_> = declared_param_types
        .iter()
        .map(|pg_type| pg_type.as_ref().map(PgType::data_type))
        .collect();
    let params = crate::params::collect_param_types(&bound, &declared_data_types)?;
    Ok((bound, params))
}

fn bind_inner(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    declared: &[Option<PgType>],
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
        Statement::DropTable { names, if_exists } => {
            let mut targets = Vec::with_capacity(names.len());
            for name in names {
                let table = if *if_exists {
                    None
                } else if catalog.get_view_by_name(name)?.is_some() {
                    return Err(plan_error(
                        SqlState::WrongObjectType,
                        format!("relation {name} is a view, not a table"),
                    ));
                } else {
                    Some(require_table(catalog, name)?.id)
                };
                targets.push(crate::DropTableTarget {
                    name: name.clone(),
                    table,
                });
            }
            Ok(BoundStatement::DropTable {
                targets,
                if_exists: *if_exists,
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
            if query_has_sequence_function(query) {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "CREATE VIEW does not support sequence functions",
                ));
            }
            let bound_query = bind_query(
                catalog,
                query,
                declared,
                &CteScope::default(),
                None,
                &[],
                &mut Vec::new(),
            )?;
            validate_create_view_columns(columns, &bound_query)?;
            let dependencies = collect_view_dependencies(catalog, declared, query, &bound_query)?;
            Ok(BoundStatement::CreateView {
                name: name.clone(),
                or_replace: *or_replace,
                columns: columns.clone(),
                query: bound_query,
                definition: definition.clone(),
                dependencies,
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
        Statement::Query(query) => bind_query(
            catalog,
            query,
            declared,
            &CteScope::default(),
            None,
            &[],
            &mut Vec::new(),
        )
        .map(BoundStatement::Query),
        Statement::Update {
            table,
            assignments,
            from,
            filter,
            returning,
        } => bind_update(
            catalog,
            table,
            assignments,
            from,
            filter.as_ref(),
            returning.as_deref(),
            declared,
        ),
        Statement::Delete {
            table,
            using,
            filter,
            returning,
        } => bind_delete(
            catalog,
            table,
            using,
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
        | Statement::RollbackToSavepoint { .. }
        | Statement::DeclareCursor { .. }
        | Statement::FetchCursor { .. }
        | Statement::CloseCursor { .. } => Err(plan_error(
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
    let table = match catalog.get_table_by_name(name)? {
        Some(table) => table,
        None if catalog.get_view_by_name(name)?.is_some() => {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "cannot modify view",
            ));
        }
        None if resolve_system_view(None, name).is_some() => {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "cannot modify system catalog",
            ));
        }
        None => {
            return Err(plan_error(
                SqlState::UndefinedTable,
                format!("table {name} does not exist"),
            ));
        }
    };
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
        BoundExpr::Array { elements, .. } => elements.iter().any(contains_aggregate),
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => contains_aggregate(array) || subscripts.iter().any(contains_aggregate),
        BoundExpr::Any { left, array, .. } => contains_aggregate(left) || contains_aggregate(array),
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
        | BoundExpr::Exists { .. }
        | BoundExpr::OuterRef { .. } => false,
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
        Expr::Array(elements) => {
            for element in elements {
                reject_qualified_check_column_refs(element)?;
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            reject_qualified_check_column_refs(array)?;
            for subscript in subscripts {
                reject_qualified_check_column_refs(subscript)?;
            }
        }
        Expr::Any { left, array, .. } => {
            reject_qualified_check_column_refs(left)?;
            reject_qualified_check_column_refs(array)?;
        }
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

#[derive(Clone, Copy, Debug)]
struct DependencyBinding {
    binding: BindingId,
    relation: TableId,
}

#[derive(Default)]
struct ViewDependencyBuilder {
    dependencies: BTreeMap<TableId, ViewDependencyColumns>,
}

enum ViewDependencyColumns {
    Relation,
    Columns(BTreeSet<ColumnId>),
    AllColumns,
}

impl ViewDependencyBuilder {
    fn add_relation(&mut self, relation: TableId) {
        self.dependencies
            .entry(relation)
            .or_insert(ViewDependencyColumns::Relation);
    }

    fn add_column(&mut self, relation: TableId, column: ColumnId) {
        match self.dependencies.entry(relation) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(ViewDependencyColumns::Columns(BTreeSet::from([column])));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => match entry.get_mut() {
                ViewDependencyColumns::Relation => {
                    entry.insert(ViewDependencyColumns::Columns(BTreeSet::from([column])));
                }
                ViewDependencyColumns::Columns(columns) => {
                    columns.insert(column);
                }
                ViewDependencyColumns::AllColumns => {}
            },
        }
    }

    fn add_all_columns(&mut self, relation: TableId) {
        self.dependencies
            .insert(relation, ViewDependencyColumns::AllColumns);
    }

    fn finish(self) -> Vec<ViewDependency> {
        self.dependencies
            .into_iter()
            .map(|(relation, columns)| match columns {
                ViewDependencyColumns::Relation => ViewDependency {
                    relation,
                    columns: Vec::new(),
                    all_columns: false,
                },
                ViewDependencyColumns::Columns(columns) => ViewDependency {
                    relation,
                    columns: columns.into_iter().collect(),
                    all_columns: false,
                },
                ViewDependencyColumns::AllColumns => ViewDependency {
                    relation,
                    columns: Vec::new(),
                    all_columns: true,
                },
            })
            .collect()
    }
}

fn collect_view_dependencies(
    catalog: &dyn CatalogManager,
    declared: &[Option<PgType>],
    ast: &Query,
    bound: &BoundQuery,
) -> Result<Vec<ViewDependency>> {
    let mut builder = ViewDependencyBuilder::default();
    collect_query_dependencies(ast, bound, &mut builder);
    collect_bound_but_unretained_cte_dependencies(
        catalog,
        declared,
        &CteScope::default(),
        ast,
        &mut builder,
    )?;
    Ok(builder.finish())
}

fn collect_query_dependencies(
    ast: &Query,
    bound: &BoundQuery,
    builder: &mut ViewDependencyBuilder,
) {
    match (&ast.body, &bound.body) {
        (QueryBody::Select(ast_select), BoundQueryBody::Select(bound_select)) => {
            collect_select_dependencies(ast_select, bound_select, &bound.order_by, builder);
        }
        (QueryBody::Values(_), BoundQueryBody::Values(values)) => {
            collect_values_dependencies(values, builder);
        }
        (
            QueryBody::SetOp {
                left: ast_left,
                right: ast_right,
                ..
            },
            BoundQueryBody::SetOp(bound_set),
        ) => {
            collect_query_dependencies(ast_left, &bound_set.left, builder);
            collect_query_dependencies(ast_right, &bound_set.right, builder);
        }
        _ => {}
    }
}

fn collect_select_dependencies(
    ast: &parser::Select,
    select: &BoundSelect,
    order_by: &[BoundOrderByItem],
    builder: &mut ViewDependencyBuilder,
) {
    let relation_bindings = select
        .from
        .as_ref()
        .map(|from| {
            collect_from_dependencies(from, builder);
            let bindings = visible_relation_bindings(from);
            collect_table_function_dependencies(from, &bindings, builder);
            bindings
        })
        .unwrap_or_default();
    for item in &ast.columns {
        match item {
            SelectItem::Wildcard => {
                for binding in &relation_bindings {
                    builder.add_all_columns(binding.relation);
                }
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                for binding in relation_bindings_with_name(select.from.as_ref(), qualifier.as_str())
                {
                    builder.add_all_columns(binding.relation);
                }
            }
            SelectItem::Expression { .. } => {}
        }
    }
    for item in &select.columns {
        collect_expr_dependencies(&item.expr, &relation_bindings, builder);
    }
    if let Some(filter) = &select.filter {
        collect_expr_dependencies(filter, &relation_bindings, builder);
    }
    for expr in &select.group_by {
        collect_expr_dependencies(expr, &relation_bindings, builder);
    }
    if let Some(having) = &select.having {
        collect_expr_dependencies(having, &relation_bindings, builder);
    }
    if let Some(BoundDistinct::On(exprs)) = &select.distinct {
        for expr in exprs {
            collect_expr_dependencies(expr, &relation_bindings, builder);
        }
    }
    for item in &select.columns {
        if let Some(relation) = item.wildcard_source {
            builder.add_all_columns(relation);
        }
    }
    for order_by in order_by {
        collect_expr_dependencies(&order_by.expr, &relation_bindings, builder);
    }
}

fn collect_values_dependencies(values: &BoundValues, builder: &mut ViewDependencyBuilder) {
    for expr in values.rows.iter().flatten() {
        collect_expr_dependencies(expr, &[], builder);
    }
}

fn collect_bound_but_unretained_cte_dependencies(
    catalog: &dyn CatalogManager,
    declared: &[Option<PgType>],
    enclosing: &CteScope,
    query: &Query,
    builder: &mut ViewDependencyBuilder,
) -> Result<()> {
    let mut scope = enclosing.clone();
    for cte in &query.with {
        let bound = bind_query(
            catalog,
            &cte.query,
            declared,
            &scope,
            None,
            &[],
            &mut Vec::new(),
        )?;
        collect_query_dependencies(&cte.query, &bound, builder);
        collect_bound_but_unretained_cte_dependencies(
            catalog, declared, &scope, &cte.query, builder,
        )?;
        let columns = derive_alias_columns(&bound.output_columns(), &cte.column_aliases, || {
            format!("CTE \"{}\"", cte.name)
        })?;
        scope.ctes.push(CteBinding {
            name: cte.name.clone(),
            query: bound,
            columns,
        });
    }
    collect_nested_query_cte_dependencies(catalog, declared, &scope, &query.body, builder)?;
    for order_by in &query.order_by {
        collect_expr_cte_dependencies(catalog, declared, &scope, &order_by.expr, builder)?;
    }
    Ok(())
}

fn collect_nested_query_cte_dependencies(
    catalog: &dyn CatalogManager,
    declared: &[Option<PgType>],
    scope: &CteScope,
    body: &QueryBody,
    builder: &mut ViewDependencyBuilder,
) -> Result<()> {
    match body {
        QueryBody::Select(select) => {
            for item in &select.columns {
                if let SelectItem::Expression { expr, .. } = item {
                    collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
                }
            }
            for from in &select.from {
                collect_from_item_cte_dependencies(catalog, declared, scope, from, builder)?;
            }
            if let Some(filter) = &select.filter {
                collect_expr_cte_dependencies(catalog, declared, scope, filter, builder)?;
            }
            for expr in &select.group_by {
                collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
            }
            if let Some(having) = &select.having {
                collect_expr_cte_dependencies(catalog, declared, scope, having, builder)?;
            }
            if let Some(parser::Distinct::On(exprs)) = &select.distinct {
                for expr in exprs {
                    collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
                }
            }
        }
        QueryBody::Values(rows) => {
            for expr in rows.iter().flatten() {
                collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
            }
        }
        QueryBody::SetOp { left, right, .. } => {
            collect_bound_but_unretained_cte_dependencies(catalog, declared, scope, left, builder)?;
            collect_bound_but_unretained_cte_dependencies(
                catalog, declared, scope, right, builder,
            )?;
        }
    }
    Ok(())
}

fn collect_from_item_cte_dependencies(
    catalog: &dyn CatalogManager,
    declared: &[Option<PgType>],
    scope: &CteScope,
    from: &FromItem,
    builder: &mut ViewDependencyBuilder,
) -> Result<()> {
    match from {
        FromItem::Table { .. } => {}
        FromItem::TableFunction { args, .. } => {
            for arg in args {
                collect_expr_cte_dependencies(catalog, declared, scope, arg, builder)?;
            }
        }
        FromItem::Derived { subquery, .. } => {
            collect_bound_but_unretained_cte_dependencies(
                catalog, declared, scope, subquery, builder,
            )?;
        }
        FromItem::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_from_item_cte_dependencies(catalog, declared, scope, left, builder)?;
            collect_from_item_cte_dependencies(catalog, declared, scope, right, builder)?;
            if let Some(condition) = condition {
                collect_expr_cte_dependencies(catalog, declared, scope, condition, builder)?;
            }
        }
    }
    Ok(())
}

fn collect_expr_cte_dependencies(
    catalog: &dyn CatalogManager,
    declared: &[Option<PgType>],
    scope: &CteScope,
    expr: &Expr,
    builder: &mut ViewDependencyBuilder,
) -> Result<()> {
    match expr {
        Expr::Subquery(query) => {
            collect_bound_but_unretained_cte_dependencies(
                catalog, declared, scope, query, builder,
            )?;
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
            collect_bound_but_unretained_cte_dependencies(
                catalog, declared, scope, subquery, builder,
            )?;
        }
        Expr::Exists { subquery, .. } => {
            collect_bound_but_unretained_cte_dependencies(
                catalog, declared, scope, subquery, builder,
            )?;
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_cte_dependencies(catalog, declared, scope, left, builder)?;
            collect_expr_cte_dependencies(catalog, declared, scope, right, builder)?;
        }
        Expr::UnaryOp { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
        }
        Expr::Function { args, .. } => {
            for arg in args {
                if let FunctionArg::Expr(expr) = arg {
                    collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
            for item in list {
                collect_expr_cte_dependencies(catalog, declared, scope, item, builder)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
            collect_expr_cte_dependencies(catalog, declared, scope, low, builder)?;
            collect_expr_cte_dependencies(catalog, declared, scope, high, builder)?;
        }
        Expr::Like { expr, pattern, .. } => {
            collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
            collect_expr_cte_dependencies(catalog, declared, scope, pattern, builder)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_expr_cte_dependencies(catalog, declared, scope, operand, builder)?;
            }
            for (when, then) in when_clauses {
                collect_expr_cte_dependencies(catalog, declared, scope, when, builder)?;
                collect_expr_cte_dependencies(catalog, declared, scope, then, builder)?;
            }
            if let Some(else_clause) = else_clause {
                collect_expr_cte_dependencies(catalog, declared, scope, else_clause, builder)?;
            }
        }
        Expr::Cast { expr, .. } => {
            collect_expr_cte_dependencies(catalog, declared, scope, expr, builder)?;
        }
        Expr::Array(elements) => {
            for element in elements {
                collect_expr_cte_dependencies(catalog, declared, scope, element, builder)?;
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_cte_dependencies(catalog, declared, scope, array, builder)?;
            for subscript in subscripts {
                collect_expr_cte_dependencies(catalog, declared, scope, subscript, builder)?;
            }
        }
        Expr::Any { left, array, .. } => {
            collect_expr_cte_dependencies(catalog, declared, scope, left, builder)?;
            collect_expr_cte_dependencies(catalog, declared, scope, array, builder)?;
        }
        Expr::Literal(_) | Expr::Placeholder(_) | Expr::ColumnRef { .. } => {}
    }
    Ok(())
}

fn collect_from_dependencies(from: &BoundFrom, builder: &mut ViewDependencyBuilder) {
    match from {
        BoundFrom::Table { table, .. } => builder.add_relation(*table),
        BoundFrom::System { .. } => {}
        BoundFrom::TableFunction { .. } => {}
        BoundFrom::Derived { query, .. } => collect_bound_query_dependencies(query, builder),
        BoundFrom::View { view, query, .. } => {
            builder.add_relation(*view);
            collect_bound_query_dependencies(query, builder);
        }
        BoundFrom::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_from_dependencies(left, builder);
            collect_from_dependencies(right, builder);
            let relation_bindings = visible_relation_bindings(from);
            if let Some(condition) = condition {
                collect_expr_dependencies(condition, &relation_bindings, builder);
            }
        }
    }
}

fn collect_bound_query_dependencies(query: &BoundQuery, builder: &mut ViewDependencyBuilder) {
    match &query.body {
        BoundQueryBody::Select(select) => {
            collect_bound_select_dependencies(select, &query.order_by, builder);
        }
        BoundQueryBody::Values(values) => collect_values_dependencies(values, builder),
        BoundQueryBody::SetOp(set_op) => {
            collect_bound_query_dependencies(&set_op.left, builder);
            collect_bound_query_dependencies(&set_op.right, builder);
        }
    }
}

fn collect_bound_select_dependencies(
    select: &BoundSelect,
    order_by: &[BoundOrderByItem],
    builder: &mut ViewDependencyBuilder,
) {
    let relation_bindings = select
        .from
        .as_ref()
        .map(|from| {
            collect_from_dependencies(from, builder);
            let bindings = visible_relation_bindings(from);
            collect_table_function_dependencies(from, &bindings, builder);
            bindings
        })
        .unwrap_or_default();
    for item in &select.columns {
        collect_expr_dependencies(&item.expr, &relation_bindings, builder);
    }
    if let Some(filter) = &select.filter {
        collect_expr_dependencies(filter, &relation_bindings, builder);
    }
    for expr in &select.group_by {
        collect_expr_dependencies(expr, &relation_bindings, builder);
    }
    if let Some(having) = &select.having {
        collect_expr_dependencies(having, &relation_bindings, builder);
    }
    if let Some(BoundDistinct::On(exprs)) = &select.distinct {
        for expr in exprs {
            collect_expr_dependencies(expr, &relation_bindings, builder);
        }
    }
    for item in &select.columns {
        if let Some(relation) = item.wildcard_source {
            builder.add_all_columns(relation);
        }
    }
    for order_by in order_by {
        collect_expr_dependencies(&order_by.expr, &relation_bindings, builder);
    }
}

fn collect_table_function_dependencies(
    from: &BoundFrom,
    bindings: &[DependencyBinding],
    builder: &mut ViewDependencyBuilder,
) {
    let mut stack = vec![from];
    while let Some(item) = stack.pop() {
        match item {
            BoundFrom::TableFunction { args, .. } => {
                for arg in args {
                    collect_expr_dependencies(arg, bindings, builder);
                }
            }
            BoundFrom::Join { left, right, .. } => {
                stack.push(left);
                stack.push(right);
            }
            _ => {}
        }
    }
}

fn visible_relation_bindings(from: &BoundFrom) -> Vec<DependencyBinding> {
    match from {
        BoundFrom::Table { table, binding, .. } => vec![DependencyBinding {
            binding: *binding,
            relation: *table,
        }],
        BoundFrom::View { view, binding, .. } => vec![DependencyBinding {
            binding: *binding,
            relation: *view,
        }],
        BoundFrom::System { .. } | BoundFrom::Derived { .. } | BoundFrom::TableFunction { .. } => {
            Vec::new()
        }
        BoundFrom::Join { left, right, .. } => {
            let mut bindings = visible_relation_bindings(left);
            bindings.extend(visible_relation_bindings(right));
            bindings
        }
    }
}

fn relation_bindings_with_name(
    from: Option<&BoundFrom>,
    qualifier: &str,
) -> Vec<DependencyBinding> {
    let mut output = Vec::new();
    if let Some(from) = from {
        collect_relation_bindings_with_name(from, qualifier, &mut output);
    }
    output
}

fn collect_relation_bindings_with_name(
    from: &BoundFrom,
    qualifier: &str,
    output: &mut Vec<DependencyBinding>,
) {
    match from {
        BoundFrom::Table {
            table,
            binding,
            name,
            alias,
            ..
        } => {
            if alias.as_deref().unwrap_or(name) == qualifier {
                output.push(DependencyBinding {
                    binding: *binding,
                    relation: *table,
                });
            }
        }
        BoundFrom::View {
            view,
            binding,
            alias,
            ..
        } => {
            if alias == qualifier {
                output.push(DependencyBinding {
                    binding: *binding,
                    relation: *view,
                });
            }
        }
        BoundFrom::Join { left, right, .. } => {
            collect_relation_bindings_with_name(left, qualifier, output);
            collect_relation_bindings_with_name(right, qualifier, output);
        }
        BoundFrom::System { .. } | BoundFrom::Derived { .. } | BoundFrom::TableFunction { .. } => {}
    }
}

fn collect_expr_dependencies(
    expr: &BoundExpr,
    bindings: &[DependencyBinding],
    builder: &mut ViewDependencyBuilder,
) {
    match expr {
        BoundExpr::InputRef { input, column, .. } => {
            if let Some(binding) = bindings.iter().find(|binding| binding.binding == *input) {
                builder.add_column(binding.relation, *column);
            }
        }
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::AggregateCall { arg: None, .. } => {}
        BoundExpr::BinaryOp { left, right, .. } => {
            collect_expr_dependencies(left, bindings, builder);
            collect_expr_dependencies(right, bindings, builder);
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => collect_expr_dependencies(expr, bindings, builder),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                collect_expr_dependencies(arg, bindings, builder);
            }
        }
        BoundExpr::Array { elements, .. } => {
            for element in elements {
                collect_expr_dependencies(element, bindings, builder);
            }
        }
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            collect_expr_dependencies(array, bindings, builder);
            for subscript in subscripts {
                collect_expr_dependencies(subscript, bindings, builder);
            }
        }
        BoundExpr::Any { left, array, .. } => {
            collect_expr_dependencies(left, bindings, builder);
            collect_expr_dependencies(array, bindings, builder);
        }
        BoundExpr::Setval {
            value, is_called, ..
        } => {
            collect_expr_dependencies(value, bindings, builder);
            if let Some(is_called) = is_called {
                collect_expr_dependencies(is_called, bindings, builder);
            }
        }
        BoundExpr::AggregateCall { arg: Some(arg), .. } => {
            collect_expr_dependencies(arg, bindings, builder);
        }
        BoundExpr::InList { expr, list, .. } => {
            collect_expr_dependencies(expr, bindings, builder);
            for item in list {
                collect_expr_dependencies(item, bindings, builder);
            }
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_dependencies(expr, bindings, builder);
            collect_expr_dependencies(low, bindings, builder);
            collect_expr_dependencies(high, bindings, builder);
        }
        BoundExpr::Like { expr, pattern, .. } => {
            collect_expr_dependencies(expr, bindings, builder);
            collect_expr_dependencies(pattern, bindings, builder);
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_dependencies(operand, bindings, builder);
            }
            for (when, then) in when_clauses {
                collect_expr_dependencies(when, bindings, builder);
                collect_expr_dependencies(then, bindings, builder);
            }
            if let Some(else_clause) = else_clause {
                collect_expr_dependencies(else_clause, bindings, builder);
            }
        }
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            collect_bound_query_dependencies(query, builder);
        }
        BoundExpr::InSubquery { expr, query, .. } => {
            collect_expr_dependencies(expr, bindings, builder);
            collect_bound_query_dependencies(query, builder);
        }
    }
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

fn query_has_sequence_function(query: &Query) -> bool {
    query
        .with
        .iter()
        .any(|cte| query_has_sequence_function(&cte.query))
        || query_body_has_sequence_function(&query.body)
        || query
            .order_by
            .iter()
            .any(|order_by| expr_has_sequence_function(&order_by.expr))
}

fn query_body_has_sequence_function(body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(select) => {
            select.columns.iter().any(select_item_has_sequence_function)
                || select.from.iter().any(from_item_has_sequence_function)
                || select
                    .filter
                    .as_ref()
                    .is_some_and(expr_has_sequence_function)
                || select.group_by.iter().any(expr_has_sequence_function)
                || select
                    .having
                    .as_ref()
                    .is_some_and(expr_has_sequence_function)
                || select
                    .distinct
                    .as_ref()
                    .is_some_and(|distinct| match distinct {
                        parser::Distinct::All => false,
                        parser::Distinct::On(exprs) => exprs.iter().any(expr_has_sequence_function),
                    })
        }
        QueryBody::Values(rows) => rows.iter().flatten().any(expr_has_sequence_function),
        QueryBody::SetOp { left, right, .. } => {
            query_has_sequence_function(left) || query_has_sequence_function(right)
        }
    }
}

fn select_item_has_sequence_function(item: &SelectItem) -> bool {
    match item {
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        SelectItem::Expression { expr, .. } => expr_has_sequence_function(expr),
    }
}

fn from_item_has_sequence_function(item: &FromItem) -> bool {
    match item {
        FromItem::Table { .. } => false,
        FromItem::TableFunction { args, .. } => args.iter().any(expr_has_sequence_function),
        FromItem::Derived { subquery, .. } => query_has_sequence_function(subquery),
        FromItem::Join {
            left,
            right,
            condition,
            ..
        } => {
            from_item_has_sequence_function(left)
                || from_item_has_sequence_function(right)
                || condition.as_ref().is_some_and(expr_has_sequence_function)
        }
    }
}

fn expr_has_sequence_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            name.eq_ignore_ascii_case("nextval")
                || name.eq_ignore_ascii_case("currval")
                || name.eq_ignore_ascii_case("setval")
                || args.iter().any(function_arg_has_sequence_function)
        }
        Expr::Subquery(query) => query_has_sequence_function(query),
        Expr::InSubquery { expr, subquery, .. } => {
            expr_has_sequence_function(expr) || query_has_sequence_function(subquery)
        }
        Expr::Exists { subquery, .. } => query_has_sequence_function(subquery),
        Expr::BinaryOp { left, right, .. } => {
            expr_has_sequence_function(left) || expr_has_sequence_function(right)
        }
        Expr::UnaryOp { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_has_sequence_function(expr)
        }
        Expr::InList { expr, list, .. } => {
            expr_has_sequence_function(expr) || list.iter().any(expr_has_sequence_function)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_has_sequence_function(expr)
                || expr_has_sequence_function(low)
                || expr_has_sequence_function(high)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_has_sequence_function(expr) || expr_has_sequence_function(pattern)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_deref().is_some_and(expr_has_sequence_function)
                || when_clauses.iter().any(|(when, then)| {
                    expr_has_sequence_function(when) || expr_has_sequence_function(then)
                })
                || else_clause
                    .as_deref()
                    .is_some_and(expr_has_sequence_function)
        }
        Expr::Cast { expr, .. } => expr_has_sequence_function(expr),
        Expr::Array(elements) => elements.iter().any(expr_has_sequence_function),
        Expr::ArraySubscript { array, subscripts } => {
            expr_has_sequence_function(array) || subscripts.iter().any(expr_has_sequence_function)
        }
        Expr::Any { left, array, .. } => {
            expr_has_sequence_function(left) || expr_has_sequence_function(array)
        }
        Expr::Literal(_) | Expr::Placeholder(_) | Expr::ColumnRef { .. } => false,
    }
}

fn function_arg_has_sequence_function(arg: &FunctionArg) -> bool {
    match arg {
        FunctionArg::Expr(expr) => expr_has_sequence_function(expr),
        FunctionArg::Wildcard => false,
    }
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
        FromItem::TableFunction { args, .. } => args.iter().any(expr_has_placeholder),
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
        Expr::Array(elements) => elements.iter().any(expr_has_placeholder),
        Expr::ArraySubscript { array, subscripts } => {
            expr_has_placeholder(array) || subscripts.iter().any(expr_has_placeholder)
        }
        Expr::Any { left, array, .. } => expr_has_placeholder(left) || expr_has_placeholder(array),
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
