use std::collections::HashSet;

use catalog::{CatalogManager, is_system_schema, resolve_system_view};
use common::{
    BindingId, ColumnDef, ColumnId, DataType, DbError, PUBLIC_SCHEMA_ID, ParsedColumnDef,
    ParsedDefault, PgType, QualifiedName, RelationKind, Result, SchemaId, SqlState, TableId,
    TableSchema, Value,
};
use parser::{
    Expr, FromItem, FunctionArg, ParsedForeignKey, Query, QueryBody, SelectItem, Statement,
};

use crate::{
    BoundExpr, BoundForeignKey, BoundForeignKeyTarget, BoundQuery, BoundStatement, CorrelatedColumn,
};

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
    search_path: Vec<SchemaId>,
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
        Self::with_outer(catalog, declared_params, &[PUBLIC_SCHEMA_ID], Vec::new())
    }

    fn with_outer(
        catalog: &'a dyn CatalogManager,
        declared_params: &[Option<PgType>],
        search_path: &[SchemaId],
        outer: Vec<OuterLink<'a>>,
    ) -> Self {
        Self {
            catalog,
            search_path: search_path.to_vec(),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindOptions {
    pub search_path: Vec<SchemaId>,
}

impl Default for BindOptions {
    fn default() -> Self {
        Self {
            search_path: vec![PUBLIC_SCHEMA_ID],
        }
    }
}

/// Bind a statement from the simple query protocol. Query parameters are not
/// allowed here.
pub fn bind(statement: &Statement, catalog: &dyn CatalogManager) -> Result<BoundStatement> {
    bind_with_options(statement, catalog, &BindOptions::default())
}

pub fn bind_with_options(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    options: &BindOptions,
) -> Result<BoundStatement> {
    let bound = bind_inner(statement, catalog, &[], options)?;
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
    bind_parameterized_with_pg_types_and_options(
        statement,
        catalog,
        declared_param_types,
        &BindOptions::default(),
    )
}

pub fn bind_parameterized_with_pg_types_and_options(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    declared_param_types: &[Option<PgType>],
    options: &BindOptions,
) -> Result<(BoundStatement, Vec<DataType>)> {
    let bound = bind_inner(statement, catalog, declared_param_types, options)?;
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
    options: &BindOptions,
) -> Result<BoundStatement> {
    match statement {
        Statement::CreateSchema {
            name,
            if_not_exists,
        } => Ok(BoundStatement::CreateSchema {
            name: name.clone(),
            if_not_exists: *if_not_exists,
        }),
        Statement::DropSchema { name, if_exists } => Ok(BoundStatement::DropSchema {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::CreateTable {
            name,
            if_not_exists,
            columns,
            primary_key,
            unique,
            compression,
            toast,
            checks,
            foreign_keys,
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
            validate_create_foreign_key_names(&name.name, primary_key, unique, foreign_keys)?;
            for column in columns {
                validate_default_value(catalog, &options.search_path, column)?;
            }
            let mut stored_columns = columns.clone();
            for column in &mut stored_columns {
                if let Some(ParsedDefault::Expr(sql)) = &column.default {
                    let bound = bind_default_expr_with_options(
                        catalog,
                        sql,
                        &BindOptions {
                            search_path: options.search_path.clone(),
                        },
                    )?;
                    column.default = Some(ParsedDefault::Stored(crate::store_bound_expression(
                        &bound,
                        sql.clone(),
                        &[],
                    )?));
                }
            }
            let check_columns = parsed_columns_as_column_defs(columns)?;
            let mut stored_checks = Vec::new();
            stored_checks.try_reserve(checks.len()).map_err(|_| {
                plan_error(SqlState::ProgramLimitExceeded, "too many CHECK constraints")
            })?;
            for check in checks {
                let bound = bind_check_expr(catalog, &name.name, &check_columns, check)?;
                stored_checks.push(crate::store_bound_expression(
                    &bound,
                    check.clone(),
                    &check_columns,
                )?);
            }
            let schema = resolve_schema_id(catalog, name, &options.search_path)?;
            let foreign_keys = bind_create_table_foreign_keys(
                catalog,
                &options.search_path,
                schema,
                &name.name,
                columns,
                primary_key,
                unique,
                foreign_keys,
            )?;
            Ok(BoundStatement::CreateTable {
                schema,
                name: name.name.clone(),
                if_not_exists: *if_not_exists,
                columns: stored_columns,
                primary_key: primary_key.clone(),
                unique: unique.clone(),
                compression: compression.unwrap_or_default(),
                toast: common::ToastOptions::default_new_table().apply_patch(toast),
                checks: stored_checks,
                foreign_keys,
            })
        }
        Statement::DropTable { names, if_exists } => {
            let mut targets = Vec::with_capacity(names.len());
            for name in names {
                let search_path = name_search_path(catalog, name, &options.search_path)?;
                let mut table = None;
                for schema in &search_path {
                    if let Some(found) = catalog.get_table_in_schema(*schema, &name.name)? {
                        table = Some(found.id);
                        break;
                    }
                    if catalog.get_view_in_schema(*schema, &name.name)?.is_some() {
                        return Err(plan_error(
                            SqlState::WrongObjectType,
                            format!("relation {name} is a view, not a table"),
                        ));
                    }
                    if catalog.get_index_in_schema(*schema, &name.name)?.is_some()
                        || catalog
                            .get_sequence_in_schema(*schema, &name.name)?
                            .is_some()
                    {
                        return Err(plan_error(
                            SqlState::WrongObjectType,
                            format!("relation {name} is not a table"),
                        ));
                    }
                }
                if table.is_none() && !if_exists {
                    return Err(plan_error(
                        SqlState::UndefinedTable,
                        format!("table {name} does not exist"),
                    ));
                }
                targets.push(crate::DropTableTarget {
                    name: name.clone(),
                    search_path,
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
            validate_default_value(catalog, &options.search_path, column)?;
            if matches!(column.default, Some(ParsedDefault::Serial)) {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "ALTER TABLE ADD COLUMN does not support SERIAL columns yet",
                ));
            }
            let table_schema = require_table(catalog, table, &options.search_path)?;
            let mut column = column.clone();
            if let Some(ParsedDefault::Expr(sql)) = &column.default {
                let bound = bind_default_expr_with_options(catalog, sql, options)?;
                column.default = Some(ParsedDefault::Stored(crate::store_bound_expression(
                    &bound,
                    sql.clone(),
                    &[],
                )?));
            }
            Ok(BoundStatement::AlterTableAddColumn {
                table: table_schema.id,
                table_name: table.name.clone(),
                if_not_exists: *if_not_exists,
                column,
            })
        }
        Statement::AlterTableDropColumn {
            table,
            if_exists,
            column,
        } => {
            let table_schema = require_table(catalog, table, &options.search_path)?;
            Ok(BoundStatement::AlterTableDropColumn {
                table: table_schema.id,
                table_name: table.name.clone(),
                if_exists: *if_exists,
                column: column.clone(),
            })
        }
        Statement::AlterTableRenameColumn {
            table,
            old_name,
            new_name,
        } => {
            let table_schema = require_table(catalog, table, &options.search_path)?;
            Ok(BoundStatement::AlterTableRenameColumn {
                table: table_schema.id,
                table_name: table.name.clone(),
                old_name: old_name.clone(),
                new_name: new_name.clone(),
            })
        }
        Statement::AlterTableRenameTable { table, new_name } => {
            let table_schema = require_table(catalog, table, &options.search_path)?;
            Ok(BoundStatement::AlterTableRenameTable {
                table: table_schema.id,
                table_name: table.name.clone(),
                new_name: new_name.clone(),
            })
        }
        Statement::AlterTableAlterColumnType {
            table,
            column,
            data_type,
            pg_type,
        } => {
            let table_schema = require_table(catalog, table, &options.search_path)?;
            if !table_schema
                .columns
                .iter()
                .any(|candidate| candidate.name == *column)
            {
                return Err(plan_error(
                    SqlState::UndefinedColumn,
                    format!("column {column} does not exist"),
                ));
            }
            Ok(BoundStatement::AlterTableAlterColumnType {
                table: table_schema.id,
                table_name: table.name.clone(),
                column: column.clone(),
                data_type: data_type.clone(),
                pg_type: pg_type.clone(),
            })
        }
        Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => {
            let table_schema = require_table(catalog, table, &options.search_path)?;
            let index_schema = resolve_schema_id(catalog, name, &options.search_path)?;
            if index_schema != table_schema.schema_id {
                return Err(plan_error(
                    SqlState::InvalidSchemaName,
                    "index and table must be in the same schema",
                ));
            }
            Ok(BoundStatement::CreateIndex {
                schema: index_schema,
                name: name.name.clone(),
                table: table_schema.name,
                columns: columns.clone(),
                unique: *unique,
            })
        }
        Statement::DropIndex { name } => {
            let index = require_index(catalog, name, &options.search_path)?;
            Ok(BoundStatement::DropIndex { index: index.id })
        }
        Statement::CreateSequence {
            name,
            options: sequence_options,
        } => Ok(BoundStatement::CreateSequence {
            schema: resolve_schema_id(catalog, name, &options.search_path)?,
            name: name.name.clone(),
            options: sequence_options.clone(),
        }),
        Statement::DropSequence { name, if_exists } => {
            let search_path = name_search_path(catalog, name, &options.search_path)?;
            let mut sequence = None;
            for schema in &search_path {
                if let Some(found) = catalog.get_sequence_in_schema(*schema, &name.name)? {
                    sequence = Some(found.id);
                    break;
                }
                if catalog.get_table_in_schema(*schema, &name.name)?.is_some()
                    || catalog.get_view_in_schema(*schema, &name.name)?.is_some()
                    || catalog.get_index_in_schema(*schema, &name.name)?.is_some()
                {
                    return Err(plan_error(
                        SqlState::WrongObjectType,
                        format!("relation {name} is not a sequence"),
                    ));
                }
            }
            if sequence.is_none() && !if_exists {
                return Err(plan_error(
                    SqlState::UndefinedTable,
                    format!("sequence {name} does not exist"),
                ));
            }
            Ok(BoundStatement::DropSequence {
                name: name.name.clone(),
                search_path,
                sequence,
                if_exists: *if_exists,
            })
        }
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
                &options.search_path,
                &CteScope::default(),
                None,
                &[],
                &mut Vec::new(),
            )?;
            validate_create_view_columns(columns, &bound_query)?;
            let stored_query = crate::store_bound_query(&bound_query)?;
            Ok(BoundStatement::CreateView {
                schema: resolve_schema_id(catalog, name, &options.search_path)?,
                name: name.name.clone(),
                or_replace: *or_replace,
                columns: columns.clone(),
                query: bound_query,
                definition: definition.clone(),
                stored_query,
                definition_search_path: options.search_path.clone(),
            })
        }
        Statement::DropView { name, if_exists } => {
            let search_path = name_search_path(catalog, name, &options.search_path)?;
            let mut view = None;
            for schema in &search_path {
                if let Some(found) = catalog.get_view_in_schema(*schema, &name.name)? {
                    view = Some(found.id);
                    break;
                }
                if catalog.get_table_in_schema(*schema, &name.name)?.is_some() {
                    return Err(plan_error(
                        SqlState::WrongObjectType,
                        format!("relation {name} is a table, not a view"),
                    ));
                }
                if catalog.get_index_in_schema(*schema, &name.name)?.is_some()
                    || catalog
                        .get_sequence_in_schema(*schema, &name.name)?
                        .is_some()
                {
                    return Err(plan_error(
                        SqlState::WrongObjectType,
                        format!("relation {name} is not a view"),
                    ));
                }
            }
            if view.is_none() && !if_exists {
                return Err(plan_error(
                    SqlState::UndefinedTable,
                    format!("view {name} does not exist"),
                ));
            }
            Ok(BoundStatement::DropView {
                name: name.name.clone(),
                search_path,
                view,
                if_exists: *if_exists,
            })
        }
        Statement::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
        } => bind_insert(
            catalog,
            table,
            &options.search_path,
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
            &options.search_path,
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
            &options.search_path,
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
            &options.search_path,
            using,
            filter.as_ref(),
            returning.as_deref(),
            declared,
        ),
        Statement::Explain { analyze, statement } => Ok(BoundStatement::Explain {
            analyze: *analyze,
            statement: Box::new(bind_inner(statement, catalog, declared, options)?),
        }),
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
        // VACUUM/ANALYZE/TRUNCATE are maintenance commands dispatched before
        // binding (they are not relational and never bind/plan). These defensive
        // arms keep the public `bind` API total if called directly.
        Statement::Vacuum { .. } | Statement::Analyze { .. } | Statement::Truncate { .. } => {
            Err(plan_error(
                SqlState::FeatureNotSupported,
                "maintenance commands do not bind",
            ))
        }
        // ALTER TABLE maintenance commands are dispatched before binding; this
        // arm keeps `bind` total while schema-evolution ALTER TABLE binds above.
        Statement::AlterTableSetCompression { .. }
        | Statement::AlterTableSetOptions { .. }
        | Statement::AlterTableAddPrimaryKey { .. }
        | Statement::AlterTableAddForeignKey { .. }
        | Statement::AlterTableDropPrimaryKey { .. }
        | Statement::AlterTableDropConstraint { .. } => Err(plan_error(
            SqlState::FeatureNotSupported,
            "ALTER TABLE is a maintenance command and does not bind",
        )),
        Statement::Copy {
            table,
            columns,
            direction,
            options: copy_options,
        } => bind_copy(
            catalog,
            table,
            &options.search_path,
            columns,
            *direction,
            copy_options,
        ),
    }
}

fn resolve_schema_id(
    catalog: &dyn CatalogManager,
    name: &QualifiedName,
    search_path: &[SchemaId],
) -> Result<SchemaId> {
    match &name.schema {
        Some(schema) => catalog
            .get_schema_by_name(schema)?
            .map(|schema| schema.id)
            .ok_or_else(|| {
                plan_error(
                    SqlState::InvalidSchemaName,
                    format!("schema \"{schema}\" does not exist"),
                )
            }),
        None => search_path.first().copied().ok_or_else(|| {
            plan_error(
                SqlState::InvalidSchemaName,
                "no schema has been selected to create in",
            )
        }),
    }
}

fn name_search_path(
    catalog: &dyn CatalogManager,
    name: &QualifiedName,
    search_path: &[SchemaId],
) -> Result<Vec<SchemaId>> {
    if name.schema.is_some() {
        Ok(vec![resolve_schema_id(catalog, name, search_path)?])
    } else {
        Ok(search_path.to_vec())
    }
}

fn require_table(
    catalog: &dyn CatalogManager,
    name: &QualifiedName,
    search_path: &[SchemaId],
) -> Result<TableSchema> {
    if name.schema.as_deref().is_some_and(is_system_schema) {
        return Err(plan_error(
            SqlState::FeatureNotSupported,
            "cannot modify system catalog",
        ));
    }
    let schemas = match &name.schema {
        Some(_) => vec![resolve_schema_id(catalog, name, search_path)?],
        None => search_path.to_vec(),
    };
    for schema in schemas {
        if let Some(found) = catalog.get_table_in_schema(schema, &name.name)? {
            if matches!(found.relation_kind, RelationKind::Toast { .. }) {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    "hidden TOAST relations are not queryable",
                ));
            }
            return Ok(found);
        }
        if catalog.get_view_in_schema(schema, &name.name)?.is_some() {
            return Err(plan_error(
                SqlState::FeatureNotSupported,
                "cannot modify view",
            ));
        }
        if catalog.get_index_in_schema(schema, &name.name)?.is_some()
            || catalog
                .get_sequence_in_schema(schema, &name.name)?
                .is_some()
        {
            return Err(plan_error(
                SqlState::WrongObjectType,
                format!("relation {name} is not a table"),
            ));
        }
    }
    if name.schema.is_none() && resolve_system_view(None, &name.name).is_some() {
        return Err(plan_error(
            SqlState::FeatureNotSupported,
            "cannot modify system catalog",
        ));
    }
    Err(plan_error(
        SqlState::UndefinedTable,
        format!("table {name} does not exist"),
    ))
}

fn require_index(
    catalog: &dyn CatalogManager,
    name: &QualifiedName,
    search_path: &[SchemaId],
) -> Result<common::IndexSchema> {
    let schemas = match &name.schema {
        Some(_) => vec![resolve_schema_id(catalog, name, search_path)?],
        None => search_path.to_vec(),
    };
    for schema in schemas {
        if let Some(index) = catalog.get_index_in_schema(schema, &name.name)? {
            return Ok(index);
        }
        if catalog.get_table_in_schema(schema, &name.name)?.is_some()
            || catalog.get_view_in_schema(schema, &name.name)?.is_some()
            || catalog
                .get_sequence_in_schema(schema, &name.name)?
                .is_some()
        {
            return Err(plan_error(
                SqlState::WrongObjectType,
                format!("relation {name} is not an index"),
            ));
        }
    }
    Err(plan_error(
        SqlState::UndefinedTable,
        format!("index {name} does not exist"),
    ))
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

fn reject_window(expr: &BoundExpr, message: &str) -> Result<()> {
    if contains_window(expr) {
        return Err(plan_error(SqlState::WindowingError, message));
    }
    Ok(())
}

fn contains_window(expr: &BoundExpr) -> bool {
    if matches!(expr, BoundExpr::WindowCall { .. }) {
        return true;
    }
    let mut found = false;
    let _ = crate::params::for_each_child(expr, &mut |child| {
        found |= contains_window(child);
        Ok(())
    });
    found
}

fn contains_aggregate(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::AggregateCall { .. } => true,
        BoundExpr::WindowCall { args, spec, .. } => {
            args.iter().any(contains_aggregate)
                || spec.partition_by.iter().any(contains_aggregate)
                || spec
                    .order_by
                    .iter()
                    .any(|item| contains_aggregate(&item.expr))
        }
        BoundExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. }
        | BoundExpr::RuntimeInSet { expr, .. } => contains_aggregate(expr),
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

fn reject_aggregate_outside_window(expr: &BoundExpr) -> Result<()> {
    if contains_aggregate_outside_window(expr) {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "aggregate calls are not allowed here",
        ));
    }
    Ok(())
}

fn contains_aggregate_outside_window(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::AggregateCall { .. } => true,
        BoundExpr::WindowCall { .. } => false,
        _ => {
            let mut found = false;
            let _ = crate::params::for_each_child(expr, &mut |child| {
                found |= contains_aggregate_outside_window(child);
                Ok(())
            });
            found
        }
    }
}

/// Validate a column's `DEFAULT` constant against its declared type. The default
/// is a constant folded by the parser; it must have the same type as the column
/// (no implicit casts), except `NULL` is accepted only when the column is
/// nullable (a `NULL` default on a `NOT NULL` column is rejected up front).
fn resolve_sequence_literal(
    catalog: &dyn CatalogManager,
    search_path: &[SchemaId],
    name: &str,
) -> Result<Option<common::SequenceSchema>> {
    if let Some((schema, relation)) = name.split_once('.') {
        if schema.is_empty() || relation.is_empty() {
            return Err(plan_error(SqlState::SyntaxError, "invalid sequence name"));
        }
        let namespace = catalog.get_schema_by_name(schema)?.ok_or_else(|| {
            plan_error(
                SqlState::InvalidSchemaName,
                format!("schema {schema} does not exist"),
            )
        })?;
        return catalog.get_sequence_in_schema(namespace.id, relation);
    }
    for schema in search_path {
        if let Some(sequence) = catalog.get_sequence_in_schema(*schema, name)? {
            return Ok(Some(sequence));
        }
        if catalog.get_table_in_schema(*schema, name)?.is_some()
            || catalog.get_view_in_schema(*schema, name)?.is_some()
            || catalog.get_index_in_schema(*schema, name)?.is_some()
        {
            return Err(plan_error(
                SqlState::WrongObjectType,
                format!("relation {name} is not a sequence"),
            ));
        }
    }
    Ok(None)
}

fn validate_default_value(
    catalog: &dyn CatalogManager,
    search_path: &[SchemaId],
    column: &ParsedColumnDef,
) -> Result<()> {
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
            if resolve_sequence_literal(catalog, search_path, name)?.is_none() {
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
            let bound = bind_default_expr_with_options(
                catalog,
                text,
                &BindOptions {
                    search_path: search_path.to_vec(),
                },
            )?;
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
        ParsedDefault::Stored(_) => {
            return Err(DbError::internal(
                "stored expression default reached parsed-statement validation",
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
    bind_default_expr_with_options(catalog, text, &BindOptions::default())
}

pub fn bind_default_expr_with_options(
    catalog: &dyn CatalogManager,
    text: &str,
    options: &BindOptions,
) -> Result<BoundExpr> {
    let parsed = parser::parse_expression(text)?;
    let mut ctx = BindContext::with_outer(catalog, &[], &options.search_path, Vec::new());
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
        BoundExpr::WindowCall { .. } => {
            return Err(plan_error(
                SqlState::WindowingError,
                "window functions are not allowed in DEFAULT or CHECK expressions",
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
        Expr::WindowFunction { args, spec, .. } => {
            for arg in args {
                if let FunctionArg::Expr(arg) = arg {
                    reject_qualified_check_column_refs(arg)?;
                }
            }
            for expr in &spec.partition_by {
                reject_qualified_check_column_refs(expr)?;
            }
            for item in &spec.order_by {
                reject_qualified_check_column_refs(&item.expr)?;
            }
            if let Some(frame) = &spec.frame {
                reject_qualified_check_column_refs_in_window_frame_bound(&frame.start)?;
                reject_qualified_check_column_refs_in_window_frame_bound(&frame.end)?;
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

fn reject_qualified_check_column_refs_in_window_frame_bound(
    bound: &parser::WindowFrameBound,
) -> Result<()> {
    match bound {
        parser::WindowFrameBound::Preceding(expr) | parser::WindowFrameBound::Following(expr) => {
            reject_qualified_check_column_refs(expr)
        }
        parser::WindowFrameBound::UnboundedPreceding
        | parser::WindowFrameBound::CurrentRow
        | parser::WindowFrameBound::UnboundedFollowing => Ok(()),
    }
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
    catalog
        .list_constraints_for_table(table.id)?
        .into_iter()
        .filter_map(|constraint| match constraint.kind {
            common::ConstraintKind::Check { expression } => Some(expression),
            _ => None,
        })
        .map(|expression| crate::lower_stored_expression(catalog, &expression, &table.columns))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn bind_create_table_foreign_keys(
    catalog: &dyn CatalogManager,
    search_path: &[SchemaId],
    table_schema: SchemaId,
    table_name: &str,
    columns: &[ParsedColumnDef],
    primary_key: &[String],
    unique: &[Vec<String>],
    foreign_keys: &[ParsedForeignKey],
) -> Result<Vec<BoundForeignKey>> {
    let mut bound = Vec::new();
    bound.try_reserve(foreign_keys.len()).map_err(|_| {
        plan_error(
            SqlState::ProgramLimitExceeded,
            "too many foreign-key constraints",
        )
    })?;
    for foreign_key in foreign_keys {
        if foreign_key.columns.is_empty() {
            return Err(plan_error(
                SqlState::InvalidForeignKey,
                "foreign key must contain at least one source column",
            ));
        }
        let source = resolve_proposed_column_names(columns, &foreign_key.columns, "foreign key")?;
        let parent = resolve_create_foreign_key_parent(
            catalog,
            search_path,
            table_schema,
            table_name,
            &foreign_key.referenced_table,
        )?;
        let (target, target_columns) = if parent.is_none() {
            let names =
                referenced_column_names(&foreign_key.referenced_columns, primary_key, table_name)?;
            validate_self_referenced_constraint(&names, primary_key, unique)?;
            let ids = resolve_proposed_column_names(columns, &names, "referenced key")?;
            (
                BoundForeignKeyTarget::SelfTable {
                    columns: names.clone(),
                },
                ids.into_iter()
                    .map(|id| {
                        columns
                            .get(usize::from(id))
                            .map(parsed_declared_column_type)
                            .ok_or_else(|| DbError::internal("resolved self FK column disappeared"))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        } else {
            let parent = parent.ok_or_else(|| {
                DbError::internal("foreign-key parent disappeared after self-reference check")
            })?;
            let names = referenced_column_names(
                &foreign_key.referenced_columns,
                &parent
                    .primary_key
                    .iter()
                    .map(|id| {
                        parent
                            .columns
                            .iter()
                            .find(|column| column.id == *id)
                            .map(|column| column.name.clone())
                            .ok_or_else(|| {
                                DbError::internal("parent primary-key column is missing")
                            })
                    })
                    .collect::<Result<Vec<_>>>()?,
                &parent.name,
            )?;
            let ids = resolve_existing_column_names(&parent, &names)?;
            if catalog
                .resolve_foreign_key_index(parent.id, &ids)?
                .is_none()
            {
                return Err(plan_error(
                    SqlState::InvalidForeignKey,
                    format!(
                        "there is no eligible unique constraint matching referenced columns on table {}",
                        parent.name
                    ),
                ));
            }
            let target_columns = ids
                .iter()
                .map(|id| {
                    parent
                        .columns
                        .iter()
                        .find(|column| column.id == *id)
                        .map(durable_declared_column_type)
                        .ok_or_else(|| DbError::internal("resolved parent FK column disappeared"))
                })
                .collect::<Result<Vec<_>>>()?;
            (
                BoundForeignKeyTarget::Existing {
                    table: parent.id,
                    columns: ids,
                },
                target_columns,
            )
        };
        if source.len() != target_columns.len() {
            return Err(plan_error(
                SqlState::InvalidForeignKey,
                "foreign key source and referenced column counts do not match",
            ));
        }
        for (source_id, (target_type, target_pg_type, target_length, target_name)) in
            source.iter().zip(target_columns)
        {
            let source_column = columns
                .get(usize::from(*source_id))
                .ok_or_else(|| DbError::internal("resolved source FK column disappeared"))?;
            let source_pg_type = source_column
                .pg_type
                .clone()
                .unwrap_or_else(|| PgType::from(&source_column.data_type));
            if source_column.data_type != target_type
                || source_pg_type != target_pg_type
                || source_column.max_length != target_length
            {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!(
                        "foreign key columns {} and {} have incompatible declared types",
                        source_column.name, target_name
                    ),
                ));
            }
        }
        bound.push(BoundForeignKey {
            name: foreign_key.name.clone(),
            columns: source,
            target,
            on_update: foreign_key.on_update,
            on_delete: foreign_key.on_delete,
        });
    }
    Ok(bound)
}

fn validate_create_foreign_key_names(
    table: &str,
    primary_key: &[String],
    unique: &[Vec<String>],
    foreign_keys: &[ParsedForeignKey],
) -> Result<()> {
    const FOREIGN_KEY_CAPACITY: usize = 4096;
    if foreign_keys.len() > FOREIGN_KEY_CAPACITY {
        return Err(plan_error(
            SqlState::ProgramLimitExceeded,
            format!("foreign key id allocator is exhausted for table {table}"),
        ));
    }
    let capacity = unique
        .len()
        .checked_add(foreign_keys.len())
        .and_then(|count| count.checked_add(usize::from(!primary_key.is_empty())))
        .ok_or_else(|| plan_error(SqlState::ProgramLimitExceeded, "too many constraints"))?;
    let mut names = HashSet::new();
    names
        .try_reserve(capacity)
        .map_err(|_| plan_error(SqlState::ProgramLimitExceeded, "too many constraint names"))?;
    if !primary_key.is_empty() {
        names.insert(format!("{table}_pkey"));
    }
    for columns in unique {
        names.insert(format!("{table}_{}_key", columns.join("_")));
    }
    for foreign_key in foreign_keys {
        if foreign_key.columns.is_empty() {
            return Err(plan_error(
                SqlState::InvalidForeignKey,
                "foreign key must contain at least one source column",
            ));
        }
        if let Some(name) = &foreign_key.name {
            if !names.insert(name.clone()) {
                return Err(plan_error(
                    SqlState::DuplicateObject,
                    format!("constraint {name} for table {table} already exists"),
                ));
            }
            continue;
        }
        let base = format!("{table}_{}_fkey", foreign_key.columns.join("_"));
        if names.insert(base.clone()) {
            continue;
        }
        let mut suffix = 1_u32;
        loop {
            let candidate = format!("{base}{suffix}");
            if names.insert(candidate) {
                break;
            }
            suffix = suffix.checked_add(1).ok_or_else(|| {
                plan_error(
                    SqlState::ProgramLimitExceeded,
                    "foreign key constraint name suffix space exhausted",
                )
            })?;
        }
    }
    Ok(())
}

/// Resolve a CREATE TABLE FK parent with the proposed table inserted into the
/// ordinary search path at its target schema. `None` denotes that self target.
fn resolve_create_foreign_key_parent(
    catalog: &dyn CatalogManager,
    search_path: &[SchemaId],
    table_schema: SchemaId,
    table_name: &str,
    referenced: &QualifiedName,
) -> Result<Option<TableSchema>> {
    if referenced.schema.is_some() {
        let schema = resolve_schema_id(catalog, referenced, search_path)?;
        if schema == table_schema && referenced.name == table_name {
            return Ok(None);
        }
        return referenced_table_in_schema(catalog, schema, &referenced.name).map(Some);
    }

    for schema in search_path {
        if *schema == table_schema && referenced.name == table_name {
            return Ok(None);
        }
        if let Some(table) = catalog.get_table_in_schema(*schema, &referenced.name)? {
            if table.relation_kind != RelationKind::User {
                return Err(plan_error(
                    SqlState::UndefinedTable,
                    format!("table {} does not exist", referenced.name),
                ));
            }
            return Ok(Some(table));
        }
        if catalog
            .get_view_in_schema(*schema, &referenced.name)?
            .is_some()
            || catalog
                .get_index_in_schema(*schema, &referenced.name)?
                .is_some()
            || catalog
                .get_sequence_in_schema(*schema, &referenced.name)?
                .is_some()
        {
            return Err(plan_error(
                SqlState::WrongObjectType,
                format!("relation {} is not a table", referenced.name),
            ));
        }
    }
    Err(plan_error(
        SqlState::UndefinedTable,
        format!("table {} does not exist", referenced.name),
    ))
}

fn referenced_table_in_schema(
    catalog: &dyn CatalogManager,
    schema: SchemaId,
    name: &str,
) -> Result<TableSchema> {
    if let Some(table) = catalog.get_table_in_schema(schema, name)?
        && table.relation_kind == RelationKind::User
    {
        return Ok(table);
    }
    if catalog.get_view_in_schema(schema, name)?.is_some()
        || catalog.get_index_in_schema(schema, name)?.is_some()
        || catalog.get_sequence_in_schema(schema, name)?.is_some()
    {
        return Err(plan_error(
            SqlState::WrongObjectType,
            format!("relation {name} is not a table"),
        ));
    }
    Err(plan_error(
        SqlState::UndefinedTable,
        format!("table {name} does not exist"),
    ))
}

fn parsed_declared_column_type(
    column: &ParsedColumnDef,
) -> (DataType, PgType, Option<u32>, String) {
    (
        column.data_type.clone(),
        column
            .pg_type
            .clone()
            .unwrap_or_else(|| PgType::from(&column.data_type)),
        column.max_length,
        column.name.clone(),
    )
}

fn durable_declared_column_type(column: &ColumnDef) -> (DataType, PgType, Option<u32>, String) {
    (
        column.data_type.clone(),
        column.wire_type(),
        column.max_length,
        column.name.clone(),
    )
}

fn referenced_column_names(
    explicit: &[String],
    primary_key: &[String],
    table: &str,
) -> Result<Vec<String>> {
    let names = if explicit.is_empty() {
        if primary_key.is_empty() {
            return Err(plan_error(
                SqlState::InvalidForeignKey,
                format!("referenced table {table} has no primary key"),
            ));
        }
        primary_key.to_vec()
    } else {
        explicit.to_vec()
    };
    let mut seen = HashSet::new();
    if names.iter().any(|name| !seen.insert(name)) {
        return Err(plan_error(
            SqlState::InvalidForeignKey,
            "foreign key contains duplicate referenced columns",
        ));
    }
    Ok(names)
}

fn validate_self_referenced_constraint(
    columns: &[String],
    primary_key: &[String],
    unique: &[Vec<String>],
) -> Result<()> {
    if columns == primary_key || unique.iter().any(|candidate| candidate == columns) {
        return Ok(());
    }
    Err(plan_error(
        SqlState::InvalidForeignKey,
        "there is no eligible unique constraint matching self-referenced columns",
    ))
}

fn resolve_proposed_column_names(
    columns: &[ParsedColumnDef],
    names: &[String],
    kind: &str,
) -> Result<Vec<ColumnId>> {
    let mut seen = HashSet::new();
    let mut resolved = Vec::new();
    for name in names {
        if !seen.insert(name) {
            return Err(plan_error(
                SqlState::InvalidForeignKey,
                format!("{kind} contains duplicate column {name}"),
            ));
        }
        let index = columns
            .iter()
            .position(|column| column.name == *name)
            .ok_or_else(|| {
                plan_error(
                    SqlState::UndefinedColumn,
                    format!("column {name} does not exist"),
                )
            })?;
        resolved.push(ColumnId::try_from(index).map_err(|_| {
            plan_error(
                SqlState::ProgramLimitExceeded,
                "foreign-key column position exceeds the catalog limit",
            )
        })?);
    }
    Ok(resolved)
}

fn resolve_existing_column_names(table: &TableSchema, names: &[String]) -> Result<Vec<ColumnId>> {
    let mut seen = HashSet::new();
    names
        .iter()
        .map(|name| {
            if !seen.insert(name) {
                return Err(plan_error(
                    SqlState::InvalidForeignKey,
                    format!("foreign key contains duplicate referenced column {name}"),
                ));
            }
            table
                .columns
                .iter()
                .find(|column| column.name == *name)
                .map(|column| column.id)
                .ok_or_else(|| {
                    plan_error(
                        SqlState::UndefinedColumn,
                        format!("column {name} does not exist on table {}", table.name),
                    )
                })
        })
        .collect()
}

/// View a `CREATE TABLE`'s not-yet-created columns as `ColumnDef`s (id = declaration
/// order) so a `CHECK` can be bound and validated before the table exists. Only the
/// name/type/nullability matter for binding; the default is irrelevant here.
fn parsed_columns_as_column_defs(columns: &[ParsedColumnDef]) -> Result<Vec<ColumnDef>> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let id = ColumnId::try_from(index).map_err(|_| {
                plan_error(
                    SqlState::ProgramLimitExceeded,
                    "column position exceeds the catalog limit",
                )
            })?;
            let object_id = u32::try_from(index)
                .map_err(|_| {
                    plan_error(
                        SqlState::ProgramLimitExceeded,
                        "column position exceeds the catalog limit",
                    )
                })?
                .checked_add(1)
                .ok_or_else(|| {
                    plan_error(
                        SqlState::ProgramLimitExceeded,
                        "column position exceeds the catalog limit",
                    )
                })?;
            Ok(ColumnDef {
                id,
                object_id,
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                max_length: column.max_length,
                default: None,
                pg_type: column.pg_type.clone(),
            })
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
        Expr::WindowFunction { args, spec, .. } => {
            args.iter().any(function_arg_has_sequence_function)
                || spec.partition_by.iter().any(expr_has_sequence_function)
                || spec
                    .order_by
                    .iter()
                    .any(|item| expr_has_sequence_function(&item.expr))
                || spec.frame.as_ref().is_some_and(|frame| {
                    window_frame_bound_has_sequence_function(&frame.start)
                        || window_frame_bound_has_sequence_function(&frame.end)
                })
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

fn window_frame_bound_has_sequence_function(bound: &parser::WindowFrameBound) -> bool {
    match bound {
        parser::WindowFrameBound::Preceding(expr) | parser::WindowFrameBound::Following(expr) => {
            expr_has_sequence_function(expr)
        }
        parser::WindowFrameBound::UnboundedPreceding
        | parser::WindowFrameBound::CurrentRow
        | parser::WindowFrameBound::UnboundedFollowing => false,
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
        Expr::WindowFunction { args, spec, .. } => {
            args.iter().any(function_arg_has_placeholder)
                || spec.partition_by.iter().any(expr_has_placeholder)
                || spec
                    .order_by
                    .iter()
                    .any(|item| expr_has_placeholder(&item.expr))
                || spec.frame.as_ref().is_some_and(|frame| {
                    window_frame_bound_has_placeholder(&frame.start)
                        || window_frame_bound_has_placeholder(&frame.end)
                })
        }
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

fn window_frame_bound_has_placeholder(bound: &parser::WindowFrameBound) -> bool {
    match bound {
        parser::WindowFrameBound::Preceding(expr) | parser::WindowFrameBound::Following(expr) => {
            expr_has_placeholder(expr)
        }
        parser::WindowFrameBound::UnboundedPreceding
        | parser::WindowFrameBound::CurrentRow
        | parser::WindowFrameBound::UnboundedFollowing => false,
    }
}
