use std::collections::HashSet;

use common::{
    CompressionSetting, DbError, IsolationLevel, PgType, Result, SequenceOptions, SqlState,
    TableOptionPatch, ToastCompression, ToastMode, ToastOptions,
};
use sqlparser::ast as sql;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::{FetchCount, SetScope, Statement};

mod ddl;
mod dml;
mod expr;
mod query;

use ddl::{
    CreateViewParts, convert_alter_table, convert_create_index, convert_create_table,
    convert_create_view, convert_truncate,
};
use dml::{convert_copy, convert_delete, convert_insert};
use expr::convert_expr;
use query::{
    convert_assignment, convert_query, convert_returning, convert_table_with_joins,
    table_name_from_table_with_joins,
};

pub fn parse_statement(sql: &str) -> Result<Statement> {
    // sqlparser 0.56 errors on `VACUUM`, so intercept it before handing the string
    // to the parser (`docs/specs/crates/parser.md`). `VACUUM` is a maintenance
    // command, not a relational statement, and never reaches bind/plan.
    if let Some(statement) = try_parse_vacuum(sql)? {
        return Ok(statement);
    }
    // sqlparser 0.56 does not parse PostgreSQL storage-parameter ALTER forms
    // consistently, so keep that narrow form in the hand parser and let
    // schema-evolution ALTER TABLE forms fall through to sqlparser.
    if let Some(statement) = try_parse_alter_table(sql)? {
        return Ok(statement);
    }
    if let Some(statement) = try_parse_reset(sql)? {
        return Ok(statement);
    }
    reject_set_global(sql)?;
    if let Some(statement) = try_parse_create_sequence(sql)? {
        return Ok(statement);
    }
    if let Some(statement) = try_parse_fetch_cursor(sql)? {
        return Ok(statement);
    }

    // sqlparser reads inline data after `COPY ... FROM STDIN` and then requires a
    // statement terminator. We stream copy-in over the wire and never carry
    // inline data, so ensure the statement is terminated. A trailing `;` is a
    // no-op for every other statement and never introduces a second one.
    let trimmed = sql.trim_end();
    let normalized = if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{trimmed};")
    };

    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, &normalized)
        .map_err(|err| parse_error(format!("failed to parse SQL: {err}")))?;

    if statements.len() != 1 {
        return Err(parse_error("expected exactly one SQL statement"));
    }

    convert_statement(statements.remove(0))
}

/// Parse a single SQL scalar expression (not a statement). Used to re-parse the
/// canonical text of a stored non-constant column `DEFAULT` so the binder can bind
/// it. The whole input must be one expression; trailing tokens are rejected.
pub fn parse_expression(sql: &str) -> Result<crate::Expr> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|err| parse_error(format!("failed to parse expression: {err}")))?;
    let expr = parser
        .parse_expr()
        .map_err(|err| parse_error(format!("failed to parse expression: {err}")))?;
    if parser.peek_token().token != Token::EOF {
        return Err(parse_error("unexpected trailing tokens after expression"));
    }
    convert_expr(&expr)
}

fn convert_statement(statement: sql::Statement) -> Result<Statement> {
    match statement {
        sql::Statement::CreateTable(table) => convert_create_table(table),
        sql::Statement::CreateIndex(index) => convert_create_index(index),
        sql::Statement::CreateView {
            or_alter,
            name,
            or_replace,
            columns,
            query,
            materialized,
            options,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            to,
            params,
        } => convert_create_view(CreateViewParts {
            or_alter,
            or_replace,
            materialized,
            name,
            columns,
            query: *query,
            options,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            to,
            params,
        }),
        sql::Statement::AlterTable {
            name,
            if_exists,
            only,
            operations,
            location,
            on_cluster,
        } => convert_alter_table(name, if_exists, only, operations, location, on_cluster),
        sql::Statement::Truncate {
            table_names,
            partitions,
            table: _,
            only,
            identity,
            cascade,
            on_cluster,
        } => convert_truncate(table_names, partitions, only, identity, cascade, on_cluster),
        sql::Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
        } => {
            if cascade || restrict || purge || temporary {
                return unsupported("unsupported DROP form");
            }
            match object_type {
                sql::ObjectType::Table => {
                    let names = names.iter().map(object_name).collect::<Result<Vec<_>>>()?;
                    reject_duplicate_relation_names(&names, "DROP TABLE")?;
                    Ok(Statement::DropTable { names, if_exists })
                }
                object_type => {
                    if names.len() != 1 {
                        return unsupported("unsupported DROP form");
                    }
                    let name = object_name(&names[0])?;
                    match object_type {
                        sql::ObjectType::View => Ok(Statement::DropView { name, if_exists }),
                        sql::ObjectType::Index if !if_exists => Ok(Statement::DropIndex { name }),
                        sql::ObjectType::Sequence => {
                            Ok(Statement::DropSequence { name, if_exists })
                        }
                        sql::ObjectType::Index => unsupported("unsupported DROP form"),
                        _ => unsupported("unsupported DROP object type"),
                    }
                }
            }
        }
        sql::Statement::Insert(insert) => convert_insert(insert),
        sql::Statement::Query(query) => Ok(Statement::Query(convert_query(*query)?)),
        sql::Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        } => {
            if or.is_some() {
                return unsupported("unsupported UPDATE form");
            }
            let from = match from {
                None => Vec::new(),
                // The standard (PostgreSQL) placement, after SET.
                Some(sql::UpdateTableFromKind::AfterSet(tables)) => tables
                    .iter()
                    .map(convert_table_with_joins)
                    .collect::<Result<Vec<_>>>()?,
                Some(sql::UpdateTableFromKind::BeforeSet(_)) => {
                    return unsupported("UPDATE FROM before SET is not supported");
                }
            };

            let table = table_name_from_table_with_joins(&table)?;
            let assignments = assignments
                .into_iter()
                .map(convert_assignment)
                .collect::<Result<Vec<_>>>()?;
            let filter = selection.map(|expr| convert_expr(&expr)).transpose()?;

            Ok(Statement::Update {
                table,
                assignments,
                from,
                filter,
                returning: convert_returning(&returning)?,
            })
        }
        sql::Statement::Delete(delete) => convert_delete(delete),
        sql::Statement::Declare { stmts } => convert_declare_cursor(stmts),
        sql::Statement::Fetch {
            name,
            direction,
            into,
        } => convert_fetch_cursor(name, direction, into),
        sql::Statement::Close { cursor } => convert_close_cursor(cursor),
        sql::Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            if describe_alias != sql::DescribeAlias::Explain
                || analyze
                || verbose
                || query_plan
                || estimate
                || format.is_some()
                || options.is_some()
            {
                return unsupported("unsupported EXPLAIN form");
            }
            match convert_statement(*statement)? {
                Statement::Query(query) => {
                    Ok(Statement::Explain(Box::new(Statement::Query(query))))
                }
                _ => unsupported("EXPLAIN supports SELECT only in v1"),
            }
        }
        sql::Statement::StartTransaction {
            modes,
            begin: _,
            transaction: _,
            modifier,
            statements,
            exception_statements,
            has_end_keyword,
        } => {
            // Accept plain `BEGIN` / `BEGIN TRANSACTION` / `START TRANSACTION`, with
            // an optional `ISOLATION LEVEL <level>` and access mode. `transaction`/
            // `begin` are pure keyword spellings of the same form, so they are
            // intentionally ignored. MySQL-style modifiers and atomic-block bodies
            // are rejected. (sqlparser 0.56 does not parse `[NOT] DEFERRABLE` in this
            // position, so a `DEFERRABLE` clause is already a parse-time syntax error
            // upstream and never reaches here.)
            if modifier.is_some()
                || !statements.is_empty()
                || exception_statements.is_some()
                || has_end_keyword
            {
                return unsupported("unsupported BEGIN/START TRANSACTION form");
            }
            let isolation = transaction_isolation_mode(&modes)?;
            Ok(Statement::Begin { isolation })
        }
        sql::Statement::Set(set) => convert_set(set),
        sql::Statement::ShowVariable { variable } => convert_show(&variable),
        sql::Statement::Discard { object_type } => match object_type {
            sql::DiscardObject::ALL => Ok(Statement::DiscardAll),
            _ => feature_not_supported("only DISCARD ALL is supported"),
        },
        sql::Statement::Commit {
            chain,
            end: _,
            modifier,
        } => {
            // Accept plain `COMMIT` and `END` (`end` is just the keyword
            // spelling). `AND CHAIN` and MySQL-style modifiers are unsupported.
            if chain || modifier.is_some() {
                return unsupported("unsupported COMMIT form");
            }
            Ok(Statement::Commit)
        }
        sql::Statement::Rollback { chain, savepoint } => {
            // `AND CHAIN` is unsupported. `ROLLBACK TO [SAVEPOINT] <name>` becomes a
            // savepoint rollback; plain `ROLLBACK` aborts the transaction.
            if chain {
                return unsupported("unsupported ROLLBACK form");
            }
            match savepoint {
                Some(name) => Ok(Statement::RollbackToSavepoint {
                    name: ident_name(&name)?,
                }),
                None => Ok(Statement::Rollback),
            }
        }
        sql::Statement::Savepoint { name } => Ok(Statement::Savepoint {
            name: ident_name(&name)?,
        }),
        sql::Statement::ReleaseSavepoint { name } => Ok(Statement::ReleaseSavepoint {
            name: ident_name(&name)?,
        }),
        sql::Statement::Copy {
            source,
            to,
            target,
            options,
            legacy_options,
            values,
        } => convert_copy(source, to, target, options, legacy_options, values),
        _ => unsupported("unsupported SQL statement"),
    }
}

fn convert_declare_cursor(mut stmts: Vec<sql::Declare>) -> Result<Statement> {
    if stmts.len() != 1 {
        return unsupported("DECLARE supports a single cursor declaration");
    }
    let statement = stmts.remove(0);
    if statement.names.len() != 1
        || statement.data_type.is_some()
        || statement.assignment.is_some()
        || statement.declare_type != Some(sql::DeclareType::Cursor)
    {
        return unsupported("DECLARE supports cursor declarations only");
    }
    if statement.binary == Some(true) {
        return feature_not_supported("BINARY cursors are not supported");
    }
    if statement.sensitive.is_some() {
        return feature_not_supported("cursor sensitivity options are not supported");
    }
    if statement.scroll.is_some() {
        return feature_not_supported("SCROLL cursors are not supported");
    }
    if statement.hold.is_some() {
        return feature_not_supported("cursor hold options are not supported");
    }
    let query = statement
        .for_query
        .ok_or_else(|| parse_error("DECLARE CURSOR requires FOR SELECT"))?;
    let query = convert_query(*query)?;
    if !cursor_query_body_is_select(&query) {
        return unsupported("DECLARE CURSOR requires SELECT");
    }
    Ok(Statement::DeclareCursor {
        name: ident_name(&statement.names[0])?,
        query,
    })
}

fn cursor_query_body_is_select(query: &crate::Query) -> bool {
    match &query.body {
        crate::QueryBody::Select(_) => true,
        crate::QueryBody::SetOp { left, right, .. } => {
            cursor_query_body_is_select(left) && cursor_query_body_is_select(right)
        }
        crate::QueryBody::Values(_) => false,
    }
}

fn convert_fetch_cursor(
    name: sql::Ident,
    direction: sql::FetchDirection,
    into: Option<sql::ObjectName>,
) -> Result<Statement> {
    if into.is_some() {
        return unsupported("FETCH INTO is not supported");
    }
    let count = match direction {
        sql::FetchDirection::Count { limit } => FetchCount::Count(fetch_count_value(&limit)?),
        sql::FetchDirection::All => FetchCount::All,
        sql::FetchDirection::Forward { limit: Some(limit) } => {
            FetchCount::Count(fetch_count_value(&limit)?)
        }
        sql::FetchDirection::Forward { limit: None } => FetchCount::One,
        _ => return unsupported("unsupported FETCH direction"),
    };
    Ok(Statement::FetchCursor {
        name: ident_name(&name)?,
        count,
    })
}

fn convert_close_cursor(cursor: sql::CloseCursor) -> Result<Statement> {
    match cursor {
        sql::CloseCursor::Specific { name } => Ok(Statement::CloseCursor {
            name: ident_name(&name)?,
        }),
        sql::CloseCursor::All => feature_not_supported("CLOSE ALL is not supported"),
    }
}

/// Convert a `SET ...` statement. Transaction-control forms retain their
/// dedicated variants. Driver-style session configuration forms become
/// `Statement::SetVariable` so the server can handle them against a connection
/// GUC store without binding or planning.
fn convert_set(set: sql::Set) -> Result<Statement> {
    match set {
        sql::Set::SetTransaction {
            modes,
            snapshot,
            session,
        } => {
            if snapshot.is_some() {
                return unsupported("SET TRANSACTION SNAPSHOT is not supported in v1");
            }
            let isolation = transaction_isolation_mode(&modes)?;
            if session {
                // `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`
                // sets the per-connection default isolation for FUTURE transactions
                // (G2). It reuses the same level mapping and access-mode handling as
                // the transaction-scoped form above.
                Ok(Statement::SetSessionCharacteristics { isolation })
            } else {
                Ok(Statement::SetTransaction { isolation })
            }
        }
        sql::Set::SingleAssignment {
            scope,
            hivevar,
            variable,
            values,
        } => {
            if hivevar {
                return unsupported("Hive SET variables are not supported");
            }
            let scope = guc_scope(scope)?;
            let name = guc_object_name(&variable)?;
            let value = set_value_text(&values)?;
            Ok(Statement::SetVariable { scope, name, value })
        }
        sql::Set::SetTimeZone { local, value } => Ok(Statement::SetVariable {
            scope: if local {
                SetScope::Local
            } else {
                SetScope::Session
            },
            name: "timezone".to_string(),
            value: set_expr_text(&value),
        }),
        sql::Set::SetNames {
            charset_name,
            collation_name,
        } => {
            if collation_name.is_some() {
                return unsupported("SET NAMES COLLATE is not supported");
            }
            Ok(Statement::SetVariable {
                scope: SetScope::Session,
                name: "client_encoding".to_string(),
                value: charset_name.value,
            })
        }
        sql::Set::SetNamesDefault {} => Ok(Statement::SetVariable {
            scope: SetScope::Session,
            name: "client_encoding".to_string(),
            value: "default".to_string(),
        }),
        _ => unsupported("unsupported SET statement"),
    }
}

fn guc_scope(scope: Option<sql::ContextModifier>) -> Result<SetScope> {
    match scope {
        None | Some(sql::ContextModifier::Session) => Ok(SetScope::Session),
        Some(sql::ContextModifier::Local) => Ok(SetScope::Local),
        Some(sql::ContextModifier::Global) => unsupported("SET GLOBAL is not supported"),
    }
}

fn guc_object_name(name: &sql::ObjectName) -> Result<String> {
    guc_name_parts(name.0.iter().filter_map(sql::ObjectNamePart::as_ident))
}

fn guc_ident_name(ident: &sql::Ident) -> String {
    ident.value.to_ascii_lowercase()
}

fn guc_name_parts<'a>(parts: impl IntoIterator<Item = &'a sql::Ident>) -> Result<String> {
    let parts = parts.into_iter().map(guc_ident_name).collect::<Vec<_>>();
    match parts.as_slice() {
        [] => Err(parse_error("empty configuration parameter name")),
        [single] => Ok(single.clone()),
        [prefix, name] => Ok(format!("{prefix}.{name}")),
        _ => unsupported("configuration parameter names support at most one dot"),
    }
}

fn set_value_text(values: &[sql::Expr]) -> Result<String> {
    if values.is_empty() {
        return Err(parse_error("SET requires a value"));
    }
    Ok(values
        .iter()
        .map(set_expr_text)
        .collect::<Vec<_>>()
        .join(", "))
}

fn set_expr_text(expr: &sql::Expr) -> String {
    match expr {
        sql::Expr::Value(value) => match value.clone().value.into_string() {
            Some(value) => value,
            None => value.to_string(),
        },
        sql::Expr::Identifier(ident) => ident.value.clone(),
        sql::Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        _ => expr.to_string(),
    }
}

fn convert_show(variable: &[sql::Ident]) -> Result<Statement> {
    if variable.is_empty() {
        return Err(parse_error("SHOW requires a parameter name or ALL"));
    }
    let parts = variable.iter().map(guc_ident_name).collect::<Vec<_>>();
    let part_refs = parts.iter().map(String::as_str).collect::<Vec<_>>();
    let name = match part_refs.as_slice() {
        ["all"] => return Ok(Statement::ShowVariable { name: None }),
        ["time", "zone"] => "timezone".to_string(),
        ["transaction", "isolation", "level"] => "transaction_isolation".to_string(),
        [single] => (*single).to_string(),
        [prefix, name] => format!("{prefix}.{name}"),
        _ => return unsupported("unsupported SHOW variable name"),
    };
    Ok(Statement::ShowVariable { name: Some(name) })
}

/// Extract an optional `ISOLATION LEVEL <level>` from a list of transaction modes,
/// mapping the SQL level onto our two levels. Access modes are validated but carry
/// no value here: `READ WRITE` (the default) is accepted and ignored, while
/// `READ ONLY` is rejected because v1 does not enforce read-only and silently
/// ignoring it would be misleading. At most one isolation-level mode is allowed.
fn transaction_isolation_mode(modes: &[sql::TransactionMode]) -> Result<Option<IsolationLevel>> {
    let mut isolation = None;
    for mode in modes {
        match mode {
            sql::TransactionMode::IsolationLevel(level) => {
                if isolation.is_some() {
                    return unsupported("multiple ISOLATION LEVEL modes");
                }
                isolation = Some(map_isolation_level(*level));
            }
            sql::TransactionMode::AccessMode(sql::TransactionAccessMode::ReadWrite) => {
                // The default; accepted and ignored (v1 is always read-write).
            }
            sql::TransactionMode::AccessMode(sql::TransactionAccessMode::ReadOnly) => {
                return unsupported("READ ONLY transactions are not supported in v1");
            }
        }
    }
    Ok(isolation)
}

/// Map the four SQL isolation levels (plus the non-standard `SNAPSHOT`) onto
/// SaguaroDB's three: Read Committed, Repeatable Read (= snapshot isolation), and
/// Serializable (SSI).
///
/// - `READ UNCOMMITTED` -> Read Committed (we never expose uncommitted data; the
///   weaker level is strengthened to the weakest we offer).
/// - `READ COMMITTED` -> Read Committed.
/// - `REPEATABLE READ` -> Repeatable Read.
/// - `SERIALIZABLE` -> Serializable (SSI): the Repeatable Read snapshot plus
///   rw-conflict tracking and dangerous-structure detection. See
///   `docs/specs/ssi.md`.
/// - `SNAPSHOT` -> Repeatable Read (snapshot isolation by definition).
fn map_isolation_level(level: sql::TransactionIsolationLevel) -> IsolationLevel {
    match level {
        sql::TransactionIsolationLevel::ReadUncommitted
        | sql::TransactionIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
        sql::TransactionIsolationLevel::RepeatableRead
        | sql::TransactionIsolationLevel::Snapshot => IsolationLevel::RepeatableRead,
        sql::TransactionIsolationLevel::Serializable => IsolationLevel::Serializable,
    }
}

/// Map a declared SQL type to its PostgreSQL wire type ([`PgType`]). This is the
/// single source of truth for the SQL-spelling → type mapping; callers derive the
/// collapsed storage [`DataType`] via [`PgType::data_type`] so the two never drift.
/// Character types report their kind without a length here — the column path folds
/// the declared length in (CAST targets report the kind only).
fn convert_pg_type(data_type: &sql::DataType) -> Result<PgType> {
    match data_type {
        // Integer widths report distinct OIDs (int2/int4/int8) but share one
        // 64-bit storage type; a display width like `INTEGER(5)` is not supported.
        sql::DataType::SmallInt(None) | sql::DataType::Int2(None) => Ok(PgType::Int2),
        sql::DataType::Integer(None) | sql::DataType::Int(None) | sql::DataType::Int4(None) => {
            Ok(PgType::Int4)
        }
        sql::DataType::BigInt(None) | sql::DataType::Int8(None) => Ok(PgType::Int8),
        sql::DataType::Custom(name, modifiers) => {
            let name = custom_type_name(name)?;
            match name.as_str() {
                "oid" | "pg_catalog.oid" => {
                    if !modifiers.is_empty() {
                        return unsupported("OID type modifiers are not supported");
                    }
                    Ok(PgType::Oid)
                }
                _ => unsupported("unsupported data type"),
            }
        }
        // Character types share the single `TEXT` storage type but report distinct
        // OIDs (text / varchar / bpchar). The declared length is applied by the
        // column path, not here.
        sql::DataType::Text => Ok(PgType::Text),
        sql::DataType::Varchar(_) => Ok(PgType::Varchar(None)),
        sql::DataType::Char(_) | sql::DataType::Character(_) => Ok(PgType::Bpchar(None)),
        sql::DataType::Boolean | sql::DataType::Bool => Ok(PgType::Bool),
        sql::DataType::Date => Ok(PgType::Date),
        // TIMESTAMP without time zone and without a fractional-seconds precision.
        // WITH TIME ZONE and an explicit precision are not supported.
        sql::DataType::Timestamp(
            None,
            sql::TimezoneInfo::None | sql::TimezoneInfo::WithoutTimeZone,
        ) => Ok(PgType::Timestamp),
        // TIMESTAMP WITH TIME ZONE / TIMESTAMPTZ (UTC-normalized), no precision.
        sql::DataType::Timestamp(None, sql::TimezoneInfo::WithTimeZone | sql::TimezoneInfo::Tz) => {
            Ok(PgType::Timestamptz)
        }
        // TIME without time zone, no fractional-seconds precision.
        sql::DataType::Time(None, sql::TimezoneInfo::None | sql::TimezoneInfo::WithoutTimeZone) => {
            Ok(PgType::Time)
        }
        sql::DataType::Interval => Ok(PgType::Interval),
        sql::DataType::Bytea => Ok(PgType::Bytea),
        sql::DataType::Uuid => Ok(PgType::Uuid),
        // DOUBLE PRECISION and its aliases (`FLOAT8`, bare `FLOAT`).
        sql::DataType::DoublePrecision
        | sql::DataType::Float8
        | sql::DataType::Double(sql::ExactNumberInfo::None) => Ok(PgType::Float8),
        // REAL / FLOAT4 (single precision).
        sql::DataType::Real | sql::DataType::Float4 => Ok(PgType::Float4),
        // `FLOAT(p)`: PostgreSQL maps p in 1..=24 to REAL and 25..=53 to DOUBLE
        // PRECISION; bare `FLOAT` is DOUBLE PRECISION.
        sql::DataType::Float(precision) => match precision {
            None => Ok(PgType::Float8),
            Some(p) if (1..=24).contains(p) => Ok(PgType::Float4),
            Some(p) if (25..=53).contains(p) => Ok(PgType::Float8),
            Some(_) => unsupported("float precision must be between 1 and 53"),
        },
        // NUMERIC / DECIMAL, optionally with (precision[, scale]).
        sql::DataType::Numeric(info) | sql::DataType::Decimal(info) => {
            let (precision, scale) = numeric_typmod(info)?;
            Ok(PgType::Numeric { precision, scale })
        }
        _ => unsupported("unsupported data type"),
    }
}

fn custom_type_name(name: &sql::ObjectName) -> Result<String> {
    match name.0.as_slice() {
        [name] => {
            let name = name
                .as_ident()
                .ok_or_else(|| parse_error("unsupported type name part"))?;
            ident_name(name)
        }
        [schema, name] => {
            let schema = schema
                .as_ident()
                .ok_or_else(|| parse_error("unsupported type schema part"))?;
            let name = name
                .as_ident()
                .ok_or_else(|| parse_error("unsupported type name part"))?;
            let schema = ident_name(schema)?;
            if schema != "pg_catalog" {
                return feature_not_supported(
                    "qualified type names are supported only in pg_catalog",
                );
            }
            Ok(format!("{schema}.{}", ident_name(name)?))
        }
        _ => feature_not_supported(
            "qualified type names with more than one schema are not supported",
        ),
    }
}

/// The wire type of a `SERIAL`-family column, or `None` for any other type. The
/// column stores a 64-bit integer with a sequence-backed default, but reports the
/// PostgreSQL width of its serial kind (`smallserial` => int2, `serial` => int4,
/// `bigserial` => int8).
pub(super) fn serial_pg_type(data_type: &sql::DataType) -> Result<Option<PgType>> {
    let sql::DataType::Custom(name, modifiers) = data_type else {
        return Ok(None);
    };
    let name = custom_type_name(name)?;
    let pg_type = match name.as_str() {
        "serial2" | "smallserial" => PgType::Int2,
        "serial" | "serial4" => PgType::Int4,
        "serial8" | "bigserial" => PgType::Int8,
        _ => return Ok(None),
    };
    if !modifiers.is_empty() {
        return unsupported("SERIAL type modifiers are not supported");
    }
    Ok(Some(pg_type))
}

/// Validate a NUMERIC/DECIMAL type modifier and return `(precision, scale)`:
/// precision must be `1..=28` (our `Decimal` limit) and scale `0..=precision`.
/// Shared by the column-type, `CAST`, and typed-literal paths so all three reject
/// the same way (and `apply_typmod`'s `scale <= precision` precondition holds).
pub(super) fn numeric_typmod(info: &sql::ExactNumberInfo) -> Result<(Option<u32>, u32)> {
    let (precision, scale) = match info {
        sql::ExactNumberInfo::None => (None, 0_u64),
        sql::ExactNumberInfo::Precision(p) => (Some(*p), 0),
        sql::ExactNumberInfo::PrecisionAndScale(p, s) => (Some(*p), *s),
    };
    if let Some(p) = precision {
        if !(1..=28).contains(&p) {
            return unsupported("numeric precision must be between 1 and 28");
        }
        if scale > p {
            return Err(parse_error(format!(
                "numeric scale {scale} must not exceed precision {p}"
            )));
        }
    }
    Ok((precision.map(|p| p as u32), scale as u32))
}

/// Extract the declared maximum length (in characters) of a bounded character
/// type (`VARCHAR(n)` / `CHAR(n)` / `CHARACTER(n)`). Returns `None` for
/// unbounded character types and all non-character types. `VARCHAR(MAX)`,
/// octet-unit lengths, and a zero length are rejected.
fn column_char_length(data_type: &sql::DataType) -> Result<Option<u32>> {
    let length = match data_type {
        sql::DataType::Varchar(length)
        | sql::DataType::Char(length)
        | sql::DataType::Character(length) => length,
        _ => return Ok(None),
    };
    match length {
        None => Ok(None),
        Some(sql::CharacterLength::Max) => unsupported("VARCHAR(MAX) is not supported"),
        Some(sql::CharacterLength::IntegerLength { length, unit }) => {
            if matches!(unit, Some(sql::CharLengthUnits::Octets)) {
                return unsupported("character length in octets is not supported");
            }
            if *length == 0 {
                return Err(parse_error(
                    "length for a character type must be at least 1",
                ));
            }
            let length = u32::try_from(*length)
                .map_err(|_| parse_error("character type length is too large"))?;
            Ok(Some(length))
        }
    }
}

fn object_name(name: &sql::ObjectName) -> Result<String> {
    // Paths that still call this helper accept only a single unqualified name.
    // Relation and function names have their own helpers because they support
    // limited schema-qualified compatibility forms.
    let [part] = name.0.as_slice() else {
        return unsupported("qualified names are not supported in v1");
    };
    let ident = part
        .as_ident()
        .ok_or_else(|| parse_error("unsupported object name part"))?;
    ident_name(ident)
}

fn reject_duplicate_relation_names(names: &[String], operation: &str) -> Result<()> {
    let mut seen = HashSet::with_capacity(names.len());
    for name in names {
        if !seen.insert(name) {
            return Err(parse_error(format!(
                "{operation} target {name} specified more than once"
            )));
        }
    }
    Ok(())
}

fn function_name(name: &sql::ObjectName) -> Result<String> {
    match name.0.as_slice() {
        [name] => {
            let name = name
                .as_ident()
                .ok_or_else(|| parse_error("unsupported function name part"))?;
            ident_name(name)
        }
        [schema, name] => {
            let schema = schema
                .as_ident()
                .ok_or_else(|| parse_error("unsupported function schema part"))?;
            let name = name
                .as_ident()
                .ok_or_else(|| parse_error("unsupported function name part"))?;
            let schema = ident_name(schema)?;
            if schema != "pg_catalog" {
                return feature_not_supported(
                    "qualified function names are supported only in pg_catalog",
                );
            }
            ident_name(name)
        }
        _ => feature_not_supported(
            "qualified function names with more than one schema are not supported",
        ),
    }
}

fn relation_name(name: &sql::ObjectName) -> Result<(Option<String>, String)> {
    match name.0.as_slice() {
        [name] => {
            let name = name
                .as_ident()
                .ok_or_else(|| parse_error("unsupported relation name part"))?;
            Ok((None, ident_name(name)?))
        }
        [schema, name] => {
            let schema = schema
                .as_ident()
                .ok_or_else(|| parse_error("unsupported relation schema part"))?;
            let name = name
                .as_ident()
                .ok_or_else(|| parse_error("unsupported relation name part"))?;
            Ok((Some(ident_name(schema)?), ident_name(name)?))
        }
        _ => unsupported("qualified names with more than one schema are not supported"),
    }
}

fn dml_target_name(name: &sql::ObjectName) -> Result<String> {
    let (schema, name) = relation_name(name)?;
    fold_dml_target_name(schema.as_deref(), name)
}

fn fold_dml_target_name(schema: Option<&str>, name: String) -> Result<String> {
    match schema {
        None | Some("public") => Ok(name),
        Some("pg_catalog" | "information_schema") => {
            feature_not_supported("system catalogs are read-only")
        }
        Some(schema) => Err(invalid_schema_name(format!(
            "schema \"{schema}\" does not exist"
        ))),
    }
}

fn ident_name(ident: &sql::Ident) -> Result<String> {
    if ident.quote_style.is_some() {
        return Err(parse_error("quoted identifiers are not supported"));
    }
    Ok(ident.value.to_ascii_lowercase())
}

fn try_parse_fetch_cursor(sql: &str) -> Result<Option<Statement>> {
    let dialect = PostgreSqlDialect {};
    let mut tokens: Vec<_> = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|err| parse_error(format!("failed to parse SQL: {err}")))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();

    let Some(Token::Word(keyword)) = tokens.first() else {
        return Ok(None);
    };
    if keyword.quote_style.is_some() || !keyword.value.eq_ignore_ascii_case("fetch") {
        return Ok(None);
    }

    if tokens.last() == Some(&Token::SemiColon) {
        tokens.pop();
    }
    if tokens
        .iter()
        .skip(1)
        .any(|token| *token == Token::SemiColon)
    {
        return Err(parse_error("expected exactly one SQL statement"));
    }

    let mut parser = FetchCursorParser { tokens, index: 1 };
    let statement = parser.parse()?;
    if !parser.is_at_end() {
        return Err(parse_error("unexpected trailing tokens after FETCH"));
    }
    Ok(Some(statement))
}

struct FetchCursorParser {
    tokens: Vec<Token>,
    index: usize,
}

impl FetchCursorParser {
    fn parse(&mut self) -> Result<Statement> {
        let count = if self.consume_word("from") {
            FetchCount::One
        } else if self.consume_word("forward") {
            if self.consume_word("from") {
                FetchCount::One
            } else {
                let count = FetchCount::Count(self.parse_count()?);
                self.expect_word("from")?;
                count
            }
        } else if self.consume_word("all") {
            self.expect_word("from")?;
            FetchCount::All
        } else if self.current_is_count() {
            let count = FetchCount::Count(self.parse_count()?);
            self.expect_word("from")?;
            count
        } else if self.current_is_unsupported_direction() {
            return unsupported("unsupported FETCH direction");
        } else {
            FetchCount::One
        };
        let name = self.parse_identifier()?;
        Ok(Statement::FetchCursor { name, count })
    }

    fn is_at_end(&self) -> bool {
        self.index >= self.tokens.len()
    }

    fn consume_word(&mut self, expected: &str) -> bool {
        if matches_word(self.tokens.get(self.index), expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, expected: &str) -> Result<()> {
        if self.consume_word(expected) {
            Ok(())
        } else {
            Err(parse_error(format!("expected {expected}")))
        }
    }

    fn current_is_count(&self) -> bool {
        matches!(self.tokens.get(self.index), Some(Token::Number(_, _)))
            || matches!(self.tokens.get(self.index), Some(Token::Minus))
    }

    fn current_is_unsupported_direction(&self) -> bool {
        [
            "absolute", "backward", "first", "last", "next", "prior", "relative",
        ]
        .iter()
        .any(|direction| matches_word(self.tokens.get(self.index), direction))
    }

    fn parse_count(&mut self) -> Result<u64> {
        if matches!(self.tokens.get(self.index), Some(Token::Minus)) {
            return unsupported("negative FETCH counts are not supported");
        }
        let Some(Token::Number(text, long)) = self.tokens.get(self.index) else {
            return Err(parse_error("expected FETCH count"));
        };
        if *long {
            return Err(parse_error("FETCH count must be an unsigned integer"));
        }
        self.index += 1;
        text.parse::<u64>()
            .map_err(|_| parse_error("FETCH count must be an unsigned integer"))
    }

    fn parse_identifier(&mut self) -> Result<String> {
        match self.tokens.get(self.index) {
            Some(Token::Word(word)) if word.quote_style.is_none() => {
                self.index += 1;
                Ok(word.value.to_ascii_lowercase())
            }
            Some(Token::Word(_)) => Err(parse_error("quoted identifiers are not supported")),
            _ => Err(parse_error("expected cursor name")),
        }
    }
}

fn fetch_count_value(value: &sql::Value) -> Result<u64> {
    let sql::Value::Number(text, false) = value else {
        return Err(parse_error("FETCH count must be an unsigned integer"));
    };
    text.parse::<u64>()
        .map_err(|_| parse_error("FETCH count must be an unsigned integer"))
}

/// Intercept `RESET <name>` / `RESET ALL` before sqlparser, which cannot parse
/// RESET in 0.56. GUC names are the one identifier path where quoted names are
/// allowed; PostgreSQL treats them case-insensitively, so normalize to lowercase.
fn try_parse_reset(sql: &str) -> Result<Option<Statement>> {
    let dialect = PostgreSqlDialect {};
    let mut tokens: Vec<_> = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|err| parse_error(format!("failed to parse SQL: {err}")))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();

    let Some(Token::Word(keyword)) = tokens.first() else {
        return Ok(None);
    };
    if !keyword.value.eq_ignore_ascii_case("reset") {
        return Ok(None);
    }

    if tokens.last() == Some(&Token::SemiColon) {
        tokens.pop();
    }
    if tokens
        .iter()
        .skip(1)
        .any(|token| *token == Token::SemiColon)
    {
        return Err(parse_error("expected exactly one SQL statement"));
    }

    let mut parts = Vec::new();
    let mut index = 1;
    while index < tokens.len() {
        match &tokens[index] {
            Token::Word(word) => {
                let ident = match word.quote_style {
                    Some(quote) => sql::Ident::with_quote(quote, word.value.clone()),
                    None => sql::Ident::new(word.value.clone()),
                };
                parts.push(ident);
                index += 1;
            }
            Token::DoubleQuotedString(value) => {
                parts.push(sql::Ident::with_quote('"', value.clone()));
                index += 1;
            }
            Token::Period if index > 1 && index + 1 < tokens.len() => {
                index += 1;
            }
            _ => return unsupported("RESET supports a configuration parameter name or ALL"),
        }
    }

    match parts.as_slice() {
        [] => Err(parse_error("RESET requires a parameter name or ALL")),
        [single] if single.value.eq_ignore_ascii_case("all") => {
            Ok(Some(Statement::ResetVariable { name: None }))
        }
        _ => Ok(Some(Statement::ResetVariable {
            name: Some(guc_name_parts(parts.iter())?),
        })),
    }
}

fn reject_set_global(sql: &str) -> Result<()> {
    let dialect = PostgreSqlDialect {};
    let tokens: Vec<_> = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|err| parse_error(format!("failed to parse SQL: {err}")))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();

    let [Token::Word(set), Token::Word(global), ..] = tokens.as_slice() else {
        return Ok(());
    };
    if set.quote_style.is_none()
        && global.quote_style.is_none()
        && set.value.eq_ignore_ascii_case("set")
        && global.value.eq_ignore_ascii_case("global")
    {
        return unsupported("SET GLOBAL is not supported");
    }
    Ok(())
}

/// Intercept `VACUUM` (and `VACUUM <table>`) before sqlparser, which cannot parse
/// it. Returns `Ok(Some(_))` for a VACUUM statement, `Ok(None)` when the input does
/// not start with the `vacuum` keyword (so the normal parse path runs), and `Err`
/// for a VACUUM with an unsupported clause (parenthesized options, multiple tables,
/// a qualified/quoted name). The optional target identifier is lowercase-normalized,
/// matching the v1 unquoted-identifier rule (`ident_name`).
fn try_parse_vacuum(sql: &str) -> Result<Option<Statement>> {
    // Strip a single trailing semicolon and surrounding whitespace; the rest is the
    // VACUUM body. `VACUUM`, `VACUUM;`, `VACUUM t`, and `VACUUM t ;` are all accepted.
    let trimmed = sql.trim();
    let body = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();

    // Split off the leading keyword; bail to the normal path if it is not `vacuum`
    // (case-insensitive). The first token must be exactly `vacuum` — `vacuumfoo` is
    // some other word, not a VACUUM with a glued argument.
    let mut tokens = body.split_whitespace();
    let Some(keyword) = tokens.next() else {
        return Ok(None);
    };
    if !keyword.eq_ignore_ascii_case("vacuum") {
        return Ok(None);
    }

    // At most one argument: the target table. Any further token (a second table, a
    // `(`-options list, …) is an unsupported VACUUM form.
    let table = match tokens.next() {
        None => None,
        Some(target) => {
            if tokens.next().is_some() {
                return unsupported("VACUUM supports an optional single table name only in v1");
            }
            // The single argument is the target table. Reject Postgres VACUUM option
            // keywords (FULL/FREEZE/ANALYZE/VERBOSE/…) so `VACUUM FULL` is a clear
            // unsupported-option error rather than silently meaning a table named
            // `full`; v1 supports none of these options.
            if is_vacuum_option_keyword(target) {
                return unsupported("VACUUM options are not supported in v1");
            }
            Some(normalize_vacuum_target(target)?)
        }
    };

    Ok(Some(Statement::Vacuum { table }))
}

/// Whether `token` is a Postgres VACUUM option keyword (none supported in v1). Used
/// to reject `VACUUM FULL`/`VACUUM ANALYZE`/… with an explicit unsupported-option
/// error instead of treating the keyword as a table name.
fn is_vacuum_option_keyword(token: &str) -> bool {
    const OPTIONS: [&str; 6] = [
        "full",
        "freeze",
        "analyze",
        "verbose",
        "disable_page_skipping",
        "skip_locked",
    ];
    OPTIONS.iter().any(|opt| token.eq_ignore_ascii_case(opt))
}

/// Validate and lowercase-normalize a `VACUUM <table>` target. The name must be a
/// bare unquoted identifier: no parenthesized options, no `schema.table`
/// qualification, no quoting — consistent with the v1 identifier rules elsewhere.
fn normalize_vacuum_target(target: &str) -> Result<String> {
    if target.starts_with('(') {
        return unsupported("VACUUM with options is not supported in v1");
    }
    if target.contains('.') {
        return unsupported("qualified names are not supported in v1");
    }
    if target.contains('"') {
        return Err(parse_error("quoted identifiers are not supported"));
    }
    let valid = !target.is_empty()
        && target
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    if !valid {
        return unsupported("VACUUM target must be a simple table name in v1");
    }
    Ok(target.to_ascii_lowercase())
}

/// Intercept only the storage-parameter `ALTER TABLE ... SET (...)` forms that
/// sqlparser does not parse consistently, plus primary-key ALTER forms currently
/// handled by SaguaroDB's narrower grammar. Other ALTER TABLE statements fall
/// through to sqlparser so schema-evolution DDL can use its AST.
fn try_parse_alter_table(sql: &str) -> Result<Option<Statement>> {
    let trimmed = sql.trim();
    let Some(first) = trimmed.split_whitespace().next() else {
        return Ok(None);
    };
    if !first.eq_ignore_ascii_case("alter") {
        return Ok(None);
    }

    // Tokenize with sqlparser's tokenizer, dropping whitespace (the CREATE
    // SEQUENCE pattern below).
    let dialect = PostgreSqlDialect {};
    let tokens: Vec<Token> = Tokenizer::new(&dialect, trimmed)
        .tokenize()
        .map_err(|err| parse_error(format!("tokenize error: {err}")))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();

    let mut i = 0;
    if !expect_word(&tokens, &mut i, "alter") {
        return Ok(None);
    }
    if !expect_word(&tokens, &mut i, "table") {
        return Ok(None);
    }
    let _only = expect_word(&tokens, &mut i, "only");
    // Table identifier: unquoted word, lowercased (the `ident_name` rule).
    let table = parse_alter_identifier(&tokens, &mut i, "expected table name after ALTER TABLE")?;
    if !expect_word(&tokens, &mut i, "set") {
        if expect_word(&tokens, &mut i, "add") {
            return try_parse_alter_table_add_primary_key(&tokens, i, table);
        }
        if expect_word(&tokens, &mut i, "drop") {
            return try_parse_alter_table_drop_primary_key(&tokens, i, table);
        }
        return Ok(None);
    }
    if !matches!(tokens.get(i), Some(Token::LParen)) {
        return Ok(None);
    }
    i += 1;
    let options = parse_alter_table_options(&tokens, &mut i)?;
    if !matches!(tokens.get(i), Some(Token::RParen)) {
        return Err(parse_error("expected ) to close the option list"));
    }
    i += 1;
    if matches!(tokens.get(i), Some(Token::SemiColon)) {
        i += 1;
    }
    if i != tokens.len() {
        return Err(parse_error("unexpected trailing input after ALTER TABLE"));
    }
    if !options.toast.is_empty() {
        return Ok(Some(Statement::AlterTableSetOptions { table, options }));
    }
    if let Some(compression) = options.compression {
        return Ok(Some(Statement::AlterTableSetCompression {
            table,
            compression,
        }));
    }
    Err(parse_error("ALTER TABLE SET requires at least one option"))
}

fn try_parse_alter_table_add_primary_key(
    tokens: &[Token],
    i: usize,
    table: String,
) -> Result<Option<Statement>> {
    let mut lookahead = i;
    if expect_word(tokens, &mut lookahead, "constraint") {
        let Some(Token::Word(word)) = tokens.get(lookahead) else {
            return Ok(None);
        };
        if word.quote_style.is_some() {
            return Ok(None);
        }
        lookahead += 1;
    }
    if !matches!(tokens.get(lookahead), Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("primary"))
    {
        return Ok(None);
    }
    parse_alter_table_add_primary_key(tokens, i, table)
}

fn parse_alter_table_add_primary_key(
    tokens: &[Token],
    mut i: usize,
    table: String,
) -> Result<Option<Statement>> {
    let constraint_name = if expect_word(tokens, &mut i, "constraint") {
        Some(parse_alter_identifier(
            tokens,
            &mut i,
            "expected constraint name after ALTER TABLE ... ADD CONSTRAINT",
        )?)
    } else {
        None
    };
    if !expect_word(tokens, &mut i, "primary") || !expect_word(tokens, &mut i, "key") {
        return Err(DbError::parse(
            SqlState::FeatureNotSupported,
            "only ALTER TABLE ... ADD PRIMARY KEY is supported",
        ));
    }
    if !matches!(tokens.get(i), Some(Token::LParen)) {
        return Err(parse_error(
            "expected ( after ALTER TABLE ... ADD PRIMARY KEY",
        ));
    }
    i += 1;
    let columns = parse_identifier_list(tokens, &mut i, "primary key")?;
    if !matches!(tokens.get(i), Some(Token::RParen)) {
        return Err(parse_error("expected ) after primary key column list"));
    }
    i += 1;
    finish_alter_statement(tokens, i)?;
    Ok(Some(Statement::AlterTableAddPrimaryKey {
        table,
        columns,
        constraint_name,
    }))
}

fn try_parse_alter_table_drop_primary_key(
    tokens: &[Token],
    i: usize,
    table: String,
) -> Result<Option<Statement>> {
    if matches!(tokens.get(i), Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("primary") || word.value.eq_ignore_ascii_case("constraint"))
    {
        return parse_alter_table_drop_primary_key(tokens, i, table);
    }
    Ok(None)
}

fn parse_alter_table_drop_primary_key(
    tokens: &[Token],
    mut i: usize,
    table: String,
) -> Result<Option<Statement>> {
    let constraint_name = if expect_word(tokens, &mut i, "primary") {
        if !expect_word(tokens, &mut i, "key") {
            return Err(parse_error(
                "expected KEY after ALTER TABLE ... DROP PRIMARY",
            ));
        }
        None
    } else if expect_word(tokens, &mut i, "constraint") {
        Some(parse_alter_identifier(
            tokens,
            &mut i,
            "expected constraint name after ALTER TABLE ... DROP CONSTRAINT",
        )?)
    } else {
        return Err(DbError::parse(
            SqlState::FeatureNotSupported,
            "only ALTER TABLE ... DROP PRIMARY KEY is supported",
        ));
    };
    finish_alter_statement(tokens, i)?;
    Ok(Some(Statement::AlterTableDropPrimaryKey {
        table,
        constraint_name,
    }))
}

fn parse_alter_identifier(tokens: &[Token], i: &mut usize, message: &str) -> Result<String> {
    let name = match tokens.get(*i) {
        Some(Token::Word(w)) if w.quote_style.is_none() => w.value.to_ascii_lowercase(),
        Some(Token::Word(_)) => return Err(parse_error("quoted identifiers are not supported")),
        _ => return Err(parse_error(message)),
    };
    *i += 1;
    Ok(name)
}

fn parse_identifier_list(tokens: &[Token], i: &mut usize, context: &str) -> Result<Vec<String>> {
    let mut columns = Vec::new();
    loop {
        columns.push(parse_alter_identifier(
            tokens,
            i,
            &format!("expected column name in {context} column list"),
        )?);
        if matches!(tokens.get(*i), Some(Token::Comma)) {
            *i += 1;
            continue;
        }
        break;
    }
    Ok(columns)
}

fn finish_alter_statement(tokens: &[Token], mut i: usize) -> Result<()> {
    if matches!(tokens.get(i), Some(Token::SemiColon)) {
        i += 1;
    }
    if i != tokens.len() {
        return Err(parse_error("unexpected trailing input after ALTER TABLE"));
    }
    Ok(())
}

fn parse_alter_table_options(tokens: &[Token], i: &mut usize) -> Result<TableOptionPatch> {
    let mut parsed = TableOptionPatch::default();
    loop {
        let name = parse_option_name(tokens, i)?;
        if !matches!(tokens.get(*i), Some(Token::Eq)) {
            return Err(parse_error(format!("expected = after {name}")));
        }
        *i += 1;
        match name.as_str() {
            "compression" => {
                if parsed.compression.is_some() {
                    return Err(parse_error("duplicate compression option"));
                }
                parsed.compression = Some(compression_from_str(&parse_enum_option_token(
                    tokens,
                    i,
                    "compression",
                )?)?);
            }
            "toast" => {
                if parsed.toast.mode.is_some() {
                    return Err(parse_error("duplicate toast option"));
                }
                parsed.toast.mode = Some(parse_toast_mode_token(tokens, i)?);
            }
            "toast_tuple_target" => {
                if parsed.toast.tuple_target.is_some() {
                    return Err(parse_error("duplicate toast_tuple_target option"));
                }
                let value = parse_u32_option_token(tokens, i, "toast_tuple_target")?;
                if !(ToastOptions::MIN_TOAST_TUPLE_TARGET..=ToastOptions::MAX_TOAST_TUPLE_TARGET)
                    .contains(&value)
                {
                    return Err(invalid_parameter_value(format!(
                        "toast_tuple_target must be between {} and {}",
                        ToastOptions::MIN_TOAST_TUPLE_TARGET,
                        ToastOptions::MAX_TOAST_TUPLE_TARGET
                    )));
                }
                parsed.toast.tuple_target = Some(value);
            }
            "toast_min_value_size" => {
                if parsed.toast.min_value_size.is_some() {
                    return Err(parse_error("duplicate toast_min_value_size option"));
                }
                let value = parse_u32_option_token(tokens, i, "toast_min_value_size")?;
                if value < ToastOptions::MIN_TOAST_MIN_VALUE_SIZE {
                    return Err(invalid_parameter_value(format!(
                        "toast_min_value_size must be at least {}",
                        ToastOptions::MIN_TOAST_MIN_VALUE_SIZE
                    )));
                }
                parsed.toast.min_value_size = Some(value);
            }
            "toast_compression" => {
                if parsed.toast.compression.is_some() {
                    return Err(parse_error("duplicate toast_compression option"));
                }
                parsed.toast.compression = Some(parse_toast_compression_token(tokens, i)?);
            }
            _ => return Err(parse_error(format!("unsupported storage option {name}"))),
        }
        if matches!(tokens.get(*i), Some(Token::Comma)) {
            *i += 1;
            continue;
        }
        break;
    }
    Ok(parsed)
}

fn parse_option_name(tokens: &[Token], i: &mut usize) -> Result<String> {
    let name = match tokens.get(*i) {
        Some(Token::Word(w)) if w.quote_style.is_none() => w.value.to_ascii_lowercase(),
        Some(Token::Word(_)) => return Err(parse_error("quoted identifiers are not supported")),
        _ => return Err(parse_error("expected storage option name")),
    };
    *i += 1;
    Ok(name)
}

fn parse_enum_option_token(tokens: &[Token], i: &mut usize, name: &str) -> Result<String> {
    let value = match tokens.get(*i) {
        Some(Token::SingleQuotedString(s)) => s.to_ascii_lowercase(),
        Some(Token::Word(w)) if w.quote_style.is_none() => w.value.to_ascii_lowercase(),
        _ => {
            return Err(parse_error(format!(
                "{name} value must be a string or identifier"
            )));
        }
    };
    *i += 1;
    Ok(value)
}

fn parse_toast_mode_token(tokens: &[Token], i: &mut usize) -> Result<ToastMode> {
    match parse_enum_option_token(tokens, i, "toast")?.as_str() {
        "off" => Ok(ToastMode::Off),
        "auto" => Ok(ToastMode::Auto),
        "aggressive" => Ok(ToastMode::Aggressive),
        other => feature_not_supported(format!("unsupported toast mode {other}")),
    }
}

fn parse_toast_compression_token(tokens: &[Token], i: &mut usize) -> Result<ToastCompression> {
    match parse_enum_option_token(tokens, i, "toast_compression")?.as_str() {
        "none" => Ok(ToastCompression::None),
        "zstd" => Ok(ToastCompression::Zstd),
        "zstd_dict" => Ok(ToastCompression::ZstdDict),
        other => feature_not_supported(format!("unsupported toast compression codec {other}")),
    }
}

fn parse_u32_option_token(tokens: &[Token], i: &mut usize, name: &str) -> Result<u32> {
    let negative = matches!(tokens.get(*i), Some(Token::Minus));
    if negative {
        *i += 1;
    }
    let Some(Token::Number(text, false)) = tokens.get(*i) else {
        return Err(parse_error(format!("{name} must be an integer literal")));
    };
    *i += 1;
    if negative {
        return Err(invalid_parameter_value(format!(
            "{name} must be a non-negative integer"
        )));
    }
    text.parse::<u32>()
        .map_err(|_| invalid_parameter_value(format!("{name} must be a non-negative integer")))
}

/// Consume `expected` at `tokens[*i]` (case-insensitive, unquoted word only),
/// advancing `*i` on a match. The free-function form (rather than a closure)
/// avoids overlapping borrows of `tokens`/`i` at call sites.
fn expect_word(tokens: &[Token], i: &mut usize, expected: &str) -> bool {
    if matches_word(tokens.get(*i), expected) {
        *i += 1;
        true
    } else {
        false
    }
}

fn try_parse_create_sequence(sql: &str) -> Result<Option<Statement>> {
    let dialect = PostgreSqlDialect {};
    let mut tokens: Vec<_> = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|err| parse_error(format!("failed to parse SQL: {err}")))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();

    if !matches_create_sequence_prefix(&tokens) {
        return Ok(None);
    }

    if tokens.last() == Some(&Token::SemiColon) {
        tokens.pop();
    }
    if tokens.iter().any(|token| matches!(token, Token::SemiColon)) {
        return Err(parse_error("expected exactly one SQL statement"));
    }

    let mut parser = SequenceParser { tokens, index: 0 };
    parser.expect_word("create")?;
    if parser.consume_word("temporary") || parser.consume_word("temp") {
        return unsupported("unsupported CREATE SEQUENCE form");
    }
    parser.expect_word("sequence")?;
    if parser.consume_word("if") {
        parser.expect_word("not")?;
        parser.expect_word("exists")?;
        return unsupported("unsupported CREATE SEQUENCE form");
    }

    let name = parser.parse_identifier()?;
    if parser.consume_token(&Token::Period) {
        return unsupported("qualified names are not supported in v1");
    }

    let mut options = SequenceOptions::default();
    let mut increment_seen = false;
    let mut min_seen = false;
    let mut max_seen = false;
    let mut start_seen = false;
    let mut cache_seen = false;
    let mut cycle_seen = false;

    while !parser.is_at_end() {
        if parser.consume_word("increment") {
            reject_duplicate_sequence_option(&mut increment_seen, "INCREMENT")?;
            parser.consume_word("by");
            options.increment = parser.parse_i64()?;
        } else if parser.consume_word("start") {
            reject_duplicate_sequence_option(&mut start_seen, "START")?;
            parser.consume_word("with");
            options.start = Some(parser.parse_i64()?);
        } else if parser.consume_word("minvalue") {
            reject_duplicate_sequence_option(&mut min_seen, "MINVALUE")?;
            options.min_value = Some(parser.parse_i64()?);
        } else if parser.consume_word("maxvalue") {
            reject_duplicate_sequence_option(&mut max_seen, "MAXVALUE")?;
            options.max_value = Some(parser.parse_i64()?);
        } else if parser.consume_word("cache") {
            reject_duplicate_sequence_option(&mut cache_seen, "CACHE")?;
            let cache = parser.parse_i64()?;
            if cache <= 0 {
                return unsupported("CACHE must be greater than zero");
            }
        } else if parser.consume_word("cycle") {
            reject_duplicate_sequence_option(&mut cycle_seen, "CYCLE")?;
            options.cycle = true;
        } else if parser.consume_word("no") {
            if parser.consume_word("minvalue") {
                reject_duplicate_sequence_option(&mut min_seen, "MINVALUE")?;
                options.min_value = None;
            } else if parser.consume_word("maxvalue") {
                reject_duplicate_sequence_option(&mut max_seen, "MAXVALUE")?;
                options.max_value = None;
            } else if parser.consume_word("cycle") {
                reject_duplicate_sequence_option(&mut cycle_seen, "CYCLE")?;
                options.cycle = false;
            } else {
                return unsupported("unsupported CREATE SEQUENCE form");
            }
        } else {
            return unsupported("unsupported CREATE SEQUENCE form");
        }
    }

    Ok(Some(Statement::CreateSequence { name, options }))
}

fn matches_create_sequence_prefix(tokens: &[Token]) -> bool {
    let mut index = 0;
    if !matches_word(tokens.get(index), "create") {
        return false;
    }
    index += 1;
    if matches_word(tokens.get(index), "temporary") || matches_word(tokens.get(index), "temp") {
        index += 1;
    }
    matches_word(tokens.get(index), "sequence")
}

fn reject_duplicate_sequence_option(seen: &mut bool, option: &str) -> Result<()> {
    if *seen {
        return unsupported(format!("duplicate CREATE SEQUENCE {option} option"));
    }
    *seen = true;
    Ok(())
}

struct SequenceParser {
    tokens: Vec<Token>,
    index: usize,
}

impl SequenceParser {
    fn is_at_end(&self) -> bool {
        self.index >= self.tokens.len()
    }

    fn consume_word(&mut self, expected: &str) -> bool {
        if matches_word(self.tokens.get(self.index), expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, expected: &str) -> Result<()> {
        if self.consume_word(expected) {
            Ok(())
        } else {
            Err(parse_error(format!("expected {expected}")))
        }
    }

    fn consume_token(&mut self, expected: &Token) -> bool {
        if self.tokens.get(self.index) == Some(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn parse_identifier(&mut self) -> Result<String> {
        match self.tokens.get(self.index) {
            Some(Token::Word(word)) if word.quote_style.is_none() => {
                self.index += 1;
                Ok(word.value.to_ascii_lowercase())
            }
            Some(Token::Word(_)) => Err(parse_error("quoted identifiers are not supported")),
            _ => Err(parse_error("expected sequence name")),
        }
    }

    fn parse_i64(&mut self) -> Result<i64> {
        let negative = self.consume_token(&Token::Minus);
        let Some(Token::Number(text, _)) = self.tokens.get(self.index) else {
            return unsupported("sequence option must be an integer literal");
        };
        self.index += 1;
        let magnitude = text
            .parse::<i128>()
            .map_err(|_| parse_error("sequence option is out of range"))?;
        let signed = if negative { -magnitude } else { magnitude };
        i64::try_from(signed).map_err(|_| {
            DbError::parse(
                SqlState::NumericValueOutOfRange,
                "sequence option is out of range",
            )
        })
    }
}

fn matches_word(token: Option<&Token>, expected: &str) -> bool {
    matches!(
        token,
        Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)
    )
}

fn parse_error(message: impl Into<String>) -> DbError {
    DbError::parse(SqlState::SyntaxError, message)
}

fn invalid_schema_name(message: impl Into<String>) -> DbError {
    DbError::parse(SqlState::InvalidSchemaName, message)
}

fn invalid_parameter_value(message: impl Into<String>) -> DbError {
    DbError::parse(SqlState::InvalidParameterValue, message)
}

fn unsupported<T>(message: impl Into<String>) -> Result<T> {
    Err(parse_error(message))
}

/// A syntactically valid but intentionally unsupported form (e.g. server-side
/// file COPY, binary format) → SQLSTATE `0A000` rather than a syntax error.
fn feature_not_supported<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::parse(SqlState::FeatureNotSupported, message))
}

/// Parse an already-lowercased compression codec string into a
/// `CompressionSetting`. The single accepted-codec list, shared by `CREATE
/// TABLE ... WITH (compression = ...)` (`ddl::parse_compression_value`) and
/// `ALTER TABLE ... SET (compression = ...)` (`try_parse_alter_table`), so
/// both sites reject an unsupported codec with the identical
/// `FeatureNotSupported` SQLSTATE and message.
pub(crate) fn compression_from_str(text: &str) -> Result<CompressionSetting> {
    match text {
        "none" => Ok(CompressionSetting::None),
        "zstd" => Ok(CompressionSetting::Zstd),
        other => feature_not_supported(format!("unsupported compression codec {other}")),
    }
}
