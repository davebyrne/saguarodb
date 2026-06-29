use common::{DataType, DbError, IsolationLevel, Result, SqlState};
use sqlparser::ast as sql;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::Statement;

mod ddl;
mod dml;
mod expr;
mod query;

use ddl::{convert_create_index, convert_create_table};
use dml::{convert_copy, convert_delete, convert_insert};
use expr::convert_expr;
use query::{
    convert_assignment, convert_query_to_select, convert_returning,
    table_name_from_table_with_joins,
};

pub fn parse_statement(sql: &str) -> Result<Statement> {
    // sqlparser 0.56 errors on `VACUUM`, so intercept it before handing the string
    // to the parser (`docs/specs/crates/parser.md`). `VACUUM` is a maintenance
    // command, not a relational statement, and never reaches bind/plan.
    if let Some(statement) = try_parse_vacuum(sql)? {
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

fn convert_statement(statement: sql::Statement) -> Result<Statement> {
    match statement {
        sql::Statement::CreateTable(table) => convert_create_table(table),
        sql::Statement::CreateIndex(index) => convert_create_index(index),
        sql::Statement::Drop {
            object_type,
            if_exists,
            mut names,
            cascade,
            restrict,
            purge,
            temporary,
        } => {
            if if_exists || names.len() != 1 || cascade || restrict || purge || temporary {
                return unsupported("unsupported DROP form");
            }
            let name = object_name(&names.remove(0))?;
            match object_type {
                sql::ObjectType::Table => Ok(Statement::DropTable { name }),
                sql::ObjectType::Index => Ok(Statement::DropIndex { name }),
                _ => unsupported("unsupported DROP object type"),
            }
        }
        sql::Statement::Insert(insert) => convert_insert(insert),
        sql::Statement::Query(query) => Ok(Statement::Select(convert_query_to_select(*query)?)),
        sql::Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        } => {
            if from.is_some() || or.is_some() {
                return unsupported("unsupported UPDATE form");
            }

            let table = table_name_from_table_with_joins(&table)?;
            let assignments = assignments
                .into_iter()
                .map(convert_assignment)
                .collect::<Result<Vec<_>>>()?;
            let filter = selection.map(|expr| convert_expr(&expr)).transpose()?;

            Ok(Statement::Update {
                table,
                assignments,
                filter,
                returning: convert_returning(&returning)?,
            })
        }
        sql::Statement::Delete(delete) => convert_delete(delete),
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
                Statement::Select(select) => {
                    Ok(Statement::Explain(Box::new(Statement::Select(select))))
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

/// Convert a `SET ...` statement. v1 supports the transaction-scoped
/// `SET TRANSACTION ISOLATION LEVEL <level>` (`session == false`) and the
/// session-scoped `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL
/// <level>` (`session == true`, the per-connection default). Both share the same
/// mode parsing (so the four SQL levels map onto our two identically, and access
/// modes follow the same accept-`READ WRITE`/reject-`READ ONLY` convention);
/// `SET TRANSACTION SNAPSHOT` and every other `SET` form are unsupported.
fn convert_set(set: sql::Set) -> Result<Statement> {
    let sql::Set::SetTransaction {
        modes,
        snapshot,
        session,
    } = set
    else {
        return unsupported("unsupported SET statement");
    };
    if snapshot.is_some() {
        return unsupported("SET TRANSACTION SNAPSHOT is not supported in v1");
    }
    let isolation = transaction_isolation_mode(&modes)?;
    if session {
        // `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` sets
        // the per-connection default isolation for FUTURE transactions (G2). It
        // reuses the same level mapping and access-mode handling as the
        // transaction-scoped form above.
        Ok(Statement::SetSessionCharacteristics { isolation })
    } else {
        Ok(Statement::SetTransaction { isolation })
    }
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

fn convert_data_type(data_type: &sql::DataType) -> Result<DataType> {
    match data_type {
        // All integer widths are backed by a single 64-bit integer; SMALLINT/
        // BIGINT and the `intN` aliases are accepted but not range-enforced.
        sql::DataType::Integer(None)
        | sql::DataType::Int(None)
        | sql::DataType::Int2(None)
        | sql::DataType::SmallInt(None)
        | sql::DataType::Int4(None)
        | sql::DataType::Int8(None)
        | sql::DataType::BigInt(None) => Ok(DataType::Integer),
        // Character types all map to the single unbounded TEXT value; any
        // declared length is a column-level constraint, captured separately by
        // `column_char_length` (CAST targets ignore the length).
        sql::DataType::Text
        | sql::DataType::Varchar(_)
        | sql::DataType::Char(_)
        | sql::DataType::Character(_) => Ok(DataType::Text),
        sql::DataType::Boolean | sql::DataType::Bool => Ok(DataType::Boolean),
        sql::DataType::Date => Ok(DataType::Date),
        // TIMESTAMP without time zone and without a fractional-seconds precision.
        // WITH TIME ZONE and an explicit precision are not supported.
        sql::DataType::Timestamp(
            None,
            sql::TimezoneInfo::None | sql::TimezoneInfo::WithoutTimeZone,
        ) => Ok(DataType::Timestamp),
        // TIME without time zone, no fractional-seconds precision.
        sql::DataType::Time(None, sql::TimezoneInfo::None | sql::TimezoneInfo::WithoutTimeZone) => {
            Ok(DataType::Time)
        }
        sql::DataType::Bytea => Ok(DataType::Bytea),
        sql::DataType::Uuid => Ok(DataType::Uuid),
        // DOUBLE PRECISION and its aliases (`FLOAT8`, bare `FLOAT`).
        sql::DataType::DoublePrecision
        | sql::DataType::Float8
        | sql::DataType::Double(sql::ExactNumberInfo::None) => Ok(DataType::Double),
        // REAL / FLOAT4 (single precision).
        sql::DataType::Real | sql::DataType::Float4 => Ok(DataType::Real),
        // `FLOAT(p)`: PostgreSQL maps p in 1..=24 to REAL and 25..=53 to DOUBLE
        // PRECISION; bare `FLOAT` is DOUBLE PRECISION.
        sql::DataType::Float(precision) => match precision {
            None => Ok(DataType::Double),
            Some(p) if (1..=24).contains(p) => Ok(DataType::Real),
            Some(p) if (25..=53).contains(p) => Ok(DataType::Double),
            Some(_) => unsupported("float precision must be between 1 and 53"),
        },
        // NUMERIC / DECIMAL, optionally with (precision[, scale]).
        sql::DataType::Numeric(info) | sql::DataType::Decimal(info) => convert_numeric_typmod(info),
        _ => unsupported("unsupported data type"),
    }
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

/// Convert a NUMERIC/DECIMAL type modifier into `DataType::Numeric`.
fn convert_numeric_typmod(info: &sql::ExactNumberInfo) -> Result<DataType> {
    let (precision, scale) = numeric_typmod(info)?;
    Ok(DataType::Numeric { precision, scale })
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
    // V1 has no schemas, so table, function, and column names are a single
    // identifier. Reject `schema.table` (and longer) here with a clear error
    // rather than letting a dotted name fail later as an unknown table.
    let [part] = name.0.as_slice() else {
        return unsupported("qualified names are not supported in v1");
    };
    let ident = part
        .as_ident()
        .ok_or_else(|| parse_error("unsupported object name part"))?;
    ident_name(ident)
}

fn ident_name(ident: &sql::Ident) -> Result<String> {
    if ident.quote_style.is_some() {
        return Err(parse_error("quoted identifiers are not supported"));
    }
    Ok(ident.value.to_ascii_lowercase())
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

fn parse_error(message: impl Into<String>) -> DbError {
    DbError::parse(SqlState::SyntaxError, message)
}

fn unsupported<T>(message: impl Into<String>) -> Result<T> {
    Err(parse_error(message))
}

/// A syntactically valid but intentionally unsupported form (e.g. server-side
/// file COPY, binary format) → SQLSTATE `0A000` rather than a syntax error.
fn feature_not_supported<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::parse(SqlState::FeatureNotSupported, message))
}
