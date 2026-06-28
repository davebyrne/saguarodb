use common::{CopyDirection, CopyFormat, CopyOptions, Result};
use sqlparser::ast as sql;

use crate::{InsertSource, Statement};

use super::expr::convert_expr;
use super::query::{
    convert_query_to_select, convert_returning, query_has_modifiers,
    table_name_from_table_with_joins,
};
use super::{feature_not_supported, ident_name, object_name, parse_error, unsupported};

pub(super) fn convert_insert(insert: sql::Insert) -> Result<Statement> {
    let sql::Insert {
        table,
        table_alias,
        columns,
        source,
        or,
        ignore,
        overwrite,
        assignments,
        partitioned,
        after_columns,
        has_table_keyword,
        on,
        returning,
        replace_into,
        priority,
        insert_alias,
        settings,
        format_clause,
        ..
    } = insert;

    if table_alias.is_some()
        || or.is_some()
        || ignore
        || overwrite
        || !assignments.is_empty()
        || partitioned.is_some()
        || !after_columns.is_empty()
        || has_table_keyword
        || on.is_some()
        || replace_into
        || priority.is_some()
        || insert_alias.is_some()
        || settings.is_some()
        || format_clause.is_some()
    {
        return unsupported("unsupported INSERT form");
    }

    let sql::TableObject::TableName(table) = table else {
        return unsupported("unsupported INSERT target");
    };
    let source = source.ok_or_else(|| parse_error("INSERT requires a source"))?;
    let source = if let sql::SetExpr::Values(values) = source.body.as_ref() {
        if query_has_modifiers(&source) {
            return unsupported("unsupported INSERT VALUES source modifiers");
        }
        InsertSource::Values(
            values
                .rows
                .iter()
                .map(|row| row.iter().map(convert_expr).collect::<Result<Vec<_>>>())
                .collect::<Result<Vec<_>>>()?,
        )
    } else if matches!(source.body.as_ref(), sql::SetExpr::Select(_)) {
        InsertSource::Query(Box::new(convert_query_to_select(*source)?))
    } else {
        return unsupported("unsupported INSERT source");
    };

    Ok(Statement::Insert {
        table: object_name(&table)?,
        columns: columns.iter().map(ident_name).collect::<Result<Vec<_>>>()?,
        source,
        returning: convert_returning(&returning)?,
    })
}

pub(super) fn convert_copy(
    source: sql::CopySource,
    to: bool,
    target: sql::CopyTarget,
    options: Vec<sql::CopyOption>,
    legacy_options: Vec<sql::CopyLegacyOption>,
    values: Vec<Option<String>>,
) -> Result<Statement> {
    // Inline COPY data (the `\.`-terminated block some parse paths attach to the
    // statement) is not part of the STDIN wire streaming we support.
    if !values.is_empty() {
        return unsupported("inline COPY data is not supported; use FROM STDIN");
    }

    // The table and column list always come from `source` (the data source for
    // COPY TO, the target table for COPY FROM); `COPY (query) TO` is rejected.
    let (table_name, columns) = match source {
        sql::CopySource::Table {
            table_name,
            columns,
        } => (table_name, columns),
        sql::CopySource::Query(_) => {
            return feature_not_supported("COPY (query) TO STDOUT is not supported");
        }
    };
    let table = object_name(&table_name)?;
    let columns = columns.iter().map(ident_name).collect::<Result<Vec<_>>>()?;

    let direction = if to {
        CopyDirection::To
    } else {
        CopyDirection::From
    };

    // Only STDIN (FROM) and STDOUT (TO) are supported; server-side files and
    // PROGRAM expose the server host and are rejected.
    match (direction, &target) {
        (CopyDirection::To, sql::CopyTarget::Stdout)
        | (CopyDirection::From, sql::CopyTarget::Stdin) => {}
        (_, sql::CopyTarget::File { .. } | sql::CopyTarget::Program { .. }) => {
            return feature_not_supported(
                "server-side file COPY is not supported; use COPY ... FROM STDIN / TO STDOUT",
            );
        }
        (CopyDirection::To, _) => return unsupported("COPY ... TO requires STDOUT"),
        (CopyDirection::From, _) => return unsupported("COPY ... FROM requires STDIN"),
    }

    let options = convert_copy_options(options, legacy_options)?;
    Ok(Statement::Copy {
        table,
        columns,
        direction,
        options,
    })
}

/// Normalize the modern (`WITH (FORMAT csv, ...)`) and legacy (`WITH CSV ...`)
/// option syntaxes into a single resolved `CopyOptions`, rejecting unsupported
/// options. The format is resolved first so per-format defaults and the
/// CSV-only checks for `QUOTE`/`ESCAPE` apply correctly.
fn convert_copy_options(
    options: Vec<sql::CopyOption>,
    legacy_options: Vec<sql::CopyLegacyOption>,
) -> Result<CopyOptions> {
    let format = copy_format(&options, &legacy_options)?;
    let mut resolved = CopyOptions::defaults_for(format);
    let mut escape_set = false;

    for option in options {
        apply_copy_option(&mut resolved, &mut escape_set, option)?;
    }
    for option in legacy_options {
        apply_legacy_copy_option(&mut resolved, &mut escape_set, option)?;
    }

    // PostgreSQL: ESCAPE defaults to the (possibly customized) QUOTE value.
    if !escape_set {
        resolved.escape = resolved.quote;
    }

    validate_copy_options(&resolved)?;
    Ok(resolved)
}

fn copy_format(
    options: &[sql::CopyOption],
    legacy_options: &[sql::CopyLegacyOption],
) -> Result<CopyFormat> {
    for option in options {
        if let sql::CopyOption::Format(ident) = option {
            return match ident.value.to_ascii_lowercase().as_str() {
                "text" => Ok(CopyFormat::Text),
                "csv" => Ok(CopyFormat::Csv),
                "binary" => feature_not_supported("COPY FORMAT binary is not supported"),
                other => unsupported(format!("unrecognized COPY format \"{other}\"")),
            };
        }
    }
    for option in legacy_options {
        match option {
            sql::CopyLegacyOption::Binary => {
                return feature_not_supported("COPY BINARY is not supported");
            }
            sql::CopyLegacyOption::Csv(_) => return Ok(CopyFormat::Csv),
            _ => {}
        }
    }
    Ok(CopyFormat::Text)
}

fn apply_copy_option(
    resolved: &mut CopyOptions,
    escape_set: &mut bool,
    option: sql::CopyOption,
) -> Result<()> {
    match option {
        // Format is resolved up front by `copy_format`.
        sql::CopyOption::Format(_) => {}
        sql::CopyOption::Delimiter(delimiter) => resolved.delimiter = delimiter,
        sql::CopyOption::Null(null_string) => resolved.null_string = null_string,
        sql::CopyOption::Header(header) => resolved.header = header,
        sql::CopyOption::Quote(quote) => {
            require_csv(resolved, "QUOTE")?;
            resolved.quote = quote;
        }
        sql::CopyOption::Escape(escape) => {
            require_csv(resolved, "ESCAPE")?;
            resolved.escape = escape;
            *escape_set = true;
        }
        sql::CopyOption::Freeze(_) => return feature_not_supported("COPY FREEZE is not supported"),
        sql::CopyOption::ForceQuote(_) => {
            return feature_not_supported("COPY FORCE_QUOTE is not supported");
        }
        sql::CopyOption::ForceNotNull(_) => {
            return feature_not_supported("COPY FORCE_NOT_NULL is not supported");
        }
        sql::CopyOption::ForceNull(_) => {
            return feature_not_supported("COPY FORCE_NULL is not supported");
        }
        sql::CopyOption::Encoding(_) => {
            return feature_not_supported("COPY ENCODING is not supported");
        }
    }
    Ok(())
}

fn apply_legacy_copy_option(
    resolved: &mut CopyOptions,
    escape_set: &mut bool,
    option: sql::CopyLegacyOption,
) -> Result<()> {
    match option {
        // Already rejected by `copy_format`; keep the arm exhaustive/defensive.
        sql::CopyLegacyOption::Binary => {
            return feature_not_supported("COPY BINARY is not supported");
        }
        sql::CopyLegacyOption::Delimiter(delimiter) => resolved.delimiter = delimiter,
        sql::CopyLegacyOption::Null(null_string) => resolved.null_string = null_string,
        // `CSV (...)` already set the format; its sub-options are CSV-valid.
        sql::CopyLegacyOption::Csv(csv_options) => {
            for csv_option in csv_options {
                match csv_option {
                    sql::CopyLegacyCsvOption::Header => resolved.header = true,
                    sql::CopyLegacyCsvOption::Quote(quote) => resolved.quote = quote,
                    sql::CopyLegacyCsvOption::Escape(escape) => {
                        resolved.escape = escape;
                        *escape_set = true;
                    }
                    sql::CopyLegacyCsvOption::ForceQuote(_) => {
                        return feature_not_supported("COPY FORCE QUOTE is not supported");
                    }
                    sql::CopyLegacyCsvOption::ForceNotNull(_) => {
                        return feature_not_supported("COPY FORCE NOT NULL is not supported");
                    }
                }
            }
        }
    }
    Ok(())
}

fn require_csv(resolved: &CopyOptions, option: &str) -> Result<()> {
    if resolved.format != CopyFormat::Csv {
        return feature_not_supported(format!(
            "COPY option {option} is only valid with FORMAT csv"
        ));
    }
    Ok(())
}

fn validate_copy_options(options: &CopyOptions) -> Result<()> {
    let is_eol = |ch: char| ch == '\r' || ch == '\n';
    if is_eol(options.delimiter) {
        return unsupported("COPY DELIMITER may not be a carriage return or newline");
    }
    // In text format `\` introduces escapes, so a backslash delimiter would make
    // field parsing ambiguous; PostgreSQL rejects it in every format.
    if options.delimiter == '\\' {
        return unsupported("COPY DELIMITER may not be a backslash");
    }
    if options.format == CopyFormat::Csv {
        if is_eol(options.quote) {
            return unsupported("COPY QUOTE may not be a carriage return or newline");
        }
        if options.delimiter == options.quote {
            return unsupported("COPY DELIMITER and QUOTE must be different");
        }
    }
    Ok(())
}

pub(super) fn convert_delete(delete: sql::Delete) -> Result<Statement> {
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return unsupported("unsupported DELETE form");
    }

    let tables = match &delete.from {
        sql::FromTable::WithFromKeyword(tables) => tables,
        sql::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return unsupported("DELETE requires exactly one table");
    }

    Ok(Statement::Delete {
        table: table_name_from_table_with_joins(&tables[0])?,
        filter: delete
            .selection
            .map(|expr| convert_expr(&expr))
            .transpose()?,
        returning: convert_returning(&delete.returning)?,
    })
}
