use common::{
    CompressionSetting, DbError, ParsedColumnDef, ParsedDefault, PgType, Result, SqlState,
    TableOptionPatch, ToastCompression, ToastMode, ToastOptions, Value,
};
use sqlparser::ast as sql;

use crate::{Expr, Statement, UnaryOp};

use super::{
    column_char_length, compression_from_str, convert_expr, convert_pg_type, feature_not_supported,
    ident_name, object_name, parse_error, serial_pg_type, unsupported,
};

pub(super) fn convert_create_index(index: sql::CreateIndex) -> Result<Statement> {
    let sql::CreateIndex {
        name,
        table_name,
        using,
        columns,
        unique,
        concurrently,
        if_not_exists,
        include,
        nulls_distinct,
        with,
        predicate,
    } = index;

    if using.is_some()
        || concurrently
        || if_not_exists
        || !include.is_empty()
        || nulls_distinct.is_some()
        || !with.is_empty()
        || predicate.is_some()
    {
        return unsupported("unsupported CREATE INDEX form");
    }

    let name = match name {
        Some(name) => object_name(&name)?,
        None => return unsupported("CREATE INDEX requires an index name in v1"),
    };
    let table = object_name(&table_name)?;
    let columns = columns
        .iter()
        .map(convert_index_column)
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() {
        return unsupported("CREATE INDEX requires at least one column");
    }

    Ok(Statement::CreateIndex {
        name,
        table,
        columns,
        unique,
    })
}

fn convert_index_column(column: &sql::IndexColumn) -> Result<String> {
    if column.operator_class.is_some() {
        return unsupported("unsupported index column operator class");
    }
    let sql::OrderByExpr {
        expr,
        options,
        with_fill,
    } = &column.column;
    if options.asc == Some(false) || options.nulls_first.is_some() || with_fill.is_some() {
        return unsupported("v1 index columns must be plain ascending columns");
    }
    match expr {
        sql::Expr::Identifier(ident) => ident_name(ident),
        _ => unsupported("index columns must be simple column names in v1"),
    }
}

pub(super) fn convert_create_table(table: sql::CreateTable) -> Result<Statement> {
    let sql::CreateTable {
        name,
        columns,
        constraints,
        hive_distribution,
        hive_formats,
        table_properties,
        with_options,
        file_format,
        location,
        or_replace,
        temporary,
        external,
        global,
        if_not_exists,
        transient,
        volatile,
        iceberg,
        query,
        without_rowid,
        like,
        clone,
        engine,
        comment,
        auto_increment_offset,
        default_charset,
        collation,
        on_commit,
        on_cluster,
        primary_key: clickhouse_primary_key,
        order_by,
        partition_by,
        cluster_by,
        clustered_by,
        options,
        inherits,
        strict,
        copy_grants,
        enable_schema_evolution,
        change_tracking,
        data_retention_time_in_days,
        max_data_extension_time_in_days,
        default_ddl_collation,
        with_aggregation_policy,
        with_row_access_policy,
        with_tags,
        external_volume,
        base_location,
        catalog,
        catalog_sync,
        storage_serialization_policy,
        ..
    } = table;

    if or_replace
        || temporary
        || external
        || global.is_some()
        || if_not_exists
        || transient
        || volatile
        || iceberg
        || !matches!(hive_distribution, sql::HiveDistributionStyle::NONE)
        || hive_formats.as_ref().is_some_and(hive_format_has_options)
        || !table_properties.is_empty()
        || file_format.is_some()
        || location.is_some()
        || query.is_some()
        || without_rowid
        || like.is_some()
        || clone.is_some()
        || engine.is_some()
        || comment.is_some()
        || auto_increment_offset.is_some()
        || default_charset.is_some()
        || collation.is_some()
        || on_commit.is_some()
        || on_cluster.is_some()
        || clickhouse_primary_key.is_some()
        || order_by.is_some()
        || partition_by.is_some()
        || cluster_by.is_some()
        || clustered_by.is_some()
        || options.as_ref().is_some_and(|options| !options.is_empty())
        || inherits.is_some()
        || strict
        || copy_grants
        || enable_schema_evolution.is_some()
        || change_tracking.is_some()
        || data_retention_time_in_days.is_some()
        || max_data_extension_time_in_days.is_some()
        || default_ddl_collation.is_some()
        || with_aggregation_policy.is_some()
        || with_row_access_policy.is_some()
        || with_tags.is_some()
        || external_volume.is_some()
        || base_location.is_some()
        || catalog.is_some()
        || catalog_sync.is_some()
        || storage_serialization_policy.is_some()
    {
        return unsupported("unsupported CREATE TABLE form");
    }

    let mut primary_key = Vec::new();
    let mut unique = Vec::new();
    let columns = columns
        .into_iter()
        .map(|column| convert_column_def(column, &mut primary_key, &mut unique))
        .collect::<Result<Vec<_>>>()?;

    for constraint in constraints {
        match constraint {
            sql::TableConstraint::PrimaryKey {
                name,
                index_name,
                index_type,
                columns,
                index_options,
                characteristics,
            } => {
                if name.is_some()
                    || index_name.is_some()
                    || index_type.is_some()
                    || !index_options.is_empty()
                    || characteristics.is_some()
                {
                    return unsupported("unsupported PRIMARY KEY constraint form");
                }
                set_primary_key(
                    &mut primary_key,
                    columns.iter().map(ident_name).collect::<Result<Vec<_>>>()?,
                )?;
            }
            sql::TableConstraint::Unique {
                name,
                index_name,
                index_type_display,
                index_type,
                columns,
                index_options,
                characteristics,
                nulls_distinct,
            } => {
                if name.is_some()
                    || index_name.is_some()
                    || !matches!(index_type_display, sql::KeyOrIndexDisplay::None)
                    || index_type.is_some()
                    || !index_options.is_empty()
                    || characteristics.is_some()
                    || !matches!(nulls_distinct, sql::NullsDistinctOption::None)
                {
                    return unsupported("unsupported UNIQUE constraint form");
                }
                let columns = columns.iter().map(ident_name).collect::<Result<Vec<_>>>()?;
                if columns.is_empty() {
                    return unsupported("UNIQUE constraint requires at least one column");
                }
                unique.push(columns);
            }
            _ => return unsupported("unsupported table constraint"),
        }
    }

    let options = convert_table_options(with_options)?;

    Ok(Statement::CreateTable {
        name: object_name(&name)?,
        columns,
        primary_key,
        unique,
        compression: options.compression,
        toast: options.toast,
    })
}

/// Parse `WITH (...)` storage options. Unknown keys are syntax errors; known
/// keys with unsupported enum values are 0A000; duplicates are syntax errors
/// (the CREATE SEQUENCE duplicate-option convention).
fn convert_table_options(options: Vec<sql::SqlOption>) -> Result<TableOptionPatch> {
    let mut parsed = TableOptionPatch::default();
    for option in options {
        let sql::SqlOption::KeyValue { key, value } = option else {
            return Err(parse_error("unsupported storage option form"));
        };
        let name = ident_name(&key)?;
        match name.as_str() {
            "compression" => {
                if parsed.compression.is_some() {
                    return Err(parse_error("duplicate compression option"));
                }
                parsed.compression = Some(parse_compression_value(&value)?);
            }
            "toast" => {
                if parsed.toast.mode.is_some() {
                    return Err(parse_error("duplicate toast option"));
                }
                parsed.toast.mode = Some(parse_toast_mode_value(&value)?);
            }
            "toast_tuple_target" => {
                if parsed.toast.tuple_target.is_some() {
                    return Err(parse_error("duplicate toast_tuple_target option"));
                }
                let value = parse_u32_storage_option("toast_tuple_target", &value)?;
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
                let value = parse_u32_storage_option("toast_min_value_size", &value)?;
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
                parsed.toast.compression = Some(parse_toast_compression_value(&value)?);
            }
            _ => return Err(parse_error(format!("unsupported storage option {name}"))),
        }
    }
    Ok(parsed)
}

/// Parse a `compression` option value: a single-quoted string or a bare
/// identifier, case-insensitively. `ident_name` is not reused here because it
/// also rejects quoted identifiers, which is not the concern for a WITH-clause
/// value.
fn parse_compression_value(value: &sql::Expr) -> Result<CompressionSetting> {
    let text = parse_enum_storage_option("compression", value)?;
    compression_from_str(&text)
}

fn parse_toast_mode_value(value: &sql::Expr) -> Result<ToastMode> {
    match parse_enum_storage_option("toast", value)?.as_str() {
        "off" => Ok(ToastMode::Off),
        "auto" => Ok(ToastMode::Auto),
        "aggressive" => Ok(ToastMode::Aggressive),
        other => feature_not_supported(format!("unsupported toast mode {other}")),
    }
}

fn parse_toast_compression_value(value: &sql::Expr) -> Result<ToastCompression> {
    match parse_enum_storage_option("toast_compression", value)?.as_str() {
        "none" => Ok(ToastCompression::None),
        "zstd" => Ok(ToastCompression::Zstd),
        "zstd_dict" => Ok(ToastCompression::ZstdDict),
        other => feature_not_supported(format!("unsupported toast compression codec {other}")),
    }
}

fn parse_enum_storage_option(name: &str, value: &sql::Expr) -> Result<String> {
    match value {
        sql::Expr::Value(v) => match &v.value {
            sql::Value::SingleQuotedString(s) => Ok(s.to_ascii_lowercase()),
            _ => Err(parse_error(format!(
                "{name} value must be a string or identifier"
            ))),
        },
        sql::Expr::Identifier(ident) => ident_name(ident),
        _ => Err(parse_error(format!(
            "{name} value must be a string or identifier"
        ))),
    }
}

fn parse_u32_storage_option(name: &str, value: &sql::Expr) -> Result<u32> {
    match value {
        sql::Expr::Value(v) => match &v.value {
            sql::Value::Number(text, false) => text.parse::<u32>().map_err(|_| {
                invalid_parameter_value(format!("{name} must be a non-negative integer"))
            }),
            _ => Err(parse_error(format!("{name} must be an integer literal"))),
        },
        sql::Expr::UnaryOp {
            op: sql::UnaryOperator::Minus,
            expr,
        } if matches!(
            expr.as_ref(),
            sql::Expr::Value(sql::ValueWithSpan {
                value: sql::Value::Number(_, false),
                ..
            })
        ) =>
        {
            Err(invalid_parameter_value(format!(
                "{name} must be a non-negative integer"
            )))
        }
        _ => Err(parse_error(format!("{name} must be an integer literal"))),
    }
}

fn invalid_parameter_value(message: impl Into<String>) -> DbError {
    DbError::parse(SqlState::InvalidParameterValue, message)
}

fn convert_column_def(
    column: sql::ColumnDef,
    primary_key: &mut Vec<String>,
    unique: &mut Vec<Vec<String>>,
) -> Result<ParsedColumnDef> {
    let mut nullable = true;
    let mut default = None;
    let serial = serial_pg_type(&column.data_type)?;

    for option in &column.options {
        if option.name.is_some() {
            return unsupported("unsupported named column constraint");
        }

        match &option.option {
            sql::ColumnOption::Null => nullable = true,
            sql::ColumnOption::NotNull => nullable = false,
            sql::ColumnOption::Default(expr) => {
                if serial.is_some() {
                    return unsupported("SERIAL columns cannot specify an explicit DEFAULT");
                }
                if default.is_some() {
                    return unsupported("column has more than one DEFAULT");
                }
                default = Some(convert_column_default(expr)?);
            }
            sql::ColumnOption::Unique {
                is_primary,
                characteristics,
            } => {
                if characteristics.is_some() {
                    return unsupported("unsupported UNIQUE/PRIMARY KEY constraint form");
                }
                let column_name = ident_name(&column.name)?;
                if *is_primary {
                    set_primary_key(primary_key, vec![column_name])?;
                    nullable = false;
                } else {
                    // Column-level UNIQUE becomes a single-column unique index.
                    unique.push(vec![column_name]);
                }
            }
            _ => return unsupported("unsupported column option"),
        }
    }

    let name = ident_name(&column.name)?;

    // A SERIAL-family column stores a 64-bit integer with a sequence default but
    // reports the wire width of its serial kind (serial => int4, etc.).
    if let Some(pg_type) = serial {
        return Ok(ParsedColumnDef {
            name,
            data_type: pg_type.data_type(),
            nullable: false,
            max_length: None,
            default: Some(ParsedDefault::Serial),
            pg_type: Some(pg_type),
        });
    }

    // Otherwise derive the storage type from the declared wire type, folding the
    // declared character length (if any) into `varchar(n)` / `char(n)`.
    let max_length = column_char_length(&column.data_type)?;
    let pg_type = match convert_pg_type(&column.data_type)? {
        PgType::Varchar(_) => PgType::Varchar(max_length),
        PgType::Bpchar(_) => PgType::Bpchar(max_length),
        other => other,
    };
    Ok(ParsedColumnDef {
        name,
        data_type: pg_type.data_type(),
        nullable,
        max_length,
        default,
        pg_type: Some(pg_type),
    })
}

/// Convert a column `DEFAULT` expression into the bounded parse-time default
/// representation. Most defaults are constants folded at parse time; the one
/// volatile form accepted here is `nextval('sequence')`, which the catalog later
/// resolves to a durable sequence id.
fn convert_column_default(expr: &sql::Expr) -> Result<ParsedDefault> {
    // Canonical SQL text, kept for the non-constant case so the binder can
    // re-parse and bind the expression at CREATE TABLE and each INSERT.
    let text = expr.to_string();
    match convert_expr(expr)? {
        Expr::Literal(value) => Ok(ParsedDefault::Const(value)),
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name.eq_ignore_ascii_case("nextval") => match args.as_slice() {
            [crate::FunctionArg::Expr(Expr::Literal(Value::Text(sequence)))] => {
                Ok(ParsedDefault::Nextval(sequence.clone()))
            }
            _ => unsupported("DEFAULT nextval requires one string literal argument"),
        },
        Expr::UnaryOp {
            op: UnaryOp::Neg,
            expr,
        } => match *expr {
            Expr::Literal(Value::Integer(value)) => {
                Ok(ParsedDefault::Const(Value::Integer(-value)))
            }
            Expr::Literal(Value::Float(value)) => {
                Ok(ParsedDefault::Const(Value::Float((-value.0).into())))
            }
            Expr::Literal(Value::Numeric(value)) => {
                Ok(ParsedDefault::Const(Value::Numeric(-value)))
            }
            // A negation of a non-constant operand is a general expression default.
            _ => Ok(ParsedDefault::Expr(text)),
        },
        // Any other expression (function calls, arithmetic, casts, ...) is carried
        // as text and bound later; column references in it fail to bind, so a
        // default cannot reference table columns.
        _ => Ok(ParsedDefault::Expr(text)),
    }
}

fn set_primary_key(primary_key: &mut Vec<String>, columns: Vec<String>) -> Result<()> {
    if !primary_key.is_empty() {
        return unsupported("multiple PRIMARY KEY constraints");
    }
    *primary_key = columns;
    Ok(())
}

fn hive_format_has_options(format: &sql::HiveFormat) -> bool {
    format.row_format.is_some()
        || format
            .serde_properties
            .as_ref()
            .is_some_and(|properties| !properties.is_empty())
        || format.storage.is_some()
        || format.location.is_some()
}
