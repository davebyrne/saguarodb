use common::{
    CompressionSetting, DbError, ForeignKeyAction, ParsedColumnDef, ParsedDefault, PgType, Result,
    SqlState, TableOptionPatch, ToastCompression, ToastMode, ToastOptions, Value,
};
use sqlparser::ast as sql;
use sqlparser::ast::Spanned;
use sqlparser::tokenizer::Location;

use crate::{Expr, ParsedForeignKey, Statement, UnaryOp};

use super::{
    column_char_length, compression_from_str, convert_expr, convert_pg_type, convert_query,
    feature_not_supported, ident_name, object_name, parse_error, reject_duplicate_relation_names,
    serial_pg_type, simple_object_name, unsupported,
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

pub(super) fn convert_alter_table(
    name: sql::ObjectName,
    if_exists: bool,
    only: bool,
    operations: Vec<sql::AlterTableOperation>,
    location: Option<sql::HiveSetLocation>,
    on_cluster: Option<sql::Ident>,
) -> Result<Statement> {
    if if_exists || location.is_some() || on_cluster.is_some() || operations.len() != 1 {
        return unsupported("unsupported ALTER TABLE form");
    }
    let table = object_name(&name)?;
    let operation = operations
        .into_iter()
        .next()
        .ok_or_else(|| DbError::internal("ALTER TABLE operation disappeared after validation"))?;
    match operation {
        sql::AlterTableOperation::AddColumn {
            if_not_exists,
            column_def,
            column_position,
            ..
        } => {
            if only {
                return unsupported("ALTER TABLE ONLY is only supported for ALTER COLUMN TYPE");
            }
            if column_position.is_some() {
                return unsupported("ALTER TABLE ADD COLUMN position is not supported");
            }
            let mut primary_key = Vec::new();
            let mut unique = Vec::new();
            let mut checks = Vec::new();
            let mut foreign_keys = Vec::new();
            let column = convert_column_def(
                column_def,
                &mut primary_key,
                &mut unique,
                &mut checks,
                &mut foreign_keys,
            )?;
            if matches!(column.default, Some(ParsedDefault::Serial)) {
                return unsupported("ALTER TABLE ADD COLUMN SERIAL is not supported");
            }
            if !primary_key.is_empty()
                || !unique.is_empty()
                || !checks.is_empty()
                || !foreign_keys.is_empty()
            {
                return unsupported("ALTER TABLE ADD COLUMN constraints are not supported in v1");
            }
            Ok(Statement::AlterTableAddColumn {
                table,
                if_not_exists,
                column,
            })
        }
        sql::AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            drop_behavior,
        } => {
            if only {
                return unsupported("ALTER TABLE ONLY is only supported for ALTER COLUMN TYPE");
            }
            if drop_behavior.is_some() {
                return unsupported("ALTER TABLE DROP COLUMN CASCADE/RESTRICT is not supported");
            }
            Ok(Statement::AlterTableDropColumn {
                table,
                if_exists,
                column: ident_name(&column_name)?,
            })
        }
        sql::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            if only {
                return unsupported("ALTER TABLE ONLY is only supported for ALTER COLUMN TYPE");
            }
            Ok(Statement::AlterTableRenameColumn {
                table,
                old_name: ident_name(&old_column_name)?,
                new_name: ident_name(&new_column_name)?,
            })
        }
        sql::AlterTableOperation::RenameTable { table_name } => {
            if only {
                return unsupported("ALTER TABLE ONLY is only supported for ALTER COLUMN TYPE");
            }
            Ok(Statement::AlterTableRenameTable {
                table,
                new_name: simple_object_name(&table_name)?,
            })
        }
        sql::AlterTableOperation::AlterColumn {
            column_name,
            op: sql::AlterColumnOperation::SetDataType { data_type, using },
        } => {
            if using.is_some() {
                return feature_not_supported("ALTER COLUMN TYPE USING is not supported");
            }
            let max_length = column_char_length(&data_type)?;
            let pg_type = match convert_pg_type(&data_type)? {
                PgType::Varchar(_) => PgType::Varchar(max_length),
                PgType::Bpchar(_) => PgType::Bpchar(max_length),
                other => other,
            };
            Ok(Statement::AlterTableAlterColumnType {
                table,
                column: ident_name(&column_name)?,
                data_type: pg_type.data_type(),
                pg_type,
            })
        }
        _ => unsupported("unsupported ALTER TABLE operation"),
    }
}

pub(super) struct CreateViewParts {
    pub(super) or_alter: bool,
    pub(super) or_replace: bool,
    pub(super) materialized: bool,
    pub(super) name: sql::ObjectName,
    pub(super) columns: Vec<sql::ViewColumnDef>,
    pub(super) query: sql::Query,
    pub(super) options: sql::CreateTableOptions,
    pub(super) cluster_by: Vec<sql::Ident>,
    pub(super) comment: Option<String>,
    pub(super) with_no_schema_binding: bool,
    pub(super) if_not_exists: bool,
    pub(super) temporary: bool,
    pub(super) to: Option<sql::ObjectName>,
    pub(super) params: Option<sql::CreateViewParams>,
}

pub(super) fn convert_create_view(parts: CreateViewParts) -> Result<Statement> {
    let CreateViewParts {
        or_alter,
        or_replace,
        materialized,
        name,
        columns,
        query,
        options,
        cluster_by,
        comment,
        with_no_schema_binding,
        if_not_exists,
        temporary,
        to,
        params,
    } = parts;
    if or_alter
        || materialized
        || !matches!(options, sql::CreateTableOptions::None)
        || !cluster_by.is_empty()
        || comment.is_some()
        || with_no_schema_binding
        || if_not_exists
        || temporary
        || to.is_some()
        || params.is_some()
    {
        return unsupported("unsupported CREATE VIEW form");
    }
    let columns = columns
        .into_iter()
        .map(|column| {
            if column.data_type.is_some()
                || column.options.as_ref().is_some_and(|opts| !opts.is_empty())
            {
                return unsupported("CREATE VIEW column types/options are not supported");
            }
            ident_name(&column.name)
        })
        .collect::<Result<Vec<_>>>()?;
    let definition = query.to_string();
    Ok(Statement::CreateView {
        name: object_name(&name)?,
        or_replace,
        columns,
        query: convert_query(query)?,
        definition,
    })
}

pub(super) fn convert_truncate(
    table_names: Vec<sql::TruncateTableTarget>,
    partitions: Option<Vec<sql::Expr>>,
    only: bool,
    identity: Option<sql::TruncateIdentityOption>,
    cascade: Option<sql::CascadeOption>,
    on_cluster: Option<sql::Ident>,
) -> Result<Statement> {
    if table_names.is_empty() {
        return feature_not_supported("TRUNCATE requires at least one table");
    }
    if partitions.is_some() {
        return feature_not_supported("TRUNCATE partitions are not supported");
    }
    if only {
        return feature_not_supported("TRUNCATE ONLY is not supported");
    }
    if identity.is_some() {
        return feature_not_supported("TRUNCATE identity options are not supported");
    }
    if cascade.is_some() {
        return feature_not_supported("TRUNCATE cascade options are not supported");
    }
    if on_cluster.is_some() {
        return feature_not_supported("TRUNCATE ON CLUSTER is not supported");
    }
    let tables = table_names
        .iter()
        .map(|table| object_name(&table.name))
        .collect::<Result<Vec<_>>>()?;
    reject_duplicate_relation_names(&tables, "TRUNCATE")?;
    Ok(Statement::Truncate { tables })
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
    let mut checks = Vec::new();
    let mut foreign_keys: Vec<(Location, ParsedForeignKey)> = Vec::new();
    let columns = columns
        .into_iter()
        .map(|column| {
            convert_column_def(
                column,
                &mut primary_key,
                &mut unique,
                &mut checks,
                &mut foreign_keys,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    for constraint in constraints {
        let constraint_start = constraint.span().start;
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
            sql::TableConstraint::Check { name, expr } => {
                if name.is_some() {
                    return unsupported("named CHECK constraints are not supported");
                }
                checks.push(expr.to_string());
            }
            sql::TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            } => {
                if characteristics.is_some() {
                    return feature_not_supported(
                        "foreign-key constraint characteristics are not supported",
                    );
                }
                foreign_keys.push((
                    constraint_start,
                    ParsedForeignKey {
                        name: name.as_ref().map(ident_name).transpose()?,
                        columns: columns.iter().map(ident_name).collect::<Result<Vec<_>>>()?,
                        referenced_table: object_name(&foreign_table)?,
                        referenced_columns: referred_columns
                            .iter()
                            .map(ident_name)
                            .collect::<Result<Vec<_>>>()?,
                        on_update: convert_foreign_key_action(on_update)?,
                        on_delete: convert_foreign_key_action(on_delete)?,
                    },
                ));
            }
            _ => return unsupported("unsupported table constraint"),
        }
    }

    let options = convert_table_options(with_options)?;
    foreign_keys.sort_by_key(|(start, _)| *start);
    let foreign_keys = foreign_keys
        .into_iter()
        .map(|(_, foreign_key)| foreign_key)
        .collect();

    Ok(Statement::CreateTable {
        name: object_name(&name)?,
        if_not_exists,
        columns,
        primary_key,
        unique,
        compression: options.compression,
        toast: options.toast,
        checks,
        foreign_keys,
    })
}

/// Parse `WITH (...)` storage options. Unknown keys are syntax errors; known
/// keys with unsupported enum values are 0A000; duplicates are syntax errors
/// (the CREATE SEQUENCE duplicate-option convention).
fn convert_table_options(options: Vec<sql::SqlOption>) -> Result<TableOptionPatch> {
    let mut parsed = TableOptionPatch::default();
    let mut saw_fillfactor = false;
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
            "fillfactor" => {
                if saw_fillfactor {
                    return Err(parse_error("duplicate fillfactor option"));
                }
                saw_fillfactor = true;
                let value = parse_fillfactor_value(&value)?;
                if !(10..=100).contains(&value) {
                    return Err(invalid_parameter_value(
                        "fillfactor must be between 10 and 100",
                    ));
                }
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

fn parse_fillfactor_value(value: &sql::Expr) -> Result<u32> {
    let (text, negative) = match value {
        sql::Expr::Value(v) => match &v.value {
            sql::Value::Number(text, false) => (text.as_str(), false),
            _ => return Err(parse_error("fillfactor must be an integer literal")),
        },
        sql::Expr::UnaryOp {
            op: sql::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            sql::Expr::Value(sql::ValueWithSpan {
                value: sql::Value::Number(text, false),
                ..
            }) => (text.as_str(), true),
            _ => return Err(parse_error("fillfactor must be an integer literal")),
        },
        _ => return Err(parse_error("fillfactor must be an integer literal")),
    };
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(parse_error("fillfactor must be an integer literal"));
    }
    if negative {
        return Err(invalid_parameter_value(
            "fillfactor must be between 10 and 100",
        ));
    }
    text.parse::<u32>()
        .map_err(|_| invalid_parameter_value("fillfactor must be between 10 and 100"))
}

fn invalid_parameter_value(message: impl Into<String>) -> DbError {
    DbError::parse(SqlState::InvalidParameterValue, message)
}

fn convert_column_def(
    column: sql::ColumnDef,
    primary_key: &mut Vec<String>,
    unique: &mut Vec<Vec<String>>,
    checks: &mut Vec<String>,
    foreign_keys: &mut Vec<(Location, ParsedForeignKey)>,
) -> Result<ParsedColumnDef> {
    let mut nullable = true;
    let mut default = None;
    let serial = serial_pg_type(&column.data_type)?;

    for option in &column.options {
        if option.name.is_some() && !matches!(option.option, sql::ColumnOption::ForeignKey { .. }) {
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
            // Column-level `CHECK (expr)` flattens into the table's check list, as
            // in PostgreSQL. A named form (`CONSTRAINT c CHECK ...`) sets
            // `option.name` and is already rejected above.
            sql::ColumnOption::Check(expr) => checks.push(expr.to_string()),
            sql::ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            } => {
                if characteristics.is_some() {
                    return feature_not_supported(
                        "foreign-key constraint characteristics are not supported",
                    );
                }
                foreign_keys.push((
                    option.option.span().start,
                    ParsedForeignKey {
                        name: option.name.as_ref().map(ident_name).transpose()?,
                        columns: vec![ident_name(&column.name)?],
                        referenced_table: object_name(foreign_table)?,
                        referenced_columns: referred_columns
                            .iter()
                            .map(ident_name)
                            .collect::<Result<Vec<_>>>()?,
                        on_update: convert_foreign_key_action(*on_update)?,
                        on_delete: convert_foreign_key_action(*on_delete)?,
                    },
                ));
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
    let pg_type = apply_column_char_length(convert_pg_type(&column.data_type)?, max_length)?;
    Ok(ParsedColumnDef {
        name,
        data_type: pg_type.data_type(),
        nullable,
        max_length,
        default,
        pg_type: Some(pg_type),
    })
}

fn convert_foreign_key_action(action: Option<sql::ReferentialAction>) -> Result<ForeignKeyAction> {
    match action.unwrap_or(sql::ReferentialAction::NoAction) {
        sql::ReferentialAction::NoAction => Ok(ForeignKeyAction::NoAction),
        sql::ReferentialAction::Restrict => Ok(ForeignKeyAction::Restrict),
        other => feature_not_supported(format!("foreign-key action {other} is not supported")),
    }
}

fn apply_column_char_length(pg_type: PgType, max_length: Option<u32>) -> Result<PgType> {
    match pg_type {
        PgType::Varchar(_) => Ok(PgType::Varchar(max_length)),
        PgType::Bpchar(_) => Ok(PgType::Bpchar(max_length)),
        PgType::Array(array) => PgType::array(apply_column_char_length(
            array.element_type().clone(),
            max_length,
        )?),
        other => Ok(other),
    }
}

/// Convert a column `DEFAULT` expression into the bounded parse-time default
/// representation. Most defaults are constants folded at parse time; the one
/// volatile form accepted here is `nextval('sequence')`, which the catalog later
/// resolves to a durable sequence id.
fn convert_column_default(expr: &sql::Expr) -> Result<ParsedDefault> {
    // Canonical SQL text is carried to the DDL binder, which parses it once and
    // persists typed durable expression IR. DML does not parse this text.
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
