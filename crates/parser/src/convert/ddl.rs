use common::{ParsedColumnDef, Result};
use sqlparser::ast as sql;

use crate::Statement;

use super::{convert_data_type, ident_name, object_name, unsupported};

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
        || !with_options.is_empty()
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
    let columns = columns
        .into_iter()
        .map(|column| convert_column_def(column, &mut primary_key))
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
            _ => return unsupported("unsupported table constraint"),
        }
    }

    Ok(Statement::CreateTable {
        name: object_name(&name)?,
        columns,
        primary_key,
    })
}

fn convert_column_def(
    column: sql::ColumnDef,
    primary_key: &mut Vec<String>,
) -> Result<ParsedColumnDef> {
    let mut nullable = true;

    for option in &column.options {
        if option.name.is_some() {
            return unsupported("unsupported named column constraint");
        }

        match &option.option {
            sql::ColumnOption::Null => nullable = true,
            sql::ColumnOption::NotNull => nullable = false,
            sql::ColumnOption::Unique {
                is_primary,
                characteristics,
            } => {
                if !is_primary {
                    return unsupported("unsupported column option");
                }
                if characteristics.is_some() {
                    return unsupported("unsupported PRIMARY KEY constraint form");
                }
                let column_name = ident_name(&column.name)?;
                set_primary_key(primary_key, vec![column_name])?;
                nullable = false;
            }
            _ => return unsupported("unsupported column option"),
        }
    }

    Ok(ParsedColumnDef {
        name: ident_name(&column.name)?,
        data_type: convert_data_type(&column.data_type)?,
        nullable,
    })
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
