use std::collections::{BTreeMap, BTreeSet};

use catalog::{
    CatalogManager, INFORMATION_SCHEMA_OID, PG_CATALOG_SCHEMA_OID, PUBLIC_SCHEMA_OID, SystemView,
    attrdef_oid, constraint_oid, index_oid, schema_oid, sequence_oid, synthetic_primary_key_oid,
    table_oid,
};
use common::{
    CatalogObjectId, ColumnDef, ColumnDefault, ConstraintKind, DbError, DependencyType,
    ForeignKeyAction, GucSetting, IsolationLevel, OrderedF32, PgProcCatalogEntry, PgType,
    RelationKind, Result, Row, SequenceSchema, SessionActivityRow, StatementContext, TableId,
    TableSchema, Value, ViewSchema, bytea, datetime, float, interval, numeric,
    pg_proc_catalog_entries, uuid,
};

const OWNER_OID: i64 = 10;
const BTREE_AM_OID: i64 = 403;
const SQL_LANGUAGE_OID: i64 = 14;
const DEFAULT_DATABASE_OID: i64 = 5;
const DEFAULT_TABLESPACE_OID: i64 = 1663;
const DEFAULT_COLLATION_OID: i64 = 100;

pub(crate) fn rows_for(
    view: SystemView,
    catalog: &dyn CatalogManager,
    ctx: &StatementContext,
) -> Result<Vec<Row>> {
    match view {
        SystemView::PgNamespace => pg_namespace_rows(catalog),
        SystemView::PgClass => pg_class_rows(catalog),
        SystemView::PgAttribute => pg_attribute_rows(catalog),
        SystemView::PgType => pg_type_rows(),
        SystemView::PgIndex => pg_index_rows(catalog),
        SystemView::PgProc => pg_proc_rows(),
        SystemView::PgConstraint => pg_constraint_rows(catalog),
        SystemView::PgAttrdef => pg_attrdef_rows(catalog),
        SystemView::PgDepend => pg_depend_rows(catalog),
        SystemView::PgDatabase => Ok(pg_database_rows(ctx)),
        SystemView::PgRoles => Ok(pg_roles_rows(ctx)),
        SystemView::PgSettings => Ok(pg_settings_rows(ctx)),
        SystemView::PgStatActivity => Ok(pg_stat_activity_rows(ctx)),
        SystemView::PgStats => pg_stats_rows(catalog),
        SystemView::InformationSchemaSchemata => information_schema_schemata_rows(catalog, ctx),
        SystemView::InformationSchemaTables => information_schema_tables_rows(catalog, ctx),
        SystemView::InformationSchemaColumns => information_schema_columns_rows(catalog, ctx),
    }
}

fn namespace_oid(schema_id: common::SchemaId) -> Result<i64> {
    if schema_id == common::PUBLIC_SCHEMA_ID {
        Ok(PUBLIC_SCHEMA_OID)
    } else {
        schema_oid(schema_id)
    }
}

fn pg_namespace_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let mut rows = vec![
        row(vec![
            int(PG_CATALOG_SCHEMA_OID),
            text("pg_catalog"),
            int(OWNER_OID),
        ]),
        row(vec![int(PUBLIC_SCHEMA_OID), text("public"), int(OWNER_OID)]),
        row(vec![
            int(INFORMATION_SCHEMA_OID),
            text("information_schema"),
            int(OWNER_OID),
        ]),
    ];
    for schema in catalog.list_schemas()? {
        if schema.id != common::PUBLIC_SCHEMA_ID {
            rows.push(row(vec![
                int(namespace_oid(schema.id)?),
                text(&schema.name),
                int(OWNER_OID),
            ]));
        }
    }
    sort_rows_by_key(&mut rows, |row| integer_at(row, 0))?;
    Ok(rows)
}

fn pg_class_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let tables = catalog.list_tables()?;
    let mut rows = Vec::new();

    for table in &tables {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        let indexes = catalog.list_indexes_for_table(table.id)?;
        let has_primary_key_index = indexes
            .iter()
            .any(|index| index.constraint.is_some() && index.columns == table.primary_key);
        let check_count = catalog
            .list_constraints_for_table(table.id)?
            .into_iter()
            .filter(|constraint| matches!(constraint.kind, ConstraintKind::Check { .. }))
            .count();
        rows.push(pg_class_table_row(
            table,
            catalog.get_table_statistics(table.id)?.as_ref(),
            !indexes.is_empty() || (!table.primary_key.is_empty() && !has_primary_key_index),
            check_count,
        )?);
        for index in indexes {
            rows.push(pg_class_index_row(
                index_oid(index.id)?,
                &index.name,
                index.columns.len(),
                index.schema_id,
            )?);
        }
        if !has_primary_key_index && !table.primary_key.is_empty() {
            rows.push(pg_class_index_row(
                synthetic_primary_key_oid(table.id)?,
                &primary_key_index_name(table),
                table.primary_key.len(),
                table.schema_id,
            )?);
        }
    }

    for sequence in catalog.list_sequences()? {
        rows.push(pg_class_sequence_row(&sequence)?);
    }

    for view in catalog.list_views()? {
        rows.push(pg_class_user_view_row(&view)?);
    }

    for view in SystemView::ALL {
        rows.push(pg_class_view_row(*view));
    }

    sort_rows_by_key(&mut rows, |row| integer_at(row, 0))?;
    Ok(rows)
}

/// `relpages`/`reltuples` come from stored ANALYZE statistics when present
/// (`docs/specs/statistics.md` §8); a never-analyzed table keeps PostgreSQL's
/// "unknown" convention (`0` pages, `-1` tuples).
fn pg_class_table_row(
    table: &TableSchema,
    statistics: Option<&common::TableStatistics>,
    relhasindex: bool,
    check_count: usize,
) -> Result<Row> {
    let oid = table_oid(table.id)?;
    let relpages = statistics.map_or(0, |stats| stats.page_count as i64);
    let reltuples = statistics.map_or(-1.0, |stats| stats.row_count as f32);
    Ok(row(vec![
        int(oid),
        text(relation_name(table)),
        int(namespace_oid(table.schema_id)?),
        int(0),
        int(OWNER_OID),
        int(0),
        int(oid),
        int(0),
        int(relpages),
        real(reltuples),
        int(0),
        int(0),
        bool_value(relhasindex),
        bool_value(false),
        text("p"),
        text(match table.relation_kind {
            RelationKind::User => "r",
            RelationKind::Toast { .. } => "t",
        }),
        int(table.columns.len() as i64),
        int(check_count as i64),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(true),
        text("d"),
        bool_value(false),
    ]))
}

fn pg_class_index_row(
    oid: i64,
    name: &str,
    natts: usize,
    schema_id: common::SchemaId,
) -> Result<Row> {
    Ok(row(vec![
        int(oid),
        text(name),
        int(namespace_oid(schema_id)?),
        int(0),
        int(OWNER_OID),
        int(BTREE_AM_OID),
        int(oid),
        int(0),
        int(0),
        real(-1.0),
        int(0),
        int(0),
        bool_value(false),
        bool_value(false),
        text("p"),
        text("i"),
        int(natts as i64),
        int(0),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(true),
        text("d"),
        bool_value(false),
    ]))
}

fn pg_class_sequence_row(sequence: &SequenceSchema) -> Result<Row> {
    let oid = sequence_oid(sequence.id)?;
    Ok(row(vec![
        int(oid),
        text(&sequence.name),
        int(namespace_oid(sequence.schema_id)?),
        int(0),
        int(OWNER_OID),
        int(0),
        int(oid),
        int(0),
        int(0),
        real(-1.0),
        int(0),
        int(0),
        bool_value(false),
        bool_value(false),
        text("p"),
        text("S"),
        int(0),
        int(0),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(true),
        text("d"),
        bool_value(false),
    ]))
}

fn pg_class_user_view_row(view: &ViewSchema) -> Result<Row> {
    let oid = table_oid(view.id)?;
    Ok(row(vec![
        int(oid),
        text(&view.name),
        int(namespace_oid(view.schema_id)?),
        int(0),
        int(OWNER_OID),
        int(0),
        int(oid),
        int(0),
        int(0),
        real(-1.0),
        int(0),
        int(0),
        bool_value(false),
        bool_value(false),
        text("p"),
        text("v"),
        int(view.columns.len() as i64),
        int(0),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(true),
        text("d"),
        bool_value(false),
    ]))
}

fn pg_class_view_row(view: SystemView) -> Row {
    let oid = view.relation_oid();
    row(vec![
        int(oid),
        text(view.name()),
        int(view.schema().oid()),
        int(0),
        int(OWNER_OID),
        int(0),
        int(oid),
        int(0),
        int(0),
        real(-1.0),
        int(0),
        int(0),
        bool_value(false),
        bool_value(false),
        text("p"),
        text("v"),
        int(view.columns().len() as i64),
        int(0),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(true),
        text("d"),
        bool_value(false),
    ])
}

fn pg_attribute_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for table in catalog.list_tables()? {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        for column in &table.columns {
            rows.push(pg_attribute_row(table_oid(table.id)?, column));
        }
    }
    for view in catalog.list_views()? {
        for column in &view.columns {
            rows.push(pg_attribute_row(table_oid(view.id)?, column));
        }
    }
    for view in SystemView::ALL {
        for column in view.columns() {
            rows.push(pg_attribute_row(view.relation_oid(), &column));
        }
    }
    sort_rows_by_key(&mut rows, |row| {
        Ok((integer_at(row, 0)?, integer_at(row, 5)?))
    })?;
    Ok(rows)
}

fn pg_attribute_row(rel_oid: i64, column: &ColumnDef) -> Row {
    let pg_type = column.wire_type();
    row(vec![
        int(rel_oid),
        text(&column.name),
        int(i64::from(pg_type.oid())),
        int(-1),
        int(i64::from(pg_type.typlen())),
        int(i64::from(column.id) + 1),
        int(0),
        int(-1),
        int(i64::from(pg_type.typmod())),
        bool_value(type_byval(&pg_type)),
        text(type_align(&pg_type)),
        text(type_storage(&pg_type)),
        text(""),
        bool_value(!column.nullable),
        bool_value(column.default.is_some()),
        bool_value(false),
        text(""),
        text(""),
        bool_value(false),
        bool_value(true),
        int(0),
        int(type_collation_oid(&pg_type)),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ])
}

fn pg_type_rows() -> Result<Vec<Row>> {
    Ok(type_entries()?
        .into_iter()
        .map(|entry| {
            row(vec![
                int(i64::from(entry.pg_type.oid())),
                text(entry.name),
                int(PG_CATALOG_SCHEMA_OID),
                int(OWNER_OID),
                int(i64::from(entry.pg_type.typlen())),
                bool_value(entry.byval),
                text("b"),
                text(entry.category),
                bool_value(true),
                text(","),
                int(0),
                int(entry.element_oid),
                int(entry.array_oid),
                bool_value(false),
                int(0),
            ])
        })
        .collect())
}

fn pg_index_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let tables = catalog.list_tables()?;
    let primary_indexes: BTreeSet<_> = catalog
        .list_constraints()?
        .into_iter()
        .filter_map(|constraint| match constraint.kind {
            ConstraintKind::PrimaryKey { index, .. } => Some(index),
            _ => None,
        })
        .collect();
    let table_by_id: BTreeMap<TableId, TableSchema> = tables
        .iter()
        .map(|table| (table.id, table.clone()))
        .collect();
    let mut rows = Vec::new();

    for table in &tables {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        let indexes = catalog.list_indexes_for_table(table.id)?;
        let has_primary_key_index = indexes
            .iter()
            .any(|index| primary_indexes.contains(&index.id));
        if !has_primary_key_index && !table.primary_key.is_empty() {
            rows.push(pg_index_row(
                synthetic_primary_key_oid(table.id)?,
                table_oid(table.id)?,
                &table.primary_key,
                true,
                true,
            ));
        }
        for index in indexes {
            if let Some(table) = table_by_id.get(&index.table) {
                if !index.columns.iter().all(|column| {
                    table
                        .columns
                        .iter()
                        .any(|candidate| candidate.id == *column)
                }) {
                    return Err(DbError::internal(format!(
                        "index {} references a missing table column",
                        index.name
                    )));
                }
                rows.push(pg_index_row(
                    index_oid(index.id)?,
                    table_oid(index.table)?,
                    &index.columns,
                    index.unique,
                    primary_indexes.contains(&index.id),
                ));
            }
        }
    }

    sort_rows_by_key(&mut rows, |row| integer_at(row, 0))?;
    Ok(rows)
}

fn pg_index_row(
    index_oid_value: i64,
    table_oid_value: i64,
    columns: &[u16],
    unique: bool,
    primary: bool,
) -> Row {
    row(vec![
        int(index_oid_value),
        int(table_oid_value),
        int(columns.len() as i64),
        int(columns.len() as i64),
        bool_value(unique),
        bool_value(primary),
        bool_value(false),
        bool_value(true),
        bool_value(false),
        bool_value(true),
        bool_value(true),
        bool_value(true),
        bool_value(false),
        text(
            columns
                .iter()
                .map(|column| (i64::from(*column) + 1).to_string())
                .collect::<Vec<_>>()
                .join(" "),
        ),
    ])
}

fn pg_proc_rows() -> Result<Vec<Row>> {
    let mut rows: Vec<_> = pg_proc_catalog_entries().iter().map(pg_proc_row).collect();
    sort_rows_by_key(&mut rows, |row| integer_at(row, 0))?;
    Ok(rows)
}

fn pg_proc_row(entry: &PgProcCatalogEntry) -> Row {
    row(vec![
        int(entry.oid),
        text(entry.name),
        int(PG_CATALOG_SCHEMA_OID),
        int(OWNER_OID),
        int(SQL_LANGUAGE_OID),
        real(1.0),
        real(0.0),
        int(entry.variadic_oid()),
        int(0),
        text("f"),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        bool_value(false),
        text("s"),
        text("s"),
        int(entry.arg_oids.len() as i64),
        int(0),
        int(entry.ret_oid),
        text(
            entry
                .arg_oids
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(" "),
        ),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        text(entry.name),
        Value::Null,
        Value::Null,
        Value::Null,
    ])
}

fn pg_constraint_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for constraint in catalog.list_constraints()? {
        let table = catalog
            .get_table(constraint.table)?
            .ok_or_else(|| DbError::internal("constraint references a missing table"))?;
        let mut row_data = ConstraintRow {
            oid: constraint_oid(constraint.id)?,
            name: constraint.name,
            namespace_oid: namespace_oid(table.schema_id)?,
            kind: "c",
            table_oid: table_oid(table.id)?,
            index_oid: 0,
            key_columns: None,
            referenced_table_oid: 0,
            update_action: "a",
            delete_action: "a",
            referenced_columns: None,
            expression: None,
        };
        match constraint.kind {
            ConstraintKind::Check { expression } => row_data.expression = Some(expression.sql),
            ConstraintKind::PrimaryKey { columns, index } => {
                row_data.kind = "p";
                row_data.index_oid = index_oid(index)?;
                row_data.key_columns = Some(attnums_array_text(&stable_attnums(&table, &columns)?));
            }
            ConstraintKind::Unique { columns, index } => {
                row_data.kind = "u";
                row_data.index_oid = index_oid(index)?;
                row_data.key_columns = Some(attnums_array_text(&stable_attnums(&table, &columns)?));
            }
            ConstraintKind::ForeignKey {
                columns,
                referenced_table,
                referenced_constraint,
                referenced_columns,
                on_update,
                on_delete,
                ..
            } => {
                let parent = catalog
                    .get_table(referenced_table)?
                    .ok_or_else(|| DbError::internal("foreign key references a missing table"))?;
                let referenced =
                    catalog
                        .get_constraint(referenced_constraint)?
                        .ok_or_else(|| {
                            DbError::internal("foreign key references a missing constraint")
                        })?;
                let backing_index = match referenced.kind {
                    ConstraintKind::PrimaryKey { index, .. }
                    | ConstraintKind::Unique { index, .. } => index,
                    _ => {
                        return Err(DbError::internal(
                            "foreign key references a non-key constraint",
                        ));
                    }
                };
                row_data.kind = "f";
                row_data.index_oid = index_oid(backing_index)?;
                row_data.key_columns = Some(attnums_array_text(&stable_attnums(&table, &columns)?));
                row_data.referenced_table_oid = table_oid(parent.id)?;
                row_data.update_action = foreign_key_action_code(on_update);
                row_data.delete_action = foreign_key_action_code(on_delete);
                row_data.referenced_columns = Some(attnums_array_text(&stable_attnums(
                    &parent,
                    &referenced_columns,
                )?));
            }
        }
        rows.push(pg_constraint_row(row_data));
    }
    sort_rows_by_key(&mut rows, |row| integer_at(row, 0))?;
    Ok(rows)
}

fn pg_constraint_row(row_data: ConstraintRow) -> Row {
    row(vec![
        int(row_data.oid),
        text(row_data.name),
        int(row_data.namespace_oid),
        text(row_data.kind),
        bool_value(false),
        bool_value(false),
        bool_value(true),
        int(row_data.table_oid),
        int(0),
        int(row_data.index_oid),
        int(0),
        int(row_data.referenced_table_oid),
        text(row_data.update_action),
        text(row_data.delete_action),
        text("s"),
        bool_value(true),
        int(0),
        bool_value(false),
        nullable_text(row_data.key_columns),
        nullable_text(row_data.referenced_columns),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        nullable_text(row_data.expression),
    ])
}

fn pg_attrdef_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for table in catalog.list_tables()? {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        for column in &table.columns {
            let Some(default) = column.default.as_ref() else {
                continue;
            };
            let Some(rendered) = render_default(catalog, default)? else {
                continue;
            };
            rows.push(row(vec![
                int(attrdef_oid(table.id, column.id)?),
                int(table_oid(table.id)?),
                int(i64::from(column.id) + 1),
                text(rendered),
            ]));
        }
    }
    sort_rows_by_key(&mut rows, |row| integer_at(row, 0))?;
    Ok(rows)
}

fn pg_depend_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for dependency in catalog.list_dependencies()? {
        let Some((classid, objid, objsubid)) = dependency_object(catalog, dependency.dependent)?
        else {
            continue;
        };
        let Some((refclassid, refobjid, refobjsubid)) =
            dependency_object(catalog, dependency.referenced)?
        else {
            continue;
        };
        let dependency_type = match dependency.dependency_type {
            DependencyType::Normal => "n",
            DependencyType::Auto => "a",
            DependencyType::Internal => "i",
        };
        rows.push(pg_depend_row(
            classid,
            objid,
            objsubid,
            refclassid,
            refobjid,
            refobjsubid,
            dependency_type,
        ));
    }
    sort_rows_by_key(&mut rows, |row| {
        Ok((
            integer_at(row, 0)?,
            integer_at(row, 1)?,
            integer_at(row, 4)?,
        ))
    })?;
    Ok(rows)
}

fn dependency_object(
    catalog: &dyn CatalogManager,
    object: CatalogObjectId,
) -> Result<Option<(i64, i64, i64)>> {
    let value = match object {
        CatalogObjectId::Schema(id) => (
            SystemView::PgNamespace.relation_oid(),
            namespace_oid(id)?,
            0,
        ),
        CatalogObjectId::Table(id) | CatalogObjectId::View(id) => {
            (SystemView::PgClass.relation_oid(), table_oid(id)?, 0)
        }
        CatalogObjectId::Index(id) => (SystemView::PgClass.relation_oid(), index_oid(id)?, 0),
        CatalogObjectId::Sequence(id) => (SystemView::PgClass.relation_oid(), sequence_oid(id)?, 0),
        CatalogObjectId::Constraint(id) => (
            SystemView::PgConstraint.relation_oid(),
            constraint_oid(id)?,
            0,
        ),
        CatalogObjectId::Function(_) => return Ok(None),
        CatalogObjectId::Column { relation, column } => {
            let dense = if let Some(table) = catalog.get_table(relation)? {
                table.dense_column_id(column)
            } else if let Some(view) = catalog.get_view(relation)? {
                view.columns
                    .iter()
                    .find(|candidate| candidate.object_id == column)
                    .map(|candidate| candidate.id)
            } else {
                None
            }
            .ok_or_else(|| DbError::internal("dependency references a missing column"))?;
            (
                SystemView::PgClass.relation_oid(),
                table_oid(relation)?,
                i64::from(dense) + 1,
            )
        }
        CatalogObjectId::ColumnDefault { relation, column } => {
            let dense = catalog
                .get_table(relation)?
                .and_then(|table| table.dense_column_id(column))
                .ok_or_else(|| {
                    DbError::internal("dependency references a missing column default")
                })?;
            (
                SystemView::PgAttrdef.relation_oid(),
                attrdef_oid(relation, dense)?,
                0,
            )
        }
        CatalogObjectId::Statistics(_) => return Ok(None),
    };
    Ok(Some(value))
}

fn pg_depend_row(
    classid: i64,
    objid: i64,
    objsubid: i64,
    refclassid: i64,
    refobjid: i64,
    refobjsubid: i64,
    deptype: &str,
) -> Row {
    row(vec![
        int(classid),
        int(objid),
        int(objsubid),
        int(refclassid),
        int(refobjid),
        int(refobjsubid),
        text(deptype),
    ])
}

fn pg_database_rows(ctx: &StatementContext) -> Vec<Row> {
    vec![row(vec![
        int(DEFAULT_DATABASE_OID),
        text(&ctx.session_info.database),
        int(OWNER_OID),
        int(6),
        text("c"),
        bool_value(false),
        bool_value(true),
        int(-1),
        int(0),
        int(0),
        int(DEFAULT_TABLESPACE_OID),
        text("C"),
        text("C"),
        Value::Null,
        Value::Null,
        Value::Null,
    ])]
}

fn pg_roles_rows(ctx: &StatementContext) -> Vec<Row> {
    vec![row(vec![
        text(&ctx.session_info.user),
        bool_value(true),
        bool_value(true),
        bool_value(true),
        bool_value(true),
        bool_value(true),
        bool_value(false),
        int(-1),
        Value::Null,
        Value::Null,
        bool_value(false),
        Value::Null,
        int(OWNER_OID),
    ])]
}

fn pg_settings_rows(ctx: &StatementContext) -> Vec<Row> {
    let mut settings = ctx.system_state.settings();
    ensure_setting(
        &mut settings,
        GucSetting {
            name: "transaction_isolation".to_string(),
            setting: isolation_setting(ctx.isolation).to_string(),
            boot_val: isolation_setting(IsolationLevel::default()).to_string(),
            reset_val: isolation_setting(IsolationLevel::default()).to_string(),
            source: "session".to_string(),
        },
    );
    ensure_setting(
        &mut settings,
        GucSetting {
            name: "default_transaction_isolation".to_string(),
            setting: isolation_setting(IsolationLevel::default()).to_string(),
            boot_val: isolation_setting(IsolationLevel::default()).to_string(),
            reset_val: isolation_setting(IsolationLevel::default()).to_string(),
            source: "default".to_string(),
        },
    );
    settings.sort_by(|left, right| left.name.cmp(&right.name));
    settings
        .into_iter()
        .map(|setting| {
            row(vec![
                text(setting.name),
                text(setting.setting),
                Value::Null,
                Value::Null,
                Value::Null,
                text("user"),
                text("string"),
                text(setting.source),
                text(setting.boot_val),
                text(setting.reset_val),
                bool_value(false),
            ])
        })
        .collect()
}

fn ensure_setting(settings: &mut Vec<GucSetting>, setting: GucSetting) {
    if !settings
        .iter()
        .any(|candidate| candidate.name.eq_ignore_ascii_case(&setting.name))
    {
        settings.push(setting);
    }
}

fn pg_stat_activity_rows(ctx: &StatementContext) -> Vec<Row> {
    let mut sessions = ctx.system_state.sessions();
    sessions.sort_by_key(|session| session.pid);
    sessions.into_iter().map(pg_stat_activity_row).collect()
}

fn pg_stat_activity_row(session: SessionActivityRow) -> Row {
    row(vec![
        int(i64::from(session.datid)),
        text(session.datname),
        int(i64::from(session.pid)),
        int(i64::from(session.usesysid)),
        text(session.usename),
        text(session.application_name),
        Value::Null,
        Value::Null,
        timestamp_tz(Some(session.backend_start)),
        timestamp_tz(session.xact_start),
        timestamp_tz(session.query_start),
        timestamp_tz(session.state_change),
        Value::Null,
        Value::Null,
        text(session.state.as_str()),
        text(session.query),
        text("client backend"),
    ])
}

fn pg_stats_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let mut tables = catalog
        .list_tables()?
        .into_iter()
        .filter(|table| table.relation_kind == RelationKind::User)
        .collect::<Vec<_>>();
    tables.sort_unstable_by_key(|table| table.id);
    let schemas = catalog
        .list_schemas()?
        .into_iter()
        .map(|schema| (schema.id, schema.name))
        .collect::<BTreeMap<_, _>>();

    let mut rows = Vec::new();
    for table in &tables {
        let Some(statistics) = catalog.get_table_statistics(table.id)? else {
            continue;
        };
        for column in &table.columns {
            let Some(column_stats) = statistics.columns.get(&column.id) else {
                continue;
            };
            let n_distinct = match &column_stats.n_distinct {
                common::NDistinct::Count(count) => *count as f32,
                common::NDistinct::Fraction(fraction) => -(fraction.get() as f32),
            };
            let mcv_vals = non_empty_array_text(
                column_stats
                    .most_common
                    .iter()
                    .map(|(value, _)| stat_value_text(value)),
            );
            let mcv_freqs = non_empty_array_text(
                column_stats
                    .most_common
                    .iter()
                    .map(|(_, freq)| common::float::format_double(freq.get())),
            );
            let histogram =
                non_empty_array_text(column_stats.histogram_bounds.iter().map(stat_value_text));
            rows.push(row(vec![
                text(
                    schemas
                        .get(&table.schema_id)
                        .map_or("public", String::as_str),
                ),
                text(&table.name),
                text(&column.name),
                real(column_stats.null_frac.get() as f32),
                int(i64::from(column_stats.avg_width)),
                real(n_distinct),
                nullable_text(mcv_vals),
                nullable_text(mcv_freqs),
                nullable_text(histogram),
                Value::Null,
            ]));
        }
    }
    Ok(rows)
}

fn stat_value_text(value: &Value) -> String {
    crate::copy::value_text(value).unwrap_or_default()
}

fn non_empty_array_text(elements: impl Iterator<Item = String>) -> Option<String> {
    let mut out = String::from("{");
    let mut any = false;
    for element in elements {
        if any {
            out.push(',');
        }
        any = true;
        if array_element_needs_quotes(&element) {
            out.push('"');
            for ch in element.chars() {
                if ch == '"' || ch == '\\' {
                    out.push('\\');
                }
                out.push(ch);
            }
            out.push('"');
        } else {
            out.push_str(&element);
        }
    }
    if !any {
        return None;
    }
    out.push('}');
    Some(out)
}

fn array_element_needs_quotes(element: &str) -> bool {
    element.is_empty()
        || element.eq_ignore_ascii_case("null")
        || element
            .chars()
            .any(|ch| matches!(ch, '{' | '}' | ',' | '"' | '\\') || ch.is_whitespace())
}

fn information_schema_schemata_rows(
    catalog: &dyn CatalogManager,
    ctx: &StatementContext,
) -> Result<Vec<Row>> {
    let mut schemas = vec!["pg_catalog".to_string(), "information_schema".to_string()];
    schemas.extend(
        catalog
            .list_schemas()?
            .into_iter()
            .map(|schema| schema.name),
    );
    schemas.sort();
    schemas.dedup();
    Ok(schemas
        .into_iter()
        .map(|schema| {
            row(vec![
                text(&ctx.session_info.database),
                text(schema),
                text(&ctx.session_info.user),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ])
        })
        .collect())
}

fn information_schema_tables_rows(
    catalog: &dyn CatalogManager,
    ctx: &StatementContext,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for table in catalog.list_tables()? {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        let schema = catalog
            .get_schema(table.schema_id)?
            .ok_or_else(|| common::DbError::internal("table references missing schema"))?;
        rows.push(information_schema_table_row(
            ctx,
            &schema.name,
            &table.name,
            "BASE TABLE",
            "YES",
        ));
    }
    for view in SystemView::ALL {
        rows.push(information_schema_table_row(
            ctx,
            view.schema().name(),
            view.name(),
            "VIEW",
            "NO",
        ));
    }
    for view in catalog.list_views()? {
        let schema = catalog
            .get_schema(view.schema_id)?
            .ok_or_else(|| common::DbError::internal("view references missing schema"))?;
        rows.push(information_schema_table_row(
            ctx,
            &schema.name,
            &view.name,
            "VIEW",
            "NO",
        ));
    }
    sort_rows_by_key(&mut rows, |row| {
        Ok((text_at(row, 1)?.to_string(), text_at(row, 2)?.to_string()))
    })?;
    Ok(rows)
}

fn information_schema_table_row(
    ctx: &StatementContext,
    schema: &str,
    name: &str,
    table_type: &str,
    is_insertable: &str,
) -> Row {
    row(vec![
        text(&ctx.session_info.database),
        text(schema),
        text(name),
        text(table_type),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        text(is_insertable),
        text("NO"),
        Value::Null,
    ])
}

fn information_schema_columns_rows(
    catalog: &dyn CatalogManager,
    ctx: &StatementContext,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for table in catalog.list_tables()? {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        let schema = catalog
            .get_schema(table.schema_id)?
            .ok_or_else(|| common::DbError::internal("table references missing schema"))?;
        for column in &table.columns {
            rows.push(information_schema_column_row(
                catalog,
                ctx,
                &schema.name,
                &table.name,
                column,
                true,
            )?);
        }
    }
    for view in SystemView::ALL {
        for column in view.columns() {
            rows.push(information_schema_column_row(
                catalog,
                ctx,
                view.schema().name(),
                view.name(),
                &column,
                false,
            )?);
        }
    }
    for view in catalog.list_views()? {
        let schema = catalog
            .get_schema(view.schema_id)?
            .ok_or_else(|| common::DbError::internal("view references missing schema"))?;
        for column in &view.columns {
            rows.push(information_schema_column_row(
                catalog,
                ctx,
                &schema.name,
                &view.name,
                column,
                false,
            )?);
        }
    }
    sort_rows_by_key(&mut rows, |row| {
        Ok((
            text_at(row, 1)?.to_string(),
            text_at(row, 2)?.to_string(),
            integer_at(row, 4)?,
        ))
    })?;
    Ok(rows)
}

fn information_schema_column_row(
    catalog: &dyn CatalogManager,
    ctx: &StatementContext,
    schema: &str,
    table: &str,
    column: &ColumnDef,
    is_updatable: bool,
) -> Result<Row> {
    let pg_type = column.wire_type();
    let column_default = match column.default.as_ref() {
        Some(default) => render_default(catalog, default)?,
        None => None,
    };
    Ok(row(vec![
        text(&ctx.session_info.database),
        text(schema),
        text(table),
        text(&column.name),
        int(i64::from(column.id) + 1),
        nullable_text(column_default),
        text(if column.nullable { "YES" } else { "NO" }),
        text(sql_data_type(&pg_type)),
        nullable_int(character_maximum_length(&pg_type).map(i64::from)),
        nullable_int(numeric_precision(&pg_type).map(i64::from)),
        nullable_int(numeric_scale(&pg_type).map(i64::from)),
        nullable_int(datetime_precision(&pg_type).map(i64::from)),
        text(&ctx.session_info.database),
        text("pg_catalog"),
        text(pg_type_name(&pg_type)),
        text("NO"),
        text("NEVER"),
        text(if is_updatable { "YES" } else { "NO" }),
    ]))
}

fn render_default(catalog: &dyn CatalogManager, default: &ColumnDefault) -> Result<Option<String>> {
    match default {
        ColumnDefault::Const(value) => Ok(Some(render_literal(value))),
        ColumnDefault::Nextval(sequence) => {
            let name = catalog
                .get_sequence(*sequence)?
                .map(|sequence| sequence.name)
                .unwrap_or_else(|| format!("<missing sequence {sequence}>"));
            Ok(Some(format!("nextval('{}')", quote_sql_text(&name))))
        }
        // Typed IR is authoritative, while its retained canonical SQL is exactly
        // what `column_default` should report.
        ColumnDefault::Expr(expression) => Ok(Some(expression.sql.clone())),
    }
}

fn render_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(true) => "TRUE".to_string(),
        Value::Boolean(false) => "FALSE".to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Text(value) => format!("'{}'", quote_sql_text(value)),
        Value::Float(value) => float::format_double(value.get()),
        Value::Real(value) => float::format_real(value.get()),
        Value::Numeric(value) => numeric::format_numeric(value),
        Value::Date(value) => format!("'{}'", datetime::format_date(*value)),
        Value::Timestamp(value) => format!("'{}'", datetime::format_timestamp(*value)),
        Value::Time(value) => format!("'{}'", datetime::format_time(*value)),
        Value::TimestampTz(value) => format!("'{}'", datetime::format_timestamptz(*value)),
        Value::Interval(value) => format!("'{}'", interval::format_interval(value)),
        Value::Bytes(value) => format!("'{}'", bytea::format_hex(value)),
        Value::Uuid(value) => format!("'{}'", uuid::format_uuid(value)),
        Value::Array(array) => format!("ARRAY{:?}", array.elements()),
    }
}

fn quote_sql_text(value: &str) -> String {
    value.replace('\'', "''")
}

fn relation_name(table: &TableSchema) -> String {
    match table.relation_kind {
        RelationKind::User => table.name.clone(),
        RelationKind::Toast { base_table } => format!("pg_toast_{base_table}"),
    }
}

fn row(values: Vec<Value>) -> Row {
    Row { values }
}

fn int(value: i64) -> Value {
    Value::Integer(value)
}

fn nullable_int(value: Option<i64>) -> Value {
    value.map(Value::Integer).unwrap_or(Value::Null)
}

fn text(value: impl Into<String>) -> Value {
    Value::Text(value.into())
}

fn nullable_text(value: Option<String>) -> Value {
    value.map(Value::Text).unwrap_or(Value::Null)
}

fn bool_value(value: bool) -> Value {
    Value::Boolean(value)
}

fn real(value: f32) -> Value {
    Value::Real(OrderedF32::new(value))
}

fn timestamp_tz(value: Option<i64>) -> Value {
    value.map(Value::TimestampTz).unwrap_or(Value::Null)
}

fn sort_rows_by_key<K: Ord>(
    rows: &mut Vec<Row>,
    mut key: impl FnMut(&Row) -> Result<K>,
) -> Result<()> {
    let mut keyed = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        keyed.push((key(&row)?, row));
    }
    keyed.sort_by(|(left, _), (right, _)| left.cmp(right));
    rows.extend(keyed.into_iter().map(|(_, row)| row));
    Ok(())
}

fn integer_at(row: &Row, index: usize) -> Result<i64> {
    match row.values.get(index) {
        Some(Value::Integer(value)) => Ok(*value),
        Some(other) => Err(common::DbError::internal(format!(
            "expected integer at system row slot {index}, got {other:?}"
        ))),
        None => Err(common::DbError::internal(format!(
            "system row is missing integer slot {index}"
        ))),
    }
}

fn text_at(row: &Row, index: usize) -> Result<&str> {
    match row.values.get(index) {
        Some(Value::Text(value)) => Ok(value),
        Some(other) => Err(common::DbError::internal(format!(
            "expected text at system row slot {index}, got {other:?}"
        ))),
        None => Err(common::DbError::internal(format!(
            "system row is missing text slot {index}"
        ))),
    }
}

fn isolation_setting(level: IsolationLevel) -> &'static str {
    match level {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
        IsolationLevel::Serializable => "serializable",
    }
}

struct ConstraintRow {
    oid: i64,
    name: String,
    namespace_oid: i64,
    kind: &'static str,
    table_oid: i64,
    index_oid: i64,
    key_columns: Option<String>,
    referenced_table_oid: i64,
    update_action: &'static str,
    delete_action: &'static str,
    referenced_columns: Option<String>,
    expression: Option<String>,
}

fn foreign_key_action_code(action: ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::NoAction => "a",
        ForeignKeyAction::Restrict => "r",
    }
}

fn attnums_array_text(columns: &[u16]) -> String {
    let attnums = columns
        .iter()
        .map(|column| (i64::from(*column) + 1).to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{attnums}}}")
}

fn stable_attnums(
    table: &TableSchema,
    columns: &[common::ColumnObjectId],
) -> Result<Vec<common::ColumnId>> {
    columns
        .iter()
        .map(|column| {
            table.dense_column_id(*column).ok_or_else(|| {
                DbError::internal(format!(
                    "constraint references missing stable column id {column} on table {}",
                    table.name
                ))
            })
        })
        .collect()
}

fn primary_key_index_name(table: &TableSchema) -> String {
    format!("{}_pkey", relation_name(table))
}

struct TypeEntry {
    pg_type: PgType,
    name: &'static str,
    category: &'static str,
    byval: bool,
    element_oid: i64,
    array_oid: i64,
}

fn type_entries() -> Result<Vec<TypeEntry>> {
    let mut entries = vec![
        type_entry(PgType::Bool, "bool", "B", true, 0, 1000),
        type_entry(PgType::Bytea, "bytea", "U", false, 0, 1001),
        type_entry(PgType::Int8, "int8", "N", true, 0, 1016),
        type_entry(PgType::Int2, "int2", "N", true, 0, 1005),
        type_entry(PgType::Int2Vector, "int2vector", "A", false, 21, 0),
        type_entry(PgType::Int4, "int4", "N", true, 0, 1007),
        type_entry(PgType::Text, "text", "S", false, 0, 1009),
        type_entry(PgType::Oid, "oid", "N", true, 0, 1028),
        type_entry(PgType::OidVector, "oidvector", "A", false, 26, 0),
        type_entry(PgType::Float4, "float4", "N", true, 0, 1021),
        type_entry(PgType::Float8, "float8", "N", true, 0, 1022),
        type_entry(PgType::CatalogInt2ArrayText, "_int2", "A", false, 21, 0),
        type_entry(PgType::CatalogOidArrayText, "_oid", "A", false, 26, 0),
        type_entry(PgType::Bpchar(None), "bpchar", "S", false, 0, 1014),
        type_entry(PgType::Varchar(None), "varchar", "S", false, 0, 1015),
        type_entry(PgType::Date, "date", "D", true, 0, 1182),
        type_entry(PgType::Time, "time", "D", true, 0, 1183),
        type_entry(PgType::Timestamp, "timestamp", "D", true, 0, 1115),
        type_entry(PgType::Timestamptz, "timestamptz", "D", true, 0, 1185),
        type_entry(PgType::Interval, "interval", "T", false, 0, 1187),
        type_entry(
            PgType::Numeric {
                precision: None,
                scale: 0,
            },
            "numeric",
            "N",
            false,
            0,
            1231,
        ),
        type_entry(PgType::Uuid, "uuid", "U", false, 0, 2951),
    ];
    let array_types = [
        (PgType::Bool, "_bool"),
        (PgType::Bytea, "_bytea"),
        (PgType::Int8, "_int8"),
        (PgType::Int4, "_int4"),
        (PgType::Text, "_text"),
        (PgType::Float4, "_float4"),
        (PgType::Float8, "_float8"),
        (PgType::Bpchar(None), "_bpchar"),
        (PgType::Varchar(None), "_varchar"),
        (PgType::Date, "_date"),
        (PgType::Time, "_time"),
        (PgType::Timestamp, "_timestamp"),
        (PgType::Timestamptz, "_timestamptz"),
        (PgType::Interval, "_interval"),
        (
            PgType::Numeric {
                precision: None,
                scale: 0,
            },
            "_numeric",
        ),
        (PgType::Uuid, "_uuid"),
    ];
    for (element, name) in array_types {
        entries.push(array_type_entry(element, name)?);
    }
    Ok(entries)
}

fn array_type_entry(element: PgType, name: &'static str) -> Result<TypeEntry> {
    let element_oid = i64::from(element.oid());
    Ok(type_entry(
        PgType::array(element)?,
        name,
        "A",
        false,
        element_oid,
        0,
    ))
}

fn type_entry(
    pg_type: PgType,
    name: &'static str,
    category: &'static str,
    byval: bool,
    element_oid: i64,
    array_oid: i64,
) -> TypeEntry {
    TypeEntry {
        pg_type,
        name,
        category,
        byval,
        element_oid,
        array_oid,
    }
}

fn type_byval(pg_type: &PgType) -> bool {
    matches!(
        pg_type,
        PgType::Bool
            | PgType::Int2
            | PgType::Int4
            | PgType::Int8
            | PgType::Oid
            | PgType::Float4
            | PgType::Float8
            | PgType::Date
            | PgType::Time
            | PgType::Timestamp
            | PgType::Timestamptz
    )
}

fn type_align(pg_type: &PgType) -> &'static str {
    match pg_type {
        PgType::Bool => "c",
        PgType::Int2 => "s",
        PgType::Int8 | PgType::Float8 | PgType::Time | PgType::Timestamp | PgType::Timestamptz => {
            "d"
        }
        _ => "i",
    }
}

fn type_storage(pg_type: &PgType) -> &'static str {
    match pg_type {
        PgType::Numeric { .. }
        | PgType::Text
        | PgType::Varchar(_)
        | PgType::Bpchar(_)
        | PgType::Bytea
        | PgType::OidVector
        | PgType::Int2Vector
        | PgType::CatalogOidArrayText
        | PgType::CatalogInt2ArrayText
        | PgType::Array(_) => "x",
        _ => "p",
    }
}

fn type_collation_oid(pg_type: &PgType) -> i64 {
    match pg_type {
        PgType::Text | PgType::Varchar(_) | PgType::Bpchar(_) => DEFAULT_COLLATION_OID,
        _ => 0,
    }
}

fn sql_data_type(pg_type: &PgType) -> String {
    match pg_type {
        PgType::Array(_) => return pg_type.format_type_name(),
        PgType::Int2 => "smallint",
        PgType::Int4 => "integer",
        PgType::Int8 => "bigint",
        PgType::Oid => "oid",
        PgType::Bool => "boolean",
        PgType::Float4 => "real",
        PgType::Float8 => "double precision",
        PgType::Numeric { .. } => "numeric",
        PgType::Text => "text",
        PgType::Varchar(_) => "character varying",
        PgType::Bpchar(_) => "character",
        PgType::Bytea => "bytea",
        PgType::Uuid => "uuid",
        PgType::Date => "date",
        PgType::Time => "time without time zone",
        PgType::Timestamp => "timestamp without time zone",
        PgType::Timestamptz => "timestamp with time zone",
        PgType::Interval => "interval",
        PgType::OidVector => "oidvector",
        PgType::Int2Vector => "int2vector",
        PgType::CatalogOidArrayText => "oid[]",
        PgType::CatalogInt2ArrayText => "smallint[]",
    }
    .to_string()
}

fn pg_type_name(pg_type: &PgType) -> String {
    match pg_type {
        PgType::Array(element) => return format!("_{}", pg_type_name(element.element_type())),
        PgType::Int2 => "int2",
        PgType::Int4 => "int4",
        PgType::Int8 => "int8",
        PgType::Oid => "oid",
        PgType::Bool => "bool",
        PgType::Float4 => "float4",
        PgType::Float8 => "float8",
        PgType::Numeric { .. } => "numeric",
        PgType::Text => "text",
        PgType::Varchar(_) => "varchar",
        PgType::Bpchar(_) => "bpchar",
        PgType::Bytea => "bytea",
        PgType::Uuid => "uuid",
        PgType::Date => "date",
        PgType::Time => "time",
        PgType::Timestamp => "timestamp",
        PgType::Timestamptz => "timestamptz",
        PgType::Interval => "interval",
        PgType::OidVector => "oidvector",
        PgType::Int2Vector => "int2vector",
        PgType::CatalogOidArrayText => "_oid",
        PgType::CatalogInt2ArrayText => "_int2",
    }
    .to_string()
}

fn character_maximum_length(pg_type: &PgType) -> Option<u32> {
    match pg_type {
        PgType::Varchar(length) | PgType::Bpchar(length) => *length,
        _ => None,
    }
}

fn numeric_precision(pg_type: &PgType) -> Option<u32> {
    match pg_type {
        PgType::Int2 => Some(16),
        PgType::Oid => Some(32),
        PgType::Int4 => Some(32),
        PgType::Int8 => Some(64),
        PgType::Float4 => Some(24),
        PgType::Float8 => Some(53),
        PgType::Numeric { precision, .. } => *precision,
        _ => None,
    }
}

fn numeric_scale(pg_type: &PgType) -> Option<u32> {
    match pg_type {
        PgType::Numeric {
            precision: Some(_),
            scale,
        } => Some(*scale),
        _ => None,
    }
}

fn datetime_precision(pg_type: &PgType) -> Option<u32> {
    match pg_type {
        PgType::Time | PgType::Timestamp | PgType::Timestamptz | PgType::Interval => Some(6),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use common::{Row, SqlState, Value};

    use super::{integer_at, non_empty_array_text, sort_rows_by_key, text_at};

    fn array_text(elements: &[&str]) -> Option<String> {
        non_empty_array_text(elements.iter().map(|element| element.to_string()))
    }

    #[test]
    fn array_text_quotes_per_postgres_array_output_rules() {
        // Plain elements stay bare.
        assert_eq!(array_text(&["a", "b2", "0.25"]).unwrap(), "{a,b2,0.25}");
        // Whitespace, commas, and braces force quotes.
        assert_eq!(
            array_text(&["Jo hn", "Sm,ith", "{x}", "a}b"]).unwrap(),
            r#"{"Jo hn","Sm,ith","{x}","a}b"}"#
        );
        // Embedded quotes and backslashes are backslash-escaped inside quotes.
        assert_eq!(
            array_text(&[r#"say "hi""#, r"back\slash"]).unwrap(),
            r#"{"say \"hi\"","back\\slash"}"#
        );
        // Empty strings and the literal word NULL (any case) must be quoted so
        // they cannot be misread as SQL NULL elements.
        assert_eq!(
            array_text(&["", "NULL", "null"]).unwrap(),
            r#"{"","NULL","null"}"#
        );
        // No elements: SQL NULL, not an empty array literal.
        assert_eq!(array_text(&[]), None);
    }

    #[test]
    fn system_row_accessors_return_internal_errors_for_malformed_rows() {
        let wrong_type = Row {
            values: vec![Value::Text("not an integer".to_string())],
        };
        assert!(matches!(
            integer_at(&wrong_type, 0),
            Err(err) if err.code == SqlState::InternalError
        ));

        let missing_slot = Row { values: vec![] };
        assert!(matches!(
            text_at(&missing_slot, 0),
            Err(err) if err.code == SqlState::InternalError
        ));
    }

    #[test]
    fn fallible_system_row_sort_propagates_key_errors() {
        let mut rows = vec![Row {
            values: vec![Value::Text("not an integer".to_string())],
        }];
        assert!(matches!(
            sort_rows_by_key(&mut rows, |row| integer_at(row, 0)),
            Err(err) if err.code == SqlState::InternalError
        ));
    }
}
