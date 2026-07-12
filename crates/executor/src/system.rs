use std::collections::BTreeMap;

use catalog::{
    CatalogManager, INFORMATION_SCHEMA_OID, PG_CATALOG_SCHEMA_OID, PUBLIC_SCHEMA_OID, SystemView,
    attrdef_oid, check_constraint_oid, index_oid, primary_key_constraint_oid, sequence_oid,
    synthetic_primary_key_oid, table_oid,
};
use common::{
    ColumnDef, ColumnDefault, GucSetting, IndexConstraintKind, IsolationLevel, OrderedF32,
    PgProcCatalogEntry, PgType, RelationKind, Result, Row, SequenceSchema, SessionActivityRow,
    StatementContext, TableId, TableSchema, Value, ViewSchema, bytea, datetime, float, interval,
    numeric, pg_proc_catalog_entries, uuid,
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
        SystemView::PgNamespace => Ok(pg_namespace_rows()),
        SystemView::PgClass => pg_class_rows(catalog),
        SystemView::PgAttribute => pg_attribute_rows(catalog),
        SystemView::PgType => Ok(pg_type_rows()),
        SystemView::PgIndex => pg_index_rows(catalog),
        SystemView::PgProc => Ok(pg_proc_rows()),
        SystemView::PgConstraint => pg_constraint_rows(catalog),
        SystemView::PgAttrdef => pg_attrdef_rows(catalog),
        SystemView::PgDepend => pg_depend_rows(catalog),
        SystemView::PgDatabase => Ok(pg_database_rows(ctx)),
        SystemView::PgRoles => Ok(pg_roles_rows(ctx)),
        SystemView::PgSettings => Ok(pg_settings_rows(ctx)),
        SystemView::PgStatActivity => Ok(pg_stat_activity_rows(ctx)),
        SystemView::InformationSchemaSchemata => Ok(information_schema_schemata_rows(ctx)),
        SystemView::InformationSchemaTables => information_schema_tables_rows(catalog, ctx),
        SystemView::InformationSchemaColumns => information_schema_columns_rows(catalog, ctx),
    }
}

fn pg_namespace_rows() -> Vec<Row> {
    vec![
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
    ]
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
            .any(|index| index.constraint == IndexConstraintKind::PrimaryKey);
        rows.push(pg_class_table_row(
            table,
            !indexes.is_empty() || (!table.primary_key.is_empty() && !has_primary_key_index),
        ));
        for index in indexes {
            rows.push(pg_class_index_row(
                index_oid(index.id),
                &index.name,
                index.columns.len(),
            ));
        }
        if !has_primary_key_index && !table.primary_key.is_empty() {
            rows.push(pg_class_index_row(
                synthetic_primary_key_oid(table.id),
                &primary_key_index_name(table),
                table.primary_key.len(),
            ));
        }
    }

    for sequence in catalog.list_sequences()? {
        rows.push(pg_class_sequence_row(&sequence));
    }

    for view in catalog.list_views()? {
        rows.push(pg_class_user_view_row(&view));
    }

    for view in SystemView::ALL {
        rows.push(pg_class_view_row(*view));
    }

    rows.sort_by_key(|row| integer_at(row, 0));
    Ok(rows)
}

fn pg_class_table_row(table: &TableSchema, relhasindex: bool) -> Row {
    let oid = table_oid(table.id);
    row(vec![
        int(oid),
        text(relation_name(table)),
        int(PUBLIC_SCHEMA_OID),
        int(0),
        int(OWNER_OID),
        int(0),
        int(oid),
        int(0),
        int(0),
        real(-1.0),
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
        int(table.checks.len() as i64),
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

fn pg_class_index_row(oid: i64, name: &str, natts: usize) -> Row {
    row(vec![
        int(oid),
        text(name),
        int(PUBLIC_SCHEMA_OID),
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
    ])
}

fn pg_class_sequence_row(sequence: &SequenceSchema) -> Row {
    let oid = sequence_oid(sequence.id);
    row(vec![
        int(oid),
        text(&sequence.name),
        int(PUBLIC_SCHEMA_OID),
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
    ])
}

fn pg_class_user_view_row(view: &ViewSchema) -> Row {
    let oid = table_oid(view.id);
    row(vec![
        int(oid),
        text(&view.name),
        int(PUBLIC_SCHEMA_OID),
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
    ])
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
            rows.push(pg_attribute_row(table_oid(table.id), column));
        }
    }
    for view in catalog.list_views()? {
        for column in &view.columns {
            rows.push(pg_attribute_row(table_oid(view.id), column));
        }
    }
    for view in SystemView::ALL {
        for column in view.columns() {
            rows.push(pg_attribute_row(view.relation_oid(), &column));
        }
    }
    rows.sort_by_key(|row| (integer_at(row, 0), integer_at(row, 5)));
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

fn pg_type_rows() -> Vec<Row> {
    type_entries()
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
        .collect()
}

fn pg_index_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let tables = catalog.list_tables()?;
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
            .any(|index| index.constraint == IndexConstraintKind::PrimaryKey);
        if !has_primary_key_index && !table.primary_key.is_empty() {
            rows.push(pg_index_row(
                synthetic_primary_key_oid(table.id),
                table_oid(table.id),
                &table.primary_key,
                true,
                true,
            ));
        }
        for index in indexes {
            if let Some(table) = table_by_id.get(&index.table) {
                rows.push(pg_index_row(
                    index_oid(index.id),
                    table_oid(index.table),
                    &index.columns,
                    index.unique,
                    index.constraint == IndexConstraintKind::PrimaryKey,
                ));
                debug_assert!(index.columns.iter().all(|column| {
                    table
                        .columns
                        .iter()
                        .any(|candidate| candidate.id == *column)
                }));
            }
        }
    }

    rows.sort_by_key(|row| integer_at(row, 0));
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

fn pg_proc_rows() -> Vec<Row> {
    let mut rows: Vec<_> = pg_proc_catalog_entries().iter().map(pg_proc_row).collect();
    rows.sort_by_key(|row| integer_at(row, 0));
    rows
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
    for table in catalog.list_tables()? {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        if !table.primary_key.is_empty() {
            let primary_key_index = catalog
                .list_indexes_for_table(table.id)?
                .into_iter()
                .find(|index| index.constraint == IndexConstraintKind::PrimaryKey);
            let (name, index_oid_value, key_columns) = primary_key_index.map_or_else(
                || {
                    (
                        primary_key_index_name(&table),
                        synthetic_primary_key_oid(table.id),
                        table.primary_key.clone(),
                    )
                },
                |index| (index.name, index_oid(index.id), index.columns),
            );
            rows.push(pg_constraint_row(ConstraintRow {
                oid: primary_key_constraint_oid(table.id),
                name,
                kind: "p",
                table_oid: table_oid(table.id),
                index_oid: index_oid_value,
                key_columns: Some(attnums_array_text(&key_columns)),
                expression: None,
            }));
        }
        for (index, check) in table.checks.iter().enumerate() {
            let check_index: u16 = index.try_into().unwrap_or(u16::MAX);
            rows.push(pg_constraint_row(ConstraintRow {
                oid: check_constraint_oid(table.id, check_index),
                name: check_constraint_name(&table, index),
                kind: "c",
                table_oid: table_oid(table.id),
                index_oid: 0,
                key_columns: None,
                expression: Some(check.clone()),
            }));
        }
    }
    rows.sort_by_key(|row| integer_at(row, 0));
    Ok(rows)
}

fn pg_constraint_row(row_data: ConstraintRow) -> Row {
    row(vec![
        int(row_data.oid),
        text(row_data.name),
        int(PUBLIC_SCHEMA_OID),
        text(row_data.kind),
        bool_value(false),
        bool_value(false),
        bool_value(true),
        int(row_data.table_oid),
        int(0),
        int(row_data.index_oid),
        int(0),
        int(0),
        text("a"),
        text("a"),
        text("s"),
        bool_value(true),
        int(0),
        bool_value(false),
        nullable_text(row_data.key_columns),
        Value::Null,
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
                int(attrdef_oid(table.id, column.id)),
                int(table_oid(table.id)),
                int(i64::from(column.id) + 1),
                text(rendered),
            ]));
        }
    }
    rows.sort_by_key(|row| integer_at(row, 0));
    Ok(rows)
}

fn pg_depend_rows(catalog: &dyn CatalogManager) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for table in catalog.list_tables()? {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        if !table.primary_key.is_empty() {
            rows.push(pg_depend_row(
                SystemView::PgConstraint.relation_oid(),
                primary_key_constraint_oid(table.id),
                0,
                SystemView::PgClass.relation_oid(),
                table_oid(table.id),
                0,
                "a",
            ));
        }
        for (index, _) in table.checks.iter().enumerate() {
            let check_index: u16 = index.try_into().unwrap_or(u16::MAX);
            rows.push(pg_depend_row(
                SystemView::PgConstraint.relation_oid(),
                check_constraint_oid(table.id, check_index),
                0,
                SystemView::PgClass.relation_oid(),
                table_oid(table.id),
                0,
                "a",
            ));
        }
        for column in &table.columns {
            let Some(default) = column.default.as_ref() else {
                continue;
            };
            let attrdef = attrdef_oid(table.id, column.id);
            let attnum = i64::from(column.id) + 1;
            rows.push(pg_depend_row(
                SystemView::PgAttrdef.relation_oid(),
                attrdef,
                0,
                SystemView::PgClass.relation_oid(),
                table_oid(table.id),
                attnum,
                "a",
            ));
            if let ColumnDefault::Nextval(sequence_id) = default {
                let sequence_owned = catalog
                    .get_sequence(*sequence_id)?
                    .as_ref()
                    .is_some_and(|sequence| sequence.owned);
                rows.push(pg_depend_row(
                    SystemView::PgAttrdef.relation_oid(),
                    attrdef,
                    0,
                    SystemView::PgClass.relation_oid(),
                    sequence_oid(*sequence_id),
                    0,
                    "n",
                ));
                rows.push(pg_depend_row(
                    SystemView::PgClass.relation_oid(),
                    sequence_oid(*sequence_id),
                    0,
                    SystemView::PgClass.relation_oid(),
                    table_oid(table.id),
                    attnum,
                    if sequence_owned { "a" } else { "n" },
                ));
            }
        }
    }
    rows.sort_by_key(|row| (integer_at(row, 0), integer_at(row, 1), integer_at(row, 4)));
    Ok(rows)
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

fn information_schema_schemata_rows(ctx: &StatementContext) -> Vec<Row> {
    ["pg_catalog", "public", "information_schema"]
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
        .collect()
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
        rows.push(information_schema_table_row(
            ctx,
            "public",
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
        rows.push(information_schema_table_row(
            ctx, "public", &view.name, "VIEW", "NO",
        ));
    }
    rows.sort_by_key(|row| (text_at(row, 1).to_string(), text_at(row, 2).to_string()));
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
        for column in &table.columns {
            rows.push(information_schema_column_row(
                catalog,
                ctx,
                "public",
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
        for column in &view.columns {
            rows.push(information_schema_column_row(
                catalog, ctx, "public", &view.name, column, false,
            )?);
        }
    }
    rows.sort_by_key(|row| {
        (
            text_at(row, 1).to_string(),
            text_at(row, 2).to_string(),
            integer_at(row, 4),
        )
    });
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
        // A non-constant default is stored as canonical SQL text, which is exactly
        // what `column_default` should report.
        ColumnDefault::Expr(text) => Ok(Some(text.clone())),
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

fn integer_at(row: &Row, index: usize) -> i64 {
    match &row.values[index] {
        Value::Integer(value) => *value,
        other => panic!("expected integer at slot {index}, got {other:?}"),
    }
}

fn text_at(row: &Row, index: usize) -> &str {
    match &row.values[index] {
        Value::Text(value) => value,
        other => panic!("expected text at slot {index}, got {other:?}"),
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
    kind: &'static str,
    table_oid: i64,
    index_oid: i64,
    key_columns: Option<String>,
    expression: Option<String>,
}

fn attnums_array_text(columns: &[u16]) -> String {
    let attnums = columns
        .iter()
        .map(|column| (i64::from(*column) + 1).to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{attnums}}}")
}

fn check_constraint_name(table: &TableSchema, index: usize) -> String {
    if index == 0 {
        format!("{}_check", relation_name(table))
    } else {
        format!("{}_check_{}", relation_name(table), index + 1)
    }
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

fn type_entries() -> Vec<TypeEntry> {
    vec![
        type_entry(PgType::Bool, "bool", "B", true, 0, 0),
        type_entry(PgType::Bytea, "bytea", "U", false, 0, 0),
        type_entry(PgType::Int8, "int8", "N", true, 0, 0),
        type_entry(PgType::Int2, "int2", "N", true, 0, 1005),
        type_entry(PgType::Int2Vector, "int2vector", "A", false, 21, 0),
        type_entry(PgType::Int4, "int4", "N", true, 0, 0),
        type_entry(PgType::Text, "text", "S", false, 0, 0),
        type_entry(PgType::Oid, "oid", "N", true, 0, 1028),
        type_entry(PgType::OidVector, "oidvector", "A", false, 26, 0),
        type_entry(PgType::Float4, "float4", "N", true, 0, 0),
        type_entry(PgType::Float8, "float8", "N", true, 0, 0),
        type_entry(PgType::CatalogInt2ArrayText, "_int2", "A", false, 21, 0),
        type_entry(PgType::CatalogOidArrayText, "_oid", "A", false, 26, 0),
        type_entry(PgType::Bpchar(None), "bpchar", "S", false, 0, 0),
        type_entry(PgType::Varchar(None), "varchar", "S", false, 0, 0),
        type_entry(PgType::Date, "date", "D", true, 0, 0),
        type_entry(PgType::Time, "time", "D", true, 0, 0),
        type_entry(PgType::Timestamp, "timestamp", "D", true, 0, 0),
        type_entry(PgType::Timestamptz, "timestamptz", "D", true, 0, 0),
        type_entry(PgType::Interval, "interval", "T", false, 0, 0),
        type_entry(
            PgType::Numeric {
                precision: None,
                scale: 0,
            },
            "numeric",
            "N",
            false,
            0,
            0,
        ),
        type_entry(PgType::Uuid, "uuid", "U", false, 0, 0),
    ]
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
    if matches!(pg_type, PgType::Array(_)) {
        return pg_type.format_type_name();
    }
    match pg_type {
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
        PgType::Array(_) => unreachable!("handled above"),
    }
    .to_string()
}

fn pg_type_name(pg_type: &PgType) -> String {
    if let PgType::Array(element) = pg_type {
        return format!("_{}", pg_type_name(element.element_type()));
    }
    match pg_type {
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
        PgType::Array(_) => unreachable!("handled above"),
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
