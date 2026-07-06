use common::{ColumnDef, DataType, PgType};

pub const PG_CATALOG_SCHEMA_OID: i64 = 11;
pub const PUBLIC_SCHEMA_OID: i64 = 2200;
pub const INFORMATION_SCHEMA_OID: i64 = 13_000;

const VIRTUAL_OID_TAG_SHIFT: u32 = 40;
const USER_TABLE_OID_TAG: i64 = 1_i64 << VIRTUAL_OID_TAG_SHIFT;
const USER_INDEX_OID_TAG: i64 = 2_i64 << VIRTUAL_OID_TAG_SHIFT;
const USER_SEQUENCE_OID_TAG: i64 = 3_i64 << VIRTUAL_OID_TAG_SHIFT;
const SYNTHETIC_PRIMARY_KEY_OID_TAG: i64 = 4_i64 << VIRTUAL_OID_TAG_SHIFT;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SystemSchema {
    PgCatalog,
    InformationSchema,
}

impl SystemSchema {
    pub fn name(self) -> &'static str {
        match self {
            SystemSchema::PgCatalog => "pg_catalog",
            SystemSchema::InformationSchema => "information_schema",
        }
    }

    pub fn oid(self) -> i64 {
        match self {
            SystemSchema::PgCatalog => PG_CATALOG_SCHEMA_OID,
            SystemSchema::InformationSchema => INFORMATION_SCHEMA_OID,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("pg_catalog") {
            Some(SystemSchema::PgCatalog)
        } else if name.eq_ignore_ascii_case("information_schema") {
            Some(SystemSchema::InformationSchema)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SystemView {
    PgNamespace,
    PgClass,
    PgAttribute,
    PgType,
    PgIndex,
    PgSettings,
    PgStatActivity,
    InformationSchemaSchemata,
    InformationSchemaTables,
    InformationSchemaColumns,
}

impl SystemView {
    pub const ALL: &'static [SystemView] = &[
        SystemView::PgNamespace,
        SystemView::PgClass,
        SystemView::PgAttribute,
        SystemView::PgType,
        SystemView::PgIndex,
        SystemView::PgSettings,
        SystemView::PgStatActivity,
        SystemView::InformationSchemaSchemata,
        SystemView::InformationSchemaTables,
        SystemView::InformationSchemaColumns,
    ];

    pub fn schema(self) -> SystemSchema {
        match self {
            SystemView::PgNamespace
            | SystemView::PgClass
            | SystemView::PgAttribute
            | SystemView::PgType
            | SystemView::PgIndex
            | SystemView::PgSettings
            | SystemView::PgStatActivity => SystemSchema::PgCatalog,
            SystemView::InformationSchemaSchemata
            | SystemView::InformationSchemaTables
            | SystemView::InformationSchemaColumns => SystemSchema::InformationSchema,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SystemView::PgNamespace => "pg_namespace",
            SystemView::PgClass => "pg_class",
            SystemView::PgAttribute => "pg_attribute",
            SystemView::PgType => "pg_type",
            SystemView::PgIndex => "pg_index",
            SystemView::PgSettings => "pg_settings",
            SystemView::PgStatActivity => "pg_stat_activity",
            SystemView::InformationSchemaSchemata => "schemata",
            SystemView::InformationSchemaTables => "tables",
            SystemView::InformationSchemaColumns => "columns",
        }
    }

    pub fn qualified_name(self) -> String {
        format!("{}.{}", self.schema().name(), self.name())
    }

    pub fn relation_oid(self) -> i64 {
        match self {
            SystemView::PgClass => 1259,
            SystemView::PgAttribute => 1249,
            SystemView::PgType => 1247,
            SystemView::PgNamespace => 2615,
            SystemView::PgIndex => 2610,
            SystemView::PgSettings => 13_100,
            SystemView::PgStatActivity => 13_101,
            SystemView::InformationSchemaSchemata => 13_200,
            SystemView::InformationSchemaTables => 13_201,
            SystemView::InformationSchemaColumns => 13_202,
        }
    }

    pub fn columns(self) -> Vec<ColumnDef> {
        match self {
            SystemView::PgNamespace => vec![
                oid_col(0, "oid"),
                name_col(1, "nspname"),
                oid_col(2, "nspowner"),
            ],
            SystemView::PgClass => vec![
                oid_col(0, "oid"),
                name_col(1, "relname"),
                oid_col(2, "relnamespace"),
                oid_col(3, "reltype"),
                oid_col(4, "relowner"),
                oid_col(5, "relam"),
                oid_col(6, "relfilenode"),
                oid_col(7, "reltablespace"),
                int4_col(8, "relpages"),
                col(9, "reltuples", DataType::Real, PgType::Float4, false),
                int4_col(10, "relallvisible"),
                oid_col(11, "reltoastrelid"),
                bool_col(12, "relhasindex"),
                bool_col(13, "relisshared"),
                text_col(14, "relpersistence"),
                text_col(15, "relkind"),
                int4_col(16, "relnatts"),
                int4_col(17, "relchecks"),
                bool_col(18, "relhasrules"),
                bool_col(19, "relhastriggers"),
                bool_col(20, "relhassubclass"),
                bool_col(21, "relrowsecurity"),
                bool_col(22, "relforcerowsecurity"),
                bool_col(23, "relispopulated"),
                text_col(24, "relreplident"),
                bool_col(25, "relispartition"),
            ],
            SystemView::PgAttribute => vec![
                oid_col(0, "attrelid"),
                name_col(1, "attname"),
                oid_col(2, "atttypid"),
                int4_col(3, "attstattarget"),
                int4_col(4, "attlen"),
                int4_col(5, "attnum"),
                int4_col(6, "atttypmod"),
                bool_col(7, "attnotnull"),
                bool_col(8, "atthasdef"),
                text_col(9, "attidentity"),
                text_col(10, "attgenerated"),
                bool_col(11, "attisdropped"),
            ],
            SystemView::PgType => vec![
                oid_col(0, "oid"),
                name_col(1, "typname"),
                oid_col(2, "typnamespace"),
                oid_col(3, "typowner"),
                int4_col(4, "typlen"),
                bool_col(5, "typbyval"),
                text_col(6, "typtype"),
                text_col(7, "typcategory"),
                bool_col(8, "typisdefined"),
                text_col(9, "typdelim"),
                oid_col(10, "typrelid"),
                oid_col(11, "typelem"),
                oid_col(12, "typarray"),
                bool_col(13, "typnotnull"),
                oid_col(14, "typbasetype"),
            ],
            SystemView::PgIndex => vec![
                oid_col(0, "indexrelid"),
                oid_col(1, "indrelid"),
                int4_col(2, "indnatts"),
                int4_col(3, "indnkeyatts"),
                bool_col(4, "indisunique"),
                bool_col(5, "indisprimary"),
                bool_col(6, "indisexclusion"),
                bool_col(7, "indimmediate"),
                bool_col(8, "indisclustered"),
                bool_col(9, "indisvalid"),
                bool_col(10, "indisready"),
                bool_col(11, "indislive"),
                bool_col(12, "indisreplident"),
                text_col(13, "indkey"),
            ],
            SystemView::PgSettings => vec![
                name_col(0, "name"),
                text_col(1, "setting"),
                nullable_text_col(2, "unit"),
                nullable_text_col(3, "category"),
                nullable_text_col(4, "short_desc"),
                text_col(5, "context"),
                text_col(6, "vartype"),
                text_col(7, "source"),
                text_col(8, "boot_val"),
                text_col(9, "reset_val"),
                bool_col(10, "pending_restart"),
            ],
            SystemView::PgStatActivity => vec![
                oid_col(0, "datid"),
                name_col(1, "datname"),
                int4_col(2, "pid"),
                oid_col(3, "usesysid"),
                name_col(4, "usename"),
                text_col(5, "application_name"),
                nullable_text_col(6, "client_addr"),
                nullable_int4_col(7, "client_port"),
                timestamptz_col(8, "backend_start", false),
                timestamptz_col(9, "xact_start", true),
                timestamptz_col(10, "query_start", true),
                timestamptz_col(11, "state_change", true),
                nullable_text_col(12, "wait_event_type"),
                nullable_text_col(13, "wait_event"),
                text_col(14, "state"),
                text_col(15, "query"),
                text_col(16, "backend_type"),
            ],
            SystemView::InformationSchemaSchemata => vec![
                text_col(0, "catalog_name"),
                name_col(1, "schema_name"),
                name_col(2, "schema_owner"),
                nullable_text_col(3, "default_character_set_catalog"),
                nullable_text_col(4, "default_character_set_schema"),
                nullable_text_col(5, "default_character_set_name"),
                nullable_text_col(6, "sql_path"),
            ],
            SystemView::InformationSchemaTables => vec![
                text_col(0, "table_catalog"),
                name_col(1, "table_schema"),
                name_col(2, "table_name"),
                text_col(3, "table_type"),
                nullable_text_col(4, "self_referencing_column_name"),
                nullable_text_col(5, "reference_generation"),
                nullable_text_col(6, "user_defined_type_catalog"),
                nullable_text_col(7, "user_defined_type_schema"),
                nullable_text_col(8, "user_defined_type_name"),
                text_col(9, "is_insertable_into"),
                text_col(10, "is_typed"),
                nullable_text_col(11, "commit_action"),
            ],
            SystemView::InformationSchemaColumns => vec![
                text_col(0, "table_catalog"),
                name_col(1, "table_schema"),
                name_col(2, "table_name"),
                name_col(3, "column_name"),
                int4_col(4, "ordinal_position"),
                nullable_text_col(5, "column_default"),
                text_col(6, "is_nullable"),
                text_col(7, "data_type"),
                nullable_int4_col(8, "character_maximum_length"),
                nullable_int4_col(9, "numeric_precision"),
                nullable_int4_col(10, "numeric_scale"),
                nullable_int4_col(11, "datetime_precision"),
                text_col(12, "udt_catalog"),
                name_col(13, "udt_schema"),
                name_col(14, "udt_name"),
                text_col(15, "is_identity"),
                text_col(16, "is_generated"),
                text_col(17, "is_updatable"),
            ],
        }
    }
}

pub fn resolve_system_view(schema: Option<&str>, name: &str) -> Option<SystemView> {
    SystemView::ALL.iter().copied().find(|view| {
        if !view.name().eq_ignore_ascii_case(name) {
            return false;
        }
        match schema {
            Some(schema) => view.schema().name().eq_ignore_ascii_case(schema),
            None => view.schema() == SystemSchema::PgCatalog,
        }
    })
}

pub fn is_system_schema(name: &str) -> bool {
    SystemSchema::from_name(name).is_some()
}

pub fn table_oid(table_id: u32) -> i64 {
    USER_TABLE_OID_TAG | i64::from(table_id)
}

pub fn index_oid(index_id: u32) -> i64 {
    USER_INDEX_OID_TAG | i64::from(index_id)
}

pub fn sequence_oid(sequence_id: u32) -> i64 {
    USER_SEQUENCE_OID_TAG | i64::from(sequence_id)
}

pub fn synthetic_primary_key_oid(table_id: u32) -> i64 {
    SYNTHETIC_PRIMARY_KEY_OID_TAG | i64::from(table_id)
}

fn col(id: u16, name: &str, data_type: DataType, pg_type: PgType, nullable: bool) -> ColumnDef {
    ColumnDef {
        id,
        name: name.to_string(),
        data_type,
        nullable,
        max_length: None,
        default: None,
        pg_type: Some(pg_type),
    }
}

fn int4_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Integer, PgType::Int4, false)
}

fn nullable_int4_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Integer, PgType::Int4, true)
}

fn oid_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Integer, PgType::Int8, false)
}

fn name_col(id: u16, name: &str) -> ColumnDef {
    text_col(id, name)
}

fn text_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Text, PgType::Text, false)
}

fn nullable_text_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Text, PgType::Text, true)
}

fn bool_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Boolean, PgType::Bool, false)
}

fn timestamptz_col(id: u16, name: &str, nullable: bool) -> ColumnDef {
    col(
        id,
        name,
        DataType::TimestampTz,
        PgType::Timestamptz,
        nullable,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn resolve_matrix_matches_schema_rules() {
        assert_eq!(
            resolve_system_view(None, "pg_class"),
            Some(SystemView::PgClass)
        );
        assert_eq!(
            resolve_system_view(Some("pg_catalog"), "pg_class"),
            Some(SystemView::PgClass)
        );
        assert_eq!(
            resolve_system_view(Some("information_schema"), "columns"),
            Some(SystemView::InformationSchemaColumns)
        );
        assert_eq!(resolve_system_view(None, "columns"), None);
        assert_eq!(resolve_system_view(Some("public"), "pg_class"), None);
        assert_eq!(resolve_system_view(Some("nosuch"), "pg_class"), None);
        assert!(is_system_schema("PG_CATALOG"));
    }

    #[test]
    fn view_oids_and_names_are_unique() {
        let mut qualified_names = HashSet::new();
        let mut oids = HashSet::new();
        for view in SystemView::ALL {
            assert!(qualified_names.insert(view.qualified_name()));
            assert!(oids.insert(view.relation_oid()));
        }
    }

    #[test]
    fn column_ids_are_dense_and_names_are_unique_per_view() {
        for view in SystemView::ALL {
            let columns = view.columns();
            let mut names = HashSet::new();
            for (index, column) in columns.iter().enumerate() {
                assert_eq!(column.id, index as u16, "column id for {view:?}");
                assert!(names.insert(column.name.clone()), "duplicate in {view:?}");
            }
        }
    }

    #[test]
    fn column_data_types_match_pg_wire_types() {
        for view in SystemView::ALL {
            for column in view.columns() {
                assert_eq!(
                    column.data_type,
                    column.wire_type().data_type(),
                    "{}.{}",
                    view.qualified_name(),
                    column.name
                );
            }
        }
    }

    #[test]
    fn oid_spaces_are_disjoint_for_same_raw_id() {
        for id in [0, 7, u32::MAX] {
            let oids = [
                table_oid(id),
                index_oid(id),
                sequence_oid(id),
                synthetic_primary_key_oid(id),
            ];
            assert_eq!(
                oids.iter().copied().collect::<HashSet<_>>().len(),
                oids.len()
            );
        }
    }
}
