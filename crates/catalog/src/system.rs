use common::{ColumnDef, DataType, PgType};

pub const PG_CATALOG_SCHEMA_OID: i64 = 11;
pub const PUBLIC_SCHEMA_OID: i64 = 2200;
pub const INFORMATION_SCHEMA_OID: i64 = 13_000;

const VIRTUAL_OID_TAG_SHIFT: u32 = 28;
pub const MAX_VIRTUAL_OID_PAYLOAD: u32 = (1 << VIRTUAL_OID_TAG_SHIFT) - 1;
const COMPOUND_OID_SUB_ID_BITS: u32 = 12;
pub const MAX_COMPOUND_OID_TABLE_ID: u32 =
    (1 << (VIRTUAL_OID_TAG_SHIFT - COMPOUND_OID_SUB_ID_BITS)) - 1;
pub const MAX_COMPOUND_OID_SUB_ID: u16 = ((1 << COMPOUND_OID_SUB_ID_BITS) - 1) as u16;
const USER_TABLE_OID_TAG: i64 = 1_i64 << VIRTUAL_OID_TAG_SHIFT;
const USER_INDEX_OID_TAG: i64 = 2_i64 << VIRTUAL_OID_TAG_SHIFT;
const USER_SEQUENCE_OID_TAG: i64 = 3_i64 << VIRTUAL_OID_TAG_SHIFT;
const SYNTHETIC_PRIMARY_KEY_OID_TAG: i64 = 4_i64 << VIRTUAL_OID_TAG_SHIFT;
const CONSTRAINT_OID_TAG: i64 = 5_i64 << VIRTUAL_OID_TAG_SHIFT;
const ATTRDEF_OID_TAG: i64 = 6_i64 << VIRTUAL_OID_TAG_SHIFT;

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
    PgProc,
    PgConstraint,
    PgAttrdef,
    PgDepend,
    PgDatabase,
    PgRoles,
    PgSettings,
    PgStatActivity,
    PgStats,
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
        SystemView::PgProc,
        SystemView::PgConstraint,
        SystemView::PgAttrdef,
        SystemView::PgDepend,
        SystemView::PgDatabase,
        SystemView::PgRoles,
        SystemView::PgSettings,
        SystemView::PgStatActivity,
        SystemView::PgStats,
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
            | SystemView::PgProc
            | SystemView::PgConstraint
            | SystemView::PgAttrdef
            | SystemView::PgDepend
            | SystemView::PgDatabase
            | SystemView::PgRoles
            | SystemView::PgSettings
            | SystemView::PgStatActivity
            | SystemView::PgStats => SystemSchema::PgCatalog,
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
            SystemView::PgProc => "pg_proc",
            SystemView::PgConstraint => "pg_constraint",
            SystemView::PgAttrdef => "pg_attrdef",
            SystemView::PgDepend => "pg_depend",
            SystemView::PgDatabase => "pg_database",
            SystemView::PgRoles => "pg_roles",
            SystemView::PgSettings => "pg_settings",
            SystemView::PgStatActivity => "pg_stat_activity",
            SystemView::PgStats => "pg_stats",
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
            SystemView::PgProc => 1255,
            SystemView::PgDatabase => 1262,
            SystemView::PgNamespace => 2615,
            SystemView::PgIndex => 2610,
            SystemView::PgConstraint => 2606,
            SystemView::PgAttrdef => 2604,
            SystemView::PgDepend => 2608,
            SystemView::PgSettings => 13_100,
            SystemView::PgStatActivity => 13_101,
            SystemView::PgRoles => 13_102,
            SystemView::PgStats => 13_103,
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
                int2_col(4, "attlen"),
                int2_col(5, "attnum"),
                int4_col(6, "attndims"),
                int4_col(7, "attcacheoff"),
                int4_col(8, "atttypmod"),
                bool_col(9, "attbyval"),
                text_col(10, "attalign"),
                text_col(11, "attstorage"),
                text_col(12, "attcompression"),
                bool_col(13, "attnotnull"),
                bool_col(14, "atthasdef"),
                bool_col(15, "atthasmissing"),
                text_col(16, "attidentity"),
                text_col(17, "attgenerated"),
                bool_col(18, "attisdropped"),
                bool_col(19, "attislocal"),
                int2_col(20, "attinhcount"),
                oid_col(21, "attcollation"),
                nullable_text_col(22, "attacl"),
                nullable_text_col(23, "attoptions"),
                nullable_text_col(24, "attfdwoptions"),
                nullable_text_col(25, "attmissingval"),
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
                int2vector_col(13, "indkey"),
            ],
            SystemView::PgProc => vec![
                oid_col(0, "oid"),
                name_col(1, "proname"),
                oid_col(2, "pronamespace"),
                oid_col(3, "proowner"),
                oid_col(4, "prolang"),
                real_col(5, "procost"),
                real_col(6, "prorows"),
                oid_col(7, "provariadic"),
                oid_col(8, "prosupport"),
                text_col(9, "prokind"),
                bool_col(10, "prosecdef"),
                bool_col(11, "proleakproof"),
                bool_col(12, "proisstrict"),
                bool_col(13, "proretset"),
                text_col(14, "provolatile"),
                text_col(15, "proparallel"),
                int2_col(16, "pronargs"),
                int2_col(17, "pronargdefaults"),
                oid_col(18, "prorettype"),
                oidvector_col(19, "proargtypes"),
                nullable_oid_array_col(20, "proallargtypes"),
                nullable_text_col(21, "proargmodes"),
                nullable_text_col(22, "proargnames"),
                nullable_text_col(23, "proargdefaults"),
                nullable_text_col(24, "protrftypes"),
                text_col(25, "prosrc"),
                nullable_text_col(26, "probin"),
                nullable_text_col(27, "proconfig"),
                nullable_text_col(28, "proacl"),
            ],
            SystemView::PgConstraint => vec![
                oid_col(0, "oid"),
                name_col(1, "conname"),
                oid_col(2, "connamespace"),
                text_col(3, "contype"),
                bool_col(4, "condeferrable"),
                bool_col(5, "condeferred"),
                bool_col(6, "convalidated"),
                oid_col(7, "conrelid"),
                oid_col(8, "contypid"),
                oid_col(9, "conindid"),
                oid_col(10, "conparentid"),
                oid_col(11, "confrelid"),
                text_col(12, "confupdtype"),
                text_col(13, "confdeltype"),
                text_col(14, "confmatchtype"),
                bool_col(15, "conislocal"),
                int4_col(16, "coninhcount"),
                bool_col(17, "connoinherit"),
                nullable_int2_array_col(18, "conkey"),
                nullable_int2_array_col(19, "confkey"),
                nullable_oid_array_col(20, "conpfeqop"),
                nullable_oid_array_col(21, "conppeqop"),
                nullable_oid_array_col(22, "conffeqop"),
                nullable_oid_array_col(23, "conexclop"),
                nullable_text_col(24, "conbin"),
            ],
            SystemView::PgAttrdef => vec![
                oid_col(0, "oid"),
                oid_col(1, "adrelid"),
                int2_col(2, "adnum"),
                text_col(3, "adbin"),
            ],
            SystemView::PgDepend => vec![
                oid_col(0, "classid"),
                oid_col(1, "objid"),
                int4_col(2, "objsubid"),
                oid_col(3, "refclassid"),
                oid_col(4, "refobjid"),
                int4_col(5, "refobjsubid"),
                text_col(6, "deptype"),
            ],
            SystemView::PgDatabase => vec![
                oid_col(0, "oid"),
                name_col(1, "datname"),
                oid_col(2, "datdba"),
                int4_col(3, "encoding"),
                text_col(4, "datlocprovider"),
                bool_col(5, "datistemplate"),
                bool_col(6, "datallowconn"),
                int4_col(7, "datconnlimit"),
                int4_col(8, "datfrozenxid"),
                int4_col(9, "datminmxid"),
                oid_col(10, "dattablespace"),
                text_col(11, "datcollate"),
                text_col(12, "datctype"),
                nullable_text_col(13, "daticulocale"),
                nullable_text_col(14, "datcollversion"),
                nullable_text_col(15, "datacl"),
            ],
            SystemView::PgRoles => vec![
                name_col(0, "rolname"),
                bool_col(1, "rolsuper"),
                bool_col(2, "rolinherit"),
                bool_col(3, "rolcreaterole"),
                bool_col(4, "rolcreatedb"),
                bool_col(5, "rolcanlogin"),
                bool_col(6, "rolreplication"),
                int4_col(7, "rolconnlimit"),
                nullable_text_col(8, "rolpassword"),
                timestamptz_col(9, "rolvaliduntil", true),
                bool_col(10, "rolbypassrls"),
                nullable_text_col(11, "rolconfig"),
                oid_col(12, "oid"),
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
            SystemView::PgStats => vec![
                name_col(0, "schemaname"),
                name_col(1, "tablename"),
                name_col(2, "attname"),
                col(3, "null_frac", DataType::Real, PgType::Float4, false),
                int4_col(4, "avg_width"),
                col(5, "n_distinct", DataType::Real, PgType::Float4, false),
                nullable_text_col(6, "most_common_vals"),
                nullable_text_col(7, "most_common_freqs"),
                nullable_text_col(8, "histogram_bounds"),
                col(9, "correlation", DataType::Real, PgType::Float4, true),
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
    USER_TABLE_OID_TAG | oid_payload(table_id)
}

pub fn index_oid(index_id: u32) -> i64 {
    USER_INDEX_OID_TAG | oid_payload(index_id)
}

pub fn sequence_oid(sequence_id: u32) -> i64 {
    USER_SEQUENCE_OID_TAG | oid_payload(sequence_id)
}

pub fn synthetic_primary_key_oid(table_id: u32) -> i64 {
    SYNTHETIC_PRIMARY_KEY_OID_TAG | oid_payload(table_id)
}

pub fn primary_key_constraint_oid(table_id: u32) -> i64 {
    CONSTRAINT_OID_TAG | compound_oid_payload(table_id, 0)
}

pub fn check_constraint_oid(table_id: u32, check_index: u16) -> i64 {
    CONSTRAINT_OID_TAG | compound_oid_payload(table_id, check_index.saturating_add(1))
}

pub fn attrdef_oid(table_id: u32, column_id: u16) -> i64 {
    ATTRDEF_OID_TAG | compound_oid_payload(table_id, column_id)
}

fn compound_oid_payload(id: u32, sub_id: u16) -> i64 {
    assert!(
        id <= MAX_COMPOUND_OID_TABLE_ID,
        "catalog table id {id} exceeds compound virtual OID table-id limit"
    );
    assert!(
        sub_id <= MAX_COMPOUND_OID_SUB_ID,
        "catalog object sub-id {sub_id} exceeds compound virtual OID sub-id limit"
    );
    i64::from((id << COMPOUND_OID_SUB_ID_BITS) | u32::from(sub_id))
}

fn oid_payload(id: u32) -> i64 {
    assert!(
        id <= MAX_VIRTUAL_OID_PAYLOAD,
        "catalog id {id} exceeds virtual OID payload limit"
    );
    i64::from(id)
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

fn int2_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Integer, PgType::Int2, false)
}

fn nullable_int4_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Integer, PgType::Int4, true)
}

fn oid_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Integer, PgType::Oid, false)
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

fn oidvector_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Text, PgType::OidVector, false)
}

fn int2vector_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Text, PgType::Int2Vector, false)
}

fn nullable_oid_array_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Text, PgType::CatalogOidArrayText, true)
}

fn nullable_int2_array_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Text, PgType::CatalogInt2ArrayText, true)
}

fn bool_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Boolean, PgType::Bool, false)
}

fn real_col(id: u16, name: &str) -> ColumnDef {
    col(id, name, DataType::Real, PgType::Float4, false)
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
        for id in [0, 7, MAX_COMPOUND_OID_TABLE_ID] {
            let oids = [
                table_oid(id),
                index_oid(id),
                sequence_oid(id),
                synthetic_primary_key_oid(id),
                primary_key_constraint_oid(id),
                attrdef_oid(id, 7),
            ];
            assert_eq!(
                oids.iter().copied().collect::<HashSet<_>>().len(),
                oids.len()
            );
            for oid in oids {
                assert!(
                    u32::try_from(oid).is_ok(),
                    "virtual oid {oid} must fit PostgreSQL oid"
                );
            }
        }
    }

    #[test]
    fn virtual_oid_helpers_reject_ids_outside_supported_payloads() {
        assert!(std::panic::catch_unwind(|| table_oid(MAX_VIRTUAL_OID_PAYLOAD + 1)).is_err());
        assert!(
            std::panic::catch_unwind(|| primary_key_constraint_oid(MAX_COMPOUND_OID_TABLE_ID + 1))
                .is_err()
        );
        assert!(std::panic::catch_unwind(|| attrdef_oid(1, MAX_COMPOUND_OID_SUB_ID + 1)).is_err());
    }
}
