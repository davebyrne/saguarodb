use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    ColumnId, DbError, FileId, IndexId, PUBLIC_SCHEMA_ID, PgType, SchemaId, SequenceId, SqlState,
    TableId, Value,
};

pub const INITIAL_SCHEMA_VERSION: u64 = 1;

pub fn public_schema_id() -> SchemaId {
    PUBLIC_SCHEMA_ID
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QualifiedName {
    pub schema: Option<String>,
    pub name: String,
}

impl QualifiedName {
    pub fn unqualified(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = &self.schema {
            write!(formatter, "{schema}.{}", self.name)
        } else {
            formatter.write_str(&self.name)
        }
    }
}

impl PartialEq<str> for QualifiedName {
    fn eq(&self, other: &str) -> bool {
        self.schema.is_none() && self.name == other
    }
}

impl PartialEq<&str> for QualifiedName {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSchema {
    pub id: SchemaId,
    pub name: String,
}

fn initial_schema_version() -> u64 {
    INITIAL_SCHEMA_VERSION
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    /// `DATE` — calendar date, value carried as `Value::Date` (days from epoch).
    Date,
    /// `TIMESTAMP` (without time zone), value carried as `Value::Timestamp`
    /// (microseconds from epoch).
    Timestamp,
    /// `TIME` (without time zone), value carried as `Value::Time` (microseconds
    /// since midnight).
    Time,
    /// `TIMESTAMP WITH TIME ZONE`, value carried as `Value::TimestampTz`
    /// (microseconds from epoch, UTC-normalized).
    TimestampTz,
    /// `INTERVAL`, value carried as `Value::Interval` (months/days/microseconds).
    Interval,
    /// `BYTEA` — raw byte string, value carried as `Value::Bytes`.
    Bytea,
    /// `UUID` — 128-bit identifier, value carried as `Value::Uuid` (16 bytes).
    Uuid,
    /// `DOUBLE PRECISION` — IEEE 754 binary64, value carried as `Value::Float`.
    Double,
    /// `REAL` — IEEE 754 binary32 (single precision), value carried as `Value::Real`.
    Real,
    /// `NUMERIC` / `DECIMAL` — exact decimal, value carried as `Value::Numeric`.
    /// `precision: None` is unconstrained `NUMERIC` (any precision/scale); `Some(p)`
    /// constrains to `p` total digits with the given `scale` (values are rounded to
    /// `scale` and rejected when the integer part exceeds `p - scale` digits).
    Numeric {
        precision: Option<u32>,
        scale: u32,
    },
    /// A rectangular PostgreSQL array. Multidimensionality belongs to the value's
    /// shape; the boxed type is always a non-array scalar element type.
    Array(ArrayType),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ArrayType(Box<DataType>);

impl ArrayType {
    pub fn new(element_type: DataType) -> crate::Result<Self> {
        if matches!(element_type, DataType::Array(_)) {
            return Err(DbError::plan(
                SqlState::DatatypeMismatch,
                "array elements cannot themselves be arrays",
            ));
        }
        Ok(Self(Box::new(element_type)))
    }

    pub(crate) fn from_validated_scalar(element_type: DataType) -> Self {
        Self(Box::new(element_type))
    }

    #[must_use]
    pub fn element_type(&self) -> &DataType {
        &self.0
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum SerializedDataType {
            Integer,
            Text,
            Boolean,
            Date,
            Timestamp,
            Time,
            TimestampTz,
            Interval,
            Bytea,
            Uuid,
            Double,
            Real,
            Numeric { precision: Option<u32>, scale: u32 },
            Array(Box<SerializedDataType>),
        }

        fn convert<E: serde::de::Error>(value: SerializedDataType) -> Result<DataType, E> {
            let value = match value {
                SerializedDataType::Integer => DataType::Integer,
                SerializedDataType::Text => DataType::Text,
                SerializedDataType::Boolean => DataType::Boolean,
                SerializedDataType::Date => DataType::Date,
                SerializedDataType::Timestamp => DataType::Timestamp,
                SerializedDataType::Time => DataType::Time,
                SerializedDataType::TimestampTz => DataType::TimestampTz,
                SerializedDataType::Interval => DataType::Interval,
                SerializedDataType::Bytea => DataType::Bytea,
                SerializedDataType::Uuid => DataType::Uuid,
                SerializedDataType::Double => DataType::Double,
                SerializedDataType::Real => DataType::Real,
                SerializedDataType::Numeric { precision, scale } => {
                    DataType::Numeric { precision, scale }
                }
                SerializedDataType::Array(element) => {
                    let element = convert::<E>(*element)?;
                    if matches!(element, DataType::Array(_)) {
                        return Err(E::custom("array elements cannot themselves be arrays"));
                    }
                    DataType::Array(ArrayType(Box::new(element)))
                }
            };
            Ok(value)
        }

        convert(SerializedDataType::deserialize(deserializer)?)
    }
}

/// Per-table at-rest page compression setting (`docs/specs/compression.md`
/// §4). Governs only the table's file envelopes; WAL full-page-image
/// compression is unconditional and independent of this setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompressionSetting {
    #[default]
    None,
    Zstd,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToastMode {
    Off,
    Auto,
    Aggressive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToastCompression {
    None,
    Zstd,
    ZstdDict,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToastOptions {
    pub mode: ToastMode,
    pub tuple_target: u32,
    pub min_value_size: u32,
    pub compression: ToastCompression,
    pub active_dict_id: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToastOptionPatch {
    pub mode: Option<ToastMode>,
    pub tuple_target: Option<u32>,
    pub min_value_size: Option<u32>,
    pub compression: Option<ToastCompression>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableOptionPatch {
    pub compression: Option<CompressionSetting>,
    pub toast: ToastOptionPatch,
}

impl ToastOptions {
    pub const DEFAULT_TOAST_TUPLE_TARGET: u32 = 2048;
    pub const MIN_TOAST_TUPLE_TARGET: u32 = 256;
    pub const MAX_TOAST_TUPLE_TARGET: u32 = 8000;
    pub const DEFAULT_TOAST_MIN_VALUE_SIZE: u32 = 1024;
    pub const AGGRESSIVE_TOAST_MIN_VALUE_SIZE: u32 = 256;
    pub const MIN_TOAST_MIN_VALUE_SIZE: u32 = 128;
    pub const MIN_TOAST_COMPRESSION_SAVINGS: usize = 16;

    pub fn default_new_table() -> Self {
        Self {
            mode: ToastMode::Auto,
            tuple_target: Self::DEFAULT_TOAST_TUPLE_TARGET,
            min_value_size: Self::DEFAULT_TOAST_MIN_VALUE_SIZE,
            compression: ToastCompression::ZstdDict,
            active_dict_id: None,
        }
    }

    pub fn legacy_catalog_default() -> Self {
        Self {
            mode: ToastMode::Off,
            tuple_target: Self::DEFAULT_TOAST_TUPLE_TARGET,
            min_value_size: Self::DEFAULT_TOAST_MIN_VALUE_SIZE,
            compression: ToastCompression::None,
            active_dict_id: None,
        }
    }

    pub fn apply_patch(&self, patch: &ToastOptionPatch) -> Self {
        let mut options = self.clone();
        if let Some(mode) = patch.mode {
            options.mode = mode;
            if mode == ToastMode::Aggressive && patch.min_value_size.is_none() {
                options.min_value_size = Self::AGGRESSIVE_TOAST_MIN_VALUE_SIZE;
            }
        }
        if let Some(tuple_target) = patch.tuple_target {
            options.tuple_target = tuple_target;
        }
        if let Some(min_value_size) = patch.min_value_size {
            options.min_value_size = min_value_size;
        }
        if let Some(compression) = patch.compression {
            options.compression = compression;
            options.active_dict_id = None;
        }
        options
    }
}

impl ToastOptionPatch {
    pub fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.tuple_target.is_none()
            && self.min_value_size.is_none()
            && self.compression.is_none()
    }
}

impl TableOptionPatch {
    pub fn is_empty(&self) -> bool {
        self.compression.is_none() && self.toast.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RelationKind {
    #[default]
    User,
    Toast {
        base_table: TableId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParsedDefault {
    Const(Value),
    Nextval(String),
    /// A non-constant `DEFAULT` expression carried as canonical SQL text. The
    /// binder re-parses and binds it at `CREATE TABLE` (to validate) and at each
    /// `INSERT` (to evaluate per row); it may not reference table columns.
    Expr(String),
    /// Internal form used while executing `SERIAL` desugaring. Explicit user
    /// defaults use `Nextval` and may not borrow a serial-owned sequence.
    OwnedNextval(String),
    /// Parse-time marker for a `SERIAL` family column. It is resolved during
    /// `CREATE TABLE` execution into an owned sequence plus `OwnedNextval(name)`.
    Serial,
}

/// A resolved column `DEFAULT`, persisted in the catalog snapshot. The
/// externally-tagged serde form (`{"Const": ...}` / `{"Nextval": id}`) is durable;
/// the enclosing `ColumnDef.default` is `#[serde(default)]`, so a pre-default
/// catalog snapshot still loads (the field reads as `None`). The constant-`DEFAULT`
/// shape landed unreleased on this branch, so no compatibility shim is kept for the
/// brief bare-`Value` form it had before this enum (dev data is resettable per the
/// runtime-data convention).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnDefault {
    Const(Value),
    Nextval(SequenceId),
    /// A non-constant `DEFAULT` expression, persisted as canonical SQL text. The
    /// binder re-parses and binds it against an empty column scope at each
    /// `INSERT`; the executor evaluates the bound form per row.
    Expr(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Maximum length in characters for a bounded character type
    /// (`VARCHAR(n)` / `CHAR(n)`). `None` for unbounded `TEXT` and all
    /// non-character types.
    #[serde(default)]
    pub max_length: Option<u32>,
    /// The column `DEFAULT`, applied when an `INSERT` omits the column. Constants
    /// are folded at parse time; sequence defaults keep a sequence name until the
    /// catalog resolves it to a durable id.
    #[serde(default)]
    pub default: Option<ParsedDefault>,
    /// The declared PostgreSQL wire type (integer width, character kind, length),
    /// captured by the parser so the protocol can report the exact OID/typmod.
    /// `None` when the parser has not labeled the column; resolved to the collapsed
    /// default (`Integer` => int8, `Text` => text) downstream.
    #[serde(default)]
    pub pg_type: Option<PgType>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Maximum length in characters for a bounded character type
    /// (`VARCHAR(n)` / `CHAR(n)`); `None` means unbounded. Enforced at write
    /// time by the executor, not represented as a distinct `DataType`.
    #[serde(default)]
    pub max_length: Option<u32>,
    /// The column `DEFAULT` applied when an `INSERT`/`COPY` omits the column.
    /// Persisted with the catalog and replayed via the `CreateTable` WAL record.
    #[serde(default)]
    pub default: Option<ColumnDefault>,
    /// The declared PostgreSQL wire type, persisted so the column reports the same
    /// OID/typmod after a restart. `None` on catalogs written before this field
    /// existed; use [`ColumnDef::wire_type`] rather than reading this directly.
    #[serde(default)]
    pub pg_type: Option<PgType>,
}

impl ColumnDef {
    /// The column's PostgreSQL wire type, resolving an unlabeled column (`None`,
    /// e.g. a pre-existing catalog snapshot) to the collapsed default derived from
    /// its `DataType` (`Integer` => int8, `Text` => text). This is what the
    /// protocol should report; it is always concrete.
    pub fn wire_type(&self) -> PgType {
        self.pg_type
            .clone()
            .unwrap_or_else(|| PgType::from(&self.data_type))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub table_id: Option<TableId>,
    pub column_id: Option<ColumnId>,
    /// The declared PostgreSQL wire type of this result column. `None` for a
    /// synthetic/computed column, which resolves to the collapsed default from
    /// `data_type`. Use [`ColumnInfo::wire_type`] rather than reading this directly.
    #[serde(default)]
    pub pg_type: Option<PgType>,
}

impl ColumnInfo {
    /// The column's PostgreSQL wire type, resolving an unlabeled column (`None`) to
    /// the collapsed default derived from its `DataType` (`Integer` => int8,
    /// `Text` => text). This is what the protocol reports; it is always concrete.
    pub fn wire_type(&self) -> PgType {
        self.pg_type
            .clone()
            .unwrap_or_else(|| PgType::from(&self.data_type))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// The declared PostgreSQL wire type for this view output column. `None`
    /// resolves to the collapsed default from `data_type`.
    #[serde(default)]
    pub pg_type: Option<PgType>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: TableId,
    #[serde(default = "public_schema_id")]
    pub schema_id: SchemaId,
    /// Physical storage-generation id for this table's heap and primary index.
    /// `0` means a legacy decoded schema is missing the field; catalog migration
    /// replaces it with a non-zero id before installation.
    #[serde(default)]
    pub storage_id: FileId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
    /// Monotonic logical schema version. It starts at 1 for a newly created
    /// relation and increments on public schema metadata changes.
    #[serde(default = "initial_schema_version")]
    pub schema_version: u64,
    /// At-rest page compression for this table's heap and index files.
    #[serde(default)]
    pub compression: CompressionSetting,
    /// The trained dictionary new heap-page writes compress against
    /// (`None` until an ALTER trains one). Index files never use it.
    #[serde(default)]
    pub active_dict_id: Option<u32>,
    /// Storage-private TOAST policy for future writes.
    #[serde(default = "ToastOptions::legacy_catalog_default")]
    pub toast: ToastOptions,
    /// Hidden companion relation for out-of-line toast chunks, when present.
    #[serde(default)]
    pub toast_table_id: Option<TableId>,
    /// User table vs. hidden toast relation metadata.
    #[serde(default)]
    pub relation_kind: RelationKind,
    /// `CHECK` constraint expressions, held as canonical SQL text (column-level
    /// and table-level checks are flattened here, as in PostgreSQL). The binder
    /// re-parses and binds each against the table's columns at `CREATE TABLE` (to
    /// validate) and at each `INSERT`/`UPDATE` (to enforce per row); the executor
    /// rejects a row whose check evaluates to `false` (a `NULL` result passes).
    #[serde(default)]
    pub checks: Vec<String>,
}

/// A durable dependency from a view to another relation. `columns` tracks specific
/// referenced columns. `all_columns` means the view depends on the relation's full
/// column set (for example `SELECT *`) and column-level DDL should treat every
/// column as referenced. A dependency with neither specific columns nor
/// `all_columns` is a relation-existence dependency such as `count(*)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ViewDependency {
    pub relation: TableId,
    pub columns: Vec<ColumnId>,
    pub all_columns: bool,
}

impl<'de> Deserialize<'de> for ViewDependency {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawViewDependency {
            relation: TableId,
            #[serde(default)]
            columns: Vec<ColumnId>,
            all_columns: Option<bool>,
        }

        let raw = RawViewDependency::deserialize(deserializer)?;
        let all_columns = raw.all_columns.unwrap_or(raw.columns.is_empty());
        Ok(Self {
            relation: raw.relation,
            columns: raw.columns,
            all_columns,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewSchema {
    pub id: TableId,
    #[serde(default = "public_schema_id")]
    pub schema_id: SchemaId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Canonical SQL text for the view query.
    pub definition: String,
    #[serde(default)]
    pub dependencies: Vec<ViewDependency>,
    #[serde(default = "initial_schema_version")]
    pub schema_version: u64,
    #[serde(default = "default_view_search_path")]
    pub definition_search_path: Vec<SchemaId>,
}

fn default_view_search_path() -> Vec<SchemaId> {
    vec![PUBLIC_SCHEMA_ID]
}

pub fn needs_toast_relation(schema: &TableSchema) -> bool {
    schema.relation_kind == RelationKind::User
        && schema.columns.iter().any(|column| {
            matches!(
                column.data_type,
                DataType::Text | DataType::Bytea | DataType::Array(_)
            )
        })
}

pub fn toast_relation_name(base_table: TableId) -> String {
    format!("\0toast_{base_table}")
}

pub fn toast_schema(base: &TableSchema, toast_id: TableId) -> TableSchema {
    TableSchema {
        id: toast_id,
        schema_id: base.schema_id,
        storage_id: toast_id,
        name: toast_relation_name(base.id),
        columns: vec![
            ColumnDef {
                id: 0,
                name: "value_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: Some(PgType::Int8),
            },
            ColumnDef {
                id: 1,
                name: "seq".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: Some(PgType::Int4),
            },
            ColumnDef {
                id: 2,
                name: "data".to_string(),
                data_type: DataType::Bytea,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: Some(PgType::Bytea),
            },
        ],
        primary_key: vec![0, 1],
        schema_version: INITIAL_SCHEMA_VERSION,
        compression: CompressionSetting::None,
        active_dict_id: None,
        toast: ToastOptions::legacy_catalog_default(),
        toast_table_id: None,
        relation_kind: RelationKind::Toast {
            base_table: base.id,
        },
        checks: Vec::new(),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceOptions {
    pub increment: i64,
    pub start: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
}

impl Default for SequenceOptions {
    fn default() -> Self {
        Self {
            increment: 1,
            start: None,
            min_value: None,
            max_value: None,
            cycle: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceSchema {
    pub id: SequenceId,
    #[serde(default = "public_schema_id")]
    pub schema_id: SchemaId,
    pub name: String,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start: i64,
    pub cycle: bool,
    pub owned: bool,
    pub last_value: i64,
    pub is_called: bool,
}

/// Catalog-visible constraint semantics carried by an index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexConstraintKind {
    #[default]
    None,
    Unique,
    PrimaryKey,
}

/// A secondary index over one or more columns of a table. `unique` rejects
/// duplicate indexed values; the B-tree stores the heap TID as a value tiebreaker
/// so duplicate non-unique keys can coexist.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSchema {
    pub id: IndexId,
    #[serde(default = "public_schema_id")]
    pub schema_id: SchemaId,
    /// Physical storage-generation id for this secondary index. `0` means a
    /// legacy decoded schema is missing the field; catalog migration replaces it
    /// with a non-zero id before installation.
    #[serde(default)]
    pub storage_id: FileId,
    pub table: TableId,
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub unique: bool,
    #[serde(default)]
    pub constraint: IndexConstraintKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncateTablePlan {
    pub table_id: TableId,
    pub new_table_storage_id: FileId,
    pub new_toast_storage_id: Option<(TableId, FileId)>,
    pub new_index_storage_ids: Vec<(IndexId, FileId)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncateCatalogUpdate {
    pub table: TableSchema,
    pub toast_table: Option<TableSchema>,
    pub indexes: Vec<IndexSchema>,
}

#[cfg(test)]
mod tests {
    use super::{
        ArrayType, ColumnDef, ColumnInfo, CompressionSetting, DataType, INITIAL_SCHEMA_VERSION,
        PgType, RelationKind, TableSchema, ToastCompression, ToastMode, ToastOptions,
        ViewDependency,
    };

    #[test]
    fn column_info_can_describe_expression_output() {
        let column = ColumnInfo {
            name: "count".to_string(),
            data_type: DataType::Integer,
            table_id: None,
            column_id: None,
            pg_type: None,
        };

        assert_eq!(column.name, "count");
        assert_eq!(column.table_id, None);
        // A synthetic column with no declared label resolves to the collapsed default.
        assert_eq!(column.wire_type(), PgType::Int8);
    }

    #[test]
    fn column_def_wire_type_resolves_unlabeled_columns() {
        let unlabeled = ColumnDef {
            id: 0,
            name: "n".to_string(),
            data_type: DataType::Integer,
            nullable: true,
            max_length: None,
            default: None,
            pg_type: None,
        };
        // No declared label => the collapsed default from the DataType.
        assert_eq!(unlabeled.wire_type(), PgType::Int8);

        // A declared label is reported verbatim.
        let labeled = ColumnDef {
            pg_type: Some(PgType::Int4),
            ..unlabeled
        };
        assert_eq!(labeled.wire_type(), PgType::Int4);
    }

    #[test]
    fn table_schema_without_compression_fields_deserializes_to_defaults() {
        // A pre-compression snapshot/WAL payload must keep loading.
        let json = r#"{
            "id": 1,
            "name": "users",
            "columns": [],
            "primary_key": []
        }"#;
        let schema: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.compression, CompressionSetting::None);
        assert_eq!(schema.storage_id, 0);
        assert_eq!(schema.schema_version, INITIAL_SCHEMA_VERSION);
        assert_eq!(schema.active_dict_id, None);
        assert_eq!(schema.toast, ToastOptions::legacy_catalog_default());
        assert_eq!(schema.toast_table_id, None);
        assert_eq!(schema.relation_kind, RelationKind::User);
    }

    #[test]
    fn compression_setting_defaults_to_none() {
        assert_eq!(CompressionSetting::default(), CompressionSetting::None);
    }

    #[test]
    fn data_type_deserialization_rejects_nested_arrays() {
        let scalar: DataType = serde_json::from_str(r#"{"Array":"Integer"}"#).unwrap();
        assert_eq!(
            scalar,
            DataType::Array(ArrayType::new(DataType::Integer).unwrap())
        );

        let error =
            serde_json::from_str::<DataType>(r#"{"Array":{"Array":"Integer"}}"#).unwrap_err();
        assert!(error.to_string().contains("cannot themselves be arrays"));
    }

    #[test]
    fn legacy_empty_view_dependency_deserializes_as_all_columns() {
        let json = r#"{"relation": 7, "columns": []}"#;
        let dependency: ViewDependency = serde_json::from_str(json).unwrap();
        assert!(dependency.all_columns);

        let relation_only_json = r#"{"relation": 7, "columns": [], "all_columns": false}"#;
        let dependency: ViewDependency = serde_json::from_str(relation_only_json).unwrap();
        assert!(!dependency.all_columns);
    }

    #[test]
    fn toast_options_defaults_are_split_for_new_and_legacy_tables() {
        assert_eq!(
            ToastOptions::default_new_table(),
            ToastOptions {
                mode: ToastMode::Auto,
                tuple_target: 2048,
                min_value_size: 1024,
                compression: ToastCompression::ZstdDict,
                active_dict_id: None,
            }
        );
        assert_eq!(
            ToastOptions::legacy_catalog_default(),
            ToastOptions {
                mode: ToastMode::Off,
                tuple_target: 2048,
                min_value_size: 1024,
                compression: ToastCompression::None,
                active_dict_id: None,
            }
        );
        assert_eq!(RelationKind::default(), RelationKind::User);
    }
}
