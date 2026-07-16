use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    ColumnId, ColumnObjectId, ConstraintId, DbError, FileId, IndexId, PgType, SchemaId, SequenceId,
    SqlState, StoredExpression, TableId, Value,
};

pub const INITIAL_SCHEMA_VERSION: u64 = 1;

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

    pub fn disabled() -> Self {
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
    /// binder resolves it once during DDL and replaces it with `Stored`; it may
    /// not reference table columns.
    Expr(String),
    /// Binder-resolved internal form passed to catalog mutation APIs.
    Stored(StoredExpression),
    /// Internal form used while executing `SERIAL` desugaring. Explicit user
    /// defaults use `Nextval` and may not borrow a serial-owned sequence.
    OwnedNextval(String),
    /// Parse-time marker for a `SERIAL` family column. It is resolved during
    /// `CREATE TABLE` execution into an owned sequence plus `OwnedNextval(name)`.
    Serial,
}

/// A resolved column `DEFAULT`, persisted in the catalog snapshot. The
/// externally-tagged serde form is part of catalog v3.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnDefault {
    Const(Value),
    Nextval(SequenceId),
    /// A non-constant `DEFAULT` expression persisted as typed durable IR, with
    /// canonical SQL retained for introspection and diagnostics.
    Expr(StoredExpression),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Maximum length in characters for a bounded character type
    /// (`VARCHAR(n)` / `CHAR(n)`). `None` for unbounded `TEXT` and all
    /// non-character types.
    pub max_length: Option<u32>,
    /// The column `DEFAULT`, applied when an `INSERT` omits the column. Constants
    /// are folded at parse time; sequence defaults keep a sequence name until the
    /// catalog resolves it to a durable id.
    pub default: Option<ParsedDefault>,
    /// The declared PostgreSQL wire type (integer width, character kind, length),
    /// captured by the parser so the protocol can report the exact OID/typmod.
    /// `None` when the parser has not labeled the column; resolved to the collapsed
    /// default (`Integer` => int8, `Text` => text) downstream.
    pub pg_type: Option<PgType>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: ColumnId,
    /// Durable per-relation identity. Zero is reserved for transient query-only
    /// columns that are never serialized into a catalog relation.
    pub object_id: ColumnObjectId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Maximum length in characters for a bounded character type
    /// (`VARCHAR(n)` / `CHAR(n)`); `None` means unbounded. Enforced at write
    /// time by the executor, not represented as a distinct `DataType`.
    pub max_length: Option<u32>,
    /// The column `DEFAULT` applied when an `INSERT`/`COPY` omits the column.
    /// Persisted inside its relation object and replayed through catalog changes.
    pub default: Option<ColumnDefault>,
    /// The declared PostgreSQL wire type, persisted so the column reports the same
    /// OID/typmod after a restart. Use [`ColumnDef::wire_type`] rather than
    /// reading this directly so transient unlabeled columns resolve consistently.
    pub pg_type: Option<PgType>,
}

impl ColumnDef {
    /// The column's PostgreSQL wire type, resolving an unlabeled column (`None`)
    /// to the collapsed default derived from its `DataType` (`Integer` => int8,
    /// `Text` => text). This is what the protocol should report; it is always
    /// concrete.
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
    pub pg_type: Option<PgType>,
}

/// Immediate referential action supported by foreign-key metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub id: ConstraintId,
    pub name: String,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    pub columns: Vec<ColumnId>,
    pub referenced_table: TableId,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    pub referenced_columns: Vec<ColumnId>,
    pub referenced_index: IndexId,
    pub on_update: ForeignKeyAction,
    pub on_delete: ForeignKeyAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintKind {
    Check {
        expression: StoredExpression,
    },
    PrimaryKey {
        #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
        columns: Vec<ColumnObjectId>,
        index: IndexId,
    },
    Unique {
        #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
        columns: Vec<ColumnObjectId>,
        index: IndexId,
    },
    ForeignKey {
        #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
        columns: Vec<ColumnObjectId>,
        referenced_table: TableId,
        referenced_constraint: ConstraintId,
        #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
        referenced_columns: Vec<ColumnObjectId>,
        on_update: ForeignKeyAction,
        on_delete: ForeignKeyAction,
        supporting_index: Option<IndexId>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintSchema {
    pub id: ConstraintId,
    pub table: TableId,
    pub name: String,
    pub kind: ConstraintKind,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub validated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: TableId,
    pub schema_id: SchemaId,
    /// Physical storage-generation id for this table's heap and primary index.
    /// Zero is reserved and rejected by catalog validation.
    pub storage_id: FileId,
    pub name: String,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    pub columns: Vec<ColumnDef>,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    pub primary_key: Vec<ColumnId>,
    /// Monotonic logical schema version. It starts at 1 for a newly created
    /// relation and increments on public schema metadata changes.
    pub schema_version: u64,
    /// At-rest page compression for this table's heap and index files.
    pub compression: CompressionSetting,
    /// The trained dictionary new heap-page writes compress against
    /// (`None` until an ALTER trains one). Index files never use it.
    pub active_dict_id: Option<u32>,
    /// Storage-private TOAST policy for future writes.
    pub toast: ToastOptions,
    /// Hidden companion relation for out-of-line toast chunks, when present.
    pub toast_table_id: Option<TableId>,
    /// User table vs. hidden toast relation metadata.
    pub relation_kind: RelationKind,
    /// Next never-reused durable column identity within this relation.
    pub next_column_object_id: ColumnObjectId,
}

impl TableSchema {
    pub fn column_by_object_id(&self, object_id: ColumnObjectId) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|column| column.object_id == object_id)
    }

    pub fn dense_column_id(&self, object_id: ColumnObjectId) -> Option<ColumnId> {
        self.column_by_object_id(object_id).map(|column| column.id)
    }

    pub fn stable_column_id(&self, column_id: ColumnId) -> Option<ColumnObjectId> {
        self.columns
            .iter()
            .find(|column| column.id == column_id)
            .map(|column| column.object_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "StoredViewSchema")]
pub struct ViewSchema {
    /// Version of the durable view-catalog payload.
    pub format_version: u32,
    pub id: TableId,
    pub schema_id: SchemaId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Canonical SQL text for the view query.
    pub definition: String,
    /// Catalog-resolved typed query used for execution and dependencies.
    pub query: crate::StoredQueryV1,
    pub schema_version: u64,
    pub definition_search_path: Vec<SchemaId>,
    /// Next never-reused durable output-column identity within this view.
    pub next_column_object_id: ColumnObjectId,
}

#[derive(Deserialize)]
struct StoredViewSchema {
    format_version: u32,
    id: TableId,
    schema_id: SchemaId,
    name: String,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    columns: Vec<ColumnDef>,
    definition: String,
    query: crate::StoredQueryV1,
    schema_version: u64,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    definition_search_path: Vec<SchemaId>,
    next_column_object_id: ColumnObjectId,
}

impl TryFrom<StoredViewSchema> for ViewSchema {
    type Error = String;

    fn try_from(stored: StoredViewSchema) -> std::result::Result<Self, Self::Error> {
        let format_version = stored.format_version;
        if format_version != VIEW_SCHEMA_FORMAT_VERSION {
            return Err(format!(
                "unsupported view catalog encoding version {format_version}"
            ));
        }
        Ok(Self {
            format_version,
            id: stored.id,
            schema_id: stored.schema_id,
            name: stored.name,
            columns: stored.columns,
            definition: stored.definition,
            query: stored.query,
            schema_version: stored.schema_version,
            definition_search_path: stored.definition_search_path,
            next_column_object_id: stored.next_column_object_id,
        })
    }
}

pub const VIEW_SCHEMA_FORMAT_VERSION: u32 = 1;

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
                object_id: 1,
                name: "value_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: Some(PgType::Int8),
            },
            ColumnDef {
                id: 1,
                object_id: 2,
                name: "seq".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: Some(PgType::Int4),
            },
            ColumnDef {
                id: 2,
                object_id: 3,
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
        toast: ToastOptions::disabled(),
        toast_table_id: None,
        relation_kind: RelationKind::Toast {
            base_table: base.id,
        },
        next_column_object_id: 4,
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

/// A secondary index over one or more columns of a table. `unique` rejects
/// duplicate indexed values; the B-tree stores the heap TID as a value tiebreaker
/// so duplicate non-unique keys can coexist.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSchema {
    pub id: IndexId,
    pub schema_id: SchemaId,
    /// Physical storage-generation id for this secondary index. `0` means a
    /// missing or invalid generation and is rejected by catalog validation.
    pub storage_id: FileId,
    pub table: TableId,
    pub name: String,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    pub columns: Vec<ColumnId>,
    pub unique: bool,
    pub constraint: Option<ConstraintId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncateTablePlan {
    pub table_id: TableId,
    pub new_table_storage_id: FileId,
    pub new_toast_storage_id: Option<(TableId, FileId)>,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    pub new_index_storage_ids: Vec<(IndexId, FileId)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncateCatalogUpdate {
    pub table: TableSchema,
    pub toast_table: Option<TableSchema>,
    #[serde(deserialize_with = "crate::durable::deserialize_bounded_vec")]
    pub indexes: Vec<IndexSchema>,
}

#[cfg(test)]
mod tests {
    use super::{
        ArrayType, ColumnDef, ColumnInfo, CompressionSetting, DataType, INITIAL_SCHEMA_VERSION,
        PgType, RelationKind, TableSchema, ToastCompression, ToastMode, ToastOptions, ViewSchema,
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
            object_id: 1,
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
    fn table_schema_requires_complete_durable_fields() {
        let json = r#"{
            "id": 1,
            "name": "users",
            "columns": [],
            "primary_key": [],
            "next_column_object_id": 1
        }"#;
        let error = serde_json::from_str::<TableSchema>(json).unwrap_err();
        assert!(error.to_string().contains("missing field `schema_id`"));
    }

    #[test]
    fn view_schema_requires_resolved_query() {
        let json = r#"{
            "format_version": 1,
            "id": 1,
            "schema_id": 1,
            "name": "view_without_query",
            "columns": [],
            "definition": "select 1",
            "schema_version": 1,
            "definition_search_path": [1],
            "next_column_object_id": 1
        }"#;
        let error = serde_json::from_str::<ViewSchema>(json).unwrap_err();
        assert!(error.to_string().contains("missing field `query`"));
    }

    #[test]
    fn view_schema_requires_a_supported_payload_version() {
        let schema = ViewSchema {
            format_version: crate::VIEW_SCHEMA_FORMAT_VERSION,
            id: 1,
            schema_id: crate::PUBLIC_SCHEMA_ID,
            name: "versioned_view".to_string(),
            columns: Vec::new(),
            definition: "values ()".to_string(),
            query: crate::StoredQueryV1 {
                version: crate::STORED_QUERY_VERSION,
                body: crate::StoredQueryBody::Values(crate::StoredValues {
                    rows: Vec::new(),
                    output_schema: Vec::new(),
                }),
                order_by: Vec::new(),
                limit: None,
                offset: None,
                row_lock: None,
                correlations: Vec::new(),
            },
            schema_version: INITIAL_SCHEMA_VERSION,
            definition_search_path: vec![crate::PUBLIC_SCHEMA_ID],
            next_column_object_id: 1,
        };
        let mut unknown = serde_json::to_value(schema).unwrap();
        unknown["format_version"] = serde_json::json!(2);
        let error = serde_json::from_value::<ViewSchema>(unknown).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported view catalog encoding version 2")
        );
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
    fn toast_options_distinguish_new_tables_from_disabled_relations() {
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
            ToastOptions::disabled(),
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
