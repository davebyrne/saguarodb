use serde::{Deserialize, Serialize};

use crate::{ColumnId, IndexId, PgType, SequenceId, TableId, Value};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParsedDefault {
    Const(Value),
    Nextval(String),
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
pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
    /// At-rest page compression for this table's heap and index files.
    #[serde(default)]
    pub compression: CompressionSetting,
    /// The trained dictionary new heap-page writes compress against
    /// (`None` until an ALTER trains one). Index files never use it.
    #[serde(default)]
    pub active_dict_id: Option<u32>,
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
/// duplicate indexed values; a non-unique index appends the primary key to make
/// each entry distinct on disk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSchema {
    pub id: IndexId,
    pub table: TableId,
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub unique: bool,
}

#[cfg(test)]
mod tests {
    use super::{ColumnDef, ColumnInfo, CompressionSetting, DataType, PgType, TableSchema};

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
        assert_eq!(schema.active_dict_id, None);
    }

    #[test]
    fn compression_setting_defaults_to_none() {
        assert_eq!(CompressionSetting::default(), CompressionSetting::None);
    }
}
