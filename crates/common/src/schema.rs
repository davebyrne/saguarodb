use serde::{Deserialize, Serialize};

use crate::{ColumnId, IndexId, TableId, Value};

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
    /// `BYTEA` — raw byte string, value carried as `Value::Bytes`.
    Bytea,
    /// `UUID` — 128-bit identifier, value carried as `Value::Uuid` (16 bytes).
    Uuid,
    /// `DOUBLE PRECISION` — IEEE 754 binary64, value carried as `Value::Float`.
    Double,
    /// `NUMERIC` / `DECIMAL` — exact decimal, value carried as `Value::Numeric`.
    /// `precision: None` is unconstrained `NUMERIC` (any precision/scale); `Some(p)`
    /// constrains to `p` total digits with the given `scale` (values are rounded to
    /// `scale` and rejected when the integer part exceeds `p - scale` digits).
    Numeric {
        precision: Option<u32>,
        scale: u32,
    },
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
    /// The column `DEFAULT` value, applied when an `INSERT` omits the column.
    /// A constant value folded at parse time; `None` when the column has no
    /// `DEFAULT` (an omitted value is then `NULL`).
    #[serde(default)]
    pub default: Option<Value>,
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
    /// The column `DEFAULT` value applied when an `INSERT`/`COPY` omits the
    /// column. A constant `Value` folded at parse time; `None` when the column
    /// has no `DEFAULT` (an omitted value is then `NULL`). Persisted with the
    /// catalog and replayed via the `CreateTable` WAL record.
    #[serde(default)]
    pub default: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub table_id: Option<TableId>,
    pub column_id: Option<ColumnId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
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
    use super::{ColumnInfo, DataType};

    #[test]
    fn column_info_can_describe_expression_output() {
        let column = ColumnInfo {
            name: "count".to_string(),
            data_type: DataType::Integer,
            table_id: None,
            column_id: None,
        };

        assert_eq!(column.name, "count");
        assert_eq!(column.table_id, None);
    }
}
