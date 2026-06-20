use common::{DataType, DbError, Result, Row, SqlState, TableSchema, Value};

/// On-page row encoding version. v1 layout is `[version][null_bitmap][columns]`.
/// Reserved so MVCC row versions (e.g. `xmin`/`xmax`) can be added later without
/// a second on-disk format break.
const ROW_FORMAT_VERSION: u8 = 1;

pub fn encode_row(schema: &TableSchema, row: &Row) -> Result<Vec<u8>> {
    if row.values.len() != schema.columns.len() {
        return Err(DbError::storage(
            SqlState::DatatypeMismatch,
            format!(
                "row has {} values but table {} has {} columns",
                row.values.len(),
                schema.name,
                schema.columns.len()
            ),
        ));
    }

    let bitmap_len = null_bitmap_len(schema.columns.len());
    let mut bytes = vec![0; 1 + bitmap_len];
    bytes[0] = ROW_FORMAT_VERSION;

    for (index, (column, value)) in schema.columns.iter().zip(&row.values).enumerate() {
        match value {
            Value::Null => {
                if !column.nullable {
                    return Err(DbError::storage(
                        SqlState::NotNullViolation,
                        format!("column {} cannot be NULL", column.name),
                    ));
                }
                set_null(&mut bytes[1..1 + bitmap_len], index);
            }
            Value::Integer(value) if column.data_type == DataType::Integer => {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Text(value) if column.data_type == DataType::Text => {
                let len = u32::try_from(value.len())
                    .map_err(|_| DbError::storage(SqlState::InternalError, "text is too large"))?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
            Value::Boolean(value) if column.data_type == DataType::Boolean => {
                bytes.push(u8::from(*value));
            }
            _ => {
                return Err(DbError::storage(
                    SqlState::DatatypeMismatch,
                    format!("value type does not match column {}", column.name),
                ));
            }
        }
    }

    Ok(bytes)
}

pub fn decode_row(schema: &TableSchema, bytes: &[u8]) -> Result<Row> {
    let bitmap_len = null_bitmap_len(schema.columns.len());
    let header_len = 1 + bitmap_len;
    if bytes.len() < header_len {
        return Err(corrupt_row("row is shorter than its header"));
    }
    if bytes[0] != ROW_FORMAT_VERSION {
        return Err(corrupt_row(format!(
            "unsupported row format version {}",
            bytes[0]
        )));
    }

    let null_bitmap = &bytes[1..header_len];
    let mut offset = header_len;
    let mut values = Vec::with_capacity(schema.columns.len());

    for (index, column) in schema.columns.iter().enumerate() {
        if is_null(null_bitmap, index) {
            values.push(Value::Null);
            continue;
        }

        let value = match column.data_type {
            DataType::Integer => {
                let raw = read_exact(bytes, &mut offset, 8)?;
                let mut array = [0; 8];
                array.copy_from_slice(raw);
                Value::Integer(i64::from_le_bytes(array))
            }
            DataType::Text => {
                let raw_len = read_exact(bytes, &mut offset, 4)?;
                let mut array = [0; 4];
                array.copy_from_slice(raw_len);
                let len = u32::from_le_bytes(array) as usize;
                let raw = read_exact(bytes, &mut offset, len)?;
                let text = String::from_utf8(raw.to_vec())
                    .map_err(|_| corrupt_row("text value is not valid UTF-8"))?;
                Value::Text(text)
            }
            DataType::Boolean => {
                let raw = read_exact(bytes, &mut offset, 1)?[0];
                match raw {
                    0 => Value::Boolean(false),
                    1 => Value::Boolean(true),
                    _ => return Err(corrupt_row("boolean value is not 0 or 1")),
                }
            }
        };
        values.push(value);
    }

    if offset != bytes.len() {
        return Err(corrupt_row("row has trailing bytes"));
    }

    Ok(Row { values })
}

fn null_bitmap_len(columns: usize) -> usize {
    columns.div_ceil(8)
}

fn set_null(bitmap: &mut [u8], index: usize) {
    bitmap[index / 8] |= 1 << (index % 8);
}

fn is_null(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1 << (index % 8)) != 0
}

fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt_row("row offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt_row("row ended unexpectedly"))?;
    *offset = end;
    Ok(raw)
}

fn corrupt_row(message: impl Into<String>) -> common::DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use common::{ColumnDef, DataType, Row, TableSchema, Value};

    use super::{ROW_FORMAT_VERSION, decode_row, encode_row};

    fn schema() -> TableSchema {
        TableSchema {
            id: 1,
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    id: 1,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
        }
    }

    #[test]
    fn encode_prefixes_row_format_version() {
        let row = Row {
            values: vec![Value::Integer(7), Value::Null],
        };
        let bytes = encode_row(&schema(), &row).unwrap();
        assert_eq!(bytes[0], ROW_FORMAT_VERSION);
    }

    #[test]
    fn decode_rejects_unknown_row_format_version() {
        let row = Row {
            values: vec![Value::Integer(7), Value::Null],
        };
        let mut bytes = encode_row(&schema(), &row).unwrap();
        bytes[0] = ROW_FORMAT_VERSION + 1;

        let err = decode_row(&schema(), &bytes).unwrap_err();
        assert!(err.message.contains("row format version"));
    }
}
