use common::{DataType, DbError, Result, Row, SqlState, TableSchema, Value};

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
    let mut bytes = vec![0; bitmap_len];

    for (index, (column, value)) in schema.columns.iter().zip(&row.values).enumerate() {
        match value {
            Value::Null => {
                if !column.nullable {
                    return Err(DbError::storage(
                        SqlState::NotNullViolation,
                        format!("column {} cannot be NULL", column.name),
                    ));
                }
                set_null(&mut bytes, index);
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
    if bytes.len() < bitmap_len {
        return Err(corrupt_row("row is shorter than null bitmap"));
    }

    let null_bitmap = &bytes[..bitmap_len];
    let mut offset = bitmap_len;
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
