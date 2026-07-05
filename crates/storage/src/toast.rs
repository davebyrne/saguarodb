use common::{DbError, RelationKind, Result, Row, SqlState, TableSchema, Value};

pub(crate) const FIRST_TOAST_VALUE_ID: u64 = 1;
#[allow(
    dead_code,
    reason = "used by the allocator once TOAST writes are wired in"
)]
pub(crate) const MAX_TOAST_VALUE_ID: u64 = i64::MAX as u64;

pub(crate) fn ensure_toast_relation(schema: &TableSchema) -> Result<()> {
    if !matches!(schema.relation_kind, RelationKind::Toast { .. }) {
        return Err(DbError::storage(
            SqlState::InternalError,
            format!("table {} is not a TOAST relation", schema.name),
        ));
    }
    Ok(())
}

pub(crate) fn value_id_from_chunk_row(schema: &TableSchema, row: &Row) -> Result<u64> {
    match row.values.first() {
        Some(Value::Integer(value)) if *value > 0 => Ok(*value as u64),
        Some(Value::Integer(value)) => Err(DbError::storage(
            SqlState::InternalError,
            format!(
                "TOAST relation {} has invalid value_id {value}",
                schema.name
            ),
        )),
        Some(_) => Err(DbError::storage(
            SqlState::InternalError,
            format!("TOAST relation {} has non-integer value_id", schema.name),
        )),
        None => Err(DbError::storage(
            SqlState::InternalError,
            format!("TOAST relation {} row is missing value_id", schema.name),
        )),
    }
}

pub(crate) fn next_after_value_id(value_id: u64) -> Result<u64> {
    value_id.checked_add(1).ok_or_else(|| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "TOAST value id allocator overflowed",
        )
    })
}

#[allow(
    dead_code,
    reason = "used by the allocator once TOAST writes are wired in"
)]
pub(crate) fn allocate_next_value_id(next_value_id: &mut u64) -> Result<u64> {
    if *next_value_id > MAX_TOAST_VALUE_ID {
        return Err(DbError::storage(
            SqlState::ProgramLimitExceeded,
            "TOAST value id allocator reached i64::MAX",
        ));
    }
    let allocated = *next_value_id;
    *next_value_id = next_after_value_id(allocated)?;
    Ok(allocated)
}
