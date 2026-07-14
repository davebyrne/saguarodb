#![cfg_attr(
    not(test),
    deny(
        clippy::arithmetic_side_effects,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::indexing_slicing
    )
)]

use common::{
    CheckedSliceReader, DbError, RelationKind, Result, Row, SqlState, TableSchema, Value,
};

use crate::codec::{DecodedPhysicalValue, ToastPointer, decode_physical_row};

pub(crate) const FIRST_TOAST_VALUE_ID: u64 = 1;
#[allow(
    dead_code,
    reason = "used by the allocator once TOAST writes are wired in"
)]
pub(crate) const MAX_TOAST_VALUE_ID: u64 = i64::MAX.cast_unsigned();
pub(crate) const TOAST_CHUNK_PAYLOAD: usize = 1900;

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
        Some(Value::Integer(value)) if *value > 0 => {
            u64::try_from(*value).map_err(|_| toast_corruption("TOAST value_id does not fit u64"))
        }
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

#[allow(
    dead_code,
    reason = "called by the row TOAST preparation path added in a later phase"
)]
pub(crate) fn build_external_stream(
    codec: u8,
    dict_id: Option<u32>,
    raw_crc32: u32,
    payload: &[u8],
) -> Result<Vec<u8>> {
    match (codec, dict_id) {
        (compress::CODEC_NONE | compress::CODEC_ZSTD, None) => {
            let mut stream = external_stream_buffer(4, payload.len())?;
            stream.extend_from_slice(&raw_crc32.to_le_bytes());
            stream.extend_from_slice(payload);
            Ok(stream)
        }
        (compress::CODEC_NONE | compress::CODEC_ZSTD, Some(_)) => Err(toast_corruption(
            "dictionary id is invalid for dict-less TOAST stream",
        )),
        (compress::CODEC_ZSTD_DICT, Some(dict_id)) if dict_id != 0 => {
            let mut stream = external_stream_buffer(8, payload.len())?;
            stream.extend_from_slice(&dict_id.to_le_bytes());
            stream.extend_from_slice(&raw_crc32.to_le_bytes());
            stream.extend_from_slice(payload);
            Ok(stream)
        }
        (compress::CODEC_ZSTD_DICT, _) => Err(toast_corruption(
            "missing dictionary id for zstd-dict TOAST stream",
        )),
        (other, _) => Err(toast_corruption(format!(
            "unknown external TOAST stream codec {other}"
        ))),
    }
}

pub(crate) fn parse_external_stream(codec: u8, stream: &[u8]) -> Result<(Option<u32>, u32, &[u8])> {
    let mut reader = CheckedSliceReader::new(stream);
    match codec {
        compress::CODEC_NONE | compress::CODEC_ZSTD => {
            let raw_crc32 = read_u32(&mut reader, "external TOAST stream")?;
            let payload = take_remaining(&mut reader, "external TOAST stream")?;
            Ok((None, raw_crc32, payload))
        }
        compress::CODEC_ZSTD_DICT => {
            let dict_id = read_u32(&mut reader, "external TOAST stream")?;
            if dict_id == 0 {
                return Err(toast_corruption(
                    "dictionary id 0 is invalid for zstd-dict TOAST stream",
                ));
            }
            let raw_crc32 = read_u32(&mut reader, "external TOAST stream")?;
            let payload = take_remaining(&mut reader, "external TOAST stream")?;
            Ok((Some(dict_id), raw_crc32, payload))
        }
        other => Err(toast_corruption(format!(
            "unknown external TOAST stream codec {other}"
        ))),
    }
}

pub(crate) fn external_pointers_in_tuple(
    schema: &TableSchema,
    tuple_bytes: &[u8],
) -> Result<Vec<ToastPointer>> {
    let physical = decode_physical_row(schema, tuple_bytes)?;
    let mut pointers = Vec::new();
    for value in physical.values {
        if let DecodedPhysicalValue::External { pointer, .. } = value {
            pointers.push(pointer);
        }
    }
    Ok(pointers)
}

pub(crate) fn chunk_row(value_id: u64, seq: usize, data: &[u8]) -> Result<Row> {
    if value_id == 0 || value_id > MAX_TOAST_VALUE_ID {
        return Err(toast_corruption(format!(
            "TOAST chunk has invalid value_id {value_id}"
        )));
    }
    let seq = i64::try_from(seq).map_err(|_| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "TOAST chunk sequence exceeds i64::MAX",
        )
    })?;
    Ok(Row {
        values: vec![
            Value::Integer(i64::try_from(value_id).map_err(|_| {
                toast_corruption("TOAST chunk value_id does not fit signed storage")
            })?),
            Value::Integer(seq),
            Value::Bytes(data.to_vec()),
        ],
    })
}

fn external_stream_buffer(header_len: usize, payload_len: usize) -> Result<Vec<u8>> {
    let capacity = header_len
        .checked_add(payload_len)
        .ok_or_else(|| toast_corruption("external TOAST stream length overflows"))?;
    let mut stream = Vec::new();
    stream
        .try_reserve_exact(capacity)
        .map_err(|_| toast_corruption("cannot allocate external TOAST stream"))?;
    Ok(stream)
}

fn read_u32(reader: &mut CheckedSliceReader<'_>, what: &str) -> Result<u32> {
    reader
        .read_u32_le()
        .map_err(|_| toast_corruption(format!("{what} is truncated")))
}

fn take_remaining<'a>(reader: &mut CheckedSliceReader<'a>, what: &str) -> Result<&'a [u8]> {
    reader
        .take_remaining()
        .map_err(|_| toast_corruption(format!("{what} has an invalid remaining range")))
}

pub(crate) fn toast_corruption(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use common::{
        ColumnDef, CompressionSetting, DataType, RelationKind, TableSchema, ToastOptions, Value,
        toast_schema,
    };

    use super::{TOAST_CHUNK_PAYLOAD, build_external_stream, parse_external_stream};
    use crate::codec::{MvccHeader, PreparedColumnValue, VarlenaPhysical, encode_row_v3_prepared};

    fn base_schema() -> TableSchema {
        TableSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 1,
            name: "base".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 1,
                    name: "body".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: Some(2),
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        }
    }

    #[test]
    fn external_stream_builder_parser_handles_supported_codecs() {
        let raw_crc32 = crc32fast::hash(b"raw logical bytes");
        let payload = b"stored payload";
        for (codec, dict_id) in [
            (compress::CODEC_NONE, None),
            (compress::CODEC_ZSTD, None),
            (compress::CODEC_ZSTD_DICT, Some(17)),
        ] {
            let stream = build_external_stream(codec, dict_id, raw_crc32, payload).unwrap();
            let (parsed_dict_id, parsed_crc, parsed_payload) =
                parse_external_stream(codec, &stream).unwrap();
            assert_eq!(parsed_dict_id, dict_id);
            assert_eq!(parsed_crc, raw_crc32);
            assert_eq!(parsed_payload, payload);
        }
    }

    #[test]
    fn external_stream_rejects_invalid_dictionary_metadata() {
        let err = build_external_stream(compress::CODEC_ZSTD, Some(9), 0, b"payload").unwrap_err();
        assert!(err.message.contains("dict-less"));

        let err =
            build_external_stream(compress::CODEC_ZSTD_DICT, Some(0), 0, b"payload").unwrap_err();
        assert!(err.message.contains("missing dictionary id"));

        let mut stream = Vec::new();
        stream.extend_from_slice(&0u32.to_le_bytes());
        stream.extend_from_slice(&123u32.to_le_bytes());
        let err = parse_external_stream(compress::CODEC_ZSTD_DICT, &stream).unwrap_err();
        assert!(err.message.contains("dictionary id 0"));
    }

    #[test]
    fn external_stream_parser_rejects_truncated_headers() {
        let err = parse_external_stream(compress::CODEC_NONE, &[1, 2, 3]).unwrap_err();
        assert!(err.message.contains("truncated"));

        let err = parse_external_stream(compress::CODEC_ZSTD_DICT, &[1, 2, 3, 4, 5]).unwrap_err();
        assert!(err.message.contains("truncated"));
    }

    #[test]
    fn toast_chunk_payload_size_fits_one_v3_chunk_row() {
        let base = base_schema();
        let toast = toast_schema(&base, 2);
        let bytes = encode_row_v3_prepared(
            &toast,
            &MvccHeader::fresh(7, 0),
            &[
                PreparedColumnValue::Value(Value::Integer(1)),
                PreparedColumnValue::Value(Value::Integer(0)),
                PreparedColumnValue::Varlena(VarlenaPhysical::Plain(vec![0; TOAST_CHUNK_PAYLOAD])),
            ],
        )
        .unwrap();

        assert!(bytes.len() + crate::page::HEADER_LEN + crate::page::SLOT_LEN <= buffer::PAGE_SIZE);
    }
}
