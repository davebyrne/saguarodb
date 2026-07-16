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

use common::{CheckedSliceReader, DbError, Lsn, Result, SqlState};
use crc32fast::Hasher;
use serde::{Deserialize, Deserializer};

use crate::{WalRecord, WalRecordKind};

const HEADER_LEN: usize = 8 + 8 + 1 + 4;
const CRC_LEN: usize = 4;

/// `HeapUpdateHeader` payload: file_id(4) + page_num(4) + slot(2) + xmax(8) +
/// t_ctid page(4) + t_ctid slot(2) + infomask(2).
const HEAP_UPDATE_HEADER_LEN: usize = 4 + 4 + 2 + 8 + 4 + 2 + 2;

const TYPE_COMMIT: u8 = 3;
const TYPE_CHECKPOINT: u8 = 4;
// Physiological redo records use compact binary payloads instead of JSON.
const TYPE_HEAP_INIT: u8 = 5;
const TYPE_HEAP_INSERT: u8 = 6;
const TYPE_HEAP_DELETE: u8 = 7;
const TYPE_FULL_PAGE_IMAGE: u8 = 8;
const TYPE_ABORT: u8 = 11;
const TYPE_HEAP_UPDATE_HEADER: u8 = 12;
const TYPE_COMMIT_WITH_SUBXIDS: u8 = 13;
const TYPE_SEQUENCE_ADVANCE: u8 = 16;
const TYPE_SET_SEQUENCE_VALUE: u8 = 17;
pub(crate) const TYPE_FULL_PAGE_IMAGE_COMPRESSED: u8 = 18;
pub(crate) const TYPE_CREATE_DICTIONARY: u8 = 19;
pub(crate) const TYPE_CATALOG_CHANGE: u8 = 31;
const MAX_CATALOG_CHANGE_PAYLOAD_BYTES: usize = 67_108_864;
const MAX_JSON_PAYLOAD_BYTES: usize = 67_108_864;
const MAX_PAGE_BYTES: usize = 8_192;
const MAX_DICTIONARY_BYTES: usize = 112_640;
const MAX_COMMITTED_SUBXIDS: usize = 65_536;

pub fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
    let payload = encode_payload(&record.kind)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| wal_error("WAL payload is too large to encode"))?;

    let capacity = HEADER_LEN
        .checked_add(payload.len())
        .and_then(|length| length.checked_add(CRC_LEN))
        .ok_or_else(|| wal_error("encoded WAL record length overflows"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| wal_error("cannot allocate encoded WAL record"))?;
    bytes.extend_from_slice(&record.lsn.to_le_bytes());
    bytes.extend_from_slice(&record.txn_id.to_le_bytes());
    bytes.push(record_type(&record.kind));
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload);

    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    bytes.extend_from_slice(&hasher.finalize().to_le_bytes());

    Ok(bytes)
}

pub fn decode_record(bytes: &[u8]) -> Result<WalRecord> {
    match decode_one(bytes, 0)? {
        DecodeResult::Record {
            record,
            next_offset,
        } if next_offset == bytes.len() => Ok(*record),
        DecodeResult::Record { .. } => Err(wal_error("WAL buffer contains trailing bytes")),
        DecodeResult::Incomplete => Err(wal_error("incomplete WAL record")),
    }
}

pub(crate) fn read_records(bytes: &[u8]) -> Result<(Vec<(WalRecord, u64)>, usize)> {
    let mut records = Vec::new();
    let mut offset = 0;

    while offset < bytes.len() {
        match decode_one(bytes, offset)? {
            DecodeResult::Record {
                record,
                next_offset,
            } => {
                let encoded_len = next_offset
                    .checked_sub(offset)
                    .ok_or_else(|| wal_error("WAL decoder moved backwards"))?;
                let encoded_len = u64::try_from(encoded_len)
                    .map_err(|_| wal_error("WAL record length does not fit u64"))?;
                if records.len() == records.capacity() {
                    records
                        .try_reserve(1)
                        .map_err(|_| wal_error("cannot grow decoded WAL record list"))?;
                }
                records.push((*record, encoded_len));
                offset = next_offset;
            }
            DecodeResult::Incomplete
                if suffix_contains_complete_record(
                    bytes,
                    offset
                        .checked_add(1)
                        .ok_or_else(|| wal_error("WAL scan offset overflows"))?,
                )? =>
            {
                return Err(wal_error(
                    "incomplete WAL record before later complete record",
                ));
            }
            DecodeResult::Incomplete => break,
        }
    }

    Ok((records, offset))
}

pub(crate) fn max_lsn(records: &[WalRecord]) -> Lsn {
    records.iter().map(|record| record.lsn).max().unwrap_or(0)
}

fn decode_one(bytes: &[u8], offset: usize) -> Result<DecodeResult> {
    let mut reader = CheckedSliceReader::at(bytes, offset)
        .map_err(|err| wal_error(format!("invalid WAL record offset: {err}")))?;
    if reader.remaining() < HEADER_LEN {
        return Ok(DecodeResult::Incomplete);
    }

    let lsn = reader
        .read_u64_le()
        .map_err(|err| wal_error(format!("invalid WAL LSN header: {err}")))?;
    let txn_id = reader
        .read_u64_le()
        .map_err(|err| wal_error(format!("invalid WAL transaction header: {err}")))?;
    let type_id = reader
        .read_u8()
        .map_err(|err| wal_error(format!("invalid WAL record type header: {err}")))?;
    let payload_len = usize::try_from(
        reader
            .read_u32_le()
            .map_err(|err| wal_error(format!("invalid WAL payload length header: {err}")))?,
    )
    .map_err(|_| wal_error("WAL payload length does not fit usize"))?;
    validate_declared_payload_length(type_id, payload_len)?;
    let payload_and_crc_len = payload_len
        .checked_add(CRC_LEN)
        .ok_or_else(|| wal_error("WAL record length overflow"))?;
    if reader.remaining() < payload_and_crc_len {
        return Ok(DecodeResult::Incomplete);
    }

    let payload = reader
        .take(payload_len)
        .map_err(|err| wal_error(format!("invalid WAL payload: {err}")))?;
    let payload_end = reader.position();
    let stored_crc = reader
        .read_u32_le()
        .map_err(|err| wal_error(format!("invalid WAL CRC footer: {err}")))?;
    let record_end = reader.position();
    let mut hasher = Hasher::new();
    let checksummed = bytes
        .get(offset..payload_end)
        .ok_or_else(|| wal_error("WAL checksum range is outside the input"))?;
    hasher.update(checksummed);
    let computed_crc = hasher.finalize();
    if computed_crc != stored_crc {
        return Err(wal_error("WAL record CRC mismatch"));
    }

    let kind = decode_payload(type_id, payload)?;

    Ok(DecodeResult::Record {
        record: Box::new(WalRecord { lsn, txn_id, kind }),
        next_offset: record_end,
    })
}

fn record_type(kind: &WalRecordKind) -> u8 {
    match kind {
        WalRecordKind::CatalogChange { .. } => TYPE_CATALOG_CHANGE,
        WalRecordKind::SequenceAdvance { .. } => TYPE_SEQUENCE_ADVANCE,
        WalRecordKind::SetSequenceValue { .. } => TYPE_SET_SEQUENCE_VALUE,
        WalRecordKind::Commit => TYPE_COMMIT,
        WalRecordKind::CommitWithSubxids { .. } => TYPE_COMMIT_WITH_SUBXIDS,
        WalRecordKind::Abort => TYPE_ABORT,
        WalRecordKind::Checkpoint { .. } => TYPE_CHECKPOINT,
        WalRecordKind::HeapInit { .. } => TYPE_HEAP_INIT,
        WalRecordKind::HeapInsert { .. } => TYPE_HEAP_INSERT,
        WalRecordKind::HeapDelete { .. } => TYPE_HEAP_DELETE,
        WalRecordKind::HeapUpdateHeader { .. } => TYPE_HEAP_UPDATE_HEADER,
        WalRecordKind::FullPageImage { .. } => TYPE_FULL_PAGE_IMAGE,
        WalRecordKind::FullPageImageCompressed { .. } => TYPE_FULL_PAGE_IMAGE_COMPRESSED,
        WalRecordKind::CreateDictionary { .. } => TYPE_CREATE_DICTIONARY,
    }
}

/// Encode a record payload: compact binary for physiological redo records,
/// JSON for the structured logical/DDL records.
fn encode_payload(kind: &WalRecordKind) -> Result<Vec<u8>> {
    match kind {
        WalRecordKind::HeapInit { file_id, page_num } => {
            let mut payload = Vec::with_capacity(8);
            payload.extend_from_slice(&file_id.to_le_bytes());
            payload.extend_from_slice(&page_num.to_le_bytes());
            Ok(payload)
        }
        WalRecordKind::HeapInsert {
            file_id,
            page_num,
            slot,
            row_bytes,
        } => {
            validate_body_length(row_bytes.len(), MAX_PAGE_BYTES, "heap-insert row")?;
            let mut payload = encoded_payload_buffer(10, row_bytes.len())?;
            payload.extend_from_slice(&file_id.to_le_bytes());
            payload.extend_from_slice(&page_num.to_le_bytes());
            payload.extend_from_slice(&slot.to_le_bytes());
            payload.extend_from_slice(row_bytes);
            Ok(payload)
        }
        WalRecordKind::HeapDelete {
            file_id,
            page_num,
            slot,
        } => {
            let mut payload = Vec::with_capacity(10);
            payload.extend_from_slice(&file_id.to_le_bytes());
            payload.extend_from_slice(&page_num.to_le_bytes());
            payload.extend_from_slice(&slot.to_le_bytes());
            Ok(payload)
        }
        WalRecordKind::HeapUpdateHeader {
            file_id,
            page_num,
            slot,
            xmax,
            t_ctid,
            infomask,
        } => {
            let (ctid_page, ctid_slot) = t_ctid;
            let mut payload = Vec::with_capacity(HEAP_UPDATE_HEADER_LEN);
            payload.extend_from_slice(&file_id.to_le_bytes());
            payload.extend_from_slice(&page_num.to_le_bytes());
            payload.extend_from_slice(&slot.to_le_bytes());
            payload.extend_from_slice(&xmax.to_le_bytes());
            payload.extend_from_slice(&ctid_page.to_le_bytes());
            payload.extend_from_slice(&ctid_slot.to_le_bytes());
            payload.extend_from_slice(&infomask.to_le_bytes());
            Ok(payload)
        }
        WalRecordKind::FullPageImage {
            file_id,
            page_num,
            image,
        } => {
            validate_body_length(image.len(), MAX_PAGE_BYTES, "full-page image")?;
            let mut payload = encoded_payload_buffer(8, image.len())?;
            payload.extend_from_slice(&file_id.to_le_bytes());
            payload.extend_from_slice(&page_num.to_le_bytes());
            payload.extend_from_slice(image);
            Ok(payload)
        }
        WalRecordKind::FullPageImageCompressed {
            file_id,
            page_num,
            codec,
            dict_id,
            payload,
        } => {
            validate_body_length(payload.len(), MAX_PAGE_BYTES, "compressed full-page image")?;
            let mut buf = encoded_payload_buffer(13, payload.len())?;
            buf.extend_from_slice(&file_id.to_le_bytes());
            buf.extend_from_slice(&page_num.to_le_bytes());
            buf.push(*codec);
            buf.extend_from_slice(&dict_id.to_le_bytes());
            buf.extend_from_slice(payload);
            Ok(buf)
        }
        WalRecordKind::CreateDictionary {
            dict_id,
            table_id,
            bytes,
        } => {
            validate_body_length(bytes.len(), MAX_DICTIONARY_BYTES, "compression dictionary")?;
            let mut buf = encoded_payload_buffer(8, bytes.len())?;
            buf.extend_from_slice(&dict_id.to_le_bytes());
            buf.extend_from_slice(&table_id.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(buf)
        }
        WalRecordKind::CatalogChange { change_set } => {
            change_set
                .validate_shape()
                .map_err(|message| wal_error(format!("invalid catalog change set: {message}")))?;
            let payload = serde_json::to_vec(kind)
                .map_err(|err| wal_error(format!("failed to serialize WAL payload: {err}")))?;
            if payload.len() > MAX_CATALOG_CHANGE_PAYLOAD_BYTES {
                return Err(wal_error("catalog change WAL payload exceeds 64 MiB"));
            }
            Ok(payload)
        }
        WalRecordKind::CommitWithSubxids { subxids } => {
            if subxids.len() > MAX_COMMITTED_SUBXIDS {
                return Err(wal_error("commit WAL payload exceeds the subxid limit"));
            }
            encode_json_payload(kind)
        }
        _ => encode_json_payload(kind),
    }
}

/// Decode a record payload given its type byte. The type byte is authoritative
/// for physiological records; JSON records additionally verify it matches.
fn decode_payload(type_id: u8, payload: &[u8]) -> Result<WalRecordKind> {
    match type_id {
        TYPE_HEAP_INIT => {
            if payload.len() != 8 {
                return Err(wal_error("WAL heap-init payload is malformed"));
            }
            let mut reader = physical_reader(payload);
            let record = WalRecordKind::HeapInit {
                file_id: read_u32(&mut reader)?,
                page_num: read_u32(&mut reader)?,
            };
            finish_physical(reader)?;
            Ok(record)
        }
        TYPE_HEAP_INSERT => {
            if payload.len() < 10 {
                return Err(wal_error("WAL heap-insert payload is truncated"));
            }
            let mut reader = physical_reader(payload);
            let file_id = read_u32(&mut reader)?;
            let page_num = read_u32(&mut reader)?;
            let slot = read_u16(&mut reader)?;
            let row_bytes = copy_remaining(&mut reader, "heap-insert row")?;
            Ok(WalRecordKind::HeapInsert {
                file_id,
                page_num,
                slot,
                row_bytes,
            })
        }
        TYPE_HEAP_DELETE => {
            if payload.len() != 10 {
                return Err(wal_error("WAL heap-delete payload is malformed"));
            }
            let mut reader = physical_reader(payload);
            let record = WalRecordKind::HeapDelete {
                file_id: read_u32(&mut reader)?,
                page_num: read_u32(&mut reader)?,
                slot: read_u16(&mut reader)?,
            };
            finish_physical(reader)?;
            Ok(record)
        }
        TYPE_HEAP_UPDATE_HEADER => {
            if payload.len() != HEAP_UPDATE_HEADER_LEN {
                return Err(wal_error("WAL heap-update-header payload is malformed"));
            }
            let mut reader = physical_reader(payload);
            let record = WalRecordKind::HeapUpdateHeader {
                file_id: read_u32(&mut reader)?,
                page_num: read_u32(&mut reader)?,
                slot: read_u16(&mut reader)?,
                xmax: read_u64(&mut reader)?,
                t_ctid: (read_u32(&mut reader)?, read_u16(&mut reader)?),
                infomask: read_u16(&mut reader)?,
            };
            finish_physical(reader)?;
            Ok(record)
        }
        TYPE_FULL_PAGE_IMAGE => {
            if payload.len() < 8 {
                return Err(wal_error("WAL full-page-image payload is truncated"));
            }
            let mut reader = physical_reader(payload);
            let file_id = read_u32(&mut reader)?;
            let page_num = read_u32(&mut reader)?;
            let image = copy_remaining(&mut reader, "full-page image")?;
            Ok(WalRecordKind::FullPageImage {
                file_id,
                page_num,
                image,
            })
        }
        TYPE_FULL_PAGE_IMAGE_COMPRESSED => {
            if payload.len() < 13 {
                return Err(wal_error("compressed full-page-image payload too short"));
            }
            let mut reader = physical_reader(payload);
            let file_id = read_u32(&mut reader)?;
            let page_num = read_u32(&mut reader)?;
            let codec = read_u8(&mut reader)?;
            let dict_id = read_u32(&mut reader)?;
            let compressed = copy_remaining(&mut reader, "compressed full-page image")?;
            Ok(WalRecordKind::FullPageImageCompressed {
                file_id,
                page_num,
                codec,
                dict_id,
                payload: compressed,
            })
        }
        TYPE_CREATE_DICTIONARY => {
            if payload.len() < 8 {
                return Err(wal_error("create-dictionary payload too short"));
            }
            let mut reader = physical_reader(payload);
            let dict_id = read_u32(&mut reader)?;
            let table_id = read_u32(&mut reader)?;
            let bytes = copy_remaining(&mut reader, "compression dictionary")?;
            Ok(WalRecordKind::CreateDictionary {
                dict_id,
                table_id,
                bytes,
            })
        }
        TYPE_COMMIT_WITH_SUBXIDS => {
            let payload: CommitWithSubxidsPayload = serde_json::from_slice(payload)
                .map_err(|err| wal_error(format!("failed to deserialize WAL payload: {err}")))?;
            Ok(match payload {
                CommitWithSubxidsPayload::CommitWithSubxids { subxids } => {
                    WalRecordKind::CommitWithSubxids { subxids }
                }
            })
        }
        _ => {
            if is_legacy_catalog_type(type_id) {
                return Err(wal_error("unsupported legacy catalog WAL record format"));
            }
            if type_id == TYPE_CATALOG_CHANGE && payload.len() > MAX_CATALOG_CHANGE_PAYLOAD_BYTES {
                return Err(wal_error("catalog change WAL payload exceeds 64 MiB"));
            }
            let kind: WalRecordKind = serde_json::from_slice(payload)
                .map_err(|err| wal_error(format!("failed to deserialize WAL payload: {err}")))?;
            if type_id != record_type(&kind) {
                return Err(wal_error("WAL record type does not match payload"));
            }
            if let WalRecordKind::CatalogChange { change_set } = &kind {
                change_set.validate_shape().map_err(|message| {
                    wal_error(format!("invalid catalog change set: {message}"))
                })?;
            }
            Ok(kind)
        }
    }
}

fn is_legacy_catalog_type(type_id: u8) -> bool {
    matches!(type_id, 1 | 2 | 9 | 10 | 14 | 15 | 20..=30)
}

fn encoded_payload_buffer(header_len: usize, body_len: usize) -> Result<Vec<u8>> {
    let capacity = header_len
        .checked_add(body_len)
        .ok_or_else(|| wal_error("WAL payload length overflows"))?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(capacity)
        .map_err(|_| wal_error("cannot allocate WAL payload"))?;
    Ok(payload)
}

fn encode_json_payload(kind: &WalRecordKind) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(kind)
        .map_err(|err| wal_error(format!("failed to serialize WAL payload: {err}")))?;
    if payload.len() > MAX_JSON_PAYLOAD_BYTES {
        return Err(wal_error("JSON WAL payload exceeds 64 MiB"));
    }
    Ok(payload)
}

fn validate_declared_payload_length(type_id: u8, payload_len: usize) -> Result<()> {
    let maximum = match type_id {
        TYPE_HEAP_INIT => 8,
        TYPE_HEAP_INSERT => 10 + MAX_PAGE_BYTES,
        TYPE_HEAP_DELETE => 10,
        TYPE_HEAP_UPDATE_HEADER => HEAP_UPDATE_HEADER_LEN,
        TYPE_FULL_PAGE_IMAGE => 8 + MAX_PAGE_BYTES,
        TYPE_FULL_PAGE_IMAGE_COMPRESSED => 13 + MAX_PAGE_BYTES,
        TYPE_CREATE_DICTIONARY => 8 + MAX_DICTIONARY_BYTES,
        _ => MAX_JSON_PAYLOAD_BYTES,
    };
    if payload_len > maximum {
        return Err(wal_error("WAL payload exceeds the record-type size limit"));
    }
    Ok(())
}

fn validate_body_length(length: usize, maximum: usize, description: &str) -> Result<()> {
    if length > maximum {
        return Err(wal_error(format!(
            "WAL {description} exceeds its size limit"
        )));
    }
    Ok(())
}

fn physical_reader(payload: &[u8]) -> CheckedSliceReader<'_> {
    CheckedSliceReader::new(payload)
}

fn read_u8(reader: &mut CheckedSliceReader<'_>) -> Result<u8> {
    reader
        .read_u8()
        .map_err(|err| wal_error(format!("WAL physical payload is truncated: {err}")))
}

fn read_u16(reader: &mut CheckedSliceReader<'_>) -> Result<u16> {
    reader
        .read_u16_le()
        .map_err(|err| wal_error(format!("WAL physical payload is truncated: {err}")))
}

fn read_u32(reader: &mut CheckedSliceReader<'_>) -> Result<u32> {
    reader
        .read_u32_le()
        .map_err(|err| wal_error(format!("WAL physical payload is truncated: {err}")))
}

fn read_u64(reader: &mut CheckedSliceReader<'_>) -> Result<u64> {
    reader
        .read_u64_le()
        .map_err(|err| wal_error(format!("WAL physical payload is truncated: {err}")))
}

fn take_remaining<'a>(reader: &mut CheckedSliceReader<'a>) -> Result<&'a [u8]> {
    reader
        .take_remaining()
        .map_err(|err| wal_error(format!("WAL physical payload is truncated: {err}")))
}

fn copy_remaining(reader: &mut CheckedSliceReader<'_>, description: &str) -> Result<Vec<u8>> {
    let source = take_remaining(reader)?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(source.len())
        .map_err(|_| wal_error(format!("cannot allocate WAL {description}")))?;
    result.extend_from_slice(source);
    Ok(result)
}

#[derive(Deserialize)]
enum CommitWithSubxidsPayload {
    CommitWithSubxids {
        #[serde(deserialize_with = "deserialize_subxids")]
        subxids: Vec<u64>,
    },
}

fn deserialize_subxids<'de, D>(deserializer: D) -> std::result::Result<Vec<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    common::deserialize_bounded_vec_named(
        deserializer,
        MAX_COMMITTED_SUBXIDS,
        "commit WAL subxid limit",
    )
}

fn finish_physical(reader: CheckedSliceReader<'_>) -> Result<()> {
    reader
        .finish()
        .map_err(|err| wal_error(format!("WAL physical payload is malformed: {err}")))
}

fn wal_error(message: impl Into<String>) -> DbError {
    DbError::wal(SqlState::InternalError, message)
}

fn suffix_contains_complete_record(bytes: &[u8], start: usize) -> Result<bool> {
    for offset in start..bytes.len() {
        match decode_one(bytes, offset) {
            Ok(DecodeResult::Record { .. }) => return Ok(true),
            Ok(DecodeResult::Incomplete) | Err(_) => {}
        }
    }
    Ok(false)
}

enum DecodeResult {
    Record {
        record: Box<WalRecord>,
        next_offset: usize,
    },
    Incomplete,
}

#[cfg(test)]
mod tests {
    use crate::{WalRecord, WalRecordKind};

    use super::{
        CRC_LEN, HEADER_LEN, MAX_CATALOG_CHANGE_PAYLOAD_BYTES, MAX_COMMITTED_SUBXIDS,
        MAX_PAGE_BYTES, TYPE_CATALOG_CHANGE, TYPE_COMMIT_WITH_SUBXIDS, TYPE_FULL_PAGE_IMAGE,
        TYPE_HEAP_DELETE, decode_payload, decode_record, encode_record,
    };

    #[test]
    fn round_trips_physical_redo_records() {
        let kinds = [
            WalRecordKind::HeapInit {
                file_id: 2,
                page_num: 5,
            },
            WalRecordKind::HeapInsert {
                file_id: 2,
                page_num: 5,
                slot: 3,
                row_bytes: vec![1, 2, 3, 4],
            },
            WalRecordKind::HeapInsert {
                file_id: 7,
                page_num: 0,
                slot: 0,
                row_bytes: Vec::new(),
            },
            WalRecordKind::HeapDelete {
                file_id: 2,
                page_num: 5,
                slot: 3,
            },
            WalRecordKind::HeapUpdateHeader {
                file_id: 2,
                page_num: 5,
                slot: 3,
                xmax: 0x0102_0304_0506_0708,
                t_ctid: (9, 11),
                infomask: 0xABCD,
            },
            WalRecordKind::HeapUpdateHeader {
                file_id: u32::MAX,
                page_num: u32::MAX,
                slot: u16::MAX,
                xmax: u64::MAX,
                t_ctid: (u32::MAX, u16::MAX),
                infomask: u16::MAX,
            },
            WalRecordKind::FullPageImage {
                file_id: 2,
                page_num: 5,
                image: vec![9u8; 8192],
            },
        ];
        for kind in kinds {
            let record = WalRecord {
                lsn: 12,
                txn_id: 4,
                kind,
            };
            let bytes = encode_record(&record).unwrap();
            assert_eq!(decode_record(&bytes).unwrap(), record);
        }
    }

    #[test]
    fn round_trips_catalog_change_and_sequence_value_records() {
        let schema = common::NamespaceSchema {
            id: 2,
            name: "app".to_string(),
        };
        let change_set = common::CatalogChangeSet::between(
            &std::collections::BTreeMap::new(),
            &std::collections::BTreeMap::from([(
                common::CatalogObjectId::Schema(schema.id),
                common::CatalogObject::Schema(schema),
            )]),
            common::CatalogAllocatorHighWater {
                next_schema_id: 3,
                ..common::CatalogAllocatorHighWater::default()
            },
        );
        let kinds = [
            WalRecordKind::CatalogChange { change_set },
            WalRecordKind::SequenceAdvance {
                sequence: 4,
                value: 11,
            },
            WalRecordKind::SetSequenceValue {
                sequence: 4,
                value: 20,
                is_called: false,
            },
        ];
        for kind in kinds {
            let record = WalRecord {
                lsn: 7,
                txn_id: 2,
                kind,
            };
            let bytes = encode_record(&record).unwrap();
            assert_eq!(decode_record(&bytes).unwrap(), record);
        }
    }

    #[test]
    fn catalog_change_rejects_unknown_version() {
        let kind = WalRecordKind::CatalogChange {
            change_set: common::CatalogChangeSet {
                version: common::CATALOG_CHANGE_SET_VERSION + 1,
                mutations: Vec::new(),
                allocator_high_water: common::CatalogAllocatorHighWater::default(),
            },
        };
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: kind.clone(),
        };
        assert!(
            encode_record(&record)
                .unwrap_err()
                .message
                .contains("unsupported catalog change-set version")
        );

        let payload = serde_json::to_vec(&kind).unwrap();
        assert!(
            decode_payload(TYPE_CATALOG_CHANGE, &payload)
                .unwrap_err()
                .message
                .contains("unsupported catalog change-set version")
        );
    }

    #[test]
    fn catalog_change_decoder_rejects_oversized_list_before_decoding_extra_mutation() {
        let mutation = common::CatalogMutation {
            before: None,
            after: Some(common::CatalogObject::Schema(common::NamespaceSchema {
                id: 2,
                name: "app".to_string(),
            })),
        };
        let mut mutations =
            vec![serde_json::to_value(mutation).unwrap(); common::MAX_CATALOG_CHANGE_MUTATIONS];
        // If the decoder materializes this element as a CatalogMutation before
        // enforcing the count limit, serde reports a type error instead of the
        // required bounded-list error.
        mutations.push(serde_json::json!("malformed over-limit mutation"));
        let payload = serde_json::to_vec(&serde_json::json!({
            "CatalogChange": {
                "change_set": {
                    "version": common::CATALOG_CHANGE_SET_VERSION,
                    "mutations": mutations,
                    "allocator_high_water": common::CatalogAllocatorHighWater::default(),
                }
            }
        }))
        .unwrap();
        assert!(
            decode_payload(TYPE_CATALOG_CHANGE, &payload)
                .unwrap_err()
                .message
                .contains("too many mutations")
        );
    }

    #[test]
    fn catalog_change_codec_rejects_oversized_payload_before_decode() {
        let oversized_payload = vec![0; MAX_CATALOG_CHANGE_PAYLOAD_BYTES + 1];
        let error = decode_payload(TYPE_CATALOG_CHANGE, &oversized_payload).unwrap_err();
        assert!(
            error.message.contains("exceeds 64 MiB"),
            "{}",
            error.message
        );

        let oversized_name = "x".repeat(MAX_CATALOG_CHANGE_PAYLOAD_BYTES);
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::CatalogChange {
                change_set: common::CatalogChangeSet {
                    version: common::CATALOG_CHANGE_SET_VERSION,
                    mutations: vec![common::CatalogMutation {
                        before: None,
                        after: Some(common::CatalogObject::Schema(common::NamespaceSchema {
                            id: 2,
                            name: oversized_name,
                        })),
                    }],
                    allocator_high_water: common::CatalogAllocatorHighWater::default(),
                },
            },
        };
        let error = encode_record(&record).unwrap_err();
        assert!(
            error.message.contains("exceeds 64 MiB"),
            "{}",
            error.message
        );
    }

    #[test]
    fn catalog_change_codec_round_trips_constraint_objects() {
        let constraint = common::ConstraintSchema {
            id: 1,
            table: 2,
            name: "items_key".to_string(),
            kind: common::ConstraintKind::Unique {
                columns: vec![3],
                index: 4,
            },
            deferrable: false,
            initially_deferred: false,
            validated: true,
        };
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::CatalogChange {
                change_set: common::CatalogChangeSet {
                    version: common::CATALOG_CHANGE_SET_VERSION,
                    mutations: vec![common::CatalogMutation {
                        before: None,
                        after: Some(common::CatalogObject::Constraint(constraint)),
                    }],
                    allocator_high_water: common::CatalogAllocatorHighWater::default(),
                },
            },
        };
        let encoded = encode_record(&record).unwrap();
        assert_eq!(decode_record(&encoded).unwrap(), record);
    }

    #[test]
    fn full_page_image_uses_compact_binary_payload() {
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::FullPageImage {
                file_id: 1,
                page_num: 0,
                image: vec![0u8; 8192],
            },
        };
        let bytes = encode_record(&record).unwrap();
        // payload is 4 (file_id) + 4 (page_num) + 8192 (image), not a JSON array.
        assert_eq!(bytes.len(), HEADER_LEN + 8 + 8192 + CRC_LEN);
    }

    #[test]
    fn round_trips_compression_physical_records() {
        let kinds = [
            WalRecordKind::FullPageImageCompressed {
                file_id: 3,
                page_num: 9,
                codec: 2,
                dict_id: 7,
                payload: vec![1, 2, 3, 4, 5],
            },
            WalRecordKind::CreateDictionary {
                dict_id: 7,
                table_id: 3,
                bytes: vec![9; 64],
            },
        ];
        for kind in kinds {
            let record = WalRecord {
                lsn: 5,
                txn_id: 11,
                kind,
            };
            let bytes = encode_record(&record).unwrap();
            assert_eq!(
                decode_record(&bytes).unwrap(),
                record,
                "kind failed round-trip"
            );
        }
    }

    #[test]
    fn non_finite_statistics_payload_fails_to_encode() {
        // serde_json would write NaN as `null`, which decode can never read
        // back — the codec must refuse the append rather than poison the log.
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::CatalogChange {
                change_set: common::CatalogChangeSet {
                    version: common::CATALOG_CHANGE_SET_VERSION,
                    mutations: vec![common::CatalogMutation {
                        before: None,
                        after: Some(common::CatalogObject::Statistics {
                            table: 5,
                            statistics: common::TableStatistics {
                                row_count: 10,
                                page_count: 1,
                                columns: std::collections::BTreeMap::from([(
                                    0,
                                    common::ColumnStatistics {
                                        null_frac: common::OrderedF64::new(f64::NAN),
                                        avg_width: 8,
                                        n_distinct: common::NDistinct::Count(1),
                                        most_common: Vec::new(),
                                        histogram_bounds: Vec::new(),
                                    },
                                )]),
                            },
                        }),
                    }],
                    allocator_high_water: common::CatalogAllocatorHighWater::default(),
                },
            },
        };
        let err = encode_record(&record).unwrap_err();
        assert!(err.message.contains("non-finite"), "{}", err.message);
    }

    #[test]
    fn compressed_fpi_uses_compact_binary_payload() {
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::FullPageImageCompressed {
                file_id: 1,
                page_num: 0,
                codec: 1,
                dict_id: 0,
                payload: vec![0u8; 100],
            },
        };
        let bytes = encode_record(&record).unwrap();
        // 4 (file_id) + 4 (page_num) + 1 (codec) + 4 (dict_id) + 100 (payload)
        assert_eq!(bytes.len(), HEADER_LEN + 13 + 100 + CRC_LEN);
    }

    #[test]
    fn create_dictionary_uses_compact_binary_payload() {
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::CreateDictionary {
                dict_id: 2,
                table_id: 5,
                bytes: vec![7u8; 50],
            },
        };
        let bytes = encode_record(&record).unwrap();
        // 4 (dict_id) + 4 (table_id) + 50 (bytes)
        assert_eq!(bytes.len(), HEADER_LEN + 8 + 50 + CRC_LEN);
    }

    #[test]
    fn decode_rejects_malformed_physical_payload() {
        // A heap-delete needs a 10-byte payload; frame one with only 4 bytes.
        let payload = [0u8; 4];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.push(TYPE_HEAP_DELETE);
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes);
        bytes.extend_from_slice(&hasher.finalize().to_le_bytes());

        let err = decode_record(&bytes).unwrap_err();
        assert_eq!(err.kind, common::ErrorKind::Wal);
    }

    #[test]
    fn decode_rejects_oversized_declared_physical_payload_before_materialization() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.push(TYPE_FULL_PAGE_IMAGE);
        let oversized = u32::try_from(8 + MAX_PAGE_BYTES + 1).unwrap();
        bytes.extend_from_slice(&oversized.to_le_bytes());

        let error = decode_record(&bytes).unwrap_err();
        assert!(error.message.contains("record-type size limit"));
    }

    #[test]
    fn encode_rejects_oversized_physical_body() {
        let error = encode_record(&WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::FullPageImage {
                file_id: 1,
                page_num: 0,
                image: vec![0; MAX_PAGE_BYTES + 1],
            },
        })
        .unwrap_err();
        assert!(error.message.contains("full-page image exceeds"));
    }

    #[test]
    fn commit_subxid_decoder_rejects_an_extra_item() {
        let payload = serde_json::to_vec(&serde_json::json!({
            "CommitWithSubxids": {
                "subxids": vec![1_u64; MAX_COMMITTED_SUBXIDS + 1]
            }
        }))
        .unwrap();
        let error = decode_payload(TYPE_COMMIT_WITH_SUBXIDS, &payload).unwrap_err();
        assert!(error.message.contains("subxid limit"));
    }

    #[test]
    fn specialized_catalog_wal_payload_is_rejected() {
        let error = decode_payload(1, br#"{"CreateTable":{"schema":{}}}"#).unwrap_err();
        assert!(
            error.message.contains("unsupported legacy catalog WAL"),
            "{}",
            error.message
        );
    }
}
