use common::{DbError, Lsn, Result, SqlState};
use crc32fast::Hasher;

use crate::{WalRecord, WalRecordKind};

const HEADER_LEN: usize = 8 + 8 + 1 + 4;
const CRC_LEN: usize = 4;

const TYPE_CREATE_TABLE: u8 = 1;
const TYPE_DROP_TABLE: u8 = 2;
const TYPE_COMMIT: u8 = 3;
const TYPE_CHECKPOINT: u8 = 4;
// Physiological redo records use compact binary payloads instead of JSON.
const TYPE_HEAP_INIT: u8 = 5;
const TYPE_HEAP_INSERT: u8 = 6;
const TYPE_HEAP_DELETE: u8 = 7;
const TYPE_FULL_PAGE_IMAGE: u8 = 8;

pub fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
    let payload = encode_payload(&record.kind)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| wal_error("WAL payload is too large to encode"))?;

    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len() + CRC_LEN);
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
        } if next_offset == bytes.len() => Ok(record),
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
                records.push((record, (next_offset - offset) as u64));
                offset = next_offset;
            }
            DecodeResult::Incomplete if suffix_contains_complete_record(bytes, offset + 1)? => {
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
    let Some(header_end) = offset.checked_add(HEADER_LEN) else {
        return Err(wal_error("WAL record offset overflow"));
    };
    if bytes.len() < header_end {
        return Ok(DecodeResult::Incomplete);
    }

    let header = &bytes[offset..header_end];
    let lsn = u64::from_le_bytes(
        header[0..8]
            .try_into()
            .map_err(|_| wal_error("invalid WAL LSN header"))?,
    );
    let txn_id = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .map_err(|_| wal_error("invalid WAL transaction header"))?,
    );
    let type_id = header[16];
    let payload_len = u32::from_le_bytes(
        header[17..21]
            .try_into()
            .map_err(|_| wal_error("invalid WAL payload length header"))?,
    ) as usize;

    let Some(payload_end) = header_end.checked_add(payload_len) else {
        return Err(wal_error("WAL payload length overflow"));
    };
    let Some(record_end) = payload_end.checked_add(CRC_LEN) else {
        return Err(wal_error("WAL record length overflow"));
    };
    if bytes.len() < record_end {
        return Ok(DecodeResult::Incomplete);
    }

    let stored_crc = u32::from_le_bytes(
        bytes[payload_end..record_end]
            .try_into()
            .map_err(|_| wal_error("invalid WAL CRC footer"))?,
    );
    let mut hasher = Hasher::new();
    hasher.update(&bytes[offset..payload_end]);
    let computed_crc = hasher.finalize();
    if computed_crc != stored_crc {
        return Err(wal_error("WAL record CRC mismatch"));
    }

    let kind = decode_payload(type_id, &bytes[header_end..payload_end])?;

    Ok(DecodeResult::Record {
        record: WalRecord { lsn, txn_id, kind },
        next_offset: record_end,
    })
}

fn record_type(kind: &WalRecordKind) -> u8 {
    match kind {
        WalRecordKind::CreateTable { .. } => TYPE_CREATE_TABLE,
        WalRecordKind::DropTable { .. } => TYPE_DROP_TABLE,
        WalRecordKind::Commit => TYPE_COMMIT,
        WalRecordKind::Checkpoint { .. } => TYPE_CHECKPOINT,
        WalRecordKind::HeapInit { .. } => TYPE_HEAP_INIT,
        WalRecordKind::HeapInsert { .. } => TYPE_HEAP_INSERT,
        WalRecordKind::HeapDelete { .. } => TYPE_HEAP_DELETE,
        WalRecordKind::FullPageImage { .. } => TYPE_FULL_PAGE_IMAGE,
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
            let mut payload = Vec::with_capacity(10 + row_bytes.len());
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
        WalRecordKind::FullPageImage {
            file_id,
            page_num,
            image,
        } => {
            let mut payload = Vec::with_capacity(8 + image.len());
            payload.extend_from_slice(&file_id.to_le_bytes());
            payload.extend_from_slice(&page_num.to_le_bytes());
            payload.extend_from_slice(image);
            Ok(payload)
        }
        _ => serde_json::to_vec(kind)
            .map_err(|err| wal_error(format!("failed to serialize WAL payload: {err}"))),
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
            Ok(WalRecordKind::HeapInit {
                file_id: read_u32(payload, 0)?,
                page_num: read_u32(payload, 4)?,
            })
        }
        TYPE_HEAP_INSERT => {
            if payload.len() < 10 {
                return Err(wal_error("WAL heap-insert payload is truncated"));
            }
            Ok(WalRecordKind::HeapInsert {
                file_id: read_u32(payload, 0)?,
                page_num: read_u32(payload, 4)?,
                slot: read_u16(payload, 8)?,
                row_bytes: payload[10..].to_vec(),
            })
        }
        TYPE_HEAP_DELETE => {
            if payload.len() != 10 {
                return Err(wal_error("WAL heap-delete payload is malformed"));
            }
            Ok(WalRecordKind::HeapDelete {
                file_id: read_u32(payload, 0)?,
                page_num: read_u32(payload, 4)?,
                slot: read_u16(payload, 8)?,
            })
        }
        TYPE_FULL_PAGE_IMAGE => {
            if payload.len() < 8 {
                return Err(wal_error("WAL full-page-image payload is truncated"));
            }
            Ok(WalRecordKind::FullPageImage {
                file_id: read_u32(payload, 0)?,
                page_num: read_u32(payload, 4)?,
                image: payload[8..].to_vec(),
            })
        }
        _ => {
            let kind: WalRecordKind = serde_json::from_slice(payload)
                .map_err(|err| wal_error(format!("failed to deserialize WAL payload: {err}")))?;
            if type_id != record_type(&kind) {
                return Err(wal_error("WAL record type does not match payload"));
            }
            Ok(kind)
        }
    }
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    bytes
        .get(offset..offset + 4)
        .and_then(|slice| slice.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| wal_error("WAL physical payload is truncated"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    bytes
        .get(offset..offset + 2)
        .and_then(|slice| slice.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(|| wal_error("WAL physical payload is truncated"))
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
        record: WalRecord,
        next_offset: usize,
    },
    Incomplete,
}

#[cfg(test)]
mod tests {
    use crate::{WalRecord, WalRecordKind};

    use super::{CRC_LEN, HEADER_LEN, TYPE_HEAP_DELETE, decode_record, encode_record};

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
}
