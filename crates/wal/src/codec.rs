use common::{DbError, Lsn, Result, SqlState};
use crc32fast::Hasher;

use crate::{WalRecord, WalRecordKind};

const HEADER_LEN: usize = 8 + 8 + 1 + 4;
const CRC_LEN: usize = 4;

const TYPE_INSERT: u8 = 1;
const TYPE_UPDATE: u8 = 2;
const TYPE_DELETE: u8 = 3;
const TYPE_CREATE_TABLE: u8 = 4;
const TYPE_DROP_TABLE: u8 = 5;
const TYPE_COMMIT: u8 = 6;
const TYPE_CHECKPOINT: u8 = 7;

pub fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(&record.kind)
        .map_err(|err| wal_error(format!("failed to serialize WAL payload: {err}")))?;
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

    let kind: WalRecordKind = serde_json::from_slice(&bytes[header_end..payload_end])
        .map_err(|err| wal_error(format!("failed to deserialize WAL payload: {err}")))?;
    if type_id != record_type(&kind) {
        return Err(wal_error("WAL record type does not match payload"));
    }

    Ok(DecodeResult::Record {
        record: WalRecord { lsn, txn_id, kind },
        next_offset: record_end,
    })
}

fn record_type(kind: &WalRecordKind) -> u8 {
    match kind {
        WalRecordKind::Insert { .. } => TYPE_INSERT,
        WalRecordKind::Update { .. } => TYPE_UPDATE,
        WalRecordKind::Delete { .. } => TYPE_DELETE,
        WalRecordKind::CreateTable { .. } => TYPE_CREATE_TABLE,
        WalRecordKind::DropTable { .. } => TYPE_DROP_TABLE,
        WalRecordKind::Commit => TYPE_COMMIT,
        WalRecordKind::Checkpoint { .. } => TYPE_CHECKPOINT,
    }
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
