use common::{DbError, Lsn, Result, SqlState};
use crc32fast::Hasher;

use crate::{WalRecord, WalRecordKind};

const HEADER_LEN: usize = 8 + 8 + 1 + 4;
const CRC_LEN: usize = 4;

/// `HeapUpdateHeader` payload: file_id(4) + page_num(4) + slot(2) + xmax(8) +
/// t_ctid page(4) + t_ctid slot(2) + infomask(2).
const HEAP_UPDATE_HEADER_LEN: usize = 4 + 4 + 2 + 8 + 4 + 2 + 2;

const TYPE_CREATE_TABLE: u8 = 1;
const TYPE_DROP_TABLE: u8 = 2;
const TYPE_COMMIT: u8 = 3;
const TYPE_CHECKPOINT: u8 = 4;
// Physiological redo records use compact binary payloads instead of JSON.
const TYPE_HEAP_INIT: u8 = 5;
const TYPE_HEAP_INSERT: u8 = 6;
const TYPE_HEAP_DELETE: u8 = 7;
const TYPE_FULL_PAGE_IMAGE: u8 = 8;
const TYPE_CREATE_INDEX: u8 = 9;
const TYPE_DROP_INDEX: u8 = 10;
const TYPE_ABORT: u8 = 11;
const TYPE_HEAP_UPDATE_HEADER: u8 = 12;
const TYPE_COMMIT_WITH_SUBXIDS: u8 = 13;
const TYPE_CREATE_SEQUENCE: u8 = 14;
const TYPE_DROP_SEQUENCE: u8 = 15;
const TYPE_SEQUENCE_ADVANCE: u8 = 16;
const TYPE_SET_SEQUENCE_VALUE: u8 = 17;
pub(crate) const TYPE_FULL_PAGE_IMAGE_COMPRESSED: u8 = 18;
pub(crate) const TYPE_CREATE_DICTIONARY: u8 = 19;
pub(crate) const TYPE_ALTER_TABLE_COMPRESSION: u8 = 20;
pub(crate) const TYPE_ALTER_TABLE_TOAST: u8 = 21;
pub(crate) const TYPE_TRUNCATE_TABLE: u8 = 22;
pub(crate) const TYPE_ALTER_TABLE_PRIMARY_KEY: u8 = 23;
pub(crate) const TYPE_UPDATE_TABLE_SCHEMA: u8 = 24;
pub(crate) const TYPE_CREATE_VIEW: u8 = 25;
pub(crate) const TYPE_REPLACE_VIEW: u8 = 26;
pub(crate) const TYPE_DROP_VIEW: u8 = 27;

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
                records.push((*record, (next_offset - offset) as u64));
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
        record: Box::new(WalRecord { lsn, txn_id, kind }),
        next_offset: record_end,
    })
}

fn record_type(kind: &WalRecordKind) -> u8 {
    match kind {
        WalRecordKind::CreateTable { .. } => TYPE_CREATE_TABLE,
        WalRecordKind::DropTable { .. } => TYPE_DROP_TABLE,
        WalRecordKind::CreateIndex { .. } => TYPE_CREATE_INDEX,
        WalRecordKind::DropIndex { .. } => TYPE_DROP_INDEX,
        WalRecordKind::CreateSequence { .. } => TYPE_CREATE_SEQUENCE,
        WalRecordKind::DropSequence { .. } => TYPE_DROP_SEQUENCE,
        WalRecordKind::CreateView { .. } => TYPE_CREATE_VIEW,
        WalRecordKind::ReplaceView { .. } => TYPE_REPLACE_VIEW,
        WalRecordKind::DropView { .. } => TYPE_DROP_VIEW,
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
        WalRecordKind::AlterTableCompression { .. } => TYPE_ALTER_TABLE_COMPRESSION,
        WalRecordKind::AlterTableToast { .. } => TYPE_ALTER_TABLE_TOAST,
        WalRecordKind::TruncateTable { .. } => TYPE_TRUNCATE_TABLE,
        WalRecordKind::AlterTablePrimaryKey { .. } => TYPE_ALTER_TABLE_PRIMARY_KEY,
        WalRecordKind::UpdateTableSchema { .. } => TYPE_UPDATE_TABLE_SCHEMA,
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
            let mut payload = Vec::with_capacity(8 + image.len());
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
            let mut buf = Vec::with_capacity(13 + payload.len());
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
            let mut buf = Vec::with_capacity(8 + bytes.len());
            buf.extend_from_slice(&dict_id.to_le_bytes());
            buf.extend_from_slice(&table_id.to_le_bytes());
            buf.extend_from_slice(bytes);
            Ok(buf)
        }
        // AlterTableCompression/AlterTableToast/TruncateTable/AlterTablePrimaryKey/
        // UpdateTableSchema: no arms — the `_ =>` serde_json fallback handles
        // logical DDL records.
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
        TYPE_HEAP_UPDATE_HEADER => {
            if payload.len() != HEAP_UPDATE_HEADER_LEN {
                return Err(wal_error("WAL heap-update-header payload is malformed"));
            }
            Ok(WalRecordKind::HeapUpdateHeader {
                file_id: read_u32(payload, 0)?,
                page_num: read_u32(payload, 4)?,
                slot: read_u16(payload, 8)?,
                xmax: read_u64(payload, 10)?,
                t_ctid: (read_u32(payload, 18)?, read_u16(payload, 22)?),
                infomask: read_u16(payload, 24)?,
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
        TYPE_FULL_PAGE_IMAGE_COMPRESSED => {
            if payload.len() < 13 {
                return Err(wal_error("compressed full-page-image payload too short"));
            }
            Ok(WalRecordKind::FullPageImageCompressed {
                file_id: read_u32(payload, 0)?,
                page_num: read_u32(payload, 4)?,
                codec: payload[8],
                dict_id: read_u32(payload, 9)?,
                payload: payload[13..].to_vec(),
            })
        }
        TYPE_CREATE_DICTIONARY => {
            if payload.len() < 8 {
                return Err(wal_error("create-dictionary payload too short"));
            }
            Ok(WalRecordKind::CreateDictionary {
                dict_id: read_u32(payload, 0)?,
                table_id: read_u32(payload, 4)?,
                bytes: payload[8..].to_vec(),
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

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    bytes
        .get(offset..offset + 8)
        .and_then(|slice| slice.try_into().ok())
        .map(u64::from_le_bytes)
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
        record: Box<WalRecord>,
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
    fn round_trips_logical_schema_and_sequence_records() {
        let kinds = [
            WalRecordKind::CreateIndex {
                schema: common::IndexSchema {
                    id: 3,
                    storage_id: 30,
                    table: 1,
                    name: "users_name".to_string(),
                    columns: vec![1],
                    unique: true,
                    constraint: common::IndexConstraintKind::None,
                },
            },
            WalRecordKind::UpdateTableSchema {
                schema: common::TableSchema {
                    id: 1,
                    storage_id: 10,
                    name: "users".to_string(),
                    columns: vec![
                        common::ColumnDef {
                            id: 0,
                            name: "id".to_string(),
                            data_type: common::DataType::Integer,
                            nullable: false,
                            max_length: None,
                            default: None,
                            pg_type: None,
                        },
                        common::ColumnDef {
                            id: 1,
                            name: "code".to_string(),
                            data_type: common::DataType::Integer,
                            nullable: true,
                            max_length: None,
                            default: None,
                            pg_type: None,
                        },
                    ],
                    primary_key: vec![0],
                    schema_version: 2,
                    compression: common::CompressionSetting::None,
                    active_dict_id: None,
                    toast: common::ToastOptions::legacy_catalog_default(),
                    toast_table_id: None,
                    relation_kind: common::RelationKind::User,
                    checks: Vec::new(),
                },
                indexes: vec![common::IndexSchema {
                    id: 3,
                    storage_id: 31,
                    table: 1,
                    name: "users_code".to_string(),
                    columns: vec![1],
                    unique: false,
                    constraint: common::IndexConstraintKind::None,
                }],
            },
            WalRecordKind::DropIndex { index: 3 },
            WalRecordKind::CreateSequence {
                schema: common::SequenceSchema {
                    id: 4,
                    name: "users_id_seq".to_string(),
                    increment: 1,
                    min_value: 1,
                    max_value: i64::MAX,
                    start: 1,
                    cycle: false,
                    owned: false,
                    last_value: 1,
                    is_called: false,
                },
            },
            WalRecordKind::DropSequence { sequence: 4 },
            WalRecordKind::CreateView {
                schema: common::ViewSchema {
                    id: 5,
                    name: "active_users".to_string(),
                    columns: vec![common::ColumnDef {
                        id: 0,
                        name: "id".to_string(),
                        data_type: common::DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    }],
                    definition: "select id from users".to_string(),
                    dependencies: vec![common::ViewDependency {
                        relation: 1,
                        columns: vec![0],
                        all_columns: false,
                    }],
                    schema_version: 1,
                },
            },
            WalRecordKind::ReplaceView {
                schema: common::ViewSchema {
                    id: 5,
                    name: "active_users".to_string(),
                    columns: vec![common::ColumnDef {
                        id: 0,
                        name: "id".to_string(),
                        data_type: common::DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    }],
                    definition: "select id from users where id > 0".to_string(),
                    dependencies: vec![common::ViewDependency {
                        relation: 1,
                        columns: vec![0],
                        all_columns: false,
                    }],
                    schema_version: 2,
                },
            },
            WalRecordKind::DropView { view: 5 },
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
    fn round_trips_compression_and_toast_records() {
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
            WalRecordKind::AlterTableCompression {
                table_id: 3,
                compression: common::CompressionSetting::Zstd,
                active_dict_id: Some(7),
            },
            WalRecordKind::AlterTableCompression {
                table_id: 4,
                compression: common::CompressionSetting::None,
                active_dict_id: None,
            },
            WalRecordKind::AlterTableToast {
                table_id: 5,
                toast: common::ToastOptions {
                    mode: common::ToastMode::Aggressive,
                    tuple_target: 4096,
                    min_value_size: 512,
                    compression: common::ToastCompression::Zstd,
                    active_dict_id: None,
                },
                toast_table_id: Some(6),
            },
            WalRecordKind::TruncateTable {
                table_id: 5,
                new_table_storage_id: 20,
                new_toast_storage_id: Some((6, 21)),
                new_index_storage_ids: vec![(7, 22), (8, 23)],
            },
            WalRecordKind::AlterTablePrimaryKey {
                table_id: 5,
                primary_key: vec![0, 2],
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
}
