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

use common::{CheckedSliceReader, DbError, Lsn, Result, SqlState, TableId};
use serde::{Deserialize, Serialize};

const MANIFEST_MAGIC: &[u8; 4] = b"SGMF";
const MANIFEST_VERSION: u32 = 3;
const MANIFEST_HEADER_LEN: usize = 16;

/// The durable control record. It is the checkpoint commit point: the redo
/// boundary (`checkpoint_lsn`), the live table ids, and the catalog snapshot,
/// written atomically as a single CRC-checked envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlData {
    pub checkpoint_lsn: Lsn,
    pub tables: Vec<TableId>,
    pub catalog: Vec<u8>,
    pub page_size: u32,
}

#[derive(Serialize, Deserialize)]
struct ControlPayload {
    checkpoint_lsn: Lsn,
    tables: Vec<TableId>,
    catalog: Vec<u8>,
    page_size: u32,
}

pub(crate) fn encode_control(control: &ControlData) -> Result<Vec<u8>> {
    let payload = ControlPayload {
        checkpoint_lsn: control.checkpoint_lsn,
        tables: control.tables.clone(),
        catalog: control.catalog.clone(),
        page_size: control.page_size,
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| corrupt_control(format!("failed to encode control payload: {err}")))?;
    let payload_len = u32::try_from(payload_bytes.len())
        .map_err(|_| corrupt_control("control payload is too large"))?;
    let checksum = crc32fast::hash(&payload_bytes);

    let envelope_len = MANIFEST_HEADER_LEN
        .checked_add(payload_bytes.len())
        .ok_or_else(|| corrupt_control("control envelope length overflows"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(envelope_len)
        .map_err(|_| corrupt_control("cannot allocate control envelope"))?;
    bytes.extend_from_slice(MANIFEST_MAGIC);
    bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&payload_bytes);
    Ok(bytes)
}

pub(crate) fn decode_control(bytes: &[u8]) -> Result<ControlData> {
    if bytes.len() < MANIFEST_HEADER_LEN {
        return Err(corrupt_control("control file is too short"));
    }
    let mut reader = CheckedSliceReader::new(bytes);
    let magic = reader
        .take(MANIFEST_MAGIC.len())
        .map_err(|_| corrupt_control("control file is too short"))?;
    if magic != MANIFEST_MAGIC {
        return Err(corrupt_control("control file magic mismatch"));
    }

    let version = read_u32(&mut reader, "control file version")?;
    if version != MANIFEST_VERSION {
        return Err(corrupt_control(format!(
            "unsupported control file version {version}",
        )));
    }

    let payload_len = usize::try_from(read_u32(&mut reader, "control file payload length")?)
        .map_err(|_| corrupt_control("control file payload length does not fit usize"))?;
    let expected_checksum = read_u32(&mut reader, "control file checksum")?;
    if reader.remaining() != payload_len {
        return Err(corrupt_control("control file length mismatch"));
    }

    let payload_bytes = reader
        .take(payload_len)
        .map_err(|_| corrupt_control("control file length mismatch"))?;
    reader
        .finish()
        .map_err(|_| corrupt_control("control file length mismatch"))?;
    if crc32fast::hash(payload_bytes) != expected_checksum {
        return Err(corrupt_control("control file checksum mismatch"));
    }

    let payload: ControlPayload = serde_json::from_slice(payload_bytes)
        .map_err(|err| corrupt_control(format!("failed to decode control payload: {err}")))?;
    validate_sorted_tables(&payload.tables)?;
    Ok(ControlData {
        checkpoint_lsn: payload.checkpoint_lsn,
        tables: payload.tables,
        catalog: payload.catalog,
        page_size: payload.page_size,
    })
}

fn read_u32(reader: &mut CheckedSliceReader<'_>, field: &str) -> Result<u32> {
    reader
        .read_u32_le()
        .map_err(|_| corrupt_control(format!("{field} is incomplete")))
}

fn validate_sorted_tables(tables: &[TableId]) -> Result<()> {
    if tables
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left >= right))
    {
        return Err(corrupt_control(
            "control file tables must contain sorted table ids without duplicates",
        ));
    }
    Ok(())
}

fn corrupt_control(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use super::{ControlData, decode_control, encode_control};

    fn control() -> ControlData {
        ControlData {
            checkpoint_lsn: 42,
            tables: vec![1, 2],
            catalog: b"catalog-bytes".to_vec(),
            page_size: 8192,
        }
    }

    #[test]
    fn round_trips_control_data() {
        let bytes = encode_control(&control()).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), control());
    }

    #[test]
    fn control_round_trips_page_size() {
        let data = ControlData {
            checkpoint_lsn: 7,
            tables: vec![1, 2],
            catalog: vec![9, 9],
            page_size: 8192,
        };
        let bytes = encode_control(&data).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), data);
    }

    #[test]
    fn rejects_payload_byte_tampering() {
        let mut bytes = encode_control(&control()).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;

        let err = decode_control(&bytes).unwrap_err();
        assert!(err.message.contains("checksum mismatch"));
    }

    #[test]
    fn rejects_trailing_bytes_outside_envelope() {
        let mut bytes = encode_control(&control()).unwrap();
        bytes.push(0);

        let err = decode_control(&bytes).unwrap_err();
        assert!(err.message.contains("length mismatch"));
    }

    #[test]
    fn rejects_legacy_manifest_version() {
        // A v1 (full-snapshot) manifest envelope must be rejected, not migrated.
        let mut bytes = encode_control(&control()).unwrap();
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes());

        let err = decode_control(&bytes).unwrap_err();
        assert!(err.message.contains("unsupported control file version"));
    }

    #[test]
    fn rejects_unsorted_or_duplicate_tables() {
        for tables in [vec![2, 1], vec![1, 1]] {
            let bytes = encode_control(&ControlData {
                checkpoint_lsn: 42,
                tables,
                catalog: Vec::new(),
                page_size: 8192,
            })
            .unwrap();

            let err = decode_control(&bytes).unwrap_err();
            assert!(err.message.contains("sorted table ids"));
        }
    }
}
