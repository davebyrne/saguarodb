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
const MANIFEST_VERSION: u32 = 4;
const MANIFEST_HEADER_LEN: usize = 16;
pub(crate) const MAX_MANIFEST_BYTES: usize = 272 * 1024 * 1024;
const MAX_MANIFEST_PAYLOAD_BYTES: usize = MAX_MANIFEST_BYTES - MANIFEST_HEADER_LEN;
const MAX_MANIFEST_TABLES: usize = 65_536;
const MAX_MANIFEST_CATALOG_BYTES: usize = 64 * 1024 * 1024;

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

#[derive(Serialize)]
struct ControlPayload<'a> {
    checkpoint_lsn: Lsn,
    tables: &'a [TableId],
    catalog: &'a [u8],
    page_size: u32,
}

#[derive(Deserialize)]
struct DecodedControlPayload {
    checkpoint_lsn: Lsn,
    #[serde(deserialize_with = "deserialize_tables")]
    tables: Vec<TableId>,
    #[serde(deserialize_with = "deserialize_catalog")]
    catalog: Vec<u8>,
    page_size: u32,
}

fn deserialize_tables<'de, D>(deserializer: D) -> std::result::Result<Vec<TableId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    common::deserialize_bounded_vec_named(
        deserializer,
        MAX_MANIFEST_TABLES,
        "manifest table collection",
    )
}

fn deserialize_catalog<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    common::deserialize_bounded_vec_named(
        deserializer,
        MAX_MANIFEST_CATALOG_BYTES,
        "manifest catalog bytes",
    )
}

pub(crate) fn encode_control(control: &ControlData) -> Result<Vec<u8>> {
    if control.tables.len() > MAX_MANIFEST_TABLES {
        return Err(corrupt_control("control file has too many table ids"));
    }
    if control.catalog.len() > MAX_MANIFEST_CATALOG_BYTES {
        return Err(corrupt_control("control catalog exceeds 64 MiB"));
    }
    validate_sorted_tables(&control.tables)?;
    let payload = ControlPayload {
        checkpoint_lsn: control.checkpoint_lsn,
        tables: &control.tables,
        catalog: &control.catalog,
        page_size: control.page_size,
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| corrupt_control(format!("failed to encode control payload: {err}")))?;
    if payload_bytes.len() > MAX_MANIFEST_PAYLOAD_BYTES {
        return Err(corrupt_control("control payload exceeds size limit"));
    }
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
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(corrupt_control("control file exceeds size limit"));
    }
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
    if payload_len > MAX_MANIFEST_PAYLOAD_BYTES {
        return Err(corrupt_control("control payload exceeds size limit"));
    }
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

    let payload: DecodedControlPayload = serde_json::from_slice(payload_bytes)
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
    use super::{ControlData, MAX_MANIFEST_PAYLOAD_BYTES, decode_control, encode_control};

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
    fn rejects_oversized_declared_payload_before_materialization() {
        let mut bytes = encode_control(&control()).unwrap();
        let oversized = u32::try_from(MAX_MANIFEST_PAYLOAD_BYTES + 1).unwrap();
        bytes[8..12].copy_from_slice(&oversized.to_le_bytes());

        let err = decode_control(&bytes).unwrap_err();
        assert!(err.message.contains("payload exceeds size limit"));
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
            let error = encode_control(&ControlData {
                checkpoint_lsn: 42,
                tables,
                catalog: Vec::new(),
                page_size: 8192,
            })
            .unwrap_err();

            assert!(error.message.contains("sorted table ids"));
        }
    }

    #[test]
    fn decoder_rejects_checksummed_unsorted_or_duplicate_tables() {
        for tables in [vec![2, 1], vec![1, 1]] {
            let payload = serde_json::to_vec(&serde_json::json!({
                "checkpoint_lsn": 42,
                "tables": tables,
                "catalog": [],
                "page_size": 8192
            }))
            .unwrap();
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"SGMF");
            bytes.extend_from_slice(&4_u32.to_le_bytes());
            bytes.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
            bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
            bytes.extend_from_slice(&payload);

            let err = decode_control(&bytes).unwrap_err();
            assert!(err.message.contains("sorted table ids"));
        }
    }
}
