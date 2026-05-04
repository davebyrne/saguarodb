use common::{DbError, Lsn, Result, SqlState, TableId};
use serde::{Deserialize, Serialize};

const MANIFEST_MAGIC: &[u8; 4] = b"SGMF";
const MANIFEST_VERSION: u32 = 1;
const MANIFEST_HEADER_LEN: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub generation: u64,
    pub checkpoint_lsn: Lsn,
    pub tables: Vec<TableId>,
}

#[derive(Serialize, Deserialize)]
struct ManifestPayload {
    generation: u64,
    checkpoint_lsn: Lsn,
    tables: Vec<TableId>,
}

pub(crate) fn encode_manifest(metadata: &SnapshotMetadata) -> Result<Vec<u8>> {
    let payload = ManifestPayload {
        generation: metadata.generation,
        checkpoint_lsn: metadata.checkpoint_lsn,
        tables: metadata.tables.clone(),
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| corrupt_manifest(format!("failed to encode manifest payload: {err}")))?;
    let payload_len = u32::try_from(payload_bytes.len())
        .map_err(|_| corrupt_manifest("snapshot manifest payload is too large"))?;
    let checksum = crc32fast::hash(&payload_bytes);

    let mut bytes = Vec::with_capacity(MANIFEST_HEADER_LEN + payload_bytes.len());
    bytes.extend_from_slice(MANIFEST_MAGIC);
    bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&payload_bytes);
    Ok(bytes)
}

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<SnapshotMetadata> {
    if bytes.len() < MANIFEST_HEADER_LEN {
        return Err(corrupt_manifest("snapshot manifest is too short"));
    }
    if &bytes[0..4] != MANIFEST_MAGIC {
        return Err(corrupt_manifest("snapshot manifest magic mismatch"));
    }

    let version = read_u32(&bytes[4..8], "snapshot manifest version")?;
    if version != MANIFEST_VERSION {
        return Err(corrupt_manifest(format!(
            "unsupported snapshot manifest version {version}",
        )));
    }

    let payload_len = read_u32(&bytes[8..12], "snapshot manifest payload length")? as usize;
    let expected_len = MANIFEST_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| corrupt_manifest("snapshot manifest length overflows"))?;
    if bytes.len() != expected_len {
        return Err(corrupt_manifest("snapshot manifest length mismatch"));
    }

    let expected_checksum = read_u32(&bytes[12..16], "snapshot manifest checksum")?;
    let payload_bytes = &bytes[MANIFEST_HEADER_LEN..];
    let actual_checksum = crc32fast::hash(payload_bytes);
    if actual_checksum != expected_checksum {
        return Err(corrupt_manifest("snapshot manifest checksum mismatch"));
    }

    let payload: ManifestPayload = serde_json::from_slice(payload_bytes)
        .map_err(|err| corrupt_manifest(format!("failed to decode manifest payload: {err}")))?;
    validate_sorted_tables(&payload.tables)?;
    Ok(SnapshotMetadata {
        generation: payload.generation,
        checkpoint_lsn: payload.checkpoint_lsn,
        tables: payload.tables,
    })
}

fn read_u32(bytes: &[u8], field: &str) -> Result<u32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| corrupt_manifest(format!("{field} is incomplete")))?;
    Ok(u32::from_le_bytes(bytes))
}

fn validate_sorted_tables(tables: &[TableId]) -> Result<()> {
    if tables.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(corrupt_manifest(
            "snapshot manifest tables must contain sorted table ids without duplicates",
        ));
    }
    Ok(())
}

fn corrupt_manifest(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use super::{SnapshotMetadata, decode_manifest, encode_manifest};

    fn metadata() -> SnapshotMetadata {
        SnapshotMetadata {
            generation: 3,
            checkpoint_lsn: 42,
            tables: vec![1, 2],
        }
    }

    #[test]
    fn manifest_rejects_payload_byte_tampering() {
        let mut bytes = encode_manifest(&metadata()).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;

        let err = decode_manifest(&bytes).unwrap_err();
        assert!(err.message.contains("checksum mismatch"));
    }

    #[test]
    fn manifest_rejects_trailing_bytes_outside_envelope() {
        let mut bytes = encode_manifest(&metadata()).unwrap();
        bytes.push(0);

        let err = decode_manifest(&bytes).unwrap_err();
        assert!(err.message.contains("length mismatch"));
    }

    #[test]
    fn manifest_rejects_legacy_json_manifest_without_binary_envelope() {
        let bytes =
            br#"{"version":1,"generation":3,"checkpoint_lsn":42,"tables":[1],"checksum":0}"#;

        let err = decode_manifest(bytes).unwrap_err();
        assert!(err.message.contains("magic mismatch"));
    }

    #[test]
    fn manifest_rejects_unsorted_or_duplicate_tables() {
        for tables in [vec![2, 1], vec![1, 1]] {
            let bytes = encode_manifest(&SnapshotMetadata {
                generation: 3,
                checkpoint_lsn: 42,
                tables,
            })
            .unwrap();

            let err = decode_manifest(&bytes).unwrap_err();
            assert!(err.message.contains("sorted table ids"));
        }
    }
}
