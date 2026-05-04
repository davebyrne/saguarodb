use common::{DbError, Lsn, Result, SqlState, TableId};
use serde::{Deserialize, Serialize};

const MANIFEST_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub generation: u64,
    pub checkpoint_lsn: Lsn,
    pub tables: Vec<TableId>,
}

#[derive(Serialize)]
struct ManifestPayload<'a> {
    version: u32,
    generation: u64,
    checkpoint_lsn: Lsn,
    tables: &'a [TableId],
}

#[derive(Deserialize, Serialize)]
struct StoredManifest {
    version: u32,
    generation: u64,
    checkpoint_lsn: Lsn,
    tables: Vec<TableId>,
    checksum: u32,
}

pub(crate) fn encode_manifest(metadata: &SnapshotMetadata) -> Result<Vec<u8>> {
    let payload = ManifestPayload {
        version: MANIFEST_VERSION,
        generation: metadata.generation,
        checkpoint_lsn: metadata.checkpoint_lsn,
        tables: &metadata.tables,
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| corrupt_manifest(format!("failed to encode manifest payload: {err}")))?;
    let stored = StoredManifest {
        version: MANIFEST_VERSION,
        generation: metadata.generation,
        checkpoint_lsn: metadata.checkpoint_lsn,
        tables: metadata.tables.clone(),
        checksum: crc32fast::hash(&payload_bytes),
    };
    serde_json::to_vec(&stored)
        .map_err(|err| corrupt_manifest(format!("failed to encode manifest: {err}")))
}

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<SnapshotMetadata> {
    let stored: StoredManifest = serde_json::from_slice(bytes)
        .map_err(|err| corrupt_manifest(format!("failed to decode manifest: {err}")))?;
    if stored.version != MANIFEST_VERSION {
        return Err(corrupt_manifest(format!(
            "unsupported snapshot manifest version {}",
            stored.version
        )));
    }

    let payload = ManifestPayload {
        version: stored.version,
        generation: stored.generation,
        checkpoint_lsn: stored.checkpoint_lsn,
        tables: &stored.tables,
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| corrupt_manifest(format!("failed to verify manifest payload: {err}")))?;
    let actual = crc32fast::hash(&payload_bytes);
    if actual != stored.checksum {
        return Err(corrupt_manifest("snapshot manifest checksum mismatch"));
    }

    Ok(SnapshotMetadata {
        generation: stored.generation,
        checkpoint_lsn: stored.checkpoint_lsn,
        tables: stored.tables,
    })
}

fn corrupt_manifest(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}
