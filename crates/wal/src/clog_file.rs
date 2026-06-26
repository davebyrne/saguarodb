//! Durable CLOG snapshot (`clog.dat`) — the persisted transaction-status map.
//!
//! The in-memory [`Clog`](crate::Clog) is reconstructed at every open — seeded from
//! this snapshot when present, else rebuilt from the WAL. The durable CLOG snapshot
//! persists transaction outcomes (and the two floors) so the WAL no longer has to
//! retain `Abort` records to remember them: it carries the statuses across restart
//! and lets recovery fold only the post-snapshot `Commit`/`Abort` records
//! (`docs/specs/mvcc.md` §5.4).
//!
//! The on-disk envelope mirrors the control record
//! (`crates/control/src/manifest.rs`): a magic + version + length + CRC32 header
//! over a JSON payload, written whole at each checkpoint via temp + rename +
//! directory fsync.

use common::{DbError, Lsn, Result, SqlState, TxnId};
use serde::{Deserialize, Serialize};

const CLOG_MAGIC: &[u8; 4] = b"SGCL";
const CLOG_VERSION: u32 = 1;
const CLOG_HEADER_LEN: usize = 16;

/// A durable snapshot of the CLOG, captured at a checkpoint.
///
/// It stores the explicit statuses for the **live window** (transaction ids at or
/// above [`committed_floor`](Self::committed_floor)) plus the two floors. Every id
/// below `committed_floor` is implicit-committed — either genuinely committed, or
/// an aborted transaction whose on-disk versions a full VACUUM reclaimed — so it
/// needs no explicit entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClogSnapshot {
    /// The WAL LSN through which this snapshot has absorbed `Commit`/`Abort`
    /// records. Recovery seeds the CLOG from this snapshot and then replays only
    /// records with `lsn > clog_lsn`, bounding the status-rebuild scan.
    pub clog_lsn: Lsn,
    /// The implicit-committed floor (`docs/specs/mvcc.md` §5.4): an unrecorded
    /// normal id strictly below it reads as committed.
    pub committed_floor: TxnId,
    /// The vacuum floor (Milestone F4c): the boundary below which a full VACUUM
    /// pass reclaimed every aborted-creator tuple. Persisting it keeps CLOG
    /// pruning aggressive across restart.
    pub vacuum_floor: TxnId,
    /// Explicitly committed ids in the live window (`>= committed_floor`), sorted.
    pub committed: Vec<TxnId>,
    /// Explicitly aborted ids in the live window (`>= committed_floor`), sorted.
    pub aborted: Vec<TxnId>,
}

#[derive(Serialize, Deserialize)]
struct ClogPayload {
    clog_lsn: Lsn,
    committed_floor: TxnId,
    vacuum_floor: TxnId,
    committed: Vec<TxnId>,
    aborted: Vec<TxnId>,
}

/// Encode a CLOG snapshot into its durable envelope (header + JSON payload).
pub(crate) fn encode_clog(snapshot: &ClogSnapshot) -> Result<Vec<u8>> {
    let payload = ClogPayload {
        clog_lsn: snapshot.clog_lsn,
        committed_floor: snapshot.committed_floor,
        vacuum_floor: snapshot.vacuum_floor,
        committed: snapshot.committed.clone(),
        aborted: snapshot.aborted.clone(),
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| corrupt_clog(format!("failed to encode CLOG payload: {err}")))?;
    let payload_len = u32::try_from(payload_bytes.len())
        .map_err(|_| corrupt_clog("CLOG payload is too large"))?;
    let checksum = crc32fast::hash(&payload_bytes);

    let mut bytes = Vec::with_capacity(CLOG_HEADER_LEN + payload_bytes.len());
    bytes.extend_from_slice(CLOG_MAGIC);
    bytes.extend_from_slice(&CLOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&payload_bytes);
    Ok(bytes)
}

/// Decode a CLOG snapshot from its durable envelope, validating the magic,
/// version, length, and CRC. A mismatch returns a storage error. The caller treats a
/// **missing** `clog.dat` as "no snapshot" and falls back to rebuilding the CLOG from
/// the WAL, but a **corrupt** one is surfaced as an error (like a bad `manifest.dat`):
/// the atomic temp+rename write never tears, so a mismatch is real corruption.
pub(crate) fn decode_clog(bytes: &[u8]) -> Result<ClogSnapshot> {
    if bytes.len() < CLOG_HEADER_LEN {
        return Err(corrupt_clog("CLOG file is too short"));
    }
    if &bytes[0..4] != CLOG_MAGIC {
        return Err(corrupt_clog("CLOG file magic mismatch"));
    }

    let version = read_u32(&bytes[4..8], "CLOG file version")?;
    if version != CLOG_VERSION {
        return Err(corrupt_clog(format!(
            "unsupported CLOG file version {version}"
        )));
    }

    let payload_len = read_u32(&bytes[8..12], "CLOG file payload length")? as usize;
    let expected_len = CLOG_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| corrupt_clog("CLOG file length overflows"))?;
    if bytes.len() != expected_len {
        return Err(corrupt_clog("CLOG file length mismatch"));
    }

    let expected_checksum = read_u32(&bytes[12..16], "CLOG file checksum")?;
    let payload_bytes = &bytes[CLOG_HEADER_LEN..];
    if crc32fast::hash(payload_bytes) != expected_checksum {
        return Err(corrupt_clog("CLOG file checksum mismatch"));
    }

    let payload: ClogPayload = serde_json::from_slice(payload_bytes)
        .map_err(|err| corrupt_clog(format!("failed to decode CLOG payload: {err}")))?;
    validate_status_lists(&payload.committed, &payload.aborted)?;
    Ok(ClogSnapshot {
        clog_lsn: payload.clog_lsn,
        committed_floor: payload.committed_floor,
        vacuum_floor: payload.vacuum_floor,
        committed: payload.committed,
        aborted: payload.aborted,
    })
}

/// Reject a payload whose status lists are not the canonical form
/// [`ClogSnapshot::live_snapshot`](crate::Clog::live_snapshot) writes: each list
/// strictly increasing (sorted, no duplicates) and the two disjoint. The CRC
/// already catches media corruption, so this only fires on an encode-side logic
/// bug — but an overlap would otherwise resolve silently to `Aborted` on load, so
/// surfacing it (as `validate_sorted_tables` does for the control record) is cheap
/// insurance against a same-id-in-both-lists mistake.
fn validate_status_lists(committed: &[TxnId], aborted: &[TxnId]) -> Result<()> {
    for list in [committed, aborted] {
        if list.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(corrupt_clog(
                "CLOG status lists must be sorted without duplicates",
            ));
        }
    }
    // Both are sorted; a linear merge finds any shared id without allocating.
    let (mut i, mut j) = (0, 0);
    while i < committed.len() && j < aborted.len() {
        match committed[i].cmp(&aborted[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                return Err(corrupt_clog(
                    "CLOG status lists must not record the same id as both committed and aborted",
                ));
            }
        }
    }
    Ok(())
}

fn read_u32(bytes: &[u8], field: &str) -> Result<u32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| corrupt_clog(format!("{field} is incomplete")))?;
    Ok(u32::from_le_bytes(bytes))
}

fn corrupt_clog(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use super::{ClogSnapshot, decode_clog, encode_clog};

    fn snapshot() -> ClogSnapshot {
        ClogSnapshot {
            clog_lsn: 42,
            committed_floor: 10,
            vacuum_floor: 7,
            committed: vec![10, 12, 15],
            aborted: vec![11, 13],
        }
    }

    #[test]
    fn round_trips_a_snapshot() {
        let bytes = encode_clog(&snapshot()).unwrap();
        assert_eq!(decode_clog(&bytes).unwrap(), snapshot());
    }

    #[test]
    fn round_trips_an_empty_window() {
        let empty = ClogSnapshot {
            clog_lsn: 0,
            committed_floor: 3,
            vacuum_floor: 3,
            committed: Vec::new(),
            aborted: Vec::new(),
        };
        let bytes = encode_clog(&empty).unwrap();
        assert_eq!(decode_clog(&bytes).unwrap(), empty);
    }

    #[test]
    fn rejects_payload_byte_tampering() {
        let mut bytes = encode_clog(&snapshot()).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;

        let err = decode_clog(&bytes).unwrap_err();
        assert!(err.message.contains("checksum mismatch"));
    }

    #[test]
    fn rejects_trailing_bytes_outside_envelope() {
        let mut bytes = encode_clog(&snapshot()).unwrap();
        bytes.push(0);

        let err = decode_clog(&bytes).unwrap_err();
        assert!(err.message.contains("length mismatch"));
    }

    #[test]
    fn rejects_truncated_header() {
        let bytes = encode_clog(&snapshot()).unwrap();
        let err = decode_clog(&bytes[..8]).unwrap_err();
        assert!(err.message.contains("length mismatch") || err.message.contains("too short"));
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = encode_clog(&snapshot()).unwrap();
        bytes[4..8].copy_from_slice(&999u32.to_le_bytes());

        let err = decode_clog(&bytes).unwrap_err();
        assert!(err.message.contains("unsupported CLOG file version"));
    }

    #[test]
    fn rejects_magic_mismatch() {
        let mut bytes = encode_clog(&snapshot()).unwrap();
        bytes[0] = b'X';

        let err = decode_clog(&bytes).unwrap_err();
        assert!(err.message.contains("magic mismatch"));
    }

    #[test]
    fn rejects_unsorted_or_duplicate_status_list() {
        for snapshot in [
            ClogSnapshot {
                committed: vec![15, 12], // unsorted
                ..snapshot()
            },
            ClogSnapshot {
                aborted: vec![13, 13], // duplicate
                ..snapshot()
            },
        ] {
            let bytes = encode_clog(&snapshot).unwrap();
            let err = decode_clog(&bytes).unwrap_err();
            assert!(err.message.contains("sorted without duplicates"));
        }
    }

    #[test]
    fn rejects_id_recorded_as_both_committed_and_aborted() {
        let bytes = encode_clog(&ClogSnapshot {
            committed: vec![10, 12, 15],
            aborted: vec![12], // 12 also committed
            ..snapshot()
        })
        .unwrap();

        let err = decode_clog(&bytes).unwrap_err();
        assert!(err.message.contains("both committed and aborted"));
    }
}
