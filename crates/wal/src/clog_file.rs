//! Durable CLOG snapshot (`clog.dat`) — the persisted transaction-status map.
//!
//! The in-memory [`Clog`](crate::Clog) is reconstructed at every open — seeded from
//! this snapshot when present, or rebuilt from the WAL only for an unrecycled
//! replay-floor-zero stream. The durable CLOG snapshot
//! persists transaction outcomes (and the two floors) so the WAL no longer has to
//! retain `Abort` records to remember them: it carries the statuses across restart
//! and lets recovery fold only the post-snapshot `Commit`/`Abort` records
//! (`docs/specs/mvcc.md` §5.4).
//!
//! The on-disk envelope mirrors the control record
//! (`crates/control/src/manifest.rs`): a magic + version + length + CRC32 header
//! over a JSON payload, written whole at each checkpoint via temp + rename +
//! directory fsync.

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

use common::{CheckedSliceReader, DbError, Lsn, Result, SqlState, TxnId};
use serde::{Deserialize, Serialize};

const CLOG_MAGIC: &[u8; 4] = b"SGCL";
const CLOG_VERSION: u32 = 2;
const CLOG_HEADER_LEN: usize = 16;
pub(crate) const MAX_CLOG_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_CLOG_FILE_BYTES: usize = CLOG_HEADER_LEN + MAX_CLOG_PAYLOAD_BYTES;
const MAX_CLOG_STATUS_COUNT: usize = 1_000_000;

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
    #[serde(deserialize_with = "deserialize_committed")]
    committed: Vec<TxnId>,
    #[serde(deserialize_with = "deserialize_aborted")]
    aborted: Vec<TxnId>,
}

fn deserialize_committed<'de, D>(deserializer: D) -> std::result::Result<Vec<TxnId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    common::deserialize_bounded_vec_named(
        deserializer,
        MAX_CLOG_STATUS_COUNT,
        "committed CLOG status list",
    )
}

fn deserialize_aborted<'de, D>(deserializer: D) -> std::result::Result<Vec<TxnId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    common::deserialize_bounded_vec_named(
        deserializer,
        MAX_CLOG_STATUS_COUNT,
        "aborted CLOG status list",
    )
}

/// Encode a CLOG snapshot into its durable envelope (header + JSON payload).
pub(crate) fn encode_clog(snapshot: &ClogSnapshot) -> Result<Vec<u8>> {
    if snapshot.committed.len() > MAX_CLOG_STATUS_COUNT
        || snapshot.aborted.len() > MAX_CLOG_STATUS_COUNT
    {
        return Err(corrupt_clog("CLOG status list exceeds the item limit"));
    }
    let payload = ClogPayload {
        clog_lsn: snapshot.clog_lsn,
        committed_floor: snapshot.committed_floor,
        vacuum_floor: snapshot.vacuum_floor,
        committed: snapshot.committed.clone(),
        aborted: snapshot.aborted.clone(),
    };
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|err| corrupt_clog(format!("failed to encode CLOG payload: {err}")))?;
    if payload_bytes.len() > MAX_CLOG_PAYLOAD_BYTES {
        return Err(corrupt_clog("CLOG payload exceeds 64 MiB"));
    }
    let payload_len = u32::try_from(payload_bytes.len())
        .map_err(|_| corrupt_clog("CLOG payload is too large"))?;
    let checksum = crc32fast::hash(&payload_bytes);

    let envelope_len = CLOG_HEADER_LEN
        .checked_add(payload_bytes.len())
        .ok_or_else(|| corrupt_clog("CLOG envelope length overflows"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(envelope_len)
        .map_err(|_| corrupt_clog("cannot allocate CLOG envelope"))?;
    bytes.extend_from_slice(CLOG_MAGIC);
    bytes.extend_from_slice(&CLOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&payload_bytes);
    Ok(bytes)
}

/// Decode a CLOG snapshot from its durable envelope, validating the magic,
/// version, length, and CRC. A mismatch returns a storage error. The caller treats a
/// **missing** `clog.dat` as "no snapshot" only for an unrecycled replay-floor-zero
/// WAL; after recycling, absence is fatal. A **corrupt** snapshot is always surfaced
/// as an error (like a bad `manifest.dat`):
/// the atomic temp+rename write never tears, so a mismatch is real corruption.
pub(crate) fn decode_clog(bytes: &[u8]) -> Result<ClogSnapshot> {
    if bytes.len() < CLOG_HEADER_LEN {
        return Err(corrupt_clog("CLOG file is too short"));
    }
    let mut reader = CheckedSliceReader::new(bytes);
    let magic = reader
        .take(CLOG_MAGIC.len())
        .map_err(|_| corrupt_clog("CLOG file is too short"))?;
    if magic != CLOG_MAGIC {
        return Err(corrupt_clog("CLOG file magic mismatch"));
    }

    let version = read_u32(&mut reader, "CLOG file version")?;
    if version != CLOG_VERSION {
        return Err(corrupt_clog(format!(
            "unsupported CLOG file version {version}"
        )));
    }

    let payload_len = usize::try_from(read_u32(&mut reader, "CLOG file payload length")?)
        .map_err(|_| corrupt_clog("CLOG payload length does not fit usize"))?;
    if payload_len > MAX_CLOG_PAYLOAD_BYTES {
        return Err(corrupt_clog("CLOG payload exceeds 64 MiB"));
    }
    let expected_checksum = read_u32(&mut reader, "CLOG file checksum")?;
    if reader.remaining() != payload_len {
        return Err(corrupt_clog("CLOG file length mismatch"));
    }

    let payload_bytes = reader
        .take(payload_len)
        .map_err(|_| corrupt_clog("CLOG file length mismatch"))?;
    reader
        .finish()
        .map_err(|_| corrupt_clog("CLOG file length mismatch"))?;
    if crc32fast::hash(payload_bytes) != expected_checksum {
        return Err(corrupt_clog("CLOG file checksum mismatch"));
    }

    let payload: ClogPayload = serde_json::from_slice(payload_bytes)
        .map_err(|err| corrupt_clog(format!("failed to decode CLOG payload: {err}")))?;
    validate_status_lists(
        payload.committed_floor,
        &payload.committed,
        &payload.aborted,
    )?;
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
fn validate_status_lists(
    committed_floor: TxnId,
    committed: &[TxnId],
    aborted: &[TxnId],
) -> Result<()> {
    for list in [committed, aborted] {
        if list
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left >= right))
        {
            return Err(corrupt_clog(
                "CLOG status lists must be sorted without duplicates",
            ));
        }
    }
    if committed
        .iter()
        .chain(aborted)
        .any(|id| *id < committed_floor)
    {
        return Err(corrupt_clog(
            "CLOG explicit status is below the committed floor",
        ));
    }
    // Both are sorted; a linear merge finds any shared id without allocating.
    let mut committed = committed.iter().peekable();
    let mut aborted = aborted.iter().peekable();
    while let (Some(left), Some(right)) = (committed.peek(), aborted.peek()) {
        match left.cmp(right) {
            std::cmp::Ordering::Less => {
                committed.next();
            }
            std::cmp::Ordering::Greater => {
                aborted.next();
            }
            std::cmp::Ordering::Equal => {
                return Err(corrupt_clog(
                    "CLOG status lists must not record the same id as both committed and aborted",
                ));
            }
        }
    }
    Ok(())
}

fn read_u32(reader: &mut CheckedSliceReader<'_>, field: &str) -> Result<u32> {
    reader
        .read_u32_le()
        .map_err(|_| corrupt_clog(format!("{field} is incomplete")))
}

fn corrupt_clog(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use super::{
        CLOG_HEADER_LEN, CLOG_MAGIC, CLOG_VERSION, ClogSnapshot, MAX_CLOG_PAYLOAD_BYTES,
        MAX_CLOG_STATUS_COUNT, decode_clog, encode_clog,
    };

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

    #[test]
    fn rejects_explicit_status_below_the_committed_floor() {
        let bytes = encode_clog(&ClogSnapshot {
            aborted: vec![9],
            ..snapshot()
        })
        .unwrap();

        let err = decode_clog(&bytes).unwrap_err();
        assert!(err.message.contains("below the committed floor"));
    }

    #[test]
    fn rejects_declared_payload_over_the_byte_limit_before_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CLOG_MAGIC);
        bytes.extend_from_slice(&CLOG_VERSION.to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from(MAX_CLOG_PAYLOAD_BYTES + 1)
                .unwrap()
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        assert_eq!(bytes.len(), CLOG_HEADER_LEN);

        let error = decode_clog(&bytes).unwrap_err();
        assert!(error.message.contains("exceeds 64 MiB"));
    }

    #[test]
    fn rejects_status_list_over_the_item_limit() {
        let snapshot = ClogSnapshot {
            committed: vec![3; MAX_CLOG_STATUS_COUNT + 1],
            ..snapshot()
        };
        let error = encode_clog(&snapshot).unwrap_err();
        assert!(error.message.contains("item limit"));
    }
}
