#![cfg_attr(
    not(test),
    deny(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

mod clog;
mod clog_file;
mod codec;
mod file;
mod record;
mod segment;

pub use clog::Clog;
pub use clog_file::ClogSnapshot;
pub use codec::{decode_record, encode_record};
pub use file::FileWalManager;
pub use record::{WalRecord, WalRecordKind, is_redo_operation};

use common::{Lsn, Result, TxnId, TxnStatusView};

/// A WAL manager is also a [`TxnStatusView`] (backed by its in-memory CLOG): the
/// supertrait lets the storage engine — which already holds an
/// `Arc<dyn WalManager>` — upcast to `&dyn TxnStatusView` for the visibility
/// predicate (`docs/specs/mvcc.md` §6) with no extra handle. Implementors satisfy
/// it by exposing their CLOG status (`Clog: TxnStatusView` does the work).
pub trait WalManager: Send + Sync + TxnStatusView {
    /// Append `record` to the log and return its assigned LSN. The record is not
    /// durable until [`WalManager::flush`].
    ///
    /// **`Abort` contract (load-bearing).** An `Abort` record MUST record the
    /// in-memory `CLOG[txn] = Aborted` status *regardless of whether the durable
    /// write succeeds* — i.e. even when this method returns `Err`. Abort durability
    /// is best-effort (a transaction with no durable `Commit` recovers as aborted
    /// anyway, `docs/specs/mvcc.md` §8), so rollback callers log-and-continue on
    /// `Err` (`crates/server/src/query/txn.rs`). They rely on this contract: without
    /// the eager in-memory `Aborted` mark, a deregistered writer whose dirty pages
    /// reached disk would be unrecorded and a later checkpoint could float the
    /// implicit-committed floor past it, making its rolled-back versions read as
    /// committed (the `Clog::live_snapshot` precondition). `Commit` records, by
    /// contrast, only affect the CLOG once durable (staged pending until `flush`).
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    fn flush(&self) -> Result<Lsn>;
    /// Replay every retained record after `lsn`, in LSN order. Redo-all recovery
    /// (`docs/specs/mvcc.md` §8) iterates this and applies the page-mutation
    /// records ([`is_redo_operation`]); the CLOG (rebuilt at open) decides
    /// visibility afterwards.
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    /// Advance the replay floor to the boundary established by the latest
    /// successful [`WalManager::persist_clog`]. Reusing that boundary token keeps
    /// recycling O(1) without accepting an arbitrary byte position inside a frame.
    fn recycle_through(&self, lsn: Lsn) -> Result<()>;
    fn flushed_lsn(&self) -> Lsn;
    /// Return the inclusive durable retained range `(replay_floor, durable_end)`.
    /// Implementations without physical recycling may use the default zero floor.
    fn retained_range(&self) -> Result<(Lsn, Lsn)> {
        Ok((0, self.flushed_lsn()))
    }
    /// Whether checkpoint should force a full maintenance pass to keep the
    /// durable CLOG live window below its format limits.
    fn needs_clog_maintenance(&self) -> Result<bool> {
        Ok(false)
    }
    /// Return the logical stream bytes after byte position `lsn`, clamped to the
    /// retained range. Positions at or beyond the current end return zero.
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;

    /// Persist a durable CLOG snapshot (`clog.dat`) covering WAL records through
    /// `clog_lsn` (`docs/specs/mvcc.md` §5.4). The checkpoint calls this after the
    /// heap and control record are durable and **before** advancing the WAL replay
    /// floor, so the snapshot remembers every transaction outcome that becomes
    /// logically obsolete. `clog_lsn` must equal the current durable WAL end, so
    /// it is necessarily a frame boundary and becomes the only boundary accepted
    /// by a subsequent replay-floor advance. It writes the envelope atomically (temp file + rename +
    /// directory fsync), then prunes the in-memory CLOG to the same live window. Recovery loads
    /// it at the next open and replays only the post-`clog_lsn` `Commit`/`Abort`
    /// records on top.
    fn persist_clog(&self, clog_lsn: Lsn) -> Result<()>;

    /// Advance the **vacuum floor** (`docs/specs/mvcc.md` §5.4, §9, Milestone F4c):
    /// the boundary `B` below which a FULL VACUUM pass (every user table, under the
    /// exclusive guard) has reclaimed every aborted-creator tuple. The caller
    /// captures `B = next_txn_id` at the *start* of such a pass (under the guard, so
    /// no id is allocated mid-pass) and calls this *after* the pass completes; the
    /// floor takes `max(current, boundary)`.
    ///
    /// Effect: [`WalManager::persist_clog`]'s snapshot drops the explicit entry of an
    /// aborted transaction whose id is `< vacuum_floor` (its on-disk versions are
    /// reclaimed, so it reads implicit-committed below the floor — vacuously correct),
    /// which bounds how long the durable CLOG must remember it. WAL replay-floor
    /// advancement does not consult the vacuum floor; an aborted transaction
    /// `>= vacuum_floor` keeps its explicit `Aborted` entry in the snapshot.
    ///
    /// **Durable across restart when a CLOG snapshot exists.** The vacuum floor is
    /// persisted in the durable CLOG snapshot (`clog.dat`, written by
    /// [`WalManager::persist_clog`]) and reloaded at open, so a full VACUUM's
    /// reclamation horizon survives restart. Before a fresh WAL's replay floor has
    /// advanced, an absent snapshot means the floor falls
    /// back to its conservative initial value — safe, since the snapshot simply retains
    /// more aborted entries until the first post-restart full VACUUM.
    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()>;

    /// Establish the CLOG implicit-committed floor at recovery, given the
    /// transaction-id `allocation_boundary` (the next id to be handed out).
    ///
    /// **No-op when the CLOG was seeded from a durable `clog.dat` snapshot** — that
    /// snapshot's `committed_floor` is authoritative and durable. This is the
    /// no-snapshot fallback only for a fresh replay-floor-zero WAL: an unrecorded normal id below the floor
    /// reads as committed (`docs/specs/mvcc.md` §5.4), so the floor is set
    /// CONSERVATIVELY to the oldest transaction in the retained WAL whose rebuilt CLOG
    /// status is not `Committed` (aborted or in-flight), or to `allocation_boundary` if
    /// every retained transaction is committed — never marking an aborted/in-flight
    /// transaction implicitly committed. The floor is monotonic; this is called once
    /// after recovery seeds the allocator.
    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()>;

    /// Resolve crashed in-flight writers to `Aborted` at the end of recovery.
    ///
    /// Under no-undo MVCC there is no undo pass: a transaction whose pages reached
    /// disk (the relaxed flush gate / steal) but which never wrote a durable
    /// `Commit`/`Abort` is rebuilt as `InProgress` (absent from the CLOG). Left
    /// unresolved, it is neither reclaimed by VACUUM (which only reclaims recorded
    /// aborts) nor pins the implicit-committed floor, so a later full VACUUM floats
    /// the floor past it and its never-committed versions read as committed
    /// (`docs/specs/mvcc.md` §5.4, §8). This marks every still-`InProgress` id in
    /// `writer_xids` (the txn ids seen in the replayed redo records) as `Aborted` in
    /// the in-memory CLOG; the recovery checkpoint persists it via `clog.dat`.
    /// Appends NO WAL record (recovery never logs). Ids already
    /// `Committed`/`Aborted` are left unchanged.
    fn resolve_in_flight_as_aborted(
        &self,
        writer_xids: &std::collections::HashSet<u64>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use common::{ErrorKind, Lsn, Result, TxnStatusView};

    use super::{
        FileWalManager, WalManager, WalRecord, WalRecordKind, decode_record, encode_record,
        is_redo_operation,
    };
    use crate::codec::{CRC_LEN, HEADER_LEN, TYPE_CATALOG_CHANGE, encode_record_at};
    use crate::segment::{
        SEGMENT_HEADER_LEN, SEGMENT_PAYLOAD_BYTES, segment_path, wal_dir, write_stream,
    };

    #[test]
    fn encode_decode_round_trip_preserves_record() {
        let record = WalRecord {
            lsn: 3,
            txn_id: 9,
            kind: WalRecordKind::CatalogChange {
                change_set: common::CatalogChangeSet {
                    version: common::CATALOG_CHANGE_SET_VERSION,
                    mutations: Vec::new(),
                    allocator_high_water: common::CatalogAllocatorHighWater::default(),
                },
            },
        };

        let bytes = encode_record(&record).unwrap();
        let decoded = decode_record(&bytes).unwrap();

        assert_eq!(decoded, record);
    }

    #[test]
    fn corrupt_crc_returns_wal_error() {
        let record = WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::Commit,
        };
        let mut bytes = encode_record(&record).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x7f;

        let err = decode_record(&bytes).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Wal);
    }

    fn raw_catalog_frame(start: Lsn, payload_len: usize) -> Vec<u8> {
        let frame_len = HEADER_LEN + payload_len + CRC_LEN;
        let end = start + u64::try_from(frame_len).unwrap();
        let mut bytes = Vec::with_capacity(frame_len);
        bytes.extend_from_slice(&end.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.push(TYPE_CATALOG_CHANGE);
        bytes.extend_from_slice(&u32::try_from(payload_len).unwrap().to_le_bytes());
        bytes.resize(HEADER_LEN + payload_len, 4);
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    #[test]
    fn recovery_rebuilds_clog_and_replays_all_records() {
        // Redo-all (`docs/specs/mvcc.md` §8, Milestone D2): `replay_from` yields
        // EVERY record; the rebuilt CLOG — not a replay filter — distinguishes
        // committed from uncommitted. (Before D2 this used the redo-committed-only
        // `replay_committed_from`, which is retired with the relaxed flush gate.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        wal.append(WalRecord::insert_for_test(10, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 10,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(11, 2)).unwrap();
        wal.flush().unwrap();

        drop(wal);
        let recovered = FileWalManager::open(&path).unwrap();
        let recovered_end = recovered.flushed_lsn();
        assert!(recovered_end > 0);
        // The CLOG carries the outcome: txn 10 committed, txn 11 not (in-flight).
        assert!(recovered.is_committed(10));
        assert!(!recovered.is_committed(11));

        // Redo-all replays every operation record (both txns'); the Commit marker
        // is metadata, not a redo operation.
        let replayed: Vec<_> = recovered
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let operations: Vec<u64> = replayed
            .iter()
            .filter(|record| is_redo_operation(&record.kind))
            .map(|record| record.txn_id)
            .collect();
        assert_eq!(operations, vec![10, 11]);
        assert!(recovered.append(WalRecord::insert_for_test(12, 3)).unwrap() > recovered_end);
    }

    #[test]
    fn abort_record_round_trips_through_the_codec() {
        let record = WalRecord {
            lsn: 5,
            txn_id: 42,
            kind: WalRecordKind::Abort,
        };
        let bytes = encode_record(&record).unwrap();
        assert_eq!(decode_record(&bytes).unwrap(), record);
    }

    #[test]
    fn recovery_rebuilds_clog_from_commit_and_abort_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        // txn 10 commits, txn 11 aborts, txn 12 is left in flight (no marker).
        wal.append(WalRecord::insert_for_test(10, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 10,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(11, 2)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 11,
            kind: WalRecordKind::Abort,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(12, 3)).unwrap();
        wal.flush().unwrap();

        drop(wal);
        let recovered = FileWalManager::open(&path).unwrap();
        // The committed txn is committed; the aborted and in-flight txns are not.
        assert!(recovered.is_committed(10));
        assert!(recovered.is_aborted(11));
        assert!(!recovered.is_committed(12));
        assert!(!recovered.is_aborted(12));

        // Redo-all replays every operation record (committed, aborted, and
        // in-flight alike); visibility is decided afterwards by the CLOG, not by a
        // replay filter (`docs/specs/mvcc.md` §8, Milestone D2). The Commit/Abort
        // markers are metadata, not redo operations.
        let operations: Vec<u64> = recovered
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .filter(|record| is_redo_operation(&record.kind))
            .map(|record| record.txn_id)
            .collect();
        assert_eq!(operations, vec![10, 11, 12]);
    }

    #[test]
    fn recovery_marks_committed_subxids_and_keeps_rolled_back_ones_aborted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        // Top txn 100 with savepoint subxids: 101 rolled back (its own Abort
        // record, header txn_id = the subxid), 102 released (carried in the top
        // commit's subxid list). The single CommitWithSubxids commits 100 + 102.
        wal.append(WalRecord::insert_for_test(101, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 101,
            kind: WalRecordKind::Abort,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(102, 2)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 100,
            kind: WalRecordKind::CommitWithSubxids { subxids: vec![102] },
        })
        .unwrap();
        wal.flush().unwrap();

        drop(wal);
        let recovered = FileWalManager::open(&path).unwrap();
        assert!(recovered.is_committed(100), "top-level txn committed");
        assert!(recovered.is_committed(102), "released subxid committed");
        assert!(recovered.is_aborted(101), "rolled-back subxid aborted");
        assert!(!recovered.is_committed(101));
    }

    #[test]
    fn commit_with_subxids_is_pending_until_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        wal.append(WalRecord {
            lsn: 0,
            txn_id: 100,
            kind: WalRecordKind::CommitWithSubxids { subxids: vec![101] },
        })
        .unwrap();
        // Before flush the commit (and its subxids) are not durable ⇒ not committed.
        assert!(!wal.is_committed(100));
        assert!(!wal.is_committed(101));
        wal.flush().unwrap();
        assert!(wal.is_committed(100));
        assert!(wal.is_committed(101));
    }

    #[test]
    fn aborted_txn_is_recorded_before_flush() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();

        wal.append(WalRecord::insert_for_test(7, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 7,
            kind: WalRecordKind::Abort,
        })
        .unwrap();
        // Abort is not fsync-gated: the txn is not committed even unflushed, and
        // stays not-committed after flush.
        assert!(!wal.is_committed(7));
        wal.flush().unwrap();
        assert!(!wal.is_committed(7));
    }

    #[test]
    fn append_and_replay_preserve_assigned_lsn_order() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();

        let first_lsn = wal.append(WalRecord::insert_for_test(1, 10)).unwrap();
        let second_lsn = wal.append(WalRecord::insert_for_test(2, 20)).unwrap();
        assert!(first_lsn > 0);
        assert!(second_lsn > first_lsn);

        let records: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![first_lsn, second_lsn]
        );
        assert_eq!(
            records
                .iter()
                .map(|record| record.txn_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn flush_advances_durable_lsn_and_commit_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();

        wal.append(WalRecord::insert_for_test(12, 1)).unwrap();
        let commit_lsn = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 12,
                kind: WalRecordKind::Commit,
            })
            .unwrap();

        assert_eq!(wal.flushed_lsn(), 0);
        assert!(!wal.is_committed(12));
        assert_eq!(wal.flush().unwrap(), commit_lsn);
        assert_eq!(wal.flushed_lsn(), commit_lsn);
        assert!(wal.is_committed(12));
    }

    #[test]
    fn failed_flush_rolls_back_pending_commit_bytes_and_poisons_live_manager() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        wal.append(WalRecord::insert_for_test(12, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 12,
            kind: WalRecordKind::Commit,
        })
        .unwrap();

        wal.fail_next_flush_for_test("simulated flush failure");

        let err = wal.flush().unwrap_err();
        assert_eq!(err.kind, ErrorKind::Io);
        assert!(!wal.is_committed(12));
        assert!(wal.append(WalRecord::insert_for_test(13, 2)).is_err());
        assert!(
            wal.append(WalRecord {
                lsn: 0,
                txn_id: 14,
                kind: WalRecordKind::Abort,
            })
            .is_err()
        );
        assert!(wal.is_aborted(14));

        drop(wal);
        let recovered = FileWalManager::open(&path).unwrap();
        let after_failed_flush: Vec<_> = recovered
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(after_failed_flush.is_empty());
        assert!(recovered.append(WalRecord::insert_for_test(13, 2)).unwrap() > 0);
        recovered
            .append(WalRecord {
                lsn: 0,
                txn_id: 13,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        recovered.flush().unwrap();

        // The entire non-durable txn 12 suffix was discarded after the failed flush.
        assert!(!recovered.is_committed(12));
        assert!(recovered.is_committed(13));
        let operations: Vec<u64> = recovered
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .filter(|record| is_redo_operation(&record.kind))
            .map(|record| record.txn_id)
            .collect();
        assert_eq!(operations, vec![13]);
    }

    #[test]
    fn concurrent_flush_waiters_all_observe_a_shared_durability_failure() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(FileWalManager::open(dir.path()).unwrap());
        for txn_id in [12, 13] {
            wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        }
        wal.fail_next_flush_for_test("simulated shared flush failure");
        let barrier = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let wal = Arc::clone(&wal);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                wal.flush()
            }));
        }
        barrier.wait();

        for handle in threads {
            assert!(handle.join().unwrap().is_err());
        }
        assert!(!wal.is_committed(12));
        assert!(!wal.is_committed(13));
    }

    #[test]
    fn post_replace_durable_end_failure_reports_unknown_outcome_and_poisons() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 12,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.fail_next_durable_end_sync_for_test("simulated metadata directory fsync failure");

        let error = wal.flush().unwrap_err();
        assert_eq!(error.kind, ErrorKind::DurabilityOutcomeUnknown);
        assert!(wal.append(WalRecord::insert_for_test(13, 1)).is_err());
        drop(wal);

        // The injected failure occurs after replacement, so this run's marker is
        // visible on reopen. A real directory-fsync error is outcome-unknown, which
        // is why server orchestration must terminate instead of rolling back.
        let reopened = FileWalManager::open(&path).unwrap();
        assert!(reopened.is_committed(12));
    }

    #[test]
    fn failed_append_after_writing_commit_rolls_back_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        wal.fail_next_post_write_seek_for_test("simulated post-write seek failure");
        let err = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 99,
                kind: WalRecordKind::Commit,
            })
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Io);
        assert!(!wal.is_committed(99));
        assert!(wal.replay_from(0).unwrap().next().is_none());

        drop(wal);
        let recovered = FileWalManager::open(&path).unwrap();
        assert!(recovered.replay_from(0).unwrap().next().is_none());
        assert!(!recovered.is_committed(99));

        assert!(recovered.append(WalRecord::insert_for_test(1, 1)).unwrap() > 0);
    }

    #[test]
    fn recycling_advances_the_replay_floor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        // Recycling advances the exclusive replay floor without rewriting records.
        wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 1,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(2, 2)).unwrap();
        let recycle_lsn = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 2,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        wal.flush().unwrap();
        wal.persist_clog(recycle_lsn).unwrap();
        let retained_lsn = wal.append(WalRecord::insert_for_test(3, 3)).unwrap();
        wal.flush().unwrap();

        wal.recycle_through(recycle_lsn).unwrap();

        let records: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![retained_lsn]
        );

        let expected_bytes: u64 = records
            .iter()
            .map(|record| encode_record(record).unwrap().len() as u64)
            .sum();
        assert_eq!(wal.bytes_after(0).unwrap(), expected_bytes);

        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        let replay_after_checkpoint: Vec<_> = reopened
            .replay_from(recycle_lsn)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(replay_after_checkpoint.len(), 1);
        assert_eq!(replay_after_checkpoint[0].lsn, retained_lsn);
    }

    #[test]
    fn recycling_rejects_a_byte_inside_a_frame_without_changing_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();
        let boundary = wal.append(WalRecord::insert_for_test(7, 1)).unwrap();
        wal.flush().unwrap();
        wal.persist_clog(boundary).unwrap();

        assert!(wal.recycle_through(boundary - 1).is_err());
        drop(wal);

        let reopened = FileWalManager::open(&path).unwrap();
        let records = reopened
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, boundary);
    }

    #[test]
    fn exact_segment_boundary_floor_reopens_with_an_empty_successor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();
        let full_page = WalRecord {
            lsn: 0,
            txn_id: 7,
            kind: WalRecordKind::FullPageImage {
                file_id: 1,
                page_num: 0,
                image: vec![0; 8192],
            },
        };
        let filler_len = u64::try_from(encode_record(&full_page).unwrap().len()).unwrap();
        let empty_insert = WalRecord {
            lsn: 0,
            txn_id: 7,
            kind: WalRecordKind::HeapInsert {
                file_id: 1,
                page_num: 0,
                slot: 0,
                row_bytes: Vec::new(),
            },
        };
        let minimum = u64::try_from(encode_record(&empty_insert).unwrap().len()).unwrap();
        let maximum = minimum + 8192;
        let mut end = 0;
        while SEGMENT_PAYLOAD_BYTES - end > maximum * 2 {
            end = wal.append(full_page.clone()).unwrap();
        }
        let remaining = SEGMENT_PAYLOAD_BYTES - end;
        let first_len = (remaining / 2).clamp(minimum, maximum);
        let second_len = remaining - first_len;
        assert!((minimum..=maximum).contains(&second_len));
        for frame_len in [first_len, second_len] {
            end = wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 7,
                    kind: WalRecordKind::HeapInsert {
                        file_id: 1,
                        page_num: 0,
                        slot: 0,
                        row_bytes: vec![0; usize::try_from(frame_len - minimum).unwrap()],
                    },
                })
                .unwrap();
        }
        assert_eq!(end, SEGMENT_PAYLOAD_BYTES);
        assert!(filler_len <= maximum);
        wal.flush().unwrap();
        wal.persist_clog(end).unwrap();
        wal.recycle_through(end).unwrap();
        wal.append(WalRecord::insert_for_test(8, 1)).unwrap();
        drop(wal);

        let reopened = FileWalManager::open(&path).unwrap();
        assert!(reopened.replay_from(0).unwrap().next().is_none());
        assert_eq!(reopened.flushed_lsn(), end);
    }

    #[test]
    fn durable_replay_floor_failure_poisons_the_live_manager() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        let boundary = wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.flush().unwrap();
        wal.persist_clog(boundary).unwrap();
        wal.fail_next_parent_sync_for_test("simulated replay-floor sync failure");

        assert!(wal.recycle_through(boundary).is_err());
        assert!(wal.flushed_lsn_result_for_test().is_err());

        drop(wal);
        let reopened = FileWalManager::open(dir.path()).unwrap();
        assert!(reopened.replay_from(0).unwrap().next().is_none());
    }

    #[test]
    fn recycled_wal_requires_its_durable_clog_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        let boundary = wal.append(WalRecord::insert_for_test(7, 1)).unwrap();
        wal.flush().unwrap();
        wal.persist_clog(boundary).unwrap();
        wal.recycle_through(boundary).unwrap();
        drop(wal);
        std::fs::remove_file(dir.path().join("clog.dat")).unwrap();

        let Err(error) = FileWalManager::open(dir.path()) else {
            panic!("recycled WAL opened without its CLOG snapshot");
        };
        assert!(error.message.contains("CLOG snapshot is missing"));
    }

    #[test]
    fn clog_snapshot_prevents_rescanning_absorbed_status_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();
        let end = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 7,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        wal.flush().unwrap();
        wal.persist_clog(end).unwrap();
        drop(wal);

        let segment = segment_path(&wal_dir(&path), 0);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment)
            .unwrap();
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN + end - 1))
            .unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x7f;
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN + end - 1))
            .unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();

        let reopened = FileWalManager::open(&path).unwrap();
        assert!(reopened.is_committed(7));
    }

    #[test]
    fn recycled_wal_rejects_a_stale_clog_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        let first = wal.append(WalRecord::insert_for_test(7, 1)).unwrap();
        wal.flush().unwrap();
        wal.persist_clog(first).unwrap();
        let stale_clog = std::fs::read(dir.path().join("clog.dat")).unwrap();

        let second = wal.append(WalRecord::insert_for_test(8, 2)).unwrap();
        wal.flush().unwrap();
        wal.persist_clog(second).unwrap();
        wal.recycle_through(second).unwrap();
        drop(wal);
        std::fs::write(dir.path().join("clog.dat"), stale_clog).unwrap();

        let Err(error) = FileWalManager::open(dir.path()) else {
            panic!("recycled WAL opened with a stale CLOG snapshot");
        };
        assert!(error.message.contains("does not cover"));
    }

    /// Build the canonical aborted-across-checkpoint layout: txn 10 committed,
    /// txn 11 aborted, then txn 12 committed. Returns the WAL and byte-LSN
    /// boundaries for txn 11's first record and txn 12's commit.
    fn aborted_across_checkpoint_layout(path: &std::path::Path) -> (FileWalManager, Lsn, Lsn) {
        let wal = FileWalManager::open(path).unwrap();
        wal.append(WalRecord::insert_for_test(10, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 10,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        let abort_lsn = wal.append(WalRecord::insert_for_test(11, 2)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 11,
            kind: WalRecordKind::Abort,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(12, 3)).unwrap();
        let recycle_at = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 12,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        wal.flush().unwrap();
        (wal, abort_lsn, recycle_at)
    }

    #[test]
    fn replay_floor_excludes_abort_but_clog_snapshot_keeps_it_aborted() {
        // The keystone of the decoupling (`docs/specs/mvcc.md` §5.4, §8): with the
        // durable CLOG snapshot, the checkpoint persists outcomes to `clog.dat` and then
        // advances the WAL replay floor — an un-vacuumed aborted transaction's
        // `Abort` record is logically excluded, yet it stays aborted after restart
        // because the snapshot remembers it. (Pre-decoupling this txn would have pinned
        // the replay floor to keep its `Abort`.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let (wal, abort_lsn, recycle_at) = aborted_across_checkpoint_layout(&path);

        // Checkpoint order: persist the snapshot (records txn 11 aborted) BEFORE
        // advancing the floor. No VACUUM ran, so the vacuum floor stays at its default and txn
        // 11's explicit `Aborted` entry is kept in the snapshot.
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        wal.recycle_through(recycle_at).unwrap();

        // Truncation is unconditional: txn 11's `Abort` (lsn 4) and insert (lsn 3) are
        // gone from the WAL even though it was never vacuumed.
        let retained: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained.iter().all(|record| record.lsn >= recycle_at),
            "nothing logically below the replay floor is exposed"
        );
        assert!(
            !retained.iter().any(|record| record.txn_id == 11),
            "the un-vacuumed aborted txn's records are dropped (no pinning)"
        );
        let _ = abort_lsn;

        // After reopen the snapshot keeps txn 11 aborted, txn 12 committed, and txn 10
        // implicit-committed below the floor — no orphan version is resurrected.
        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        assert!(
            reopened.is_aborted(11),
            "the snapshot keeps the abort across restart"
        );
        assert!(reopened.is_committed(12));
        assert!(reopened.is_committed(10));
    }

    #[test]
    fn repeated_checkpoint_keeps_an_unvacuumed_abort_aborted_across_recovery() {
        // Regression guard for the recovery floor + repeated pruning. After
        // decoupling, the recovery-time `establish_recovery_committed_floor` MUST be a
        // no-op when a snapshot is loaded: the logical retained WAL no longer includes the abort,
        // so re-deriving the floor from it would float past txn 11, and the NEXT
        // checkpoint's snapshot would then drop txn 11's explicit `Aborted` entry — its
        // surviving tuples would read as committed (corruption). This exercises two full
        // checkpoint+recovery cycles and asserts txn 11 stays aborted throughout.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let (wal, _abort_lsn, recycle_at) = aborted_across_checkpoint_layout(&path);
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        wal.recycle_through(recycle_at).unwrap();
        drop(wal);

        // Recovery cycle 1: load the snapshot, then recovery calls the floor establisher
        // (a no-op here) and the post-replay checkpoint re-persists the snapshot.
        let r1 = FileWalManager::open(&path).unwrap();
        r1.establish_recovery_committed_floor(13).unwrap();
        assert!(
            r1.is_aborted(11),
            "the abort survives the recovery floor establisher"
        );
        r1.persist_clog(r1.flushed_lsn()).unwrap();
        drop(r1);

        // Recovery cycle 2: the re-persisted snapshot must STILL record txn 11 aborted.
        let r2 = FileWalManager::open(&path).unwrap();
        assert!(
            r2.is_aborted(11),
            "the un-vacuumed abort is not dropped by repeated checkpoint pruning"
        );
        assert!(r2.is_committed(12));
    }

    #[test]
    fn vacuum_floor_drops_a_reclaimed_abort_from_the_next_snapshot() {
        // The vacuum floor bounds the durable CLOG, not WAL recycling: once a full
        // VACUUM reclaims txn 11's tuples (floor advanced past 11), `persist_clog` drops
        // its explicit `Aborted` entry and it reads implicit-committed (vacuously correct
        // — its tuples are gone). Contrast with the un-vacuumed case above, where the
        // entry is kept.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let (wal, _abort_lsn, recycle_at) = aborted_across_checkpoint_layout(&path);

        // A full VACUUM starting at next_txn_id == 13 reclaimed every aborted-creator
        // tuple < 13, including txn 11's.
        wal.set_vacuum_floor(13).unwrap();
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        wal.recycle_through(recycle_at).unwrap();
        drop(wal);

        let reopened = FileWalManager::open(&path).unwrap();
        // txn 11 is now implicit-committed below the floor (its entry was dropped), which
        // is vacuous because VACUUM reclaimed its only on-disk versions.
        assert!(!reopened.is_aborted(11));
        assert!(reopened.is_committed(11));
        assert!(reopened.is_committed(12));
    }

    #[test]
    fn redo_all_after_checkpoint_excludes_boundary_and_metadata() {
        // Redo-all replay after a checkpoint (`docs/specs/mvcc.md` §8): `replay_from`
        // yields the post-checkpoint records, and recovery applies the page-mutation
        // ones — skipping the retained boundary `Commit` and the `Checkpoint`
        // marker, which are metadata, not redo operations. (Before D2 this used the
        // redo-committed-only `replay_committed_from`.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();

        wal.append(WalRecord::insert_for_test(10, 1)).unwrap();
        let checkpoint_lsn = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 10,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        wal.flush().unwrap();
        // txn 10 is committed, so its insert (below checkpoint_lsn) is recyclable.
        wal.persist_clog(checkpoint_lsn).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 0,
            kind: WalRecordKind::Checkpoint {
                redo_lsn: checkpoint_lsn,
            },
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(20, 2)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 20,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.flush().unwrap();
        wal.recycle_through(checkpoint_lsn).unwrap();

        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        let operations: Vec<_> = reopened
            .replay_from(checkpoint_lsn)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .filter(|record| is_redo_operation(&record.kind))
            .collect();

        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].txn_id, 20);
        assert!(operations[0].lsn > checkpoint_lsn);
    }

    #[test]
    fn bytes_after_counts_encoded_records_after_exclusive_lsn() {
        let r1 = WalRecord::insert_for_test(1, 1);
        let r2 = WalRecord::insert_for_test(2, 2);
        let r3 = WalRecord::insert_for_test(3, 3);
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();

        let first_lsn = wal.append(r1.clone()).unwrap();
        let second_lsn = wal.append(r2.clone()).unwrap();
        let third_lsn = wal.append(r3.clone()).unwrap();

        let encoded_second = encode_record(&WalRecord {
            lsn: second_lsn,
            ..r2
        })
        .unwrap()
        .len() as u64;
        let encoded_third = encode_record(&WalRecord {
            lsn: third_lsn,
            ..r3
        })
        .unwrap()
        .len() as u64;

        assert_eq!(
            wal.bytes_after(first_lsn).unwrap(),
            encoded_second + encoded_third
        );
        assert_eq!(
            wal.bytes_after(first_lsn - 1).unwrap(),
            encoded_second + encoded_third + 1
        );
        assert_eq!(wal.bytes_after(third_lsn + 1).unwrap(), 0);
    }

    #[test]
    fn incomplete_unflushed_trailing_record_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();
        let first_lsn = wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.flush().unwrap();

        let (_, encoded) = encode_record_at(WalRecord::insert_for_test(2, 2), first_lsn).unwrap();
        let partial = &encoded[..10];
        OpenOptions::new()
            .append(true)
            .open(segment_path(&wal_dir(&path), 0))
            .unwrap()
            .write_all(partial)
            .unwrap();
        drop(wal);

        let reopened = FileWalManager::open(&path).unwrap();
        let records: Vec<_> = reopened
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, first_lsn);
    }

    #[test]
    fn large_incomplete_unflushed_trailing_record_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        let first_lsn = wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let encoded = raw_catalog_frame(first_lsn, 1_000_000);
        write_stream(&wal_dir(dir.path()), first_lsn, &encoded[..500_000])
            .unwrap()
            .sync_all()
            .unwrap();

        let reopened = FileWalManager::open(dir.path()).unwrap();
        let records = reopened
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, first_lsn);
    }

    #[test]
    fn checksum_invalid_cross_segment_unflushed_tail_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        drop(wal);
        let encoded = raw_catalog_frame(0, usize::try_from(SEGMENT_PAYLOAD_BYTES).unwrap() + 100);
        write_stream(&wal_dir(dir.path()), 0, &encoded)
            .unwrap()
            .sync_all()
            .unwrap();
        let second_path = segment_path(&wal_dir(dir.path()), 1);
        let mut second = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&second_path)
            .unwrap();
        second.seek(SeekFrom::End(-1)).unwrap();
        second.write_all(&[encoded.last().unwrap() ^ 0xff]).unwrap();
        second.sync_all().unwrap();

        let reopened = FileWalManager::open(dir.path()).unwrap();
        assert!(reopened.replay_from(0).unwrap().next().is_none());
        assert!(!second_path.exists());
    }

    #[test]
    fn checksum_invalid_durable_final_record_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        let end = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 7,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let path = segment_path(&wal_dir(dir.path()), 0);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN + end - 1))
            .unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x7f;
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN + end - 1))
            .unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();

        let Err(error) = FileWalManager::open(dir.path()) else {
            panic!("WAL with a checksum-invalid durable frame opened successfully");
        };
        assert!(error.message.contains("CRC"));
    }

    #[test]
    fn corrupt_complete_record_before_eof_returns_wal_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();
        let first_lsn = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 1,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        wal.append(WalRecord::insert_for_test(2, 2)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 3,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path(&wal_dir(&path), 0))
            .unwrap();
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN + first_lsn + 5))
            .unwrap();
        file.write_all(&[0x7f]).unwrap();
        file.sync_all().unwrap();

        let Err(err) = FileWalManager::open(&path) else {
            panic!("corrupt WAL record was accepted");
        };
        assert_eq!(err.kind, ErrorKind::Wal);
    }

    #[test]
    fn corrupt_middle_record_length_is_not_treated_as_trailing_partial() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = FileWalManager::open(&path).unwrap();
        let first_lsn = wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.append(WalRecord::insert_for_test(2, 2)).unwrap();
        wal.append(WalRecord::insert_for_test(3, 3)).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path(&wal_dir(&path), 0))
            .unwrap();
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN + first_lsn + 17))
            .unwrap();
        file.write_all(&1000_u32.to_le_bytes()).unwrap();
        file.sync_all().unwrap();

        let Err(err) = FileWalManager::open(&path) else {
            panic!("corrupt middle WAL record was accepted as a trailing partial");
        };
        assert_eq!(err.kind, ErrorKind::Wal);
    }

    #[test]
    fn manager_replays_a_record_stream_across_a_segment_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        let mut end = 0;
        let mut count = 0_u32;
        while end <= SEGMENT_PAYLOAD_BYTES {
            end = wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 7,
                    kind: WalRecordKind::FullPageImage {
                        file_id: 1,
                        page_num: count,
                        image: vec![u8::try_from(count % 251).unwrap(); 8192],
                    },
                })
                .unwrap();
            count += 1;
        }
        wal.flush().unwrap();
        drop(wal);

        let reopened = FileWalManager::open(dir.path()).unwrap();
        let mut replay_count = 0_u32;
        let mut replay_end = 0;
        for record in reopened.replay_from(0).unwrap() {
            replay_count += 1;
            replay_end = record.unwrap().lsn;
        }
        assert_eq!(replay_count, count);
        assert_eq!(replay_end, end);
        assert!(segment_path(&wal_dir(dir.path()), 1).exists());

        let commit_lsn = reopened
            .append(WalRecord {
                lsn: 0,
                txn_id: 7,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
        reopened.flush().unwrap();
        reopened.persist_clog(commit_lsn).unwrap();
        reopened
            .append(WalRecord {
                lsn: 0,
                txn_id: 7,
                kind: WalRecordKind::Checkpoint {
                    redo_lsn: commit_lsn,
                },
            })
            .unwrap();
        reopened.flush().unwrap();
        reopened.recycle_through(commit_lsn).unwrap();
        drop(reopened);

        assert!(!segment_path(&wal_dir(dir.path()), 0).exists());
        let recycled = FileWalManager::open(dir.path()).unwrap();
        assert!(recycled.is_committed(7));
        let retained = recycled
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(retained.len(), 1);
        assert!(matches!(retained[0].kind, WalRecordKind::Checkpoint { .. }));
    }

    #[test]
    fn failed_flush_truncates_a_commit_crossing_a_segment_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path()).unwrap();
        let target = SEGMENT_PAYLOAD_BYTES - 8_000;
        let mut end = 0;
        let mut page_num = 0_u32;
        while end + 8_225 < target {
            end = wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 7,
                    kind: WalRecordKind::FullPageImage {
                        file_id: 1,
                        page_num,
                        image: vec![0; 8192],
                    },
                })
                .unwrap();
            page_num += 1;
        }
        while end < target {
            end = wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 7,
                    kind: WalRecordKind::HeapInit {
                        file_id: 1,
                        page_num,
                    },
                })
                .unwrap();
            page_num += 1;
        }
        assert!(end < SEGMENT_PAYLOAD_BYTES);
        let commit_lsn = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 7,
                kind: WalRecordKind::CommitWithSubxids {
                    subxids: (100_000..102_000).collect(),
                },
            })
            .unwrap();
        assert!(commit_lsn > SEGMENT_PAYLOAD_BYTES);

        wal.fail_next_flush_for_test("simulated cross-segment flush failure");
        assert!(wal.flush().is_err());
        assert!(!wal.is_committed(7));
        drop(wal);

        let reopened = FileWalManager::open(dir.path()).unwrap();
        assert!(!reopened.is_committed(7));
        assert!(reopened.replay_from(0).unwrap().next().is_none());
    }
}
