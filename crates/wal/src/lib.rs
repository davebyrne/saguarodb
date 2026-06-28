mod clog;
mod clog_file;
mod codec;
mod file;
mod record;

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
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    fn flush(&self) -> Result<Lsn>;
    /// Replay every retained record after `lsn`, in LSN order. Redo-all recovery
    /// (`docs/specs/mvcc.md` §8) iterates this and applies the page-mutation
    /// records ([`is_redo_operation`]); the CLOG (rebuilt at open) decides
    /// visibility afterwards.
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;
    fn flushed_lsn(&self) -> Lsn;
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;

    /// Persist a durable CLOG snapshot (`clog.dat`) covering WAL records through
    /// `clog_lsn` (`docs/specs/mvcc.md` §5.4). The checkpoint calls this after the
    /// heap and control record are durable and **before** truncating the WAL, so
    /// the snapshot remembers every transaction's outcome that truncation is about
    /// to drop. It prunes the in-memory CLOG to its live window, then writes the
    /// envelope atomically (temp file + rename + directory fsync). Recovery loads
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
    /// which bounds how long the durable CLOG must remember it. WAL truncation does not
    /// consult the vacuum floor (it is unconditional); an aborted transaction
    /// `>= vacuum_floor` keeps its explicit `Aborted` entry in the snapshot.
    ///
    /// **Durable across restart when a CLOG snapshot exists.** The vacuum floor is
    /// persisted in the durable CLOG snapshot (`clog.dat`, written by
    /// [`WalManager::persist_clog`]) and reloaded at open, so a full VACUUM's
    /// reclamation horizon survives restart. When NO snapshot is present (a fresh
    /// database, or a data directory from a pre-durable-CLOG build) the floor falls
    /// back to its conservative initial value — safe, since the snapshot simply retains
    /// more aborted entries until the first post-restart full VACUUM.
    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()>;

    /// Establish the CLOG implicit-committed floor at recovery, given the
    /// transaction-id `allocation_boundary` (the next id to be handed out).
    ///
    /// **No-op when the CLOG was seeded from a durable `clog.dat` snapshot** — that
    /// snapshot's `committed_floor` is authoritative and durable. This is the
    /// no-snapshot fallback only (fresh database, or a pre-durable-CLOG data directory
    /// whose WAL was conservatively truncated): an unrecorded normal id below the floor
    /// reads as committed (`docs/specs/mvcc.md` §5.4), so the floor is set
    /// CONSERVATIVELY to the oldest transaction in the retained WAL whose rebuilt CLOG
    /// status is not `Committed` (aborted or in-flight), or to `allocation_boundary` if
    /// every retained transaction is committed — never marking an aborted/in-flight
    /// transaction implicitly committed. The floor is monotonic; this is called once
    /// after recovery seeds the allocator.
    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use common::{ErrorKind, Lsn, Result, TxnStatusView};

    use super::{
        FileWalManager, WalManager, WalRecord, WalRecordKind, decode_record, encode_record,
        is_redo_operation,
    };

    #[test]
    fn encode_decode_round_trip_preserves_record() {
        let record = WalRecord {
            lsn: 3,
            txn_id: 9,
            kind: WalRecordKind::DropTable { table: 7 },
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

    #[test]
    fn recovery_rebuilds_clog_and_replays_all_records() {
        // Redo-all (`docs/specs/mvcc.md` §8, Milestone D2): `replay_from` yields
        // EVERY record; the rebuilt CLOG — not a replay filter — distinguishes
        // committed from uncommitted. (Before D2 this used the redo-committed-only
        // `replay_committed_from`, which is retired with the relaxed flush gate.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
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
        assert_eq!(recovered.flushed_lsn(), 3);
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
        assert_eq!(
            recovered.append(WalRecord::insert_for_test(12, 3)).unwrap(),
            4
        );
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
        let path = dir.path().join("wal.dat");
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
        let path = dir.path().join("wal.dat");
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
        let path = dir.path().join("wal.dat");
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
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();

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
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();

        assert_eq!(wal.append(WalRecord::insert_for_test(1, 10)).unwrap(), 1);
        assert_eq!(wal.append(WalRecord::insert_for_test(2, 20)).unwrap(), 2);

        let records: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![1, 2]
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
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();

        wal.append(WalRecord::insert_for_test(12, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 12,
            kind: WalRecordKind::Commit,
        })
        .unwrap();

        assert_eq!(wal.flushed_lsn(), 0);
        assert!(!wal.is_committed(12));
        assert_eq!(wal.flush().unwrap(), 2);
        assert_eq!(wal.flushed_lsn(), 2);
        assert!(wal.is_committed(12));
    }

    #[test]
    fn failed_flush_rolls_back_pending_commit_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
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
        assert!(wal.replay_from(0).unwrap().next().is_none());

        assert_eq!(wal.append(WalRecord::insert_for_test(13, 2)).unwrap(), 3);
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 13,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.flush().unwrap();

        drop(wal);
        let recovered = FileWalManager::open(&path).unwrap();
        // txn 12's commit was rolled back with the failed flush, so its bytes are
        // gone; only txn 13's committed insert survives in the WAL.
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
    fn failed_append_after_writing_commit_rolls_back_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
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

        assert_eq!(
            recovered.append(WalRecord::insert_for_test(1, 1)).unwrap(),
            1
        );
    }

    #[test]
    fn truncate_before_retains_boundary_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();

        // `truncate_before` retains `record.lsn >= boundary` and drops the rest
        // unconditionally (`docs/specs/mvcc.md` §5.4, §8).
        wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 1,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(2, 2)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 2,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(3, 3)).unwrap();
        wal.flush().unwrap();

        // Truncate below the third insert (lsn 5); records lsn 1-4 are dropped.
        wal.truncate_before(5).unwrap();

        let records: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![5]
        );

        let expected_bytes: u64 = records
            .iter()
            .map(|record| encode_record(record).unwrap().len() as u64)
            .sum();
        assert_eq!(wal.bytes_after(0).unwrap(), expected_bytes);

        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        let replay_after_checkpoint: Vec<_> = reopened
            .replay_from(5)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(replay_after_checkpoint.is_empty());
    }

    /// Build the canonical aborted-across-checkpoint layout: txn 10 committed
    /// (lsn 1-2), txn 11 ABORTED (insert lsn 3, abort lsn 4), txn 12 committed
    /// (lsn 5-6). Returns `(wal, abort_lsn, truncate_at)` where `abort_lsn` (3) is
    /// txn 11's first record and `truncate_at` (6) is txn 12's commit.
    fn aborted_across_checkpoint_layout(path: &std::path::Path) -> (FileWalManager, Lsn, Lsn) {
        let wal = FileWalManager::open(path).unwrap();
        wal.append(WalRecord::insert_for_test(10, 1)).unwrap(); // lsn 1
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 10,
            kind: WalRecordKind::Commit,
        })
        .unwrap(); // lsn 2
        let abort_lsn = wal.append(WalRecord::insert_for_test(11, 2)).unwrap(); // lsn 3
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 11,
            kind: WalRecordKind::Abort,
        })
        .unwrap(); // lsn 4
        wal.append(WalRecord::insert_for_test(12, 3)).unwrap(); // lsn 5
        let truncate_at = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 12,
                kind: WalRecordKind::Commit,
            })
            .unwrap(); // lsn 6
        wal.flush().unwrap();
        (wal, abort_lsn, truncate_at)
    }

    #[test]
    fn decoupled_truncation_drops_abort_record_but_clog_snapshot_keeps_it_aborted() {
        // The keystone of the decoupling (`docs/specs/mvcc.md` §5.4, §8): with the
        // durable CLOG snapshot, the checkpoint persists outcomes to `clog.dat` and then
        // truncates the WAL UNCONDITIONALLY — an un-vacuumed aborted transaction's
        // `Abort` record is dropped, yet it stays aborted (invisible) after restart
        // because the snapshot remembers it. (Pre-decoupling this txn would have pinned
        // truncation to keep its `Abort`.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let (wal, abort_lsn, truncate_at) = aborted_across_checkpoint_layout(&path);

        // Checkpoint order: persist the snapshot (records txn 11 aborted) BEFORE
        // truncating. No VACUUM ran, so the vacuum floor stays at its default and txn
        // 11's explicit `Aborted` entry is kept in the snapshot.
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        wal.truncate_before(truncate_at).unwrap();

        // Truncation is unconditional: txn 11's `Abort` (lsn 4) and insert (lsn 3) are
        // gone from the WAL even though it was never vacuumed.
        let retained: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained.iter().all(|record| record.lsn >= truncate_at),
            "truncation is unconditional — nothing below the boundary is retained"
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
        // no-op when a snapshot is loaded: the truncated WAL no longer retains the abort,
        // so re-deriving the floor from it would float past txn 11, and the NEXT
        // checkpoint's snapshot would then drop txn 11's explicit `Aborted` entry — its
        // surviving tuples would read as committed (corruption). This exercises two full
        // checkpoint+recovery cycles and asserts txn 11 stays aborted throughout.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let (wal, _abort_lsn, truncate_at) = aborted_across_checkpoint_layout(&path);
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        wal.truncate_before(truncate_at).unwrap();
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
        // The vacuum floor now bounds the durable CLOG, not WAL truncation: once a full
        // VACUUM reclaims txn 11's tuples (floor advanced past 11), `persist_clog` drops
        // its explicit `Aborted` entry and it reads implicit-committed (vacuously correct
        // — its tuples are gone). Contrast with the un-vacuumed case above, where the
        // entry is kept.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let (wal, _abort_lsn, truncate_at) = aborted_across_checkpoint_layout(&path);

        // A full VACUUM starting at next_txn_id == 13 reclaimed every aborted-creator
        // tuple < 13, including txn 11's.
        wal.set_vacuum_floor(13).unwrap();
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        wal.truncate_before(truncate_at).unwrap();
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
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();

        wal.append(WalRecord::insert_for_test(10, 1)).unwrap();
        let checkpoint_lsn = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 10,
                kind: WalRecordKind::Commit,
            })
            .unwrap();
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
        // txn 10 is committed, so its insert (below checkpoint_lsn) is truncatable.
        wal.truncate_before(checkpoint_lsn).unwrap();

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
        assert_eq!(operations[0].lsn, checkpoint_lsn + 2);
    }

    #[test]
    fn bytes_after_counts_encoded_records_after_exclusive_lsn() {
        let r1 = WalRecord::insert_for_test(1, 1);
        let r2 = WalRecord::insert_for_test(2, 2);
        let r3 = WalRecord::insert_for_test(3, 3);
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();

        wal.append(r1.clone()).unwrap();
        wal.append(r2.clone()).unwrap();
        wal.append(r3.clone()).unwrap();

        let encoded_second = encode_record(&WalRecord { lsn: 2, ..r2 }).unwrap().len() as u64;
        let encoded_third = encode_record(&WalRecord { lsn: 3, ..r3 }).unwrap().len() as u64;

        assert_eq!(wal.bytes_after(1).unwrap(), encoded_second + encoded_third);
    }

    #[test]
    fn incomplete_trailing_record_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();
        wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.flush().unwrap();

        let partial = &encode_record(&WalRecord::insert_for_test(2, 2)).unwrap()[..10];
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(partial)
            .unwrap();

        let reopened = FileWalManager::open(&path).unwrap();
        let records: Vec<_> = reopened
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, 1);
    }

    #[test]
    fn corrupt_complete_record_before_eof_returns_wal_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let first = encode_record(&WalRecord {
            lsn: 1,
            txn_id: 1,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        let mut corrupt = encode_record(&WalRecord::insert_for_test(2, 2)).unwrap();
        corrupt[5] ^= 0x7f;
        let third = encode_record(&WalRecord {
            lsn: 3,
            txn_id: 3,
            kind: WalRecordKind::Commit,
        })
        .unwrap();

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(&first).unwrap();
        file.write_all(&corrupt).unwrap();
        file.write_all(&third).unwrap();
        file.sync_all().unwrap();

        let Err(err) = FileWalManager::open(&path) else {
            panic!("corrupt WAL record was accepted");
        };
        assert_eq!(err.kind, ErrorKind::Wal);
    }

    #[test]
    fn corrupt_middle_record_length_is_not_treated_as_trailing_partial() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let first = encode_record(&WalRecord::insert_for_test(1, 1)).unwrap();
        let mut corrupt = encode_record(&WalRecord::insert_for_test(2, 2)).unwrap();
        let third = encode_record(&WalRecord::insert_for_test(3, 3)).unwrap();

        corrupt[17..21].copy_from_slice(&u32::MAX.to_le_bytes());

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(&first).unwrap();
        file.write_all(&corrupt).unwrap();
        file.write_all(&third).unwrap();
        file.sync_all().unwrap();

        let Err(err) = FileWalManager::open(&path) else {
            panic!("corrupt middle WAL record was accepted as a trailing partial");
        };
        assert_eq!(err.kind, ErrorKind::Wal);
    }
}
