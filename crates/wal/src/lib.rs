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
    /// Effect: [`WalManager::truncate_before`] stops *pinning* an aborted
    /// transaction whose id is `< vacuum_floor` (its on-disk versions are reclaimed,
    /// so dropping its `Abort` record and flooring the implicit-committed boundary
    /// past it cannot resurrect anything — "implicit-committed below floor" is
    /// vacuously correct for it). An in-flight transaction, or an aborted one
    /// `>= vacuum_floor`, still pins.
    ///
    /// **Durable across restart when a CLOG snapshot exists.** The vacuum floor is
    /// persisted in the durable CLOG snapshot (`clog.dat`, written by
    /// [`WalManager::persist_clog`]) and reloaded at open, so a full VACUUM's
    /// reclamation horizon survives restart. When NO snapshot is present (a fresh
    /// database, or a data directory from a pre-durable-CLOG build) the floor falls
    /// back to its conservative initial value. That fallback is SAFE: without a
    /// snapshot the WAL is un-truncated, so truncation is conservative once more (every
    /// aborted txn pins) until the first post-restart full VACUUM — never less safe,
    /// only less aggressive — and recovery rebuilds the CLOG from the surviving WAL.
    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()>;

    /// Establish the CLOG implicit-committed floor at recovery, given the
    /// transaction-id `allocation_boundary` (the next id to be handed out).
    ///
    /// Any unrecorded normal id below the floor reads as committed (its `Commit`
    /// record was truncated by a prior checkpoint while its tuples survive,
    /// `docs/specs/mvcc.md` §5.4). The floor is therefore set CONSERVATIVELY: to
    /// the oldest transaction in the retained WAL whose rebuilt CLOG status is not
    /// `Committed` (aborted or still in-flight), or to `allocation_boundary` if
    /// every retained transaction is committed. Conservative truncation
    /// ([`WalManager::truncate_before`]) guarantees every transaction dropped below
    /// that oldest non-committed one was committed, so flooring just under it never
    /// marks an aborted/in-flight transaction implicitly committed. The floor is
    /// monotonic; this is called once after recovery seeds the allocator.
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

        // Conservative truncation (`docs/specs/mvcc.md` §5.4, §8) only drops a
        // prefix of COMMITTED transactions, so commit each insert's txn here. (Bare
        // uncommitted inserts would now PIN truncation — exercised separately in
        // `conservative_truncation_pins_an_aborted_transaction`.)
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

        // Truncate below the third insert (lsn 5); txns 1 and 2 are committed, so
        // their records (lsn 1-4) are dropped.
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

    #[test]
    fn conservative_truncation_pins_an_aborted_transaction() {
        // The conservative-truncation guard (`docs/specs/mvcc.md` §5.4, §8,
        // Milestone D): a checkpoint must never truncate past an aborted (or
        // in-flight) transaction, even when later transactions committed — its
        // `Abort` record must survive so its on-disk (relaxed-flush) versions stay
        // hidden after restart. Layout: txn 10 committed, txn 11 aborted, txn 12
        // committed; a checkpoint then asks to truncate everything below txn 12's
        // commit. Truncation must clamp at txn 11's first record (the pin).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();

        wal.append(WalRecord::insert_for_test(10, 1)).unwrap(); // lsn 1
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 10,
            kind: WalRecordKind::Commit,
        })
        .unwrap(); // lsn 2
        let pin_lsn = wal.append(WalRecord::insert_for_test(11, 2)).unwrap(); // lsn 3
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 11,
            kind: WalRecordKind::Abort,
        })
        .unwrap(); // lsn 4
        wal.append(WalRecord::insert_for_test(12, 3)).unwrap(); // lsn 5
        let commit_12 = wal
            .append(WalRecord {
                lsn: 0,
                txn_id: 12,
                kind: WalRecordKind::Commit,
            })
            .unwrap(); // lsn 6
        wal.flush().unwrap();

        // Ask to truncate everything below txn 12's commit (lsn 6); the aborted
        // txn 11 (lsn 3) pins truncation, so nothing below lsn 3 is dropped either
        // (its committed predecessor txn 10 stays too — bounded cost).
        wal.truncate_before(commit_12).unwrap();

        let retained: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained.iter().all(|record| record.lsn >= pin_lsn),
            "truncation clamped at the aborted txn's first record"
        );
        // The aborted txn's `Abort` record survives, so its status is reconstructible.
        assert!(
            retained
                .iter()
                .any(|record| record.txn_id == 11 && matches!(record.kind, WalRecordKind::Abort)),
            "the pinned aborted txn's Abort record is retained"
        );

        // After reopen, the aborted txn is still aborted (never implicitly
        // committed), the committed ones are committed, and the floor never crossed
        // the aborted txn.
        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        reopened.establish_recovery_committed_floor(13).unwrap();
        assert!(reopened.is_committed(10));
        assert!(reopened.is_aborted(11));
        assert!(reopened.is_committed(12));
    }

    /// Build the canonical aborted-across-checkpoint layout used by the F4c tests:
    /// txn 10 committed (lsn 1-2), txn 11 ABORTED (insert lsn 3, abort lsn 4), txn 12
    /// committed (lsn 5-6). Returns `(wal, pin_lsn, truncate_at)` where `pin_lsn` (3)
    /// is txn 11's first record and `truncate_at` (6) is txn 12's commit. Without the
    /// F4c relaxation, truncating at `truncate_at` clamps at `pin_lsn` (txn 11 pins).
    fn aborted_across_checkpoint_layout(path: &std::path::Path) -> (FileWalManager, Lsn, Lsn) {
        let wal = FileWalManager::open(path).unwrap();
        wal.append(WalRecord::insert_for_test(10, 1)).unwrap(); // lsn 1
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 10,
            kind: WalRecordKind::Commit,
        })
        .unwrap(); // lsn 2
        let pin_lsn = wal.append(WalRecord::insert_for_test(11, 2)).unwrap(); // lsn 3
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
        (wal, pin_lsn, truncate_at)
    }

    #[test]
    fn vacuum_floor_lets_truncation_drop_a_reclaimed_aborted_txn_no_resurrection() {
        // THE critical F4c test (`docs/specs/mvcc.md` §5.4, §9, Milestone F4c). After a
        // FULL VACUUM pass has reclaimed an aborted txn's on-disk versions, the vacuum
        // floor is advanced past it, and truncation may THEN drop its `Abort` record and
        // float the implicit-committed floor past it — WITHOUT resurrecting anything,
        // because nothing on disk references it. Here txn 11 (aborted) is BELOW the
        // vacuum floor (set to 12), so it no longer pins: truncation drops everything
        // below txn 12's commit, and after reopen no record of txn 11 survives. (Its id
        // reads committed-via-floor, which is vacuously correct: VACUUM reclaimed every
        // tuple it created, so there is nothing to read as a committed ghost.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let (wal, _pin_lsn, truncate_at) = aborted_across_checkpoint_layout(&path);

        // A full VACUUM pass that started at next_txn_id == 13 reclaimed every
        // aborted-creator tuple with id < 13 — including txn 11's — so the floor is 13.
        wal.set_vacuum_floor(13).unwrap();
        wal.truncate_before(truncate_at).unwrap();

        // The aborted txn 11 NO LONGER pins: its records (including its `Abort`) are
        // dropped along with the committed prefix.
        let retained: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained.iter().all(|record| record.lsn >= truncate_at),
            "the vacuumed aborted txn no longer pins truncation"
        );
        assert!(
            !retained
                .iter()
                .any(|record| record.txn_id == 11 && matches!(record.kind, WalRecordKind::Abort)),
            "the reclaimed aborted txn's Abort record is dropped"
        );

        // After reopen + recovery floor: txn 11 is gone from the WAL and below the floor,
        // so nothing reads as a committed ghost — its tuples were reclaimed, so there is
        // no on-disk version to resurrect. (This is the no-resurrection property.)
        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        reopened.establish_recovery_committed_floor(13).unwrap();
        let after_reopen: Vec<_> = reopened
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            !after_reopen
                .iter()
                .any(|record| record.txn_id == 11 && is_redo_operation(&record.kind)),
            "no flushed version of the reclaimed aborted txn survives in the WAL"
        );
        assert!(reopened.is_committed(12));
    }

    #[test]
    fn without_vacuum_floor_an_aborted_txn_still_pins_truncation() {
        // Counter-test (the F4c relaxation is GATED, not blanket): the SAME layout with
        // NO vacuum floor advanced (its tuples were never reclaimed) keeps the aborted
        // txn 11 pinning — its `Abort` is retained and the floor never floats past it,
        // so after restart it stays aborted (invisible). This proves the relaxation
        // fires only for aborted txns BELOW the vacuum floor.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let (wal, pin_lsn, truncate_at) = aborted_across_checkpoint_layout(&path);

        // No `set_vacuum_floor`: the floor stays at its conservative initial value, so
        // txn 11 is NOT below it and still pins.
        wal.truncate_before(truncate_at).unwrap();

        let retained: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained.iter().all(|record| record.lsn >= pin_lsn),
            "without a vacuum floor the aborted txn still pins truncation"
        );
        assert!(
            retained
                .iter()
                .any(|record| record.txn_id == 11 && matches!(record.kind, WalRecordKind::Abort)),
            "the un-vacuumed aborted txn's Abort record is retained"
        );

        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        reopened.establish_recovery_committed_floor(13).unwrap();
        assert!(
            reopened.is_aborted(11),
            "the pinned aborted txn stays aborted"
        );
        assert!(reopened.is_committed(12));
    }

    #[test]
    fn vacuum_floor_only_relaxes_aborts_below_it() {
        // The relaxation is bounded by the floor: an aborted txn AT/ABOVE the vacuum
        // floor still pins (its tuples may not be reclaimed). Layout has txn 11 aborted;
        // a floor of 11 does NOT cover id 11 (the boundary is exclusive: `id < floor`),
        // so txn 11 still pins. A floor of 12 covers it and it stops pinning.
        let dir = tempfile::tempdir().unwrap();

        // Floor == 11: does not cover id 11 ⇒ still pins.
        let path_a = dir.path().join("wal_a.dat");
        let (wal_a, pin_lsn, truncate_at) = aborted_across_checkpoint_layout(&path_a);
        wal_a.set_vacuum_floor(11).unwrap();
        wal_a.truncate_before(truncate_at).unwrap();
        let retained_a: Vec<_> = wal_a
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained_a.iter().all(|record| record.lsn >= pin_lsn),
            "an aborted txn at the floor (id == floor) still pins"
        );

        // Floor == 12: covers id 11 ⇒ stops pinning, truncation reaches the boundary.
        let path_b = dir.path().join("wal_b.dat");
        let (wal_b, _pin_lsn, truncate_at_b) = aborted_across_checkpoint_layout(&path_b);
        wal_b.set_vacuum_floor(12).unwrap();
        wal_b.truncate_before(truncate_at_b).unwrap();
        let retained_b: Vec<_> = wal_b
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained_b.iter().all(|record| record.lsn >= truncate_at_b),
            "an aborted txn below the floor (id < floor) stops pinning"
        );
    }

    #[test]
    fn truncation_shrinks_the_wal_further_after_vacuum() {
        // The relaxation has EFFECT: after the vacuum floor advances past a previously-
        // pinning aborted txn, `truncate_before` retains strictly fewer bytes than the
        // pinned case. Same layout in two WALs; one vacuumed, one not.
        let dir = tempfile::tempdir().unwrap();

        let pinned_path = dir.path().join("pinned.dat");
        let (pinned, _pin, truncate_at) = aborted_across_checkpoint_layout(&pinned_path);
        pinned.truncate_before(truncate_at).unwrap();
        let pinned_bytes = pinned.bytes_after(0).unwrap();

        let vacuumed_path = dir.path().join("vacuumed.dat");
        let (vacuumed, _pin2, truncate_at2) = aborted_across_checkpoint_layout(&vacuumed_path);
        vacuumed.set_vacuum_floor(13).unwrap();
        vacuumed.truncate_before(truncate_at2).unwrap();
        let vacuumed_bytes = vacuumed.bytes_after(0).unwrap();

        assert!(
            vacuumed_bytes < pinned_bytes,
            "vacuumed WAL ({vacuumed_bytes} bytes) must be smaller than pinned ({pinned_bytes} bytes)"
        );
    }

    #[test]
    fn vacuum_floor_resets_on_reopen_without_a_clog_snapshot() {
        // No-snapshot fallback safety (`docs/specs/mvcc.md` §5.4, F4c): with NO durable
        // CLOG snapshot (`persist_clog` is never called here, so no `clog.dat` exists)
        // the vacuum floor falls back to its conservative initial value at reopen, and
        // truncation is conservative again until the first post-restart full VACUUM.
        // Here the floor is advanced, then the WAL is reopened (NOT truncated first),
        // and an aborted txn that WOULD have been relaxed now pins again — provably
        // correct. (When a snapshot IS present the floor is instead loaded from it; see
        // `file::tests::reopen_loads_vacuum_floor_from_snapshot`.)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let (wal, pin_lsn, truncate_at) = aborted_across_checkpoint_layout(&path);
        wal.set_vacuum_floor(13).unwrap();
        drop(wal);

        // Reopen: with no snapshot the floor falls back to its conservative initial value.
        let reopened = FileWalManager::open(&path).unwrap();
        reopened.truncate_before(truncate_at).unwrap();
        let retained: Vec<_> = reopened
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            retained.iter().all(|record| record.lsn >= pin_lsn),
            "after reopen the vacuum floor is reset, so the aborted txn pins again"
        );
        assert!(
            retained
                .iter()
                .any(|record| record.txn_id == 11 && matches!(record.kind, WalRecordKind::Abort)),
            "the aborted txn's Abort record is retained again post-reopen"
        );
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
