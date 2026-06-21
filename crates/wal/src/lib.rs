mod clog;
mod codec;
mod file;
mod record;

pub use clog::Clog;
pub use codec::{decode_record, encode_record};
pub use file::FileWalManager;
pub use record::{WalRecord, WalRecordKind};

use common::{Lsn, Result, TxnStatusView};

/// A WAL manager is also a [`TxnStatusView`] (backed by its in-memory CLOG): the
/// supertrait lets the storage engine — which already holds an
/// `Arc<dyn WalManager>` — upcast to `&dyn TxnStatusView` for the visibility
/// predicate (`docs/specs/mvcc.md` §6) with no extra handle. Implementors satisfy
/// it by exposing their CLOG status (`Clog: TxnStatusView` does the work).
pub trait WalManager: Send + Sync + TxnStatusView {
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    fn flush(&self) -> Result<Lsn>;
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    fn replay_committed_from(
        &self,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;
    fn flushed_lsn(&self) -> Lsn;
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use common::{ErrorKind, Result, TxnStatusView};

    use super::{
        FileWalManager, WalManager, WalRecord, WalRecordKind, decode_record, encode_record,
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
    fn recovery_discovers_committed_transactions_and_ignores_uncommitted_records() {
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
        assert!(recovered.is_committed(10));
        assert!(!recovered.is_committed(11));

        let committed: Vec<_> = recovered
            .replay_committed_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].txn_id, 10);
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
        assert!(!recovered.is_committed(11));
        assert!(!recovered.is_committed(12));

        // Redo replays only the committed txn's operation record; the aborted
        // txn's records (and the Commit/Abort markers themselves) are excluded.
        let committed: Vec<_> = recovered
            .replay_committed_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].txn_id, 10);
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
        assert!(!recovered.is_committed(12));
        assert!(recovered.is_committed(13));
        let committed: Vec<_> = recovered
            .replay_committed_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].txn_id, 13);
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

        wal.append(WalRecord::insert_for_test(1, 1)).unwrap();
        wal.append(WalRecord::insert_for_test(2, 2)).unwrap();
        wal.append(WalRecord::insert_for_test(3, 3)).unwrap();
        wal.flush().unwrap();

        wal.truncate_before(2).unwrap();

        let records: Vec<_> = wal
            .replay_from(0)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            records.iter().map(|record| record.lsn).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let expected_bytes: u64 = records
            .iter()
            .map(|record| encode_record(record).unwrap().len() as u64)
            .sum();
        assert_eq!(wal.bytes_after(0).unwrap(), expected_bytes);

        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        let replay_after_checkpoint: Vec<_> = reopened
            .replay_from(2)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            replay_after_checkpoint
                .iter()
                .map(|record| record.lsn)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn committed_replay_after_checkpoint_excludes_boundary_and_metadata() {
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
        wal.truncate_before(checkpoint_lsn).unwrap();

        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        let committed: Vec<_> = reopened
            .replay_committed_from(checkpoint_lsn)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].txn_id, 20);
        assert_eq!(committed[0].lsn, checkpoint_lsn + 2);
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
