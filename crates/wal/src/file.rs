use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use common::{DbError, Lsn, Result, TxnId, TxnStatus, TxnStatusView};

use crate::codec::{max_lsn, read_records};
use crate::{Clog, WalManager, WalRecord, WalRecordKind, encode_record};

pub struct FileWalManager {
    path: PathBuf,
    state: Mutex<WalState>,
}

struct WalState {
    file: File,
    records: Vec<StoredRecord>,
    next_lsn: Lsn,
    flushed_lsn: Lsn,
    flushed_offset: u64,
    last_lsn: Lsn,
    last_offset: u64,
    /// Authoritative transaction-status map, rebuilt at open from the durable
    /// `Commit`/`Abort` records and updated as records are flushed (see
    /// [`Clog`]). Supersedes the old single-bit committed set.
    clog: Clog,
    pending_commits: HashSet<u64>,
    poisoned: Option<String>,
    #[cfg(test)]
    fail_next_flush: Option<String>,
    #[cfg(test)]
    fail_next_post_write_seek: Option<String>,
    #[cfg(test)]
    fail_next_parent_sync: Option<String>,
}

#[derive(Clone)]
struct StoredRecord {
    record: WalRecord,
    encoded_len: u64,
}

impl FileWalManager {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|err| {
                DbError::io(format!(
                    "failed to create WAL directory {}: {err}",
                    parent.display()
                ))
            })?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|err| DbError::io(format!("failed to open WAL {}: {err}", path.display())))?;
        if !existed {
            sync_parent_dir(&path)?;
        }

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|err| DbError::io(format!("failed to read WAL {}: {err}", path.display())))?;
        let (decoded, consumed) = read_records(&bytes)?;
        if consumed < bytes.len() {
            file.set_len(consumed as u64).map_err(|err| {
                DbError::io(format!(
                    "failed to truncate incomplete WAL tail {}: {err}",
                    path.display()
                ))
            })?;
            file.sync_all().map_err(|err| {
                DbError::io(format!(
                    "failed to fsync truncated WAL tail {}: {err}",
                    path.display()
                ))
            })?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|err| DbError::io(format!("failed to seek WAL {}: {err}", path.display())))?;

        let records: Vec<_> = decoded
            .into_iter()
            .map(|(record, encoded_len)| StoredRecord {
                record,
                encoded_len,
            })
            .collect();
        let retained: Vec<_> = records.iter().map(|stored| stored.record.clone()).collect();
        let flushed_lsn = max_lsn(&retained);
        let flushed_offset = records.iter().map(|stored| stored.encoded_len).sum();
        let clog = rebuild_clog(&records, flushed_lsn);
        let last_lsn = flushed_lsn;
        let last_offset = flushed_offset;

        Ok(Self {
            path,
            state: Mutex::new(WalState {
                file,
                records,
                next_lsn: last_lsn + 1,
                flushed_lsn,
                flushed_offset,
                last_lsn,
                last_offset,
                clog,
                pending_commits: HashSet::new(),
                poisoned: None,
                #[cfg(test)]
                fail_next_flush: None,
                #[cfg(test)]
                fail_next_post_write_seek: None,
                #[cfg(test)]
                fail_next_parent_sync: None,
            }),
        })
    }
}

impl WalManager for FileWalManager {
    fn append(&self, mut record: WalRecord) -> Result<Lsn> {
        let mut state = self.lock_state()?;
        let assigned_lsn = state.next_lsn;
        record.lsn = assigned_lsn;

        let bytes = encode_record(&record)?;
        let start_offset = state.file.stream_position().map_err(|err| {
            DbError::io(format!(
                "failed to record WAL append offset {}: {err}",
                self.path.display()
            ))
        })?;
        if let Err(err) = state.file.write_all(&bytes) {
            if let Err(rollback_err) = rollback_append(&mut state.file, start_offset, &self.path) {
                state.poisoned = Some(rollback_err.message.clone());
                return Err(rollback_err);
            }
            return Err(DbError::io(format!(
                "failed to append WAL {}: {err}",
                self.path.display()
            )));
        }
        let seek_result = {
            #[cfg(test)]
            {
                if let Some(message) = state.fail_next_post_write_seek.take() {
                    Err(DbError::io(message))
                } else {
                    state.file.seek(SeekFrom::End(0)).map_err(|err| {
                        DbError::io(format!(
                            "failed to seek after WAL append {}: {err}",
                            self.path.display()
                        ))
                    })
                }
            }
            #[cfg(not(test))]
            {
                state.file.seek(SeekFrom::End(0)).map_err(|err| {
                    DbError::io(format!(
                        "failed to seek after WAL append {}: {err}",
                        self.path.display()
                    ))
                })
            }
        };
        if let Err(err) = seek_result {
            if let Err(rollback_err) = rollback_append(&mut state.file, start_offset, &self.path) {
                state.poisoned = Some(rollback_err.message.clone());
                return Err(rollback_err);
            }
            return Err(err);
        }

        match record.kind {
            // A commit only becomes visible in the CLOG once it is durable, so it
            // is staged as pending until `flush` fsyncs it.
            WalRecordKind::Commit => {
                state.pending_commits.insert(record.txn_id);
            }
            // Abort is not fsync-gated: recording it eagerly is safe because a
            // transaction with no durable commit is recovered as aborted anyway.
            WalRecordKind::Abort => {
                state.clog.set_aborted(record.txn_id);
            }
            _ => {}
        }
        state.next_lsn += 1;
        state.last_lsn = assigned_lsn;
        state.last_offset = start_offset + bytes.len() as u64;
        state.records.push(StoredRecord {
            record,
            encoded_len: bytes.len() as u64,
        });

        Ok(assigned_lsn)
    }

    fn flush(&self) -> Result<Lsn> {
        let mut state = self.lock_state()?;
        let sync_result = {
            #[cfg(test)]
            {
                if let Some(message) = state.fail_next_flush.take() {
                    Err(DbError::io(message))
                } else {
                    state.file.sync_all().map_err(|err| {
                        DbError::io(format!(
                            "failed to fsync WAL {}: {err}",
                            self.path.display()
                        ))
                    })
                }
            }
            #[cfg(not(test))]
            {
                state.file.sync_all().map_err(|err| {
                    DbError::io(format!(
                        "failed to fsync WAL {}: {err}",
                        self.path.display()
                    ))
                })
            }
        };
        if let Err(err) = sync_result {
            if let Err(rollback_err) = rollback_unflushed(&mut state, &self.path) {
                state.poisoned = Some(rollback_err.message.clone());
                return Err(rollback_err);
            }
            return Err(err);
        }
        state.flushed_lsn = state.last_lsn;
        state.flushed_offset = state.last_offset;
        let pending = std::mem::take(&mut state.pending_commits);
        for txn_id in pending {
            state.clog.set_committed(txn_id);
        }
        Ok(state.flushed_lsn)
    }

    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>> {
        let state = self.lock_state()?;
        let records: Vec<_> = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn > lsn)
            .map(|stored| Ok(stored.record.clone()))
            .collect();
        Ok(Box::new(records.into_iter()))
    }

    fn truncate_before(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.lock_state()?;

        // CONSERVATIVE TRUNCATION (`docs/specs/mvcc.md` §5.4, §8, Milestone D).
        // The relaxed flush gate (D1) lets an aborted/in-flight transaction's pages
        // reach the heap, and recovery rebuilds the CLOG from the retained WAL. So
        // truncation must never drop a non-committed transaction's records: if it
        // did, its `Abort` record (or absence of a `Commit`) would be gone, the
        // implicit-committed floor would float above it, and its on-disk versions
        // would wrongly read as committed (corruption — VACUUM, Milestone F, is what
        // will reclaim aborted data and re-enable aggressive truncation).
        //
        // Find the oldest transaction with a record below the requested boundary
        // that is NOT durably committed (aborted or in-flight). It "pins"
        // truncation: clamp the effective boundary to its earliest record so that
        // record, and everything after it, is retained. Under Stage-1 serialized
        // writers no write transaction is in-flight during a checkpoint, so in
        // practice this pin is an *aborted* transaction; the active case is covered
        // for safety/Stage-2.
        let pin = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn < lsn)
            .filter(|stored| represents_transaction(&stored.record))
            .filter(|stored| !state.clog.is_committed(stored.record.txn_id))
            .min_by_key(|stored| stored.record.lsn)
            .map(|stored| (stored.record.lsn, stored.record.txn_id));
        let effective_lsn = match pin {
            Some((pin_lsn, _)) => lsn.min(pin_lsn),
            None => lsn,
        };

        let retained: Vec<_> = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn >= effective_lsn)
            .cloned()
            .collect();
        // Every transaction dropped below `effective_lsn` is committed (the pin
        // above retained the oldest non-committed one), so advancing the
        // implicit-committed floor past them keeps their surviving tuples readable
        // as committed even though their `Commit` records are gone. Crucially the
        // floor never crosses the pinned (non-committed) transaction: its records
        // are retained, so it is not among the dropped set, and the max below is the
        // greatest *dropped* (committed) id. A retained `Abort` keeps its explicit
        // status regardless (recorded status wins over the floor).
        let truncated_floor = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn < effective_lsn)
            .filter(|stored| represents_transaction(&stored.record))
            .map(|stored| stored.record.txn_id)
            .max()
            .map(|max_truncated| max_truncated + 1)
            // Defensive clamp: never advance the floor to or past the pinned
            // (non-committed) transaction, even if id/LSN order were ever not
            // strictly aligned (it is under Stage-1 serialized writers).
            .map(|floor| match pin {
                Some((_, pin_txn)) => floor.min(pin_txn),
                None => floor,
            });
        let temp_path = self.path.with_extension("tmp");
        {
            let mut temp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)
                .map_err(|err| {
                    DbError::io(format!(
                        "failed to open temporary WAL {}: {err}",
                        temp_path.display()
                    ))
                })?;

            for stored in &retained {
                temp_file
                    .write_all(&encode_record(&stored.record)?)
                    .map_err(|err| {
                        DbError::io(format!(
                            "failed to write temporary WAL {}: {err}",
                            temp_path.display()
                        ))
                    })?;
            }
            temp_file.sync_all().map_err(|err| {
                DbError::io(format!(
                    "failed to fsync temporary WAL {}: {err}",
                    temp_path.display()
                ))
            })?;
        }

        fs::rename(&temp_path, &self.path).map_err(|err| {
            DbError::io(format!(
                "failed to replace WAL {} with {}: {err}",
                self.path.display(),
                temp_path.display()
            ))
        })?;

        if let Err(err) = sync_parent_dir_after_wal_replace(&self.path, &mut state) {
            state.poisoned = Some(err.message.clone());
            return Err(err);
        }

        let mut file = match OpenOptions::new().read(true).write(true).open(&self.path) {
            Ok(file) => file,
            Err(err) => {
                let message = format!("failed to reopen WAL {}: {err}", self.path.display());
                state.poisoned = Some(message.clone());
                return Err(DbError::io(message));
            }
        };
        if let Err(err) = file.seek(SeekFrom::End(0)) {
            let message = format!("failed to seek WAL {}: {err}", self.path.display());
            state.poisoned = Some(message.clone());
            return Err(DbError::io(message));
        }

        state.file = file;
        state.records = retained;
        state.last_lsn = state
            .records
            .iter()
            .map(|stored| stored.record.lsn)
            .max()
            .unwrap_or(0);
        state.last_offset = state.records.iter().map(|stored| stored.encoded_len).sum();
        state.flushed_offset = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn <= state.flushed_lsn)
            .map(|stored| stored.encoded_len)
            .sum();
        let previous_floor = state.clog.committed_floor();
        state.clog = rebuild_clog(&state.records, state.flushed_lsn);
        // Preserve the monotonic floor across the rebuild, then advance it past the
        // transactions this truncation removed.
        state.clog.set_committed_floor(previous_floor);
        if let Some(floor) = truncated_floor {
            state.clog.set_committed_floor(floor);
        }
        state.pending_commits = pending_commits(&state.records, state.flushed_lsn);

        Ok(())
    }

    fn flushed_lsn(&self) -> Lsn {
        self.state
            .lock()
            .map(|state| state.flushed_lsn)
            .unwrap_or(0)
    }

    fn bytes_after(&self, lsn: Lsn) -> Result<u64> {
        let state = self.lock_state()?;
        Ok(state
            .records
            .iter()
            .filter(|stored| stored.record.lsn > lsn)
            .map(|stored| stored.encoded_len)
            .sum())
    }

    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()> {
        let mut state = self.lock_state()?;
        // The floor must not cross any retained transaction that is not durably
        // committed (aborted or in-flight): such a transaction's versions may be on
        // disk (relaxed flush gate), and flooring past it would mark it implicitly
        // committed (`docs/specs/mvcc.md` §5.4, §8). So the floor is the oldest
        // non-committed retained transaction id, or the allocation boundary if every
        // retained transaction is committed. Conservative truncation already
        // guarantees every transaction dropped below the oldest retained
        // non-committed one was committed, so ids below this floor are all committed.
        let oldest_non_committed = state
            .records
            .iter()
            .filter(|stored| represents_transaction(&stored.record))
            .map(|stored| stored.record.txn_id)
            .filter(|&txn_id| !state.clog.is_committed(txn_id))
            .min();
        let floor = match oldest_non_committed {
            Some(oldest) => allocation_boundary.min(oldest),
            None => allocation_boundary,
        };
        state.clog.set_committed_floor(floor);
        Ok(())
    }
}

impl TxnStatusView for FileWalManager {
    fn status(&self, xid: TxnId) -> TxnStatus {
        // A short lock per probe (acceptable for the B-milestone MVP; the
        // visibility predicate calls this per tuple during scans in B3.6). A
        // poisoned lock degrades to `InProgress` — the conservative "not yet
        // committed" answer, so a tuple is hidden rather than wrongly shown.
        // Contention under heavy concurrent scans is a Milestone E concern (the
        // CLOG may then want a sharded or lock-free read path).
        self.state
            .lock()
            .map(|state| state.clog.status(xid))
            .unwrap_or(TxnStatus::InProgress)
    }
}

impl FileWalManager {
    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, WalState>> {
        let state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("WAL manager lock was poisoned"))?;
        if let Some(message) = &state.poisoned {
            return Err(DbError::wal(
                common::SqlState::InternalError,
                message.clone(),
            ));
        }
        Ok(state)
    }
}

#[cfg(test)]
impl FileWalManager {
    pub(crate) fn fail_next_flush_for_test(&self, message: impl Into<String>) {
        self.state.lock().unwrap().fail_next_flush = Some(message.into());
    }

    pub(crate) fn fail_next_post_write_seek_for_test(&self, message: impl Into<String>) {
        self.state.lock().unwrap().fail_next_post_write_seek = Some(message.into());
    }

    pub(crate) fn fail_next_parent_sync_for_test(&self, message: impl Into<String>) {
        self.state.lock().unwrap().fail_next_parent_sync = Some(message.into());
    }

    pub(crate) fn flushed_lsn_result_for_test(&self) -> Result<Lsn> {
        let state = self.lock_state()?;
        Ok(state.flushed_lsn)
    }
}

/// Whether `record` represents a real transaction whose CLOG outcome the
/// conservative-truncation guard must protect (`docs/specs/mvcc.md` §5.4). True
/// for operation/`Commit`/`Abort` records; FALSE for `txn_id == 0` system metadata
/// and for the `Checkpoint` marker. The marker carries the transaction-id
/// allocation high-water in its `txn_id` field (so `next_txn_id` survives
/// truncation), but that id is an already-settled transaction's, not a transaction
/// that still needs its records retained or the floor held below it — counting it
/// here would (e.g. after two checkpoints with no write between, when the second
/// checkpoint's boundary lands on the first's marker) clamp the floor at the last
/// committed transaction and hide its committed rows.
fn represents_transaction(record: &WalRecord) -> bool {
    record.txn_id != 0 && !matches!(record.kind, WalRecordKind::Checkpoint { .. })
}

/// Rebuild the CLOG from the durable records (`lsn <= flushed_lsn`): each
/// `Commit` marks its txn committed, each `Abort` marks its txn aborted. This is
/// the recovery-time CLOG reconstruction described in `docs/specs/mvcc.md` §8 —
/// the WAL `Commit`/`Abort` records are the durable source of truth; the CLOG
/// itself is in-memory for the A–D MVP (a durable CLOG file is a Milestone F
/// concern). A transaction with neither record is `InProgress` by default.
fn rebuild_clog(records: &[StoredRecord], flushed_lsn: Lsn) -> Clog {
    let mut clog = Clog::new();
    for stored in records
        .iter()
        .filter(|stored| stored.record.lsn <= flushed_lsn)
    {
        match stored.record.kind {
            WalRecordKind::Commit => clog.set_committed(stored.record.txn_id),
            WalRecordKind::Abort => clog.set_aborted(stored.record.txn_id),
            _ => {}
        }
    }
    clog
}

fn pending_commits(records: &[StoredRecord], flushed_lsn: Lsn) -> HashSet<u64> {
    records
        .iter()
        .filter(|stored| stored.record.lsn > flushed_lsn)
        .filter(|stored| matches!(stored.record.kind, WalRecordKind::Commit))
        .map(|stored| stored.record.txn_id)
        .collect()
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| {
                DbError::io(format!(
                    "failed to fsync WAL directory {}: {err}",
                    parent.display()
                ))
            })?;
    }
    Ok(())
}

fn sync_parent_dir_after_wal_replace(path: &Path, _state: &mut WalState) -> Result<()> {
    #[cfg(test)]
    if let Some(message) = _state.fail_next_parent_sync.take() {
        return Err(DbError::io(message));
    }

    sync_parent_dir(path)
}

fn rollback_append(file: &mut File, offset: u64, path: &Path) -> Result<()> {
    file.set_len(offset).map_err(|err| {
        DbError::io(format!(
            "failed to truncate failed WAL append {}: {err}",
            path.display()
        ))
    })?;
    file.seek(SeekFrom::Start(offset)).map_err(|err| {
        DbError::io(format!(
            "failed to seek after failed WAL append rollback {}: {err}",
            path.display()
        ))
    })?;
    file.sync_all().map_err(|err| {
        DbError::io(format!(
            "failed to fsync failed WAL append rollback {}: {err}",
            path.display()
        ))
    })?;
    Ok(())
}

fn rollback_unflushed(state: &mut WalState, path: &Path) -> Result<()> {
    state.file.set_len(state.flushed_offset).map_err(|err| {
        DbError::io(format!(
            "failed to truncate unflushed WAL tail {}: {err}",
            path.display()
        ))
    })?;
    state
        .file
        .seek(SeekFrom::Start(state.flushed_offset))
        .map_err(|err| {
            DbError::io(format!(
                "failed to seek after unflushed WAL rollback {}: {err}",
                path.display()
            ))
        })?;
    state.file.sync_all().map_err(|err| {
        DbError::io(format!(
            "failed to fsync unflushed WAL rollback {}: {err}",
            path.display()
        ))
    })?;
    state
        .records
        .retain(|stored| stored.record.lsn <= state.flushed_lsn);
    state.last_lsn = state.flushed_lsn;
    state.last_offset = state.flushed_offset;
    state.pending_commits.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{WalManager, WalRecord, WalRecordKind};

    use super::FileWalManager;

    #[test]
    fn truncate_before_parent_sync_failure_poisons_wal_before_state_update() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();

        wal.append(WalRecord {
            lsn: 0,
            txn_id: 1,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.flush().unwrap();
        wal.fail_next_parent_sync_for_test("parent sync failed");

        let err = wal.truncate_before(1).unwrap_err();
        assert!(err.message.contains("parent sync failed"));

        let err = wal.flushed_lsn_result_for_test().unwrap_err();
        assert!(err.message.contains("parent sync failed"));
    }
}
