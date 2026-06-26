use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use common::{DbError, FIRST_NORMAL_XID, Lsn, Result, TxnId, TxnStatus, TxnStatusView};

use crate::clog_file::{ClogSnapshot, decode_clog, encode_clog};
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
    /// Authoritative transaction-status map, reconstructed at open — seeded from the
    /// durable `clog.dat` snapshot when present (then a post-snapshot `Commit`/`Abort`
    /// fold), else rebuilt from those records — and updated as records are flushed
    /// (see [`Clog`]). Supersedes the old single-bit committed set.
    clog: Clog,
    pending_commits: HashSet<u64>,
    /// The vacuum floor (`docs/specs/mvcc.md` §5.4, §9, Milestone F4c): the boundary
    /// below which a FULL VACUUM pass has reclaimed every aborted-creator tuple, so
    /// `truncate_before` may drop those aborted transactions' `Abort` records and
    /// float the implicit-committed floor past them. Loaded from the durable CLOG
    /// snapshot (`clog.dat`) at open when one is present — so it survives restart —
    /// else seeded to `FIRST_NORMAL_XID` (no aborted txn is below it ⇒ the pin is
    /// fully conservative). See [`WalManager::set_vacuum_floor`] for why the
    /// conservative no-snapshot fallback is safe.
    vacuum_floor: TxnId,
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
        // Prefer the durable CLOG snapshot (`docs/specs/mvcc.md` §5.4): seed the
        // statuses + floors from `clog.dat` and fold only the post-snapshot
        // `Commit`/`Abort` records, bounding the rebuild scan and carrying the
        // vacuum floor across restart. An ABSENT snapshot (fresh database, or a data
        // directory from a pre-durable-CLOG build) falls back to rebuilding the CLOG
        // from the full retained WAL; a CORRUPT snapshot propagates its error
        // (atomic temp+rename means a torn write never occurs, so a CRC/version
        // mismatch is real corruption, surfaced like a bad `manifest.dat`).
        let clog_path = clog_path_for(&path);
        let (clog, vacuum_floor) = match load_clog_snapshot(&clog_path)? {
            Some(snapshot) => {
                let mut clog = Clog::from_snapshot(&snapshot);
                fold_commit_abort_after(&mut clog, &records, snapshot.clog_lsn, flushed_lsn);
                (clog, snapshot.vacuum_floor)
            }
            None => (rebuild_clog(&records, flushed_lsn), FIRST_NORMAL_XID),
        };
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
                // Loaded from the durable CLOG snapshot when present (so a full
                // VACUUM's reclamation horizon survives restart), else the
                // fully-conservative boundary (`docs/specs/mvcc.md` §5.4).
                vacuum_floor,
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

        // CONSERVATIVE TRUNCATION (`docs/specs/mvcc.md` §5.4, §8, Milestone D) with
        // the Milestone-F4c relaxation for vacuumed aborts (§9).
        // The relaxed flush gate (D1) lets an aborted/in-flight transaction's pages
        // reach the heap, and recovery rebuilds the CLOG from the retained WAL. So
        // truncation must never drop a non-committed transaction's records UNLESS its
        // on-disk versions are provably gone: if it did, its `Abort` record (or absence
        // of a `Commit`) would be gone, the implicit-committed floor would float above
        // it, and its on-disk versions would wrongly read as committed (corruption).
        //
        // Find the oldest transaction with a record below the requested boundary that
        // "pins" truncation: a NON-committed transaction whose on-disk versions are NOT
        // provably reclaimed. The clamp retains its earliest record and everything
        // after it. Under Stage-1 serialized writers — and under Stage-2's exclusive
        // checkpoint/VACUUM guard — no write transaction is in-flight during a
        // checkpoint, so in practice a pin is an *aborted* transaction; the in-flight
        // case is covered for safety.
        //
        // F4c relaxation: an aborted transaction whose id is `< vacuum_floor` does NOT
        // pin. A FULL VACUUM pass reclaimed every aborted-creator tuple with creator id
        // below that boundary (heap + index), so dropping its `Abort` and flooring the
        // implicit-committed boundary past it cannot resurrect anything ("implicit-
        // committed below floor" is vacuously correct — there is no surviving version
        // to read). This relaxation is gated STRICTLY on a CLOG-recorded `Aborted`
        // status (`is_aborted`): an in-flight / not-yet-settled id below `vacuum_floor`
        // (which should not exist under the exclusive guard, but we are defensive) is
        // NOT provably reclaimed and therefore still pins. The reclamation is durable
        // before this runs — the checkpoint flushes+fsyncs all dirty pages
        // (`flush_dirty_pages` + `store.sync_all`) before `truncate_before` consults the
        // floor — so no `Abort` is ever dropped while its tuples remain on disk.
        let vacuum_floor = state.vacuum_floor;
        let pin = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn < lsn)
            .filter(|stored| represents_transaction(&stored.record))
            .filter(|stored| {
                let txn_id = stored.record.txn_id;
                // A transaction pins truncation when it is not durably committed AND
                // its on-disk versions are not provably reclaimed. The F4c relaxation
                // is the second clause: a CLOG-recorded `Aborted` txn below the vacuum
                // floor has no surviving version, so it stops pinning.
                let reclaimed_abort = state.clog.is_aborted(txn_id) && txn_id < vacuum_floor;
                !state.clog.is_committed(txn_id) && !reclaimed_abort
            })
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
        // Every transaction dropped below `effective_lsn` is either committed or a
        // VACUUM-reclaimed abort below `vacuum_floor` (the pin above retained the
        // oldest non-committed, not-yet-reclaimed one). Advancing the
        // implicit-committed floor past them is safe: a committed txn's surviving
        // tuples stay readable as committed even though its `Commit` is gone, and a
        // reclaimed-aborted txn has NO surviving tuple, so reading it
        // implicitly-committed is vacuous (nothing references it). Crucially the
        // floor never crosses a still-pinned (non-committed, un-reclaimed)
        // transaction: its records are retained, so it is not among the dropped set,
        // and the max below is the greatest *dropped* id. A retained `Abort` keeps
        // its explicit status regardless (recorded status wins over the floor).
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

    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()> {
        // Monotonic; runtime-resident but persisted to `clog.dat` (see the trait doc):
        // a full VACUUM pass under the exclusive guard reclaimed every aborted-creator
        // tuple below `boundary`, so `truncate_before` may now drop those aborted
        // transactions' `Abort` records and float the implicit-committed floor past
        // them. Never lowered.
        let mut state = self.lock_state()?;
        state.vacuum_floor = state.vacuum_floor.max(boundary);
        Ok(())
    }

    fn persist_clog(&self, clog_lsn: Lsn) -> Result<()> {
        // Serialize the live-window snapshot (statuses + both floors) atomically, then
        // prune the in-memory CLOG to match. Write-then-mutate: a failed durable write
        // leaves the in-memory floor exactly where it was, so the next open still
        // reconciles against the previous snapshot. The checkpoint calls this after
        // the heap + control record are durable and before `truncate_before`, so the
        // snapshot durably remembers every outcome truncation is about to drop
        // (`docs/specs/mvcc.md` §5.4).
        //
        // The CLOG only records commits once they are flushed (`set_committed` runs in
        // `flush`), so the snapshot can only attest to outcomes through `flushed_lsn`.
        // Clamp `clog_lsn` to it: stamping a higher value would, on the next open,
        // skip folding a `Commit` in `(flushed_lsn, clog_lsn]` (the fold replays only
        // `lsn > clog_lsn`) and resurrect that durable transaction as in-progress.
        // Clamping down is always safe — the fold is idempotent over the re-replayed
        // range. The checkpoint passes `flushed_lsn`, so this is a guard, not a change.
        let mut state = self.lock_state()?;
        let clog_lsn = clog_lsn.min(state.flushed_lsn);
        let vacuum_floor = state.vacuum_floor;
        let snapshot = state.clog.live_snapshot(clog_lsn, vacuum_floor);
        write_clog_file(&self.clog_path(), &snapshot)?;
        state.clog.prune_to(snapshot.committed_floor);
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
    /// Path of the durable CLOG snapshot, a sibling of the WAL file
    /// (`<data-dir>/clog.dat` next to `<data-dir>/wal.dat`).
    fn clog_path(&self) -> PathBuf {
        clog_path_for(&self.path)
    }

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
/// the recovery-time CLOG reconstruction described in `docs/specs/mvcc.md` §8, used
/// as the **no-snapshot fallback** — the WAL `Commit`/`Abort` records are the durable
/// source of truth. When a durable CLOG snapshot (`clog.dat`) is present, `open`
/// instead seeds from it and folds only the post-snapshot records (see
/// [`load_clog_snapshot`]). A transaction with neither record is `InProgress` by
/// default.
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

/// The durable CLOG snapshot path for a WAL at `wal_path` (its `clog.dat` sibling).
fn clog_path_for(wal_path: &Path) -> PathBuf {
    wal_path.with_file_name("clog.dat")
}

/// Load the durable CLOG snapshot, or `None` when none exists yet. An absent file
/// is the fresh-database / pre-durable-CLOG-build case (the caller rebuilds from
/// the WAL); a present-but-corrupt file propagates its error, exactly like a bad
/// `manifest.dat` (atomic temp+rename means a torn write never occurs).
fn load_clog_snapshot(path: &Path) -> Result<Option<ClogSnapshot>> {
    match fs::read(path) {
        Ok(bytes) => decode_clog(&bytes).map(Some),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(DbError::io(format!(
            "failed to read CLOG file {}: {err}",
            path.display()
        ))),
    }
}

/// Fold the durable `Commit`/`Abort` records strictly after the snapshot's
/// `clog_lsn` (and at or below `flushed_lsn`) onto a CLOG seeded from that
/// snapshot, bringing it current with the WAL (`docs/specs/mvcc.md` §5.4).
fn fold_commit_abort_after(
    clog: &mut Clog,
    records: &[StoredRecord],
    clog_lsn: Lsn,
    flushed_lsn: Lsn,
) {
    for stored in records
        .iter()
        .filter(|stored| stored.record.lsn > clog_lsn && stored.record.lsn <= flushed_lsn)
    {
        match stored.record.kind {
            WalRecordKind::Commit => clog.set_committed(stored.record.txn_id),
            WalRecordKind::Abort => clog.set_aborted(stored.record.txn_id),
            _ => {}
        }
    }
}

/// Write the durable CLOG snapshot atomically: temp file + fsync + rename + parent
/// directory fsync (mirrors the control-record store in `crates/control`).
fn write_clog_file(path: &Path, snapshot: &ClogSnapshot) -> Result<()> {
    let bytes = encode_clog(snapshot)?;
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|err| {
                DbError::io(format!(
                    "failed to open temporary CLOG file {}: {err}",
                    tmp_path.display()
                ))
            })?;
        file.write_all(&bytes).map_err(|err| {
            DbError::io(format!(
                "failed to write temporary CLOG file {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            DbError::io(format!(
                "failed to fsync temporary CLOG file {}: {err}",
                tmp_path.display()
            ))
        })?;
    }
    fs::rename(&tmp_path, path).map_err(|err| {
        DbError::io(format!(
            "failed to replace CLOG file {} with {}: {err}",
            path.display(),
            tmp_path.display()
        ))
    })?;
    sync_parent_dir(path)
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
    use common::TxnStatusView;

    use crate::clog_file::decode_clog;
    use crate::{WalManager, WalRecord, WalRecordKind};

    use super::{FileWalManager, clog_path_for};

    /// Append committed txn 10, aborted txn 11, committed txn 12, and flush.
    fn commit_abort_commit(wal: &FileWalManager) {
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
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 12,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.flush().unwrap();
    }

    #[test]
    fn persist_clog_then_reopen_restores_statuses_from_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();
        commit_abort_commit(&wal);

        // No vacuum yet, so the aborted txn 11 stays explicit in the snapshot.
        let clog_lsn = wal.flushed_lsn();
        wal.persist_clog(clog_lsn).unwrap();

        // The on-disk snapshot records the outcomes and the absorbed LSN directly.
        // The aborted txn 11 pins the floor at 11, so the committed txn 10 below it is
        // implicit-committed (dropped) while txn 12 above it stays explicit.
        let bytes = std::fs::read(clog_path_for(&path)).unwrap();
        let snapshot = decode_clog(&bytes).unwrap();
        assert_eq!(snapshot.clog_lsn, clog_lsn);
        assert_eq!(snapshot.committed_floor, 11);
        assert_eq!(snapshot.committed, vec![12]);
        assert_eq!(snapshot.aborted, vec![11]);

        // Reopen: the CLOG is seeded from the snapshot, so the statuses survive — txn
        // 10 reads implicit-committed below the floor, 11 explicit-aborted, 12 committed.
        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        assert!(reopened.is_committed(10));
        assert!(reopened.is_aborted(11));
        assert!(reopened.is_committed(12));
    }

    #[test]
    fn reopen_folds_commit_abort_records_after_the_snapshot_lsn() {
        // The heart of the feature: seed from the snapshot, then replay only the
        // post-`clog_lsn` `Commit`/`Abort` records. Persist after txn 10 commits, then
        // commit txn 20 and abort txn 21 (both beyond `clog_lsn`), reopen, and check
        // the snapshot status (10) AND the folded statuses (20, 21) are all present.
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
        wal.flush().unwrap();
        let clog_lsn = wal.flushed_lsn();
        wal.persist_clog(clog_lsn).unwrap();

        // Records appended AFTER the snapshot — only reconstructible by the fold.
        wal.append(WalRecord::insert_for_test(20, 2)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 20,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.append(WalRecord::insert_for_test(21, 3)).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 21,
            kind: WalRecordKind::Abort,
        })
        .unwrap();
        wal.flush().unwrap();

        drop(wal);
        let reopened = FileWalManager::open(&path).unwrap();
        assert!(reopened.is_committed(10)); // from the snapshot
        assert!(reopened.is_committed(20)); // folded from a post-snapshot Commit
        assert!(reopened.is_aborted(21)); // folded from a post-snapshot Abort
    }

    #[test]
    fn reopen_loads_vacuum_floor_from_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();
        commit_abort_commit(&wal);

        // A full VACUUM advanced the floor to 13 (every aborted-creator < 13 reclaimed).
        wal.set_vacuum_floor(13).unwrap();
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        drop(wal);

        // Reopen and re-persist: the snapshot still carries vacuum_floor 13, proving it
        // was loaded (it is not reset to the conservative boundary as before).
        let reopened = FileWalManager::open(&path).unwrap();
        reopened.persist_clog(reopened.flushed_lsn()).unwrap();
        let snapshot = decode_clog(&std::fs::read(clog_path_for(&path)).unwrap()).unwrap();
        assert_eq!(snapshot.vacuum_floor, 13);
        // The reclaimed abort 11 (< 13) is now implicit-committed, not explicit.
        assert!(!snapshot.aborted.contains(&11));
        assert_eq!(snapshot.committed_floor, 13);
    }

    #[test]
    fn absent_clog_snapshot_rebuilds_statuses_from_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();
        commit_abort_commit(&wal);
        // No persist_clog: no clog.dat exists (the pre-durable-CLOG / fresh case).
        drop(wal);

        let reopened = FileWalManager::open(&path).unwrap();
        assert!(reopened.is_committed(10));
        assert!(reopened.is_aborted(11));
        assert!(reopened.is_committed(12));
    }

    #[test]
    fn corrupt_clog_snapshot_fails_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.dat");
        let wal = FileWalManager::open(&path).unwrap();
        commit_abort_commit(&wal);
        wal.persist_clog(wal.flushed_lsn()).unwrap();
        drop(wal);

        // Corrupt the snapshot payload; an atomic temp+rename never tears a write, so
        // a CRC mismatch is real corruption and must surface (like a bad manifest.dat).
        let clog_path = clog_path_for(&path);
        let mut bytes = std::fs::read(&clog_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&clog_path, &bytes).unwrap();

        let Err(err) = FileWalManager::open(&path) else {
            panic!("a corrupt CLOG snapshot must fail open");
        };
        assert!(err.message.contains("checksum mismatch"));
    }

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
