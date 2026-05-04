use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use common::{DbError, Lsn, Result};

use crate::codec::{max_lsn, read_records};
use crate::{WalManager, WalRecord, WalRecordKind, encode_record};

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
    committed_txns: HashSet<u64>,
    pending_commits: HashSet<u64>,
    poisoned: Option<String>,
    #[cfg(test)]
    fail_next_flush: Option<String>,
    #[cfg(test)]
    fail_next_post_write_seek: Option<String>,
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
        let committed_txns = committed_transactions(&records, flushed_lsn);
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
                committed_txns,
                pending_commits: HashSet::new(),
                poisoned: None,
                #[cfg(test)]
                fail_next_flush: None,
                #[cfg(test)]
                fail_next_post_write_seek: None,
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

        if matches!(record.kind, WalRecordKind::Commit) {
            state.pending_commits.insert(record.txn_id);
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
        state.committed_txns.extend(pending);
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

    fn replay_committed_from(
        &self,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>> {
        let state = self.lock_state()?;
        let committed_after: HashSet<_> = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn > lsn)
            .filter(|stored| matches!(stored.record.kind, WalRecordKind::Commit))
            .filter(|stored| state.committed_txns.contains(&stored.record.txn_id))
            .map(|stored| stored.record.txn_id)
            .collect();
        let records: Vec<_> = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn > lsn)
            .filter(|stored| committed_after.contains(&stored.record.txn_id))
            .filter(|stored| is_logical_operation(&stored.record.kind))
            .map(|stored| Ok(stored.record.clone()))
            .collect();
        Ok(Box::new(records.into_iter()))
    }

    fn truncate_before(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.lock_state()?;
        let retained: Vec<_> = state
            .records
            .iter()
            .filter(|stored| stored.record.lsn >= lsn)
            .cloned()
            .collect();
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
        state.committed_txns = committed_transactions(&state.records, state.flushed_lsn);
        state.pending_commits = pending_commits(&state.records, state.flushed_lsn);
        if let Err(err) = sync_parent_dir(&self.path) {
            state.poisoned = Some(err.message.clone());
            return Err(err);
        }

        Ok(())
    }

    fn is_committed(&self, txn_id: u64) -> bool {
        self.state
            .lock()
            .map(|state| state.committed_txns.contains(&txn_id))
            .unwrap_or(false)
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
}

fn committed_transactions(records: &[StoredRecord], flushed_lsn: Lsn) -> HashSet<u64> {
    records
        .iter()
        .filter(|stored| stored.record.lsn <= flushed_lsn)
        .filter(|stored| matches!(stored.record.kind, WalRecordKind::Commit))
        .map(|stored| stored.record.txn_id)
        .collect()
}

fn pending_commits(records: &[StoredRecord], flushed_lsn: Lsn) -> HashSet<u64> {
    records
        .iter()
        .filter(|stored| stored.record.lsn > flushed_lsn)
        .filter(|stored| matches!(stored.record.kind, WalRecordKind::Commit))
        .map(|stored| stored.record.txn_id)
        .collect()
}

fn is_logical_operation(kind: &WalRecordKind) -> bool {
    matches!(
        kind,
        WalRecordKind::Insert { .. }
            | WalRecordKind::Update { .. }
            | WalRecordKind::Delete { .. }
            | WalRecordKind::CreateTable { .. }
            | WalRecordKind::DropTable { .. }
    )
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
