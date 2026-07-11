use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use common::{DbError, QueryCancel, Result, SqlState};

/// Dictionary file: [magic "SGDC"][version u8][dict_id u32 LE][table_id u32 LE]
/// [payload_len u32 LE][crc32(payload) u32 LE][payload] (`compression.md` §7).
const DICT_MAGIC: [u8; 4] = *b"SGDC";
const DICT_FORMAT_VERSION: u8 = 1;
const DICT_HEADER_LEN: usize = 4 + 1 + 4 + 4 + 4 + 4;

/// Cap trained dictionaries at ~110 KiB (zstd's customary maximum).
const MAX_DICT_BYTES: usize = 112_640;
static TRAINING_ACTIVE: AtomicBool = AtomicBool::new(false);

struct TrainingPermit;

impl Drop for TrainingPermit {
    fn drop(&mut self) {
        TRAINING_ACTIVE.store(false, Ordering::Release);
    }
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

/// Train a zstd dictionary from page-image samples. `None` when the corpus is
/// too small for ZDICT (a freshly created or tiny table) — callers proceed
/// dict-less; training failure is never a statement error.
///
/// At-rest value is page-size-dependent: on 8 KiB pages over 4 KiB blocks a
/// trained dictionary rarely changes the reclaimed block count (a compressible
/// page already fits one block, and hole punching cannot go below one block).
/// It begins to help only at 16/32 KiB builds, where a page can span several
/// 4 KiB blocks and the dictionary can drop the count (measured numbers and
/// mechanism in `docs/specs/compression.md` §11).
pub fn train_dictionary(samples: &[Vec<u8>]) -> Option<Vec<u8>> {
    if samples.len() < 8 {
        return None;
    }
    zstd::dict::from_samples(samples, MAX_DICT_BYTES).ok()
}

/// Cancellation-aware dictionary training for foreground DDL. ZDICT exposes no
/// interruption callback, so training owns a bounded copy on a helper thread while
/// the caller polls the statement token. Cancellation returns promptly and the
/// side-effect-free helper is allowed to finish in the background.
pub fn train_dictionary_cancelable(
    samples: &[Vec<u8>],
    cancel: &QueryCancel,
) -> Result<Option<Vec<u8>>> {
    cancel.check()?;
    if samples.len() < 8 {
        return Ok(None);
    }
    loop {
        cancel.check()?;
        if TRAINING_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let permit = TrainingPermit;
    let samples = samples.to_vec();
    let (tx, rx) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("saguarodb-dict-training".to_string())
        .spawn(move || {
            let _permit = permit;
            let _ = tx.send(train_dictionary(&samples));
        })
        .map_err(|err| DbError::internal(format!("failed to start dictionary training: {err}")))?;
    loop {
        cancel.check()?;
        match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(dictionary) => return Ok(dictionary),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(DbError::internal("dictionary training worker stopped"));
            }
        }
    }
}

/// Immutable dictionary files under `<data>/dicts/<dict_id>.dict`.
pub struct DictStore {
    dir: PathBuf,
}

impl DictStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|err| {
            DbError::io(format!(
                "failed to create dict directory {}: {err}",
                dir.display()
            ))
        })?;
        Ok(Self { dir })
    }

    fn path(&self, dict_id: u32) -> PathBuf {
        self.dir.join(format!("{dict_id}.dict"))
    }

    /// Persist a dictionary durably (temp + fsync + rename + dir fsync, the
    /// control-file pattern). Idempotent: an existing file is left untouched
    /// so recovery replay of `CreateDictionary` is safe.
    pub fn save(&self, dict_id: u32, table_id: u32, bytes: &[u8]) -> Result<()> {
        let path = self.path(dict_id);
        if path.exists() {
            return Ok(());
        }
        let mut out = Vec::with_capacity(DICT_HEADER_LEN + bytes.len());
        out.extend_from_slice(&DICT_MAGIC);
        out.push(DICT_FORMAT_VERSION);
        out.extend_from_slice(&dict_id.to_le_bytes());
        out.extend_from_slice(&table_id.to_le_bytes());
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(bytes).to_le_bytes());
        out.extend_from_slice(bytes);

        let tmp = self.dir.join(format!("{dict_id}.dict.tmp"));
        fs::write(&tmp, &out)
            .map_err(|err| DbError::io(format!("failed to write {}: {err}", tmp.display())))?;
        File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|err| DbError::io(format!("failed to fsync {}: {err}", tmp.display())))?;
        fs::rename(&tmp, &path)
            .map_err(|err| DbError::io(format!("failed to rename dict file: {err}")))?;
        File::open(&self.dir)
            .and_then(|d| d.sync_all())
            .map_err(|err| DbError::io(format!("failed to fsync dict directory: {err}")))?;
        Ok(())
    }

    /// Remove a dictionary whose creating statement failed before durable commit.
    /// Also clears a temporary file left by a failed save. Idempotent when neither
    /// path exists; the directory fsync makes the removal durable before service
    /// continues.
    pub fn remove(&self, dict_id: u32) -> Result<()> {
        let path = self.path(dict_id);
        let tmp = self.dir.join(format!("{dict_id}.dict.tmp"));
        let mut removed = false;
        for candidate in [&path, &tmp] {
            match fs::remove_file(candidate) {
                Ok(()) => removed = true,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(DbError::io(format!(
                        "failed to remove dictionary file {}: {err}",
                        candidate.display()
                    )));
                }
            }
        }
        if removed {
            File::open(&self.dir)
                .and_then(|d| d.sync_all())
                .map_err(|err| {
                    DbError::io(format!("failed to fsync dictionary directory: {err}"))
                })?;
        }
        Ok(())
    }

    /// Load every `*.dict` file, CRC-validated: `(dict_id, table_id, bytes)`.
    pub fn load_all(&self) -> Result<Vec<(u32, u32, Vec<u8>)>> {
        let mut out = Vec::new();
        let entries = fs::read_dir(&self.dir)
            .map_err(|err| DbError::io(format!("failed to read dict directory: {err}")))?;
        for entry in entries {
            let entry = entry.map_err(|err| DbError::io(format!("dict dir entry: {err}")))?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("dict") {
                continue;
            }
            let bytes = fs::read(&path)
                .map_err(|err| DbError::io(format!("failed to read {}: {err}", path.display())))?;
            out.push(decode_dict_file(&bytes, &path)?);
        }
        Ok(out)
    }
}

fn decode_dict_file(bytes: &[u8], path: &Path) -> Result<(u32, u32, Vec<u8>)> {
    if bytes.len() < DICT_HEADER_LEN || bytes[..4] != DICT_MAGIC {
        return Err(corrupt(format!("bad dictionary file {}", path.display())));
    }
    if bytes[4] != DICT_FORMAT_VERSION {
        return Err(corrupt(format!(
            "unknown dict file version in {}",
            path.display()
        )));
    }
    let dict_id = u32::from_le_bytes(bytes[5..9].try_into().expect("4 bytes"));
    let table_id = u32::from_le_bytes(bytes[9..13].try_into().expect("4 bytes"));
    let len = u32::from_le_bytes(bytes[13..17].try_into().expect("4 bytes")) as usize;
    let stored_crc = u32::from_le_bytes(bytes[17..21].try_into().expect("4 bytes"));
    let payload = bytes
        .get(DICT_HEADER_LEN..DICT_HEADER_LEN + len)
        .ok_or_else(|| corrupt(format!("dict file {} truncated", path.display())))?;
    if crc32fast::hash(payload) != stored_crc {
        return Err(corrupt(format!(
            "dict file {} CRC mismatch",
            path.display()
        )));
    }
    Ok((dict_id, table_id, payload.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelable_training_honors_a_pending_timeout() {
        let cancel = QueryCancel::new();
        cancel.request(common::CancelReason::StatementTimeout);
        let samples = vec![vec![b'x'; 1024]; 8];

        let err = train_dictionary_cancelable(&samples, &cancel).unwrap_err();

        assert_eq!(err.code, SqlState::QueryCanceled);
    }

    #[test]
    fn dict_store_saves_and_loads_with_crc() {
        let dir = tempfile::tempdir().unwrap();
        let store = DictStore::open(dir.path().join("dicts")).unwrap();
        store.save(1, 42, b"dict-one-bytes").unwrap();
        store.save(2, 43, b"dict-two-bytes").unwrap();
        // Idempotent re-save (recovery replays CreateDictionary).
        store.save(1, 42, b"dict-one-bytes").unwrap();

        let mut all = store.load_all().unwrap();
        all.sort_by_key(|(id, _, _)| *id);
        assert_eq!(
            all,
            vec![
                (1, 42, b"dict-one-bytes".to_vec()),
                (2, 43, b"dict-two-bytes".to_vec()),
            ]
        );
    }

    #[test]
    fn dict_store_rejects_tampered_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = DictStore::open(dir.path().join("dicts")).unwrap();
        store.save(1, 42, b"dict-one-bytes").unwrap();
        let path = dir.path().join("dicts").join("1.dict");
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        assert!(store.load_all().is_err());
    }

    #[test]
    fn dict_store_remove_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = DictStore::open(dir.path().join("dicts")).unwrap();
        store.save(7, 42, b"prepared-dictionary").unwrap();

        store.remove(7).unwrap();
        assert!(store.load_all().unwrap().is_empty());
        store.remove(7).unwrap();
    }
}
