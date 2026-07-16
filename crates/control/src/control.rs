use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use common::{DbError, Lsn, Result, SqlState, TableId};

use crate::ControlData;
use crate::manifest::{MAX_MANIFEST_BYTES, decode_control, encode_control};

/// Persists the durable control record (the checkpoint commit point). The record
/// is written atomically via a temp file + rename + directory fsync.
pub trait ControlStore: Send + Sync {
    /// Load the current control record, or `None` when none exists yet.
    fn load(&self) -> Result<Option<ControlData>>;

    /// Atomically write a new control record. This is the durable commit point of
    /// a checkpoint: it must run only after heap pages are fsynced and before the
    /// WAL is truncated.
    fn store(&self, checkpoint_lsn: Lsn, tables: &[TableId], catalog: &[u8]) -> Result<()>;
}

pub struct FileControlStore {
    data_dir: PathBuf,
    page_size: u32,
}

impl FileControlStore {
    pub fn open(data_dir: impl AsRef<Path>, page_size: u32) -> Result<Self> {
        fs::create_dir_all(data_dir.as_ref()).map_err(|err| {
            DbError::io(format!(
                "failed to create data directory {}: {err}",
                data_dir.as_ref().display()
            ))
        })?;
        Ok(Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            page_size,
        })
    }

    fn manifest_path(&self) -> PathBuf {
        self.data_dir.join("manifest.dat")
    }

    fn manifest_tmp_path(&self) -> PathBuf {
        self.data_dir.join("manifest.dat.tmp")
    }
}

impl ControlStore for FileControlStore {
    fn load(&self) -> Result<Option<ControlData>> {
        match File::open(self.manifest_path()) {
            Ok(mut file) => {
                let bytes = read_manifest_bounded(&mut file, &self.manifest_path())?;
                let control = decode_control(&bytes)?;
                if control.page_size != self.page_size {
                    return Err(DbError::storage(
                        SqlState::InternalError,
                        format!(
                            "page size mismatch: data directory uses {} bytes, this binary uses {}",
                            control.page_size, self.page_size
                        ),
                    ));
                }
                Ok(Some(control))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(DbError::io(format!(
                "failed to read control file {}: {err}",
                self.manifest_path().display()
            ))),
        }
    }

    fn store(&self, checkpoint_lsn: Lsn, tables: &[TableId], catalog: &[u8]) -> Result<()> {
        let mut table_copy = Vec::new();
        table_copy
            .try_reserve_exact(tables.len())
            .map_err(|_| DbError::io("cannot allocate control table list"))?;
        table_copy.extend_from_slice(tables);
        let mut catalog_copy = Vec::new();
        catalog_copy
            .try_reserve_exact(catalog.len())
            .map_err(|_| DbError::io("cannot allocate control catalog buffer"))?;
        catalog_copy.extend_from_slice(catalog);
        let control = ControlData {
            checkpoint_lsn,
            tables: table_copy,
            catalog: catalog_copy,
            page_size: self.page_size,
        };
        let bytes = encode_control(&control)?;
        let tmp_path = self.manifest_tmp_path();
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|err| {
                    DbError::io(format!(
                        "failed to open temporary control file {}: {err}",
                        tmp_path.display()
                    ))
                })?;
            file.write_all(&bytes).map_err(|err| {
                DbError::io(format!(
                    "failed to write temporary control file {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.sync_all().map_err(|err| {
                DbError::io(format!(
                    "failed to fsync temporary control file {}: {err}",
                    tmp_path.display()
                ))
            })?;
        }
        fs::rename(&tmp_path, self.manifest_path()).map_err(|err| {
            DbError::io(format!(
                "failed to replace control file {} with {}: {err}",
                self.manifest_path().display(),
                tmp_path.display()
            ))
        })?;
        fsync_dir(&self.data_dir)
    }
}

fn read_manifest_bounded(file: &mut File, path: &Path) -> Result<Vec<u8>> {
    let declared_len = file
        .metadata()
        .map_err(|err| {
            DbError::io(format!(
                "failed to inspect control file {}: {err}",
                path.display()
            ))
        })?
        .len();
    let declared_len = usize::try_from(declared_len)
        .map_err(|_| DbError::io("control file length does not fit this platform"))?;
    if declared_len > MAX_MANIFEST_BYTES {
        return Err(DbError::storage(
            SqlState::InternalError,
            "control file exceeds size limit",
        ));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(declared_len)
        .map_err(|_| DbError::io("cannot allocate control file buffer"))?;
    let mut chunk = [0_u8; 8192];
    loop {
        let read = file.read(&mut chunk).map_err(|err| {
            DbError::io(format!(
                "failed to read control file {}: {err}",
                path.display()
            ))
        })?;
        if read == 0 {
            return Ok(bytes);
        }
        let next_len = bytes
            .len()
            .checked_add(read)
            .ok_or_else(|| DbError::io("control file length overflows"))?;
        if next_len > MAX_MANIFEST_BYTES {
            return Err(DbError::storage(
                SqlState::InternalError,
                "control file exceeds size limit",
            ));
        }
        bytes
            .try_reserve(read)
            .map_err(|_| DbError::io("cannot grow control file buffer"))?;
        let source = chunk
            .get(..read)
            .ok_or_else(|| DbError::io("control file read exceeded buffer"))?;
        bytes.extend_from_slice(source);
    }
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|err| {
            DbError::io(format!(
                "failed to fsync data directory {}: {err}",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use crate::manifest::MAX_MANIFEST_BYTES;

    use super::{ControlStore, FileControlStore};

    #[test]
    fn load_rejects_page_size_mismatch_with_clean_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(dir.path(), 8192).unwrap();
        store.store(1, &[], &[]).unwrap();

        let mismatched = FileControlStore::open(dir.path(), 16384).unwrap();
        let err = mismatched.load().unwrap_err();
        assert!(err.message.contains("page size"), "got: {}", err.message);
        assert!(err.message.contains("8192"));
        assert!(err.message.contains("16384"));
    }

    #[test]
    fn load_rejects_oversized_manifest_before_reading_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.dat");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len(u64::try_from(MAX_MANIFEST_BYTES + 1).unwrap())
            .unwrap();
        let store = FileControlStore::open(dir.path(), 8192).unwrap();

        let err = store.load().unwrap_err();
        assert!(err.message.contains("control file exceeds size limit"));
    }
}
