use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use buffer::{PAGE_SIZE, PageData, PageLoader, PageStore};
use common::{DbError, FileId, IndexId, PageNum, Result, TableId};

/// File-id high bit marking a table's primary-key index file. A table's heap file
/// id is its table id; its index file id is the table id with this bit set, so a
/// single page store serves both without collision (v1 table ids are small).
pub(crate) const INDEX_FILE_BIT: FileId = 0x8000_0000;

/// File-id high *two* bits marking a secondary-index file. Distinct from the
/// primary-key index tag (top bit only): a secondary file id is `index_id` with
/// both bits set, so heaps (no high bit), primary-key indexes (top bit), and
/// secondary indexes (top two bits) never collide while table and index ids stay
/// under `0x4000_0000` (always true in v1).
pub(crate) const SECONDARY_INDEX_BITS: FileId = 0xC000_0000;

/// The primary-key index file id for a table (distinct from its heap file id).
pub(crate) fn index_file_id(table: TableId) -> FileId {
    table | INDEX_FILE_BIT
}

/// The file id for a secondary index, tagged so it shares the page store with
/// heaps and primary-key indexes without collision.
pub(crate) fn secondary_index_file_id(index: IndexId) -> FileId {
    debug_assert_eq!(
        index & SECONDARY_INDEX_BITS,
        0,
        "secondary index id {index} does not fit in 30 bits"
    );
    index | SECONDARY_INDEX_BITS
}

/// Mutable page home backed by one file per table: the heap at `<dir>/<id>.heap`
/// and the primary-key index at `<dir>/<table>.idx`, with page `n` stored at byte
/// offset `n * PAGE_SIZE`. Pages are loaded on a buffer miss and written back in
/// place when flushed.
pub struct HeapPageStore {
    dir: PathBuf,
    files: Mutex<HashMap<FileId, Arc<File>>>,
}

impl HeapPageStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|err| {
            DbError::io(format!(
                "failed to create heap directory {}: {err}",
                dir.display()
            ))
        })?;
        Ok(Self {
            dir,
            files: Mutex::new(HashMap::new()),
        })
    }

    fn path(&self, file_id: FileId) -> PathBuf {
        if file_id & SECONDARY_INDEX_BITS == SECONDARY_INDEX_BITS {
            self.dir
                .join(format!("{}.sidx", file_id & !SECONDARY_INDEX_BITS))
        } else if file_id & INDEX_FILE_BIT != 0 {
            self.dir.join(format!("{}.idx", file_id & !INDEX_FILE_BIT))
        } else {
            self.dir.join(format!("{file_id}.heap"))
        }
    }

    /// Return a shared handle to a table's heap file, opening (and optionally
    /// creating) it on first use. Returns `None` only when `create` is false and
    /// the file does not exist.
    fn handle(&self, file_id: FileId, create: bool) -> Result<Option<Arc<File>>> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| DbError::internal("heap store lock poisoned"))?;
        if let Some(file) = files.get(&file_id) {
            return Ok(Some(file.clone()));
        }
        let path = self.path(file_id);
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(&path)
        {
            Ok(file) => {
                let handle = Arc::new(file);
                files.insert(file_id, handle.clone());
                Ok(Some(handle))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound && !create => Ok(None),
            Err(err) => Err(DbError::io(format!(
                "failed to open heap file {}: {err}",
                path.display()
            ))),
        }
    }
}

fn page_offset(page_num: PageNum) -> u64 {
    page_num as u64 * PAGE_SIZE as u64
}

impl PageLoader for HeapPageStore {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
        let Some(file) = self.handle(file_id, false)? else {
            return Ok(None);
        };
        let mut buf = [0u8; PAGE_SIZE];
        match file.read_exact_at(&mut buf, page_offset(page_num)) {
            Ok(()) => Ok(Some(PageData(buf))),
            // A full page does not exist at this offset (beyond EOF or a short
            // tail). Treated as not-present; redo/checkpoint will rewrite it.
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(DbError::io(format!(
                "failed to read heap page {file_id}/{page_num}: {err}"
            ))),
        }
    }
}

impl PageStore for HeapPageStore {
    fn write_page(&self, file_id: FileId, page_num: PageNum, data: &PageData) -> Result<()> {
        let file = self
            .handle(file_id, true)?
            .expect("handle with create=true is always Some");
        file.write_all_at(&data.0, page_offset(page_num))
            .map_err(|err| {
                DbError::io(format!(
                    "failed to write heap page {file_id}/{page_num}: {err}"
                ))
            })
    }

    fn page_count(&self, file_id: FileId) -> Result<PageNum> {
        let Some(file) = self.handle(file_id, false)? else {
            return Ok(0);
        };
        let len = file
            .metadata()
            .map_err(|err| DbError::io(format!("failed to stat file {file_id}: {err}")))?
            .len();
        Ok((len / PAGE_SIZE as u64) as PageNum)
    }

    fn sync_all(&self) -> Result<()> {
        let handles: Vec<Arc<File>> = {
            let files = self
                .files
                .lock()
                .map_err(|_| DbError::internal("heap store lock poisoned"))?;
            files.values().cloned().collect()
        };
        for file in handles {
            file.sync_all()
                .map_err(|err| DbError::io(format!("failed to fsync heap file: {err}")))?;
        }
        // fsync the directory so newly created heap files are durable.
        let dir = File::open(&self.dir).map_err(|err| {
            DbError::io(format!(
                "failed to open heap directory {} for fsync: {err}",
                self.dir.display()
            ))
        })?;
        dir.sync_all().map_err(|err| {
            DbError::io(format!(
                "failed to fsync heap directory {}: {err}",
                self.dir.display()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use buffer::{PAGE_SIZE, PageData, PageLoader, PageStore};

    use super::HeapPageStore;

    fn page(fill: u8) -> PageData {
        PageData([fill; PAGE_SIZE])
    }

    #[test]
    fn writes_and_reads_back_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeapPageStore::open(dir.path()).unwrap();

        store.write_page(2, 0, &page(0xAA)).unwrap();
        store.write_page(2, 2, &page(0xCC)).unwrap();
        store.sync_all().unwrap();

        assert_eq!(store.load_page(2, 0).unwrap(), Some(page(0xAA)));
        assert_eq!(store.load_page(2, 2).unwrap(), Some(page(0xCC)));
    }

    #[test]
    fn load_missing_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeapPageStore::open(dir.path()).unwrap();

        assert_eq!(store.load_page(7, 0).unwrap(), None);
    }

    #[test]
    fn sparse_gap_reads_zeroed_page_and_beyond_eof_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeapPageStore::open(dir.path()).unwrap();

        store.write_page(1, 0, &page(0x11)).unwrap();
        store.write_page(1, 2, &page(0x22)).unwrap();

        // Page 1 is a hole between two written pages: a full, zeroed page exists.
        assert_eq!(store.load_page(1, 1).unwrap(), Some(page(0x00)));
        // Page 3 is beyond the end of the file: not present.
        assert_eq!(store.load_page(1, 3).unwrap(), None);
    }

    #[test]
    fn reopen_sees_previously_written_pages() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = HeapPageStore::open(dir.path()).unwrap();
            store.write_page(4, 1, &page(0x5A)).unwrap();
            store.sync_all().unwrap();
        }
        let reopened = HeapPageStore::open(dir.path()).unwrap();
        assert_eq!(reopened.load_page(4, 1).unwrap(), Some(page(0x5A)));
    }

    #[test]
    fn index_and_heap_files_are_separate() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeapPageStore::open(dir.path()).unwrap();
        let index = super::index_file_id(5);

        store.write_page(5, 0, &page(0x11)).unwrap();
        store.write_page(index, 0, &page(0x22)).unwrap();
        store.sync_all().unwrap();

        assert_eq!(store.load_page(5, 0).unwrap(), Some(page(0x11)));
        assert_eq!(store.load_page(index, 0).unwrap(), Some(page(0x22)));
        assert!(dir.path().join("5.heap").exists());
        assert!(dir.path().join("5.idx").exists());
    }

    #[test]
    fn heap_primary_and_secondary_index_files_do_not_collide() {
        let dir = tempfile::tempdir().unwrap();
        let store = HeapPageStore::open(dir.path()).unwrap();
        // Same numeric id, three namespaces: heap 5, primary-key index 5,
        // secondary index 5 must all be distinct files.
        let primary = super::index_file_id(5);
        let secondary = super::secondary_index_file_id(5);
        assert_ne!(primary, secondary);

        store.write_page(5, 0, &page(0x11)).unwrap();
        store.write_page(primary, 0, &page(0x22)).unwrap();
        store.write_page(secondary, 0, &page(0x33)).unwrap();
        store.sync_all().unwrap();

        assert_eq!(store.load_page(5, 0).unwrap(), Some(page(0x11)));
        assert_eq!(store.load_page(primary, 0).unwrap(), Some(page(0x22)));
        assert_eq!(store.load_page(secondary, 0).unwrap(), Some(page(0x33)));
        assert!(dir.path().join("5.heap").exists());
        assert!(dir.path().join("5.idx").exists());
        assert!(dir.path().join("5.sidx").exists());
    }
}
