use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use buffer::{PAGE_SIZE, PageData, PageLoader, PageStore};
use common::{DbError, FileId, PageNum, Result};

/// File-id high bit marking a table generation's primary-key index file. A
/// relation generation's heap file id is its `storage_id`; its primary-index file
/// id is the `storage_id` with this bit set, so a single page store serves both
/// without collision.
pub(crate) const INDEX_FILE_BIT: FileId = 0x8000_0000;

/// File-id high *two* bits marking a secondary-index file. Distinct from the
/// primary-key index tag (top bit only): a secondary file id is the index
/// generation's `storage_id` with both bits set, so heaps (no high bit),
/// primary-key indexes (top bit), and secondary indexes (top two bits) never
/// collide while storage ids stay under `0x4000_0000` (enforced by the catalog).
pub(crate) const SECONDARY_INDEX_BITS: FileId = 0xC000_0000;

/// The heap file id for a relation generation.
pub(crate) fn heap_file_id(storage_id: FileId) -> FileId {
    storage_id
}

/// The primary-key index file id for a table generation.
pub(crate) fn primary_index_file_id(storage_id: FileId) -> FileId {
    storage_id | INDEX_FILE_BIT
}

/// The file id for a secondary index, tagged so it shares the page store with
/// heaps and primary-key indexes without collision.
pub(crate) fn secondary_index_file_id(storage_id: FileId) -> FileId {
    // Catalog construction/deserialization rejects storage ids carrying any
    // file-kind bits. Keep this pure mapping total; callers only receive
    // catalog-validated ids.
    storage_id | SECONDARY_INDEX_BITS
}

/// Filesystem allocation quantum assumed for hole punching. Punching is a
/// space optimization only — on a filesystem with larger blocks the punch
/// reclaims nothing and correctness is unaffected.
const FS_BLOCK_SIZE: usize = 4096;

/// Mutable page home backed by one file per relation generation: heaps at
/// `<dir>/<storage_id>.heap`, primary-key indexes at `<dir>/<storage_id>.idx`,
/// and secondary indexes at `<dir>/<storage_id>.sidx`, with page `n` stored at
/// byte offset `n * PAGE_SIZE`. Pages are loaded on a buffer miss and written
/// back in place when flushed.
pub struct HeapPageStore {
    dir: PathBuf,
    files: Mutex<HashMap<FileId, Arc<File>>>,
    compression: Arc<compress::CompressionRegistry>,
    /// Set when fallocate reports the fs cannot punch holes; skip thereafter.
    punch_unsupported: std::sync::atomic::AtomicBool,
}

impl HeapPageStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_compression(dir, Arc::new(compress::CompressionRegistry::new()))
    }

    pub fn open_with_compression(
        dir: impl AsRef<Path>,
        compression: Arc<compress::CompressionRegistry>,
    ) -> Result<Self> {
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
            compression,
            punch_unsupported: std::sync::atomic::AtomicBool::new(false),
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

    fn fsync_dir(&self) -> Result<()> {
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

    /// Read the raw on-disk slot for a page, if it exists. `Ok(None)` means the
    /// page is not present (beyond EOF or a short tail); a hole between two
    /// written pages reads back as a zeroed slot, matching sparse-file semantics.
    fn read_slot(&self, file_id: FileId, page_num: PageNum) -> Result<Option<[u8; PAGE_SIZE]>> {
        let Some(file) = self.handle(file_id, false)? else {
            return Ok(None);
        };
        let mut buf = [0u8; PAGE_SIZE];
        match file.read_exact_at(&mut buf, page_offset(page_num)) {
            Ok(()) => Ok(Some(buf)),
            // A full page does not exist at this offset (beyond EOF or a short
            // tail). Treated as not-present; redo/checkpoint will rewrite it.
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(DbError::io(format!(
                "failed to read heap page {file_id}/{page_num}: {err}"
            ))),
        }
    }

    /// Read and decode a page slot, applying the file's compression config.
    /// `lenient` controls how a corrupt envelope is reported: `false` (normal
    /// reads) surfaces it loudly; `true` (recovery redo) reports the page as
    /// absent so a zeroed frame is repaired by the following FullPageImage.
    fn decode_slot(
        &self,
        file_id: FileId,
        page_num: PageNum,
        lenient: bool,
    ) -> Result<Option<PageData>> {
        let Some(buf) = self.read_slot(file_id, page_num)? else {
            return Ok(None);
        };
        match self.compression.decompress_page(&buf, PAGE_SIZE) {
            Ok(Some(image)) => {
                let actual_len = image.len();
                let bytes: [u8; PAGE_SIZE] = image.try_into().map_err(|_| {
                    DbError::storage(
                        common::SqlState::InternalError,
                        format!("decompressed page has {actual_len} bytes, expected {PAGE_SIZE}"),
                    )
                })?;
                Ok(Some(PageData(bytes)))
            }
            Ok(None) => Ok(Some(PageData(buf))),
            // Corrupt envelope: absent for redo (zeroed frame + FPI repair, which
            // is strictly better detection than a torn raw page's garbage
            // PageLSN); loud corruption error for normal reads.
            Err(_) if lenient => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Best-effort hole punch over `[offset, offset + len)`; never fails the
    /// write. Stale trailing bytes left by a failed or skipped punch are never
    /// read back — the envelope is length-delimited.
    #[cfg(target_os = "linux")]
    fn punch_hole(&self, file: &File, offset: u64, len: u64) {
        use std::sync::atomic::Ordering;
        if self.punch_unsupported.load(Ordering::Relaxed) {
            return;
        }
        let result = rustix::fs::fallocate(
            file,
            rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
            offset,
            len,
        );
        if matches!(
            result,
            Err(rustix::io::Errno::OPNOTSUPP | rustix::io::Errno::INVAL)
        ) {
            self.punch_unsupported.store(true, Ordering::Relaxed);
        }
        // Any punch failure is tolerated: the slot keeps stale trailing
        // bytes, which the length-delimited envelope decode never reads.
    }

    #[cfg(not(target_os = "linux"))]
    fn punch_hole(&self, _file: &File, _offset: u64, _len: u64) {}
}

fn page_offset(page_num: PageNum) -> u64 {
    page_num as u64 * PAGE_SIZE as u64
}

impl PageLoader for HeapPageStore {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
        self.decode_slot(file_id, page_num, false)
    }

    fn load_page_lenient(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
        self.decode_slot(file_id, page_num, true)
    }
}

impl PageStore for HeapPageStore {
    fn write_page(&self, file_id: FileId, page_num: PageNum, data: &PageData) -> Result<()> {
        let file = self
            .handle(file_id, true)?
            .ok_or_else(|| DbError::internal("heap file creation returned no handle"))?;
        let offset = page_offset(page_num);
        if let Some(envelope) = self.compression.compress_page_at_rest(file_id, &data.0)? {
            // Smallest whole number of fs blocks holding the envelope; only
            // worthwhile when it frees at least one block of the page's slot.
            // This 4 KiB (FS_BLOCK_SIZE) quantum is why a trained dictionary
            // buys little at rest on 8 KiB pages: a compressible page already
            // fits one block, and nothing (dict or not) goes below one block.
            // Larger pages leave room between block boundaries for a dictionary
            // to lower the block count (docs/specs/compression.md §11).
            let used = envelope.len().div_ceil(FS_BLOCK_SIZE) * FS_BLOCK_SIZE;
            if used < PAGE_SIZE {
                // Write the envelope zero-padded to a FULL slot, then punch the
                // trailing blocks. Writing the whole slot keeps st_size — and so
                // page_count = st_size / PAGE_SIZE, which seeds the allocator and
                // bounds VACUUM's full-extent scan — identical to the raw path
                // even when this page is the file's current tail (a short write
                // there would under-report the extent; PUNCH_HOLE|KEEP_SIZE
                // preserves st_size but never grows it). No set_len: a stale
                // metadata read racing a concurrent later-page write could
                // truncate it. The punch then returns the padding blocks.
                let mut slot = [0u8; PAGE_SIZE];
                slot[..envelope.len()].copy_from_slice(&envelope);
                file.write_all_at(&slot, offset).map_err(|err| {
                    DbError::io(format!(
                        "failed to write heap page {file_id}/{page_num}: {err}"
                    ))
                })?;
                self.punch_hole(&file, offset + used as u64, (PAGE_SIZE - used) as u64);
                return Ok(());
            }
        }
        // Note: this raw write also handles a partially-punched prior slot;
        // writing the full 8 KiB re-allocates (un-punches) the hole.
        file.write_all_at(&data.0, offset).map_err(|err| {
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
        self.fsync_dir()
    }

    fn sync_files(&self, file_ids: &[FileId]) -> Result<()> {
        let handles = {
            let files = self
                .files
                .lock()
                .map_err(|_| DbError::internal("heap store lock poisoned"))?;
            file_ids
                .iter()
                .filter_map(|file_id| files.get(file_id).cloned().map(|file| (*file_id, file)))
                .collect::<Vec<_>>()
        };
        for (file_id, file) in handles {
            file.sync_all().map_err(|err| {
                DbError::io(format!("failed to fsync heap file {file_id}: {err}"))
            })?;
        }
        self.fsync_dir()
    }

    fn remove_file(&self, file_id: FileId) -> Result<()> {
        self.files
            .lock()
            .map_err(|_| DbError::internal("heap store lock poisoned"))?
            .remove(&file_id);
        let path = self.path(file_id);
        match std::fs::remove_file(&path) {
            Ok(()) => self.fsync_dir(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(DbError::io(format!(
                "failed to remove heap file {}: {err}",
                path.display()
            ))),
        }
    }

    fn list_file_ids(&self) -> Result<Vec<FileId>> {
        let mut ids = Vec::new();
        let entries = std::fs::read_dir(&self.dir).map_err(|err| {
            DbError::io(format!(
                "failed to read heap directory {}: {err}",
                self.dir.display()
            ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|err| DbError::io(format!("heap dir entry: {err}")))?;
            let path = entry.path();
            let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
                continue;
            };
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Ok(storage_id) = stem.parse::<FileId>() else {
                continue;
            };
            match extension {
                "heap" => ids.push(heap_file_id(storage_id)),
                "idx" => ids.push(primary_index_file_id(storage_id)),
                "sidx" => ids.push(secondary_index_file_id(storage_id)),
                _ => {}
            }
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
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
        let index = super::primary_index_file_id(5);

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
        let primary = super::primary_index_file_id(5);
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

    use compress::{CompressionRegistry, FileCompression};
    use std::sync::Arc;

    fn compressible_page() -> PageData {
        let mut data = [0u8; PAGE_SIZE];
        let row = b"repetitive-row-content-abcdefghijklmnopqrstuvwxyz;";
        for (i, byte) in row.iter().cycle().take(PAGE_SIZE).enumerate() {
            data[i] = *byte;
        }
        PageData(data)
    }

    fn zstd_store(dir: &std::path::Path) -> (HeapPageStore, Arc<CompressionRegistry>) {
        let registry = Arc::new(CompressionRegistry::new());
        registry.set_file_config(1, FileCompression::Zstd { dict_id: None });
        let store = HeapPageStore::open_with_compression(dir, registry.clone()).unwrap();
        (store, registry)
    }

    #[test]
    fn compressed_write_round_trips_through_load() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _registry) = zstd_store(dir.path());
        let page = compressible_page();
        store.write_page(1, 0, &page).unwrap();
        store.sync_all().unwrap();
        assert_eq!(store.load_page(1, 0).unwrap().unwrap().0, page.0);
        // page_count is st_size-based and unaffected by punching.
        assert_eq!(store.page_count(1).unwrap(), 1);
    }

    #[test]
    fn mixed_raw_and_compressed_slots_coexist_in_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(CompressionRegistry::new());
        let store = HeapPageStore::open_with_compression(dir.path(), registry.clone()).unwrap();

        store.write_page(1, 0, &compressible_page()).unwrap(); // raw (no config yet)
        registry.set_file_config(1, FileCompression::Zstd { dict_id: None });
        store.write_page(1, 1, &compressible_page()).unwrap(); // compressed

        assert_eq!(
            store.load_page(1, 0).unwrap().unwrap().0,
            compressible_page().0
        );
        assert_eq!(
            store.load_page(1, 1).unwrap().unwrap().0,
            compressible_page().0
        );

        // Rewriting page 1 raw (config back to None) un-punches it.
        registry.set_file_config(1, FileCompression::None);
        store.write_page(1, 1, &compressible_page()).unwrap();
        assert_eq!(
            store.load_page(1, 1).unwrap().unwrap().0,
            compressible_page().0
        );
    }

    #[test]
    fn corrupt_envelope_errors_strictly_but_reads_none_leniently() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _registry) = zstd_store(dir.path());
        store.write_page(1, 0, &compressible_page()).unwrap();

        // Flip one payload byte on disk (past the 18-byte envelope header).
        let path = dir.path().join("1.heap");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[compress::ENVELOPE_HEADER_LEN + 4] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let err = store.load_page(1, 0).unwrap_err();
        assert_eq!(err.code, common::SqlState::InternalError);
        // Lenient (redo) load treats the torn slot as absent → zeroed → FPI repair.
        assert!(store.load_page_lenient(1, 0).unwrap().is_none());
    }

    #[test]
    fn hole_punch_reclaims_blocks_when_supported() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _registry) = zstd_store(dir.path());
        for page_num in 0..8 {
            store.write_page(1, page_num, &compressible_page()).unwrap();
        }
        store.sync_all().unwrap();

        let meta = std::fs::metadata(dir.path().join("1.heap")).unwrap();
        use std::os::unix::fs::MetadataExt;
        let allocated = meta.blocks() * 512;
        let logical = meta.len();
        assert_eq!(logical, 8 * PAGE_SIZE as u64);
        // On a hole-punch filesystem each 8K slot keeps only its first 4K block.
        // Skip (don't fail) where the fs doesn't support punching.
        if allocated >= logical {
            eprintln!("skipping: filesystem did not reclaim punched blocks");
            return;
        }
        assert!(
            allocated <= logical / 2 + 4096,
            "allocated={allocated} logical={logical}"
        );
    }
}
