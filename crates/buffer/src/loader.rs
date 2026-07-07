use common::{FileId, PageNum, Result};

use crate::PageData;

pub trait PageLoader: Send + Sync {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>>;

    /// Like `load_page`, but a page whose stored form fails validation (a torn
    /// compressed envelope) is reported as absent instead of an error. ONLY for
    /// recovery redo, where a zeroed frame is re-established by a FullPageImage;
    /// normal reads must use `load_page`, which surfaces corruption loudly.
    fn load_page_lenient(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
        self.load_page(file_id, page_num)
    }
}

/// A read/write page home: loads pages on a buffer miss and flushes dirty pages
/// back to their durable location. Implementations map `(file_id, page_num)` to
/// a byte offset in a per-file backing store.
pub trait PageStore: PageLoader {
    /// Write a page to its home location. Does not fsync; durability is the
    /// caller's responsibility via `sync_all` (e.g. at checkpoint).
    fn write_page(&self, file_id: FileId, page_num: PageNum, data: &PageData) -> Result<()>;

    /// Durably flush all previously written pages to stable storage.
    fn sync_all(&self) -> Result<()>;

    /// The number of pages currently stored for `file_id` (its on-disk extent),
    /// or `0` if the file does not exist. Used to seed page allocation so a freshly
    /// allocated page never reuses one that already exists on disk after recovery.
    fn page_count(&self, file_id: FileId) -> Result<PageNum>;

    /// Remove the durable file for `file_id`, if it exists. Callers must first
    /// ensure no buffer frames for the file are pinned or still needed.
    fn remove_file(&self, file_id: FileId) -> Result<()>;

    /// List durable file ids known to this store. Used for startup orphan cleanup.
    fn list_file_ids(&self) -> Result<Vec<FileId>>;
}
