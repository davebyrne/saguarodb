use common::{FileId, PageNum, Result};

use crate::PageData;

pub trait PageLoader: Send + Sync {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>>;
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
}
