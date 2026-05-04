# `buffer` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`buffer` manages in-memory page frames, page latches, dirty tracking, statement rollback, and snapshot iteration. V1 never evicts dirty pages; dirty pages are made clean only after a successful snapshot checkpoint.

## Depends On

- `common`
- `parking_lot`

## Page Model

- Page size: 8192 bytes.
- Frames are addressed by `(FileId, PageNum)`.
- The buffer pool may read clean pages from the current snapshot files through an injected page loader.
- Dirty pages remain in memory until snapshot checkpoint or rollback.

## Public API

```rust
pub struct PageData(pub [u8; PAGE_SIZE]);

pub struct PageInfo {
    pub file_id: FileId,
    pub page_num: PageNum,
    pub data: PageData,
    pub is_dirty: bool,
}

pub trait PageLoader: Send + Sync {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>>;
}

pub trait BufferPool: Send + Sync {
    fn read_page(&self, file_id: FileId, page_num: PageNum) -> Result<PageReadGuard>;
    fn write_page(&self, file_id: FileId, page_num: PageNum, txn_id: u64) -> Result<PageWriteGuard>;
    fn new_page(&self, file_id: FileId, txn_id: u64) -> Result<PageWriteGuard>;
    fn load_page(&self, file_id: FileId, page_num: PageNum, data: PageData) -> Result<()>;
    fn iter_pages(&self) -> Result<Box<dyn Iterator<Item = PageInfo>>>;
    fn mark_all_clean(&self) -> Result<()>;
    fn rollback(&self, txn_id: u64) -> Result<()>;
    fn commit(&self, txn_id: u64) -> Result<()>;
}
```

`MemoryBufferPool::new(frame_count, flush_policy, page_loader)` stores `Box<dyn FlushPolicy>` and `Arc<dyn PageLoader>`. `read_page` first checks resident frames; on a miss it calls `page_loader.load_page(file_id, page_num)`. `Some(data)` is inserted as a clean page and returned. `None` means the page does not exist and returns `ErrorKind::Storage` / `SqlState::InternalError` with message `page not found`. Loader I/O errors propagate as `ErrorKind::Io`.

In production, the server supplies a `SnapshotPageLoader` that wraps `SnapshotManager::current_table_pages(file_id as TableId)` and returns the matching `page_num` when present. The `buffer` crate defines only the trait; it does not depend on `snapshot`.

`MemoryBufferPool::empty(frame_count)` is a test helper that uses a never-flush policy and a `NoopPageLoader` returning `Ok(None)`.

`load_page(file_id, page_num, data)` is used during snapshot loading. If the page is not resident, it inserts `data` as a clean frame. If `(file_id, page_num)` is already resident, it must leave resident bytes, dirty state, dirty transaction ID, and rollback metadata unchanged, then still advance `next_page_num_by_file` to at least `page_num + 1` and return `Ok(())`. It must not mark the page dirty or create rollback metadata. `iter_pages` returns pages currently known to the buffer pool. Snapshot writing combines dirty pages from `iter_pages` with clean pages copied from current snapshot files.

`commit(txn_id)` is cleanup-only: it discards before-images and new-page tracking after WAL flush succeeds. It must not perform I/O and should not fail for a valid `txn_id`. If it fails after a durable WAL commit, server treats that as fatal and does not roll back.

## Page Guards

```rust
pub struct PageReadGuard { /* owned guard */ }
pub struct PageWriteGuard { /* owned guard */ }

impl PageReadGuard {
    pub fn file_id(&self) -> FileId;
    pub fn page_num(&self) -> PageNum;
    pub fn data(&self) -> &[u8; PAGE_SIZE];
}

impl PageWriteGuard {
    pub fn file_id(&self) -> FileId;
    pub fn page_num(&self) -> PageNum;
    pub fn data(&self) -> &[u8; PAGE_SIZE];
    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE];
}
```

Read guards hold a read latch and unpin on drop. Write guards hold a write latch, set dirty state for `txn_id`, and unpin on drop.

`new_page(file_id, txn_id)` allocates the next unused page number for that file and returns a `PageWriteGuard` whose `page_num()` identifies the new page. The pool tracks `next_page_num_by_file`; `load_page(file_id, page_num, ...)` advances this counter to at least `page_num + 1`, and rollback of a new page removes the page but does not need to reuse its page number in v1.

Guards are owned and object-safe. They may internally hold `Arc<Frame>`.

## Frame Descriptor

Each frame tracks:

- `file_id`
- `page_num`
- `pin_count`
- `dirty`
- `dirty_since_snapshot`
- `dirty_txn_id`
- `reference_bit`
- latch state

`dirty_txn_id` is the last transaction that modified the page. It is not enough for rollback by itself; before-images and new-page tracking handle rollback.

## Rollback Tracking

The buffer pool tracks active write transactions:

```rust
pub struct TxnDirtyState {
    pub before_images: HashMap<(FileId, PageNum), PageData>,
    pub new_pages: Vec<(FileId, PageNum)>,
}
```

Rules:

- On first `write_page(file, page, txn_id)` for an existing page by that `txn_id`, copy the current in-memory page into `before_images`.
- On repeated writes to the same page by the same `txn_id`, do not replace the original before-image.
- On `new_page(file, txn_id)`, record the allocated page in `new_pages`.
- On `rollback(txn_id)`, restore all before-images and invalidate/free all newly allocated pages for that transaction.
- On `commit(txn_id)`, discard before-images and new-page tracking. Pages remain dirty until snapshot checkpoint.

This preserves committed in-memory changes from earlier transactions that have not yet been snapshotted.

## Eviction

V1 uses clock eviction:

- Clean, unpinned pages may be evicted.
- Dirty pages are never evicted in V1 because `FlushPolicy::can_flush` always returns false.
- If all candidate frames are dirty or pinned, return a storage/buffer error.

Future physical WAL may allow dirty eviction through `FlushPolicy`.

## Snapshot Interaction

After `SnapshotManager::commit_snapshot` succeeds, checkpoint calls `mark_all_clean()`. This clears dirty flags and rollback metadata must already be empty because checkpoint holds the global write guard.

`iter_pages` must provide stable page data for snapshot writing. Checkpoint holds the write guard, so no statement mutates pages concurrently.

## Invariants

- No dirty page is evicted in v1.
- Rollback restores page state to exactly what it was before the failed transaction first touched each page.
- New pages from failed transactions are not visible after rollback.
- Commit does not flush pages; it only discards rollback metadata.
- Checkpoint is the only operation that marks committed dirty pages clean.

## Acceptance Tests

- First write stores a before-image; second write by same txn does not replace it.
- Rollback restores a page that was already dirty from a prior committed txn.
- Rollback invalidates pages allocated by the failed txn.
- Commit discards before-images but leaves pages dirty.
- Dirty pages are skipped by eviction.
- `mark_all_clean` makes previously dirty pages evictable.
- `iter_pages` returns dirty in-memory data for snapshot composition.
