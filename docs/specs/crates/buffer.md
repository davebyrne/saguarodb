# `buffer` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`buffer` manages in-memory page frames, page latches, dirty tracking, statement rollback, and in-place dirty-page flushing to a `PageStore`. Eviction can steal (flush, then evict) committed dirty pages once stealing is enabled, so the committed working set is not bound by the pool size during normal operation.

## Depends On

- `common`
- `parking_lot`

## Page Model

- Page size: 8192 bytes.
- Frames are addressed by `(FileId, PageNum)`.
- The buffer pool reads pages from the heap files through an injected `PageStore`.
- Dirty pages remain in memory until a checkpoint flushes them to the heap, an eviction steals them, or rollback discards them.

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

pub trait PageStore: PageLoader {
    fn write_page(&self, file_id: FileId, page_num: PageNum, data: &PageData) -> Result<()>;
    fn sync_all(&self) -> Result<()>;
    fn page_count(&self, file_id: FileId) -> Result<PageNum>;
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
    fn flush_committed_pages(&self) -> Result<()>;
    fn fetch_for_redo(&self, file_id: FileId, page_num: PageNum) -> Result<PageWriteGuard>;
    fn enable_stealing(&self);
}
```

`flush_committed_pages` writes every flushable dirty page (per `FlushPolicy`) to its home via the `PageStore`. It does not fsync or mark frames clean; checkpoint calls it, then `PageStore::sync_all`, then `mark_all_clean`. `fetch_for_redo` returns a writable frame for recovery redo, inserting a zeroed frame when the page is absent from the store (a new page being re-established); it marks the frame dirty under the recovery txn id (`0`). `enable_stealing` turns on eviction-flush-on-steal; it is off at construction and the server enables it during startup, before redo (the durable on-disk index means recovery rebuilds nothing in memory, so redo may spill).

`MemoryBufferPool::new(frame_count, flush_policy, store)` stores `Box<dyn FlushPolicy>` and `Arc<dyn PageStore>`. `read_page` first checks resident frames; on a miss it calls `store.load_page(file_id, page_num)`. `Some(data)` is inserted as a clean page and returned. `None` means the page does not exist and returns `ErrorKind::Storage` / `SqlState::InternalError` with message `page not found`. Loader I/O errors propagate as `ErrorKind::Io`.

In production, the server supplies a `HeapPageStore` (a `PageStore`) backed by per-table heap files. The `buffer` crate defines only the traits and does not depend on `storage` or `control`.

`PageStore` extends `PageLoader` with `write_page`, `sync_all`, and `page_count` for in-place dirty-page flushing. `storage::HeapPageStore` implements it over one file per table — the heap at `<data>/heap/<file_id>.heap` and the primary-key index at `<data>/heap/<table>.idx` (index file ids carry a high bit) — page `n` at byte offset `n * PAGE_SIZE`, positioned I/O. `write_page` does not fsync; `sync_all` fsyncs all open files and the directory; `page_count` returns a file's on-disk extent in pages. `new_page` seeds its allocator from `page_count` the first time it allocates into a file, so after recovery (which no longer preloads pages) a new page never reuses one that already exists on disk.

`MemoryBufferPool::empty(frame_count)` is a test helper that uses a never-flush policy and a `NoopPageStore` returning `Ok(None)` from `load_page` and discarding writes.

`load_page(file_id, page_num, data)` inserts `data` as a clean frame if the page is not resident. If `(file_id, page_num)` is already resident, it must leave resident bytes, dirty state, dirty transaction ID, and rollback metadata unchanged, then still advance `next_page_num_by_file` to at least `page_num + 1` and return `Ok(())`. It must not mark the page dirty or create rollback metadata. `iter_pages` returns pages currently known to the buffer pool (used by checkpoint flushing and the storage page scan).

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

`new_page(file_id, txn_id)` allocates the next unused page number for that file and returns a `PageWriteGuard` whose `page_num()` identifies the new page. The fresh-page insertion path must reject an already resident `(file_id, page_num)` with an internal error rather than overwriting it. The pool tracks `next_page_num_by_file`; `load_page(file_id, page_num, ...)` advances this counter to at least `page_num + 1`. Rollback removes a transaction's new pages and restores each affected file's allocation counter to its pre-transaction value, so the rolled-back page numbers are reusable — this is required because a rebuilt B-tree in a reused file (an index id freed by a rolled-back create) must place its metapage at page 0. Restoring the counter is safe under the v1 single-writer model, where no other transaction raised it; the rolled-back pages were never flushed, so they do not exist on disk.

Guards are owned and object-safe. They may internally hold `Arc<Frame>`.

## Frame Descriptor

Each frame tracks:

- `file_id`
- `page_num`
- `pin_count`
- `dirty`
- `dirty_txn_id`
- `reference_bit`
- `needs_fpi` (true when the next modification must log a full-page image: set on load and after `mark_clean`/rollback-restore, cleared by `PageWriteGuard::take_needs_fpi`; false for freshly allocated pages, whose `HeapInit` is their own redo base)
- latch state

`dirty_txn_id` is the last transaction that modified the page. It is not enough for rollback by itself; before-images and new-page tracking handle rollback.

## Rollback Tracking

The buffer pool tracks active write transactions:

```rust
pub struct TxnDirtyState {
    pub before_images: HashMap<(FileId, PageNum), PageData>,
    pub new_pages: Vec<(FileId, PageNum)>,
    pub next_page_before: HashMap<FileId, PageNum>,
}
```

Rules:

- On first `write_page(file, page, txn_id)` for an existing page by that `txn_id`, copy the current in-memory page into `before_images`.
- On repeated writes to the same page by the same `txn_id`, do not replace the original before-image.
- On `new_page(file, txn_id)`, record the allocated page in `new_pages`, and the first time the txn allocates into a file record that file's pre-allocation counter in `next_page_before`.
- On `rollback(txn_id)`, restore all before-images, invalidate/free all newly allocated pages for that transaction, and restore each affected file's allocation counter from `next_page_before`.
- On `commit(txn_id)`, discard before-images and new-page tracking. Pages remain dirty until a checkpoint flushes them in place to the heap.

This preserves committed in-memory changes from earlier transactions that have not yet been flushed.

## Eviction

V1 uses clock eviction:

- Clean, unpinned pages may be evicted (re-read from the heap on demand).
- When stealing is enabled (`enable_stealing`), a committed dirty unpinned page that the `FlushPolicy` admits is *stolen*: flushed to its heap home, then evicted. The flush write happens outside the pool lock — the victim is pinned as a reservation, written, then evicted on re-lock if no reader pinned it meanwhile (otherwise another victim is tried). This removes the in-RAM working-set ceiling during normal operation. Because a committed page's WAL is already flushed through its commit record, a committed page is always WAL-durable, so the policy needs only the committed check.
- A dirty page the policy refuses (uncommitted), or any pinned page, is skipped.
- If no frame can be freed (all pinned, or all dirty and unflushable), return a storage/buffer error.

Stealing is off until `enable_stealing`. The server enables it during startup before redo; with the durable on-disk index there is no in-memory directory to rebuild, so recovery may spill and its working set is not bounded by the pool size.

## Checkpoint Interaction

Checkpoint holds the global write guard, so no statement mutates pages concurrently. It calls `flush_committed_pages` (writes flushable dirty pages to the `PageStore`), then `PageStore::sync_all`, then `mark_all_clean` (clears dirty flags and re-arms `needs_fpi`). At quiesce every dirty page is committed, so `FlushPolicy` admits them all.

## Invariants

- Uncommitted dirty pages are never written to the heap; only committed (hence WAL-durable) dirty pages are stolen.
- A page is never written to the heap before its redo records are WAL-durable: checkpoint flushes after `wal.flush()`, and eviction-steal only flushes committed pages.
- Rollback restores page state to exactly what it was before the failed transaction first touched each page, and re-arms `needs_fpi`.
- New pages from failed transactions are not visible after rollback, and their page numbers become re-allocatable (the allocation counter is restored).
- Commit does not flush pages; it only discards rollback metadata.
- A committed dirty page is marked clean only when it is flushed to the heap — in place by checkpoint, or immediately before eviction by a steal.

## Acceptance Tests

- First write stores a before-image; second write by same txn does not replace it.
- Rollback restores a page that was already dirty from a prior committed txn.
- Rollback invalidates pages allocated by the failed txn and resets the allocation counter so the next transaction reuses those page numbers.
- Commit discards before-images but leaves pages dirty.
- A committed dirty page is stolen (flushed then evicted) when stealing is enabled.
- A dirty page the policy refuses, or any dirty page when stealing is disabled, is not evicted (error when no other victim).
- A committed working set larger than the pool spills to the heap and reads back correctly.
- `mark_all_clean` makes previously dirty pages evictable.
- `iter_pages` returns in-memory page data for checkpoint flushing and the storage page scan.
