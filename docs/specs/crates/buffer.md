# `buffer` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`buffer` manages in-memory page frames, page latches, dirty tracking, and in-place dirty-page flushing to a `PageStore`. Eviction can steal (flush, then evict) any WAL-durable dirty page once stealing is enabled, so the working set is not bound by the pool size during normal operation. Abort is status-based (Milestone D1, `mvcc.md` §4 Decision 3): the buffer pool does **no** page-content undo and no page reclamation on rollback — a rolled-back transaction's pages stay dirty-but-evictable, hidden by the CLOG (the before-image mechanism earlier milestones used is retired).

## Depends On

- `common`
- `parking_lot`

## Page Model

- Page size: 8192 bytes.
- Frames are addressed by `(FileId, PageNum)`.
- The buffer pool reads pages from the heap files through an injected `PageStore`.
- Dirty pages remain in memory until a checkpoint flushes them to the heap or an eviction steals them; rollback does **not** discard them (status-based abort — they stay dirty-but-evictable, hidden by the CLOG).

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

    /// Like `load_page`, but a page whose stored form fails validation (e.g. a
    /// torn compressed envelope, `docs/specs/compression.md` §5) is reported
    /// as absent instead of an error. Default impl delegates to `load_page`
    /// unchanged, so an implementation with no lenient distinction (nothing to
    /// validate beyond the raw bytes) needs no override.
    fn load_page_lenient(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
        self.load_page(file_id, page_num)
    }
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
    fn page_count(&self, file_id: FileId) -> Result<PageNum>;
    fn abandon_unpublished_new_page(&self, guard: PageWriteGuard) -> Result<()>;
    fn is_page_abandoned(&self, file_id: FileId, page_num: PageNum) -> bool;
    fn mark_all_clean(&self) -> Result<()>;
    fn rollback(&self, txn_id: u64) -> Result<()>;
    fn commit(&self, txn_id: u64) -> Result<()>;
    fn flush_dirty_pages(&self) -> Result<()>;
    fn fetch_for_redo(&self, file_id: FileId, page_num: PageNum) -> Result<PageWriteGuard>;
    fn enable_stealing(&self);
}

pub struct MemoryBufferPool { /* the concrete BufferPool implementation */ }
```

`flush_dirty_pages` writes every flushable dirty page (per `FlushPolicy`) to its home via the `PageStore`, regardless of whether its dirtying transaction committed (the CLOG hides the non-committed tuples). It does not fsync or mark frames clean; checkpoint calls it, then `PageStore::sync_all`, then `mark_all_clean`. `fetch_for_redo` returns a writable frame for recovery redo, loading a miss via `store.load_page_lenient` (not `load_page`) and inserting a zeroed frame both when the page is absent from the store (a new page being re-established) and when the stored page fails validation (e.g. a torn compressed envelope) — recovery redo is the ONE caller that uses the lenient form, since a torn stored page there is exactly like a torn raw page: it was dirty, so its first post-checkpoint modification logged a `FullPageImage` that redo will replay, making a zeroed-then-repaired frame sound (`docs/specs/compression.md` §5, `docs/specs/crates/storage.md`). It marks the frame dirty under the recovery txn id (`0`). Every other caller (`read_page`/`write_page`/normal `get_or_insert_clean` misses) uses the strict `load_page`, which surfaces the same failure loudly as page corruption. `enable_stealing` turns on eviction-flush-on-steal; it is off at construction and the server enables it during startup, before redo (the durable on-disk index means recovery rebuilds nothing in memory, so redo may spill).

`MemoryBufferPool::new(frame_count, flush_policy, store)` stores `Box<dyn FlushPolicy>` and `Arc<dyn PageStore>`. `read_page` first checks resident frames; on a miss it calls `store.load_page(file_id, page_num)`. `Some(data)` is inserted as a clean page and returned. `None` means the page does not exist and returns `ErrorKind::Storage` / `SqlState::InternalError` with a message of the form `page not found: file_id=…, page_num=…`. A loader error from `store.load_page` is propagated unchanged, so its `ErrorKind` is whatever the injected `PageStore` returns (a file-backed store yields `ErrorKind::Io`).

In production, the server supplies a `HeapPageStore` (a `PageStore`) backed by per-table heap files. The `buffer` crate defines only the traits and does not depend on `storage` or `control`.

`PageStore` extends `PageLoader` with `write_page`, `sync_all`, and `page_count` for in-place dirty-page flushing. `storage::HeapPageStore` implements it over one file per table — the heap at `<data>/heap/<file_id>.heap` and the storage identity index at `<data>/heap/<table>.idx` (index file ids carry a high bit) — page `n` at byte offset `n * PAGE_SIZE`, positioned I/O. `write_page` does not fsync; `sync_all` fsyncs all open files and the directory; `page_count` returns a file's on-disk extent in pages. `new_page` seeds its allocator from `page_count` the first time it allocates into a file, so after recovery (which no longer preloads pages) a new page never reuses one that already exists on disk.

`MemoryBufferPool::empty(frame_count)` is a test helper that uses a never-flush policy and a `NoopPageStore` returning `Ok(None)` from `load_page` and discarding writes.

`load_page(file_id, page_num, data)` inserts `data` as a clean frame if the page is not resident. If `(file_id, page_num)` is already resident, it must leave resident bytes, dirty state, and dirty transaction ID unchanged, then still advance `next_page_num_by_file` to at least `page_num + 1` and return `Ok(())`. It must not mark the page dirty. `iter_pages` returns pages currently known to the buffer pool (used by checkpoint flushing and the storage page scan). `page_count(file_id)` returns the file's **full extent** — `max(PageStore::page_count, next_page_num_by_file)` — i.e. every page `0..page_count` that has ever existed for the file, including pages currently evicted to disk (which `iter_pages` omits) and freshly allocated pages not yet flushed (which the on-disk extent omits). Abandoned unpublished tail pages are trimmed from `next_page_num_by_file`; abandoned interior holes may still sit below `page_count` and are reported by `is_page_abandoned`. A full-extent scan (VACUUM, `mvcc.md` §9) iterates `0..page_count`, skips abandoned holes, and faults each non-abandoned page in, so an evicted dead tuple is never missed.

`rollback(txn_id)` and `commit(txn_id)` are both no-op bookkeeping clears: under status-based abort (`mvcc.md` §4 Decision 3) the buffer pool tracks no per-transaction page state, undoes nothing, and reclaims nothing. A rolled-back transaction's pages — both ones it modified and ones it freshly allocated (`new_page`) — stay resident as dirty-but-evictable frames; the CLOG hides their tuples and VACUUM (Milestone F) reclaims them. They must not be I/O and should not fail for a valid `txn_id`; if `commit` fails after a durable WAL commit, the server treats that as fatal and does not roll back.

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
    pub fn take_needs_fpi(&self) -> bool;  // true once if this write must log a full-page image
    pub fn restore_needs_fpi(&self);       // re-arm only after a failed first-touch WAL attempt
}
```

Read guards hold a read latch and unpin on drop. Write guards hold a write latch, set dirty state for `txn_id`, and unpin on drop.

`new_page(file_id, txn_id)` allocates the next unused page number for that file and returns a `PageWriteGuard` whose `page_num()` identifies the new page. That guard is tagged as an unpublished fresh allocation until its mutable bytes are exposed via `data_mut`; normal `write_page`/redo guards are not abandonable. The fresh-page insertion path must reject an already resident `(file_id, page_num)` with an internal error rather than overwriting it. The pool tracks `next_page_num_by_file`; `load_page(file_id, page_num, ...)` advances this counter to at least `page_num + 1`. On normal execution and rollback, the allocation counter only advances: rollback does **not** reclaim a transaction's freshly allocated pages or reset the counter (status-based abort, Milestone D1). Those pages stay resident (their tuples invisible via the CLOG) and are replayed by redo-all recovery, so reusing their numbers would diverge from the recovered state and dangle the index entries that point at them. Under the monotonic table/index id allocator, a file id freed by a rolled-back create is never reused, so not resetting the counter is harmless. The allocator is seeded from the on-disk extent on first allocation into a file (`page_count`), so a freshly allocated page never reuses a checkpointed-but-not-replayed on-disk page; under concurrent writers (E2b) the extent read **and** the seed run under one continuous hold of the pool lock so the counter cannot be seeded below the true extent by an interleaving allocation.

If a caller allocates a fresh page and the page's first redo append fails before
any bytes are published into the frame, it must call
`abandon_unpublished_new_page(guard)` while it still owns that unpublished guard.
This is not rollback; it is the cancellation of an allocation that has no redo
record and no page bytes. The pool refuses to abandon any guard not returned by
`new_page` and refuses after `data_mut` has exposed mutable bytes. On success it
consumes the guard, removes the resident frame, rolls back the file high-water
when the page is at the tail, and tracks any interior abandoned page number for
reuse before the file grows again. `is_page_abandoned` is exposed for full-extent
maintenance scans so they can skip an abandoned interior hole instead of treating
it as a missing durable page. Once any page bytes have been published, or once a
WAL record names the page, abandonment must not be used.

`new_page`'s page-number allocation, `next_page_num`/`advance_next_page_num`, and the page-table insert all run under the pool lock, so two concurrent `new_page` for the same file always receive distinct page numbers.

Guards are owned and object-safe. They may internally hold `Arc<Frame>`.

## Frame Descriptor

Each frame tracks:

- `file_id`
- `page_num`
- `pin_count`
- `dirty`
- `dirty_txn_id`
- `reference_bit`
- `needs_fpi` (true when the next modification must log a full-page image: set on load and after `mark_clean`, cleared by `PageWriteGuard::take_needs_fpi`; false for freshly allocated pages, whose `HeapInit` is their own redo base. If a caller takes this flag and then fails before publishing any page bytes — including scratch/preflight failure or failure to append the required first-touch WAL record — it may call `PageWriteGuard::restore_needs_fpi` while still holding the same write guard so the next successful modification still logs the required FPI.)
- `evicting` (set under the pool lock when a steal reserves this dirty frame for an out-of-lock flush+evict; while set, no accessor hands the frame out — see Eviction, Milestone E2b)
- latch state

`dirty_txn_id` is the last transaction that modified the page (informational; the relaxed flush policy no longer gates on it).

## Abort is status-based (no rollback tracking)

The buffer pool keeps **no** per-transaction rollback state. Abort is status-based (Milestone D1, `mvcc.md` §4 Decision 3, §8): there is no before-image undo and no page reclamation. The before-image mechanism (`record_before_image`, the `BeforeImage` store, `restore_dirty_state`) and the new-page/allocation-counter rollback tracking that earlier milestones used are retired.

Rules:

- `write_page`/`new_page` mark the frame dirty under `txn_id` and track nothing extra.
- `rollback(txn_id)` and `commit(txn_id)` do nothing to pages: a rolled-back (or committed) transaction's pages stay resident and dirty until a checkpoint flushes them or an eviction steals them. A rolled-back transaction's tuples are hidden by the CLOG (`CLOG[txn] = Aborted`) and reclaimed by VACUUM (Milestone F); keeping them resident matches what redo-all recovery replays. No pins are leaked — the statement's `PageWriteGuard`s are dropped (unpinning their frames) before rollback/commit runs.

This preserves committed in-memory changes from earlier transactions that have not yet been flushed.

## Eviction

The buffer pool uses clock eviction:

- Clean, unpinned pages may be evicted (re-read from the heap on demand).
- When stealing is enabled (`enable_stealing`), a dirty unpinned page that the `FlushPolicy` admits is *stolen*: under the pool lock the victim is reserved (`pin_count == 0` is required, then the frame is marked **`evicting`** and pinned), and the flush write happens **outside** the pool lock. While `evicting` is set, every resident-page lookup (`read_page`/`write_page`/`get_or_insert_clean`) treats the frame as in-transition and retries (after the steal removes it, the page reloads from the store with the flushed bytes), so **no concurrent writer can modify the frame whose bytes the steal is flushing**. This is load-bearing under concurrent writers (E2b): without it a steal could flush a stale snapshot of a frame a writer was concurrently modifying and then drop the frame, silently losing the write — a race the single global writer lock previously masked. The steal first calls `FlushPolicy::ensure_durable` (forces the WAL) so the page's records are durable before the page is, then flushes it to its heap home; on re-lock it evicts the frame (`pin_count == 0`, which holds because `evicting` blocked any new pin). On a flush error it clears `evicting` and releases the reservation. With the relaxed flush gate (Milestone D1), the stolen page need not be committed — `ensure_durable` provides the write-ahead guarantee the pre-D1 committed-only steal got for free (a committed page's WAL was already flushed through its `Commit`).
- A dirty page the policy refuses (for example, its page-LSN is not yet WAL-durable), or any pinned page, is skipped.
- If no frame can be freed (all pinned, or all dirty and unflushable), return a storage/buffer error.

Stealing is off until `enable_stealing`. The server enables it during startup before redo; with the durable on-disk index there is no in-memory directory to rebuild, so recovery may spill and its working set is not bounded by the pool size.

## Checkpoint Interaction

Checkpoint holds the **exclusive** checkpoint guard (E2b lock inversion, `common.md`), so it drains all in-flight writers and runs alone — no statement mutates pages concurrently with the checkpoint body (the same "no in-flight writer at checkpoint" guarantee as the pre-E2b single exclusive writer). It calls `flush_dirty_pages` (writes flushable dirty pages to the `PageStore`), then `PageStore::sync_all`, then `mark_all_clean` (clears dirty flags and re-arms `needs_fpi`). After `wal.flush()` (which checkpoint runs first) every dirty page is WAL-durable, so `FlushPolicy` admits them all — committed, aborted, and (under Stage 2) in-flight alike.

## Invariants

- A page is never written to the heap before its redo records are WAL-durable: checkpoint flushes after `wal.flush()`, and eviction-steal forces the WAL (`ensure_durable`) before writing a stolen page.
- Under concurrent writers (E2b), a frame a steal is flushing is marked `evicting` and is never handed out for read or write, so a steal can never flush a stale snapshot and then drop a write made concurrently to that frame. Page-number allocation and the extent seed are pool-lock-atomic, so concurrent `new_page` never collide or seed below the true on-disk extent.
- Uncommitted/aborted dirty pages MAY be written to the heap (relaxed flush gate, Milestone D1); they are hidden by the CLOG and reclaimed by VACUUM. The flush gate's only requirement is WAL-durability.
- Rollback does no page undo and reclaims no pages: a rolled-back transaction's pages (modified or freshly allocated) stay resident as dirty-but-evictable frames.
- During normal execution and rollback, the allocation counter only advances; rolled-back page numbers are not reused. The only exception is cancellation of an unpublished fresh allocation via `abandon_unpublished_new_page`.
- Commit does not flush pages; pages stay dirty until a checkpoint flushes them in place to the heap.
- A dirty page is marked clean only when it is flushed to the heap — in place by checkpoint, or immediately before eviction by a steal. The narrow unpublished-fresh-page failure path removes the frame with `abandon_unpublished_new_page` instead of marking it clean.

## Acceptance Tests

- Rollback does NOT undo an in-place modification: the modified bytes remain and the page stays dirty (status-based abort).
- Rollback keeps a freshly allocated page resident and dirty, and does not reuse its page number.
- Commit leaves pages dirty (it discards no rollback metadata — there is none).
- A WAL-durable dirty page is stolen (flushed then evicted) when stealing is enabled, including one dirtied by an aborted transaction.
- A dirty page the policy refuses (not WAL-durable), or any dirty page when stealing is disabled, is not evicted (error when no other victim).
- A working set larger than the pool spills to the heap and reads back correctly.
- `mark_all_clean` makes previously dirty pages evictable.
- `iter_pages` returns in-memory page data for checkpoint flushing and the storage page scan.
- `fetch_for_redo` loads a miss through `load_page_lenient`, not `load_page`; a `PageLoader` with no override falls back to `load_page` unchanged (the default impl).
