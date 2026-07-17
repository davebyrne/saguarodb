# `buffer` Crate Specification

**Date:** 2026-07-17
**Status:** Living crate contract

## Purpose

`buffer` owns 8 KiB page frames, page latches, dirty-page metadata, steal
eviction, the checkpoint publication fence, and incremental checkpoint batches.
Abort is status-based: rollback neither undoes page bytes nor reclaims published
pages; CLOG visibility hides aborted versions until VACUUM.

## Page and Store APIs

Frames are addressed by `(FileId, PageNum)`. `PageLoader` provides strict normal
loads and a recovery-only lenient load for a torn stored page. `PageStore`
provides positioned writes, whole-store or bounded-file fsync, extent queries,
file removal, and file enumeration. The buffer crate does not depend on storage.

`BufferPool` provides page read/write/new guards, full-extent `page_count`,
recovery fetch, unpublished-page abandonment, relation-scoped flush/clean,
steal enablement, retired-file discard, and these checkpoint APIs:

```rust
fn checkpoint_fence(&self) -> CheckpointFence;
fn checkpoint_dirty_keys(&self) -> Result<Vec<(FileId, PageNum)>>;
fn dirty_page_table(&self) -> Result<Vec<DirtyPageEntry>>;
fn flush_checkpoint_batch(
    &self,
    candidates: &[(FileId, PageNum)],
) -> Result<CheckpointBatchStats>;
```

`page_count` is `max(on-disk extent, in-memory allocation high-water)`, including
evicted pages. Unpublished abandoned holes are reusable and full-extent scans
skip them. Published allocation numbers are monotonic, including after abort.

## Page Publication

Acquiring a `PageWriteGuard` or calling `data_mut` does not establish a durable
dirty record. A successful mutation must append WAL with `append_positioned`,
apply the bytes, stamp the page with `position.record_lsn`, then call
`guard.publish_position(position)` before releasing the guard. Failed first-touch
work restores `needs_fpi`; unpublished new pages may be abandoned.

Every write guard holds the shared side of the buffer-owned `CheckpointFence`
for its lifetime. The checkpoint takes the exclusive side only while capturing
the final in-memory metadata. Sequence append/publication uses the same shared
side through storage.

Each frame tracks:

```text
dirty, dirty_txn_id
rec_lsn                 first WAL record-start boundary not on durable page storage
latest_page_lsn         latest stored end-LSN published into the page
generation              increments for every published mutation
checkpoint_flush:
    captured_generation
    captured_page_lsn
    first_redirty_lsn
needs_fpi
checkpoint_flushing
```

The first publication while clean sets `rec_lsn = position.replay_from`.
Subsequent publications preserve the earliest boundary. Publication during a
reserved checkpoint flush additionally retains the earliest post-capture
boundary. Fresh pages begin with `needs_fpi = false` because `HeapInit` is their
redo base; loaded and durably flushed pages require an FPI on next modification.
Recovery publishes the replayed position through the same metadata path.

## Incremental Checkpoint Flush

The server snapshots dirty keys once and passes bounded slices. A batch never
waits for a busy page latch. It skips absent, clean, evicting, already reserved,
or currently latched frames and reports attempted/flushed/skipped/redirtied
counts.

For each acquired candidate, the buffer reserves it against eviction and file
discard, captures bytes, generation, PageLSN, and `rec_lsn`, and re-arms
`needs_fpi` before releasing the latch. It makes WAL durable through the captured
PageLSN, writes the copy, and fsyncs every represented file. After fsync:

- unchanged generation with no redirty becomes clean and clears redo metadata;
- a redirtied frame stays dirty with `rec_lsn = first_redirty_lsn`;
- a missing frame is safe only through the represented file fsync.

Every success and failure path clears reservations. File discard and relation
generation cleanup return busy while any frame is pinned, evicting, or reserved
by a checkpoint.

## Eviction and Other Flushes

Steal eviction reserves an unpinned WAL-admissible dirty frame, forces WAL,
writes and fsyncs its containing file, then removes the frame. Accessors retry
while `evicting`, so an evicted snapshot cannot lose a concurrent mutation.
Stealing is disabled until server startup enables it. Relation-rewrite helpers
may flush/fsync/mark clean a bounded file set under their relation locks; there
is no global clean-every-frame operation.

## Invariants

- Page storage never precedes the WAL that describes the captured PageLSN.
- Every production page mutation publishes an exact `WalPosition`.
- A conditional-clean decision cannot clear a concurrent redirty.
- Newly dirty pages are not added to a running checkpoint pass.
- Checkpoint I/O holds neither a transaction-long guard nor the exclusive
  publication fence.
- Uncommitted and aborted versions may reach page storage, but CLOG continues to
  determine visibility.
