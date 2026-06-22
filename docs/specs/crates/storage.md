# `storage` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`storage` owns table files, row serialization, page-backed row storage, the durable on-disk primary-key and secondary B-tree indexes, normal data operations, schema file operations, and recovery apply operations.

## Depends On

- `common`
- `buffer`
- `wal`

`storage` must not depend on `planner`.

## Public Traits

```rust
pub trait RowIterator: Send {
    fn next(&mut self) -> Result<Option<StoredRow>>;
    fn schema(&self) -> &[ColumnInfo];
}

pub trait StorageEngine: Send + Sync {
    fn insert(&self, ctx: &StatementContext, table: TableId, row: Row) -> Result<RowId>;
    fn get(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<Option<Row>>;
    fn delete(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<bool>;
    fn update(&self, ctx: &StatementContext, table: TableId, key: &Key, row: Row) -> Result<bool>;
    fn scan(&self, ctx: &StatementContext, table: TableId) -> Result<Box<dyn RowIterator>>;
    fn scan_range(&self, ctx: &StatementContext, table: TableId, range: &KeyRange) -> Result<Box<dyn RowIterator>>;
    fn index_scan(&self, ctx: &StatementContext, table: TableId, index: IndexId, range: &KeyRange) -> Result<Box<dyn RowIterator>>;
    fn rollback_txn(&self, txn_id: u64) -> Result<()>;
    fn commit_txn(&self, txn_id: u64) -> Result<()>;
}

pub trait SchemaOperations: Send + Sync {
    fn create_table(&self, ctx: &StatementContext, schema: &TableSchema) -> Result<()>;
    fn drop_table(&self, ctx: &StatementContext, table: TableId) -> Result<()>;
    fn create_index(&self, ctx: &StatementContext, schema: &IndexSchema) -> Result<()>;
    fn drop_index(&self, ctx: &StatementContext, index: IndexId) -> Result<()>;
}

pub trait RecoveryOperations: Send + Sync {
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, table: TableId) -> Result<()>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, index: IndexId) -> Result<()>;
}
```

`RecoveryOperations` carries only DDL replay; row-level recovery is physiological page redo via `apply_physical_redo` (see Heap Page Store), not the storage `StorageEngine` methods. Normal methods append WAL records. `rollback_txn` restores storage-owned table metadata; index and heap page bytes (including B-tree splits) are restored by `BufferPool::rollback` via its before-images and new-page tracking. `commit_txn` discards storage rollback metadata after WAL flush succeeds. `commit_txn` is cleanup-only, must not perform I/O, and should not fail for a valid `txn_id`. `RecoveryOperations` must not append WAL records.

## Table Storage

Each table is page-backed. Full rows live in heap pages; a durable, non-clustered B-tree maps each primary key to its `RowLocation`, stored in a separate index file per table (see Primary-Key Index). The clustered on-disk B-tree (rows in the leaves, no separate heap) remains future work behind the existing storage traits.

- `insert` inserts by primary key (heap row plus B-tree entry) and adds an entry to every secondary index on the table.
- `get` does a primary-key lookup through the B-tree.
- `scan` / `scan_range` walk the primary-key B-tree leaves in key order and read rows from their heap locations.
- `index_scan` walks a secondary index, which points directly at heap TIDs, and reads each row from its heap location (no primary-key indirection; see Secondary Indexes).
- `delete` marks the visible version deleted in place (MVCC delete; see below) and
  retains its index entries. `update` writes a new heap version, chains the old
  version forward to it (`xmax` + `t_ctid`), and inserts a per-version entry into
  every index (MVCC update; see below), retaining all old entries.

### Snapshot visibility on reads

Every heap row materialized for a user-facing read — `get` (point lookup),
`scan_range` (sequential scan), and `index_scan` (index → heap) — is filtered
through the MVCC visibility predicate (`common::is_visible`, `docs/specs/mvcc.md`
§6) before it is returned. The engine decodes each candidate tuple's
`xmin`/`xmax`/`infomask`, evaluates it against the statement's `Snapshot`
(`ctx.snapshot`), the current transaction (`ctx.txn_id`), and the CLOG-backed
`TxnStatusView` (`PageBackedStorageEngine::txn_status_view`); an **invisible
version is skipped, not returned**. Under single-writer autocommit the captured
snapshot is degenerate ("sees all committed" — empty `xip`, `xmax` past every
allocated id), so every committed row and own write stays visible and read
results are unchanged from the pre-MVCC engine.

- **Index → heap is skip, not error.** An index entry that resolves to a tuple
  invisible to the snapshot (or whose line pointer is `DEAD`/absent) is skipped
  rather than raising an internal error. This is the forward-looking contract for
  per-version index entries that VACUUM has not yet reclaimed (Milestone B4/F).
- **No `t_ctid` walk on reads.** With one index entry per version, a scan collects
  every candidate TID from the index and visibility-checks each at the heap; the
  forward `t_ctid` chain is not followed for `SELECT` (it serves update-locating
  and conflict detection in later milestones).
- **Index backfill is unfiltered; DML locates the visible version.** `create_index`
  backfill reads the *current physical* tuple (not the snapshot-visible version) to
  recompute index keys, so it uses the unfiltered heap read. `delete` and `update`
  instead locate the *visible* version (the row the executor matched) via the
  visibility predicate (`locate_visible_version`); `delete` stamps its `xmax` in
  place, `update` stamps it and chains it to the new version. Neither removes an
  index entry.

## Page Format

Page header (22 bytes, version 2):

```text
PageID:      4 bytes
PageType:    1 byte
PageVersion: 1 byte
NumSlots:    2 bytes
FreeSpace:   2 bytes
PageLSN:     8 bytes
Checksum:    4 bytes
```

`PageVersion` is `2`; unknown versions (including the legacy v1 value `1`) are rejected as page corruption. The whole header is covered by `Checksum` (the checksum field itself excepted), so `PageLSN` is checksummed.

`PageLSN` is the LSN of the WAL record that last modified the page. It is stamped on every mutation by `page::set_page_lsn`. It is the basis for PageLSN-gated redo replay and for deciding when a dirty page is safe to flush (see `wal.md` and `buffer.md`).

V1 development builds do not migrate older page formats. Existing page files without `PageVersion = 2` are rejected as corrupt during load/recovery.

`PageType` is `1` for a heap data page and `2` for a B-tree index node. `validate`/`is_valid` accept both (the data-page slot-layout check runs only for type `1`); the index node body layout is described under Primary-Key Index.

Page body (data page):

- Slot array grows down from the top.
- Row bytes grow up from the bottom.
- Delete marks slots dead.
- Intra-page compaction (`page::prune_and_compact`, Milestone F) relocates the
  surviving live tuples down to the bottom, rewriting their line pointers'
  `offset` and recomputing `FreeStart`; it is driven by VACUUM, not by inserts.

### Line Pointers (heap slot array)

A heap slot is a 6-byte `[offset: u16][len: u16][flags: u16]` **line pointer
(ItemId)** whose `flags` field is one of four states (`mvcc.md` §5.2):

- `NORMAL` (`2`) — `(offset, len)` address a live tuple on this page.
- `DEAD` (`1`) — the tuple was removed but the line pointer is retained because
  index entries may still reference it; reclaimed to `UNUSED` only after index
  vacuum.
- `UNUSED` (`0`) — free for reuse. Produced by `page::reclaim_line_pointers`
  (VACUUM, Milestone F3b); `insert_row` recycles the lowest `UNUSED` slot id before
  appending a fresh one, bounding the slot array under delete→vacuum→insert churn.
  It reuses **`UNUSED` only, never `DEAD`** (see `insert_row` below) — an `UNUSED`
  slot is guaranteed by the F2b → F3a → F3b ordering to have no dangling index
  entry, while a `DEAD` slot may still have one.
- `REDIRECT` (`3`) — points at another slot on the same page. *Reserved for HOT
  (Milestone H); no path produces it yet.*

The numeric values preserve the pre-MVCC encoding, so `NORMAL` is exactly the
former "live" slot and `DEAD` is the former tombstoned slot. Neither MVCC `delete`
nor MVCC `update` tombstones any more — both keep the superseded version on a
still-`NORMAL` line pointer and hide it by visibility (see MVCC Delete / MVCC
Update below), so dead tuples linger physically until VACUUM (Milestone F), the
only producer of `DEAD` (via `page::prune_and_compact`, F2b) and `UNUSED` (via
`page::reclaim_line_pointers`, F3b). The VACUUM passes have no production caller
until the orchestration (F4a), so at this milestone no runtime path yet stamps a
heap slot `DEAD`/`UNUSED`. `validate` accepts `NORMAL`, `DEAD`, and `UNUSED` flags on a data page (so a
VACUUM-compacted page is valid); `REDIRECT` and any other value is corruption.
The `(offset, offset+len) ≤ FreeStart`/in-bounds invariant is enforced **only for
`NORMAL` line pointers** — after compaction a `DEAD`/`UNUSED` slot's
`(offset, len)` no longer names live bytes and is left unconstrained, while a
genuinely corrupt `NORMAL` slot (overlap, out of bounds, end past `FreeStart`)
still fails validation.

**Stable `(page, slot)` contract.** An index entry references a
`(page, line-pointer-slot)`. The tuple bytes a line pointer names may later be
relocated *within the page* (intra-page compaction, Milestone F) by rewriting the
line pointer's `(offset, len)` — the slot id is stable across that relocation and
no index is touched. `RowId`/`RowLocation` already encode `(page_num, slot_num)`
and are unchanged; they remain valid across intra-page compaction.

### In-Place Tuple-Header Mutation

`page::set_tuple_header(data, slot_num, xmax, t_ctid, infomask, lsn)` overwrites
the `xmax`, `t_ctid`, and `infomask` fields of the v2 tuple at a `NORMAL` slot
**in place**, stamps the page-LSN with `lsn`, and refreshes the checksum (exactly
like `insert_row`/`delete_row`). These are fixed-width header fields, so the
tuple keeps its exact offset and length — nothing is relocated and the page is
not compacted. The header offsets live solely in
`codec::set_mvcc_header_fields`, which `set_tuple_header` calls on the slot's
byte range, so `page.rs` never duplicates the header layout. A non-live
(`DEAD`/`UNUSED`/out-of-bounds) slot or a non-v2 tuple is a misuse and returns a
structured `DbError` rather than panicking. This is the substrate for `UPDATE`
/`DELETE` version stamping (Milestone B commits 8–9). Both MVCC `delete` (with
`t_ctid = INVALID_TID`) and MVCC `update` (with `t_ctid = new_tid`, the forward
chain pointer) emit it under the WAL (`HeapUpdateHeader`; see MVCC Delete / MVCC
Update below).

### Intra-Page Compaction and Line-Pointer Reclaim (VACUUM primitives)

`page::prune_and_compact(data, dead_slots, lsn)` is the intra-page heap-prune
primitive (`mvcc.md` §9 / Milestone F2). `dead_slots` are line pointers the caller
(F2b) has already classified as dead-to-everyone via `common::is_dead_to_all`;
this primitive does **not** classify — it only rewrites the page. In one pass it:

- Flips each `dead_slot` `NORMAL → DEAD`, **retaining** the slot id (index entries
  may still reference it; reclaiming to `UNUSED` is the separate step below). A
  `dead_slot` that is not currently a live `NORMAL` line pointer (already
  `DEAD`/`UNUSED`, or out of bounds) is a misuse and returns a structured
  `DbError`.
- Compacts the surviving `NORMAL` tuples so their bytes are contiguous from
  `HeaderLen` upward — reclaiming the freed bytes of the now-`DEAD` slots and any
  prior gaps — and **rewrites each survivor's line-pointer `offset`** to its new
  location. The slot-id array order and ids are stable and every survivor's `len`
  is unchanged, so `read_row(data, slot)` returns the identical bytes for the same
  slot id after compaction. `FreeStart` is recomputed for the compacted layout.
  Survivors are copied through a scratch buffer before being written back, so
  overlapping source/destination ranges never corrupt a tuple.
- Stamps the page-LSN with `lsn` and refreshes the checksum via `set_page_lsn`
  (exactly like `set_tuple_header`), so the checksum covers the compacted bytes,
  then revalidates the result.

`page::reclaim_line_pointers(data, slots, lsn)` flips each listed line pointer
`DEAD → UNUSED`, making its slot id reusable (`mvcc.md` §9 / Milestone F3b). Each
slot must currently be `DEAD`; a non-`DEAD` slot (still `NORMAL`/already `UNUSED`,
or out of bounds) is a misuse and returns a structured `DbError` — the cheap guard
against reclaiming a never-pruned slot. It stamps the page-LSN with `lsn` and
refreshes the checksum via `set_page_lsn`.

`insert_row` recycles the **lowest `UNUSED`** slot id before appending a fresh one
(F3b): it scans the slot array, and if a slot is `UNUSED` rewrites it to
`(new_offset, len, NORMAL)`, otherwise appends at `num_slots` (the historical
behavior). This bounds the slot array under delete→vacuum→insert churn. It reuses
**`UNUSED` only, never `DEAD`** — a `DEAD` slot may still have a dangling index
entry (index vacuum has not run for it), so reusing it would let a stale entry
resolve to the new tuple (silent corruption); an `UNUSED` slot is guaranteed by
the F2b → F3a → F3b ordering to have no index entry, the safety hinge for reuse.
The scan is O(slots-on-page) per insert (a free-slot map is a deferred
optimization). With no `UNUSED` slot the append path runs exactly as before, so
existing insert behavior is unchanged until VACUUM produces an `UNUSED` slot. The
`log_insert` path logs the `HeapInsert` for the slot id `insert_row` actually
produced (the reused or appended one), so its redo — which re-runs `insert_row` —
reproduces the same slot id under LSN-ordered replay.

`prune_and_compact` is consumed by the heap-prune VACUUM pass (`vacuum_heap`,
F2b, below); `page::reclaim_line_pointers` is consumed by the engine's
`reclaim_line_pointers` pass (F3b, below), which has no production caller until
the VACUUM orchestration (F4a).

### Heap-Prune VACUUM Pass (`vacuum_heap`, F2b)

`vacuum_heap(schema, horizon) -> Vec<RowLocation>` is the engine heap-prune pass
(`mvcc.md` §9 / Milestone F2b). It physically reclaims the tuples that are
dead-to-everyone at `horizon` from every heap page of `schema`'s table and returns
their TIDs. It has no production caller yet (VACUUM orchestration is F4a), so it is
a runtime no-op at this milestone.

- **Classify.** For each `NORMAL` slot on a page it decodes the tuple's MVCC header
  (`codec::decode_mvcc_header` ⇒ `xmin`/`xmax`/`infomask`) and calls
  `common::is_dead_to_all(xmin, xmax, infomask, horizon, txn_status_view())` against
  the live CLOG. Only dead-to-all slots are pruned: a live version (`xmax ==
  INVALID_XID`), an in-flight or aborted deleter, and a committed delete at or above
  the horizon are all left `NORMAL` and untouched (the predicate's
  aborted-creator-any-age / committed-delete-strictly-below-horizon asymmetry).
- **Prune + log.** A page with at least one dead slot is rewritten by
  `page::prune_and_compact` (survivors stay byte-identical at their stable slot ids,
  so no index entry is touched) and logged as a **single unconditional**
  `FullPageImage` — a prune+compact relocates survivors and is not expressible as a
  delta, so it is never gated on `take_needs_fpi` (mirrors `btree::log_full_page`).
  The FPI's LSN becomes the page's new PageLSN. A page with no dead slots is skipped
  entirely: no WAL record and no mutation.
- **Full-extent scan.** It iterates `0..BufferPool::page_count(heap_file_id)`,
  faulting each page in (resident or from disk), rather than only the resident pages
  `iter_pages` reports — an evicted page holding dead tuples must still be vacuumed,
  else GC is incomplete.
- **Latching.** Per page it takes the per-heap structural latch then the frame write
  latch (lock order structural → frame → WAL) and releases both before the next page
  (never held across pages). VACUUM runs under the exclusive concurrency guard at
  this milestone (no concurrent writers), so these uncontended latches are
  forward-looking.
- **Maintenance txn.** Pages are dirtied and logged under txn id `0` — the
  recovery/maintenance convention (shared with `fetch_for_redo`) — because VACUUM is
  non-transactional maintenance: its reclamation must not be undone by an abort or
  depend on a user commit. Recovery reinstalls each `FullPageImage` purely by
  PageLSN gating, independent of the record's `txn_id`, so a crash mid-VACUUM leaves
  every pruned page either pre-prune or exactly the compacted image, never torn.
- It does **not** reclaim line pointers `DEAD → UNUSED` (F3b) or vacuum indexes
  (F3a); those are separate, later steps.

### Index VACUUM Pass (`vacuum_indexes`, F3a)

`vacuum_indexes(schema, dead_tids: &HashSet<RowLocation>)` removes the dangling
index entries that `vacuum_heap` left behind (`mvcc.md` §9 / Milestone F3a). After
the heap prune marks a dead tuple's line pointer `DEAD`, every index entry pointing
at that TID still lingers (it would resolve to a DEAD slot); this pass deletes those
entries from the table's **primary-key index and every live secondary index**, so no
index entry resolves to a dead slot before the line pointers are reclaimed
`DEAD → UNUSED` (F3b). It has no production caller yet (VACUUM orchestration is F4a),
so it is a runtime no-op at this milestone.

- **Remove by dead-TID membership, not by key.** The heap prune already compacted the
  page, so the dead tuple's key bytes are gone and the entry's key cannot be
  recomputed. Each index leaf stores the heap TID as its value, so entries are matched
  by **value-set (dead-TID) membership** instead: an entry is removed iff its stored
  `RowLocation` is in `dead_tids`. Live versions' entries (value not in the set) are
  left intact.
- **Single-pass leaf walk.** Each index is vacuumed by `BTree::remove_values_in`,
  which walks the leaf chain once (left to right via the right-sibling `link`s),
  decodes each leaf entry's value, and shifts the matching entries out with
  `index_page::remove_entry` under that leaf's frame write latch — no re-descent per
  entry. A leaf that changed is logged as a single `FullPageImage` (the
  `btree::log_full_page` pattern); an unchanged leaf is skipped (no WAL, no mutation).
- **No node merging — B-link safe vs lock-free scanners.** VACUUM runs under the
  exclusive guard but lock-free readers scan concurrently (they take no guard, only a
  short-lived per-leaf read latch, and follow the right-sibling `link`). The pass
  never merges or frees a leaf and never rewrites a right-sibling link, so the leaf
  chain a reader is walking is structurally unchanged; an emptied leaf is left in
  place (accepted bloat, mirroring the heap's leave-pages-in-place stance). The
  per-leaf write latch a removal takes is mutually exclusive with a reader's read latch
  on the same leaf, so a reader sees each leaf either fully before or fully after the
  shift, never torn; and because only *dead* TIDs are shifted, a concurrent scanner
  can never miss or duplicate a *live* entry.
- **Latching.** Each index is vacuumed under *its own* per-index structural latch,
  acquired and released around that index's whole walk and never held while another
  index's latch is taken (rule 1: never two structural latches at once).
- **Maintenance txn.** Removals are logged under txn id `0` (`VACUUM_TXN`), the
  recovery/maintenance convention, so they are never undone by an abort and do not pin
  WAL truncation. Recovery reinstalls each changed leaf's `FullPageImage` by PageLSN
  gating, independent of the record's `txn_id`.
- It does **not** reclaim line pointers `DEAD → UNUSED` (F3b); the slots stay `DEAD`
  until that later step.

### Line-Pointer Reclaim Pass (`reclaim_line_pointers`, F3b)

`reclaim_line_pointers(schema, dead_tids: &HashSet<RowLocation>)` is the third
VACUUM phase (`mvcc.md` §9 / Milestone F3b): it flips each `dead_tid`'s heap line
pointer `DEAD → UNUSED`, freeing its slot id so a future `insert_row` can recycle
it (bounding the slot array under churn). `dead_tids` are the TIDs `vacuum_heap`
(F2b) pruned to `DEAD` and `vacuum_indexes` (F3a) has since stripped of every index
entry. It has no production caller yet (VACUUM orchestration is F4a), so it is a
runtime no-op at this milestone.

- **Ordering invariant — F2b → F3a → F3b.** This MUST run only after
  `vacuum_indexes` removed every index entry for these TIDs, so an `UNUSED` slot
  has no dangling index entry. That is the safety hinge for `insert_row`'s
  `UNUSED`-only reuse: reusing a slot with a surviving index entry would let a stale
  entry resolve to the new tuple written into it (silent corruption). F4a enforces
  the order; here it is a documented precondition the caller owns. The underlying
  `page::reclaim_line_pointers` errors on a non-`DEAD` slot, which catches the gross
  misordering of reclaiming a never-pruned slot (it cannot by itself prove the index
  entries are gone — that is F4a's responsibility).
- **Per page, lock order structural → frame → WAL.** TIDs are grouped by heap page;
  each page is reclaimed under the per-heap structural latch then the frame write
  latch (released before the next page — rule 1), and logged as a single
  unconditional `FullPageImage` under the maintenance txn id (`0`). Recovery
  reinstalls the reclaimed page by PageLSN gating, independent of the record's
  `txn_id`. A reclaim (FPI: slot → `UNUSED`) followed by a later
  insert-into-reused-slot (`HeapInsert`) replays in LSN order to the final state
  (the new row at that slot), so a crash mid-reclaim leaves the page either
  pre-reclaim or exactly the reclaimed image, never torn.

### MVCC Delete

`delete(ctx, table, key)` marks the **visible** version of `key` deleted in place
rather than tombstoning it (`mvcc.md` §3.2 invariant 1):

1. **Locate the visible version.** The primary-key index may carry an entry per
   version, so `delete` collects the candidate TIDs (`scan_key(key)`), decodes each
   tuple's physical header, and selects the one visible to the statement's snapshot
   (`ctx.snapshot`/`ctx.txn_id`) via `common::is_visible` — the row the executor
   matched (under snapshot isolation at most one version of a key is visible). If
   none is visible (already deleted, aborted, or absent), the delete affects no row
   and returns `Ok(false)` (the missing-row semantics).
2. **Stamp `xmax` in place (with an atomic conflict check).** It stamps
   `xmax = ctx.txn_id` on that version's tuple header through the shared
   `stamp_xmax_logged` path. Under the page's `write_page` frame latch — and
   **before** appending any WAL record or mutating the page — it re-reads the
   version's *current physical* header `xmax`/`infomask` and runs the first-updater-
   wins check `common::write_conflict` (`mvcc.md` §7.3, Milestone E1b). On
   `Conflict` (another transaction has already claimed this version's `xmax` and is
   committed or still in-flight) it fails fast with `SqlState::SerializationFailure`
   (`40001`) **without appending the `HeapUpdateHeader` or stamping** — the winning
   writer's `xmax` stands. On `Proceed` (no deleter, the deleter aborted, or it is
   this txn's own lock) it appends a `HeapUpdateHeader { file_id, page_num, slot,
   xmax = ctx.txn_id, t_ctid = INVALID_TID, infomask = unchanged }` (or a
   `FullPageImage` on the page's first touch since the last checkpoint) and applies
   `page::set_tuple_header` with that record's LSN. The read-classify-stamp sequence
   is atomic on the frame latch, so two concurrent writers racing for this version
   serialize on the latch and the loser observes the winner's `xmax` (no TOCTOU). Under
   the current serialized-writer model the located version's `xmax` is `INVALID_XID`
   (or own), so the check is a runtime no-op; it becomes load-bearing under
   concurrent writers (Milestone E2b). `t_ctid` stays `INVALID_TID` (a delete has no
   successor) and `infomask` is carried through unchanged (no hint bits set here —
   that is the optional commit 10). The line pointer **stays `NORMAL`**: the tuple is
   physically present and is hidden purely by visibility once the deleter commits.
3. **Retain index entries.** No primary-key or secondary index entry is removed.
   The dead version and its entries linger until VACUUM (Milestone F) reclaims them.

This is the first point internal state diverges from a single-version heap: a
deleted tuple and its index entries persist (the accepted interim cost). External
SQL behavior is unchanged — a committed `DELETE` then `SELECT` does not see the row
— and **delete-then-reinsert of the same key now succeeds**, because the
committed-deleted version no longer blocks the re-insert (the uniqueness check
ignores committed-deleted/aborted versions). On abort, the buffer pool's
before-image undo restores the page (un-stamping `xmax`); since no index entry was
removed, no index repair is needed. Recovery replays the `HeapUpdateHeader` redo
(PageLSN-gated), so a committed delete stays hidden and an aborted one (no durable
`Commit`) leaves the row visible.

### MVCC Update

`update(ctx, table, key, row)` writes a **new heap version** and chains the old
one to it (Postgres-style, non-HOT; `mvcc.md` §3.2 invariants 1, 3, 5). The
primary key may not change (a changed PK is a `DatatypeMismatch` error), so the new
version carries the same PK as the old. The flow is ordered for correct uniqueness:

1. **Locate the visible old version.** Like `delete`, `update` locates the version
   the statement's snapshot sees via `locate_visible_version(key)` (the candidate
   TIDs from `scan_key(key)` filtered by `common::is_visible`), **not**
   `search(key)`. This matters once a key carries several versions' entries — after
   a delete-then-reinsert, `search` could return a dead version; targeting the
   *visible* one is what makes the right row the one updated. No visible version ⇒
   the update affects no row and returns `Ok(false)`.
2. **Write the new version.** The replacement row is written as a fresh heap tuple
   at a **new TID** through the normal insert/heap-write + WAL path, stamping
   `xmin = ctx.txn_id`, `xmax = INVALID_XID`, `t_ctid = INVALID_TID` (it is the
   latest version).
3. **Chain the old version forward (with an atomic conflict check).** The old
   version's header is stamped `xmax = ctx.txn_id` **and** `t_ctid = new_tid` in
   place via the same `stamp_xmax_logged` path as `delete`, so it runs the identical
   atomic first-updater-wins `write_conflict` check under the frame latch before any
   WAL append (step 2 above): if another transaction already claimed the old
   version's `xmax`, the update fails fast with `SqlState::SerializationFailure`
   (`40001`). The line pointer stays `NORMAL`; `infomask` is carried through. This
   stamping happens *before* the new version's uniqueness checks, so the old version
   reads as own-deleted (`xmax == ctx.txn_id`) and does not falsely self-conflict.
   Because the new version (step 2) was written *before* this stamp, a `40001` here
   leaves a transient **orphan**: the new heap tuple (the per-version index entries
   of step 4 below have not run yet, so only the tuple is orphaned). No manual
   cleanup is needed — the error aborts the transaction, so the orphan (xmin = the
   aborting txn) becomes invisible via CLOG = Aborted and is reclaimed by VACUUM
   (Milestone F). (An early pre-write conflict check to avoid the transient orphan is
   a deferred optimization; the authoritative check stays atomic at stamp time.)
4. **Insert per-version entries into all indexes.** A new `(key, new_tid)` entry is
   inserted into the primary-key index and a new `(secondary_key, new_tid)` entry
   into **every** secondary index — changed-column or not. Because reads do not walk
   `t_ctid` (every version is independently indexed; one entry per version), the new
   TID needs its own entry in every index, or a scan on an unchanged secondary value
   would find only the superseded old version's entry. Skipping unchanged-column
   indexes is a HOT optimization (Milestone H) and would be a correctness bug here.
   Unique indexes (PK and unique secondary) run the visibility-aware
   `unique_conflict_kind` check: a value unchanged from the old version does not
   self-conflict (the old version is own-deleted), but a value colliding with a
   *different* live row raises `UniqueViolation` (or `SerializationFailure` if that
   row is held only by an in-progress inserter — §7.3, E1c).
5. **Retain all old entries.** No old index entry — PK or secondary — is removed.

After a committed `UPDATE`, both versions coexist in the heap: the old version
(`xmax = txn`, `t_ctid → new`, invisible to later snapshots) and the new live
version, with every old index entry lingering until VACUUM (Milestone F) reclaims
the dead version and its entries. External SQL is unchanged: a later snapshot sees
the new value via a sequential scan, an index scan on the changed column, and a
scan on an unchanged secondary value (the new version's entry resolves all three).
An older snapshot that predates the update still resolves the old version through
its retained entries. On abort (statement error → autocommit rollback), the buffer
pool's before-image undo restores every page the update touched — the new tuple's
heap page and the index pages gain their new entries on a first `new_page`/
`write_page` for the transaction, so the undo removes them, and the old version's
header is un-stamped; combined with the `Abort` record (CLOG marks the txn
aborted), no orphan new version is visible. Recovery replays the new tuple's
`HeapInsert`/`FullPageImage`, the old version's `HeapUpdateHeader`, and the new
index-entry page images (all PageLSN-gated), so a committed update's new value
survives restart and an aborted one leaves the old value.

## Row Serialization

```text
[row_format_version: 1 byte][infomask: 2][xmin: 8][xmax: 8][t_ctid: 6][null_bitmap][col1_data][col2_data]...
```

- `row_format_version`: `2`. `decode_row` also accepts legacy `1` tuples (which
  omit the MVCC header — `[version=1][null_bitmap][columns]`); all other versions
  are rejected as corrupt.
- MVCC tuple header (v2 only), all little-endian:
  - `infomask`: 2-byte hint bits. Bit 0 `XMIN_COMMITTED`, bit 1 `XMIN_ABORTED`,
    bit 2 `XMAX_COMMITTED`, bit 3 `XMAX_ABORTED` cache settled transaction status
    to skip a CLOG probe; bit 4 `HEAP_ONLY` and bit 5 `HOT_UPDATED` are reserved
    for HOT; bits 6–15 reserved (zero). No bits are set on insert; later
    milestones populate them. The four `*_COMMITTED`/`*_ABORTED` settled-status
    bit constants are owned by `common` (so the `common::is_visible` predicate and
    the tuple codec share one definition) and re-exported by the codec; the two
    HOT bits stay storage-private.
  - `xmin`: 8-byte `u64` creator transaction id.
  - `xmax`: 8-byte `u64` deleter transaction id; `0` (`INVALID_XID`) means the
    version is live/not-deleted.
  - `t_ctid`: forward successor pointer `(page_num: u32, slot: u16)` = 6 bytes.
    The sentinel `INVALID_TID = (u32::MAX, u16::MAX)` means "no successor / this
    is the latest version" (the encoder does not know its own slot, so insert
    stamps the sentinel).
- Insert stamps `xmin = txn_id`, `xmax = INVALID_XID`, `t_ctid = INVALID_TID`,
  `infomask = 0`; the creating `txn_id` flows from `StatementContext.txn_id`.
- Legacy v1 tuples decode with synthesized `xmin = FROZEN_XID`,
  `xmax = INVALID_XID`, `t_ctid = INVALID_TID`, `infomask = XMIN_COMMITTED`, so
  pre-MVCC rows are always visible.
- The reserved xid sentinels live in `common`: `INVALID_XID = 0`,
  `FROZEN_XID = 2`; the transaction-id allocator must assign real ids strictly
  above the reserved range (`FIRST_NORMAL_XID = 3`).
- `Integer`: 8-byte little-endian i64.
- `Text`: 4-byte length prefix plus UTF-8 bytes.
- `Boolean`: 1 byte.
- `Null`: bit set in null bitmap, no bytes.

Serialization uses catalog `TableSchema` column order.

## Primary-Key Index

Each table has a durable, non-clustered B+-tree mapping `Key -> RowLocation`, in
its own file. The index file id is the table id with a high bit set, so it never
collides with the heap file id (the bare table id); `HeapPageStore` writes it to
`<data>/heap/<table>.idx`. Rows stay in the heap; the tree replaces the former
in-memory directory.

### Multi-entry ordering

The B-tree is a **multi-entry** structure ordered by the composite `(key, value)`
where `value` is the leaf value (the `RowLocation` for the primary-key index).
**Duplicate user-keys are allowed**, disambiguated and ordered by their value
bytes (the `IndexValue::encode` form, compared as raw little-endian bytes — a
stable total order, not necessarily numeric). The tree no longer rejects duplicate
keys structurally; **primary-key uniqueness is now an engine-level check** (see
Error Handling and the note below). This is the index-per-version substrate
(`mvcc.md` §3.2 invariant 3): for now the primary-key index still stores exactly
one `RowLocation` per key (single version).

- **API.** `insert(txn_id, key, value)` inserts one `(key, value)` entry (duplicate
  keys allowed). `remove(txn_id, key, value)` removes the single matching
  `(key, value)` entry, leaving other entries that share the key intact.
  `scan_key(key)` returns every value whose key equals `key`, in `(key, value)`
  order. `search(key)` returns the first (lowest-value) entry for a key — the sole
  entry for the single-version primary-key index. `range(range)` walks keys in
  order and may now yield multiple values per key. `update` (in-place value
  overwrite) is removed; an engine row relocation is a `remove(old)` +
  `insert(new)`.
- **Pages.** Page 0 is a metapage holding the current root page number. Other
  pages are leaf or internal nodes sharing the standard page header (so they get
  the same PageLSN, checksum, and torn-page protection). A 5-byte node sub-header
  carries a leaf flag and a link (right-sibling for a leaf, leftmost child for an
  internal node); entries are a sorted slotted array of `[key_len][key][value]`.
  **The on-disk node layout is unchanged.** A leaf entry's value is an encoded
  `RowLocation`; an internal entry's value is a child page number. An internal
  **separator's `key` field holds the composite `encoded key ++ value`** of the
  boundary leaf entry (the encoded key is self-delimiting, so the trailing value
  tiebreaker needs no length prefix), so routing can disambiguate equal user-keys
  that straddle a node split. Index-node slots are a narrower 4-byte
  `[offset: u16][len: u16]` pair (no dead flag, since a delete removes the slot
  outright), distinct from the 6-byte `[offset][len][flags]` slot used by heap
  data pages.
- **Lookup / scan.** `search`/`scan_key` descend from the root to the leaf at the
  key's lower bound and walk the right-sibling chain; `scan`/`scan_range` find the
  start leaf and walk the chain in `(key, value)` order. Equal keys that straddle
  a leaf boundary are followed via the right-sibling link, so no entry is skipped
  or duplicated.
- **Insert.** Places the entry in `(key, value)` sorted position; a full node
  splits at a byte-balanced point (so variable-length keys do not overflow a half)
  and propagates a composite separator upward, growing the tree by a level on a
  root split. Routing descends to the left of the first separator strictly greater
  than the probe (a separator equal to the probe routes right, since a separator is
  the right child's first `(key, value)`).
- **Delete.** Removes the specific `(key, value)` entry; underfull nodes are not
  merged (accepted bloat).
- **Update.** A row update relocates its heap tuple, so the engine moves the
  index entry by `remove(key, old_location)` then `insert(key, new_location)`. A
  row update that would change the primary key itself is rejected by the engine
  with `SqlState::DatatypeMismatch` (primary-key updates are not supported).
- **Crash safety.** Every node mutation logs a `FullPageImage` and stamps the
  page-LSN, so the index is recovered by the same redo path as the heap and needs
  no rebuild. The node layout is unchanged, so recovery replays these full-page
  images exactly as before. Page allocation is seeded from each file's on-disk
  extent so a new node never reuses an existing page after recovery.
- **Keys.** Keys are stored in a self-describing byte form and ordered by decoding
  to `Key` and comparing with `Ord`; equal keys are then ordered by their raw
  value bytes.

**Primary-key uniqueness (visibility/CLOG-aware liveness check).** Because the tree
no longer rejects duplicate keys, the engine `insert` enforces uniqueness with a
shared visibility-aware check (`unique_conflict_kind`): it `scan_key(pk)`s the
primary-key index and, for each candidate TID, reads the *physical* tuple header
and classifies that version (`common::classify_unique_conflict` →
`UniqueConflict`). The decision is a **liveness ("dirty") check, not a snapshot
read**: it consults the CLOG (`TxnStatusView`) plus the tuple's `infomask` hint
bits — never a `Snapshot` — so it sees concurrently in-flight and already-committed
state. The three-way classification (Milestone E1c, `mvcc.md` §7.3):

- **`None` (dead, ignored)** — the creator is aborted, or the version is
  committed-deleted (`xmax` committed, or `xmax == current_txn` deleted-by-me, e.g.
  an UPDATE's own superseded old version). It does not occupy the key.
- **`Violation` ⇒ `SqlState::UniqueViolation` (`23505`)** — the version is alive
  *and* a definite duplicate: its creator is committed, is `current_txn` itself (a
  live version I already hold), or is frozen/reserved.
- **`InFlight` ⇒ `SqlState::SerializationFailure` (`40001`)** — the version is
  alive but only *potentially* a duplicate: its creator is **another in-progress
  transaction** that has not committed and may yet abort. Under the fail-fast
  first-updater-wins policy (§7.3) uniqueness is undecidable, so the inserter fails
  fast for the client to retry rather than blocking; if the other inserter aborts,
  the retry succeeds.

`unique_conflict_kind` returns the **strongest** conflict across all candidates
(precedence `Violation > InFlight > None`): a single committed-live duplicate is a
definite `23505` even if another candidate is only in-flight. A DEAD/UNUSED line
pointer contributes no conflict. This replaces the earlier temporary
presence-probe; **under serialized writers (Stage 1) no concurrent uncommitted
inserter exists, so the `InFlight` arm never fires at runtime and the check rejects
exactly the same inputs with `23505` as before** — it becomes load-bearing once
writers run concurrently (Milestone E2b). Once versioning (Milestone B4) stamps
`xmax`/writes aborted versions, a dead version with the same key no longer blocks a
re-insert.

The B-tree is generic over its leaf value type, but every index — primary-key and
secondary — now stores a fixed-width `RowLocation` (heap TID), so all indexes are
uniform (see Secondary Indexes). Internally the tree treats values as opaque bytes
and uses them as the equal-key tiebreaker.

## Secondary Indexes

A table may have any number of secondary indexes. Each is its own durable B-tree
in its own file, tagged with the top two file-id bits (distinct from the heap and
the primary-key index) and written to `<data>/heap/<index_id>.sidx`. Index ids
are a separate id space from table ids; the reserved primary-key index id is never
used for a secondary index.

- **Entry layout.** A secondary index stores `indexed_columns -> RowLocation`
  (heap TID), uniform with the primary-key index — every index is now
  `(key → heap TID)`. Reads go secondary index → `RowLocation` → heap, with no
  primary-key indirection. (Previously secondary indexes stored the primary key
  and reads chained through the primary-key index; that indirection is removed.)
- **Key shape.** The secondary key is the encoded indexed column(s) alone; the
  primary key is no longer embedded. Duplicate indexed values (including multiple
  rows whose indexed value is NULL) coexist as ordinary multi-entry rows,
  disambiguated by the trailing heap TID in the tree's `(key, tid)` ordering. A
  unique secondary index enforces uniqueness through the **same shared
  visibility/CLOG-aware liveness check** the primary-key index uses
  (`unique_conflict_kind` / `common::classify_unique_conflict`): it conflicts only
  with an alive-or-potentially-alive version of the key, ignoring dead
  (creator-aborted) and committed-deleted versions. For a non-NULL indexed value it
  returns `SqlState::UniqueViolation` (`23505`) when a committed/own/frozen-live
  duplicate exists, and `SqlState::SerializationFailure` (`40001`, retry — §7.3,
  Milestone E1c) when the only conflicting version was created by another
  in-progress transaction (undecidable). The check is **skipped for a NULL indexed
  value**: SQL treats NULLs as distinct, so NULL never participates in a unique
  constraint (neither `23505` nor `40001`), and distinct NULL rows coexist naturally
  via their differing heap TIDs. This replaces the earlier temporary presence-probe;
  under serialized writers the `40001` arm never fires and single-version behavior
  is unchanged — it becomes load-bearing once writers run concurrently (Milestone
  E2b).
- **Lookup / range.** `index_scan(table, index, range)` constrains the leading
  indexed columns; the range bounds hold exactly those columns, and comparison
  ignores each stored key's trailing TID tiebreaker (the leaf value). An equality
  bound thus matches every row sharing the indexed value, and an inclusive upper
  bound includes all of its rows. Results are returned in index order, each read
  directly from the heap at its TID. The `StoredRow.key` is recovered from the heap
  row's primary key.
- **Maintenance.** `insert` adds an entry to every index. `delete` removes **no**
  entry — it stamps the deleted version's `xmax` in place and retains its entries
  (VACUUM reclaims them; see MVCC Delete). `update` removes the old entries and
  inserts the new ones (all removals before any insertion, so an unchanged unique
  value is not seen as a duplicate). A unique-index conflict during `insert` or
  `update` returns `SqlState::UniqueViolation` for a committed-live duplicate, or
  `SqlState::SerializationFailure` (`40001`) when the key is held only by another
  in-progress inserter (Milestone E1c, §7.3).
- **Create / drop.** `create_index` registers the index, builds an empty tree,
  and backfills it by scanning the live rows through the primary-key index
  (a duplicate value for a unique index fails the build with `UniqueViolation`).
  `drop_index` marks the index dropped and leaves its pages in place (accepted
  bloat, like `drop_table`). `drop_table` (and its recovery replay) cascades to
  mark the table's secondary indexes dropped too, keeping storage's index set
  consistent with the catalog's drop-table cascade. The engine learns a table's
  live indexes from the installed index schemas (`install_index_schemas`) plus
  in-session creates.
- **Crash safety.** Like the primary-key index, every secondary node mutation
  logs a `FullPageImage` and stamps the page-LSN, so index pages recover through
  the same redo path as the heap. Index *metadata* (which indexes exist) is made
  durable by the `CreateIndex` / `DropIndex` WAL records — replayed into both
  catalog and storage — plus the catalog snapshot at each checkpoint.

## Heap Page Store

`HeapPageStore` is the mutable page home for in-place dirty-page flushing. It
implements `buffer::PageStore` over one file per table: the heap at
`<data>/heap/<file_id>.heap`, the primary-key index at `<data>/heap/<table>.idx`
(index file ids carry the high bit), and each secondary index at
`<data>/heap/<index_id>.sidx` (file ids carry the top two bits), storing page `n`
at byte offset `n * PAGE_SIZE` with positioned reads/writes. `load_page` returns a complete page or `None`
(missing file or beyond-EOF / short tail); `write_page` writes in place without
fsync; `sync_all` fsyncs all open files and the directory; `page_count` returns a
file's on-disk extent in pages, used to seed page allocation after recovery.

`apply_physical_redo(page, lsn, kind)` replays one physiological redo record
(`HeapInit`/`HeapInsert`/`HeapDelete`/`HeapUpdateHeader`/`FullPageImage`) onto a page buffer, gated by
the page-LSN: a record whose effect is already present (`page_lsn(page) >= lsn`) is
skipped, making replay idempotent. `FullPageImage` is validated to be exactly
`PAGE_SIZE` bytes before install. Recovery uses it to redo committed records after
the checkpoint LSN.

## WAL Interaction

Normal data operations append physiological redo records as they mutate pages, stamping the page-LSN with each record's LSN:

- A row insert logs `HeapInsert { file_id, page_num, slot, row_bytes }`, or a `FullPageImage` if this is the first modification of the page since the last checkpoint (torn-page protection). A fresh page first logs `HeapInit`.
- An MVCC row delete logs `HeapUpdateHeader { file_id, page_num, slot, xmax, t_ctid, infomask }` to stamp `xmax` in place on the still-`NORMAL` line pointer (or a `FullPageImage` on first touch); it does not tombstone (see MVCC Delete). `HeapDelete { file_id, page_num, slot }` is still logged by `update`'s relocate path (an update is a delete followed by an insert), retired in Milestone B4.9.
- Each primary-key or secondary index node mutated during the operation logs a `FullPageImage` of that node (the indexes use full-page-image redo throughout). `create_table` initializes the primary-key index, and `create_index` initializes and backfills a secondary index, logged the same way.
- `SchemaOperations::create_table` / `drop_table` / `create_index` / `drop_index` log `CreateTable` / `DropTable` / `CreateIndex` / `DropIndex`. Recovery replays each into both the catalog and storage metadata; the index pages come back through the full-page-image redo above.

Server query orchestration appends `Commit` and flushes WAL after the statement succeeds. Storage should not append commit records.

## Recovery Mode

The storage engine can be initialized in recovery mode. In recovery mode:

- Normal `StorageEngine` methods are not used.
- Row recovery is physiological page redo: the server drives `apply_physical_redo` over committed records, PageLSN-gated and idempotent. DDL records replay via `RecoveryOperations`.
- No WAL append occurs.
- The primary-key and secondary indexes are durable on disk, so their pages are recovered by the same redo (full-page-image records) as the heap; there is no in-memory directory to rebuild. Which indexes exist is reinstalled from the catalog at startup (`install_index_schemas`).

Concrete page-backed storage exports:

```rust
pub enum StorageMode {
    Recovery,
    Normal,
}

impl PageBackedStorageEngine {
    pub fn open(
        buffer_pool: Arc<dyn BufferPool>,
        wal: Arc<dyn WalManager>,
        mode: StorageMode,
    ) -> Result<Self>;

    pub fn install_schemas(&self, schemas: Vec<TableSchema>) -> Result<()>;
    pub fn install_index_schemas(&self, schemas: Vec<IndexSchema>) -> Result<()>;
    pub fn set_mode(&self, mode: StorageMode) -> Result<()>;
}
```

`open` stores shared `Arc` handles to the buffer pool and WAL manager and initializes empty table metadata. It does not read schemas from disk; server startup installs catalog schemas explicitly with `install_schemas` (tables) and `install_index_schemas` (secondary indexes) after loading the catalog snapshot, so DML maintains the indexes.

`PageBackedStorageEngine` implements `StorageEngine`, `SchemaOperations`, and `RecoveryOperations`. Server code stores `Arc<PageBackedStorageEngine>` for v1 so startup can call concrete recovery-mode methods and query execution can pass `storage.as_ref()` as both `&dyn StorageEngine` and `&dyn SchemaOperations`.

`RecoveryOperations` is implemented directly for `PageBackedStorageEngine`. There is no separate public `StorageRecovery` adapter in v1; `crates/storage/src/recovery.rs` contains the `impl RecoveryOperations for PageBackedStorageEngine`, which delegates to the recovery-mode helpers (`apply_create_table_without_wal` / `apply_drop_table_without_wal`) defined on `PageBackedStorageEngine` in `engine.rs`.

## Structural Write Latches (Milestone E2a)

Stage-2 concurrency (`docs/specs/mvcc.md` §7.1, §10 E2a) serializes structural
mutations **within** one index or one table heap, while allowing concurrent
writers across *different* indexes/heaps and lock-free B-link readers. The on-disk
B-tree splits without latch coupling (it releases the page latch between levels and
re-acquires the parent to propagate a split), and heap free-space search reads a
page, drops the read latch, then re-acquires write — both are unsafe for concurrent
structural writers (a fully-concurrent B-link tree and a free-space map are deferred,
`mvcc.md` §12). A per-index / per-heap structural latch held across the *whole*
operation closes both windows.

- **Registry.** `PageBackedStorageEngine` holds a registry mapping `FileId →
  Arc<parking_lot::Mutex<()>>` (a `Mutex<HashMap<…>>`, lazily populated).
  `structural_latch(file_id)` locks the registry mutex only **briefly** — to look up
  or lazily insert the file's latch — and drops it before the caller locks the
  returned `Arc`, so the registry never serializes the structural work; only
  same-file structural ops contend. It returns the SAME `Arc` for a given `FileId`
  across calls (so two writers on one index/heap share a latch) and a DIFFERENT one
  per file (so writers on different indexes/heaps run concurrently). The engine is
  shared via `Arc`, so the latches are shared across all transactions/connections.
- **Per-index latch — atomic uniqueness-check-AND-insert.** Every index structural
  mutation holds that index's `structural_latch(index_file_id)` across the WHOLE
  operation. The critical correctness requirement: the visibility-aware uniqueness
  check (`unique_conflict_kind`, which scans the index) and the `BTree::insert` (which
  mutates it, including any leaf split, parent-split propagation, and root split +
  metapage `set_root`) run under **one** hold of the latch — otherwise two concurrent
  inserts of the same key could both pass the check and both insert a duplicate. This
  applies to the PK insert path (`insert` and `update`'s new-version PK entry),
  `insert_secondary_entry` (each secondary; a non-unique secondary just holds the
  latch across the insert), and `create_index`'s backfill inserts (each holds the new
  index's latch). The dead-code `BTree::remove`/`search` (future VACUUM) are not wired
  yet.
- **Per-heap-file insertion latch — closes the free-space TOCTOU.** `write_new_row`
  holds `structural_latch(heap_file_id)` across the whole free-space search +
  `new_page`/`insert_row` + WAL log, making "find space / extend / insert / log"
  atomic against another inserter on the same table heap (two concurrent inserters can
  no longer both target the last slot). The UPDATE/DELETE in-place `xmax` stamping
  (`stamp_xmax_logged`) targets a known slot under the buffer-pool frame latch + the
  E1b conflict check and allocates no free space, so it does **not** take the heap
  insertion latch.
- **Lock-ordering contract** (followed uniformly to prevent deadlock):
  1. **Never hold two structural latches simultaneously.** Each structural latch is
     acquired, the mutation runs, then it is released *before* the next is taken — the
     heap-insertion latch (in `write_new_row`) is released before the index latches;
     the PK-index latch is released before each secondary-index latch. Because no
     structural latch is ever held while acquiring another, there is no multi-latch
     deadlock regardless of index order. (`insert`/`update` therefore write the heap
     tuple first under the heap latch, then take the PK latch for the atomic
     check-and-insert.)
  2. **Never acquire a structural latch while holding a buffer-pool frame latch.** The
     order is always structural latch → (inside the btree/heap op) frame latch →
     (inside the WAL append) WAL mutex. No path takes `read_page`/`write_page` and then
     acquires a structural latch. The E1b `stamp_xmax_logged` takes only a frame latch
     (no structural latch), so it does not participate in this ordering.

These latches are **uncontended** until E2b removes the global exclusive writer lock:
under serialized writers only one writer runs at a time, so this commit installs the
substrate with zero runtime behavior change. Real contention/atomicity stress tests
arrive in E2b once writers overlap.

## Page-Backed V1 Simplifications

- Structural mutations within one index or one heap file serialize on that file's
  per-`FileId` structural write latch (above); under the still-global writer lock
  (E2b removes it) the latches are uncontended, so heap and index page modifications
  effectively run single-writer today.
- The primary-key index is durable on disk, so nothing is rebuilt after recovery.
- Compaction may be skipped unless a page runs out of free space (and B-tree nodes are never merged).
- Before any page mutation, storage must obtain a write page guard with `ctx.txn_id`.
- New pages allocated during a statement must be tracked by buffer rollback through `new_page(file, txn_id)`.
- Index and heap page changes (including B-tree splits) are rolled back by the buffer pool's before-images and new-page tracking, so `rollback_txn(txn_id)` only restores storage-owned table and index metadata.
- `drop_table` records table metadata in storage rollback metadata before marking the table dropped; `create_index` / `drop_index` record index metadata the same way, so a rolled-back create removes the index and a rolled-back drop restores it. V1 does not physically delete heap or index pages; committed drops are reflected by omitting the table or index from later checkpoints.

## Error Handling

- Duplicate primary key (committed-live duplicate): `SqlState::UniqueViolation`.
- Duplicate value in a unique secondary index (insert, update, or backfill, committed-live duplicate): `SqlState::UniqueViolation`.
- Unique key (primary or secondary) held only by another in-progress inserter — undecidable: `SqlState::SerializationFailure` (`40001`, retry — §7.3, Milestone E1c).
- Update that changes the primary key: `SqlState::DatatypeMismatch` (primary-key updates are not supported).
- `index_scan` on a dropped or unknown index: `SqlState::UndefinedTable`.
- Missing update/delete key: return `Ok(false)`.
- Corrupt page checksum: `ErrorKind::Storage`.
- Page layout or index invariant violation: `ErrorKind::Storage` or `Internal` depending on source.

## Acceptance Tests

- Insert then get returns the row.
- Duplicate insert fails without changing existing row.
- Update replaces a row by primary key.
- Delete removes a row by primary key.
- Scan returns all rows with `StoredRow` identity.
- Range scan returns expected ordered keys.
- Recovery DDL apply mutates metadata without WAL append.
- A reopened engine reads rows through the durable on-disk index (no rebuild).
- A B-tree splits correctly under variable-length keys (byte-balanced) and stays searchable.
- After a restart, inserting a row or growing the index never reuses an on-disk page.
- Failed insert that allocated a new page rolls back newly allocated pages through buffer rollback.
- Heap, primary-key index, and secondary index files for the same numeric id stay distinct.
- A secondary B-tree stores heap TIDs and a prefix range matches the indexed columns regardless of the trailing TID tiebreaker; an index scan resolves to heap TIDs directly.
- `create_index` backfills existing rows; `index_scan` returns them, and a non-unique index returns every row for a value.
- Insert, update, and delete keep a secondary index in sync.
- A unique index rejects a duplicate value on insert and on backfill, but allows multiple NULLs.
- A dropped index is no longer maintained or scannable; a rolled-back create removes it.
- `create_index` logs a `CreateIndex` record; recovery-apply index methods append no WAL.
- After a restart, a secondary index created post-checkpoint is replayed (catalog + storage metadata and its rebuilt tree) and remains scannable.
