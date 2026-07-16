# `storage` Crate Specification

**Date:** 2026-07-12
**Status:** Living crate contract

`SchemaOperations` includes namespace creation and deletion. Namespace metadata
is carried by the caller's generic `CatalogChange`; these operations allocate no
relation files.

## Purpose

`storage` owns table files, row serialization, page-backed row storage, the durable on-disk storage-identity and catalog B-tree indexes, normal data operations, sequence runtime state, schema file operations, and recovery apply operations.

## Depends On

- `common`
- `buffer`
- `wal`
- `compress` — at-rest page envelopes, WAL full-page-image compression, TOAST
  payload compression, and dictionary resolution

`storage` must not depend on `planner`.

## Public Traits

```rust
pub trait RowIterator: Send {
    fn next(&mut self) -> Result<Option<StoredRow>>;
    fn schema(&self) -> &[ColumnInfo];
}

pub trait RelationSnapshot: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn relation_epoch(&self) -> u64;
    fn table_schema_version(&self, table: TableId) -> Option<u64>;
    fn table_storage_id(&self, table: TableId) -> Option<FileId>;
}

pub struct LockedRow {
    // private: identity, row, table, owning transaction, granted mode
}

pub enum LockRowResult {
    Locked(LockedRow),
    Deleted,
    Skipped,
}

pub trait StorageEngine: Send + Sync {
    fn capture_relation_snapshot(&self) -> Result<Arc<dyn RelationSnapshot>>;
    fn insert(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, row: Row) -> Result<RowId>;
    fn get(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, key: &Key) -> Result<Option<Row>>;
    fn referenced_key_exists(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, access_index: IndexId, key: &Key) -> Result<bool>;
    fn lock_unique_conflict(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, key: &Key, mode: TupleLockMode) -> Result<Option<LockedRow>>;
    fn dependent_row_exists(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, probe: DependentRowProbe<'_>) -> Result<bool>;
    fn delete(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, key: &Key) -> Result<bool>;
    fn lock_row(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        identity: &RowIdentity,
        mode: TupleLockMode,
        wait_policy: TupleLockWaitPolicy,
    ) -> Result<LockRowResult>;
    fn update_locked(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, target: &LockedRow, row: Row) -> Result<bool>;
    fn delete_locked(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, target: &LockedRow) -> Result<bool>;
    fn update(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, key: &Key, row: Row) -> Result<bool>;
    fn update_requiring_update_lock(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, key: &Key, row: Row) -> Result<bool>;
    fn scan(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId) -> Result<Box<dyn RowIterator>>;
    fn for_each_visible_row(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        visitor: &mut dyn FnMut(StoredRow) -> Result<()>,
    ) -> Result<()>;
    fn scan_range(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, range: &KeyRange) -> Result<Box<dyn RowIterator>>;
    fn index_scan(&self, ctx: &StatementContext, relations: &dyn RelationSnapshot, table: TableId, index: IndexId, range: &KeyRange) -> Result<Box<dyn RowIterator>>;
    fn rollback_txn(&self, txn_id: u64) -> Result<()>;
    fn commit_txn(&self, txn_id: u64) -> Result<()>;
}

pub trait SchemaOperations: Send + Sync {
    fn create_table(&self, ctx: &StatementContext, schema: &TableSchema) -> Result<()>;
    fn drop_table(&self, ctx: &StatementContext, table: TableId) -> Result<()>;
    fn update_table_schema(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        indexes: &[IndexSchema],
    ) -> Result<()>;
    fn create_index(&self, ctx: &StatementContext, schema: &IndexSchema, gc_horizon: u64) -> Result<()>;
    fn drop_index(&self, ctx: &StatementContext, index: IndexId) -> Result<()>;
    fn create_sequence(&self, ctx: &StatementContext, schema: &SequenceSchema) -> Result<()>;
    fn drop_sequence(&self, ctx: &StatementContext, sequence: SequenceId) -> Result<()>;
}

// Views have no physical storage operation. Their metadata changes only
// through the catalog's generic change-set path.

pub trait RecoveryOperations: Send + Sync {
    fn reconcile_catalog_change(&self, change_set: &CatalogChangeSet) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_table_schema(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_index_schema(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_table(&self, table: TableId) -> Result<()>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, index: IndexId) -> Result<()>;
    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()>;
    fn apply_drop_sequence(&self, sequence: SequenceId) -> Result<()>;
    fn apply_sequence_advance(&self, sequence: SequenceId, value: i64) -> Result<()>;
    fn apply_set_sequence_value(
        &self,
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<()>;
    /// Rebuild the derived table identity tree from heap rows after recovery
    /// replay. Must not append WAL.
    fn apply_rebuild_table_identity(&self, schema: TableSchema) -> Result<()>;
}
```

### Tuple lock, resolve, mutate pipeline

`lock_row` is the storage boundary between a scan-time `RowIdentity` and a tuple
version safe to recheck or mutate. It first acquires the requested transaction-owned
tuple mode through `StatementContext::tuple_locks`, then follows the physical
`t_ctid` update chain independently of the statement snapshot. An aborted updater
leaves the predecessor current; an in-progress legacy `xmax` holder is waited out
without a page latch for `Block`, rejected with `55P03` for `NoWait`, or returned as
`Skipped` for `SkipLocked`; a committed update advances to its successor; and a
committed delete returns `Deleted`. REDIRECT roots and cross-page/non-HOT successors
are followed with cycle detection.

The returned `LockedRow` contains the latest physical `RowId`, logical identity,
and materialized row. A primary-key-changing successor is protected by acquiring
both its predecessor and current logical tags before return. For a heap-identity
table, a HOT successor retains the root's hidden identity; an independently indexed
non-HOT successor uses its own physical identity. If a successor tag cannot be
obtained under `NOWAIT`/`SKIP LOCKED`, or resolution ends in an error, skip, or
delete, acquisition receipts restore only the grants made by that attempt.
The initial physical member must match the scan identity's creator transaction and
logical primary key (or its physical heap identity for an independently indexed heap
tuple); a mismatched or reused TID is treated as deleted rather than retargeted to an
unrelated row, including a same-key tuple inserted into a VACUUM-reused slot.

Successor materialization uses a sees-all-committed snapshot while preserving the
caller's live-subxid set, so external TOAST chunks owned by a post-snapshot successor
can be read. `update_locked` and `delete_locked` verify that the supplied physical
identity is still the latest version and that the context's tuple-lock manager still
reports the required live grant. They also compare the capability's materialized row
with that freshly resolved version before using it for SSI write accounting, then
mutate that exact heap location. They do
not re-run a key lookup that could retarget the operation after verification.
`delete_locked` and a primary-key-changing `update_locked` require `Update`; an
identity-preserving update requires at least `NoKeyUpdate`. A weaker recorded mode
is rejected rather than treated as mutation authority. Capabilities are also bound
to their table and owning transaction, so they cannot be replayed through another
statement context or relation. The
ordinary `update`/`delete` entry points remain available for existing callers, but
they also take a blocking `NoKeyUpdate`/`Update` tuple lock before mutation so they
cannot bypass an explicit row lock. Those legacy entry points retain
first-updater-wins behavior: if resolution advances beyond their snapshot-selected
identity or finds it deleted, they return `40001`. `lock_row` is used by both the
locking-SELECT `LockRows` operator and UPDATE/DELETE EvalPlanQual. Production
UPDATE/DELETE lock and resolve a scan identity, requalify a successor through the
executor, then call `update_locked`/`delete_locked`; the ordinary entry points
remain for callers that explicitly require legacy first-updater-wins behavior.

### Referential-integrity probes

`referenced_key_exists` is a current-state (dirty), not statement-snapshot,
lookup through a catalog-resolved declared primary-key or UNIQUE constraint
index. It classifies physical candidates by CLOG/infomask state, waits for an
in-progress creator or deleter without retaining a frame/structural latch,
restarts the access-path scan, follows HOT/REDIRECT state, and rechecks the key
after acquiring transaction-owned `TupleLockMode::KeyShare` on the actual current
parent identity. A committed delete or referenced-key change is missing. The
lock remains held through the normal transaction lock lifetime.

`dependent_row_exists` performs the inverse current-state child lookup described
by `DependentRowProbe`: it uses a supplied exact-column child index or a full heap
extent scan, may exclude one exact `RowIdentity` for self-reference, skips
aborted/dead/reclaimed versions, and waits then restarts for in-progress creators
or deleters. Page bytes are copied/decoded before any conflict wait, so no buffer
frame or structural latch crosses a wait. Invalid access-index metadata and
corrupt HOT/REDIRECT state are structured storage errors.

Read Committed accepts the settled current result after a wait. Repeatable Read
and Serializable return `SerializationFailure` (`40001`) when a current row whose
presence is required by either probe is outside the retained transaction
snapshot. A dependent probe therefore returns `40001` after waiting for a
committed post-snapshot child update/delete even when its restart finds that the
child no longer matches the parent key. `update_requiring_update_lock` is the
ordinary-update companion used when a referenced non-primary-key column requires
`TupleLockMode::Update` even though the storage identity is unchanged.

`lock_unique_conflict` reuses the same current-state creator wait, HOT-chain
recheck, retained-snapshot rule, and tuple-lock machinery for the primary-key
`ON CONFLICT` arbiter. It returns the settled locked row rather than a boolean,
so `DO NOTHING` can skip before outgoing FK validation and `DO UPDATE` can
evaluate and mutate the exact conflict even when its creator committed after the
statement snapshot. Before probing it retains a `NoKeyUpdate` reservation on the
proposed primary-key tuple-lock tag. Every ordinary primary-key insert acquires
the same reservation before heap preparation/writes, closing the no-conflict
probe-to-insert gap without carrying an index, page, or frame latch across FK
checks or waits. A no-conflict arbiter keeps the reservation through validation
and insertion. When a conflict exists, it restores that reservation and rechecks
while acquiring only the action's ordinary row-lock mode, avoiding persistent
reservation locks for `DO NOTHING`.

`RelationSnapshot` captures the table/index generation `Arc`s and table schema
versions/storage ids a statement should resolve plus the storage relation epoch observed
while capturing them. The epoch increments whenever storage publishes or
restores relation metadata. Every statement, including Repeatable Read and
Serializable statements, captures a fresh relation snapshot only after acquiring
all referenced table locks and revalidating bound schema versions. Those isolation
levels retain only their MVCC snapshot. Explicit transactions retain table locks
for relations actually referenced, which pins their generations without eagerly
pinning unrelated tables. Reads and writes use only the statement-captured
generations; missing/stale write handles remain errors. A relation snapshot is
retained through its statement stream/portal/COPY lifetime and then released.

`RecoveryOperations` carries storage-owned application of committed generic catalog changes; row-level recovery is physiological page redo via `apply_physical_redo` (see Heap Page Store), not the storage `StorageEngine` methods. Before schema operations that create, drop, or rewrite physical generations, `apply_catalog_change` appends and immediately flushes `CatalogChange` in normal mode. The record remains CLOG-gated and uncommitted, but its allocator high-water is durable before orphan physical files can exist, so failed/crashed DDL cannot reuse their ids. Sequence DDL installs/removes storage's in-memory sequence state in addition to catalog metadata. `nextval` and `setval` append and flush `SequenceAdvance` / `SetSequenceValue` records before updating runtime state, without rollback tracking, so aborted transactions keep sequence gaps. Relation-swap truncate and schema-rewrite preparation receive catalog changes whose allocator high-water marks reserve fresh storage ids before replacement heap/index files are initialized. `rollback_txn` restores storage-owned DDL metadata; heap and index page bytes are not undone under status-based abort, and aborted versions/entries stay physically present but invisible through the CLOG until VACUUM reclaims them. Unpublished relation-generation files created by an aborted truncate prepare may be removed after buffer pin checks. If rollback removes a generation that had already been published to relation snapshots, storage queues it as retired instead of deleting its files immediately; normal retired-generation cleanup removes it after all `Arc` snapshots drain. `commit_txn` discards storage rollback metadata after WAL flush succeeds and queues committed drop generations for retired cleanup; it remains cleanup-only, must not perform I/O, and should not fail for a valid `txn_id`. Recovery operations must not append WAL records.

`RecoveryOperations::reconcile_catalog_change` is the single entry point for
that storage-owned metadata reconciliation. It consumes the complete generic
change set and performs no WAL append.

## Table Storage

Each table is page-backed. Full rows live in heap pages; a durable, non-clustered identity B-tree maps each row's storage identity to its `RowLocation`, stored in a separate reserved index file per table (see Storage Identity Index). For tables with a primary key, the storage identity is the logical primary-key tuple. For tables without a primary key, it is a hidden heap identity derived from the row's root TID. The clustered on-disk B-tree (rows in the leaves, no separate heap) remains future work behind the existing storage traits.

- `insert` inserts a heap row plus storage identity B-tree entry and adds an entry to every catalog index on the table.
- `get` does a storage-identity lookup through the reserved B-tree.
- `scan` / `scan_range` walk the reserved identity B-tree leaves in key order and read rows from their heap locations.
- `for_each_visible_row` visits visible rows from a retained relation snapshot without requiring a table-sized result vector. The page-backed engine walks the identity B-tree one leaf page at a time; the default trait implementation may delegate through `scan` for simple test engines.
- `index_scan` walks a catalog index, which points directly at heap TIDs, and reads each row from its heap location (no identity-index indirection; see Catalog Indexes).

Range collection and visible-row materialization poll `ctx.cancel` at B-tree leaf
and candidate-row boundaries, so large ordinary and secondary-index scans do not
hide statement timeout or user cancellation until the full range is materialized.
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
- **`t_ctid` walk on reads (HOT, Milestone H1).** Every index-driven read path
  (`get`, `scan_range`, `index_scan`, and the UPDATE/DELETE `locate_visible_version`)
  resolves an index entry's TID through `resolve_visible_in_chain`: it follows a
  `REDIRECT` root to its same-page `NORMAL` target (a redirect-to-redirect /
  redirect-to-dead is a structured error), then walks the forward `t_ctid` chain
  returning the first version `is_visible` accepts. The walk is **bounded to one HOT
  chain segment**: it follows `t_ctid` into a successor only when the current tuple
  is `HOT_UPDATED` and the successor is `HEAP_ONLY` (un-indexed) on the same page,
  and **stops** at the latest version, an off-page successor, or any successor that
  is *not* `HEAP_ONLY` (independently indexed, reached via its own entry). This
  preserves "one visible row per index entry" — exactly the un-indexed members are
  crossed — so no row is double-returned; a cyclic `t_ctid` is a structured error,
  not a spin. The walk is read-latch-only (no page mutation; pruning is the
  UPDATE/VACUUM path, H3). The resolved live version's `RowId` is what a scan yields,
  not the index TID. The **uniqueness check** (`unique_conflict_kind`) likewise must
  resolve the chain — via `collect_chain_versions` (the same `REDIRECT` + bounded
  `t_ctid` resolution, but gathering *all* physically-present members rather than the
  one visible version, since a unique conflict may be with a non-visible-but-alive
  version). Reading only the root slot would miss the live version after a HOT update
  collapses the root to a dead tuple or (post-VACUUM) a `REDIRECT`, silently admitting
  a duplicate of the unchanged key.
- **HOT-update fast path (Milestone H2).** `update` attempts a HOT update before the
  normal path (`try_hot_update`): eligible when no indexed column changed (the new
  row's PK and every secondary key match the predecessor's), the TOAST physical state
  is inline-only (no external pointer in the predecessor and the successor can be
  prepared without externalizing any value), AND the prepared tuple fits on the
  predecessor's own page (`try_hot_insert_on_page`). Inline raw and inline-compressed
  `TEXT`/`BYTEA` values are eligible; rows that own external TOAST chunks fall back to
  the normal fully-indexed update path. When eligible it writes the prepared v3
  version as a `HEAP_ONLY` tuple on that page, stamps the predecessor
  `xmax`/`t_ctid → new` with `HOT_UPDATED` (`stamp_xmax_logged`, keeping the
  atomic row-conflict classifier), and inserts **no index entries** — the H1 walk
  reaches the new version via the root. Logged with existing `HeapInsert`
  (`HEAP_ONLY` carried in the row bytes) + `HeapUpdateHeader` records; recovery redoes
  both. When ineligible (indexed column changed, external TOAST ownership, or no
  same-page room after update-path prune) it falls back to the normal fully-indexed
  update.
- **Update-path pruning (Milestone H3).** When a HOT update has no same-page room,
  `try_hot_insert_on_page(.., prune_horizon = Some(ctx.gc_horizon))` first runs the H3
  prune on that page — `classify_page_for_prune(.., allow_dead_roots = false)` then
  `apply_prune_plan` (shared with `vacuum_heap`) — to collapse its committed-dead HOT
  prefixes (REDIRECT the dead root to the live tail, free dead heap-only members to
  `UNUSED`, compact), then retries the same-page insert; only if there is STILL no room
  does it fall back to a normal update. The prune runs under the heap structural latch
  the insert already holds and the frame write latch — mutating ONLY that single page —
  and **never marks a root `DEAD`** (`allow_dead_roots = false`), so it needs no index
  vacuum/line-pointer reclaim under another latch (a fully-dead chain is left for
  VACUUM). It logs its own unconditional `FullPageImage` under the writer's `txn_id`
  (idempotent PageLSN-gated redo; it only reclaims dead-to-all versions, so it is
  correct regardless of the txn's outcome). Lock-free readers re-resolve through line
  pointers (incl. any new `REDIRECT`), so they stay correct; the writer never takes the
  exclusive guard. The GC horizon is read from `StatementContext::gc_horizon` (the
  server captures `gc_horizon()` for the write); a stale/smaller horizon only prunes
  less, never unsafely. For TOAST-enabled tables, any chain containing an external
  TOAST pointer is left for full VACUUM rather than update-path pruning, so the
  parent tuple bytes that own hidden chunk rows are not discarded outside the
  server-owned TOAST cleanup sequence.
- **Index backfill; DML locates the visible version; HOT broken-chain guard.**
  `create_index(ctx, schema, gc_horizon)` backfills while the server holds the
  target table's `Share` lock (excluding DML writers while allowing readers),
  with the GC horizon threaded in. The new secondary
  index generation is not published in storage until its empty tree is created
  and backfill succeeds; relation snapshots captured during the build treat the
  catalog-visible index as unavailable and executor fallback must use a table
  scan. A non-HOT single-version root is indexed from its *current physical*
  tuple only when it is not
  `is_dead_to_all` at that horizon. A HOT chain (root + heap-only members, via
  `collect_chain_versions`) is checked for a **broken chain**: if two or more
  not-`is_dead_to_all` versions disagree on the new index's key(s), the build aborts
  with retryable `SerializationFailure` (`40001`); otherwise the single distinct live
  key is indexed (unconditionally — not gated on the builder's snapshot, so an older
  concurrent reader's version is still indexable), the entry pointing at the chain
  ROOT.
  `delete` and `update` locate the *visible* version via `locate_visible_version`;
  `delete` stamps its `xmax` in place, `update` stamps it and chains it to the new
  version (HOT or non-HOT). Neither removes an index entry.

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

Development builds do not migrate older page formats. Existing page files without `PageVersion = 2` are rejected as corrupt during load/recovery.

`PageType` is `1` for a heap data page and `2` for a B-tree index node. `validate`/`is_valid` accept both (the data-page slot-layout check runs only for type `1`); the index node body layout is described under Storage Identity Index.

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
- `REDIRECT` (`3`) — points at another slot **on the same page**; the target slot
  id is stored in the line pointer's `offset` field. Produced by HOT pruning
  (Milestone H3, `page::set_redirect`) so a collapsed HOT root's stable, indexed
  slot keeps resolving to the surviving live tail (the first not-dead-to-all chain
  member, which may itself be a `HEAP_ONLY` successor). A `REDIRECT` root's index
  entry is **live** — index vacuum never removes it. **Read-side resolution (H1):**
  `page::slot_state(data, slot)` classifies a slot (`Normal`/`Dead`/`Unused`/
  `Redirect(target)`) without reading the tuple, and the engine follows a
  `Redirect` to its target (which must be `NORMAL`).

The numeric values preserve the pre-MVCC encoding, so `NORMAL` is exactly the
former "live" slot and `DEAD` is the former tombstoned slot. Neither MVCC `delete`
nor MVCC `update` tombstones any more — both keep the superseded version on a
still-`NORMAL` line pointer and hide it by visibility (see MVCC Delete / MVCC
Update below), so dead tuples linger physically until VACUUM (Milestone F), the
producer of `DEAD` (via `page::{prune_and_compact, mark_slots_dead}`, F2b/H3),
`UNUSED` (via `page::reclaim_line_pointers`, F3b, for a DEAD root **and**
`page::free_slots_to_unused`, H3, for a chain's heap-only members), and `REDIRECT`
(via `page::set_redirect`, H3, collapsing a HOT chain's dead prefix). The live
VACUUM orchestration `PageBackedStorageEngine::vacuum` (F4a) drives all of these.
`validate` accepts `NORMAL`, `DEAD`, `UNUSED`, and `REDIRECT` flags on a data page
(so both a VACUUM-compacted and a HOT-pruned page are valid); any other value is
corruption. A `REDIRECT`'s `offset` field is a same-page **target slot id**, so
`validate` requires it to be in-bounds (`< num_slots`) but does **not** subject it
to the byte-region check; the resolver enforces the target is `NORMAL`.
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

**HOT-prune primitives (Milestone H3).** `vacuum_heap`'s chain-aware collapse
rewrites line pointers individually and then compacts once, via:

- `page::free_slots_to_unused(data, slots)` — flips listed `NORMAL` slots **directly
  to `UNUSED`** (no `DEAD` intermediary), for a chain's `HEAP_ONLY` members, which
  have no index entry of their own (so there is no dangling entry to strip first — the
  key HOT win). A non-`NORMAL` target is a misuse (`DbError`). Refreshes the checksum
  but does NOT stamp the PageLSN — the trailing `compact` does.
- `page::set_redirect(data, slot, target)` — overwrites a slot's line pointer with a
  `REDIRECT` to `target` (a same-page slot id, stored in the `offset` field), the
  collapse result that keeps a stable indexed root resolving to the surviving live
  tail. The engine guarantees `target` is `NORMAL`; the read resolver re-checks it.
- `page::mark_slots_dead(data, slots)` — flips listed `NORMAL` **or `REDIRECT`** slots
  to `DEAD` (both are index-referenced roots of a fully-dead chain, so F3a strips their
  entries and F3b reclaims them); an already-`DEAD`/`UNUSED` slot is a misuse.
- `page::compact(data, lsn)` — relocates ONLY the `NORMAL` survivors' bytes contiguously
  (rewriting each survivor's line-pointer `offset`), reclaiming the bytes freed by every
  now-non-`NORMAL` slot, then stamps `lsn` and revalidates. It marks nothing dead itself
  (the engine set the slot states); it shares its relocation body with `prune_and_compact`.

`prune_and_compact` is consumed by the non-chain heap-prune case; the H3 primitives
above by `vacuum_heap`'s chain collapse (`classify_page_for_prune` / `plan_chain`,
F2b, below); `page::reclaim_line_pointers` by the engine's `reclaim_line_pointers`
pass (F3b, below). All are reached from the live VACUUM orchestration
`PageBackedStorageEngine::vacuum` (F4a, below).

### Heap-Prune VACUUM Pass (`vacuum_heap`, F2b)

`vacuum_heap(schema, horizon) -> (Vec<RowLocation>, usize)` is the engine heap-prune
pass (`mvcc.md` §9 / Milestone F2b + H3). It reclaims the tuples that are
dead-to-everyone at `horizon` from every heap page of `schema`'s table, collapsing HOT
chains, and returns `(dead_root_tids, freed_member_count)`: newly produced and
pre-existing DEAD-root TIDs feed
F3a/F3b, and `freed_member_count` (heap-only members freed straight to `UNUSED`, which
carry no index entry) is folded into the VACUUM command tag's reclaimed count. It is
the first phase of the live VACUUM orchestration `vacuum` (F4a, below).

- **Chain-aware classification (Milestone H3, `classify_page_for_prune` /
  `collect_prune_chain` / `plan_chain`).** Rather than classifying isolated slots,
  `vacuum_heap` walks each HOT chain rooted at an index-referenced slot — a `NORMAL`
  non-`HEAP_ONLY` slot (a non-HOT row or a chain head) or an existing `REDIRECT` — and
  collapses it. A `HEAP_ONLY` `NORMAL` slot is a chain MEMBER reached only via its root's
  `t_ctid` (the H1 segment rule), never a root; **a `REDIRECT` root's target slot is also
  a chain MEMBER, not an independent root** — `classify_page_for_prune` marks it a member
  up front (it is reached only through the redirect, never via a readable
  `HOT_UPDATED → t_ctid` step). This makes a re-collapse (more HOT updates grew the chain
  from a prior collapse's redirect target, then VACUUM again) plan that chain EXACTLY ONCE
  via the REDIRECT root, so the plan never frees a slot twice or both frees and redirects a
  slot. A non-HOT row is a one-member chain, so the same logic subsumes the pre-HOT case.
  Deadness is re-derived per member via
  `common::is_dead_to_all(xmin, xmax, infomask, horizon, txn_status_view())` against the
  live CLOG.
- **Per chain (in order):**
  - **Abort truncation (F4c, chain-aware).** Where a `HOT_UPDATED` member's successor
    has an aborted creator (`XMIN_ABORTED` hint or `status(xmin) == Aborted`) — the
    rolled-back tail of an aborted HOT UPDATE, always the chain TAIL — reset that member
    **in place** (un-HOT: `xmax → INVALID_XID`, `t_ctid → INVALID_TID`, clear
    `HOT_UPDATED` + settled `XMAX_*`, preserving `xmin`/`XMIN_*`/`HEAP_ONLY`, via
    `codec::set_mvcc_header_fields`) and free the aborted successor (and anything past it,
    all `HEAP_ONLY`) straight to `UNUSED`. Leaves NO on-disk reference to the aborted txn
    (creator OR deleter) and truncates the chain before the prefix collapse.
  - **Committed-dead prefix collapse.** On the truncated chain, find `L` = the first
    member that is **not** `is_dead_to_all`:
    - **Root dead-to-all and an `L` exists** → root → `REDIRECT → L` (its index entry
      resolves via the redirect); every dead `HEAP_ONLY` member strictly before `L` →
      `UNUSED` directly (no index entry). For a `NORMAL` root, the dead head IS the root
      slot and simply becomes the `REDIRECT`.
    - **Whole chain dead-to-all** (no `L`) → root → `DEAD` (returned for F3a/F3b); every
      `HEAP_ONLY` member → `UNUSED`.
    - **`L` is the head** → nothing to collapse.
  - **Abort-cleanup of a kept root (non-HOT, F4c root-cause).** A live chain head/root
    whose own `xmax` is a **definitively aborted** deleter (`xmax != INVALID_XID` AND
    `XMAX_ABORTED` hint or `status(xmax) == Aborted`) — the surviving predecessor of a
    non-HOT aborted UPDATE/DELETE — is reset **in place** exactly as before (un-HOT shape),
    so the stamp (the only on-disk reference to the aborted txn as a deleter) does not
    survive to be misread as an implicit-committed delete after truncation (`mvcc.md`
    §5.4). VACUUM holds target `Share`, so no target writer remains and `xmax`'s status is settled — the reset
    fires only on a definitive abort, never on an in-progress xmax.
- **Apply + log.** A page with any collapse/free/dead/reset work is rewritten — the
  header resets applied **first** (before compaction relocates survivors), then the
  line-pointer rewrites (`free_slots_to_unused` → `set_redirect` → `mark_slots_dead`),
  then `page::compact` (NORMAL survivors stay byte-identical at their **stable** slot
  ids, so no index entry is touched — index-referenced slot ids are never renumbered,
  only tuple BYTES move) — and logged as a **single unconditional** `FullPageImage`
  (a compaction relocates survivors and is not expressible as a delta; the in-place
  header resets fold into the same image; never gated on `take_needs_fpi`, mirroring
  `btree::log_full_page`). The FPI's LSN becomes the page's new PageLSN. A page with no
  work is skipped entirely: no WAL record and no mutation. **`apply_prune_plan` is
  atomic:** it builds the post-prune image on a SCRATCH copy of the page and writes it
  back into the live frame only after every mutation plus the `FullPageImage` append
  succeeds; on any error the frame is left byte-identical (a valid, stale checksum), so a
  malformed plan can never corrupt the page.
- **Return.** Only **DEAD-root TIDs** are returned for index/line-pointer reclaim,
  including DEAD roots left by a crash or canceled earlier pass. Rediscovering
  them makes F2b/F3a/F3b restartable: a later pass finishes dangling-index cleanup
  before making the slot reusable. A
  `REDIRECT` root keeps a LIVE index entry (NOT returned, so F3a skips it) and heap-only
  members freed to `UNUSED` never had an entry (`freed_member_count` only).
- **Full-extent scan.** It iterates `0..BufferPool::page_count(heap_file_id)`,
  faulting each page in (resident or from disk), rather than only the resident pages
  `iter_pages` reports — an evicted page holding dead tuples must still be vacuumed,
  else GC is incomplete. It skips buffer-reported abandoned fresh-page holes and
  reads a page before taking its write latch, so an uninitialized sparse page is
  skipped without being dirtied or flushed.
- **Latching.** Per page it takes the per-heap structural latch then the frame write
  latch (lock order structural → frame → WAL) and releases both before the next page
  (never held across pages). VACUUM holds target `Share`, so no target writer runs
  during the pass; the latches keep the same lock ordering as normal
  heap mutations and make the page-level primitive safe if reused elsewhere.
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
entries from the table's **reserved identity index and every live catalog index**, so no
index entry resolves to a dead slot before the line pointers are reclaimed
`DEAD → UNUSED` (F3b). `dead_tids` are exactly `vacuum_heap`'s returned DEAD-root
TIDs, so a **`REDIRECT` root is never in the set** (it keeps a LIVE index entry that
resolves via the redirect — H1) and is therefore inherently skipped; the H3 collapse
relies on this. It is the middle phase of the live VACUUM orchestration
`vacuum` (F4a, below).

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
- **No node merging — B-link safe with concurrent scanners.** VACUUM holds target
  `Share` while `AccessShare` readers scan concurrently (plus a
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
  WAL recycling. Recovery reinstalls each changed leaf's `FullPageImage` by PageLSN
  gating, independent of the record's `txn_id`.
- It does **not** reclaim line pointers `DEAD → UNUSED` (F3b); the slots stay `DEAD`
  until that later step.

### Line-Pointer Reclaim Pass (`reclaim_line_pointers`, F3b)

`reclaim_line_pointers(schema, dead_tids: &HashSet<RowLocation>)` is the third
VACUUM phase (`mvcc.md` §9 / Milestone F3b): it flips each `dead_tid`'s heap line
pointer `DEAD → UNUSED`, freeing its slot id so a future `insert_row` can recycle
it (bounding the slot array under churn). `dead_tids` are the TIDs `vacuum_heap`
(F2b) pruned to `DEAD` and `vacuum_indexes` (F3a) has since stripped of every index
entry. It is the final phase of the live VACUUM orchestration `vacuum` (F4a, below).

- **Ordering invariant — F2b → F3a → F3b.** This MUST run only after
  `vacuum_indexes` removed every index entry for these TIDs, so an `UNUSED` slot
  has no dangling index entry. That is the safety hinge for `insert_row`'s
  `UNUSED`-only reuse: reusing a slot with a surviving index entry would let a stale
  entry resolve to the new tuple written into it (silent corruption). The `vacuum`
  orchestration (F4a, below) enforces the order by calling F2b → F3a → F3b in
  sequence. The underlying
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

### VACUUM Orchestration (`vacuum`, F4a)

`vacuum(schema, horizon) -> usize` is the live entry point (`mvcc.md` §9 / Milestone
F4a) that ties the three reclamation phases together for one table and returns the
count of heap tuples reclaimed (for the `VACUUM` command tag). It calls, **in this
mandatory order on one dead-TID set**:

1. `vacuum_heap(schema, horizon)` (F2b) — prune dead-to-all tuples to `DEAD`,
   collecting their TIDs.
2. `vacuum_indexes(schema, &dead)` (F3a) — strip every identity + catalog index entry for
   those TIDs.
3. `reclaim_line_pointers(schema, &dead)` (F3b) — flip the now entry-free line
   pointers `DEAD → UNUSED`.

When the heap scan finds neither newly dead nor pre-existing DEAD roots, the index
and line-pointer phases are skipped. Foreground cancelable variants poll the token
at heap pages, B-tree leaves, index boundaries, line-pointer pages, and TOAST-owner
scan pages. Returning between phases is safe because a partially processed root
remains `DEAD` (never reusable) and a later heap scan rediscovers it; F3b still runs
only after the current pass completes F3a for the full dead-TID set.
The order is the safety invariant: F3b must run only after F3a removed every index
entry for these TIDs, or `insert_row`'s `UNUSED`-slot reuse could resolve a stale index
entry to the new tuple (silent corruption). The server's `run_vacuum` calls this under
the **exclusive** checkpoint guard with the GC horizon captured once after the guard,
so no writer runs during the pass and the horizon accounts for every live reader
snapshot — VACUUM reclaims only versions `xmax < horizon` that no current-or-future
snapshot can see live (no data loss; see `docs/specs/crates/server.md` and `mvcc.md`
§9/§10 F4a).

### TOAST VACUUM Helpers

TOAST chunk cleanup is intentionally outside the public `StorageEngine` trait and
owned by `PageBackedStorageEngine` plus server VACUUM orchestration:

- `toast_value_ids_pending_vacuum(schema, horizon) -> Vec<u64>` scans a user table's
  heap pages, computes the same full-VACUUM prune plan as `vacuum_heap`, and decodes
  the physical v3 parent tuple bytes that the plan would remove. It extracts external
  TOAST pointers without detoasting or reading hidden chunks. It does not mutate the
  parent table.
- `delete_toast_values(ctx, base_schema, value_ids) -> usize` deletes hidden
  `(value_id, seq)` chunk rows through the normal MVCC delete path using the supplied
  transaction context. It scans the hidden relation by primary-key prefix
  `(value_id)`, validates each row's full key `(value_id, seq)`, stamps `xmax` on
  visible chunks, and relies on ordinary WAL, visibility, index, commit/abort, and
  later VACUUM behavior. It must not physically remove chunks directly.
- `vacuum_hidden_toast_relation(base_schema, horizon)` runs ordinary `vacuum` on the
  linked hidden TOAST relation. This reclaims chunks deleted by a committed TOAST
  cleanup transaction and aborted chunks whose creating transaction rolled back or was
  resolved aborted during recovery.
- Direct `vacuum(schema, horizon)` on a TOAST-enabled user table rejects the pass if
  the parent heap contains external value ids that full VACUUM would prune. This keeps
  storage-level callers from discarding the only parent bytes that identify committed
  hidden chunks needing cleanup. The server's coordinated VACUUM path must call
  `toast_value_ids_pending_vacuum`, attempt the hidden-chunk cleanup, and then use
  `vacuum_after_toast_cleanup(schema, horizon)` for the parent prune.

The server must delete visible hidden chunks in a real maintenance transaction and
commit it before parent heap pruning removes the owning tuple bytes. After that commit,
the parent table's coordinated `vacuum_after_toast_cleanup` may discard the parent
tuple, and the hidden TOAST relation can be vacuumed with a horizon that includes the
committed cleanup xid. If a chunk delete transaction aborts before commit, the parent
prune must not run for that table in that pass; a later VACUUM can retry from
still-present parent tuple bytes. If no visible chunks are deleted because the pending
parents and chunks belong to aborted transactions, the server may still use
`vacuum_after_toast_cleanup`; the hidden relation's own VACUUM reclaims those aborted
chunks by their MVCC headers.

### Legacy MVCC Delete Entry Point

The executor's production DELETE path resolves and requalifies a locked identity,
then uses `delete_locked` as described above. The compatibility entry point
`delete(ctx, table, key)` marks the **visible** version of `key` deleted in place
rather than tombstoning it (`mvcc.md` §3.2 invariant 1):

1. **Locate the visible version.** The reserved identity index may carry an entry
   per version, so `delete` collects the candidate TIDs (`scan_key(key)`), decodes
   each tuple's physical header, and selects the one visible to the statement's snapshot
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
   `Conflict` (another committed transaction has already claimed this version's
   `xmax`) it returns `SqlState::SerializationFailure` (`40001`) **without appending
   the `HeapUpdateHeader` or stamping** — the winning writer's `xmax` stands. If the
   holder is still in progress, the caller drops any higher-level latch, blocks on
   that transaction, and rechecks the current header: committed ⇒ `40001`, aborted
   ⇒ proceed. On `Proceed` (no deleter, the deleter aborted, or it is this txn's
   own lock) it appends a `HeapUpdateHeader { file_id, page_num, slot, xmax =
   ctx.txn_id, t_ctid = INVALID_TID, infomask = unchanged }` (or a `FullPageImage`
   on the page's first touch since the last checkpoint) and applies
   `page::set_tuple_header` with that record's LSN. The read-classify-stamp sequence
   is atomic on the frame latch, so two concurrent writers racing for this version
   serialize on the latch and the loser observes the winner's `xmax` (no TOCTOU).
   `t_ctid` stays `INVALID_TID` (a delete has no successor) and `infomask` is
   carried through unchanged (no hint bits set here — that is the optional commit
   10). The line pointer **stays `NORMAL`**: the tuple is physically present and is
   hidden purely by visibility once the deleter commits.
3. **Retain index entries.** No identity-index or catalog-index entry is removed.
   The dead version and its entries linger until VACUUM (Milestone F) reclaims them.

This is the first point internal state diverges from a single-version heap: a
deleted tuple and its index entries persist (the accepted interim cost). External
SQL behavior is unchanged — a committed `DELETE` then `SELECT` does not see the row
— and **delete-then-reinsert of the same key now succeeds**, because the
committed-deleted version no longer blocks the re-insert (the uniqueness check
ignores committed-deleted/aborted versions). On abort, the page is not physically
undone: the tuple may retain `xmax = aborting_txn`, but the CLOG reports that
transaction as `Aborted`, so visibility treats the delete as non-effective and the
row remains visible. Since no index entry was removed, no index repair is needed.
Recovery replays the `HeapUpdateHeader` redo (PageLSN-gated), so a committed delete
stays hidden and an aborted one (no durable `Commit`) leaves the row visible.

### MVCC Update

`update(ctx, table, key, row)` writes a **new heap version** and chains the old
one to it (Postgres-style, non-HOT; `mvcc.md` §3.2 invariants 1, 3, 5). A
primary-key change is allowed and is treated as an indexed-column change: it is
not HOT-eligible, the replacement storage identity is inserted, and primary-key
uniqueness is checked before the update succeeds. The flow is ordered for correct
uniqueness:

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
   atomic `write_conflict` check under the frame latch before any WAL append (step
   2 above): if another committed transaction already claimed the old version's
   `xmax`, the update returns `SqlState::SerializationFailure` (`40001`); if the
   holder is still in progress, the caller waits and rechecks.
   The line pointer stays `NORMAL`; `infomask` is carried through. This stamping
   happens *before* the new version's uniqueness checks, so the old version reads as
   own-deleted (`xmax == ctx.txn_id`) and does not falsely self-conflict. Because
   the new version (step 2) was written *before* this stamp, a final `40001` here
   leaves a transient **orphan**: the new heap tuple (the per-version index entries
   of step 4 below have not run yet, so only the tuple is orphaned). No manual
   cleanup is needed — the error aborts the transaction, so the orphan (xmin = the
   aborting txn) becomes invisible via CLOG = Aborted and is reclaimed by VACUUM
   (Milestone F). (An early pre-write conflict check to avoid the transient orphan is
   a deferred optimization; the authoritative check stays atomic at stamp time.)
4. **Insert per-version entries into all indexes.** A new `(identity_key, new_tid)`
   entry is inserted into the reserved identity index and a new `(index_key, new_tid)`
   entry into **every** catalog index — changed-column or not. Because reads do not walk
   `t_ctid` (every version is independently indexed; one entry per version), the new
   TID needs its own entry in every index, or a scan on an unchanged secondary value
   would find only the superseded old version's entry. Skipping unchanged-column
   indexes is a HOT optimization (Milestone H) and would be a correctness bug here.
   Unique indexes (PK and unique secondary) run the visibility-aware
   `unique_conflict_kind` check: a value unchanged from the old version does not
   self-conflict (the old version is own-deleted), but a value colliding with a
   *different* live row raises `UniqueViolation`; if the only holder is an
   in-progress inserter, the writer waits for that transaction and rechecks
   (§7.3).
5. **Retain all old entries.** No old index entry — PK or secondary — is removed.

After a committed `UPDATE`, both versions coexist in the heap: the old version
(`xmax = txn`, `t_ctid → new`, invisible to later snapshots) and the new live
version, with every old index entry lingering until VACUUM (Milestone F) reclaims
the dead version and its entries. External SQL is unchanged: a later snapshot sees
the new value via a sequential scan, an index scan on the changed column, and a
scan on an unchanged secondary value (the new version's entry resolves all three).
An older snapshot that predates the update still resolves the old version through
its retained entries. On abort (statement error → autocommit rollback), the page
bytes are not physically undone: the new tuple and its index entries may remain,
and the old version may retain `xmax = aborting_txn`, but the CLOG reports that
transaction as `Aborted`. Visibility therefore skips the new aborted version and
treats the old version's aborted `xmax` as non-effective, leaving the old value
visible; VACUUM later reclaims the orphaned aborted version and entries. Recovery
replays the new tuple's `HeapInsert`/`FullPageImage`, the old version's
`HeapUpdateHeader`, and the new index-entry page images (all PageLSN-gated), so a
committed update's new value survives restart and an aborted one leaves the old
value.

## Row Serialization

```text
[row_format_version: 1 byte][infomask: 2][xmin: 8][xmax: 8][t_ctid: 6][null_bitmap][col1_data][col2_data]...
```

- `row_format_version`: ordinary INSERT and non-HOT UPDATE emit prepared row
  format `3`; the legacy `encode_row` helper still emits v2 for tests and
  compatibility helpers. `decode_row` accepts legacy
  `1` tuples (which omit the MVCC header —
  `[version=1][null_bitmap][columns]`), v2 tuples, and v3 tuples whose varlena
  columns are physically plain. All other versions are rejected as corrupt. V3
  compressed/external varlena payloads are exposed by the storage-private
  physical decoder until storage read paths materialize them.
- MVCC tuple header (v2 and v3 only), all little-endian:
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
- `Text`, `Bytea`, and arrays in v1/v2: 4-byte length prefix plus logical bytes.
- `Text`, `Bytea`, and arrays in v3: the same 4-byte little-endian length word uses the
  top two bits as a physical tag and the low 30 bits as `stored_len`:
  - `00 PLAIN`: `stored_len` bytes are the raw logical bytes. This is
    byte-identical to v2 for every supported plain value; v3 plain rows add no
    per-column overhead beyond the row version byte.
  - `01 COMPRESSED`: `stored_len` bytes are
    `[codec:u8][dict_id:u32 LE][raw_len:u32 LE][raw_crc32:u32 LE][payload]`.
    `codec = 1` means zstd with `dict_id = 0`; `codec = 2` means zstd with a
    nonzero `dict_id`. `codec = 0` is invalid for inline compressed values.
    `raw_crc32` is IEEE CRC-32 over the uncompressed logical bytes and is
    preserved in physical decode for later detoast validation.
  - `10 EXTERNAL`: `stored_len` must be exactly `17`, and the bytes are
    `[value_id:u64 LE][raw_len:u32 LE][stored_len:u32 LE][codec:u8]`.
    The base table schema supplies the hidden TOAST relation id; the pointer
    stores only the value id within that relation and the external stream
    metadata. Pointer `codec` is `0` for raw external streams, `1` for zstd, or
    `2` for zstd with a dictionary. The 17-byte pointer intentionally has no
    dictionary-id slot; dictionary-compressed streams store the dictionary id in
    the stream header so the pointer stays fixed-width.
  - `11`: reserved; decoding returns a corruption-class storage error.
  The low 30-bit length limit (`2^30 - 1`) is the supported v3 varlena length
  cap; encode attempts above it return `SqlState::ProgramLimitExceeded`.
  Decode attempts that find persisted v3 varlena metadata above this cap return
  a corruption-class storage error.
- Array logical bytes are versioned independently of the row envelope:
  `[version=1][element_type][ndim:u8][cardinality:u32 LE]`, followed by
  `ndim` pairs of `[length:u32 LE][lower_bound:i32 LE]`, a one-bit-per-element
  null bitmap, and row-major non-null scalar element payloads. Element type tags
  `0..12` cover, in order, integer, text, boolean, date, timestamp, time,
  timestamptz, interval, bytea, uuid, double, real, and numeric; numeric adds
  `[precision:u32 LE][scale:u32 LE]`, with `u32::MAX` meaning unconstrained
  precision. Text/bytea elements have `u32` lengths; other scalar encodings match
  their row encodings. Array payloads reject unknown versions/tags, impossible
  shapes, type mismatches, truncated data, bitmap padding bits, and trailing
  bytes as corruption. The same complete payload, prefixed by a `u32` length,
  is B-tree key value tag `14`; existing key tags `0..13` are unchanged.
  Cardinality above `common::MAX_ARRAY_ELEMENTS` is rejected before allocating
  element storage. A complete durable array payload is capped at 64 MiB; encode,
  decode, and TOAST materialization validate that cap before payload allocation,
  decompression, or hidden-chunk reads.
- External TOAST stream bytes, stored in the hidden TOAST relation's chunk rows,
  are self-describing after consulting the pointer codec:
  - `codec = 0`: `[raw_crc32:u32 LE][raw bytes]`
  - `codec = 1`: `[raw_crc32:u32 LE][zstd payload bytes]`
  - `codec = 2`: `[dict_id:u32 LE][raw_crc32:u32 LE][zstd-dict payload bytes]`
    with nonzero `dict_id`
  `raw_crc32` is IEEE CRC-32 over the uncompressed logical bytes. The pointer's
  `stored_len` is the total external stream length including this stream header.
  Storage splits external streams into hidden-relation chunk rows with
  `TOAST_CHUNK_PAYLOAD = 1900` bytes per chunk. The chunk size is chosen to keep
  one v3 row `(value_id, seq, data BYTEA)` with a full chunk on a fresh 8 KiB heap
  page including line-pointer overhead. Stream writes require the base schema's
  `toast_table_id`, allocate a monotonic `value_id`, and insert chunks under the
  caller's transaction with contiguous `seq` values starting at `0`. Stream reads
  scan the hidden relation by primary-key prefix `(value_id)`, require visible
  chunks to be contiguous and in order, concatenate `data`, and verify the byte
  length equals the pointer's `stored_len`. Missing, duplicate, out-of-order, or
  mismatched chunks are corruption-class storage errors. Detoast then decompresses
  when needed, verifies `raw_len`, and verifies `raw_crc32`.
- `Boolean`: 1 byte.
- `Null`: bit set in null bitmap, no bytes.

Serialization uses catalog `TableSchema` column order.

## Storage Identity Index

Each table has a durable, non-clustered B+-tree mapping `Key -> RowLocation`, in
its own file. The heap file id is the table schema's current `storage_id`; the
primary-key/identity index file id is that same generation id with the high bit
set, so it never collides with the heap file. `HeapPageStore` writes it to
`<data>/heap/<storage_id>.idx`. For tables with a primary key, the key is the
logical primary-key tuple. For tables without a primary key, the key is a hidden
heap identity derived from the row's root TID. Rows stay in the heap; the tree
replaces the former in-memory directory.

`ALTER TABLE ... ADD/DROP PRIMARY KEY` changes which key the identity tree uses.
Before commit, storage validates the existing heap under the exclusive
maintenance guard: primary-key adds reject NULL key values, duplicate live keys,
and live HOT chains whose versions would produce different new identity keys
(`SerializationFailure`, retry after the chain becomes dead-to-all or is vacuumed).
Committed-deleted predecessor versions that are still retained for older readers
may share the future primary-key value with their successor; they are indexed for
visibility but do not count as duplicate live keys.
After the DDL commit is durable, the identity tree is reset and rebuilt from heap
rows under the table's identity rewrite gate, so concurrent identity scans cannot
observe the transient empty or partially rebuilt tree. In normal execution the
reset and inserts log full-page-image redo for the identity B-tree and the server
flushes that WAL before checkpoint replay-floor advancement can run. The committed generic
catalog change carrying the table and backing-index replacement is the recovery
source of truth when the
crash happens before a checkpoint containing the rebuild completes: recovery
installs primary-key metadata while replaying WAL, defers the derived identity
rebuild until after all retained WAL records have replayed and crashed writers
have been marked aborted, and performs that recovery-only rebuild without
appending WAL.

### Multi-entry ordering

The B-tree is a **multi-entry** structure ordered by the composite `(key, value)`
where `value` is the leaf value (the `RowLocation` for the identity index).
**Duplicate logical keys are allowed**, disambiguated and ordered by their value
bytes (the `IndexValue::encode` form, compared as raw little-endian bytes — a
stable total order, not necessarily numeric). The tree no longer rejects duplicate
keys structurally; **primary-key uniqueness is an engine-level check** for tables
that declare one (see Error Handling and the note below). This is the
index-per-version substrate (`mvcc.md` §3.2 invariant 3): the identity index stores
one `RowLocation` per physical tuple version, so old versions keep their entries
until VACUUM removes the dangling TIDs.

- **API.** `insert(txn_id, key, value)` inserts one `(key, value)` entry (duplicate
  keys allowed). `remove(txn_id, key, value)` removes the single matching
  `(key, value)` entry, leaving other entries that share the key intact.
  `scan_key(key)` returns every value whose key equals `key`, in `(key, value)`
  order. `search(key)` returns the first (lowest-value) entry for a key and is
  only a structural helper; MVCC lookup paths use `scan_key` plus visibility to
  choose the visible version. `range(range)` walks keys in order and may yield
  multiple values per key. The old in-place `update` operation is removed:
  storage updates write a new tuple version, retain the old index entries, and
  insert new per-version entries for the new TID.
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
  the right child's first `(key, value)`). Leaf inserts also verify the right
  sibling when the chosen leaf's lower bound lands at the end; if a previously
  failed parent/root separator append left a leaf split reachable only through the
  leaf chain, the insert chases right rather than appending into the stale left
  leaf and breaking key order.
- **Delete.** Removes the specific `(key, value)` entry; underfull nodes are not
  merged (accepted bloat).
- **Update.** MVCC row updates retain old index entries and insert a new
  per-version entry for the new heap TID in every relevant index. The B-tree
  `remove(key, value)` primitive remains available for maintenance/VACUUM-style
  exact-entry removal, but normal DML does not call it. A row update that changes
  the primary key falls back to the non-HOT path and is rejected only if the
  replacement key violates uniqueness.
- **Crash safety.** Every node mutation logs a `FullPageImage` and stamps the
  page-LSN, so the index is recovered by the same redo path as the heap and needs
  no rebuild. Mutations are staged in scratch page images and copied into the
  live frame only after the matching WAL append succeeds, so a failed append does
  not leave unlogged index bytes in memory. If a fresh node's first image append
  fails before bytes are published, the unpublished page allocation is abandoned in
  the buffer pool: its resident frame is removed, tail high-water rolls back when
  possible, and an interior abandoned page number is reused before the file grows.
  During an internal split, the new right node is logged first, then the old
  internal node is logged with a fence separator that points at the new right node
  before any parent (or root/metapage) separator is exposed. If the parent/root
  update later fails, the stale parent still routes into the old node and that
  fence reaches the new right subtree; if the parent/root update succeeds, the
  fence is redundant but harmless because probes at or beyond the separator route
  directly to the right node from the parent. That ordering keeps every committed
  prefix of the split sequence searchable without any post-parent deferred page
  rewrite. The node layout is unchanged, so recovery replays these full-page
  images exactly as before. Page allocation is seeded from each file's on-disk
  extent so a new node never reuses an existing page after recovery.
- **Keys.** Keys are stored in a self-describing byte form and ordered by decoding
  to `Key` and comparing with `Ord`; equal keys are then ordered by their raw
  value bytes.

**Primary-key uniqueness (visibility/CLOG-aware liveness check).** Because the tree
no longer rejects duplicate keys, the engine `insert` enforces uniqueness for
tables with a primary key using a shared visibility-aware check
(`unique_conflict_kind`): it `scan_key(pk)`s the identity index and, for each
candidate TID, reads the *physical* tuple header and classifies that version
(`common::classify_unique_conflict` →
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
- **`WouldBlock(txn)` ⇒ wait and recheck** — the version is alive but only
  *potentially* a duplicate: its creator is **another in-progress transaction**
  that has not committed and may yet abort. Uniqueness is undecidable until that
  transaction finishes, so the writer drops the structural latch, waits on the
  creator (`docs/specs/deadlock.md`), then rechecks: committed ⇒ `23505`, aborted
  ⇒ no conflict.

`unique_conflict_kind` returns the **strongest** conflict across all candidates
(precedence `Violation > WouldBlock > None`): a single committed-live duplicate is a
definite `23505` even if another candidate is only in-flight. A DEAD/UNUSED line
pointer contributes no conflict. Once versioning stamps `xmax`/writes aborted
versions, a dead version with the same key no longer blocks a re-insert.

The B-tree is generic over its leaf value type, but every index — identity and
catalog — stores a fixed-width `RowLocation` (heap TID), so all indexes are
uniform (see Catalog Indexes). Internally the tree treats values as opaque bytes
and uses them as the equal-key tiebreaker.

## Catalog Indexes

A table may have any number of catalog indexes, including the primary-key
constraint index when the table declares a primary key. Each is its own durable B-tree
in its own file, tagged with the top two file-id bits (distinct from the heap and
the identity index) and written to `<data>/heap/<storage_id>.sidx`, where
`storage_id` is the secondary index schema's current physical generation. Index
ids are stable logical catalog identities; storage ids name the current physical
files and may change on relation-swap truncate.

- **Entry layout.** A catalog index stores `indexed_columns -> RowLocation`
  (heap TID), uniform with the identity index — every index is `(key → heap TID)`.
  Reads go catalog index → `RowLocation` → heap, with no identity-index
  indirection. (Previously secondary indexes stored the primary key and reads
  chained through the storage identity index; that indirection is removed.)
- **Key shape.** The catalog-index key is the encoded indexed column(s) alone; the
  primary key is no longer embedded. Duplicate indexed values (including multiple
  rows whose indexed value is NULL) coexist as ordinary multi-entry rows,
  disambiguated by the trailing heap TID in the tree's `(key, tid)` ordering. A
  unique catalog index enforces uniqueness through the **same shared
  visibility/CLOG-aware liveness check** the identity index uses
  (`unique_conflict_kind` / `common::classify_unique_conflict`): it conflicts only
  with an alive-or-potentially-alive version of the key, ignoring dead
  (creator-aborted) and committed-deleted versions. For a non-NULL indexed value it
  returns `SqlState::UniqueViolation` (`23505`) when a committed/own/frozen-live
  duplicate exists. If the only conflicting non-NULL value is held by another
  in-progress inserter, the writer waits for that transaction and rechecks instead
  of returning a duplicate verdict from an undecidable state. The check is
  **skipped for a NULL indexed value**: SQL treats NULLs as distinct, so NULL never
  participates in a unique constraint, and distinct NULL rows coexist naturally via
  their differing heap TIDs.
- **Lookup / range.** `index_scan(table, index, range)` constrains the leading
  indexed columns; the range bounds hold exactly those columns, and comparison
  ignores each stored key's trailing TID tiebreaker (the leaf value). An equality
  bound thus matches every row sharing the indexed value, and an inclusive upper
  bound includes all of its rows. Results are returned in index order, each read
  directly from the heap at its TID. The `StoredRow.key` is recovered as the
  logical primary key for tables with one, or as the hidden heap identity for
  tables without one. A committed dropped index remains physically scan-readable
  for statements that planned before the drop; it is excluded from `table_indexes`
  and therefore no longer maintained by later writes. A rolled-back index create
  removes storage metadata entirely and remains unscannable.
- **Entry size.** Before descending or mutating pages, B-tree insertion rejects
  an encoded `(key, value)` that cannot fit in a fresh leaf node or whose future
  internal separator could not fit in a fresh internal node. The error is
  `SqlState::ProgramLimitExceeded` (`54000`) because the limit is user-data
  dependent, not page corruption.
- **Maintenance.** `insert` adds an entry to every index. `delete` removes **no**
  entry — it stamps the deleted version's `xmax` in place and retains its entries
  (VACUUM reclaims them; see MVCC Delete). `update` likewise removes no old index
  entry; it inserts a new per-version entry into every index for the new heap TID,
  while old entries linger until VACUUM. A unique-index conflict during `insert` or
  `update` returns `SqlState::UniqueViolation` for a committed-live duplicate; when
  the key is held only by another in-progress inserter, the writer waits for that
  transaction and rechecks (§7.3).
- **Create / drop.** `create_index` appends the logical WAL record, builds an
  empty tree, and backfills it by scanning the live rows through the primary-key
  index (a duplicate value for a unique index fails the build with
  `UniqueViolation`). Storage publishes the new `IndexGeneration` only after
  the build succeeds and records rollback metadata before publication; until
  publication, `index_scan` reports the index unavailable for snapshots that do
  not contain it.
  `drop_index` marks the index dropped and leaves its pages in place (accepted
  bloat, like `drop_table`). `drop_table` (and its recovery replay) cascades to
  mark the table's catalog indexes dropped too; when the table has a hidden
  TOAST relation, the hidden relation and its catalog indexes are marked
  dropped as metadata as well. This keeps storage's table/index set consistent
  with the catalog's drop-table cascade. The engine learns a table's live
  indexes from the installed index schemas (`install_index_schemas`) plus
  in-session creates.
- **Crash safety.** Like the identity index, every catalog-index node mutation
  logs a `FullPageImage` and stamps the page-LSN, so index pages recover through
  the same redo path as the heap. Index *metadata* (which indexes exist) is made
  durable by generic `CatalogChange` WAL — replayed into both catalog and
  storage metadata — plus the catalog snapshot at each checkpoint.

## Sequence Runtime

The page-backed engine implements `common::SequenceManager`. `create_sequence`
installs a `SequenceSchema` in storage's sequence map; `drop_sequence` removes
it. `nextval(txn_id, sequence)` validates that the sequence exists, computes the
next value from `(last_value, is_called, increment, min_value, max_value, cycle)`,
appends and flushes `SequenceAdvance { sequence, value }`, updates the live
state, and returns the value. `setval(txn_id, sequence, value, is_called)`
range-checks the value, appends and flushes `SetSequenceValue`, updates live
state, and returns the supplied value. `sequence_exists(sequence)` checks the
runtime sequence map without advancing the sequence or writing WAL; executor
`currval` uses it so prepared statements do not return values for dropped
sequences. These value changes are non-transactional and are not restored by
`rollback_txn`.

## Heap Page Store

`HeapPageStore` is the mutable page home for in-place dirty-page flushing. It
implements `buffer::PageStore` over one file per relation generation: table and
TOAST heaps at `<data>/heap/<storage_id>.heap`, primary-key indexes at
`<data>/heap/<storage_id>.idx` (file ids carry the high bit), and secondary
indexes at `<data>/heap/<storage_id>.sidx` (file ids carry the top two bits),
storing page `n` at byte offset `n * PAGE_SIZE` with positioned reads/writes.
`load_page` returns a complete page or `None`
(missing file or beyond-EOF / short tail); `write_page` writes in place without
fsync; `sync_all` fsyncs all open files and the directory; `page_count` returns a
file's on-disk extent in pages, used to seed page allocation after recovery.
`HeapPageStore::open(dir)` opens with a fresh, default (all-raw)
`compress::CompressionRegistry`; `HeapPageStore::open_with_compression(dir,
registry)` opens sharing a registry instance with the caller — the server
constructs one `CompressionRegistry` and passes it to both the `HeapPageStore`
(at-rest envelopes) and `PageBackedStorageEngine::open_with_compression` (WAL
FPIs), so a file's config is consulted consistently by both paths (see
At-Rest Page Compression).

`apply_physical_redo(page, lsn, kind)` replays one physiological redo record
(`HeapInit`/`HeapInsert`/`HeapDelete`/`HeapUpdateHeader`/`FullPageImage`) onto a page buffer, gated by
the page-LSN: a record whose effect is already present (`page_lsn(page) >= lsn`) is
skipped, making replay idempotent. `FullPageImage` is validated to be exactly
`PAGE_SIZE` bytes before install. Recovery uses it to redo every physical page
mutation after the checkpoint LSN, regardless of the dirtying transaction's
outcome; the CLOG decides whether replayed versions are visible. A WAL
`FullPageImageCompressed` record is normalized to a decompressed raw
`FullPageImage` by the caller (`server`) before it reaches
`apply_physical_redo`, so this function itself only ever sees raw `PAGE_SIZE`
images (see At-Rest Page Compression, and `docs/specs/crates/wal.md`).

## At-Rest Page Compression

`HeapPageStore` transparently compresses page slots per-file using
`saguarodb-compress`'s codec, envelope, and dictionary machinery
(`docs/specs/compression.md`). None of `PageStore`/`PageLoader`, the buffer
pool, or any code above the store is aware of this — every method still reads
and writes exactly `PAGE_SIZE` logical bytes; compression is folded entirely
into `write_page`'s encode step and `load_page`'s decode step.

- **Envelope detection on load.** `decode_slot` reads the raw on-disk slot,
  then hands it to `CompressionRegistry::decompress_page`: `Ok(None)` (not an
  envelope — a raw page or an all-zero sparse hole) returns the raw bytes as
  today; `Ok(Some(image))` returns the decompressed `PAGE_SIZE` image; `Err`
  (a structurally invalid envelope, an unresolvable dictionary, or a
  decompressed length other than `PAGE_SIZE`) is the corruption case (below).
- **Write path.** `write_page` asks `CompressionRegistry::compress_page_at_rest`
  for the file's envelope. When it returns `None` (no config, or the envelope
  would not be smaller than a raw page), the full raw image is written exactly
  as before. Otherwise the smallest whole number of filesystem blocks
  (`FS_BLOCK_SIZE = 4096`, a conservative assumed allocation quantum — see
  `docs/specs/compression.md` §5 and §12 for the page-size-agnostic framing)
  needed to hold the envelope is computed; if that is fewer blocks than the
  page's full slot, the envelope is written **zero-padded out to a full
  `PAGE_SIZE` slot** at the page's normal offset, and only then are the
  trailing blocks punched with `fallocate(FALLOC_FL_PUNCH_HOLE |
  FALLOC_FL_KEEP_SIZE)`. Writing the full slot before punching — rather than a
  short write of just the envelope — is what keeps `st_size` (and so
  `page_count = st_size / PAGE_SIZE`, allocator seeding, and VACUUM's
  full-extent scan) exactly right even when the page being written is the
  file's current tail; a short write there would under-report the extent.
  `KEEP_SIZE` means the punch never changes `st_size`. If the envelope would
  not fit in fewer blocks than the raw slot, the raw image is written instead
  (which naturally un-punches any earlier hole at that offset).
- **Hole punching is best-effort and latches off.** `punch_hole` never fails
  the write: an `EOPNOTSUPP`/`EINVAL` `fallocate` result is recorded once (an
  `AtomicBool` on the store) and punching is skipped thereafter for that
  store — correct either way, since a skipped punch merely reclaims nothing
  and the length-delimited envelope decode never reads the stale trailing
  bytes. Punching is a no-op on non-Linux targets.
- **`open_with_compression` and config registration.** A `HeapPageStore`
  constructed via `open_with_compression` shares one `CompressionRegistry`
  with the storage engine. The engine registers each file's config
  (`register_table_compression`) whenever schemas are installed at
  startup/recovery (`install_schemas`, `install_index_schemas`), on `CREATE
  TABLE`, on `CREATE INDEX`, and on `ALTER TABLE ... SET (compression)`: the
  heap file gets `(codec, active_dict_id)` from the table's `compression`
  setting, and every index file for that table (the identity index and every
  catalog index) gets the SAME codec but **never** the heap's trained
  dictionary — a heap-trained dictionary does not fit B-tree node content, so
  index files always compress dict-less (or not at all). A file with no
  registered config always writes/reads raw.
- **Strict vs. lenient loads.** `PageLoader::load_page` is strict: an invalid
  envelope is a loud, structured corruption error, exactly like any other page
  corruption on a normal read/write path. `PageLoader::load_page_lenient` (see
  `docs/specs/crates/buffer.md`) reports the same failure as an absent page
  instead. Only recovery redo (`BufferPool::fetch_for_redo`) uses the lenient
  form: a torn compressed envelope is exactly like a torn raw page mid-write —
  it was dirty, so its first post-checkpoint modification logged an FPI that
  redo will replay — so treating it as a zeroed missing frame is sound and
  strictly better than trusting a torn raw page's garbage bytes and garbage
  PageLSN.
- **`fpi_record_kind` policy at the FPI sites.** Every call site that logs
  a WAL full-page image builds its record through
  `engine::fpi_record_kind(compression, file_id, page_num, image)`, which asks
  `CompressionRegistry::compress_fpi` (unconditional — independent of the
  file's at-rest config) and emits `WalRecordKind::FullPageImageCompressed`
  when it shrinks the image, `WalRecordKind::FullPageImage` (raw) otherwise —
  self-describing per record, so the WAL never expands. The five steady-state
  DML/VACUUM sites are:
  `BTree::log_full_page` (every B-tree node mutation — the identity index,
  every catalog index, and index vacuum's leaf rewrite all share this one
  function), `log_insert` (a heap row's first-touch-since-checkpoint FPI),
  `stamp_xmax_logged` (the `UPDATE`/`DELETE` in-place `xmax`/`t_ctid` stamp's
  first-touch FPI), `apply_prune_plan` (the heap-prune VACUUM pass, F2b/H3),
  and `reclaim_line_pointers` (the line-pointer-reclaim VACUUM pass, F3b). The
  `ALTER TABLE` rewrite (`rewrite_table_pages`, below) is a sixth caller of the
  same helper, logging one FPI per re-encoded page.
- **`set_table_compression(schema)`.** Installs an ALTERed schema into the
  live `TableState` and re-registers the heap file's config plus every live
  secondary-index file's config (still dict-less) under the new setting. Pure
  in-memory bookkeeping — it appends no WAL and takes no page latch; the
  caller (the server's `ALTER TABLE` handler, or generic catalog-change recovery
  through `apply_update_table_schema`) owns WAL record emission and ordering
  (`docs/specs/compression.md` §8, and `docs/specs/crates/server.md`).
- **`sample_heap_pages(schema, cap)`.** Returns up to `cap` **heap-only**
  initialized page images, evenly sampled across the heap file's current
  extent (`page::is_initialized`, the `PAGE_TYPE_DATA` check). Used by `ALTER
  TABLE ... SET (compression = 'zstd')` to build a dictionary-training corpus.
  The caller holds target `AccessExclusive`, so sampled images are stable; an
  abandoned fresh-page hole is skipped without being faulted in. Foreground
  compression ALTER polls the statement token at each sampled page.
- **`sample_toast_values(ctx, schema, max_samples, max_bytes)`.** Returns
  bounded logical `TEXT`/`BYTEA`/array-payload samples for TOAST value-dictionary
  training. Array samples retain the versioned durable payload and are decoded
  and element-type checked before admission, regardless of whether the stored
  value was plain, inline-compressed, or external.
  It walks heap pages directly instead of calling the public scan iterator so
  `max_samples`/`max_bytes` bound memory on large tables. Each tuple's MVCC header
  is decoded first and filtered through the normal visibility predicate using `ctx`;
  only visible rows are then decoded as full physical rows. Inline raw values may be
  truncated to the remaining byte budget. Inline compressed and external TOAST values
  contribute their full logical bytes only when their declared logical size fits the
  remaining byte budget; oversized compressed/external values are skipped before
  decompression or hidden-chunk reads. Invisible rows are skipped without decoding
  varlena bodies or reading hidden chunks. Empty values and non-toastable columns are
  skipped. The scan polls `ctx.cancel` at heap-page and row boundaries, so sparse or
  mostly-unsuitable tables remain responsive to statement cancellation. The server
  calls this under target `AccessExclusive`, the catalog-DDL mutex, and the shared
  writer guard for `ALTER TABLE ... SET
  (toast_compression = zstd_dict)`.
- **`rewrite_table_pages(schema)`.** Re-encodes every **initialized** page —
  heap AND index (`page::is_any_page_initialized`, which accepts both
  `PAGE_TYPE_DATA` and `PAGE_TYPE_INDEX`, unlike the heap-only check
  `sample_heap_pages` uses) — of the table's heap file, identity-index file,
  and every live catalog-index file, across each file's full current
  extent, skipping abandoned fresh-page holes. For each such page, under that
  file's structural latch and the page's buffer-pool write guard, it captures
  the current image, logs it as a single unconditional
  `FullPageImage`/`FullPageImageCompressed` under the maintenance txn id
  (`VACUUM_TXN`), and stamps the FPI's assigned LSN as the page's new
  PageLSN — exactly the `vacuum_heap`/`reclaim_line_pointers` pattern
  (`docs/specs/compression.md` §8). Logical bytes are unchanged; only the
  page-header PageLSN (and its checksum) advances. This is what makes a torn
  write during the caller's subsequent page flush repairable by redo
  replaying the page's own FPI, instead of the WAL-free "just dirty it"
  version this once was. Returns the number of pages touched (and so the
  number of FPIs logged). The caller (`ALTER TABLE`) must flush the WAL
  (write-ahead of the page writes) before flushing the buffer pool and
  fsyncing the store so every dirtied page is actually re-encoded under the
  new config (see `docs/specs/crates/server.md` and
  `docs/specs/compression.md` §8) — `flush_dirty_pages` itself does not gate
  on PageLSN (it passes `page_lsn: None` and assumes the caller already made
  the WAL durable), so skipping this flush would not error; it would let a
  torn page write precede its FPI being durable, i.e. silent corruption on
  recovery. It returns `RewriteTablePages { pages_touched, file_ids }`, where
  `file_ids` is the deduplicated heap/identity/catalog-index set visited. The
  caller holds target `AccessExclusive` and uses file-scoped
  flush/sync/clean operations for exactly the rewritten heap/index ids. Unrelated
  writers remain concurrent and their frames are untouched.
- **Corruption semantics.** An envelope validation failure is a distinct
  structured corruption-class error (`SqlState::InternalError`), never
  confused with "this is a raw page." A normal `load_page`/`write_page` fault
  propagates it loudly, like any other page corruption. `fetch_for_redo` maps
  it to a zeroed frame via `load_page_lenient` instead, relying on the
  post-checkpoint `FullPageImage` to re-establish the page (see "Strict vs.
  lenient loads" above).

## WAL Interaction

Normal data operations append physiological redo records as they mutate pages, stamping the page-LSN with each record's LSN:

- A row insert logs `HeapInsert { file_id, page_num, slot, row_bytes }`, or a `FullPageImage` if this is the first modification of the page since the last checkpoint (torn-page protection). A fresh page first logs `HeapInit`.
- An MVCC row delete logs `HeapUpdateHeader { file_id, page_num, slot, xmax, t_ctid, infomask }` to stamp `xmax` in place on the still-`NORMAL` line pointer (or a `FullPageImage` on first touch); it does not tombstone (see MVCC Delete). An MVCC row update writes a new tuple version through the normal insert/heap-write WAL path, stamps the old version's `xmax`/`t_ctid` with `HeapUpdateHeader` or `FullPageImage`, and inserts new per-version index entries without removing old ones.
- Each identity or catalog index node mutated during the operation logs a `FullPageImage` of that node (the indexes use full-page-image redo throughout). `create_table` initializes the identity index, and `create_index` initializes and backfills a catalog index, logged the same way.
- The caller appends and flushes one generic `CatalogChange` before invoking schema
  operations. It may atomically carry tables, hidden relations, indexes,
  sequences, views, schemas, and statistics. Schema operations append only the
  dependent physical page WAL required for relation/index construction.
- `SchemaOperations::update_table_schema` initializes replacement B-tree pages
  only after the caller has appended the change containing fresh rewrite storage
  ids. Recovery applies committed table/index objects through
  `RecoveryOperations` without appending WAL; physical replacement pages are
  restored from normal page redo records.
- `SequenceManager::nextval` / `setval` log `SequenceAdvance` /
  `SetSequenceValue` and flush that WAL before the live value changes. Recovery
  replays these value records unconditionally against storage's installed
  sequence state.
- Every full-page image storage logs — heap or index, DML or VACUUM — goes
  through `fpi_record_kind`, which compresses it unconditionally and logs
  `FullPageImageCompressed` in place of `FullPageImage` whenever that shrinks
  the record (see At-Rest Page Compression). This is independent of whether
  the page's own file is configured to compress at rest.

Server query orchestration appends `Commit` and flushes WAL after the statement succeeds. Storage should not append commit records.

## Recovery Mode

The storage engine can be initialized in recovery mode. In recovery mode:

- Normal `StorageEngine` methods are not used.
- Row recovery is physiological page redo: the server drives `apply_physical_redo` over every physical page-mutation record, PageLSN-gated and idempotent. Committed `CatalogChange` objects are installed in LSN order and relation/index/sequence replacements are reflected through `RecoveryOperations`.
- Sequence value records replay via `RecoveryOperations` regardless of CLOG
  status because sequence advancement is non-transactional.
- No WAL append occurs.
- The identity and catalog indexes are durable on disk, so their pages are recovered by the same redo (full-page-image records) as the heap; there is no in-memory directory to rebuild. Which catalog indexes exist is reinstalled from the catalog at startup (`install_index_schemas`).

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

    /// Open sharing `compression` with the caller's `HeapPageStore` (the
    /// server injects the SAME instance into both, `docs/specs/compression.md`
    /// §5a). `open` is equivalent to this with a fresh, default (all-raw)
    /// registry.
    pub fn open_with_compression(
        buffer_pool: Arc<dyn BufferPool>,
        wal: Arc<dyn WalManager>,
        mode: StorageMode,
        compression: Arc<compress::CompressionRegistry>,
    ) -> Result<Self>;

    pub fn install_schemas(&self, schemas: Vec<TableSchema>) -> Result<()>;
    pub fn install_index_schemas(&self, schemas: Vec<IndexSchema>) -> Result<()>;
    pub fn install_sequences(&self, schemas: Vec<SequenceSchema>) -> Result<()>;
    pub fn sequence_schemas_for_checkpoint(&self) -> Result<Vec<SequenceSchema>>;
    pub fn set_mode(&self, mode: StorageMode) -> Result<()>;

    /// Install an ALTERed table schema's compression setting into the live
    /// state and re-register file configs. No WAL (see At-Rest Page
    /// Compression).
    pub fn set_table_compression(&self, schema: &TableSchema) -> Result<()>;
    /// Install an ALTERed table schema's TOAST metadata into the live state.
    /// No WAL; the caller owns logical record emission and commit ordering.
    pub fn set_table_toast_metadata(&self, schema: &TableSchema) -> Result<()>;
    /// Validate globally unique target and replacement storage ids, then publish
    /// a complete committed multi-table TRUNCATE batch under one storage state
    /// write lock. No WAL is appended here.
    pub fn publish_truncate_tables(
        &self,
        updates: Vec<TruncateCatalogUpdate>,
    ) -> Result<()>;
    /// Up to `cap` evenly-sampled initialized heap page images, for
    /// dictionary training.
    pub fn sample_heap_pages(&self, schema: &TableSchema, cap: usize) -> Result<Vec<Vec<u8>>>;
    pub fn sample_heap_pages_cancelable(
        &self,
        schema: &TableSchema,
        cap: usize,
        cancel: &QueryCancel,
    ) -> Result<Vec<Vec<u8>>>;
    /// Bounded logical TEXT/BYTEA value bytes for TOAST dictionary training.
    pub fn sample_toast_values(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        max_samples: usize,
        max_bytes: usize,
    ) -> Result<Vec<Vec<u8>>>;
    /// Parent-table VACUUM after the server has performed the coordinated TOAST
    /// chunk cleanup/check and any required committed hidden-chunk deletes.
    pub fn vacuum_after_toast_cleanup(
        &self,
        schema: &TableSchema,
        horizon: u64,
    ) -> Result<usize>;
    /// Re-encode every initialized page (heap + all index files) of
    /// `schema`'s table so a following flush writes them under the current
    /// config. Logs a FullPageImage per page and stamps its LSN as the new
    /// PageLSN (torn-page repair, like VACUUM). Returns the number of pages
    /// touched.
    pub fn rewrite_table_pages(&self, schema: &TableSchema) -> Result<RewriteTablePages>;
    /// Heap page count for `schema`'s current storage generation (file-size
    /// based). ANALYZE records it as `TableStatistics.page_count`
    /// (`docs/specs/statistics.md` §5); it feeds cost estimates, not
    /// correctness.
    pub fn heap_page_count(&self, schema: &TableSchema) -> Result<PageNum>;
}
```

`RewriteTablePages` contains `pages_touched: usize` and `file_ids: Vec<FileId>`;
the file ids are sorted/deduplicated for deterministic scoped flushing.

`open` stores shared `Arc` handles to the buffer pool and WAL manager and initializes empty table, TOAST value-id allocator, and sequence metadata. It does not read schemas from disk; server startup installs catalog schemas explicitly with `install_schemas` (tables and hidden TOAST relations), `install_index_schemas` (catalog indexes), and `install_sequences` after loading the catalog snapshot, so DML maintains the indexes and sequence functions can advance existing sequences. In normal mode, `install_schemas` seeds every hidden TOAST relation's in-memory value-id allocator by physically scanning its heap rows (including aborted and in-flight tuples) and setting `next_value_id = 1 + max(value_id)`, or `1` when the relation has no chunks. In recovery mode, `install_schemas` intentionally leaves TOAST allocator entries absent: post-checkpoint physical redo may install additional chunk rows after schema metadata is loaded, so the recovery-to-normal transition reseeds every live hidden TOAST relation after redo has finished and before maintenance or DML can prune rows. Checkpoint uses `sequence_schemas_for_checkpoint` to copy live `(last_value, is_called)` state back into the catalog snapshot it serializes.

`PageBackedStorageEngine` implements `StorageEngine`, `SchemaOperations`, `common::SequenceManager`, and `RecoveryOperations`. Server code stores `Arc<PageBackedStorageEngine>` so startup can call concrete recovery-mode methods, query execution can pass `storage.as_ref()` as both `&dyn StorageEngine` and `&dyn SchemaOperations`, and `StatementContext` can carry the same value as the sequence manager.

Normal standalone TRUNCATE preparation computes one catalog change for the
complete target set and appends it before replacement files are initialized.
Multi-table publication uses
`publish_truncate_tables(Vec<TruncateCatalogUpdate>)`, which validates the
complete update set and swaps every table/index generation under one storage
state write lock. The server also holds `relation_publish_gate` across catalog
and storage batch publication, so new relation snapshots cannot observe a
committed subset.

Transactional TRUNCATE uses a distinct transactional batch publication path.
It validates the complete update set before mutation, records the old base,
TOAST, and secondary-index schemas in the transaction's existing storage
before-image state, records the previous TOAST allocator state, installs every
replacement generation under one storage state write lock, and removes the now-live
replacement files from the unpublished-file set. Normal `commit_txn` cleanup retires
the old generations and makes the final replacements permanent. Normal
`rollback_txn` restores the old schemas and allocator state, then queues the outgoing
replacement generations for reference-aware retirement so a relation snapshot that
was already using one cannot lose its files. Unpublished preparation files that were
never installed are removed directly. The transactional path never appends or flushes
a Commit record itself. Replacement storage ids remain reserved after rollback. See
`docs/specs/table-locks.md` for ownership and visibility rules.

Before-images use first-write semantics across repeated truncates of one logical
table. Replacement files remain transaction-tracked either as unpublished
preparations, the currently installed generation, or reference-counted superseded
generations. Commit retains the last installed generation and retires the original
and superseded replacements; rollback restores the original and retires every
installed replacement. The server runs best-effort retired-generation cleanup only
after dropping the relation-publication guard. Restoring the old TOAST allocator on
rollback is a generation-swap exception to process-monotonic allocation: ids consumed
only in discarded replacement files may be reused, but an id is never reused within
any surviving physical generation.

`RecoveryOperations` is implemented directly for `PageBackedStorageEngine`. There is no separate public `StorageRecovery` adapter; `crates/storage/src/recovery.rs` contains the `impl RecoveryOperations for PageBackedStorageEngine`, which delegates to the recovery-mode helpers (`apply_create_table_without_wal` / `apply_drop_table_without_wal`, schema metadata setters, truncate generation publication, plus sequence create/drop/value replay helpers) defined on `PageBackedStorageEngine` in `engine.rs`.

## TOAST Value ID Allocation

Hidden TOAST relations store chunk keys as `(value_id INTEGER, seq INTEGER)`.
Storage owns an in-memory per-TOAST-relation allocator for `value_id`; it is
intentionally not part of the public `StorageEngine` trait and is consumed by
the storage-private TOAST write path. Allocation starts at `1`, is monotonic for
the life of each surviving physical generation, and is not rolled back on an
ordinary transaction abort. Transactional TRUNCATE rollback may restore the old
generation's allocator while discarding the complete replacement generation, as
specified above; it never reuses an id in a surviving generation. The
allocator refuses to hand out any value above `i64::MAX` because the hidden
relation key stores `value_id` as `Value::Integer`; exceeding that bound returns
`SqlState::ProgramLimitExceeded` with a clear TOAST allocator message.

Allocator seeding scans physical heap pages of the hidden TOAST relation rather
than snapshot-visible rows: every `NORMAL` line pointer is decoded and its first
column is read as `value_id`, regardless of the tuple's `xmin`/`xmax` status.
This includes committed, aborted, and in-flight chunk rows, preventing value-id
reuse after aborts or crash replay. The scan ignores uninitialized/sparse pages
and non-live line pointers, propagates page/row corruption as structured storage
errors, and treats `value_id <= 0` or non-integer/missing `value_id` as
corruption. Normal `CREATE TABLE` seeds a newly created hidden TOAST relation at
`1`. Recovery metadata apply (`apply_create_table_without_wal`) does not seed
hidden TOAST relations because later physical redo can add chunk rows for the
same relation; `set_mode(StorageMode::Normal)` seeds live hidden TOAST
relations from the final post-redo physical state. `alloc_toast_value_id` also
lazily seeds on a missing cache entry as a defensive fallback, so replay/order
changes cannot make allocation reuse an ID already present in physical chunk
rows.

## TOAST Row Preparation

Storage owns a storage-private row preparation helper that converts a logical
`Row` into row-format v3 bytes for INSERT and the normal non-HOT UPDATE path.
Index keys are computed from the caller's logical row before physical TOAST
encoding, so identity and catalog indexes store logical keys rather than TOAST
pointers.

Preparation first validates the logical primary-key and live secondary-index
keys using the same B-tree entry-size rules as index insertion. This preflight
runs before any external TOAST chunks are written, so oversized indexed values
return `SqlState::ProgramLimitExceeded` without leaving orphan chunk rows.

Hidden TOAST relations bypass TOAST recursively and are encoded as plain v3 rows.
Catalog v3 requires every user table with a toastable column to reference its
catalog-created hidden TOAST relation. Storage treats a missing reference as
catalog corruption rather than retaining an inline-only compatibility path.

For user tables with a hidden TOAST relation, non-null `TEXT`, `BYTEA`, and array
values whose logical byte length is at least `toast.min_value_size` are candidates.
Storage computes `raw_crc32` over the logical bytes, tries the table's configured
value compression (`none`, `zstd`, or `zstd_dict` with the active dictionary; when
`zstd_dict` has no active dictionary it falls back to plain zstd), and keeps an
inline compressed envelope only when the complete inline compressed representation
saves at least `ToastOptions::MIN_TOAST_COMPRESSION_SAVINGS` bytes versus plain.
Inline compression is attempted even when the row already fits; this is what lets
medium values benefit from dictionaries.

The helper computes the exact v3 length of the inline candidate row before
materializing the final parent bytes. If the candidate is at or below
`toast.tuple_target` and fits a heap page, it returns that row. With
`toast.mode = Off`, externalization is disabled; the inline candidate is returned
only if it fits a page. Otherwise storage length-simulates replacing candidates
with fixed-width external pointers, largest current inline representation first,
until the parent row meets the target and page limit, or until every candidate is
external. Simulation happens before chunk writes and avoids constructing a full
oversized all-inline row. If the final simulated parent cannot fit a page, the
helper returns `ProgramLimitExceeded` without writing chunks. For the planned
external values, storage writes the complete external stream to the hidden
relation under the caller's transaction, stores real `ToastPointer`s in the
parent row, and returns the v3 parent bytes.

HOT updates are enabled for TOAST-enabled tables only while the HOT chain remains
inline-only. The HOT path prepares the successor tuple with the same v3 inline
representation used by normal writes, including configured inline compression, but
does not write external chunks. If normal TOAST policy would externalize the
successor, or if the predecessor already contains an external pointer, HOT declines
and the normal fully-indexed update writes a fresh parent tuple plus any required
TOAST chunks. This is an intentional v1 limitation: it recovers HOT for common
small/medium `TEXT` and `BYTEA` values without making update-path HOT pruning
responsible for hidden chunk cleanup. Full VACUUM remains the only path that may
discard parent tuple bytes that own external chunks.

## TOAST Read Materialization

User-facing storage reads return logical `Row` values. Visibility resolution is
header-only: `get`, primary-key scans, and secondary-index scans first resolve the
visible heap tuple using only MVCC header fields and HOT-chain metadata. They do
not decompress inline values or read external TOAST chunks for invisible tuples.

After a tuple is known visible, storage decodes the physical v3 row and
materializes each value:

- Plain values become their ordinary logical `Value`.
- Inline compressed `TEXT`/`BYTEA` values are decompressed with the stored codec
  and dictionary id, checked against the stored raw length, checked against the
  stored `raw_crc32`, and then rebuilt as `Value::Text` or `Value::Bytes`.
- External `TEXT`/`BYTEA` values read the hidden TOAST relation using the same
  statement snapshot, reconstruct the complete stream in `(value_id, seq)` order,
  parse the stream header for dictionary id and `raw_crc32`, decompress/validate
  the payload, and rebuild the logical value.

Visible rows with missing, duplicate, out-of-order, length-mismatched, CRC-bad,
UTF-8-invalid, or dictionary-unresolvable TOAST data return a structured storage
error. Invisible rows with broken external chunks are skipped without touching
those chunks.

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

`ALTER TABLE ... ADD/DROP PRIMARY KEY` additionally takes a per-table identity
rewrite gate exclusively while resetting and rebuilding the reserved identity
B-tree. Normal identity-index reads and inserts take the shared side of that gate,
so they keep ordinary B-link concurrency with each other but cannot overlap the
destructive rebuild.

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
  mutation holds that physical index file's `structural_latch(file_id)` across the WHOLE
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

These latches are load-bearing under the current shared-writer model: writers on
the same heap or index serialize structural mutations here, while writers touching
different files can proceed concurrently.

## Page-Backed Simplifications

- Structural mutations within one index or one heap file serialize on that file's
  per-`FileId` structural write latch (above). Concurrent writers (E2b) run under
  the shared writer guard, so two writers touching the same heap or index file
  serialize on that file's structural latch while writers on different files
  proceed in parallel.
- The identity index is durable on disk, so nothing is rebuilt after recovery.
- Compaction may be skipped unless a page runs out of free space (and B-tree nodes are never merged).
- Before any page mutation, storage must obtain a write page guard with `ctx.txn_id`.
- New pages allocated during a statement are not reclaimed on rollback; their page numbers remain consumed so runtime state matches redo-all recovery.
- Index and heap page changes (including B-tree splits) are not physically undone on rollback. `rollback_txn(txn_id)` restores storage-owned table and index metadata, may delete unpublished truncate replacement files that no committed catalog state can reference, and retires any rollback-removed published generation until retained snapshots release it; row/index versions written by the aborted transaction stay on pages and are hidden by the CLOG until VACUUM.
- `drop_table` records table metadata in storage rollback metadata before marking the table dropped; `create_index` / `drop_index` record index metadata the same way, so a rolled-back create removes the index and a rolled-back drop restores it. A committed drop queues the previous table/index generations for retired cleanup; dropped metadata no longer protects the files once the commit cleanup has recorded those retired generations.

## Error Handling

- Duplicate primary key (committed-live duplicate): `SqlState::UniqueViolation`.
- Duplicate value in a unique catalog index (insert, update, or backfill, committed-live duplicate): `SqlState::UniqueViolation`.
- Unique key (primary or catalog index) held only by another in-progress inserter — undecidable until that transaction finishes; wait and recheck, surfacing `UniqueViolation` only if the holder commits.
- `index_scan` on an unknown, rolled-back, wrong-table, or dropped-table index:
  `SqlState::UndefinedTable`. A committed dropped index for a live table remains
  scan-readable over its retained physical entries but is no longer maintained.
- Missing update/delete key: return `Ok(false)`.
- Corrupt page checksum: `ErrorKind::Storage`.
- Page layout or index invariant violation: `ErrorKind::Storage` or `Internal` depending on source.

## Acceptance Tests

- Insert then get returns the row.
- Duplicate insert fails without changing existing row.
- Update replaces a row by storage identity key.
- Delete removes a row by storage identity key.
- Scan returns all rows with `StoredRow` identity.
- Range scan returns expected ordered keys.
- Recovery DDL apply mutates metadata without WAL append.
- Multi-table truncate preparation failure can be rolled back without publishing
  any target; batch publication rejects a late cross-update storage-id collision
  without partial publication and otherwise swaps every prepared base,
  secondary-index, and hidden-TOAST generation under one storage state lock.
- Direct batch publication rejects duplicate logical table targets before any
  table/index generation, rollback bookkeeping, retired-generation queue, or
  relation epoch changes.
- A reopened engine reads rows through the durable on-disk index (no rebuild).
- A B-tree splits correctly under variable-length keys (byte-balanced) and stays searchable.
- After a restart, inserting a row or growing the index never reuses an on-disk page.
- Failed insert that allocated a new page rolls back newly allocated pages through buffer rollback.
- Heap, identity-index, and catalog-index files for the same numeric id stay distinct.
- A catalog-index B-tree stores heap TIDs and a prefix range matches the indexed columns regardless of the trailing TID tiebreaker; an index scan resolves to heap TIDs directly.
- `create_index` backfills existing rows; `index_scan` returns them, and a non-unique index returns every row for a value.
- Insert, update, and delete keep a catalog index in sync.
- A unique index rejects a duplicate value on insert and on backfill, but allows multiple NULLs.
- A dropped index is no longer maintained but retained entries remain scannable for
  already-planned readers; a rolled-back create removes it.
- A secondary index is not visible to relation snapshots captured while its physical tree/backfill is still in progress.
- `create_index` consumes a caller-supplied preceding `CatalogChange`; recovery-apply index methods append no WAL.
- After a restart, a catalog index created post-checkpoint is replayed (catalog + storage metadata and its rebuilt tree) and remains scannable.
