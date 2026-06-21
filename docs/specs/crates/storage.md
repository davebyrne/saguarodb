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
- `index_scan` walks a secondary index for the matching primary keys, then resolves each through the primary-key index to its heap row (see Secondary Indexes).
- `delete` / `update` keep every secondary index in sync with the heap.

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
- Compaction may be implemented lazily.

## Row Serialization

```text
[row_format_version: 1 byte][null_bitmap][col1_data][col2_data]...
```

- `row_format_version`: `1`; unknown versions are rejected as corrupt. Reserved so MVCC row versions can be added later without a second on-disk format break.
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

- **Pages.** Page 0 is a metapage holding the current root page number. Other
  pages are leaf or internal nodes sharing the standard page header (so they get
  the same PageLSN, checksum, and torn-page protection). A 5-byte node sub-header
  carries a leaf flag and a link (right-sibling for a leaf, leftmost child for an
  internal node); entries are a sorted slotted array of `[key_len][key][value]`,
  where a leaf value is an encoded `RowLocation` and an internal value is a child
  page number.
- **Lookup / scan.** `get` descends from the root to a leaf; `scan`/`scan_range`
  find the start leaf and walk the right-sibling chain in key order.
- **Insert.** Places the entry in sorted position; a full node splits at a
  byte-balanced point (so variable-length keys do not overflow a half) and
  propagates a separator upward, growing the tree by a level on a root split.
- **Delete.** Removes the entry; underfull nodes are not merged (accepted bloat).
- **Update.** Overwrites the leaf entry's `RowLocation` in place.
- **Crash safety.** Every node mutation logs a `FullPageImage` and stamps the
  page-LSN, so the index is recovered by the same redo path as the heap and needs
  no rebuild. Page allocation is seeded from each file's on-disk extent so a new
  node never reuses an existing page after recovery.
- **Keys.** Keys are stored in a self-describing byte form and ordered by decoding
  to `Key` and comparing with `Ord`, matching the previous in-memory ordering.

The B-tree is generic over its leaf value type: the primary-key index stores a
fixed-width `RowLocation`, and a secondary index stores the row's primary `Key`
(see Secondary Indexes). Internally the tree treats values as opaque bytes.

## Secondary Indexes

A table may have any number of secondary indexes. Each is its own durable B-tree
in its own file, tagged with the top two file-id bits (distinct from the heap and
the primary-key index) and written to `<data>/heap/<index_id>.sidx`. Index ids
are a separate id space from table ids; the reserved primary-key index id is never
used for a secondary index.

- **Entry layout.** A secondary index stores `indexed_columns -> primary_key`,
  not `-> RowLocation`. Because the primary key is immutable and never moves, a
  row relocation (every `update`) touches only the primary-key index; a secondary
  index changes only when one of its indexed columns changes. Reads therefore go
  secondary index → primary key → primary-key index → `RowLocation` → heap.
- **Key shape.** A non-unique index keys on `[indexed.. , primary_key]`, so every
  entry is distinct. A unique index keys on `[indexed..]` alone, so the tree
  rejects a duplicate indexed value with `SqlState::UniqueViolation` — except when
  an indexed value is NULL, where the primary key is appended too, so NULLs stay
  distinct (SQL treats NULLs as unequal). The leaf value is the encoded primary
  key in all cases.
- **Lookup / range.** `index_scan(table, index, range)` constrains the leading
  indexed columns; the range bounds hold exactly those columns, and comparison
  ignores each stored key's trailing primary key. An equality bound thus matches
  every row sharing the indexed value, and an inclusive upper bound includes all
  of its rows. Results are returned in index order, resolved to current heap rows.
- **Maintenance.** `insert` adds an entry to every index; `delete` removes the
  entry computed from the row being deleted; `update` removes the old entries and
  inserts the new ones (all removals before any insertion, so an unchanged unique
  value is not seen as a duplicate). A unique-index conflict during `insert` or
  `update` returns `SqlState::UniqueViolation`.
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
(`HeapInit`/`HeapInsert`/`HeapDelete`/`FullPageImage`) onto a page buffer, gated by
the page-LSN: a record whose effect is already present (`page_lsn(page) >= lsn`) is
skipped, making replay idempotent. `FullPageImage` is validated to be exactly
`PAGE_SIZE` bytes before install. Recovery uses it to redo committed records after
the checkpoint LSN.

## WAL Interaction

Normal data operations append physiological redo records as they mutate pages, stamping the page-LSN with each record's LSN:

- A row insert logs `HeapInsert { file_id, page_num, slot, row_bytes }`, or a `FullPageImage` if this is the first modification of the page since the last checkpoint (torn-page protection). A fresh page first logs `HeapInit`.
- A row delete logs `HeapDelete { file_id, page_num, slot }` (or a `FullPageImage` on first touch). An update is a delete followed by an insert.
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

## Page-Backed V1 Simplifications

- Single writer means heap and index page modifications do not need fine-grained locks.
- The primary-key index is durable on disk, so nothing is rebuilt after recovery.
- Compaction may be skipped unless a page runs out of free space (and B-tree nodes are never merged).
- Before any page mutation, storage must obtain a write page guard with `ctx.txn_id`.
- New pages allocated during a statement must be tracked by buffer rollback through `new_page(file, txn_id)`.
- Index and heap page changes (including B-tree splits) are rolled back by the buffer pool's before-images and new-page tracking, so `rollback_txn(txn_id)` only restores storage-owned table and index metadata.
- `drop_table` records table metadata in storage rollback metadata before marking the table dropped; `create_index` / `drop_index` record index metadata the same way, so a rolled-back create removes the index and a rolled-back drop restores it. V1 does not physically delete heap or index pages; committed drops are reflected by omitting the table or index from later checkpoints.

## Error Handling

- Duplicate primary key: `SqlState::UniqueViolation`.
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
- A secondary B-tree stores primary keys and a prefix range matches the indexed columns regardless of the trailing primary key.
- `create_index` backfills existing rows; `index_scan` returns them, and a non-unique index returns every row for a value.
- Insert, update, and delete keep a secondary index in sync.
- A unique index rejects a duplicate value on insert and on backfill, but allows multiple NULLs.
- A dropped index is no longer maintained or scannable; a rolled-back create removes it.
- `create_index` logs a `CreateIndex` record; recovery-apply index methods append no WAL.
- After a restart, a secondary index created post-checkpoint is replayed (catalog + storage metadata and its rebuilt tree) and remains scannable.
