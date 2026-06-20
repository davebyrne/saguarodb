# `storage` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`storage` owns table files, row serialization, page-backed row storage, the in-memory primary-key directory used by v1, normal data operations, schema file operations, and recovery apply operations.

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
    fn rollback_txn(&self, txn_id: u64) -> Result<()>;
    fn commit_txn(&self, txn_id: u64) -> Result<()>;
}

pub trait SchemaOperations: Send + Sync {
    fn create_table(&self, ctx: &StatementContext, schema: &TableSchema) -> Result<()>;
    fn drop_table(&self, ctx: &StatementContext, table: TableId) -> Result<()>;
}

pub trait RecoveryOperations: Send + Sync {
    fn apply_insert(&self, table: TableId, key: Key, row: Row) -> Result<()>;
    fn apply_update(&self, table: TableId, key: Key, row: Row) -> Result<()>;
    fn apply_delete(&self, table: TableId, key: Key) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, table: TableId) -> Result<()>;
}
```

Normal methods append WAL records. `rollback_txn` restores storage-owned in-memory state such as primary-key directory entries; page bytes are restored by `BufferPool::rollback`. `commit_txn` discards storage rollback metadata after WAL flush succeeds. `commit_txn` is cleanup-only, must not perform I/O, and should not fail for a valid `txn_id`. `RecoveryOperations` must not append WAL records.

## Table Storage

Each table is page-backed. V1 stores full rows in table pages and maintains an in-memory `BTreeMap<Key, RowLocation>` primary-key directory per table. The full clustered on-disk B-tree remains future work behind the existing storage traits.

V1 only supports the primary-key index:

- `insert` inserts by primary key.
- `get` does primary-key lookup.
- `scan_range` walks the in-memory primary-key directory and reads rows from page locations.
- Secondary indexes are future work.

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

Page body:

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

## Heap Page Store

`HeapPageStore` is the mutable page home for in-place dirty-page flushing. It
implements `buffer::PageStore` over one file per table at `<data>/heap/<file_id>.heap`,
storing page `n` at byte offset `n * PAGE_SIZE` with positioned reads/writes.
`load_page` returns a complete page or `None` (missing file or beyond-EOF / short
tail); `write_page` writes in place without fsync; `sync_all` fsyncs all open heap
files and the directory. It is introduced for the redo-WAL/flushing model and
becomes the buffer pool's backing store when recovery and checkpoint adopt it.

## WAL Interaction

Normal storage and schema operations append logical WAL records before mutating pages or table metadata:

- `Insert`: table, key, row.
- `Update`: table, key, new row.
- `Delete`: table, key.
- `SchemaOperations::create_table`: `CreateTable` with the fully assigned table schema.
- `SchemaOperations::drop_table`: `DropTable` with the table ID.

Server query orchestration appends `Commit` and flushes WAL after the statement succeeds. Storage should not append commit records.

## Recovery Mode

The storage engine can be initialized in recovery mode. In recovery mode:

- Normal `StorageEngine` methods should not be used by server queries.
- `RecoveryOperations::apply_*` mutates pages and primary-key directories directly.
- No WAL append occurs.
- Apply operations are idempotency-aware only where needed by recovery. With v1 snapshots, replay applies each post-snapshot record exactly once.

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
    pub fn rebuild_directories(&self) -> Result<()>;
    pub fn set_mode(&self, mode: StorageMode) -> Result<()>;
}
```

`open` stores shared `Arc` handles to the buffer pool and WAL manager and initializes empty table metadata plus empty in-memory primary-key directories. It does not read schemas from disk; server startup installs catalog schemas explicitly with `install_schemas` after loading the catalog snapshot.

`PageBackedStorageEngine` implements `StorageEngine`, `SchemaOperations`, and `RecoveryOperations`. Server code stores `Arc<PageBackedStorageEngine>` for v1 so startup can call concrete recovery-mode methods and query execution can pass `storage.as_ref()` as both `&dyn StorageEngine` and `&dyn SchemaOperations`.

`RecoveryOperations` is implemented directly for `PageBackedStorageEngine`. There is no separate public `StorageRecovery` adapter in v1; `crates/storage/src/recovery.rs` contains the recovery-mode helper functions and the `impl RecoveryOperations for PageBackedStorageEngine`.

## Page-Backed V1 Simplifications

- Single writer means page and primary-key-directory modifications do not need fine-grained locks.
- A table directory can be rebuilt by scanning known table pages after snapshot load.
- Compaction may be skipped unless a page runs out of free space.
- Before any page mutation, storage must obtain a write page guard with `ctx.txn_id`.
- New pages allocated during a statement must be tracked by buffer rollback through `new_page(file, txn_id)`.
- In-memory primary-key directory mutations must be tracked per `txn_id` so `rollback_txn(txn_id)` can restore them if the statement fails after page mutation.
- `drop_table` must record table schema, primary-key directory, and table metadata in storage rollback metadata before mutation. V1 does not physically delete table pages during the statement; committed drops are reflected by omitting the table from later snapshots.

## Error Handling

- Duplicate primary key: `SqlState::UniqueViolation`.
- Missing update/delete key: return `Ok(false)`.
- Corrupt page checksum: `ErrorKind::Storage`.
- Page layout or primary-key-directory invariant violation: `ErrorKind::Storage` or `Internal` depending on source.

## Acceptance Tests

- Insert then get returns the row.
- Duplicate insert fails without changing existing row.
- Update replaces a row by primary key.
- Delete removes a row by primary key.
- Scan returns all rows with `StoredRow` identity.
- Range scan returns expected ordered keys.
- Recovery apply insert/update/delete mutates pages without WAL append.
- Rebuilding the primary-key directory from pages preserves lookup correctness.
- Failed insert that allocated a new page rolls back newly allocated pages through buffer rollback.
