# `catalog` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`catalog` owns schema metadata, stable table/column IDs, and name-to-ID resolution for binder. Its persisted form is included in the control record and updated by replaying WAL DDL records for changes after the checkpoint.

## Depends On

- `common`

## Data Model

```rust
pub struct Catalog {
    tables_by_name: HashMap<String, TableId>,
    tables_by_id: HashMap<TableId, TableSchema>,
    next_table_id: TableId,
    indexes_by_name: HashMap<String, IndexId>,
    indexes_by_id: HashMap<IndexId, IndexSchema>,
    next_index_id: IndexId,
}

pub struct CatalogSnapshot {
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub next_table_id: TableId,
    pub indexes_by_name: HashMap<String, IndexId>,
    pub indexes_by_id: HashMap<IndexId, IndexSchema>,
    pub next_index_id: IndexId,
}
```

`TableSchema`, `ColumnDef`, `DataType`, and `IndexSchema` live in `common`.

Table IDs and index IDs are independent namespaces; both are monotonically
increasing and never reused. `next_index_id` starts at
`PRIMARY_KEY_INDEX_ID + 1`, because index id `0` is reserved for a table's
primary-key index and is never assigned to a secondary index. The three index
fields deserialize with defaults (empty maps, `next_index_id =
PRIMARY_KEY_INDEX_ID + 1`) so catalogs persisted before secondary indexes
existed still load.

## Public API

```rust
pub trait CatalogManager: Send + Sync {
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>>;
    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>>;
    fn list_tables(&self) -> Result<Vec<TableSchema>>;
    fn snapshot(&self) -> Result<CatalogSnapshot>;
    fn restore(&self, snapshot: CatalogSnapshot) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, id: TableId) -> Result<()>;
    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<TableSchema>;
    fn drop_table(&self, id: TableId) -> Result<()>;

    fn get_index_by_name(&self, name: &str) -> Result<Option<IndexSchema>>;
    fn list_indexes_for_table(&self, table: TableId) -> Result<Vec<IndexSchema>>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, id: IndexId) -> Result<()>;
    fn create_index(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<IndexSchema>;
    fn drop_index(&self, id: IndexId) -> Result<()>;
}
```

Methods return owned schema copies. The catalog is stored behind an `RwLock`. `snapshot` and `restore` are used by server DDL rollback to restore metadata if storage or WAL work fails before statement success.

The concrete implementation is `MemoryCatalog`. It is constructed with `MemoryCatalog::empty()` (or the equivalent `Default`) for a fresh database, or `MemoryCatalog::try_from_snapshot(snapshot)` to load a persisted snapshot through the validated path; the unchecked `from_snapshot` constructor is crate-internal.

`apply_create_table` and `apply_drop_table` are recovery-only APIs. `apply_create_table` inserts a fully assigned historical `TableSchema`, rejects conflicting names or IDs, and advances `next_table_id` to at least `schema.id + 1`. It must not reassign table or column IDs. `apply_drop_table` removes an existing schema by ID without assigning IDs; a missing ID returns `SqlState::UndefinedTable`.

`create_index` resolves the table and column names, assigns an `IndexId`, and returns the stored `IndexSchema`; `drop_index` removes an index by ID, returning `SqlState::UndefinedTable` for a missing ID (indexes share the relation namespace, so there is no dedicated SQLSTATE). `apply_create_index` and `apply_drop_index` are the matching recovery-only APIs: `apply_create_index` inserts a fully assigned historical `IndexSchema`, rejects conflicting names or IDs, and advances `next_index_id` to at least `schema.id + 1`; `apply_drop_index` removes an existing index by ID. `list_indexes_for_table` returns a table's indexes ordered by ID and is how storage learns which indexes to maintain on DML.

## Create Table Rules

- Table name must be unique; a duplicate name returns `SqlState::DuplicateTable`.
- Column names must be unique within table; duplicate column definitions return `SqlState::SyntaxError`.
- Primary key column names must exist.
- Duplicate primary-key column names return `SqlState::SyntaxError`.
- Exactly one primary-key column is required; an empty or composite primary key returns `SqlState::DatatypeMismatch`.
- Primary key columns are implicitly non-null.
- `ColumnId`s are assigned in declared column order starting at zero.
- Empty catalogs start with `next_table_id = 1`; `TableId` is assigned from `next_table_id`.

## Create Index Rules

- Index name must be unique among indexes (indexes have their own name space, separate from tables); a duplicate index name returns `SqlState::DuplicateTable`, the same code reused for the shared relation namespace.
- The target table must exist; otherwise `SqlState::UndefinedTable`.
- Index column names must exist on the target table; otherwise `SqlState::UndefinedColumn`.
- Duplicate index column names and an empty column list return `SqlState::SyntaxError`.
- Index columns keep the order written.
- `IndexId` is assigned from `next_index_id`, starting at `PRIMARY_KEY_INDEX_ID + 1`.
- The `unique` flag is recorded here; duplicate-value rejection for unique indexes happens at the storage layer, not in the catalog.
- Dropping a table cascades in the catalog to remove every index on that table. The same cascade runs on the recovery `apply_drop_table` path, so the durable `DropTable` record alone restores the post-drop state.

## Catalog Persistence

The catalog serializes into the control record (`manifest.dat`) at each checkpoint. The wire format is JSON via `serde_json`; the crate exposes the free functions `serialize_catalog` / `deserialize_catalog`. The index fields carry `#[serde(default)]`, so a catalog persisted before secondary indexes existed still deserializes (empty index maps, `next_index_id = PRIMARY_KEY_INDEX_ID + 1`).

On startup:

1. The control store loads the current catalog bytes from the control record.
2. Catalog deserializes into memory.
3. Recovery replays post-checkpoint `CreateTable`, `DropTable`, `CreateIndex`, and `DropIndex` records into both catalog and storage using `apply_create_table` / `apply_drop_table` / `apply_create_index` / `apply_drop_index`.

Catalog mutations update memory immediately. Durability before the next checkpoint is provided by WAL records.

`restore` and startup loading must validate catalog snapshots before installing them. Public construction from persisted snapshots must use the validated path; unchecked snapshot installation is an implementation detail internal to the crate. Validation requires every name index entry to point at an existing schema with the same name and ID, every schema to have a reverse name index entry, column IDs assigned in declared order starting at zero, unique column IDs, unique column names, exactly one primary key column, a primary key column ID that exists, a non-null primary key column, and `next_table_id >= max(table_id) + 1`. Index validation additionally requires every index name entry to point at an existing index with the same name and ID, every index schema to have a reverse name entry, the index ID to differ from the reserved `PRIMARY_KEY_INDEX_ID`, the referenced table to exist, a non-empty column list, every index column to exist on the referenced table, unique index column IDs, and `next_index_id >= max(index_id) + 1`. Invalid loaded snapshots return `InternalError` because they represent durable catalog corruption.

## WAL Interaction

`CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX` are logged. The executor/storage orchestration must ensure catalog mutation and storage file mutation are part of the same statement-level commit.

If a normal DDL statement fails after catalog mutation but before statement success, the caller must restore the previous catalog snapshot before returning the error.

Recovery apply methods must update catalog state consistently with storage state.

## Invariants

- Name map and ID map are consistent, for both tables and indexes.
- IDs are never reused after drop.
- Table, index, and column ID assignment is overflow-guarded: rather than wrap or reuse, an exhausted ID space returns `SqlState::InternalError`.
- Index id `PRIMARY_KEY_INDEX_ID` is reserved and never assigned to a secondary index.
- Every secondary index references an existing table and existing columns on it; dropping a table removes its indexes.
- Binder is the only consumer that resolves names for query planning.
- Executor/storage should use `TableId`, `ColumnId`, and `IndexId` after binding.

## Acceptance Tests

- Create table assigns table and column IDs.
- Duplicate table is rejected.
- Duplicate column is rejected.
- Primary key on missing column is rejected.
- Drop removes name and ID lookup.
- Serialization round-trip preserves `next_table_id`.
- Recovery create/drop updates catalog without name leaks into executor.
- Create index resolves columns and assigns monotonically increasing index IDs.
- Duplicate index name, missing table, missing column, and duplicate/empty columns are rejected with the documented SQLSTATEs.
- Dropping a table cascades to its indexes.
- Serialization round-trip preserves indexes and `next_index_id`; a snapshot without index fields loads as an empty index set.
- Snapshot validation rejects an index that references a missing table, uses the reserved primary-key index ID, or carries a stale `next_index_id`.
