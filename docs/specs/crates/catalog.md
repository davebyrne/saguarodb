# `catalog` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`catalog` owns schema metadata, stable table/column IDs, and name-to-ID resolution for binder. Its persisted form is included in snapshots and replayed through WAL for changes after the snapshot.

## Depends On

- `common`

## Data Model

```rust
pub struct Catalog {
    tables_by_name: HashMap<String, TableId>,
    tables_by_id: HashMap<TableId, TableSchema>,
    next_table_id: TableId,
}

pub struct CatalogSnapshot {
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub next_table_id: TableId,
}
```

`TableSchema`, `ColumnDef`, and `DataType` live in `common`.

IDs are monotonically increasing and never reused.

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
}
```

Methods return owned schema copies. V1 stores catalog behind an `RwLock`. `snapshot` and `restore` are used by server DDL rollback to restore metadata if storage or WAL work fails before statement success.

`apply_create_table` and `apply_drop_table` are recovery-only APIs. `apply_create_table` inserts a fully assigned historical `TableSchema`, rejects conflicting names or IDs, and advances `next_table_id` to at least `schema.id + 1`. It must not reassign table or column IDs. `apply_drop_table` removes an existing schema by ID without assigning IDs.

## Create Table Rules

- Table name must be unique.
- Column names must be unique within table; duplicate column definitions return `SqlState::SyntaxError`.
- Primary key column names must exist.
- Duplicate primary-key column names return `SqlState::SyntaxError`.
- Primary key columns are implicitly non-null.
- `ColumnId`s are assigned in declared column order starting at zero.
- Empty catalogs start with `next_table_id = 1`; `TableId` is assigned from `next_table_id`.

## Catalog Persistence

The catalog serializes into the control record (`manifest.dat`) at each checkpoint.

On startup:

1. Snapshot manager loads current catalog bytes.
2. Catalog deserializes into memory.
3. Recovery replays post-snapshot `CreateTable` and `DropTable` records into both catalog and storage using `apply_create_table` and `apply_drop_table`.

Catalog mutations update memory immediately. Durability before snapshot is provided by WAL records.

`restore` and startup loading must validate catalog snapshots before installing them. Public construction from persisted snapshots must use the validated path; unchecked snapshot installation is an implementation detail internal to the crate. Validation requires every name index entry to point at an existing schema with the same name and ID, every schema to have a reverse name index entry, column IDs assigned in declared order starting at zero, unique column IDs, unique column names, exactly one primary key column for v1, a primary key column ID that exists, a non-null primary key column, and `next_table_id >= max(table_id) + 1`. Invalid loaded snapshots return `InternalError` because they represent durable catalog corruption.

## WAL Interaction

`CREATE TABLE` and `DROP TABLE` are logged. The executor/storage orchestration must ensure catalog mutation and storage file mutation are part of the same statement-level commit.

If a normal DDL statement fails after catalog mutation but before statement success, the caller must restore the previous catalog snapshot before returning the error.

Recovery apply methods must update catalog state consistently with storage state.

## Invariants

- Name map and ID map are consistent.
- IDs are never reused after drop.
- Binder is the only consumer that resolves names for query planning.
- Executor/storage should use `TableId` and `ColumnId` after binding.

## Acceptance Tests

- Create table assigns table and column IDs.
- Duplicate table is rejected.
- Duplicate column is rejected.
- Primary key on missing column is rejected.
- Drop removes name and ID lookup.
- Serialization round-trip preserves `next_table_id`.
- Recovery create/drop updates catalog without name leaks into executor.
