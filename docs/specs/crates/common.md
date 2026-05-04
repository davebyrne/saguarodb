# `common` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`common` defines stable cross-crate types and small traits that must not depend on implementation crates. It is the leaf crate for the workspace.

## Owns

- Stable identifiers: `TableId`, `ColumnId`, `IndexId`, `BindingId`, `FileId`, `PageNum`, `Lsn`.
- SQL values and row envelopes: `Value`, `Row`, `Key`, `StoredRow`, `ExecRow`, `RowIdentity`.
- Schema description types: `DataType`, `ParsedColumnDef`, `ColumnDef`, `ColumnInfo`, `TableSchema`.
- Query access helpers: `KeyRange`.
- Error model: `DbError`, `ErrorKind`, `SqlState`, `Result<T>`.
- Statement context and future transaction extension point.
- Cross-cutting traits: `FlushPolicy`, `ConcurrencyController`.

## Public Types

```rust
pub type TableId = u32;
pub type ColumnId = u16;
pub type IndexId = u32;
pub const PRIMARY_KEY_INDEX_ID: IndexId = 0;
pub type BindingId = u32;
pub type PageNum = u32;
pub type FileId = u32;
pub type Lsn = u64;

pub struct RowId {
    pub page_num: PageNum,
    pub slot_num: u16,
}

pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
}

pub struct Row {
    pub values: Vec<Value>,
}

pub struct Key(pub Vec<Value>);

pub enum KeyRange {
    Exact(Key),
    Range { start: std::ops::Bound<Key>, end: std::ops::Bound<Key> },
    All,
}

pub struct StoredRow {
    pub row_id: RowId,
    pub key: Key,
    pub row: Row,
}

pub struct ExecRow {
    pub row: Row,
    pub identity: Option<RowIdentity>,
}

pub struct RowIdentity {
    pub row_id: RowId,
    pub key: Key,
}
```

`Value` ordering is used for B-tree keys. V1 ordering is total and deterministic: `Null < Boolean < Integer < Text`, with natural ordering inside each variant. SQL comparison semantics still apply in expression evaluation; B-tree ordering is a storage ordering.

## Column Lifecycle Types

```rust
pub enum DataType {
    Integer,
    Text,
    Boolean,
}

pub struct ParsedColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub table_id: Option<TableId>,
    pub column_id: Option<ColumnId>,
}

pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
}
```

`ParsedColumnDef` is parser output and never has IDs. `ColumnDef` is catalog-owned and always has stable IDs. `ColumnInfo` describes result columns and may be derived from expressions, so table/column IDs are optional.

## Error Model

```rust
pub struct DbError {
    pub kind: ErrorKind,
    pub code: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

pub enum ErrorKind {
    Parse,
    Plan,
    Execute,
    Storage,
    Io,
    Wal,
    Protocol,
    Internal,
}

pub enum SqlState {
    SuccessfulCompletion,
    SyntaxError,
    UndefinedTable,
    UndefinedColumn,
    DuplicateTable,
    DatatypeMismatch,
    DivisionByZero,
    NumericValueOutOfRange,
    NotNullViolation,
    UniqueViolation,
    IoError,
    InternalError,
}

pub type Result<T> = std::result::Result<T, DbError>;
```

All crates return `common::Result<T>`. Crates should map low-level errors into the nearest `ErrorKind` and SQLSTATE at the boundary where context is available.

`DbError` exposes convenience constructors used consistently across crates: `DbError::parse(code, message)`, `DbError::plan(code, message)`, `DbError::execute(code, message)`, `DbError::storage(code, message)`, `DbError::wal(code, message)`, `DbError::protocol(code, message)`, `DbError::io(message)`, and `DbError::internal(message)`. Constructors set `kind`, `code`, and `message`; `io` uses `SqlState::IoError`, and `internal` uses `SqlState::InternalError`.

## Statement Context

```rust
pub struct StatementContext {
    pub txn_id: u64,
}
```

V1 uses one `txn_id` per autocommit statement. Future MVCC may add snapshot and isolation fields without changing storage method signatures.

## Flush Policy

```rust
pub struct PageFlushInfo {
    pub dirty_txn_id: u64,
    pub page_lsn: Option<Lsn>,
}

pub trait FlushPolicy: Send + Sync {
    fn can_flush(&self, info: &PageFlushInfo) -> bool;
}
```

V1 implementation always returns `false`. Dirty pages are not evicted or flushed incrementally. The `page_lsn` field exists for future physical WAL without changing the trait.

## Concurrency Controller

```rust
pub trait ConcurrencyController: Send + Sync {
    fn begin_read(&self) -> Result<ReadGuard>;
    fn begin_write(&self) -> Result<WriteGuard>;
}

pub struct RwLockConcurrencyController { /* parking_lot::RwLock<()> */ }

impl RwLockConcurrencyController {
    pub fn new() -> Self;
}

pub struct ReadGuard { /* owned guard */ }
pub struct WriteGuard { /* owned guard */ }
```

V1 implementation uses `parking_lot` owned `RwLock` guards internally. Reads run concurrently. Writes, DML, DDL, and checkpoints acquire the write guard. Guards are owned to keep the trait object-safe.

## Invariants

- IDs are stable and never reused within a database.
- `Row` carries only values. Schemas are external.
- `ExecRow.identity` is preserved through filters, sort, limit, and projection; joins and aggregates produce `None`.
- `common` must not depend on any other SaguaroDB crate.
- `FlushPolicy` must not reference `wal` types directly beyond `Lsn`.

## Acceptance Tests

- `Value` ordering is deterministic across variants and values.
- `ColumnInfo` can represent base table columns and expression aliases.
- `ExecRow` can carry row identity independently of projected columns.
- `FlushPolicy` can be mocked by buffer tests without linking the WAL crate.
