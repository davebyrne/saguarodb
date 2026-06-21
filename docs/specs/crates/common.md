# `common` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`common` defines stable cross-crate types and small traits that must not depend on implementation crates. It is the leaf crate for the workspace.

## Owns

- Stable identifiers: `TableId`, `ColumnId`, `IndexId`, `BindingId`, `FileId`, `PageNum`, `Lsn`.
- SQL values and row envelopes: `Value`, `Row`, `Key`, `StoredRow`, `ExecRow`, `RowIdentity`.
- Schema description types: `DataType`, `ParsedColumnDef`, `ColumnDef`, `ColumnInfo`, `TableSchema`, `IndexSchema`.
- Query access helpers: `KeyRange`.
- Error model: `DbError`, `ErrorKind`, `SqlState`, `Result<T>`.
- Statement context and the transaction extension point.
- Runtime MVCC types: `Snapshot`, `TxnStatus`, `IsolationLevel` (see `docs/specs/mvcc.md`).
- The tuple-visibility predicate `is_visible` and the `TxnStatusView` trait, plus
  the `infomask` settled-status hint-bit constants `XMIN_COMMITTED`,
  `XMIN_ABORTED`, `XMAX_COMMITTED`, `XMAX_ABORTED` (the single source of truth for
  these bits; `storage`'s tuple codec re-uses them).
- Cross-cutting traits: `FlushPolicy`, `ConcurrencyController`, `TxnStatusView`.

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

pub struct IndexSchema {
    pub id: IndexId,
    pub table: TableId,
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub unique: bool,
}
```

`ParsedColumnDef` is parser output and never has IDs. `ColumnDef` is catalog-owned and always has stable IDs. `ColumnInfo` describes result columns and may be derived from expressions, so table/column IDs are optional.

`IndexSchema` is the catalog-owned secondary-index metadata type. A `unique` index rejects duplicate non-NULL indexed values (NULLs are distinct); a non-unique index admits duplicates. On disk every index entry is disambiguated by the heap TID it points at (see `storage` Secondary Indexes), so no metadata distinguishes the two beyond the `unique` flag.

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
    QueryCanceled,
    FeatureNotSupported,
    InFailedSqlTransaction,
    IoError,
    InternalError,
}

pub type Result<T> = std::result::Result<T, DbError>;
```

All crates return `common::Result<T>`. Crates should map low-level errors into the nearest `ErrorKind` and SQLSTATE at the boundary where context is available.

`SqlState::InFailedSqlTransaction` maps to SQLSTATE `25P02`: a statement other
than `COMMIT`/`ROLLBACK` issued inside an already-failed (`'E'`) transaction block.
The server raises it while gating an aborted transaction block (see
`docs/specs/crates/server.md` and `docs/specs/mvcc.md` §7.2).

`DbError` exposes convenience constructors used consistently across crates: `DbError::parse(code, message)`, `DbError::plan(code, message)`, `DbError::execute(code, message)`, `DbError::storage(code, message)`, `DbError::wal(code, message)`, `DbError::protocol(code, message)`, `DbError::io(message)`, and `DbError::internal(message)`. Constructors set `kind`, `code`, and `message`; `io` uses `SqlState::IoError`, and `internal` uses `SqlState::InternalError`.

`DbError` derives `thiserror::Error` with `#[error("{message}")]`, so it is a real `std::error::Error` whose `Display` renders the `message` field.

## Statement Context

```rust
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Arc<Snapshot>,
    pub isolation: IsolationLevel,
}
```

A statement (autocommit or one statement of an explicit transaction) carries one
`txn_id`. The `snapshot` is the visibility snapshot threaded into the storage
engine's read paths (see `docs/specs/mvcc.md` §5.5, §6) and is **consulted by scans
and point lookups** (Appendix A commit 6): invisible versions are skipped. It is
held behind an `Arc` so the executor clones a `StatementContext` per scan operator
by bumping a refcount rather than deep-cloning the `xip` vector — which matters
once concurrent transactions make `xip` non-empty (Milestone C). The server's
transaction read/write paths build it with
`StatementContext::with_snapshot(txn_id, snapshot)` (default isolation) or
`StatementContext::with_snapshot_and_isolation(txn_id, snapshot, isolation)`.
`StatementContext::new(txn_id)` fills `snapshot` with the equivalent
`Snapshot::sees_all_committed()` placeholder (every committed row and own write is
visible, so pre-capture call sites — tests, recovery scaffolding — filter nothing)
and `isolation` with the default (`IsolationLevel::ReadCommitted`). `isolation`
selects the server's snapshot-capture timing (Read Committed = fresh per statement,
Repeatable Read = captured once per transaction); the storage engine does not
consult it. `StatementContext` is `Clone` but not `Copy`.

## MVCC Types

Runtime-only MVCC types (no `serde`/durable derives; CLOG's on-disk status
representation is a separate concern). See `docs/specs/mvcc.md` for the model.

```rust
pub struct Snapshot {
    pub xmin: u64,     // lowest still-running xid; below this, status is settled via CLOG
    pub xmax: u64,     // next xid to be assigned; >= xmax is invisible (the future)
    pub xip: Vec<u64>, // in-progress xids in [xmin, xmax) at snapshot capture
}

pub enum TxnStatus { InProgress, Committed, Aborted }

pub enum IsolationLevel { ReadCommitted, RepeatableRead /* = snapshot isolation */ }
```

`Snapshot::empty()` (also `Default`) is the degenerate `{ xmin: 0, xmax: 0, xip:
[] }` placeholder; because `xmax = 0` every transaction is "in the future", so it
sees **nothing** under `is_visible`. `Snapshot::sees_all_committed()` is the
single-writer autocommit placeholder (`{ xmin: u64::MAX, xmax: u64::MAX, xip: []
}`): no transaction is in the future and there are no in-progress ids, so every
committed transaction — and the reader's own writes via the predicate's
`current_txn` path — is visible. `StatementContext::new` uses
`sees_all_committed()`, not `empty()`. `IsolationLevel::default()` is
`ReadCommitted` (Postgres' default). The `snapshot` is consulted by the storage
engine's scans/point lookups (Appendix A commit 6); `isolation` is honored from
Milestone G.

## Visibility

The pure tuple-visibility predicate (`docs/specs/mvcc.md` §6) lives in `common`,
along with the transaction-status view it consults and the `infomask` hint bits.

```rust
pub const XMIN_COMMITTED: u16 = 1 << 0;
pub const XMIN_ABORTED:   u16 = 1 << 1;
pub const XMAX_COMMITTED: u16 = 1 << 2;
pub const XMAX_ABORTED:   u16 = 1 << 3;

pub trait TxnStatusView {
    fn status(&self, xid: u64) -> TxnStatus;
    fn is_committed(&self, xid: u64) -> bool { /* status == Committed */ }
    fn is_aborted(&self, xid: u64) -> bool { /* status == Aborted */ }
}

pub fn is_visible(
    xmin: u64, xmax: u64, infomask: u16,
    snapshot: &Snapshot, current_txn: u64,
    status: &dyn TxnStatusView,
) -> bool;
```

- `TxnStatusView` exposes transaction status to the predicate without `common`
  depending on `wal`. The CLOG-backed impl (`impl TxnStatusView for Clog`, and the
  `dyn WalManager` supertrait) lives in `wal`; reserved ids (`< FIRST_NORMAL_XID`,
  incl. `FROZEN_XID`) must read as `Committed`.
- `is_visible` returns true iff the creator `xmin` is visible (own write, or
  settled-committed and in the snapshot's past) **and** the deleter `xmax` does not
  hide the row (invalid, or itself not visible). `xmax == current_txn` hides the
  row (own delete); the Read-Committed command-id nuance is deferred to Milestone
  G (no command ids yet).
- `infomask` hint bits (`XMIN_*`/`XMAX_*`) short-circuit the `TxnStatusView` probe
  for a settled xid. The four bits are the canonical definition shared with the
  storage tuple codec, which re-exports them.
- `is_visible` is pure (no I/O, no locks beyond whatever the caller's
  `TxnStatusView` takes per probe) and is not yet called by any scan (B3.6).

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

V1's `WalFlushPolicy` admits committed (or recovery, txn 0), WAL-durable pages; the checkpoint flushes them in place to the heap. The `page_lsn` field lets eviction-flush-on-steal check WAL durability without changing the trait.

## Concurrency Controller

```rust
pub trait ConcurrencyController: Send + Sync {
    fn begin_read(&self) -> Result<ReadGuard>;
    fn begin_write(&self) -> Result<WriteGuard>;
}

pub struct RwLockConcurrencyController { /* lock: Arc<parking_lot::RwLock<()>> */ }

impl RwLockConcurrencyController {
    pub fn new() -> Self;
}

impl Default for RwLockConcurrencyController { /* delegates to new() */ }

pub struct ReadGuard { /* owned ArcRwLockReadGuard */ }
pub struct WriteGuard { /* owned ArcRwLockWriteGuard */ }
```

V1 implementation holds the `RwLock` in an `Arc` and hands out `parking_lot` owned guards (`ArcRwLockReadGuard` / `ArcRwLockWriteGuard`) acquired via `read_arc()` / `write_arc()`. Reads run concurrently. Writes, DML, DDL, and checkpoints acquire the write guard. Guards are owned to keep the trait object-safe.

## Invariants

- IDs are stable and never reused within a database.
- `Row` carries only values. Schemas are external.
- `ExecRow.identity` is preserved through filters, sort, limit, and projection; joins and aggregates produce `None`.
- `common` must not depend on any other SaguaroDB crate.
- `FlushPolicy` must not reference `wal` types directly beyond `Lsn`.
- Most public value, row, schema, error, id, flush, and context types derive serde `Serialize`/`Deserialize`; this serializability is part of the public/persistence contract.

## Acceptance Tests

- `Value` ordering is deterministic across variants and values.
- `ColumnInfo` can represent base table columns and expression aliases.
- `ExecRow` can carry row identity independently of projected columns.
- `FlushPolicy` can be mocked by buffer tests without linking the WAL crate.
