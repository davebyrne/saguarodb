# `common` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`common` defines stable cross-crate types and small traits that must not depend on implementation crates. It is the leaf crate for the workspace.

## Owns

- Stable identifiers: `TableId`, `ColumnId`, `IndexId`, `BindingId`, `FileId`, `PageNum`, `Lsn`.
- SQL values and row envelopes: `Value`, `Row`, `Key`, `StoredRow`, `ExecRow`, `RowIdentity`.
- The shared boolean-text decoder `parse_bool_text(&str) -> Option<bool>`
  (PostgreSQL `boolin` accept-set), reused by the `protocol` extended-query
  parameter path and the `COPY` import path so both share one accept-set; each
  caller maps `None` to its own SQLSTATE.
- Schema description types: `DataType`, `ParsedColumnDef`, `ColumnDef`, `ColumnInfo`, `TableSchema`, `IndexSchema`.
- Query access helpers: `KeyRange`.
- Error model: `DbError`, `ErrorKind`, `SqlState`, `Result<T>`.
- Statement context and the transaction extension point.
- Runtime MVCC types: `Snapshot`, `TxnStatus`, `IsolationLevel` (see `docs/specs/mvcc.md`).
- The tuple-visibility predicate `is_visible` and the `TxnStatusView` trait, plus
  the `infomask` settled-status hint-bit constants `XMIN_COMMITTED`,
  `XMIN_ABORTED`, `XMAX_COMMITTED`, `XMAX_ABORTED` (the single source of truth for
  these bits; `storage`'s tuple codec re-uses them).
- The pure write-conflict classifiers (see `docs/specs/mvcc.md` §7.3): the
  uniqueness liveness check `version_conflicts` and its three-way refinement
  `classify_unique_conflict -> UniqueConflict` (`None`/`Violation`/`InFlight`, which
  splits a definite duplicate `23505` from an in-flight-other `40001`), and the
  write-write row-lock check `write_conflict -> WriteConflict`.
- The pure VACUUM reclaimability oracle `is_dead_to_all` (see
  `docs/specs/mvcc.md` §9), the sibling of `is_visible`: it answers "is this
  version dead to **every** snapshot?" against a single scalar GC `horizon`, used
  by VACUUM (Milestone F) rather than by snapshot-relative reads.
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
    Float(OrderedF64), // DOUBLE PRECISION (total-order f64 wrapper)
    Text(String),
    Date(i64),       // days from the Unix epoch (1970-01-01)
    Timestamp(i64),  // microseconds from the Unix epoch (no time zone)
    Bytes(Vec<u8>),  // BYTEA, raw bytes
    Uuid([u8; 16]),  // UUID, 16 bytes
}
```

`Value::Date`/`Value::Timestamp` are backed by `i64`, `Value::Bytes` by
`Vec<u8>`, and `Value::Uuid` by `[u8; 16]`, so the derived `Ord`/`Hash` give
correct chronological / lexicographic ordering and key/dedup behavior. `f64` does
not implement `Ord`/`Eq`/`Hash`, so `Value::Float` wraps it in the `float`
module's `OrderedF64`, which supplies a total order matching PostgreSQL's float
btree semantics (`NaN` sorts greatest and equals itself, `-0.0 == +0.0`) and a
consistent `Hash`, keeping `Value`'s derives valid for keys, `DISTINCT`, and
grouping. The `datetime` module provides the proleptic Gregorian calendar
conversions and the `YYYY-MM-DD` / `YYYY-MM-DD HH:MM:SS[.ffffff]` parse/format
helpers (`days_from_civil`, `civil_from_days`, `parse_date`, `format_date`,
`parse_timestamp`, `format_timestamp`); the `bytea` module provides the hex
`\x...` parse/format helpers (`parse_hex`, `format_hex`, hex-only — no legacy
escape); the `uuid` module provides the canonical `8-4-4-4-12` parse/format
helpers (`parse_uuid` lenient, `format_uuid` canonical lowercase); the `float`
module provides the `format_double` / `parse_double` helpers (round-trippable
text: fixed-point for moderate magnitudes, `e±NN` scientific for extreme
exponents, and `Infinity`/`-Infinity`/`NaN` for non-finite values).
All are shared by the parser, executor, protocol, and COPY paths; there is no
external date/time/uuid/float dependency.

```rust
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

`Value` ordering is used for B-tree keys. The ordering is total and deterministic: `Null < Boolean < Integer < Text`, with natural ordering inside each variant. SQL comparison semantics still apply in expression evaluation; B-tree ordering is a storage ordering.

## Column Lifecycle Types

```rust
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Date,
    Timestamp,
    Bytea,
    Uuid,
    Double,
}

pub struct ParsedColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub max_length: Option<u32>,  // VARCHAR(n)/CHAR(n) length; None = unbounded
}

pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub max_length: Option<u32>,  // VARCHAR(n)/CHAR(n) length; None = unbounded
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
    InvalidColumnReference,
    DuplicateTable,
    DatatypeMismatch,
    DivisionByZero,
    NumericValueOutOfRange,
    StringDataRightTruncation,
    InvalidTextRepresentation,
    BadCopyFileFormat,
    NotNullViolation,
    UniqueViolation,
    QueryCanceled,
    FeatureNotSupported,
    InFailedSqlTransaction,
    NoActiveSqlTransaction,
    InvalidSavepointSpecification,
    SerializationFailure,
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

`SqlState::NoActiveSqlTransaction` maps to SQLSTATE `25P01`: a savepoint command
(`SAVEPOINT`/`RELEASE`/`ROLLBACK TO`) issued with no open transaction block.
`SqlState::InvalidSavepointSpecification` maps to `3B001`: `RELEASE`/`ROLLBACK TO`
named a savepoint that does not exist. Both are raised on the savepoint path; see
`docs/specs/savepoints.md` §2.

`SqlState::SerializationFailure` maps to SQLSTATE `40001`: a write conflict under
MVCC's fail-fast, first-updater-wins policy. It arises in two cases (no blocking, no
deadlock detection): a **write-write** conflict, when the losing UPDATE/DELETE finds
the target version's `xmax` row-lock already held by a committed or in-progress
transaction (classifier `common::mvcc::write_conflict`); and a **concurrent-inserter
unique** conflict (Milestone E1c), when an INSERT finds the unique key held only by
another in-progress inserter that may yet abort, so uniqueness is undecidable
(classifier `common::mvcc::classify_unique_conflict` → `UniqueConflict::InFlight`).
A committed-live duplicate is instead a definite `UniqueViolation` (`23505`). See
`docs/specs/mvcc.md` §7.3, Milestone E.

`SqlState::InvalidTextRepresentation` maps to SQLSTATE `22P02`: a text field could
not be parsed into its target type. `SqlState::BadCopyFileFormat` maps to SQLSTATE
`22P04`: a `COPY ... FROM` input row is structurally malformed (wrong column count
or an unterminated CSV quote). Both are raised on the `COPY` import path; see
`docs/specs/copy.md` §7.

`DbError` exposes convenience constructors used consistently across crates: `DbError::parse(code, message)`, `DbError::plan(code, message)`, `DbError::execute(code, message)`, `DbError::storage(code, message)`, `DbError::wal(code, message)`, `DbError::protocol(code, message)`, `DbError::io(message)`, and `DbError::internal(message)`. Constructors set `kind`, `code`, and `message`; `io` uses `SqlState::IoError`, and `internal` uses `SqlState::InternalError`.

`DbError` derives `thiserror::Error` with `#[error("{message}")]`, so it is a real `std::error::Error` whose `Display` renders the `message` field.

## Statement Context

```rust
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Arc<Snapshot>,
    pub isolation: IsolationLevel,
    pub gc_horizon: u64,
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
consult it. `gc_horizon` carries the GC horizon (minimum advertised snapshot `xmin`)
the server captured for the statement; it is consumed ONLY by the storage engine's
HOT update-path prune (`docs/specs/mvcc.md` §10 Milestone H3) and defaults to `0`
(prune nothing committed-dead) for read/pre-capture/test contexts, set on write paths
via `StatementContext::with_gc_horizon(gc_horizon)`. A stale/smaller horizon only
prunes less, never unsafely. `StatementContext` is `Clone` but not `Copy`.

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

pub fn is_dead_to_all(
    xmin: u64, xmax: u64, infomask: u16,
    horizon: u64,
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
- `is_dead_to_all` is the VACUUM-side sibling of `is_visible` (`mvcc.md` §9): it
  returns true iff the version is dead to **every** possible snapshot, given the GC
  `horizon` (the oldest still-running xid). Reclaimable iff **either** the creator
  aborted (`XMIN_ABORTED`, or `status(xmin) == Aborted`) — **no age requirement**,
  an aborted creator is universally invisible — **or** it is committed-deleted
  below the horizon (`xmax != 0`, settled-committed via `XMAX_COMMITTED` or
  `status(xmax) == Committed`, **and** `xmax < horizon`, strict). A live committed
  version, an aborted/in-progress deleter, or a committed delete with
  `xmax >= horizon` is not reclaimable. Pure and honours the same `infomask` hint
  bits to skip CLOG probes; takes a scalar `horizon` rather than a `Snapshot`. No
  production caller yet (Milestone F2+ wires it into prune/vacuum).

## Flush Policy

```rust
pub struct PageFlushInfo {
    pub dirty_txn_id: u64,
    pub page_lsn: Option<Lsn>,
}

pub trait FlushPolicy: Send + Sync {
    fn can_flush(&self, info: &PageFlushInfo) -> bool;
    /// Force every WAL record durable up to now, so a dirty page about to be
    /// written to its home satisfies write-ahead logging. Default no-op (tests).
    fn ensure_durable(&self) -> Result<()> { Ok(()) }
}
```

`WalFlushPolicy` admits any **WAL-durable** dirty page (`page_lsn ≤ wal.flushed_lsn()`), committed or not (Milestone D1, `mvcc.md` §8): the committedness gate of earlier milestones is dropped because a heap page holds versions from several transactions, and the CLOG hides the non-committed ones. The checkpoint flushes such pages in place to the heap. `ensure_durable` (implemented by `WalFlushPolicy` as `wal.flush`) is called by the buffer pool's steal path before writing a stolen — possibly uncommitted — dirty page, giving write-ahead logging (the page's records reach disk before the page does); the pre-D1 committed-only steal needed no such force.

## Concurrency Controller

The controller is the **writer-vs-checkpoint** coordination primitive. As of Milestone E2b (`mvcc.md` §7.1 Stage 2, §10 E2b) the lock is **inverted**: writers take the SHARED side and the checkpoint takes the EXCLUSIVE side, so many write-transactions run concurrently while a checkpoint drains them and runs alone.

```rust
pub trait ConcurrencyController: Send + Sync {
    /// SHARED writer guard — many concurrent writers; blocks only behind a checkpoint.
    fn begin_writer(&self) -> Result<WriteGuard>;
    /// EXCLUSIVE checkpoint guard — drains all writers, then runs alone.
    fn begin_checkpoint(&self) -> Result<CheckpointGuard>;
    /// SHARED guard for a non-writing exclusion participant (default = begin_writer).
    fn begin_shared(&self) -> Result<WriteGuard> { self.begin_writer() }
}

pub struct RwLockConcurrencyController { /* lock: Arc<parking_lot::RwLock<()>> */ }

impl RwLockConcurrencyController {
    pub fn new() -> Self;
}

impl Default for RwLockConcurrencyController { /* delegates to new() */ }

pub struct WriteGuard { /* owned ArcRwLockReadGuard — the SHARED side */ }
pub struct CheckpointGuard { /* owned ArcRwLockWriteGuard — the EXCLUSIVE side */ }
```

The implementation holds a `parking_lot::RwLock` in an `Arc` and hands out owned guards. **Writers** acquire the SHARED side (`begin_writer` → `read_arc()`): many run at once, relying on per-row conflict detection (E1) and the per-index / per-heap structural latches (E2a) — not this lock — for write-write safety. The **checkpoint** acquires the EXCLUSIVE side (`begin_checkpoint` → `write_arc()`): it blocks until every in-flight writer has drained, then holds off any new writer until it returns, so the checkpoint body runs with **no in-flight writer** (preserving the recovery / truncation invariant: every transaction below the truncation boundary is settled and captured by `persist_clog`'s snapshot — `mvcc.md` §5.4, §8). The shared side is re-entrant (a connection re-acquiring it cannot self-deadlock), so the "at most one writer guard per transaction" rule is a correctness assertion at the transaction layer, not a deadlock guard. **Readers take no guard at all** and run lock-free. Guards are owned to keep the trait object-safe. (Pre-E2b the lock was the other way around — `begin_read`/`begin_write` with a single exclusive writer; the inversion is the only API/behavior change.)

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
