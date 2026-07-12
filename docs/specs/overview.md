# SaguaroDB Overview Specification

**Date:** 2026-07-04
**Status:** Living system contract

The user catalog is schema-aware. `public` exists by default; `CREATE SCHEMA` and
restrictive `DROP SCHEMA` are transactional, one- and two-part user relation names
are supported, and unqualified names follow the session `search_path`. Catalog
objects remain ID-based after binding, while views persist their definition search
path by schema id so later session changes cannot retarget them.

## 1. Overview

SaguaroDB is a SQL-compatible relational database written in Rust. It is a standalone server that accepts client connections over a network, executes SQL queries against a page-oriented storage engine, and returns results over the PostgreSQL wire protocol.

### Goals

- Standalone multi-client server
- PostgreSQL wire protocol support for startup, cancellation, simple query,
  extended query, and COPY sub-protocol messages (abstracted for future custom
  protocol)
- Page-oriented storage engine with a durable on-disk non-clustered storage-identity B-tree per table (primary-key values when present, hidden heap identity otherwise; abstracted for future clustered/on-disk-index work)
- PostgreSQL-style MVCC with snapshot isolation: multi-statement transactions plus autocommit for standalone statements
- Data types: `INTEGER` (i64; `SMALLINT`/`INT2`, `INTEGER`/`INT`/`INT4`, and `BIGINT`/`INT8` all share one 64-bit integer storage but report their distinct PostgreSQL width OIDs (`int2`/`int4`/`int8`; bare `INTEGER` is `int4`), and `int2`/`int4` values are range-checked at write and cast time (`SqlState::NumericValueOutOfRange` when out of range); `SERIAL`/`SMALLSERIAL`/`BIGSERIAL` family column pseudo-types desugar to `INTEGER NOT NULL DEFAULT nextval('<owned-sequence>')` and report their serial kind's width), `TEXT` (`VARCHAR(n)`/`CHAR(n)`/`CHARACTER(n)` are stored as `TEXT` with a max-length-of-`n`-characters constraint enforced at write time, and reported on the wire as `varchar`/`bpchar`/`text` with the declared length; not blank-padded), `BOOLEAN`, `DATE` (calendar date written `DATE 'YYYY-MM-DD'`, stored as days from the Unix epoch), `TIMESTAMP` (without time zone, written `TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff]'`, stored as microseconds from the Unix epoch), `TIME` (without time zone, written `TIME 'HH:MM:SS[.ffffff]'`, stored as microseconds since midnight), `TIMESTAMP WITH TIME ZONE`/`TIMESTAMPTZ` (UTC-normalized: an input offset is converted to UTC, always displayed as `...+00`), `INTERVAL` (months/days/microseconds kept separate, PostgreSQL `postgres`-style text; compares by canonical estimate so `1 mon` = `30 days`; supports `interval ± interval`, `interval * integer`, unary `- interval`, and calendar-aware `DATE`/`TIMESTAMP`/`TIMESTAMPTZ`/`TIME` `± interval`), `BYTEA` (raw byte string; hex text I/O `\xDEADBEEF`), `UUID` (16 bytes; canonical `8-4-4-4-12` text), `DOUBLE PRECISION` (IEEE 754 `f64`; `FLOAT8`/`FLOAT` accepted as aliases; supports arithmetic and `SUM`/`AVG`), `REAL` (IEEE 754 `f32`; `FLOAT4`/`FLOAT(1..24)` accepted as aliases; supports arithmetic and `SUM`/`AVG`), `NUMERIC`/`DECIMAL` (exact decimal written `NUMERIC 'D.DDD'`, optional `(precision[, scale])` up to 28 digits; values rounded to the column scale on store; supports arithmetic and `SUM`/`AVG`), `NULL`
- SQL subset: `CREATE TABLE [IF NOT EXISTS]` (with column `NULL`/`NOT NULL`, optional primary key, constant `DEFAULT`, `DEFAULT nextval('<sequence>')`, non-constant expression `DEFAULT` (bound against an empty scope, evaluated per row), `SERIAL`/`SMALLSERIAL`/`BIGSERIAL` family constraints, `CHECK (...)` constraints (column-level and table-level, unnamed; enforced per row on `INSERT`/`UPDATE`/`COPY FROM` with `SqlState::CheckViolation`), and optional trailing `WITH (...)` storage options: `compression = 'none' | 'zstd'`, `toast = 'off' | 'auto' | 'aggressive'`, `toast_tuple_target = <integer>`, `toast_min_value_size = <integer>`, and `toast_compression = 'none' | 'zstd' | 'zstd_dict'`; see `docs/specs/compression.md` and TOAST metadata below), `DROP TABLE [IF EXISTS]`, `CREATE [OR REPLACE] VIEW <name> [(cols)] AS <select>`, `DROP VIEW [IF EXISTS]`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`, `CREATE SEQUENCE`, `DROP SEQUENCE [IF EXISTS]`, `INSERT ... VALUES`, `INSERT ... SELECT`, standalone `VALUES (...), (...)` (as a query — top-level, in `FROM`, or as a subquery body), set operations `UNION [ALL]` / `INTERSECT [ALL]` / `EXCEPT [ALL]` (arms must have the same column count and identical types, except a bare `NULL` column adopts the sibling arm's type when at least one arm types all its own columns (otherwise an explicit cast is required); `ORDER BY`/`LIMIT` apply to the combined result; the `ALL` forms use multiset semantics), non-recursive CTEs `WITH name [(cols)] AS (query), ...` (inlined as named derived tables; a CTE name shadows a catalog table; `WITH RECURSIVE` is not supported), `SELECT` (with an optional `FROM` — including user views expanded from stored definitions, a FROM-less scalar projection such as `SELECT 1` or `SELECT count(*)`, `DISTINCT`, `WHERE`, inner/cross/left/right/full joins, scalar / `[NOT] IN` / `[NOT] EXISTS` subqueries (correlated in `WHERE`, the select list, and `HAVING`; equality shapes run as hash semi/anti joins), `[LEFT JOIN] LATERAL` derived tables (`docs/specs/subqueries.md`), `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, sequence functions `nextval`/`currval`/`setval`, statement clock functions `current_timestamp` and `now()`, PostgreSQL-compatible system information functions `version()`, `current_database()`, `current_catalog`, `current_schema`, `current_user`, `session_user`, `user`, `pg_backend_pid()`, and `current_setting(text)`, plus PostgreSQL-compatible catalog introspection/probe functions such as `format_type`, `pg_get_indexdef`, `pg_get_expr`, `pg_get_constraintdef`, `pg_table_is_visible`, `to_regclass`, and `has_*_privilege`), forward-only read-only SQL cursors (`DECLARE name CURSOR FOR SELECT ...`, `FETCH name`, `FETCH [FORWARD] [n] FROM name`, `FETCH ALL FROM name`, `CLOSE name`) inside explicit transaction blocks, `UPDATE` (including `UPDATE ... FROM <items>` — extra relations joined with the target, `docs/specs/subqueries.md` §8), `DELETE` (including `DELETE ... USING <items>`), `INSERT`/`UPDATE`/`DELETE ... RETURNING <expr_list | *>` (the statement produces a result set evaluated over each affected row — the new row for `INSERT`/`UPDATE`, the deleted row for `DELETE`), `INSERT ... ON CONFLICT [(pk)] DO NOTHING | DO UPDATE SET ... [WHERE ...]` (upsert; the conflict arbiter is the primary key only — `excluded.<col>` references the proposed row), `EXPLAIN`, transaction control (`BEGIN`/`START TRANSACTION [ISOLATION LEVEL <level>]`, `COMMIT`, `ROLLBACK`, `SET TRANSACTION ISOLATION LEVEL <level>`, `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` — Read Committed / Repeatable Read / Serializable, setting the per-connection default for future transactions; `SET`/`SHOW`/`RESET` of session configuration parameters including PostgreSQL-compatible `transaction_isolation` and `default_transaction_isolation`; `DISCARD ALL`; SERIALIZABLE is Serializable Snapshot Isolation (SSI), see `docs/specs/ssi.md`; and savepoints `SAVEPOINT`/`RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT` — nested subtransactions, see `docs/specs/savepoints.md`), the maintenance commands `VACUUM [table]`, `TRUNCATE [TABLE] <table>`, `ALTER TABLE <table> SET (compression = 'none' | 'zstd')` (full heap/index rewrite), `ALTER TABLE <table> SET (toast = ..., toast_tuple_target = ..., toast_min_value_size = ..., toast_compression = ...)` (future-write-only TOAST policy update; mixed page-compression/TOAST SET lists are rejected), `ALTER TABLE [ONLY] <table> ADD [CONSTRAINT <name>] PRIMARY KEY (cols...)` / `ALTER TABLE [ONLY] <table> DROP PRIMARY KEY | DROP CONSTRAINT <name>`, schema-evolution DDL `ALTER TABLE <table> ADD COLUMN [IF NOT EXISTS] <column>`, `ALTER TABLE <table> DROP COLUMN [IF EXISTS] <column>`, `ALTER TABLE <table> RENAME COLUMN <old> TO <new>`, and `ALTER TABLE <table> RENAME TO <new>`, and the bulk-transfer command `COPY <table> [(cols)] FROM STDIN | TO STDOUT [WITH (...)]` (text/CSV, simple-query only; see `docs/specs/copy.md`); binder rejects unsupported parsed forms.
- Schema-evolution DDL also supports `ALTER TABLE [ONLY] <table> ALTER [COLUMN]
  <column> TYPE <type>` and `... SET DATA TYPE <type>`. It performs an explicit-cast
  full heap/index rewrite transactionally; `USING` expressions are unsupported.
- `VACUUM ANALYZE` and `VACUUM ANALYZE <table>` are compatibility spellings of
  the corresponding VACUUM commands. The reclamation pass still runs; `ANALYZE`
  itself is discarded because the rule-based planner has no statistics catalog.
- Rule-based query planner (no cost-based optimization)
- Transaction-owned table locks coordinate reads, writes, DDL, and maintenance by
  logical `TableId`, share the row-lock deadlock graph, and provide transactional
  multi-table TRUNCATE; see `docs/specs/table-locks.md`.
- Primary-key and secondary-index access paths (full table scans otherwise)
- WAL with crash recovery
- Async networking (Tokio) with blocking thread pool for query execution

### Non-Goals

- Time-travel / as-of queries
- Mutual TLS / client-certificate authentication (optional server-side TLS is supported)
- Authentication
- Replication
- Custom wire protocol (designed for, not implemented)

## 2. Architecture

Trait-boundary architecture: a single binary with each major subsystem communicating through well-defined Rust traits. Traits act as hard seams for testability and future swappability.

### Crate Layout

```
saguarodb/
├── Cargo.toml              (workspace root)
├── crates/
│   ├── server/             (binary — entry point, wires everything together)
│   ├── protocol/           (wire protocol trait + PostgreSQL implementation)
│   ├── parser/             (wrapper around sqlparser-rs, produces internal AST)
│   ├── planner/            (rule-based query planner, produces execution plans)
│   ├── executor/           (query execution engine, evaluates plans against storage)
│   ├── storage/            (storage engine trait + page-backed table implementation)
│   ├── control/            (control record — checkpoint commit point)
│   ├── buffer/             (buffer pool — in-memory page cache)
│   ├── wal/                (write-ahead log)
│   ├── catalog/            (table metadata, schema definitions)
│   ├── compress/           (compression codecs, at-rest page envelope, TOAST value helpers, dictionaries)
│   └── common/             (shared types: DataType, Value, Row, errors, config)
```

### Dependency Flow

```
server → protocol, parser, planner, executor, control, storage, buffer, wal, catalog, compress, common
protocol → common
parser → common
planner → parser, catalog, common
executor → planner, storage, catalog, common
storage → buffer, wal, compress, common
control → common
buffer → common
wal → common
catalog → common
compress → common
```

No circular dependencies. `common` and `compress` are leaf crates (`compress` also depends only on `common`). `server` is the root. `compress` is consumed by `storage` (at-rest page compression, WAL full-page-image compression, and TOAST value payload compression/decompression) and `server` (constructs and shares the `CompressionRegistry`/`DictStore`); `wal` does not depend on `compress` (`docs/specs/compression.md`, `docs/specs/crates/compress.md`).

### Cargo Package and Crate Naming

Workspace package names use the project prefix, for example `saguarodb-common` and `saguarodb-storage`. Internal crates depend on those packages through short aliases:

```toml
[dependencies]
common = { package = "saguarodb-common", path = "../common" }
storage = { package = "saguarodb-storage", path = "../storage" }
```

Internal Rust code should use paths like `common::Result` and `storage::StorageEngine`. A future public facade crate may expose paths such as `saguarodb::common::DbError`, but internal crates should not depend on that facade.

### Core Types (in `common`)

#### Identifiers

```rust
pub type TableId = u32;
pub type ColumnId = u16;
pub type IndexId = u32;
pub type SequenceId = u32;
pub const PRIMARY_KEY_INDEX_ID: IndexId = 0;
pub type BindingId = u32;
pub type PageNum = u32;
pub type FileId = u32;
pub type Lsn = u64;

/// Physical address of a row on disk
pub struct RowId {
    pub page_num: PageNum,
    pub slot_num: u16,
}
```

#### Value and Row

```rust
/// A single SQL value. This is the fundamental unit of data throughout the system.
/// Implements Ord for use as B-tree keys; the cross-variant order is the
/// declaration order below (Null sorts first).
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(OrderedF64),// DOUBLE PRECISION; total-order f64 wrapper
    Real(OrderedF32), // REAL; total-order f32 wrapper
    Numeric(Decimal), // NUMERIC/DECIMAL; exact decimal (compares by value)
    Text(String),  // Future: consider Arc<str> for zero-copy from buffer pool
    Date(i64),     // days from the Unix epoch (1970-01-01)
    Timestamp(i64),// microseconds from the Unix epoch (no time zone)
    Time(i64),     // microseconds since midnight (no time zone)
    TimestampTz(i64),// microseconds from the Unix epoch, UTC-normalized
    Interval(Interval),// months/days/micros; compares by canonical estimate
    Bytes(Vec<u8>),// BYTEA, raw bytes
    Uuid([u8; 16]),// UUID, 16 bytes
    Array(SqlArray),// homogeneous rectangular SQL array
}

/// An ordered sequence of values representing one tuple.
/// Columns are accessed by position index. The schema (column names, types)
/// is carried externally — the Row is a "bare tuple."
pub struct Row {
    pub values: Vec<Value>,
}

/// Composite key for lookups (primary key may span multiple columns)
pub struct Key(pub Vec<Value>);

/// A row together with its physical identity. Returned by storage scans
/// so that UPDATE/DELETE can target the exact row without relying on
/// primary key columns being present in the projected output.
pub struct StoredRow {
    pub row_id: RowId,
    pub key: Key,
    pub row: Row,
}

/// Key range for index/primary key scans. Defined in common so both
/// planner and storage can use it without a dependency between them.
pub enum KeyRange {
    Exact(Key),                            // WHERE pk = value
    Range { start: std::ops::Bound<Key>, end: std::ops::Bound<Key> },  // WHERE pk BETWEEN a AND b
    All,                                   // Full index scan
}
```

`Value` derives `Ord`/`Eq`/`Hash` from its declaration order, and that derived order **is** the durable B-tree key ordering (the on-disk index compares decoded `Key(Vec<Value>)` values directly). Variant order in `Value` (and, by the same conservatism, `DataType`) is therefore a durable on-disk contract: new variants must be **appended** at the end of the enum — never inserted or reordered mid-enum — unless the key ordering/encoding is deliberately revisited and migrated. `Array` is appended after `Uuid`; its `SqlArray` payload preserves a scalar element type, up to six dimensions with lower bounds, and flattened row-major elements. `crates/common/src/value.rs` is the authoritative definition; see `docs/specs/crates/common.md`.

#### Data Types

```rust
/// SQL data types. Defined in common — used by parser, planner, catalog, and executor.
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Date,
    Timestamp,
    Time,
    TimestampTz,
    Interval,
    Bytea,
    Uuid,
    Double,
    Real,
    Numeric { precision: Option<u32>, scale: u32 },
    Array(ArrayType),
}

// Constructed with ArrayType::new(scalar); inspected with element_type().

pub enum ToastMode {
    Off,
    Auto,
    Aggressive,
}

pub enum ToastCompression {
    None,
    Zstd,
    ZstdDict,
}

pub struct ToastOptions {
    pub mode: ToastMode,
    pub tuple_target: u32,
    pub min_value_size: u32,
    pub compression: ToastCompression,
    pub active_dict_id: Option<u32>,
}

pub enum RelationKind {
    User,
    Toast { base_table: TableId },
}
```

#### Column Types

Three distinct column representations for three distinct lifecycle stages:

```rust
/// Parsed column — output of the parser. No IDs assigned yet.
/// Used in Statement::CreateTable and other AST nodes.
pub struct ParsedColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Catalog column — stored in the catalog with assigned IDs.
/// The authoritative schema definition for an existing table.
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Column metadata for result sets and plan output schemas.
/// Bridges internal ColumnDef (with IDs) to wire protocol RowDescription (with names).
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub table_id: Option<TableId>,
    pub column_id: Option<ColumnId>,
    pub pg_type: Option<PgType>,
}

/// View-output metadata before the catalog assigns dense ColumnDef IDs.
pub struct ViewColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub pg_type: Option<PgType>,
}
```

`ParsedColumnDef` → catalog assigns IDs → `ColumnDef`. `ColumnInfo` is derived from `ColumnDef` for result set descriptions. `ViewColumn` carries planner-derived view output metadata, including nullability, before the catalog persists it as dense `ColumnDef` values.

#### Error Types

```rust
/// Structured error type carrying enough information for the PostgreSQL
/// ErrorResponse message (severity, SQLSTATE code, message, detail, hint).
pub struct DbError {
    pub kind: ErrorKind,
    pub code: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

pub enum ErrorKind {
    /// Syntax error, unsupported SQL feature
    Parse,
    /// Unknown table, ambiguous column, type mismatch
    Plan,
    /// Division by zero, constraint violation, overflow
    Execute,
    /// Corrupted page, storage invariant violation
    Storage,
    /// Disk full, permission denied
    Io,
    /// Corrupt WAL record, incomplete write
    Wal,
    /// Malformed message, unexpected protocol state
    Protocol,
    /// Bug, unexpected internal state
    Internal,
}

/// PostgreSQL-compatible SQLSTATE codes (5-char strings).
/// SaguaroDB implements a small subset; the enum is extensible.
pub enum SqlState {
    SuccessfulCompletion,       // 00000
    SyntaxError,                // 42601
    UndefinedTable,             // 42P01
    InvalidSchemaName,          // 3F000
    UndefinedColumn,            // 42703
    UndefinedObject,            // 42704
    InvalidColumnReference,     // 42P10
    WrongObjectType,            // 42809
    DuplicateTable,             // 42P07
    DatatypeMismatch,           // 42804
    DivisionByZero,             // 22012
    InvalidParameterValue,      // 22023
    NumericValueOutOfRange,     // 22003
    StringDataRightTruncation,  // 22001
    NotNullViolation,           // 23502
    UniqueViolation,            // 23505
    CheckViolation,             // 23514
    DependentObjectsStillExist,  // 2BP01
    ObjectNotInPrerequisiteState, // 55000
    QueryCanceled,              // 57014
    FeatureNotSupported,        // 0A000
    InFailedSqlTransaction,     // 25P02
    ProgramLimitExceeded,       // 54000
    SerializationFailure,       // 40001
    IoError,                    // 58030
    InternalError,              // XX000
    // ... extensible
}

/// All Result types in SaguaroDB use DbError.
pub type Result<T> = std::result::Result<T, DbError>;
```

#### Statement Context

```rust
/// Passed to every storage operation. Carries the transaction id, the MVCC
/// snapshot used for visibility, the isolation level, the GC horizon, and the
/// server-installed runtime handles (row-lock conflict waiter, cancellation token,
/// live subxid set, SSI tracker, sequence runtime, session identity, system state,
/// and catalog introspection provider).
/// docs/specs/crates/common.md "Statement Context" is the authoritative
/// field-by-field contract.
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Arc<Snapshot>,
    pub isolation: IsolationLevel,
    pub conflict_waiter: Arc<dyn ConflictWaiter>,
    pub cancel: Arc<QueryCancel>,
    pub live_txns: Arc<[u64]>,
    pub gc_horizon: u64,
    pub ssi_tracker: Arc<dyn SsiTracker>,
    pub sequence_manager: Arc<dyn SequenceManager>,
    pub session_sequences: Arc<SessionSequenceState>,
    pub session_info: Arc<SessionInfo>,
    pub system_state: Arc<dyn SystemStateProvider>,
    pub catalog_introspection: Arc<dyn CatalogIntrospectionProvider>,
}
```

#### Flush Policy

```rust
/// Information about a dirty page, passed to FlushPolicy to decide whether it
/// can be flushed. The struct is extensible so new fields don't change the
/// trait signature.
pub struct PageFlushInfo {
    pub dirty_txn_id: u64,      // informational; the MVCC gate no longer reads it.
    pub page_lsn: Option<Lsn>,  // WAL-durability check; checkpoint/steal pass None (WAL flushed first).
}

/// Abstraction so the buffer pool can decide whether a dirty page is safe to
/// flush, without depending on the wal crate. WalFlushPolicy admits any
/// WAL-durable dirty page (with MVCC the committedness gate is dropped — an
/// uncommitted/aborted page may be flushed, hidden by the CLOG); checkpoint
/// flushes them via flush_dirty_pages. The steal path calls `ensure_durable`
/// (forces the WAL) before writing a possibly-uncommitted stolen page.
pub trait FlushPolicy: Send + Sync {
    fn can_flush(&self, info: &PageFlushInfo) -> bool;
    fn ensure_durable(&self) -> Result<()> { Ok(()) }
}
```

#### Concurrency Controller

```rust
/// Coarse checkpoint exclusion. DML, DDL, and WAL-writing maintenance use a
/// SHARED writer guard; `begin_checkpoint()` yields an EXCLUSIVE guard used by
/// checkpoint to drain them. Readers take no controller guard; table locks
/// separately coordinate logical relation access.
///
/// Guards are owned types (no lifetime parameter) that hold Arc references
/// internally and release the guard on Drop. This keeps the trait object-safe
/// (usable as Box<dyn ConcurrencyController>) and avoids GAT complexity.
/// The concrete controller uses an RwLock internally: all page/WAL writers share
/// it and checkpoint takes it exclusively. Readers take no controller guard.
pub trait ConcurrencyController: Send + Sync {
    fn begin_writer(&self) -> Result<WriteGuard>;
    fn begin_writer_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard>;
    fn begin_checkpoint(&self) -> Result<CheckpointGuard>;
    fn begin_checkpoint_cancelable(&self, cancel: &QueryCancel) -> Result<CheckpointGuard>;
    fn begin_shared(&self) -> Result<WriteGuard> { self.begin_writer() }
    fn begin_shared_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard>;
}

pub struct RwLockConcurrencyController { /* parking_lot::RwLock<()> */ }

impl RwLockConcurrencyController {
    pub fn new() -> Self;
}

/// Owned shared writer guard. Holds an Arc to the lock internally.
/// Concurrent writers hold it simultaneously; releases on Drop. Send safe.
pub struct WriteGuard { /* Arc<RwLock<...>> + guard state */ }

/// Owned exclusive guard for checkpoint. Drains in-flight writers and releases
/// on Drop. Send safe.
pub struct CheckpointGuard { /* Arc<RwLock<...>> + guard state */ }
```

**Design rationale — owned guards over GATs:** All major traits in this system are used as trait objects (`Box<dyn BufferPool>`, `Box<dyn ConcurrencyController>`, etc.). GATs (`type WriteGuard<'a> where Self: 'a`) would make these traits non-object-safe, forcing generics throughout the crate dependency graph. Owned guards with Arc internals add negligible overhead (one Arc clone per guarded statement) and keep the trait boundaries clean. This is the standard pattern in Rust database projects.

Most layers below the parser use `TableId`/`ColumnId`/`BindingId` instead of strings. The binder (phase 1 of the planner crate) resolves ordinary DML names to IDs and assigns physical slot positions via the catalog. Selected DDL plans intentionally carry names into execution when current catalog state must be consulted there, such as `CREATE TABLE IF NOT EXISTS`, `DROP TABLE IF EXISTS`, `DROP SEQUENCE IF EXISTS`, and `CREATE TABLE ... SERIAL` owned-sequence naming.

`FlushPolicy` lives in `common` so the buffer pool can decide whether a dirty page is flushable without depending on the `wal` crate. `WalFlushPolicy` admits any WAL-durable dirty page (with MVCC the committedness gate is dropped — uncommitted/aborted pages may be flushed and are hidden by the CLOG); checkpoint uses it via `flush_dirty_pages` to flush dirty pages in place to the heap, and eviction-flush-on-steal uses it (plus `ensure_durable`, which forces the WAL first) to steal dirty pages during eviction, removing the in-RAM working-set ceiling during normal operation.

## 3. Wire Protocol

The `protocol` crate owns message encoding/decoding and connection state machine logic, but does NOT own async task orchestration or raw IO. The server crate owns the Tokio tasks and drives the protocol layer's codec.

### Responsibility Split

| Concern | Owner |
|---|---|
| TCP accept, async task spawning, `spawn_blocking` | `server` |
| Message framing, encode/decode, state machine | `protocol` |
| Byte-level reads/writes | `server` (passes bytes to `protocol`) |

### Protocol Traits

The protocol layer is generic over IO — it works with byte buffers, not sockets:

```rust
/// Incoming message from a client (decoded by the protocol layer)
pub enum ClientMessage {
    Startup { user: String, database: Option<String>, application_name: Option<String> },
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: i32, secret_key: i32 },  // cancel an in-flight query
    Query(String),                                       // simple query
    // Extended query protocol:
    Parse { name: String, query: String, param_types: Vec<i32> },
    Bind { portal: String, statement: String, param_formats: Vec<i16>,
           params: Vec<Option<Vec<u8>>>, result_formats: Vec<i16> },
    Describe { kind: StatementKind, name: String },
    Execute { portal: String, max_rows: i32 },
    Close { kind: StatementKind, name: String },
    Sync,
    Flush,
    // COPY sub-protocol:
    CopyData(Vec<u8>),
    CopyDone,
    CopyFail(String),
    Terminate,
}

/// Outgoing message to a client (encoded by the protocol layer)
pub enum ServerMessage {
    SslAccepted,                // single 'S' byte
    SslRejected,                // single 'N' byte
    AuthenticationOk,
    BackendKeyData { process_id: i32, secret_key: i32 },  // identity for CancelRequest
    ParameterStatus { key: String, value: String },
    ReadyForQuery(u8),          // transaction-status byte: 'I' idle, 'T' in block, 'E' failed block
    RowDescription { columns: Vec<ColumnInfo>, formats: Vec<i16> },  // per-field 0=text,1=binary
    DataRow(Vec<Option<Vec<u8>>>),  // each column already encoded to its wire bytes
    CommandComplete(String),
    // Extended query protocol:
    ParseComplete,
    BindComplete,
    CloseComplete,
    PortalSuspended,
    ParameterDescription(Vec<i32>),
    NoData,
    // COPY sub-protocol:
    CopyInResponse { overall_format: i8, column_formats: Vec<i16> },
    CopyOutResponse { overall_format: i8, column_formats: Vec<i16> },
    CopyData(Vec<u8>),
    CopyDone,
    ErrorResponse { severity: String, code: String, message: String },
}

/// Stateful codec: decodes bytes into ClientMessages, encodes ServerMessages into bytes.
pub trait ProtocolCodec: Send {
    /// Feed incoming bytes, return decoded messages (may return 0 or more).
    fn decode(&mut self, buf: &[u8]) -> Result<Vec<ClientMessage>>;

    /// Encode a server message into bytes for transmission.
    fn encode(&self, msg: &ServerMessage) -> Vec<u8>;
}

/// Connection state machine — tracks where we are in the protocol lifecycle.
/// Handles non-query messages (startup, SSL/GSS negotiation, terminate) directly.
/// Query messages are handled by the server's streaming pipeline (see below).
pub trait ConnectionState: Send {
    /// Handle a non-query client message, return response messages.
    fn handle_message(&mut self, msg: ClientMessage) -> Result<Vec<ServerMessage>>;

    /// Has the client sent Terminate?
    fn is_terminated(&self) -> bool;
}
```

### Query Result Architecture

A `SELECT` **streams** its rows: the `spawn_blocking` producer owns the `PlanExecutor` and pushes row batches through a bounded channel to the async connection task, which writes them to the socket as they arrive (`docs/specs/streaming.md`). This bounds server memory (the whole result is never materialized) and applies TCP backpressure — a slow client blocks the producer on a full channel rather than letting it run ahead. DML, DML `RETURNING`, DDL, and `EXPLAIN` are still computed inside `spawn_blocking` and returned as complete results. `COPY` requests are bound in the query pipeline but return `BeginCopyIn`/`BeginCopyOut` outcomes; the connection task then drives the COPY sub-protocol, with COPY-out streamed through its own bounded channel.

```
Async connection task (Tokio)           Blocking thread (spawn_blocking)
─────────────────────────────          ────────────────────────────────
1. Decode Query msg
2. execute_simple_streamed(sql, session_ctx, tx) ─►
                                        3. Parse → Bind → Plan
                                        4. Build + open PlanExecutor
                               ◄─────   5. send Start { columns }
6. Send RowDescription
                               ◄─────   7. send Rows(batch), … (blocks
7'. Loop: recv batch,                      on a full channel)
    encode + write DataRows
                               ◄─────   8. return StreamOutcome::Streamed
                                            (executor closed; slot returned)
9. Send CommandComplete + ReadyForQuery
```

The producer holds the snapshot's GC-horizon advertisement and any transaction guard for the whole stream, exactly as the materializing path did, so MVCC visibility and transaction semantics are unchanged. Streaming does not affect SQL behavior; it is the executor's pull-based `PlanExecutor` boundary put to use. Extended-protocol `Execute.max_rows` uses the same open-query shape to suspend read-only SELECT portals with `PortalSuspended` and resume them on a later `Execute`.

This keeps the protocol layer testable without IO and keeps blocking work off Tokio threads.

### PostgreSQL Simple Query Flow

1. **SSLRequest handling:** Many clients (psql, libpq-based drivers) send an `SSLRequest` before the real startup. The server detects this (8-byte message with code `80877103`). When TLS is configured (`--tls-cert-file`/`--tls-key-file`), it replies with a single `S` byte and performs the TLS handshake, after which the client sends its `StartupMessage` over the encrypted stream. When TLS is not configured, it replies with a single `N` byte and the client continues in plaintext (or retries with a plain `StartupMessage`). TLS is server-side only; no client certificate is requested. A `GSSENCRequest` (GSSAPI transport encryption) is likewise declined with a single `N` byte, after which the client continues with an `SSLRequest` or `StartupMessage`.
2. **Startup:** Client sends `StartupMessage` (version 3.0, user, database). Server responds `AuthenticationOk` → `ParameterStatus` (server_version, etc.) → `BackendKeyData` → `ReadyForQuery`.
3. **Query cycle:** Client sends `Query` (SQL string). Server responds with:
   - `RowDescription` (column names and types) for SELECT
   - `DataRow` (one per result row) for SELECT
   - `CommandComplete` (e.g., `INSERT 0 1`, `SELECT 5`)
   - `ReadyForQuery`
4. **Query error handling:** If a query fails, server sends `ErrorResponse` then `ReadyForQuery`. The connection stays open.
5. **Protocol decode error handling:** If decoding client bytes fails, server sends `ErrorResponse` then `ReadyForQuery` and closes the connection because the codec buffer state may be unrecoverable.
6. **Termination:** Client sends `Terminate`. Server closes connection.

### PostgreSQL Extended Query Flow

The extended protocol supports parameterized statements, prepared statements,
portals, and binary parameter/result encoding:

1. **Parse:** Client sends `Parse` (statement name, SQL with `$n` placeholders,
   optional parameter type OIDs). The server prepares the statement, resolving
   each parameter's type from the declared OID or by inference from context,
   records referenced table/view schema versions for bound data plans, and replies
   `ParseComplete`.
2. **Bind:** Client sends `Bind` (portal name, statement name, parameter format
   codes, parameter values, result format codes). The server decodes the values
   (text or binary) into a portal and replies `BindComplete`.
3. **Describe:** `Describe` of a statement returns `ParameterDescription` then
   `RowDescription`/`NoData`; of a portal returns `RowDescription`/`NoData` in
   the portal's result formats.
4. **Execute:** The server runs the portal and streams `DataRow`s in the
   requested result formats. No `RowDescription` (that comes from Describe) and
   no `ReadyForQuery` (that comes from Sync). SELECT with `max_rows == 0` drains
   to `CommandComplete`; read-only SELECT with `max_rows > 0` sends at most that
   many rows and returns `PortalSuspended` when more rows remain, so a later
   `Execute` can resume the same portal. Non-SELECT statements ignore `max_rows`.
5. **Sync:** The server sends `ReadyForQuery`. An error earlier in the sequence
   sends `ErrorResponse` and then skips messages until `Sync`.
6. **Close/Flush:** `Close` drops a statement or portal (`CloseComplete`);
   `Flush` flushes pending output. Named and unnamed statements/portals are
   supported.

### Protocol Scope — What We Skip

- Mutual TLS / client-certificate authentication (optional server-side TLS is supported; see SSLRequest handling above)
- GSSAPI transport encryption (GSSENCRequest declined with `N`)
- Authentication beyond accepting any connection
- `NOTIFY/LISTEN`

`COPY ... FROM STDIN`/`TO STDOUT` (text/CSV) **is** supported via the simple
query protocol and its COPY sub-protocol; see `docs/specs/copy.md`. COPY through
the extended query protocol, server-side file COPY, `COPY (query) TO`, and
`FORMAT binary` are rejected.

### PostgreSQL Wire Encoding Details

All integer fields are big-endian. All server messages except the SSL negotiation reply are one-byte tag plus a four-byte length that includes the length field but not the tag. The SSL negotiation reply is exactly a single byte: `S` for acceptance, `N` for rejection.

- Client `SSLRequest`: startup-style packet with length `8` and code `80877103`.
- Client `GSSENCRequest`: startup-style packet with length `8` and code `80877104`; declined with a single `N` byte.
- Client `Startup`: startup-style packet with protocol `196608` (3.0), nul-terminated key/value parameters, and final `\0`; the server reads `user`, optional `database`, and optional `application_name`.
- Client `Query`: tag `Q`, length, nul-terminated SQL string.
- Client `Terminate`: tag `X`, length `4`.
- Server `AuthenticationOk`: tag `R`, length `8`, auth code `0`.
- Server `ParameterStatus`: tag `S`, `key\0value\0`; startup emits `server_version=16.0`, `server_encoding=UTF8`, `client_encoding=UTF8`, `DateStyle=ISO`, `integer_datetimes=on`, `standard_conforming_strings=on`, `TimeZone=UTC`, and `application_name` echoed from the client's startup parameters (empty when not supplied).
- Server `ReadyForQuery`: tag `Z`, length `5`, transaction-status byte sourced from the session's transaction state (`I` idle, `T` in a transaction block, `E` failed transaction block). Standalone statements run in autocommit and report `I`; inside a `BEGIN`/`COMMIT` block the byte is `T`, or `E` once a statement in the block has failed.
- Server `RowDescription`: tag `T`, field count, then for each column `name\0`, `table_oid = 0`, `attr_num = 0`, mapped type OID, type size, `type_modifier = -1`, and text `format_code = 0`.
- Server `DataRow`: tag `D`, column count, then `int32 byte_length` plus UTF-8 text bytes, or `-1` for `NULL`.
- Server `CommandComplete`: tag `C`, nul-terminated tags `SELECT n`, `INSERT 0 n`, `UPDATE n`, `DELETE n`, `CREATE TABLE`, `DROP TABLE`, `CREATE VIEW`, `DROP VIEW`, `CREATE INDEX`, `DROP INDEX`, `CREATE SEQUENCE`, `DROP SEQUENCE`, `ALTER TABLE`, `EXPLAIN`, `DECLARE CURSOR`, `FETCH n`, `CLOSE CURSOR`, `SET`, `SHOW`, `RESET`, `DISCARD ALL`, `VACUUM`, `ANALYZE`, `TRUNCATE TABLE`, or `COPY n`.
- Server `ErrorResponse`: tag `E`, fields `S` severity, `C` SQLSTATE, `M` message, then final `\0`.

A `RowDescription` field's type OID, size, and modifier (`atttypmod`) come from the column's declared PostgreSQL wire type (`common::PgType`; see `docs/specs/crates/protocol.md`): the distinct integer widths `int2` (`21`), `int4` (`23`), `int8` (`20`); catalog `oid` (`26`); the virtual-catalog vector/array identities `int2vector` (`22`), `oidvector` (`30`), `int2[]` (`1005`), and `oid[]` (`1028`); the character kinds `text` (`25`), `varchar` (`1043`), `bpchar` (`1042`) with a `n + 4` length modifier; `BOOLEAN` (`16`), `DATE` (`1082`), `TIMESTAMP` (`1114`), `TIME` (`1083`), `TIMESTAMP WITH TIME ZONE` (`1184`), `INTERVAL` (`1186`), `BYTEA` (`17`), `UUID` (`2950`), `DOUBLE PRECISION` (`701`), `REAL` (`700`), and `NUMERIC` (`1700`, modifier packing precision/scale). A column with no declared wire type (e.g. a computed expression) falls back to the collapsed default (`Integer` => `int8`, `Text` => `text`), except selected extended-protocol parameters preserve their declared `PgType` and registry-backed scalar functions may preserve an unambiguous `pg_proc` result wire type such as `oid`. The simple query path always sends text: integers are decimal i64 strings, text is raw UTF-8, booleans are `t`/`f`, dates are `YYYY-MM-DD`, timestamps are `YYYY-MM-DD HH:MM:SS[.ffffff]`, times are `HH:MM:SS[.ffffff]`, timestamptz is `YYYY-MM-DD HH:MM:SS[.ffffff]+00` (always UTC), interval uses PostgreSQL `postgres`-style text (e.g. `1 year 2 mons 3 days 04:05:06`), bytea is hex `\x...`, uuid is the canonical `8-4-4-4-12` form, doubles and reals use a round-trippable form (fixed-point for moderate magnitudes, `e±NN` scientific otherwise; `Infinity`/`-Infinity`/`NaN` for non-finite), numerics use their decimal text preserving scale, and null fields use length `-1`. (A binary integer result uses the column's declared width — an `int2`/`int4` value is 2 or 4 big-endian bytes, not the 8-byte `int8` form; a binary `oid` result is 4 unsigned big-endian bytes; a binary integer parameter may be bound as 2, 4, or 8 bytes and is sign-extended, while a binary `oid` parameter is 4 bytes and decoded unsigned and a text `oid` parameter is range-checked to `0..=u32::MAX`. Binary `DATE` is an i32 day count from 2000-01-01; binary `TIMESTAMP` is an i64 microsecond count from 2000-01-01; binary `TIME` is an i64 microsecond count since midnight; binary `TIMESTAMP WITH TIME ZONE` is an i64 microsecond count from 2000-01-01 UTC; binary `INTERVAL` is i64 micros + i32 days + i32 months; binary `BYTEA` is the raw bytes; binary `UUID` is the 16 raw bytes; binary `DOUBLE PRECISION` is the 8-byte big-endian IEEE 754 value; binary `REAL` is the 4-byte big-endian IEEE 754 value; binary `NUMERIC` is PostgreSQL's base-10000 `NumericVar` format.) The extended query protocol additionally supports binary parameters and results — `RowDescription` carries a per-field format code (`0` = text, `1` = binary) and `DataRow` carries the already-encoded wire bytes for that format; text-backed virtual-catalog vector/array columns are reported and sent as text even if binary was requested.

### Server Query Service Boundary

The protocol crate owns only message codecs and connection state for non-query messages. The server connection loop delegates SQL strings to the server-owned query service:

```rust
pub struct QueryService { /* parser, catalog, planner, executor, guards */ }

impl QueryService {
    /// Execute a SQL statement. For SELECT queries, this builds the executor
    /// pipeline and materializes rows on the calling blocking thread.
    pub fn execute_sql(&self, sql: &str) -> Result<ExecutionResult>;
}

pub enum ExecutionResult {
    /// Materialized row result for SELECT.
    Query {
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
    },
    /// DML or DDL result (INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE,
    /// CREATE INDEX, DROP INDEX, CREATE SEQUENCE, DROP SEQUENCE, ALTER TABLE)
    Modified { command: String, count: u64 },
    /// DML statement with a RETURNING clause: it both modifies rows and produces a
    /// result set. `count` drives the DML command tag (e.g. `INSERT 0 n`);
    /// `columns`/`rows` are the RETURNING projection sent as RowDescription + DataRows.
    ModifiedReturning { command: String, count: u64, columns: Vec<ColumnInfo>, rows: Vec<Row> },
    /// EXPLAIN result — the formatted plan without executing it
    Explanation { text: String },
}
```

For `Explanation`, the server writes one text row with column name `QUERY PLAN`, then sends `CommandComplete("EXPLAIN")`.

For `Query`, the `QueryService` implementation:
1. Parses, binds, and plans the query
2. Pulls rows from `PlanExecutor::next()` until EOF
3. Returns rows and column metadata as `ExecutionResult::Query`

The protocol layer never touches storage directly. The `server` crate owns `QueryService`, which wires together the binder, planner, executor, and storage.

## 4. SQL Parsing & AST

First-class rectangular arrays are supported in columns, casts, constructors,
comparisons, one-based subscripts, `op ANY(array)`, PostgreSQL text/binary
parameters and results, and COPY fields. `array_agg` and `string_agg` are
aggregates. `unnest(array)` and integer `generate_series(start, stop [, step])`
are one-column table functions and are implicitly lateral when their arguments
reference preceding FROM items.

The `parser` crate wraps `sqlparser-rs` (PostgreSQL dialect) and translates its AST into our own internal representation. This keeps the external dependency contained and gives us a narrow, explicit definition of exactly what SaguaroDB supports. Unsupported syntax is rejected here, not deep in the executor.

### Internal AST Types

The AST uses strings for identifiers — name resolution to IDs happens in the planner.

```rust
pub enum Statement {
    CreateTable {
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,  // UNIQUE constraints; each becomes a unique index
        compression: Option<CompressionSetting>,
        toast: ToastOptionPatch,
        checks: Vec<String>,
    },
    DropTable { names: Vec<String>, if_exists: bool },
    AlterTableAddColumn {
        table: String,
        if_not_exists: bool,
        column: ParsedColumnDef,
    },
    AlterTableDropColumn {
        table: String,
        if_exists: bool,
        column: String,
    },
    AlterTableRenameColumn { table: String, old_name: String, new_name: String },
    AlterTableRenameTable { table: String, new_name: String },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { name: String },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },
    CreateView {
        name: String,
        or_replace: bool,
        columns: Vec<String>,
        query: Query,
        definition: String,
    },
    DropView { name: String, if_exists: bool },
    Insert {
        table: String,
        columns: Vec<String>,
        source: InsertSource,
        on_conflict: Option<OnConflict>,     // INSERT ... ON CONFLICT ... (upsert)
        returning: Option<Vec<SelectItem>>,  // INSERT ... RETURNING <items>
    },
    Query(Query),
    Update {
        table: String,
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
        returning: Option<Vec<SelectItem>>,  // UPDATE ... RETURNING <items>
    },
    Delete {
        table: String,
        filter: Option<Expr>,
        returning: Option<Vec<SelectItem>>,  // DELETE ... RETURNING <items>
    },
    Explain(Box<Statement>),
    // Transaction control (docs/specs/mvcc.md §10 G) and savepoints
    // (docs/specs/savepoints.md); executed by the server, not bound/planned.
    Begin { isolation: Option<IsolationLevel> },  // BEGIN / START TRANSACTION [ISOLATION LEVEL ...]
    Commit,                                       // COMMIT / END
    Rollback,                                     // ROLLBACK (without a savepoint)
    Savepoint { name: String },                   // SAVEPOINT <name>
    ReleaseSavepoint { name: String },            // RELEASE [SAVEPOINT] <name>
    RollbackToSavepoint { name: String },         // ROLLBACK ... TO [SAVEPOINT] <name>
    SetTransaction { isolation: Option<IsolationLevel> },            // SET TRANSACTION ... (txn-scoped)
    SetSessionCharacteristics { isolation: Option<IsolationLevel> }, // session default isolation
    SetVariable { scope: SetScope, name: String, value: String },    // SET/SET LOCAL <guc>
    ResetVariable { name: Option<String> },                          // RESET <guc> / RESET ALL
    ShowVariable { name: Option<String> },                           // SHOW <guc> / SHOW ALL
    DiscardAll,                                                      // DISCARD ALL
    Vacuum { table: Option<String> },             // VACUUM [table] — maintenance, not bound/planned
    Truncate { tables: Vec<String> },             // TRUNCATE [TABLE] <name> [, ...] — maintenance, not bound/planned
    AlterTableSetCompression {                    // ALTER TABLE <table> SET (compression = ...)
        table: String,
        compression: CompressionSetting,
    },
    AlterTableSetOptions {                        // ALTER TABLE <table> SET (toast..., ...)
        table: String,
        options: TableOptionPatch,
    },
    AlterTableAddPrimaryKey {
        table: String,
        columns: Vec<String>,
        constraint_name: Option<String>,
    },
    AlterTableDropPrimaryKey {
        table: String,
        constraint_name: Option<String>,
    },
    Copy {                                        // COPY <table> [(cols)] FROM STDIN | TO STDOUT
        table: String,                            // (docs/specs/copy.md)
        columns: Vec<String>,
        direction: CopyDirection,
        options: CopyOptions,
    },
}

pub enum InsertSource {
    Values(Vec<Vec<Expr>>),  // INSERT INTO t VALUES (...)
    Query(Box<Query>),       // INSERT INTO t SELECT ...
}

/// ON CONFLICT [target] DO NOTHING | DO UPDATE SET ... [WHERE ...]. The arbiter
/// is the primary key (validated by the binder, and revalidated for prepared
/// statements when an arbiter is bound).
pub struct OnConflict {
    pub target: Option<ConflictTarget>,
    pub action: ConflictAction,
}

pub enum ConflictTarget {
    Columns(Vec<String>),  // the binder requires exactly the primary-key columns
}

pub enum ConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<Assignment>,  // may reference the `excluded` pseudo-table
        filter: Option<Expr>,
    },
}

pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

// A query expression: an optional WITH clause, a body, and the query-level
// ORDER BY/LIMIT/OFFSET (which sit outside the body, so a set operation orders
// and limits the combined result and the CTEs are visible to the whole body).
pub struct Query {
    pub with: Vec<Cte>,  // WITH CTEs (non-recursive), inlined as named derived tables
    pub body: QueryBody,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// A common table expression: `name [(col, ...)] AS (query)`. `column_aliases`
/// optionally renames the CTE's output columns left to right.
pub struct Cte {
    pub name: String,
    pub column_aliases: Vec<String>,
    pub query: Box<Query>,
}

pub enum QueryBody {
    Select(Select),
    Values(Vec<Vec<Expr>>),  // VALUES (1,'a'), (2,'b')
    SetOp { op: SetOp, all: bool, left: Box<Query>, right: Box<Query> },  // a UNION b
}

pub enum SetOp {
    Union,
    Intersect,
    Except,
}

pub struct Select {
    pub distinct: Option<Distinct>,
    pub columns: Vec<SelectItem>,
    pub from: Vec<FromItem>,  // empty for a FROM-less SELECT (`SELECT 1`)
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

pub enum Distinct {
    All,             // SELECT DISTINCT
    On(Vec<Expr>),   // SELECT DISTINCT ON (expr, ...)
}

pub enum SelectItem {
    Wildcard,                                          // *
    QualifiedWildcard(String),                         // table.*
    Expression { expr: Expr, alias: Option<String> },  // expr AS alias
}

pub enum FromItem {
    Table { schema: Option<String>, name: String, alias: Option<String> },
    // A derived table: (SELECT ...) AS alias [(col, ...)]. The alias is required;
    // column_aliases optionally renames the subquery's output columns.
    Derived { subquery: Box<Query>, alias: String, column_aliases: Vec<String> },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        condition: Option<Expr>,
    },
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

pub enum Expr {
    Literal(Value),
    Placeholder(u32),  // extended-protocol parameter `$n` (1-based)
    ColumnRef { table: Option<String>, column: String },
    Subquery(Box<Query>),  // scalar subquery (SELECT ...) as a value
    InSubquery { expr: Box<Expr>, subquery: Box<Query>, negated: bool },  // x [NOT] IN (SELECT ...)
    Exists { subquery: Box<Query>, negated: bool },  // [NOT] EXISTS (SELECT ...)
    BinaryOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    Function { name: String, args: Vec<FunctionArg>, distinct: bool },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,  // ILIKE when true; plain LIKE when false
        escape: Option<char>,    // default Some('\\'); ESCAPE '' disables (None)
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
        pg_type: PgType,  // declared wire type of the cast target (OID/typmod reporting)
    },
}

pub enum FunctionArg {
    Expr(Expr),
    Wildcard,
}

pub enum BinOp {
    // Arithmetic
    Add, Sub, Mul, Div, Mod,
    // Comparison
    Eq, Neq, Lt, LtEq, Gt, GtEq,
    // Logical
    And, Or,
    // String
    Concat,
    // NULL-safe comparison (never returns NULL)
    IsDistinctFrom, IsNotDistinctFrom,
}

pub enum UnaryOp {
    Neg,   // -x
    Not,   // NOT x
}
```

`FROM` relation names may be unquoted one- or two-part names. The parser stores a
two-part name's schema on `FromItem::Table`; binder owns resolution.

`FromItem::Join.condition` is `None` only for `JoinType::Cross`. Inner, left, right, and full joins require an `ON` predicate. The parser rejects `USING` and `NATURAL` joins, and rejects `ON`/`USING` with `CROSS JOIN`.

Function call parsing preserves aggregate syntax: `COUNT(*)` is `Function { name: "count", args: vec![FunctionArg::Wildcard], distinct: false }`; aggregate `DISTINCT` sets `distinct = true` so the binder can carry it through (e.g. `COUNT(DISTINCT x)`). Ordinary function names may be unqualified or qualified with `pg_catalog`; `pg_catalog.<function>(...)` normalizes to the same lowercase function name as `<function>(...)`, while other qualified function schemas are rejected. `Select.distinct` records the optional `SELECT DISTINCT` / `DISTINCT ON (...)` modifier.

### Public API

```rust
pub fn parse(sql: &str) -> Result<Statement>
```

## 5. Binder & Query Planner

The `planner` crate contains three distinct phases, each with its own public API:

```rust
/// Phase 1: Bind — resolve names, validate types, assign slots
pub fn bind(statement: &Statement, catalog: &dyn CatalogManager) -> Result<BoundStatement>;

/// Phase 2: Logical plan — translate bound statement into relational algebra
pub fn logical_plan(bound: &BoundStatement) -> Result<LogicalPlan>;

/// Phase 3: Physical plan — choose access methods and algorithms
pub fn physical_plan(logical: &LogicalPlan, catalog: &dyn CatalogManager) -> Result<PhysicalPlan>;
```

All three phases are separate modules within the `planner` crate. All three are implemented — the physical planner is trivial (rule-based), but the boundary is real. A future cost-based optimizer replaces only `physical_plan` without touching binding or logical planning.

### Phase 1: Binder

The binder performs semantic analysis and name resolution. Its output is a `BoundStatement` — a validated, ID-resolved, slot-assigned representation of the query. No downstream phase does table, column, or index name lookups for ordinary DML. Schema-evolution `ALTER TABLE` binds the target relation to `TableId` and prepared execution rejects the cached plan if that table is dropped or its `schema_version` changes before execution. Selected DDL cases intentionally defer name work to execution: `DROP TABLE IF EXISTS` and `DROP SEQUENCE` carry the normalized object name plus `IF EXISTS` through planning so prepared statements resolve existence at execution time, `CREATE TABLE IF NOT EXISTS` carries the table name so the duplicate-table no-op decision uses the current catalog, and `CREATE TABLE` with `SERIAL` chooses owned sequence names at execution time so prepared DDL observes current sequence-name collisions. The binder is the primary SQL type checker; the executor may still defensively validate runtime DML values before storage writes.

The binder:
- Resolves table names to `TableId` via the catalog
- Assigns a unique `BindingId` to each table occurrence in FROM (critical for self-joins and aliases)
- Resolves column references to `BoundExpr::InputRef` with physical slot positions
- Validates types (e.g., `WHERE` clause is boolean, arithmetic operands are numeric)
- Rejects unsupported features (e.g., unknown functions)
- Expands `SELECT *` into explicit column lists

```rust
pub struct DropTableTarget {
    pub name: String,
    pub table: Option<TableId>,
}

/// Fully resolved statement. Names are resolved except for documented
/// execution-time DDL cases; all types are checked and all column references are
/// assigned physical slot positions.
pub enum BoundStatement {
    CreateTable {
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        compression: CompressionSetting,
        toast: ToastOptions,
        checks: Vec<String>,
    },
    DropTable { targets: Vec<DropTableTarget>, if_exists: bool },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: BoundInsertSource,
        on_conflict: Option<BoundOnConflict>,
        returning: Option<BoundReturning>,
    },
    Query(BoundQuery),
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: BoundSelect,
        returning: Option<BoundReturning>,
    },
    Delete {
        table: TableId,
        source: BoundSelect,
        returning: Option<BoundReturning>,
    },
    Explain(Box<BoundStatement>),
    // COPY <table> [(cols)] FROM STDIN | TO STDOUT. Resolved table + column ids
    // (COPY order; defaulted to all columns in catalog order). Not lowered to a
    // LogicalPlan — the server drives COPY directly (docs/specs/copy.md).
    Copy {
        table: TableId,
        columns: Vec<ColumnId>,
        direction: CopyDirection,
        options: CopyOptions,
    },
}

/// A bound RETURNING clause: the projection expressions evaluated over each
/// affected full row (the inserted/updated NEW row, or the deleted OLD row) and
/// the result-set column metadata that becomes the statement's RowDescription.
pub struct BoundReturning {
    pub exprs: Vec<BoundExpr>,
    pub output_schema: Vec<ColumnInfo>,
}

/// A bound INSERT ... ON CONFLICT action (arbiter = the primary key). Bound
/// arbiters are carried as column ids so prepared statements can revalidate them
/// after primary-key DDL. DoUpdate's assignment values and filter are bound over
/// `existing ++ excluded` — the existing target row in slots 0..n and the proposed
/// row in slots n..2n.
pub enum BoundOnConflict {
    DoNothing {
        target: Option<Vec<ColumnId>>,
    },
    DoUpdate {
        target: Vec<ColumnId>,
        assignments: Vec<(ColumnId, BoundExpr)>,
        filter: Option<BoundExpr>,
    },
}

pub enum BoundInsertSource {
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
    Query(Box<BoundQuery>),
}

/// A bound query expression: a body plus the query-level ORDER BY/LIMIT/OFFSET
/// (mirrors the AST Query; the modifiers live here, not on BoundSelect).
pub struct BoundQuery {
    pub body: BoundQueryBody,
    pub order_by: Vec<BoundOrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

pub enum BoundQueryBody {
    Select(Box<BoundSelect>),
    Values(BoundValues),  // literal row set; columns named column1, column2, ...
    SetOp(BoundSetOp),    // UNION/INTERSECT/EXCEPT; arms reconciled to identical types
}

pub enum BoundDistinct {
    All,                  // SELECT DISTINCT
    On(Vec<BoundExpr>),   // SELECT DISTINCT ON (exprs)
}

/// A fully bound SELECT block — all names resolved, types checked, slots assigned.
pub struct BoundSelect {
    pub distinct: Option<BoundDistinct>,
    pub columns: Vec<BoundSelectItem>,
    pub from: Option<BoundFrom>,  // None for a FROM-less SELECT (`SELECT 1`)
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub having: Option<BoundExpr>,
    pub output_schema: Vec<ColumnInfo>,
}

pub struct BoundSelectItem {
    pub expr: BoundExpr,
    pub alias: String,  // resolved name (original alias or column name)
    pub wildcard_source: Option<TableId>, // physical table whose `*` produced this item
}

pub enum BoundFrom {
    Table {
        table: TableId,
        binding: BindingId,
        name: String,
        alias: Option<String>,
        schema: Vec<ColumnDef>,
    },
    System {
        view: SystemView,
        binding: BindingId,
        alias: Option<String>,
        schema: Vec<ColumnDef>,
    },
    // A derived table (SELECT ...) AS alias [(cols)], bound in its own scope; its
    // columns are projected into the outer scope at `binding`'s slots.
    Derived {
        query: Box<BoundQuery>,
        binding: BindingId,
        alias: String,
        schema: Vec<ColumnDef>,
    },
    View {
        view: TableId,
        schema_version: u64,
        query: Box<BoundQuery>,
        binding: BindingId,
        alias: String,
        schema: Vec<ColumnDef>,
    },
    Join {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        join_type: JoinType,
        condition: Option<BoundExpr>,
    },
}
```

`BoundFrom::Join.condition` is `None` only for `JoinType::Cross`; all other join types have a boolean `Some(condition)`. The executor treats `None` as `TRUE` only for cross joins.

`BoundFrom::View` is a user view from the catalog. The binder parses and binds
the stored definition in the view's own scope (caller CTEs do not affect it),
checks that its output width still matches the cataloged view columns, registers
the view as the visible input binding, and lowers the stored query like a derived
table. Prepared plans track the view's `schema_version` plus underlying table
versions so view replacement or base-table shape changes force a reprepare.

`BoundFrom::System` is a read-only virtual system view from `pg_catalog` or
`information_schema`. Bare FROM names prefer CTEs and user tables before falling
back to `pg_catalog`; qualified `information_schema` views must name that schema.
System-view columns bind like ordinary input slots but carry no underlying table
id. Unknown schemas are `InvalidSchemaName`, and system catalog names are not
valid DML or COPY targets.

### Bound Expressions

The binder resolves all string-based column references and assigns each one a physical slot position. The executor evaluates expressions by indexing directly into the row's values array — no name or ID lookups at runtime.

**Binding:** Each occurrence of a table in the FROM clause gets a unique `BindingId`. In `FROM users a JOIN users b`, `a` and `b` are different bindings of the same `TableId`. The binder tracks the mapping from `(BindingId, ColumnId)` → slot position. For output nullability, bindings on the null-supplying side of an outer join are marked nullable before binding projection/filter/order expressions (`RIGHT` side of `LEFT JOIN`, `LEFT` side of `RIGHT JOIN`, both sides of `FULL JOIN`), and that nullability flows into derived tables, views, and `INSERT ... SELECT` checks.

**Slot:** The `slot` field is the zero-based index into the combined tuple that the operator receives. For a join of `users a (id, name)` and `users b (id, name)`:
- `a.id` → slot 0
- `a.name` → slot 1
- `b.id` → slot 2
- `b.name` → slot 3

The executor just does `row.values[slot]`.

```rust
/// Expression with all column references resolved to physical slot positions.
/// Used in BoundStatement and all plan nodes. The executor evaluates by
/// slot index — O(1) column access.
pub enum BoundExpr {
    Literal {
        value: Value,
        data_type: DataType,
        nullable: bool,
    },
    InputRef {
        input: BindingId,         // which relation instance (for plan debugging / EXPLAIN)
        column: ColumnId,         // which column (for plan debugging / EXPLAIN)
        slot: usize,              // physical position in the row — what the executor uses
        data_type: DataType,      // resolved type — avoids re-lookup during evaluation
        nullable: bool,           // resolved nullability
    },
    BinaryOp {
        left: Box<BoundExpr>,
        op: BinOp,
        right: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    Function {
        name: String,
        args: Vec<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    AggregateCall {
        func: AggregateFunc,
        arg: Option<Box<BoundExpr>>,
        distinct: bool,
        data_type: DataType,
        nullable: bool,
    },
    LocalRef {
        slot: usize,
        data_type: DataType,
        nullable: bool,
    },
    IsNull {
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    IsNotNull {
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    InList {
        expr: Box<BoundExpr>,
        list: Vec<BoundExpr>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Between {
        expr: Box<BoundExpr>,
        low: Box<BoundExpr>,
        high: Box<BoundExpr>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Like {
        expr: Box<BoundExpr>,
        pattern: Box<BoundExpr>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Case {
        operand: Option<Box<BoundExpr>>,
        when_clauses: Vec<(BoundExpr, BoundExpr)>,
        else_clause: Option<Box<BoundExpr>>,
        data_type: DataType,
        nullable: bool,
    },
    Cast {
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
}
```

Every `BoundExpr` variant carries its resolved output type and nullability. Binder fills these fields before logical planning, including typed `Value::Null` literals from context; if a `NULL` literal has no valid typing context, binder rejects it with `SqlState::DatatypeMismatch`. For `NULL IN (...)`, binder may infer the left-side `NULL` type from the first typed list expression. The detailed metadata rules live in `docs/specs/crates/planner.md` and are authoritative for implementation.

### Phase 2: Logical Planner

Translates a `BoundStatement` into a `LogicalPlan` — relational algebra describing *what* to compute. No access method decisions.

```rust
pub enum LogicalPlan {
    // DDL — passes through to physical plan unchanged
    CreateTable {
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        compression: CompressionSetting,
        toast: ToastOptions,
        checks: Vec<String>,
    },
    DropTable { targets: Vec<DropTableTarget>, if_exists: bool },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },

    // DML
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: Box<LogicalPlan>,
        on_conflict: Option<BoundOnConflict>,
        returning: Option<BoundReturning>,
    },
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: Box<LogicalPlan>,
        returning: Option<BoundReturning>,
    },
    Delete {
        table: TableId,
        source: Box<LogicalPlan>,
        returning: Option<BoundReturning>,
    },

    // Query operators
    Scan { table: TableId, filter: Option<BoundExpr> },
    SystemScan { view: SystemView, filter: Option<BoundExpr> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, condition: Option<BoundExpr>, join_type: JoinType },
    Filter { source: Box<LogicalPlan>, predicate: BoundExpr },
    Projection { source: Box<LogicalPlan>, expressions: Vec<BoundExpr>, output_schema: Vec<ColumnInfo> },
    Sort { source: Box<LogicalPlan>, order_by: Vec<BoundOrderByItem> },
    Distinct { source: Box<LogicalPlan>, on_keys: Vec<BoundExpr> },
    Limit { source: Box<LogicalPlan>, count: u64, offset: Option<u64> },
    Aggregate {
        source: Box<LogicalPlan>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
    SetOp { op: SetOp, all: bool, left: Box<LogicalPlan>, right: Box<LogicalPlan> },
}

pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub arg: Option<BoundExpr>,  // None for COUNT(*)
    pub distinct: bool,
    pub data_type: DataType,
    pub nullable: bool,
}

pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    StddevSamp, // STDDEV / STDDEV_SAMP (divisor n - 1)
    StddevPop,  // STDDEV_POP (divisor n)
    VarSamp,    // VARIANCE / VAR_SAMP (divisor n - 1)
    VarPop,     // VAR_POP (divisor n)
    BoolAnd,    // BOOL_AND — true when every non-NULL input is true
    BoolOr,     // BOOL_OR — true when any non-NULL input is true
}

pub struct BoundOrderByItem {
    pub expr: BoundExpr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}
```

Aggregate calls use a two-stage representation. Binder converts `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, the statistical aggregates `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP` and `VARIANCE`/`VAR_SAMP`/`VAR_POP`, and `BOOL_AND`/`BOOL_OR` into `BoundExpr::AggregateCall`; scalar functions remain `BoundExpr::Function`. Logical planning extracts unique aggregate calls into `AggregateExpr` values and rewrites expressions above the `Aggregate` node to `BoundExpr::LocalRef`. The `Aggregate` output row layout is group-by values first, then aggregate values, so aggregate slot `i` is read as `LocalRef { slot: group_by.len() + i, ... }`. `AggregateCall` must not reach executor scalar evaluation.

Aggregate `DISTINCT` (e.g. `COUNT(DISTINCT x)`) is supported: the binder carries the flag into `AggregateExpr.distinct`, and the executor de-duplicates the argument values before aggregating. `DISTINCT` combined with a wildcard argument (`COUNT(DISTINCT *)`) is rejected with `ErrorKind::Plan` / `SqlState::SyntaxError`. Aggregate return types are fixed: `COUNT` returns non-null `INTEGER`; `SUM` and `AVG` accept either numeric type and return it (`AVG(integer)` uses integer division truncated toward zero; `AVG(double precision)` is true division), rejecting non-numeric arguments with `SqlState::DatatypeMismatch`; `MIN` and `MAX` return the argument type and are nullable; `STDDEV`/`VARIANCE` (and their `_SAMP`/`_POP` forms) take a numeric argument and return `DOUBLE PRECISION`; `BOOL_AND`/`BOOL_OR` take a boolean argument and return `BOOLEAN`. Empty aggregate inputs return `0` for `COUNT` and `NULL` for the rest (sample variance/stddev also return `NULL` for a single value).

`SELECT DISTINCT` sets `BoundSelect.distinct`, and logical planning inserts a `Distinct` node between any `Sort` and the `Projection`, so whole output rows (plain `DISTINCT`) or the first row per key (`DISTINCT ON`) survive after ordering. For plain `SELECT DISTINCT`, every `ORDER BY` expression must also appear in the select list. For `SELECT DISTINCT ON (keys)`, the `Distinct` node's `on_keys` are the bound key expressions, the binder rejects aggregates in the keys, and each leading `ORDER BY` expression (up to the number of keys) must be one of the keys (keys absent from `ORDER BY` are allowed). Both violations are rejected with `SqlState::InvalidColumnReference` (`42P10`).

### Phase 3: Physical Planner

Translates a `LogicalPlan` into a `PhysicalPlan` — chooses access methods and join algorithms. The physical planner is trivial (rule-based), but the boundary is real from day one.

```rust
pub enum PhysicalPlan {
    // DDL
    CreateTable {
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        compression: CompressionSetting,
        toast: ToastOptions,
        checks: Vec<String>,
    },
    DropTable { targets: Vec<DropTableTarget>, if_exists: bool },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },

    // DML
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: Box<PhysicalPlan>,
        on_conflict: Option<BoundOnConflict>,
        returning: Option<BoundReturning>,
    },
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: Box<PhysicalPlan>,
        returning: Option<BoundReturning>,
    },
    Delete {
        table: TableId,
        source: Box<PhysicalPlan>,
        returning: Option<BoundReturning>,
    },

    // Access methods
    SeqScan { table: TableId, table_name: String, filter: Option<BoundExpr> },
    SystemScan { view: SystemView, output_schema: Vec<ColumnInfo>, filter: Option<BoundExpr> },
    IndexScan { table: TableId, table_name: String, index: IndexId, range: KeyRange, full_filter: Option<BoundExpr>, filter: Option<BoundExpr> },

    // Join algorithms
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Option<BoundExpr>,
        join_type: JoinType,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
    },
    MergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        residual: Option<BoundExpr>,
        join_type: JoinType,
    },

    // Other operators
    Filter { source: Box<PhysicalPlan>, predicate: BoundExpr },
    Projection { source: Box<PhysicalPlan>, expressions: Vec<BoundExpr>, output_schema: Vec<ColumnInfo> },
    Sort { source: Box<PhysicalPlan>, order_by: Vec<BoundOrderByItem> },
    Distinct { source: Box<PhysicalPlan>, on_keys: Vec<BoundExpr> },
    Limit { source: Box<PhysicalPlan>, count: u64, offset: Option<u64> },
    Aggregate {
        source: Box<PhysicalPlan>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
    SetOp { op: SetOp, all: bool, left: Box<PhysicalPlan>, right: Box<PhysicalPlan> },
}

```

`KeyRange` is defined in `common` (see Core Types) so both the planner and storage crates can reference it without depending on each other.

The executor receives a `PhysicalPlan` and only works with `BoundExpr`. Column access is by slot index (`row.values[slot]`) — O(1), no lookups. The `BindingId` and `ColumnId` fields in `InputRef` exist only for EXPLAIN output and debugging.

Statement setup binds far enough to discover logical table ids, acquires the
required table locks, revalidates schema versions/object identity, and then
captures the storage relation-generation snapshot used by execution. A newly
captured MVCC snapshot is paired with relation `Arc`s using the relation epoch
under `ServerComponents.relation_publish_gate`; relation-swap publication and
rollback take the gate's write side. Repeatable Read and Serializable retain
only their first MVCC snapshot. Their relation-generation snapshot is recaptured
per statement after locking. Because explicit transactions retain table locks
for every relation actually referenced, a recapture cannot change a referenced
relation behind the transaction, while unrelated tables are not eagerly pinned.
A session conflicting with transactional TRUNCATE waits before relation capture
and therefore sees the committed replacement or restored original, never the
uncommitted replacement. A newly referenced table whose generation changed after
a retained MVCC snapshot follows PostgreSQL's non-MVCC-safe TRUNCATE behavior:
the current generation is used, with ordinary tuple visibility still evaluated
against the retained MVCC snapshot.

`PRIMARY_KEY_INDEX_ID = 0` is reserved for storage's per-table identity index and is not assigned to catalog indexes. An `IndexScan` carries either that reserved identity-index id for predicates on the planned table schema's declared primary key, or a catalog index id for secondary/catalog indexes. `IndexScan.filter` holds residual predicates not consumed by that index's range (re-checked by the scan operator, so the choice of index never changes results). For `WHERE id = 7 AND name = 'Ada'`, a declared primary key on `id` uses `PRIMARY_KEY_INDEX_ID` with `Exact(Key([7]))` and the residual filter is `name = 'Ada'`; for `WHERE id = 7`, the residual filter is `None`. `IndexScan.full_filter` holds the original scan predicate so execution can defensively fall back to a full scan if the statement's captured relation generation lacks a catalog index chosen during planning. Scan plan nodes capture `table_name` at planning time solely for EXPLAIN/debug output; execution still uses `table`.

`SystemScan` is the planner and executor source for virtual system views. It is
not considered for storage index selection and carries the view plus full output
schema so execution and EXPLAIN/debug output do not need to re-resolve the
registry entry.

The three-phase pipeline (`bind` → `logical_plan` → `physical_plan`) means a future cost-based optimizer replaces only `physical_plan`, choosing among multiple physical alternatives per logical operator. The binder and logical planner are unchanged.

### Planner Rules (Applied in Order)

1. **Index lookup:** If `WHERE` has an equality or range comparison on the leading column of the table's declared primary key, emit `IndexScan` with `PRIMARY_KEY_INDEX_ID`. If it has an equality or range comparison on the leading column of a catalog index, emit `IndexScan` with that catalog index. Both forms carry a `KeyRange::Exact` (equality) or `KeyRange::Range` (range) over the column, the original predicate in `full_filter`, and any residual predicate in `filter`.
2. **Index choice:** When several indexed leading columns are constrained, prefer an equality over a range, primary-key identity access over catalog indexes, then the lower index id.
3. **Predicate pushdown:** Push `WHERE` conditions as close to the scan nodes as possible.
4. **Join ordering:** Process joins left to right as written. Eligible inner/semi/anti equi joins use `HashJoin`. A left/right/full join with at least one extractable cross-side equality and no DML identity source uses `MergeJoin`, with remaining `ON` conjuncts evaluated internally as residuals. Cross, non-equi, and DML-identity outer joins use `NestedLoopJoin`. Merge join owns spillable internal sorts but publishes no ordering property.
5. **Projection pushdown:** Optional. If implemented, only read columns that are needed downstream and rebase expression slots against each child output schema.

### EXPLAIN

`Statement::Explain` is handled by server `QueryService`, not by the executor. The server binds the inner statement, acquires its ordinary object-lifetime locks, plans the inner bound statement only, formats the resulting `PhysicalPlan` with planner-owned `format_explain`, and returns `ExecutionResult::Explanation`. `logical_plan` and `physical_plan` do not accept `BoundStatement::Explain` directly. Each plan node implements a `Display`-like method that shows the operator type, table/index involved, and any filter predicates.

### Planner Non-Goals

- Cost-based optimization
- Join reordering
- Merge joins
- Query plan caching

## 6. Query Executor

The `executor` crate takes a `PhysicalPlan` and evaluates it using the Volcano (iterator) execution model.

### Execution Model: Volcano / Pull-Based

Each plan node implements a pull-based iterator. The root pulls rows one at a time, which cascades down the tree to the scan nodes.

```rust
/// Row envelope that flows through the executor pipeline. Carries optional
/// physical identity (RowId + Key) so UPDATE/DELETE can target the source row.
/// SELECT operators ignore the handle; DML operators use it.
pub struct ExecRow {
    pub row: Row,
    pub identity: Option<RowIdentity>,
}

pub struct RowIdentity {
    pub row_id: RowId,
    pub key: Key,
}

pub trait PlanExecutor {
    /// Schema of the rows this operator produces.
    fn output_schema(&self) -> &[ColumnInfo];

    /// Open the operator, initialize state.
    fn open(&mut self) -> Result<()>;

    /// Pull the next row. Returns None when exhausted. Cancellation is polled by
    /// the query engine between rows (via `ExecutionContext.cancel`), not inside
    /// each operator's `next`.
    fn next(&mut self) -> Result<Option<ExecRow>>;

    /// Pull up to max_rows at once. The current implementation calls next() in a loop.
    /// Future vectorized execution can override with batch-native logic.
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<ExecRow>> {
        // default implementation
        let mut batch = Vec::with_capacity(max_rows);
        for _ in 0..max_rows {
            match self.next()? {
                Some(row) => batch.push(row),
                None => break,
            }
        }
        Ok(batch)
    }

    /// Close the operator, release resources (page pins, file handles, etc.)
    fn close(&mut self) -> Result<()>;
}
```

A cooperative cancellation token: `ExecutionContext.cancel` is a `&QueryCancel` the query engine checks between rows (and between rows of INSERT/UPDATE/DELETE write loops), aborting with `SqlState::QueryCanceled`. The token atomically retains the first `CancelReason` until reset, so a `CancelRequest` on a side connection reports `due to user request` and a statement timer reports `due to statement timeout`. Materializing paths drain children through a cancel-aware collector; blocking join, sort, aggregate, and set-operation work also polls at practical build/scan boundaries so expiration cannot remain hidden until a large `open()` finishes. Row-lock and storage maintenance waits use the same token.

`next_batch` has a default implementation so operators only implement `next()`. A future vectorized engine overrides `next_batch` with columnar processing. `output_schema()` allows callers to know the shape of rows without pulling, which is needed for `RowDescription`, EXPLAIN, and projection validation.

**ExecRow identity flow:**
- **Scan operators** (`SeqScanOp`, `IndexScanOp`): Construct `ExecRow` from `StoredRow`, populating `identity` from the `StoredRow`'s `row_id` and `key`.
- **System scan operator** (`SystemScanOp`): Materializes read-only virtual `pg_catalog`/`information_schema` rows from catalog metadata, static compatibility registries, and `StatementContext.system_state`, applies the scan filter, and emits rows with no identity. `pg_settings` reads the connection-backed GUC provider, and `pg_stat_activity` reads the server `SessionRegistry` in real connections; library/no-op contexts return empty activity rows.
- **Filter, Sort, Limit**: Pass `ExecRow` through unchanged (identity preserved).
- **Projection**: Rewrites `exec_row.row` (narrowed columns) but preserves `identity`.
- **Join, Aggregate**: Produce new rows — `identity` is `None` (these rows don't correspond to a single source row).
- **UPDATE/DELETE executor**: Reads `identity` from each `ExecRow` to call `storage.delete(ctx, table, &key)` or `storage.update(ctx, table, &key, new_row)`.
- **SELECT protocol layer**: Ignores `identity`, sends only `exec_row.row`.

### Operators

| Operator | Behavior |
|---|---|
| `SeqScanOp` | Iterates all rows in a table via storage, applies optional filter |
| `IndexScanOp` | Looks up rows through the chosen index — `scan_range` for the storage identity index, `index_scan` for a catalog index — and applies residual `IndexScan.filter` when present. If a catalog index is unavailable in the statement's captured relation generation, falls back to a table scan and applies `IndexScan.full_filter` |
| `SystemScanOp` | Emits computed rows for `pg_catalog` and `information_schema` virtual views, applies optional filter, and carries no row identity |
| `NestedLoopJoinOp` | Uses `work_mem`-bounded rewindable tapes for both inputs and streams matches/NULL extension; right/full unmatched detection uses externally sorted matched ordinals without re-evaluating predicates. |
| `HashJoinOp` | Builds the planner-selected side (right by default; left when statistics estimate it is smaller) in a reservation-accounted, key-sorted contiguous table while it fits, then releases it and falls back to a bounded rewindable spill-tape probe; NULL keys never match and output remains logical left ++ right. |
| `ApplyOp` / `LateralApplyOp` | Use one operator-local `work_mem` account for LRU correlation metadata and spillable scalar-column/row results; LATERAL replays one inner row at a time and preserves outer identity. |
| `MergeJoinOp` | Stable-sorts both inputs with one shared `work_mem` account; NULL-bearing keys never match. Rewindable spill tapes and externally sorted match ordinals bound duplicate groups while residuals run once per pair; output identity is cleared. |
| `FilterOp` | Passes through rows matching the predicate |
| `ProjectionOp` | Evaluates expressions, outputs narrowed columns |
| `SortOp` | Evaluates keys once and uses a stable `work_mem`-bounded external sort, spilling anonymous runs below the configured temp directory; preserves row identity. Blocking operator. |
| `DistinctOp` | Uses bounded external key and ordinal sorts to retain the first row of each distinct key in input order; NULL keys collapse together and identity is cleared. Blocking operator. |
| `LimitOp` | Stops pulling after N rows |
| `AggregateOp` | Folds global aggregates directly; grouped aggregates use a bounded external key sort, online non-DISTINCT states, and bounded per-expression DISTINCT argument sorts sharing one budget; streams one result per group and clears identity. Blocking operator. |
| `SetOpOp` | Uses bounded external key and ordinal sorts for UNION/INTERSECT/EXCEPT distinct and multiset semantics, preserves left-to-right output order, and clears identity. Blocking operator. |

### Expression Evaluator

A recursive function that takes a `BoundExpr` and an `ExecRow` and returns a `Value`. Column access is by slot index (`exec_row.row.values[input_ref.slot]`) — no schema lookup needed at evaluation time. Handles arithmetic, comparisons, the NULL-safe `IS [NOT] DISTINCT FROM`, string concatenation (`||`), boolean logic, NULL propagation (three-valued logic), `CASE`, `CAST`, `IN`, `LIKE`, `BETWEEN`, and the scalar functions `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `SUBSTRING`, the math functions `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND`, `SQRT`, `POWER`/`POW`, `MOD`, the string functions `REPLACE`, `POSITION`, `CONCAT`, `LEFT`, `RIGHT`, `EXTRACT(field FROM date/timestamp)`, statement clock functions `CURRENT_TIMESTAMP`/`NOW()`, the PostgreSQL-compatible system information functions `VERSION`, `CURRENT_DATABASE`, `CURRENT_CATALOG`, `CURRENT_SCHEMA`, `CURRENT_USER`, `SESSION_USER`, `USER`, `PG_BACKEND_PID`, and `CURRENT_SETTING(text)`, and PostgreSQL-compatible catalog introspection/probe functions such as `FORMAT_TYPE`, `PG_GET_INDEXDEF`, `PG_GET_EXPR`, `PG_GET_CONSTRAINTDEF`, `PG_TABLE_IS_VISIBLE`, `TO_REGCLASS`, `TO_REGTYPE`, `PG_GET_SERIAL_SEQUENCE`, and `HAS_*_PRIVILEGE` (`COALESCE`/`NULLIF` are desugared to `CASE` by the binder). These scalar functions are dispatched through the scalar function registry in `common` (`docs/specs/crates/common.md`), which pairs each function's bind-time signature check with its evaluator so it is defined once for both the binder and this evaluator. Catalog introspection functions read `StatementContext.catalog_introspection` and propagate provider errors; provider `None` maps to SQL `NULL` for nullable metadata lookups. Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP`, `VARIANCE`/`VAR_SAMP`/`VAR_POP`, `BOOL_AND`, `BOOL_OR`) are evaluated by `AggregateOp`, not scalar expression evaluation. Type information is carried in bound expressions (`data_type`, `nullable`), so the evaluator can validate without external lookups.

Expression semantics:

- Comparisons with `NULL` return `NULL`; `WHERE` and `HAVING` keep only `TRUE`.
- `LIKE`/`ILIKE` require text operands, support `%` and `_`, and use the pattern escape character (default backslash, overridable with `ESCAPE c`, disabled with `ESCAPE ''`) to escape `%`, `_`, or the escape character. `ILIKE` matches case-insensitively. If the value or pattern is `NULL`, the result is `NULL`.
- `IN` returns `TRUE` on the first non-null equal item, `FALSE` when no item matches and no list item is `NULL`, and `NULL` when the left side is `NULL` or no item matches but some list item is `NULL`. `NOT IN` applies SQL `NOT`.
- `BETWEEN` evaluates as `(expr >= low) AND (expr <= high)`; `NOT BETWEEN` applies SQL `NOT`.
- String concatenation `||` requires text operands and returns `NULL` if either side is `NULL`. The scalar functions `UPPER`/`LOWER`/`LENGTH`/`TRIM` (text), `SUBSTRING(text, start[, length])`, and the math functions `ABS`/`FLOOR`/`CEIL`/`CEILING`/`ROUND`/`SQRT`/`POWER`/`POW`/`MOD` (over `INTEGER`/`DOUBLE PRECISION`) are NULL-propagating; `LENGTH` and `SUBSTRING` count Unicode characters, and `SUBSTRING` uses 1-based positions clamped to the string and rejects a negative length. `FLOOR`/`CEIL`/`ROUND` keep an integer unchanged and round a double (`ROUND` half-to-even); `SQRT`/`POWER` return `DOUBLE`; `MOD` is integer-only. The string functions `REPLACE`/`POSITION`/`LEFT`/`RIGHT` are NULL-propagating; `CONCAT` ignores NULL arguments and never returns NULL. `EXTRACT(field FROM source)` returns the `year`/`month`/`day`/`hour`/`minute`/`second` component of a `DATE`/`TIMESTAMP` as `DOUBLE PRECISION` (NULL-propagating). `CURRENT_TIMESTAMP` and `NOW()` return the statement start timestamp as non-null `TIMESTAMP WITH TIME ZONE`, stable within one statement. Zero-argument system information functions are non-nullable: `VERSION()` returns the SaguaroDB PostgreSQL-compatible version string, `CURRENT_DATABASE()`/`CURRENT_CATALOG` return the startup database, `CURRENT_SCHEMA` returns `public`, `CURRENT_USER`/`SESSION_USER`/`USER` return the startup user, and `PG_BACKEND_PID()` returns the connection process id. `CURRENT_SETTING(text)` is NULL-propagating for a NULL name, otherwise reads the statement's `SystemStateProvider` and returns `SqlState::UndefinedObject` (`42704`) for an unknown parameter. PostgreSQL catalog introspection/probe functions are registry-backed scalar functions; metadata lookups that miss return `NULL`, provider failures propagate as execution errors, `FORMAT_TYPE` formats supported PostgreSQL type OIDs, privilege probes return `TRUE` while SaguaroDB has no grant model, and unimplemented definition/description helpers return `NULL`. `CURRENT_DATE` is not supported.
- Searched `CASE WHEN condition THEN value ...` chooses the first `WHEN` whose condition evaluates to `TRUE`; `FALSE` and `NULL` conditions do not match. Simple `CASE operand WHEN value THEN result ...` compares `operand = value` with SQL comparison semantics and chooses the first comparison that evaluates to `TRUE`. If no branch matches, both forms return `ELSE` or `NULL`.
- `CASE` result typing: binder requires all non-`NULL` `THEN` and `ELSE` expressions to have the same `DataType`; `NULL` branches are allowed and make the output nullable. If every result branch is `NULL`, binder rejects the expression with `SqlState::DatatypeMismatch`.
- Explicit `CAST` conversion matrix: same-type casts are identity; `NULL` casts to `NULL`; `INTEGER -> TEXT` uses decimal i64 formatting; `BOOLEAN -> TEXT` returns `true` or `false`; `TEXT -> INTEGER` parses a base-10 i64 with optional leading sign and no surrounding whitespace; `TEXT -> BOOLEAN` accepts case-insensitive `true`, `t`, `1`, `false`, `f`, and `0`. `INTEGER -> BOOLEAN`, `BOOLEAN -> INTEGER`, malformed text, and all other pairs return `SqlState::DatatypeMismatch`.
- `ORDER BY` defaults match PostgreSQL: ascending sorts `NULL` last, descending sorts `NULL` first, unless `NULLS FIRST` or `NULLS LAST` is specified. A bare positive integer literal in `ORDER BY` is a 1-based reference to the nth output column, resolved by the binder.

### DDL and DML

`INSERT`, `UPDATE`, and `DELETE` are handled directly by the executor (not through the iterator model), call into storage, and return the affected row count. `CREATE TABLE`, `DROP TABLE`, `CREATE VIEW`, `DROP VIEW`, `CREATE INDEX`, `DROP INDEX`, `CREATE SEQUENCE`, and `DROP SEQUENCE` also return `ExecutionResult::Modified`, using the matching command names with `count = 0`. Conditional table/view DDL no-ops (`CREATE TABLE IF NOT EXISTS` when the table already exists, `DROP TABLE IF EXISTS` when no table or view exists, `DROP VIEW IF EXISTS` when no view or table exists) return the same command tags without mutating catalog/storage or appending logical DDL WAL records; `CREATE TABLE IF NOT EXISTS` still validates the requested table definition shape before suppressing a duplicate-table conflict. If the name exists in the shared user relation namespace with the wrong kind (for example `DROP TABLE`/`DROP TABLE IF EXISTS` naming a view or `DROP VIEW`/`DROP VIEW IF EXISTS` naming a table), execution returns `SqlState::WrongObjectType`.

## 7. Storage Engine

The `storage` crate owns the on-disk data format, page-backed row storage, and each table's durable on-disk storage-identity B-tree.

### Row Iterator

```rust
/// Fallible iterator over rows from the storage engine. Returns StoredRow
/// so that DML operations can target the physical row for modification.
/// Rows are copied out of the buffer pool. A future version may return
/// zero-copy references into pinned pages.
pub trait RowIterator: Send {
    fn next(&mut self) -> Result<Option<StoredRow>>;

    /// Schema of the rows this iterator produces.
    fn schema(&self) -> &[ColumnInfo];
}
```

The executor's scan operators convert `StoredRow` → `ExecRow`:
```rust
ExecRow {
    row: stored_row.row,
    identity: Some(RowIdentity { row_id: stored_row.row_id, key: stored_row.key }),
}
```

The `ExecRow` then flows through the entire executor pipeline with identity preserved (see ExecRow identity flow above). This ensures projection pushdown can't accidentally remove the information needed to target a row for modification.

### Storage Engine Traits

Data operations and DDL are separate traits because DDL includes file creation and catalog updates while DML operates within existing table pages. Both run under the shared writer guard; table modes and the server catalog publication gate provide their distinct logical exclusion. Transactional DDL stages catalog changes in the transaction-local overlay and publishes them after durable commit.

```rust
pub trait StorageEngine: Send + Sync {
    fn capture_relation_snapshot(&self) -> Result<Arc<dyn RelationSnapshot>>;

    /// Insert a row, returns its physical RowId
    fn insert(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        row: Row,
    ) -> Result<RowId>;

    /// Point lookup by storage identity key
    fn get(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<Option<Row>>;

    /// Delete by storage identity key
    fn delete(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<bool>;

    /// Update by storage identity key
    fn update(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
    ) -> Result<bool>;

    /// Full table scan
    fn scan(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
    ) -> Result<Box<dyn RowIterator>>;

    /// Full table scan callback used by rewrite DDL to avoid table-sized vectors.
    fn for_each_visible_row(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        visitor: &mut dyn FnMut(StoredRow) -> Result<()>,
    ) -> Result<()>;

    /// Range scan over the storage identity access path
    fn scan_range(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>>;

    fn index_scan(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        index: IndexId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>>;

    /// Restore storage-owned in-memory metadata changed by an uncommitted statement.
    /// Page bytes are restored separately by BufferPool::rollback.
    fn rollback_txn(&self, txn_id: u64) -> Result<()>;

    /// Discard storage-owned rollback metadata after WAL flush succeeds.
    fn commit_txn(&self, txn_id: u64) -> Result<()>;
}

pub trait SchemaOperations: Send + Sync {
    /// Create a new table's storage files
    fn create_table(&self, ctx: &StatementContext, schema: &TableSchema) -> Result<()>;

    /// Drop a table's storage files
    fn drop_table(&self, ctx: &StatementContext, table: TableId) -> Result<()>;
}
```

Every operation takes a `StatementContext`. It carries the `txn_id`, the MVCC `snapshot` used for visibility, the `isolation` level, and the `gc_horizon` — so each storage operation sees and stamps versions consistently without changing any call sites.

`scan_range` serves the storage identity access path. For `KeyRange::Exact`, it is a point lookup that returns an iterator (consistent interface). For `KeyRange::Range`, it walks the identity B-tree leaves from start to end. For `KeyRange::All`, it is equivalent to `scan`. `IndexScan` plan nodes with `index != PRIMARY_KEY_INDEX_ID` use `index_scan(table, index, range)`, which walks the catalog-index B-tree and reads each entry's heap row directly at the stored TID (catalog indexes point at heap TIDs, uniform with the identity index — no identity-index indirection).

### Page Format (8KB Pages)

```
+----------+----------+-------------+----------+-----------+----------+
| PageID   | PageType | PageVersion | NumSlots | FreeSpace | Checksum |
| 4 bytes  | 1 byte   | 1 byte      | 2 bytes  | 2 bytes   | 4 bytes  |
+----------+----------+-------------+----------+-----------+----------+
| Slot Array  (offset, length pairs — grows downward)    |
+--------------------------------------------------------+
| Free Space                                             |
+--------------------------------------------------------+
| Row Data  (serialized rows — grows upward from bottom) |
+--------------------------------------------------------+
```

Slotted page design. Slot array at the top points to variable-length rows packed at the bottom. Deleting marks the slot as dead. Pages can be compacted when free space is fragmented.

**Checksum:** CRC32 computed over entire page content (excluding the checksum field). Verified on every read from disk. Recomputed on every flush to disk.

**PageVersion:** `2` for the current page format. Unknown versions (including the legacy `1`) are rejected as page corruption.

Development builds do not migrate older page formats. Existing page files without `PageVersion = 2` are rejected as corrupt during load/recovery.

**PageLSN:** The page header carries an 8-byte PageLSN — the LSN of the WAL record that last modified the page — stamped on every mutation. Redo replay is gated by it (a record is applied only if `page_lsn < record.lsn`), and it determines when a dirty page is safe to flush. See the Write-Ahead Log section.

### Page-Backed Storage-Identity Structure

- Heap pages store full serialized rows.
- A durable, non-clustered on-disk B-tree (`Key -> RowLocation`) in a separate file per table maps the table's storage identity to physical heap slots. Tables with a primary key use primary-key values as the identity; tables without one use a hidden heap identity derived from the root heap slot.
- `RowLocation` stores `file_id`, `page_num`, and `slot_num`.
- The B-tree is durable, so nothing is rebuilt on startup; its pages are recovered by redo like any other page.
- A future clustered B-tree (rows in the leaves) can replace this internal access path without changing the public storage traits.

### Row Serialization

```
[row_format_version: 1 byte][infomask: 2][xmin: 8][xmax: 8][t_ctid: 6][null_bitmap][col1_data][col2_data]...
```

- `row_format_version`: ordinary INSERT and non-HOT UPDATE emit prepared row format `3`; the legacy `encode_row` helper still emits v2 for tests and compatibility helpers. `decode_row` still accepts legacy `1` tuples (`[version=1][null_bitmap][columns]`, no MVCC header), v2 tuples, and v3 tuples whose varlena values are plain; other versions are rejected as corrupt.
- MVCC tuple header (v2/v3, little-endian): `infomask` (2-byte hint bits — `XMIN_COMMITTED`/`XMIN_ABORTED`/`XMAX_COMMITTED`/`XMAX_ABORTED` cache settled CLOG status, `HEAP_ONLY`/`HOT_UPDATED` mark HOT-chain tuples (Milestone H: a heap-only successor and the root that was HOT-updated to it), rest reserved-zero), `xmin` (8-byte creator txn id), `xmax` (8-byte deleter txn id; `0` = live), and `t_ctid` (forward successor pointer `(page: u32, slot: u16)`; sentinel `(u32::MAX, u16::MAX)` = latest version).
- Insert stamps `xmin = txn_id` (from `StatementContext.txn_id`), `xmax = 0`, `t_ctid = sentinel`, `infomask = 0`. Legacy v1 tuples decode as frozen/always-visible (`xmin = FROZEN_XID`, `xmax = 0`).
- `INTEGER`: 8 bytes, little-endian i64
- `TEXT`/`BYTEA`: v1/v2 use a 4-byte length prefix plus bytes. V3 uses the top two bits of that same length word as a tag: `00` plain (byte-identical to v2), `01` inline compressed (`codec`, `dict_id`, `raw_len`, `raw_crc32`, payload), `10` a 17-byte external TOAST pointer, and `11` reserved/corrupt. The low 30 bits are the supported varlena length cap. External TOAST stream bytes are stored in the hidden TOAST relation as `[raw_crc32][raw]` for codec `none`, `[raw_crc32][zstd payload]` for codec `zstd`, or `[dict_id][raw_crc32][zstd-dict payload]` for codec `zstd_dict`; the pointer's `stored_len` includes this stream header. Storage chunks those stream bytes into `(value_id, seq, data)` rows with `TOAST_CHUNK_PAYLOAD = 1900`, scans them by `(value_id)` primary-key prefix, and treats missing, duplicate, out-of-order, or length-mismatched chunks as corruption.
- `BOOLEAN`: 1 byte
- `NULL`: represented in the null bitmap, no data bytes

Storage uses a TOAST-aware row preparation helper for ordinary INSERT and normal non-HOT UPDATE. The helper converts logical rows to v3 parent tuple bytes, preflights identity and catalog-index key sizes before any chunk writes, bypasses recursive TOAST for hidden TOAST relations, preserves inline-only behavior for legacy user tables without a companion TOAST relation, attempts configured value compression for eligible medium `TEXT`/`BYTEA` values, length-simulates largest-first externalization before writing chunks, then writes TOAST chunks under the caller's transaction only after the final parent tuple is known to fit a page. HOT UPDATE reuses the inline preparation logic with `HEAP_ONLY` set and is eligible for TOAST-enabled tables only when the predecessor has no external TOAST pointer and the successor can be prepared without external chunks; any external-pointer owner or would-be-externalized successor falls back to the normal fully-indexed update path.

User-facing storage reads resolve tuple visibility from MVCC headers before materializing v3 TOAST values. Only visible parent tuples decompress inline compressed values or read external chunks from the hidden TOAST relation; invisible tuples with missing or corrupt chunks are skipped without touching those chunks.

VACUUM integrates hidden TOAST cleanup before discarding parent tuple bytes. Under
the target table's `Share` lock and shared writer guard, storage first identifies external value ids owned by
parent tuples that full VACUUM would prune without detoasting. The server deletes
visible hidden chunks for those value ids in a real committed maintenance transaction,
then uses the coordinated TOAST parent-prune path and runs ordinary VACUUM on the
hidden TOAST relation. Direct storage-level parent `vacuum` rejects TOAST-enabled
tables when full VACUUM would prune external pointers before this cleanup check.
Update-path HOT pruning leaves chains with external pointers for full VACUUM,
preserving the parent bytes needed to find chunk ownership.

### File Layout

Files are named by stable numeric ID, not by user-visible names. This avoids rename issues (future `ALTER TABLE RENAME`), filesystem-unsafe characters in table names, and name collisions.

**Heap and index files (the mutable page home):**
- `data/heap/<storage_id>.heap` — table/TOAST relation-generation slotted data pages, page `n` at byte offset `n * PAGE_SIZE`. Written in place by checkpoint flush or eviction; files grow by appending pages.
- `data/heap/<storage_id>.idx` — a table/TOAST generation's storage-identity B-tree (metapage at page 0, then leaf/internal nodes), same page layout and offsets.
- `data/heap/<storage_id>.sidx` — a catalog-index generation's B-tree, using a separate tagged `FileId` namespace from heaps and identity indexes.

**Control record (the single source of truth for the current checkpoint):**
- `data/manifest.dat` — a versioned binary envelope (magic `SGMF`, version, payload length, CRC32 over the payload) whose JSON payload holds the redo boundary `checkpoint_lsn`, sorted table IDs, and the catalog snapshot.

The control record is updated atomically via write-to-temp + fsync + rename (atomic on POSIX) + directory fsync. Recovery reads it to find the redo boundary and catalog. See Checkpoint below for the full protocol.

**Other files:**
- `data/wal.dat` — write-ahead log (append-only)

Table names are purely a catalog-level concept. The storage engine uses stable logical table/index IDs for metadata lookup, then routes page I/O through the current `storage_id` stored in `TableSchema` / `IndexSchema`.

## 8. Buffer Pool

The `buffer` crate manages a fixed-size pool of in-memory page frames.

### Trait

```rust
pub trait BufferPool: Send + Sync {
    /// Fetch a page for reading. Returns a guard that unpins on drop.
    fn read_page(&self, file_id: FileId, page_num: PageNum) -> Result<PageReadGuard>;

    /// Fetch a page for writing. txn_id identifies the active statement.
    /// Returns a guard that unpins on drop and automatically marks dirty.
    /// (With MVCC, abort is status-based — no before-image is saved.)
    fn write_page(&self, file_id: FileId, page_num: PageNum, txn_id: u64) -> Result<PageWriteGuard>;

    /// Allocate a new page in the given file, return it locked for writing.
    /// The returned PageWriteGuard exposes page_num() for row-location tracking.
    fn new_page(&self, file_id: FileId, txn_id: u64) -> Result<PageWriteGuard>;

    /// Abandon an unpublished fresh page whose first redo append failed before
    /// any bytes were published; consumes the new-page guard and removes/reuses
    /// the allocation instead of leaving a phantom page in the full extent.
    fn abandon_unpublished_new_page(&self, guard: PageWriteGuard) -> Result<()>;

    /// True for an interior abandoned fresh-page hole that full-extent maintenance
    /// should skip.
    fn is_page_abandoned(&self, file_id: FileId, page_num: PageNum) -> bool;

    /// Insert an exact clean page into the pool (does not mark it dirty).
    fn load_page(&self, file_id: FileId, page_num: PageNum, data: PageData) -> Result<()>;

    /// Rollback: a no-op bookkeeping clear. With MVCC, abort is status-based
    /// (`docs/specs/mvcc.md` §4 Decision 3) — no page undo, no page reclamation.
    /// A rolled-back transaction's pages stay dirty-but-evictable, hidden by the
    /// CLOG and reclaimed by VACUUM.
    fn rollback(&self, txn_id: u64) -> Result<()>;

    /// Commit: a no-op cleanup (no per-transaction page metadata is tracked).
    /// Changes are eligible to be flushed at the next checkpoint.
    fn commit(&self, txn_id: u64) -> Result<()>;

    /// Iterate all frames (used by checkpoint flushing and the storage page scan). Returns (file_id, page_num, data, is_dirty).
    fn iter_pages(&self) -> Result<Box<dyn Iterator<Item = PageInfo>>>;

    /// Mark all dirty pages as clean (called by checkpoint after flushing them to the heap).
    fn mark_all_clean(&self) -> Result<()>;
    fn mark_files_clean(&self, file_ids: &[FileId]) -> Result<()>;
    fn flush_dirty_pages(&self) -> Result<()>;
    fn flush_dirty_pages_for_files(&self, file_ids: &[FileId]) -> Result<()>;
    // Recovery redo adds fetch_for_redo; see buffer.md.
}
```

### RAII Page Guards

```rust
/// Owned read guard for a buffer pool frame. Holds an Arc to the frame
/// internally. Dereferences to the page data (&[u8; PAGE_SIZE]).
/// Unpins the frame and releases the read latch on Drop.
pub struct PageReadGuard { /* Arc<Frame> + latch state */ }

/// Owned write guard for a buffer pool frame. Holds an Arc to the frame
/// internally. Dereferences to mutable page data (&mut [u8; PAGE_SIZE]).
/// Marks the page dirty (with the current txn_id), unpins the frame,
/// and releases the write latch on Drop.
pub struct PageWriteGuard { /* Arc<Frame> + latch state */ }

impl PageReadGuard {
    pub fn file_id(&self) -> FileId;
    pub fn page_num(&self) -> PageNum;
    pub fn data(&self) -> &[u8; PAGE_SIZE];
}

impl PageWriteGuard {
    pub fn file_id(&self) -> FileId;
    pub fn page_num(&self) -> PageNum;
    pub fn data(&self) -> &[u8; PAGE_SIZE];
    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE];
}
```

`new_page(file_id, txn_id)` allocates the next unused page number for that file and returns a guard whose `page_num()` identifies the new page. The fresh-page insertion path rejects an already resident `(file_id, page_num)` with an internal error instead of overwriting it. The pool tracks `next_page_num_by_file`; `load_page(file_id, page_num, data)` inserts `data` as a clean frame when the page is not resident. If `(file_id, page_num)` is already resident, `load_page` leaves resident bytes, dirty state, and dirty transaction ID unchanged, still advances the next-page counter to at least `page_num + 1`, and returns `Ok(())`. Rollback does not remove fresh pages. Only `abandon_unpublished_new_page(guard)` removes or marks reusable an unpublished fresh page, and callers use it only when the first redo append for that page failed before any page image was published. The API consumes the still-held `new_page` guard and refuses abandonment after mutable bytes have been exposed.

Guards are owned types (no lifetime parameter) — same rationale as the concurrency controller guards. They hold `Arc` references to the buffer pool frame internally, which keeps `BufferPool` object-safe. The Arc overhead is one reference count per page access, negligible compared to the I/O it represents.

Guards eliminate manual pin/unpin errors: a page is pinned for exactly the lifetime of the guard. Early returns, panics, and `?` propagation all unpin correctly via `Drop`.

### Design

- **Frame:** A slot holding one 8KB page. Pool size is configurable (default: 1024 frames = 8MB).
- **Page descriptor:** Tracks `(file_id, page_number)`, pin count, dirty flag, reference bit, `dirty_txn_id` (the txn that last dirtied it), and `needs_fpi` (whether the next modification must log a full-page image).
- **No rollback tracking (MVCC):** the buffer pool keeps no per-transaction page state. Abort is status-based (`docs/specs/mvcc.md` §4 Decision 3): `rollback(txn_id)` undoes nothing and reclaims nothing — a rolled-back transaction's pages (modified or freshly allocated) stay resident as dirty-but-evictable frames, hidden by the CLOG and reclaimed by VACUUM. (The before-image store and new-page rollback tracking that the pre-MVCC model used are retired in Milestone D1.)
- **PageStore / PageLoader:** The buffer pool is constructed with an `Arc<dyn PageStore>`, which extends the read-only `PageLoader`:

```rust
pub trait PageLoader: Send + Sync {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>>;
}
```

On a `read_page` miss, the pool asks the loader for a clean page. `Some(data)` is inserted as a clean frame; `None` returns `ErrorKind::Storage` / `SqlState::InternalError` with message `page not found`; loader I/O failures propagate as `ErrorKind::Io`. In production, the server supplies a `HeapPageStore` (a `PageStore`) that reads page `n` of table `file_id` from `data/heap/<file_id>.heap`. `MemoryBufferPool::empty(frame_count)` is a test helper using a never-flush policy and a no-op store that returns `Ok(None)`.

- **FlushPolicy:** The buffer pool is constructed with a `Box<dyn FlushPolicy>`. `flush_dirty_pages` (checkpoint) consults it per dirty page: `WalFlushPolicy` admits any dirty page whose page-LSN is WAL-durable (with MVCC the committedness check is gone). The same policy gates eviction-flush-on-steal, whose path also calls `ensure_durable` to force the WAL before writing a possibly-uncommitted stolen page.

### Eviction: Clock Algorithm (Single-Bit)

- Clock hand sweeps through frames
- Each frame has a reference bit, set on access
- On sweep: if bit is 1, clear to 0 and skip. If bit is 0 and unpinned, check dirty flag.
- **If clean:** evict immediately. The page can be re-read from its heap file if needed later.
- **If dirty:** when stealing is enabled, steal it if the `FlushPolicy` admits it (WAL-durable): force the WAL (`ensure_durable`), flush the page to its heap home outside the pool lock, then evict. With MVCC a stolen page need not be committed (the CLOG hides an uncommitted/aborted one). A page the policy refuses (not WAL-durable), or any dirty page when stealing is off, is skipped. The server enables stealing at startup, before redo.
- If no frame can be freed — every frame is pinned, or every unpinned frame is dirty and unflushable (not WAL-durable) — the buffer pool returns an out-of-frames error.

### Working Set and the Buffer Pool

During normal operation the working set is not bound by the pool size: eviction-flush-on-steal writes dirty pages to the heap and evicts them, so a large dataset (or a large in-flight transaction) spills rather than erroring. With concurrent writers:
- Each statement dirtys a modest number of pages
- Pages stay dirty in memory until a checkpoint flushes them in place or an eviction steals them
- The buffer pool default (1024 frames = 8MB) keeps a small-to-medium working set resident; larger sets spill to the heap
- **Recovery** spills too: stealing is enabled before redo, so the redo working set is not bounded by the buffer pool either (the durable on-disk index means nothing is rebuilt in memory)

### Concurrency

- Frame-level read/write latches managed by the page guards (multiple concurrent readers, exclusive writer)
- Page table mapping `(file_id, page_num)` to frame protected by a separate latch
- Multiple threads can read different pages concurrently

## 9. Control Store

The `control` crate owns the durable **control record** — the checkpoint commit point. It does not write whole-table snapshots; table data lives in mutable relation-generation heap files (see Storage Engine) and is flushed in place. The control record is a single atomic file holding the redo boundary, the live table ids, and the catalog snapshot.

### Trait

```rust
pub struct ControlData {
    pub checkpoint_lsn: Lsn,   // redo boundary
    pub tables: Vec<TableId>,  // sorted, no duplicates
    pub catalog: Vec<u8>,      // serialized catalog snapshot
}

pub trait ControlStore: Send + Sync {
    /// Load the current control record, or None if none exists yet.
    fn load(&self) -> Result<Option<ControlData>>;

    /// Atomically write a new control record (the checkpoint commit point).
    fn store(&self, checkpoint_lsn: Lsn, tables: &[TableId], catalog: &[u8]) -> Result<()>;
}
```

`store` writes `data/manifest.dat` via temp file + fsync + rename + directory fsync. The rename is the checkpoint commit point: the caller must fsync the heap (`PageStore::sync_all`) **before** `store`, and truncate the WAL only **after** it.

The control record uses a versioned binary envelope: magic `SGMF`, a `u32` version, payload length, CRC32 over the payload, and a JSON payload of `checkpoint_lsn`, sorted `tables`, and `catalog`. Decode rejects magic/version/length/checksum mismatch, malformed JSON, and unsorted or duplicate table IDs. The legacy full-snapshot manifest (version `1`) is rejected, not migrated.

### checkpoint_lsn

`checkpoint_lsn` is the **authoritative redo boundary**: the WAL high-water mark whose effects are reflected in the heap. Recovery reads it from the control record and replays WAL records with `LSN > checkpoint_lsn` (redo-all — see below). The WAL `Checkpoint` record is optional metadata; the control record is authoritative.

## 10. Write-Ahead Log (WAL)

The `wal` crate provides durability with a **physiological redo WAL**: physical-redo records describe page changes (`HeapInit`, `HeapInsert`, `HeapDelete`, `HeapUpdateHeader`, `FullPageImage`) gated by a per-page LSN, alongside logical DDL records (`CreateTable`, `DropTable`, `CreateIndex`, `DropIndex`, `CreateSequence`, `DropSequence`, `CreateView`, `ReplaceView`, `DropView`, `CreateSchema`, `DropSchema`, `AlterTableCompression`, `AlterTableToast`, `TruncateTable`, `AlterTablePrimaryKey`, `UpdateTableSchema`, `CreateDictionary`), non-transactional sequence value records (`SequenceAdvance`, `SetSequenceValue`), and the `Commit`/`Abort`/`Checkpoint` markers. Recovery is **redo-all**: it replays every physical record under PageLSN gating regardless of the transaction's outcome, and the CLOG (rebuilt from `Commit`/`Abort`) decides visibility afterward; an aborted/in-flight transaction's replayed versions are invisible. Logical DDL records install objects only for committed transactions; skipped aborted/in-flight create/truncate/schema-rewrite records still reserve their schema/table/view/index/sequence/dictionary/storage IDs or carried rewrite storage IDs so orphan page files, catalog IDs, dictionary IDs, or relation files cannot be reused. Sequence value records replay unconditionally because sequence advancement is non-transactional. (See `docs/specs/mvcc.md` §8 for the full recovery contract.)

### Durability Model: Heap Files + Redo WAL + Flush Checkpoint

Table data lives in mutable per-generation heap files; pages are mutated in the buffer pool and written back in place. In-place page writes with a logical-only WAL would be unrecoverable (a torn page has no consistent base), so the engine uses:

- **Per-page LSN (PageLSN)** in the page header, stamped with the LSN of the record that last modified the page. Redo is gated by it (apply only if `page_lsn < record.lsn`), making replay idempotent.
- **Full-page writes (FPW)** for torn-page protection: the first modification of a page after each checkpoint logs a `FullPageImage`; later modifications log deltas. Redo reinstalls the image (repairing any torn write) before applying deltas. A freshly allocated page is its own base via `HeapInit`.
- **Flush-based checkpoint**: dirty pages are flushed in place to the heap and fsynced, then the control record advances the redo boundary, then the durable CLOG snapshot (`clog.dat`) is written, then the WAL prefix is truncated. With MVCC the flush gate requires only WAL-durability (not committedness), so uncommitted/aborted dirty pages may be flushed too — they are hidden by the CLOG and reclaimed by VACUUM. WAL truncation is unconditional: it drops every record below the boundary, relying on the CLOG snapshot (written first) to remember aborted outcomes (`docs/specs/mvcc.md` §5.4/§8).

This gives the invariants:
1. After a crash, recovery loads the heap as of the last control record and replays redo records with `LSN > checkpoint_lsn`; PageLSN gating plus full-page images make this idempotent and torn-page-safe. (With MVCC this is redo-all + CLOG visibility.)
2. The WAL captures all operations since the redo boundary.
3. Checkpoint cost is O(pages changed), not O(database size).

**Trade-offs:**
- Normal operation and recovery both spill dirty pages to the heap via eviction-flush-on-steal; the working set is not bounded by the buffer pool size (the durable on-disk index means recovery rebuilds nothing in memory). The steal path forces the WAL durable before writing a stolen page (write-ahead), so a possibly-uncommitted stolen page is always recoverable.
- Startup replays WAL from the last checkpoint — bounded by checkpoint frequency.

**MVCC** is implemented (see `docs/specs/mvcc.md`): snapshot isolation, multi-statement transactions, concurrent writers, VACUUM, and HOT, all built on this redo WAL. Row format v2 carries the per-version `xmin`/`xmax`/`t_ctid`/`infomask` tuple header that visibility and version chains rely on. (None of this changed the `BufferPool` or `StorageEngine` traits.)

### WAL Record Format

```
+--------+--------+--------+----------+
| LSN    | TxnID  | Type   | Length   |
| 8 bytes| 8 bytes| 1 byte | 4 bytes  |
+--------+--------+--------+----------+
| Payload (variable length)  | CRC32   |
|                            | 4 bytes |
+----------------------------+---------+
```

- **LSN:** Monotonically increasing identifier for each record.
- **TxnID:** The per-transaction id used for MVCC visibility, write-write conflict detection, and WAL. Multi-statement transactions share one id across their statements; an autocommit statement gets its own.
- **Type:** One of the logical operation types below.
- **Payload:** Depends on type.
- **CRC32:** Integrity check over the entire record.

**Record types and payloads:**

| Type | Payload |
|---|---|
| `CreateTable` | serialized `TableSchema` (logical id, storage id, name, columns, primary key) |
| `DropTable` | `TableId` |
| `CreateIndex` | serialized `IndexSchema` (logical id, storage id, table, name, columns, unique) |
| `DropIndex` | `IndexId` |
| `CreateSchema` | serialized `NamespaceSchema` |
| `DropSchema` | `SchemaId` |
| `TruncateTable` | `TableId`, new table/TOAST/index storage ids |
| `AlterTablePrimaryKey` | `table_id`, primary-key `ColumnId` list |
| `UpdateTableSchema` | serialized `TableSchema` and carried `IndexSchema` list for schema-evolution DDL |
| `Commit` | (empty — marks the transaction as committed) |
| `Abort` | (empty — marks the transaction as aborted; `txn_id` in the header) |
| `Checkpoint` | `redo_lsn` — marks a completed checkpoint. WAL records before it can be truncated. |
| `HeapInit` | `FileId`, `PageNum` — initialize a fresh heap page |
| `HeapInsert` | `FileId`, `PageNum`, `slot`, encoded row bytes |
| `HeapDelete` | `FileId`, `PageNum`, `slot` |
| `HeapUpdateHeader` | `FileId`, `PageNum`, `slot`, `xmax`, `t_ctid` (`PageNum`, `u16`), `infomask` — in-place mutation of a v2 tuple header (MVCC version stamping; redo via `page::set_tuple_header`, emitted by the update/delete path via `stamp_xmax_logged`) |
| `FullPageImage` | `FileId`, `PageNum`, full page image (torn-page protection) |

Transaction ids `0..FIRST_NORMAL_XID` (3) are reserved: `INVALID_XID = 0` (no/non-transactional record), `1`, and `FROZEN_XID = 2` (always-committed/visible). Real statement transaction ids are allocated at or above `FIRST_NORMAL_XID = 3`. The `Checkpoint` marker reuses the per-record `txn_id` header field to carry the transaction-id high-water mark at checkpoint time, so the allocator boundary survives WAL truncation (preventing id reuse after a truncating checkpoint + restart); the CLOG additionally treats unrecorded normal ids below the truncation floor as committed (see §5.4 of `docs/specs/mvcc.md`).

### WAL Trait

```rust
pub trait WalManager: Send + Sync {
    /// Append a record to the WAL buffer (not yet durable).
    fn append(&self, record: WalRecord) -> Result<Lsn>;

    /// Flush all buffered WAL records to disk (fsync). Returns the flushed LSN.
    fn flush(&self) -> Result<Lsn>;

    /// Iterate records from a given LSN (for recovery, redo-all). The iterator is
    /// fallible — a corrupt record mid-replay returns an error.
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;

    /// Truncate WAL records before the given LSN (after checkpoint). Unconditional:
    /// drops every record with `record.lsn < lsn`; the caller must persist the CLOG
    /// snapshot covering `lsn` first (see below / §5.4).
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;

    /// Last LSN known to be durable after fsync.
    fn flushed_lsn(&self) -> Lsn;

    /// Total encoded bytes of retained records whose stored LSN is > lsn.
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;

    /// Persist the durable CLOG snapshot (`clog.dat`) through `clog_lsn`; the
    /// checkpoint calls this after the control record and before `truncate_before`.
    fn persist_clog(&self, clog_lsn: Lsn) -> Result<()>;
    /// Advance the vacuum floor (Milestone F4c); bounds `clog.dat` pruning.
    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()>;
    /// Establish the CLOG implicit-committed floor at recovery (no-op when a durable
    /// `clog.dat` snapshot was loaded; conservative re-derivation otherwise).
    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()>;
}
// `WalManager: TxnStatusView`, so `status`/`is_committed`/`is_aborted` come from the
// CLOG (seeded from `clog.dat`, else rebuilt from `Commit`/`Abort`). The
// redo-committed-only `replay_committed_from` is retired with MVCC.
```

`append(record)` always assigns the next monotonically increasing LSN and writes that LSN into the encoded record. Callers may pass `record.lsn = 0`; `append` ignores the caller-provided LSN. Replay preserves the stored LSN from disk.

`replay_from(lsn)` is strictly exclusive: it inspects only records whose stored `record.lsn > lsn`. Recovery passes the control record `checkpoint_lsn`, so replay starts after the last record whose effects are already reflected in the heap, and (redo-all) applies every page-mutation record, deciding visibility via the CLOG.

`truncate_before(lsn)` may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`. **Unconditional truncation (MVCC):** it drops every record below `lsn`, including aborted transactions' records. It is safe because the checkpoint calls `persist_clog` — which durably records every aborted outcome in the CLOG snapshot `clog.dat` — *before* truncating, and under the exclusive checkpoint guard no write transaction is in flight (`docs/specs/mvcc.md` §5.4/§8). Truncation writes retained records to a temporary WAL, fsyncs it, renames it over the live WAL, and immediately fsyncs the parent directory. If that directory fsync fails, the WAL manager is poisoned and returns the error before reopening the WAL or mutating retained-record in-memory state.

`bytes_after(lsn)` is server checkpoint accounting only. It counts encoded bytes for retained WAL records with stored `LSN > lsn`; if `lsn` predates the retained WAL after truncation, it returns the encoded byte size of all retained records.

### Durability Rules

One rule ensures redo recovery is correct:

**In-place page flushing.** Dirty pages are written back to their heap file by the checkpoint and by eviction-steal. Each write is protected by the page's redo records — a full-page image on the first modification since the last checkpoint, deltas thereafter — so a torn write is repairable during redo. With MVCC, uncommitted/aborted dirty pages may also be flushed (the flush gate requires only WAL-durability); they are hidden by the CLOG and reclaimed by VACUUM, and the steal path forces the WAL before writing a possibly-uncommitted page.

The WAL is the source of durability between checkpoints:
- On commit, the WAL is flushed through the commit record (`fsync`). The data is durable in the WAL even though the dirty heap pages may still be in memory.
- The buffer pool holds modified pages in memory until the next checkpoint flushes them to the heap.
- Each heap page is recoverable from the last checkpoint plus the redo records after it.

This gives a clean invariant: **after a crash, PageLSN-gated redo-all (with full-page images) restores every heap page to its post-boundary on-disk state, and the CLOG (rebuilt from `Commit`/`Abort`) decides which versions are visible.**

### Write Protocol

Data writes, DDL, and WAL-writing maintenance coordinate through the `ConcurrencyController`'s **shared** writer guard, so unrelated operations can run concurrently while checkpoint drains all of them. DML write-write safety comes from table/row-conflict coordination plus per-index and per-heap structural latches (lock order: table → structural → frame → WAL). DDL is transaction-scoped and uses catalog overlays plus schema/name locks so uncommitted catalog changes remain private and commit publication cannot race another catalog change. The protocol for a single autocommit DML statement:

1. Bind/preflight without mutation to discover table ids.
2. Assign and register `txn_id`, acquire the shared writer guard through the cancelable timed-poll form, then acquire xid-owned table locks in ascending id order and revalidate. Cancellation returns the token's reason-specific error.
3. Capture the statement snapshot and execute through storage.
4. If execution fails: append an `Abort` record (which records the txn `Aborted` in the in-memory CLOG; not fsynced) and only then deregister it from the active-transaction registry, then `storage.rollback_txn(txn_id)` (DDL-metadata restore, deletion of unpublished truncate replacement files, and retired-generation protection for rollback-removed published generations), `buffer_pool.rollback(txn_id)` (bookkeeping clear; no page undo), and catalog restore only for catalog-mutating DDL; return error to client and drop the statement guard if cleanup succeeds. Abort is **status-based** with MVCC (`docs/specs/mvcc.md` §4 Decision 3): the failed statement's heap versions stay in place, hidden by the CLOG (`Aborted`) and reclaimed by VACUUM — there is no before-image page undo. If the Abort append fails before the commit record is durable, log the failure, attempt to flush WAL, and exit without deregistering the transaction. If post-abort cleanup fails, normal query paths also exit fatally rather than returning with uncertain DDL metadata; direct internal callers surface the cleanup error for tests.
5. Check the cancellation token at the last safe pre-durable boundary, then append a `Commit` record for this `txn_id`. Cancellation before this boundary follows the abort path; after the durable flush begins, cleanup and success reporting remain authoritative.
6. Flush WAL through the commit record to disk (`fsync`)
7. The statement is now durable and must not be rolled back or reported as a normal SQL failure
8. `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)` — cleanup-only (the buffer pool tracks no rollback metadata under status-based abort); deregister the txn from the active-transaction registry (its CLOG status is already `Committed`, set when the WAL flush made the `Commit` durable)
9. Drop the shared writer guard
10. Call best-effort `record_commit_and_maybe_checkpoint(&components)`; it may acquire the exclusive checkpoint guard for a checkpoint
11. Return success to the client once the commit is durable and cleanup has completed. If this post-commit checkpoint step fails, log the checkpoint failure and leave the transaction committed; the server must not roll it back or report a normal SQL error for a transaction that already committed.

`storage.commit_txn` and `buffer_pool.commit` are cleanup-only in-memory operations and must not perform I/O. For a valid `txn_id`, they should not fail. If either returns an error after WAL flush through the `Commit` record succeeded, the server must not call rollback. It logs the fatal internal error, flushes WAL, and terminates because recovery will replay the durable commit.

Autocommit reads take no `ConcurrencyController` guard and proceed concurrently
with writers. An explicit transaction takes the shared side before its first
retained object lock so a later write cannot invert checkpoint/object order. Reads
take `AccessShare`, so relation-changing `AccessExclusive` holders block them.

### Failed Statement Rollback

If a write statement errors after mutating pages but before commit (e.g., a constraint violation mid-batch INSERT, or an internal error after allocating a page), dirty pages from that `txn_id` are not physically undone under MVCC. They must instead be made invisible by recording `CLOG[txn_id] = Aborted` before the transaction is deregistered, and any fresh unreachable page without a redo record must not be left dirty for checkpoint/steal to flush.

**Policy (MVCC): status-based abort, no page undo.**

With MVCC (Milestone D1, `docs/specs/mvcc.md` §4 Decision 3) abort is purely status-based: a transaction's heap/index page mutations are **not** undone. A rolled-back transaction's versions stay in the heap, hidden by the CLOG (`Aborted`) and reclaimed by VACUUM (Milestone F). This replaces the pre-MVCC before-image rollback (the buffer pool no longer saves or restores before-images), which could not un-flush a page the relaxed flush gate may already have evicted and is incompatible with concurrent writers (Milestone E).

**Success path:**
1. `write_page(file, page, txn_id)` — marks the page dirty, returns write guard
2. ... (statement executes, modifying pages) ...
3. Append `Commit` record, flush WAL (sets CLOG → `Committed`)
4. `storage.commit_txn(txn_id)` / `buffer_pool.commit(txn_id)` — no-op cleanup

**Failure path:**
1. `write_page(file, page, txn_id)` — marks the page dirty
2. ... (statement fails mid-execution) ...
3. Append an `Abort` record (CLOG → `Aborted`; not fsynced) and deregister the txn from the active-transaction registry only after that append succeeds
4. `storage.rollback_txn(txn_id)` — restores engine-owned DDL metadata (table/index/sequence schema shadow state), may delete unpublished truncate replacement files, and retires rollback-removed published generations until relation snapshots drain; it does NOT undo heap/index page content or roll back sequence value advances
5. `buffer_pool.rollback(txn_id)` — no-op bookkeeping clear (the dirty pages stay, hidden by the CLOG)
6. Catalog restore returns DDL metadata to the pre-statement state when catalog state changed
7. WAL records for this `txn_id` remain but have no `Commit` — recovery replays them (redo-all) and the CLOG hides them
8. Error returned to client

If any cleanup step fails before the commit record is durable, the server treats process state as unsafe: it logs the failure, attempts to flush WAL, and exits instead of returning to service.

**Why no undo:** an aborted version is hidden by the CLOG check the visibility predicate already performs — the same mechanism snapshot isolation needs — so undoing the page would be redundant work, and (post-D1) impossible once the page has been stolen to disk. VACUUM reclaims the space later.

### Checkpoint

The checkpoint flushes dirty pages in place to the heap and advances the redo boundary. Cost is O(pages changed), not O(database size). The previous control record stays valid until the new one is committed.

**Checkpoint protocol:**

1. Acquire the exclusive checkpoint guard (`begin_checkpoint`), which drains in-flight writers and explicit transactions that retained the shared checkpoint-participant guard; autocommit readers remain controller-guard-free.
2. `wal.flush()` — a page's redo must be durable before the page is written.
3. `buffer_pool.flush_dirty_pages()` — write flushable dirty pages to the heap `PageStore` (committed, aborted, and — under Stage 2 — in-flight alike; all WAL-durable after step 2, and the CLOG hides the non-committed tuples).
4. `store.sync_all()` — fsync the heap before advancing the redo boundary.
5. `checkpoint_lsn = wal.flushed_lsn()`.
6. `control.store(checkpoint_lsn, sorted_table_ids, catalog_bytes)` — the durable commit point (atomic temp + fsync + rename + directory fsync).
6b. `wal.persist_clog(checkpoint_lsn)` — write the durable CLOG snapshot `clog.dat` (every transaction outcome plus both floors) before truncating, so it remembers every outcome the truncation drops (`mvcc.md` §5.4).
7. Append `WalRecord { txn_id: <txn-id high-water>, kind: Checkpoint { redo_lsn: checkpoint_lsn } }`, flush WAL, then `truncate_before(checkpoint_lsn)` (unconditional — `persist_clog` ran in step 6b, so every dropped outcome is durable in `clog.dat`; `mvcc.md` §5.4).
8. `buffer_pool.mark_all_clean()` — clears dirty flags and re-arms full-page-image protection.
9. Attempt relation-generation cleanup: remove unreferenced truncate/drop-retired generations and untracked orphan files only after buffer pin/transition checks. Dropped metadata is not live-file protection once commit has queued the retired generation.
10. Drop write guard.

**Crash safety analysis:** the ordering is heap fsync (4) → control record (6) → WAL truncation (7).
- Crash before step 6: the control record is unchanged; recovery falls back to the previous `checkpoint_lsn`, and this cycle's full-page images (logged since that boundary) repair any torn heap write.
- Crash between steps 6 and 7: the new control record is durable and the heap is consistent; the un-truncated WAL tail replays idempotently under PageLSN gating.
- Crash after step 7: consistent.

**Checkpoint frequency:** Triggered by configurable thresholds — every N committed statements or M bytes of WAL. `CheckpointState.last_checkpoint_lsn` starts from the loaded manifest checkpoint LSN, and `CheckpointState.commits_since_checkpoint` starts at `0`. After each successful write statement and after its statement guard is dropped, server calls best-effort `record_commit_and_maybe_checkpoint(&components)`, which increments the commit counter and triggers `run_checkpoint(&components)` when `commits_since_checkpoint >= config.checkpoint_every_n_commits` or `wal.bytes_after(last_checkpoint_lsn)? >= config.checkpoint_wal_bytes`. A successful checkpoint stores the new checkpoint LSN and resets the commit counter to `0`. If a post-commit checkpoint attempt fails, the failure is logged and the non-reset counter lets a later write retry. Checkpoint is also triggered on clean shutdown. More frequent checkpoints mean shorter WAL replay on startup but more I/O.

### Crash Recovery (REDO)

The control record names the redo boundary and the catalog. Recovery loads the heap as of that boundary and replays every redo record on top (redo-all); the CLOG, rebuilt from `Commit`/`Abort`, then decides which versions are visible. DDL records install catalog/storage objects only for committed transactions; skipped aborted/in-flight create/truncate/schema-rewrite records still reserve their table/index/sequence/storage IDs or carried rewrite storage IDs so orphan page files, relation-generation files, or catalog IDs are not reused.

**Recovery uses physiological page redo plus a DDL replay trait** so replayed operations do not re-append to the WAL:

```rust
pub trait RecoveryOperations: Send + Sync {
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, table: TableId) -> Result<()>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, index: IndexId) -> Result<()>;
    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()>;
    fn apply_drop_sequence(&self, sequence: SequenceId) -> Result<()>;
    fn apply_sequence_advance(&self, sequence: SequenceId, value: i64) -> Result<()>;
    fn apply_set_sequence_value(&self, sequence: SequenceId, value: i64, is_called: bool) -> Result<()>;
    fn apply_set_table_compression(&self, schema: TableSchema) -> Result<()>;
    fn apply_set_table_toast_metadata(&self, schema: TableSchema) -> Result<()>;
    fn apply_truncate_table(&self, update: TruncateCatalogUpdate) -> Result<()>;
}
```

Row recovery is `storage::apply_physical_redo(page, lsn, kind)`, gated by the page-LSN. Table/index/sequence DDL, table metadata ALTER records, and committed relation-swap truncate records replay through `RecoveryOperations` when committed. Sequence value records replay through `RecoveryOperations` unconditionally because sequence advancement is non-transactional. Recovery replay must not append WAL.

Concrete storage is opened with:

```rust
impl PageBackedStorageEngine {
    pub fn open(
        buffer_pool: Arc<dyn BufferPool>,
        wal: Arc<dyn WalManager>,
        mode: StorageMode,
    ) -> Result<Self>;
}
```

`open` stores shared `Arc` handles to the buffer pool and WAL manager and initializes empty table/index/sequence metadata plus empty TOAST value-id allocator state. It does not read schemas from disk; server startup installs catalog schemas explicitly with `install_schemas`, `install_index_schemas`, and `install_sequences` after loading the catalog snapshot. In normal mode, `install_schemas` seeds each hidden TOAST relation's in-memory `value_id` allocator by physically scanning chunk rows and setting the next id to `1 + max(value_id)` across committed, aborted, and in-flight tuples. In recovery mode, TOAST allocator seeding is deferred until the recovery-to-normal transition so physical redo after schema install cannot leave a stale cached next id, and maintenance cannot prune redone rows before the allocator has recorded their high-water mark.

**Recovery procedure** (driven by the server startup sequence):

1. `control.load()` — the redo boundary `checkpoint_lsn` and catalog bytes. If none: fresh database.
2. Initialize storage in recovery mode and the catalog; install table, index, and sequence schemas from the catalog snapshot.
3. Enable eviction-flush-on-steal (`buffer.enable_stealing()`) so redo may spill — the durable index means nothing is rebuilt in memory, so the recovery working set is not bounded by the pool.
4. Redo-all: replay every record with `LSN > checkpoint_lsn` (`WalManager::replay_from`): physical-redo records via `apply_physical_redo` (PageLSN-gated; torn/missing pages are zeroed so a `FullPageImage`/`HeapInit` rebuilds them) — heap and index pages alike, regardless of transaction outcome — committed table/index/sequence/view DDL, committed table metadata ALTER records, committed `UpdateTableSchema`, and committed `TruncateTable` relation swaps through catalog plus `RecoveryOperations`; allocator-only reservation for skipped aborted/in-flight `CreateTable` / `CreateView` / `CreateIndex` / `CreateSequence` / `CreateDictionary` / `TruncateTable` IDs and skipped `UpdateTableSchema` rewrite storage IDs; and unconditional sequence value records through `RecoveryOperations`. `AlterTablePrimaryKey` installs metadata during replay and records the table for a deferred identity-tree rebuild after replay and crashed-writer abort resolution. The CLOG (rebuilt from `Commit`/`Abort`) decides tuple visibility; aborted/in-flight versions are invisible.
5. If records were replayed: checkpoint to persist the redone state and advance the boundary.
6. Attempt relation-generation cleanup while no user readers exist.
7. Switch to normal mode with `storage.set_mode(StorageMode::Normal)`.

**Idempotency:** PageLSN gating applies each record's effect at most once, so replay is safe even when the heap already reflects some post-boundary work (e.g. a partially completed prior checkpoint).

### File

`data/wal.dat` — single file, append-only. Old segments before the last completed checkpoint can be truncated.

## 11. Catalog

The `catalog` crate manages metadata about all database objects.

### Data Structures

```rust
pub struct Catalog {
    tables_by_name: HashMap<String, TableId>,
    tables_by_id: HashMap<TableId, TableSchema>,
    views_by_name: HashMap<String, TableId>,
    views_by_id: HashMap<TableId, ViewSchema>,
    next_table_id: TableId,
    indexes_by_name: HashMap<String, IndexId>,
    indexes_by_id: HashMap<IndexId, IndexSchema>,
    next_index_id: IndexId,
    sequences_by_name: HashMap<String, SequenceId>,
    sequences_by_id: HashMap<SequenceId, SequenceSchema>,
    next_sequence_id: SequenceId,
}

pub struct TableSchema {
    pub id: TableId,
    pub storage_id: FileId,
    pub name: String,
    pub columns: Vec<ColumnDef>,       // ColumnDef with assigned IDs
    pub primary_key: Vec<ColumnId>,
    pub compression: CompressionSetting,
    pub active_dict_id: Option<u32>,
    pub toast: ToastOptions,
    pub toast_table_id: Option<TableId>,
    pub relation_kind: RelationKind,
    pub schema_version: u64,
}

pub struct ViewDependency {
    pub relation: TableId,
    pub columns: Vec<ColumnId>,
    pub all_columns: bool,
}
```

`ColumnDef`, `DataType`, `ToastOptions`, relation/index/view/sequence schemas, and their input forms are defined in `common`. Column IDs are dense row slots within a schema version; rewrite DDL may renumber them and must remap surviving index/view metadata before publication. Logical table/index IDs remain stable while `storage_id` identifies the current physical generation and changes on TRUNCATE or rewrite. Public schema changes increment `schema_version`, which prepared execution revalidates after table-lock acquisition. Tables and views share the public relation-name namespace; hidden TOAST relations are stored only by ID and are never user-resolvable. ADD/DROP column preflight is read-only and repeated after the server holds the catalog publication gate plus target `AccessExclusive`, because an earlier conditional no-op decision may race another DDL. Catalog loading validates identifiers, storage-id uniqueness, TOAST bounds/cross-links, view dependencies, and table/view namespace collisions; unchecked snapshot installation remains crate-private.

`create_table_with_options` assigns column IDs, stores resolved TOAST/CHECK
metadata, and creates hidden TOAST metadata for tables with `TEXT`/`BYTEA`;
adding the first toastable column does the same when policy requires it. Hidden
relations are named `"\0toast_<base_table_id>"`, use `(value_id BIGINT, seq
INTEGER, data BYTEA)` with primary key `(value_id, seq)`, disable page compression,
and have distinct storage ids. Legacy snapshots use
`ToastOptions::legacy_catalog_default()` and default missing relation kind to
`User`. Validation enforces TOAST bounds, nonzero dictionary/storage ids,
cross-links, and duplicate storage-id rules while preserving the legacy raw
table/index collision allowed by file-kind bits. Older view dependencies with no
`all_columns` field and no columns retain `all_columns = true` compatibility.

The catalog is the authority for name-to-ID resolution. Table IDs, catalog-index IDs, and sequence IDs are stable and never reused (monotonically increasing in independent namespaces; index id `0` is reserved for storage's per-table identity index). User tables, user views, user-visible indexes, public sequences, and primary-key auto-names share the public relation-name namespace exposed through `pg_class`/`to_regclass`; duplicate names across those kinds are rejected. Rollback `restore` reinstalls a previous object map but preserves the current allocator high-water marks so a failed DDL cannot cause later objects to reuse table/index IDs whose storage pages may still exist as aborted artifacts, or sequence IDs observed in WAL. The binder resolves ordinary table/index/column names to IDs so that the planner, executor, and storage engine work with stable relation/index IDs plus schema-version-local column IDs; `DROP TABLE IF EXISTS` and `DROP SEQUENCE` resolve by name at execution time to preserve extended-protocol prepared-statement semantics, `CREATE TABLE IF NOT EXISTS` makes its duplicate-table no-op decision at execution time, and `CREATE TABLE ... SERIAL` chooses its owned sequence names at execution time to avoid stale prepared-plan collision checks.

The catalog crate also owns a static virtual system-view registry for the
driver-oriented `pg_catalog` and `information_schema` surface. The registry
describes virtual schemas, relation names, column descriptors, and deterministic
32-bit-compatible OID derivation, but it is not serialized in `CatalogSnapshot`,
WAL, manifests, or heap storage.

### Catalog Trait

```rust
pub trait CatalogManager: Send + Sync {
    /// Resolve a table name to its schema (used by the binder)
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>>;

    /// Get schema by ID (used by executor/storage)
    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>>;

    /// List all tables
    fn list_tables(&self) -> Result<Vec<TableSchema>>;
    fn get_view_by_name(&self, name: &str) -> Result<Option<ViewSchema>>;
    fn get_view(&self, id: TableId) -> Result<Option<ViewSchema>>;
    fn list_views(&self) -> Result<Vec<ViewSchema>>;

    fn snapshot(&self) -> Result<CatalogSnapshot>;
    fn restore(&self, snapshot: CatalogSnapshot) -> Result<()>;
    fn reserve_table_id(&self, id: TableId) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_table_schema(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_table_and_index_schemas(
        &self,
        schema: TableSchema,
        indexes: &[IndexSchema],
    ) -> Result<()>;
    fn apply_drop_table(&self, id: TableId) -> Result<()>;

    /// Register a new table. Accepts parsed columns (no IDs), assigns
    /// TableId and ColumnIds, returns the completed TableSchema.
    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        compression: CompressionSetting,
    ) -> Result<TableSchema>;
    fn create_table_with_options(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        compression: CompressionSetting,
        toast: ToastOptions,
        checks: Vec<String>,
    ) -> Result<TableSchema>;

    /// Remove a table
    fn drop_table(&self, id: TableId) -> Result<()>;
    fn rename_table(&self, id: TableId, new_name: String) -> Result<TableSchema>;
    fn preflight_add_table_column(
        &self,
        id: TableId,
        if_not_exists: bool,
        column: &ParsedColumnDef,
    ) -> Result<TableColumnAlteration>;
    fn add_table_column(&self, id: TableId, column: ParsedColumnDef) -> Result<TableSchema>;
    fn preflight_drop_table_column(
        &self,
        id: TableId,
        if_exists: bool,
        column: &str,
    ) -> Result<TableColumnAlteration>;
    fn drop_table_column(&self, id: TableId, column: &str) -> Result<TableSchema>;
    fn rename_table_column(
        &self,
        id: TableId,
        old_name: &str,
        new_name: String,
    ) -> Result<TableSchema>;

    fn set_table_compression(
        &self,
        table: TableId,
        compression: CompressionSetting,
        active_dict_id: Option<u32>,
    ) -> Result<TableSchema>;
    fn set_table_toast_metadata(
        &self,
        table: TableId,
        toast: ToastOptions,
        toast_table_id: Option<TableId>,
    ) -> Result<TableSchema>;
    fn set_table_primary_key(&self, table: TableId, primary_key: Vec<ColumnId>)
        -> Result<TableSchema>;
    fn add_table_primary_key_index(
        &self,
        table: TableId,
        primary_key: Vec<ColumnId>,
        index: IndexSchema,
    ) -> Result<TableSchema>;
    fn drop_table_primary_key_index(&self, table: TableId, index: IndexId) -> Result<TableSchema>;
    fn allocate_dictionary_id(&self) -> Result<u32>;
    fn reserve_dictionary_id(&self, id: u32) -> Result<()>;
    fn allocate_storage_id(&self) -> Result<FileId>;
    fn reserve_storage_id(&self, id: FileId) -> Result<()>;
    fn prepare_truncate_table(&self, table: TableId) -> Result<TruncateTablePlan>;
    fn build_truncate_table_update(
        &self,
        plan: &TruncateTablePlan,
    ) -> Result<TruncateCatalogUpdate>;
    fn apply_truncate_table(&self, plan: &TruncateTablePlan) -> Result<TruncateCatalogUpdate>;
    fn apply_truncate_tables(
        &self,
        plans: &[TruncateTablePlan],
    ) -> Result<Vec<TruncateCatalogUpdate>>;
    fn apply_truncate_updates(&self, updates: &[TruncateCatalogUpdate]) -> Result<()>;

    fn get_index_by_name(&self, name: &str) -> Result<Option<IndexSchema>>;
    fn get_index(&self, id: IndexId) -> Result<Option<IndexSchema>>;
    fn list_indexes_for_table(&self, table: TableId) -> Result<Vec<IndexSchema>>;
    fn reserve_index_id(&self, id: IndexId) -> Result<()>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_update_index_schema(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, id: IndexId) -> Result<()>;
    fn create_index(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<IndexSchema>;
    fn create_index_with_constraint(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
        constraint: IndexConstraintKind,
    ) -> Result<IndexSchema>;
    fn drop_index(&self, id: IndexId) -> Result<()>;

    fn get_sequence_by_name(&self, name: &str) -> Result<Option<SequenceSchema>>;
    fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceSchema>>;
    fn list_sequences(&self) -> Result<Vec<SequenceSchema>>;
    fn reserve_sequence_id(&self, id: SequenceId) -> Result<()>;
    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()>;
    fn apply_drop_sequence(&self, id: SequenceId) -> Result<()>;
    fn create_sequence(
        &self,
        name: String,
        options: SequenceOptions,
        owned: bool,
    ) -> Result<SequenceSchema>;
    fn drop_sequence(&self, id: SequenceId) -> Result<()>;
    fn apply_create_view(&self, schema: ViewSchema) -> Result<()>;
    fn apply_replace_view(&self, schema: ViewSchema) -> Result<()>;
    fn apply_drop_view(&self, id: TableId) -> Result<()>;
    fn create_view(
        &self,
        name: String,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema>;
    fn replace_view(
        &self,
        id: TableId,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema>;
    fn drop_view(&self, id: TableId) -> Result<()>;
}
```

```rust
pub struct CatalogSnapshot {
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub views_by_name: HashMap<String, TableId>,
    pub views_by_id: HashMap<TableId, ViewSchema>,
    pub next_table_id: TableId,
    pub indexes_by_name: HashMap<String, IndexId>,
    pub indexes_by_id: HashMap<IndexId, IndexSchema>,
    pub next_index_id: IndexId,
    pub sequences_by_name: HashMap<String, SequenceId>,
    pub sequences_by_id: HashMap<SequenceId, SequenceSchema>,
    pub next_sequence_id: SequenceId,
    pub next_dictionary_id: u32,
    pub next_storage_id: FileId,
}
```

Empty catalogs start with `next_table_id = 1`, `next_index_id = 1`, `next_sequence_id = 1`, and `next_storage_id = 1`. `apply_create_table` and `apply_drop_table` are recovery-only APIs. `apply_create_table` inserts a fully assigned historical schema without changing logical IDs and advances `next_table_id` and `next_storage_id` past that schema; user tables enter the table name map, while hidden TOAST relations are installed by ID only. `reserve_table_id` and `reserve_storage_id` advance allocators without installing schemas; `apply_drop_table` removes by ID without assigning IDs and cascades a user-table drop to its linked hidden TOAST relation metadata. `apply_create_index`/`apply_update_index_schema`/`apply_drop_index` do the same for catalog indexes, and `reserve_index_id` advances `next_index_id` past a skipped historical index ID without installing an index schema. Normal `CREATE INDEX` may make the catalog index visible before the storage tree is published, but storage publishes the new `IndexGeneration` only after the empty tree is created and backfill succeeds; a scan defensively falls back to a table scan if its statement-captured relation generation lacks the planned index. `prepare_truncate_table` burns fresh storage ids for the base table, optional hidden TOAST table, and catalog indexes without publishing them; `build_truncate_table_update` returns the updated schemas without mutating the catalog so storage can prepare empty files before commit; `apply_truncate_table` revalidates and swaps only `storage_id` fields after durable commit. `preflight_add_table_column` and `preflight_drop_table_column` validate schema-evolution changes without mutating state so the server can avoid snapshot fencing for harmless conditional statements. ADD/DROP column rewrites use `add_table_column`/`drop_table_column` to allocate fresh `storage_id`s as part of the logical schema change, then storage publishes the matching `UpdateTableSchema` record. Renames are metadata-only and keep existing storage ids. `apply_update_table_and_index_schemas` is the recovery path for committed rewrite DDL and validates the replayed table schema together with its carried index schemas before publishing either, so remapped primary-key constraint indexes are checked against the replayed table metadata. Primary-key catalog helpers atomically add or drop the table's primary-key metadata together with the backing primary-key constraint index. `create_view` validates the shared table/view name namespace, assigns a relation ID from `next_table_id`, and stores dependency metadata; `CREATE OR REPLACE VIEW` preserves the relation ID and increments `schema_version`; `drop_view` removes only view metadata. `create_sequence` validates and normalizes options, assigns a `SequenceId`, stores a `SequenceSchema`, and returns it; `apply_create_sequence`/`apply_drop_sequence` are the matching recovery-only APIs, and `reserve_sequence_id` advances `next_sequence_id` past a skipped historical sequence ID without installing a schema. Recovery uses the reserve methods for aborted/in-flight `CreateTable` / `CreateIndex` / `CreateSequence` / `TruncateTable` / `UpdateTableSchema` WAL records so their IDs and rewrite storage IDs are not reused while physical page records, relation-generation files, or logical sequence IDs may have been observed in WAL.

For multi-table TRUNCATE, `apply_truncate_tables` validates every prepared plan
against one catalog state, rejects any replacement storage id reused across the
batch, and publishes the complete set of storage-id swaps under one catalog
write lock. Storage performs the same global collision check before one-lock
batch publication. The single-plan apply method remains the recovery path for
each existing logical WAL record. Transactional top-level commit instead calls
`apply_truncate_updates` with its prebuilt overlay batch; the method reconstructs
and validates the equivalent plans against one catalog state, reserves all carried
storage ids, and atomically publishes every base/TOAST/index schema or none.
The transaction-local read view stores only those replacement schemas and delegates
unrelated lookups to the live catalog rather than cloning the complete catalog for
each statement.

### Persistence

The catalog is stored in the control record (`data/manifest.dat`) at each checkpoint. Loaded into memory on startup. All reads from the in-memory copy. Mutations update memory; persistence happens at the next checkpoint. Between checkpoints, the WAL ensures catalog changes (CREATE/DROP TABLE, TRUNCATE, schema-evolution ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP SEQUENCE, CREATE/REPLACE/DROP VIEW, dictionaries, and table metadata ALTERs) are durable.

### WAL Integration

`CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, `CREATE SEQUENCE`, `DROP SEQUENCE`, `CREATE/REPLACE/DROP VIEW`, relation-swap `TRUNCATE`, schema-evolution `UpdateTableSchema`, and table metadata ALTERs including `AlterTablePrimaryKey`, `CreateDictionary`, `AlterTableCompression`, and `AlterTableToast` are logged to the WAL. Conditional table DDL no-ops do not emit logical DDL records. On crash recovery, the catalog is loaded from the control record and updated by replaying committed table/index/sequence/view/dictionary/table-metadata records, committed `TruncateTable`, and committed `UpdateTableSchema`. Aborted/in-flight create, truncate, and schema-rewrite records are not installed, but their IDs or carried storage IDs are reserved so later objects do not reuse file names, storage IDs, or catalog IDs whose orphan records may have been replayed.

### Concurrency

Wrapped in `RwLock`. Reads take a read lock. DDL takes a write lock. DDL is infrequent so this is not a bottleneck.

## 12. Server & Connection Management

The `server` crate is the binary entry point.

### Startup Sequence

1. Load configuration (data directory, port, buffer pool size)
2. Initialize the control store (`FileControlStore`) and heap page store (`HeapPageStore` over `data/heap`)
3. Initialize WAL — open or create `data/wal.dat`
4. Initialize buffer pool with configured frames, the `WalFlushPolicy`, and the heap page store
5. Load the control record (`control.load()`): the redo boundary `checkpoint_lsn` and catalog bytes (none if absent)
6. Initialize storage engine in **recovery mode** with `PageBackedStorageEngine::open(buffer_pool.clone(), wal.clone(), StorageMode::Recovery)`
7. Initialize catalog from the control catalog bytes (or empty); install table, index, and sequence schemas into storage from the catalog snapshot
8. Enable eviction-flush-on-steal (`buffer.enable_stealing()`); the durable index means redo rebuilds nothing in memory and may spill
9. Redo-all: replay every record with `LSN > checkpoint_lsn` (`WalManager::replay_from`): physical-redo via `storage::apply_physical_redo` (PageLSN-gated; torn/missing pages zeroed so a `FullPageImage`/`HeapInit` rebuilds them), heap and index pages alike regardless of transaction outcome, committed table/index/sequence/view DDL, committed table metadata ALTER records, committed `UpdateTableSchema`, and committed relation-swap truncate records through catalog plus `RecoveryOperations`, allocator-only reservation for skipped aborted/in-flight `CreateTable` / `CreateView` / `CreateIndex` / `CreateSequence` / `CreateDictionary` / `TruncateTable` IDs and skipped `UpdateTableSchema` rewrite storage IDs, and unconditional sequence value records through `RecoveryOperations`; primary-key ALTERs defer the derived identity-tree rebuild until after replay and crashed-writer abort resolution — the CLOG decides tuple visibility; no WAL appended in recovery mode
10. Build `ServerComponents` with catalog, storage, buffer pool, WAL, control store, heap store, concurrency controller, shutdown state, checkpoint state initialized from the control `checkpoint_lsn`, and `next_txn_id` initialized from the allocator scan over all retained WAL records (`replay_from(0)`, including committed subxids and the `Checkpoint` marker high-water).
11. If records were replayed: `run_checkpoint(&components)` to persist the redone state to the heap and index and advance the redo boundary
12. Attempt relation-generation cleanup while no user readers exist.
13. Switch storage engine to **normal mode** with `storage.set_mode(StorageMode::Normal)` (WAL appending enabled)
13. Construct `QueryService` from `components`
14. Start Tokio runtime, bind TCP listener (default port 5433)

Recovery computes `next_txn_id` by scanning all retained records from `WalManager::replay_from(0)`, including committed operations, uncommitted operations, `Commit` records, committed subxids in `CommitWithSubxids`, and the `Checkpoint` marker's high-water, while ignoring `txn_id = 0` records. Scanning all retained records, not only records after the control `checkpoint_lsn`, covers a crash after the manifest/CLOG checkpoint is durable but before the checkpoint marker is appended; after a completed truncation, the retained marker preserves the allocation boundary. `next_txn_id` starts at `max_txn_id + 1`, or `FIRST_NORMAL_XID` when no user transaction records remain. If the maximum retained user transaction ID is `u64::MAX`, startup fails with a structured WAL/internal error instead of wrapping or saturating the next transaction ID. Step 13 transitions to normal operation where `StorageEngine` methods append WAL records.

The server binary accepts `--data-dir <PATH>`, `--port <PORT>`, `--buffer-pool-frames <N>`, `--checkpoint-every-n-commits <N>`, `--checkpoint-wal-bytes <BYTES>`, `--auto-vacuum-dead-rows <N>`, `--shutdown-timeout-ms <MS>`, `--deadlock-timeout-ms <MS>`, `--tls-cert-file <PATH>`, `--tls-key-file <PATH>`, and `--help`. Defaults are `./data`, `5433`, `1024`, `100`, `67108864`, `10000`, `30000`, and `1000` milliseconds for deadlock detection. `--auto-vacuum-dead-rows` is the checkpoint auto-prune threshold (committed dead versions since the last auto-prune; a checkpoint folds in a VACUUM pass once it is reached); `0` disables auto-prune. TLS is off unless both `--tls-cert-file` and `--tls-key-file` are supplied (providing only one is an error). The server parses these flags with `std::env::args`; `--port` accepts `1..=65535`, the other numeric flags must be positive nonzero integers except `--auto-vacuum-dead-rows`, which also accepts `0` to disable auto-prune, and invalid input prints usage to stderr and exits with code `2`.

`statement_timeout` is session configuration rather than a startup flag. Its
canonical value is an integer number of milliseconds (`0` disables it), while
`SET` accepts PostgreSQL time units and `SHOW` renders an exact human-readable
unit. Regular and `LOCAL` assignments follow transaction/savepoint rollback and
commit rules. The simple protocol times a `Query` lifecycle; extended protocol
cycles time/restart on `Parse`/`Bind`/`Describe`/`Execute` and recover at `Sync`.
Expiration records `CancelReason::StatementTimeout` in the same reason-aware token
used by `CancelRequest`, returning SQLSTATE `57014`.

### Connection Handling

```
Tokio listener (async)
  └─ accept() loop
       └─ spawn async task per connection
            └─ Protocol codec decodes client messages
            └─ For Query messages:
                 └─ create bounded row channel; spawn_blocking:
                      query_service.execute_simple_streamed(sql, session_ctx, row_tx)
                      → Bind → Plan → build PlanExecutor
                      → SELECT: push row batches into the channel (backpressure);
                        other statements return an ExecutionResult
                 └─ async task: drain channel, encode + write DataRows to wire
            └─ For non-query messages: handle inline
```

The production executor crate never owns SQL strings. It executes `PhysicalPlan` values through `QueryEngine::execute`; SQL parsing, binding, planning, and statement guard acquisition are owned by the server's `QueryService`.

### Concurrency Control

All statement-level concurrency is coordinated through the `ConcurrencyController` trait (defined in `common`):

- **Read-only statements** (`SELECT`, `EXPLAIN`): bind/plan first. Autocommit reads then acquire statement-owned `AccessShare` without a controller guard. An explicit transaction first acquires/retains the shared checkpoint-participant guard, then transaction-owned `AccessShare`. Both revalidate and capture the statement relation snapshot afterward.
- **DML statements** (`INSERT`, `UPDATE`, `DELETE`): bind to discover relations, allocate/register the autocommit xid (or use the explicit transaction's top xid), acquire the shared writer guard, then acquire xid-owned table locks and revalidate before snapshot capture. Targets use `RowExclusive`; read sources use `AccessShare`. Row and table waits share the same graph node and deadlock manager.
- **DDL statements** (`CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, sequences, views, schema-evolution `ALTER TABLE`): transaction-scoped. Explicit transactions retain their catalog overlay and schema/name/table/sequence locks through commit or rollback; autocommit uses the same lifecycle for one statement. Catalog changes become globally visible only after the transaction's commit record is durable. The publication gate makes the combined catalog snapshot visible atomically; object modes scope data/lifetime access.
- **Maintenance statements** (`VACUUM`, `TRUNCATE`, and supported maintenance ALTER): not relational. `VACUUM` and ALTER remain rejected inside explicit blocks; TRUNCATE is the exception defined in `docs/specs/table-locks.md`. Standalone maintenance uses the shared writer guard. VACUUM takes `Share`; TRUNCATE and ALTER take `AccessExclusive`. Transactional TRUNCATE retains its locks and generation undo through top-level commit/rollback.
- Shared writer guards are held for the operation lifetime. Actual checkpoint alone takes the exclusive guard and drains all page/WAL writers. `WalFlushPolicy` admits any WAL-durable page; uncommitted/aborted pages may reach the heap but remain hidden by CLOG.

The concrete `ConcurrencyController` is an `RwLock`: all page/WAL writers take it shared and `begin_checkpoint()` takes it exclusively. Foreground waits use cancelable timed-poll forms; background checkpoint uses the unconditional form. Readers take no controller guard. Table locks coordinate relation access and the catalog publication gate serializes catalog undo.

**Other latches:**
- **Buffer pool:** Frame-level read/write latches managed by page guards.
- **Catalog:** Internal `RwLock` (reads concurrent, DDL exclusive). The catalog's own lock is separate from the `ConcurrencyController` — the catalog lock protects metadata consistency, while the `ConcurrencyController` coordinates statement-level access.
- **WAL appends:** Serialized internally; the structural-latch lock order (structural → frame → WAL) keeps concurrent writers consistent.

### Graceful Shutdown

`ServerComponents` owns `shutdown: Arc<ShutdownState>`. The listener stops accepting new connections when shutdown starts, and every query execution holds an in-flight guard from before `spawn_blocking` until its response or error has been written. If shutdown has begun, starting a new query returns `ErrorKind::Internal` / `SqlState::InternalError` with message `server is shutting down`.

On SIGINT/SIGTERM:
1. Stop accepting new connections
2. Wait for in-flight queries to complete, up to `Config.shutdown_timeout_ms`
3. If all in-flight queries finish before the timeout, run checkpoint, flush WAL, close files, and exit successfully
4. If the timeout expires, skip checkpoint and skip the final WAL flush, return an internal timeout error, and let process shutdown proceed without running finalization concurrently with in-flight query execution. Successful write statements still flush their own commit records before returning.

### Configuration

```rust
pub struct Config {
    pub data_dir: PathBuf,            // default: "./data"
    pub port: u16,                    // default: 5433
    pub buffer_pool_frames: usize,    // default: 1024 (8MB)
    pub checkpoint_every_n_commits: u64, // default: 100
    pub checkpoint_wal_bytes: u64,    // default: 64 * 1024 * 1024
    pub auto_vacuum_dead_rows: u64,   // default: 10000 (0 disables auto-prune)
    pub shutdown_timeout_ms: u64,     // default: 30000
    pub deadlock_timeout_ms: u64,     // default: 1000
    pub tls_cert_file: Option<PathBuf>, // default: None (PEM cert chain)
    pub tls_key_file: Option<PathBuf>,  // default: None (PEM private key)
}
```

Loaded from command-line args only. There is no environment-variable or config-file loading.

## 13. Future Work (Designed For, Not Implemented)

- **Time-Travel / As-Of Queries:** In-heap versions make snapshot reads cheap, but there is no syntax to read as of a historical point.
- **Concurrent B-link Writer Protocol:** Index writers serialize on per-index structural latches; a fully concurrent B-link tree writer protocol and fuzzy checkpointing are future work. Row-level blocking and deadlock detection are already implemented by the server lock manager.
- **Cost-Based Optimizer:** `LogicalPlan` → `PhysicalPlan` boundary exists. A cost-based optimizer slots between them, choosing physical access methods and join algorithms without changing the executor. The current rule-based planner already chooses among primary-key identity access and catalog indexes, preferring primary-key identity access when available; a cost model would replace that heuristic.
- **Vectorized Execution:** `PlanExecutor::next_batch()` is defined with a default implementation. A vectorized engine overrides it with columnar batch processing.
- **Custom Wire Protocol:** `ProtocolCodec` and `ConnectionState` traits are protocol-agnostic. A custom protocol implements these traits.
- **Additional Data Types:** `DataType` and `Value` enums are extensible. Row serialization format supports new types via the null bitmap + column data pattern.
- **Remaining Subquery Deferrals** (`docs/specs/subqueries.md` §1.1, §12): correlated-`IN` decorrelation; correlated subqueries in join `ON`, `ORDER BY`, `DISTINCT` keys, DML assignments, `RETURNING`, and `ON CONFLICT`; outer references from set-operation arms, `VALUES` lists, and non-`LATERAL` derived-table bodies; Apply re-`open` rescans and index-aware per-row template replanning; `LATERAL` references crossing an explicit join's boundary (the Apply input is the join's own subtree); `LATERAL` on the nullable side of `RIGHT`/`FULL` joins.
