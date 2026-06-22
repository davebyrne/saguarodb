# SaguaroDB Overview Specification

**Date:** 2026-05-03
**Status:** Draft

## 1. Overview

SaguaroDB is a SQL-compatible relational database written in Rust. It is a standalone server that accepts client connections over a network, executes SQL queries against a page-oriented storage engine, and returns results over the PostgreSQL wire protocol.

### V1 Goals

- Standalone multi-client server
- PostgreSQL simple query wire protocol (abstracted for future custom protocol)
- Page-oriented storage engine with a durable on-disk non-clustered primary-key B-tree (abstracted for future MVCC and clustered/on-disk-index work)
- Autocommit only (no multi-statement transactions)
- Data types: `INTEGER` (i64), `TEXT`, `BOOLEAN`, `NULL`
- V1 SQL subset: `CREATE TABLE`, `DROP TABLE`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`, `INSERT ... VALUES`, `INSERT ... SELECT`, `SELECT` (with `WHERE`, inner/cross/left/right/full joins, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`), `UPDATE`, `DELETE`, `EXPLAIN`, transaction control (`BEGIN`/`START TRANSACTION [ISOLATION LEVEL <level>]`, `COMMIT`, `ROLLBACK`, `SET TRANSACTION ISOLATION LEVEL <level>` — Read Committed / Repeatable Read; SERIALIZABLE aliases Repeatable Read, no SSI), and the maintenance command `VACUUM [table]`; binder rejects unsupported parsed forms
- Rule-based query planner (no cost-based optimization)
- Primary-key and secondary-index access paths (full table scans otherwise)
- WAL with crash recovery
- Async networking (Tokio) with blocking thread pool for query execution

### V1 Non-Goals

- Multi-statement transactions / MVCC (designed for, not implemented)
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
│   └── common/             (shared types: DataType, Value, Row, errors, config)
```

### Dependency Flow

```
server → protocol, parser, planner, executor, control, storage, buffer, wal, catalog, common
protocol → common
parser → common
planner → parser, catalog, common
executor → planner, storage, catalog, common
storage → buffer, wal, common
control → common
buffer → common
wal → common
catalog → common
```

No circular dependencies. `common` is the leaf crate. `server` is the root.

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
/// Implements Ord for use as B-tree keys (NULL sorts first, then Bool, Integer, Text).
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),  // Future: consider Arc<str> for zero-copy from buffer pool
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

#### Data Types

```rust
/// SQL data types. Defined in common — used by parser, planner, catalog, and executor.
pub enum DataType {
    Integer,
    Text,
    Boolean,
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
}
```

`ParsedColumnDef` → catalog assigns IDs → `ColumnDef`. `ColumnInfo` is derived from `ColumnDef` for result set descriptions.

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
/// V1 implements a small subset; the enum is extensible.
pub enum SqlState {
    SuccessfulCompletion,       // 00000
    SyntaxError,                // 42601
    UndefinedTable,             // 42P01
    UndefinedColumn,            // 42703
    DuplicateTable,             // 42P07
    DatatypeMismatch,           // 42804
    DivisionByZero,             // 22012
    NumericValueOutOfRange,     // 22003
    NotNullViolation,           // 23502
    UniqueViolation,            // 23505
    QueryCanceled,              // 57014
    FeatureNotSupported,        // 0A000
    InFailedSqlTransaction,     // 25P02
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
/// Passed to every storage operation. V1 populates only txn_id (autocommit).
/// Future MVCC adds snapshot_id, isolation_level, etc. without changing the API.
pub struct StatementContext {
    pub txn_id: u64,
    // Future: snapshot_id, isolation_level, write_set, etc.
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
/// Coarse concurrency control. Server query orchestration acquires a guard
/// before executing any statement. V1 implementation: RwLock (shared for reads, exclusive for writes).
/// Future MVCC replaces the implementation while preserving this boundary.
///
/// Guards are owned types (no lifetime parameter) that hold Arc references
/// internally and release the lock on Drop. This keeps the trait object-safe
/// (usable as Box<dyn ConcurrencyController>) and avoids GAT complexity.
/// V1 uses parking_lot::ArcRwLockReadGuard / ArcRwLockWriteGuard internally.
pub trait ConcurrencyController: Send + Sync {
    fn begin_read(&self) -> Result<ReadGuard>;
    fn begin_write(&self) -> Result<WriteGuard>;
}

pub struct RwLockConcurrencyController { /* parking_lot::RwLock<()> */ }

impl RwLockConcurrencyController {
    pub fn new() -> Self;
}

/// Owned read guard. Holds an Arc to the lock internally.
/// Releases the shared lock on Drop. Send + Sync safe.
pub struct ReadGuard { /* Arc<RwLock<...>> + guard state */ }

/// Owned write guard. Holds an Arc to the lock internally.
/// Releases the exclusive lock on Drop. Send safe.
pub struct WriteGuard { /* Arc<RwLock<...>> + guard state */ }
```

**Design rationale — owned guards over GATs:** All major traits in this system are used as trait objects (`Box<dyn BufferPool>`, `Box<dyn ConcurrencyController>`, etc.). GATs (`type ReadGuard<'a> where Self: 'a`) would make these traits non-object-safe, forcing generics throughout the crate dependency graph. Owned guards with Arc internals add negligible overhead (one Arc clone per statement) and keep the trait boundaries clean. This is the standard pattern in Rust database projects.

All layers below the parser use `TableId`/`ColumnId`/`BindingId` instead of strings. The binder (phase 1 of the planner crate) resolves names to IDs and assigns physical slot positions via the catalog. The logical planner, physical planner, executor, and storage engine never do name lookups — they work exclusively with stable IDs and slot indices.

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
    ParameterDescription(Vec<i32>),
    NoData,
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

V1 materializes SELECT results inside `spawn_blocking`, returns them as an `ExecutionResult::Query`, and then the async connection task writes those rows to the socket. This keeps the first server implementation simple while preserving the executor's pull-based `PlanExecutor` boundary for a future streaming bridge.

```
Async task (Tokio)                      Blocking thread (spawn_blocking)
─────────────────                       ────────────────────────────────
1. Decode Query msg
2. Call query_service.execute_sql(sql)
                               ─────►   3. Parse → Bind → Plan
                                        4. Build PlanExecutor
                               ◄─────   5. Return ExecutionResult with
                                            columns + materialized rows
6. Send RowDescription
7. Loop over rows:
   encode DataRow
   write to TcpStream
8. Send CommandComplete
9. Send ReadyForQuery
```

**Future streaming:** A later implementation can replace materialized SELECT rows with a bounded channel of capacity 64. The producer would own `PlanExecutor` in a blocking task, and the async task would read rows from the receiver. That change does not affect the protocol crate or SQL semantics.

All v1 results are fully computed in `spawn_blocking` and returned as a complete `ExecutionResult`.

This keeps the protocol layer testable without IO and keeps blocking work off Tokio threads.

### PostgreSQL Simple Query Flow (V1 Subset)

1. **SSLRequest handling:** Many clients (psql, libpq-based drivers) send an `SSLRequest` before the real startup. The server detects this (8-byte message with code `80877103`). When TLS is configured (`--tls-cert-file`/`--tls-key-file`), it replies with a single `S` byte and performs the TLS handshake, after which the client sends its `StartupMessage` over the encrypted stream. When TLS is not configured, it replies with a single `N` byte and the client continues in plaintext (or retries with a plain `StartupMessage`). TLS is server-side only; no client certificate is requested. A `GSSENCRequest` (GSSAPI transport encryption) is likewise declined with a single `N` byte, after which the client continues with an `SSLRequest` or `StartupMessage`.
2. **Startup:** Client sends `StartupMessage` (version 3.0, user, database). Server responds `AuthenticationOk` → `ParameterStatus` (server_version, etc.) → `ReadyForQuery`.
3. **Query cycle:** Client sends `Query` (SQL string). Server responds with:
   - `RowDescription` (column names and types) for SELECT
   - `DataRow` (one per result row) for SELECT
   - `CommandComplete` (e.g., `INSERT 0 1`, `SELECT 5`)
   - `ReadyForQuery`
4. **Query error handling:** If a query fails, server sends `ErrorResponse` then `ReadyForQuery`. The connection stays open.
5. **Protocol decode error handling:** If decoding client bytes fails, server sends `ErrorResponse` then `ReadyForQuery` and closes the connection because the codec buffer state may be unrecoverable.
6. **Termination:** Client sends `Terminate`. Server closes connection.

### PostgreSQL Extended Query Flow (V1)

The extended protocol supports parameterized statements, prepared statements,
portals, and binary parameter/result encoding:

1. **Parse:** Client sends `Parse` (statement name, SQL with `$n` placeholders,
   optional parameter type OIDs). The server prepares the statement, resolving
   each parameter's type from the declared OID or by inference from context, and
   replies `ParseComplete`.
2. **Bind:** Client sends `Bind` (portal name, statement name, parameter format
   codes, parameter values, result format codes). The server decodes the values
   (text or binary) into a portal and replies `BindComplete`.
3. **Describe:** `Describe` of a statement returns `ParameterDescription` then
   `RowDescription`/`NoData`; of a portal returns `RowDescription`/`NoData` in
   the portal's result formats.
4. **Execute:** The server runs the portal and streams `DataRow`s in the
   requested result formats, then `CommandComplete`. No `RowDescription` (that
   comes from Describe) and no `ReadyForQuery` (that comes from Sync). Each
   Execute is its own autocommit unit; `max_rows` is treated as "all rows".
5. **Sync:** The server sends `ReadyForQuery`. An error earlier in the sequence
   sends `ErrorResponse` and then skips messages until `Sync`.
6. **Close/Flush:** `Close` drops a statement or portal (`CloseComplete`);
   `Flush` flushes pending output. Named and unnamed statements/portals are
   supported.

### V1 Protocol Scope — What We Skip

- Mutual TLS / client-certificate authentication (optional server-side TLS is supported; see SSLRequest handling above)
- GSSAPI transport encryption (GSSENCRequest declined with `N`)
- Authentication beyond accepting any connection
- `COPY`, `NOTIFY/LISTEN`

### PostgreSQL Wire Encoding Details

All integer fields are big-endian. All server messages except the SSL negotiation reply are one-byte tag plus a four-byte length that includes the length field but not the tag. The SSL negotiation reply is exactly a single byte: `S` for acceptance, `N` for rejection.

- Client `SSLRequest`: startup-style packet with length `8` and code `80877103`.
- Client `GSSENCRequest`: startup-style packet with length `8` and code `80877104`; declined with a single `N` byte.
- Client `Startup`: startup-style packet with protocol `196608` (3.0), nul-terminated key/value parameters, and final `\0`; V1 reads `user`, optional `database`, and optional `application_name`.
- Client `Query`: tag `Q`, length, nul-terminated SQL string.
- Client `Terminate`: tag `X`, length `4`.
- Server `AuthenticationOk`: tag `R`, length `8`, auth code `0`.
- Server `ParameterStatus`: tag `S`, `key\0value\0`; startup emits `server_version=16.0`, `server_encoding=UTF8`, `client_encoding=UTF8`, `DateStyle=ISO`, `integer_datetimes=on`, `standard_conforming_strings=on`, `TimeZone=UTC`, and `application_name` echoed from the client's startup parameters (empty when not supplied).
- Server `ReadyForQuery`: tag `Z`, length `5`, transaction-status byte sourced from the session's transaction state (`I` idle, `T` in a transaction block, `E` failed transaction block). The session is always idle in v1's autocommit model, so the byte is `I` in every interaction; the non-idle bytes arrive with transaction lifecycle support.
- Server `RowDescription`: tag `T`, field count, then for each column `name\0`, `table_oid = 0`, `attr_num = 0`, mapped type OID, type size, `type_modifier = -1`, and text `format_code = 0`.
- Server `DataRow`: tag `D`, column count, then `int32 byte_length` plus UTF-8 text bytes, or `-1` for `NULL`.
- Server `CommandComplete`: tag `C`, nul-terminated tags `SELECT n`, `INSERT 0 n`, `UPDATE n`, `DELETE n`, `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, `EXPLAIN`, or `VACUUM`.
- Server `ErrorResponse`: tag `E`, fields `S` severity, `C` SQLSTATE, `M` message, then final `\0`.

Type mapping uses PostgreSQL OIDs `INTEGER` as `INT8` (`20`, size `8`), `TEXT` (`25`, size `-1`), and `BOOLEAN` (`16`, size `1`). The simple query path always sends text: integers are decimal i64 strings, text is raw UTF-8, booleans are `t`/`f`, and null fields use length `-1`. The extended query protocol additionally supports binary parameters and results — `RowDescription` carries a per-field format code (`0` = text, `1` = binary) and `DataRow` carries the already-encoded wire bytes for that format.

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
    /// DML or DDL result (INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX)
    Modified { command: String, count: u64 },
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

The `parser` crate wraps `sqlparser-rs` (PostgreSQL dialect) and translates its AST into our own internal representation. This keeps the external dependency contained and gives us a narrow, explicit definition of exactly what SaguaroDB supports. Unsupported syntax is rejected here, not deep in the executor.

### Internal AST Types (V1)

The AST uses strings for identifiers — name resolution to IDs happens in the planner.

```rust
pub enum Statement {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { name: String },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { name: String },
    Insert { table: String, columns: Vec<String>, source: InsertSource },
    Select(SelectStatement),
    Update { table: String, assignments: Vec<Assignment>, filter: Option<Expr> },
    Delete { table: String, filter: Option<Expr> },
    Explain(Box<Statement>),
}

pub enum InsertSource {
    Values(Vec<Vec<Expr>>),       // INSERT INTO t VALUES (...)
    Query(Box<SelectStatement>),  // INSERT INTO t SELECT ...
}

pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

pub struct SelectStatement {
    pub columns: Vec<SelectItem>,
    pub from: Vec<FromItem>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

pub enum SelectItem {
    Wildcard,                                          // *
    QualifiedWildcard(String),                         // table.*
    Expression { expr: Expr, alias: Option<String> },  // expr AS alias
}

pub enum FromItem {
    Table { name: String, alias: Option<String> },
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
    ColumnRef { table: Option<String>, column: String },
    BinaryOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    Function { name: String, args: Vec<FunctionArg>, distinct: bool },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Cast { expr: Box<Expr>, data_type: DataType },
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
}

pub enum UnaryOp {
    Neg,   // -x
    Not,   // NOT x
}
```

`FromItem::Join.condition` is `None` only for `JoinType::Cross`. Inner, left, right, and full joins require an `ON` predicate. V1 rejects `USING` and `NATURAL` joins, and rejects `ON`/`USING` with `CROSS JOIN`.

Function call parsing preserves aggregate syntax: `COUNT(*)` is `Function { name: "count", args: vec![FunctionArg::Wildcard], distinct: false }`; aggregate `DISTINCT` sets `distinct = true` so binder can reject it in v1.

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

All three phases are separate modules within the `planner` crate. V1 implements all three — the physical planner is trivial (rule-based), but the boundary is real. A future cost-based optimizer replaces only `physical_plan` without touching binding or logical planning.

### Phase 1: Binder

The binder performs semantic analysis and name resolution. Its output is a `BoundStatement` — a validated, ID-resolved, slot-assigned representation of the query. No downstream phase does name lookups. The binder is the primary SQL type checker; the executor may still defensively validate runtime DML values before storage writes.

The binder:
- Resolves table names to `TableId` via the catalog
- Assigns a unique `BindingId` to each table occurrence in FROM (critical for self-joins and aliases)
- Resolves column references to `BoundExpr::InputRef` with physical slot positions
- Validates types (e.g., `WHERE` clause is boolean, arithmetic operands are numeric)
- Rejects unsupported features (e.g., composite primary keys, unknown functions)
- Expands `SELECT *` into explicit column lists

```rust
/// Fully resolved statement. All names resolved, all types checked,
/// all column references assigned physical slot positions.
pub enum BoundStatement {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    Insert { table: TableId, columns: Vec<ColumnId>, source: BoundInsertSource },
    Select(BoundSelect),
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: BoundSelect },
    Delete { table: TableId, source: BoundSelect },
    Explain(Box<BoundStatement>),
}

pub enum BoundInsertSource {
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
    Query(Box<BoundSelect>),
}

/// A fully bound SELECT — all names resolved, types checked, slots assigned.
pub struct BoundSelect {
    pub columns: Vec<BoundSelectItem>,
    pub from: BoundFrom,
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub having: Option<BoundExpr>,
    pub order_by: Vec<BoundOrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub output_schema: Vec<ColumnInfo>,
}

pub struct BoundSelectItem {
    pub expr: BoundExpr,
    pub alias: String,  // resolved name (original alias or column name)
}

pub enum BoundFrom {
    Table {
        table: TableId,
        binding: BindingId,
        alias: Option<String>,
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

### Bound Expressions

The binder resolves all string-based column references and assigns each one a physical slot position. The executor evaluates expressions by indexing directly into the row's values array — no name or ID lookups at runtime.

**Binding:** Each occurrence of a table in the FROM clause gets a unique `BindingId`. In `FROM users a JOIN users b`, `a` and `b` are different bindings of the same `TableId`. The binder tracks the mapping from `(BindingId, ColumnId)` → slot position.

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

Every `BoundExpr` variant carries its resolved output type and nullability. Binder fills these fields before logical planning, including typed `Value::Null` literals from context; if a V1 `NULL` literal has no valid typing context, binder rejects it with `SqlState::DatatypeMismatch`. For `NULL IN (...)`, binder may infer the left-side `NULL` type from the first typed list expression. The detailed metadata rules live in `docs/specs/crates/planner.md` and are authoritative for implementation.

### Phase 2: Logical Planner

Translates a `BoundStatement` into a `LogicalPlan` — relational algebra describing *what* to compute. No access method decisions.

```rust
pub enum LogicalPlan {
    // DDL — passes through to physical plan unchanged
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },

    // DML
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<LogicalPlan> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<LogicalPlan> },
    Delete { table: TableId, source: Box<LogicalPlan> },

    // Query operators
    Scan { table: TableId, filter: Option<BoundExpr> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, condition: Option<BoundExpr>, join_type: JoinType },
    Filter { source: Box<LogicalPlan>, predicate: BoundExpr },
    Projection { source: Box<LogicalPlan>, expressions: Vec<BoundExpr>, output_schema: Vec<ColumnInfo> },
    Sort { source: Box<LogicalPlan>, order_by: Vec<BoundOrderByItem> },
    Limit { source: Box<LogicalPlan>, count: u64, offset: Option<u64> },
    Aggregate {
        source: Box<LogicalPlan>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
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
}

pub struct BoundOrderByItem {
    pub expr: BoundExpr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}
```

Aggregate calls use a two-stage representation. Binder converts `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` into `BoundExpr::AggregateCall`; scalar functions remain `BoundExpr::Function`. Logical planning extracts unique aggregate calls into `AggregateExpr` values and rewrites expressions above the `Aggregate` node to `BoundExpr::LocalRef`. The `Aggregate` output row layout is group-by values first, then aggregate values, so aggregate slot `i` is read as `LocalRef { slot: group_by.len() + i, ... }`. `AggregateCall` must not reach executor scalar evaluation.

Aggregate `DISTINCT` is rejected in v1 with `ErrorKind::Plan`; `AggregateExpr.distinct` is always `false`. Aggregate return types are fixed: `COUNT` returns non-null `INTEGER`; `SUM(integer)` returns nullable `INTEGER`; `AVG(integer)` returns nullable `INTEGER` using integer division truncated toward zero; `MIN` and `MAX` return the argument type and are nullable. `SUM` and `AVG` reject non-integer arguments with `SqlState::DatatypeMismatch`. Empty aggregate inputs return `0` for `COUNT` and `NULL` for `SUM`, `AVG`, `MIN`, and `MAX`.

### Phase 3: Physical Planner

Translates a `LogicalPlan` into a `PhysicalPlan` — chooses access methods and join algorithms. V1's physical planner is trivial (rule-based), but the boundary is real from day one.

```rust
pub enum PhysicalPlan {
    // DDL
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },

    // DML
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<PhysicalPlan> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<PhysicalPlan> },
    Delete { table: TableId, source: Box<PhysicalPlan> },

    // Access methods
    SeqScan { table: TableId, table_name: String, filter: Option<BoundExpr> },
    IndexScan { table: TableId, table_name: String, index: IndexId, range: KeyRange, filter: Option<BoundExpr> },

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
    // Future: MergeSortJoin

    // Other operators
    Filter { source: Box<PhysicalPlan>, predicate: BoundExpr },
    Projection { source: Box<PhysicalPlan>, expressions: Vec<BoundExpr>, output_schema: Vec<ColumnInfo> },
    Sort { source: Box<PhysicalPlan>, order_by: Vec<BoundOrderByItem> },
    Limit { source: Box<PhysicalPlan>, count: u64, offset: Option<u64> },
    Aggregate {
        source: Box<PhysicalPlan>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
}

```

`KeyRange` is defined in `common` (see Core Types) so both the planner and storage crates can reference it without depending on each other.

The executor receives a `PhysicalPlan` and only works with `BoundExpr`. Column access is by slot index (`row.values[slot]`) — O(1), no lookups. The `BindingId` and `ColumnId` fields in `InputRef` exist only for EXPLAIN output and debugging.

`PRIMARY_KEY_INDEX_ID = 0` identifies the primary-key index; secondary indexes use their own ids. An `IndexScan` carries the chosen index id, and `IndexScan.filter` holds residual predicates not consumed by that index's range (re-checked by the scan operator, so the choice of index never changes results). For `WHERE id = 7 AND name = 'Ada'`, the scan range is `Exact(Key([7]))` on the primary key and the residual filter is `name = 'Ada'`; for `WHERE id = 7`, the residual filter is `None`. Scan plan nodes capture `table_name` at planning time solely for EXPLAIN/debug output; execution still uses `table`.

The three-phase pipeline (`bind` → `logical_plan` → `physical_plan`) means a future cost-based optimizer replaces only `physical_plan`, choosing among multiple physical alternatives per logical operator. The binder and logical planner are unchanged.

### Planner Rules (V1 — Applied in Order)

1. **Index lookup:** If `WHERE` has an equality or range comparison on the leading column of an index — the primary-key index (`index = PRIMARY_KEY_INDEX_ID`) or a secondary index (its own id) — emit `IndexScan` with that index, a `KeyRange::Exact` (equality) or `KeyRange::Range` (range) over the column, and any residual predicate in `filter`.
2. **Index choice:** When several indexes' leading columns are constrained, prefer an equality over a range, the primary key over a secondary index, then the lower index id.
3. **Predicate pushdown:** Push `WHERE` conditions as close to the scan nodes as possible.
4. **Join ordering:** Process joins left to right as written. An inner join whose `ON` predicate is a conjunction of `left_column = right_column` equalities becomes a `HashJoin` (its `left_keys`/`right_keys` are the paired key slots); every other join (outer, cross, non-equi) is a `NestedLoopJoin`. Join `condition` is `None` only for `Cross` and `Some(boolean_expr)` for every other join type.
5. **Projection pushdown:** Optional for initial v1. If implemented, only read columns that are needed downstream and rebase expression slots against each child output schema.

### EXPLAIN

`Statement::Explain` is handled by server `QueryService`, not by the executor. The server acquires a read guard, binds the inner statement to `BoundStatement::Explain(inner_bound)`, plans the inner bound statement only, formats the resulting `PhysicalPlan` with planner-owned `format_explain`, and returns `ExecutionResult::Explanation`. `logical_plan` and `physical_plan` do not accept `BoundStatement::Explain` directly. Each plan node implements a `Display`-like method that shows the operator type, table/index involved, and any filter predicates.

### V1 Planner Non-Goals

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

    /// Pull up to max_rows at once. V1 implementation: calls next() in a loop.
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

V1 has a cooperative cancellation token: `ExecutionContext.cancel` is an `&AtomicBool` the query engine checks between rows (and between rows of INSERT/UPDATE/DELETE write loops), aborting with `SqlState::QueryCanceled`. A `CancelRequest` on a side connection sets that flag via the server's `CancelRegistry`. Operators themselves stay cancellation-free; the polling lives in the query engine, so a future statement-timeout can reuse the same token without changing operator semantics.

`next_batch` has a default implementation so V1 operators only implement `next()`. A future vectorized engine overrides `next_batch` with columnar processing. `output_schema()` allows callers to know the shape of rows without pulling, which is needed for `RowDescription`, EXPLAIN, and projection validation.

**ExecRow identity flow:**
- **Scan operators** (`SeqScanOp`, `IndexScanOp`): Construct `ExecRow` from `StoredRow`, populating `identity` from the `StoredRow`'s `row_id` and `key`.
- **Filter, Sort, Limit**: Pass `ExecRow` through unchanged (identity preserved).
- **Projection**: Rewrites `exec_row.row` (narrowed columns) but preserves `identity`.
- **Join, Aggregate**: Produce new rows — `identity` is `None` (these rows don't correspond to a single source row).
- **UPDATE/DELETE executor**: Reads `identity` from each `ExecRow` to call `storage.delete(ctx, table, &key)` or `storage.update(ctx, table, &key, new_row)`.
- **SELECT protocol layer**: Ignores `identity`, sends only `exec_row.row`.

### Operators

| Operator | Behavior |
|---|---|
| `SeqScanOp` | Iterates all rows in a table via storage, applies optional filter |
| `IndexScanOp` | Looks up rows through the chosen index — `scan_range` for the primary key, `index_scan` for a secondary index — and applies residual `IndexScan.filter` when present |
| `NestedLoopJoinOp` | For each left row, scans right for matches. Buffers right side on first pass. |
| `HashJoinOp` | Inner equi-join: builds a probe table over the right input keyed by `right_keys`, probes with `left_keys`; rows with a NULL key never match. |
| `FilterOp` | Passes through rows matching the predicate |
| `ProjectionOp` | Evaluates expressions, outputs narrowed columns |
| `SortOp` | Materializes all input, sorts in memory, emits in order. Blocking operator. |
| `LimitOp` | Stops pulling after N rows |
| `AggregateOp` | Groups rows by key in a hash map, computes aggregates, emits results. Blocking operator. |

### Expression Evaluator

A recursive function that takes a `BoundExpr` and an `ExecRow` and returns a `Value`. Column access is by slot index (`exec_row.row.values[input_ref.slot]`) — no schema lookup needed at evaluation time. Handles arithmetic, comparisons, string concatenation (`||`), boolean logic, NULL propagation (three-valued logic), `CASE`, `CAST`, `IN`, `LIKE`, `BETWEEN`, and the scalar functions `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `ABS`, and `SUBSTRING`. Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) are evaluated by `AggregateOp`, not scalar expression evaluation. Type information is carried in bound expressions (`data_type`, `nullable`), so the evaluator can validate without external lookups.

V1 expression semantics:

- Comparisons with `NULL` return `NULL`; `WHERE` and `HAVING` keep only `TRUE`.
- `LIKE` requires text operands, is case-sensitive, supports `%` and `_`, and uses backslash to escape `%`, `_`, or `\`. V1 does not support a SQL `ESCAPE` clause. If the value or pattern is `NULL`, the result is `NULL`.
- `IN` returns `TRUE` on the first non-null equal item, `FALSE` when no item matches and no list item is `NULL`, and `NULL` when the left side is `NULL` or no item matches but some list item is `NULL`. `NOT IN` applies SQL `NOT`.
- `BETWEEN` evaluates as `(expr >= low) AND (expr <= high)`; `NOT BETWEEN` applies SQL `NOT`.
- String concatenation `||` requires text operands and returns `NULL` if either side is `NULL`. The scalar functions `UPPER`/`LOWER`/`LENGTH`/`TRIM` (text) and `ABS` (integer) and `SUBSTRING(text, start[, length])` are NULL-propagating; `LENGTH` and `SUBSTRING` count Unicode characters, and `SUBSTRING` uses 1-based positions clamped to the string and rejects a negative length.
- Searched `CASE WHEN condition THEN value ...` chooses the first `WHEN` whose condition evaluates to `TRUE`; `FALSE` and `NULL` conditions do not match. Simple `CASE operand WHEN value THEN result ...` compares `operand = value` with SQL comparison semantics and chooses the first comparison that evaluates to `TRUE`. If no branch matches, both forms return `ELSE` or `NULL`.
- `CASE` result typing: binder requires all non-`NULL` `THEN` and `ELSE` expressions to have the same `DataType`; `NULL` branches are allowed and make the output nullable. If every result branch is `NULL`, binder rejects the expression with `SqlState::DatatypeMismatch`.
- Explicit `CAST` conversion matrix: same-type casts are identity; `NULL` casts to `NULL`; `INTEGER -> TEXT` uses decimal i64 formatting; `BOOLEAN -> TEXT` returns `true` or `false`; `TEXT -> INTEGER` parses a base-10 i64 with optional leading sign and no surrounding whitespace; `TEXT -> BOOLEAN` accepts case-insensitive `true`, `t`, `1`, `false`, `f`, and `0`. `INTEGER -> BOOLEAN`, `BOOLEAN -> INTEGER`, malformed text, and all other pairs return `SqlState::DatatypeMismatch`.
- `ORDER BY` defaults match PostgreSQL: ascending sorts `NULL` last, descending sorts `NULL` first, unless `NULLS FIRST` or `NULLS LAST` is specified. A bare positive integer literal in `ORDER BY` is a 1-based reference to the nth output column, resolved by the binder.

### DDL and DML

`INSERT`, `UPDATE`, and `DELETE` are handled directly by the executor (not through the iterator model), call into storage, and return the affected row count. `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX` also return `ExecutionResult::Modified`, using command names `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX` with `count = 0`.

## 7. Storage Engine

The `storage` crate owns the on-disk data format, page-backed row storage, and the durable on-disk primary-key B-tree index.

### Row Iterator

```rust
/// Fallible iterator over rows from the storage engine. Returns StoredRow
/// so that DML operations can target the physical row for modification.
/// V1 copies rows out of the buffer pool. A future version may return
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

Data operations and DDL are separate traits — they have different concurrency semantics (DDL involves file creation and catalog updates, DML operates within existing table pages), and a future MVCC implementation may handle transactional DDL differently from DML.

```rust
pub trait StorageEngine: Send + Sync {
    /// Insert a row, returns its physical RowId
    fn insert(&self, ctx: &StatementContext, table: TableId, row: Row) -> Result<RowId>;

    /// Point lookup by primary key
    fn get(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<Option<Row>>;

    /// Delete by primary key
    fn delete(&self, ctx: &StatementContext, table: TableId, key: &Key) -> Result<bool>;

    /// Update by primary key
    fn update(&self, ctx: &StatementContext, table: TableId, key: &Key, row: Row) -> Result<bool>;

    /// Full table scan
    fn scan(&self, ctx: &StatementContext, table: TableId) -> Result<Box<dyn RowIterator>>;

    /// Range scan over the primary key access path
    fn scan_range(
        &self,
        ctx: &StatementContext,
        table: TableId,
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

Every operation takes a `StatementContext`. In V1 this carries only the autocommit `txn_id`. When MVCC is added, the context gains snapshot visibility, isolation level, and write-set tracking — without changing any call sites.

`scan_range` serves primary-key `IndexScan` plan nodes. For `KeyRange::Exact`, it is a point lookup that returns an iterator (consistent interface). For `KeyRange::Range`, it walks the primary-key B-tree leaves from start to end. For `KeyRange::All`, it is equivalent to `scan`. Secondary-index `IndexScan` nodes use `index_scan(table, index, range)`, which walks the secondary B-tree and reads each entry's heap row directly at the stored TID (secondary indexes point at heap TIDs, uniform with the primary-key index — no primary-key indirection).

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

V1 development builds do not migrate older page formats. Existing page files without `PageVersion = 2` are rejected as corrupt during load/recovery.

**PageLSN:** The page header carries an 8-byte PageLSN — the LSN of the WAL record that last modified the page — stamped on every mutation. Redo replay is gated by it (a record is applied only if `page_lsn < record.lsn`), and it determines when a dirty page is safe to flush. See the Write-Ahead Log section.

### Page-Backed Primary-Key Structure

- Heap pages store full serialized rows.
- A durable, non-clustered on-disk B-tree (`Key -> RowLocation`) in a separate file per table maps primary keys to physical heap slots.
- `RowLocation` stores `file_id`, `page_num`, and `slot_num`.
- The B-tree is durable, so nothing is rebuilt on startup; its pages are recovered by redo like any other page.
- A future clustered B-tree (rows in the leaves) can replace this internal access path without changing the public storage traits.

### Row Serialization

```
[row_format_version: 1 byte][infomask: 2][xmin: 8][xmax: 8][t_ctid: 6][null_bitmap][col1_data][col2_data]...
```

- `row_format_version`: `2`, the MVCC tuple layout. `decode_row` still accepts legacy `1` tuples (`[version=1][null_bitmap][columns]`, no MVCC header); other versions are rejected as corrupt.
- MVCC tuple header (v2 only, little-endian): `infomask` (2-byte hint bits — `XMIN_COMMITTED`/`XMIN_ABORTED`/`XMAX_COMMITTED`/`XMAX_ABORTED` cache settled CLOG status, `HEAP_ONLY`/`HOT_UPDATED` reserved for HOT, rest reserved-zero), `xmin` (8-byte creator txn id), `xmax` (8-byte deleter txn id; `0` = live), and `t_ctid` (forward successor pointer `(page: u32, slot: u16)`; sentinel `(u32::MAX, u16::MAX)` = latest version).
- Insert stamps `xmin = txn_id` (from `StatementContext.txn_id`), `xmax = 0`, `t_ctid = sentinel`, `infomask = 0`. Legacy v1 tuples decode as frozen/always-visible (`xmin = FROZEN_XID`, `xmax = 0`).
- `INTEGER`: 8 bytes, little-endian i64
- `TEXT`: 4-byte length prefix + UTF-8 bytes
- `BOOLEAN`: 1 byte
- `NULL`: represented in the null bitmap, no data bytes

### File Layout

Files are named by stable numeric ID, not by user-visible names. This avoids rename issues (future `ALTER TABLE RENAME`), filesystem-unsafe characters in table names, and name collisions.

**Heap and index files (the mutable page home):**
- `data/heap/<TableId>.heap` — slotted data pages, page `n` at byte offset `n * PAGE_SIZE`. Written in place by checkpoint flush or eviction; files grow by appending pages.
- `data/heap/<TableId>.idx` — the table's primary-key B-tree (metapage at page 0, then leaf/internal nodes), same page layout and offsets.

**Control record (the single source of truth for the current checkpoint):**
- `data/manifest.dat` — a versioned binary envelope (magic `SGMF`, version, payload length, CRC32 over the payload) whose JSON payload holds the redo boundary `checkpoint_lsn`, sorted table IDs, and the catalog snapshot.

The control record is updated atomically via write-to-temp + fsync + rename (atomic on POSIX) + directory fsync. Recovery reads it to find the redo boundary and catalog. See Checkpoint below for the full protocol.

**Other files:**
- `data/wal.dat` — write-ahead log (append-only)

Table names are purely a catalog-level concept — the storage engine only sees table IDs, which map directly to heap file names.

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
    // (Checkpoint flush and recovery redo add flush_dirty_pages / fetch_for_redo; see buffer.md.)
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

`new_page(file_id, txn_id)` allocates the next unused page number for that file and returns a guard whose `page_num()` identifies the new page. The fresh-page insertion path rejects an already resident `(file_id, page_num)` with an internal error instead of overwriting it. The pool tracks `next_page_num_by_file`; `load_page(file_id, page_num, data)` inserts `data` as a clean frame when the page is not resident. If `(file_id, page_num)` is already resident, `load_page` leaves resident bytes, dirty state, dirty transaction ID, and rollback metadata unchanged, still advances the next-page counter to at least `page_num + 1`, and returns `Ok(())`. Rollback of a new page removes the page but does not need to reuse its page number in v1.

Guards are owned types (no lifetime parameter) — same rationale as the concurrency controller guards. They hold `Arc` references to the buffer pool frame internally, which keeps `BufferPool` object-safe. The Arc overhead is one reference count per page access, negligible compared to the I/O it represents.

Guards eliminate manual pin/unpin errors: a page is pinned for exactly the lifetime of the guard. Early returns, panics, and `?` propagation all unpin correctly via `Drop`.

### Design

- **Frame:** A slot holding one 8KB page. Pool size is configurable (default: 1024 frames = 8MB).
- **Page descriptor:** Tracks `(file_id, page_number)`, pin count, dirty flag, reference bit, `dirty_txn_id` (the txn that last dirtied it), and `needs_fpi` (whether the next modification must log a full-page image).
- **No rollback tracking (MVCC):** the buffer pool keeps no per-transaction page state. Abort is status-based (`docs/specs/mvcc.md` §4 Decision 3): `rollback(txn_id)` undoes nothing and reclaims nothing — a rolled-back transaction's pages (modified or freshly allocated) stay resident as dirty-but-evictable frames, hidden by the CLOG and reclaimed by VACUUM. (The before-image store and new-page rollback tracking that the pre-MVCC v1 model used are retired in Milestone D1.)
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

During normal operation the working set is not bound by the pool size: eviction-flush-on-steal writes dirty pages to the heap and evicts them, so a large dataset (or a large in-flight transaction) spills rather than erroring. With V1's single-writer autocommit:
- Each statement dirtys a modest number of pages
- Pages stay dirty in memory until a checkpoint flushes them in place or an eviction steals them
- The buffer pool default (1024 frames = 8MB) keeps a small-to-medium working set resident; larger sets spill to the heap
- **Recovery** spills too: stealing is enabled before redo, so the redo working set is not bounded by the buffer pool either (the durable on-disk index means nothing is rebuilt in memory)

### Concurrency

- Frame-level read/write latches managed by the page guards (multiple concurrent readers, exclusive writer)
- Page table mapping `(file_id, page_num)` to frame protected by a separate latch
- Multiple threads can read different pages concurrently

## 9. Control Store

The `control` crate owns the durable **control record** — the checkpoint commit point. It does not write whole-table snapshots; table data lives in mutable per-table heap files (see Storage Engine) and is flushed in place. The control record is a single atomic file holding the redo boundary, the live table ids, and the catalog snapshot.

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

The `wal` crate provides durability with a **physiological redo WAL**: physical-redo records describe page changes (`HeapInit`, `HeapInsert`, `HeapDelete`, `HeapUpdateHeader`, `FullPageImage`) gated by a per-page LSN, alongside logical DDL records (`CreateTable`, `DropTable`, `CreateIndex`, `DropIndex`) and the `Commit`/`Abort`/`Checkpoint` markers. With MVCC (`feat/mvcc`), recovery is **redo-all**: it replays every physical record under PageLSN gating regardless of the transaction's outcome, and the CLOG (rebuilt from `Commit`/`Abort`) decides visibility afterward; an aborted/in-flight transaction's replayed versions are invisible. Logical DDL records replay only for committed transactions. (See `docs/specs/mvcc.md` §8 for the full Milestone-D recovery contract.)

### V1 Durability Model: Heap Files + Redo WAL + Flush Checkpoint

Table data lives in mutable per-table heap files; pages are mutated in the buffer pool and written back in place. In-place page writes with a logical-only WAL would be unrecoverable (a torn page has no consistent base), so v1 uses:

- **Per-page LSN (PageLSN)** in the page header, stamped with the LSN of the record that last modified the page. Redo is gated by it (apply only if `page_lsn < record.lsn`), making replay idempotent.
- **Full-page writes (FPW)** for torn-page protection: the first modification of a page after each checkpoint logs a `FullPageImage`; later modifications log deltas. Redo reinstalls the image (repairing any torn write) before applying deltas. A freshly allocated page is its own base via `HeapInit`.
- **Flush-based checkpoint**: dirty pages are flushed in place to the heap and fsynced, then the control record advances the redo boundary, then the WAL prefix is truncated. With MVCC the flush gate requires only WAL-durability (not committedness), so uncommitted/aborted dirty pages may be flushed too — they are hidden by the CLOG and reclaimed by VACUUM. WAL truncation is conservative: it never drops an aborted/in-flight transaction's records (`docs/specs/mvcc.md` §5.4/§8).

This gives the invariants:
1. After a crash, recovery loads the heap as of the last control record and replays redo records with `LSN > checkpoint_lsn`; PageLSN gating plus full-page images make this idempotent and torn-page-safe. (With MVCC this is redo-all + CLOG visibility.)
2. The WAL captures all operations since the redo boundary.
3. Checkpoint cost is O(pages changed), not O(database size).

**Trade-offs:**
- Normal operation and recovery both spill dirty pages to the heap via eviction-flush-on-steal; the working set is not bounded by the buffer pool size (the durable on-disk index means recovery rebuilds nothing in memory). The steal path forces the WAL durable before writing a stolen page (write-ahead), so a possibly-uncommitted stolen page is always recoverable.
- Startup replays WAL from the last checkpoint — bounded by checkpoint frequency.

**Future upgrade paths** (none change the `BufferPool` or `StorageEngine` traits):
- **MVCC** — in progress on `feat/mvcc` (see `docs/specs/mvcc.md`). Row format v2 carries the per-version `xmin`/`xmax`/`t_ctid`/`infomask` tuple header; the redo WAL is the prerequisite. Visibility, version chains, and transactions land in later milestones.

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
- **TxnID:** Unique per statement in V1. Essential for future multi-statement transactions.
- **Type:** One of the logical operation types below.
- **Payload:** Depends on type.
- **CRC32:** Integrity check over the entire record.

**Record types and payloads:**

| Type | Payload |
|---|---|
| `CreateTable` | serialized `TableSchema` (name, columns, primary key) |
| `DropTable` | `TableId` |
| `CreateIndex` | serialized `IndexSchema` (id, table, name, columns, unique) |
| `DropIndex` | `IndexId` |
| `Commit` | (empty — marks the transaction as committed) |
| `Abort` | (empty — marks the transaction as aborted; `txn_id` in the header) |
| `Checkpoint` | `redo_lsn` — marks a completed checkpoint. WAL records before it can be truncated. |
| `HeapInit` | `FileId`, `PageNum` — initialize a fresh heap page |
| `HeapInsert` | `FileId`, `PageNum`, `slot`, encoded row bytes |
| `HeapDelete` | `FileId`, `PageNum`, `slot` |
| `HeapUpdateHeader` | `FileId`, `PageNum`, `slot`, `xmax`, `t_ctid` (`PageNum`, `u16`), `infomask` — in-place mutation of a v2 tuple header (MVCC version stamping; redo via `page::set_tuple_header`, not yet emitted by the engine) |
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

    /// Truncate WAL records before the given LSN (after checkpoint). Conservative:
    /// never drops a non-committed transaction's records (see below).
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;

    /// Last LSN known to be durable after fsync.
    fn flushed_lsn(&self) -> Lsn;

    /// Total encoded bytes of retained records whose stored LSN is > lsn.
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;

    /// Establish the CLOG implicit-committed floor at recovery, conservatively.
    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()>;
}
// `WalManager: TxnStatusView`, so `status`/`is_committed`/`is_aborted` come from the
// rebuilt CLOG. The redo-committed-only `replay_committed_from` is retired with MVCC.
```

`append(record)` always assigns the next monotonically increasing LSN and writes that LSN into the encoded record. Callers may pass `record.lsn = 0`; `append` ignores the caller-provided LSN. Replay preserves the stored LSN from disk.

`replay_from(lsn)` is strictly exclusive: it inspects only records whose stored `record.lsn > lsn`. Recovery passes the control record `checkpoint_lsn`, so replay starts after the last record whose effects are already reflected in the heap, and (redo-all) applies every page-mutation record, deciding visibility via the CLOG.

`truncate_before(lsn)` may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`. **Conservative truncation (MVCC):** it never drops an aborted/in-flight transaction's records — it pins on the oldest non-committed transaction below `lsn` — so an aborted-but-flushed transaction stays invisible across restart (`docs/specs/mvcc.md` §5.4/§8). Truncation writes retained records to a temporary WAL, fsyncs it, renames it over the live WAL, and immediately fsyncs the parent directory. If that directory fsync fails, the WAL manager is poisoned and returns the error before reopening the WAL or mutating retained-record in-memory state.

`bytes_after(lsn)` is server checkpoint accounting only. It counts encoded bytes for retained WAL records with stored `LSN > lsn`; if `lsn` predates the retained WAL after truncation, it returns the encoded byte size of all retained records.

### V1 Durability Rules

One rule ensures redo recovery is correct:

**In-place page flushing.** Dirty pages are written back to their heap file by the checkpoint and by eviction-steal. Each write is protected by the page's redo records — a full-page image on the first modification since the last checkpoint, deltas thereafter — so a torn write is repairable during redo. With MVCC, uncommitted/aborted dirty pages may also be flushed (the flush gate requires only WAL-durability); they are hidden by the CLOG and reclaimed by VACUUM, and the steal path forces the WAL before writing a possibly-uncommitted page.

The WAL is the source of durability between checkpoints:
- On commit, the WAL is flushed through the commit record (`fsync`). The data is durable in the WAL even though the dirty heap pages may still be in memory.
- The buffer pool holds modified pages in memory until the next checkpoint flushes them to the heap.
- Each heap page is recoverable from the last checkpoint plus the committed redo records after it.

This gives a clean invariant: **after a crash, PageLSN-gated redo (with full-page images) restores every heap page to its last committed state.**

### V1 Write Protocol

All writes are serialized through the `ConcurrencyController`. The protocol for a single autocommit statement:

1. Acquire exclusive write guard via `controller.begin_write()`
2. Assign a statement-level `txn_id` and register it in the active-transaction registry (`ServerComponents.active_txns`). The CLOG status is `InProgress` implicitly (the default for any unsettled normal id).
3. Execute the statement through the storage engine (which appends WAL records for each logical operation: insert, update, delete).
4. If execution fails: append an `Abort` record (which records the txn `Aborted` in the CLOG; not fsynced) and deregister it from the active-transaction registry, then `storage.rollback_txn(txn_id)` (DDL-metadata restore), `buffer_pool.rollback(txn_id)` (bookkeeping clear; no page undo), and catalog restore when needed; return error to client and drop write guard if cleanup succeeds. Abort is **status-based** with MVCC (`docs/specs/mvcc.md` §4 Decision 3): the failed statement's heap versions stay in place, hidden by the CLOG (`Aborted`) and reclaimed by VACUUM — there is no before-image page undo. If cleanup fails before the commit record is durable, log the failure, attempt to flush WAL, and exit.
5. Append a `Commit` record for this `txn_id`
6. Flush WAL through the commit record to disk (`fsync`)
7. The statement is now durable and must not be rolled back or reported as a normal SQL failure
8. `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)` — cleanup-only (the buffer pool tracks no rollback metadata under status-based abort); deregister the txn from the active-transaction registry (its CLOG status is already `Committed`, set when the WAL flush made the `Commit` durable)
9. Drop write guard (releases exclusive lock)
10. Call `record_commit_and_maybe_checkpoint(&components)`; it may acquire its own write guard for a checkpoint
11. Return success to the client

`storage.commit_txn` and `buffer_pool.commit` are cleanup-only in-memory operations and must not perform I/O. For a valid `txn_id`, they should not fail. If either returns an error after WAL flush through the `Commit` record succeeded, the server must not call rollback. It logs the fatal internal error, flushes WAL, and terminates because recovery will replay the durable commit.

Reads acquire a shared read guard via `controller.begin_read()` and proceed concurrently with each other. A write blocks until all read guards are released.

### Failed Statement Rollback

If a write statement errors after mutating pages but before commit (e.g., a constraint violation mid-batch INSERT, or an internal error after allocating a page), dirty pages from that `txn_id` must be rolled back — they must not be visible to subsequent reads or included in the next checkpoint.

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
3. Append an `Abort` record (CLOG → `Aborted`; not fsynced) and deregister the txn from the active-transaction registry
4. `storage.rollback_txn(txn_id)` — restores engine-owned DDL metadata (table/index schema shadow state); it does NOT touch heap/index page content
5. `buffer_pool.rollback(txn_id)` — no-op bookkeeping clear (the dirty pages stay, hidden by the CLOG)
6. Catalog restore returns DDL metadata to the pre-statement state when catalog state changed
7. WAL records for this `txn_id` remain but have no `Commit` — recovery replays them (redo-all) and the CLOG hides them
8. Error returned to client

If any cleanup step fails before the commit record is durable, the server treats process state as unsafe: it logs the failure, attempts to flush WAL, and exits instead of returning to service.

**Why no undo:** an aborted version is hidden by the CLOG check the visibility predicate already performs — the same mechanism snapshot isolation needs — so undoing the page would be redundant work, and (post-D1) impossible once the page has been stolen to disk. VACUUM reclaims the space later.

### Checkpoint

The checkpoint flushes dirty pages in place to the heap and advances the redo boundary. Cost is O(pages changed), not O(database size). The previous control record stays valid until the new one is committed.

**Checkpoint protocol:**

1. Acquire exclusive write guard (no statement in-flight).
2. `wal.flush()` — a page's redo must be durable before the page is written.
3. `buffer_pool.flush_dirty_pages()` — write flushable dirty pages to the heap `PageStore` (committed, aborted, and — under Stage 2 — in-flight alike; all WAL-durable after step 2, and the CLOG hides the non-committed tuples).
4. `store.sync_all()` — fsync the heap before advancing the redo boundary.
5. `checkpoint_lsn = wal.flushed_lsn()`.
6. `control.store(checkpoint_lsn, sorted_table_ids, catalog_bytes)` — the durable commit point (atomic temp + fsync + rename + directory fsync).
7. Append `WalRecord { txn_id: <txn-id high-water>, kind: Checkpoint { redo_lsn: checkpoint_lsn } }`, flush WAL, then `truncate_before(checkpoint_lsn)` (conservative — never drops an aborted/in-flight transaction's records, `mvcc.md` §5.4).
8. `buffer_pool.mark_all_clean()` — clears dirty flags and re-arms full-page-image protection.
9. Drop write guard.

**Crash safety analysis:** the ordering is heap fsync (4) → control record (6) → WAL truncation (7).
- Crash before step 6: the control record is unchanged; recovery falls back to the previous `checkpoint_lsn`, and this cycle's full-page images (logged since that boundary) repair any torn heap write.
- Crash between steps 6 and 7: the new control record is durable and the heap is consistent; the un-truncated WAL tail replays idempotently under PageLSN gating.
- Crash after step 7: consistent.

**Checkpoint frequency:** Triggered by configurable thresholds — every N committed statements or M bytes of WAL. `CheckpointState.last_checkpoint_lsn` starts from the loaded manifest checkpoint LSN, and `CheckpointState.commits_since_checkpoint` starts at `0`. After each successful write statement and after its statement guard is dropped, server calls `record_commit_and_maybe_checkpoint(&components)`, which increments the commit counter and triggers `run_checkpoint(&components)` when `commits_since_checkpoint >= config.checkpoint_every_n_commits` or `wal.bytes_after(last_checkpoint_lsn)? >= config.checkpoint_wal_bytes`. A successful checkpoint stores the new checkpoint LSN and resets the commit counter to `0`. Checkpoint is also triggered on clean shutdown. More frequent checkpoints mean shorter WAL replay on startup but more I/O.

### Crash Recovery (REDO)

The control record names the redo boundary and the catalog. Recovery loads the heap as of that boundary and replays committed redo records on top.

**Recovery uses physiological page redo plus a DDL replay trait** so replayed operations do not re-append to the WAL:

```rust
pub trait RecoveryOperations: Send + Sync {
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, table: TableId) -> Result<()>;
}
```

Row recovery is `storage::apply_physical_redo(page, lsn, kind)`, gated by the page-LSN; DDL replays through `RecoveryOperations`. Both modify pages without appending WAL.

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

`open` stores shared `Arc` handles to the buffer pool and WAL manager and initializes empty table metadata. It does not read schemas from disk; server startup installs catalog schemas explicitly with `install_schemas` after loading the catalog snapshot.

**Recovery procedure** (driven by the server startup sequence):

1. `control.load()` — the redo boundary `checkpoint_lsn` and catalog bytes. If none: fresh database.
2. Initialize storage in recovery mode and the catalog; `install_schemas`.
3. Enable eviction-flush-on-steal (`buffer.enable_stealing()`) so redo may spill — the durable index means nothing is rebuilt in memory, so the recovery working set is not bounded by the pool.
4. Redo-all: replay every record with `LSN > checkpoint_lsn` (`WalManager::replay_from`): physical-redo records via `apply_physical_redo` (PageLSN-gated; torn/missing pages are zeroed so a `FullPageImage`/`HeapInit` rebuilds them) — heap and index pages alike, regardless of transaction outcome — DDL via `RecoveryOperations` only for committed transactions. The CLOG (rebuilt from `Commit`/`Abort`) decides visibility; aborted/in-flight versions are invisible.
5. If records were replayed: checkpoint to persist the redone state and advance the boundary.
6. Switch to normal mode with `storage.set_mode(StorageMode::Normal)`.

**Idempotency:** PageLSN gating applies each record's effect at most once, so replay is safe even when the heap already reflects some post-boundary work (e.g. a partially completed prior checkpoint).

### File

`data/wal.dat` — single file, append-only. Old segments before the last completed checkpoint can be truncated.

## 10. Catalog

The `catalog` crate manages metadata about all database objects.

### Data Structures

```rust
pub struct Catalog {
    tables_by_name: HashMap<String, TableId>,
    tables_by_id: HashMap<TableId, TableSchema>,
    next_table_id: TableId,
}

pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,       // ColumnDef with assigned IDs
    pub primary_key: Vec<ColumnId>,
}
```

`ColumnDef` (with `id`, `name`, `data_type`, `nullable`) and `DataType` are defined in `common`. The catalog uses `ColumnDef` for stored schemas. The parser uses `ParsedColumnDef` (no IDs). The catalog's `create_table` accepts `ParsedColumnDef` and assigns `ColumnId`s, producing a `TableSchema` with `ColumnDef`. Public construction from persisted catalog snapshots must use validated loading; unchecked snapshot installation is crate-internal only.

The catalog is the authority for name-to-ID resolution. IDs are stable and never reused (monotonically increasing). The binder resolves all names to IDs so that the planner, executor, and storage engine work exclusively with IDs.

### Catalog Trait

```rust
pub trait CatalogManager: Send + Sync {
    /// Resolve a table name to its schema (used by the binder)
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>>;

    /// Get schema by ID (used by executor/storage)
    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>>;

    /// List all tables
    fn list_tables(&self) -> Result<Vec<TableSchema>>;

    fn snapshot(&self) -> Result<CatalogSnapshot>;
    fn restore(&self, snapshot: CatalogSnapshot) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, id: TableId) -> Result<()>;

    /// Register a new table. Accepts parsed columns (no IDs), assigns
    /// TableId and ColumnIds, returns the completed TableSchema.
    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<TableSchema>;

    /// Remove a table
    fn drop_table(&self, id: TableId) -> Result<()>;
}
```

```rust
pub struct CatalogSnapshot {
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub next_table_id: TableId,
}
```

Empty catalogs start with `next_table_id = 1`. `apply_create_table` and `apply_drop_table` are recovery-only APIs. `apply_create_table` inserts a fully assigned historical schema without changing IDs and advances `next_table_id` past that schema ID; `apply_drop_table` removes by ID without assigning IDs.

### Persistence

The catalog is stored in the control record (`data/manifest.dat`) at each checkpoint. Loaded into memory on startup. All reads from the in-memory copy. Mutations update memory; persistence happens at the next checkpoint. Between checkpoints, the WAL ensures catalog changes (CREATE/DROP TABLE, CREATE/DROP INDEX) are durable.

### WAL Integration

`CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX` are logged to the WAL. On crash recovery, the catalog is loaded from the control record and updated by replaying committed `CreateTable`/`DropTable`/`CreateIndex`/`DropIndex` records.

### Concurrency

Wrapped in `RwLock`. Reads take a read lock. DDL takes a write lock. DDL is infrequent so this is not a bottleneck.

## 11. Server & Connection Management

The `server` crate is the binary entry point.

### Startup Sequence

1. Load configuration (data directory, port, buffer pool size)
2. Initialize the control store (`FileControlStore`) and heap page store (`HeapPageStore` over `data/heap`)
3. Initialize WAL — open or create `data/wal.dat`
4. Initialize buffer pool with configured frames, the `WalFlushPolicy`, and the heap page store
5. Load the control record (`control.load()`): the redo boundary `checkpoint_lsn` and catalog bytes (none if absent)
6. Initialize storage engine in **recovery mode** with `PageBackedStorageEngine::open(buffer_pool.clone(), wal.clone(), StorageMode::Recovery)`
7. Initialize catalog from the control catalog bytes (or empty); `storage.install_schemas(catalog.list_tables()?)`
8. Enable eviction-flush-on-steal (`buffer.enable_stealing()`); the durable index means redo rebuilds nothing in memory and may spill
9. Redo-all: replay every record with `LSN > checkpoint_lsn` (`WalManager::replay_from`): physical-redo via `storage::apply_physical_redo` (PageLSN-gated; torn/missing pages zeroed so a `FullPageImage`/`HeapInit` rebuilds them), heap and index pages alike regardless of transaction outcome, DDL via `RecoveryOperations` only for committed transactions — the CLOG decides visibility; no WAL appended in recovery mode
10. Build `ServerComponents` with catalog, storage, buffer pool, WAL, control store, heap store, concurrency controller, shutdown state, checkpoint state initialized from the control `checkpoint_lsn`, and `next_txn_id` initialized to one greater than the maximum retained user WAL `txn_id`.
11. If records were replayed: `run_checkpoint(&components)` to persist the redone state to the heap and index and advance the redo boundary
12. Switch storage engine to **normal mode** with `storage.set_mode(StorageMode::Normal)` (WAL appending enabled)
13. Construct `QueryService` from `components`
14. Start Tokio runtime, bind TCP listener (default port 5433)

Recovery computes `next_txn_id` by scanning all retained records from `WalManager::replay_from(checkpoint_lsn)`, including committed operations, uncommitted operations, and `Commit` records, while ignoring `txn_id = 0` records. `next_txn_id` starts at `max_txn_id + 1`, or `1` when no user transaction records remain. If the maximum retained user transaction ID is `u64::MAX`, startup fails with a structured WAL/internal error instead of wrapping or saturating the next transaction ID. Step 13 transitions to normal operation where `StorageEngine` methods append WAL records.

The server binary accepts `--data-dir <PATH>`, `--port <PORT>`, `--buffer-pool-frames <N>`, `--checkpoint-every-n-commits <N>`, `--checkpoint-wal-bytes <BYTES>`, `--auto-vacuum-dead-rows <N>`, `--shutdown-timeout-ms <MS>`, `--tls-cert-file <PATH>`, `--tls-key-file <PATH>`, and `--help`. Defaults are `./data`, `5433`, `1024`, `100`, `67108864`, `10000`, and `30000`. `--auto-vacuum-dead-rows` is the checkpoint auto-prune threshold (committed dead versions since the last auto-prune; a checkpoint folds in a VACUUM pass once it is reached); `0` disables auto-prune. TLS is off unless both `--tls-cert-file` and `--tls-key-file` are supplied (providing only one is an error). V1 parses these flags with `std::env::args`; `--port` accepts `1..=65535`, the other numeric flags must be positive nonzero integers except `--auto-vacuum-dead-rows`, which also accepts `0` to disable auto-prune, and invalid input prints usage to stderr and exits with code `2`.

### Connection Handling

```
Tokio listener (async)
  └─ accept() loop
       └─ spawn async task per connection
            └─ Protocol codec decodes client messages
            └─ For Query messages:
                 └─ spawn_blocking: query_service.execute_sql(sql)
                      → Bind → Plan → build PlanExecutor
                      → pull rows from PlanExecutor into ExecutionResult::Query
                 └─ async task: encode materialized rows, write to wire
            └─ For non-query messages: handle inline
```

The production executor crate never owns SQL strings. It executes `PhysicalPlan` values through `QueryEngine::execute`; SQL parsing, binding, planning, and statement guard acquisition are owned by the server's `QueryService`.

### Concurrency Control (V1)

All concurrency is managed through the `ConcurrencyController` trait (defined in `common`):

- **Read-only statements** (`SELECT`, `EXPLAIN`): server query orchestration parses SQL to classify the statement, calls `begin_read()`, receives a read guard, then binds and plans. `SELECT` invokes `QueryEngine`; `EXPLAIN` formats the inner physical plan and does not invoke the executor. Multiple readers proceed concurrently.
- **Read-write statements** (`INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`): server query orchestration parses SQL to classify the statement, calls `begin_write()`, receives a write guard, binds and plans, allocates the statement `txn_id`, then invokes `QueryEngine`. Blocks until all other guards are released. Writes are fully serialized.
- **Maintenance statements** (`VACUUM [table]`): not relational — they do not bind or plan. Like checkpoint, `VACUUM` takes the **exclusive** concurrency guard (`begin_checkpoint`), so it runs with no concurrent writer (readers stay lock-free), and it is rejected inside an explicit transaction block. See `docs/specs/mvcc.md` §9/§10 Milestone F for the orchestration (heap-prune → index-vacuum → line-pointer-reclaim) and the GC-horizon safety argument.
- The guard is held for the entire statement lifetime. Checkpoint runs under the exclusive write guard and `WalFlushPolicy` admits only committed pages, so uncommitted data never reaches the heap.

**V1 implementation:** The concrete `ConcurrencyController` is an `RwLock`. `begin_read()` acquires a shared lock, `begin_write()` acquires an exclusive lock. This is the foundation for safe page mutation, DDL, concurrent scans, and redo-only recovery.

**Other latches:**
- **Buffer pool:** Frame-level read/write latches managed by page guards.
- **Catalog:** Internal `RwLock` (reads concurrent, DDL exclusive). The catalog's own lock is separate from the `ConcurrencyController` — the catalog lock protects metadata consistency, while the `ConcurrencyController` protects statement-level isolation.
- **WAL appends:** Serialized by the write guard (no separate WAL mutex needed).

This is intentionally simple. The exclusive write guard limits write throughput to one statement at a time, but it makes the durability and recovery model correct and safe for heap and index page modifications. Future MVCC replaces the `ConcurrencyController` implementation with row-level concurrency control while preserving the server-facing orchestration API.

### Graceful Shutdown

`ServerComponents` owns `shutdown: Arc<ShutdownState>`. The listener stops accepting new connections when shutdown starts, and every query execution holds an in-flight guard from before `spawn_blocking` until its response or error has been written. If shutdown has begun, starting a new query returns `ErrorKind::Internal` / `SqlState::InternalError` with message `server is shutting down`.

On SIGINT/SIGTERM:
1. Stop accepting new connections
2. Wait for in-flight queries to complete, up to `Config.shutdown_timeout_ms`
3. If all in-flight queries finish before the timeout, run checkpoint, flush WAL, close files, and exit successfully
4. If the timeout expires, skip checkpoint and skip the final WAL flush, return an internal timeout error, and let process shutdown proceed without running finalization concurrently with in-flight query execution. Successful write statements still flush their own commit records before returning.

### Configuration (V1)

```rust
pub struct Config {
    pub data_dir: PathBuf,            // default: "./data"
    pub port: u16,                    // default: 5433
    pub buffer_pool_frames: usize,    // default: 1024 (8MB)
    pub checkpoint_every_n_commits: u64, // default: 100
    pub checkpoint_wal_bytes: u64,    // default: 64 * 1024 * 1024
    pub shutdown_timeout_ms: u64,     // default: 30000
    pub tls_cert_file: Option<PathBuf>, // default: None (PEM cert chain)
    pub tls_key_file: Option<PathBuf>,  // default: None (PEM private key)
}
```

Loaded from command-line args only in V1. No environment-variable or config-file loading in V1.

## 12. Future Work (Designed For, Not Implemented)

- **MVCC / Transactions:** `StatementContext` carries `txn_id` and is extensible for snapshot visibility. The `ConcurrencyController` trait returns owned guards so a simple `RwLock` implementation can later be swapped for a transaction manager. WAL record format includes `TxnID`.
- **Cost-Based Optimizer:** `LogicalPlan` → `PhysicalPlan` boundary exists. A cost-based optimizer slots between them, choosing physical access methods and join algorithms without changing the executor. The current rule-based planner already chooses among the primary-key and secondary indexes; a cost model would replace that heuristic.
- **Vectorized Execution:** `PlanExecutor::next_batch()` is defined with a default implementation. A vectorized engine overrides it with columnar batch processing.
- **Custom Wire Protocol:** `ProtocolCodec` and `ConnectionState` traits are protocol-agnostic. A custom protocol implements these traits.
- **Additional Data Types:** `DataType` and `Value` enums are extensible. Row serialization format supports new types via the null bitmap + column data pattern.
