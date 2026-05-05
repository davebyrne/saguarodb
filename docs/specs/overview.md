# SaguaroDB Overview Specification

**Date:** 2026-05-03
**Status:** Draft

## 1. Overview

SaguaroDB is a SQL-compatible relational database written in Rust. It is a standalone server that accepts client connections over a network, executes SQL queries against a page-oriented storage engine, and returns results over the PostgreSQL wire protocol.

### V1 Goals

- Standalone multi-client server
- PostgreSQL simple query wire protocol (abstracted for future custom protocol)
- Page-oriented storage engine with a v1 in-memory primary-key directory (abstracted for future MVCC and on-disk B-tree work)
- Autocommit only (no multi-statement transactions)
- Data types: `INTEGER` (i64), `TEXT`, `BOOLEAN`, `NULL`
- V1 SQL subset: `CREATE TABLE`, `DROP TABLE`, `INSERT ... VALUES`, `SELECT` (with `WHERE`, inner/cross/left/right/full joins, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`), `UPDATE`, `DELETE`, `EXPLAIN`; binder rejects unsupported parsed forms
- Rule-based query planner (no cost-based optimization)
- Primary-key access path only (full table scans otherwise)
- WAL with crash recovery
- Async networking (Tokio) with blocking thread pool for query execution

### V1 Non-Goals

- Multi-statement transactions / MVCC (designed for, not implemented)
- Secondary indexes (designed for, not implemented)
- Extended query protocol / prepared statements
- SSL/TLS
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
│   ├── snapshot/           (snapshot manager — manifest, snapshot read/write)
│   ├── buffer/             (buffer pool — in-memory page cache)
│   ├── wal/                (write-ahead log)
│   ├── catalog/            (table metadata, schema definitions)
│   └── common/             (shared types: DataType, Value, Row, errors, config)
```

### Dependency Flow

```
server → protocol, parser, planner, executor, snapshot, storage, buffer, wal, catalog, common
protocol → common
parser → common
planner → parser, catalog, common
executor → planner, storage, catalog, common
storage → buffer, wal, common
snapshot → buffer, common
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
/// Information about a dirty page, passed to FlushPolicy to decide
/// whether it can be flushed. Extensible struct so future fields
/// (e.g., page_lsn for physical WAL) don't change the trait signature.
pub struct PageFlushInfo {
    pub dirty_txn_id: u64,
    pub page_lsn: Option<Lsn>,  // V1: always None. Future physical WAL populates this.
}

/// Abstraction so the buffer pool can check whether a dirty page is safe to
/// flush/evict, without depending on the wal crate.
/// V1 implementation always returns false — dirty pages are never flushed
/// except during snapshot checkpoint. Future physical WAL implementation
/// checks WAL durability and commit status to enable incremental flushing.
pub trait FlushPolicy: Send + Sync {
    fn can_flush(&self, info: &PageFlushInfo) -> bool;
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

`FlushPolicy` lives in `common` so the buffer pool can determine whether a dirty page is eligible for eviction without depending on the `wal` crate. V1's implementation always returns `false` (dirty pages are never evicted — only flushed during snapshot checkpoint). A future physical WAL implementation would return `true` for committed, WAL-durable pages, enabling incremental eviction.

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
    Startup { user: String, database: Option<String> },
    SslRequest,
    Query(String),
    Terminate,
}

/// Outgoing message to a client (encoded by the protocol layer)
pub enum ServerMessage {
    SslRejected,                // single 'N' byte
    AuthenticationOk,
    ParameterStatus { key: String, value: String },
    ReadyForQuery,
    RowDescription(Vec<ColumnInfo>),
    DataRow(Vec<Option<String>>),  // text-format values
    CommandComplete(String),
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
/// Handles non-query messages (startup, SSL, terminate) directly.
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

1. **SSLRequest handling:** Many clients (psql, libpq-based drivers) send an `SSLRequest` before the real startup. The protocol detects this (8-byte message with code `80877103`) and responds with a single `N` byte (SSL not supported). The client then retries with a normal `StartupMessage`.
2. **Startup:** Client sends `StartupMessage` (version 3.0, user, database). Server responds `AuthenticationOk` → `ParameterStatus` (server_version, etc.) → `ReadyForQuery`.
3. **Query cycle:** Client sends `Query` (SQL string). Server responds with:
   - `RowDescription` (column names and types) for SELECT
   - `DataRow` (one per result row) for SELECT
   - `CommandComplete` (e.g., `INSERT 0 1`, `SELECT 5`)
   - `ReadyForQuery`
4. **Query error handling:** If a query fails, server sends `ErrorResponse` then `ReadyForQuery`. The connection stays open.
5. **Protocol decode error handling:** If decoding client bytes fails, server sends `ErrorResponse` then `ReadyForQuery` and closes the connection because the codec buffer state may be unrecoverable.
6. **Termination:** Client sends `Terminate`. Server closes connection.

### V1 Protocol Scope — What We Skip

- Extended query protocol (Parse/Bind/Execute) — no prepared statements
- SSL/TLS (SSLRequest is explicitly rejected with `N`)
- Authentication beyond accepting any connection
- `COPY`, `NOTIFY/LISTEN`
- `CancelRequest` flow

### PostgreSQL Wire Encoding Details

All integer fields are big-endian. All server messages except SSL rejection are one-byte tag plus a four-byte length that includes the length field but not the tag. SSL rejection is exactly the single byte `N`.

- Client `SSLRequest`: startup-style packet with length `8` and code `80877103`.
- Client `Startup`: startup-style packet with protocol `196608` (3.0), nul-terminated key/value parameters, and final `\0`; V1 reads `user` and optional `database`.
- Client `Query`: tag `Q`, length, nul-terminated SQL string.
- Client `Terminate`: tag `X`, length `4`.
- Server `AuthenticationOk`: tag `R`, length `8`, auth code `0`.
- Server `ParameterStatus`: tag `S`, `key\0value\0`; startup emits at least `server_version=16.0`, `server_encoding=UTF8`, `client_encoding=UTF8`, `DateStyle=ISO`, and `integer_datetimes=on`.
- Server `ReadyForQuery`: tag `Z`, length `5`, status byte `I`.
- Server `RowDescription`: tag `T`, field count, then for each column `name\0`, `table_oid = 0`, `attr_num = 0`, mapped type OID, type size, `type_modifier = -1`, and text `format_code = 0`.
- Server `DataRow`: tag `D`, column count, then `int32 byte_length` plus UTF-8 text bytes, or `-1` for `NULL`.
- Server `CommandComplete`: tag `C`, nul-terminated tags `SELECT n`, `INSERT 0 n`, `UPDATE n`, `DELETE n`, `CREATE TABLE`, `DROP TABLE`, or `EXPLAIN`.
- Server `ErrorResponse`: tag `E`, fields `S` severity, `C` SQLSTATE, `M` message, then final `\0`.

Type mapping uses PostgreSQL OIDs `INTEGER` as `INT8` (`20`, size `8`), `TEXT` (`25`, size `-1`), and `BOOLEAN` (`16`, size `1`). V1 rows are text format only: integers are decimal i64 strings, text is raw UTF-8, booleans are `t`/`f`, and null fields use length `-1`.

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
    /// DML or DDL result (INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE)
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
    Insert { table: String, columns: Vec<String>, source: InsertSource },
    Select(SelectStatement),
    Update { table: String, assignments: Vec<Assignment>, filter: Option<Expr> },
    Delete { table: String, filter: Option<Expr> },
    Explain(Box<Statement>),
}

pub enum InsertSource {
    Values(Vec<Vec<Expr>>),       // INSERT INTO t VALUES (...)
    Query(Box<SelectStatement>),  // INSERT INTO t SELECT ... (future, parsed but rejected in V1)
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
- Rejects unsupported features (e.g., `INSERT ... SELECT` in V1)
- Expands `SELECT *` into explicit column lists

```rust
/// Fully resolved statement. All names resolved, all types checked,
/// all column references assigned physical slot positions.
pub enum BoundStatement {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    Insert { table: TableId, columns: Vec<ColumnId>, source: BoundInsertSource },
    Select(BoundSelect),
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: BoundSelect },
    Delete { table: TableId, source: BoundSelect },
    Explain(Box<BoundStatement>),
}

pub enum BoundInsertSource {
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
    // Future: Query(BoundSelect)
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
    // Future: HashJoin, MergeSortJoin

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

V1 has one index identifier: `PRIMARY_KEY_INDEX_ID = 0`. The physical planner uses that value for every primary-key `IndexScan`. `IndexScan.filter` holds residual predicates not consumed by the primary-key range. For `WHERE id = 7 AND name = 'Ada'`, the scan range is `Exact(Key([7]))` and the residual filter is `name = 'Ada'`; for `WHERE id = 7`, the residual filter is `None`. Scan plan nodes capture `table_name` at planning time solely for EXPLAIN/debug output; execution still uses `table`.

The three-phase pipeline (`bind` → `logical_plan` → `physical_plan`) means a future cost-based optimizer replaces only `physical_plan`, choosing among multiple physical alternatives per logical operator. The binder and logical planner are unchanged.

### Planner Rules (V1 — Applied in Order)

1. **Primary key lookup:** If `WHERE` has an equality on the primary key, emit `IndexScan` with `index = PRIMARY_KEY_INDEX_ID`, `KeyRange::Exact`, and any non-key residual predicate in `filter`.
2. **Primary key range:** If `WHERE` has a range comparison on the primary key, emit `IndexScan` with `index = PRIMARY_KEY_INDEX_ID`, `KeyRange::Range`, and any non-key residual predicate in `filter`.
3. **Predicate pushdown:** Push `WHERE` conditions as close to the scan nodes as possible.
4. **Join ordering:** Process joins left to right as written. All joins are `NestedLoopJoin`. Join `condition` is `None` only for `Cross` and `Some(boolean_expr)` for every other join type.
5. **Projection pushdown:** Optional for initial v1. If implemented, only read columns that are needed downstream and rebase expression slots against each child output schema.

### EXPLAIN

`Statement::Explain` is handled by server `QueryService`, not by the executor. The server acquires a read guard, binds the inner statement to `BoundStatement::Explain(inner_bound)`, plans the inner bound statement only, formats the resulting `PhysicalPlan` with planner-owned `format_explain`, and returns `ExecutionResult::Explanation`. `logical_plan` and `physical_plan` do not accept `BoundStatement::Explain` directly. Each plan node implements a `Display`-like method that shows the operator type, table/index involved, and any filter predicates.

### V1 Planner Non-Goals

- Cost-based optimization
- Join reordering
- Hash joins or merge joins
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

    /// Pull the next row. Returns None when exhausted.
    /// Implementations should call cancellation.is_cancelled() periodically
    /// and return an error if true (e.g., every N rows in a scan).
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

V1 has no executor cancellation token in the public crate API. A future version may add a cooperative token for CancelRequest or statement timeout without changing v1 operator semantics.

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
| `IndexScanOp` | Looks up rows by primary key via the storage primary-key access path and applies residual `IndexScan.filter` when present |
| `NestedLoopJoinOp` | For each left row, scans right for matches. Buffers right side on first pass. |
| `FilterOp` | Passes through rows matching the predicate |
| `ProjectionOp` | Evaluates expressions, outputs narrowed columns |
| `SortOp` | Materializes all input, sorts in memory, emits in order. Blocking operator. |
| `LimitOp` | Stops pulling after N rows |
| `AggregateOp` | Groups rows by key in a hash map, computes aggregates, emits results. Blocking operator. |

### Expression Evaluator

A recursive function that takes a `BoundExpr` and an `ExecRow` and returns a `Value`. Column access is by slot index (`exec_row.row.values[input_ref.slot]`) — no schema lookup needed at evaluation time. Handles arithmetic, comparisons, boolean logic, NULL propagation (three-valued logic), `CASE`, `CAST`, `IN`, `LIKE`, and `BETWEEN`. Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) are evaluated by `AggregateOp`, not scalar expression evaluation. Type information is carried in bound expressions (`data_type`, `nullable`), so the evaluator can validate without external lookups.

V1 expression semantics:

- Comparisons with `NULL` return `NULL`; `WHERE` and `HAVING` keep only `TRUE`.
- `LIKE` requires text operands, is case-sensitive, supports `%` and `_`, and uses backslash to escape `%`, `_`, or `\`. V1 does not support a SQL `ESCAPE` clause. If the value or pattern is `NULL`, the result is `NULL`.
- `IN` returns `TRUE` on the first non-null equal item, `FALSE` when no item matches and no list item is `NULL`, and `NULL` when the left side is `NULL` or no item matches but some list item is `NULL`. `NOT IN` applies SQL `NOT`.
- `BETWEEN` evaluates as `(expr >= low) AND (expr <= high)`; `NOT BETWEEN` applies SQL `NOT`.
- Searched `CASE WHEN condition THEN value ...` chooses the first `WHEN` whose condition evaluates to `TRUE`; `FALSE` and `NULL` conditions do not match. Simple `CASE operand WHEN value THEN result ...` compares `operand = value` with SQL comparison semantics and chooses the first comparison that evaluates to `TRUE`. If no branch matches, both forms return `ELSE` or `NULL`.
- `CASE` result typing: binder requires all non-`NULL` `THEN` and `ELSE` expressions to have the same `DataType`; `NULL` branches are allowed and make the output nullable. If every result branch is `NULL`, binder rejects the expression with `SqlState::DatatypeMismatch`.
- Explicit `CAST` conversion matrix: same-type casts are identity; `NULL` casts to `NULL`; `INTEGER -> TEXT` uses decimal i64 formatting; `BOOLEAN -> TEXT` returns `true` or `false`; `TEXT -> INTEGER` parses a base-10 i64 with optional leading sign and no surrounding whitespace; `TEXT -> BOOLEAN` accepts case-insensitive `true`, `t`, `1`, `false`, `f`, and `0`. `INTEGER -> BOOLEAN`, `BOOLEAN -> INTEGER`, malformed text, and all other pairs return `SqlState::DatatypeMismatch`.
- `ORDER BY` defaults match PostgreSQL: ascending sorts `NULL` last, descending sorts `NULL` first, unless `NULLS FIRST` or `NULLS LAST` is specified.

### DDL and DML

`INSERT`, `UPDATE`, and `DELETE` are handled directly by the executor (not through the iterator model), call into storage, and return the affected row count. `CREATE TABLE` and `DROP TABLE` also return `ExecutionResult::Modified`, using command names `CREATE TABLE` and `DROP TABLE` with `count = 0`.

## 7. Storage Engine

The `storage` crate owns the on-disk data format, page-backed row storage, and the v1 in-memory primary-key directory.

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

`scan_range` serves `IndexScan` plan nodes. For `KeyRange::Exact`, it is a point lookup that returns an iterator (consistent interface). For `KeyRange::Range`, it walks the in-memory primary-key directory from start to end. For `KeyRange::All`, it is equivalent to `scan`. V1 uses this for primary key scans; a future on-disk B-tree or secondary indexes can replace the internal access path without changing the trait.

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

**PageVersion:** `1` for the v1 page format. Unknown versions are rejected as page corruption.

V1 development builds do not migrate unversioned page headers. Existing page files without `PageVersion = 1` are rejected as corrupt during snapshot load/recovery.

**No PageLSN:** Because V1 uses a logical WAL with snapshot checkpoints, there is no per-page LSN. Dirty pages are never flushed individually — only as a complete snapshot. The `dirty_txn_id` is tracked in the buffer pool's page descriptor (in memory, not on disk) for future use by `FlushPolicy`. A future physical WAL would add a `PageLSN` field to the page header and enable incremental page flushing.

### Page-Backed Primary-Key Structure

- Table pages store full serialized rows.
- An in-memory `BTreeMap<Key, RowLocation>` maps primary keys to physical page slots.
- `RowLocation` stores `file_id`, `page_num`, and `slot_num`.
- On startup or after snapshot load, storage rebuilds the directory by scanning known table pages.
- A future on-disk clustered B-tree can replace this internal access path without changing the public storage traits.

### Row Serialization

```
[null_bitmap][col1_data][col2_data]...
```

- `INTEGER`: 8 bytes, little-endian i64
- `TEXT`: 4-byte length prefix + UTF-8 bytes
- `BOOLEAN`: 1 byte
- `NULL`: represented in the null bitmap, no data bytes

### File Layout

Files are named by stable numeric ID, not by user-visible names. This avoids rename issues (future `ALTER TABLE RENAME`), filesystem-unsafe characters in table names, and name collisions.

**Snapshot files (written only during checkpoint):**
- `data/snap_<generation>/table_<TableId>.tbl` — table data snapshot
- `data/snap_<generation>/catalog.dat` — catalog metadata snapshot

**Generation:** A monotonically increasing integer. Each completed checkpoint writes to a new generation directory.

**Manifest (the single source of truth for which snapshot is current):**
- `data/manifest.dat` — contains a versioned binary envelope with magic `SGMF`, manifest version, payload length, CRC32 over the exact stored JSON payload bytes, and a payload containing current generation number, checkpoint LSN, and sorted table IDs

The manifest is updated atomically via write-to-temp + rename (atomic on POSIX). Recovery reads the manifest to find the current snapshot. See Snapshot Checkpoint below for the full protocol.

**Other files:**
- `data/wal.dat` — write-ahead log (append-only)

The catalog maps `TableId` → file path via the manifest. Table names are purely a catalog-level concept — the storage engine only sees IDs and file paths.

Each table file contains slotted data pages. Files grow by appending new pages.

## 8. Buffer Pool

The `buffer` crate manages a fixed-size pool of in-memory page frames.

### Trait

```rust
pub trait BufferPool: Send + Sync {
    /// Fetch a page for reading. Returns a guard that unpins on drop.
    fn read_page(&self, file_id: FileId, page_num: PageNum) -> Result<PageReadGuard>;

    /// Fetch a page for writing. txn_id identifies the active statement.
    /// On the first write to a page by this txn_id, the buffer pool saves a
    /// before-image (copy of the page data before mutation). This enables
    /// rollback if the statement fails.
    /// Returns a guard that unpins on drop and automatically marks dirty.
    fn write_page(&self, file_id: FileId, page_num: PageNum, txn_id: u64) -> Result<PageWriteGuard>;

    /// Allocate a new page in the given file, return it locked for writing.
    /// The returned PageWriteGuard exposes page_num() for row-location tracking.
    fn new_page(&self, file_id: FileId, txn_id: u64) -> Result<PageWriteGuard>;

    /// Load an exact clean page during snapshot loading.
    fn load_page(&self, file_id: FileId, page_num: PageNum, data: PageData) -> Result<()>;

    /// Rollback: restore all pages modified by this txn_id to their before-images,
    /// and invalidate/free all pages allocated by this txn_id via new_page().
    /// Discards both tracking structures after restore.
    fn rollback(&self, txn_id: u64) -> Result<()>;

    /// Commit: discard before-images and new-page tracking for this txn_id.
    /// Changes are now permanent and eligible for future snapshots.
    fn commit(&self, txn_id: u64) -> Result<()>;

    /// Iterate all frames (for snapshot writing). Returns (file_id, page_num, data, is_dirty).
    /// Server checkpoint composition overlays these pages on clean snapshot pages.
    fn iter_pages(&self) -> Result<Box<dyn Iterator<Item = PageInfo>>>;

    /// Mark all dirty pages as clean (called after a successful snapshot commit).
    fn mark_all_clean(&self) -> Result<()>;
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
- **Page descriptor:** Tracks `(file_id, page_number)`, pin count, dirty flag, reference bit, `dirty_txn_id` (the txn that last dirtied it), and `dirty_since_snapshot` flag (true if modified since the last snapshot — distinct from "currently being modified by an active txn").
- **Before-image store:** Per active `txn_id`, two tracking structures:
  - `before_images: HashMap<(FileId, PageNum), PageData>` — on the first `write_page` call for an existing page by a `txn_id`, the current page data is copied here.
  - `new_pages: Vec<(FileId, PageNum)>` — pages allocated via `new_page` by this `txn_id`.
  
  On `rollback(txn_id)`: before-images are restored (existing pages return to pre-statement state) and newly allocated pages are invalidated/freed. On `commit(txn_id)`: both tracking structures are discarded. This correctly handles the case where a page was already dirtied by a prior committed txn (rollback restores to the post-prior-commit state, not the snapshot state) and also handles failed inserts that allocated new pages before the error.
- **PageLoader:** The buffer pool is constructed with an `Arc<dyn PageLoader>`:

```rust
pub trait PageLoader: Send + Sync {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>>;
}
```

On a `read_page` miss, the pool asks the loader for a clean page. `Some(data)` is inserted as a clean frame; `None` returns `ErrorKind::Storage` / `SqlState::InternalError` with message `page not found`; loader I/O failures propagate as `ErrorKind::Io`. In production, the server supplies a `SnapshotPageLoader` that wraps `SnapshotManager::current_table_pages(file_id as TableId)` and returns the matching page when present. `MemoryBufferPool::empty(frame_count)` is a test helper using a never-flush policy and a no-op loader that returns `Ok(None)`.

- **FlushPolicy:** The buffer pool is constructed with a `Box<dyn FlushPolicy>`. Before evicting any dirty page, it calls `flush_policy.can_flush(&PageFlushInfo { dirty_txn_id, page_lsn: None })`. **V1's `FlushPolicy` always returns `false`** — dirty pages are never flushed except during snapshot checkpoint. The trait exists so a future physical WAL can enable incremental flushing (populating `page_lsn` and checking WAL durability).

### Eviction: Clock Algorithm (Single-Bit)

- Clock hand sweeps through frames
- Each frame has a reference bit, set on access
- On sweep: if bit is 1, clear to 0 and skip. If bit is 0 and unpinned, check dirty flag.
- **If clean:** evict immediately. The page can be re-read from the table file (last snapshot) if needed later.
- **If dirty:** skip. V1 never evicts dirty pages — they must stay in memory until the next snapshot checkpoint clears their dirty flag.
- If all frames are dirty and pinned, the buffer pool returns an error (out of frames). This is the V1 memory limitation.

### V1 Limitation: Working Set Must Fit in Buffer Pool

Between snapshots, all dirty pages must remain in memory. With V1's single-writer autocommit:
- Each statement dirtys a modest number of pages
- After commit, pages are dirty but committed — they stay in memory until the next snapshot
- Snapshots clear all dirty flags, making those frames eligible for eviction again
- The buffer pool default (1024 frames = 8MB) is adequate for small-to-medium datasets
- For larger datasets, increase the buffer pool size or snapshot more frequently

### Concurrency

- Frame-level read/write latches managed by the page guards (multiple concurrent readers, exclusive writer)
- Page table mapping `(file_id, page_num)` to frame protected by a separate latch
- Multiple threads can read different pages concurrently

## 9. Snapshot Manager

The snapshot manager owns the on-disk snapshot lifecycle: writing new snapshots, loading existing ones, and managing the manifest. It lives in the dedicated `snapshot` crate and operates on page data supplied by the buffer pool and catalog through server checkpoint orchestration.

### Trait

```rust
pub struct SnapshotMetadata {
    pub generation: u64,
    pub checkpoint_lsn: Lsn,
    pub tables: Vec<TableId>,
}

pub struct LoadedSnapshot {
    pub metadata: SnapshotMetadata,
    pub catalog_bytes: Vec<u8>,
}

pub struct SnapshotPage {
    pub page_num: PageNum,
    pub data: PageData,
}

pub trait SnapshotManager: Send + Sync {
    /// Load the current snapshot from the manifest.
    /// Returns metadata and catalog bytes describing what was loaded, or None if no snapshot exists.
    /// Populates the buffer pool with pages from the snapshot files.
    fn load_current(&self, buffer_pool: &dyn BufferPool) -> Result<Option<LoadedSnapshot>>;

    /// Read clean page-numbered data for one table from the current snapshot.
    /// Returns an empty vector when no current snapshot/table file exists.
    fn current_table_pages(&self, table: TableId) -> Result<Vec<SnapshotPage>>;

    /// Begin writing a new snapshot. Returns a writer that accepts pages.
    fn begin_snapshot(&self) -> Result<SnapshotWriter>;

    /// Commit a completed snapshot: fsync files, atomically swap manifest.
    /// After this returns, the new snapshot is the current one.
    fn commit_snapshot(&self, writer: SnapshotWriter, checkpoint_lsn: Lsn) -> Result<SnapshotMetadata>;

    /// Delete orphaned snapshot directories (generations other than current).
    fn cleanup_old_snapshots(&self) -> Result<()>;
}
```

Table file names are deterministic (`table_<TableId>.tbl`) and are not separately exposed in `SnapshotMetadata`. The on-disk manifest may store only table IDs because the file name can be derived from the table ID.

Manifest decode validates the binary envelope magic, version, payload length, checksum over the exact stored payload bytes, JSON payload, and strictly ascending table IDs. Legacy JSON-object manifests are rejected rather than migrated in v1 development builds.

### SnapshotWriter

```rust
/// Writes a complete snapshot to a new generation directory.
/// The checkpoint protocol feeds it ALL pages for ALL tables (dirty from
/// the buffer pool, clean from the current snapshot files).
pub struct SnapshotWriter {
    generation: u64,
    // Writes to data/snap_<generation>/
}

impl SnapshotWriter {
    /// Write all pages for a table file.
    pub fn write_table(&mut self, table: TableId, pages: &[SnapshotPage]) -> Result<()>;

    /// Write the catalog snapshot.
    pub fn write_catalog(&mut self, catalog: &[u8]) -> Result<()>;
}
```

The server composes each table file from two sources before calling `write_table`:
- **Dirty pages** — read from the buffer pool (the current in-memory state)
- **Clean pages** — copied from the current snapshot files with `current_table_pages`

`write_table` preserves sparse page numbers. Table files store `u32 page_count`, followed by repeated `u32 page_num` and 8192 bytes of page data.

### checkpoint_lsn

The manifest's `checkpoint_lsn` is the **authoritative WAL replay boundary**. It is the LSN of the last WAL record whose effects are included in this snapshot — i.e., the WAL's high-water mark at the time the snapshot was started.

Recovery reads `checkpoint_lsn` from the manifest and replays all committed WAL records with `LSN > checkpoint_lsn`. The WAL `Checkpoint` record is optional metadata (useful for WAL truncation) but is NOT required for recovery correctness — the manifest is sufficient.

## 10. Write-Ahead Log (WAL)

The `wal` crate ensures durability using a **logical WAL** — records describe operations (insert row, delete key) not page modifications (write bytes at offset). Recovery replays operations through the storage API.

### V1 Durability Model: In-Memory Pages + WAL + Snapshot Checkpoint

A logical WAL combined with in-place mutable table pages has a fundamental problem: a crash can leave on-disk pages half-updated if dirty pages are flushed independently. Logical replay assumes a consistent starting snapshot, so it cannot recover from arbitrary partial page writes.

**V1 solves this by never flushing individual dirty pages during normal operation.** All modified pages stay in the buffer pool (memory). The WAL is the sole source of durability between snapshots. Table files on disk are only ever written as a complete, consistent snapshot during checkpoint.

This gives three invariants:
1. Table files on disk always reflect a complete, consistent page snapshot.
2. The WAL captures all committed operations since that snapshot.
3. Recovery loads the snapshot and replays the WAL — starting from known-good table pages and rebuilt primary-key directories.

**Trade-offs:**
- Simplest correctness model: no partial-flush, no page-level recovery, no PageLSN.
- Working set between snapshots must fit in the buffer pool. With V1's single-writer autocommit, each statement dirtys a modest number of pages, and snapshots clear them.
- Startup replays WAL from last snapshot — bounded by checkpoint frequency.

**Future upgrade paths** (none change the `BufferPool` or `StorageEngine` traits):
- **Atomic snapshot checkpoint** (write to new files, swap manifest) — allows concurrent reads during checkpoint.
- **Physical WAL** (page images/deltas, PageLSN) — enables incremental page flushing between checkpoints, removing the memory constraint.

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
| `Insert` | `TableId`, serialized `Key`, serialized `Row` |
| `Update` | `TableId`, serialized `Key`, serialized new `Row` |
| `Delete` | `TableId`, serialized `Key` |
| `CreateTable` | serialized `TableSchema` (name, columns, primary key) |
| `DropTable` | `TableId` |
| `Commit` | (empty — marks the transaction as committed) |
| `Checkpoint` | generation number and `checkpoint_lsn` — marks a completed snapshot. WAL records before `checkpoint_lsn` can be truncated. |

`txn_id = 0` is reserved for non-transactional system metadata records. V1 uses it only for `Checkpoint`. User statement transaction IDs start at `1`.

### WAL Trait

```rust
pub trait WalManager: Send + Sync {
    /// Append a record to the WAL buffer (not yet durable).
    fn append(&self, record: WalRecord) -> Result<Lsn>;

    /// Flush all buffered WAL records to disk (fsync). Returns the flushed LSN.
    fn flush(&self) -> Result<Lsn>;

    /// Iterate records from a given LSN (for recovery). The iterator is
    /// fallible — a corrupt record mid-replay returns an error.
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;

    /// Iterate only operation records whose transaction has a commit record
    /// after the given LSN. Used by recovery.
    fn replay_committed_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;

    /// Truncate WAL records before the given LSN (after checkpoint).
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;

    /// Query whether a txn_id has a durable or replayed commit record.
    fn is_committed(&self, txn_id: u64) -> bool;

    /// Last LSN known to be durable after fsync.
    fn flushed_lsn(&self) -> Lsn;

    /// Total encoded bytes of retained records whose stored LSN is > lsn.
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;
}
```

`append(record)` always assigns the next monotonically increasing LSN and writes that LSN into the encoded record. Callers may pass `record.lsn = 0`; `append` ignores the caller-provided LSN. Replay preserves the stored LSN from disk.

`replay_from(lsn)` and `replay_committed_from(lsn)` are strictly exclusive: both inspect only records whose stored `record.lsn > lsn`. Recovery passes the manifest `checkpoint_lsn`, so replay starts after the last WAL record whose effects are already included in the snapshot. `replay_committed_from` returns committed logical operation records only (`Insert`, `Update`, `Delete`, `CreateTable`, `DropTable`); it never yields `Commit` or `Checkpoint` metadata records.

`truncate_before(lsn)` may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`. Truncation writes retained records to a temporary WAL, fsyncs it, renames it over the live WAL, and immediately fsyncs the parent directory. If that directory fsync fails, the WAL manager is poisoned and returns the error before reopening the WAL or mutating retained-record in-memory state.

`bytes_after(lsn)` is server checkpoint accounting only. It counts encoded bytes for retained WAL records with stored `LSN > lsn`; if `lsn` predates the retained WAL after truncation, it returns the encoded byte size of all retained records.

### V1 Durability Rules

One rule ensures redo-only recovery is correct:

**No incremental page flushing.** Dirty pages are never flushed to table files during normal operation — not by eviction, not by background threads, not for any reason. The `FlushPolicy` V1 implementation always returns `false`. Only the snapshot checkpoint writes pages to disk, and it writes ALL dirty pages as an atomic unit.

The WAL is the sole source of durability between snapshots:
- On commit, the WAL is flushed through the commit record (`fsync`). The data is durable in the WAL even though no table pages have been written.
- The buffer pool holds all modified pages in memory until the next snapshot.
- Table files on disk reflect the last completed snapshot — always a consistent page set.

This gives a clean invariant: **table files on disk only ever contain a complete, consistent snapshot.**

### V1 Write Protocol

All writes are serialized through the `ConcurrencyController`. The protocol for a single autocommit statement:

1. Acquire exclusive write guard via `controller.begin_write()`
2. Assign a statement-level `txn_id`
3. Execute the statement through the storage engine (which appends WAL records for each logical operation: insert, update, delete). Buffer pool saves before-images on first page touch.
4. If execution fails: `storage.rollback_txn(txn_id)`, `buffer_pool.rollback(txn_id)`, and catalog restore when needed; return error to client and drop write guard if rollback cleanup succeeds. If rollback cleanup fails before the commit record is durable, log the rollback failure, attempt to flush WAL, and exit because the process may contain visible partial statement state.
5. Append a `Commit` record for this `txn_id`
6. Flush WAL through the commit record to disk (`fsync`)
7. The statement is now durable and must not be rolled back or reported as a normal SQL failure
8. `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)` — discard rollback metadata and before-images
9. Drop write guard (releases exclusive lock)
10. Call `record_commit_and_maybe_checkpoint(&components)`; it may acquire its own write guard for a checkpoint
11. Return success to the client

`storage.commit_txn` and `buffer_pool.commit` are cleanup-only in-memory operations and must not perform I/O. For a valid `txn_id`, they should not fail. If either returns an error after WAL flush through the `Commit` record succeeded, the server must not call rollback. It logs the fatal internal error, flushes WAL, and terminates because recovery will replay the durable commit.

Reads acquire a shared read guard via `controller.begin_read()` and proceed concurrently with each other. A write blocks until all read guards are released.

### Failed Statement Rollback

If a write statement errors after mutating pages but before commit (e.g., a constraint violation mid-batch INSERT, or an internal error after allocating a page), dirty pages from that `txn_id` must be rolled back — they must not be visible to subsequent reads or included in the next snapshot.

**V1 policy: before-image rollback.**

The buffer pool saves a before-image (copy of the page data) on the first write to each page by a `txn_id`. On failure, before-images are restored; on success, they are discarded.

**Success path:**
1. `write_page(file, page, txn_id)` — saves before-image on first touch, returns write guard
2. ... (statement executes, modifying pages) ...
3. Append `Commit` record, flush WAL
4. `storage.commit_txn(txn_id)` — discards storage-owned rollback metadata
5. `buffer_pool.commit(txn_id)` — discards before-images (changes are permanent)

**Failure path:**
1. `write_page(file, page, txn_id)` — saves before-image on first touch
2. ... (statement fails mid-execution) ...
3. `storage.rollback_txn(txn_id)` — restores primary-key directories and table metadata
4. `buffer_pool.rollback(txn_id)` — restores all pages to their before-images
5. Catalog restore returns DDL metadata to the pre-statement snapshot when catalog state changed
6. WAL records for this `txn_id` remain but have no `Commit` — ignored by recovery
7. Error returned to client

If any rollback cleanup step fails before the commit record is durable, the server treats process state as unsafe: it logs the rollback failure, attempts to flush WAL, and exits instead of returning to service.

**Why before-images, not snapshot reload:** A page may have been dirtied by a prior *committed* transaction that has not yet been snapshotted. Reloading from the snapshot file would lose that committed change. Before-images capture the page state *at the moment this txn_id first touched it*, which correctly preserves prior committed modifications.

**Memory cost:** One 8KB copy per page touched by the active statement, held only for the statement duration. With V1's single-writer autocommit, this is bounded by the number of pages a single statement modifies.

### Snapshot Checkpoint

The checkpoint writes a complete, consistent snapshot to **new files** via the `SnapshotManager`, then atomically swaps the manifest. The previous snapshot remains intact until the new one is committed. Crash-safe at every step.

**Checkpoint protocol:**

1. Acquire exclusive write guard (ensures no statement is in-flight, all commits are final)
2. Record `checkpoint_lsn` = current WAL high-water mark (the last flushed LSN)
3. `let writer = snapshot_manager.begin_snapshot()`
4. For each live catalog table: compose pages (dirty from buffer pool overlaid on clean pages from `current_table_pages`) and write via `writer.write_table(table_id, pages)`
5. Serialize catalog and write via `writer.write_catalog(bytes)`
6. `let metadata = snapshot_manager.commit_snapshot(writer, checkpoint_lsn)` — this fsyncs all files, writes the manifest atomically, fsyncs the directory, and returns the durable manifest metadata
7. `buffer_pool.mark_all_clean()` — all pages are now reflected in the snapshot
8. Append `WalRecord { txn_id: 0, kind: Checkpoint { generation: metadata.generation, checkpoint_lsn } }` record to WAL (metadata is not required for recovery, but v1 writes it for observability and WAL tests)
9. Flush WAL, then truncate WAL before `checkpoint_lsn`; WAL truncation fsyncs the replacement rename in the parent directory before reopening the WAL or replacing retained-record in-memory state
10. `snapshot_manager.cleanup_old_snapshots()`
11. Drop write guard

**Crash safety analysis:**
- Crash during steps 3-5: `commit_snapshot` was never called. Manifest still points to the old snapshot. The partial new generation directory is an orphan — cleaned up on recovery by `cleanup_old_snapshots()`.
- Crash during step 6 (inside `commit_snapshot`):
  - Before manifest rename: old manifest intact, orphan directory cleaned up.
  - During manifest rename: either old or new manifest survives — both are valid.
  - After manifest rename, before fsync: new manifest may or may not be durable. If lost, old manifest is restored by filesystem recovery — still valid.
- Crash during steps 7-10: new manifest is committed. Old snapshot may not be deleted yet — harmless, cleaned up on next startup.

**Checkpoint frequency:** Triggered by configurable thresholds — every N committed statements or M bytes of WAL. `CheckpointState.last_checkpoint_lsn` starts from the loaded manifest checkpoint LSN, and `CheckpointState.commits_since_checkpoint` starts at `0`. After each successful write statement and after its statement guard is dropped, server calls `record_commit_and_maybe_checkpoint(&components)`, which increments the commit counter and triggers `run_checkpoint(&components)` when `commits_since_checkpoint >= config.checkpoint_every_n_commits` or `wal.bytes_after(last_checkpoint_lsn)? >= config.checkpoint_wal_bytes`. A successful checkpoint stores the new checkpoint LSN and resets the commit counter to `0`. Checkpoint is also triggered on clean shutdown. More frequent checkpoints mean shorter WAL replay on startup but more I/O and disk space (two snapshot generations coexist briefly).

### Crash Recovery (REDO Only for V1)

The manifest always points to a complete, consistent snapshot. Recovery loads it and replays committed WAL records.

**Recovery uses a separate API** so that replayed operations do not re-append to the WAL:

```rust
/// Applied during crash recovery. Replays a logical WAL record directly
/// to storage without appending new WAL records.
pub trait RecoveryOperations: Send + Sync {
    fn apply_insert(&self, table: TableId, key: Key, row: Row) -> Result<()>;
    fn apply_update(&self, table: TableId, key: Key, row: Row) -> Result<()>;
    fn apply_delete(&self, table: TableId, key: Key) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, table: TableId) -> Result<()>;
}
```

The `StorageEngine` implementor also implements `RecoveryOperations`. The normal `insert`/`update`/`delete` methods append WAL records; the `apply_*` methods modify pages directly without WAL interaction.

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

`open` stores shared `Arc` handles to the buffer pool and WAL manager and initializes empty table metadata plus empty in-memory primary-key directories. It does not read schemas from disk; server startup installs catalog schemas explicitly with `install_schemas` after loading the catalog snapshot.

**Recovery procedure** (driven by the server startup sequence):

1. `snapshot_manager.load_current(buffer_pool)` — reads manifest, loads snapshot pages into the buffer pool, and returns catalog bytes plus `checkpoint_lsn` from the manifest (the authoritative replay boundary). If no manifest exists: fresh database.
2. Initialize storage engine in recovery mode and catalog from snapshot
3. Call `storage.install_schemas(catalog.list_tables()?)` and `storage.rebuild_directories()`
4. Scan the WAL forward, collecting all `txn_id`s with a `Commit` record where `LSN > checkpoint_lsn`
5. Replay committed WAL records in LSN order with `WalManager::replay_committed_from`, calling `RecoveryOperations::apply_*`
6. Discard records for uncommitted transactions — they have no `Commit` record
7. Clean up orphaned snapshot directories
8. If records were replayed: trigger checkpoint to persist as a new snapshot
9. Switch to normal mode with `storage.set_mode(StorageMode::Normal)`

**No idempotency concerns:** The snapshot reflects exactly the state at `checkpoint_lsn`. V1 does not flush pages between snapshots. WAL records after `checkpoint_lsn` have NOT been applied to the snapshot files. Each replays exactly once.

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

The catalog is included in the snapshot checkpoint: `data/snap_<gen>/catalog.dat`. Loaded into memory on startup from the current snapshot. All reads from the in-memory copy. Mutations update memory; persistence happens at the next checkpoint. Between checkpoints, the WAL ensures catalog changes (CREATE/DROP TABLE) are durable.

### WAL Integration

`CREATE TABLE` and `DROP TABLE` are logged to the WAL. On crash recovery, the catalog is rebuilt by loading the snapshot version and replaying catalog-related WAL records.

### Concurrency

Wrapped in `RwLock`. Reads take a read lock. DDL takes a write lock. DDL is infrequent so this is not a bottleneck.

## 11. Server & Connection Management

The `server` crate is the binary entry point.

### Startup Sequence

1. Load configuration (data directory, port, buffer pool size)
2. Initialize snapshot manager
3. Create server-owned `SnapshotPageLoader` from the snapshot manager
4. Initialize buffer pool with configured frames, never-flush policy, and snapshot page loader
5. Initialize WAL — open or create `data/wal.dat`
6. Load snapshot: `snapshot_manager.load_current(buffer_pool)` — reads manifest, loads table files into the buffer pool, and returns `LoadedSnapshot` with catalog bytes and `checkpoint_lsn`.
7. Initialize storage engine in **recovery mode** with `PageBackedStorageEngine::open(buffer_pool.clone(), wal.clone(), StorageMode::Recovery)`
8. Initialize catalog from loaded snapshot data
9. Call `storage.install_schemas(catalog.list_tables()?)` and `storage.rebuild_directories()`
10. Replay committed WAL records with `LSN > checkpoint_lsn` through `WalManager::replay_committed_from` and `RecoveryOperations` (uses storage engine in recovery mode — modifies pages and primary-key directories without appending WAL records)
11. Build `ServerComponents` with catalog, storage, buffer pool, WAL, snapshot manager, concurrency controller, shutdown state, checkpoint state initialized from the loaded manifest checkpoint LSN, and `next_txn_id` initialized to one greater than the maximum retained user WAL `txn_id`.
12. `snapshot_manager.cleanup_old_snapshots()`
13. If WAL records were replayed: trigger checkpoint with `run_checkpoint(&components)` to persist replayed changes as a new snapshot
14. Switch storage engine to **normal mode** with `storage.set_mode(StorageMode::Normal)` (WAL appending enabled)
15. Construct `QueryService` from `components`
16. Start Tokio runtime, bind TCP listener (default port 5433)

Steps 6-10 use the storage engine's `RecoveryOperations` trait, which requires the buffer pool and page/directory logic but does not append to the WAL. Recovery computes `next_txn_id` by scanning all retained records from `WalManager::replay_from(checkpoint_lsn)`, including committed operations, uncommitted operations, and `Commit` records, while ignoring `txn_id = 0` checkpoint metadata. `next_txn_id` starts at `max_txn_id + 1`, or `1` when no user transaction records remain. If the maximum retained user transaction ID is `u64::MAX`, startup fails with a structured WAL/internal error instead of wrapping or saturating the next transaction ID. Step 14 transitions to normal operation where `StorageEngine` methods append WAL records.

The server binary accepts `--data-dir <PATH>`, `--port <PORT>`, `--buffer-pool-frames <N>`, `--checkpoint-every-n-commits <N>`, `--checkpoint-wal-bytes <BYTES>`, `--shutdown-timeout-ms <MS>`, and `--help`. Defaults are `./data`, `5433`, `1024`, `100`, `67108864`, and `30000`. V1 parses these flags with `std::env::args`; `--port` accepts `1..=65535`, all other numeric flags must be positive nonzero integers, and invalid input prints usage to stderr and exits with code `2`.

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
- **Read-write statements** (`INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`): server query orchestration parses SQL to classify the statement, calls `begin_write()`, receives a write guard, binds and plans, allocates the statement `txn_id`, then invokes `QueryEngine`. Blocks until all other guards are released. Writes are fully serialized.
- The guard is held for the entire statement lifetime. Dirty pages are never flushed between snapshots (V1's `FlushPolicy` always returns `false`), so there is no risk of partially-committed data reaching disk.

**V1 implementation:** The concrete `ConcurrencyController` is an `RwLock`. `begin_read()` acquires a shared lock, `begin_write()` acquires an exclusive lock. This is the foundation for safe page mutation, DDL, concurrent scans, and redo-only recovery.

**Other latches:**
- **Buffer pool:** Frame-level read/write latches managed by page guards.
- **Catalog:** Internal `RwLock` (reads concurrent, DDL exclusive). The catalog's own lock is separate from the `ConcurrencyController` — the catalog lock protects metadata consistency, while the `ConcurrencyController` protects statement-level isolation.
- **WAL appends:** Serialized by the write guard (no separate WAL mutex needed).

This is intentionally simple. The exclusive write guard limits write throughput to one statement at a time, but it makes the durability and recovery model correct and safe for page and primary-key-directory modifications. Future MVCC replaces the `ConcurrencyController` implementation with row-level concurrency control while preserving the server-facing orchestration API.

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
}
```

Loaded from command-line args only in V1. No environment-variable or config-file loading in V1.

## 12. Future Work (Designed For, Not Implemented)

- **MVCC / Transactions:** `StatementContext` carries `txn_id` and is extensible for snapshot visibility. The `ConcurrencyController` trait returns owned guards so a simple `RwLock` implementation can later be swapped for a transaction manager. WAL record format includes `TxnID`.
- **Secondary Indexes:** `IndexId` type defined, `KeyRange` supports range scans, `.idx` file extension reserved. Storage engine can add `index_scan` method. Catalog can add `IndexSchema`.
- **Cost-Based Optimizer:** `LogicalPlan` → `PhysicalPlan` boundary exists. A cost-based optimizer slots between them, choosing physical access methods and join algorithms without changing the executor.
- **Vectorized Execution:** `PlanExecutor::next_batch()` is defined with a default implementation. A vectorized engine overrides it with columnar batch processing.
- **INSERT ... SELECT:** `InsertSource::Query` variant exists in the AST. The logical/physical plans already model inserts as `source: Box<LogicalPlan>` / `source: Box<PhysicalPlan>`, so this can be enabled later by binding query sources.
- **Custom Wire Protocol:** `ProtocolCodec` and `ConnectionState` traits are protocol-agnostic. A custom protocol implements these traits.
- **Physical WAL + Incremental Flushing:** V1 keeps all dirty pages in memory and flushes only during snapshot checkpoints (manifest-based, crash-safe). A future physical WAL logs page images/deltas, adds `PageLSN` to the page header, and enables incremental dirty-page flushing between checkpoints — removing the memory limitation. The `FlushPolicy` trait already has the right interface; the V1 implementation just needs to return `true` for committed pages instead of always `false`. The `WalManager` and `BufferPool` traits abstract this change.
- **Incremental Checkpoints:** V1 writes ALL pages during every checkpoint (full snapshot). A future version could write only dirty pages, using the manifest to compose a logical snapshot from multiple generations. This reduces checkpoint I/O for large datasets.
- **Additional Data Types:** `DataType` and `Value` enums are extensible. Row serialization format supports new types via the null bitmap + column data pattern.
