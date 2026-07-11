# `common` Crate Specification

**Date:** 2026-07-04
**Status:** Living crate contract

## Purpose

`common` defines stable cross-crate types and small traits that must not depend on implementation crates. It is the leaf crate for the workspace.

## Owns

- Stable identifiers: `TableId`, `ColumnId`, `IndexId`, `SequenceId`, `BindingId`, `FileId`, `PageNum`, `Lsn`.
- SQL values and row envelopes: `Value`, `Row`, `Key`, `StoredRow`, `ExecRow`, `RowIdentity`.
- The shared boolean-text decoder `parse_bool_text(&str) -> Option<bool>`
  (PostgreSQL `boolin` accept-set), reused by the `protocol` extended-query
  parameter path and the `COPY` import path so both share one accept-set; each
  caller maps `None` to its own SQLSTATE.
- Schema description types: `DataType`, `ParsedColumnDef`, `ColumnDef`,
  `ColumnInfo`, `TableSchema`, `IndexSchema`, `ViewColumn`,
  `ViewDependency`, `ViewSchema`, `SequenceOptions`, and `SequenceSchema`.
- Relation-generation catalog handoff types: `TruncateTablePlan` and
  `TruncateCatalogUpdate`.
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
  `classify_unique_conflict -> UniqueConflict` (`None`/`Violation`/`WouldBlock`,
  which splits a definite duplicate `23505` from an in-flight-other the writer
  blocks on — `docs/specs/deadlock.md`), and the write-write row-lock check
  `write_conflict -> WriteConflict` (`Proceed`/`Conflict`/`WouldBlock`).
- The pure VACUUM reclaimability oracle `is_dead_to_all` (see
  `docs/specs/mvcc.md` §9), the sibling of `is_visible`: it answers "is this
  version dead to **every** snapshot?" against a single scalar GC `horizon`, used
  by VACUUM (Milestone F) rather than by snapshot-relative reads.
- Cross-cutting traits: `FlushPolicy`, `ConcurrencyController`, `TxnStatusView`.
- The scalar function-dispatch registry: a table pairing each built-in scalar
  function's bind-time signature check with its run-time evaluator, so a function
  is defined once and consulted by both `planner` (binding) and `executor`
  (evaluation). See "Scalar Function Registry" below.

## Public Types

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

pub struct RowId {
    pub page_num: PageNum,
    pub slot_num: u16,
}

pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(OrderedF64), // DOUBLE PRECISION (total-order f64 wrapper)
    Real(OrderedF32),  // REAL (total-order f32 wrapper)
    Numeric(Decimal),  // NUMERIC/DECIMAL (exact decimal; compares by value)
    Text(String),
    Date(i64),       // days from the Unix epoch (1970-01-01)
    Timestamp(i64),  // microseconds from the Unix epoch (no time zone)
    Time(i64),       // microseconds since midnight (no time zone)
    TimestampTz(i64),// microseconds from the Unix epoch, UTC-normalized
    Interval(Interval),// months/days/micros; compares by canonical estimate
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
grouping. `Value::Real` wraps `f32` the same way in `OrderedF32` (with
`format_real`/`parse_real`). The `datetime` module provides the proleptic Gregorian calendar
conversions and the `YYYY-MM-DD` / `YYYY-MM-DD HH:MM:SS[.ffffff]` parse/format
helpers (`days_from_civil`, `civil_from_days`, `parse_date`, `format_date`,
`parse_timestamp`, `format_timestamp`, `parse_time`, `format_time`, `parse_timestamptz` (offset→UTC), `format_timestamptz` (UTC `+00`)). `Value::Interval`
wraps the `interval` module's `Interval { months, days, micros }`, which compares
and hashes by a canonical estimate (a month = 30 days, a day = 24 hours, so
`1 mon` == `30 days`) while storing the components exactly; it provides
`parse_interval`/`format_interval` and the PostgreSQL binary codec. The `bytea` module provides the hex
`\x...` parse/format helpers (`parse_hex`, `format_hex`, hex-only — no legacy
escape); the `uuid` module provides the canonical `8-4-4-4-12` parse/format
helpers (`parse_uuid` lenient, `format_uuid` canonical lowercase); the `float`
module provides the `format_double` / `parse_double` helpers (round-trippable
text: fixed-point for moderate magnitudes, `e±NN` scientific for extreme
exponents, and `Infinity`/`-Infinity`/`NaN` for non-finite values). `Value::Numeric`
wraps `rust_decimal::Decimal` (re-exported as `common::Decimal`), an exact base-10
value that compares and hashes *by value* (`1.0` == `1.00`) while carrying its own
display scale; the `numeric` module provides `parse_numeric`/`format_numeric`,
`apply_typmod` (round to a `NUMERIC(p, s)` modifier), the PostgreSQL base-10000
binary codec (`to_pg_binary`/`from_pg_binary`), and `Decimal` conversions.
All are shared by the parser, executor, protocol, and COPY paths; there is no
external date/time/uuid/float dependency (`rust_decimal` backs `NUMERIC`).

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

`Value` ordering is used for B-tree keys. The ordering is total and deterministic, following the enum's declaration order: `Null < Boolean < Integer < Float < Real < Numeric < Text < Date < Timestamp < Time < TimestampTz < Interval < Bytes < Uuid`, with natural ordering inside each variant. Because the derived `Ord` **is** the durable B-tree key ordering, variant order is a durable contract: new variants must be appended at the end of the enum — never inserted or reordered mid-enum — unless the key ordering/encoding is deliberately revisited and migrated (see `docs/specs/rust-style.md`, Serialization and Durable Formats). SQL comparison semantics still apply in expression evaluation; B-tree ordering is a storage ordering.

## Column Lifecycle Types

```rust
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
}

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

pub struct ToastOptionPatch {
    pub mode: Option<ToastMode>,
    pub tuple_target: Option<u32>,
    pub min_value_size: Option<u32>,
    pub compression: Option<ToastCompression>,
}

pub struct TableOptionPatch {
    pub compression: Option<CompressionSetting>,
    pub toast: ToastOptionPatch,
}

pub enum RelationKind {
    User,
    Toast { base_table: TableId },
}

pub enum ParsedDefault {
    Const(Value),
    Nextval(String),
    OwnedNextval(String),
    Serial,
    Expr(String),  // non-constant default, as canonical SQL text
}

pub enum ColumnDefault {
    Const(Value),
    Nextval(SequenceId),
    Expr(String),  // non-constant default, as canonical SQL text
}

pub struct ParsedColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub max_length: Option<u32>,  // VARCHAR(n)/CHAR(n) length; None = unbounded
    pub default: Option<ParsedDefault>,
    pub pg_type: Option<PgType>,  // declared PostgreSQL wire identity
}

pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub max_length: Option<u32>,  // VARCHAR(n)/CHAR(n) length; None = unbounded
    pub default: Option<ColumnDefault>,
    pub pg_type: Option<PgType>,  // persisted declared PostgreSQL wire identity
}

pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub table_id: Option<TableId>,
    pub column_id: Option<ColumnId>,
    pub pg_type: Option<PgType>,  // result-column wire identity
}

pub struct TableSchema {
    pub id: TableId,
    pub storage_id: FileId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
    pub schema_version: u64,
    pub compression: CompressionSetting,
    pub active_dict_id: Option<u32>,
    pub toast: ToastOptions,
    pub toast_table_id: Option<TableId>,
    pub relation_kind: RelationKind,
    pub checks: Vec<String>,  // CHECK constraint expressions, canonical SQL text
}

pub struct IndexSchema {
    pub id: IndexId,
    pub storage_id: FileId,
    pub table: TableId,
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub unique: bool,
    pub constraint: IndexConstraintKind,
}

pub enum IndexConstraintKind {
    None,
    Unique,
    PrimaryKey,
}

pub struct ViewColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub pg_type: Option<PgType>,
}

pub struct ViewDependency {
    pub relation: TableId,
    pub columns: Vec<ColumnId>,
    pub all_columns: bool,       // SELECT * / relation-wide column-set dependency
}

pub struct ViewSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub definition: String,
    pub dependencies: Vec<ViewDependency>,
    pub schema_version: u64,
}

pub struct SequenceOptions {
    pub increment: i64,
    pub start: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
}

pub struct SequenceSchema {
    pub id: SequenceId,
    pub name: String,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start: i64,
    pub cycle: bool,
    pub owned: bool,
    pub last_value: i64,
    pub is_called: bool,
}

pub struct TruncateTablePlan {
    pub table_id: TableId,
    pub new_table_storage_id: FileId,
    pub new_toast_storage_id: Option<(TableId, FileId)>,
    pub new_index_storage_ids: Vec<(IndexId, FileId)>,
}

pub struct TruncateCatalogUpdate {
    pub table: TableSchema,
    pub toast_table: Option<TableSchema>,
    pub indexes: Vec<IndexSchema>,
}
```

`ParsedColumnDef` is parser output and never has IDs. `ColumnDef` is catalog-owned
and has dense IDs within a specific `TableSchema.schema_version`: IDs match the
row slot order and may be renumbered by rewrite-style schema evolution such as
`DROP COLUMN`. Metadata that survives a schema version change must be remapped
by the catalog instead of treating `ColumnId` as a cross-version identity.
`ColumnInfo` describes result columns and may be derived from expressions, so
table/column IDs are optional. `TableSchema.schema_version` and
`ViewSchema.schema_version` start at `1` and increment on public schema metadata
changes. The optional `pg_type` fields are presentation metadata for PostgreSQL
protocol OIDs/typmods only; semantic type checking still uses `DataType`.
Supported presentation-only identities include width/kind refinements
(`int2`/`int4`/`int8`, `varchar`/`bpchar`), catalog `oid`, and catalog
vector/array identities (`int2vector`, `oidvector`, `int2[]`/`_int2`,
`oid[]`/`_oid`) that collapse to existing `Integer` or `Text` semantics.

`TableSchema.id` and `IndexSchema.id` are stable logical catalog identities.
`storage_id` is the current physical relation-generation id used by storage to
derive heap, primary-index, and secondary-index file ids. The field is durable
and serde-defaulted for compatibility; `storage_id == 0` means a legacy catalog
snapshot omitted the field and must be migrated by the catalog load path before
validation or use. Live schemas must never retain `storage_id == 0`. Legacy
catalog migration preserves existing file names by allowing a table relation and
a secondary index to keep the same raw storage id when their old logical ids
matched; storage file-kind bits still make the actual heap/primary/secondary
file ids distinct. Fresh allocations come from one monotonic allocator and do
not intentionally create those cross-kind raw collisions.

`ViewColumn` is the catalog input shape for view output columns before dense
`ColumnId`s are assigned. Stored `ViewSchema.columns` use `ColumnDef` with dense
column IDs and no defaults. `ViewDependency.columns` lists referenced column IDs;
`all_columns = true` represents relation-wide dependencies such as `SELECT *`;
neither specific columns nor `all_columns` represents relation-existence-only
dependencies such as `count(*)`. For compatibility with snapshots written before
`all_columns` existed, deserializing a dependency with the field absent and
`columns = []` treats it as `all_columns = true`; newly serialized
relation-existence dependencies always include `all_columns = false`.

`ToastOptions` is durable per-table policy for storage-private TOAST handling.
It does not change public SQL values: `Value::Text(String)` and
`Value::Bytes(Vec<u8>)` remain fully materialized across parser, binder,
planner, executor, protocol, COPY, indexes, and public storage traits.
`ToastOptions::default_new_table()` is the intended default for newly created
user tables after TOAST storage support is fully wired:
`mode = Auto`, `tuple_target = 2048`, `min_value_size = 1024`,
`compression = ZstdDict`, `active_dict_id = None`.
`ToastOptions::legacy_catalog_default()` is used by serde defaults for catalog
snapshots written before TOAST existed:
`mode = Off`, `tuple_target = 2048`, `min_value_size = 1024`,
`compression = None`, `active_dict_id = None`. `RelationKind::default()` is
`User`. `TableSchema.toast_table_id = None` means no hidden TOAST relation is
known for that table.
Catalog validation rejects TOAST policy values outside the durable bounds:
`tuple_target` must be in `256..=8000`, `min_value_size` must be at least `128`,
and `active_dict_id = Some(0)` is invalid because dictionary id `0` is the
reserved "no dictionary" sentinel.

`ToastOptionPatch` / `TableOptionPatch` are parser-to-binder option carriers, not
durable catalog state. `ToastOptions::apply_patch` implements the SQL merge rule:
omitted options preserve the base options; `toast = aggressive` plus omitted
`toast_min_value_size` stores `AGGRESSIVE_TOAST_MIN_VALUE_SIZE`; explicit
`toast_compression` clears `active_dict_id`.

`needs_toast_relation(schema)`, `toast_relation_name(base_table)`, and
`toast_schema(base, toast_id)` define the hidden TOAST relation metadata shared
by catalog/executor/storage phases. A user table needs a hidden relation when it
has a `TEXT` or `BYTEA` column. The generated relation is named
`"\0toast_<base_table_id>"`, has `(value_id BIGINT, seq INTEGER, data BYTEA)`
with primary key `(value_id, seq)`, uses at-rest page `compression = none`, and
has `RelationKind::Toast { base_table }`.

`IndexSchema` is the catalog-owned secondary-index metadata type. A `unique`
index rejects duplicate non-NULL indexed values (NULLs are distinct); a
non-unique index admits duplicates. On disk every index entry is disambiguated by
the heap TID it points at (see `storage` Secondary Indexes), so no metadata
distinguishes the two beyond the `unique` flag. Secondary indexes have their own
`storage_id`; it is independent of the index's logical `id`.
`constraint` records whether the index backs no SQL constraint, a `UNIQUE`
constraint, or a `PRIMARY KEY` constraint.

`TruncateTablePlan` is the catalog-produced allocation plan for a future
relation-generation swap: it names the logical table plus the fresh physical
storage ids allocated for the base relation, optional hidden TOAST relation, and
all secondary indexes. `TruncateCatalogUpdate` is the publication handoff: it
contains the same schemas after only their `storage_id` fields have changed.

`SequenceOptions` is parser/planner input for `CREATE SEQUENCE`; absent
start/min/max values mean "use the direction-dependent defaults." `SequenceSchema`
is the catalog-owned durable sequence metadata. `last_value`/`is_called` are the
checkpoint baseline for the storage runtime's current sequence state.
`ColumnDefault::Nextval(SequenceId)` is the stored form of
`DEFAULT nextval('<sequence>')` and is evaluated by the executor through the
statement's sequence manager. `ColumnDefault::Expr(String)` / `ParsedDefault::Expr(String)`
are the stored form of a non-constant `DEFAULT` expression, held as canonical SQL
text: the binder re-parses it (`parser::parse_expression`) and binds it against an
empty column scope both at `CREATE TABLE` (to validate its type and reject column
references, aggregates, subqueries, and parameters) and at each `INSERT` (to
evaluate it per omitted row). `ParsedDefault::Serial` is the parse-time marker
for a `SERIAL` family column during `CREATE TABLE`; the executor replaces it with
the internal `ParsedDefault::OwnedNextval(name)` after creating the owned
sequence, and the catalog resolves that internal form to
`ColumnDefault::Nextval(SequenceId)`. User-written defaults use
`ParsedDefault::Nextval(name)` and may not borrow an owned SERIAL sequence.

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
    InvalidSchemaName,
    UndefinedColumn,
    UndefinedObject,
    InvalidColumnReference,
    WrongObjectType,
    DuplicateTable,
    DuplicateCursor,
    DatatypeMismatch,
    DivisionByZero,
    InvalidParameterValue,
    NumericValueOutOfRange,
    StringDataRightTruncation,
    InvalidTextRepresentation,
    BadCopyFileFormat,
    NotNullViolation,
    UniqueViolation,
    CheckViolation,
    CardinalityViolation,
    DependentObjectsStillExist,
    ObjectNotInPrerequisiteState,
    InvalidCursorName,
    QueryCanceled,
    FeatureNotSupported,
    InFailedSqlTransaction,
    NoActiveSqlTransaction,
    InvalidSavepointSpecification,
    ProgramLimitExceeded,
    SerializationFailure,
    DeadlockDetected,
    IoError,
    InternalError,
}

impl SqlState {
    pub fn code(self) -> &'static str;
    pub fn from_code(code: &str) -> Option<Self>;
}

pub type Result<T> = std::result::Result<T, DbError>;
```

All crates return `common::Result<T>`. Crates should map low-level errors into the nearest `ErrorKind` and SQLSTATE at the boundary where context is available.
`SqlState::code` is the single source of truth for PostgreSQL wire SQLSTATE
strings, and `SqlState::from_code` is the reverse parser for known codes.

`DuplicateCursor` maps to `42P03` for a duplicate SQL cursor declaration.
`InvalidCursorName` maps to `34000` for `FETCH`/`CLOSE` of a cursor that is not
open in the current session.

`SqlState::CheckViolation` maps to SQLSTATE `23514`: a proposed row violates a
table's `CHECK` constraint — the constraint expression evaluated to `false` for
the row (a `NULL`/unknown result passes, matching PostgreSQL). `TableSchema.checks`
holds each `CHECK` expression as canonical SQL text (column-level and table-level
checks flattened together); the binder re-parses and binds each against the table's
columns at `CREATE TABLE` (to validate: boolean result, resolvable columns, no
aggregates/subqueries/parameters) and at each `INSERT`/`UPDATE` (to enforce per
row).

`SqlState::UndefinedObject` maps to SQLSTATE `42704`: an object-like name is not
recognized when no more specific relation/column SQLSTATE applies. The server
uses it for `SHOW` of an unknown configuration parameter.

`SqlState::InvalidSchemaName` maps to SQLSTATE `3F000`: a schema-qualified SQL
name referenced a schema that SaguaroDB does not expose.

`SqlState::InFailedSqlTransaction` maps to SQLSTATE `25P02`: a statement other
than `COMMIT`/`ROLLBACK` issued inside an already-failed (`'E'`) transaction block.
The server raises it while gating an aborted transaction block (see
`docs/specs/crates/server.md` and `docs/specs/mvcc.md` §7.2).

`SqlState::NoActiveSqlTransaction` maps to SQLSTATE `25P01`: a savepoint command
(`SAVEPOINT`/`RELEASE`/`ROLLBACK TO`) issued with no open transaction block.
`SqlState::InvalidSavepointSpecification` maps to `3B001`: `RELEASE`/`ROLLBACK TO`
named a savepoint that does not exist. Both are raised on the savepoint path; see
`docs/specs/savepoints.md` §2.

`SqlState::SerializationFailure` maps to SQLSTATE `40001`: a write-write conflict
against a **committed**-superseded version — the losing UPDATE/DELETE finds the
target version's `xmax` row-lock held by a transaction that has committed since this
writer's snapshot (classifier `common::mvcc::write_conflict` → `WriteConflict::
Conflict`). A conflict against an *in-progress* holder no longer maps here:
SaguaroDB now **blocks** on it (`WriteConflict::WouldBlock` / `UniqueConflict::
WouldBlock`) and only surfaces `40001` (or `23505` for a unique key) if the holder
turns out to have committed. See `docs/specs/mvcc.md` §7.3 and `docs/specs/deadlock.md`.

`SqlState::DeadlockDetected` maps to SQLSTATE `40P01`: the timeout-based deadlock
detector found a cycle of blocked writers and aborted a victim (the detecting
waiter). See `docs/specs/deadlock.md`.

`SqlState::InvalidTextRepresentation` maps to SQLSTATE `22P02`: a text field could
not be parsed into its target type. `SqlState::BadCopyFileFormat` maps to SQLSTATE
`22P04`: a `COPY ... FROM` input row is structurally malformed (wrong column count
or an unterminated CSV quote). Both are raised on the `COPY` import path; see
`docs/specs/copy.md` §7.

`SqlState::InvalidParameterValue` maps to SQLSTATE `22023`; sequence DDL uses it
for semantically invalid options such as `INCREMENT BY 0`, `MINVALUE > MAXVALUE`,
or `START` outside the min/max bounds.

`SqlState::ProgramLimitExceeded` maps to SQLSTATE `54000`; storage uses it when
user data exceeds a supported implementation limit, such as a row or varlena
value that cannot fit the supported durable format.

`SqlState::ObjectNotInPrerequisiteState` maps to SQLSTATE `55000`; sequence
`currval` uses it when a connection has not yet called `nextval` or
`setval(..., true)` for that sequence.

`SqlState::DependentObjectsStillExist` maps to SQLSTATE `2BP01`; catalog DDL uses
it when a sequence is still referenced by a column default and cannot be dropped
without a cascade/default-removal feature.

`DbError` exposes convenience constructors used consistently across crates: `DbError::parse(code, message)`, `DbError::plan(code, message)`, `DbError::execute(code, message)`, `DbError::storage(code, message)`, `DbError::wal(code, message)`, `DbError::protocol(code, message)`, `DbError::io(message)`, and `DbError::internal(message)`. Constructors set `kind`, `code`, and `message`; `io` uses `SqlState::IoError`, and `internal` uses `SqlState::InternalError`.

`DbError` derives `thiserror::Error` with `#[error("{message}")]`, so it is a real `std::error::Error` whose `Display` renders the `message` field.

## Statement Context

```rust
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Arc<Snapshot>,
    pub isolation: IsolationLevel,
    pub gc_horizon: u64,
    pub conflict_waiter: Arc<dyn ConflictWaiter>,
    pub cancel: Arc<QueryCancel>,
    pub live_txns: Arc<[TxnId]>,
    pub ssi_tracker: Arc<dyn SsiTracker>,
    pub sequence_manager: Arc<dyn SequenceManager>,
    pub session_sequences: Arc<SessionSequenceState>,
    pub session_info: Arc<SessionInfo>,
    pub system_state: Arc<dyn SystemStateProvider>,
    pub catalog_introspection: Arc<dyn CatalogIntrospectionProvider>,
}
```

`QueryCancel` stores the first `CancelReason` (`UserRequest` or
`StatementTimeout`) atomically until the connection resets it for the next
statement. `check()` maps either reason to `SqlState::QueryCanceled` (`57014`)
with a reason-specific message.

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
consult it. `statement_timestamp_micros` captures the statement start time as UTC
microseconds since the Unix epoch; SQL clock functions (`CURRENT_TIMESTAMP` and
`now()`) read this value so repeated calls within one statement are stable, and
tests may override it with `with_statement_timestamp_micros(...)`. `gc_horizon`
carries the GC horizon (minimum advertised snapshot `xmin`) the server captured
for the statement; it is consumed ONLY by the storage engine's HOT update-path
prune (`docs/specs/mvcc.md` §10 Milestone H3) and defaults to `0` (prune nothing
committed-dead) for read/pre-capture/test contexts, set on write paths via
`StatementContext::with_gc_horizon(gc_horizon)`. A stale/smaller horizon only
prunes less, never unsafely. `conflict_waiter`, `cancel`, `live_txns`, and
`ssi_tracker` carry the server-owned concurrency services documented in
`docs/specs/deadlock.md`, `docs/specs/savepoints.md`, and `docs/specs/ssi.md`.
`sequence_manager` is the runtime sequence implementation used by
`nextval`/`setval` and by `currval`'s execution-time existence check;
`session_sequences` is the per-connection map storing `currval`'s last returned
values. `session_info` carries connection identity (`user`, `database`,
`backend_pid`) for system information functions; it defaults to the single built-in
`saguarodb` database/user with pid `0`. Server connection plumbing installs the
real per-connection value when system information functions are wired into query
execution. `system_state` is the runtime provider for virtual system catalog
session data (`pg_settings`, `pg_stat_activity`, and `current_setting`); default
contexts install an empty no-op provider. `catalog_introspection` is the runtime
provider for PostgreSQL-compatible metadata functions such as `pg_get_indexdef`,
`pg_get_constraintdef`, `pg_table_is_visible`, `to_regclass`, and
`pg_get_serial_sequence`; it accepts only primitive OID/name inputs so `common`
remains a leaf crate. Default contexts install a no-op provider that returns
`Ok(NULL)`/`Ok(FALSE)`/`Ok(pass-through expression text)` as appropriate, and
real providers return `Result` so catalog-read failures propagate instead of
being flattened into "object not found". Default contexts install loud/no-op test
implementations where appropriate. `StatementContext` is `Clone` but not `Copy`.

```rust
pub struct GucSetting {
    pub name: String,
    pub setting: String,
    pub boot_val: String,
    pub reset_val: String,
    pub source: String,
}

pub enum SessionState {
    Active,
    Idle,
    IdleInTransaction,
    IdleInTransactionAborted,
}

pub struct SessionActivityRow {
    pub datid: i32,
    pub datname: String,
    pub pid: i32,
    pub usesysid: i32,
    pub usename: String,
    pub application_name: String,
    pub backend_start: i64,
    pub xact_start: Option<i64>,
    pub query_start: Option<i64>,
    pub state_change: Option<i64>,
    pub state: SessionState,
    pub query: String,
}

pub trait SystemStateProvider: Send + Sync + Debug {
    fn settings(&self) -> Vec<GucSetting>;
    fn setting(&self, name: &str) -> Option<String>;
    fn sessions(&self) -> Vec<SessionActivityRow>;
}

pub trait CatalogIntrospectionProvider: Send + Sync + Debug {
    fn pg_get_indexdef(
        &self,
        index_oid: i64,
        column: Option<i64>,
        pretty: bool,
    ) -> Result<Option<String>>;
    fn pg_get_constraintdef(&self, constraint_oid: i64, pretty: bool) -> Result<Option<String>>;
    fn pg_get_expr(&self, expr: &str, relation_oid: i64, pretty: bool) -> Result<Option<String>>;
    fn pg_get_userbyid(&self, role_oid: i64) -> Result<Option<String>>;
    fn pg_table_is_visible(&self, relation_oid: i64) -> Result<bool>;
    fn to_regclass(&self, name: &str) -> Result<Option<i64>>;
    fn pg_get_serial_sequence(&self, table: &str, column: &str) -> Result<Option<String>>;
}
```

`SystemStateProvider` lives in `common` so `executor` can read virtual-catalog
session data without depending on `server`. Callers that have session/server state
install a provider with `StatementContext::with_system_state`; contexts without
that state use `no_system_state()`, whose methods return empty rows or `None`.
`setting(name)` defaults to a case-insensitive lookup over `settings()`, so
`pg_settings` and `current_setting` stay consistent unless an implementation
deliberately overrides the lookup for efficiency while preserving the same
semantics.

## Scalar Function Registry

`common` owns the built-in scalar function registry so that each function is
defined in one place instead of split across `planner` (binding) and `executor`
(evaluation). Each function is one `ScalarFunction` entry:

```rust
pub struct ScalarFunction {
    pub name: &'static str,
    pub null_handling: NullHandling,
    pub signature: fn(name: &str, args: &[ArgType]) -> Result<DataType>,
    pub eval: fn(ctx: &StatementContext, values: &[Value]) -> Result<Value>,
}
```

`lookup_scalar_function(name) -> Option<&'static ScalarFunction>` resolves a
lowercase name against the table (the sole authority for which scalar functions
exist). The binder builds an `ArgType { data_type, literal }` per bound argument
(the `literal` value is populated only for constants, and consulted only by
`EXTRACT` to validate its field name) and calls `signature`, which validates arity
and argument types and returns the result `DataType`; signature failures are
`ErrorKind::Plan`. Result nullability is not returned by `signature` — it is
derived centrally by `ScalarFunction::result_nullable`: a `Propagate` function's
result is nullable when any argument is, `Nullable` and `EvaluateNullable`
functions are always nullable, and a `NeverNull` function's result is never
nullable. `Nullable` functions short-circuit to `NULL` on `NULL` arguments but
may also return `NULL` for non-`NULL` metadata misses. `EvaluateNullable`
functions are always evaluated and own their NULL semantics (for example,
`format_type(oid, NULL)` treats the typmod as omitted, while
`format_type(NULL, typmod)` returns `NULL`). The executor evaluates the arguments,
applies the same NULL policy (`Propagate`/`Nullable` short-circuit to `NULL` when
any argument is `NULL`; `EvaluateNullable`/`NeverNull` are always evaluated), and
calls `eval`; evaluation domain failures are `ErrorKind::Execute`. `NeverNull`
covers `CONCAT` (which ignores `NULL` arguments) and the zero-argument system
information functions. The registry also exposes
`scalar_function_arg_hint(name, arity, index)` for functions whose registered
signature admits exactly one possible argument type at `index` for that arity
(used by the binder to type untyped `NULL` literals and placeholders). Hints are
inferred from the registered signature instead of maintained as a parallel
name/arity table; ambiguous arguments, such as numeric functions accepting both
`INTEGER` and `DOUBLE`, return `None`. The inference uses exhaustive search only
for small arities and a cheap uniform-argument fallback for higher-arity
signatures such as variadic `CONCAT`.

The registry holds the ordinary scalar functions (text, math, string,
`SUBSTRING`, `EXTRACT`), statement clock functions, system information functions,
and PostgreSQL-compatible catalog introspection/probe functions; their
per-function signatures and semantics are specified in
`docs/specs/crates/planner.md` (binding) and `docs/specs/crates/executor.md`
(evaluation). Aggregate functions, sequence functions (`nextval`/`currval`/`setval`),
and the NULL-folding forms `COALESCE`/`NULLIF` are intentionally not registry
entries: they have their own bound representations and binding rules.

`common` also owns the static `pg_proc` compatibility metadata for registered
built-ins/probe helpers (`PgProcCatalogEntry` plus
`pg_proc_catalog_entries()`/`pg_proc_catalog_entry()`), so executor system scans,
planner output metadata, and function-definition helpers use one
OID/name/signature source. `scalar_function_result_pg_type(name, arity,
data_type)` derives an unambiguous result wire identity from that table when one
exists, and `scalar_function_arg_pg_type(name, arity, index)` does the same for
argument positions so the planner can preserve inferred OID parameter wire
metadata. Definition helpers such as `pg_get_function_arguments`,
`pg_get_function_result`, `pg_get_functiondef`, `pg_function_is_visible`, and
`oidvectortypes` are
compatibility helpers over this static table; they do not imply user-defined
function support. The metadata must cover every registered scalar-function name,
and entries must use decodable PostgreSQL type OIDs; `concat` is advertised as a
variadic text helper via `provariadic`, and `oidvectortypes` advertises an
`oidvector` argument even though SaguaroDB stores the catalog value as text. Math
function metadata includes the integer/double overload rows accepted by the
runtime signatures. Privilege-probe metadata includes text-shaped rows plus
common OID-shaped overloads so client reflection can discover the object-OID
forms that the runtime signature checker already accepts.

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

pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead, // = snapshot isolation
    Serializable,   // = SSI: the Repeatable Read snapshot plus rw-conflict
                    //   tracking and dangerous-structure detection (docs/specs/ssi.md)
}
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
  `TxnStatusView` takes per probe). Storage scan and HOT-chain traversal paths use
  it as the production MVCC visibility predicate.
- `is_dead_to_all` is the VACUUM-side sibling of `is_visible` (`mvcc.md` §9): it
  returns true iff the version is dead to **every** possible snapshot, given the GC
  `horizon` (the oldest still-running xid). Reclaimable iff **either** the creator
  aborted (`XMIN_ABORTED`, or `status(xmin) == Aborted`) — **no age requirement**,
  an aborted creator is universally invisible — **or** it is committed-deleted
  below the horizon (`xmax != 0`, settled-committed via `XMAX_COMMITTED` or
  `status(xmax) == Committed`, **and** `xmax < horizon`, strict). A live committed
  version, an aborted/in-progress deleter, or a committed delete with
  `xmax >= horizon` is not reclaimable. Pure and honours the same `infomask` hint
  bits to skip CLOG probes; takes a scalar `horizon` rather than a `Snapshot`.
  Storage uses it for VACUUM/prune decisions and cleanup of dead versions below
  the active snapshot horizon.

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

The controller is the **page/WAL-writer-vs-checkpoint** coordination primitive. DML, DDL, and WAL-writing maintenance take the SHARED side; checkpoint takes the EXCLUSIVE side and drains them. Logical table access is coordinated separately by the server table-lock manager.

```rust
pub trait ConcurrencyController: Send + Sync {
    /// SHARED writer guard — many concurrent page/WAL writers; blocks behind checkpoint.
    fn begin_writer(&self) -> Result<WriteGuard>;
    fn begin_writer_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard>;
    /// EXCLUSIVE guard — drains all writers, then runs alone.
    fn begin_checkpoint(&self) -> Result<CheckpointGuard>;
    fn begin_checkpoint_cancelable(&self, cancel: &QueryCancel) -> Result<CheckpointGuard>;
    /// SHARED guard for a non-writing exclusion participant (default = begin_writer).
    fn begin_shared(&self) -> Result<WriteGuard> { self.begin_writer() }
    fn begin_shared_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard>;
}

pub struct RwLockConcurrencyController { /* lock: Arc<parking_lot::RwLock<()>> */ }

impl RwLockConcurrencyController {
    pub fn new() -> Self;
}

impl Default for RwLockConcurrencyController { /* delegates to new() */ }

pub struct WriteGuard { /* owned ArcRwLockReadGuard — the SHARED side */ }
pub struct CheckpointGuard { /* owned ArcRwLockWriteGuard — the EXCLUSIVE side */ }
```

The implementation holds a `parking_lot::RwLock` in an `Arc` and hands out owned guards. DML, DDL, WAL-writing maintenance, and any explicit transaction before its first retained object lock acquire the SHARED side (`begin_writer` → `read_arc()`); table/sequence locks, the catalog-DDL mutex, row conflicts, and storage latches provide finer exclusion. Autocommit readers take no controller guard. Checkpoint alone acquires the EXCLUSIVE side. Foreground SQL uses cancelable shared/checkpoint forms that poll timed acquisition and return the token's reason-specific `QueryCanceled` error; background checkpoint callers retain the unconditional forms. The shared side is re-entrant, and guards are owned to keep the trait object-safe.

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
