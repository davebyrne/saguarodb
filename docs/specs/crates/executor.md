# `executor` Crate Specification

**Date:** 2026-05-03
**Status:** Living crate contract

DDL physical plans carry resolved schema ids. CREATE TABLE/INDEX/SEQUENCE/VIEW
uses schema-scoped catalog creation, CREATE/DROP SCHEMA emits the corresponding
logical WAL through `SchemaOperations`, and conditional drops resolve within the
qualified schema. CREATE VIEW persists the effective definition search path.
`pg_namespace` includes user schemas and `pg_class.relnamespace` reflects each
user object's schema.

## Purpose

`executor` evaluates `PhysicalPlan` values. It owns physical operators, expression evaluation, DML/DDL orchestration, and conversion to `ExecutionResult`.

## Depends On

- `common`
- `catalog`
- `storage`
- `planner` plan types
- `spill` query-local memory budgets, temporary tapes, and external sorting

## Execution Model

Execution is Volcano-style pull execution. Operators return `ExecRow`, not plain `Row`, so DML identity survives filters and projections.

```rust
pub trait PlanExecutor {
    fn output_schema(&self) -> &[ColumnInfo];
    fn open(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<ExecRow>>;
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<ExecRow>>;
    fn close(&mut self) -> Result<()>;
}
```

Default `next_batch` calls `next` in a loop. Operators should release page pins and owned resources in `close` and `Drop`.

### Opt-in execution instrumentation

`QueryEngine::analyze_query(ctx, plan) -> Result<ExplainAnalysis>` is the analysis-only SELECT driver used by `EXPLAIN ANALYZE`. It first reserves the complete deterministic main-tree `PlanNodeLayout`, then resolves uncorrelated subqueries through a profile-aware pre-pass, constructs an instrumented main operator tree, and drains it through the normal `OpenQuery` fetch/close lifecycle into a discard sink. The statement execution clock uses monotonic `Instant` and covers subquery resolution, executor construction, open, drain, and close. The returned report contains cumulative per-node metrics and overall execution time; it never materializes main-query result rows.

The profile-aware pre-pass assigns each executed uncorrelated scalar, EXISTS, or IN subquery an `InitPlan` ordinal before resolving its nested subqueries. Init-plan layouts draw from the shared next-ID counter after the complete main range, and nested entries carry their parent's ordinal. Each resolved init plan executes through the same instrumented builder and shared collector, then is recorded for analyzed formatting; entries are returned in ordinal order. Main IDs therefore match plain EXPLAIN exactly. Apply template construction uses this same analysis state, so its uncorrelated init work is recorded rather than silently omitted, while correlated subplans remain in the main layout and aggregate their physical loops under the template IDs. The ordinary resolver used by `execute`/`open_query` is unchanged.

Each visible physical node is wrapped by `InstrumentedExecutor` only on this opt-in path. Ordinary `execute`, streaming, and `open_query` construction pass no profile state and allocate no wrappers, clocks, collectors, or metric synchronization. A successful `open` begins one loop; a failed open does not. `open`, `next`, `next_batch`, and `close` call durations contribute to inclusive node time. Rows count successful `Some(row)` results or the rows returned by a batch-native `next_batch`. Startup time runs through the first completed fetch call, or equals total time when a loop closes without fetching. A wrapper merges loop-local counters into the shared collector once at close, or on drop when an opened loop was not closed, so the mutex is never acquired per row. Counter and duration overflow saturate and cannot change query correctness.

The profiled builder validates the complete plan/layout shape before constructing operators, including stored Apply subplans that an empty outer input might never execute. Apply and Lateral Apply then retain the fixed layout subtree for their subplan template. Every dynamically rebuilt inner executor reuses that subtree and shared collector, aggregating physical executions under the template IDs; a memo hit does not fabricate a loop. Parent timings include time spent driving children, so node times must not be summed. The analyzed formatter reports per-loop averages while cumulative values remain in `ExplainAnalysis`.

### Streaming SELECT output

`QueryEngine::execute_query_streamed` is the streaming counterpart of the SELECT
arm of `execute`. Instead of materializing rows into `ExecutionResult::Query`, it
drives the operator tree into a caller-supplied `RowSink` in batches, so the
server can stream results through a bounded channel without the `executor` crate
depending on any channel type (`docs/specs/streaming.md`).

```rust
pub trait RowSink {
    fn start(&mut self, columns: &[ColumnInfo]) -> Result<()>;   // once, before rows
    fn push(&mut self, rows: Vec<Row>) -> Result<ControlFlow<()>>; // Break = stop early
}

impl QueryEngine {
    pub fn execute_query_streamed(
        &self, ctx: &ExecutionContext<'_>, plan: &PhysicalPlan,
        sink: &mut dyn RowSink, batch_size: usize,
    ) -> Result<u64>; // rows streamed

    pub fn open_query<'a>(
        &self, ctx: &'a ExecutionContext<'_>, plan: &PhysicalPlan,
    ) -> Result<OpenQuery<'a>>;
}

pub enum FetchStatus {
    Exhausted { count: u64 },
    Suspended { count: u64 },
}

pub struct OpenQuery<'a> { /* owns an opened Box<dyn PlanExecutor + 'a> */ }

impl OpenQuery<'_> {
    pub fn output_schema(&self) -> &[ColumnInfo];
    pub fn fetch(
        &mut self,
        max_rows: Option<u64>,
        sink: &mut dyn RowSink,
        batch_size: usize,
    ) -> Result<FetchStatus>;
    pub fn close(&mut self) -> Result<()>;
}
```

`start` is called once with the output schema (even for an empty result), then
`push` receives row batches of at most `batch_size` until the plan is exhausted
or the sink returns `ControlFlow::Break` (e.g. the consumer is gone), after which
the one-shot streaming call closes the executor. `OpenQuery::fetch` calls
`start` for each fetch call, emits at most `max_rows` rows when a bound is
supplied, and returns `Suspended` only when a one-row lookahead confirms there
are rows remaining. The lookahead row is buffered and delivered by the next
fetch. `fetch(None)` drains until exhaustion unless the sink asks to stop early;
on exhaustion, error, explicit `close`, or `Drop`, the executor is closed.
Cancellation is polled between rows exactly as the materializing path does. The
materializing `execute_query` and one-shot `execute_query_streamed` paths are
expressed through `OpenQuery`, so streamed, materialized, and cursor-facing
results share the same open/fetch/close behavior. The caller must hold the
snapshot's GC-horizon advertisement and any transaction guard for the whole
`OpenQuery` lifetime, as with `execute`.

## Query Engine Boundary

The concrete server `QueryService` wires:

```text
parse -> bind -> logical_plan -> physical_plan -> execute
```

For SELECT, it either materializes plain `Row` values into `ExecutionResult::Query` or streams them through `execute_query_streamed` (see above); for DML/DDL, it executes immediately and returns command metadata. Streaming drives the same operators without changing their semantics.

`ExecutionResult` has four variants: `Query` (SELECT rows and columns), `Modified { command, count }` (DML/DDL), `ModifiedReturning { command, count, columns, rows }` (a DML statement with a `RETURNING` clause — it both modifies rows and produces a result set; `count` drives the DML command tag while `columns`/`rows` are the `RETURNING` projection), and `Explanation { text }` (EXPLAIN). `QueryEngine::execute` produces `Query`, `Modified`, and `ModifiedReturning`; `Explanation` is produced by the server's `QueryService`. Plain EXPLAIN does not call the executor, while analyzed EXPLAIN calls `QueryEngine::analyze_query` and returns only the formatted profile.

Production execution uses an explicit context:

```rust
pub struct ExecutionContext<'a> {
    pub statement: StatementContext,
    pub relations: Arc<dyn RelationSnapshot>,
    pub catalog: Arc<dyn CatalogManager>,
    pub storage: &'a dyn StorageEngine,
    pub schema_ops: &'a dyn SchemaOperations,
    pub gc_horizon: u64,
    pub cancel: &'a QueryCancel,
    pub spill: spill::SpillConfig,
}

pub struct QueryEngine;

impl QueryEngine {
    pub fn execute(&self, ctx: &ExecutionContext<'_>, plan: &PhysicalPlan) -> Result<ExecutionResult>;
}
```

`spill` is captured when a query or portal is opened. Every blocking/stateful
physical operator derives an independent operator budget; multiple spill
structures internal to one operator share that operator's budget. Library/test
contexts may use `SpillConfig::default`; the server supplies the effective
session `work_mem` and `<data-dir>/tmp`.

The owned catalog handle is normally the live catalog. For statements that scan
virtual system catalogs, the server instead installs an immutable statement
snapshot captured under its shared catalog-publication gate after object-lock
convergence. This keeps system rows coherent without cloning the catalog for
ordinary data statements.

`QueryEngine::execute` passes `ctx.statement` to storage and schema operations. It does not allocate transaction IDs, append commit records, flush WAL, or call storage/buffer commit or rollback; server query orchestration owns those statement-level concerns.

`ctx.cancel` is a `QueryCancel` polled between rows in the row-producing loop and the INSERT/UPDATE/DELETE write loops. Its first recorded `CancelReason` selects the `QueryCanceled` message (`due to user request` or `due to statement timeout`). Materializing executor paths use `collect_all_cancelable`, which polls while draining children; scan/filter row-suppression loops and LIMIT offset skipping poll directly; nested-loop and hash join builds poll their outer/inner build loops; aggregate evaluation polls input rows; and the external sorter polls while accepting, sorting, spilling, merging, and emitting records. DISTINCT and set-operation drain/group loops also poll directly. A long blocking `open()` therefore cannot hide an expired statement timer until the full result has been built.

## Operators

| Operator | Behavior |
|---|---|
| `SeqScanOp` | Calls `StorageEngine::scan`, converts `StoredRow` to `ExecRow`, and applies the scan filter. A missing table generation is an execution error; statement locking/revalidation must make it unreachable during normal execution |
| `IndexScanOp` | For the primary-key index calls `StorageEngine::scan_range`; for a secondary index calls `StorageEngine::index_scan`. If a planned secondary index is defensively unavailable in the statement-captured relation generation, falls back to `StorageEngine::scan` and applies `PhysicalPlan::IndexScan.full_filter`. Missing table generations are execution errors. Converts `StoredRow` to `ExecRow`, then applies the active filter (`filter` for normal index scans, `full_filter` for fallback) when present |
| `SystemScanOp` | Materializes virtual catalog rows from the immutable statement catalog/provider snapshot captured by the server after lock convergence; applies filters and emits rows with no identity |
| `NestedLoopJoinOp` | Stores both inputs in `work_mem`-bounded rewindable spill tapes, streams inner/cross/semi/anti/left/right/full results, records matched right ordinals in a bounded external sorter so volatile predicates run only once per pair, and applies NULL extension; clears identity except documented semi/anti and DML-spine cases |
| `HashJoinOp` | Builds the planner-selected side (right by default; left when statistics estimate it is smaller) in a reservation-accounted, key-sorted contiguous table while it fits, then releases it and falls back to a bounded rewindable spill-tape probe. NULL keys never match; output remains logical left ++ right regardless of build side and is streamed rather than buffered |
| `MergeJoinOp` | Internally stable-sorts both inputs under one shared operator `work_mem` budget. It streams left/right/full equi joins, evaluates residual predicates once per candidate pair, treats every NULL-bearing key as unmatched, and clears identity. Equal-key right groups use rewindable spill tapes and matched ordinals use an external sorter, bounding skewed duplicate groups; deterministic key progression is not an ordering guarantee |
| `FilterOp` | Evaluates predicate, preserves identity |
| `ProjectionOp` | Rewrites row values, preserves identity |
| `SortOp` | Evaluates sort keys once, uses the query's `work_mem`-bounded stable external sorter, spills anonymous runs under the configured temporary directory when needed, streams the merged result, and preserves identity |
| `DistinctOp` | Uses bounded external key and ordinal sorts to emit the first input row of each distinct `on_keys` tuple in input order; NULL keys collapse together; clears identity |
| `LimitOp` | Skips offset, emits count rows, preserves identity |
| `AggregateOp` | Global aggregates fold directly without a group sort. Grouped aggregates use a bounded external key sort and stream one group at a time through constant-memory non-DISTINCT states; each DISTINCT expression uses a bounded argument sorter sharing the operator budget (metadata/file use scales with the number of DISTINCT expressions), variance uses an online state, and identity is cleared |
| `SetOpOp` | Uses bounded external key and ordinal sorts for UNION/INTERSECT/EXCEPT distinct and multiset semantics, preserves the established left-to-right output order, and clears identity |
| `ValuesOp` | Emits literal rows, identity is `None` |

`COPY TO` uses the same table scan and row decoding as SELECT, then formats the
requested columns with the COPY text/CSV encoder. A missing generation is an
execution error under the same statement-snapshot contract as `SeqScanOp`.

## Identity Rules

- Scans set `identity = Some(RowIdentity { row_id, xmin, key })`.
- System scans set `identity = None`; they are read-only virtual relations.
- Filter, sort, limit, and projection preserve identity.
- Join, aggregate, and distinct clear identity.
- `UPDATE` and `DELETE` require identity on each source row. If a plan produces a row without identity for DML, executor returns `ErrorKind::Internal`.

## Virtual System Scans

`SystemScanOp` evaluates `PhysicalPlan::SystemScan` for the read-only virtual
system catalog surface. `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
`pg_index`, `pg_constraint`, `pg_attrdef`, `pg_depend`,
`information_schema.schemata`, `information_schema.tables`, and
`information_schema.columns` are computed from `CatalogManager` metadata and the
static registry owned by the catalog crate. Hidden TOAST relations are
storage-private and are omitted from PostgreSQL-facing relation rows (`pg_class`,
`pg_attribute`, `pg_index`, `pg_constraint`, `pg_attrdef`, and `pg_depend`).
User table rows report `reltoastrelid = 0` rather than exposing an OID for an
omitted hidden relation.
`pg_attribute` includes common
PostgreSQL 16 metadata columns used by table-description probes, with harmless
constants for unsupported features (no inheritance, missing values, ACLs, or
per-column options). `pg_proc` exposes compatibility rows from
`common::pg_proc_catalog_entries()` for SaguaroDB built-in/probe functions;
`concat` sets `provariadic` for its variadic text signature.
`pg_type` exposes rows for the scalar, array, and catalog presentation types
SaguaroDB reports. `typelem` is populated for vector and exposed array rows;
`typarray` is populated on scalar rows whose companion array row is exposed.
`pg_database` and `pg_roles` expose the current session database/user. `pg_settings` combines
`StatementContext.system_state.settings()` with synthesized transaction isolation
rows. `pg_stat_activity` reflects `StatementContext.system_state.sessions()`; the
server wires this to its live `SessionRegistry`, while the no-op provider used by
library tests is empty. `pg_stats` exposes one row per analyzed column
(`docs/specs/statistics.md` §8), sorted by table id then column id: `null_frac`,
`avg_width`, `n_distinct` (PostgreSQL sign convention — positive count,
negative fraction of the row count), and MCV/histogram values rendered in
their wire text form inside PostgreSQL-style `{...}` array text (quoted per
array-output rules); empty lists render as SQL NULL and `correlation` is
always NULL. `pg_class.relpages`/`reltuples` report stored statistics for
analyzed user tables and keep the `0`/`-1` "unknown" convention otherwise.

Rows are rebuilt for each execution, sorted deterministically, and filtered with
the bound scan predicate using the same `predicate_matches` semantics as storage
scans. OID columns report PostgreSQL `oid` wire metadata. Legacy catalog-only
vector fields keep text values while reporting PostgreSQL-compatible wire
identities; ordinary SQL arrays are first-class `SqlArray` values. Extended-query
result format selection keeps only those legacy vector fields in text when a
client asks for binary results. System scans never record
SSI reads and never carry `RowIdentity`, so they cannot be a DML source.

## Expression Evaluation

The `TableFunctionOp` materializes one-column rows for `UNNEST` and integer
`GENERATE_SERIES`. `UNNEST(NULL)` and a NULL series argument produce no rows;
array NULL elements produce NULL rows. Series endpoints are inclusive, negative
steps are supported, a direction that cannot reach the endpoint is empty, step
zero is `InvalidParameterValue`, and output is capped by the array element guard.
Correlated table functions are re-executed by lateral `Apply` after argument
substitution.

```rust
pub fn eval_expr(expr: &BoundExpr, row: &ExecRow) -> Result<Value>;
```

The evaluator handles:

- Literals and `InputRef`.
- Arithmetic: `+`, `-`, `*`, `/`, `%` (and unary `-`). Both operands must share one numeric family — `INTEGER`, `DOUBLE PRECISION`, `REAL`, or `NUMERIC` (any `(p, s)`) — with no implicit coercion between families; the result is that family (`NUMERIC` arithmetic yields an unconstrained `NUMERIC`). `%` is supported for `INTEGER` and `NUMERIC` but rejected for the floating-point families (`DOUBLE PRECISION`, `REAL`). `NUMERIC` follows PostgreSQL's scale rules: `+`/`-` use the larger operand scale, `*` sums the operand scales, and `/` produces up to 28 significant digits; overflow beyond `Decimal`'s range is `SqlState::NumericValueOutOfRange`. `INTERVAL` arithmetic sits outside the numeric families: `interval ± interval` (component-wise), `interval * integer` (each component scaled), and unary `- interval` all yield `INTERVAL`; `DATE/TIMESTAMP/TIMESTAMP WITH TIME ZONE/TIME ± interval` yields the temporal type (`DATE ± interval` yields `TIMESTAMP`), where months are applied calendar-aware (clamping the day-of-month, e.g. `2024-01-31 + 1 month = 2024-02-29`) before whole days and then microseconds, and `TIME ± interval` uses only the interval's time component, wrapping into `[0, 24h)`. Overflow of any interval/temporal result is `SqlState::NumericValueOutOfRange`.
- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`. `DOUBLE PRECISION` and `REAL` compare with a total order matching PostgreSQL's float operators: `NaN` equals itself and sorts greater than every other value, and `-0.0` equals `+0.0`. `NUMERIC` compares by value, so `1.0`, `1.00`, and `1` are equal (and collapse together under `DISTINCT`/`GROUP BY`). `INTERVAL` compares by a canonical estimate (a month = 30 days, a day = 24 hours), so `1 mon` equals `30 days`.
- NULL-safe comparison: `IS DISTINCT FROM` and `IS NOT DISTINCT FROM`. Two NULLs are not distinct, a NULL and a non-NULL are distinct, and otherwise ordinary equality applies; the result is always a boolean, never NULL. (`COALESCE` and `NULLIF` are desugared to `CASE` by the binder and evaluate as such.)
- Arrays: constructors evaluate their flattened elements into a rectangular
  `SqlArray` with one-based lower bounds. A complete subscript coordinate returns
  the selected element; NULL, incomplete, or out-of-range coordinates return
  NULL. Array comparisons use the durable `SqlArray` ordering. `left op
  ANY(array)` returns true on the first true element comparison, NULL when no
  comparison is true but at least one is NULL, and false otherwise (including an
  empty array). Explicit array-to-array casts apply the scalar cast to every
  element while retaining dimensions and lower bounds.
- String concatenation: `||` over text operands, NULL-propagating; non-text operands return `SqlState::DatatypeMismatch`.
- Boolean: `AND`, `OR`, `NOT` with SQL three-valued logic.
- `IS NULL`, `IS NOT NULL`.
- `IN`, `BETWEEN`, `LIKE`.
- `CASE`.
- `CAST`.
- Sequence expressions: `nextval(sequence_id)` calls `StatementContext.sequence_manager.nextval`, records the returned value in the session sequence state, and returns it as `Value::Integer`; `currval(sequence_id)` first checks that the sequence still exists (`SqlState::UndefinedTable` if it was dropped), then reads the session sequence state and returns `SqlState::ObjectNotInPrerequisiteState` if the sequence has not been used on this connection; `setval(sequence_id, value[, is_called])` evaluates its arguments, returns `NULL` with no side effect when any argument is `NULL`, otherwise calls `SequenceManager::setval`, records the returned value in session state only when `is_called` is true, and returns it.
- Scalar functions are dispatched through the scalar function registry in `common` (`docs/specs/crates/common.md`): scalar evaluation resolves the function by name via `common::lookup_scalar_function`, applies the entry's NULL policy, and calls its evaluator. The registered functions are `UPPER`, `LOWER`, `LENGTH`, `TRIM` (text), and `SUBSTRING(text, start[, length])`, the math functions `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND`, `SQRT`, `POWER`/`POW`, and `MOD`, and the string functions `REPLACE`, `POSITION`, `LEFT`, and `RIGHT`. These are NULL-propagating (any NULL argument yields NULL). `CONCAT` is the exception: it ignores NULL arguments and returns the empty string (never NULL) when every argument is NULL. `CURRENT_TIMESTAMP` and `now()` read `StatementContext.statement_timestamp_micros`, return `Value::TimestampTz`, and are stable within one statement. PostgreSQL-compatible system information functions read statement/session state: `VERSION()` returns `PostgreSQL 16.0 (SaguaroDB <crate-version>)`; `CURRENT_DATABASE()` and `CURRENT_CATALOG` return the startup database; `CURRENT_SCHEMA` returns `public`; `CURRENT_USER`, `SESSION_USER`, and `USER` return the startup user; `PG_BACKEND_PID()` returns the connection's `BackendKeyData` process id as `Value::Integer`; and `CURRENT_SETTING(text)` reads `StatementContext.system_state.setting(name)`, returns `Value::Text`, and returns `SqlState::UndefinedObject` (`42704`) when the parameter is unknown. PostgreSQL catalog introspection functions read `StatementContext.catalog_introspection`: `FORMAT_TYPE(oid, typmod)` formats supported type OIDs through `PgType`, treats `NULL` typmod as omitted (`-1`), returns `NULL` only for a `NULL` type OID, and returns `???` for unknown non-NULL types; `TO_REGTYPE(text)` resolves supported bare type names and `pg_catalog.`-qualified type names through `PgType`; `PG_GET_INDEXDEF`, `PG_GET_CONSTRAINTDEF`, `PG_GET_EXPR`, `PG_GET_USERBYID`, `PG_TABLE_IS_VISIBLE`, `TO_REGCLASS`, and `PG_GET_SERIAL_SEQUENCE` delegate to the provider and propagate provider errors; provider `None` becomes SQL `NULL` for nullable metadata lookups. `PG_GET_INDEXDEF(oid, column, pretty)` returns `NULL` when the OID is missing or a nonzero requested column number is outside the index key list; column `0` renders the full definition. The default no-op provider returns pass-through text for `PG_GET_EXPR(text, oid[, pretty])`; real providers may return `NULL` when an expression cannot be rendered. Privilege functions return `TRUE` for non-NULL probes because SaguaroDB has no grant model yet; relation-size and temp-schema probes return harmless compatibility constants; description and unsupported definition helpers return `NULL`. `REPLACE` leaves the string unchanged for an empty `from` (unlike Rust's `str::replace`); `POSITION` is the 1-based character index (0 if absent, 1 for an empty substring); `LEFT`/`RIGHT` count characters and treat a negative count as removing characters from the far end (PostgreSQL semantics). `EXTRACT(field FROM source)` returns the `year`/`month`/`day`/`hour`/`minute`/`second` of a `DATE` or `TIMESTAMP` as `DOUBLE PRECISION` (a DATE has zero-valued time components; `second` includes the fractional part). `LENGTH` and `SUBSTRING` count Unicode characters, not bytes; `SUBSTRING` uses 1-based start positions clamped to the string and rejects a negative length with `SqlState::DatatypeMismatch`. `FLOOR`/`CEIL`/`ROUND` leave an integer unchanged and round a double (`ROUND` is round-half-to-even, matching PostgreSQL's `round(double precision)`); `ABS` of `i64::MIN` returns `SqlState::NumericValueOutOfRange`; `SQRT` of a negative number and a non-finite `POWER` result return `NumericValueOutOfRange`; `MOD` by zero returns `SqlState::DivisionByZero`.
- Function-listing helpers `PG_GET_FUNCTION_ARGUMENTS`, `PG_GET_FUNCTION_RESULT`, `PG_GET_FUNCTIONDEF`, `PG_FUNCTION_IS_VISIBLE`, and `OIDVECTORTYPES` render from the static built-in/probe `pg_proc` compatibility table and do not imply user-defined function support.
- `ARRAY_AGG` builds a one-dimensional, one-based `SqlArray` from all input
  values (including NULLs); `STRING_AGG` concatenates non-NULL text values with
  its per-row delimiter (a NULL delimiter contributes no separator). Both return
  NULL for an empty effective input and honor aggregate `DISTINCT`.
- Aggregate functions are evaluated by `AggregateOp`, not by scalar evaluation.
- `LocalRef` indexes into the current `ExecRow` values. `AggregateCall` must not reach scalar evaluation; logical planning rewrites it before physical execution.
- `Parameter` (`$n`) references must be substituted to literals before execution. One reaching the evaluator is an internal error (`"unbound parameter $N reached the executor"`).
- Subquery expressions (`ScalarSubquery`, `Exists`, `InSubquery`) must be resolved to constants before scalar evaluation; one reaching the evaluator is an internal error. See "Subquery Resolution" below.

Division by zero returns `SqlState::DivisionByZero` for both integer and double precision (PostgreSQL also raises on float division by zero rather than producing infinity). Integer overflow in scalar arithmetic or integer aggregate accumulation returns `SqlState::NumericValueOutOfRange`; double-precision arithmetic follows IEEE 754 (overflow yields infinity rather than erroring).

Expression semantics:

- Comparisons with `NULL` return `Value::Null`; `WHERE` and `HAVING` keep only `Value::Boolean(true)`.
- Boolean `AND`, `OR`, and `NOT` use SQL three-valued logic.
- `LIKE`/`ILIKE` require text operands, support `%` for any sequence and `_` for one character, and use the pattern's escape character (default backslash) to escape `%`, `_`, or the escape character itself. The escape character before any other character is treated as a literal escape character followed by that character, and a trailing lone escape character is literal. `ESCAPE c` overrides the escape character and `ESCAPE ''` disables escaping. `ILIKE` matches case-insensitively (both sides and the escape character are lowercased before matching). If the value or pattern is `NULL`, the result is `NULL`.
- `IN` returns `TRUE` on the first non-null equal item, `FALSE` when no item matches and no list item is `NULL`, and `NULL` when the left side is `NULL` or no item matches but some list item is `NULL`. `NOT IN` applies SQL `NOT` to that result.
- `BETWEEN` evaluates as `(expr >= low) AND (expr <= high)` using the same comparison and boolean null semantics. `NOT BETWEEN` applies SQL `NOT`.
- Searched `CASE WHEN condition THEN value ...` chooses the first `WHEN` whose condition evaluates to `TRUE`; `FALSE` and `NULL` conditions do not match. Simple `CASE operand WHEN value THEN result ...` compares `operand = value` with SQL comparison semantics and chooses the first comparison that evaluates to `TRUE`. If no branch matches, both forms return `ELSE` or `NULL`.
- `CASE` result typing is validated by binder: all non-`NULL` `THEN` and `ELSE` expressions must have the same `DataType`; `NULL` branches are allowed and make the output nullable. If every result branch is `NULL`, binder rejects the expression with `SqlState::DatatypeMismatch`.
- Explicit `CAST` conversion matrix: same-type casts are identity; `NULL` casts to `NULL`; `INTEGER -> TEXT` uses decimal i64 formatting; `BOOLEAN -> TEXT` returns `true` or `false`; `TEXT -> INTEGER` parses a base-10 i64 with optional leading sign and no surrounding whitespace; `TEXT -> BOOLEAN` accepts case-insensitive `true`, `t`, `1`, `false`, `f`, and `0`. `DATE -> TEXT` formats `YYYY-MM-DD`; `TEXT -> DATE` parses `YYYY-MM-DD` and rejects impossible dates. `TIMESTAMP -> TEXT` formats `YYYY-MM-DD HH:MM:SS[.ffffff]`; `TEXT -> TIMESTAMP` parses that form and rejects impossible date/times. `TIME -> TEXT` formats `HH:MM:SS[.ffffff]`; `TEXT -> TIME` parses that form and rejects impossible times. `TIMESTAMP WITH TIME ZONE -> TEXT` formats `...+00` (UTC); `TEXT -> TIMESTAMP WITH TIME ZONE` parses an optional offset to UTC; `TIMESTAMP <-> TIMESTAMP WITH TIME ZONE` reinterprets the same microsecond instant (the naive wall clock is taken as UTC). `INTERVAL <-> TEXT` formats/parses the PostgreSQL `postgres`-style text. `BYTEA -> TEXT` formats the hex `\x...` form; `TEXT -> BYTEA` parses it (hex only). `UUID -> TEXT` formats the canonical `8-4-4-4-12` form; `TEXT -> UUID` parses it (lenient). `DOUBLE PRECISION -> TEXT` uses a round-trippable form (fixed-point for moderate magnitudes, `e±NN` scientific otherwise, with `Infinity`/`-Infinity`/`NaN` spellings); `TEXT -> DOUBLE PRECISION` parses decimal/scientific notation and those special spellings; `INTEGER -> DOUBLE PRECISION` is exact-as-`f64`; `DOUBLE PRECISION -> INTEGER` rounds half-to-even and returns `SqlState::NumericValueOutOfRange` for `NaN`/infinity/out-of-range. `REAL` casts mirror `DOUBLE PRECISION` (`REAL <-> TEXT`, `REAL <-> INTEGER` half-to-even, `INTEGER -> REAL`, and `REAL <-> DOUBLE PRECISION`; `REAL` reaches `NUMERIC` via `DOUBLE PRECISION`). `NUMERIC <-> TEXT` formats/parses the decimal text (scale preserved); `NUMERIC -> INTEGER` rounds half-away-from-zero (PostgreSQL's `numeric` rounding) and is range-checked; `INTEGER -> NUMERIC` is exact; `NUMERIC <-> DOUBLE PRECISION` converts via `f64` (lossy); a `CAST` to `NUMERIC(p, s)` rounds to `s` (half away from zero) and returns `SqlState::NumericValueOutOfRange` when the integer part exceeds `p - s` digits, while a `CAST` to bare `NUMERIC` is identity. `INTEGER -> BOOLEAN`, `BOOLEAN -> INTEGER`, `DATE`/`TIMESTAMP <-> INTEGER`, `DATE <-> TIMESTAMP`, malformed text, and all other pairs return `SqlState::DatatypeMismatch`.
- `ORDER BY` defaults match PostgreSQL: ascending sorts `NULL` last, descending sorts `NULL` first, unless `NULLS FIRST` or `NULLS LAST` is specified. A bare positive integer literal in `ORDER BY` is a 1-based reference to the nth output column, resolved by the binder.
- Type mismatches in expression evaluation return `SqlState::DatatypeMismatch`.

Aggregate execution groups input rows by the `GROUP BY` expressions into ordered groups and emits one output row per group (group-key columns first, then the aggregates); with no `GROUP BY` the entire input is a single group. A `DISTINCT` aggregate argument (e.g. `COUNT(DISTINCT x)`) de-duplicates its argument values before aggregating. Return-type rules: `COUNT` returns `0` for empty input and ignores nulls for `COUNT(expr)`; `SUM`, `AVG`, `MIN`, and `MAX` return `NULL` for empty input. `SUM` and `AVG` require a numeric argument (`INTEGER`, `DOUBLE PRECISION`, `REAL`, or `NUMERIC`) and otherwise return `SqlState::DatatypeMismatch`; the result type matches the argument family (`NUMERIC` yields an unconstrained `NUMERIC`). `AVG(integer)` uses integer division truncated toward zero, while `AVG(double precision)`, `AVG(real)`, and `AVG(numeric)` are true division. `MIN` and `MAX` order any `Value` type (including text and boolean) via the value ordering, ignoring nulls. `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP` and `VARIANCE`/`VAR_SAMP`/`VAR_POP` take a numeric argument and return `DOUBLE PRECISION`: they ignore nulls, the sample forms return `NULL` for fewer than two values and the population forms return `NULL` for no values (population variance of a single value is `0`). `BOOL_AND`/`BOOL_OR` take a boolean argument, ignore nulls, and return `NULL` when there is no non-null input (otherwise the logical AND/OR of the inputs).

### Subquery Resolution

Uncorrelated subqueries are resolved to constants by a one-time pre-pass over the physical plan, run at the start of `QueryEngine::execute` before any operator is built. The pass walks every expression in the plan (scan/join/filter predicates, projection and sort and distinct expressions, aggregate group keys and arguments, `Values` rows, and `UPDATE` assignments) and rewrites each subquery expression:

- A scalar subquery `(SELECT ...)` is executed under the statement's snapshot; an empty result becomes `NULL`, exactly one row becomes its single column value (as a typed literal), and more than one row returns `SqlState::CardinalityViolation` (`21000`).
- `[NOT] EXISTS (...)` becomes a boolean literal: whether the sub-plan produced at least one row, negated for `NOT EXISTS`.
- `expr [NOT] IN (SELECT ...)` drains the single column once into a
  `work_mem`-bounded spill tape registered in the statement's query-local
  runtime value-set registry. The transient expression references that set and
  preserves the existing three-valued behavior, including the project's empty
  set behavior. Scalar subqueries pull at most two rows and `EXISTS` at most one.

Each uncorrelated subquery's bound SELECT is planned (`logical_plan` +
`physical_plan`) and executed once; the pass recurses so nested subqueries are
resolved bottom-up. Correlated scalar, `[NOT] EXISTS`, and `[NOT] IN` subqueries
in `WHERE`, `HAVING`, and projection positions are hoisted into `Apply` nodes and
executed per distinct correlation key (unless volatile), using spill-backed
memoized results. The remaining position and decorrelation limits are specified
in `docs/specs/subqueries.md`.

## DML Execution

`INSERT` (from `VALUES` or `SELECT`):

- Materialize the source plan fully before inserting any row, so that `INSERT ... SELECT` reading the target table observes only pre-insert rows.
- For each source row, build row values in table column order. Omitted columns use their bound table schema default: `ColumnDefault::Const(value)` clones the constant, `ColumnDefault::Nextval(sequence_id)` advances the sequence through `StatementContext.sequence_manager` and records the value for `currval`, and `ColumnDefault::Expr` evaluates the bound default expression the binder attached to the INSERT over an empty row (the expression cannot reference columns); no default yields `NULL`. (COPY FROM supplies the same bound table schema and expression defaults — the binder attaches them to `BoundStatement::Copy` for the omitted columns — so an omitted `ColumnDefault::Expr` column is evaluated per row under COPY too.)
- Validate runtime values match destination column types. `NULL` is accepted at this step and checked by row-constraint validation.
- Coerce `NUMERIC(p, s)` column values to the column scale (`coerce_numeric_columns`): each `Value::Numeric` is rounded to `s` (half away from zero) and rejected with `SqlState::NumericValueOutOfRange` when the integer part exceeds `p - s` digits. Bare `NUMERIC` columns and non-numeric values are unchanged. Runs before constraint validation, so it covers `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE`, and `COPY ... FROM`.
- Validate per-column row constraints (`validate_row_constraints`): non-null, and the bounded character-type length — a `Text` value whose character count exceeds a column's `max_length` (`VARCHAR(n)`/`CHAR(n)`) is rejected with `SqlState::StringDataRightTruncation` (`22001`). This runs on the full row, so it covers `INSERT ... VALUES`, `INSERT ... SELECT`, and `COPY ... FROM`.
- Validate `CHECK` constraints (`validate_check_constraints`) over the full proposed row, using the bound `CHECK` expressions the binder attached to the INSERT: each constraint that evaluates to `false` is rejected with `SqlState::CheckViolation` (`23514`); a `true` or `NULL` (unknown) result passes. This runs before conflict arbitration, so a proposed row that violates a check is rejected even under `ON CONFLICT DO NOTHING` (matching PostgreSQL). `COPY ... FROM` enforces the same checks per row (the binder attaches the bound table schema and `CHECK` expressions to `BoundStatement::Copy`), aborting the whole COPY on a violation.
- Call `StorageEngine::insert`.
- Return `Modified { command: "INSERT", count }`.

`INSERT ... ON CONFLICT` (arbiter = primary key): before executing, any bound conflict arbiter is rechecked against the current table primary key, so a prepared statement whose arbiter no longer matches primary-key DDL is rejected with `FeatureNotSupported` rather than silently using a different arbiter. Prepared statements also carry referenced table schema versions, so primary-key DDL after prepare rejects the cached plan even when targetless `DO NOTHING` originally bound with no arbiter. For each source row, build the full insert row, then probe the visible row at the proposed primary key through storage's identity lookup (snapshot visibility, so the statement's own earlier inserts are seen — a duplicate key within one multi-row INSERT is caught). On a conflict: `DO NOTHING` skips the row (not counted, no `RETURNING` row); `DO UPDATE` evaluates the assignment values and optional `WHERE` over the combined `existing ++ proposed` row (`excluded.<col>` is the proposed row), and — when the `WHERE` passes — writes the new row via `StorageEngine::update` (counted toward `INSERT 0 n`, and projected by `RETURNING`). With no conflict the row is inserted normally. The arbiter is the primary key only: a conflict on a unique **secondary** index is not arbitrated here and surfaces as a `UniqueViolation` (`23505`) from `insert`, aborting the statement. A non-prepared statement on a table with no primary key has no conflict arbiter for targetless `DO NOTHING` and therefore attempts the insert normally.

`UPDATE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Acquire `NoKeyUpdate`, or `Update` when any primary-key column is assigned,
  and resolve the latest tuple version.
- Under Read Committed, when resolution advances, inject the locked row into a
  clone of the DML source plan and execute it to recheck all qualifications and
  rebuild the combined target/FROM row. Under retained-snapshot isolation, a
  successor or concurrent delete returns `40001`.
- Evaluate assignments against the qualifying latest source row.
- Build a full replacement row. If the update changes primary-key columns, storage writes a non-HOT version and enforces primary-key uniqueness on the replacement key.
- Validate per-column row constraints on the replacement row (`validate_row_constraints`): non-null and bounded character-type length, same as INSERT.
- Validate `CHECK` constraints on the replacement row (`validate_check_constraints`), same as INSERT: a constraint evaluating to `false` is rejected with `SqlState::CheckViolation` (`23514`); `true`/`NULL` passes. The `ON CONFLICT DO UPDATE` path applies the same check to the row it writes.
- Call `StorageEngine::update_locked` with the exact locked capability.
- Return count.

`DELETE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Acquire `Update` and resolve the latest tuple version, applying the same
  isolation and source-plan EPQ rules as UPDATE.
- Call `StorageEngine::delete_locked`; `RETURNING` sees the latest deleted row.
- Return count.

`LockRowsOp` implements top-level locking SELECT. For each identity-preserving
candidate from its child it calls `StorageEngine::lock_row` with the bound tuple
mode and wait policy. `SKIP LOCKED` candidates are skipped. A concurrently deleted
candidate is skipped under Read Committed; Repeatable Read / Serializable return
`40001`. A granted candidate is rebuilt from the latest locked version. If locking advanced to a
successor, the original WHERE predicate is re-evaluated; an unchanged candidate
is not evaluated twice, which preserves volatile-expression semantics. The SELECT
projection is evaluated over the locked row. Advancing to a successor is allowed
only under Read Committed; Repeatable Read and Serializable return `40001` rather
than exposing a version newer than their retained snapshot.
Because LIMIT/OFFSET wrap `LockRows`, the operator locks only rows consumed by the
limit; sort keys are computed from the pre-lock snapshot row and are not resorted
if a concurrent update changes them.

`RETURNING` (INSERT/UPDATE/DELETE): when the plan carries a `BoundReturning`, the executor evaluates the projection expressions over each affected full row — the inserted/updated NEW row for INSERT/UPDATE, the deleted OLD row for DELETE — and collects the result rows. For UPDATE/DELETE a row is collected only when exact-version storage mutation (`update_locked`/`delete_locked`) returned `true`; for an INSERT every inserted row is returned. The statement then returns `ModifiedReturning { command, count, columns, rows }` (with the `BoundReturning.output_schema` as `columns`) instead of `Modified`, so the affected-row count still drives the DML command tag.

If a write errors after mutating pages or storage-owned metadata, the executor propagates the error without rolling back itself (consistent with `QueryEngine::execute` not calling storage/buffer commit or rollback). The server query orchestration — or the test harness — owns recovery and calls `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)` before returning the error.

## DDL Execution

`CREATE TABLE`:

- Server query orchestration acquires the shared writer guard and then the
  exclusive catalog publication gate (CREATE has no existing object lock), holding
  the gate through Commit or rollback restore. Catalog binders/readers are blocked.
- For `IF NOT EXISTS`, validate the table definition shape first (columns,
  primary key, and unique-constraint column references). If the table already
  exists, return the normal command tag without creating serial sequences,
  mutating catalog/storage, or appending logical DDL WAL records.
  If a view already exists with the requested name, return
  `SqlState::DuplicateTable` because user tables and views share the relation
  name namespace.
- For `SERIAL` family columns, choose owned sequence names/ids at execution time
  (`<table>_<column>_seq`, with the smallest free numeric suffix if needed),
  create each owned sequence first (`owned: true`, default sequence options),
  then replace the parse-time `ParsedDefault::Serial` marker with the internal
  owned `nextval` default before creating the table. If any later table or
  unique-index step fails, drop the created serial sequences as part of
  statement cleanup.
- Create catalog/storage metadata and append logical WAL while the publication
  gate excludes readers. On pre-commit failure restore the catalog/storage state;
  after Commit release the gate so the complete durable object becomes visible.
- Return `Modified { command: "CREATE TABLE", count: 0 }`.

`DROP TABLE [IF EXISTS] <name> [, ...]`:

- Resolve every target in the binder for plain `DROP TABLE`; if any name belongs
  to a view, return `SqlState::WrongObjectType`. For `DROP TABLE IF EXISTS`,
  carry ordered names through planning and resolve the complete list at
  execution time under the catalog publication gate and xid-owned table locks. Skip absent tables without
  catalog/storage mutation or logical DDL WAL records, but return
  `SqlState::WrongObjectType` if a view owns any requested name.
- Call `SchemaOperations::drop_table`.
- For each column default that references an owned sequence, call
  `SchemaOperations::drop_sequence` in the same statement.
- Call `CatalogManager::drop_table`, then remove the owned sequences from the
  catalog.
- `SchemaOperations::drop_table` appends the `DropTable` WAL operation record
  and each owned sequence appends a sibling `DropSequence`; server query
  orchestration appends the statement `Commit`.
- Apply every target under one statement transaction and return `Modified {
  command: "DROP TABLE", count: 0 }`. A pre-commit failure restores the entire
  statement; WAL uses the existing per-object records under one transaction id.

`CREATE INDEX`:

- Server query orchestration acquires the write guard before execution.
- Use `CatalogManager::create_index` to validate the table/columns/name and assign the `IndexId`.
- Call `SchemaOperations::create_index` to build and backfill the secondary tree; storage does not publish the new index generation until that build succeeds. On failure, roll back the catalog with `CatalogManager::drop_index` before returning the error (mirroring `CREATE TABLE`).
- `SchemaOperations::create_index` appends the `CreateIndex` WAL operation record; server query orchestration appends the statement `Commit`.
- Return `Modified { command: "CREATE INDEX", count: 0 }`.

`DROP INDEX`:

- Resolve the index to its `IndexId` in binder.
- Call `SchemaOperations::drop_index`.
- Call `CatalogManager::drop_index`.
- `SchemaOperations::drop_index` appends the `DropIndex` WAL operation record; server query orchestration appends the statement `Commit`.
- Return `Modified { command: "DROP INDEX", count: 0 }`.

`CREATE SEQUENCE`:

- Server query orchestration acquires the DDL guard before execution.
- Use `CatalogManager::create_sequence` to validate options and assign the
  `SequenceId`.
- Call `SchemaOperations::create_sequence`, which appends the `CreateSequence`
  WAL operation record. On failure, remove the catalog sequence before returning
  the error.
- Return `Modified { command: "CREATE SEQUENCE", count: 0 }`.

`DROP SEQUENCE`:

- Resolve the sequence name at execution time. A missing sequence returns
  `SqlState::UndefinedTable` unless `IF EXISTS` was present.
- For an existing sequence, call `CatalogManager::drop_sequence` first so a
  referenced sequence is rejected with `SqlState::DependentObjectsStillExist`
  before storage changes. Then call `SchemaOperations::drop_sequence`, which
  appends the `DropSequence` WAL operation record; if storage fails, restore the
  catalog sequence before returning the error.
- For `IF EXISTS` and a missing sequence, perform no catalog or WAL mutation and
  return the normal command tag.
- Return `Modified { command: "DROP SEQUENCE", count: 0 }`.

`CREATE VIEW` / `CREATE OR REPLACE VIEW`:

- Server query orchestration uses shared writer then existing-view
  `AccessExclusive` (for replace), then the exclusive catalog publication gate.
  A new view has no existing object lock and goes directly to the gate.
- The binder has already bound the view query, validated the optional output
  column list, rejected query parameters, and attached durable dependencies.
- Create/replace metadata and append WAL while the gate blocks catalog readers;
  release it only after Commit or rollback restore.
- For `OR REPLACE` with an existing view, call `CatalogManager::replace_view`
  (same id/name, incremented schema version), then
  `SchemaOperations::replace_view`, which appends `ReplaceView`; if storage/WAL
  append fails, restore the previous view schema before returning the error.
- A non-`OR REPLACE` duplicate relation name returns `SqlState::DuplicateTable`.
- Return `Modified { command: "CREATE VIEW", count: 0 }`.

`DROP VIEW`:

- Resolve the view name at execution time. A missing view returns
  `SqlState::UndefinedTable` unless `IF EXISTS` was present.
- If the name belongs to an existing table, return `SqlState::WrongObjectType`
  rather than treating the statement as missing or as an `IF EXISTS` no-op.
- For an existing view, call `SchemaOperations::drop_view`, which appends the
  `DropView` WAL operation record, then `CatalogManager::drop_view`.
- For `IF EXISTS` and a missing view, perform no catalog or WAL mutation and
  return the normal command tag.
- Return `Modified { command: "DROP VIEW", count: 0 }`.

`ALTER TABLE` schema evolution:

- `ADD COLUMN [IF NOT EXISTS]`, `DROP COLUMN [IF EXISTS]`, `ALTER [COLUMN] ...
  [SET DATA] TYPE`, `RENAME COLUMN`, and `RENAME TO` execute through the DDL path under the shared writer guard, then
  target `AccessExclusive`, then the catalog publication gate.
- `RENAME COLUMN` and `RENAME TO` are metadata updates: mutate catalog schema,
  then call `SchemaOperations::update_table_schema` with the updated table and
  current secondary-index schemas. Catalog rejects these renames when dependent
  views or stored CHECK expressions would leave SQL text stale.
- `ADD COLUMN` and `DROP COLUMN` are logical rewrites. Target `AccessExclusive`
  drains users of the old generation before the executor runs these plans. The executor first
  calls the catalog preflight helper, returning no-op/error outcomes before
  materializing rows. For a real rewrite it scans visible rows under the old
  schema, applies the catalog schema change with fresh storage ids, calls
  `SchemaOperations::update_table_schema` to install empty replacement storage,
  transforms each old row, validates the new row shape, and reinserts it through
  normal storage `insert` so heap, primary-key, secondary-index, and TOAST paths
  stay shared with DML.
- `ADD COLUMN` evaluates the column default per existing row (`NULL`, constant,
  `nextval`, or bound expression default) and creates hidden TOAST storage first
  when the catalog allocated a hidden TOAST relation for the new toastable
  column. `DROP COLUMN` uses catalog-remapped secondary-index schemas so WAL and
  recovery install matching table/index metadata.
- `ALTER COLUMN TYPE` preserves column identity, explicitly casts every visible
  value and constant default, and rebuilds fresh heap/TOAST/index generations.
  Identical wire types are metadata no-ops. `USING`, dependent views, CHECK
  constraints, and expression defaults are rejected in this first implementation.
- Successful schema-evolution statements return
  `Modified { command: "ALTER TABLE", count: 0 }`; no-op `IF [NOT] EXISTS`
  forms return the same tag without catalog/storage mutation.

## Statement Guards

Statement guards are owned by server query orchestration, not by the executor crate. Plain SELECT and non-sequence-mutating EXPLAIN take no autocommit `ConcurrencyController` guard but do take `AccessShare` on referenced tables. Locking SELECT takes the shared participant guard and target `RowShare` through the transaction-owned path. Analyzed EXPLAIN containing `nextval` or `setval` uses the existing write classification, sequence locks, WAL lifecycle, and autocommit commit; plain EXPLAIN never evaluates sequence expressions. DML, COPY FROM, DDL, and WAL-writing maintenance acquire `ConcurrencyController::begin_writer`; DDL additionally takes the catalog publication gate. Table modes are `RowExclusive` for DML targets, `Share` for CREATE INDEX/VACUUM, and `AccessExclusive` for DROP/ALTER/TRUNCATE. Actual checkpoint alone takes `begin_checkpoint`. Shared writer and table-lock guards live for the full statement. Transaction-owned grants normally live through top-level completion; `ROLLBACK TO SAVEPOINT` restores the earlier captured grant set. See `docs/specs/crates/server.md` and `docs/specs/table-locks.md`.

## Acceptance Tests

- `SeqScanOp` returns rows with identity.
- `SystemScanOp` executes filters, joins, ordering, and projection over virtual catalog rows and returns rows with no identity.
- `FilterOp` preserves identity.
- `ProjectionOp` preserves identity while changing values.
- `NestedLoopJoinOp` clears identity.
- `HashJoinOp` joins inner equi-join rows on one or more key columns and excludes rows with a NULL join key.
- `MergeJoinOp` implements spillable left/right/full equi joins, including residual rejection and NULL extension, and clears identity.
- `UPDATE WHERE` modifies only matched rows.
- `DELETE WHERE` deletes only matched rows.
- Failed write triggers rollback (driven by the server/harness, not the executor) and does not expose partial changes.
- Scalar expression evaluator implements SQL NULL boolean cases.
- Aggregate operator computes `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP`, `VARIANCE`/`VAR_SAMP`/`VAR_POP`, `BOOL_AND`, `BOOL_OR`.
