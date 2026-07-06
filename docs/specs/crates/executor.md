# `executor` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`executor` evaluates `PhysicalPlan` values. It owns physical operators, expression evaluation, DML/DDL orchestration, and conversion to `ExecutionResult`.

## Depends On

- `common`
- `catalog`
- `storage`
- `planner` plan types

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
}
```

`start` is called once with the output schema (even for an empty result), then
`push` receives row batches of at most `batch_size` until the plan is exhausted
or the sink returns `ControlFlow::Break` (e.g. the consumer is gone), after which
the executor is closed. Cancellation is polled between rows exactly as the
materializing path does. The materializing `execute_query` is itself expressed as
this same drive with an in-memory collecting sink, so streamed and materialized
results cannot diverge. The caller must hold the snapshot's GC-horizon
advertisement and any transaction guard for the whole call, as with `execute`.

## Query Engine Boundary

The concrete server `QueryService` wires:

```text
parse -> bind -> logical_plan -> physical_plan -> execute
```

For SELECT, it either materializes plain `Row` values into `ExecutionResult::Query` or streams them through `execute_query_streamed` (see above); for DML/DDL, it executes immediately and returns command metadata. Streaming drives the same operators without changing their semantics.

`ExecutionResult` has four variants: `Query` (SELECT rows and columns), `Modified { command, count }` (DML/DDL), `ModifiedReturning { command, count, columns, rows }` (a DML statement with a `RETURNING` clause — it both modifies rows and produces a result set; `count` drives the DML command tag while `columns`/`rows` are the `RETURNING` projection), and `Explanation { text }` (EXPLAIN). `QueryEngine::execute` produces `Query`, `Modified`, and `ModifiedReturning`; `Explanation` is produced by the server's `QueryService` (EXPLAIN never calls the executor), but the variant lives in the executor crate's `ExecutionResult`.

Production execution uses an explicit context:

```rust
pub struct ExecutionContext<'a> {
    pub statement: StatementContext,
    pub catalog: &'a dyn CatalogManager,
    pub storage: &'a dyn StorageEngine,
    pub schema_ops: &'a dyn SchemaOperations,
    pub cancel: &'a AtomicBool,
}

pub struct QueryEngine;

impl QueryEngine {
    pub fn execute(&self, ctx: &ExecutionContext<'_>, plan: &PhysicalPlan) -> Result<ExecutionResult>;
}
```

`QueryEngine::execute` passes `ctx.statement` to storage and schema operations. It does not allocate transaction IDs, append commit records, flush WAL, or call storage/buffer commit or rollback; server query orchestration owns those statement-level concerns.

`ctx.cancel` is polled between rows in the row-producing loop and the INSERT/UPDATE/DELETE write loops; when it is set (from another connection's `CancelRequest`), execution aborts with `DbError::execute(SqlState::QueryCanceled, "canceling statement due to user request")`. Cancellation is observed at these row boundaries, not mid-operator (e.g. during a sort or join build phase).

## Operators

| Operator | Behavior |
|---|---|
| `SeqScanOp` | Calls `StorageEngine::scan`, converts `StoredRow` to `ExecRow`, applies scan filter if present |
| `IndexScanOp` | For the primary-key index calls `StorageEngine::scan_range`; for a secondary index calls `StorageEngine::index_scan`. Converts `StoredRow` to `ExecRow`, then applies `PhysicalPlan::IndexScan.filter` when present |
| `SystemScanOp` | Materializes rows for a virtual `pg_catalog`/`information_schema` system view from catalog metadata, the static registry, and `StatementContext.system_state`; applies scan filter if present; emits rows with no identity |
| `NestedLoopJoinOp` | Buffers right side, implements inner/cross/left/right/full joins with NULL extension for missing side rows, emits concatenated rows, clears identity |
| `HashJoinOp` | Inner equi-join: builds a probe table over the right side keyed by `right_keys`, probes with `left_keys`; rows with a NULL key column never match; emits concatenated rows, clears identity |
| `FilterOp` | Evaluates predicate, preserves identity |
| `ProjectionOp` | Rewrites row values, preserves identity |
| `SortOp` | Materializes all input, sorts in memory, preserves identity |
| `DistinctOp` | Streams input, emitting the first row of each distinct `on_keys` tuple (tracked in a `BTreeSet`) and dropping later duplicates; NULL keys collapse together; clears identity |
| `LimitOp` | Skips offset, emits count rows, preserves identity |
| `AggregateOp` | Groups input by the `GROUP BY` expressions (a single group when there is none), emits one row per group, de-duplicates `DISTINCT` aggregate arguments, clears identity |
| `ValuesOp` | Emits literal rows, identity is `None` |

## Identity Rules

- Scans set `identity = Some(RowIdentity { row_id, key })`.
- System scans set `identity = None`; they are read-only virtual relations.
- Filter, sort, limit, and projection preserve identity.
- Join, aggregate, and distinct clear identity.
- `UPDATE` and `DELETE` require identity on each source row. If a plan produces a row without identity for DML, executor returns `ErrorKind::Internal`.

## Virtual System Scans

`SystemScanOp` evaluates `PhysicalPlan::SystemScan` for the read-only virtual
system catalog surface. `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
`pg_index`, `information_schema.schemata`, `information_schema.tables`, and
`information_schema.columns` are computed from `CatalogManager` metadata and the
static registry owned by the catalog crate. `pg_settings` combines
`StatementContext.system_state.settings()` with synthesized transaction isolation
rows. `pg_stat_activity` reflects `StatementContext.system_state.sessions()`; the
server wires this to its live `SessionRegistry`, while the no-op provider used by
library tests is empty.

Rows are rebuilt for each execution, sorted deterministically, and filtered with
the bound scan predicate using the same `predicate_matches` semantics as storage
scans. System scans never record SSI reads and never carry `RowIdentity`, so they
cannot be a DML source.

## Expression Evaluation

```rust
pub fn eval_expr(expr: &BoundExpr, row: &ExecRow) -> Result<Value>;
```

The evaluator handles:

- Literals and `InputRef`.
- Arithmetic: `+`, `-`, `*`, `/`, `%` (and unary `-`). Both operands must share one numeric family — `INTEGER`, `DOUBLE PRECISION`, `REAL`, or `NUMERIC` (any `(p, s)`) — with no implicit coercion between families; the result is that family (`NUMERIC` arithmetic yields an unconstrained `NUMERIC`). `%` is supported for `INTEGER` and `NUMERIC` but rejected for the floating-point families (`DOUBLE PRECISION`, `REAL`). `NUMERIC` follows PostgreSQL's scale rules: `+`/`-` use the larger operand scale, `*` sums the operand scales, and `/` produces up to 28 significant digits; overflow beyond `Decimal`'s range is `SqlState::NumericValueOutOfRange`. `INTERVAL` arithmetic sits outside the numeric families: `interval ± interval` (component-wise), `interval * integer` (each component scaled), and unary `- interval` all yield `INTERVAL`; `DATE/TIMESTAMP/TIMESTAMP WITH TIME ZONE/TIME ± interval` yields the temporal type (`DATE ± interval` yields `TIMESTAMP`), where months are applied calendar-aware (clamping the day-of-month, e.g. `2024-01-31 + 1 month = 2024-02-29`) before whole days and then microseconds, and `TIME ± interval` uses only the interval's time component, wrapping into `[0, 24h)`. Overflow of any interval/temporal result is `SqlState::NumericValueOutOfRange`.
- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`. `DOUBLE PRECISION` and `REAL` compare with a total order matching PostgreSQL's float operators: `NaN` equals itself and sorts greater than every other value, and `-0.0` equals `+0.0`. `NUMERIC` compares by value, so `1.0`, `1.00`, and `1` are equal (and collapse together under `DISTINCT`/`GROUP BY`). `INTERVAL` compares by a canonical estimate (a month = 30 days, a day = 24 hours), so `1 mon` equals `30 days`.
- NULL-safe comparison: `IS DISTINCT FROM` and `IS NOT DISTINCT FROM`. Two NULLs are not distinct, a NULL and a non-NULL are distinct, and otherwise ordinary equality applies; the result is always a boolean, never NULL. (`COALESCE` and `NULLIF` are desugared to `CASE` by the binder and evaluate as such.)
- String concatenation: `||` over text operands, NULL-propagating; non-text operands return `SqlState::DatatypeMismatch`.
- Boolean: `AND`, `OR`, `NOT` with SQL three-valued logic.
- `IS NULL`, `IS NOT NULL`.
- `IN`, `BETWEEN`, `LIKE`.
- `CASE`.
- `CAST`.
- Sequence expressions: `nextval(sequence_id)` calls `StatementContext.sequence_manager.nextval`, records the returned value in the session sequence state, and returns it as `Value::Integer`; `currval(sequence_id)` first checks that the sequence still exists (`SqlState::UndefinedTable` if it was dropped), then reads the session sequence state and returns `SqlState::ObjectNotInPrerequisiteState` if the sequence has not been used on this connection; `setval(sequence_id, value[, is_called])` evaluates its arguments, returns `NULL` with no side effect when any argument is `NULL`, otherwise calls `SequenceManager::setval`, records the returned value in session state only when `is_called` is true, and returns it.
- Scalar functions are dispatched through the scalar function registry in `common` (`docs/specs/crates/common.md`): scalar evaluation resolves the function by name via `common::lookup_scalar_function`, applies the entry's NULL policy, and calls its evaluator. The registered functions are `UPPER`, `LOWER`, `LENGTH`, `TRIM` (text), and `SUBSTRING(text, start[, length])`, the math functions `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND`, `SQRT`, `POWER`/`POW`, and `MOD`, and the string functions `REPLACE`, `POSITION`, `LEFT`, and `RIGHT`. These are NULL-propagating (any NULL argument yields NULL). `CONCAT` is the exception: it ignores NULL arguments and returns the empty string (never NULL) when every argument is NULL. `CURRENT_TIMESTAMP` and `now()` read `StatementContext.statement_timestamp_micros`, return `Value::TimestampTz`, and are stable within one statement. PostgreSQL-compatible system information functions read statement/session state: `VERSION()` returns `PostgreSQL 16.0 (SaguaroDB <crate-version>)`; `CURRENT_DATABASE()` and `CURRENT_CATALOG` return the startup database; `CURRENT_SCHEMA` returns `public`; `CURRENT_USER`, `SESSION_USER`, and `USER` return the startup user; `PG_BACKEND_PID()` returns the connection's `BackendKeyData` process id as `Value::Integer`; and `CURRENT_SETTING(text)` reads `StatementContext.system_state.setting(name)`, returns `Value::Text`, and returns `SqlState::UndefinedObject` (`42704`) when the parameter is unknown. `REPLACE` leaves the string unchanged for an empty `from` (unlike Rust's `str::replace`); `POSITION` is the 1-based character index (0 if absent, 1 for an empty substring); `LEFT`/`RIGHT` count characters and treat a negative count as removing characters from the far end (PostgreSQL semantics). `EXTRACT(field FROM source)` returns the `year`/`month`/`day`/`hour`/`minute`/`second` of a `DATE` or `TIMESTAMP` as `DOUBLE PRECISION` (a DATE has zero-valued time components; `second` includes the fractional part). `LENGTH` and `SUBSTRING` count Unicode characters, not bytes; `SUBSTRING` uses 1-based start positions clamped to the string and rejects a negative length with `SqlState::DatatypeMismatch`. `FLOOR`/`CEIL`/`ROUND` leave an integer unchanged and round a double (`ROUND` is round-half-to-even, matching PostgreSQL's `round(double precision)`); `ABS` of `i64::MIN` returns `SqlState::NumericValueOutOfRange`; `SQRT` of a negative number and a non-finite `POWER` result return `NumericValueOutOfRange`; `MOD` by zero returns `SqlState::DivisionByZero`.
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
- `expr [NOT] IN (SELECT ...)` materializes the subquery's single column into an `InList` of literals, so the existing `IN`/`NOT IN` three-valued-logic evaluation applies unchanged (including `NULL` items).

Each subquery's bound SELECT is planned (`logical_plan` + `physical_plan`) and executed once; the pass recurses so nested subqueries are resolved bottom-up. Because the subqueries are uncorrelated, a single execution under the statement snapshot is correct; correlated subqueries are not yet supported.

## DML Execution

`INSERT` (from `VALUES` or `SELECT`):

- Materialize the source plan fully before inserting any row, so that `INSERT ... SELECT` reading the target table observes only pre-insert rows.
- For each source row, build row values in table column order. Omitted columns use their catalog default: `ColumnDefault::Const(value)` clones the constant, `ColumnDefault::Nextval(sequence_id)` advances the sequence through `StatementContext.sequence_manager` and records the value for `currval`, and `ColumnDefault::Expr` evaluates the bound default expression the binder attached to the INSERT over an empty row (the expression cannot reference columns); no default yields `NULL`. (COPY FROM supplies the same bound expression defaults — the binder attaches them to `BoundStatement::Copy` for the omitted columns — so an omitted `ColumnDefault::Expr` column is evaluated per row under COPY too.)
- Validate runtime values match destination column types. `NULL` is accepted at this step and checked by row-constraint validation.
- Coerce `NUMERIC(p, s)` column values to the column scale (`coerce_numeric_columns`): each `Value::Numeric` is rounded to `s` (half away from zero) and rejected with `SqlState::NumericValueOutOfRange` when the integer part exceeds `p - s` digits. Bare `NUMERIC` columns and non-numeric values are unchanged. Runs before constraint validation, so it covers `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE`, and `COPY ... FROM`.
- Validate per-column row constraints (`validate_row_constraints`): non-null, and the bounded character-type length — a `Text` value whose character count exceeds a column's `max_length` (`VARCHAR(n)`/`CHAR(n)`) is rejected with `SqlState::StringDataRightTruncation` (`22001`). This runs on the full row, so it covers `INSERT ... VALUES`, `INSERT ... SELECT`, and `COPY ... FROM`.
- Validate `CHECK` constraints (`validate_check_constraints`) over the full proposed row, using the bound `CHECK` expressions the binder attached to the INSERT: each constraint that evaluates to `false` is rejected with `SqlState::CheckViolation` (`23514`); a `true` or `NULL` (unknown) result passes. This runs before conflict arbitration, so a proposed row that violates a check is rejected even under `ON CONFLICT DO NOTHING` (matching PostgreSQL). `COPY ... FROM` enforces the same checks per row (the binder attaches the table's bound `CHECK` expressions to `BoundStatement::Copy`), aborting the whole COPY on a violation.
- Call `StorageEngine::insert`.
- Return `Modified { command: "INSERT", count }`.

`INSERT ... ON CONFLICT` (arbiter = primary key): for each source row, build the full insert row, then probe the visible row at the proposed primary key with `StorageEngine::get` (snapshot visibility, so the statement's own earlier inserts are seen — a duplicate key within one multi-row INSERT is caught). On a conflict: `DO NOTHING` skips the row (not counted, no `RETURNING` row); `DO UPDATE` evaluates the assignment values and optional `WHERE` over the combined `existing ++ proposed` row (`excluded.<col>` is the proposed row), and — when the `WHERE` passes — writes the new row via `StorageEngine::update` (counted toward `INSERT 0 n`, and projected by `RETURNING`). With no conflict the row is inserted normally. The arbiter is the primary key only: a conflict on a unique **secondary** index is not arbitrated here and surfaces as a `UniqueViolation` (`23505`) from `insert`, aborting the statement.

`UPDATE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Evaluate assignments against the source row.
- Build a full replacement row. The primary-key column cannot change; storage rejects an update whose replacement key differs with `SqlState::DatatypeMismatch` ("primary key updates are not supported").
- Validate per-column row constraints on the replacement row (`validate_row_constraints`): non-null and bounded character-type length, same as INSERT.
- Validate `CHECK` constraints on the replacement row (`validate_check_constraints`), same as INSERT: a constraint evaluating to `false` is rejected with `SqlState::CheckViolation` (`23514`); `true`/`NULL` passes. The `ON CONFLICT DO UPDATE` path applies the same check to the row it writes.
- Call `StorageEngine::update`.
- Return count.

`DELETE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Call `StorageEngine::delete`.
- Return count.

`RETURNING` (INSERT/UPDATE/DELETE): when the plan carries a `BoundReturning`, the executor evaluates the projection expressions over each affected full row — the inserted/updated NEW row for INSERT/UPDATE, the deleted OLD row for DELETE — and collects the result rows. For UPDATE/DELETE a row is collected only when storage actually mutated it (`update`/`delete` returned `true`); for an INSERT every inserted row is returned. The statement then returns `ModifiedReturning { command, count, columns, rows }` (with the `BoundReturning.output_schema` as `columns`) instead of `Modified`, so the affected-row count still drives the DML command tag.

If a write errors after mutating pages or storage-owned metadata, the executor propagates the error without rolling back itself (consistent with `QueryEngine::execute` not calling storage/buffer commit or rollback). The server query orchestration — or the test harness — owns recovery and calls `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)` before returning the error.

## DDL Execution

`CREATE TABLE`:

- Server query orchestration acquires the write guard before execution.
- For `SERIAL` family columns, choose owned sequence names at execution time
  (`<table>_<column>_seq`, with the smallest free numeric suffix if needed),
  create each owned sequence first (`owned: true`, default sequence options),
  then replace the parse-time `ParsedDefault::Serial` marker with the internal
  owned `nextval` default before creating the table. If any later table or
  unique-index step fails, drop the created serial sequences as part of
  statement cleanup.
- Use `CatalogManager::create_table` to assign IDs.
- Call `SchemaOperations::create_table`.
- `SchemaOperations::create_table` appends the `CreateTable` WAL operation record; server query orchestration appends the statement `Commit`.
- Return `Modified { command: "CREATE TABLE", count: 0 }`.

`DROP TABLE`:

- Resolve table in binder.
- Call `SchemaOperations::drop_table`.
- For each column default that references an owned sequence, call
  `SchemaOperations::drop_sequence` in the same statement.
- Call `CatalogManager::drop_table`, then remove the owned sequences from the
  catalog.
- `SchemaOperations::drop_table` appends the `DropTable` WAL operation record
  and each owned sequence appends a sibling `DropSequence`; server query
  orchestration appends the statement `Commit`.
- Return `Modified { command: "DROP TABLE", count: 0 }`.

`CREATE INDEX`:

- Server query orchestration acquires the write guard before execution.
- Use `CatalogManager::create_index` to validate the table/columns/name and assign the `IndexId`.
- Call `SchemaOperations::create_index` to build and backfill the secondary tree; on failure, roll back the catalog with `CatalogManager::drop_index` before returning the error (mirroring `CREATE TABLE`).
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

## Statement Guards

Statement guards are owned by server query orchestration, not by the executor crate. The server parses SQL to classify the top-level statement: lock-free SELECT and EXPLAIN take **no** `ConcurrencyController` guard; INSERT, UPDATE, DELETE, and SELECTs whose bound tree contains `nextval`/`setval` acquire the shared writer guard `ConcurrencyController::begin_writer` (many DML writers run concurrently); CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX, CREATE SEQUENCE, DROP SEQUENCE, checkpoint, and `VACUUM` take the exclusive `begin_checkpoint` guard. EXPLAIN runs bind and plan for the inner statement, formats the physical plan in server/planner code, and never calls the executor. A writer's guard lives for the full statement (and, in an explicit transaction, the whole write-transaction). See `docs/specs/crates/server.md` and `docs/specs/mvcc.md` §7 for the full concurrency model.

## Acceptance Tests

- `SeqScanOp` returns rows with identity.
- `SystemScanOp` executes filters, joins, ordering, and projection over virtual catalog rows and returns rows with no identity.
- `FilterOp` preserves identity.
- `ProjectionOp` preserves identity while changing values.
- `NestedLoopJoinOp` clears identity.
- `HashJoinOp` joins inner equi-join rows on one or more key columns and excludes rows with a NULL join key.
- `UPDATE WHERE` modifies only matched rows.
- `DELETE WHERE` deletes only matched rows.
- Failed write triggers rollback (driven by the server/harness, not the executor) and does not expose partial changes.
- Scalar expression evaluator implements SQL NULL boolean cases.
- Aggregate operator computes `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP`, `VARIANCE`/`VAR_SAMP`/`VAR_POP`, `BOOL_AND`, `BOOL_OR`.
