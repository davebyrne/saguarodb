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

## Query Engine Boundary

The concrete server `QueryService` wires:

```text
parse -> bind -> logical_plan -> physical_plan -> execute
```

For SELECT, it materializes plain `Row` values into `ExecutionResult::Query`. For DML/DDL, it executes immediately and returns command metadata. A future server streaming bridge may drive `PlanExecutor` directly without changing physical operator semantics.

`ExecutionResult` has three variants: `Query` (SELECT rows and columns), `Modified { command, count }` (DML/DDL), and `Explanation { text }` (EXPLAIN). `QueryEngine::execute` produces only `Query` and `Modified`; `Explanation` is produced by the server's `QueryService` (EXPLAIN never calls the executor), but the variant lives in the executor crate's `ExecutionResult`.

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
- Filter, sort, limit, and projection preserve identity.
- Join, aggregate, and distinct clear identity.
- `UPDATE` and `DELETE` require identity on each source row. If a plan produces a row without identity for DML, executor returns `ErrorKind::Internal`.

## Expression Evaluation

```rust
pub fn eval_expr(expr: &BoundExpr, row: &ExecRow) -> Result<Value>;
```

The evaluator handles:

- Literals and `InputRef`.
- Arithmetic: `+`, `-`, `*`, `/`, `%`.
- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`.
- String concatenation: `||` over text operands, NULL-propagating; non-text operands return `SqlState::DatatypeMismatch`.
- Boolean: `AND`, `OR`, `NOT` with SQL three-valued logic.
- `IS NULL`, `IS NOT NULL`.
- `IN`, `BETWEEN`, `LIKE`.
- `CASE`.
- `CAST`.
- Scalar functions `UPPER`, `LOWER`, `LENGTH`, `TRIM` (text), `ABS` (integer), and `SUBSTRING(text, start[, length])`. All are NULL-propagating (any NULL argument yields NULL). `LENGTH` and `SUBSTRING` count Unicode characters, not bytes; `SUBSTRING` uses 1-based start positions clamped to the string and rejects a negative length with `SqlState::DatatypeMismatch`.
- Aggregate functions are evaluated by `AggregateOp`, not by scalar evaluation.
- `LocalRef` indexes into the current `ExecRow` values. `AggregateCall` must not reach scalar evaluation; logical planning rewrites it before physical execution.
- `Parameter` (`$n`) references must be substituted to literals before execution. One reaching the evaluator is an internal error (`"unbound parameter $N reached the executor"`).

Division by zero returns `SqlState::DivisionByZero`. Integer overflow in scalar arithmetic or integer aggregate accumulation returns `SqlState::NumericValueOutOfRange`.

Expression semantics:

- Comparisons with `NULL` return `Value::Null`; `WHERE` and `HAVING` keep only `Value::Boolean(true)`.
- Boolean `AND`, `OR`, and `NOT` use SQL three-valued logic.
- `LIKE` requires text operands, is case-sensitive, supports `%` for any sequence and `_` for one character, and uses backslash to escape `%`, `_`, or `\`. A backslash before any other character is treated as a literal backslash followed by that character, and a trailing lone backslash is a literal backslash. `LIKE` does not support an `ESCAPE` clause. If the value or pattern is `NULL`, the result is `NULL`.
- `IN` returns `TRUE` on the first non-null equal item, `FALSE` when no item matches and no list item is `NULL`, and `NULL` when the left side is `NULL` or no item matches but some list item is `NULL`. `NOT IN` applies SQL `NOT` to that result.
- `BETWEEN` evaluates as `(expr >= low) AND (expr <= high)` using the same comparison and boolean null semantics. `NOT BETWEEN` applies SQL `NOT`.
- Searched `CASE WHEN condition THEN value ...` chooses the first `WHEN` whose condition evaluates to `TRUE`; `FALSE` and `NULL` conditions do not match. Simple `CASE operand WHEN value THEN result ...` compares `operand = value` with SQL comparison semantics and chooses the first comparison that evaluates to `TRUE`. If no branch matches, both forms return `ELSE` or `NULL`.
- `CASE` result typing is validated by binder: all non-`NULL` `THEN` and `ELSE` expressions must have the same `DataType`; `NULL` branches are allowed and make the output nullable. If every result branch is `NULL`, binder rejects the expression with `SqlState::DatatypeMismatch`.
- Explicit `CAST` conversion matrix: same-type casts are identity; `NULL` casts to `NULL`; `INTEGER -> TEXT` uses decimal i64 formatting; `BOOLEAN -> TEXT` returns `true` or `false`; `TEXT -> INTEGER` parses a base-10 i64 with optional leading sign and no surrounding whitespace; `TEXT -> BOOLEAN` accepts case-insensitive `true`, `t`, `1`, `false`, `f`, and `0`. `DATE -> TEXT` formats `YYYY-MM-DD`; `TEXT -> DATE` parses `YYYY-MM-DD` and rejects impossible dates. `TIMESTAMP -> TEXT` formats `YYYY-MM-DD HH:MM:SS[.ffffff]`; `TEXT -> TIMESTAMP` parses that form and rejects impossible date/times. `BYTEA -> TEXT` formats the hex `\x...` form; `TEXT -> BYTEA` parses it (hex only). `INTEGER -> BOOLEAN`, `BOOLEAN -> INTEGER`, `DATE`/`TIMESTAMP <-> INTEGER`, `DATE <-> TIMESTAMP`, malformed text, and all other pairs return `SqlState::DatatypeMismatch`.
- `ORDER BY` defaults match PostgreSQL: ascending sorts `NULL` last, descending sorts `NULL` first, unless `NULLS FIRST` or `NULLS LAST` is specified. A bare positive integer literal in `ORDER BY` is a 1-based reference to the nth output column, resolved by the binder.
- Type mismatches in expression evaluation return `SqlState::DatatypeMismatch`.

Aggregate execution groups input rows by the `GROUP BY` expressions into ordered groups and emits one output row per group (group-key columns first, then the aggregates); with no `GROUP BY` the entire input is a single group. A `DISTINCT` aggregate argument (e.g. `COUNT(DISTINCT x)`) de-duplicates its argument values before aggregating. Return-type rules: `COUNT` returns `0` for empty input and ignores nulls for `COUNT(expr)`; `SUM`, `AVG`, `MIN`, and `MAX` return `NULL` for empty input. `SUM` and `AVG` require integer input and otherwise return `SqlState::DatatypeMismatch`; `AVG(integer)` uses integer division truncated toward zero. `MIN` and `MAX` order any `Value` type (including text and boolean) via the value ordering, ignoring nulls.

## DML Execution

`INSERT` (from `VALUES` or `SELECT`):

- Materialize the source plan fully before inserting any row, so that `INSERT ... SELECT` reading the target table observes only pre-insert rows.
- For each source row, build row values in table column order.
- Validate runtime values match destination column types. `NULL` is accepted at this step and checked by row-constraint validation.
- Validate per-column row constraints (`validate_row_constraints`): non-null, and the bounded character-type length — a `Text` value whose character count exceeds a column's `max_length` (`VARCHAR(n)`/`CHAR(n)`) is rejected with `SqlState::StringDataRightTruncation` (`22001`). This runs on the full row, so it covers `INSERT ... VALUES`, `INSERT ... SELECT`, and `COPY ... FROM`.
- Call `StorageEngine::insert`.
- Return `Modified { command: "INSERT", count }`.

`UPDATE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Evaluate assignments against the source row.
- Build a full replacement row. The primary-key column cannot change; storage rejects an update whose replacement key differs with `SqlState::DatatypeMismatch` ("primary key updates are not supported").
- Validate per-column row constraints on the replacement row (`validate_row_constraints`): non-null and bounded character-type length, same as INSERT.
- Call `StorageEngine::update`.
- Return count.

`DELETE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Call `StorageEngine::delete`.
- Return count.

If a write errors after mutating pages or storage-owned metadata, the executor propagates the error without rolling back itself (consistent with `QueryEngine::execute` not calling storage/buffer commit or rollback). The server query orchestration — or the test harness — owns recovery and calls `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)` before returning the error.

## DDL Execution

`CREATE TABLE`:

- Server query orchestration acquires the write guard before execution.
- Use `CatalogManager::create_table` to assign IDs.
- Call `SchemaOperations::create_table`.
- `SchemaOperations::create_table` appends the `CreateTable` WAL operation record; server query orchestration appends the statement `Commit`.
- Return `Modified { command: "CREATE TABLE", count: 0 }`.

`DROP TABLE`:

- Resolve table in binder.
- Call `SchemaOperations::drop_table`.
- Call `CatalogManager::drop_table`.
- `SchemaOperations::drop_table` appends the `DropTable` WAL operation record; server query orchestration appends the statement `Commit`.
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

## Statement Guards

Statement guards are owned by server query orchestration, not by the executor crate. The server parses SQL to classify the top-level statement: SELECT and EXPLAIN are lock-free readers and take **no** `ConcurrencyController` guard; INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX, and DROP INDEX acquire the shared writer guard `ConcurrencyController::begin_writer` (many writers run concurrently); checkpoint and `VACUUM` take the exclusive `begin_checkpoint` guard. SELECT runs bind, plan, and `QueryEngine::execute` lock-free. EXPLAIN runs bind and plan for the inner statement, formats the physical plan in server/planner code, and never calls the executor. A writer's guard lives for the full statement (and, in an explicit transaction, the whole write-transaction). See `docs/specs/crates/server.md` and `docs/specs/mvcc.md` §7 for the full concurrency model.

## Acceptance Tests

- `SeqScanOp` returns rows with identity.
- `FilterOp` preserves identity.
- `ProjectionOp` preserves identity while changing values.
- `NestedLoopJoinOp` clears identity.
- `HashJoinOp` joins inner equi-join rows on one or more key columns and excludes rows with a NULL join key.
- `UPDATE WHERE` modifies only matched rows.
- `DELETE WHERE` deletes only matched rows.
- Failed write triggers rollback (driven by the server/harness, not the executor) and does not expose partial changes.
- Scalar expression evaluator implements SQL NULL boolean cases.
- Aggregate operator computes `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.
