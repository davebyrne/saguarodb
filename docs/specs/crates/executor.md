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

V1 uses Volcano-style pull execution. Operators return `ExecRow`, not plain `Row`, so DML identity survives filters and projections.

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

For SELECT, it materializes plain `Row` values into `ExecutionResult::Query` in v1. For DML/DDL, it executes immediately and returns command metadata. A future server streaming bridge may drive `PlanExecutor` directly without changing physical operator semantics.

Production execution uses an explicit context:

```rust
pub struct ExecutionContext<'a> {
    pub statement: StatementContext,
    pub catalog: &'a dyn CatalogManager,
    pub storage: &'a dyn StorageEngine,
    pub schema_ops: &'a dyn SchemaOperations,
}

pub struct QueryEngine;

impl QueryEngine {
    pub fn execute(&self, ctx: &ExecutionContext<'_>, plan: &PhysicalPlan) -> Result<ExecutionResult>;
}
```

`QueryEngine::execute` passes `ctx.statement` to storage and schema operations. It does not allocate transaction IDs, append commit records, flush WAL, or call storage/buffer commit or rollback; server query orchestration owns those statement-level concerns.

## Operators

| Operator | Behavior |
|---|---|
| `SeqScanOp` | Calls `StorageEngine::scan`, converts `StoredRow` to `ExecRow`, applies scan filter if present |
| `IndexScanOp` | Calls `StorageEngine::scan_range`, converts `StoredRow` to `ExecRow`, then applies `PhysicalPlan::IndexScan.filter` when present |
| `NestedLoopJoinOp` | Buffers right side, implements inner/cross/left/right/full joins with NULL extension for missing side rows, emits concatenated rows, clears identity |
| `HashJoinOp` | Inner equi-join: builds a probe table over the right side keyed by `right_keys`, probes with `left_keys`; rows with a NULL key column never match; emits concatenated rows, clears identity |
| `FilterOp` | Evaluates predicate, preserves identity |
| `ProjectionOp` | Rewrites row values, preserves identity |
| `SortOp` | Materializes all input, sorts in memory, preserves identity |
| `LimitOp` | Skips offset, emits count rows, preserves identity |
| `AggregateOp` | Materializes groups, emits aggregate rows, clears identity |
| `ValuesOp` | Emits literal rows, identity is `None` |

## Identity Rules

- Scans set `identity = Some(RowIdentity { row_id, key })`.
- Filter, sort, limit, and projection preserve identity.
- Join and aggregate clear identity.
- `UPDATE` and `DELETE` require identity on each source row. If a plan produces a row without identity for DML, executor returns `ErrorKind::Internal`.

## Expression Evaluation

```rust
pub fn eval_expr(expr: &BoundExpr, row: &ExecRow) -> Result<Value>;
```

V1 evaluator handles:

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

Division by zero returns `SqlState::DivisionByZero`. Integer overflow in scalar arithmetic or integer aggregate accumulation returns `SqlState::NumericValueOutOfRange`.

V1 expression semantics:

- Comparisons with `NULL` return `Value::Null`; `WHERE` and `HAVING` keep only `Value::Boolean(true)`.
- Boolean `AND`, `OR`, and `NOT` use SQL three-valued logic.
- `LIKE` requires text operands, is case-sensitive, supports `%` for any sequence and `_` for one character, and uses backslash to escape `%`, `_`, or `\`. V1 does not support an `ESCAPE` clause. If the value or pattern is `NULL`, the result is `NULL`.
- `IN` returns `TRUE` on the first non-null equal item, `FALSE` when no item matches and no list item is `NULL`, and `NULL` when the left side is `NULL` or no item matches but some list item is `NULL`. `NOT IN` applies SQL `NOT` to that result.
- `BETWEEN` evaluates as `(expr >= low) AND (expr <= high)` using the same comparison and boolean null semantics. `NOT BETWEEN` applies SQL `NOT`.
- Searched `CASE WHEN condition THEN value ...` chooses the first `WHEN` whose condition evaluates to `TRUE`; `FALSE` and `NULL` conditions do not match. Simple `CASE operand WHEN value THEN result ...` compares `operand = value` with SQL comparison semantics and chooses the first comparison that evaluates to `TRUE`. If no branch matches, both forms return `ELSE` or `NULL`.
- `CASE` result typing is validated by binder: all non-`NULL` `THEN` and `ELSE` expressions must have the same `DataType`; `NULL` branches are allowed and make the output nullable. If every result branch is `NULL`, binder rejects the expression with `SqlState::DatatypeMismatch`.
- Explicit `CAST` conversion matrix: same-type casts are identity; `NULL` casts to `NULL`; `INTEGER -> TEXT` uses decimal i64 formatting; `BOOLEAN -> TEXT` returns `true` or `false`; `TEXT -> INTEGER` parses a base-10 i64 with optional leading sign and no surrounding whitespace; `TEXT -> BOOLEAN` accepts case-insensitive `true`, `t`, `1`, `false`, `f`, and `0`. `INTEGER -> BOOLEAN`, `BOOLEAN -> INTEGER`, malformed text, and all other pairs return `SqlState::DatatypeMismatch`.
- `ORDER BY` defaults match PostgreSQL: ascending sorts `NULL` last, descending sorts `NULL` first, unless `NULLS FIRST` or `NULLS LAST` is specified. A bare positive integer literal in `ORDER BY` is a 1-based reference to the nth output column, resolved by the binder.
- Type mismatches in expression evaluation return `SqlState::DatatypeMismatch`.

Aggregate execution follows planner return-type rules: `COUNT` returns `0` for empty input and ignores nulls for `COUNT(expr)`; `SUM`, `AVG`, `MIN`, and `MAX` return `NULL` for empty input. `AVG(integer)` uses integer division truncated toward zero.

## DML Execution

`INSERT` (from `VALUES` or `SELECT`):

- Materialize the source plan fully before inserting any row, so that `INSERT ... SELECT` reading the target table observes only pre-insert rows.
- For each source row, build row values in table column order.
- Validate runtime values match destination column types. `NULL` is accepted at this step and checked by nullability validation.
- Validate non-null constraints.
- Call `StorageEngine::insert`.
- Return `Modified { command: "INSERT", count }`.

`UPDATE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Evaluate assignments against the source row.
- Build a full replacement row.
- Call `StorageEngine::update`.
- Return count.

`DELETE`:

- Build source executor.
- For each source `ExecRow`, read identity key.
- Call `StorageEngine::delete`.
- Return count.

If a write errors after mutating pages or storage-owned metadata, executor/server orchestration must call `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)` before returning the error.

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

## Statement Guards

Statement guards are owned by server query orchestration, not by the executor crate. The server parses SQL to classify the top-level statement, acquires `ConcurrencyController::begin_read` for SELECT and EXPLAIN or `begin_write` for INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, and checkpoint. SELECT runs bind, plan, and `QueryEngine::execute` while holding that guard. EXPLAIN runs bind and plan for the inner statement, formats the physical plan in server/planner code, and never calls the executor. The guard lives for the full statement.

## Acceptance Tests

- `SeqScanOp` returns rows with identity.
- `FilterOp` preserves identity.
- `ProjectionOp` preserves identity while changing values.
- `NestedLoopJoinOp` clears identity.
- `HashJoinOp` joins inner equi-join rows on one or more key columns and excludes rows with a NULL join key.
- `UPDATE WHERE` modifies only matched rows.
- `DELETE WHERE` deletes only matched rows.
- Failed write calls rollback and does not expose partial changes.
- Scalar expression evaluator implements SQL NULL boolean cases.
- Aggregate operator computes `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.
