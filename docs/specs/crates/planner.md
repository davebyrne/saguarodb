# `planner` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`planner` converts parser AST into executable physical plans through three explicit phases:

1. Bind: resolve names, validate types, assign slots.
2. Logical plan: describe what to compute.
3. Physical plan: choose access methods and algorithms.

Physical planning is rule-based and naive, but the phase boundary is real.

## Depends On

- `common`
- `catalog`
- `parser`

## Public API

```rust
pub fn bind(statement: &Statement, catalog: &dyn CatalogManager) -> Result<BoundStatement>;
pub fn bind_parameterized(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    declared_param_types: &[Option<DataType>],
) -> Result<(BoundStatement, Vec<DataType>)>;
pub fn collect_param_types(
    statement: &BoundStatement,
    declared: &[Option<DataType>],
) -> Result<Vec<DataType>>;
pub fn substitute_params(statement: &BoundStatement, params: &[Value]) -> Result<BoundStatement>;
pub fn logical_plan(bound: &BoundStatement) -> Result<LogicalPlan>;
pub fn physical_plan(logical: &LogicalPlan, catalog: &dyn CatalogManager) -> Result<PhysicalPlan>;
```

`bind` is the simple-query entry point and rejects `$n` parameters with `SqlState::SyntaxError`. `bind_parameterized`
binds an extended-protocol statement, resolving each parameter's type from the
`Parse`-declared OID when given, otherwise inferring it from context (like a `NULL`
literal); it returns the bound statement and the resolved parameter types by position.
`substitute_params` replaces each `BoundExpr::Parameter` with a type-checked literal of
the bound value before planning and execution. `collect_param_types` and
`substitute_params` live in the crate's `params` module and are re-exported from the
crate root; `bind`/`bind_parameterized` call `collect_param_types` internally.

## Binder Contract

Binder output is fully resolved for DML and most DDL. No downstream phase performs table, column, or index name lookup; the executor may still defensively validate runtime DML values before storage writes. `DROP SEQUENCE` is a deliberate exception: it carries the normalized sequence name plus the `IF EXISTS` flag so extended-protocol prepared statements resolve existence at execution time instead of baking in a stale missing-object no-op. `CREATE TABLE` with `SERIAL` is another exception: the binder records the SERIAL columns, and the executor chooses owned sequence names at execution time under the DDL guard so prepared DDL cannot bake in stale collision checks.

Binder responsibilities:

- Resolve table names to `TableId`.
- Assign unique `BindingId` to every table occurrence.
- Resolve columns to `BoundExpr::InputRef`.
- Assign slot indices in operator input rows.
- Expand wildcards.
- Resolve `ORDER BY` items: a bare positive integer literal is a 1-based
  reference to the nth output column (PostgreSQL ordinal `ORDER BY`); an
  out-of-range position is rejected with `SqlState::SyntaxError`. A bare
  unqualified name first matches an output column alias. All other `ORDER BY`
  expressions bind as ordinary value expressions.
- Validate `WHERE` and join predicates are boolean.
- Validate insert/update value types and nullability. For `INSERT ... SELECT`, bind the query, require its output column count to match the target columns, and validate each output expression's type and nullability against the target column. A `NOT NULL` column may be omitted from an `INSERT` only when it has a non-`NULL` `ColumnDefault::Const` or a `ColumnDefault::Nextval`; otherwise the omission is rejected with `SqlState::NotNullViolation`.
- Validate each `CREATE TABLE` column `DEFAULT` against the column type (no implicit casts): `ParsedDefault::Const(value)` must match the column's `DataType` (any `Numeric` value matches any `NUMERIC(p, s)` column), else `SqlState::DatatypeMismatch`; a `NULL` default is accepted only on a nullable column (else `SqlState::NotNullViolation`). `ParsedDefault::Nextval(name)` must resolve to an existing non-owned sequence (`SqlState::UndefinedTable` if missing, `SqlState::DependentObjectsStillExist` if it names an owned SERIAL sequence) and requires an `INTEGER` target column. `ParsedDefault::Serial` requires an `INTEGER` target column and records the SERIAL column name and ordinal on the bound `CREATE TABLE`; generated owned sequence names are chosen by the executor.
- Bind `ON CONFLICT` (`bind_on_conflict`): the arbiter is **always the primary key**. An explicit conflict target must name exactly the primary-key column(s) — any other column list (a secondary unique index) is rejected with `FeatureNotSupported`; a missing target is allowed for `DO NOTHING` but rejected for `DO UPDATE`. `DO NOTHING` binds to `BoundOnConflict::DoNothing`. `DO UPDATE SET ... [WHERE ...]` binds over **two** bindings — the target table (slots `0..n`, bare columns resolve here) and a `qualified_only` `excluded` pseudo-table (slots `n..2n`, only `excluded.<col>` resolves) — so a bare column means the existing row and `excluded.<col>` the proposed row (matching PostgreSQL, no ambiguity). The primary key cannot be assigned and duplicate assignments are rejected, as in `UPDATE`.
- Bind `RETURNING` (`bind_returning`, shared by INSERT/UPDATE/DELETE): the projection items bind against a single binding of the target table in catalog (slot) order, so the expressions reference the affected full row by slot. `*`/`table.*` expand to all table columns; expressions, aliases, and `derive_alias` work as in the `SELECT` list; aggregate calls are rejected (`DatatypeMismatch`). The result is `Some(BoundReturning { exprs, output_schema })` (the `RowDescription`), or `None` with no clause. `RETURNING` expressions may carry `$n` parameters — `collect_param_types`/`substitute_params` traverse them.
- Bind `COPY` (`bind_copy`): resolve the table to `TableId` and the column list to `ColumnId`s (reusing the INSERT column resolver — empty list defaults to all columns in catalog order, duplicates are `DatatypeMismatch`, unknown columns `UndefinedColumn`), carrying `direction`/`options` through. Unlike INSERT it does not reject an omitted NOT NULL column up front; that surfaces per row at insert time (matching PostgreSQL). COPY is not lowered to a `LogicalPlan` — `logical_plan` rejects `BoundStatement::Copy` (internal error); the server drives COPY directly (`docs/specs/copy.md`).
- Bind `CREATE SEQUENCE` as a pass-through carrying the normalized
  `SequenceOptions`. Bind `DROP SEQUENCE` as a pass-through carrying the
  normalized sequence name and `IF EXISTS`; the executor resolves the sequence
  at statement execution time so a prepared `DROP SEQUENCE IF EXISTS` does not
  remain a no-op if the sequence is created after `Parse` and before `Execute`.
- Validate aggregate usage and `GROUP BY` rules.
- Validate `CASE` result typing: all non-`NULL` `THEN` and `ELSE` expressions must have the same `DataType`; `NULL` branches are allowed and make the output nullable; all-`NULL` result branches are rejected with `SqlState::DatatypeMismatch`.
- Reject unsupported forms. Concretely, the binder rejects: an empty primary key (`SqlState::DatatypeMismatch`) and duplicate primary-key columns (`SqlState::SyntaxError`) in `CREATE TABLE` (a composite multi-column primary key is accepted); an `UPDATE` assigning the primary-key column (`SqlState::DatatypeMismatch`); and duplicate `UPDATE` assignments or duplicate `INSERT` target columns (`SqlState::DatatypeMismatch`).

```rust
pub enum BoundStatement {
    CreateTable {
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        serial: Vec<SerialColumn>,
    },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },
    Insert { table: TableId, columns: Vec<ColumnId>, source: BoundInsertSource, on_conflict: Option<BoundOnConflict>, returning: Option<BoundReturning> },
    Select(BoundSelect),
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: BoundSelect, returning: Option<BoundReturning> },
    Delete { table: TableId, source: BoundSelect, returning: Option<BoundReturning> },
    Explain(Box<BoundStatement>),
    // COPY <table> [(cols)] FROM STDIN | TO STDOUT. Resolved table + column ids
    // (COPY order; defaulted to all columns in catalog order). Not lowered to a
    // LogicalPlan — the server drives COPY directly (docs/specs/copy.md).
    Copy { table: TableId, columns: Vec<ColumnId>, direction: CopyDirection, options: CopyOptions },
}

pub enum BoundInsertSource {
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
    Query(Box<BoundSelect>),
}

// A bound RETURNING clause: the projection expressions evaluated over each
// affected full row, and the result-set column metadata (the RowDescription).
pub struct BoundReturning { exprs: Vec<BoundExpr>, output_schema: Vec<ColumnInfo> }

// A bound ON CONFLICT action (arbiter = primary key). DoUpdate's assignment value
// expressions and the optional filter are bound over `target ++ excluded` — the
// existing row in slots 0..n and the proposed row in slots n..2n.
pub enum BoundOnConflict {
    DoNothing,
    DoUpdate { assignments: Vec<(ColumnId, BoundExpr)>, filter: Option<BoundExpr> },
}

pub struct BoundSelect {
    pub distinct: Option<BoundDistinct>,  // All | On(keys)
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

pub enum BoundDistinct {
    All,                  // SELECT DISTINCT
    On(Vec<BoundExpr>),   // SELECT DISTINCT ON (exprs)
}

pub struct BoundSelectItem {
    pub expr: BoundExpr,
    pub alias: String,
}

pub enum BoundFrom {
    Table {
        table: TableId,
        binding: BindingId,
        alias: Option<String>,
        schema: Vec<ColumnDef>,
    },
    Derived {                     // (SELECT ...) AS alias [(cols)]
        select: Box<BoundSelect>,
        binding: BindingId,
        alias: String,
        schema: Vec<ColumnDef>,   // derived columns projected into the outer scope
    },
    Join {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        join_type: JoinType,
        condition: Option<BoundExpr>,
    },
}
```

A `BoundFrom::Derived` binds its inner `SELECT` in a fresh (uncorrelated) scope and exposes its output columns under `alias`, renamed left to right by the optional column-alias list (more aliases than columns is `SqlState::SyntaxError`). The derived columns occupy a contiguous slot range at the derived binding, just like a base table, so logical planning lowers a derived table to its inner SELECT's plan (no dedicated plan node or executor operator); an outer `WHERE` over a standalone derived table becomes a `Filter` above it. Derived-column references have no underlying table (their `ColumnInfo.table_id` is `None`).

`BoundSelect` is also used as the source for `UPDATE` and `DELETE`, preserving filters and row identity through execution.

`CREATE INDEX` binds as a pass-through (name, table, columns, unique), like `CREATE TABLE`: the catalog validates that the table and columns exist and the index name is unused at execute time. `DROP INDEX` resolves the index name to its `IndexId` at bind time, rejecting an unknown index with `UndefinedTable` (mirroring `DROP TABLE`).

`BoundFrom::Join.condition` is `None` only for `JoinType::Cross`; all other join types have a boolean `Some(condition)`. The binder rejects missing `ON` predicates for non-cross joins; the parser rejects `ON`/`USING`/`NATURAL` on a `CROSS JOIN` at parse time (`SqlState::SyntaxError`). The executor treats a cross join's `None` condition as `TRUE`. A comma-separated `FROM a, b, ...` list desugars into a left-deep chain of `JoinType::Cross` joins, each with `condition: None`.

## Bound Expressions

```rust
pub enum BoundExpr {
    Literal {
        value: Value,
        data_type: DataType,
        nullable: bool,
    },
    Parameter {
        index: usize, // 0-based; replaced with a Literal by substitute_params
        data_type: DataType,
        nullable: bool,
    },
    InputRef {
        input: BindingId,
        column: ColumnId,
        slot: usize,
        data_type: DataType,
        nullable: bool,
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
    // Subquery expressions. Each carries its inner SELECT as a `Box<BoundSelect>`
    // bound in its own (uncorrelated) scope, preserved unchanged through logical
    // and physical planning and evaluated by the executor.
    ScalarSubquery {              // (SELECT ...) used as a single value
        select: Box<BoundSelect>,
        data_type: DataType,      // the subquery's single output column type
        nullable: bool,           // always true (an empty result is NULL)
    },
    Exists {                      // [NOT] EXISTS (SELECT ...)
        select: Box<BoundSelect>,
        negated: bool,
        data_type: DataType,      // Boolean
        nullable: bool,           // false (EXISTS never yields NULL)
    },
    InSubquery {                  // expr [NOT] IN (SELECT ...)
        expr: Box<BoundExpr>,     // left operand (outer scope)
        select: Box<BoundSelect>, // single-column subquery
        negated: bool,
        data_type: DataType,      // Boolean
        nullable: bool,
    },
}
```

Every `BoundExpr` variant carries its resolved output `data_type` and `nullable` value. Binder assigns these fields before logical planning, and logical/physical planning preserves them when rewriting expressions. `slot` is the runtime access path for `InputRef` and `LocalRef`; `input` and `column` are for debugging, EXPLAIN, and future rebinding. A `Value::Null` literal is typed by context during binding; if no context can determine a valid `DataType`, binder rejects it with `SqlState::DatatypeMismatch`. For `NULL IN (...)`, binder may infer the left-side `NULL` type from the first typed list expression, and rejects the expression only when the list also provides no type context.

Expression metadata rules:

- `Literal`: type comes from the value; `Value::Null` uses the binder-assigned context type and is nullable.
- `InputRef`: type and nullability come from the source column.
- Arithmetic `BinaryOp` and `UnaryOp::Neg`: integer output, nullable when any operand is nullable.
- Comparison `BinaryOp`, boolean `BinaryOp`, `UnaryOp::Not`, `InList`, `Between`, and `Like`: boolean output; nullable when SQL three-valued logic can produce `NULL` from nullable operands.
- `IsNull` and `IsNotNull`: boolean output and `nullable = false`.
- `Case`: binder-selected result type; nullable when any selected result expression is nullable or no `ELSE` exists.
- `Cast`: target type; nullable matches the input expression.
- `AggregateCall` and `LocalRef`: use the aggregate/group output metadata assigned by logical planning.
- `ScalarSubquery`, `Exists`, `InSubquery`: the binder binds the inner SELECT in a fresh, uncorrelated scope (it does not see the outer query's columns). A scalar subquery and the right side of `IN` must produce exactly one output column (else `SqlState::SyntaxError`); a scalar subquery's type is that column's type and it is always nullable. `EXISTS` is a non-null boolean. For `IN`/`NOT IN`, the left operand is type-checked against the subquery's column type (no implicit casts; mismatch is `SqlState::DatatypeMismatch`). These variants are constants with respect to the outer query, so the outer aggregate/grouping analyses treat them as leaves (only `InSubquery`'s left operand participates in the outer scope). Logical/physical planning preserve the inner `BoundSelect` unchanged; the executor plans and runs it.

## Shared Plan Expression Types

```rust
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

pub enum UnaryOp {
    Neg,
    Not,
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub arg: Option<BoundExpr>,
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

Aggregate calls use a two-stage representation. Binder converts `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, the statistical aggregates `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP` and `VARIANCE`/`VAR_SAMP`/`VAR_POP`, and the boolean aggregates `BOOL_AND`/`BOOL_OR` into `BoundExpr::AggregateCall`; scalar functions remain `BoundExpr::Function`. Logical planning extracts unique aggregate calls from SELECT, HAVING, and ORDER BY expressions into `AggregateExpr` values, then rewrites aggregate and grouped-expression references above the `Aggregate` node to `BoundExpr::LocalRef`. The `Aggregate` output row layout is group-by values first, then aggregate values. `BoundExpr::AggregateCall` is illegal in physical plans handed to executor scalar evaluation.

Aggregate `DISTINCT` (e.g. `COUNT(DISTINCT x)`) is supported: the binder carries the flag into `AggregateExpr.distinct`, and the executor de-duplicates the argument values before aggregating. `DISTINCT` combined with a wildcard argument (`COUNT(DISTINCT *)`) is rejected with `ErrorKind::Plan` / `SqlState::SyntaxError`. Aggregate return types are fixed: `COUNT` returns non-null `INTEGER`; `SUM` and `AVG` accept either numeric type and return that same type (`AVG(integer)` uses integer division truncated toward zero; `AVG(double precision)` is true floating-point division), rejecting non-numeric arguments with `SqlState::DatatypeMismatch`; `MIN` and `MAX` return the argument type and are nullable. `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP` and `VARIANCE`/`VAR_SAMP`/`VAR_POP` accept a numeric argument and return nullable `DOUBLE PRECISION`; `BOOL_AND`/`BOOL_OR` require a boolean argument and return nullable `BOOLEAN`. Empty aggregate inputs return `0` for `COUNT` and `NULL` for the rest (the sample variance/stddev forms also return `NULL` for a single value).

Scalar functions remain `BoundExpr::Function`. Binder validates each call's arity and argument types and assigns its result type: `UPPER(text)`, `LOWER(text)`, `TRIM(text)` return `TEXT`; `LENGTH(text)` returns `INTEGER`; `SUBSTRING(text, integer[, integer])` returns `TEXT`. The math functions accept either numeric type (`INTEGER` or `DOUBLE PRECISION`): `ABS`, `FLOOR`, `CEIL`/`CEILING`, and `ROUND` return their argument's type (`FLOOR`/`CEIL`/`ROUND` of an `INTEGER` is the integer itself; of a `DOUBLE` they round and stay `DOUBLE`); `SQRT` and `POWER`/`POW` always return `DOUBLE` (an `INTEGER` argument is widened, matching PostgreSQL's `sqrt(int)`); `MOD(integer, integer)` returns `INTEGER` (integer-only, like the `%` operator). The string functions `REPLACE(text, text, text)`, `LEFT(text, integer)`, and `RIGHT(text, integer)` return `TEXT`; `POSITION(text, text)` returns `INTEGER`. All of the above are NULL-propagating, so the result is nullable when any argument is. `CONCAT(text, ...)` is variadic (one or more `TEXT` arguments), ignores NULL arguments, and always returns a non-nullable `TEXT` (the empty string when every argument is NULL); non-text arguments must be cast explicitly. `EXTRACT(field FROM source)` binds as `extract('field', source)`: the field literal must name a supported field (`year`/`month`/`day`/`hour`/`minute`/`second`), the source must be `DATE` or `TIMESTAMP`, and the result is `DOUBLE PRECISION` (nullable when the source is). Unknown function names, wrong arity, and argument-type mismatches are rejected with `ErrorKind::Plan` (`SyntaxError` for unknown names and arity, `DatatypeMismatch` for argument types). Aggregates may appear as scalar-function arguments (e.g. `ABS(SUM(id))`); logical planning rewrites the nested aggregate as usual.

`COALESCE` and `NULLIF` are not NULL-propagating, so the binder desugars them to `BoundExpr::Case` rather than leaving them as `Function`s. `COALESCE(v1, ..., vn)` becomes `CASE WHEN v1 IS NOT NULL THEN v1 ... ELSE vn END`; all arguments must share one type (no implicit cast, with a bare untyped NULL taking its type from a sibling — all-NULL is `DatatypeMismatch`), and the result is non-nullable exactly when at least one argument is. `NULLIF(a, b)` becomes `CASE WHEN a = b THEN NULL ELSE a END`; the operands must be comparable (same type) and the result type is `a`'s type, always nullable. `BinOp::IsDistinctFrom` / `IsNotDistinctFrom` bind like a comparison (same-type operands, with one untyped NULL taking the sibling's type) but always yield a non-nullable `Boolean`: two NULLs are not distinct, a NULL and a non-NULL are distinct, otherwise ordinary equality applies.

## Logical Plan

```rust
pub enum LogicalPlan {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<LogicalPlan>, returning: Option<BoundReturning> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<LogicalPlan>, returning: Option<BoundReturning> },
    Delete { table: TableId, source: Box<LogicalPlan>, returning: Option<BoundReturning> },
    Scan { table: TableId, filter: Option<BoundExpr> },
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
}
```

Logical plan contains no access method choices.

For a `SELECT DISTINCT` (`BoundSelect.distinct`), logical planning inserts a
`Distinct` node between any `Sort` and the `Projection`, so keeping the first
row of each distinct key preserves the requested ordering and the later `Limit`
applies to the distinct rows. The `on_keys` depend on the form:

- `All` (plain `SELECT DISTINCT`): the projection expressions, so whole output
  rows are de-duplicated. The binder enforces PostgreSQL's rule that every
  `ORDER BY` expression also appears in the select list, otherwise rejecting it
  with `ErrorKind::Plan` / `SqlState::InvalidColumnReference` (`42P10`).
- `On(keys)` (`SELECT DISTINCT ON (keys)`): the bound `keys`, so the first row
  per key is kept. The binder rejects aggregates inside the keys, and requires
  each leading `ORDER BY` expression (up to the number of keys) to be one of the
  keys — keys absent from `ORDER BY` are allowed — otherwise
  `InvalidColumnReference` (`42P10`). With no `ORDER BY` the kept row per key is
  unspecified.

In an aggregate query the `DISTINCT ON` keys are subject to the same
grouped-expression rule as the select list and `ORDER BY` (a non-grouped,
non-aggregate key is rejected with `SqlState::DatatypeMismatch`), and the
`on_keys` receive the same group rewrite as the projection expressions so they
read the `Aggregate` output.

## Plan-Time Simplification

After logical planning, the planner runs a result-preserving simplification pass
over the `LogicalPlan` (`logical_plan` returns the simplified plan):

- **Constant folding.** Literal-only integer arithmetic, comparison (all types,
  using each value's ordering), and `||` concat sub-expressions, integer
  negation, `NOT`, `IS NULL`, and `IS NOT NULL` over a literal are collapsed to a
  `Literal`. Folding is skipped for any operation that could fail at runtime —
  integer overflow and divide/modulo by zero are left intact so the executor
  raises the same error it would have without folding. Double-precision
  arithmetic and negation are left to the executor (not folded).
- **Boolean simplification.** `AND`/`OR` over two boolean constants fold to a
  constant, and a redundant constant operand is dropped while the other operand
  is kept: `TRUE AND x → x`, `FALSE OR x → x` (and the symmetric forms). The
  planner does **not** collapse `FALSE AND x → FALSE` or `TRUE OR x → TRUE` when
  `x` is not constant: the executor evaluates both operands eagerly (no
  short-circuit), so discarding `x` could suppress a runtime error (e.g. division
  by zero) it would otherwise raise. A simplification therefore never drops a
  non-constant operand.
- **Constant-true predicate removal.** A `Scan.filter` or `Filter.predicate` that
  folds to constant `TRUE` is dropped (`Scan.filter` becomes `None`; the `Filter`
  node is replaced by its source).

The pass never changes a query's result set or output schema; it only narrows
expressions and removes no-op predicates, which can in turn make an index range
usable (e.g. `id = 3 + 4` folds to `id = 7`).

## Physical Plan

```rust
pub enum PhysicalPlan {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<PhysicalPlan>, returning: Option<BoundReturning> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<PhysicalPlan>, returning: Option<BoundReturning> },
    Delete { table: TableId, source: Box<PhysicalPlan>, returning: Option<BoundReturning> },
    SeqScan { table: TableId, table_name: String, filter: Option<BoundExpr> },
    IndexScan { table: TableId, table_name: String, index: IndexId, range: KeyRange, filter: Option<BoundExpr> },
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
}
```

## Physical Rules

- A scan with an equality or range predicate on the leading column of an index — the primary-key index or a secondary index — becomes an `IndexScan` over that index (`index = PRIMARY_KEY_INDEX_ID` for the primary key, else the secondary index id), with `range` an exact or bounded `KeyRange` over that column.
- When more than one index's leading column is constrained, the planner picks the best: an equality match beats a range, the primary key beats a secondary index (it is the canonical access path and reads no separate secondary file), and a lower index id breaks remaining ties.
- `filter` stores residual predicates not consumed by the chosen index's range, re-checked by the scan operator (so the choice of index never changes results). For `WHERE id = 7 AND name = 'Ada'`, the primary-key index wins with exact key `7` and the residual filter is `name = 'Ada'`. For `WHERE id = 7`, `filter` is `None`.
- A lower-bound and an upper-bound comparison on the *same* index column fuse into one two-sided `KeyRange::Range`, consuming both conjuncts. For `WHERE id > 5 AND id < 10`, the range is `(5, 10)` (both bounds excluded) and the residual filter is `None`. This remains a single-column range; multi-column composite-index ranges are not produced.
- Otherwise scans are `SeqScan`.
- Only a literal comparand of type `Integer`, `Text`, or `Boolean` qualifies for an `IndexScan`; a parameter, expression, or other-typed comparand falls back to `SeqScan`.
- The planner emits only `Exact` or bounded `Range` key ranges. The EXPLAIN formatter can additionally render a full-index `KeyRange::All` as `all`, but the planner never produces one.
- `table_name` is captured at planning time solely for EXPLAIN/debug output; execution still uses `table`.
- Joins are left-to-right nested loop joins. The planner supports `Inner`, `Cross`, `Left`, `Right`, and `Full` join types. Logical and physical join `condition` is `None` only for `Cross` and `Some(boolean_expr)` for every other join type.
- An `Inner` join whose `condition` contains at least one `left_column = right_column` equality conjunct becomes a `HashJoin` on those equality pairs. `left_keys` and `right_keys` are the paired key column slots, relative to each child row (right slots are rebased by the left child width; join inputs are left-deep, so a child row's column positions match its global slots). Any remaining (non-equi or expression) conjuncts are re-checked in a `Filter` above the `HashJoin`, using their global joined-row slots. An inner join with no column-equality conjunct, and every outer or cross join, stays a `NestedLoopJoin`.
- Sort and aggregate are blocking operators.
- The planner performs no projection pushdown: `LogicalPlan::Projection` maps straight to `PhysicalPlan::Projection`, and logical planning always wraps a top-level `Projection`.

## EXPLAIN

`EXPLAIN` ownership is split cleanly:

- Parser emits `Statement::Explain(inner)`.
- Binder emits `BoundStatement::Explain(inner_bound)`.
- `logical_plan` and `physical_plan` do not accept `BoundStatement::Explain` directly; callers must unwrap and plan the inner bound statement.
- The planner crate exposes `format_explain(plan: &PhysicalPlan) -> String`.
- The server `QueryService` handles the outer `EXPLAIN` statement lock-free by binding the inner statement, building logical and physical plans for that inner statement, formatting the physical plan with `format_explain`, and returning `ExecutionResult::Explanation`.

The executor crate is not called for `EXPLAIN`.

`format_explain` renders each physical node on its own indented line with a stable label vocabulary, including: `SeqScan table=name(id) filter=yes|none`, `IndexScan table=name(id) index=N range=exact(...)|range(...) filter=yes|none`, `NestedLoopJoin type=… condition=yes|none`, `HashJoin keys=N`, `Filter`, `Projection exprs=N`, `Sort keys=N`, `Distinct keys=N`, `Limit count=… offset=…`, `Aggregate groups=… aggregates=…`, `Values rows=N`, `CreateTable`, `DropTable table=…`, `Create[Unique]Index name on table`, `DropIndex index=N`, `CreateSequence name`, `DropSequence name if_exists=true|false`, and `Insert`/`Update`/`Delete table=…`.

## Acceptance Tests

- Binder resolves aliases and self-joins with distinct `BindingId`s.
- Binder rejects ambiguous unqualified columns.
- Binder expands wildcard projection into explicit bound expressions.
- Binder binds `INSERT ... SELECT` into `BoundInsertSource::Query`, rejecting column-count, type, and nullability mismatches against the target.
- Logical planner emits logical nodes without `SeqScan` or `IndexScan`.
- Physical planner chooses `IndexScan` for an equality or range predicate on a primary-key or secondary-index leading column, preferring the primary key and exact matches, and preserves residual predicates in `IndexScan.filter`.
- Physical planner falls back to `SeqScan` when no index's leading column is constrained.
- Physical planner chooses `HashJoin` for an inner join with a column-equality `ON` predicate and falls back to `NestedLoopJoin` for outer, cross, and non-equi joins.
- `EXPLAIN` returns a readable physical plan tree.
