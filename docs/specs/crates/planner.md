# `planner` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`planner` converts parser AST into executable physical plans through three explicit phases:

1. Bind: resolve names, validate types, assign slots.
2. Logical plan: describe what to compute.
3. Physical plan: choose access methods and algorithms.

V1 physical planning is rule-based and naive, but the phase boundary is real.

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

`bind` is the simple-query entry point and rejects `$n` parameters. `bind_parameterized`
binds an extended-protocol statement, resolving each parameter's type from the
`Parse`-declared OID when given, otherwise inferring it from context (like a `NULL`
literal); it returns the bound statement and the resolved parameter types by position.
`substitute_params` replaces each `BoundExpr::Parameter` with a type-checked literal of
the bound value before planning and execution.

## Binder Contract

Binder output is fully resolved. No downstream phase performs name lookup. The binder is the primary SQL type checker; the executor may still defensively validate runtime DML values before storage writes.

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
- Validate insert/update value types and nullability. For `INSERT ... SELECT`, bind the query, require its output column count to match the target columns, and validate each output expression's type and nullability against the target column.
- Validate aggregate usage and `GROUP BY` rules.
- Validate `CASE` result typing: all non-`NULL` `THEN` and `ELSE` expressions must have the same `DataType`; `NULL` branches are allowed and make the output nullable; all-`NULL` result branches are rejected with `SqlState::DatatypeMismatch`.
- Reject unsupported v1 forms.

```rust
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
    pub alias: String,
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

`BoundSelect` is also used as the source for `UPDATE` and `DELETE`, preserving filters and row identity through execution.

`CREATE INDEX` binds as a pass-through (name, table, columns, unique), like `CREATE TABLE`: the catalog validates that the table and columns exist and the index name is unused at execute time. `DROP INDEX` resolves the index name to its `IndexId` at bind time, rejecting an unknown index with `UndefinedTable` (mirroring `DROP TABLE`).

`BoundFrom::Join.condition` is `None` only for `JoinType::Cross`; all other join types have a boolean `Some(condition)`. The binder rejects missing `ON` predicates for non-cross joins and rejects `ON`/`USING`/`NATURAL` with `CROSS JOIN` in v1. The executor treats a cross join's `None` condition as `TRUE`.

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
}
```

Every `BoundExpr` variant carries its resolved output `data_type` and `nullable` value. Binder assigns these fields before logical planning, and logical/physical planning preserves them when rewriting expressions. `slot` is the runtime access path for `InputRef` and `LocalRef`; `input` and `column` are for debugging, EXPLAIN, and future rebinding. A `Value::Null` literal is typed by context during binding; if no context can determine a valid V1 `DataType`, binder rejects it with `SqlState::DatatypeMismatch`. For `NULL IN (...)`, binder may infer the left-side `NULL` type from the first typed list expression, and rejects the expression only when the list also provides no type context.

Expression metadata rules:

- `Literal`: type comes from the value; `Value::Null` uses the binder-assigned context type and is nullable.
- `InputRef`: type and nullability come from the source column.
- Arithmetic `BinaryOp` and `UnaryOp::Neg`: integer output, nullable when any operand is nullable.
- Comparison `BinaryOp`, boolean `BinaryOp`, `UnaryOp::Not`, `InList`, `Between`, and `Like`: boolean output; nullable when SQL three-valued logic can produce `NULL` from nullable operands.
- `IsNull` and `IsNotNull`: boolean output and `nullable = false`.
- `Case`: binder-selected result type; nullable when any selected result expression is nullable or no `ELSE` exists.
- `Cast`: target type; nullable matches the input expression.
- `AggregateCall` and `LocalRef`: use the aggregate/group output metadata assigned by logical planning.

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

Aggregate calls use a two-stage representation. Binder converts `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` into `BoundExpr::AggregateCall`; scalar functions remain `BoundExpr::Function`. Logical planning extracts unique aggregate calls from SELECT, HAVING, and ORDER BY expressions into `AggregateExpr` values, then rewrites aggregate and grouped-expression references above the `Aggregate` node to `BoundExpr::LocalRef`. The `Aggregate` output row layout is group-by values first, then aggregate values. `BoundExpr::AggregateCall` is illegal in physical plans handed to executor scalar evaluation.

Aggregate `DISTINCT` is rejected in v1 with `ErrorKind::Plan`; `AggregateExpr.distinct` is always `false`. Aggregate return types are fixed: `COUNT` returns non-null `INTEGER`; `SUM(integer)` returns nullable `INTEGER`; `AVG(integer)` returns nullable `INTEGER` using integer division truncated toward zero; `MIN` and `MAX` return the argument type and are nullable. `SUM` and `AVG` reject non-integer arguments with `SqlState::DatatypeMismatch`. Empty aggregate inputs return `0` for `COUNT` and `NULL` for `SUM`, `AVG`, `MIN`, and `MAX`.

Scalar functions remain `BoundExpr::Function`. Binder validates each call's arity and argument types and assigns its result type: `UPPER(text)`, `LOWER(text)`, `TRIM(text)` return `TEXT`; `LENGTH(text)` returns `INTEGER`; `ABS(integer)` returns `INTEGER`; `SUBSTRING(text, integer[, integer])` returns `TEXT`. All are NULL-propagating, so the result is nullable when any argument is. Unknown function names, wrong arity, and argument-type mismatches are rejected with `ErrorKind::Plan` (`SyntaxError` for unknown names and arity, `DatatypeMismatch` for argument types). Aggregates may appear as scalar-function arguments (e.g. `ABS(SUM(id))`); logical planning rewrites the nested aggregate as usual.

## Logical Plan

```rust
pub enum LogicalPlan {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<LogicalPlan> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<LogicalPlan> },
    Delete { table: TableId, source: Box<LogicalPlan> },
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
```

Logical plan contains no access method choices.

## Physical Plan

```rust
pub enum PhysicalPlan {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { table: TableId },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<PhysicalPlan> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<PhysicalPlan> },
    Delete { table: TableId, source: Box<PhysicalPlan> },
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

## V1 Physical Rules

- A scan with an equality or range predicate on the leading column of an index — the primary-key index or a secondary index — becomes an `IndexScan` over that index (`index = PRIMARY_KEY_INDEX_ID` for the primary key, else the secondary index id), with `range` an exact or bounded `KeyRange` over that column.
- When more than one index's leading column is constrained, the planner picks the best: an equality match beats a range, the primary key beats a secondary index (it avoids the secondary → primary-key → heap indirection), and a lower index id breaks remaining ties.
- `filter` stores residual predicates not consumed by the chosen index's range, re-checked by the scan operator (so the choice of index never changes results). For `WHERE id = 7 AND name = 'Ada'`, the primary-key index wins with exact key `7` and the residual filter is `name = 'Ada'`. For `WHERE id = 7`, `filter` is `None`.
- Otherwise scans are `SeqScan`.
- `table_name` is captured at planning time solely for EXPLAIN/debug output; execution still uses `table`.
- Joins are left-to-right nested loop joins. V1 supports `Inner`, `Cross`, `Left`, `Right`, and `Full` join types. Logical and physical join `condition` is `None` only for `Cross` and `Some(boolean_expr)` for every other join type.
- An `Inner` join whose `condition` is a conjunction of `left_column = right_column` equalities becomes a `HashJoin`. `left_keys` and `right_keys` are the paired key column slots, relative to each child row (right slots are rebased by the left child width; join inputs are left-deep, so a child row's column positions match its global slots). All other joins — outer, cross, non-equi, or predicates over expressions rather than bare columns — stay `NestedLoopJoin`.
- Sort and aggregate are blocking operators.
- Projection pushdown may be disabled in initial v1 implementation. If enabled, expressions must be slot-rebased against child output schemas.

## EXPLAIN

`EXPLAIN` ownership is split cleanly:

- Parser emits `Statement::Explain(inner)`.
- Binder emits `BoundStatement::Explain(inner_bound)`.
- `logical_plan` and `physical_plan` do not accept `BoundStatement::Explain` directly; callers must unwrap and plan the inner bound statement.
- The planner crate exposes `format_explain(plan: &PhysicalPlan) -> String`.
- The server `QueryService` handles the outer `EXPLAIN` statement by acquiring a read guard, binding the inner statement, building logical and physical plans for that inner statement, formatting the physical plan with `format_explain`, and returning `ExecutionResult::Explanation`.

The executor crate is not called for `EXPLAIN`.

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
