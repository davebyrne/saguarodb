# `planner` Crate Specification

**Date:** 2026-07-04
**Status:** Living crate contract

User relation names carry an optional schema qualifier. `BindOptions.search_path`
contains the effective schema ids captured by the server for the statement. An
explicit qualifier resolves only in that schema and an unknown schema returns
`InvalidSchemaName`; an unqualified name searches the path in order. CTEs shadow
only unqualified catalog names. Creation uses the first effective path schema,
while stored views persist the path ids used at creation and rebind definitions
against that path rather than the caller's current path.

## Purpose

`planner` converts parser AST into executable physical plans through three explicit phases:

1. Bind: resolve names, validate types, assign slots.
2. Logical plan: describe what to compute.
3. Physical plan: choose access methods and algorithms.

Physical planning is predominantly rule-based, with statistics-backed
cardinality estimates and cost comparisons for scan selection and hash-join
build-side choice. Join order remains left-to-right as written.

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
pub fn bind_parameterized_with_pg_types(
    statement: &Statement,
    catalog: &dyn CatalogManager,
    declared_param_types: &[Option<PgType>],
) -> Result<(BoundStatement, Vec<DataType>)>;
pub fn collect_param_types(
    statement: &BoundStatement,
    declared: &[Option<DataType>],
) -> Result<Vec<DataType>>;
pub fn collect_param_pg_types(
    statement: &BoundStatement,
    resolved_data_types: &[DataType],
    declared: &[Option<PgType>],
) -> Result<Vec<PgType>>;
pub fn substitute_params(statement: &BoundStatement, params: &[Value]) -> Result<BoundStatement>;
pub fn logical_plan(bound: &BoundStatement) -> Result<LogicalPlan>;
pub fn physical_plan(logical: &LogicalPlan, catalog: &dyn CatalogManager) -> Result<PhysicalPlan>;
```

`bind` is the simple-query entry point and rejects `$n` parameters with `SqlState::SyntaxError`. `bind_parameterized`
binds an extended-protocol statement from collapsed declared `DataType`s.
`bind_parameterized_with_pg_types` is the server-facing variant: it resolves each
parameter's semantic type from the `Parse`-declared `PgType` when given,
otherwise inferring it from context (like a `NULL` literal), while preserving the
declared `PgType` for result metadata when a parameter is selected. It also keeps
unambiguous `pg_proc` argument wire hints on placeholders, so
`collect_param_pg_types` can report inferred catalog OID parameters as PostgreSQL
`oid` rather than collapsed `int8`. Both variants return the bound statement and
the resolved parameter data types by position.
`substitute_params` replaces each `BoundExpr::Parameter` with a type-checked literal of
the bound value before planning and execution. `collect_param_types`,
`collect_param_pg_types`, and `substitute_params` live in the crate's `params`
module and are re-exported from the crate root; `bind`/`bind_parameterized` call
`collect_param_types` internally.

## Binder Contract

Binder output is fully resolved for DML and most DDL. No downstream phase performs table, column, or index name lookup; the executor may still defensively validate runtime DML values before storage writes. Schema-evolution `ALTER TABLE` binds the target table to `TableId` and carries the original name only for diagnostics/explain output, so prepared schema-evolution DDL is rejected if the bound table is dropped or its `schema_version` changes before execution. `DROP TABLE IF EXISTS` and `DROP SEQUENCE` are deliberate exceptions: they carry normalized object names plus the `IF EXISTS` flag so extended-protocol prepared statements resolve existence at execution time instead of baking in a stale missing-object no-op. Multi-table DROP retains input order and executes as one statement. `CREATE TABLE IF NOT EXISTS` also carries the table name through execution so the duplicate-table no-op decision uses the current catalog. `CREATE TABLE` with `SERIAL` is another exception: the SERIAL marker travels on the parsed column list itself (`ParsedColumnDef.default = ParsedDefault::Serial`; no parallel list is threaded through the plan), and the executor derives the SERIAL columns and chooses owned sequence names at execution time under the DDL guard so prepared DDL cannot bake in stale collision checks.

Binder responsibilities:

- Resolve table names to `TableId`; if a catalog bug or corrupted snapshot exposes a
  hidden TOAST relation through name lookup, reject it with `FeatureNotSupported`
  rather than binding it as a user-queryable relation.
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
- Validate insert/update value types and nullability. `INSERT ... VALUES`, `UPDATE SET`, and `ON CONFLICT DO UPDATE SET` expression assignments are strict except for PostgreSQL-compatible `TIMESTAMPTZ` expressions assigned to `TIMESTAMP` columns, which the binder wraps in an explicit `BoundExpr::Cast` to `TIMESTAMP`; nullability is still checked after the cast. For `INSERT ... SELECT`, bind the query, require its output column count to match the target columns, and validate each output expression's type and nullability against the target column with no assignment cast. A `NOT NULL` column may be omitted from an `INSERT` only when it has a non-`NULL` `ColumnDefault::Const`, a `ColumnDefault::Nextval`, or a `ColumnDefault::Expr` (an expression default may still evaluate to `NULL`, caught per row at insert time); otherwise the omission is rejected with `SqlState::NotNullViolation`. For each omitted column with a `ColumnDefault::Expr`, the binder re-parses and binds the default's text against an empty scope and attaches the bound expression to the `INSERT`, so the executor evaluates it per row.
- Validate each `CREATE TABLE` column `DEFAULT` against the column type (no implicit casts): `ParsedDefault::Const(value)` must match the column's `DataType` (any `Numeric` value matches any `NUMERIC(p, s)` column), else `SqlState::DatatypeMismatch`; a `NULL` default is accepted only on a nullable column (else `SqlState::NotNullViolation`). `ParsedDefault::Nextval(name)` must resolve to an existing non-owned sequence (`SqlState::UndefinedTable` if missing, `SqlState::DependentObjectsStillExist` if it names an owned SERIAL sequence) and requires an `INTEGER` target column. `ParsedDefault::Serial` requires an `INTEGER` target column; the marker stays on the parsed column list (nothing extra is recorded on the bound `CREATE TABLE`), and the executor derives the SERIAL columns and chooses generated owned sequence names at execution time. `ParsedDefault::Expr(text)` is a non-constant default: the binder re-parses the text (`parser::parse_expression`) and binds it against an empty column scope (so a column reference fails as `SqlState::UndefinedColumn`), rejects aggregates/subqueries/parameters (`SqlState::FeatureNotSupported`), and requires the result type be assignable to the column (`SqlState::DatatypeMismatch`); a `NULL` result is not checked here (it is caught per row at insert). The text is stored as `ColumnDefault::Expr` and re-bound per statement at `INSERT`.
- Validate each `CREATE TABLE` `CHECK` constraint (`Statement::CreateTable.checks`, canonical text): the binder re-parses the text (`parser::parse_expression`) and binds it against the table's not-yet-created columns registered as a single binding at slot 0 (`bind_check_expr`), so an unqualified column reference resolves (a `CHECK` may name the row's columns, unlike a `DEFAULT`), an unknown column fails as `SqlState::UndefinedColumn`, table-qualified column references are rejected with `SqlState::FeatureNotSupported` so stored check text remains table-rename-safe, aggregates/subqueries/parameters are rejected (`SqlState::FeatureNotSupported`, shared with `DEFAULT` via `reject_non_constraint_safe`), and a non-boolean result is `SqlState::DatatypeMismatch`. The bound form is discarded at `CREATE TABLE`; the text is stored on `TableSchema.checks`. At each `INSERT`/`UPDATE`, the binder re-binds the table's checks against the table's columns (`bind_table_checks`) and attaches the bound expressions to the statement, so the executor evaluates them over each affected full row.
- Bind `ON CONFLICT` (`bind_on_conflict`): the arbiter is **always the primary key**. An explicit conflict target must name exactly the primary-key column(s) — any other column list (a secondary unique index) is rejected with `FeatureNotSupported`; a missing target is allowed for `DO NOTHING` but rejected for `DO UPDATE`. The bound form stores the arbiter column IDs when a primary key exists, including targetless `DO NOTHING`, so prepared statements revalidate the same arbiter after primary-key DDL; if no primary key exists at bind time, targetless `DO NOTHING` binds with no arbiter. `DO UPDATE SET ... [WHERE ...]` binds over **two** bindings — the target table (slots `0..n`, bare columns resolve here) and a `qualified_only` `excluded` pseudo-table (slots `n..2n`, only `excluded.<col>` resolves) — so a bare column means the existing row and `excluded.<col>` the proposed row (matching PostgreSQL, no ambiguity). Duplicate assignments are rejected, as in `UPDATE`.
- Bind `RETURNING` (`bind_returning`, shared by INSERT/UPDATE/DELETE): the projection items bind against a single binding of the target table in catalog (slot) order, so the expressions reference the affected full row by slot. `*`/`table.*` expand to all table columns; expressions, aliases, and `derive_alias` work as in the `SELECT` list; aggregate calls are rejected (`DatatypeMismatch`). The result is `Some(BoundReturning { exprs, output_schema })` (the `RowDescription`), or `None` with no clause. `RETURNING` expressions may carry `$n` parameters — `collect_param_types`/`substitute_params` traverse them.
- Bind `COPY` (`bind_copy`): resolve the table to `TableId`, retain the bound `TableSchema`, and resolve the column list to `ColumnId`s (reusing the INSERT column resolver — empty list defaults to all columns in catalog order, duplicates are `DatatypeMismatch`, unknown columns `UndefinedColumn`), carrying `direction`/`options` through. Unlike INSERT it does not reject an omitted NOT NULL column up front; that surfaces per row at insert time (matching PostgreSQL). For `COPY FROM` the binder also attaches the omitted columns' bound expression `DEFAULT`s (`bind_omitted_expr_defaults`) and the table's bound `CHECK` constraints (`bind_table_checks`) to `BoundStatement::Copy`, so the executor applies defaults and enforces checks per row exactly as INSERT does; `COPY TO` binds neither. COPY is not lowered to a `LogicalPlan` — `logical_plan` rejects `BoundStatement::Copy` (internal error); the server drives COPY directly (`docs/specs/copy.md`).
- Cursor control statements (`DECLARE`/`FETCH`/`CLOSE`) do not bind as normal
  statements. The server binds the inner `DECLARE ... FOR SELECT` query when it
  starts the SQL cursor worker; `FETCH`/`CLOSE` resolve names against the
  connection's cursor registry.
- Bind every `DROP TABLE` target to `TableId` when `IF EXISTS` is absent; if any
  name belongs to a view, return `SqlState::WrongObjectType`. Bind `DROP TABLE
  IF EXISTS` as a pass-through carrying ordered normalized names; the executor
  resolves every target at statement execution time so a prepared conditional
  drop does not bake in stale existence, skips missing targets, and returns
  `SqlState::WrongObjectType` if any name belongs to a view. Execution resolves
  the complete target list before changing catalog or storage.
- Bind `CREATE VIEW` by binding its query, validating any explicit view column
  list against the query output width, rejecting query parameters, and deriving
  durable `ViewDependency` metadata. Specific column references become named
  column dependencies; `SELECT *` / `table.*` become `all_columns` dependencies,
  including when the wildcard occurs inside a derived table or CTE used by the
  view; relation references with no columns (for example `count(*)`) become
  relation-existence dependencies. Dependencies also include bound CTE
  definitions even when they are not referenced by the final query body, because
  the stored SQL is rebound on later view use and those CTEs must remain
  bindable. `CREATE VIEW` rejects `nextval`/`currval`/`setval` sequence
  functions until durable sequence dependencies are represented. Bind
  `DROP VIEW` as a pass-through carrying the normalized view name and `IF
  EXISTS`.
- Resolve user views in `FROM` by parsing their stored query definition and
  inlining it as a `BoundFrom::View`, which lowers like a derived table but
  retains the view id/schema version for dependency tracking and prepared-plan
  invalidation. DML target resolution rejects view names with
  `FeatureNotSupported`.
- Bind `CREATE SEQUENCE` as a pass-through carrying the normalized
  `SequenceOptions`. Bind `DROP SEQUENCE` as a pass-through carrying the
  normalized sequence name and `IF EXISTS`; the executor resolves the sequence
  at statement execution time so a prepared `DROP SEQUENCE IF EXISTS` does not
  remain a no-op if the sequence is created after `Parse` and before `Execute`.
- Validate aggregate usage and `GROUP BY` rules.
- Validate `CASE` result typing: all non-`NULL` `THEN` and `ELSE` expressions must have the same `DataType`; `NULL` branches are allowed and make the output nullable; all-`NULL` result branches are rejected with `SqlState::DatatypeMismatch`.
- Reject unsupported forms. Concretely, the binder rejects duplicate primary-key columns (`SqlState::SyntaxError`) in `CREATE TABLE` (empty primary keys and composite multi-column primary keys are accepted), and duplicate `UPDATE` assignments or duplicate `INSERT` target columns (`SqlState::DatatypeMismatch`).

```rust
pub struct DropTableTarget {
    pub name: String,
    pub table: Option<TableId>,
}

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
    AlterTableAddColumn { table: TableId, table_name: String, if_not_exists: bool, column: ParsedColumnDef },
    AlterTableDropColumn { table: TableId, table_name: String, if_exists: bool, column: String },
    AlterTableRenameColumn { table: TableId, table_name: String, old_name: String, new_name: String },
    AlterTableRenameTable { table: TableId, table_name: String, new_name: String },
    AlterTableAlterColumnType { table: TableId, table_name: String, column: String, data_type: DataType, pg_type: PgType },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },
    CreateView { name: String, or_replace: bool, columns: Vec<String>, query: BoundQuery, definition: String, dependencies: Vec<ViewDependency> },
    DropView { name: String, if_exists: bool },
    Insert { table: TableId, columns: Vec<ColumnId>, source: BoundInsertSource, on_conflict: Option<BoundOnConflict>, returning: Option<BoundReturning> },
    Query(BoundQuery),
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: BoundSelect, returning: Option<BoundReturning> },
    Delete { table: TableId, source: BoundSelect, returning: Option<BoundReturning> },
    Explain { analyze: bool, statement: Box<BoundStatement> },
    // COPY <table> [(cols)] FROM STDIN | TO STDOUT. Resolved table + column ids
    // (COPY order; defaulted to all columns in catalog order). Not lowered to a
    // LogicalPlan — the server drives COPY directly (docs/specs/copy.md).
    Copy { table: TableId, columns: Vec<ColumnId>, direction: CopyDirection, options: CopyOptions },
}

pub enum BoundInsertSource {
    Values { rows: Vec<Vec<BoundExpr>>, output_schema: Vec<ColumnInfo> },
    Query(Box<BoundQuery>),
}

// A bound RETURNING clause: the projection expressions evaluated over each
// affected full row, and the result-set column metadata (the RowDescription).
pub struct BoundReturning { exprs: Vec<BoundExpr>, output_schema: Vec<ColumnInfo> }

// A bound ON CONFLICT action (arbiter = primary key). DoUpdate's assignment value
// expressions and the optional filter are bound over `target ++ excluded` — the
// existing row in slots 0..n and the proposed row in slots n..2n.
pub enum BoundOnConflict {
    DoNothing { target: Option<Vec<ColumnId>> },
    DoUpdate { target: Vec<ColumnId>, assignments: Vec<(ColumnId, BoundExpr)>, filter: Option<BoundExpr> },
}

// A bound query expression: a bound body plus the query-level ORDER BY/LIMIT/
// OFFSET that apply to its whole result. Mirrors parser::Query; the modifiers live
// here (not on BoundSelect) so a future set-operation body orders and limits the
// combined result. `output_schema()` delegates to the body. Carried by the
// top-level statement, derived tables, INSERT ... SELECT, and subquery exprs.
pub struct BoundQuery {
    pub body: BoundQueryBody,
    pub order_by: Vec<BoundOrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// The bound body of a query expression. `Select` is boxed (it is far larger than
// the other variants).
pub enum BoundQueryBody {
    Select(Box<BoundSelect>),
    Values(BoundValues),
    SetOp(BoundSetOp),
}

// A bound set operation. Both arms are bound in their own scopes and reconciled:
// same column count and identical column types (strict, no implicit casts).
// output_schema is the reconciled result (left arm's names, shared types). `all`
// keeps duplicates (UNION ALL); otherwise the result is de-duplicated.
pub struct BoundSetOp {
    pub op: SetOp,            // Union | Intersect | Except (re-exported from parser)
    pub all: bool,
    pub left: Box<BoundQuery>,
    pub right: Box<BoundQuery>,
    pub output_schema: Vec<ColumnInfo>,
}

// A bound VALUES body: a literal row set. Every row has the same width as
// output_schema; each column's type is the common type of its entries (a bare NULL
// takes the column type; an all-NULL column is rejected). Columns are named
// column1, column2, ... Lowers directly to the existing Values plan node.
pub struct BoundValues {
    pub rows: Vec<Vec<BoundExpr>>,
    pub output_schema: Vec<ColumnInfo>,
}

// A query's result column, described independently of the body that produced it.
// output_schema() gives name + type (the wire RowDescription); output_columns()
// adds nullability. Used by derived-table and INSERT-source binding (and, later,
// set-operation reconciliation) without matching on the body variant.
pub struct OutputColumn { pub name: String, pub data_type: DataType, pub nullable: bool }
// impl BoundQuery { fn output_schema(&self) -> &[ColumnInfo]; fn output_columns(&self) -> Vec<OutputColumn>; }

pub struct BoundSelect {
    pub distinct: Option<BoundDistinct>,  // All | On(keys)
    pub columns: Vec<BoundSelectItem>,
    pub from: Option<BoundFrom>,          // None for a FROM-less SELECT (`SELECT 1`)
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub having: Option<BoundExpr>,
    pub output_schema: Vec<ColumnInfo>,   // this block's result-set columns
}

pub enum BoundDistinct {
    All,                  // SELECT DISTINCT
    On(Vec<BoundExpr>),   // SELECT DISTINCT ON (exprs)
}

pub struct BoundSelectItem {
    pub expr: BoundExpr,
    pub alias: String,
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
    Derived {                     // (SELECT ...) AS alias [(cols)]
        query: Box<BoundQuery>,
        binding: BindingId,
        alias: String,
        schema: Vec<ColumnDef>,   // derived columns projected into the outer scope
    },
    View {                        // user view expanded from catalog definition
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

For `CREATE TABLE`, binder defaults an absent `compression` option to
`CompressionSetting::None`. It merges `Statement::CreateTable.toast` into
`ToastOptions::default_new_table()`: omitted TOAST options keep the default,
`toast = aggressive` with no explicit `toast_min_value_size` stores
`ToastOptions::AGGRESSIVE_TOAST_MIN_VALUE_SIZE`, and any explicit
`toast_compression` clears `active_dict_id` in the resolved `ToastOptions`
(new tables have no active dictionary yet). Storage-parameter `ALTER TABLE`
maintenance statements do not bind. Schema-evolution `ALTER TABLE` and
`CREATE`/`DROP VIEW` statements have `BoundStatement` → `LogicalPlan` →
`PhysicalPlan` variants so the parser/planner surface is explicit.
Schema-evolution ALTER and view DDL statements execute through the
executor/server DDL path.
`CREATE VIEW` binding rejects query parameters and sequence functions
(`SqlState::FeatureNotSupported`) so stored view SQL is never coupled to
execute-time parameter substitution or untracked sequence dependencies. When a
view supplies an explicit column list, its length must exactly match the bound
query output width and names must be unique (`SqlState::SyntaxError`).

A `BoundFrom::Derived` exposes its output columns under `alias`, renamed left to right by the optional column-alias list (more aliases than columns is `SqlState::SyntaxError`). A non-`LATERAL` derived table binds its inner query in a fresh (sibling-free) scope and lowers to its inner query's plan (no dedicated plan node); a `LATERAL` one binds through the correlated-child-query path — sibling references become correlation entries on the bound query — and lowers to an `Apply` plan node (`docs/specs/subqueries.md` §7). The derived columns occupy a contiguous slot range at the derived binding, just like a base table; an outer `WHERE` over a standalone derived table becomes a `Filter` above it. Derived-column references have no underlying table (their `ColumnInfo.table_id` is `None`).

A `BoundFrom::View` represents a user view from the catalog. The binder parses
the stored query text in the view's own scope (caller CTEs do not affect stored
name resolution) and inlines the bound query like a derived table, while
retaining the view relation id and schema version so prepared statements can be
invalidated when the view changes or is dropped.

A `BoundFrom::System` represents a virtual system view from `pg_catalog` or
`information_schema`. Bare names resolve in this order: CTE, user table, then
user view, then `pg_catalog` system view, so a user relation named `pg_class`
shadows the bare system view. Qualified `public.<name>` resolves user tables or
user views; qualified system schemas resolve only the registry view named in that
schema; unknown schemas fail with `SqlState::InvalidSchemaName`. System-view
output columns have no underlying table id. DML and COPY targets cannot modify
system catalog names or user views and are rejected before planning.

A top-level `SELECT` binds to `BoundStatement::Query(BoundQuery)`. Binding a `BoundQuery` binds its body (a `BoundSelect`) and, for a `SELECT` body, binds the query-level `ORDER BY` against that block's output columns (the `ORDER BY`/`DISTINCT` validation is coupled and stays together). Logical planning lowers the body, then applies the wrapper's `ORDER BY`/`LIMIT`/`OFFSET`; the aggregate-context `ORDER BY` rewrite stays with the body because it depends on the body's `group_by`/aggregates. `BoundSelect` (without the query-level modifiers) is also used directly as the source for `UPDATE` and `DELETE`, preserving filters and row identity through execution.

A FROM-less `SELECT` (`SELECT 1`, `SELECT count(*)`) binds with `from = None`: no bindings are registered, so any column reference fails with `SqlState::UndefinedColumn`. Logical planning lowers a `None` source to a single unit row — a one-row, zero-column `LogicalPlan::Values` node (already supported by the physical planner and executor) — with the `WHERE` clause, if any, applied as a `Filter` above it; the projection and any aggregation stack on top unchanged. `UPDATE`/`DELETE` always bind a real table, so their `from` is always `Some`.

A `VALUES` body binds each row's expressions (a leaf, with no bindings — column references inside cannot resolve) and infers each column's type from its first non-`NULL` entry under the strict no-implicit-cast rule: every other entry must match exactly (a bare `NULL` adopts the type), rows must all be the same width, and an all-`NULL` column has no inferable type — all violations are `DatatypeMismatch`/`SyntaxError`. It lowers directly to the existing `LogicalPlan::Values` node, with the query-level `ORDER BY` (resolved against the output columns by position or name, like a set operation) and `LIMIT`/`OFFSET` stacked on top. Because a derived table and an `INSERT ... <query>` source read the query's columns through `output_columns()`, `FROM (VALUES ...)`, `x IN (VALUES ...)`, and a scalar `(VALUES ...)` all work with no further code.

**CTEs** (`WITH name [(cols)] AS (query), ...`) are bound once and **inlined as named derived tables** — a CTE reference in `FROM` reuses the existing derived-table machinery, so there is no dedicated plan node or executor operator. Binding threads a CTE scope (`CteScope` on `BindContext`) through query binding: each CTE is bound in the scope of the enclosing CTEs and its earlier siblings only (so it is non-recursive — a self-reference finds no such table and fails with `UndefinedTable`; a later sibling is not yet visible), and its output columns are renamed by the optional column-alias list. The scope reaches subqueries and derived tables (a CTE is visible inside nested queries), and a CTE name **shadows** a catalog table of the same name (matching PostgreSQL). A duplicate CTE name within one `WITH` is a `SyntaxError`; `WITH RECURSIVE` is rejected at parse time. A `FROM cte` reference registers a derived binding over the CTE's columns and inlines a clone of the CTE's bound query (each reference gets its own copy). A subquery inside a `VALUES` row also sees the enclosing CTEs (VALUES has no FROM, but a row expression may hold a subquery). Known limitation: because an *unreferenced* CTE is bound (so its errors surface) but then dropped rather than inlined, an extended-protocol parameter (`$n`) used *only* inside an unreferenced CTE is not reported in the parameter description — parameters are collected from the final inlined plan.

A **set operation** (`UNION`/`INTERSECT`/`EXCEPT`) binds both arms in their own scopes and reconciles them: the arms must have the same number of columns and — strictly, no implicit casts — identical column types (`SyntaxError` / `DatatypeMismatch` otherwise). A bare `NULL` output column has no type of its own and adopts the sibling arm's type: one arm is bound to discover its column types, then the other is bound with those types as the `expected` type for its `NULL` columns (threaded through `bind_query`/`bind_select`/`bind_values`); if the first arm cannot bind on its own (a `NULL` column needing the other arm's type), the arms are bound in the other order. Resolution therefore requires at least one arm that types *all* its columns without help, which then types the other arm's `NULL`s. If neither arm is self-typing — a column `NULL` in both arms, or bare `NULL`s split across the two arms so each arm needs the other (`SELECT NULL, 1 UNION SELECT 2, NULL`) — the query is rejected and an explicit cast is required. The result columns take the left arm's names and the shared types, and are nullable if either arm's are. The query-level `ORDER BY` (which now applies to VALUES and set-op bodies too) resolves against the combined output by 1-based position or output-column name only — a set operation has no single input scope — and each item becomes a `LocalRef` to that output slot, evaluated by a `Sort` stacked above the set-operation node; `LIMIT`/`OFFSET` stack above that. Lowering produces `LogicalPlan::SetOp { op, all, left, right }` → `PhysicalPlan::SetOp`, executed by `SetOpOp` with bounded external key and ordinal sorts. The `ALL` forms use multiset semantics: `UNION ALL` concatenates, `INTERSECT ALL` emits `min(count_left, count_right)` copies in left order, and `EXCEPT ALL` emits `max(0, count_left − count_right)` copies. The distinct forms retain the established first left-to-right occurrence selected by union membership, intersection, or difference. Row equality is structural over the whole row with `NULL == NULL`; output rows carry no heap identity.

`CREATE INDEX` binds as a pass-through (name, table, columns, unique), like `CREATE TABLE`: the catalog validates that the table and columns exist and the index name is unused at execute time. `DROP INDEX` resolves the index name to its `IndexId` at bind time, rejecting an unknown index with `UndefinedTable` (mirroring `DROP TABLE`).

`BoundFrom::Join.condition` is `None` only for `JoinType::Cross`; all other join types have a boolean `Some(condition)`. The binder rejects missing `ON` predicates for non-cross joins; the parser rejects `ON`/`USING`/`NATURAL` on a `CROSS JOIN` at parse time (`SqlState::SyntaxError`). The executor treats a cross join's `None` condition as `TRUE`. A comma-separated `FROM a, b, ...` list desugars into a left-deep chain of `JoinType::Cross` joins, each with `condition: None`. For output expression nullability, the binder marks every binding on the null-supplying side of an outer join nullable (`RIGHT` side of `LEFT JOIN`, `LEFT` side of `RIGHT JOIN`, both sides of `FULL JOIN`) before binding `WHERE`, projection, `GROUP BY`, `HAVING`, `ORDER BY`, and `DISTINCT`; this nullability flows into `output_columns()`, derived tables, `INSERT ... SELECT` assignment checks, and view output metadata.

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
        pg_type: Option<PgType>, // Parse-declared wire identity for output metadata
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
        pg_type: Option<PgType>, // pg_proc-derived wire identity when unambiguous
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
    // Subquery expressions. Each carries its inner query as a `Box<BoundQuery>`
    // bound in its own (uncorrelated) scope, preserved unchanged through logical
    // and physical planning and evaluated by the executor.
    ScalarSubquery {              // (SELECT ...) used as a single value
        query: Box<BoundQuery>,
        data_type: DataType,      // the subquery's single output column type
        nullable: bool,           // always true (an empty result is NULL)
    },
    Exists {                      // [NOT] EXISTS (SELECT ...)
        query: Box<BoundQuery>,
        negated: bool,
        data_type: DataType,      // Boolean
        nullable: bool,           // false (EXISTS never yields NULL)
    },
    InSubquery {                  // expr [NOT] IN (SELECT ...)
        expr: Box<BoundExpr>,     // left operand (outer scope)
        query: Box<BoundQuery>,   // single-column subquery
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
- `ScalarSubquery`, `Exists`, `InSubquery`: the binder binds the inner SELECT in a fresh, uncorrelated scope (it does not see the outer query's columns). A scalar subquery and the right side of `IN` must produce exactly one output column (else `SqlState::SyntaxError`); a scalar subquery's type is that column's type and it is always nullable. `EXISTS` is a non-null boolean. For `IN`/`NOT IN`, the left operand is type-checked against the subquery's column type (no implicit casts; mismatch is `SqlState::DatatypeMismatch`). These variants are constants with respect to the outer query, so the outer aggregate/grouping analyses treat them as leaves (only `InSubquery`'s left operand participates in the outer scope). Logical/physical planning preserve the inner `BoundQuery` unchanged; the executor plans and runs it.

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
    StddevSamp, // STDDEV / STDDEV_SAMP (divisor n - 1)
    StddevPop,  // STDDEV_POP (divisor n)
    VarSamp,    // VARIANCE / VAR_SAMP (divisor n - 1)
    VarPop,     // VAR_POP (divisor n)
    BoolAnd,    // BOOL_AND — true when every non-NULL input is true
    BoolOr,     // BOOL_OR — true when any non-NULL input is true
    ArrayAgg,   // ARRAY_AGG(value) — array of input values, including NULLs
    StringAgg,  // STRING_AGG(text, delimiter) — concatenated non-NULL values
}

pub struct BoundOrderByItem {
    pub expr: BoundExpr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}
```

Aggregate calls use a two-stage representation. Binder converts `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, the statistical aggregates `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP` and `VARIANCE`/`VAR_SAMP`/`VAR_POP`, and the boolean aggregates `BOOL_AND`/`BOOL_OR` into `BoundExpr::AggregateCall`; scalar functions remain `BoundExpr::Function`. Logical planning extracts unique aggregate calls from SELECT, HAVING, and ORDER BY expressions into `AggregateExpr` values, then rewrites aggregate and grouped-expression references above the `Aggregate` node to `BoundExpr::LocalRef`. The `Aggregate` output row layout is group-by values first, then aggregate values. `BoundExpr::AggregateCall` is illegal in physical plans handed to executor scalar evaluation.

Aggregate `DISTINCT` (e.g. `COUNT(DISTINCT x)`) is supported: the binder carries the flag into `AggregateExpr.distinct`, and the executor de-duplicates the argument values before aggregating. `DISTINCT` combined with a wildcard argument (`COUNT(DISTINCT *)`) is rejected with `ErrorKind::Plan` / `SqlState::SyntaxError`. Aggregate return types are fixed: `COUNT` returns non-null `INTEGER`; `SUM` and `AVG` accept either numeric type and return that same type (`AVG(integer)` uses integer division truncated toward zero; `AVG(double precision)` is true floating-point division), rejecting non-numeric arguments with `SqlState::DatatypeMismatch`; `MIN` and `MAX` return the argument type and are nullable. `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP` and `VARIANCE`/`VAR_SAMP`/`VAR_POP` accept a numeric argument and return nullable `DOUBLE PRECISION`; `BOOL_AND`/`BOOL_OR` require a boolean argument and return nullable `BOOLEAN`. Empty aggregate inputs return `0` for `COUNT` and `NULL` for the rest (the sample variance/stddev forms also return `NULL` for a single value).

Scalar functions remain `BoundExpr::Function`. The set of scalar functions and their signatures live in the scalar function registry in `common` (`docs/specs/crates/common.md`); the binder resolves each ordinary call through `common::lookup_scalar_function` and runs the entry's signature check, so an unregistered scalar-function name is rejected there (`function <name> is not supported in v1`). For untyped `NULL` literals and placeholders, the binder asks the registry for an argument hint; hints are derived from the registered signature and are returned only when the argument has one unambiguous type for that arity. For high-arity calls the registry avoids exhaustive search and only returns hints for uniform-argument signatures such as variadic `CONCAT(text, ...)`. When `pg_proc` metadata gives one unambiguous result wire type for the function name/arity/result `DataType`, the binder stores that `PgType` on `BoundExpr::Function` so output schemas describe OID-returning helpers as PostgreSQL `oid` rather than collapsed `int8`. Binder validates each call's arity and argument types and assigns its result type: `UPPER(text)`, `LOWER(text)`, `TRIM(text)` return `TEXT`; `LENGTH(text)` returns `INTEGER`; `SUBSTRING(text, integer[, integer])` returns `TEXT`. The math functions accept either numeric type (`INTEGER` or `DOUBLE PRECISION`): `ABS`, `FLOOR`, `CEIL`/`CEILING`, and `ROUND` return their argument's type (`FLOOR`/`CEIL`/`ROUND` of an `INTEGER` is the integer itself; of a `DOUBLE` they round and stay `DOUBLE`); `SQRT` and `POWER`/`POW` always return `DOUBLE` (an `INTEGER` argument is widened, matching PostgreSQL's `sqrt(int)`); `MOD(integer, integer)` returns `INTEGER` (integer-only, like the `%` operator). The string functions `REPLACE(text, text, text)`, `LEFT(text, integer)`, and `RIGHT(text, integer)` return `TEXT`; `POSITION(text, text)` returns `INTEGER`. `CURRENT_TIMESTAMP` and `now()` are zero-argument, non-nullable statement clock functions returning `TIMESTAMP WITH TIME ZONE`. PostgreSQL-compatible system information functions include zero-argument, non-nullable functions: `VERSION()`, `CURRENT_DATABASE()`, `CURRENT_CATALOG`, `CURRENT_SCHEMA`, `CURRENT_USER`, `SESSION_USER`, and `USER` return `TEXT`; `PG_BACKEND_PID()` returns `INTEGER` (SaguaroDB's single integer storage type, exposed like other integer results). `CURRENT_SETTING(text)` returns `TEXT`, pushes a `TEXT` type expectation into its single argument (so `current_setting($1)` infers `$1` as text), and is nullable only when the argument is nullable. PostgreSQL catalog introspection compatibility functions are registry entries too: `FORMAT_TYPE(oid, typmod)` returns nullable `TEXT`; `PG_GET_INDEXDEF(oid[, column_no, pretty])`, `PG_GET_EXPR(text, oid[, pretty])`, `PG_GET_CONSTRAINTDEF(oid[, pretty])`, `PG_GET_USERBYID(oid)`, `PG_GET_SERIAL_SEQUENCE(text, text)`, `TO_REGCLASS(text)`, `TO_REGTYPE(text)`, and description/definition stubs return nullable `TEXT` or nullable integer OIDs as appropriate; visibility, temp-schema, relation-size, and privilege probes return `BOOLEAN`/`INTEGER`. Privilege probes accept PostgreSQL-compatible arity families: table/schema/database/sequence/function/any-column/role probes accept 2 or 3 text/OID-shaped arguments, while column probes accept 3 or 4. A bare `CURRENT_SCHEMA` first resolves like an ordinary unqualified column name, so a real column named `current_schema` wins; only unresolved references bind as the system information function. All ordinary scalar functions above are NULL-propagating, so the result is nullable when any argument is; metadata lookup functions whose object can be missing are always nullable; `CONCAT`, the statement clock functions, and the zero-argument system information functions are always non-nullable. `CONCAT(text, ...)` is variadic (one or more `TEXT` arguments), ignores NULL arguments, and always returns a non-nullable `TEXT` (the empty string when every argument is NULL); non-text arguments must be cast explicitly. `EXTRACT(field FROM source)` binds as `extract('field', source)`: the field literal must name a supported field (`year`/`month`/`day`/`hour`/`minute`/`second`), the source must be `DATE` or `TIMESTAMP`, and the result is `DOUBLE PRECISION` (nullable when the source is). Unknown function names, wrong arity, and argument-type mismatches are rejected with `ErrorKind::Plan` (`SyntaxError` for unknown names and arity, `DatatypeMismatch` for argument types). Aggregates may appear as scalar-function arguments (e.g. `ABS(SUM(id))`); logical planning rewrites the nested aggregate as usual.

`COALESCE` and `NULLIF` are not NULL-propagating, so the binder desugars them to `BoundExpr::Case` rather than leaving them as `Function`s. `COALESCE(v1, ..., vn)` becomes `CASE WHEN v1 IS NOT NULL THEN v1 ... ELSE vn END`; all arguments must share one type (no implicit cast, with a bare untyped NULL taking its type from a sibling — all-NULL is `DatatypeMismatch`), and the result is non-nullable exactly when at least one argument is. `NULLIF(a, b)` becomes `CASE WHEN a = b THEN NULL ELSE a END`; the operands must be comparable (same type) and the result type is `a`'s type, always nullable. `BinOp::IsDistinctFrom` / `IsNotDistinctFrom` bind like a comparison (same-type operands, with one untyped NULL taking the sibling's type) but always yield a non-nullable `Boolean`: two NULLs are not distinct, a NULL and a non-NULL are distinct, otherwise ordinary equality applies.

`ARRAY_AGG(value)` accepts one non-array expression and returns its scalar array
type; it includes NULL inputs and is NULL for an empty group. `STRING_AGG(value,
delimiter)` requires two text expressions, skips NULL values, treats a NULL
delimiter as empty, and is NULL when no non-NULL value exists. Both participate
in the existing aggregate `DISTINCT` machinery.

Array expressions use dedicated bound nodes. `BoundExpr::Array` carries a flat
row-major element expression list plus rectangular dimensions and one scalar
element type; repeated SQL `[]` dimensions never create nested `DataType::Array`
elements. An empty or all-NULL constructor needs an array type context (for
example `ARRAY[]::integer[]`). `BoundExpr::ArraySubscript` requires integer
indexes and returns the scalar element type, nullable for NULL or out-of-range
indexes. `BoundExpr::Any` requires a comparison operator and an array whose
element type exactly matches the left operand; this context infers an undeclared
array parameter in the common `column = ANY($1)` form. Planner expression walks
recurse through every child of these nodes.

## Logical Plan

`UNNEST(array)` and integer `GENERATE_SERIES(start, stop [, step])` in `FROM`
bind as one-column table functions. Aliases and one optional column alias define
their visible binding. `UNNEST` returns the scalar array element type and is
nullable; `GENERATE_SERIES` returns non-null `INTEGER`. Table-function arguments
are implicitly lateral: when placed to the right of another FROM item, logical
planning lowers them to `Apply` correlations evaluated once per left row.
`WITH ORDINALITY` and other table functions remain unsupported.
Subqueries in table-function arguments are rejected until those argument plans
participate in subquery hoisting. RIGHT/FULL joins with table functions are
rejected because dependent outer-right/full Apply semantics are not implemented.
As with lateral derived tables, a function inside an explicit join may reference
only that join's left subtree; crossing an earlier comma-join boundary is
`FeatureNotSupported` rather than an internal planning error.

```rust
pub enum LogicalPlan {
    CreateTable { name: String, if_not_exists: bool, columns: Vec<ParsedColumnDef>, primary_key: Vec<String>, unique: Vec<Vec<String>>, compression: CompressionSetting, toast: ToastOptions, checks: Vec<String> },
    DropTable { targets: Vec<DropTableTarget>, if_exists: bool },
    AlterTableAddColumn { table: TableId, table_name: String, if_not_exists: bool, column: ParsedColumnDef },
    AlterTableDropColumn { table: TableId, table_name: String, if_exists: bool, column: String },
    AlterTableRenameColumn { table: TableId, table_name: String, old_name: String, new_name: String },
    AlterTableRenameTable { table: TableId, table_name: String, new_name: String },
    AlterTableAlterColumnType { table: TableId, table_name: String, column: String, data_type: DataType, pg_type: PgType },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { name: String, options: SequenceOptions },
    DropSequence { name: String, if_exists: bool },
    CreateView { name: String, or_replace: bool, columns: Vec<String>, query: BoundQuery, definition: String, dependencies: Vec<ViewDependency> },
    DropView { name: String, if_exists: bool },
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<LogicalPlan>, on_conflict: Option<BoundOnConflict>, returning: Option<BoundReturning> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<LogicalPlan>, returning: Option<BoundReturning> },
    Delete { table: TableId, source: Box<LogicalPlan>, returning: Option<BoundReturning> },
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
```

Logical plan contains no access method choices. `SystemScan` is the logical
source for a bound virtual system view; its optional `filter` is the bound `WHERE`
predicate pushed to the source the same way it is for a base-table `Scan`.

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

The following is a structural synopsis of the relational variants, not a
field-for-field duplicate of the public enum. The exact field contract lives in
`crates/planner/src/physical.rs`; DDL/DML fields additionally follow the bound
contracts above.

```rust
pub enum PhysicalPlan {
    CreateSchema { name: String, if_not_exists: bool },
    DropSchema { name: String, if_exists: bool },
    CreateTable { schema: SchemaId, name: String, if_not_exists: bool, columns: Vec<ParsedColumnDef>, primary_key: Vec<String>, unique: Vec<Vec<String>>, compression: CompressionSetting, toast: ToastOptions, checks: Vec<String> },
    DropTable { targets: Vec<DropTableTarget>, if_exists: bool },
    AlterTableAddColumn { table: TableId, table_name: String, if_not_exists: bool, column: ParsedColumnDef },
    AlterTableDropColumn { table: TableId, table_name: String, if_exists: bool, column: String },
    AlterTableRenameColumn { table: TableId, table_name: String, old_name: String, new_name: String },
    AlterTableRenameTable { table: TableId, table_name: String, new_name: String },
    AlterTableAlterColumnType { table: TableId, table_name: String, column: String, data_type: DataType, pg_type: PgType },
    CreateIndex { schema: SchemaId, name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { index: IndexId },
    CreateSequence { schema: SchemaId, name: String, options: SequenceOptions },
    DropSequence { name: String, search_path: Vec<SchemaId>, sequence: Option<SequenceId>, if_exists: bool },
    CreateView { name: String, or_replace: bool, columns: Vec<String>, query: BoundQuery, definition: String, dependencies: Vec<ViewDependency> },
    DropView { name: String, if_exists: bool },
    Insert { table: TableId, columns: Vec<ColumnId>, source: Box<PhysicalPlan>, on_conflict: Option<BoundOnConflict>, returning: Option<BoundReturning> },
    Update { table: TableId, assignments: Vec<(ColumnId, BoundExpr)>, source: Box<PhysicalPlan>, returning: Option<BoundReturning> },
    Delete { table: TableId, source: Box<PhysicalPlan>, returning: Option<BoundReturning> },
    SeqScan { table: TableId, table_name: String, filter: Option<BoundExpr> },
    SystemScan { view: SystemView, output_schema: Vec<ColumnInfo>, filter: Option<BoundExpr> },
    IndexScan { table: TableId, table_name: String, index: IndexId, range: KeyRange, full_filter: Option<BoundExpr>, filter: Option<BoundExpr> },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Option<BoundExpr>,
        join_type: JoinType,
        identity_from: Option<JoinSide>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: JoinType,
        identity_from: Option<JoinSide>,
        build_left: bool,
    },
    MergeJoin {
        left: Box<PhysicalPlan>, right: Box<PhysicalPlan>,
        left_keys: Vec<usize>, right_keys: Vec<usize>,
        residual: Option<BoundExpr>, join_type: JoinType,
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
    SetOp { op: SetOp, all: bool, left: Box<PhysicalPlan>, right: Box<PhysicalPlan> },
}
```

## Physical Rules

- A scan with an equality or range predicate on the leading column of the table schema's declared primary key becomes an `IndexScan` over `PRIMARY_KEY_INDEX_ID`, with `range` an exact or bounded `KeyRange` over that column. A scan with an equality or range predicate on the leading column of a catalog index becomes an `IndexScan` over that catalog index id.
- When more than one indexed leading column is constrained, the planner picks the best: an equality match beats a range, primary-key identity access beats catalog indexes, and a lower index id breaks remaining ties.
- `filter` stores residual predicates not consumed by the chosen index's range, re-checked by the scan operator (so the choice of index never changes results). For `WHERE id = 7 AND name = 'Ada'`, a declared primary key on `id` wins with exact key `7` and the residual filter is `name = 'Ada'`. For `WHERE id = 7`, `filter` is `None`. `full_filter` stores the original scan predicate for executor fallback when a catalog index chosen from the current catalog is unavailable for an older retained relation generation; normal index scans use only `filter`.
- A lower-bound and an upper-bound comparison on the *same* index column fuse into one two-sided `KeyRange::Range`, consuming both conjuncts. For `WHERE id > 5 AND id < 10`, the range is `(5, 10)` (both bounds excluded) and the residual filter is `None`. This remains a single-column range; multi-column composite-index ranges are not produced.
- Otherwise scans are `SeqScan`.
- A `LogicalPlan::SystemScan` maps directly to `PhysicalPlan::SystemScan`; it is
  not considered for storage index selection and carries the system view's full
  output schema for later execution and EXPLAIN/debug output.
- Any scalar literal comparand qualifies for an `IndexScan` (`literal_key` accepts every scalar `Value` kind); a `NULL` or array comparand and a non-constant expression fall back to `SeqScan`. Extended-protocol parameters are substituted into the bound statement before per-execution planning, so the planner never sees a parameter comparand. Constant expressions are folded by the simplification pass before physical planning.
- With statistics on the scanned table, `plan_scan` compares `seq_scan_cost(pages, rows)` against `index_scan_cost(matches, rows)` (`planner::estimate`, constants per `docs/specs/statistics.md` §9.2) and keeps the eligible `IndexScan` only when it is not costlier; without statistics the always-index rule is unchanged.
- The planner emits only `Exact` or bounded `Range` key ranges. The EXPLAIN formatter can additionally render a full-index `KeyRange::All` as `all`, but the planner never produces one.
- `table_name` is captured at planning time solely for EXPLAIN/debug output; execution still uses `table`.
- Joins are left-to-right nested loop joins. The planner supports `Inner`, `Cross`, `Left`, `Right`, and `Full` join types. Logical and physical join `condition` is `None` only for `Cross` and `Some(boolean_expr)` for every other join type.
- An `Inner` join whose `condition` contains at least one `left_column = right_column` equality conjunct becomes a `HashJoin` on those equality pairs. The node carries `build_left: bool` (`docs/specs/statistics.md` §9.2): set only for a plain inner join outside a DML spine when both inputs are fully analyzed (`plan_fully_analyzed`) and the left side's row estimate is smaller, so the executor builds its hash table over the smaller input; semi/anti joins and any un-analyzed input keep the historical build-right shape. `left_keys` and `right_keys` are the paired key column slots, relative to each child row (right slots are rebased by the left child width; join inputs are left-deep, so a child row's column positions match its global slots). Any remaining (non-equi or expression) conjuncts are re-checked in a `Filter` above the `HashJoin`, using their global joined-row slots. An inner join with no column-equality conjunct stays a `NestedLoopJoin`.
- A `Left`, `Right`, or `Full` join with no DML identity source and at least one extractable cross-side column equality becomes a `MergeJoin`; remaining conjuncts are its internal `residual`, because filtering above an outer join would change NULL-extension semantics. Outer joins with no equality key, outer DML identity spines, cross joins, and non-equality joins remain `NestedLoopJoin`. Merge join performs its own sorts and does not publish an ordering property, so an SQL `ORDER BY` still plans a `Sort`.
- Sort and aggregate are blocking operators.
- The planner performs no projection pushdown: `LogicalPlan::Projection` maps straight to `PhysicalPlan::Projection`, and logical planning always wraps a top-level `Projection`.

## EXPLAIN

`EXPLAIN` ownership is split cleanly:

- Parser emits `Statement::Explain { analyze, statement }`.
- Binder preserves the flag in `BoundStatement::Explain { analyze, statement }`.
- `logical_plan` and `physical_plan` do not accept `BoundStatement::Explain` directly; callers must unwrap and plan the inner bound statement.
- The planner crate exposes `PlanNodeLayout::new(plan)`, `format_explain(plan: &PhysicalPlan, catalog: &dyn CatalogManager) -> String`, and `estimated_rows(plan: &PhysicalPlan, catalog: &dyn CatalogManager) -> u64` (the cardinality estimator, `docs/specs/statistics.md` §9.1).
- The server `QueryService` handles the outer statement after normal snapshot and object-lock setup. Plain EXPLAIN formats the inner physical plan without execution; analyzed EXPLAIN calls the executor's analysis-only driver and passes its report to `format_explain_analyze`.

`format_explain` appends ` (rows=N)` — the estimated output row count — to every data-producing node line (scans, joins, Apply, filters, projections, sorts, distinct, limits, aggregates, `Values`, set operations, and the `Insert`/`Update`/`Delete` heads); DDL nodes carry no estimate. Estimates come from `planner::estimate` reading ANALYZE statistics through the catalog: base scans use the stored `row_count` (default 1000 when never analyzed), scan-level predicates resolve `column op literal` shapes against MCVs, histograms, and null fractions, and every unresolvable shape uses fixed defaults (equality `0.005`, ranges `1/3`, other predicates `0.5`, join-key/grouping distinct counts `200`, semi/anti joins keep half the left side, system views `100` rows). Upper `Filter` nodes estimate from predicate shape alone — column statistics resolve only at scan level in v1. Estimates are advisory and never affect correctness.

`PlanNodeLayout` assigns execution-local, zero-based node IDs in deterministic pre-order: root first, a unary child next, binary left before right, and Apply input before subplan. `new_with_next(plan, &mut next)` lets a caller share one allocation counter across the main tree and init-plan trees, keeping every ID globally distinct within one explanation without exposing mutable layout state. The IDs are stable for an unchanged physical tree but are not durable or catalog identifiers. The immutable layout exposes only its node `id()` and checked `child(index)` lookup; IDs are explanation/execution metadata and are not stored in `PhysicalPlan` variants.

`format_explain` renders each physical node on its own indented line, prefixed exactly once by `[node=N]`, with a stable label vocabulary including: `SeqScan table=name(id) filter=yes|none`, `SystemScan view=schema.name filter=yes|none`, `IndexScan table=name(id) index=N range=exact(...)|range(...) filter=yes|none`, `NestedLoopJoin type=… condition=yes|none`, `HashJoin keys=N build=left|right`, `MergeJoin type=Left|Right|Full keys=N residual=yes|none`, `Filter`, `Projection exprs=N`, `Sort keys=N`, `Distinct keys=N`, `Limit count=… offset=…`, `Aggregate groups=… aggregates=…`, `Values rows=N`, `CreateTable`, `DropTable tables=… if_exists=true|false`, `Create[Unique]Index name on table`, `DropIndex index=N`, `CreateSequence name`, `DropSequence name if_exists=true|false`, and `Insert`/`Update`/`Delete table=…`.

The planner also owns the executor-to-formatter reporting DTOs `NodeExecutionMetrics`, `InitPlanAnalysis`, and `ExplainAnalysis`, plus `format_explain_analyze`. Cumulative node metrics are keyed by `PlanNodeId`; the formatter divides startup time, total time, and rows by `loops`, rendering exact average rows as an integer and fractional averages with two decimal places. Times use three decimal milliseconds. A missing/zero-loop node renders `(never executed)`. Estimated rows remain on analyzed lines, init-plan sections (when present) precede the final `Execution Time: N.NNN ms` line, and node timing is inclusive rather than additive. SQL exposes this through SELECT-only `EXPLAIN ANALYZE` and `EXPLAIN (ANALYZE [TRUE|FALSE])`; false uses the plain formatter.

## Acceptance Tests

- Binder resolves aliases and self-joins with distinct `BindingId`s.
- Binder rejects ambiguous unqualified columns.
- Binder/planner preserve ordered multi-table DROP targets; a late missing or
  wrong-kind target fails before execution can remove an earlier table.
- Binder expands wildcard projection into explicit bound expressions.
- Binder binds `INSERT ... SELECT` into `BoundInsertSource::Query`, rejecting column-count, type, and nullability mismatches against the target.
- Binder resolves `pg_catalog` and `information_schema` views as `BoundFrom::System`, while preserving CTE/user-table precedence for bare names and rejecting system-catalog write targets.
- Logical planner emits logical nodes without `SeqScan` or `IndexScan`.
- Logical and physical planning preserve system views as `SystemScan`.
- Physical planner chooses `IndexScan` for an equality or range predicate on a declared-primary-key or catalog-index leading column, preferring exact matches and primary-key identity access, preserves residual predicates in `IndexScan.filter`, and preserves the original scan predicate in `IndexScan.full_filter` for generation-snapshot fallback.
- Physical planner falls back to `SeqScan` when no index's leading column is constrained.
- Physical planner chooses `HashJoin` for eligible inner/semi/anti equi joins, `MergeJoin` for eligible outer equi joins, and `NestedLoopJoin` for cross and non-equi joins.
- `EXPLAIN` returns a readable physical plan tree.
