# Correlated Subqueries, LATERAL, and Join-Sourced DML

**Status:** Implemented on branch `subqueries` (milestones S0–S6). Correlated
subqueries execute via Apply in `WHERE`, the `SELECT` list, and `HAVING`;
equality shapes decorrelate to semi/anti joins; `LATERAL` derived tables and
`UPDATE ... FROM` / `DELETE ... USING` are supported. Remaining deferrals are
listed in §1.1 and `docs/specs/overview.md` §13.

This document specifies correlated subquery execution and the features built on
it: correlated `(SELECT ...)` / `[NOT] EXISTS` / `[NOT] IN`, semi/anti joins,
`LATERAL`, and `UPDATE ... FROM` / `DELETE ... USING`. It is the system-level
contract for the feature; it complements `docs/specs/overview.md` (SQL subset,
planner/executor architecture) and `docs/specs/mvcc.md` (snapshot visibility).
It supersedes the prior rule that every subquery is bound in its own fresh,
uncorrelated scope.

## 1. Purpose and scope

Uncorrelated subqueries are already implemented: the binder binds a subquery
body in a fresh scope, and the executor resolves each subquery expression to a
constant in a one-time pre-pass before the main plan runs
(`crates/executor/src/subquery.rs`). Correlation is impossible by construction:
an outer column reference fails name resolution, and there is no per-outer-row
execution point.

This feature adds:

- **Correlated subquery expressions** — a subquery may reference columns of any
  enclosing query, at any depth, in `WHERE`, the `SELECT` list, and `HAVING`
  (positions staged; see §12).
- **Semi/anti join execution** — decorrelatable `[NOT] EXISTS` / `IN` shapes
  run as hash or nested-loop semi/anti joins instead of per-row re-execution.
- **`LATERAL`** — a derived table in `FROM` may reference columns of FROM items
  to its left.
- **`UPDATE ... FROM` / `DELETE ... USING`** — DML whose source is a join of
  the target table with additional tables.

### 1.1 Non-goals (deferred, documented)

- Correlated subqueries in join `ON` conditions and in `ORDER BY` expressions
  (rejected with `FeatureNotSupported` until a later milestone). This includes
  a correlated `SELECT`-list subquery that `SELECT DISTINCT` or an `ORDER BY`
  output alias duplicates into the distinct keys / sort keys — the duplicate
  copy sits in an unhoisted position and trips the same rejection.
- Outer references from inside a **set-operation arm**, a **`VALUES` list**,
  or a **derived table's body** within a subquery (rejected with
  `FeatureNotSupported`; the outer scope chain is still threaded there so the
  error names the construct instead of claiming the column does not exist —
  `VALUES` entries bind in per-entry throwaway contexts, so there is no single
  accumulator for their `OuterRef` slots). `LATERAL` (S4) makes *sibling*
  references expressible via the `LATERAL` keyword; cross-level outer
  references from a non-`LATERAL` derived-table body — legal in PostgreSQL —
  remain rejected with `FeatureNotSupported` until explicitly lifted, a
  documented divergence.
- `WITH RECURSIVE` (unrelated machinery; still rejected).
- Decorrelation beyond the conjunctive-equality rules of §6 (no general
  unnesting; non-matching shapes use the Apply fallback, which is always
  correct).
- Cost-based choice between Apply and decorrelation. Decorrelation is a rule:
  when it applies, it is used.
- `MERGE`.

## 2. Semantics

The observable behavior matches PostgreSQL, except where §1.1 documents a
deferral:

- **Scalar subquery cardinality.** A correlated scalar subquery is evaluated
  per outer row; producing more than one row for any outer row is a runtime
  error, `SqlState::CardinalityViolation` (21000). An empty result is `NULL`.
- **`IN` three-valued logic.** `expr [NOT] IN (SELECT c ...)` keeps SQL
  three-valued semantics per outer row: if no inner row equals `expr` and any
  inner value (or `expr` itself) is `NULL`, the result is `NULL`, not false.
  Decorrelation to an anti join is applied only when provably `NULL`-safe (§6).
- **`EXISTS`.** Yields a non-null boolean per outer row; evaluation may stop at
  the first inner row.
- **Snapshot and isolation.** Inner executions run under the same
  `StatementContext` — same transaction id, snapshot, isolation level, and SSI
  tracker — as the outer statement. A correlated subquery never sees data the
  outer statement could not see, and its reads are SSI-tracked exactly like
  outer reads.
- **Volatility.** A subplan containing a sequence-function expression
  (`nextval`, `setval`, or `currval` — `currval` reads per-session state that
  outer-row evaluation may advance) is re-executed for every outer row, never
  memoized. Statement-stable functions (`now()`, `current_timestamp`) do not
  inhibit memoization.
- **`UPDATE ... FROM` / `DELETE ... USING` match semantics.** The source is an
  inner join: a target row with no join match is not modified. A target row
  matched by multiple source rows is modified **once**, using the first match
  in scan order; subsequent matches for the same target row are skipped.
- **Aggregate attribution.** PostgreSQL attributes an aggregate whose
  arguments reference only outer-level columns to the outer query. SaguaroDB
  does not implement outer-level aggregation and rejects the form
  (`FeatureNotSupported`) rather than silently evaluating it at the inner
  level; a mixed inner/outer argument belongs to the inner query in both
  systems and is allowed.
- **Grouped rule for correlations.** A correlation entry's outer column is
  evaluated against the enclosing query's rows, so in an aggregate outer
  query it must obey the same grouped-expression rule as any other
  expression: a correlated reference to an ungrouped outer column in
  `HAVING`/the select list is rejected.

## 3. Architecture overview

The design is **plan-level Apply (dependent join)**, not expression-level
rescan:

1. The **binder** resolves outer references and records, per subquery, an
   ordered **correlation list** of the outer columns it uses (§4).
2. A **planner pass** hoists each correlated subquery expression out of the
   expression tree into an **`Apply` plan node** above the expression's input
   plan, replacing the expression with a `LocalRef` to the column the Apply
   appends (§5).
3. The executor's **`ApplyOp`** re-executes the inner plan per outer row by
   substituting the outer row's correlation values as literals — the same
   clone-and-substitute mechanism as the existing uncorrelated pre-pass, moved
   from "once at plan root" to "per outer row inside one operator" (§5.2).
4. A **decorrelation rule** turns the hot shapes (equality-correlated
   `EXISTS`, uncorrelated `IN`) into semi/anti joins before Apply-hoisting
   runs; everything the rule does not match falls back to Apply (§6).

Expression evaluation (`eval_expr`) stays pure — it never executes plans. This
was chosen over threading a subplan runtime through `common::StatementContext`,
which cannot be done cleanly: `ExecutionContext` holds borrowed
`&'a dyn CatalogManager` / `&'a dyn StorageEngine` references while the trait
seams on `StatementContext` are `'static` `Arc`s.

**Uncorrelated subqueries are unchanged.** The existing pre-pass
(`resolve_plan_subqueries`) still resolves them to constants once per
statement (init-plan behavior).

## 4. Binder: correlation

### 4.1 Scope stack

Binding a subquery pushes a **child scope** whose parent is the enclosing
scope. Name resolution tries the innermost scope first and walks outward on
failure; ambiguity within one scope is still an error, and an inner binding
shadows an outer one. CTE bodies and view expansions remain uncorrelated
scopes: SQL does not allow a CTE or view body to reference the consuming
query's columns, and the binder does not link them to an outer scope.

### 4.2 `OuterRef` and correlation lists

A name that resolves to an **enclosing** scope binds as a new expression
variant:

```rust
BoundExpr::OuterRef {
    slot: usize,        // index into the enclosing subquery's correlation list
    data_type: DataType,
    nullable: bool,
}
```

The correlation list is carried on **`BoundQuery`** (empty for uncorrelated
queries), so a subquery expression's body and — later — a `LATERAL` derived
table share one representation:

```rust
pub struct CorrelatedColumn {
    /// The outer column, as an expression valid in the *immediately enclosing*
    /// scope (an `InputRef`, or an `OuterRef` when the reference chains
    /// further out).
    pub outer: BoundExpr,
    pub data_type: DataType,
    pub nullable: bool,
}
```

During binding, a scope accumulates entries tagged with the **scope distance**
at which the name resolved; when a subquery boundary unwinds, entries that
resolved past the immediate parent are re-interned into the parent's
accumulator and their `outer` becomes an `OuterRef` into the parent's list.
Duplicate references to the same outer column re-use their slot. No bound
expression is ever rewritten by this translation — only the lists are.

`OuterRef { slot }` always indexes the correlation list of the **immediately
enclosing** subquery (the PostgreSQL `PARAM_EXEC` model). A reference that
skips levels chains: if a depth-2 subquery references a depth-0 column, the
depth-2 list entry's `outer` is itself an `OuterRef` into the depth-1 list,
and the depth-1 subquery becomes correlated in turn. An empty correlation list
means the subquery is uncorrelated and takes the existing pre-pass path.

### 4.3 Passes that assumed constant subqueries

The parameter pass (`crates/planner/src/params.rs`) already recurses into
subquery bodies for both collection and substitution — no work needed there.
The logical-planner aggregate/grouping rewrite passes, however, treat a
subquery body as an opaque leaf today; with correlation they must traverse
the carried `BoundQuery` (and the correlation lists' `outer` expressions,
which live in the enclosing scope's terms).

### 4.4 Position guard

The hoisting pass lifts every correlated subquery in a supported position
(§5.1); one that remains as an expression sits in a position the planner does
not hoist (join `ON`, `ORDER BY`, DML assignments, `RETURNING`,
`ON CONFLICT`, ...) and is rejected by the executor pre-pass with
`SqlState::FeatureNotSupported`. During milestone S1 — before Apply existed —
this same guard rejected every correlated subquery, which made binder
correlation behavior-neutral on its own.

## 5. Apply: plan node and operator

### 5.1 Plan node

```rust
// Same shape at both plan levels (LogicalPlan::Apply / PhysicalPlan::Apply).
Apply {
    input: Box<Plan>,          // outer side
    subplan: Box<Plan>,        // inner template, contains OuterRef exprs
    correlations: Vec<BoundExpr>, // per OuterRef slot: expr over the outer row
    kind: ApplyKind,
}

pub enum ApplyKind {
    /// Appends one column: the scalar subquery's value per outer row.
    Scalar { data_type: DataType },
    /// Appends one non-null boolean column: EXISTS (negated already applied).
    Exists { negated: bool },
    /// Appends one boolean column: `operand [NOT] IN (subplan)` with
    /// three-valued logic; `operand` is an expression over the outer row.
    In { operand: Box<BoundExpr>, negated: bool },
}
```

An Apply always appends its column; the hoisted expression consumes it via
the replacement `LocalRef`, and an enclosing projection drops it from the
statement's visible output. Two consequences of the plan shapes:

- A single-table `WHERE` lowers into the scan's own filter, so a predicate
  containing subquery candidates is pulled back out and split into `AND`
  conjuncts: conjuncts without candidates return to the scan's filter
  (keeping index selection), decorrelatable conjuncts become semi/anti joins
  stacked above the scan (§6), and the rest hoist through an Apply consumed
  by a `Filter` above everything.
- An `UPDATE`/`DELETE` source must produce exactly the target table's row
  shape, so after hoisting the planner layers a projection back to the
  table's columns above the Apply (row identity passes through projections).

The hoisting pass runs on the logical plan at the start of physical planning
(it needs the catalog for row widths): for each expression tree containing a
correlated subquery, it inserts `Apply` above the expression's input plan and
replaces the subquery expression with a `LocalRef` to the appended column.
Multiple correlated subqueries in one expression tree stack one Apply per
subquery; an `IN` operand's own correlated subqueries are hoisted before the
operand is captured. Subquery bodies planned during hoisting are hoisted in
turn, so nesting works at any depth.

### 5.2 `ApplyOp` (executor)

- Holds the `&'a ExecutionContext<'_>` it was built with; `build_executor`'s
  existing signature (`Box<dyn PlanExecutor + 'a>` borrowing `ctx` for `'a`)
  already permits this.
- **At construction**, runs the uncorrelated pre-pass once over the inner
  template, so nested uncorrelated subqueries become constants once per
  statement, not once per outer row. The statement-level pre-pass does **not**
  descend into `Apply` subplans; `ApplyOp` construction owns them.
- On the analysis-only executor path, the pre-pass shares the statement's
  profiling state. Uncorrelated work in an Apply template is emitted as an
  init plan once when that template is constructed; the correlated Apply
  subplan itself remains in the main layout and its repeated physical
  executions aggregate under the template node IDs. Ordinary execution keeps
  the existing resolver and memoization behavior unchanged.
- **Per outer row**: evaluates the `correlations` expressions against the
  outer row, substitutes `OuterRef { slot }` → `Literal` throughout a clone of
  the template, builds the inner executor, and runs it. `Exists` pulls at most
  one row; `Scalar` errors on a second row (21000); `In` drains the single
  inner column into a rewindable spill tape and applies the existing
  three-valued membership semantics through a fresh reader.
- **Memoization**: results are cached in an LRU hash map keyed by the tuple of
  correlation values, only when the template is volatile-free (§2). Keys,
  entries, scalar heap data, and map capacity share the operator's `work_mem`
  account; column and row results are rewindable spill tapes using that same
  account. Entries are evicted when metadata cannot fit.
- Rebuilding the inner executor per outer row is the accepted v1 cost; reusing
  a built executor via re-`open` is a deferred optimization (§12 S6,
  `overview.md` §13) and must not change behavior.
- An `OuterRef` is not a literal, so index selection inside the template sees
  no usable key: template scans plan as full scans plus filters. Substituting
  first and re-planning per row (index-aware rescans) is deferred (§12 S6,
  `overview.md` §13); the S3 decorrelation rules are the fast path for
  equality shapes.
- The `In` kind memoizes the materialized subquery column, not the membership
  verdict: the operand is evaluated per outer row independently of the
  correlation key.
- The volatility probe (§2) covers the template's expressions, nested Apply
  templates, and the bound bodies of not-yet-resolved subquery expressions —
  the last because a nested template's uncorrelated subqueries are resolved
  lazily at that nested operator's construction, i.e. once per outer memo
  miss rather than once per statement (an accepted v1 cost).

### 5.3 One plan rewriter

The pre-pass, the OuterRef substitution, and the hoisting pass share one
generic structural rewriter (`planner::rewrite_plan_exprs` /
`planner::rewrite_expr`, with `f: FnMut(&BoundExpr) -> Result<Option<BoundExpr>>`),
so the passes cannot drift as plan nodes are added. The plan-level walker
rewrites an Apply node's `correlations` and `In` operand (expressions over
that plan's rows) but never descends into its `subplan` — a separate
`OuterRef` namespace owned by the Apply operator.

## 6. Decorrelation: semi/anti joins

### 6.1 Join types

`JoinType` gains `Semi` and `Anti`. Both the nested-loop and hash join
operators implement them: the output schema is the **left (outer) side only**;
`Semi` emits a left row on its first match and moves on; `Anti` emits a left
row only if no right row matches. The hash variants keep the existing
build/probe structure with early-out probes.

### 6.2 Rules

Applied to top-level `AND` conjuncts of `WHERE`/`HAVING` predicates during
the hoisting pipeline (§5.1); a shape the rules do not match falls back to
Apply (correlated) or the pre-pass (uncorrelated), so the rules are pure
optimization:

- **`[NOT] EXISTS`** whose body is a plain single-table `SELECT` (no
  grouping, `DISTINCT`, ordering, limits, joins, or derived tables) and whose
  only use of the outer scope is a conjunction of `inner_col = outer_col`
  equality predicates — anywhere else in the body (remaining conjuncts,
  projection, or a nested subquery's correlation entries) an outer reference
  disqualifies. The equalities are stripped and become the join condition of
  a `Semi`/`Anti` join; the remaining body conjuncts stay on the inner scan
  (index-selectable). A chained correlation entry (`OuterRef` outer) is
  allowed: it lands in the join condition, keeps the join on the nested-loop
  path, and is substituted by the enclosing Apply.
- **Uncorrelated `col IN (SELECT ...)`**: `Semi` hash join on
  `col = subquery output`. The operand must be a plain column reference
  (hash-key-shaped); anything else keeps the pre-pass `InList` path.
- **Uncorrelated `col NOT IN (SELECT ...)`**: `Anti` hash join **only when**
  the operand column and the subquery's output column are both non-nullable
  (by binder nullability); otherwise the pre-pass preserves the three-valued
  `NULL` semantics.

The equi-key extraction accepts `LocalRef` as well as `InputRef` slots, so a
`HAVING` decorrelation (post-aggregate `LocalRef` correlation entries) also
takes the hash path. Correlated `IN` decorrelation (combining the operand key
with correlation keys) is deliberately deferred; it runs as Apply.

## 7. LATERAL

`[INNER | LEFT] JOIN LATERAL (subquery) alias ON <cond>` and the comma form
`, LATERAL (subquery) alias` make FROM items to the left visible inside the
derived table. The binder binds the lateral body through the same
correlated-child-query path as subquery expressions — the partial FROM scope
assembled so far is the immediate parent, so sibling references become
correlation entries on the derived `BoundQuery` and enclosing-scope
references chain outward as usual. FROM lowering produces
`ApplyKind::Lateral`: the subplan is the full derived query, the appended
columns are its entire output schema, and per outer row every matching inner
row yields one output row (`left_join` emits one null-padded row when none
match). The `ON` condition evaluates per combined (outer ++ inner) row inside
the operator — its slots are the FROM scope's, which already match. The memo
stores the **unfiltered** inner rows per correlation key, since the condition
may reference outer columns. Those rows live in rewindable spill tapes, and the
operator reads one row per `next()` call instead of buffering a pending output
queue.

Restrictions:

- `LATERAL` on the nullable side of a `RIGHT`/`FULL` join is rejected
  (`FeatureNotSupported`).
- A sibling-referencing `LATERAL` must be the **right** side of its explicit
  join, and its sibling references must stay **inside that join's subtree** —
  the Apply's input is the join's left subtree, so a reference crossing the
  join boundary (an earlier comma sibling) cannot be supplied
  (`FeatureNotSupported`; comma-form FROM lists fold left-deep, so a
  comma-form lateral always sees the whole preceding prefix and is
  unrestricted). Sibling slots are rebased onto the subtree's row at
  lowering. A lateral whose correlations are purely chained (enclosing-scope
  only) is unrestricted: it lowers to an Apply over a unit `VALUES` row,
  carrying its correlation list for the enclosing Apply to substitute.

Non-`LATERAL` derived tables remain uncorrelated scopes: sibling references
still fail name resolution, and cross-level outer references keep the §1.1
`FeatureNotSupported` rejection after S4.

## 8. `UPDATE ... FROM` / `DELETE ... USING`

These are correlated joins on the write path.

### 8.1 Row identity through joins

`ExecRow` carries `identity: Option<RowIdentity>` (physical `RowId` + creator
transaction + key),
which the Update/Delete executors use to target heap tuples. Join operators
currently produce combined rows with `identity: None`. The join plan nodes
The join plan nodes which may appear on a DML source spine (`NestedLoopJoin`,
`HashJoin`) carry:

```rust
identity_from: Option<JoinSide>,   // None = plain query joins (no identity)
```

`JoinSide` has only `Left` (nothing plants a DML target on the right). When
set, the join copies the left side's `RowIdentity` into every combined row it
emits. Only DML-source lowering sets it — a marking pass walks the source's
left spine (through filters, projections, and the Applys that `LATERAL` items
lower to, into every inner/cross join) after the bound source is lowered, so
the target scan's identity flows to the top. Semi/anti joins need no marker: they emit the left `ExecRow` whole.
`RowIdentity` never enters the `Value` domain — `Value`'s variant order is a
durable on-disk key-ordering contract and must not grow variants for
executor-internal needs.

The identity side of such a join is never null-padded: the source join spine
is inner/cross joins only (§2).

### 8.2 Plan shape and dedupe

`UPDATE t SET ... FROM f WHERE p` binds the target table first (slots `0..`)
and folds the FROM/USING items onto it comma-style (cross joins + `WHERE`),
so the source produces the combined (target ++ FROM) row; `SET` expressions
and the `WHERE` see the combined scope. A `joined_source` flag travels from
the binder through the plans to the executor (width cannot stand in for it —
a zero-column FROM item keeps the combined width equal to the table's): the
target prefix is the row to update/delete, assignments
evaluate against the full combined row, and a `HashSet` of target row ids
skips a target already processed — implementing §2's "modified once, first
match in scan order" deterministically. `RETURNING` emits one row per
modified target and sees the target columns only (the new row for `UPDATE`,
the old-row prefix for `DELETE`) — a documented divergence from PostgreSQL,
which also exposes the matched FROM row's columns.

Explicit `JOIN` items, derived tables, and `LATERAL` items all work. A
FROM-item join's `ON` clause sees only that join's operands (its condition is
rebased onto the join's own row at lowering); referencing the target — or any
FROM entry outside the join — is rejected with the PostgreSQL-style "invalid
reference to FROM-clause entry" error, and the same predicate belongs in
`WHERE`.

`ON CONFLICT` is unaffected (INSERT-only). `UPDATE ... FROM` follows the same
tuple-lock and EvalPlanQual rules as plain `UPDATE`: Read Committed substitutes a
concurrent successor into the complete joined source and reruns qualification;
Repeatable Read / Serializable return `40001` (`docs/specs/mvcc.md` §7,
`docs/specs/deadlock.md`).

## 9. EXPLAIN

`EXPLAIN` renders Apply nodes with their kind and correlation count
(`Apply (Exists, correlated on 2 columns)`), lateral Applies as
`Nested Loop Lateral`, and semi/anti joins as `Hash Semi Join`,
`Hash Anti Join`, `Nested Loop Semi Join`, `Nested Loop Anti Join`, following
the existing plan-tree text format.

`EXPLAIN ANALYZE` aggregates every physical execution of a correlated Apply
template under its fixed node IDs, so inner loop counts reflect executed keys;
memo hits do not fabricate loops. Executed uncorrelated scalar, EXISTS, and IN
subqueries appear once in deterministic `InitPlan` sections (including parent
markers for nesting), with node IDs allocated after the complete main tree.

## 10. Errors

| Condition | SQLSTATE |
|---|---|
| Correlated scalar subquery returns >1 row for some outer row | `21000` `CardinalityViolation` |
| Correlated subquery in a not-yet-supported position (join `ON`, `ORDER BY`) | `0A000` `FeatureNotSupported` |
| Outer reference from a set-operation arm, `VALUES` list, or derived-table body (§1.1) | `0A000` `FeatureNotSupported` |
| Correlated subquery before milestone S2 (staging guard, §4.4) | `0A000` `FeatureNotSupported` |
| Aggregate whose argument references only outer-level columns (§2) | `0A000` `FeatureNotSupported` |
| Correlated reference to an ungrouped column of an aggregate outer query (§2) | `42804` `DatatypeMismatch` |
| Correlation into a CTE or view body | normal name-resolution error (`42703`) |

## 11. Testing

- **planner**: scope-stack resolution (shadowing, qualified outer refs,
  depth-2 chaining, CTE/view isolation), correlation-list construction,
  hoisting shapes, decorrelation rules firing (and declining when
  `NULL`-unsafe), `identity_from` planning.
- **executor**: ApplyOp per-row semantics (cardinality, three-valued `IN`,
  EXISTS early-out), memoization on/off by volatility, semi/anti join
  operators, identity propagation through both join operators, DML dedupe.
- **server integration**: correlated subqueries under all three isolation
  levels; extended-protocol prepared statements mixing `$n` parameters with
  correlation; `EXPLAIN` output; `UPDATE ... FROM` / `DELETE ... USING`
  end-to-end including `RETURNING` and multi-match dedupe; volatile subplans
  (`nextval`) executing per row.

## 12. Milestones

- **S0** — this spec.
- **S1** — binder correlation: scope stack, `OuterRef`, correlation lists,
  recursing passes, executor staging guard. Behavior-neutral.
- **S2** — one generic plan rewriter; `Apply` node + hoisting pass
  (`WHERE` / `SELECT` list / `HAVING`); `ApplyOp` with memoization; `EXPLAIN`.
  Correlated subqueries work.
- **S3** — `Semi`/`Anti` join types (nested-loop + hash); decorrelation rules.
- **S4** — `LATERAL`.
- **S5** — `identity_from` join propagation, then
  `UPDATE ... FROM` / `DELETE ... USING` with dedupe. Implemented.
- **S6** — polish, implemented as: cancellation polling per outer row and
  inside Apply's inner drains, and the README / `overview.md` documentation
  sweep. `LIMIT 1` injection above `EXISTS` templates was evaluated and
  dropped as inert: the Exists drain already pulls at most one row, and this
  engine's materializing operators do their work in `open()`, which a
  plan-level `LIMIT` cannot reach. Deferred to `overview.md` §13: ApplyOp
  re-`open` rescans (per-row rebuild is the accepted cost) and index-aware
  per-row template replanning.

Each milestone updates the SQL-subset language in `docs/specs/overview.md` and
the affected crate specs in the same change, per the repository's spec-sync
rule.
