# Correlated Subqueries, LATERAL, and Join-Sourced DML

**Status:** Approved design — implementation staged on branch `subqueries`
(§12 milestones; the Status line is updated as milestones land).

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
  (rejected with `FeatureNotSupported` until a later milestone).
- Outer references from inside a **set-operation arm** or a **derived table's
  body** within a subquery (rejected with `FeatureNotSupported`; the outer
  scope chain is still threaded there so the error names the construct instead
  of claiming the column does not exist). `LATERAL` (S4) makes *sibling*
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

## 3. Architecture overview

The design is **plan-level Apply (dependent join)**, not expression-level
rescan:

1. The **binder** resolves outer references and records, per subquery, an
   ordered **correlation list** of the outer columns it uses (§4).
2. A **planner pass** hoists each correlated subquery expression out of the
   expression tree into an **`Apply` plan node** above the expression's input
   plan, replacing the expression with an `InputRef` to a column the Apply
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

### 4.4 Staging guard

Until the executor supports Apply (§12 milestone S2), a plan containing a
correlated subquery is rejected by the executor pre-pass with
`SqlState::FeatureNotSupported` and a message naming the construct. Binder
correlation (milestone S1) is therefore behavior-neutral on its own.

## 5. Apply: plan node and operator

### 5.1 Plan node

```rust
PhysicalPlan::Apply {
    input: Box<PhysicalPlan>,        // outer side
    subplan: Box<PhysicalPlan>,      // inner template, contains OuterRef exprs
    correlations: Vec<BoundExpr>,    // per slot: expr over the *outer* row
    kind: ApplyKind,
}

pub enum ApplyKind {
    /// Appends one column: the scalar subquery's value per outer row.
    Scalar { data_type: DataType },
    /// Appends one non-null boolean column: EXISTS (negated already applied).
    Exists { negated: bool },
    /// Appends one boolean column: `operand [NOT] IN (subplan)` with
    /// three-valued logic; `operand` is an expression over the outer row.
    In { operand: BoundExpr, negated: bool },
}
```

The logical planner hoists correlated subquery expressions bottom-up: for each
expression tree containing one, it inserts `Apply` above the expression's
input plan and replaces the subquery expression with an `InputRef` to the
appended column. Multiple correlated subqueries in one expression tree stack
one Apply per subquery. A projection above the consumer drops appended columns
from the statement's visible output.

### 5.2 `ApplyOp` (executor)

- Holds the `&'a ExecutionContext<'_>` it was built with; `build_executor`'s
  existing signature (`Box<dyn PlanExecutor + 'a>` borrowing `ctx` for `'a`)
  already permits this.
- **At construction**, runs the uncorrelated pre-pass once over the inner
  template, so nested uncorrelated subqueries become constants once per
  statement, not once per outer row. The statement-level pre-pass does **not**
  descend into `Apply` subplans; `ApplyOp` construction owns them.
- **Per outer row**: evaluates the `correlations` expressions against the
  outer row, substitutes `OuterRef { slot }` → `Literal` throughout a clone of
  the template, builds the inner executor, and runs it. `Exists` pulls at most
  one row; `Scalar` errors on a second row (21000); `In` materializes the
  single inner column and applies the existing three-valued `InList`
  evaluation.
- **Memoization**: results are cached keyed by the tuple of correlation
  values, only when the template is volatile-free (§2). The cache is
  per-operator, bounded only by the statement's lifetime.
- Rebuilding the inner executor per outer row is the accepted v1 cost; reusing
  a built executor via re-`open` is a later optimization (§12 milestone S6)
  and must not change behavior.

### 5.3 One plan rewriter

The existing pre-pass and the new OuterRef substitution share one generic
structural rewriter (`rewrite_plan_exprs(plan, f)` where
`f: FnMut(&BoundExpr) -> Result<Option<BoundExpr>>`), so the two passes cannot
drift as plan nodes are added.

## 6. Decorrelation: semi/anti joins

### 6.1 Join types

`JoinType` gains `Semi` and `Anti`. Both the nested-loop and hash join
operators implement them: the output schema is the **left (outer) side only**;
`Semi` emits a left row on its first match and moves on; `Anti` emits a left
row only if no right row matches. The hash variants keep the existing
build/probe structure with early-out probes.

### 6.2 Rules

Applied during logical planning, before Apply-hoisting; a shape the rules do
not match falls back to Apply, so the rules are pure optimization:

- **`[NOT] EXISTS`** in `WHERE`, where the subquery's correlation appears only
  as a conjunction of `inner_col = outer_col` equality predicates (any other
  use of an outer reference disqualifies): strip those predicates, join the
  outer input to the de-correlated inner on the equality keys as
  `Semi`/`Anti`. Equality keys make it a hash join by the existing equi-join
  rule; otherwise nested-loop.
- **Uncorrelated `IN (SELECT c ...)`** in `WHERE`: `Semi` hash join on
  `operand = c`.
- **Uncorrelated `NOT IN (SELECT c ...)`** in `WHERE`: `Anti` hash join
  **only when** `operand` and `c` are both non-nullable (by binder
  nullability); otherwise Apply preserves the three-valued `NULL` semantics.

Correlated `IN` decorrelation (combining the operand key with correlation
keys) is deliberately deferred; it runs as Apply.

## 7. LATERAL

`[INNER | LEFT] JOIN LATERAL (subquery) alias ON ...` and the comma form
`, LATERAL (subquery) alias` make FROM items to the left visible inside the
derived table. The binder gives the lateral body a scope whose parent is the
partial FROM row assembled so far; the planner produces a **lateral Apply**: a
generalization of `ApplyKind` where the inner is a full table expression and
the appended columns are the inner's entire output schema, with inner-join
(row dropped when inner is empty) and left-join (null-padded) variants.
Non-`LATERAL` derived tables remain uncorrelated scopes: sibling references
still fail name resolution, and cross-level outer references keep the §1.1
`FeatureNotSupported` rejection after S4.

## 8. `UPDATE ... FROM` / `DELETE ... USING`

These are correlated joins on the write path.

### 8.1 Row identity through joins

`ExecRow` carries `identity: Option<RowIdentity>` (physical `RowId` + key),
which the Update/Delete executors use to target heap tuples. Join operators
currently produce combined rows with `identity: None`. The join plan nodes
(`NestedLoopJoin`, `HashJoin`) gain:

```rust
identity_from: Option<JoinSide>,   // None = today's behavior
```

When set, the join copies that side's `RowIdentity` into every combined row it
emits. Only DML-source planning sets it; the binder always plants the target
table as the **left** input with `identity_from: Some(Left)`. `RowIdentity`
never enters the `Value` domain — `Value`'s variant order is a durable on-disk
key-ordering contract and must not grow variants for executor-internal needs.

The identity side of such a join is never null-padded: the source join is an
inner join (§2). The operator asserts this invariant.

### 8.2 Plan shape and dedupe

`UPDATE t SET ... FROM f WHERE p` plans its `source` as
`target ⨝ FROM-tables` (filtered by `p`) with the target left; `SET`
expressions and `RETURNING` may reference all joined columns. The Update and
Delete executors keep a `HashSet<RowId>` of already-written targets and skip a
combined row whose target identity was already processed — implementing §2's
"modified once, first match in scan order" deterministically. `RETURNING`
emits one row per modified target row (not per join match).

`ON CONFLICT` is unaffected (INSERT-only). `UPDATE ... FROM` follows the same
first-updater-wins / row-lock rules as plain `UPDATE` (`docs/specs/mvcc.md`
§7, `docs/specs/deadlock.md`).

## 9. EXPLAIN

`EXPLAIN` renders Apply nodes with their kind and correlation count
(`Apply (Exists, correlated on 2 columns)`), lateral Applies as
`Nested Loop Lateral`, and semi/anti joins as `Hash Semi Join`,
`Hash Anti Join`, `Nested Loop Semi Join`, `Nested Loop Anti Join`, following
the existing plan-tree text format.

## 10. Errors

| Condition | SQLSTATE |
|---|---|
| Correlated scalar subquery returns >1 row for some outer row | `21000` `CardinalityViolation` |
| Correlated subquery in a not-yet-supported position (join `ON`, `ORDER BY`) | `0A000` `FeatureNotSupported` |
| Outer reference from a set-operation arm or a derived-table body (§1.1) | `0A000` `FeatureNotSupported` |
| Correlated subquery before milestone S2 (staging guard, §4.4) | `0A000` `FeatureNotSupported` |
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
  `UPDATE ... FROM` / `DELETE ... USING` with dedupe.
- **S6** — polish: ApplyOp re-`open` rescan, `LIMIT 1` injection for `EXISTS`
  subplans, README updates, and an `overview.md` §13 entry for the remaining
  deferrals (correlated-`IN` decorrelation, correlation in join `ON` /
  `ORDER BY` / set-operation arms / non-`LATERAL` derived-table bodies).

Each milestone updates the SQL-subset language in `docs/specs/overview.md` and
the affected crate specs in the same change, per the repository's spec-sync
rule.
