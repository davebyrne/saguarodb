# SaguaroDB Window Functions Specification

**Date:** 2026-07-19
**Status:** In progress — milestones M0–M4 are complete; implementation
milestones M5–M6 are pending. Window calls parse, bind, validate, lower to
logical and physical Window nodes, and execute the whole-partition ranking,
distribution, `ntile`, `lag`, and `lead` families with spill-backed ordering.
Frame-respecting value functions and window aggregates return a structured
`0A000` staging error until M5. The affected crate specifications,
`docs/specs/overview.md`, `README.md`, and `AGENTS.md` are updated milestone by
milestone as listed in §11.

This document specifies window functions across the parser, binder, planner,
executor, and SQL protocol surface. It is the system-level contract for the
feature and complements `docs/specs/overview.md` and the crate contracts under
`docs/specs/crates/`.

## 1. Purpose and scope

A window function computes a value for each input row from a partition of the
query result without collapsing that partition to one row. SaguaroDB now
executes the M4 whole-partition families; M5 completes frame-respecting value
functions and aggregates. The implementation uses the existing external-sort
and spill infrastructure so correctness does not depend on the input fitting
in memory.

The supported function set is:

- Ranking and distribution: `row_number()`, `rank()`, `dense_rank()`,
  `ntile(n)`, `percent_rank()`, and `cume_dist()`.
- Offset functions: `lag(value [, offset [, default]])` and
  `lead(value [, offset [, default]])` in their one-, two-, and three-argument
  forms.
- Frame value functions: `first_value(value)`, `last_value(value)`, and
  `nth_value(value, n)`.
- Every existing aggregate as a window aggregate: `count`, `sum`, `avg`,
  `min`, `max`, `stddev_samp` (and `stddev`), `stddev_pop`, `var_samp` (and
  `variance`), `var_pop`, `bool_and`, `bool_or`, `array_agg`, and
  `string_agg`. These are the 13 `AggregateFunc` variants; aliases do not add
  variants.

Both `ROWS` and `RANGE` frames are supported, with
`UNBOUNDED PRECEDING`, `UNBOUNDED FOLLOWING`, `CURRENT ROW`, and literal
`N PRECEDING` / `N FOLLOWING` bounds. A single-bound shorthand such as
`ROWS 1 PRECEDING` means `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`.

### Goals

- Match PostgreSQL partition, peer, ordering, default-frame, frame-bound, and
  NULL semantics for the supported surface.
- Preserve SQL evaluation order: window evaluation occurs after grouping and
  `HAVING`, but before query-level sorting, duplicate elimination, final
  projection, and limiting.
- Support several window specifications and structurally deduplicate identical
  calls while keeping deterministic plan and output ordering.
- Remain correct under constrained `work_mem` by sorting and buffering through
  spill-backed operators rather than retaining whole inputs in memory.
- Keep expression evaluation pure: window calls are lifted into plan nodes and
  replaced with local result slots before executor expression evaluation.

### 1.1 Non-goals (v1)

- `GROUPS` frame mode is deferred and rejected with `0A000`
  `FeatureNotSupported`.
- Frame `EXCLUDE` is deferred. `sqlparser` 0.56 cannot parse the syntax, so it
  fails at the parser boundary with `42601` `SyntaxError` rather than reaching
  SaguaroDB's structured feature gate.
- A query-level `WINDOW` clause, named windows, and `OVER window_name` are
  deferred and rejected with `0A000` `FeatureNotSupported`.
- Aggregate `FILTER` on a window call is deferred and rejected with `0A000`
  `FeatureNotSupported`.
- `IGNORE NULLS` and `RESPECT NULLS` syntax is deferred and rejected with
  `0A000` `FeatureNotSupported`. Supported functions use PostgreSQL's default
  respect-NULLs behavior.
- `DISTINCT` in a window aggregate is deferred and rejected with `0A000`
  `FeatureNotSupported`.
- Window functions in `CREATE VIEW` are deferred and rejected at durable-query
  serialization with `0A000` `FeatureNotSupported`.
- Correlated subqueries in window arguments, `PARTITION BY`, or window
  `ORDER BY` are deferred and rejected at bind time with `0A000`
  `FeatureNotSupported`. The correlated-subquery hoister does not support
  those positions in v1.
- Frame offsets must be literals. A non-literal offset is rejected with
  `0A000` `FeatureNotSupported`.
- `lag` and `lead` offsets must be bind-time integer constants. A variable
  offset is rejected with `0A000` `FeatureNotSupported`.
- A locking clause (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, or
  `FOR KEY SHARE`) cannot be combined with window functions and is rejected
  with `0A000` `FeatureNotSupported`.
- Compatible but non-identical `OVER` specifications do not share a sort. Each
  distinct specification has its own spill-backed sort; sort sharing is a
  future optimization and has no semantic effect.

## 2. Semantics

### 2.1 Partitions, ordering, peers, and frames

For each distinct `OVER` specification, `PARTITION BY` divides the input into
partitions. With no partition expressions, the complete input is one
partition. The window `ORDER BY` orders rows within each partition. Rows for
which all window ordering expressions compare equal are **peers**; with no
window ordering expressions, every row in the partition is a peer.

A frame selects a range of rows relative to the current row inside its
partition. It never crosses a partition boundary. `ROWS` bounds are physical
row positions in window order. `RANGE CURRENT ROW` expands to the current
row's complete peer group. A `RANGE` offset compares the single ordering key
against a direction-aware threshold; a row with a NULL ordering key gets its
NULL peer group as its frame.

When `ORDER BY` is present and no frame is written, the effective frame is:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

It includes all rows from the partition start through the current row's last
peer. Without `ORDER BY`, the default is the whole partition, equivalent to:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
```

Frame indices and threshold arithmetic are checked and clamped to the
partition. Overflow while forming a `RANGE` threshold clamps toward the
corresponding unbounded edge, matching PostgreSQL `in_range` behavior.

### 2.2 Function behavior

- `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`, `cume_dist`,
  `lag`, and `lead` operate over the whole partition and ignore the frame.
  Ranking and distribution functions use peer groups where applicable.
  `percent_rank` is `0` for a one-row partition; `cume_dist` includes the
  current row's complete peer group.
- `first_value`, `last_value`, `nth_value`, and every window aggregate respect
  the current row's frame. In particular, default-frame `last_value` returns
  the value from the last peer of the current row, not necessarily the last
  row of the partition.
- `ntile` evaluates its argument once, against the first row of the partition.
  A NULL argument produces NULL for the complete partition. A value less than
  or equal to zero raises `22014` `InvalidArgumentForNtile`.
- `lag` and `lead` default the offset to one. A NULL offset returns NULL. A
  negative constant offset reverses direction. If the target lies outside the
  partition, the optional default expression is evaluated against the current
  row; without a default the result is NULL.
- Unlike frame offsets and `lag`/`lead` offsets, which are bind-time constants,
  `nth_value` evaluates `n` for each current row, matching PostgreSQL. NULL `n`
  produces NULL, an `n` less than one raises `22016`
  `InvalidArgumentForNthValue`, and a target outside the current frame produces
  NULL.
- An empty frame yields NULL for value functions and aggregates, except
  `count`, which yields zero. Aggregate-specific NULL handling otherwise
  remains the same as for grouped aggregation. `lag`, `lead`, and the frame
  value functions respect NULL values rather than searching past them.

### 2.3 `RANGE` offset types

An offset `RANGE` frame requires exactly one `ORDER BY` expression. The binder
accepts only these ordering-key and offset combinations:

| Ordering key | Offset |
|---|---|
| `Integer` | `Integer` |
| `Numeric` | `Numeric` |
| `Double` or `Real` | the same type as the key |
| `Timestamp` or `TimestampTz` | `Interval` |
| `Date` | `Interval` (matches PostgreSQL; see below) |

The `Date` entry follows PostgreSQL's btree
`in_range(date, date, interval, ...)` support function, which implements the
offset by promoting the date to timestamp microseconds.

The offset literal is resolved to the type required by the ordering key at
bind time. Any number of ordering keys other than one is `42P20`
`WindowingError`. Any other key type is `0A000` `FeatureNotSupported`, with
`RANGE with offset PRECEDING/FOLLOWING is not supported for column type ...`.
A literal that cannot bind as the required offset type is `42804`
`DatatypeMismatch`. `RANGE` frames without offset bounds do not require exactly
one ordering key.

## 3. Architecture overview

Window evaluation occupies this position in a SELECT pipeline:

```text
source → Aggregate → HAVING → Window* → Sort → Distinct → Projection → Limit
```

`Aggregate` and `HAVING` are present only for aggregate queries. `Window*`
means zero or more chained window nodes. One node is created for each distinct
structural `(partition_by, order_by, frame)` specification, in the order in
which specifications first appear. Structurally identical window calls share
one appended result column. Calls with different specifications do not share a
node or sort in v1.

This placement permits an aggregate result inside a window argument, for
example `sum(count(*)) OVER ()`: ordinary aggregates are rewritten first, and
the window consumes the aggregate's local slot. The reverse nesting, a window
call inside an aggregate argument, is rejected with `42803` `GroupingError`.

Each Window node appends its function results to its source row. For group
`k`, appended slots begin at:

```text
B_k = W0 + sum(function_count_i for i < k)
```

In aggregate context, `W0` is `group_by.len() + aggregates.len()`, the output
width of the Aggregate node. Only in non-aggregate context is `W0`
`BoundSelect::source_width`, the FROM-row width captured immediately after FROM
binding. This prevents later rewrites from inferring the base width from a
schema that has already changed. Window calls in projection, sort, and distinct
expressions are replaced with `LocalRef` slots at these positions.

## 4. Parser

The parser AST represents a window call separately from an ordinary scalar or
aggregate call so existing `Expr::Function` consumers cannot accidentally
treat `OVER` as scalar evaluation:

```rust
Expr::WindowFunction {
    name: String,
    args: Box<[FunctionArg]>,
    distinct: bool,
    spec: Box<WindowSpec>,
}
```

The argument slice and specification are boxed so the dedicated variant stays
in the existing `Expr` size class rather than enlarging every expression-bearing
query node.

`WindowSpec` contains `partition_by`, `order_by: Vec<OrderByItem>`, and an
optional `WindowFrame`. A frame contains `units` (`Rows` or `Range`), `start`,
and `end: WindowFrameBound`. Bounds represent unbounded preceding/following,
current row, and offset preceding/following. Conversion normalizes a shorthand
frame to an explicit `CURRENT ROW` end.

Parser conversion gives `GROUPS`, named-window forms, `FILTER`, and NULL-
treatment modifiers their precise `0A000` errors. The query-level named
`WINDOW` clause is split from the existing catch-all unsupported-query check.
`EXCLUDE` remains the `sqlparser`-originated `42601` case described in §1.1.

## 5. Binder

The bound expression layer adds `WindowFunc` (the 11 named function families
plus `Aggregate(AggregateFunc)`), `BoundWindowSpec`, `BoundWindowFrame`, and
`BoundFrameBound`. `ROWS` offsets are resolved to `u64`; `RANGE` offsets are
resolved to typed `Value`s. A call binds as:

```rust
BoundExpr::WindowCall {
    func,
    args,
    spec,
    data_type,
    nullable,
}
```

The binder applies the ordinary aggregate argument packing and type rules to
window aggregates, including empty args for `count(*)` and the packed-array
convention for `string_agg`. It resolves omitted frames to the defaults in
§2.1 and validates all offsets at bind time; executor validation is defensive.

Function-name resolution checks the window-only name map before falling
through to the scalar-function registry. Calling a window-only function without
`OVER`, for example `row_number()`, is `42809` `WrongObjectType` with `window
function row_number requires an OVER clause`. Conversely, any name used with
`OVER` that is neither a window function nor an aggregate, whether a known
scalar such as `abs` or an unknown name, is `42809` `WrongObjectType` with
`OVER specified, but <name> is not a window function nor an aggregate`.

### 5.1 Placement and nesting

Window calls are allowed in the SELECT list, query `ORDER BY`, and
`DISTINCT` / `DISTINCT ON` keys. They are rejected with `42P20` in `WHERE`,
`GROUP BY`, `HAVING`, join `ON`, `VALUES`, DML `RETURNING`, `UPDATE SET`,
`ON CONFLICT SET`, column `DEFAULT`, and `CHECK` expressions. Window calls may
not be nested. A window call inside an ordinary aggregate argument is also
`42803` `GroupingError`; an aggregate inside a window argument is supported as
described in §3.

Uncorrelated scalar subqueries in window arguments, `PARTITION BY`, and window
`ORDER BY` are supported: the existing uncorrelated-subquery pre-pass resolves
them before window execution. Correlated subqueries in those positions are
rejected at bind time with `0A000` `FeatureNotSupported`; v1 hoisting does not
support that position.

All bound-expression walkers descend into the call's arguments and window
specification where relevant: parameter collection/substitution, grouping
validation, aggregate detection, simplification, sequence detection, object
collection, and structural rewriting. A `BoundExpr::WindowCall`, including one
whose function is `WindowFunc::Aggregate(...)`, must not itself count as an
ordinary aggregate in `contains_aggregate`, aggregate-context detection, or
`collect_aggregates`. Those walkers descend only into its arguments and its
`PARTITION BY` and `ORDER BY` expressions. Otherwise, `sum(x) OVER ()` would
spuriously create a group-less Aggregate node. Durable stored-query conversion
rejects the call with `0A000`, so v1 cannot persist it in a view.

### 5.2 Validation matrix

| Condition | SQLSTATE |
|---|---|
| Window call in a forbidden clause or nested window use | `42P20` `WindowingError` |
| Window call inside an aggregate argument | `42803` `GroupingError` |
| Correlated subquery in a window argument, `PARTITION BY`, or window `ORDER BY` | `0A000` `FeatureNotSupported` |
| Frame start is `UNBOUNDED FOLLOWING` | `42P20` `WindowingError` |
| Frame end is `UNBOUNDED PRECEDING` | `42P20` `WindowingError` |
| `CURRENT ROW` start with a `PRECEDING` end | `42P20` `WindowingError` |
| `FOLLOWING` start with a `PRECEDING` end | `42P20` `WindowingError` |
| `FOLLOWING` start with a `CURRENT ROW` end | `42P20` `WindowingError` |
| Offset `RANGE` frame without exactly one ordering key | `42P20` `WindowingError` |
| Unsupported offset `RANGE` ordering-key type | `0A000` `FeatureNotSupported` |
| Window-only function called without `OVER` | `42809` `WrongObjectType` |
| `OVER` on a known scalar or unknown function name | `42809` `WrongObjectType` |
| Deferred syntax that reaches a feature gate (§1.1) | `0A000` `FeatureNotSupported` |
| NULL frame offset | `22004` `NullValueNotAllowed` |
| Negative frame offset | `22013` `InvalidPrecedingOrFollowingSize` |
| Wrong function arity | `42601` `SyntaxError` |
| Wrong argument type | `42804` `DatatypeMismatch` |

`ROWS` offsets must be non-NULL, non-negative integer literals. `RANGE`
offsets must be non-NULL, non-negative literals accepted by §2.3. The binder
also derives each function's result type and nullability: ranking functions
are non-null except partition-wide NULL `ntile`; offset/value functions and
ordinary aggregates retain their semantic nullability.

## 6. Planner

The logical and physical plans each add:

```rust
Window {
    source: Box<Plan>,
    spec: BoundWindowSpec,
    functions: Vec<WindowFuncExpr>,
}
```

`WindowFuncExpr` carries the function, rewritten arguments, result type, and
nullability. The Window output schema is its source schema followed by these
columns; no schema is stored redundantly on the node. The physical lowering is
one-to-one, and row estimation passes the source estimate through unchanged.

The logical planner collects calls in first-appearance order, groups them by
structurally identical specification, structurally deduplicates identical
calls, creates the Window stack after Aggregate/HAVING, and rewrites window
uses in sort keys, distinct keys, and projection expressions to their appended
`LocalRef`s. The query-level Sort remains even when a Window operator happens
to emit a compatible ordering; v1 performs no order or sort elision.

Window planning and correlated-subquery hoisting obey a fixed-width boundary
invariant: rows entering a Window node are exactly `W0` columns wide, where
`W0` is the input width assumed when its `LocalRef` slots were rewritten.
Hoisting runs after logical planning and can otherwise violate that invariant:
a correlated `WHERE` conjunct can produce `Scan → Apply → Filter` immediately
below the Window stack, while a correlated `HAVING` conjunct can produce
`Aggregate → Apply → Filter → Window`. Because Apply appends columns, whenever
hoisting inserts an Apply beneath a Window node above the width-defining
source, it also inserts a narrowing Projection between the consuming Filter
and the Window. That Projection selects `LocalRef` slots `0..W0`, restoring the
pre-hoist width before window columns are appended. This follows the existing
`restore_dml_source_shape` precedent for reshaping Apply-widened DML sources.
As a defense against malformed plans, `WindowOp::new` validates that the source
`output_schema` width equals the expected input width and returns
`DbError::internal` on mismatch.

## 7. Executor

`WindowOp` in `crates/executor/src/ops/window.rs` uses a
sort-then-partition-tape design.

### 7.1 Sorting and partition buffering

During `open`, the operator defensively validates frame offsets, drains its
child into `ExternalSorter<SpillRow>`, and sorts by partition expressions
`ASC NULLS LAST` followed by the declared window ordering keys. Sorting reuses
the Sort operator's key comparison. The currently private `compare_keys` and
`compare_key_value` helpers in `crates/executor/src/ops/sort.rs` must therefore
be exposed, for example as `pub(crate)`, as part of M4. Drain and sort paths
poll cancellation.

During `next`, a partition-at-a-time state machine buffers one complete
partition into `SpillTape<SpillRow>`, tracks its row count, then evaluates the
functions in a second pass. The boundary and target cursors move only forward:
the current-row cursor, peer probe, lag/lead offset cursors, frame-start,
frame-last, and frame-end probes, and the growing-mode aggregate feed. Each
cursor caches at most one row, so cursor memory is O(1). Forked cursors serve
three consumers: `nth_value` lookup, sliding-mode aggregate recomputation when
the frame head moves, and per-row collect-aggregate (`array_agg` and
`string_agg`) recomputation. Each re-scan starts from a fork of the frame-start
cursor. Per-row `nth_value.n` can make its target index non-monotone, and its
lookup path is O(frame width) in the worst case.

The only required spill-crate behavior change is fork/`Clone` for
`SpillTapeReader`. Both in-memory and disk readers are a position plus shared
`Arc` state, and the disk reader already seeks for each read.

### 7.2 Function and frame evaluation

Peer-group tracking produces ranking and distribution results. `ntile` is
initialized once from the partition's first row. Constant-offset lag/lead
cursors provide delayed or advanced rows. Frame cursors implement
first/last/nth access, checked `ROWS` bounds, peer-expanded `RANGE CURRENT ROW`,
and monotone probes against direction-aware `RANGE` thresholds. Runtime errors
and NULL behavior follow §2.2 and §10.

Window aggregates reuse `AggregateState` in three modes:

- **WholePartition** computes an invariant whole-partition result once and
  caches it.
- **Growing** handles an `UNBOUNDED PRECEDING` start with one accumulator and
  per-peer-group snapshots, giving O(n) evaluation.
- **Sliding** recomputes when the frame head moves, matching PostgreSQL's
  behavior for non-invertible aggregates.

Scalar aggregate states gain a cheap `snapshot()`. Collect aggregates
(`array_agg` and `string_agg`) cannot snapshot their tapes and always recompute
per row over the frame.

### 7.3 Order, spill, and memory contract

The operator emits rows in partition keys `ASC NULLS LAST`, then declared
window order, then stable input order. `ExternalSorter`'s internal ordinal
provides stability. The planner still retains a separate query-level Sort, so
the Window emission order is not a promise about final query ordering.

The sorter, partition tape, and collect-aggregate tapes are charged to one
per-operator `SpillContext`. Cursor state and scalar aggregate state are
O(number of functions) and remain uncharged, consistent with `AggregateOp`.
Forced-spill execution must produce exactly the same rows as a large-memory
run and must report created spill files.

## 8. EXPLAIN

The physical node renders in the existing plan-tree format as:

```text
Window partition_by=[...] order_by=[...] frame=... functions=[...]
```

`partition_by` and `order_by` are empty lists when absent. `functions` follows
the node's stable first-appearance order. `frame` is omitted when the effective
frame is the PostgreSQL default; an explicit non-default frame is rendered in
normalized `ROWS|RANGE BETWEEN ... AND ...` form. `EXPLAIN ANALYZE` uses the
same node label and ordinary operator timing/row counters.

## 9. Errors

The feature adds these `SqlState` variants to `common`:

| Variant | Code | Use |
|---|---:|---|
| `WindowingError` | `42P20` | Window validation and `RANGE` key count |
| `NullValueNotAllowed` | `22004` | NULL frame offset |
| `InvalidPrecedingOrFollowingSize` | `22013` | Negative frame offset |
| `InvalidArgumentForNtile` | `22014` | Runtime nonpositive `ntile` argument |
| `InvalidArgumentForNthValue` | `22016` | Runtime `nth_value` argument less than one |

The consolidated behavior, including existing states, is:

| Condition | SQLSTATE |
|---|---|
| Forbidden-clause placement, nested window use, or frame-order violation | `42P20` `WindowingError` |
| Window call inside an aggregate argument | `42803` `GroupingError` |
| Correlated subquery in a window argument, `PARTITION BY`, or window `ORDER BY` | `0A000` `FeatureNotSupported` |
| Offset `RANGE` frame without exactly one ordering key | `42P20` `WindowingError` |
| Unsupported offset `RANGE` ordering-key type | `0A000` `FeatureNotSupported` |
| Window-only function called without `OVER` | `42809` `WrongObjectType` |
| NULL frame offset | `22004` `NullValueNotAllowed` |
| Negative frame offset | `22013` `InvalidPrecedingOrFollowingSize` |
| `ntile` argument is non-NULL and <= 0 | `22014` `InvalidArgumentForNtile` |
| `nth_value` argument is non-NULL and < 1 | `22016` `InvalidArgumentForNthValue` |
| `OVER` on a known scalar or unknown function name | `42809` `WrongObjectType` |
| Wrong function arity | `42601` `SyntaxError` |
| Wrong argument type | `42804` `DatatypeMismatch` |
| Deferred supported-parser surface | `0A000` `FeatureNotSupported` |
| `EXCLUDE` rejected by `sqlparser` 0.56 | `42601` `SyntaxError` |

Frame offset validation occurs primarily at bind time and is repeated by
`WindowOp::open` as a defense against malformed plans. The `22014` and `22016`
checks are runtime checks because their arguments are evaluated in partition
or row context.

## 10. Testing

- **planner** (`crates/planner/src/lib.rs`): positive binding for every
  function family and frame form; default-frame resolution; result types and
  nullability; the complete placement, nesting, frame-sanity, deferred-feature,
  arity, argument-type, and `RANGE` validation matrix; aggregate-in-window
  success and window-in-aggregate rejection; structural call deduplication,
  multi-spec node order, LocalRef slots, parameter substitution, plan shape,
  and EXPLAIN text.
- **executor** (`crates/executor/src/ops/window.rs` and
  `crates/executor/src/lib.rs`): partition resets, peers and ranking,
  distribution edge cases, lag/lead direction and defaults, frame boundaries,
  NULLs, empty frames, all aggregate modes, collect aggregates, output
  stability, cancellation, and open/close failure injection. Forced-spill
  tests use `MIN_WORK_MEM_BYTES`, assert `files_created > 0`, and compare rows
  with a large-`work_mem` run.
- **server integration** (`crates/server/tests/e2e_sql.rs`, tests named
  `e2e_window_*`): all supported functions and frame types through SQL;
  grouping with `sum(count(*)) OVER ()`; query ordering by a window result;
  windows in subqueries, CTEs, and derived tables; DISTINCT after window
  evaluation; multiple specifications; precise SQLSTATEs; forced spill;
  EXPLAIN and EXPLAIN ANALYZE; extended-protocol parameters; cancellation.

## 11. Milestones

- **M0 — `docs(specs): window functions design`.** Create this full design
  contract. Documentation updated: `docs/specs/window-functions.md` only.
- **M1 — `feat(parser): parse window function calls` (complete).** Add the parser AST,
  conversions, shorthand normalization, and parser rejection matrix; leave a
  binder staging guard. Documentation updated: this spec and
  `docs/specs/crates/parser.md`.
- **M2 — `feat(planner): bind and validate window function calls` (complete).** Add the
  SQLSTATEs used at bind time, bound types, function and frame validation,
  placement/nesting guards, result typing, `source_width`, all expression-
  walker arms, the durable-view rejection, and a temporary planning guard.
  Documentation updated: this spec, `docs/specs/crates/planner.md`, and
  `docs/specs/crates/common.md`.
- **M3 — `feat(planner): plan window functions` (complete).** Add logical and physical
  Window nodes, collection/deduplication and LocalRef rewriting, row-estimate
  passthrough, plan walkers, and EXPLAIN registration; execution remains a
  structured staging error. Documentation updated: this spec,
  `docs/specs/crates/planner.md`, and the architecture sections of
  `docs/specs/overview.md` affected by the new plan node.
- **M4 — `feat(executor): execute ranking and offset window functions` (complete).** Add
  the WindowOp sorting/partition skeleton and ranking, distribution, ntile,
  lag, and lead execution, including `22014`, spill, cancellation, failure,
  and `e2e_window_*` coverage; frame-respecting functions remain gated.
  Documentation updated: this spec, `docs/specs/crates/common.md`, and
  `docs/specs/crates/executor.md`.
- **M5 — `feat(executor): window frames and aggregates over windows`.** Add
  `ROWS`/`RANGE` frames, first/last/nth value, `22016`, all 13 aggregates,
  aggregate snapshots and three execution modes, and SpillTape reader forks.
  Documentation updated: this spec, `docs/specs/crates/common.md`,
  `docs/specs/crates/executor.md`, `docs/specs/crates/spill.md`, the SELECT
  subset in `docs/specs/overview.md`, `README.md`, and `AGENTS.md`.
- **M6 — `docs/test(sql): window functions polish`.** Mark this spec
  implemented; finalize the parser/planner/executor sections of
  `docs/specs/overview.md`; add EXPLAIN ANALYZE, extended-protocol, and
  cancellation integration coverage; run the production panic-policy and
  stale-documentation sweeps. Documentation updated: this spec,
  `docs/specs/overview.md`, and every stale window-function statement found in
  `README.md`, `AGENTS.md`, and `docs/specs/`.

Every implementation milestone runs focused tests followed by
`cargo fmt --all`, workspace Clippy with `-D warnings`, and
`cargo test --workspace`, and updates all affected documentation in the same
change.
