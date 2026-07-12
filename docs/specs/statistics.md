# SaguaroDB Statistics & ANALYZE Specification

**Date:** 2026-07-12
**Status:** Design — implementation planned on branch `feat/statistics`. Until a
milestone below lands, `docs/specs/overview.md` and `docs/specs/crates/*.md`
describe the current (no-statistics) behavior and remain authoritative; each
milestone updates them alongside the code.

## 1. Overview

SaguaroDB has no optimizer statistics: `ANALYZE` is accepted only as a
discarded compatibility modifier of `VACUUM`, the planner is purely rule-based,
and `pg_class.reltuples` is synthesized. This spec adds:

- A durable per-table / per-column **statistics catalog** (row counts, page
  counts, null fraction, average width, n_distinct, most-common values,
  equi-height histogram).
- A real **`ANALYZE [table]`** maintenance command (and `VACUUM ANALYZE`
  stops discarding the modifier), collecting statistics by sampling.
- **Introspection**: `pg_class.reltuples`/`relpages` backed by stored
  statistics, plus a `pg_stats` virtual view.
- **Planner consumption**, staged: cardinality/selectivity estimation with
  `rows=` estimates in `EXPLAIN`, then the first cost-based decisions
  (hash-join build side, seq-vs-index scan choice).

### Goals

- Statistics survive restart and crash recovery with the same durability
  lifecycle as other catalog state (WAL logical record + manifest snapshot).
- Zero behavior change for un-analyzed tables: without statistics the planner
  uses exactly today's rules, so nothing regresses until `ANALYZE` runs.
- The estimation seam is the groundwork for the future cost-based optimizer
  (`overview.md` §13): it slots entirely inside `physical_plan`.

### Non-Goals (v1)

- Join reordering (follow-on project; this spec only produces the estimates it
  will need).
- Widening index-scan predicate eligibility (literal `Integer`/`Text`/`Boolean`
  comparands only, single leading column — unchanged here; separate project).
- Extended statistics (multi-column correlations, expression statistics),
  physical-order correlation, per-index statistics.
- `ANALYZE <table> (columns...)` column lists; `EXPLAIN ANALYZE` (still
  rejected); `pg_statistic` raw catalog emulation (only the `pg_stats` view);
  `pg_stat_user_tables` / `last_analyze` tracking.
- A background analyze daemon. Auto-analyze piggybacks on the checkpoint like
  auto-prune (Milestone H).
- Statistics-driven plan invalidation. Prepared statements keep their cached
  plans (and old estimates) until re-prepared; `ANALYZE` does not bump
  `schema_version`.

## 2. Statistics model

New module `crates/common/src/statistics.rs` (in `common`: shared by `catalog`,
`wal`, `planner`, and `server`):

```rust
/// Distinct-value estimate for a column.
pub enum NDistinct {
    /// Absolute estimate; stable as the table grows.
    Count(u64),
    /// Fraction of `row_count` (0.0..=1.0]; the distinct count scales with
    /// the table (e.g. a unique column is Fraction(1.0)).
    Fraction(OrderedF64),
}

pub struct ColumnStatistics {
    /// Fraction of sampled rows that are NULL in this column.
    pub null_frac: OrderedF64,
    /// Mean encoded width in bytes of non-null sampled values.
    pub avg_width: u32,
    pub n_distinct: NDistinct,
    /// Most-common values with their estimated overall frequency,
    /// most frequent first. At most `statistics target` entries.
    pub most_common: Vec<(Value, OrderedF64)>,
    /// Equi-height histogram bounds over sampled non-MCV values, ascending.
    /// At most `statistics target + 1` bounds. Empty when the MCV list
    /// already covers every sampled distinct value.
    pub histogram_bounds: Vec<Value>,
}

pub struct TableStatistics {
    /// Live rows visible to the ANALYZE snapshot (exact under the v1
    /// full-scan collector).
    pub row_count: u64,
    /// Heap page count at collection time.
    pub page_count: u64,
    /// Keyed by ColumnId. BTreeMap for deterministic serialization.
    pub columns: BTreeMap<ColumnId, ColumnStatistics>,
}
```

Column ids are **dense** and are remapped by `DROP COLUMN` (ids above the
dropped column shift down, mirroring the catalog's index remapping), so
per-column statistics cannot blindly survive schema changes — after a drop
they would attach to the wrong columns, and the generic `UpdateTableSchema`
replay path cannot know which column was dropped. The catalog therefore
applies one conservative reconciliation rule in its single schema-replacement
funnel (used by both live DDL and recovery replay): statistics survive a
schema replacement only when every prior `(id, name, type)` column is
unchanged — i.e. pure `ADD COLUMN` or metadata-only updates, including table
renames. Any other column change (drop, rename) clears the per-column map but
keeps `row_count`/`page_count`; the next ANALYZE restores the rest.

All fields use `OrderedF64` (not raw `f64`) so the types keep `Eq`/`Hash` —
required because they ride inside `WalRecordKind`, which derives `Eq`. `Value`
is already `Serialize`/`Deserialize`/`Ord`, so MCVs and histogram bounds reuse
the existing catalog JSON encoding and ordering semantics.

## 3. Durable storage

Statistics are catalog state, keyed off the schema objects but versioned
independently of them:

- `CatalogSnapshot` gains `#[serde(default)] statistics: HashMap<TableId,
  TableStatistics>` — the same backward-compatible evolution used when views,
  secondary indexes, sequences, and dictionaries were added
  (`crates/catalog/src/memory.rs`). Old manifests deserialize with empty
  statistics; no manifest (`SGMF`) version bump.
- Catalog API: `get_table_statistics(TableId)` and
  `set_table_statistics(TableId, TableStatistics)` (set validates the table is
  a live user relation and every statistics column id exists). Schema-change
  behavior follows the reconciliation rule above (§2): `ADD COLUMN` and
  renames of the table keep statistics; `DROP COLUMN` and `RENAME COLUMN`
  clear the per-column map (counts stay); `DROP TABLE` removes the entry.
  Snapshot load prunes orphan statistics rather than rejecting them —
  advisory data must never block startup.
- **`schema_version` is NOT bumped** by a statistics update. Cached prepared
  plans stay valid and keep their old estimates until re-prepare — an
  intentional, documented divergence from PostgreSQL's relcache invalidation.
- `TRUNCATE` (including transactional TRUNCATE) leaves statistics untouched.
  They go stale until the next ANALYZE, and the generation-undo rollback path
  never has to restore them.

## 4. WAL record and recovery

New logical record, mirroring the DDL record lifecycle:

```rust
/// Maintenance: replaces a user table's optimizer statistics.
/// CLOG-gated on replay like DDL: applied only for committed transactions.
UpdateTableStatistics {
    table_id: TableId,
    statistics: TableStatistics,
},
```

- Classified as a redo operation (`is_redo_operation` → true).
- Recovery applies committed records to the catalog in LSN order (last write
  wins); the storage engine ignores it. A skipped uncommitted/aborted record
  needs **no id or storage reservation** (unlike `CreateTable`) — it allocates
  nothing.
- If the record's `table_id` no longer resolves at apply time (table dropped
  later in the log), the record is skipped.
- Normal durability lifecycle: the record is appended before the in-memory
  catalog is updated (WAL-before-state, same ordering as DDL), the maintenance
  transaction's `Commit` is flushed before success is reported, and the next
  checkpoint's manifest snapshot absorbs the statistics so WAL truncation is
  safe.
- The WAL codec itself refuses to encode a non-finite payload
  (`TableStatistics::is_finite`), so the append fails cleanly instead of
  poisoning the log. Record decode happens for every retained record
  regardless of its transaction's outcome, so a non-finite payload (which
  JSON writes as `null` and cannot read back) would otherwise break replay of
  the whole log even if the transaction aborted. `set_table_statistics`
  enforces the same rule at the catalog/manifest boundary.

## 5. Collection: the ANALYZE pass

`ANALYZE` is a **maintenance command** (`StatementClass::Maintenance`): not
bound or planned, rejected inside an explicit transaction block with the
shared maintenance rule (`SqlState::FeatureNotSupported`, "maintenance
commands cannot run inside a transaction block", matching VACUUM — a
documented divergence from PostgreSQL, which allows ANALYZE in a transaction;
our catalog mutations are non-transactional). Orchestration lives in
`crates/server/src/query/analyze.rs` (`run_analyze_pass`) and follows
`run_vacuum`'s shape (`crates/server/src/query/vacuum.rs`):

1. Resolve targets under the catalog publication gate: one named user table,
   or every `RelationKind::User` table sorted by id. Hidden TOAST relations
   and views are never analyzed.
2. Allocate a maintenance transaction id, take the SHARED writer guard
   (`begin_writer_cancelable`) and the transaction-owned object-lock guard.
3. Acquire **`AccessShare`** on every target with the same
   acquire-many + revalidate loop VACUUM uses. `AccessShare` (not `Share`):
   the pass only reads the heap, so concurrent writers keep flowing; the
   statistics write at the end is a short catalog-gate critical section.
   Two concurrent ANALYZEs of one table are benign (both compute valid
   statistics; last committed LSN wins).
4. Capture an MVCC snapshot registered like any reader (advertising its
   `xmin`, so a concurrent VACUUM's GC horizon respects the scan).
5. Per table: full heap scan of snapshot-visible rows through the storage
   engine's **streaming** row pass (one row materialized/detoasted at a time —
   never the whole table), maintaining an exact live row count and a
   **reservoir sample** (Algorithm R) of `300 × statistics_target` rows,
   width-capped per §6's wide-value rule. Page count from the heap extent.
   Cancellation is checked per leaf page by the streaming pass;
   `statement_timeout` applies.
6. Compute per-column statistics from the sample (§6).
7. Under the catalog publication gate: append one `UpdateTableStatistics`
   record per table, update the in-memory catalog.
8. Append `Commit`, flush the WAL, run the standard
   post-durable-commit cleanup. One transaction covers all targets: a crash
   mid-pass applies none of them.

Randomness: seed from `getrandom` (already a server dependency) into a small
deterministic PRNG (xorshift/splitmix64, no new crate dependency); tests
inject a fixed seed.

The v1 collector is a **full scan** (exact `row_count` as a byproduct;
consistent with VACUUM's full-extent pass). Two-stage page sampling is a
future optimization behind the same interface.

## 6. Estimators

With whole-sample size `s` (all sampled rows, nulls included), retained
non-null (narrow) sampled count `n`, distinct-in-narrow-sample `d`, narrow
values appearing exactly once `f1`, and total live rows `N`:

- `null_frac` = nulls in sample / `s`; `avg_width` = mean approximate encoded
  width of non-null sampled values (payload length for TEXT/BYTEA, storage
  size for fixed-width kinds; wide values counted by width; informational
  only).
- **Wide values**: a non-null sampled value wider than 1 KiB (PostgreSQL's
  `WIDTH_THRESHOLD` analog) is not retained in the sample — only its width
  is. It counts toward `null_frac` (as non-null) and `avg_width`, but is
  excluded from the MCV list, histogram bounds, and the n_distinct estimator.
  This bounds ANALYZE memory on TOAST-heavy tables: without it a sample of
  `300 × target` fully materialized megabyte documents would be held in
  memory at once.
- `n_distinct` (over the narrow values):
  - no narrow sampled values (`n == 0`) → `Count(0)`;
  - every narrow value distinct (`d == n`) → `Fraction(1.0)` (unique-ish);
  - `f1 == 0` (every value repeated — sample likely saw them all) →
    `Count(d)`;
  - otherwise the Haas–Stokes estimator
    `D̂ = d · n / (n − f1 + f1 · n / N)`; stored as `Fraction(D̂ / N)` when
    `D̂ > 0.1 × N` (scales with growth), else `Count(D̂)`.
- **MCV list**: up to `statistics_target` narrow sampled values that occur
  more than once, most frequent first, each with frequency `count / s` — an
  **overall** fraction of the whole sample (nulls included), so
  `Σ mcv_freqs + null_frac ≤ 1` and the §9.1 equality-miss formula stays
  non-negative. Skipped when the column looks unique.
- **Histogram**: equi-height bounds over the sampled values not in the MCV
  list — `statistics_target + 1` bounds from min to max (fewer when fewer
  values remain, never fewer than two — a single leftover value stores no
  histogram). Omitted when the MCV list covers every sampled distinct value.
- **Non-finite values**: a sampled `DOUBLE PRECISION`/`REAL` value that is NaN
  or ±Infinity is excluded from the MCV list and histogram bounds (it still
  counts toward `n_distinct` and the width/null fractions). The catalog's JSON
  manifest payload cannot round-trip non-finite floats — serde_json writes
  them as `null` and fails to read that back — so the catalog rejects
  non-finite statistics at the durable boundary
  (`set_table_statistics`), and the collector must never produce them.

`statistics_target` comes from a new integer session GUC
`default_statistics_target` (default `100`, range `1..=1000`), registered in
the existing GUC table and readable via `SHOW`/`current_setting`.

## 7. SQL surface

- **`ANALYZE`** and **`ANALYZE <table>`**: new `Statement::Analyze { table:
  Option<String> }`, intercepted before sqlparser like VACUUM. The intercept
  fires only on a statement-initial `ANALYZE` token; `EXPLAIN ANALYZE` remains
  a parse error. Command tag: `ANALYZE` (added to the protocol
  `CommandComplete` tag list).
- **`VACUUM ANALYZE [<table>]`**: `Statement::Vacuum` gains `analyze: bool`.
  The reclamation pass runs first (unchanged), then the ANALYZE pass over the
  same targets. Tag remains `VACUUM`. This intentionally changes the
  documented "ANALYZE is discarded" behavior — `overview.md`, the crate
  specs, and the repository instruction files that restate the SQL subset
  (`CLAUDE.md`/`AGENTS.md`) are updated in the same milestone.
- Extended protocol: maintenance statements already flow through
  `run_prepared_maintenance` carrying the raw parsed statement; ANALYZE gets
  this for free.
- Rejected inside a transaction block; allowed in autocommit only, like all
  maintenance commands.

## 8. Introspection

- `pg_class`: `reltuples` and `relpages` report stored statistics when present
  (`row_count`, `page_count`); the current synthesized values remain the
  fallback for never-analyzed relations.
- New `pg_stats` virtual view (static registry + `SystemScan`, like the other
  system views), columns: `schemaname` (`public`), `tablename`, `attname`,
  `null_frac`, `avg_width`, `n_distinct` (PostgreSQL convention: positive
  count, or negative fraction for `Fraction(f)` → `-f`), `most_common_vals`,
  `most_common_freqs`, `histogram_bounds` (rendered as PostgreSQL-style
  `{...}` array text — the text-backed-array precedent already exists for
  `int2vector`/`oidvector`), `correlation` (always NULL in v1).

## 9. Planner consumption

Statistics reach the planner through the catalog it already depends on; the
consumption seam sits entirely inside `physical_plan`, matching the
cost-based-optimizer slot reserved in `overview.md` §13.

### 9.1 Cardinality & selectivity estimation (Milestone F)

A new `planner::estimate` module computes an estimated output row count for
every physical plan node:

- Base relations: `row_count` from statistics; a fixed default for
  un-analyzed tables (assume PostgreSQL-style 10 pages / ~1000 rows rather
  than 0, so empty-stats behavior is neutral).
- Filter selectivity on `col op literal`:
  - `=`: MCV hit → its frequency; miss → `(1 − Σ mcv_freqs − null_frac) /
    max(n_distinct − |mcv|, 1)`. No statistics → `0.005`.
  - `<`, `<=`, `>`, `>=`: sum of qualifying MCV frequencies + histogram
    fraction (linear interpolation inside the boundary bucket). No
    statistics → `1/3`.
  - `IS NULL` → `null_frac`; `IS NOT NULL` → `1 − null_frac`.
  - `AND` multiplies, `OR` is `a + b − ab`, `NOT` complements; everything
    clamps to `[0, 1]` (independence assumption).
  - Anything else (LIKE, expressions, subquery results): fixed defaults.
- Join output: equi-join selectivity `1 / max(nd_left, nd_right)`; cross join
  multiplies; semi/anti and outer joins get simple bounded rules.
- Aggregate/Distinct output: n_distinct of the grouping keys when available,
  capped by input estimate.

`EXPLAIN` appends ` (rows=N)` to every plan node line. This changes visible
EXPLAIN output; the format is specified in `planner.md` and existing tests are
updated in the same milestone.

### 9.2 First cost-based decisions (Milestone G)

Constants (v1 `const`s, GUCs later): `seq_page_cost = 1.0`,
`random_page_cost = 4.0`, `cpu_tuple_cost = 0.01`,
`cpu_operator_cost = 0.0025`.

- **Hash-join build side**: today `HashJoinOp` always builds the right input.
  When both inputs have estimates, the planner swaps an **inner** hash join's
  children so the smaller estimated input is the build side, wrapping the
  join in a projection that restores the original output column order.
  Semi/anti joins (asymmetric) and joins without estimates are not swapped.
- **Seq-vs-index choice**: today an eligible predicate always chooses
  `IndexScan`. With statistics:
  `cost(seq) = pages·seq_page_cost + rows·cpu_tuple_cost`;
  `cost(index) = matches·(random_page_cost + cpu_tuple_cost) + descent`.
  The cheaper wins. **Without statistics the current always-index rule is
  preserved unchanged** — un-analyzed databases plan exactly as today.

Index-predicate *eligibility* (literal-only comparands, single column) is
explicitly unchanged by this project.

## 10. Auto-analyze (Milestone H)

Mirrors checkpoint auto-prune:

- `ServerComponents` gains `rows_changed_since_analyze`, incremented on the
  commit path by each committed INSERT/UPDATE/DELETE/COPY row (same hook
  points as `dead_rows_since_vacuum`).
- New startup option `--auto-analyze-changed-rows <n>` (default `10000`,
  `0` disables).
- `run_checkpoint` gains a step after auto-prune: when the counter exceeds
  the threshold, run the ANALYZE pass over all user tables under the already
  held exclusive checkpoint guard, then reset the counter. Uses the built-in
  default statistics target (no session).

Like auto-prune, this fires only on the commit path — a known limitation
shared with checkpointing itself, resolved by a future background scheduler.

## 11. Interactions and invariants

- **VACUUM**: the ANALYZE snapshot advertises `xmin`, so concurrent VACUUM
  cannot reclaim versions the scan still needs. `VACUUM ANALYZE` runs the
  passes sequentially in that order.
- **SSI**: ANALYZE performs no SIREAD tracking (maintenance, not a user
  query); it can neither cause nor suffer serialization aborts.
- **DDL**: `AccessShare` on targets blocks `AccessExclusive` DDL for the
  duration of the pass (and vice versa); the revalidate loop handles catalog
  changes between resolution and lock grant, exactly like VACUUM.
- **Deadlock**: lock acquisition goes through the shared object-lock manager
  and participates in the existing wait-for graph and timeout detector.
- **Durable-format conservatism**: statistics ride in the catalog JSON blob
  (serde-default field) and a new WAL record variant appended at the end of
  `WalRecordKind` — both established, backward-compatible evolution patterns.
  `Value` variant order is untouched.

## 12. Milestones

Each milestone is a reviewable commit (or small commit series) with its own
tests, spec updates, and a green `cargo fmt` / `clippy -D warnings` /
`cargo test --workspace`.

- **A — Statistics types + catalog storage.** `common::statistics`,
  `CatalogSnapshot.statistics` (serde-default), get/set/removal hooks wired to
  DROP TABLE / DROP COLUMN, serialization round-trip + old-manifest
  compatibility tests. No SQL surface.
- **B — WAL record + recovery.** `UpdateTableStatistics`, recovery apply
  (committed-only, LSN order, dropped-table skip), replay + checkpoint
  round-trip tests at the wal/server level.
- **C — Collector.** Reservoir sampling over a snapshot-visible heap scan,
  estimator math (§6) as pure functions, deterministic-seed unit tests on
  synthetic distributions (uniform, skewed, unique, all-null, mostly-null).
- **D — SQL surface.** Parser intercept for `ANALYZE [table]`,
  `Vacuum.analyze` flag, `run_analyze` orchestration (§5),
  `default_statistics_target` GUC, command tag, txn-block rejection, e2e
  tests: analyze → inspect → restart → statistics survive; crash before
  commit → no statistics; `VACUUM ANALYZE` end-to-end. Spec + instruction
  files updated for the semantics change.
- **E — Introspection.** `pg_class.reltuples`/`relpages` from statistics,
  `pg_stats` view, psql-visible smoke test.
- **F — Estimation + EXPLAIN.** `planner::estimate`, selectivity rules,
  `rows=` in EXPLAIN, existing EXPLAIN tests updated. No plan-shape changes.
- **G — First cost decisions.** Hash-join build-side swap; seq-vs-index cost
  choice with the no-stats fallback preserving today's plans. EXPLAIN-based
  plan-choice tests over analyzed vs un-analyzed tables.
- **H — Auto-analyze.** Counter, flag, checkpoint hook, threshold tests.

Follow-on projects (out of scope, enabled by this): greedy join reordering;
index-eligibility widening (parameters, more types, composite ranges); page
sampling; cost GUCs; extended statistics.
