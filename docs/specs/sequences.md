# SaguaroDB Sequences and SERIAL Specification

**Date:** 2026-06-29
**Status:** Draft

## 1. Overview

Sequences are named, durable, monotonic number generators. They are the
mechanism behind auto-incrementing keys: the `SERIAL` pseudo-type is sugar for
an `INTEGER` column whose `DEFAULT` calls `nextval` on an owned sequence. This
spec adds three things that build on each other:

1. **Column `DEFAULT`** — a general, durable per-column default value applied
   when a column is omitted on `INSERT`/`COPY` or written as the `DEFAULT`
   keyword. Bounded for v1 to constant literals and `nextval(...)` calls.
2. **Sequences** — a new catalog object (`CREATE SEQUENCE` / `DROP SEQUENCE`)
   with WAL-logged advancement and crash recovery, plus the `nextval`,
   `currval`, and `setval` functions.
3. **`SERIAL`** — `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` column types that desugar
   into `INTEGER NOT NULL DEFAULT nextval(<owned sequence>)`.

There is **no new `Value` or `DataType` variant**. Sequence values are `i64`
(`INTEGER`, PostgreSQL OID 20), and `SERIAL` columns are stored as `INTEGER`.
Consequently this feature touches no row/page encoding, no wire-protocol OID
mapping, and no key codec — the surface is catalog metadata, WAL records, the
binder/executor, and server wiring.

### Supported

- `CREATE SEQUENCE <name> [INCREMENT [BY] n] [START [WITH] n]
  [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE]
  [CACHE n] [[NO] CYCLE]` — `CACHE` is parsed and ignored (a no-op under the
  no-cache durability model).
- `DROP SEQUENCE [IF EXISTS] <name>`.
- `nextval('<name>')` — advance and return the next value (BIGINT). Side
  effecting and **non-transactional**: its advance is never rolled back.
- `currval('<name>')` — return the value most recently produced by `nextval`
  for `<name>` **in the current session**. Errors if `nextval` has not been
  called for `<name>` this session.
- `setval('<name>', n [, is_called])` — reposition the sequence. Non-transactional.
- Column `DEFAULT <expr>` in `CREATE TABLE` (literal or `nextval(...)`).
- `DEFAULT` keyword in `INSERT ... VALUES` and omitted-column defaulting in
  `INSERT` and `COPY ... FROM`.
- `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` (and `SERIAL2`/`SERIAL4`/`SERIAL8`)
  column types in `CREATE TABLE`.

### Out of scope (v1 deferrals)

- `lastval()` (no cross-sequence "last touched" session tracking).
- A functional `CACHE` (per-session value pre-allocation) — parsed, ignored.
- `ALTER SEQUENCE`, `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT`, identity
  columns (`GENERATED ... AS IDENTITY`).
- `OWNED BY` clause on `CREATE SEQUENCE`; sequence ownership is established only
  implicitly by `SERIAL`.
- General `DEFAULT` expressions beyond literals and `nextval(...)` (e.g.
  arbitrary arithmetic, `now()`); the binder rejects them for now.
- `CREATE SEQUENCE AS <type>` data-type clause; sequences are always `i64`.

## 2. Decisions

These were settled during design and drive the rest of the spec.

1. **General column `DEFAULT`, not a SERIAL-only attribute.** SERIAL desugars to
   a real, visible, overridable default. The catalog stores a bounded
   `ColumnDefault` enum so default storage stays serializable and simple.
2. **WAL-logged advancement, no cache.** Each `nextval`/`setval` appends a
   logical WAL record. No extra fsync per call — the record rides the
   surrounding commit's WAL flush, so any value a committed transaction relied
   on is durable and never reissued. Checkpoint-only durability was rejected
   because it would reissue values after a crash (duplicate `SERIAL` keys).
3. **Function set: `nextval` + `currval` + `setval`.** `currval` requires new
   per-connection session state; `lastval` is deferred.
4. **`CACHE` is a no-op.** Behavioral options (`INCREMENT`, `START`, `MINVALUE`,
   `MAXVALUE`, `CYCLE`) are honored; `CACHE` is accepted and ignored.

## 3. Column DEFAULT

### 3.1 Types (`common`)

```rust
/// A column default. Bounded for v1 to a constant literal or a nextval call.
pub enum ColumnDefault {
    Literal(Value),
    Nextval(SequenceId),
}
```

`ColumnDefault` is added as `Option<ColumnDefault>` to both `ParsedColumnDef`
and `ColumnDef` (`common::schema`). It is `serde`-serializable so it round-trips
through the catalog manifest snapshot.

**Crate boundary.** `common` is the leaf crate and cannot depend on `parser`, so
the unbound default expression cannot live on `common::ParsedColumnDef` (which
the parser AST reuses directly in `Statement::CreateTable`). Instead the parser
AST carries the raw default in a **parser-side column wrapper**:

```rust
// parser::ast
pub struct ColumnDefAst {
    pub column: ParsedColumnDef,   // common type: name, data_type, nullable
    pub default: Option<Expr>,     // unbound parser Expr (None for SERIAL)
    pub serial: bool,              // SERIAL/BIGSERIAL/... => binder owns desugaring
}
// Statement::CreateTable { name, columns: Vec<ColumnDefAst>, primary_key }
```

The binder lowers each `ColumnDefAst` into a `common::ParsedColumnDef` with its
`default: Option<ColumnDefault>` filled in (explicit `nextval('existing')` is
resolved to a concrete `SequenceId` here; constant literals become
`ColumnDefault::Literal`). The catalog then assigns ids to produce the stored
`ColumnDef`, which carries the same `ColumnDefault`. SERIAL's implicit default is
finalized at execution time (§6), because the owned sequence's id is not
allocated until the `CREATE TABLE` runs.

### 3.2 Parser

- `Statement::CreateTable.columns` becomes `Vec<ColumnDefAst>` (§3.1) so a column
  can carry its unbound default expression and SERIAL flag.
- New column option: `DEFAULT <expr>` in a `CREATE TABLE` column definition,
  alongside the existing `NULL`/`NOT NULL`/`PRIMARY KEY` handling
  (`parser/src/convert/ddl.rs`).
- `SERIAL` family column types (§6): recognized in `convert/ddl.rs`; the parser
  emits `column.data_type = Integer`, `nullable = false`, and `serial = true`.
- New `INSERT` value form: the `DEFAULT` keyword as a value in
  `INSERT ... VALUES (...)`. Represented as a new `Expr::Default` marker (only
  valid in an `INSERT` value position; rejected elsewhere by the binder).

### 3.3 Binder

- When binding `CREATE TABLE`, lower each column's optional default `Expr`:
  - A constant literal → `ColumnDefault::Literal(value)`, type-checked against
    the column type (no implicit casts; `NULL` only if the column is nullable).
  - `nextval('<name>')` → resolve `<name>` to a `SequenceId` →
    `ColumnDefault::Nextval(id)`.
  - Anything else → bind error (`FeatureNotSupported`).
- Relax the omitted-column rule: `validate_insert_omissions`
  (`planner/src/binder/dml.rs`) must **not** reject an omitted `NOT NULL` column
  that has a default. The default supplies the value.
- Bind the `DEFAULT` keyword in `VALUES`: each `Expr::Default` is replaced by
  the target column's bound default, or a bind error if the column has none.

### 3.4 Executor

- `map_and_insert_row` (`executor/src/query.rs`) currently fills omitted columns
  with `Value::Null`. It changes to: for an omitted column, apply its
  `ColumnDefault` if present (evaluating `Nextval` against the sequence manager),
  else `NULL` (or the existing NOT-NULL error path for non-defaulted NOT NULL
  columns — which the binder already enforces).
- `COPY ... FROM` (`executor/src/copy.rs`) applies the same defaulting for
  columns absent from the COPY column list, for consistency with `INSERT`.

## 4. Sequences

### 4.1 Catalog object (`common` + `catalog`)

```rust
pub type SequenceId = u32; // own allocator, high-water-marked like TableId

pub struct SequenceSchema {
    pub id: SequenceId,
    pub name: String,
    pub increment: i64,        // non-zero; negative => descending
    pub min_value: i64,
    pub max_value: i64,
    pub start: i64,
    pub cycle: bool,
    /// True if this sequence was created implicitly by a SERIAL column (so DROP
    /// TABLE cascade-drops it and DROP SEQUENCE on it errors). False for a
    /// standalone CREATE SEQUENCE. The table↔sequence link is NOT stored here as
    /// a back-reference (that would create a mutual id dependency with the
    /// column default); it is discovered through the owning column's
    /// `ColumnDefault::Nextval(this_id)` (§6).
    pub owned: bool,
    /// Checkpoint baseline of the runtime value. Live advancement happens in the
    /// SequenceManager; this field is the value serialized at checkpoint and the
    /// starting point recovery fast-forwards from.
    pub last_value: i64,
    pub is_called: bool,
}
```

The `catalog` crate gains a sequence map paralleling its table/index maps:
`create_sequence`, `drop_sequence`, `get_sequence_by_name`,
`apply_create_sequence`/`apply_drop_sequence` (recovery-only), and
`reserve_sequence_id` (advance the allocator high-water mark without creating an
object), mirroring the existing `reserve_table_id`/`reserve_index_id`.

The manifest catalog snapshot (serialized as JSON into the control record) gains
a `sequences` field. This is an additive change; the catalog-snapshot version is
bumped and old snapshots deserialize with an empty sequence set
(`#[serde(default)]`).

### 4.2 Runtime: `SequenceManager` (`storage`)

The live current value lives in a `SequenceManager` owned by the storage engine
(which already owns WAL append and the recovery replay loop). Per sequence it
holds `(last_value, is_called)` behind a per-sequence lock so concurrent writers
(the shared writer guard permits parallel DML) get unique values.

```text
nextval(id):
    lock sequence id
    compute next from (last_value, is_called, increment, min, max, cycle):
        if !is_called: next = start          // first nextval returns START WITH
        else:          next = last_value + increment, bounded by min/max
        if next overflows min/max:
            if cycle: wrap to max/min (per increment sign)
            else:     error (sequence exhausted)
    set last_value = next, is_called = true
    append WAL: SequenceAdvance { id, value: next }   (no fsync here)
    unlock
    return next

setval(id, n, is_called):
    lock; set last_value = n, is_called; append WAL SetSequenceValue { id, n, is_called }; unlock
```

The manager exposes the current `(last_value, is_called)` for every sequence to
the checkpoint path, which writes them into the catalog snapshot baseline.

### 4.3 WAL records (`wal`)

Four new logical record kinds, encoded like the existing logical DDL records
(JSON payloads):

```rust
CreateSequence { schema: SequenceSchema }     // CLOG-gated (txn-scoped, like DDL)
DropSequence   { sequence: SequenceId }       // CLOG-gated
SequenceAdvance { sequence: SequenceId, value: i64 }       // UNCONDITIONAL replay
SetSequenceValue { sequence: SequenceId, value: i64, is_called: bool } // UNCONDITIONAL
```

**Replay gating is the crux of gap semantics.** `CreateSequence`/`DropSequence`
are part of a transaction (a `CREATE SEQUENCE`, or the `CREATE TABLE` that
desugars `SERIAL`); they are replayed only if that transaction committed, exactly
like `CreateTable`/`DropTable`. `SequenceAdvance`/`SetSequenceValue` are
**non-transactional**: recovery applies them regardless of the surrounding
transaction's commit/abort, fast-forwarding each sequence to the maximum
advanced value (and the last `setval`). This is what guarantees a value handed
out before a crash is never reissued, even if its transaction aborted.

As with all storage operations, these records are appended only in normal
operation; recovery never appends WAL (`docs/specs/crates/wal.md`).

### 4.4 Durability and recovery flow

- **Steady state:** `nextval` advances in memory and appends `SequenceAdvance`.
  No per-call fsync. When the enclosing statement commits (autocommit or
  explicit), the server's existing `append_and_flush_commit` flushes the WAL,
  making every advance ordered before that commit durable.
- **Checkpoint:** `run_checkpoint` (`server/src/checkpoint.rs`) already
  serializes the catalog under the exclusive guard. It additionally pulls each
  sequence's current `(last_value, is_called)` from the `SequenceManager` into
  the snapshot baseline before writing the control record, and WAL truncation
  proceeds as today.
- **Recovery:** load sequence definitions + baseline values from the manifest;
  reserve sequence ids; replay post-checkpoint WAL — install committed
  `CreateSequence`, remove committed `DropSequence`, and unconditionally
  fast-forward each surviving sequence past every `SequenceAdvance` / last
  `SetSequenceValue`. No value is ever reissued.

## 5. Functions: nextval / currval / setval

These are scalar functions returning BIGINT. The sequence-name argument is a
string literal resolved to a `SequenceId` at bind time (unknown name →
`UndefinedTable`/`42P01`, which PostgreSQL also uses for sequences).

- **`nextval`/`setval` are writes.** The binder marks any statement whose bound
  tree contains a `nextval`/`setval` call with a `mutates_sequences` flag. The
  server uses this to route even a `SELECT nextval('s')` through the write path
  (`autocommit_write`: writer guard + txn id + WAL commit flush), instead of the
  read path. Inside an explicit transaction they run on the existing write path;
  their advances are not rolled back on `ROLLBACK`.
- **`currval` is a pure read** of per-connection session state — no guard, no
  WAL. It reads the session's "last value produced by `nextval` for this
  sequence" map; missing entry → object-not-in-prerequisite-state error.

### Session state and executor threading

A per-connection `SessionSequenceState` (a `SequenceId -> i64` map) is added to
the server session. Expression evaluation in the executor is given two new
handles: a shared `&dyn SequenceManager` (for `nextval`/`setval`) and a mutable
`&mut SessionSequenceState` (updated by `nextval`, read by `currval`). This is
the one place the otherwise-pure expression evaluator gains side effects; it is
confined to these three functions.

`nextval`/`setval`/`currval` are permitted in projection lists, `INSERT`/`COPY`
values, `UPDATE` SET expressions, and column `DEFAULT`s. Their evaluation count
and order in a `WHERE` clause are unspecified (volatile), matching PostgreSQL.

## 6. SERIAL

`SERIAL`/`BIGSERIAL`/`SMALLSERIAL` (and `SERIAL2`/`SERIAL4`/`SERIAL8`) are valid
only as a column type in a `CREATE TABLE` column definition. All map to the same
`INTEGER` (i64) storage — width is not enforced, consistent with the existing
integer aliases.

Desugaring is split across bind and execute because the owned sequence's
`SequenceId` is not allocated until the statement runs. The dependency is now
one-way — the table's column default needs the sequence id, but the sequence
stores no table reference (`owned: bool` only, §4.1) — so the order is simply
"sequences first, then table," with no placeholder to patch:

- **Binder:** for each `serial` column, validate it is a top-level `CREATE TABLE`
  column, choose the sequence name `<table>_<column>_seq` (append the smallest
  free numeric suffix if taken), and set the column to `INTEGER` + `NOT NULL`.
  Record a pending owned-sequence request in `BoundStatement::CreateTable`. The
  column's `ColumnDefault` is left to be finalized at execution.
- **Executor (`execute_create_table`), in order, one autocommit transaction:**
  1. For each pending request, `catalog.create_sequence(...)` (increment 1,
     start 1, min 1, max `i64::MAX`, no cycle, `owned: true`) → `SequenceId`,
     appending a `CreateSequence` WAL record. The record is final — it carries no
     table id.
  2. Fill each SERIAL column's `ColumnDefault::Nextval(seq_id)`.
  3. `catalog.create_table(...)` with the finalized defaults.

Because the sequences and table are created in the same autocommit transaction,
they share its commit/abort outcome — if `CREATE TABLE` aborts, neither exists
(the `CreateSequence` records are CLOG-gated to the same txn).

**Ownership rules:**

- `DROP TABLE t` cascade-drops its owned sequences: for each column of `t` whose
  default is `ColumnDefault::Nextval(s)` with `catalog.sequence(s).owned`, emit a
  `DropSequence` record alongside `DropTable` in the same transaction.
- `DROP SEQUENCE s` where `s.owned` → dependency error. To name the owning table
  the catalog scans tables for the column whose default is `Nextval(s)` (rare op;
  a linear scan is fine). `IF EXISTS` suppresses the not-found case, not this one.

## 7. Server dispatch

- `CREATE SEQUENCE` / `DROP SEQUENCE` are classified `StatementClass::Ddl`:
  autocommit-only, take the shared writer guard, append their logical WAL
  records, and are **rejected inside an explicit transaction block** by the
  existing DDL-in-block path.
- Statements flagged `mutates_sequences` are routed to the write path even when
  they are syntactically `SELECT`.
- `currval` adds no routing change (pure read).

## 8. Error handling

| Condition | SQLSTATE |
|---|---|
| `nextval`/`currval`/`setval` on unknown sequence | `42P01` UndefinedTable |
| `nextval` past MAXVALUE / below MINVALUE, `NO CYCLE` | sequence-exhausted (`2200H`) / `NumericValueOutOfRange` |
| `currval` before `nextval` in this session | object-not-in-prerequisite-state (`55000`) |
| `SERIAL` outside a `CREATE TABLE` column def | bind error (`FeatureNotSupported`/`SyntaxError`) |
| `DEFAULT` expr beyond literal/`nextval` | `FeatureNotSupported` |
| `DEFAULT nextval('missing')` at `CREATE TABLE` | `42P01` UndefinedTable |
| `CREATE`/`DROP SEQUENCE` inside a txn block | existing DDL-in-block error |
| `DROP SEQUENCE` of a column-owned sequence | dependency error (`2BP01`-style) |
| `INCREMENT BY 0`, or `MINVALUE > MAXVALUE`, etc. | invalid sequence definition (`22023`) |

## 9. Testing

- **parser**: sequence grammar + every option, `SERIAL` family, `DEFAULT`
  column option and `DEFAULT` value keyword, rejection cases.
- **catalog**: create/drop, id allocation high-water behavior, snapshot
  round-trip including sequences + baseline values, old-snapshot compatibility.
- **storage / recovery**: crash after N `nextval`s → no value reissued; an
  aborted transaction's `nextval` keeps the gap; `setval` then crash; `CYCLE`
  wrap and `NO CYCLE` exhaustion; concurrent `nextval` from parallel writers
  yields all-unique values.
- **server integration**: `SERIAL` end-to-end insert and id read-back via
  `currval`; `setval` repositioning; `DROP TABLE` cascade-drops the owned
  sequence; `DROP SEQUENCE` of an owned sequence errors; `SELECT nextval(...)`
  routes through the write path and is durable; DDL-in-transaction rejection of
  `CREATE`/`DROP SEQUENCE`.

## 10. Build order

A single implementation plan, sequenced as independently testable commits:

1. **Column `DEFAULT` (literals only).** Parser `DEFAULT` option + `DEFAULT`
   value keyword, `ColumnDefault` in `common`, catalog storage + manifest
   round-trip, binder omitted-column relaxation, executor application. No
   sequences yet — defaults restricted to literals.
2. **Sequence catalog object + DDL.** `SequenceId`/`SequenceSchema`, catalog
   map + allocator + recovery hooks, manifest field, `CreateSequence`/
   `DropSequence` WAL records, `CREATE`/`DROP SEQUENCE` parsing and server DDL
   dispatch, recovery replay. No functions yet.
3. **Functions + runtime.** `SequenceManager` with WAL-logged advance/setval and
   recovery fast-forward, `nextval`/`currval`/`setval` evaluation, session
   state + executor threading, write-path routing for sequence-mutating
   statements.
4. **`SERIAL` desugaring + ownership.** `SERIAL` family parsing, desugar to
   sequence + `ColumnDefault::Nextval`, owned-sequence cascade on `DROP TABLE`,
   `DROP SEQUENCE` ownership guard.
5. **Consistency + specs.** `COPY ... FROM` default application, update
   `docs/specs/overview.md` and the affected `docs/specs/crates/*.md`, and a
   full `cargo fmt` / `clippy` / `test` sweep.

## 11. Spec impact

When implemented, update in the same change:

- `docs/specs/overview.md` — data types (note `SERIAL` family → `INTEGER`), SQL
  subset (`CREATE`/`DROP SEQUENCE`, `DEFAULT`, sequence functions), and the
  `Value`/`DataType` discussion (unchanged, but defaults are new).
- `docs/specs/crates/parser.md` — sequence grammar, `DEFAULT`, `SERIAL`.
- `docs/specs/crates/catalog.md` — sequence object, allocator, snapshot field.
- `docs/specs/crates/wal.md` — the four new record kinds and their replay gating.
- `docs/specs/crates/storage.md` — `SequenceManager`, recovery fast-forward.
- `docs/specs/crates/executor.md` — sequence functions and default application.
- `docs/specs/crates/server.md` — DDL dispatch, write-path routing, session
  state, checkpoint baseline.
