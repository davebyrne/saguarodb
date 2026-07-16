# SaguaroDB Sequences and SERIAL Specification

**Date:** 2026-07-10
**Status:** Implemented feature spec (revised after rebase onto
expression `DEFAULT`, RETURNING, `ON CONFLICT`, and SSI work)

> **Revision note.** This spec was first drafted against a base where column
> `DEFAULT` did not exist. The branch has since gained **column defaults**
> (`ColumnDef.default: Option<ColumnDefault>`, including constants, `nextval`,
> and expression defaults), `RETURNING`, `INSERT ... ON CONFLICT`,
> composite/`UNIQUE` keys, and real **SSI** (`SERIALIZABLE` is now its own
> isolation level). The plan below is retained as implementation history and
> updated to describe the current contract; §12 records the interactions.

## 1. Overview

Sequences are named, durable, monotonic number generators. They are the
mechanism behind auto-incrementing keys: the `SERIAL` pseudo-type is sugar for
an `INTEGER` column whose `DEFAULT` calls `nextval` on an owned sequence. This
spec adds three things that build on each other:

1. **Generalize column `DEFAULT`** — the stored default is the bounded
   `ColumnDefault` enum, applied through the shared `build_insert_row` funnel.
   Constants stay constant, `Nextval` advances a sequence, and `Expr` stores
   typed durable IR that is lowered and evaluated per omitted row.
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
- `currval('<name>')` — return the session value most recently established by
  `nextval('<name>')` or `setval('<name>', n, true)`. Errors if no such value
  exists for `<name>` in the current session.
- `setval('<name>', n [, is_called])` — reposition the sequence.
  Non-transactional. With `is_called = false`, the next `nextval` returns `n`
  and the session's `currval` state is not changed.
- Column `DEFAULT <const>`, bounded expression defaults such as
  `DEFAULT upper('hi')`, and `DEFAULT nextval('<existing-sequence>')` in
  `CREATE TABLE`. Omitted columns are filled from the default at `INSERT` and
  `COPY ... FROM` via the existing `build_insert_row` funnel (so `RETURNING` and
  `ON CONFLICT` get it for free).
- `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` (and `SERIAL2`/`SERIAL4`/`SERIAL8`)
  column types in `CREATE TABLE`, desugared to `INTEGER NOT NULL DEFAULT
  nextval('<owned-sequence>')`.

### Out of scope (v1 deferrals)

- The `DEFAULT` keyword as a value in `INSERT ... VALUES (DEFAULT, ...)` — no
  `Expr::Default` marker is added; omit the column from the column list to get
  its default instead. (Deferred; the earlier draft included it.)
- `lastval()` (no cross-sequence "last touched" session tracking).
- A functional `CACHE` (per-session value pre-allocation) — parsed, ignored.
- `ALTER SEQUENCE`, `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT`, identity
  columns (`GENERATED ... AS IDENTITY`).
- `OWNED BY` clause on `CREATE SEQUENCE`; sequence ownership is established only
  implicitly by `SERIAL`.
- Defaults that reference table columns, aggregates, subqueries, parameters, or
  unsupported functions; expression defaults are otherwise supported when they
  bind against an empty column scope and type-check against the target column.
- `CREATE SEQUENCE AS <type>` data-type clause; sequences are always `i64`.

## 2. Decisions

These were settled during design and drive the rest of the spec.

1. **General column `DEFAULT`, not a SERIAL-only attribute.** SERIAL desugars to
   a real, visible, overridable default. The durable field is
   `ColumnDef.default: Option<ColumnDefault>` (a bounded enum), rather than a
   parallel sequence-only attribute — keeping one default mechanism.
2. **WAL-logged advancement, no cache.** Each `nextval`/`setval` appends and
   flushes a logical WAL record before updating live sequence state and
   returning, so any value handed to a client is durable and never reissued.
   Checkpoint-only durability was rejected because it would reissue values after
   a crash (duplicate keys).
3. **Function set: `nextval` + `currval` + `setval`.** `currval` requires new
   per-connection session state; `lastval` is deferred.
4. **`CACHE` is a no-op.** Behavioral options (`INCREMENT`, `START`, `MINVALUE`,
   `MAXVALUE`, `CYCLE`) are honored; `CACHE` is accepted and ignored.
5. **Widen the durable default field.** `ColumnDef.default` becomes
   `Option<ColumnDefault>`, whose variants are `Const(Value)`,
   `Nextval(SequenceId)`, and `Expr(StoredExpression)`.
   Constant `DEFAULT` landed unreleased on this branch, so changing its on-disk
   shape is acceptable. The catalog snapshot is versioned catalog-v3 JSON and
   older formats are rejected (`serialize_catalog`). `ColumnDefault` derives its serde
   (externally tagged); no compatibility shim is kept for the brief bare-`Value`
   form it had before this enum, since dev data is resettable (runtime-data
   convention).
6. **No `DEFAULT` keyword in `VALUES` for v1.** Reusing the existing
   omitted-column funnel is enough for SERIAL; adding `Expr::Default` is deferred.

## 3. Column DEFAULT

### 3.1 What exists today

- `common::ParsedColumnDef.default` is `Option<ParsedDefault>` and
  `common::ColumnDef.default` is `Option<ColumnDefault>`
  (`crates/common/src/schema.rs`), `#[serde(default)]`, and round-trip through
  the catalog snapshot.
- The parser converts `DEFAULT <expr>` through `convert_column_default`
  (`crates/parser/src/convert/ddl.rs`): literals and unary-minus numerics become
  `ParsedDefault::Const`, a valid `nextval('<sequence>')` becomes
  `ParsedDefault::Nextval`, and other expression forms are carried as canonical
  SQL text in `ParsedDefault::Expr` for binder validation.
- The executor applies defaults in one funnel,
  `build_insert_row(statement, schema, columns, values, default_exprs) -> Row`
  (`crates/executor/src/query.rs`), filling each omitted column from
  `ColumnDefault::Const`, `ColumnDefault::Nextval`, `ColumnDefault::Expr`, or
  `NULL`. It is **shared by `INSERT`, `INSERT ... ON CONFLICT`, and
  `COPY ... FROM`**.
- The binder already permits omitting a `NOT NULL` column when it has a non-NULL
  default (`validate_insert_omissions`, `crates/planner/src/binder/dml.rs`).

### 3.2 Types (`common`) — generalize the default

Widen the two default fields from a bare constant to a bounded enum. Because the
non-constant default (`nextval`) needs name→id resolution, the **parse-time**
carrier holds the sequence *name* (a `String`, fine in the leaf crate) and the
**durable** carrier holds the resolved `SequenceId`:

```rust
// parse-time, on ParsedColumnDef.default: Option<ParsedDefault>
pub enum ParsedDefault {
    Const(Value),
    Nextval(String),
    Expr(StoredExpression),
    OwnedNextval(String),
    Serial,
}

// durable, on ColumnDef.default: Option<ColumnDefault>
pub enum ColumnDefault {
    Const(Value),
    Nextval(SequenceId),
    Expr(StoredExpression),
}
```

Both are `serde`-serializable. `ParsedDefault::Serial` is a parse-time marker for
the SERIAL family; execution replaces it with internal
`ParsedDefault::OwnedNextval(name)` after creating the owned sequence. User
defaults use `Nextval(name)` and may not borrow an owned sequence. Keeping
`Const(Value)` as a variant preserves the existing constant behavior unchanged;
`Expr(StoredExpression)` stores typed durable IR plus canonical SQL for display. See Decision 5
for the on-disk migration.

### 3.3 Parser

- `convert_column_default`: constants fold to `ParsedDefault::Const(value)` as
  today; `nextval('<string-literal>')` becomes `ParsedDefault::Nextval(name)`;
  other expression forms become `ParsedDefault::Expr(text)` for binder
  validation. A malformed `nextval` remains a parse error.
- Recognize the `SERIAL` family (§6) in `convert/ddl.rs`: emit
  `data_type = Integer`, `nullable = false`, and
  `default = Some(ParsedDefault::Serial)`. Reject an explicit `DEFAULT` on a
  `SERIAL` family column.

### 3.4 Binder

- `CREATE TABLE` default validation (`validate_default_value`,
  `crates/planner/src/binder/mod.rs`):
  - `Const(v)` → existing type-check (no implicit casts; `NULL` only if nullable).
  - `Nextval(name)` → look the sequence up in the catalog (error `42P01` if
    missing), reject owned sequences with `2BP01`, and require the column type to
    be `INTEGER`. The resolved `SequenceId` is produced when the catalog turns
    `ParsedColumnDef` into `ColumnDef` (the catalog owns the sequence registry);
    the binder's job is pre-validation and good error messages.
  - `Serial` → require `INTEGER` (the parser has already normalized the type)
    and carry the SERIAL column name and ordinal on the bound `CREATE TABLE`;
    execution chooses the owned sequence name from the then-current catalog.
  - `Expr(text)` → parse and bind once against an empty column scope, reject
    aggregates/subqueries/parameters, require the result type to be assignable to
    the column, and persist typed IR as `ColumnDefault::Expr`.
- `validate_insert_omissions`: `has_usable_default` treats `Nextval` and `Expr`
  defaults as usable, so an omitted `NOT NULL` SERIAL/`nextval`/expression-default
  column is not rejected up front. An expression default that evaluates to `NULL`
  is caught per row by normal NOT NULL validation.

### 3.5 Executor

- `build_insert_row` fills omitted columns by **evaluating a `ColumnDefault`**:
  `Const(v)` → `v`; `Nextval(seq_id)` → call the sequence manager's `nextval`
  (advance + `SequenceAdvance` WAL + update session `currval` state);
  `Expr(stored)` → lower typed IR directly and evaluate it over an empty row;
  absent default → `NULL`. Because this is the shared funnel, `INSERT`,
  `ON CONFLICT`, and `COPY ... FROM` all get sequence-backed and expression
  defaults with no per-path work — see §12 for the ordering consequences.

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
a `sequences` field. This is an additive change carried by `#[serde(default)]` on
the catalog-v3 field; older catalog formats are rejected, so a current snapshot deserializes with an
empty sequence set.

### 4.2 Runtime: `SequenceManager` (`storage`)

The live current value lives in a `SequenceManager` owned by the storage engine
(which already owns WAL append and the recovery replay loop). Per sequence it
holds `(last_value, is_called)` behind a per-sequence lock so concurrent writers
(the shared writer guard permits parallel DML) get unique values.

The **`SequenceManager` trait is declared in `common`** (concrete impl in
`storage`), mirroring the existing `SsiTracker`/`ConflictWaiter` traits, so that
`StatementContext` can carry an `Arc<dyn SequenceManager>` (§5) without `common`
depending on `storage`.

```text
nextval(id):
    lock sequence id
    compute next from (last_value, is_called, increment, min, max, cycle):
        if !is_called: next = last_value     // initial last_value is START WITH;
                                             // setval(..., false) makes nextval return n
        else:          next = last_value + increment, bounded by min/max
        if next overflows min/max:
            if cycle: wrap to max/min (per increment sign)
            else:     error (sequence exhausted)
    append and flush WAL: SequenceAdvance { id, value: next }
    set last_value = next, is_called = true
    unlock
    return next

setval(id, n, is_called):
    lock; validate n; append and flush WAL SetSequenceValue { id, n, is_called };
    set last_value = n, is_called; unlock

sequence_exists(id):
    check the runtime sequence map without advancing or writing WAL
```

The manager exposes the current `(last_value, is_called)` for every sequence to
the checkpoint path, which writes them into the catalog snapshot baseline.

### 4.3 WAL records (`wal`)

Sequence create/drop metadata is carried as `CatalogObject::Sequence` inside
the generic `CatalogChange`. The two non-transactional value records remain:

```rust
SequenceAdvance { sequence: SequenceId, value: i64 }       // UNCONDITIONAL replay
SetSequenceValue { sequence: SequenceId, value: i64, is_called: bool } // UNCONDITIONAL
```

**Replay gating is the crux of gap semantics.** Sequence catalog mutations are
part of a transaction and publish only from a committed `CatalogChange`.
`SequenceAdvance`/`SetSequenceValue` are
**non-transactional**: recovery applies them regardless of the surrounding
transaction's commit/abort, fast-forwarding each sequence to the maximum
advanced value (and the last `setval`). This is what guarantees a value handed
out before a crash is never reissued, even if its transaction aborted.

As with all storage operations, these records are appended only in normal
operation; recovery never appends WAL (`docs/specs/crates/wal.md`).

### 4.4 Durability and recovery flow

- **Steady state:** `nextval`/`setval` append and flush their sequence-value WAL
  records before updating live sequence state and returning. This makes a value
  handed to a client durable even if the surrounding transaction later aborts or
  the process crashes before another commit/checkpoint/shutdown flush.
- **Checkpoint:** `run_checkpoint` (`server/src/checkpoint.rs`) already
  serializes the catalog under the exclusive guard. It additionally pulls each
  sequence's current `(last_value, is_called)` from the `SequenceManager` into
  the snapshot baseline before writing the control record, and WAL truncation
  proceeds as today.
- **Recovery:** load sequence definitions + baseline values from the manifest;
  reserve sequence ids from all catalog allocator high-water values; apply
  committed sequence mutations, and unconditionally
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
- **`currval` is a pure read** of per-connection session state after verifying
  that the bound sequence ID still exists. It takes no writer guard and writes no
  WAL. Dropped sequence → `42P01`; missing session entry →
  object-not-in-prerequisite-state error.

### Session state and executor threading

Follow the pattern SSI already established: `StatementContext` carries the SSI
tracker as a handle threaded into the executor (`crates/common/src/context.rs`).
Add two handles there the same way:

- a shared `Arc<dyn SequenceManager>` (for `nextval`/`setval` and non-mutating
  existence checks used by `currval`), defaulting to a no-op/None for read-only
  contexts that never touch sequences; and
- the per-connection `currval` state — a `SequenceId -> i64` map behind interior
  mutability (`Arc<Mutex<SessionSequenceState>>`) since `StatementContext` fields
  are shared `Arc`s — updated by `nextval` and `setval(..., true)`, read by
  `currval`.

This is the one place the otherwise-pure expression evaluator gains side effects;
it is confined to these three functions and does not perturb SSI tracking (§12),
because sequence ops touch no heap tuples.

`nextval`/`setval`/`currval` are permitted in projection lists, `INSERT`/`COPY`
values, `UPDATE` SET expressions, and column `DEFAULT`s. Their evaluation count
and order in a `WHERE` clause are unspecified (volatile), matching PostgreSQL.

## 6. SERIAL Desugaring

`SERIAL`/`BIGSERIAL`/`SMALLSERIAL` (and `SERIAL2`/`SERIAL4`/`SERIAL8`) are valid
only as a column type in a `CREATE TABLE` column definition. All map to the same
64-bit `INTEGER` storage but report their serial kind's PostgreSQL wire width
(`serial` => `int4`, `smallserial` => `int2`, `bigserial` => `int8`), consistent
with the integer aliases; the executor range-checks `int2`/`int4` values at write,
so a `SMALLSERIAL` column rejects a value past the `int2` range (e.g. reaching
32768).

Desugaring is split across bind and execute because the owned sequence's
`SequenceId` is not allocated until the statement runs. The dependency is now
one-way — the table's column default needs the sequence id, but the sequence
stores no table reference (`owned: bool` only, §4.1) — so the order is simply
"sequences first, then table," with no placeholder to patch:

- **Parser/binder:** a SERIAL column is represented by
  `ParsedDefault::Serial`; binder preserves that marker on
  `BoundStatement::CreateTable`. The owned sequence name is chosen at execution
  time from the current catalog, so prepared statements cannot reserve stale
  names.
- **Executor (`execute_create_table`), in order, one autocommit transaction:**
  1. Choose each owned sequence name from the current catalog under the DDL guard:
     `<table>_<column>_seq`, appending the smallest free numeric suffix if taken.
     For each request, `catalog.create_sequence(...)` (increment 1, start 1, min
     1, max `i64::MAX`, no cycle, `owned: true`) → `SequenceId`. The final
     sequence objects are included with the table in one generic catalog change.
  2. Set each SERIAL column's default to internal
     `ParsedDefault::OwnedNextval(<generated name>)`.
  3. `catalog.create_table_with_options(...)`, which resolves explicit `Nextval(name)`
     defaults only against non-owned sequences and resolves
     `OwnedNextval(name)` only against owned sequences created for SERIAL →
     `ColumnDefault::Nextval(id)` on the stored `ColumnDef`.

Because the sequences and table are created in the same autocommit transaction,
they share its commit/abort outcome — if `CREATE TABLE` aborts, neither exists
(the containing `CatalogChange` is CLOG-gated to that txn).

A SERIAL column may participate in a (possibly composite) `PRIMARY KEY` or a
`UNIQUE` constraint — both now exist — with no special handling: the default
fills the value before the key/uniqueness checks run in `build_insert_row`.

**Ownership rules:**

- `DROP TABLE t` cascade-drops its owned sequences: for each column of `t` whose
  default is `ColumnDefault::Nextval(s)` with `catalog.sequence(s).owned`, include
  the sequence removal with the table removal in the same catalog change.
- `DROP COLUMN` follows the same Auto ownership edge for that column, removing
  its owned sequence in the same catalog change and physical transaction. It
  takes the owned sequence's exclusive object lock before catalog or storage
  removal. A user-owned sequence referenced by an ordinary `nextval` default
  survives when the column and its default are removed.
- `DROP SEQUENCE s` where `s.owned` → dependency error. To name the owning table
  the catalog scans tables for the column whose default is `Nextval(s)` (rare op;
  a linear scan is fine). `IF EXISTS` suppresses the not-found case, not this one.

## 7. Server dispatch

- `CREATE SEQUENCE` / `DROP SEQUENCE` are classified `StatementClass::Ddl` and
  participate in explicit transactions. DROP resolves the sequence id,
  takes `SequenceExclusive` plus the schema/name locks through the server lock
  manager and revalidates before appending WAL. CREATE takes the schema/name locks
  after its shared writer guard. Both mutate transaction-local catalog/runtime
  state, append their
  logical WAL records, and are
  transaction-scoped through the catalog/storage journals inside a block.
  Retained object locks release after top-level commit/rollback, or return to a
  captured earlier grant set on `ROLLBACK TO SAVEPOINT`, in step with the catalog
  and storage journals so other sessions never observe provisional state.
- Statements flagged `mutates_sequences` are routed to the write path even when
  they are syntactically `SELECT`; binding collects their sequence ids and the
  xid owner takes `SequenceAccess` before snapshot/execution. DML defaults do the
  same. DROP TABLE includes every owned sequence in its ordered object-lock set
  and takes `SequenceExclusive` before the catalog publication gate.
- `currval` remains a pure read with no writer-guard routing, but binding includes
  its resolved sequence id and execution takes `SequenceAccess` before
  revalidation so DROP cannot expose provisional absence.

## 8. Error handling

| Condition | SQLSTATE |
|---|---|
| `nextval`/`currval`/`setval` on unknown sequence | `42P01` UndefinedTable |
| `nextval` past MAXVALUE / below MINVALUE, `NO CYCLE` | sequence-exhausted (`2200H`) / `NumericValueOutOfRange` |
| `currval` before `nextval`/`setval(..., true)` in this session | object-not-in-prerequisite-state (`55000`) |
| `SERIAL` outside a `CREATE TABLE` column def, `SERIAL` with explicit `DEFAULT`, or unsupported type modifiers | bind/parse error (`FeatureNotSupported`/`SyntaxError`) |
| `DEFAULT nextval('missing')` at `CREATE TABLE` | `42P01` UndefinedTable |
| Explicit `DEFAULT nextval('<owned-serial-sequence>')` | `2BP01` DependentObjectsStillExist |
| `DROP SEQUENCE` of a sequence referenced directly or inside a typed default/CHECK expression | `2BP01` DependentObjectsStillExist |
| `INCREMENT BY 0`, or `MINVALUE > MAXVALUE`, etc. | `22023` InvalidParameterValue |

## 9. Testing

- **parser**: sequence grammar + every option, `DEFAULT <const>` and
  `DEFAULT nextval('s')` column options, rejection cases (incl. `DEFAULT` as a
  `VALUES` item still being rejected, and non-`nextval` function defaults), plus
  the `SERIAL` family cases.
- **catalog**: create/drop, id allocation high-water behavior, snapshot
  round-trip including sequences + baseline values, old-snapshot compatibility.
- **storage / recovery**: crash after N `nextval`s → no value reissued; an
  aborted transaction's `nextval` keeps the gap; `setval` then crash; `CYCLE`
  wrap and `NO CYCLE` exhaustion; concurrent `nextval` from parallel writers
  yields all-unique values.
- **server integration**: `setval` repositioning; `SELECT nextval(...)` routes
  through the write path and is durable; transactional commit, rollback, and
  savepoint behavior for `CREATE`/`DROP SEQUENCE`; end-to-end SERIAL insert and id read-back via
  `INSERT ... RETURNING id` (primary idiom) and `currval`; `DROP TABLE`
  and `DROP COLUMN` cascade-dropping the owned sequence; `DROP SEQUENCE` of an
  owned sequence erroring.
- **interactions (§12)**: `INSERT ... ON CONFLICT DO NOTHING` on an existing key
  still consumes a sequence value (observable gap); `excluded.<serial_col>` sees
  the `nextval`-filled value; a `SERIALIZABLE` transaction that calls `nextval`
  (with no heap reads/writes) commits without a `40001`, and a SERIALIZABLE abort
  of a txn that did touch heap does not roll back its sequence advance.

## 10. Build order

A single implementation plan, sequenced as independently testable commits:

1. **Generalize the default field (no sequences yet).** Introduce
   `ColumnDefault`/`ParsedDefault` enums, migrate `ParsedColumnDef.default` and
   `ColumnDef.default` to them with `Const` preserving today's constant behavior
   and `Expr` carrying non-constant defaults as `StoredExpression`, update
   `convert_column_default`, `validate_default_value`, the catalog snapshot
   (the snapshot is catalog-v3 JSON), and the `build_insert_row` fill step to
   lower and evaluate the stored default. Existing DEFAULT tests stay green;
   `Nextval` has no producer yet.
2. **Sequence catalog object + DDL.** `SequenceId`/`SequenceSchema`, catalog
   map + allocator + recovery hooks, manifest field, generic catalog WAL,
   `CREATE`/`DROP SEQUENCE` parsing and server DDL dispatch, recovery replay. No
   functions yet.
3. **Functions + runtime.** `SequenceManager` with WAL-logged advance/setval and
   recovery fast-forward, `nextval`/`currval`/`setval` evaluation, session
   state + `StatementContext` threading, write-path routing for
   sequence-mutating statements, and the `build_insert_row` `Nextval` evaluation.
   Also wire `nextval` as an explicit `DEFAULT nextval('existing')` resolved in
   `catalog.create_table`.
4. **`SERIAL` desugaring + ownership.** `SERIAL` family parsing, desugar to
   owned sequence + `ParsedDefault::OwnedNextval`, owned-sequence cascade on
   `DROP TABLE`, `DROP SEQUENCE` ownership guard.
5. **Specs + sweep.** `COPY ... FROM` already inherits defaults via the shared
   funnel — add tests rather than code; update `docs/specs/overview.md` and the
   affected `docs/specs/crates/*.md`; full `cargo fmt` / `clippy` / `test` sweep.

## 11. Spec impact

This feature updates:

- `docs/specs/overview.md` — data types (note `SERIAL` family → `INTEGER`) and
  SQL subset (`CREATE`/`DROP SEQUENCE`, `DEFAULT nextval`, sequence functions,
  and `SERIAL`). No `Value`/`DataType` change.
- `docs/specs/crates/parser.md` — sequence grammar, `DEFAULT`, `SERIAL`.
- `docs/specs/crates/catalog.md` — sequence object, allocator, snapshot field.
- `docs/specs/crates/wal.md` — the four new record kinds and their replay gating.
- `docs/specs/crates/storage.md` — `SequenceManager`, recovery fast-forward.
- `docs/specs/crates/executor.md` — sequence functions and default application.
- `docs/specs/crates/server.md` — DDL dispatch, write-path routing, session
  state, checkpoint baseline.

## 12. Interactions with RETURNING, ON CONFLICT, and SSI

These features landed after the first draft; all three flow through the shared
`build_insert_row` funnel (`crates/executor/src/query.rs`), which fills defaults
**before** the conflict probe and before `RETURNING` projection.

- **RETURNING.** Because defaults (including a `Nextval`) are filled before
  `eval_returning` (`query.rs:457`), `INSERT INTO t (...) RETURNING id` returns
  the generated SERIAL value directly. This is the **primary** id read-back idiom
  and is preferred over `currval` (no session round-trip, no separate query).
  `UPDATE`/`DELETE ... RETURNING` have no sequence interaction unless an
  assignment itself calls `nextval`.
- **ON CONFLICT (gap on conflict).** `build_insert_row` constructs the proposed
  row — evaluating any `Nextval` default — *before* the primary-key arbiter probe
  (`query.rs:299` → key → probe). Therefore `nextval` is consumed **even when
  `DO NOTHING` skips the insert** because of a conflict, producing an observable
  sequence gap. This is intentional and matches PostgreSQL; it is consistent with
  the non-transactional "advances are never rolled back" rule (§4.3). Workloads
  that upsert mostly-conflicting rows will burn sequence values — expected.
  `excluded.<col>` for a SERIAL column reflects the `nextval`-filled proposed
  value.
- **SSI.** `nextval`/`setval` touch only in-memory sequence state and WAL — never
  heap tuples — so they record no SIREAD locks or rw-edges
  (`crates/common/src/context.rs` `SsiTracker`; recording happens only at scan
  operators and storage writes). A statement that *only* calls a sequence
  function can never be the cause of a `40001` serialization abort, and a
  `StatementContext` for such work keeps the no-op `NoSsiTracker` default. If a
  `SERIALIZABLE` transaction is aborted by SSI for its heap activity, its
  sequence advances are **not** undone — the same gap semantics as any abort.
  `SERIALIZABLE` is now its own `IsolationLevel` variant; sequences need no
  per-isolation special-casing.
