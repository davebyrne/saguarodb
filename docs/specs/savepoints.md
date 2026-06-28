# SaguaroDB Savepoints / Subtransactions Specification

**Date:** 2026-06-27
**Status:** Draft

## 1. Overview

Savepoints let a transaction mark a point it can later roll back to without
aborting the whole transaction, and nest such points. SaguaroDB implements them
with **subtransaction xids (subxids)** layered on the existing PostgreSQL-style
MVCC, with **no before-image undo** — exactly the path `docs/specs/mvcc.md` §12
anticipated ("sub-transaction xids + CLOG; no undo needed"). A rolled-back
subtransaction's row versions stay in the heap, made invisible by the CLOG, and
are reclaimed by VACUUM like any aborted xid.

This promotes savepoints from a documented non-goal to an implemented feature.

### Supported (full PostgreSQL semantics)

- `SAVEPOINT <name>` — establish a savepoint (open a subtransaction).
- `RELEASE SAVEPOINT <name>` — release (merge) a savepoint into its parent.
- `ROLLBACK TO SAVEPOINT <name>` (and `ROLLBACK TO <name>`) — undo work since the
  savepoint; the savepoint remains active for continued work.
- **Nesting** — savepoints form a stack; inner levels are subtransactions of
  outer ones.
- **Same-name re-establishment** — `SAVEPOINT s` may be issued again; `RELEASE`/
  `ROLLBACK TO s` target the most recent `s`. An older same-named savepoint
  becomes reachable again after the newer one is released/rolled-back.
- **Failed-state recovery** — `ROLLBACK TO SAVEPOINT s` recovers a transaction
  that entered the failed (`25P02`) state after `s` was established, clearing the
  failed state and continuing from `s`.
- **Cross-transaction correctness** — after a transaction commits, other
  transactions see exactly its released subtransactions' rows and never its
  rolled-back ones.
- **Crash recovery** — the same guarantee holds across a crash/restart.

### Out of scope (unchanged deferrals)

- Implicit per-statement subtransactions (PL/pgSQL-style `EXCEPTION` blocks). A
  raw statement error still poisons the block to the failed state; `ROLLBACK TO`
  is the recovery mechanism, matching PostgreSQL's interactive SQL behavior.
- `cmin`/`cmax` command-id intra-statement visibility (still deferred, per
  `mvcc.md` Milestone G).

## 2. Grammar & SQLSTATEs

```
SAVEPOINT          identifier
RELEASE [SAVEPOINT] identifier
ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] identifier
```

Identifiers normalize to lowercase (quoted identifiers remain unsupported).

- `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` outside a transaction block →
  `NoActiveSqlTransaction` (`25P01`).
- `RELEASE`/`ROLLBACK TO` of a name with no matching live savepoint →
  `InvalidSavepointSpecification` (`3B001`). Like any statement error, this aborts
  the block to the failed (`'E'`) state (PostgreSQL behavior); a subsequent
  `ROLLBACK TO` of an *existing* savepoint can still recover it.
- These commands via the **extended** query protocol are rejected
  (`FeatureNotSupported`), like other transaction control (simple-query only).

Two `common::SqlState` variants are added: `NoActiveSqlTransaction` (`25P01`) and
`InvalidSavepointSpecification` (`3B001`).

## 3. Subtransaction model

A top-level transaction `T` (xid allocated at `BEGIN`, as today) carries a
**savepoint stack** of levels, each with its own subxid drawn from the same
`next_txn_id` space:

```
stack (bottom → top): [ (name_1, subxid_1), (name_2, subxid_2), ... ]
writing xid = stack.last().subxid, or T if the stack is empty
```

Subxids are allocated **eagerly** at `SAVEPOINT` (consistent with today's eager
top-level allocation at `BEGIN`; lazy assignment is a future optimization).
A write statement stamps the current writing xid as the tuple's `xmin`
(`xmax` for deletes), unchanged from today — a subxid is just an xid.

The transaction also keeps a **live-(sub)xid set** = `T` plus every subxid not
rolled back (open *and* released). It is what visibility and the conflict
classifiers treat as "self" (§4), and its members stay registered in the active
set until the top settles.

### Operations on the stack and CLOG

- **`SAVEPOINT s`**: allocate `subxid`, register it active, add it to the live-set,
  push `(s, subxid)`.
- **`RELEASE SAVEPOINT s`**: a **pure in-memory stack merge** — pop the nearest
  level named `s` (and any levels above it) into their parent. The popped subxids
  are **not** marked in the CLOG and **not** deregistered: they stay in the active
  set and in the live-set. This is load-bearing for atomicity — a released subxid
  must *not* become visible to other transactions before the top commits. While
  `T` is in progress its rows stay invisible to others (still in their snapshots'
  `xip`) and visible to `T` (own-write); they settle `Committed` only at the
  top-level `COMMIT`. (This is precisely why a flat CLOG suffices without
  `pg_subtrans`: **a subxid reads `Committed` only after its top commits** — until
  then it is either in `xip` or recorded `Aborted`.)
- **`ROLLBACK TO SAVEPOINT s`**: find the nearest level named `s`; mark its subxid
  and every subxid above it **`Aborted`** in the CLOG, deregister them and remove
  them from the live-set, pop the levels above `s`, and replace `s`'s subxid with a
  **fresh** subxid (PG keeps `s` active for continued work). Clears the failed
  state if set.
- **Top-level `COMMIT`**: `T` and every live-set subxid (open or released — i.e.
  all non-rolled-back) commit durably together (§5). The in-memory CLOG statuses
  for the whole family are set `Committed` first, then `{T}` ∪ all live-set
  subxids are removed from the active registry in a **single latched batch**
  (`deregister_all`). This atomicity is load-bearing: the active set holds the
  family as independent entries (a concurrent reader cannot map `S→T` — that is
  the `pg_subtrans` job we avoid), so visibility of the commit flips per-xid as
  each leaves the active set. A per-id deregister loop would let a concurrent
  snapshot `capture` observe a torn commit (a released subxid visible while `T`
  still appears in-progress, or vice versa). The batch makes a concurrent
  `capture` see the family either all-present (all invisible) or all-absent (all
  settled), mirroring the capture-vs-`register_allocated` guarantee (`mvcc.md`
  §7.1). Rolled-back subxids stay aborted.
- **Top-level `ROLLBACK`** (or disconnect/crash): `T` and all its remaining
  (sub)xids abort — set `Aborted`, then the same single latched batch
  `deregister_all`.

## 4. Visibility & own-writes

The reading transaction's **live-(sub)xid set** (§3 — `T` plus its non-rolled-back
subxids; small) travels on `StatementContext`/`Snapshot`. The "self" check that is
today `xid == current_txn` (a scalar) generalizes to "`xid` ∈ the live-set" in
**three** places — `is_visible`/`txn_effect_visible` (own-write) **and** the two
conflict classifiers `common::mvcc::write_conflict` and `classify_unique_conflict`
(own row-lock; §9). All three otherwise unchanged. The live-set check stays
positionally **first** in `txn_effect_visible` (before the future `>= xmax` and
`xip` checks), so an own subxid allocated *after* a Repeatable Read snapshot
(`subxid >= snapshot.xmax`) is still seen by its owner.

Consequences, all via the existing machinery — **no `pg_subtrans` mapping** (which
holds precisely because a released subxid stays registered/in `xip` until the top
commits, §3):

- **My own live subxid** (open or released) → self → visible / not a conflict.
- **My own rolled-back subxid** → removed from the live-set on `ROLLBACK TO`, so it
  is *not* self; it falls to the CLOG → `Aborted` → invisible (even to me).
- **Another transaction's in-progress *or released* subxid** → it is still in the
  snapshot's `xip` (the active registry holds it until that top commits; §3, §6) →
  invisible.
- **A settled subxid** (its top has committed) → CLOG: released → `Committed` →
  visible; rolled-back → `Aborted` → invisible.

`xmin`/`xmax`/infomask hint bits are unchanged. A `DELETE`/`UPDATE` under a subxid
stamps `xmax = subxid`. Two cases the conflict classifiers must handle:
- **Rolled-back subxid's stale `xmax`** → the next writer treats it as released —
  the **same** path the first-updater-wins classifier already takes for an aborted
  deleter's `xmax` (it consults CLOG status; see `mvcc.md` §7.3). Confirmed to work
  as-is.
- **A still-live earlier (sub)xid of the *same* transaction** (e.g. the top deleted
  a key, then a savepoint re-inserts it) → must be treated as **self** via the
  live-set, not a foreign lock; otherwise the transaction spuriously conflicts with
  itself (`40001`/`23505`). This is the live-set generalization of the classifiers
  above.

## 5. CLOG, WAL & crash recovery (durability-critical)

Each subxid is an ordinary CLOG entry (`InProgress`/`Committed`/`Aborted`). The
hard requirement: **recovery must distinguish a committed transaction's released
subxids (keep their rows) from its rolled-back subxids (hide their rows)** — an
in-memory-only scheme would lose released-subxid rows on a crash.

- **`ROLLBACK TO`** appends an `Abort` WAL record for each rolled-back subxid (not
  fsynced; recovery reads it), so the subxid recovers as aborted.
- **Top-level `COMMIT`** records, durably, the **set of committed subxids** (the
  live + released ones) alongside `T`. Recovery marks `T` and those subxids
  `Committed`.
- A subxid that is neither in a durable commit set nor `Abort`-logged (e.g. open
  under a top that never committed) recovers via the existing **in-flight =
  aborted** rule.
- **CLOG truncation / floors**: the existing `committed_floor`/`vacuum_floor`
  conservatism — never drop an aborted xid's record above the vacuum floor — must
  apply to rolled-back **subxids** too, so a rolled-back subxid below a naive floor
  never wrongly reads `Committed`. This is the trickiest interaction and gets
  dedicated recovery/truncation tests.

## 6. Active registry, snapshot & GC horizon

- The active-transaction registry tracks **subxids alongside top-level xids**.
  `capture_snapshot` includes active subxids in `xip`, so other transactions see
  an in-progress (or released-but-not-top-committed) subxid as in-progress
  (invisible). `xmin`/`xmax` are computed over the combined set as today.
- A subxid stays registered from `SAVEPOINT` until it settles: **`ROLLBACK TO`
  deregisters** the rolled-back subxids (marked `Aborted`); **`RELEASE` does
  not** (it is an in-memory merge, §3 — the released subxid stays registered, and
  in others' `xip`, until the top commits). The top-level `COMMIT`/`ROLLBACK`
  deregisters `{T}` ∪ all remaining subxids in **one latched batch**
  (`ActiveTxnRegistry::deregister_all`, after their CLOG statuses are set), so a
  concurrent `capture` never sees a partially-settled family, then recomputes the
  GC horizon (advertised-`xmin`). No `pg_subtrans`.

## 7. VACUUM

`common::mvcc::is_dead_to_all` is unchanged. A rolled-back subxid's rows take the
aborted-creator branch (reclaim immediately, no horizon gate); a committed
subxid's rows behave like any committed creator. Subxids are reclaimed as
ordinary xids.

## 8. Server transaction lifecycle & protocol

- `Transaction` gains the savepoint stack. A new `StatementClass::Savepoint(...)`
  is routed through the transaction-control lifecycle (not bound/planned), like
  `BEGIN`/`COMMIT`/`ROLLBACK`.
- The failed-state gate permits `ROLLBACK TO SAVEPOINT` (recovery) in addition to
  `COMMIT`/`ROLLBACK`; all other statements still return `25P02`.
- Command tags: `SAVEPOINT`, `RELEASE`, `ROLLBACK` (PostgreSQL's tags).

## 9. Crate responsibilities

- `common`: the two new SQLSTATEs; the live-(sub)xid set on the visibility inputs;
  and generalizing the scalar `== current_txn` self-check to "∈ live-set" in **all
  three** of `is_visible`/`txn_effect_visible` (own-write), `write_conflict`, and
  `classify_unique_conflict` (own row-lock) — so a transaction never spuriously
  conflicts with its own earlier subtransaction.
- `parser`: `Statement::Savepoint`/`ReleaseSavepoint`/`RollbackToSavepoint`
  (sqlparser 0.56 already parses all three; today they are rejected).
- `wal`: subxid-aware top-level `Commit` record (carrying the committed subxid
  set) + recovery rebuild + truncation-floor handling.
- `server`: `Transaction` savepoint stack + live-set; `SAVEPOINT`/`RELEASE`
  (in-memory merge) / `ROLLBACK TO` handlers; failed-state recovery; active-registry
  subxid tracking (released subxids stay registered until the top commits) plus a
  new `ActiveTxnRegistry::deregister_all(&[TxnId])` for the atomic family-deregister
  at top-level COMMIT/ROLLBACK; `StatementClass::Savepoint` routing; command tags;
  threading the live-set into every statement's `StatementContext`.
- `storage`: pass the live-set through to the conflict classifiers; the `xmax`
  stale-lock (aborted-subxid) case already works via CLOG status.

## 10. Implementation milestones

One cohesive feature, staged as reviewed commits that land together before merge:

- **M1** — parser + `StatementClass` + `Transaction` savepoint stack + eager subxid
  allocation + `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` + CLOG settle + own-transaction
  visibility (live-set) + failed-state recovery. Single-connection-correct.
- **M2** — cross-transaction visibility: subxids in the active registry / snapshot
  `xip`; concurrency tests.
- **M3** — WAL/recovery durability: subxid-aware commit record, recovery rebuild,
  truncation/floor; crash-recovery tests.

## 11. Testing

- **common** unit tests: visibility with subxids (own live / own rolled-back /
  other in-progress / other settled released vs rolled-back).
- **wal/recovery** tests: a transaction that releases one savepoint and rolls back
  another, then commits, survives a crash — released rows kept, rolled-back rows
  hidden; truncation/floor never resurrects a rolled-back subxid.
- **server concurrency** tests: a second transaction sees a committed
  transaction's released rows and never its rolled-back rows.
- **server integration** (simple query, via psql or the harness): the full SQL
  surface — nested savepoints, same-name re-establishment, `RELEASE`, `ROLLBACK
  TO`, `ROLLBACK TO` recovering a failed (`25P02`) transaction, and the error
  paths (`25P01` outside a block, `3B001` unknown savepoint, extended-protocol
  rejection).
