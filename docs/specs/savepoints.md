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
  `InvalidSavepointSpecification` (`3B001`).
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

### Operations on the stack and CLOG

- **`SAVEPOINT s`**: allocate `subxid`, register it active, push `(s, subxid)`.
- **`RELEASE SAVEPOINT s`**: pop the nearest level named `s` and every level above
  it; mark each popped subxid **`Committed`** in the CLOG (its writes now belong
  to the parent; their final fate follows the top-level commit), and deregister
  them from the active set.
- **`ROLLBACK TO SAVEPOINT s`**: find the nearest level named `s`; mark its subxid
  and every subxid above it **`Aborted`** in the CLOG, deregister them, pop the
  levels above `s`, and replace `s`'s subxid with a **fresh** subxid (PG keeps `s`
  active for continued work). Clears the failed state if set.
- **Top-level `COMMIT`**: `T` and every still-live (released or open) subxid commit
  durably together (§5). Rolled-back subxids stay aborted.
- **Top-level `ROLLBACK`** (or disconnect/crash): `T` and all its subxids abort.

## 4. Visibility & own-writes

Subxids reuse the existing predicate (`common::mvcc::is_visible` /
`txn_effect_visible`) with one change: the **own-write check** generalizes from
`xid == current_txn` to "`xid` is one of the reading transaction's *live*
(sub)xids." The reading transaction's live-(sub)xid set (small — at most the
savepoint depth plus `T`) travels on `StatementContext`/`Snapshot`.

Consequences, all via the existing machinery — **no `pg_subtrans` mapping**:

- **My own live subxid** → own-write → visible.
- **My own rolled-back subxid** → removed from the live set on `ROLLBACK TO`, so it
  is *not* own-write; it falls to the CLOG → `Aborted` → invisible (even to me).
- **Another transaction's in-progress subxid** → it is in the snapshot's `xip`
  (the active registry tracks subxids; §6) → invisible.
- **A settled subxid** (its top committed) → CLOG: released → `Committed` →
  visible; rolled-back → `Aborted` → invisible.

`xmin`/`xmax`/infomask hint bits are unchanged. A `DELETE`/`UPDATE` under a subxid
stamps `xmax = subxid`; if that subxid is later rolled back, the next writer that
encounters the stale `xmax` lock must treat it as released — the **same** path the
first-updater-wins conflict classifier already takes for an aborted deleter's
`xmax` (it consults CLOG status; see `mvcc.md` §7.3). The implementation verifies
this generalizes to subxids.

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
  an in-progress subxid as in-progress (invisible). `xmin`/`xmax` are computed
  over the combined set as today.
- `RELEASE`/`ROLLBACK TO` deregister the settled subxids, recomputing the GC
  horizon (advertised-`xmin`) like any txn end. No `pg_subtrans`.

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
  the small `is_visible` own-write generalization.
- `parser`: `Statement::Savepoint`/`ReleaseSavepoint`/`RollbackToSavepoint`
  (sqlparser 0.56 already parses all three; today they are rejected).
- `wal`: subxid-aware top-level `Commit` record (carrying the committed subxid
  set) + recovery rebuild + truncation-floor handling.
- `server`: `Transaction` savepoint stack; `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`
  handlers; failed-state recovery; active-registry subxid tracking;
  `StatementClass::Savepoint` routing; command tags.
- `storage`: confirm the `xmax` stale-lock conflict path generalizes to subxids
  (expected: it already consults CLOG status).

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
