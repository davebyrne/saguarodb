# SaguaroDB Blocking Writes & Deadlock Detection Specification

**Date:** 2026-06-28
**Status:** Draft

## 1. Overview

This replaces SaguaroDB's previous **fail-fast first-updater-wins** write-write
conflict policy (`mvcc.md` §7.3) with **blocking + timeout-based deadlock
detection**, matching PostgreSQL's row-lock behavior. A writer that encounters a
row locked by another *in-progress* transaction now **waits** for that transaction
to finish instead of aborting immediately; deadlocks (waiters forming a cycle) are
broken by aborting a victim.

This reverses the prior deliberate "no blocking, no deadlock detection" decision,
so the previous `40001`-on-in-progress-conflict behavior is gone (the `40001`
serialization failure now arises only for a *committed*-superseded conflict — see
§3).

### Scope

- Applies to write-write conflicts at both isolation levels (Read Committed and
  Repeatable Read). The wait is identical; only the post-wait outcome differs by
  what the holder did (§3), not by isolation level.
- Covers the two conflict sites: the `xmax` row lock (UPDATE/DELETE, and an
  UPDATE's old-version stamp) and the unique-key conflict (INSERT, and an UPDATE
  that writes a new index entry).
- **No EvalPlanQual / row re-evaluation.** After a wait, a writer either proceeds
  (holder aborted) or fails with `40001` (holder committed) — it does not re-read
  and re-qualify the updated row version. (PostgreSQL's Read Committed
  re-evaluation is intentionally out of scope.)

## 2. Where the wait happens

The conflict is detected deep in the storage engine, under a page latch, when a
writer re-reads the target version's header before stamping `xmax`. A writer
**must not block while holding a latch**, and **must not re-run the whole
statement** (a multi-row UPDATE would double-apply already-stamped rows). So the
wait is **per row, after releasing the latch, re-attempting only the stamp**:

```
under page latch: re-read xmax / scan unique candidates
   conflict with an in-progress holder B  → return WouldBlock(B)   (latch dropped)
   conflict with a committed holder        → 40001
   no conflict                              → stamp / proceed
caller loop (holds the StatementContext, not a latch):
   WouldBlock(B) → conflict_waiter.wait_for(me, B)?   // parks the spawn_blocking thread
                 → re-attempt the stamp/scan:
                       B aborted    → row free → stamp / proceed
                       B committed  → 40001
                       new holder C → wait_for(me, C)?   (loop)
```

The new row version (for INSERT/UPDATE) is still written **once, before** the
wait loop, exactly as today; a successful proceed makes it the live version, an
abort leaves it an invisible orphan reclaimed by VACUUM. The wait loop re-attempts
only the `xmax` stamp (or the unique scan), so it adds **no extra orphans** and
never re-executes the statement.

Execution runs on `tokio::task::spawn_blocking` threads, so parking a writer's
thread does not stall the async runtime.

## 3. Post-wait semantics

When `wait_for(me, B)` returns (B has finished), the writer re-checks the row:

- **B aborted** → its lock evaporated; the row is free → proceed (stamp `xmax`, or
  treat the unique candidate as non-conflicting).
- **B committed** → the row changed under the writer's snapshot →
  `SqlState::SerializationFailure` (`40001`) for the `xmax` lock, or
  `SqlState::UniqueViolation` (`23505`) for a unique-key conflict. Identical at
  Read Committed and Repeatable Read (no re-evaluation).
- **A different in-progress holder C** appeared → wait again on C.

`wait_for` itself returns early (without the holder finishing) only for:

- **Deadlock** → `SqlState::DeadlockDetected` (`40P01`); the statement errors and
  (inside a transaction block) poisons it to the failed state.
- **Cancel** → `SqlState::QueryCanceled` (`57014`), from the existing
  per-statement cancel flag.

## 4. The lock manager & deadlock detection

`LockManager` (a new `Arc` field on `ServerComponents`) owns the wait coordination:

- State: `Mutex<{ waits_for_top: HashMap<TopId, TopId> }>` + a `Condvar`, plus a
  shared `ActiveTxnRegistry` handle and the configured `deadlock_timeout`. **The
  wait-for graph is keyed and valued by *top-level* txn ids** (`TopId`), not subxids:
  a transaction waits for at most one other at a time, and a deadlock is between
  *transactions*. Each (sub)xid is canonicalized to its top via `registry.top_of`
  (identity for a top-level id) at edge **insert** time.
- `wait_for(waiter_subxid, blocker_subxid, cancel)` — the engine passes the writer's
  *writing xid* (`ctx.txn_id`, the innermost subxid) and the stamped `xmax` (also
  possibly a subxid). Under the lock: insert the edge
  `top_of(waiter_subxid) → top_of(blocker_subxid)` into `waits_for_top`, then loop:
  - blocker no longer active (`registry.is_active(blocker_subxid)` is false) → return
    `Ok` (re-check the row). **`is_active` is keyed on the specific blocker subxid**
    (held as a local, *not* in the graph), because a partial `ROLLBACK TO` aborts and
    deregisters only that subxid — a waiter on it must then proceed even though the
    top is still live.
  - `cancel` set → return `Err(QueryCanceled)`;
  - `condvar.wait_timeout(poll_interval)`; accumulate elapsed; every
    `deadlock_timeout` of accumulated wait, run cycle detection from
    `top_of(waiter_subxid)`.
  - On exit (any branch), remove this waiter's `top → …` edge.
- **Deadlock detection (single critical section).** Cycle detection, victim
  selection, and removal of the victim's edge happen **together, under the held
  `LockManager` lock** (the detector already holds it at the `wait_timeout` tick) —
  so a chosen victim is no longer a graph node when any other detector reads the
  graph, which is what makes §9's "exactly one victim" hold even though every waiter
  is its own detector. Detection walks `waits_for_top` **top → top** (each top has at
  most one outgoing edge); a cycle is a walk that revisits the waiter's own top.
  Because both endpoints were canonicalized at insert, the next hop
  `waits_for_top[current_top]` is well-defined regardless of which subxid stamped the
  row or which subxid a blocked transaction is currently parked under — closing the
  cross-subxid case (e.g. `{101→200, 201→101}` for tops 100/200 becomes
  `{100→200, 200→100}`, a detected cycle). **Victim = the detecting waiter's
  transaction**, which returns `Err(DeadlockDetected)` and drops its edge in the
  same critical section. (`top_of` is backed by a small in-memory subxid→top map
  maintained only for *active* transactions — distinct from a durable `pg_subtrans`,
  and not used by the visibility path.) A `poll_interval` of ~100 ms bounds cancel
  latency; cycle detection runs only at the full `deadlock_timeout`.

### Waking waiters (lost-wakeup-safe)

Whenever a (sub)xid leaves the active set — top-level commit/abort/rollback **and a
partial `ROLLBACK TO SAVEPOINT`** (`abort_subxids`, which deregisters only the
rolled-back subxids) — **after** the deregister, the lifecycle calls
`lock_manager.on_txn_finished()`, which takes the `LockManager` lock and
`notify_all`. Waiters wake and re-check `registry.is_active(blocker)`. (Including
partial rollback is a latency optimization — a waiter on a rolled-back subxid would
otherwise proceed only on its next poll tick; correctness holds either way.) Lock
ordering is `LockManager → ActiveTxnRegistry`
(the registry is a leaf lock and never acquires the `LockManager` lock), and
`on_txn_finished` runs after deregister while taking the `LockManager` lock — so a
finishing transaction cannot slip its wakeup between a waiter's `is_active` check
and its `condvar.wait`. A missed wakeup, were one possible, would only delay a
waiter to the next poll tick, never lose correctness.

## 5. Cancel & graceful shutdown

The wait honors the per-statement cancel flag, polled on each `poll_interval` tick,
so a blocked writer responds to a client `CancelRequest` within ~100 ms. The cancel
flag is the connection's `Arc<AtomicBool>` (from `begin_cancelable`); since the
conflict point only has the `StatementContext` (not the `ExecutionContext` that
currently borrows `&AtomicBool`), `StatementContext` carries the cancel handle as a
field (§6) so the storage wait-loop can thread it into `wait_for`. A blocked writer
keeps holding
its `InFlightQueryGuard`, so graceful shutdown's `wait_for_idle` accounts for it
and times out gracefully (within `--shutdown-timeout-ms`) exactly as for any
long-running statement — no new hang path.

## 6. Surface changes

- New `common::SqlState::DeadlockDetected` → SQLSTATE `40P01`.
- New startup flag `--deadlock-timeout-ms <MS>` (default **1000**, PostgreSQL's
  `deadlock_timeout`), on `Config`.
- `WriteConflict` gains `WouldBlock(TxnId)`; `UniqueConflict`'s `InFlight` becomes
  `WouldBlock(TxnId)` (carrying the in-flight creator's xid). The pure classifiers
  `write_conflict` / `classify_unique_conflict` return the blocker.
- A `ConflictWaiter` trait in `common`; `StatementContext` carries
  `Arc<dyn ConflictWaiter>` AND the cancel handle `Arc<AtomicBool>`. The default
  `ConflictWaiter` (read/test contexts) **errors loudly** (`InternalError`) if its
  `wait_for` is ever actually called — a real `WouldBlock` must never reach it, so a
  mis-wired write context fails fast instead of spinning forever (`WouldBlock →
  no-op Ok → re-attempt → WouldBlock → …`). The server sets the real `LockManager`
  waiter and the connection's cancel `Arc` on every write-capable context. (Neither
  `Arc<dyn ConflictWaiter>` nor `Arc<AtomicBool>` is `PartialEq`/`Eq`, so
  `StatementContext`'s derived `PartialEq`/`Eq` must be hand-rolled to exclude both
  new fields — comparing the existing value fields as today.)

## 7. Crate responsibilities

- `common`: the `DeadlockDetected` SQLSTATE; the `ConflictWaiter` trait + the
  `StatementContext` waiter and cancel fields (the default waiter errors on use);
  `WriteConflict::WouldBlock` / `UniqueConflict::WouldBlock` and the classifier
  changes.
- `storage`: `stamp_xmax_logged` / `unique_conflict_kind` return `WouldBlock(b)`;
  the engine's INSERT/UPDATE/DELETE methods wrap the conflict point in a wait-retry
  loop driven by `ctx.conflict_waiter`, threading `ctx.cancel`.
- `server`: the `LockManager` (implements `ConflictWaiter`); `ActiveTxnRegistry::
  is_active` and `top_of` (the active subxid→top map, populated when a savepoint
  subxid is allocated and pruned on deregister); wiring into `ServerComponents` /
  `execution_context` (waiter + cancel); the wake calls on commit/abort/rollback
  **and partial `ROLLBACK TO`**; and the `--deadlock-timeout-ms` flag.

## 8. Implementation milestones

Each lands as a reviewed commit:

1. Spec + `SqlState::DeadlockDetected` (`40P01`) + `--deadlock-timeout-ms`.
2. `common`: classifiers return the blocker (`WouldBlock`); `ConflictWaiter` trait +
   `StatementContext` field.
3. `storage`: `WouldBlock` outcomes + engine wait-retry loops.
4. `server`: `LockManager` + registry `is_active`; wire into components /
   `execution_context`; wake on commit/abort.
5. Tests + psql smoke.

## 9. Testing

- **common** unit tests: `write_conflict` / `classify_unique_conflict` return
  `WouldBlock(holder)` for an in-progress holder, `Conflict` / `Violation` for a
  committed one, `Proceed` / `None` for an aborted one or self.
- **server** concurrency tests: a second writer blocks on an in-progress writer and
  then **proceeds** when the holder aborts, or fails **`40001`** when it commits; a
  two-transaction **deadlock** aborts exactly one victim with `40P01` while the
  other proceeds; a `CancelRequest` interrupts a blocked writer with `57014`.
- **no-regression**: existing single-writer and reader-not-blocked behavior is
  unchanged; recovery is unaffected (blocking changes only runtime conflict
  handling, not durable records).
