# Serializable Snapshot Isolation (SSI)

**Date:** 2026-07-10
**Status:** Implemented feature specification

This document specifies SaguaroDB's `SERIALIZABLE` isolation level, implemented as
Serializable Snapshot Isolation (SSI) layered on the existing snapshot-isolation
machinery. It is the system-level contract for the feature; it complements
`docs/specs/mvcc.md` (snapshot isolation, visibility, write-write conflicts) and
`docs/specs/deadlock.md` (blocking writers + deadlock detection), and supersedes
the prior decision that `SERIALIZABLE` is a bare alias for Repeatable Read.

## 1. Purpose and scope

Snapshot isolation (Repeatable Read) and Read Committed are already implemented.
Snapshot isolation permits two well-known anomalies that violate serializability:

- **Write skew** — two transactions each read an overlapping set, then each writes
  a disjoint part based on what it read; serially, the second would have seen the
  first's write. (E.g. two on-call doctors each check "≥1 other on call" and both
  go off call.)
- **Read-only / phantom anomalies** — including a transaction that inserts a row
  matching another transaction's predicate after that transaction scanned it.

This feature makes `SERIALIZABLE` transactions truly serializable by detecting the
read-write (rw) antidependencies that snapshot isolation ignores, and aborting a
transaction before any cycle in the apparent serialization order can commit.

### 1.1 Guarantee

The serializability guarantee holds **among `SERIALIZABLE` transactions**: any set
of transactions that all run at `SERIALIZABLE` executes with a serializable
schedule. This matches PostgreSQL's model. Concurrent `READ COMMITTED` /
`REPEATABLE READ` transactions are **not** part of the SSI graph; a `SERIALIZABLE`
transaction still gets full snapshot-isolation guarantees against them, but mixing
isolation levels does not extend serializability to the lower-level transactions.

`READ COMMITTED` and `REPEATABLE READ` are **unchanged** — same lock-free read path,
no read-tracking overhead, byte-for-byte the same behavior. SSI overhead is paid
only by `SERIALIZABLE` transactions.

### 1.2 Granularity decision (Implementation A)

SIREAD locks (the record of what a transaction read) are taken at two granularities:

- **Tuple** — a full-key point read through the storage identity index for a declared primary key (`WHERE pk = k`)
  records a SIREAD on `(table, key)`.
- **Relation** — a sequential scan, a range scan, or any non-point predicate
  records a SIREAD on the whole `(table)`.

This is **fully serializable**, including against phantoms: an `INSERT` into a table
conflicts with any concurrent relation-level reader of that table. It is
deliberately coarser than PostgreSQL's index-range predicate locks: a scan and a
concurrent write of the *same table* can produce a **false-positive** serialization
failure even when they touch disjoint rows. This is the accepted trade for a
tractable, low-risk first implementation; over-locking is always *safe* (it can only
cause extra aborts, never a missed anomaly).

### 1.3 Non-goals (deferred, documented)

- **Fine-grained predicate locks** (index-range / next-key / gap locks) that would
  let an out-of-range write avoid conflicting with a range reader. A later milestone.
- **Granularity promotion** beyond a simple memory safety valve (§5.4).
- **Read-only-transaction optimizations** (safe snapshots, deferral). A later
  milestone.
- **Cross-isolation-level serializability** (see §1.1).

## 2. Background: the SSI algorithm

SaguaroDB follows the Cahill/Ports formulation (the basis of PostgreSQL's SSI).

A **rw-antidependency** `T_r →rw T_w` exists when transaction `T_r` reads a version
of an item and transaction `T_w` produces a *later* version of that item that `T_r`
does not see — i.e. `T_w` is concurrent with `T_r` (`T_w`'s write is invisible to
`T_r`'s snapshot). It captures "`T_r` would have to come *before* `T_w` in any
equivalent serial order."

Cahill's theorem: every cycle in the dependency graph of a snapshot-isolation
schedule contains a transaction `T_pivot` with **two consecutive rw-antidependency
edges** — one inbound (`T_in →rw T_pivot`) and one outbound (`T_pivot →rw T_out`) —
and `T_out` is the first of the three to commit. Detecting this **dangerous
structure** and aborting one participant breaks every potential cycle. SSI may abort
some transactions that were not in fact part of a cycle (false positives), but never
allows a non-serializable schedule (no false negatives).

## 3. Architecture

SSI reuses the structural pattern established by blocking + deadlock detection:

- **`SsiTracker` trait** (in `common`, `Send + Sync`), threaded onto
  `StatementContext` exactly like `ConflictWaiter`. The read path calls it to record
  reads; the write path calls it to register writes and form edges. The default
  implementation, `NoSsiTracker`, does nothing — installed for `READ COMMITTED` /
  `REPEATABLE READ` so their paths are untouched. A `SERIALIZABLE` transaction
  installs the real tracker.
- **`SerializableConflictManager`** (in `server`, a sibling of `LockManager`), the
  real `SsiTracker`. It owns the SIREAD lock table, the rw-conflict graph, and the
  per-transaction conflict flags, behind its own lock(s). It is constructed in
  `recovery.rs`/`app.rs` and held in `ServerComponents`.
- **Top-level keying.** Like the deadlock wait-for graph, all SSI state is keyed by
  **top-level** transaction id; a savepoint subxid canonicalizes to its top via
  `ActiveTxnRegistry::top_of`. A read or write performed under a subxid is attributed
  to the enclosing top-level transaction.
- **In-memory and transient.** The SIREAD table, the graph, and the flags are
  process-local and never persisted. **No WAL, manifest, snapshot, or page-format
  change.** A crash aborts all in-flight transactions, so there is nothing to recover;
  this matches the deadlock wait-for graph.

A `SERIALIZABLE` SSI abort surfaces as `SqlState::SerializationFailure` (**40001**) —
the same code already used for a committed-write conflict (`docs/specs/mvcc.md` §7.3)
— so existing client retry logic applies unchanged.

## 4. Isolation-level wiring

- Add `IsolationLevel::Serializable` (`crates/common/src/mvcc`). `READ COMMITTED` and
  `REPEATABLE READ` are unchanged.
- The parser stops aliasing `SERIALIZABLE` to `RepeatableRead`
  (`crates/parser/src/convert`): `SERIALIZABLE` → `IsolationLevel::Serializable`. All
  three entry forms route through it: `BEGIN ISOLATION LEVEL SERIALIZABLE`,
  `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`, and
  `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE`.
- **Snapshot selection.** A `SERIALIZABLE` transaction takes a **single
  per-transaction snapshot**, exactly like `REPEATABLE READ` (one stable snapshot
  captured at the first statement, held for the transaction's life), *plus* read
  tracking. `snapshot_for_transaction` (`query/txn.rs`) treats `Serializable` like
  `RepeatableRead` for snapshot capture; the difference is the installed tracker.

## 5. SIREAD locks (the read side)

### 5.1 What is recorded, and where

SIREAD locks are recorded at the **executor's scan operators**, which know the access
method (so they distinguish a point lookup from a scan) and run for `SELECT` and the
`WHERE`-scan of `UPDATE`/`DELETE`:

- **`IndexScan` through the storage identity index for a declared primary key with a full exact key** →
  `record_tuple_read(table, key)`. Recorded **even when no row matches**, so a
  later `INSERT` of that key is caught as a phantom.
- **`SeqScan`, non-primary-key `IndexScan`, composite-primary-key prefix `IndexScan`, or `IndexScan` over a range** →
  `record_relation_read(table)`.

Two reads do **not** flow through the scan operators and so record their SIREAD lock at
their own site (a missed read here is a silent serializability hole):

- **`COPY ... TO`** scans the whole relation → `record_relation_read(table)` in
  `CopyOut::new`.
- **The `INSERT ... ON CONFLICT` primary-key arbiter probe** is a point read of the
  proposed key → `record_tuple_read(table, key)` (recorded even when the key is absent,
  for phantom protection), right before the storage identity lookup.

Recording at the scan operator (rather than in the storage `is_visible` path) is both
cleaner — one site per access method — and correct for the chosen granularity: the
relation lock covers every row a scan could have seen, including rows not yet
inserted.

### 5.2 The SIREAD lock table

```
relation_readers: HashMap<TableId, HashSet<TopTxnId>>
tuple_readers:    HashMap<(TableId, Key), HashSet<TopTxnId>>
```

plus, per `SERIALIZABLE` transaction, the snapshot it read under (needed for the
concurrency test in §6) and the set of keys/tables it has locked (for cleanup).

### 5.3 Lifetime

A SIREAD lock must outlive the reading transaction: a write that arrives *after* the
reader commits can still form an rw-edge, as long as the writer was concurrent with
the reader. A reader's SIREAD locks are therefore released only once **no transaction
concurrent with it remains active** — i.e. when the GC horizon
(`ActiveTxnRegistry`'s `oldest_xmin`) advances past the reader. Release is driven from
the same point the GC horizon is recomputed; a committed `SERIALIZABLE` transaction's
manager state (locks, graph node, flags) is retained until then.

### 5.4 Memory safety valve

To bound memory without full granularity promotion: if a single transaction
accumulates more than a fixed threshold of tuple SIREAD locks on one table, its tuple
locks for that table are collapsed into a single relation lock. This stays correct
(coarser ⇒ more conservative) and caps per-transaction state. The threshold is a
compile-time constant initially (no new startup flag).

Primary-key DDL uses the same conservative direction. `ALTER TABLE ... ADD/DROP
PRIMARY KEY` changes the table identity keyspace, so before the storage identity
tree is rebuilt any retained tuple SIREAD locks for that table are promoted to a
relation lock, and any retained tuple write records for that table are marked as
relation-granularity conflict-out candidates. Future writes under hidden heap
identity or a new primary-key tuple therefore still see readers that originally
locked an old key, and future exact reads under the new keyspace still see
writers retained from the old keyspace.

### 5.5 Savepoints

SIREAD locks are attributed to the top-level transaction and are **retained across a
partial `ROLLBACK TO`** (releasing them would be an optimization, and keeping them is
safe — it can only over-conflict). This mirrors the deadlock graph's top-level keying.

## 6. rw-edge formation

An rw-antidependency edge `T_r →rw T_w` (`T_r` read an item, `T_w` wrote a later,
concurrent version of it that `T_r` could not see) must be formed regardless of
which transaction acted first. There are two orderings, and **both** must be caught
(the write-side check alone — PostgreSQL's `CheckForSerializableConflictIn` — is *not*
sufficient; the read-side `CheckForSerializableConflictOut` is equally required):

- **Conflict-in (reader before writer).** At the storage write points —
  `stamp_xmax_logged` (the `UPDATE`/`DELETE` version stamp, `engine/dml.rs`) and the
  insert path (`engine/mod.rs`, `engine/index.rs`) — a `SERIALIZABLE` writer calls
  `note_write(table, key)`. The manager forms `T_r →rw T_w` for each **already-recorded**
  SIREAD holder `T_r` of that item: `relation_readers[t]` ∪ `tuple_readers[(t, k)]`
  (for an insert, the `relation_readers[t]` hit is the phantom protection — a scan of
  the table conflicts with a new row; the `tuple_readers[(t, k)]` hit covers a point
  reader that read "key `k` absent" before its insertion).
- **Conflict-out (writer before reader).** At read time — when a `SERIALIZABLE`
  transaction records a SIREAD lock (§5.1, at the executor scan operators / COPY-TO) —
  the manager forms `T_r →rw T_w` for each **already-recorded** *writer* `T_w` of the
  item being read. This requires the dual of the reader table: a **writer table**
  populated by `note_write`, `relation_writers[t]` and `tuple_writers[(t, k)]`. A
  relation read consults `relation_writers[t]` (any concurrent writer of any row in
  the scanned table); a tuple read consults `tuple_writers[(t, k)]`.

Conflict-in and conflict-out are exact duals: both form the edge `T_r →rw T_w` and
both use the same concurrency test.

**Whole-relation writes.** Transactional `TRUNCATE` calls
`note_relation_write(table)` before installing a replacement generation. On the
write side it forms `T_r →rw T_w` for every concurrent holder in
`relation_readers[table]` and every tuple-reader entry for that table. It records
the writer in `relation_writers[table]`. A later relation or tuple SIREAD on the
table consults that relation-writer entry and forms the same conflict-out edge.
This makes a generation replacement conflict with all logical rows, including
point reads, absent-key reads, and scans, without enumerating heap tuples. Repeated
TRUNCATE by one top-level transaction is idempotent in SSI tracking.

**Concurrency test.** An edge `T_r →rw T_w` is relevant only when `T_w` is *not
visible* to `T_r`'s snapshot (`T_w` committed after `T_r`'s snapshot, or is still
in-flight). If `T_w` were already visible to `T_r`, then `T_r` read `T_w`'s own
version and there is no antidependency. The manager evaluates this from `T_r`'s stored
snapshot. Self-edges (`T_r == T_w`) are never formed. Edges are formed only between
`SERIALIZABLE` transactions (both endpoints tracked).

Edge formation **records** the edge in both endpoints' edge sets; the abort decision
is §7. The writer table has the same lifetime and top-level keying as the SIREAD
reader table (§5.3, §5.5). This is purely additive to the write/read paths: SSI never
blocks, and write-write conflicts continue to block and deadlock-detect exactly as in
`docs/specs/deadlock.md`.

## 7. Dangerous-structure detection and abort

Each `SERIALIZABLE` transaction tracks two flags — `has_in_conflict` (some
`T_in →rw self` exists) and `has_out_conflict` (some `self →rw T_out` exists) — and,
at commit, an in-memory monotonic **commit sequence number** (process-local, not a
durable commit timestamp).

A **dangerous structure** is a pivot transaction `T_pivot` with both
`has_in_conflict` and `has_out_conflict` (so edges `T_in →rw T_pivot →rw T_out` exist)
where its outbound neighbor `T_out` **has already committed while `T_pivot` has not**
— `T_out` commits first. By Cahill's theorem, every cycle contains such a pivot, so
checking this condition at every transaction catches the cycle at whichever pivot
satisfies it; a pivot that itself commits before its `T_out` is not the dangerous one
and may proceed.

**Victim: the acting transaction (synchronous abort).** Breaking *any one* of the
structure's two edges breaks the structure, and the transaction whose action *just
detected* it — the writer that formed the edge, or the transaction attempting to
commit — is always one of the structure's participants. So the manager aborts **that
acting transaction** with `SerializationFailure` (40001), on its own thread, at the
point it is acting. No transaction ever has to abort another transaction running on a
different connection, so there is **no asynchronous "doomed-transaction" flag** and no
cross-thread cancellation. (PostgreSQL may instead pick the pivot to abort fewer
transactions overall; aborting the actor is a correctness-preserving simplification —
it never misses an anomaly, but can abort a different victim than an oracle would.)

**Detection runs at two moments** — both are necessary, because `T_out` committing
first is the trigger and it can become true either before or after the second edge
exists:

1. **On edge formation** (in `note_write`) — the new inbound edge `T_r →rw T_w` can
   make the acting writer `T_w` a pivot (it already had an outbound edge whose target
   committed first). `T_w` aborts. The other shape — the reader `T_r` becoming a pivot
   via this new outbound edge — cannot be dangerous yet, because its `T_out` is the
   still-in-flight writer `T_w`, which has not committed first.
2. **On commit** — when transaction `T` commits it may (a) be a pivot whose own
   `T_out` already committed first, or (b) be the `T_out` whose committing-first makes
   an in-neighbor pivot dangerous. Either way the committing `T` is a participant, so
   `T` aborts. The check inspects `T`'s own flags and its in-neighbors' flags.

The commit-time check sits in `commit_transaction` (`query/txn.rs`) **before the WAL
`Commit` record is flushed**, so a `SERIALIZABLE` transaction that loses the SSI check
is rolled back and never becomes durable. A transaction aborted by SSI follows the
ordinary abort path (`docs/specs/mvcc.md` §8): CLOG/status abort, no undo, dead
versions reclaimed by VACUUM.

> A conservative variant (abort on *any* pivot with both flags, ignoring commit
> order) is also correct but aborts more often. The first cut uses the commit-order
> condition above to keep the false-positive rate down; the commit-sequence numbers it
> needs are cheap and in-memory.

## 8. Transaction lifecycle integration

- **Begin.** A `SERIALIZABLE` transaction registers a graph node with the manager and
  installs the real tracker on its `StatementContext`. Its snapshot is the per-txn
  snapshot of §4.
- **Read.** Scan operators record SIREAD locks (§5).
- **Write.** Storage write points form edges (§6).
- **Commit.** `commit_transaction` runs the §7 commit-time check before the WAL
  flush; on pass it commits normally; on failure it aborts with 40001. The committed
  transaction's manager state is retained for the SIREAD-lifetime window (§5.3) and
  its commit-sequence number is assigned.
- **Abort / rollback.** The manager drops the transaction's edges and flags. Its
  SIREAD locks are released subject to the lifetime rule (§5.3) — an aborted
  transaction's reads cannot participate in a real anomaly, so they may be released
  promptly.
- **Cleanup hooks** attach at the existing registry deregister points
  (`deregister_all` on top-level commit/abort) and the GC-horizon recomputation, the
  same places `LockManager::on_txn_finished` is already called.

## 9. Coexistence and recovery

- **Blocking + deadlock detection** (`docs/specs/deadlock.md`) is orthogonal:
  write-write conflicts still block and can raise 40001 (committed conflict) or 40P01
  (deadlock); SSI adds its own 40001 path on rw-cycles. A `SERIALIZABLE` writer can
  both block on a write-write conflict and participate in SSI edges; the two
  mechanisms do not interact beyond sharing the registry and the abort path.
- **VACUUM / HOT.** SIREAD locks reference `(table, key)` and `(table)`, not physical
  TIDs, so HOT chain collapse and VACUUM are unaffected. The SIREAD-lifetime horizon
  (§5.3) is the existing GC horizon; SSI does not hold versions back beyond it.
- **Primary-key DDL.** `ALTER TABLE ... ADD/DROP PRIMARY KEY` promotes retained tuple
  SIREAD locks on the table to relation locks before changing the storage identity
  keyspace. This is intentionally conservative and avoids stale tuple keys.
- **Crash recovery.** No durable SSI state; nothing to replay. Recovery operations
  append no WAL and run no SSI tracking.

## 10. Edge cases and correctness notes

- **Aborted reader or writer.** An aborted transaction's reads/writes form no durable
  anomaly; edges to/from an aborting transaction are dropped on abort.
- **Read-only `SERIALIZABLE` transactions** take SIREAD locks and can be the `T_in` of
  a structure, but never the `T_pivot` (a pivot needs an outbound edge, which requires
  a write). They will not be the preferred victim; this naturally avoids many
  needless aborts even without the dedicated read-only optimization.
- **Single-statement / autocommit `SERIALIZABLE`.** Tracking still applies, but a lone
  autocommit statement cannot form a two-edge structure with itself; overhead is a
  short-lived SIREAD set released at commit.
- **`SERIALIZABLE` with savepoints.** Top-level keying (§3, §5.5) keeps detection
  sound across subtransactions, exactly as the deadlock graph does.

## 11. Testing strategy

- **Anomaly suite** (server integration tests, concurrent connections, mirroring
  `tests/deadlock.rs`):
  - *Write skew* — two `SERIALIZABLE` transactions read-then-write the overlapping
    set; exactly one commits, the other gets 40001. The same workload under
    `REPEATABLE READ` is allowed to commit both (proving the levels differ and SI lets
    the anomaly through).
  - *Phantom* — `T1` scans a table under a predicate; `T2` inserts a matching row;
    one of the pair fails with 40001.
  - *rw-cycle* — a constructed two-/three-transaction cycle yields exactly one 40001
    victim; the survivors commit.
  - *No false negative regression* — RC/RR behavior and the existing MVCC/visibility
    tests are unchanged.
- **Manager unit tests** — edge formation and the concurrency test; pivot detection
  with commit ordering; SIREAD lifetime release at the GC horizon; the memory safety
  valve; top-level keying under savepoints.
- **psql smoke** — two concurrent `SERIALIZABLE` psql sessions reproduce write skew
  and show one session getting `ERROR: could not serialize access ...`.

## 12. Implementation milestones

Each milestone is a focused, independently reviewable commit (the SSI graph stays a
no-op until the read and write hooks are both live, so intermediate states are safe).

1. **Spec + isolation wiring** — this document; `IsolationLevel::Serializable`; parser
   un-alias; `SERIALIZABLE` uses the RR per-txn snapshot (behaves as RR until tracking
   lands); update `docs/specs/mvcc.md` §10 and `docs/specs/overview.md`.
2. **`SsiTracker` trait + threading** — trait in `common`, `NoSsiTracker` default,
   `StatementContext` field + builder; install the real tracker only for
   `SERIALIZABLE`.
3. **`SerializableConflictManager` skeleton** — SIREAD lock table, per-txn
   registration, GC-horizon-driven release, wiring into `ServerComponents` /
   `recovery.rs` (no edges yet).
4. **Read recording** — executor scan operators call `record_tuple_read` /
   `record_relation_read`.
5. **rw-edge formation** — `note_write` hooks at the storage write points; the
   concurrency test; flag maintenance.
6. **Dangerous-structure detection + abort** — pivot condition with commit-sequence
   ordering; edge-time and commit-time checks; the `commit_transaction` hook before
   the WAL flush; 40001.
7. **Tests + verification** — the anomaly suite, manager unit tests, psql smoke;
   `fmt`/`clippy`/`test` workspace-green.

## 13. Future work

- Fine-grained index-range predicate locks (§1.3) to cut false positives on
  index-range reads with out-of-range writes.
- Read-only safe-snapshot / deferral optimization.
- Granularity promotion (tuple → page → relation) with a page-lock layer.
