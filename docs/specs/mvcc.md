# SaguaroDB MVCC — Design & Implementation Plan

**Date:** 2026-06-21
**Status:** Draft
**Branch:** `feat/mvcc`
**Foundation:** `develop` @ `7035c89` (redo-WAL / on-disk-B-tree architecture)

This document is the canonical design and sequenced implementation plan for
multi-version concurrency control (MVCC) in SaguaroDB. It elaborates the
"Future Work: MVCC / Transactions" item in `docs/specs/overview.md`. Where this
document and `overview.md` disagree once implementation begins, the contract
being changed must be updated in `overview.md` and the relevant crate spec in
the same change (per `AGENTS.md`).

---

## 1. Goals and scope

### Goals

- **Snapshot isolation** as the core correctness model.
- **Multi-statement transactions** (`BEGIN` / `COMMIT` / `ROLLBACK`), with
  autocommit preserved as an implicit single-statement transaction.
- **Concurrent readers** that never block writers and never take the global
  write lock.
- **Concurrent writers** with row-level write-write conflict detection.
- A **single, internally consistent storage model** — the Postgres family
  (in-heap versions, index-per-version, no undo, VACUUM). The baseline is
  Postgres-without-HOT; HOT is a later, purely additive optimization.

### Non-goals (initial)

- **Transactional DDL** — DDL stays non-transactional (takes the exclusive lock,
  commits immediately, is rejected inside an explicit transaction block).
- **Serializable isolation (SSI)** — only snapshot isolation (and Read Committed)
  initially.
- **Time-travel / as-of queries.**
- **Savepoints / sub-transactions** — deferred (they fit the model via
  sub-transaction xids without undo; see §12).
- **HOT (heap-only tuples)** — deferred to Milestone H. The baseline is built
  HOT-ready so HOT adds, rather than reworks.

---

## 2. Foundation already in place (`develop` @ `7035c89`)

MVCC builds on prerequisites that are **already merged** into `develop`. The
overview spec states "the redo WAL is the prerequisite" for MVCC; it is done.

| Capability | Where | Relevance to MVCC |
|---|---|---|
| Redo WAL with **PageLSN** gating + **full-page writes** | `crates/wal`, `crates/server/src/recovery.rs` | Idempotent, torn-page-safe physiological redo — the substrate for redo-all recovery |
| **On-disk** non-clustered primary-key B-tree + secondary indexes | `crates/storage/src/btree.rs`, `engine.rs` | Recovery rebuilds nothing in memory; indexes are durable |
| **Eviction-flush-on-steal** + incremental checkpoint + control record | `crates/buffer`, `crates/server/src/checkpoint.rs` | Working set not bounded by the buffer pool |
| Row format **v1 with a reserved version byte** | `crates/storage/src/codec.rs` (`ROW_FORMAT_VERSION = 1`) | Single chokepoint for adding `xmin`/`xmax` |
| Page format **v2 with PageLSN** | `crates/storage/src/page.rs` (`HEADER_LEN = 22`) | Per-page redo gating |
| Per-statement `txn_id`; durable-commit set | `ServerComponents.next_txn_id`, `crates/wal` (`committed_txns`) | Seed for the transaction id allocator and CLOG |
| Owned-guard `ConcurrencyController`, extensible `StatementContext` | `crates/common` | Designed-for seams the MVCC layer swaps/extends |

### What is missing (the MVCC layer this plan adds)

- Per-row version metadata (`xmin`/`xmax`/`t_ctid`) and version chains.
- A durable transaction status map (CLOG) consulted for visibility.
- Snapshots and a visibility predicate threaded into every scan.
- Multi-statement transaction lifecycle and protocol transaction status.
- Concurrent writers + conflict detection (replacing the global write lock).
- Redo-all recovery driven by CLOG (replacing redo-committed-only).
- Garbage collection (VACUUM).

---

## 3. The model: Postgres-family MVCC

### 3.1 The governing principle — where old versions physically live

Every MVCC engine must answer: *when a reader's snapshot needs an old version,
where does that version come from, and how does an index find it?* The two
production answers are **coupled packages**, not free-mix options:

| | Old versions in an **undo log** | Old versions in the **heap** |
|---|---|---|
| **One index entry per key** | InnoDB / Oracle — index/clustered holds the *current* row; reader walks undo; rollback = apply undo | (off-diagonal — avoid) |
| **One index entry per version** | (incoherent) | **Postgres** — index finds each version in the heap; rollback = CLOG + VACUUM |

- InnoDB keeps **single-entry indexes because it has undo**: the clustered index
  stores only the current row, so older versions must live elsewhere.
- Postgres **avoids undo because its heap holds every version**: nothing needs
  reconstructing, so an aborted version is just a heap tuple the CLOG marks
  invisible, reclaimed by VACUUM.

SaguaroDB's foundation is a **non-clustered heap + a separate B-tree**, with
**no undo** and **redo-all physiological recovery** — all Postgres-family. The
consistent choice is therefore the **Postgres diagonal**: in-heap versions +
index-per-version. (An earlier draft proposed a single-entry index pointing at
the newest version with an in-heap back-chain; that is the off-diagonal cell —
correct but architecturally inconsistent, and antagonistic to HOT. It is
rejected. See §4, Decision 2.)

### 3.2 Invariants of the model

1. **In-heap versions.** Every version of a row is a separate heap tuple.
   `UPDATE` inserts a new tuple; the old tuple is retained (marked, not removed)
   until VACUUM. `DELETE` marks the current tuple deleted in place.
2. **No undo.** Aborted and dead versions remain in the heap, invisible via CLOG,
   reclaimed by VACUUM.
3. **Uniform indexes.** Every index (PK and secondary) is `(key → heap TID)`,
   with **one entry per version**, duplicates allowed, pointing at a **stable
   line pointer**. The PK index additionally enforces uniqueness as a
   visibility-aware semantic check.
4. **Line-pointer indirection.** An index entry references a `(page,
   line-pointer-slot)`; tuple bytes may move *within* a page (compaction)
   by updating the line pointer, not the index.
5. **Forward version chains.** A tuple's header carries a forward `t_ctid`
   pointer to its successor version (Postgres `t_ctid`), used for update-locating,
   conflict detection, and (later) HOT.
6. **Visibility by status, not by presence.** A version is visible iff its
   `xmin` is committed-and-visible to the snapshot and its `xmax` is not — decided
   by `xmin`/`xmax` against the snapshot and the CLOG, with `infomask` hint bits
   caching settled status.
7. **Redo-all recovery.** Recovery redoes every record; CLOG decides visibility;
   any transaction without a durable `Commit` at crash is recovered as aborted.
8. **VACUUM reclaims.** Dead tuples, their index entries, and their line pointers
   are reclaimed against an oldest-snapshot horizon.
9. **HOT-ready, HOT-deferred.** The baseline has line pointers, `t_ctid`,
   indexed-column-change detection, and heap-recheck — everything HOT needs.
   HOT (Milestone H) adds the same-page/no-index-change fast path, `REDIRECT`
   line pointers, and chain pruning without removing anything.

---

## 4. Key design decisions

Decisions 2, 3, and 4 are a **mutually-reinforcing triad**: in-heap versions ⇒
no undo ⇒ redo-all. Decisions 1, 5, 6 are comparatively independent.

**Decision 1 — Snapshot model: xid snapshots + CLOG.**
A snapshot is `{xmin, xmax, xip}` (Postgres style); a commit/abort status map
(CLOG) answers committed/aborted/in-progress. *Chosen over* commit-timestamp
ordering, which needs a second counter and a durable txn→commit-ts mapping for an
ordering snapshot isolation does not require. The xid model reuses the existing
monotonic `next_txn_id` and durable-commit set; visibility persists nothing extra
per version. The one cost — a CLOG probe per tuple — is cached away by `infomask`
hint bits. Commit-ts can be layered later if time-travel/SSI is ever wanted.

**Decision 2 — Version storage: index-per-version (Postgres), HOT deferred.**
Indexes hold one entry per version, pointing at stable line pointers; old versions
live in the heap. *Chosen over* (a) single-entry-index + in-heap back-chain
(off-diagonal, inconsistent with no-undo, blocks HOT) and (b) full InnoDB
(clustered index + undo, contradicts no-undo and the non-clustered heap). HOT is
*defined* as the optimization on the index-per-version baseline, so this choice is
the on-ramp to HOT rather than a detour. Cost paid now: the B-tree must allow
multiple entries per key, and uniqueness becomes visibility-aware — both required
for HOT regardless.

**Decision 3 — Abort: no undo.**
Abort = write an `Abort` record + `CLOG[t] = Aborted`; the transaction's versions
stay in the heap, invisible, reclaimed by VACUUM. *Chosen over* before-image undo
(one before-image per `(txn,page)` cannot undo one of two concurrent writers on a
shared page — incompatible with concurrent writers) and ARIES physiological undo
(a large redundant subsystem that erases what the CLOG check already hides).
Abort-as-invisibility is the *same* CLOG check snapshot isolation already needs —
zero marginal mechanism — and keeps commit and abort O(1). A statement error
aborts the whole transaction (enters the `'E'` failed state; must `ROLLBACK`),
which removes any need for partial-statement undo. Retires the buffer-pool
before-image mechanism. Even savepoints fit later via sub-transaction xids, not
undo.

**Decision 4 — Recovery: redo-all + CLOG visibility.**
Redo every record; recover in-flight-at-crash transactions as aborted; relax the
flush gate to WAL-durability only. *Entailed* by Decisions 2+3: with per-version
(not per-page) committedness, the page-level `is_committed` flush gate is
incoherent (one page holds versions from several transactions). *Chosen over*
keeping redo-committed-only, which would require no-steal-for-uncommitted (pin all
of a transaction's dirty pages until commit) — resurrecting the precise
buffer-pool-bound working-set limit MVCC exists to remove. Reuses the existing
idempotent, PageLSN-gated redo engine.

**Decision 5 — Concurrency rollout: readers first, writers last.**
Stage 1 (Milestones C–D): concurrent snapshot readers + serialized writers (one
global writer lock held for the whole write-transaction). Stage 2 (Milestone E):
concurrent writers + row-level conflict detection. *Chosen over* going straight to
concurrent writers, which maximizes simultaneous unknowns. Rework is near-zero
(Stage 1 reuses the existing lock at coarser granularity; conflict detection is
additive) and Stage 1 is a correct, useful, shippable waypoint.

**Decision 6 — DDL: non-transactional initially.**
DDL takes the exclusive lock, commits immediately, and is rejected inside an
explicit transaction block. *Chosen over* transactional DDL, which requires making
the catalog itself MVCC (versioned, abort-undoable) plus transactional file
lifecycle — a second large subsystem orthogonal to data MVCC. Defers cleanly and
additively.

---

## 5. Format and contract changes (durable)

The on-disk format break is confined to the tuple header (§5.1). All other
durable changes are additive (new WAL record kinds). For the A–D MVP the CLOG is
kept **in memory**, rebuilt at recovery from the durable `Commit`/`Abort` WAL
records (§5.4); a standalone durable CLOG file is deferred to Milestone F.

### 5.1 Tuple header — row format v2

`crates/storage/src/codec.rs` is the single reader/writer of the row version byte.
Bump `ROW_FORMAT_VERSION` to `2` and widen the header **once** to everything MVCC
will ever need:

```
+-----------+-----------+--------+--------+----------------+-------------+----------+
| version=2 | infomask  | xmin   | xmax   | t_ctid         | null bitmap | payloads |
| 1 byte    | 2 bytes   | 8 (u64)| 8 (u64)| 6 (page4,slot2)| ceil(n/8)   | ...      |
+-----------+-----------+--------+--------+----------------+-------------+----------+
```

- `xmin` — transaction id that created this version.
- `xmax` — transaction id that deleted/superseded it (`0`/invalid = live).
- `t_ctid` — forward pointer `(page_num: u32, slot: u16)` to the successor
  version; self/sentinel = this is the latest version.
- `infomask` — hint bits, including `XMIN_COMMITTED`, `XMIN_ABORTED`,
  `XMAX_COMMITTED`, `XMAX_ABORTED` (caches of settled CLOG status to avoid CLOG
  probes), with remaining bits reserved for HOT (`HEAP_ONLY`, `HOT_UPDATED`).
- `decode_row` branches on the version byte; v1 tuples decode with implicit
  `xmin = frozen`, `xmax = invalid` (always visible) for any pre-existing data.
- Insert stamps `xmin = txn_id`, `xmax = invalid`, `t_ctid = self`.

### 5.2 Line pointers (heap page slot array)

`crates/storage/src/page.rs` slot entries (`[offset, len, flags]`) become explicit
**line pointers (ItemIds)** with states:

- `NORMAL` — `(offset, len)` address a live tuple on this page.
- `DEAD` — tuple removed but the line pointer is retained because index entries
  may still reference it (reclaimed to `UNUSED` only after index vacuum).
- `UNUSED` — free for reuse. `insert_row` recycles the **lowest** `UNUSED` slot id
  before appending a fresh one (F3b), which bounds the slot array under
  delete→vacuum→insert churn. It reuses **`UNUSED` only, never `DEAD`** — a `DEAD`
  slot may still have a dangling index entry (index vacuum has not run for it),
  whereas an `UNUSED` slot is guaranteed (by the F2b → F3a → F3b ordering) to have
  no index entry, so recycling it cannot let a stale entry resolve to the new
  tuple.
- `REDIRECT` *(reserved; used by HOT in Milestone H)* — points at another slot on
  the same page.

Contract: **indexes reference `(page, line-pointer-slot)`; tuple bytes may be
relocated within a page (compaction) by rewriting the line pointer without
touching any index.** `RowId` becomes `(page_num, line-pointer-slot)` and remains
valid across intra-page compaction.

### 5.3 New WAL record kinds

`crates/wal/src/record.rs` (`WalRecordKind`) gains:

- `Abort` — marks a transaction aborted (payload: empty; `txn_id` in the header).
- `HeapUpdateHeader { file_id, page_num, slot, xmax, t_ctid, infomask }` — an
  in-place physiological update of a tuple header (set `xmax`/`t_ctid` on
  `UPDATE`/`DELETE`, or settle hint bits). Redo applies it under PageLSN gating
  like the other heap records.

Index-entry inserts/removals continue to be logged as today (full-page images of
B-tree pages). VACUUM operations (heap prune, index vacuum, line-pointer reclaim)
are likewise WAL-logged page mutations.

### 5.4 CLOG — transaction status map

A map `txn_id → {InProgress, Committed, Aborted}`, recording the outcome of every
transaction.

**MVP decision (A–D): the CLOG is in-memory, rebuilt from the WAL.** The durable
source of truth for a transaction's outcome is its `Commit`/`Abort` WAL record
(already durable). The CLOG (`crates/wal/src/clog.rs`, `Clog`, keyed by `txn_id`
and answering `status(txn_id) -> common::TxnStatus`) is an in-memory structure
that is (i) updated at runtime on commit (set at flush) and abort (set at append),
and (ii) **rebuilt at recovery by scanning the durable `Commit`/`Abort` records**,
exactly as §8 describes. A standalone durable CLOG *file* and its truncation are
only needed for GC (§9) and to bound recovery scans, so they are deferred to
Milestone F — the A–D MVP invents no new versioned/checksummed durable format,
because recovery rebuilds the CLOG from the WAL regardless. The `Clog` lives in
`crates/wal` because it supersedes the `committed_txns` set previously in
`crates/wal/src/file.rs` and is reconstructed during recovery's WAL scan.

- Rebuilt at recovery from durable `Commit`/`Abort` WAL records (supersedes the
  single-bit `committed_txns` set in `crates/wal/src/file.rs` as the authoritative
  status source). `FileWalManager::is_committed` is now `clog.status(txn) ==
  Committed`, so the redo-committed-only flush/replay gate is behavior-identical.
- Reserved ids below `FIRST_NORMAL_XID` (including `FROZEN_XID`) read as
  `Committed`/visible; an unrecorded normal id reads as `InProgress`.
- An **implicit-committed floor**: an unrecorded normal id strictly below the
  floor reads as `Committed`, covering a *committed* transaction whose `Commit`
  record a checkpoint truncated. An explicit recorded status (e.g. `Aborted`) is
  checked first and always wins over the floor.
- Consulted by the visibility predicate (B3) and the flush policy at runtime.

**Conservative truncation / floor (Milestone D, the critical guard).** Once the
relaxed flush gate (§8) lets an aborted/in-flight transaction's pages reach the
heap, the floor rule "unrecorded-below-floor ⇒ committed" is only sound if such a
transaction is never *below* the floor. So:

- WAL truncation (`WalManager::truncate_before`) advances only across a **prefix
  of committed transactions**: it clamps the truncation LSN to the earliest record
  of the *oldest* transaction below the requested boundary whose CLOG status is not
  `Committed` (aborted or, under Stage 2, in-flight). That transaction "pins"
  truncation — its records (notably its `Abort`) are retained so its status stays
  reconstructible at the next recovery. No write transaction is ever in-flight
  during a checkpoint — under Stage-1 serialized writers trivially, and under
  Stage-2 (E2b) because the checkpoint takes the **exclusive** guard that drains all
  shared writers first — so in practice the pin is an *aborted* transaction. (This
  preserved invariant is exactly why the E2b inversion needs no fuzzy checkpoint.)
- The floor is advanced (at truncation, and re-established at recovery) only up to
  — never across — that oldest non-committed transaction: at recovery the floor is
  `min(allocation_boundary, oldest_non_committed_retained_xid)`. Because truncation
  guarantees everything dropped below the oldest retained non-committed transaction
  was committed, ids below the floor are all genuinely committed.
- **F4c relaxation (live) — the vacuum floor.** An aborted transaction pins WAL
  truncation only *until VACUUM reclaims its on-disk versions*. The WAL manager
  tracks a **vacuum floor** `B`: the boundary below which a FULL VACUUM pass (every
  user table, under the exclusive guard) has reclaimed every aborted-creator tuple
  (heap + index; aborted-creator reclaim has **no age requirement** — §9 F1, so one
  pass reclaims every such tuple it scans). `truncate_before` then stops pinning an
  aborted transaction with id `< B` and lets the floor float past it:
  `pin = represents_transaction(rec) && !is_committed(txn) && !(is_aborted(txn) && txn < B)`.
  This is safe because the transaction has **no surviving on-disk version**, so
  "implicit-committed below floor" is vacuously correct for it. The relaxation is
  gated STRICTLY on a CLOG-recorded `Aborted` status: an in-flight / not-yet-settled
  id below `B` (which cannot occur under the exclusive guard, but is handled
  defensively) is **not** provably reclaimed and still pins. An aborted transaction
  `>= B` still pins.
  - **Computing/advancing `B`.** `B = next_txn_id` captured at the *start* of a full
    pass under the exclusive guard (no id is allocated mid-pass), set as
    `vacuum_floor = max(vacuum_floor, B)` *after* the pass. Only a FULL pass advances
    it (on-demand `VACUUM` with no table, and the checkpoint auto-prune over all
    tables — F4b); a single-table `VACUUM t` does **not** (other tables' aborted
    tuples survive). The catalog is not MVCC-versioned, so user-table tuples are the
    only place aborted-creator versions live.
  - **Durability ordering (the critical invariant).** The vacuum floor is only ever
    *consulted* by `truncate_before`, which a checkpoint runs **after**
    `flush_dirty_pages` + `store.sync_all`. So by the time any `Abort` is dropped, the
    VACUUM's reclamation is fsynced into the heap — auto-prune is reclaimed in the
    *same* checkpoint (it runs before `flush_dirty_pages`); an on-demand full VACUUM's
    dirtied pages are flushed by the *next* checkpoint before that checkpoint's
    `truncate_before`. No `Abort` is ever dropped while its reclaimed tuples are still
    only in memory.
  - **In-memory, reset-at-restart.** The vacuum floor is **not** durable; it resets to
    `FIRST_NORMAL_XID` at every WAL open. That is safe: after a crash the WAL is
    un-truncated again, so truncation is conservative once more (every aborted txn
    pins) until the first post-restart full VACUUM — never less safe, only less
    aggressive — and recovery rebuilds the CLOG from the surviving WAL regardless.
- **(Still deferred to F — durable CLOG file)** A standalone durable CLOG file,
  truncatable below the GC horizon (§9) and coordinated with checkpoint/WAL
  truncation, would let the vacuum floor / implicit-committed boundary survive
  restart. It remains deferred; the in-memory vacuum floor above is the F4c
  mechanism and needs no new durable format.

### 5.5 `StatementContext` and the snapshot type (`crates/common`)

```rust
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Snapshot,          // new
    pub isolation: IsolationLevel,   // new
}

pub struct Snapshot {
    pub xmin: u64,         // lowest still-running xid; below this, status is settled via CLOG
    pub xmax: u64,         // next xid to be assigned; >= xmax is invisible (the future)
    pub xip: Vec<u64>,     // in-progress xids in [xmin, xmax) at snapshot capture
}

pub enum IsolationLevel { ReadCommitted, RepeatableRead /* = snapshot */ }
```

An **active-transaction registry** on `ServerComponents` (the set of in-progress
`txn_id`s) feeds snapshot capture and the GC horizon.

---

## 6. Visibility

A version `v` with creator `xmin = C` and deleter `xmax = D` is **visible** to a
transaction `T` holding snapshot `S` iff:

1. **Creator is visible:** `C` is `T` itself (own write), **or**
   `C < S.xmax ∧ C ∉ S.xip ∧ CLOG[C] = Committed`.
2. **Deleter does not hide it:** `D` is invalid/zero, **or** `D` is *not* visible
   by the same test (the delete is in the future, in-progress to others, or
   aborted), **or** `D` is `T` and the delete happened earlier in `T`'s own
   history under Read Committed.

Hint bits short-circuit the CLOG probe: a version whose `infomask` already records
`XMIN_COMMITTED`/`XMAX_COMMITTED`/`*_ABORTED` is judged without touching CLOG;
the first visitor after a transaction settles sets the hint.

Snapshot acquisition timing is the isolation knob: **Read Committed** captures a
fresh snapshot per statement; **Repeatable Read** captures one snapshot at the
first statement of the transaction and reuses it. The level is selected per
transaction by `BEGIN`/`SET TRANSACTION ISOLATION LEVEL` (Milestone G1, §10), and
a per-connection default is set by `SET SESSION CHARACTERISTICS AS TRANSACTION
ISOLATION LEVEL <level>` (Milestone G2, §10). The precedence for a new
transaction is: **explicit `BEGIN`/`START TRANSACTION ISOLATION LEVEL` > `SET
TRANSACTION` (current txn, before its first query) > session default > Read
Committed**.

---

## 7. Concurrency model and transaction lifecycle

### 7.1 Rollout

- **Stage 1 (Milestones C–D): concurrent readers, serialized writers.** Readers
  capture a snapshot under a brief latch and run lock-free (no
  `ConcurrencyController` guard). Writers serialize by holding the existing
  exclusive guard (`crates/common/src/concurrency.rs`) for the **whole
  write-transaction** (the owned guard is stored on the connection `Session`).
- **Stage 2 (Milestone E): concurrent writers.** *(implemented, E2b.)* The global
  exclusive writer lock is **inverted** into a shared-writer / exclusive-checkpoint
  guard: writers take the **shared** guard (`begin_writer`) and run concurrently,
  the checkpoint takes the **exclusive** guard (`begin_checkpoint`) and drains them.
  Write-write safety comes from the E1 first-updater-wins conflict detection
  (`40001`) and the E2a per-index / per-heap structural latches plus the buffer
  pool's per-frame latches — not from a writer lock. Readers stay lock-free. The
  exclusive checkpoint guard preserves the "no in-flight writer at checkpoint"
  invariant verbatim, so Milestone-D recovery / conservative WAL truncation stays
  correct with no fuzzy checkpoint.

### 7.2 Lifecycle

- `BEGIN` allocates `txn_id`, registers it active, and (per isolation) may capture
  the transaction snapshot.
- Each statement's writes are stamped with the shared `txn_id` and are invisible to
  others until commit.
- `COMMIT` appends a `Commit` record, flushes (fsync), sets `CLOG[t] = Committed`,
  and deregisters the transaction.
- `ROLLBACK` (or any statement error) appends an `Abort` record, sets
  `CLOG[t] = Aborted`, and deregisters; versions become invisible (no page undo).
- **Autocommit** is an implicit `BEGIN ... COMMIT` around one statement, routed
  through the same machinery (generalizing today's `execute_write_bound` in
  `crates/server/src/query.rs`).

### 7.3 Write-write conflicts (Stage 2)

`xmax` doubles as a row lock. A writer re-reads the target version's physical
tuple header immediately before stamping and tentatively stamps `xmax = my_txn`.
Another writer encountering a non-invalid `xmax` it did not stamp itself consults
the other transaction's status:

- **aborted** (`XMAX_ABORTED` hint, or CLOG `Aborted`) → **proceed**: the prior
  lock evaporated, its delete never happened;
- **committed** (`XMAX_COMMITTED` hint, or CLOG `Committed`) → **serialization
  failure**;
- **in-progress** (another live writer holds the lock) → **serialization
  failure** as well.

**Policy decision (fail-fast, first-updater-wins):** SaguaroDB does **not** block
on an in-progress conflict and runs **no deadlock detection**. The first writer to
stamp `xmax` wins; every later writer that finds a committed-or-in-progress lock
aborts immediately with `SqlState::SerializationFailure` (`40001`). (Blocking +
deadlock detection is deferred — §10 Milestone E, §12.) Treating in-progress as a
hard conflict, rather than blocking, is what makes the check pure and lock-free.

The pure classifier is `common::mvcc::write_conflict(xmax, infomask, current_txn,
status) -> WriteConflict` (`Proceed`/`Conflict`); it takes **no snapshot** because
the row lock is an actual-status check, not a snapshot-relative read. It is a
sibling of `version_conflicts`, not a duplicate: `version_conflicts` answers "is
*some* version with this key alive?" (uniqueness, keyed on a candidate's creator);
`write_conflict` answers "may I lock *this* version, or did another txn beat me to
its `xmax`?" (first-updater-wins, keyed on the candidate's deleter).

Concurrent inserts of the same unique key are resolved by the **same status check**
(Milestone E1c). The uniqueness classifier `common::mvcc::classify_unique_conflict(
xmin, xmax, infomask, current_txn, status) -> UniqueConflict`
(`None`/`Violation`/`InFlight`) refines the boolean `version_conflicts` (which is
just `classify != None`) by distinguishing, for an alive candidate, whether its
creator is settled or in-flight:

- **`None`** — the candidate is dead (creator aborted, or committed-deleted /
  deleted-by-me): no conflict.
- **`Violation`** — alive *and* a definite duplicate (creator committed, is
  `current_txn` itself, or frozen/reserved) ⇒ `SqlState::UniqueViolation` (`23505`).
- **`InFlight`** — alive but its creator is **another in-progress transaction** that
  may yet abort, so uniqueness is undecidable ⇒ fail fast with
  `SqlState::SerializationFailure` (`40001`, retry) rather than blocking — the same
  first-updater-wins policy as `write_conflict`.

The engine (`unique_conflict_kind`) returns the strongest conflict across the key's
candidates (precedence `Violation > InFlight > None`). Under serialized writers
(Stage 1) no concurrent uncommitted inserter exists, so the `InFlight` arm never
fires at runtime and a duplicate key still raises `23505` exactly as before; it
becomes load-bearing once writers run concurrently (E2b).

---

## 8. Recovery and durability

*(Implemented in Milestone D — D1 + D2 combined; §10.)*

- **Flush policy** (`crates/server/src/recovery.rs`, `WalFlushPolicy`): the
  `is_committed`/`dirty_txn_id` committedness gate is **dropped**; only the
  WAL-durability gate (`page_lsn ≤ flushed_lsn`) remains. A heap page holds
  versions from several transactions, so page-level committedness is incoherent.
  Uncommitted and aborted dirty pages may now be evicted/flushed — they are
  invisible via the CLOG and reclaimed by VACUUM (§9).
- **Write-ahead on steal**: because the relaxed gate admits *uncommitted* pages,
  whose WAL records are not yet flushed, the buffer pool's eviction (steal) path
  forces the WAL durable (`FlushPolicy::ensure_durable` → `wal.flush`) **before**
  writing a stolen dirty page to its home. The pre-D1 committed-only steal needed
  no such force (a committed page's WAL — including its `Commit` — was already
  durable).
- **Recovery — redo-all**: redo via `replay_from` (not the retired
  `replay_committed_from`), applying every PHYSICAL redo record
  (heap/index/header/full-page-image) under PageLSN gating, regardless of the
  dirtying transaction's outcome. The CLOG — rebuilt from the durable
  `Commit`/`Abort` records as the WAL opens — decides visibility afterwards. Any
  transaction with neither a durable `Commit` nor `Abort` at crash (in-flight at
  crash) is treated as **Aborted**. There is **no undo pass**.
  - **Logical catalog records** (`CreateTable`/`DropTable`/`CreateIndex`/
    `DropIndex`) are the one exception: they mutate the durable catalog directly
    (not idempotent PageLSN-gated page bytes), so redo gates them by the rebuilt
    CLOG — only a *committed* DDL replays. DDL is non-transactional and commits
    immediately (§4 Decision 6), so an aborted/in-flight DDL is skipped; its
    index/heap pages may still replay harmlessly as unreferenced, invisible orphan
    pages.
- **Checkpoint** ordering is unchanged in shape (`crates/server/src/checkpoint.rs`):
  `wal.flush` → `flush_dirty_pages` → `store.sync_all` → control record →
  `Checkpoint` marker → `truncate_before` → `mark_all_clean`. (`flush_committed_pages`
  is renamed `flush_dirty_pages`: it now spills committed, aborted, and — under
  Stage 2 — in-flight dirty pages alike, since all are WAL-durable once `wal.flush`
  has run and the CLOG hides the non-committed ones.)
- **Conservative WAL truncation / `committed_floor`** (the critical D guard; see
  §5.4): truncation never drops a transaction that is not durably committed
  (aborted or in-flight) **and whose on-disk versions are not provably reclaimed**,
  and the implicit-committed floor never crosses such a transaction. Otherwise an
  aborted transaction's flushed-but-now-orphan versions, with its `Abort` record
  truncated and the floor floated above it, would read as *committed* after restart
  — corruption. **F4c relaxation (live):** an aborted transaction below the **vacuum
  floor** `B` (a full VACUUM pass reclaimed every aborted-creator tuple `< B`, made
  durable before the truncation that consults `B`) no longer pins, because it has no
  surviving version to resurrect; see §5.4.
- **Consequence**: after a crash the heap may contain flushed-then-aborted/dead
  versions. This is correct (CLOG hides them; VACUUM reclaims them). Heap
  cleanliness is a VACUUM responsibility, not a recovery responsibility.

---

## 9. Garbage collection (VACUUM)

Required for bounded space — and more urgent than under a single-entry-index
design, because index entries accumulate per version as well as heap tuples.

- **Horizon**: the **minimum `xmin` advertised by any currently-live snapshot**
  (`ActiveTxnRegistry::oldest_xmin`), or — when no snapshot is advertised — the next
  id to be assigned (`next_txn_id`); nothing older than the future can be needed.
  Captured **once** at the start of a VACUUM pass (`ServerComponents::gc_horizon`,
  F1). It only advances as snapshots are released; a concurrent `BEGIN`/capture can
  only advertise an `xmin >= horizon` once any already-finished transaction is
  settled-past (see the race-free argument below), so it never lowers the captured
  horizon.
  - **Why not the oldest active transaction id.** VACUUM's committed-delete branch
    reclaims a version when `xmax < horizon`, which is safe only when
    `horizon <= every live snapshot's xmin`. A snapshot freezes its `xmin` at
    capture (`xmin = oldest active id then`, or `next_txn_id` if none) for its whole
    life, and `snapshot.xmin <= the capturing txn's own id`. As the then-oldest
    transaction deregisters, the active-id minimum (`oldest()`) advances **above** a
    still-live snapshot's frozen `xmin`. A version with committed `xmax = X` where
    `X` is in that snapshot's `xip` is seen **live** by the snapshot
    (`txn_effect_visible` treats an `xip` deleter as not-effective), so reclaiming
    it (`horizon > X`) would lose a row the snapshot still scans. Worst case, an
    **autocommit `SELECT` is not its own transaction and never registers in the
    active set at all**, so the oldest-active-id rule ignores its snapshot entirely
    — a single long `SELECT` + a concurrent commit-of-a-`DELETE` + a VACUUM pass
    would lose a row. The min advertised `xmin` is always `<= oldest()`, so it is
    strictly safer and never reclaims anything the oldest-active-id rule retained.
  - **Advertisement**: every snapshot capture (`capture_snapshot`, including the
    autocommit-read path) publishes its `xmin` into a refcounted multiset
    (`xmins: BTreeMap<xmin, count>`) under the **same** registry latch that reads the
    active set, and holds an RAII `AdvertisedSnapshot` guard for exactly the
    snapshot's usable lifetime (its `Drop` releases the advertisement under the
    latch). Read Committed advertises per statement (released at statement end);
    Repeatable Read advertises once for the transaction (held on the `Transaction`,
    released at commit/abort); the autocommit read/write paths advertise across the
    statement's execution.
  - **Race-freedom** (capture vs. horizon): at the instant `gc_horizon` reads the
    min advertised `xmin` `H` under the registry latch, every snapshot that is live
    OR being captured has `xmin >= H` or is not-yet-usable. A capture publishes
    `xmins[xmin]++` in the *same* latched critical section that reads `active`, and
    the snapshot is not returned/usable until that section completes; `gc_horizon`
    reads `oldest_xmin()` under the same latch, so the mutex total order leaves no
    window where the horizon exceeds a usable snapshot's `xmin`. A snapshot published
    **after** the horizon read derives its `xmin` from an `active`/`next_txn_id`
    state in which any txn already gone-from-active — i.e. any committed deleter the
    horizon could have reclaimed — is settled-past, so that later snapshot's `xmin`
    is above any reclaimed `xmax` and it cannot see a reclaimed version live. This
    mirrors the existing `register_allocated`/capture latch discipline that closes
    the torn-snapshot window.
- **Reclaimability** (`common::is_dead_to_all(xmin, xmax, infomask, horizon,
  status)`, F1): a version is *dead to everyone* — safe to physically reclaim —
  iff **either** its **creator aborted** (`XMIN_ABORTED` hint, or
  `status(xmin) == Aborted`) **or** it is **committed-deleted below the horizon**
  (`xmax != 0` **and** the delete is settled-committed via the `XMAX_COMMITTED`
  hint or `status(xmax) == Committed` **and** `xmax < horizon`, strict). This is
  the VACUUM-side **sibling of `is_visible`**: `is_visible` answers "visible to
  **my** snapshot?"; `is_dead_to_all` answers "invisible to **everyone**, now and
  forever?" — over a single scalar `horizon` that summarizes every live snapshot.
  - **The asymmetry** — the aborted-creator branch has **no age requirement** (an
    aborted creator is universally invisible, so its `xmin` need not be below the
    horizon), whereas the committed-delete branch **requires `xmax < horizon`** (a
    delete at or above the horizon may still be in some live snapshot's
    future/in-progress set, which therefore still sees the row as live). A live
    committed version (`xmax == 0`), an aborted-deleter, or an in-progress-deleter
    is never reclaimable; a committed delete with `xmax >= horizon` is not *yet*
    reclaimable. The predicate is pure and honours the same `infomask` hint bits as
    `is_visible` to skip CLOG probes.
- **Heap prune** (intra-page, `storage::vacuum_heap`, F2b): for every heap page of
  a table — scanning the **full extent** `0..page_count` (resident *and* evicted
  pages, via `BufferPool::page_count`, so an evicted dead tuple is never missed),
  not just resident pages — classify each `NORMAL` tuple with
  `is_dead_to_all(horizon)` (decoding its `xmin`/`xmax`/`infomask` and settling
  against the live CLOG), mark the dead-to-all line pointers `DEAD`, and compact the
  surviving tuples (`page::prune_and_compact`; this finally adds the page compaction
  that `page.rs` lacks today — `DELETE` is currently a non-reclaiming tombstone). Per
  page the pass takes the per-heap structural latch then the frame write latch (lock
  order structural → frame → WAL), released before the next page. Each pruned page is
  logged as a **single unconditional `FullPageImage`** (a compaction relocates
  survivors and is not expressible as a delta), so recovery reinstalls the compacted
  page byte-for-byte by PageLSN gating; a page with no dead tuples is skipped (no WAL,
  no mutation). Survivors stay byte-identical at their stable slot ids, so no index
  entry is touched. The pass runs under the maintenance txn id (`0`) so its
  reclamation is never undone by an abort, and returns the reclaimed dead TIDs (fed to
  index vacuum). It does **not** reclaim line pointers `DEAD → UNUSED` (that is the
  separate step below). Called by the live VACUUM orchestration (F4a, below) as its
  first phase.
- **Index vacuum** (`storage::vacuum_indexes(schema, dead_tids)`, F3a): remove the
  dangling index entries `vacuum_heap` left behind — for the table's primary-key
  index **and every live secondary index**, delete every entry whose value (the heap
  TID) is in `dead_tids`. Entries are matched by **dead-TID membership, not by key**:
  the heap prune already compacted the page, so the dead tuple's key bytes are gone
  and the key cannot be recomputed; the leaf's stored TID is the only handle left.
  Each index is vacuumed in a single leaf-chain walk (`BTree::remove_values_in`),
  shifting matching entries out of each leaf under its frame write latch and logging a
  `FullPageImage` of every changed leaf (the `btree::log_full_page` pattern, redone by
  PageLSN gating regardless of txn id), under the maintenance txn id (`0`). Each index
  is vacuumed under **its own** per-index structural latch (never two at once). It is
  **B-link-safe against concurrent lock-free scanners**: no leaf is merged or freed and
  no right-sibling link is rewritten (an emptied leaf is left in place — accepted
  bloat), and the per-leaf write latch is mutually exclusive with a reader's per-leaf
  read latch, so a concurrent scanner can neither miss nor duplicate a live entry. It
  does **not** reclaim line pointers (the next step). Called by the F4a orchestration
  as its middle phase.
- **Line-pointer reclaim** (`storage::reclaim_line_pointers(schema, dead_tids)`,
  F3b): flip each `dead_tid`'s heap line pointer `DEAD → UNUSED`, freeing its slot
  id for reuse. The TIDs are grouped by heap page and each page is rewritten once
  under the per-heap structural latch then the frame write latch (lock order
  structural → frame → WAL, released before the next page), logged as a single
  unconditional `FullPageImage` under the maintenance txn id (`0`); recovery
  reinstalls it by PageLSN gating. **This MUST run only after `vacuum_indexes`
  (F3a) removed every index entry for these TIDs** — the F2b → F3a → F3b ordering
  is the safety hinge for slot reuse below. `insert_row` recycles the lowest
  `UNUSED` slot id before appending a fresh one (bounding the slot array under
  churn), reusing **`UNUSED` only, never `DEAD`**: a `DEAD` slot may still have a
  dangling index entry, whereas the ordering guarantees an `UNUSED` slot has none,
  so the recycled slot cannot let a stale index entry resolve to the new tuple
  (silent corruption). A reclaim (FPI: slot → `UNUSED`) followed by a later
  insert-into-reused-slot (`HeapInsert`) replays in LSN order to the final state.
  Called by the F4a orchestration as its final phase.
- **Orchestration** (`storage::PageBackedStorageEngine::vacuum(schema, horizon)`,
  F4a): the live entry point that ties the three phases together **in their mandatory
  order** — `vacuum_heap` (F2b) → `vacuum_indexes` (F3a) → `reclaim_line_pointers`
  (F3b) — on one set of dead TIDs, returning the count reclaimed. The order is the
  safety invariant: index entries for a TID must be gone (F3a) *before* its line
  pointer is reclaimed to `UNUSED` (F3b), or `insert_row`'s slot reuse could resolve a
  stale index entry to the new tuple. When the heap prune finds nothing dead, the
  index and line-pointer phases are skipped.
- **The `VACUUM` SQL command** (server, F4a): `VACUUM` (every user table) or
  `VACUUM <table>` (one table; errors if it does not exist). Classified
  `StatementClass::Maintenance` — it does **not** bind or plan — and rejected inside an
  explicit transaction block (like DDL, with `SqlState::FeatureNotSupported`, since
  `VACUUM` is non-transactional). `QueryService::run_vacuum` resolves the target
  table(s), then acquires the **exclusive** checkpoint guard (`begin_checkpoint`) for
  the whole pass, captures `gc_horizon()` **once, after the guard is held**, and calls
  `engine.vacuum(schema, horizon)` for each target; the command tag is `VACUUM`.
  **No data loss (the horizon-under-the-guard argument):** under the exclusive guard no
  writer runs, so no committed-deleter appears mid-pass; and the horizon — captured
  after acquiring the guard — is the minimum `xmin` advertised by any live snapshot,
  **including lock-free readers** (which advertise their `xmin`, §9). Every reclaimed
  version has `xmax < horizon`, i.e. its delete committed before every live snapshot's
  `xmin`, so no current snapshot can see it live, and any reader that starts mid-pass
  freezes `xmin >= horizon` (the deleter is in its settled past). Capturing the horizon
  *after* the guard is load-bearing: a concurrent writer cannot then advance it, and it
  already accounts for every reader advertised at that instant. VACUUM therefore never
  reclaims a version any snapshot needs. This is exactly why the GC-horizon fix
  (minimum advertised `xmin`, not oldest active id) had to land before VACUUM went live.
- **Triggering**: an on-demand `VACUUM` command (F4a, live) **plus auto-prune folded
  into the checkpoint behind a threshold** (F4b, live). A server-wide counter
  (`ServerComponents::dead_rows_since_vacuum`) accumulates committed dead versions —
  each committed `DELETE` row and each committed `UPDATE` row is one dead version, added
  on a successful, durable commit only (never on abort). When a checkpoint runs and the
  count reaches `config.auto_vacuum_dead_rows` (CLI `--auto-vacuum-dead-rows`, default
  `10000`; `0` disables auto-prune), the checkpoint captures `gc_horizon()` **under the
  exclusive guard it already holds** and runs the F4a orchestration over every user
  table **before** flushing dirty pages — so the vacuum's pages and full-page images are
  made durable by that same checkpoint — then resets the counter. This bounds heap +
  index space under sustained churn with no operator action. It inherits F4a's
  no-data-loss safety verbatim (horizon captured under the guard; only versions no live
  snapshot can see are reclaimed). Opportunistic pruning during scans is deferred.
- **F4c — WAL-truncation relaxation for reclaimed aborts (live).** A FULL VACUUM pass
  (on-demand `VACUUM` with no table, or the auto-prune over all tables) advances the WAL
  **vacuum floor** `B = next_txn_id` captured under the guard at the start of the pass.
  Because aborted-creator reclaim has no age requirement, the pass reclaims every
  aborted-creator tuple (heap + index) below `B`, so `truncate_before` may then drop
  those aborted transactions' `Abort` records and float the implicit-committed floor
  past them — they have no surviving on-disk version, so it is vacuously committed-
  below-floor (§5.4). The floor is in-memory (reset-at-restart, conservative again until
  the first post-restart full VACUUM) and consulted only after the reclamation is durable
  (the checkpoint flushes+fsyncs dirty pages before `truncate_before`). A single-table
  `VACUUM t` does NOT advance it. A *durable CLOG file* (which would carry the floor
  across restart) remains deferred (§5.4).

---

## 10. Sequenced implementation plan

Each milestone leaves the system **correct and shippable**; each sub-step is
roughly one commit (per the repository's incremental-commit cadence). Touch-points
reference current files.

### Milestone A — Foundations *(single-writer/autocommit; behavior unchanged)*

- **A1 — Row format v2.** Widen the tuple header in `storage/src/codec.rs` (§5.1);
  the one durable break. `decode_row` handles v1+v2; insert stamps
  `xmin`/`xmax`/`t_ctid`. Reserve `infomask` hint/HOT bits.
- **A2 — `common` types.** `Snapshot`, `TxnStatus`, `IsolationLevel`; extend
  `StatementContext` (unused fields for now).
- **A3 — CLOG + `Abort` record + active-txn registry.** Add `WalRecordKind::Abort`;
  build the in-memory status map (`Clog`, rebuilt at recovery from `Commit`/`Abort`
  WAL records — the durable CLOG file is deferred to F per §5.4) that supersedes
  `committed_txns` as authority; add the active-txn registry on `ServerComponents`;
  route the existing autocommit commit/rollback through CLOG (rollback now also
  appends an `Abort` record, unflushed). Autocommit behavior is unchanged.

### Milestone B — Index-per-version storage model *(single-writer/autocommit; MVCC-correct internally)*

*Commit-by-commit breakdown: Appendix A.*

- **B1 — Line-pointer formalization.** Slot states `NORMAL/DEAD/UNUSED` (§5.2);
  the index→stable-line-pointer contract; `HeapUpdateHeader` WAL record + redo. No
  behavior change (still one version per row).
- **B2 — Multi-entry, uniform indexes.** Rework `storage/src/btree.rs` to allow
  duplicate keys (ordered by `(key, tid)`, value = TID); make all indexes point at
  heap TIDs; **convert secondary indexes from secondary→PK to secondary→heap-TID**
  (rewrites `engine.rs` `index_scan` / secondary-key handling — migrates the merged
  secondary-index feature, with spec updates). PK uniqueness becomes a
  visibility-aware check (trivial here).
- **B3 — Visibility + snapshot threading.** Thread the snapshot into
  `StatementContext`; add the visibility predicate (§6) at the heap materialization
  sites (`engine.rs` `read_location`, `scan_range`/`index_scan` loops); index scans
  yield candidate TIDs, the heap visibility-checks each and sets hint bits. Replace
  "index points at a dead row ⇒ error" with "skip invisible." Autocommit snapshot
  sees all committed → behavior unchanged.
- **B4 — Versioning UPDATE/DELETE.** UPDATE: insert the new version, stamp the old
  tuple's `xmax` + `t_ctid→new`, insert per-version index entries for the new
  tuple into all indexes, retain old entries. DELETE: stamp `xmax` in place, retain
  index entries. Uniqueness consults visibility.

### Milestone C — Multi-statement transactions *(concurrent readers, serialized writers)*

- **C1 — Transaction-control SQL.** `BEGIN`/`COMMIT`/`ROLLBACK` arms in
  `parser/convert.rs` (before the catch-all) + internal `Statement` variants
  (sqlparser 0.56 already yields the nodes).
- **C2 — Session state + protocol status byte.** Add `tx: TransactionState` to
  `Session` (`server/src/connection.rs`); make `ReadyForQuery` carry `'I'/'T'/'E'`
  (today hardcoded `'I'` at `protocol/src/codec.rs`), supplied from the session.
- **C3 — Lifecycle + concurrency relaxation** *(implemented)*. Generalize the
  query path (`server/src/query.rs`) into a real transaction lifecycle, with
  autocommit routed through the same machinery as an implicit single-statement
  transaction:
  - **Lifecycle.** `BEGIN` allocates a `txn_id`, registers it active, sets the
    session to `'T'`, and (per isolation) sets up the snapshot policy. Statements
    inside the block share that `txn_id`; writes are stamped with it and reads use
    the transaction's snapshot. `COMMIT` = append `Commit` → `flush` (fsync) →
    `CLOG=Committed` (set at flush) → post-durable cleanup → deregister → `'I'`.
    `ROLLBACK` (or any statement error) = append `Abort` → `CLOG=Aborted` →
    before-image undo → deregister. A statement error poisons the block to the
    `'E'` failed state; while `'E'`, every statement except `COMMIT`/`ROLLBACK` is
    rejected with `25P02` (`SqlState::InFailedSqlTransaction`). `COMMIT` of an
    `'E'` block issues `ROLLBACK` (Postgres behavior). `BEGIN` inside a block is a
    no-op warning that stays `'T'`; `COMMIT`/`ROLLBACK` with no open block are
    no-op warnings that stay `'I'`. The open transaction is held on the connection
    `Session`; a client disconnect aborts it (releasing the write guard and the
    registry entry).
  - **Stage-1 concurrency.** Readers run lock-free: a read-only statement/
    transaction takes **no** `ConcurrencyController` guard. It captures its
    snapshot under the active-transaction-registry latch (so the snapshot is not
    torn relative to `next_txn_id`; id allocation and registration are done under
    the same latch) and reads via the buffer pool's per-frame latches, skipping an
    in-flight writer's uncommitted versions by MVCC visibility. Writers serialize:
    a write transaction acquires the existing exclusive write guard **lazily** on
    its first write statement and holds the owned guard on the `Session` for the
    whole write-transaction, releasing it at `COMMIT`/`ROLLBACK`/disconnect. A
    read-only explicit transaction never takes the write guard, so it stays
    concurrent. Autocommit write = acquire for the one statement, release at the
    implicit commit. DDL takes the exclusive guard and commits immediately
    (non-transactional, §4 Decision 6) and is **rejected inside an explicit
    transaction block**. This is Stage 1: many readers concurrent with at most one
    writer; concurrent writers and write-write conflict detection are Milestone E.
  - **Snapshot per isolation.** Default Read Committed captures a fresh snapshot at
    the start of each statement; Repeatable Read captures one snapshot at the first
    statement and reuses it. The snapshot is shared via `Arc` so the executor does
    not deep-clone the (now-possibly-non-empty) `xip` vector per scan operator.
  - **Before-image undo is retained through C3** (see §11 and D1 below): `ROLLBACK`
    /abort still uses `buffer_pool.rollback(txn)` plus the `Abort` record +
    `CLOG=Aborted`. Retiring it requires the relaxed flush gate, so it moves to D1.

### Milestone D — Recovery & durability rework *(implemented; D1 + D2 in one commit)*

D1 and D2 are **entailed together** (§4 Decision 4) and were landed as a single
commit: relaxing the flush gate (D1) lets uncommitted/aborted pages reach disk,
and only redo-all + CLOG-visibility recovery (D2) can correctly hide them after a
crash; before-image undo cannot coexist with the relaxed gate (it cannot un-flush
an already-evicted page).

- **D1 — Relax flush policy** (§8), **and retire the buffer-pool before-image
  undo** (abort becomes pure invisibility). The flush gate is relaxed to
  WAL-durability only, which lets aborted/uncommitted dirty pages be
  flushed/evicted (the steal path forces the WAL first — §8). Before-image undo is
  removed: `record_before_image`/`rollback`-restore and their `BeforeImage` storage
  are gone from `crates/buffer`; `ROLLBACK`/statement-error-abort is now `Abort`
  record + `CLOG = Aborted` + deregister, with **no page undo**. A rolled-back
  transaction's pages (modified or freshly allocated) stay resident as
  dirty-but-evictable frames, hidden by the CLOG, and match what redo-all replays.
- **D2 — Redo-all recovery** + CLOG visibility: `replay_from` applies every
  physical record under PageLSN gating; the CLOG (built from `Commit`/`Abort` at
  WAL open) decides visibility; in-flight-at-crash = aborted. Logical catalog
  records are CLOG-gated (§8).
- **Conservative truncation / floor** (§5.4): truncation never crosses an
  aborted/in-flight transaction and the floor never marks one implicitly committed
  (the critical correctness guard). Aggressive truncation past aborted
  transactions is **deferred to Milestone F**, once VACUUM reclaims aborted
  versions. Crash tests cover checkpoint boundaries, the committed-via-floor path,
  torn pages, eviction of uncommitted/aborted pages, and the aborted-across-
  checkpoint invisibility guarantee.

*A–D = MVCC MVP: snapshot reads + multi-statement transactions + serial writers +
correct recovery. Correct, but bloats heap and indexes until F.*

### Milestone E — Concurrent writers + conflict detection

Commit breakdown (confirmed). E1 introduces fail-fast write-write conflict
detection (§7.3); E2 replaces the global writer lock with finer structural latches
plus a checkpoint-coordination guard.

- **E1a — SQLSTATE + pure predicate.** Add `SqlState::SerializationFailure`
  (`40001`, wire-mapped in `crates/server/src/connection.rs`) and the pure
  `common::mvcc::write_conflict(xmax, infomask, current_txn, status) ->
  WriteConflict` classifier (`Proceed`/`Conflict`) with table-driven tests. No
  engine wiring yet.
- **E1b — UPDATE/DELETE conflict checks.** Wire `write_conflict` into the
  update/delete locating path: re-read the target version's physical header,
  classify, and on `Conflict` abort the statement with `40001` (fail-fast,
  first-updater-wins; §7.3).
- **E1c — Concurrent-inserter unique conflicts.** Apply the same fail-fast policy
  to two transactions racing to claim the same unique key, surfacing `40001`
  (rather than blocking) on the conflicting index entry's tuple.
- **E2a — Structural write latches.** Replace the single global writer lock with
  **per-index and per-heap-file** structural write latches. A fully-concurrent
  B-tree is **deferred**: the current B-tree split protocol has no latch coupling
  (no B-link/right-link hand-over-hand), so a per-index latch is the correct
  granularity for now.
- **E2b — Shared-writer / exclusive-checkpoint guard (the lock inversion).**
  *(implemented.)* Invert the existing exclusive writer lock into a shared-writer /
  exclusive-checkpoint guard: writers take the shared guard (`begin_writer`) and run
  concurrently; the checkpointer takes the exclusive guard (`begin_checkpoint`),
  draining all writers and running alone. This turns the E1 conflict detection and
  the E2a per-index / per-heap structural latches into load-bearing, contended
  mechanisms. It **preserves the "no in-flight writer at checkpoint" invariant** that
  Milestone-D recovery/truncation relies on (conservative truncation never crosses
  an in-flight writer — §8, §5.4), so recovery stays correct without a fuzzy
  checkpoint. The buffer pool's steal-eviction is made concurrency-safe under
  overlapping writers (an `evicting` frame is never handed out, so a steal can never
  flush a stale snapshot of a frame a writer is concurrently modifying; page-number
  allocation and the extent seed are pool-lock-atomic).

**Deferred from Milestone E** (§12): the true concurrent / B-link writer protocol
(latch-coupled, fully-concurrent B-tree); blocking + deadlock detection (instead
of fail-fast `40001`); fuzzy checkpoint (checkpointing with writers in flight);
and per-tuple CLOG-probe contention reduction.

### Milestone F — VACUUM / GC *(near-MVP in this model)*

- **F1 — Horizon + reclaimability predicate.** The pure
  `common::is_dead_to_all(xmin, xmax, infomask, horizon, status)` reclaimability
  oracle (sibling of `is_visible`; aborted-creator any age **or** committed-delete
  `< horizon`) and the `ServerComponents::gc_horizon` accessor (the **minimum
  advertised snapshot `xmin`** via `ActiveTxnRegistry::oldest_xmin`, else
  `next_txn_id` — not the oldest active id; see §9 Horizon). No engine wiring yet —
  runtime no-op. **F2 — Heap prune + compaction.** **F3 — Index vacuum +
  line-pointer reclaim.** **F4a — On-demand `VACUUM` (live).** `engine.vacuum`
  orchestrates F2b → F3a → F3b in order; the `VACUUM [table]` command
  (`StatementClass::Maintenance`, parsed before sqlparser, rejected in a transaction
  block) runs under the exclusive checkpoint guard with the GC horizon captured once
  after the guard — the first real reclamation behavior change (§9). **F4b —
  auto-prune at checkpoint (live).** A checkpoint folds a VACUUM pass over every user
  table into itself when `dead_rows_since_vacuum >= --auto-vacuum-dead-rows` (committed
  dead versions since the last auto-prune; default `10000`, `0` disables), under the
  guard it already holds, with the horizon captured under that guard and the vacuum run
  before `flush_dirty_pages` so its pages/FPIs are durable that checkpoint — bounding
  space without operator `VACUUM`, with F4a's no-data-loss safety. **F4c —
  WAL-truncation relaxation for reclaimed aborts (live).** A full VACUUM pass advances
  the in-memory WAL **vacuum floor** `B` (= `next_txn_id` at the start of the pass,
  captured under the guard); `truncate_before` then stops pinning — and floats the
  implicit-committed floor past — aborted transactions `< B`, whose on-disk versions the
  pass reclaimed, while still pinning in-flight or un-vacuumed aborts. The reclamation is
  durable before the truncation that drops the `Abort` (checkpoint flush+fsync precedes
  `truncate_before`); the floor is reset-at-restart (conservative again until the first
  post-restart full VACUUM). A durable CLOG file remains deferred (§5.4).

### Milestone G — Isolation levels & polish

Read Committed (per-statement snapshot) vs Repeatable Read (per-transaction
snapshot); `'E'` failed-transaction handling; serialization-failure surfacing;
savepoints via sub-transaction xids (optional, deferred).

- **G1 — transaction-scoped isolation SQL.** *(implemented.)*
  `BEGIN [TRANSACTION] ISOLATION LEVEL <level>`,
  `START TRANSACTION ISOLATION LEVEL <level>`, and
  `SET TRANSACTION ISOLATION LEVEL <level>` are parsed and applied to the current
  transaction, **activating Repeatable Read** (the per-transaction snapshot,
  advertisement, and write-conflict machinery were built in C–F and were dormant
  until now — G1 is only the SQL + wiring that selects the level).
  - **Four SQL levels → two.** SaguaroDB has two levels, so the four standard SQL
    levels are mapped: `READ UNCOMMITTED` → **Read Committed** (we never expose
    uncommitted data; the weaker request is strengthened to our weakest);
    `READ COMMITTED` → **Read Committed**; `REPEATABLE READ` → **Repeatable Read**;
    `SERIALIZABLE` → **Repeatable Read**. SERIALIZABLE is an **alias** for snapshot
    isolation (Repeatable Read): we do **not** implement SSI / predicate-based
    serializability, so a SERIALIZABLE transaction gets a stable per-transaction
    snapshot but no serialization-anomaly prevention beyond write-write conflicts.
    The non-standard `SNAPSHOT` level also maps to Repeatable Read.
  - **`BEGIN`/`START TRANSACTION` isolation** is read at BEGIN: an explicit
    `ISOLATION LEVEL` mode sets `Transaction.isolation`; with no mode the
    transaction inherits the **session default** (`Session.default_isolation`, Read
    Committed unless raised by `SET SESSION CHARACTERISTICS` — see G2). An explicit
    level on a `BEGIN` issued **inside** an already-open block is ignored (Postgres:
    there is already a transaction in progress).
  - **`SET TRANSACTION ISOLATION LEVEL`** sets the **current** transaction's level
    and is valid **only before the transaction's first query** (i.e. before its
    snapshot was captured). After the first statement it errors with
    `SET TRANSACTION ISOLATION LEVEL must be called before any query`
    (`FeatureNotSupported`), which — like any in-block statement error — poisons the
    block to `'E'`. The guard is the `Transaction.first_statement_ran` flag, set
    when a data/query statement captures the transaction snapshot. Inside an
    already-failed (`'E'`) block it is rejected with `25P02`
    (`InFailedSqlTransaction`) like any non-COMMIT/ROLLBACK statement. With **no**
    open transaction (autocommit) `SET TRANSACTION` is a no-op success that stays
    `Idle`
    (the implicit single-statement transaction runs no query for the level to
    affect; Postgres warns and no-ops — we mirror the no-op).
  - **Access modes.** `READ WRITE` (the default) is accepted and ignored — v1 is
    always read-write. `READ ONLY` is **rejected** (`SyntaxError`) rather than
    silently ignored, since v1 enforces no read-only restriction and accepting it
    would be misleading. `[NOT] DEFERRABLE` is not parsed by sqlparser 0.56 in this
    position and so is already an upstream parse error.
  - **Write conflicts under RR.** No new machinery: a Repeatable Read transaction
    that writes a row another transaction changed and committed **after** its
    snapshot hits the existing first-updater-wins detection and surfaces `40001`
    (`SerializationFailure`), exactly as a concurrent autocommit conflict does.
- **G2 — session-default isolation.** *(implemented.)*
  `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` sets a
  **per-connection default** isolation (`Session.default_isolation`, default Read
  Committed) used by FUTURE transactions. It reuses G1's four-to-two level mapping
  (`READ UNCOMMITTED`/`READ COMMITTED` → Read Committed; `REPEATABLE READ`/
  `SERIALIZABLE`/`SNAPSHOT` → Repeatable Read) and the same access-mode handling
  (`READ WRITE` accepted-and-ignored, `READ ONLY` rejected at parse time).
  - **Inheritance precedence** for a new transaction: explicit
    `BEGIN`/`START TRANSACTION ISOLATION LEVEL` > `SET TRANSACTION` (current txn,
    before its first query) > **session default** > Read Committed. A plain `BEGIN`
    with no explicit level reads `Session.default_isolation`
    (`begin_transaction(isolation.unwrap_or(session_default))`); an explicit level
    overrides it for that one transaction.
  - **Does not change the current open transaction.** Unlike `SET TRANSACTION`,
    `SET SESSION CHARACTERISTICS` has no before-first-query rule and is allowed
    inside a transaction block; it updates the session default for FUTURE
    transactions only and leaves an already-open transaction's `isolation`
    unchanged (Postgres-compatible). With no isolation-level mode (e.g. `READ WRITE`
    only) it is a no-op success. Inside an already-failed (`'E'`) block it is
    rejected with `25P02` (`InFailedSqlTransaction`) like any non-COMMIT/ROLLBACK
    statement, leaving the default unchanged.
  - **Persistence and reset.** The default persists across transactions on the
    connection (it is threaded in/out of the query path beside the transaction
    slot) and resets to Read Committed for each new connection (the field is
    per-`Session`).
  - **Autocommit.** A single autocommit statement has exactly one snapshot, so
    Read Committed vs Repeatable Read is functionally moot for it; the session
    default is not threaded into the autocommit single-statement snapshot path.

### Milestone H — HOT *(deferred, purely additive)*

- **H1** `REDIRECT` line pointers + root-line-pointer indexing. **H2** HOT-update
  fast path (same-page + no indexed-column change ⇒ heap-only tuple, no new index
  entries; index points at the root). **H3** HOT pruning folded into page access
  and VACUUM. Reuses A–G unchanged.

### Unlocks summary

| Milestone | Unlocks | Concurrency |
|---|---|---|
| A | Format + types + CLOG | single writer |
| B | Index-per-version, line pointers, visibility, version chains | single writer |
| C | `BEGIN/COMMIT/ROLLBACK`, snapshot reads | concurrent readers, serial writers |
| D | Large txns, MVCC-correct recovery | concurrent readers, serial writers |
| **A–D** | **MVCC MVP** | — |
| E | True write concurrency | concurrent writers |
| F | Bounded space (VACUUM) | — |
| G | Isolation levels, savepoints | — |
| H | HOT | additive optimization |

---

## 11. Cross-cutting concerns

- **Spec updates.** Every phase touching a durable format or public contract
  updates `docs/specs/overview.md` and the relevant crate spec in the same change.
  The format break is confined to A1.
- **Secondary-index migration.** B2 changes secondary indexes from secondary→PK to
  secondary→heap-TID and re-specs the merged secondary-index feature.
- **Executor identity.** `RowId` becomes `(page, line-pointer)`, stable across
  intra-page compaction; UPDATE/DELETE target by TID.
- **Before-image retirement.** **D1 (implemented)** removed the buffer-pool
  before-image rollback; abort is now status-based. `record_before_image`, the
  before-image `rollback` restore, the `BeforeImage` storage, and
  `restore_dirty_state` are gone from `crates/buffer/src/pool.rs`; the buffer
  pool's `rollback` is now a bookkeeping clear that reclaims nothing (a rolled-back
  transaction's pages stay dirty-but-evictable, hidden by the CLOG, matching
  redo-all). The server abort path (`crates/server/src/query.rs`) appends `Abort` +
  sets `CLOG = Aborted` + deregisters; the `storage.rollback_txn` it still calls is
  DDL *metadata* restoration (table/index schema shadow state for a failed
  in-unit CREATE/DROP), not page undo. (Originally sequenced in C3, but retiring it
  required the relaxed flush gate — otherwise aborted dirty pages stay unflushable
  under the `is_committed` gate and pin the buffer pool, a liveness bug. §4
  Decision 4 entails the flush-gate relaxation and abort-as-invisibility together,
  so retirement belonged with D1.) Before-image retirement is a hard prerequisite
  for E (one before-image per page cannot serve concurrent writers).
- **Error codes.** C3 adds `SqlState::InFailedSqlTransaction` (`25P02`): the `'E'`
  transaction state rejects all but `COMMIT`/`ROLLBACK` with it. Milestone E adds
  `SqlState::SerializationFailure` (`40001`).
- **WAL additions.** `Abort`, `HeapUpdateHeader` (§5.3); recovery handles both.

---

## 12. Deferred / future work

- **Concurrent / B-link writer protocol** — a latch-coupled, fully-concurrent
  B-tree; deferred from Milestone E (E2a takes per-index structural latches
  instead, because the current split protocol has no latch coupling).
- **Blocking + deadlock detection** — wait-for-lock with cycle detection, instead
  of Milestone E's fail-fast first-updater-wins `40001` (§7.3).
- **Fuzzy checkpoint** — checkpointing with writers in flight; Milestone E keeps
  the "no in-flight writer at checkpoint" invariant via the shared-writer /
  exclusive-checkpoint guard (E2b), so Milestone-D recovery/truncation stays
  correct.
- **Per-tuple CLOG-probe contention** — reducing repeated CLOG probes on hot
  tuples (beyond the `infomask` hint bits) under concurrent writers.
- **HOT** — Milestone H (above); the baseline is built for it.
- **Transactional DDL** — requires catalog MVCC + transactional file lifecycle;
  additive later, does not invalidate data MVCC.
- **Serializable (SSI)** — layer predicate/SIREAD tracking on snapshot isolation.
- **Savepoints / sub-transactions** — sub-transaction xids + CLOG; no undo needed.
- **Time-travel / as-of** — would motivate adding commit timestamps (Decision 1
  leaves the door open; versions already carry `xmin`/`xmax`).

---

## 13. Open questions (to settle during implementation)

- Exact `infomask` bit layout and which hints are set eagerly vs lazily.
- Whether/when to commit to HOT (Milestone H) — affects how aggressively B2 invests
  in root-line-pointer structure vs plain per-version entries.
- Index-vacuum strategy: bulk TID-list sweep vs incremental; interaction with
  concurrent scans under E.
- CLOG on-disk representation and truncation cadence vs checkpoint frequency.
  *(A–D MVP decision: the CLOG is in-memory, rebuilt at recovery from the durable
  `Commit`/`Abort` WAL records — see §5.4. A durable CLOG file and its truncation
  are deferred to Milestone F, when GC needs them to bound recovery scans; until
  then this question is open only for F.)*
  *(D resolution — WAL/CLOG truncation vs aborted transactions: the MVP truncates
  the WAL only across a prefix of committed transactions; an aborted (or in-flight)
  transaction pins truncation and keeps its `Abort` record, and the recovery floor
  never crosses it (§5.4, §8). This keeps aborted-but-flushed versions invisible
  across restart without a durable CLOG or an undo pass. **F4c resolution (live):**
  once a FULL VACUUM pass reclaims an aborted transaction's on-disk versions, the WAL
  **vacuum floor** advances past it, truncation drops its `Abort`, and the
  implicit-committed floor floats past it — its versions are gone, so it is trivially
  implicit-committed — bounding the WAL retained for long-lived aborted ids. The
  floor is in-memory (reset-at-restart, conservative again after a crash) and is only
  consulted after the reclamation is durable; a standalone durable CLOG file that
  carries the floor across restart remains deferred to F.)*
- Snapshot representation cost (`xip` as `Vec` vs a more compact structure) — fine
  at the target concurrency; revisit only if measured.
- Frozen-xid / wraparound handling for very old `xmin` values (far off; the
  monotonic `u64` allocator defers this, but VACUUM should freeze settled tuples).

---

## Appendix A — Milestone B commit plan

Milestone B is the largest milestone and reworks merged B-tree / secondary-index
code, so it is decomposed into ordered, commit-sized tasks. **Every commit
compiles and keeps the existing test suite green; external autocommit behavior is
unchanged across all of B** — the work is entirely internal (the storage engine
becomes MVCC). The durability, rollback, and concurrency models are untouched
(Milestones C–E), so B is self-contained on top of Milestone A.

**Entry state (delivered by Milestone A):** tuples carry
`xmin`/`xmax`/`t_ctid`/`infomask`, stamped on insert (`xmin = txn`,
`xmax = invalid`, `t_ctid = self`); CLOG and the `Abort` record exist;
`Snapshot`/`TxnStatus`/`IsolationLevel` exist and `StatementContext` has (unused)
`snapshot`/`isolation` fields; an active-transaction registry exists on
`ServerComponents`.

### B1 — Heap line pointers & in-place header mutation

1. **`feat(storage): in-place tuple-header mutation + line-pointer contract`**
   - *Does:* page-level primitive to set `xmax`/`t_ctid`/`infomask` on an existing
     tuple at a slot without relocating it (fixed-width fields ⇒ same-size
     mutation; no compaction in B), refreshing PageLSN and checksum. Formalize the
     slot as a line pointer with states `NORMAL`/`DEAD`/`UNUSED` (`REDIRECT`
     reserved for HOT) and document the stable-`(page, slot)` contract;
     `UNUSED`-reclaim and `REDIRECT` are defined-but-unexercised (owned by F/H).
   - *Touches:* `storage/src/page.rs`, `storage/src/codec.rs`.
   - *Tests:* decode-after-mutate round-trip; checksum/PageLSN refreshed.

2. **`feat(wal): add HeapUpdateHeader record and redo`**
   - *Does:* `WalRecordKind::HeapUpdateHeader { file_id, page_num, slot, xmax,
     t_ctid, infomask }` with codec + `apply_physical_redo` under PageLSN gating;
     not yet emitted by the engine.
   - *Touches:* `wal/src/record.rs` (+ codec), `server/src/recovery.rs`; spec:
     `wal.md` + overview WAL section.
   - *Tests:* append→replay round-trip leaves the header identical; idempotent under
     PageLSN gating.

### B2 — Uniform heap-TID, multi-entry indexes *(single-version preserved)*

3. **`feat(storage): multi-entry B-tree keyed by (key, tid)`**
   - *Does:* replace the unique `key → RowLocation` tree (which rejects duplicate
     keys) with a multi-entry tree ordered by `(key, tid)`: `insert(key, tid)`,
     `remove(key, tid)`, `scan_key(key) → tids`, `range`; keep FullPageImage
     logging. Migrate the PK index (one tid per key for now); PK uniqueness kept via
     an engine presence-probe.
   - *Touches:* `storage/src/btree.rs`, `storage/src/index_page.rs`,
     `storage/src/engine.rs`; spec: `storage.md`.
   - *Tests:* duplicate-key insert/remove/scan ordering; existing PK queries
     unchanged.

4. **`feat(storage): point secondary indexes directly at heap TIDs`**
   - *Does:* convert `secondary→PK` to `secondary→heap-TID`; drop the PK-embedding
     tiebreaker in `secondary_index_key` (now disambiguated by `(key, tid)`) and the
     `secondary→PK→heap` indirection in `index_scan`; maintain secondary entries by
     TID; unique secondary keeps a temporary presence-check.
   - *Touches:* `storage/src/engine.rs`; spec re-documents the secondary-index
     feature.
   - *Tests:* secondary point/range scans, non-unique duplicates, NULLs — unchanged.

### B3 — Visibility

5. **`feat(common): tuple visibility predicate + transaction-status view`**
   - *Does:* pure visibility function over
     `(xmin, xmax, infomask, &Snapshot, &dyn TxnStatusView)` (§6); a `TxnStatusView`
     trait (`status(xid)`) backed by CLOG, injected into the storage engine.
   - *Touches:* `common`, `storage/src/engine.rs`, `server/src/app.rs` +
     `recovery.rs` (wire CLOG → engine).
   - *Tests:* table-driven predicate cases (committed/aborted/in-progress/own-write,
     delete visible/not).

6. **`feat(storage): apply snapshot visibility to scans and point lookups`**
   - *Does:* thread the snapshot (via `StatementContext`) + status view into
     `read_location`/`scan_range`/`index_scan`; filter invisible versions; replace
     "index → dead row = error" with "skip invisible"; capture a degenerate
     autocommit snapshot in `server/src/query.rs` (empty `xip`, sees all committed),
     so results are unchanged.
   - *Touches:* `storage/src/engine.rs`, `server/src/query.rs`.
   - *Tests:* existing read results unchanged; one hand-built-snapshot test proves a
     tuple is correctly hidden/shown.

### B4 — Versioning writes

> Land visibility-aware uniqueness (7) **before** the versioning commits (8–9), or
> there is a window where delete-then-reinsert wrongly fails.

7. **`feat(storage): MVCC-aware unique-constraint enforcement`**
   - *Does:* probe for a *visible-or-in-flight* version with the key (PK + unique
     secondary), ignoring dead/aborted versions; remove the presence-checks from
     commits 3–4. A no-op while single-version, so it lands safely ahead of
     versioning.
   - *Touches:* `storage/src/engine.rs` (insert/update uniqueness paths).
   - *Tests:* duplicate-key → `UniqueViolation`; unique-secondary respected.

8. **`feat(storage): DELETE marks the version deleted in place (xmax)`**
   - *Does:* locate the visible version, stamp its `xmax` in place (commits 1–2),
     retain its index entries (VACUUM cleans them); drop slot-dead tombstoning.
   - *Touches:* `storage/src/engine.rs` (`delete`, `delete_row_logged`).
   - *Tests:* delete+select (autocommit) hides the row; entry retained;
     delete-then-reinsert now allowed.

9. **`feat(storage): UPDATE writes a new version and chains it`**
   - *Does:* locate the **visible** old version (`locate_visible_version`, snapshot +
     `ctx.txn_id` — not `search(key)`, which after a delete-then-reinsert could
     target a dead version), write the new version (`xmin = txn`, `xmax = invalid`,
     `t_ctid = self`), stamp the old version's `xmax = txn` + `t_ctid→new` (the
     forward chain, invariant 5), insert a per-version entry into **all** indexes
     (PK and every secondary), and retain every old entry; drop
     relocate-tombstone-repoint; keep rejecting PK changes.
   - *All indexes, not only changed ones:* because reads do not walk `t_ctid` (every
     version is independently indexed, §3.2 invariant 3 — one entry per version), the
     new heap TID needs its own entry in **every** index, including secondaries whose
     columns did not change; otherwise a scan on an unchanged secondary value would
     find only the old version's entry (now superseded). Skipping unchanged-column
     indexes is a **HOT optimization (Milestone H)** and would be a correctness bug
     here. No old entry is ever *removed* (VACUUM's job, Milestone F).
   - *Uniqueness:* the new version must not conflict with the old version it
     supersedes but must conflict with other live rows. Stamping the old version's
     `xmax = txn` *before* the new entries' uniqueness checks makes the MVCC
     `unique_conflict_kind` treat it as own-deleted (non-conflicting); a changed
     unique secondary value colliding with a different live row raises
     `UniqueViolation` (the statement error → txn abort → before-image undo restores
     everything).
   - *Touches:* `storage/src/engine.rs` (`update`).
   - *Tests:* update+select sees the new value (seq scan, index scan on the changed
     column, and a scan on an *unchanged* secondary column — the all-indexes check);
     both versions present internally (old: `xmax=txn`, `t_ctid→new`); a secondary
     scan by the *old* value resolves the old version via a hand-built old snapshot;
     unique-secondary conflict vs. other live rows but not self; PK change rejected;
     update after delete-then-reinsert targets the visible version.

### Optional / follow-on (land in B or defer to G)

10. **`perf(storage): cache settled status via infomask hint bits`** — set
    `XMIN_COMMITTED`/`XMAX_COMMITTED`/`*_ABORTED` once a transaction is settled to
    skip CLOG probes. Requires a durability decision (log via `HeapUpdateHeader`
    vs. treat as recomputable), hence deferred-friendly; B is correct without it
    (always consult CLOG).

### Sequencing notes

- **Structure → read → write:** B1–B2 build the substrate (header mutation, uniform
  multi-entry TID indexes) with zero behavior change; B3 adds read-side visibility
  (all-visible under autocommit); B4 flips writes to versioning — the first point
  internal state diverges (old versions linger until Milestone F's VACUUM, the
  accepted interim cost).
- **No-regression window** is avoided by landing visibility-aware uniqueness (7)
  before delete/update (8–9).
- **Recovery is unaffected during B:** under autocommit single-writer every
  statement is its own committed transaction, so the existing
  `replay_committed_from` redo model replays the new
  `HeapUpdateHeader`/`HeapInsert`/index records correctly and the flush policy still
  never flushes uncommitted pages. Redo-all (Milestone D) is needed only once
  multi-statement / concurrent writers arrive.
- **Reads do not walk `t_ctid`:** with index-per-version every version is
  independently indexed, so a scan collects all candidate TIDs from the index and
  visibility-checks each; the forward `t_ctid` chain is maintained for later
  update-locating / conflict detection (Milestone E), not for plain `SELECT`.
- **Spec updates ride along** per `AGENTS.md`: commit 2 → `wal.md`; commits 3–4,
  8–9 → `storage.md` (and re-spec the secondary-index feature); commit 6 → the
  executor/storage read contract.
