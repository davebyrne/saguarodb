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
durable changes are additive (new WAL record kinds, a new CLOG file).

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
- `UNUSED` — free for reuse.
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

### 5.4 CLOG — durable transaction status map

A durable map `txn_id → {InProgress, Committed, Aborted}` (two bits per txn),
recording the outcome of every transaction.

- Rebuilt at recovery from `Commit`/`Abort` WAL records (supersedes the
  single-bit `committed_txns` set in `crates/wal/src/file.rs` as the authoritative
  status source).
- Consulted by the visibility predicate and the flush policy at runtime.
- Truncatable below the GC horizon (§9), coordinated with checkpoint/WAL
  truncation. Transactions older than the horizon are implicitly committed
  (their versions are either reclaimed or frozen).

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
first statement of the transaction and reuses it (see Milestone G).

---

## 7. Concurrency model and transaction lifecycle

### 7.1 Rollout

- **Stage 1 (Milestones C–D): concurrent readers, serialized writers.** Readers
  capture a snapshot under a brief latch and run lock-free (no
  `ConcurrencyController` guard). Writers serialize by holding the existing
  exclusive guard (`crates/common/src/concurrency.rs`) for the **whole
  write-transaction** (the owned guard is stored on the connection `Session`).
- **Stage 2 (Milestone E): concurrent writers.** The global writer lock is
  replaced by a transaction manager; many write-transactions run concurrently,
  relying on the buffer pool's existing frame latches for page safety.

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

`xmax` doubles as a row lock. A writer tentatively stamps `xmax = my_txn`. Another
writer encountering a live `xmax` consults the other transaction's CLOG status:

- in-progress → block (or fail per policy);
- committed *after my snapshot* → **serialization failure** (`SqlState::SerializationFailure`, `40001`);
- aborted → proceed.

First-updater-wins. Concurrent inserts of the same unique key are resolved by the
same status check on the conflicting index entry's tuple.

---

## 8. Recovery and durability

- **Flush policy** (`crates/server/src/recovery.rs`, `WalFlushPolicy`): drop the
  `is_committed` gate; keep the WAL-durability gate (`page_lsn ≤ flushed_lsn`).
  Uncommitted versions may be evicted — they are invisible.
- **Recovery**: redo via `replay_from` (not `replay_committed_from`), applying all
  heap/index/header records under PageLSN gating; build the CLOG from
  `Commit`/`Abort` records as replay proceeds. Any transaction with neither a
  durable `Commit` nor `Abort` at crash is recorded `Aborted`. No undo pass.
- **Checkpoint** ordering is unchanged in shape (`crates/server/src/checkpoint.rs`):
  `wal.flush` → `flush_committed_pages` → `store.sync_all` → control record →
  `Checkpoint` marker → `truncate_before` → `mark_all_clean`. CLOG is persisted/
  truncated in coordination with the control record and WAL truncation.
- **Consequence**: after a crash the heap may contain flushed-then-aborted/dead
  versions. This is correct (CLOG hides them; VACUUM reclaims them). Heap
  cleanliness is a VACUUM responsibility, not a recovery responsibility.

---

## 9. Garbage collection (VACUUM)

Required for bounded space — and more urgent than under a single-entry-index
design, because index entries accumulate per version as well as heap tuples.

- **Horizon**: the oldest `xmin` across the active-transaction registry; a version
  is *dead to everyone* when its `xmax` is committed and `< horizon`, or its `xmin`
  is aborted.
- **Heap prune** (intra-page): mark dead tuples' line pointers `DEAD` and compact
  live tuples (this finally adds the page compaction that `page.rs` lacks today —
  `DELETE` is currently a non-reclaiming tombstone). WAL-logged.
- **Index vacuum**: remove index entries pointing at dead TIDs from every index.
- **Line-pointer reclaim**: `DEAD → UNUSED` once no index entry references the
  slot.
- **Triggering**: an on-demand `VACUUM` command plus opportunistic pruning during
  scans. CLOG truncation below the horizon piggybacks here.

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
  build the durable status map (supersedes `committed_txns` as authority); add the
  active-txn registry on `ServerComponents`; route the existing autocommit
  commit/rollback through CLOG.

### Milestone B — Index-per-version storage model *(single-writer/autocommit; MVCC-correct internally)*

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
- **C3 — Lifecycle + concurrency relaxation.** Generalize the write path
  (`server/src/query.rs`): `txn_id` at `BEGIN`, shared across statements; snapshot
  capture per isolation; `COMMIT` = `Commit`+flush; `ROLLBACK`/error = `Abort`.
  Readers go lock-free; writers hold the existing owned write guard for the whole
  write-transaction. **Retire the buffer-pool before-image undo** (abort =
  invisible). Autocommit = implicit `BEGIN/COMMIT`.

### Milestone D — Recovery & durability rework

- **D1 — Relax flush policy** (§8). **D2 — Redo-all recovery** + CLOG visibility;
  in-flight-at-crash = aborted; build CLOG during replay. Crash tests across
  checkpoint boundaries, torn pages, eviction of uncommitted pages.

*A–D = MVCC MVP: snapshot reads + multi-statement transactions + serial writers +
correct recovery. Correct, but bloats heap and indexes until F.*

### Milestone E — Concurrent writers + conflict detection

- **E1 — Conflicts** (§7.3): `xmax`-as-lock, first-updater-wins, `40001`;
  concurrent-inserter unique conflicts. **E2 — Replace the writer lock** with a
  transaction manager on the buffer pool's frame latches.

### Milestone F — VACUUM / GC *(near-MVP in this model)*

- **F1 — Horizon.** **F2 — Heap prune + compaction.** **F3 — Index vacuum +
  line-pointer reclaim.** **F4 — On-demand `VACUUM`, opportunistic pruning, CLOG
  truncation** (§9).

### Milestone G — Isolation levels & polish

`SET TRANSACTION ISOLATION LEVEL`; Read Committed (per-statement snapshot) vs
Repeatable Read (per-transaction snapshot); `'E'` failed-transaction handling;
serialization-failure surfacing; savepoints via sub-transaction xids (optional).

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
- **Before-image retirement.** C3 removes the buffer-pool before-image rollback;
  abort becomes status-based. This is a hard prerequisite for E (one before-image
  per page cannot serve concurrent writers).
- **Error codes.** Add `SqlState::SerializationFailure` (`40001`); the `'E'`
  transaction state rejects all but `COMMIT`/`ROLLBACK`.
- **WAL additions.** `Abort`, `HeapUpdateHeader` (§5.3); recovery handles both.

---

## 12. Deferred / future work

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
- Snapshot representation cost (`xip` as `Vec` vs a more compact structure) — fine
  at the target concurrency; revisit only if measured.
- Frozen-xid / wraparound handling for very old `xmin` values (far off; the
  monotonic `u64` allocator defers this, but VACUUM should freeze settled tuples).
