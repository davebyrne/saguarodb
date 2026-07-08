# SaguaroDB Streaming Executor Bridge Specification

**Date:** 2026-07-01
**Status:** Draft

## 1. Overview

Today the server fully materializes every SELECT result. The blocking task
drains the `PlanExecutor` into a `Vec` and returns
`ExecutionResult::Query { columns, rows }`; the async connection task then writes
those rows to the socket (`docs/specs/overview.md` §Query Result Architecture,
`connection/simple.rs`). The pull-based `PlanExecutor` boundary
(`open`/`next`/`next_batch`/`close`) was deliberately preserved for a future
streaming bridge (`overview.md:431,450`, `crates/executor.md:41`).

This specification defines that bridge: SELECT results flow through a **bounded
channel** from a blocking producer that owns the `PlanExecutor` to the async task
that writes the socket. This is not a redesign — it connects a channel across a
seam that already exists and is already proven by the COPY-out path.

### Motivation

- **Lifts the total-materialization memory ceiling.** A large SELECT no longer
  buffers its entire result set in server memory before the first byte is sent.
- **Adds TCP backpressure.** A slow client naturally throttles the scan: when the
  channel fills, the producer blocks on send, pinning bounded memory instead of
  running ahead.
- **Makes cancellation responsive.** `ctx.cancel` is already polled at row
  boundaries; streaming means a `CancelRequest` (or a future statement timeout)
  stops the scan mid-result rather than after full materialization.

### Scope

**In scope:**

- `ExecutionResult::Query` (SELECT) only.
- Both the simple-query path (`connection/simple.rs::run_query`) and the
  extended-protocol `Execute` path (`connection/extended.rs::run_execute`).
- Autocommit SELECT and SELECT inside an explicit `BEGIN`/`COMMIT` block.

**Explicitly out of scope (unchanged — still materialized or immediate):**

- `Modified` (DML row counts) and `ModifiedReturning` (RETURNING). RETURNING must
  run the entire DML to completion before commit, so streaming its rows buys no
  memory relief and adds complexity.
- `Explanation` (EXPLAIN) — a single row.
- `BeginCopyIn` / `BeginCopyOut` — COPY drives its own sub-protocol (and COPY-out
  is already streamed, see §2).

**Not built here, but deliberately unblocked (see §8):** portal `max_rows` +
`PortalSuspended`, `DECLARE`/`FETCH` cursors, and responsive statement timeouts.

## 2. Precedent: the COPY-out bridge

The COPY-out path (`connection/copy.rs::run_copy_out`,
`query/copy.rs::run_copy_out_stream`) already implements exactly this
producer/consumer split, and the SELECT bridge mirrors its shape:

- A bounded `tokio::sync::mpsc` channel connects the two halves.
- The **blocking producer** (`spawn_blocking`) owns the whole read: it captures
  the snapshot, holds the GC-horizon advertisement across the entire scan, builds
  the `ExecutionContext`, drives the operator tree, and pushes results into the
  channel via `blocking_send` (which blocks — and thus applies backpressure —
  when the channel is full).
- The **async consumer** drains the channel and writes protocol messages.
- The transaction slot is threaded into the producer and returned through
  `task.await`; the async side gets it back only after the stream completes.

The one structural difference from COPY: COPY-out is **two-phase** (dispatch
returns a cheap `BeginCopyOut` request with no execution, then a *second*
`spawn_blocking` drives the export). SELECT stays **single-phase** — we must not
parse or snapshot twice — so the stream-vs-materialize decision is made *inside*
the single producer task (§4).

```
Async connection task                    Blocking producer (spawn_blocking)
─────────────────────                    ──────────────────────────────────
mpsc::channel(64)                        owns snapshot + GC-horizon advert
spawn producer(row_tx) ───────────────►  + ExecutionContext + PlanExecutor + txn
recv() loop:                             open() → sink.start(columns)
  Start{columns} → RowDescription        loop: pull ≤N rows → sink.push(batch)
  Rows(batch)    → DataRow*                    (blocking_send → backpressure)
                                         close(); drop advertisement; drop ctx
(task.await) ◄──────────────────────────  return (txn, default_iso, Outcome)
CommandComplete + ReadyForQuery
```

## 3. The executor seam: `RowSink`

The `tokio` channel is a server concern; the `executor` crate must not depend on
it (`docs/specs/crates/README.md` dependency edges). The executor therefore gains
a small sink trait and a streaming drive that **reuses the exact
open/cancel/close logic already in `execute_query`**:

```rust
/// A consumer of streamed query output. The engine calls `start` once with the
/// output schema, then `push` with row batches until the input is exhausted or
/// the sink asks to stop.
pub trait RowSink {
    /// Called once, before any rows, with the query's output columns.
    fn start(&mut self, columns: &[ColumnInfo]) -> Result<()>;

    /// Push a batch of rows. `ControlFlow::Break` stops the scan early (e.g. the
    /// downstream consumer is gone); the engine then closes the executor and
    /// returns the count streamed so far.
    fn push(&mut self, rows: Vec<Row>) -> Result<ControlFlow<()>>;
}

impl QueryEngine {
    /// Build + open the executor for a query plan, emit the schema to `sink`,
    /// then pull rows one at a time — polling cancellation between rows — into
    /// batches of at most `batch_size`, pushing each to `sink`, until exhausted or
    /// `Break`. Closes the executor on every path (success, error, early stop).
    /// Returns the number of rows streamed.
    pub fn execute_query_streamed(
        &self,
        ctx: &ExecutionContext<'_>,
        plan: &PhysicalPlan,
        sink: &mut dyn RowSink,
        batch_size: usize,
    ) -> Result<u64>;

    /// Open the same query drive without exhausting it immediately. The returned
    /// handle owns the opened operator tree and can be fetched in bounded chunks
    /// by cursors/portal suspension.
    pub fn open_query<'a>(
        &self,
        ctx: &'a ExecutionContext<'_>,
        plan: &PhysicalPlan,
    ) -> Result<OpenQuery<'a>>;
}

pub enum FetchStatus {
    Exhausted { count: u64 },
    Suspended { count: u64 },
}

impl OpenQuery<'_> {
    pub fn output_schema(&self) -> &[ColumnInfo];
    pub fn fetch(
        &mut self,
        max_rows: Option<u64>,
        sink: &mut dyn RowSink,
        batch_size: usize,
    ) -> Result<FetchStatus>;
    pub fn close(&mut self) -> Result<()>;
}
```

Notes:

- `open_query` performs the same uncorrelated-subquery resolution that
  `QueryEngine::execute` does before building the executor, and reuses the
  existing `open_executor` / `close_after` / per-row cancellation checks so open
  failure, cancellation granularity, and close-on-error behavior are identical to
  the materializing path (the drive pulls with `next`, not `next_batch`, so
  cancellation stays per-row). `execute_query_streamed` is `open_query` plus a
  single unbounded fetch and close.
- A bounded `OpenQuery::fetch(Some(n))` emits at most `n` rows. If exactly `n`
  rows were emitted, it pulls one additional row as lookahead: `Suspended` means
  that buffered row exists, while `Exhausted` means the query really ended at the
  boundary. `fetch(Some(0))` emits no rows and uses the same lookahead rule.
- The existing materializing `execute_query` is re-expressed as
  `OpenQuery::fetch(None)` driven by a `Vec`-collecting sink. This keeps every
  current caller and test that matches `ExecutionResult::Query { rows, .. }`
  working with no change, and guarantees the streamed and materialized paths
  cannot diverge (they are the same drive loop).
- The engine's public dispatch is unchanged for non-query plans; only the
  SELECT-producing arm is reachable through the streaming entry (the server only
  calls it for `Read` statements, §4).

## 4. Server orchestration

### 4.1 Single-phase producer and the outcome enum

The producer runs the existing `dispatch` pipeline (parse → classify → route →
bind → plan → execute) but is handed the channel sender. Only the SELECT arm
uses it; every other statement returns its result directly. The producer's return
value becomes:

```rust
enum StreamOutcome {
    /// SELECT rows were pushed through the channel; carries the authoritative
    /// row count for the `SELECT n` command tag.
    Streamed { count: u64 },
    /// Everything else — handled by the async side exactly as today.
    Direct(ExecutionResult),
    /// `DISCARD ALL`: write the result and clear connection-owned objects.
    SessionReset(ExecutionResult),
}
```

New server entry points (mirroring the existing `execute_simple_*` /
`execute_prepared_*` shapes, plus a channel sender):

- `execute_simple_streamed(sql, txn, default_isolation, session_ctx, row_tx)
  -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>)`
- analogous `execute_prepared_*_streamed` entry points for the extended `Execute`
  path. `session_ctx` carries the connection's cancellation flag,
  `SessionSequenceState`, `SessionInfo`, and `SessionGucs`.

For a `Read` statement that is a plain SELECT, the read helpers build a
channel-backed `RowSink` and call `execute_query_streamed`, returning
`Streamed { count }`. EXPLAIN (also a `Read`) returns `Direct(Explanation)`; the
sink is used only for the plain-SELECT sub-case. All non-read arms return
`Direct(result)` (or `SessionReset(result)` for `DISCARD ALL`) and never touch the
channel.

### 4.2 Sink threading

The sink threads through **only the read seam**; write/DDL/EXPLAIN/COPY paths are
untouched:

- `autocommit_read` (autocommit SELECT) — snapshot + `_advertised` are held
  across the drive, identical to `copy_out_autocommit`.
- `run_bound_in_transaction` → `run_plan` (in-transaction SELECT) — the
  per-statement advertisement and the txn slot are threaded in/out as today; the
  drive stays inside the existing `catch_unwind` panic firewall in `run_plan`; a
  mid-stream error takes the existing `Err` arm that sets `txn.failed = true`.

`run_plan` gains an optional sink parameter. When present and the plan is a query
(not DML/DDL/EXPLAIN), it calls `execute_query_streamed`; otherwise it behaves as
today. Threading an `Option<&mut dyn RowSink>` keeps writes and DDL on their
current materialize/return path.

### 4.3 Async consumer (both protocols)

`connection/simple.rs::run_query` and `connection/extended.rs::run_execute` each:

1. Create a bounded channel and `spawn_blocking` the streaming producer with the
   sender (moving the txn slot + default isolation in, as today).
2. Drain the receiver: the first message (`Start { columns }`) emits
   `RowDescription` (simple protocol only — the extended path's `RowDescription`
   comes from `Describe`, so it consumes `Start` without emitting one);
   subsequent `Rows(batch)` messages emit `DataRow`s in the portal's result
   formats.
3. `task.await` to reclaim the txn slot + isolation and the `StreamOutcome`.
4. Emit the terminal messages:
   - `Streamed { count }` → `CommandComplete("SELECT count")` (+ `ReadyForQuery`
     with the recomputed status byte on the simple path; the extended path defers
     `ReadyForQuery` to `Sync`, as today).
   - `Direct(result)` → the existing result writer handles
     `Modified`/`ModifiedReturning`/`Explanation`, and the existing COPY handling
     drives `BeginCopyIn`/`BeginCopyOut` (the in-flight-query guard is retained in
     the async task until the outcome is known, then handed to the COPY driver or
     dropped, exactly as today).

The in-flight-query guard is held in the async task for the whole streaming
duration so the statement keeps counting as in-flight for graceful-shutdown
coordination.

## 5. Lifetime invariants (all preserved)

Because the entire read execution stays inside the blocking task and only
*completes* once the executor is fully drained and closed, every existing
invariant holds exactly as it does for `copy_out_autocommit` /
`copy_out_transaction`:

- **GC-horizon advertisement** is held for the whole scan — until the producer
  returns — so VACUUM cannot reclaim a version the stream still sees, *even while
  the producer is blocked on a full channel* (`docs/specs/mvcc.md` §9).
- **Transaction slot and the single write guard** return through `task.await`;
  the session receives them back only after streaming finishes, so an
  in-transaction SELECT's snapshot and guard lifetime are unchanged.
- **Snapshot and page pins** live on the blocking stack for the drive's duration;
  `close()` releases pins before the producer returns.
- **Statement isolation / SSI tracking** is unchanged: `execute_query_streamed`
  builds the executor through the same `build_executor` path, so relation/tuple
  reads are recorded identically.

## 6. Error handling and edge cases

| Case | Behavior |
|---|---|
| Pre-stream error (parse/bind/plan/`open`) | No `Start` is sent; `task.await` yields `Err`. Simple path: `ErrorResponse` + `ReadyForQuery`. Extended path: `ErrorResponse`, session marked failed. No `RowDescription`. |
| Mid-stream error (storage read, cancellation) | `RowDescription` + some `DataRow`s already sent; then `ErrorResponse` (+ `ReadyForQuery` on the simple path). Valid in the PostgreSQL protocol. Inside a transaction the block is poisoned (`txn.failed = true`). |
| Client / socket write error | The consumer records the error, stops reading, and drops the receiver; the producer's next `blocking_send` fails, the sink returns `Break`, the executor is closed, and the producer returns. The connection then errors out. Mirrors COPY-out's `write_err` handling. |
| Cancellation (`CancelRequest`) | `ctx.cancel` is polled per row in the drive loop, producing `QueryCanceled` mid-stream. This is the foundation for responsive statement timeouts. |
| Zero-row SELECT | `sink.start` fires before the drive loop, so `RowDescription` is always emitted (matching current behavior). |

## 7. Backpressure, batching, and tuning

- The channel is a **bounded** `tokio::sync::mpsc` with capacity
  `STREAM_CHANNEL_CAPACITY` (**64**, matching `overview.md`). Bounding the channel
  is what provides backpressure and the memory ceiling.
- Channel items are **row batches**: the drive pulls rows one at a time (with
  `next`, so cancellation stays per-row) and accumulates up to `STREAM_BATCH_ROWS`
  (**64**) of them before pushing a batch. The `(capacity, batch size)` pair is a
  tuning knob trading channel operations against peak buffered rows — at the
  defaults, at most about `64 × 64 = 4096` rows are buffered before the producer
  blocks (a bounded, constant ceiling regardless of result size, which is the
  point). It affects **neither correctness nor the wire protocol** — only
  throughput and memory.
- Row identity is dropped at the sink boundary (the sink takes plain `Row`, as
  `execute_query` does today with `row.row`); SELECT output never needs
  `RowIdentity`.

## 8. Paving the way for the unlocks (not built here)

The design positions the following features so that each is a localized,
additive change rather than a rework. None are implemented in this work.

- **Portal `max_rows` + `PortalSuspended`, and `DECLARE`/`FETCH` cursors.** These
  require the `PlanExecutor` (with its snapshot, pins, and txn) to survive across
  protocol round-trips — fetch *n* rows, then *suspend* rather than drain to
  completion. The evolution is localized to the producer: turn its "drain to
  completion" loop into a **command channel** carrying `Fetch(n)` / `Close`
  messages, the direct analogue of COPY-in's existing `Chunk` / `Done` / `Fail`
  command channel (`query/copy.rs::drive_copy_in`). The producer thread stays
  parked between fetches, holding the snapshot and advertisement. The `RowSink`
  batch drive and the `ControlFlow::Break` early-stop signal are already the
  fetch-*n*-then-stop primitive this needs.
- **Responsive statement timeouts.** Cancellation is already observed per row in
  the drive loop; a timeout simply sets `ctx.cancel` from a timer, and the stream
  stops at the next batch boundary.
- **Homes that already exist.** The portal registry (`self.portals`) is where a
  suspended portal's parked-producer handle would live; the extended-protocol
  `Execute` already routes through the session transaction slot.

Per `overview.md:450`, none of this affects the protocol crate or SQL semantics.

## 9. Testing

- **Executor:** `execute_query_streamed` produces the same rows, order, and
  columns as materialized `execute_query` (parity, driven by re-expression on the
  shared drive); an early `Break` stops the scan and still calls `close`; open
  failure and mid-drive error both close the executor.
- **Server integration (both protocols):** streamed SELECT correctness;
  zero-row SELECT emits `RowDescription`; a result larger than the channel
  capacity exercises backpressure without deadlock; mid-stream cancellation
  yields `ErrorResponse` after some `DataRow`s; client disconnect aborts the scan;
  an in-transaction SELECT preserves snapshot semantics and poisons the block on a
  mid-stream error; a streamed SELECT concurrent with VACUUM confirms the
  advertisement holds (no reclaim of visible versions).
- **Spec updates in the same change:** `overview.md` §Query Result Architecture
  (flip "materializes" → the streaming bridge, keeping the note that it does not
  affect the protocol crate or SQL semantics), `crates/executor.md` (the
  `RowSink` trait and `execute_query_streamed`), and `crates/server.md` (the
  streaming producer/consumer and outcome enum).

## 10. Non-goals

- No change to physical operators or their semantics.
- No change to the wire protocol or to SQL behavior.
- No change to DML, DDL, EXPLAIN, or COPY execution paths.
- No implementation of `max_rows`, `PortalSuspended`, cursors, or statement
  timeouts (see §8).
