# SaguaroDB Cursor Specification

**Date:** 2026-07-08
**Status:** Extended-protocol portal suspension implemented; SQL cursors planned

## 1. Goal

Cursor support is split across two layers. Extended-protocol portal suspension is
implemented; the SQL cursor layer remains planned follow-on work.

1. PostgreSQL extended-protocol portal suspension: honor `Execute.max_rows`,
   return `PortalSuspended` when rows remain, and let a later `Execute` resume
   the same portal.
2. SQL cursors: `DECLARE <name> CURSOR FOR <select>`, `FETCH [FORWARD]
   [<count> | ALL] FROM <name>`, and `CLOSE <name>`.

The implementation reuses the streaming executor bridge rather than materializing
cursor results. A suspended cursor owns an open executor plus the
MVCC snapshot, relation-generation snapshot, page pins, cancellation handle, and
GC-horizon advertisement needed to keep that executor correct.

## 2. Implemented Scope

Supported:

- Extended-protocol `Execute` with `max_rows > 0` for read-only SELECT portals.
- `PortalSuspended` (`s`) for a partially drained extended-protocol portal.
- Suspended portal cleanup on portal `Close`, transaction end, portal
  replacement, `DISCARD ALL`, autocommit `Sync`, simple `Query`, connection
  close, cancellation, and error paths.

Planned follow-on SQL cursor support:

- Forward-only, read-only SQL cursors over `SELECT`.
- SQL `DECLARE`, `FETCH`, and `CLOSE` in explicit transaction blocks.

Out of scope:

- `SCROLL`, `BACKWARD`, `ABSOLUTE`, `RELATIVE`, `MOVE`.
- `WITH HOLD`, `BINARY`, `INSENSITIVE`, and updatable cursors.
- `WHERE CURRENT OF`.
- Cursors over DML `RETURNING`, `COPY`, `EXPLAIN`, maintenance commands, or DDL.
- Durable cursors or recovery of open cursors.
- Simultaneous fetch execution on the same session; suspended portal workers are
  session-local and fetches are sequenced by the connection loop.

## 3. Design Constraints

- `executor` must not depend on Tokio or protocol channel types.
- `server` owns sockets, protocol sequencing, portals, cursor registries, and
  cursor worker tasks.
- A suspended cursor must not store borrowed executor state in the async
  `Session`. Park the open executor inside a blocking worker and communicate
  with it through typed commands.
- Recovery, WAL, manifest, catalog snapshots, and row/page encodings are
  unchanged. Open cursors are process-local session state.
- Planned SQL cursors should be rejected outside explicit transaction blocks.
  This avoids implicit transactions spanning `ReadyForQuery` and matches
  PostgreSQL's ordinary transaction-scoped cursor model.

## 4. Executor Changes

The executor exposes an open-query abstraction that can fetch a bounded number
of rows without closing the executor:

```rust
pub enum FetchStatus {
    Exhausted { count: u64 },
    Suspended { count: u64 },
}

pub struct OpenQuery<'a> {
    // stores output columns, Box<dyn PlanExecutor + 'a>, cancel flag,
    // pending lookahead row, and close/exhaustion state
}

impl QueryEngine {
    pub fn open_query<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'_>,
        plan: &PhysicalPlan,
    ) -> Result<OpenQuery<'a>>;
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

Rules:

- `open_query` performs the same uncorrelated-subquery resolution as
  `execute_query_streamed`.
- `fetch(Some(n))` emits at most `n` rows. If the fetch reaches the bound, it
  performs a one-row lookahead: `Suspended` means a row remains buffered for the
  next fetch, while `Exhausted` means the query ended at the boundary. `n = 0`
  emits no rows and uses the same lookahead rule.
- `fetch(None)` drains to exhaustion.
- `close` is idempotent and is also called from `Drop` as a best-effort cleanup.
- `execute_query_streamed` uses `open_query` plus `fetch(None)` so bounded fetch,
  full streaming, and materialized execution share the same drive path.

## 5. Server Cursor Worker

The server-local portal cursor worker lives under
`crates/server/src/query/cursor.rs`.

Worker commands:

```rust
enum CursorCommand {
    Fetch {
        max_rows: Option<u64>,
        row_tx: mpsc::Sender<StreamMessage>,
        reply_tx: oneshot::Sender<Result<CursorFetchStatus>>,
    },
}
```

Worker fetch status:

```rust
enum CursorFetchStatus {
    Exhausted { count: u64 },
    Suspended { count: u64 },
}
```

The blocking worker owns:

- `OpenQuery`.
- The `ExecutionContext` inputs it needs for the lifetime of the open query.
- The captured MVCC and relation snapshots.
- Any per-statement GC-horizon advertisement not already owned by the transaction.
- The query text for activity tracking diagnostics.

Lifecycle rules:

- A worker starts only after parse, bind, logical planning, physical planning,
  snapshot capture, and relation-snapshot validation have succeeded.
- Each `Fetch` pushes `StreamMessage::Start` once before the first rows if the
  consumer needs column metadata, then `Rows` batches.
- A dropped row receiver is treated as early stop for that fetch, not as a leaked
  cursor. The cursor remains usable unless the worker itself hit an error.
- Closing or replacing the portal drops the worker handle. Cancellation is
  delivered through the session's shared cancel flag. Worker error, exhaustion,
  cancellation, or connection drop closes the executor and releases
  snapshots/page pins.

## 6. Extended Protocol Behavior

### 6.1 Protocol crate

- `ServerMessage::PortalSuspended` encodes as PostgreSQL server message tag
  `b's'` with length `4`.

### 6.2 Connection state

The connection `Portal` is a state machine:

```rust
enum Portal {
    Bound(BoundPortal),
    Suspended(SuspendedPortal),
}
```

`BoundPortal` keeps today's prepared statement, bound parameter values, and
result formats. `SuspendedPortal` stores the worker handle, result formats, output
columns, and cumulative row count if needed for diagnostics.

### 6.3 Execute behavior

- For non-SELECT statements, `max_rows` is ignored as PostgreSQL does today.
- For SELECT with `max_rows == 0`, preserve the current full-drain streaming
  behavior.
- For SELECT with `max_rows > 0`:
  - If the portal is `Bound`, start a cursor worker and fetch up to `max_rows`.
  - If exhausted, send `CommandComplete("SELECT n")` and remove/finish the
    portal as appropriate.
  - If rows remain, store `SuspendedPortal` and send `PortalSuspended`.
  - If the portal is already `Suspended`, send another fetch command to the same
    worker.

Autocommit rule:

- A suspended portal created outside an explicit transaction may be resumed only
  before the next `Sync` or simple `Query`. If either arrives while such a portal
  is still suspended, close it before `ReadyForQuery`.
- Inside an explicit transaction, a suspended portal may survive across `Sync`
  until closed, exhausted, transaction end, or a successful `ROLLBACK TO
  SAVEPOINT`. Savepoint rollback changes the transaction's live subxid set, so
  any cursor worker holding the old statement context must be discarded.

This keeps portal suspension from exposing implicit transactions that remain
open after the server reports idle.

### 6.4 Portal cleanup

Close suspended portals when:

- `Close Portal` names them.
- A new `Bind` replaces the same portal name.
- `DISCARD ALL` runs.
- The transaction commits or rolls back.
- A successful `ROLLBACK TO SAVEPOINT` runs while a transaction-scoped portal is
  suspended.
- The connection closes.
- The extended-protocol error skip state reaches `Sync` for an autocommit
  suspended portal.
- A simple `Query` arrives while an autocommit portal is suspended.

## 7. SQL Cursor Follow-On Plan

### 7.1 Parser

Add AST variants:

```rust
DeclareCursor { name: String, query: Query }
FetchCursor { name: String, count: FetchCount }
CloseCursor { name: String }
```

`FetchCount`:

```rust
enum FetchCount {
    One,
    Count(u64),
    All,
}
```

Accepted syntax:

- `DECLARE name CURSOR FOR SELECT ...`
- `FETCH FROM name`
- `FETCH name`
- `FETCH FORWARD FROM name`
- `FETCH FORWARD n FROM name`
- `FETCH n FROM name`
- `FETCH ALL FROM name`
- `CLOSE name`

Rejected syntax:

- `DECLARE ... SCROLL`, `NO SCROLL`, `WITH HOLD`, `BINARY`, `INSENSITIVE`.
- `FETCH BACKWARD`, `ABSOLUTE`, `RELATIVE`, negative counts.
- `CLOSE ALL` in the first SQL cursor slice.
- Quoted cursor identifiers, matching the general quoted-identifier rule.

### 7.2 Planner/binder

Cursor control is server-driven, like transaction control and savepoints:

- `DECLARE` binds the SELECT to validate it, reject parameters in the first
  simple-query SQL cursor slice, and capture result columns.
- `DECLARE` rejects non-SELECT bodies and sequence-mutating SELECTs in the first
  SQL cursor slice.
- `FETCH` and `CLOSE` resolve cursor names against the session cursor registry at
  execution time.
- Extended-protocol prepared cursor SQL is rejected in the first SQL cursor
  slice, matching savepoint's simple-query-only treatment unless there is a later
  reason to support it.

### 7.3 Server execution

Add `Session.cursors: HashMap<String, SqlCursor>`.

`DECLARE`:

- Requires an open explicit transaction in healthy state.
- Captures the transaction-appropriate MVCC/relation snapshot:
  - Read Committed: capture a statement snapshot and keep its advertisement in
    the worker for the cursor lifetime.
  - Repeatable Read / Serializable: reuse the transaction snapshot and relation
    snapshot; the transaction owns the long-lived advertisement.
- Starts a cursor worker but does not fetch rows.
- Stores the worker under the normalized cursor name.
- Returns command tag `DECLARE CURSOR`.

`FETCH`:

- Requires an open explicit transaction in healthy state.
- Sends a fetch command to the named cursor worker.
- Streams rows like SELECT and returns `CommandComplete("FETCH n")`.
- If the cursor exhausts, keep it open at end-of-cursor until `CLOSE` or
  transaction end; repeated `FETCH` returns zero rows.

`CLOSE`:

- Requires an open explicit transaction in healthy state.
- Closes and removes the named cursor.
- Returns `CommandComplete("CLOSE CURSOR")`.

Transaction end:

- `COMMIT` and `ROLLBACK` close every SQL cursor before dropping the transaction
  snapshot/guard state.
- If cursor close fails during transaction end, treat it like other post-statement
  cleanup uncertainty: return a structured error before reporting the transaction
  complete when still pre-commit, or fatal if the durable commit point has already
  passed.

## 8. Activity, Cancellation, and Shutdown

- A cursor worker counts as an in-flight query while it is actively fetching, not
  while it is merely parked between fetches. Its open snapshot still contributes
  to the GC horizon through its advertisement.
- `CancelRequest` during a fetch sets the same session cancel flag and the worker
  observes it at row boundaries.
- A parked cursor should not keep `pg_stat_activity.state = active`; the session
  remains idle-in-transaction while the cursor is open but not fetching.
- Graceful shutdown must close parked cursor workers on connection shutdown and
  must wait for active fetches through the existing in-flight query guard.

## 9. Test Plan

Protocol unit tests:

- `PortalSuspended` encodes to `s` with length `4`.
- `Execute.max_rows` decode coverage remains intact.

Executor tests:

- `OpenQuery::fetch(Some(n))` emits at most `n` rows and returns `Suspended`
  only when a lookahead row remains buffered.
- A second fetch resumes at the next row.
- `fetch(None)` drains and returns `Exhausted`.
- `close` runs on exhausted, suspended, error, and drop paths.
- Cancellation between rows returns `QueryCanceled`.

Server extended-protocol tests:

- `Execute max_rows=2` over a five-row SELECT sends two rows and
  `PortalSuspended`.
- Repeated `Execute` drains the same portal in order and ends with
  `CommandComplete("SELECT 5")`.
- Binary result formats are preserved across suspended fetches.
- `Close Portal` releases the worker.
- Rebinding the same portal closes the old worker.
- Autocommit `Sync` closes a still-suspended portal.
- Explicit-transaction suspended portals survive `Sync` and close at commit.
- `ROLLBACK TO SAVEPOINT` closes transaction-scoped suspended portals.
- Client disconnect closes a suspended worker and releases GC-horizon pins.

Planned SQL cursor tests:

- `DECLARE` outside a transaction errors.
- `DECLARE c CURSOR FOR SELECT ...`; `FETCH 2 FROM c`; `FETCH ALL FROM c`;
  `CLOSE c` returns expected rows and command tags.
- Repeated fetch after exhaustion returns zero rows.
- `COMMIT`/`ROLLBACK` close open cursors.
- `VACUUM` cannot reclaim a version still visible to an open cursor snapshot.
- Unsupported cursor options return accurate SQLSTATEs.

Recovery/durability tests:

- None required for cursor state itself because cursors are not durable.
- Existing recovery tests should continue to pass, proving no WAL/control format
  changed.

## 10. Implementation Status

Implemented portal-suspension layer:

- `ServerMessage::PortalSuspended` and codec coverage.
- `Execute.max_rows` handling for read-only SELECT portals.
- `executor::OpenQuery` and shared fetch path for bounded fetch and full-drain
  streaming.
- Server cursor worker plumbing for suspended portals.
- Cleanup on portal replacement, `Close`, `DISCARD ALL`, transaction end,
  autocommit `Sync`, simple `Query`, disconnect, cancellation, and error paths.
- Integration coverage for MVCC snapshot retention, cancellation, binary result
  formats, and lifecycle cleanup.

Remaining SQL cursor sequence:

1. Add parser AST and parser tests for `DECLARE`/`FETCH`/`CLOSE`.
2. Add server-side SQL cursor registry and simple-query execution.
3. Add SQL cursor integration coverage for transaction scoping, exhaustion,
   cleanup, unsupported options, and VACUUM horizon retention.
4. Update `overview.md`, `crates/parser.md`, `crates/planner.md`, and
   `crates/server.md` from planned to implemented SQL cursor behavior once that
   layer lands.

## 11. Verification

Run narrow tests as each layer lands:

```bash
cargo test -p saguarodb-protocol
cargo test -p saguarodb-executor
cargo test -p saguarodb-parser
cargo test -p saguarodb-server connection::tests
cargo test -p saguarodb-server --test transactions -- --nocapture
```

Before handoff:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
