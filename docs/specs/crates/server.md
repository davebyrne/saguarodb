# `server` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`server` is the binary crate. It wires all components, runs startup/recovery, owns Tokio networking, bridges protocol messages to query execution, and manages graceful shutdown.

## Depends On

- `common`
- `protocol`
- `parser`
- `planner`
- `executor`
- `storage`
- `buffer`
- `wal`
- `control`
- `catalog`

No library crate depends on `server`.

## Modules

`app` (component bundle + `AppState`), `cancel` (`BackendKey { process_id, secret_key }` and the process-wide `CancelRegistry`), `checkpoint`, `config`, `connection`, `query`, `recovery`, `shutdown`, and `tls` (`build_acceptor`).

## Configuration

```rust
pub struct Config {
    pub data_dir: PathBuf,
    pub port: u16,
    pub buffer_pool_frames: usize,
    pub checkpoint_every_n_commits: u64,
    pub checkpoint_wal_bytes: u64,
    pub shutdown_timeout_ms: u64,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
}
```

V1 fsyncs WAL on every commit. There is no `wal_flush_interval_ms` in the server spec.

Defaults:

- `data_dir = "./data"`
- `port = 5433`
- `buffer_pool_frames = 1024`
- `checkpoint_every_n_commits = 100`
- `checkpoint_wal_bytes = 64 * 1024 * 1024`
- `shutdown_timeout_ms = 30000`
- `tls_cert_file = None`
- `tls_key_file = None`

Binary CLI flags:

- `--data-dir <PATH>` sets `Config.data_dir`; default `./data`.
- `--port <PORT>` sets `Config.port`; default `5433`.
- `--buffer-pool-frames <N>` sets `Config.buffer_pool_frames`; default `1024`.
- `--checkpoint-every-n-commits <N>` sets `Config.checkpoint_every_n_commits`; default `100`.
- `--checkpoint-wal-bytes <BYTES>` sets `Config.checkpoint_wal_bytes`; default `67108864`.
- `--shutdown-timeout-ms <MS>` sets `Config.shutdown_timeout_ms`; default `30000`.
- `--tls-cert-file <PATH>` sets `Config.tls_cert_file`; PEM certificate chain. Optional; defaults to disabled.
- `--tls-key-file <PATH>` sets `Config.tls_key_file`; PEM private key. Optional; defaults to disabled.
- `--help` prints usage and exits with code `0`.

V1 parses flags with `std::env::args`; do not add a CLI parser dependency. `--port` accepts `1..=65535`; all other numeric flags must be positive nonzero integers. Unknown flags, missing values, non-numeric numeric values, or out-of-range numeric values print usage to stderr and exit with code `2`. TLS is enabled only when both `--tls-cert-file` and `--tls-key-file` are supplied; supplying exactly one is an error that prints usage to stderr and exits with code `2`.

## Startup Sequence

1. Load configuration.
2. Initialize the control store (`FileControlStore`) and the heap page store (`HeapPageStore` over `<data>/heap`).
3. Initialize the WAL manager.
4. Initialize the buffer pool with the configured frame count, the `WalFlushPolicy`, and the heap page store as its `PageStore`. `WalFlushPolicy::can_flush` admits a dirty page iff it is **WAL-durable** (`page_lsn ≤ wal.flushed_lsn()`); the earlier committedness gate is dropped (Milestone D1, `mvcc.md` §8), so uncommitted/aborted dirty pages may be flushed/evicted (hidden by the CLOG). `WalFlushPolicy::ensure_durable` (called by the buffer pool's steal path before writing a stolen page) flushes the WAL, giving write-ahead logging for the now-possibly-uncommitted stolen page.
5. Enable eviction-flush-on-steal (`buffer_pool.enable_stealing()`), immediately after constructing the pool and before loading the control record. The durable on-disk index means recovery rebuilds nothing in memory, so redo may spill and the recovery working set is not bounded by the pool size.
6. Load the control record (`control.load()`): the redo boundary `checkpoint_lsn` and catalog bytes (none if no control record exists yet).
7. Initialize catalog from the control catalog bytes, or empty catalog if no control record exists.
8. Initialize storage engine in recovery mode with `PageBackedStorageEngine::open(buffer_pool.clone(), wal.clone(), StorageMode::Recovery)`.
9. Call `storage.install_schemas(catalog.list_tables()?)` and `storage.install_index_schemas(indexes)`, where `indexes` is gathered via `catalog.list_indexes_for_table` for each table, so recovery replay and later DML maintain the secondary indexes.
10. Redo-all: replay every record with `LSN > checkpoint_lsn` (`WalManager::replay_from`) via `storage::apply_physical_redo` (PageLSN-gated; torn/missing pages are zeroed so a `FullPageImage`/`HeapInit` re-establishes them), regardless of the dirtying transaction's outcome — the CLOG (rebuilt at WAL open) decides visibility afterward, and an aborted/in-flight transaction's replayed versions are invisible (`mvcc.md` §8). Heap, primary-key-index, and secondary-index pages replay the same way. The `Commit`/`Abort`/`Checkpoint` markers are skipped (they are not page mutations). DDL records (`CreateTable`/`DropTable`/`CreateIndex`/`DropIndex`) replay through `RecoveryOperations` **only when their transaction is committed** (they mutate the durable catalog directly, not idempotent PageLSN-gated pages, so an aborted DDL's catalog change must not take effect).
11. Create `ServerComponents` with catalog, storage, buffer pool, WAL, control store, heap store, concurrency controller, shutdown state, checkpoint state initialized from the control `checkpoint_lsn`, `next_txn_id` initialized to one greater than the maximum retained user WAL `txn_id`, and an empty `active_txns` registry (the WAL manager rebuilt its CLOG from the retained `Commit`/`Abort` records on `open`).
12. If records were replayed, run `run_checkpoint(&components)` to persist the redone state to the heap and index and advance the redo boundary.
13. Switch storage engine to normal mode with `storage.set_mode(StorageMode::Normal)`.
14. Construct query service from `components`.
15. Start Tokio runtime and bind listener.

Recovery mode must not append WAL records.

Recovery computes `next_txn_id` from all retained WAL records with stored
`LSN > checkpoint_lsn` by calling `WalManager::replay_from(checkpoint_lsn)`, not
`replay_committed_from`. That retained set includes the `Checkpoint` marker
(appended just after the boundary), which carries the transaction-id high-water
mark, so the allocator boundary is recovered even when every data record below the
checkpoint was truncated; without it the allocator would restart low and reissue
ids that already stamped committed tuples. Include committed operations,
uncommitted operations, `Commit` records, and the `Checkpoint` marker's high-water;
ignore only records with `txn_id = 0`.
`ServerComponents.next_txn_id` starts at `max_txn_id + 1`, or `FIRST_NORMAL_XID`
when no user transaction records remain. If the maximum retained user transaction
ID is `u64::MAX`, startup fails with a structured WAL/internal error instead of
wrapping or saturating the next transaction ID. This prevents a new statement from
reusing an old uncommitted `txn_id` and accidentally making pre-crash records look
committed.

After computing `next_txn_id`, recovery calls
`WalManager::establish_recovery_committed_floor(next_txn_id)`. The
implicit-committed floor lets an unrecorded normal id below it read as committed,
covering a *committed* transaction whose `Commit` record a checkpoint truncated
(see `wal.md` "Implicit-committed floor" and `mvcc.md` §5.4). Because the relaxed
flush gate (Milestone D) lets an aborted/in-flight transaction's pages reach the
heap, the floor is set **conservatively**: to the oldest transaction in the
retained WAL whose CLOG status is not `Committed` (aborted or in-flight), or to the
allocation boundary if every retained transaction is committed — never crossing a
non-committed transaction, or its replayed versions would wrongly become visible.
Conservative WAL truncation (`wal.md`, `mvcc.md` §5.4/§8) guarantees every
transaction dropped below that oldest non-committed one was committed, so ids below
the floor are all genuinely committed.

## Query Service Wiring

The concrete `QueryService` in `crates/server/src/query.rs` performs:

```text
parse(sql)
bind(statement, catalog)
logical_plan(bound)
physical_plan(logical, catalog)
engine.execute(execution_context, physical)
```

The server constructs `ExecutionContext { statement, catalog, storage, schema_ops, cancel }` for each physical plan. The `QueryEngine` receives the server-allocated `StatementContext` and never allocates transaction IDs, appends commit records, flushes WAL, or calls storage/buffer commit or rollback.

### Transaction lifecycle (Milestone C)

The query path is a real transaction lifecycle; autocommit is an implicit single-statement transaction routed through the same machinery. A simple query carries the connection's transaction slot (`Option<Transaction>`, held on the `Session`) into `QueryService::execute_simple(sql, slot, cancel)`, which returns the (possibly mutated) slot. The connection derives its `ReadyForQuery` byte from the returned slot (`I`/`T`/`E`).

- **BEGIN**: allocate a `txn_id` (and register it active) atomically under the registry latch, set the slot to an open `InTransaction` (`'T'`). `BEGIN` inside an open block is a no-op warning that stays `'T'` (Postgres-compatible). DDL inside a block is rejected (`FeatureNotSupported`); DDL is non-transactional.
- **Statements inside the block** share the transaction's `txn_id`; writes are stamped with it; reads use the transaction's snapshot (per isolation, below).
- **COMMIT**: append `Commit` → `flush` (fsync) → `CLOG=Committed` (set inside `flush`) → `storage.commit_txn`/`buffer_pool.commit` cleanup → deregister → release the write guard → `record_commit_and_maybe_checkpoint`. The slot returns to `Idle` (`'I'`). A read-only explicit transaction (no write guard, no writes) commits with no WAL record.
- **ROLLBACK** (or any statement error): append `Abort` → `CLOG=Aborted` → deregister → release the write guard → `Idle`. Abort is **status-based** (Milestone D1, `mvcc.md` §4 Decision 3): there is no page undo. The transaction's modified tuples stay in the heap, hidden by the CLOG and reclaimed by VACUUM. The `storage.rollback_txn`/`buffer_pool.rollback` calls still run, but `storage.rollback_txn` only restores engine-owned DDL metadata (table/index schema shadow state) and `buffer_pool.rollback` is now a bookkeeping clear that reclaims no pages. Abort is not fsync-gated (a transaction with no durable `Commit` is recovered as aborted regardless).
- **Failed (`'E'`) state**: any statement error inside an explicit block poisons it to `'E'` and does **not** end it. While `'E'`, every statement except `COMMIT`/`ROLLBACK` is rejected with `SqlState::InFailedSqlTransaction` (SQLSTATE `25P02`). `COMMIT` of an `'E'` block issues `ROLLBACK` (returns `Idle`). `COMMIT`/`ROLLBACK` with no open block are no-op warnings that stay `Idle`.
- **Autocommit**: a data/DDL statement with no open block runs as an implicit `BEGIN…COMMIT` around the one statement (allocate, snapshot, execute, commit-or-abort), preserving the prior external behavior exactly.
- **Disconnect**: an open transaction held on a dropped `Session` is aborted (status-based: `Abort` record + `CLOG=Aborted` + write-guard release + deregister, no page undo), so a client that disconnects mid-transaction leaks neither the guard nor a registry entry.

### Concurrency — Stage 2 (concurrent readers AND writers; Milestone E)

As of Milestone E2b the global writer lock is **inverted** into a shared-writer / exclusive-checkpoint guard (`common.md`, `mvcc.md` §10 E2b), so write-transactions now run concurrently.

- **Readers run lock-free.** A read-only statement/transaction takes **no** `ConcurrencyController` guard. It captures its snapshot under the active-transaction-registry latch and reads via the buffer pool's per-frame latches, so it runs concurrently with in-flight writers and skips their uncommitted versions by MVCC visibility. (Unchanged from Stage 1.)
- **Writers run concurrently.** A write transaction acquires the **SHARED** writer guard (`begin_writer`) **lazily** on its first write statement and holds the owned guard on the `Session` for the whole write-transaction, releasing it at COMMIT/ROLLBACK/disconnect. Many writers hold it at once; write-write safety comes from per-row conflict detection (E1: first-updater-wins `40001`) and the per-index / per-heap structural latches in `storage` (E2a), not from this lock. Autocommit write = acquire the shared guard for the one statement, release at the implicit commit. DDL also runs under the shared guard and commits immediately (non-transactional, rejected inside a block); a fresh DDL-built file is not yet visible to other transactions, so its backfill is uncontended.
- **Checkpoint excludes writers.** `run_checkpoint` takes the **EXCLUSIVE** checkpoint guard (`begin_checkpoint`), which drains all in-flight writers and then runs alone — preserving the Milestone-D "no in-flight writer at checkpoint" invariant verbatim (so recovery / conservative WAL truncation stay correct without a fuzzy checkpoint). The `acquire-at-most-one-writer-guard-per-transaction` reentrancy tripwire is now a cheap correctness assertion (the shared guard is re-entrant), not a deadlock guard.

Deferred from Milestone E (`mvcc.md` §12): a fully-concurrent / B-link writer protocol (so E2a takes per-index latches instead), blocking + deadlock detection (instead of fail-fast `40001`), and a fuzzy checkpoint (checkpointing with writers in flight).

### Snapshot capture (per isolation)

Snapshot capture (`capture_snapshot(own_txn)`) builds the `Snapshot` consistently with the registry and the id allocator under one registry latch (`snapshot_with_boundary`): it reads the active set, then reads `next_txn_id` as `xmax`, so a concurrently-begun writer can never be both absent from `xip` and `< xmax`. `xip = active_ids` minus `own_txn` (own writes are seen via the predicate's `current_txn` path), and `xmin = oldest active id` or `xmax` if none are active. A read uses `own_txn = 0`. Id allocation and registration are done together under the latch (`register_allocated`) to close the same torn-snapshot window. The snapshot is shared via `Arc<Snapshot>` (`StatementContext.snapshot`), so the executor clones a `StatementContext` per scan operator by bumping a refcount rather than deep-cloning the now-possibly-non-empty `xip` vector. Isolation is the capture-timing knob: **Read Committed** (default) captures a fresh snapshot per statement; **Repeatable Read** captures one snapshot at the transaction's first statement and reuses it.

`QueryService::execute_sql`/`execute_prepared` run with no cancellation; the connection uses `execute_simple` for simple queries and `execute_prepared_in_session`/`execute_prepared_cancelable` for extended `Execute` (in-transaction vs. autocommit, respectively), passing the connection's shared cancellation flag (an `Arc<AtomicBool>`) as `ExecutionContext.cancel`. The flag is cleared before each query and set when a `CancelRequest` for that backend arrives, so the in-flight query aborts with `SqlState::QueryCanceled` (SQLSTATE `57014`).

`EXPLAIN` is a query-service exception to the uniform execution path. For `BoundStatement::Explain(inner)`, `QueryService` plans `inner` to a `PhysicalPlan`, calls planner `format_explain(&physical)`, and returns `ExecutionResult::Explanation { text }` without calling `QueryEngine::execute`.

Statement guard policy:

- No guard: SELECT and EXPLAIN (lock-free readers), and a read-only explicit transaction.
- Shared writer guard (`begin_writer`, held for the whole write-transaction, many concurrent): INSERT, UPDATE, DELETE, and an explicit transaction once its first write runs. Acquired lazily.
- Shared writer guard (`begin_writer`, per statement): CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX (DDL is non-transactional and rejected inside a block).
- Exclusive checkpoint guard (`begin_checkpoint`, drains all writers, runs alone): checkpoint only.

Bind and plan run under the same statement guard as execution (for writers) so catalog state cannot change between name resolution and execution.

Write statement protocol (autocommit; an explicit write transaction is the same but the guard spans all its statements and the commit/abort happens at COMMIT/ROLLBACK):

1. Acquire the shared writer guard (lazily, on the first write in an explicit transaction; concurrent with other writers).
2. Allocate `txn_id` and register it active (atomically under the registry latch).
3. Execute storage/catalog operations.
4. If execution fails, append `Abort` (`CLOG=Aborted`), call `storage.rollback_txn(txn_id)` (DDL-metadata restore only), `buffer_pool.rollback(txn_id)` (bookkeeping clear; no page undo), and catalog `restore` when needed, then return error. Abort is status-based — the failed statement's heap versions stay invisible via the CLOG, not undone. In an explicit transaction the statement error instead poisons the block to `'E'` and the abort runs at ROLLBACK.
5. Append WAL `Commit`.
6. Flush WAL.
7. The statement/transaction is now durable and must not be rolled back or reported as a normal SQL failure.
8. Call `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)` to discard in-memory rollback metadata.
9. Release the shared writer guard.
10. Call `record_commit_and_maybe_checkpoint(&components)`.
11. Return success.

For DDL, catalog and storage mutations are part of the same statement-level commit. `CreateTable` and `DropTable` WAL replay must update both catalog and storage. Normal DDL execution must restore the previous catalog state if storage mutation, WAL append, or WAL flush fails before the commit record is durable.

If `storage.rollback_txn`, `buffer_pool.rollback`, or catalog `restore` fails before the commit record is durable, the server treats that as fatal. It logs the rollback failure, attempts to flush WAL, and exits instead of returning to service with possibly visible partial statement state.

`storage.commit_txn` and `buffer_pool.commit` are cleanup-only in-memory operations and must not perform I/O. For a valid `txn_id`, they should not fail. If either returns an error after WAL flush through the `Commit` record succeeded, the server must not call rollback and must not restore the catalog. Treat it as a fatal internal error: log it, flush WAL, and terminate the process because recovery will replay the durable commit.

Checkpoint may run after successful writes according to configured thresholds. It is called after the statement's shared writer guard is released because `run_checkpoint` acquires the exclusive checkpoint guard, which must drain all writers (including this connection's, were it still held).

`ServerComponents.storage` is the concrete `Arc<PageBackedStorageEngine>` in v1. Startup uses it for `install_schemas` and `set_mode`. Query execution passes `components.storage.as_ref()` to `ExecutionContext.storage` as `&dyn StorageEngine` and to `ExecutionContext.schema_ops` as `&dyn SchemaOperations`. Recovery passes the same concrete value as `&dyn RecoveryOperations`.

## Query Results

V1 materializes SELECT rows in `spawn_blocking` as `ExecutionResult::Query` and then writes them to the socket from the async connection task. A future streaming bridge may use a bounded channel with capacity 64, where the blocking producer owns `PlanExecutor` and the async task owns socket writes. That future change must not alter protocol message encoding or physical operator semantics.

## Checkpoint Orchestration

`ServerComponents` is the server-owned component bundle that exists before `QueryService` is constructed:

```rust
pub struct ServerComponents {
    pub config: Config,
    pub catalog: Arc<dyn CatalogManager>,
    pub storage: Arc<PageBackedStorageEngine>,
    pub buffer_pool: Arc<dyn BufferPool>,
    pub wal: Arc<dyn WalManager>,
    pub control: Arc<dyn ControlStore>,
    pub store: Arc<dyn PageStore>,
    pub concurrency: Arc<dyn ConcurrencyController>,
    pub checkpoint: CheckpointState,
    pub shutdown: Arc<ShutdownState>,
    pub next_txn_id: AtomicU64,
    pub active_txns: ActiveTxnRegistry,
    pub tls: Option<TlsAcceptor>,
    pub cancel_registry: CancelRegistry,
}

pub struct AppState {
    pub components: Arc<ServerComponents>,
    pub query_service: Arc<QueryService>,
}
```

`active_txns` is the active-transaction registry: an `ActiveTxnRegistry` wrapping
a `Mutex<BTreeSet<TxnId>>` of currently in-progress transaction ids, with an
`O(log n)` minimum. The lifecycle registers a `txn_id` when it is allocated
(`register_allocated`, which advances `next_txn_id` and inserts the id under the
same latch) and deregisters it on commit or rollback. With concurrent readers and
**concurrent** writers (Stage 2, E2b), several write transactions may be registered
at once, and a read's snapshot capture may observe any of them; the set is no longer
always empty between statements. Snapshot capture (`capture_snapshot` via
`snapshot_with_boundary`) reads `active_ids()` for `xip` (excluding the statement's
own txn) and the minimum for `xmin`, taking the registry latch across the active-set
read and the `next_txn_id` read so the snapshot is not torn relative to a
concurrent `BEGIN`. The GC horizon (Milestone F) reads its minimum. The CLOG that
records settled transaction outcomes lives in the WAL manager (`Clog`, rebuilt from
`Commit`/`Abort` records; see `docs/specs/crates/wal.md`), separate from this
registry of still-running transactions.

Checkpoint flushes dirty pages in place to the heap and advances the redo
boundary; its cost is O(pages changed), not O(database size). Driven by the
server under the **exclusive checkpoint guard** (E2b), which drains all in-flight
shared writers and runs alone:

1. Acquire the exclusive checkpoint guard (`begin_checkpoint`) — waits for all in-flight writers to drain, then holds off any new writer until the checkpoint returns.
2. `wal.flush()` (a page's redo must be durable before the page is written).
3. `buffer_pool.flush_dirty_pages()` — write every flushable dirty page to the heap `PageStore`. With the relaxed flush gate (Milestone D1, `mvcc.md` §8) this spills committed, aborted, and — under Stage 2 — in-flight dirty pages alike; all are WAL-durable after (2), and the CLOG hides the non-committed tuples.
4. `store.sync_all()` — fsync the heap before advancing the redo boundary.
5. `checkpoint_lsn = wal.flushed_lsn()`.
6. `control.store(checkpoint_lsn, sorted_table_ids, catalog_bytes)` — the durable commit point.
7. Append the `Checkpoint { redo_lsn }` WAL marker stamped with the transaction-id high-water mark (`txn_id = next_txn_id - 1`, so the allocator boundary survives truncation; see `wal.md`), `wal.flush()`, `wal.truncate_before(checkpoint_lsn)`. Truncation is **conservative**: it never drops an aborted/in-flight transaction's records (it pins on the oldest non-committed one), so aborted-but-flushed versions stay invisible across restart (`wal.md`, `mvcc.md` §5.4/§8).
8. `buffer_pool.mark_all_clean()` (clears dirty flags, re-arms `needs_fpi`).
9. Release the shared writer guard.

The durability-critical ordering is: heap fsync (4) before the control record (6) before WAL truncation (7). A crash before the control record falls back to the previous redo boundary, where this cycle's full-page images repair any torn heap writes.

`checkpoint.rs` exposes component-level APIs, not query-service APIs:

```rust
pub struct CheckpointState {
    pub last_checkpoint_lsn: AtomicU64,
    pub commits_since_checkpoint: AtomicU64,
    pub checkpoints: AtomicU64, // count of completed checkpoints (observability/tests)
}

pub fn run_checkpoint(components: &ServerComponents) -> Result<()>;
pub fn record_commit_and_maybe_checkpoint(components: &ServerComponents) -> Result<()>;
```

`run_checkpoint` resets `last_checkpoint_lsn` to the checkpoint LSN and `commits_since_checkpoint` to `0` after the control record and WAL checkpoint marker are durable. `record_commit_and_maybe_checkpoint` is called after each successful write statement, after the statement write guard has been dropped. It increments `commits_since_checkpoint` and triggers `run_checkpoint` when either `commits_since_checkpoint >= config.checkpoint_every_n_commits` or `wal.bytes_after(last_checkpoint_lsn)? >= config.checkpoint_wal_bytes`. If checkpoint fails, leave the counters unchanged except for the recorded commit so a later write can retry.

## Connection Handling

For each accepted TCP connection:

1. Create protocol codec and connection state.
2. Read bytes from socket.
3. Decode client messages.
4. Handle startup/terminate through protocol state.
5. For simple `Query` messages, run `QueryService::execute_sql` using the blocking thread pool.
6. Encode and write server messages.
7. On query execution errors, send `ErrorResponse` and `ReadyForQuery` and keep the connection open.
8. On protocol decode errors, send `ErrorResponse` and `ReadyForQuery`, then close the connection because the codec buffer state may be unrecoverable.
9. On Terminate or unrecoverable IO error, close connection.

The connection also serves the extended query protocol, holding per-connection
prepared-statement and portal maps (named and unnamed). `Parse` calls
`QueryService::prepare_sql` (mapping the declared parameter type OIDs, `0` =
unspecified) and replies `ParseComplete`. `Bind` decodes each parameter value
(text or binary, per the Bind format codes, via `decode_value`) into a portal
and replies `BindComplete`. `Describe` replies `ParameterDescription` +
`RowDescription`/`NoData` for a statement, or `RowDescription`/`NoData` in the
portal's result formats for a portal. `Execute` runs the portal on the blocking
thread pool, streaming `DataRow`s in the requested result formats followed by
`CommandComplete` (no `RowDescription`, no `ReadyForQuery`); `max_rows` is
treated as all rows. `Execute` participates in the session's CURRENT transaction:
when an explicit transaction is open on the session (`Session.txn` is `Some`), the
portal runs *inside* that transaction via `QueryService::execute_prepared_in_session`,
which routes through the same in-transaction machinery the simple-query path uses —
the session's single write guard is reused (or lazily acquired once on the first
write), the transaction's snapshot/isolation applies, the `'E'` failed-state gate
rejects non-control statements with `25P02`, and a transaction-control portal
(BEGIN/COMMIT/ROLLBACK) is dispatched through `handle_transaction_control` so it
affects `Session.txn` exactly like a simple-query control statement. With no open
transaction (`Session.txn` is `None`), `Execute` is its own autocommit unit via
`QueryService::execute_prepared_cancelable`. Routing both protocols through the one
transaction slot keeps the invariant that a connection acquires the (shared) writer
guard at most once per transaction, so an extended write on a connection already
inside a write transaction never acquires a second guard. Under E2b the shared guard
is re-entrant (a second acquire would not self-deadlock), so this is now a cheap
correctness assertion — leaking a second guard would keep a writer in flight past
commit/abort and could stall a checkpoint draining writers. `Sync` sends
`ReadyForQuery`; `Flush` flushes; `Close` drops a statement or portal and replies
`CloseComplete`. An error inside an extended sequence sends `ErrorResponse` and then
skips the remaining extended messages until `Sync`; a simple `Query` also clears
that aborted state.

The per-connection session tracks a `TransactionState` (`Idle` -> `b'I'`,
`InTransaction` -> `b'T'`, `Failed` -> `b'E'`; defaulting to `Idle`) via its
`status_byte()` mapping. Every `ReadyForQuery` the server sends sources its
transaction-status byte from this session state rather than hardcoding `b'I'`:
the startup `ReadyForQuery`, the trailing `ReadyForQuery` after each simple
query (success, error, and shutdown-rejected), the `Sync` `ReadyForQuery`, and
the decode-error `ReadyForQuery` once a session exists (the pre-startup
negotiation-error `ReadyForQuery` predates the session and uses the idle byte
directly). The session holds the open explicit transaction (`Option<Transaction>`)
and updates `TransactionState` from it after each simple query, so `ReadyForQuery`
reports `b'I'`/`b'T'`/`b'E'` per the lifecycle above. The transaction slot is moved
into the per-statement `spawn_blocking` task and taken back with the result, so the
whole statement (including any owned write guard) runs on one thread. The same slot
is threaded through extended-protocol `Execute` (see above), which moves the slot
into its blocking task and takes it back, so both protocols share one transaction
context and the guard is acquired at most once per connection. On disconnect the
session's `Drop` aborts any open transaction so the write guard and registry entry
are not leaked.

SSL negotiation happens before startup. A client may lead with an `SSLRequest`. When TLS is configured (`--tls-cert-file`/`--tls-key-file`), the server replies `SslAccepted` (`S`), performs the TLS handshake, and serves the rest of the session over the encrypted stream; otherwise it replies `SslRejected` (`N`) and the client continues in plaintext. TLS is server-side only; no client certificate is requested or verified. A client may also lead with a `GSSENCRequest` (GSSAPI transport encryption), which is unsupported: the server declines it with a single `N` byte and keeps negotiating, since the client typically follows with an `SSLRequest` or `StartupMessage`. A client that opens directly with a `StartupMessage` is served in plaintext. If a client bundles data after an `SSLRequest`/`GSSENCRequest` before receiving the negotiation reply, the server treats it as a protocol error, sends `ErrorResponse` and `ReadyForQuery`, and closes the connection.

Query cancellation uses a process-wide `CancelRegistry` on `ServerComponents` mapping a per-connection `BackendKey { process_id, secret_key }` to that connection's cancellation flag. At startup the server allocates a key (a counter-based `process_id` and a random `secret_key`), registers the connection's flag, and sends `BackendKeyData` after the `ParameterStatus` messages and before `ReadyForQuery`. A `CancelRequest` arrives on its own connection (handled during negotiation, before startup): the server looks up the `BackendKey`, sets the matching flag, and closes without any reply; an unknown or stale key is ignored. The connection deregisters its key on disconnect. See the cancellation flag plumbing under Connection Handling.

## Graceful Shutdown

`ServerComponents` owns a `shutdown: Arc<ShutdownState>` used by the listener and connection tasks. `ShutdownState` tracks whether the server is still accepting new work and counts in-flight query executions. A query increments the count before entering `spawn_blocking` and decrements after its response is encoded or an error response is written. If shutdown has begun, `begin_query` returns `ErrorKind::Internal` / `SqlState::InternalError` with message `server is shutting down`.

On SIGINT/SIGTERM:

1. Stop accepting new connections.
2. Wait for in-flight queries up to `Config.shutdown_timeout_ms`.
3. If all in-flight queries finish before the timeout, run checkpoint, flush WAL, close files, and exit successfully.
4. If the timeout expires, skip checkpoint and skip the final WAL flush, return an internal timeout error, and let process shutdown proceed without running finalization concurrently with in-flight query execution. Successful write statements still flush their own commit records before returning.

If checkpoint fails during shutdown, log the error and exit. WAL durability still preserves committed changes.

## Acceptance Tests

- Startup with no control record creates empty catalog and empty storage.
- Startup with a control record loads the redo boundary and catalog.
- Recovery replays only committed records after the control record's checkpoint LSN.
- Failed write rolls back buffer pages and does not append commit.
- Successful write appends commit, flushes WAL, commits buffer before returning.
- Checkpoint flushes dirty pages to the heap and advances the control checkpoint LSN.
- Protocol startup and simple query work over a loopback TCP connection.
- An extended-protocol Parse/Bind/Describe/Execute/Sync sequence runs a parameterized query over a loopback connection with both text and binary parameter and result encodings.
- An error inside an extended sequence is reported and the following messages are skipped until Sync, after which the connection is reusable.
- Startup sends `BackendKeyData`, and a `CancelRequest` carrying a registered backend key sets that backend's cancellation flag (and is ignored for an unknown key).
- With TLS disabled, an `SSLRequest` is rejected with `N` and the same connection then completes a plaintext startup.
- With TLS enabled, an `SSLRequest` is accepted with `S`, the TLS handshake completes, and a simple query runs over the encrypted stream.
- Supplying exactly one of `--tls-cert-file`/`--tls-key-file` is rejected during config parsing.
- A `GSSENCRequest` is declined with `N`; the client may then negotiate SSL or start in plaintext on the same connection.
- Graceful shutdown runs checkpoint after in-flight query completes.
