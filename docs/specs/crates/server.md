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
- `snapshot`
- `catalog`

No library crate depends on `server`.

## Configuration

```rust
pub struct Config {
    pub data_dir: PathBuf,
    pub port: u16,
    pub buffer_pool_frames: usize,
    pub checkpoint_every_n_commits: u64,
    pub checkpoint_wal_bytes: u64,
    pub shutdown_timeout_ms: u64,
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

Binary CLI flags:

- `--data-dir <PATH>` sets `Config.data_dir`; default `./data`.
- `--port <PORT>` sets `Config.port`; default `5433`.
- `--buffer-pool-frames <N>` sets `Config.buffer_pool_frames`; default `1024`.
- `--checkpoint-every-n-commits <N>` sets `Config.checkpoint_every_n_commits`; default `100`.
- `--checkpoint-wal-bytes <BYTES>` sets `Config.checkpoint_wal_bytes`; default `67108864`.
- `--shutdown-timeout-ms <MS>` sets `Config.shutdown_timeout_ms`; default `30000`.
- `--help` prints usage and exits with code `0`.

V1 parses flags with `std::env::args`; do not add a CLI parser dependency. `--port` accepts `1..=65535`; all other numeric flags must be positive nonzero integers. Unknown flags, missing values, non-numeric numeric values, or out-of-range numeric values print usage to stderr and exit with code `2`.

## Startup Sequence

1. Load configuration.
2. Initialize snapshot manager.
3. Create the server-owned `SnapshotPageLoader` from the snapshot manager.
4. Initialize buffer pool with the configured frame count, never-flush policy, and snapshot page loader.
5. Initialize WAL manager.
6. Load current snapshot with `snapshot_manager.load_current(buffer_pool)`, returning table pages in the buffer pool plus catalog bytes in `LoadedSnapshot`.
7. Initialize storage engine in recovery mode with `PageBackedStorageEngine::open(buffer_pool.clone(), wal.clone(), StorageMode::Recovery)`.
8. Initialize catalog from snapshot catalog bytes, or empty catalog if no snapshot exists.
9. Call `storage.install_schemas(catalog.list_tables()?)` and `storage.rebuild_directories()` so page-backed storage has schemas and primary-key directories before WAL replay.
10. Replay committed WAL records with `LSN > checkpoint_lsn` through `WalManager::replay_committed_from` and `RecoveryOperations`.
11. Create `ServerComponents` with catalog, storage, buffer pool, WAL, snapshot manager, concurrency controller, shutdown state, checkpoint state initialized from the loaded manifest checkpoint LSN, and `next_txn_id` initialized to one greater than the maximum retained user WAL `txn_id`.
12. Clean up orphaned snapshots.
13. If WAL records were replayed, run checkpoint with `run_checkpoint(&components)` to persist recovered state.
14. Switch storage engine to normal mode with `storage.set_mode(StorageMode::Normal)`.
15. Construct query service from `components`.
16. Start Tokio runtime and bind listener.

Recovery mode must not append WAL records.

Recovery computes `next_txn_id` from all retained WAL records with stored `LSN > checkpoint_lsn` by calling `WalManager::replay_from(checkpoint_lsn)`, not from `replay_committed_from`. Include committed operations, uncommitted operations, and `Commit` records; ignore only records with `txn_id = 0`. `ServerComponents.next_txn_id` starts at `max_txn_id + 1`, or `1` when no user transaction records remain. This prevents a new statement from reusing an old uncommitted `txn_id` and accidentally making pre-crash records look committed.

## Query Service Wiring

The concrete `QueryService` in `crates/server/src/query.rs` performs:

```text
parse(sql)
bind(statement, catalog)
logical_plan(bound)
physical_plan(logical, catalog)
engine.execute(execution_context, physical)
```

The server constructs `ExecutionContext { statement, catalog, storage, schema_ops }` for each physical plan. The `QueryEngine` receives the server-allocated `StatementContext` and never allocates transaction IDs, appends commit records, flushes WAL, or calls storage/buffer commit or rollback.

`EXPLAIN` is the only query-service exception to the uniform execution path. For `BoundStatement::Explain(inner)`, `QueryService` acquires the read guard, plans `inner` to a `PhysicalPlan`, calls planner `format_explain(&physical)`, and returns `ExecutionResult::Explanation { text }` without calling `QueryEngine::execute`.

Statement guard policy:

- Read guard: SELECT and EXPLAIN.
- Write guard: INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, checkpoint.

`QueryService::execute_sql` parses SQL first to classify the top-level statement, then acquires the read/write guard before bind or planning. Bind and plan run under the same statement guard as execution so catalog state cannot change between name resolution and execution.

Write statement protocol:

1. Acquire write guard.
2. Allocate `txn_id`.
3. Execute storage/catalog operations.
4. If execution fails, call `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)` and return error.
5. Append WAL `Commit`.
6. Flush WAL.
7. The statement is now durable and must not be rolled back or reported as a normal SQL failure.
8. Call `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)` to discard in-memory rollback metadata.
9. Release write guard.
10. Call `record_commit_and_maybe_checkpoint(&components)`.
11. Return success.

For DDL, catalog and storage mutations are part of the same statement-level commit. `CreateTable` and `DropTable` WAL replay must update both catalog and storage. Normal DDL execution must restore the previous catalog state if storage mutation, WAL append, or WAL flush fails before the commit record is durable.

`storage.commit_txn` and `buffer_pool.commit` are cleanup-only in-memory operations and must not perform I/O. For a valid `txn_id`, they should not fail. If either returns an error after WAL flush through the `Commit` record succeeded, the server must not call rollback and must not restore the catalog. Treat it as a fatal internal error: log it, flush WAL, and terminate the process because recovery will replay the durable commit.

Checkpoint may run after successful writes according to configured thresholds. It is called after the statement write guard is released because `run_checkpoint` acquires its own write guard.

`ServerComponents.storage` is the concrete `Arc<PageBackedStorageEngine>` in v1. Startup uses it for `install_schemas`, `rebuild_directories`, and `set_mode`. Query execution passes `components.storage.as_ref()` to `ExecutionContext.storage` as `&dyn StorageEngine` and to `ExecutionContext.schema_ops` as `&dyn SchemaOperations`. Recovery passes the same concrete value as `&dyn RecoveryOperations`.

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
    pub snapshot_manager: Arc<dyn SnapshotManager>,
    pub concurrency: Arc<dyn ConcurrencyController>,
    pub checkpoint: CheckpointState,
    pub shutdown: Arc<ShutdownState>,
    pub next_txn_id: AtomicU64,
}

pub struct AppState {
    pub components: Arc<ServerComponents>,
    pub query_service: Arc<QueryService>,
}
```

Checkpoint is driven by server or a checkpoint service:

1. Acquire write guard.
2. Read `checkpoint_lsn = wal.flushed_lsn()`.
3. Begin snapshot.
4. Compose table pages from buffer dirty pages plus current snapshot clean pages.
5. Serialize catalog into snapshot writer.
6. Commit snapshot.
7. Mark all buffer pages clean.
8. Append WAL checkpoint metadata with `txn_id: 0`.
9. Truncate WAL before `checkpoint_lsn`.
10. Clean up old snapshots.
11. Release write guard.

Checkpoint must not delete the previous snapshot before manifest swap is durable.

Page composition is server-owned. For each live catalog table, server loads clean pages with `snapshot_manager.current_table_pages(table_id)`, overlays matching pages from `buffer_pool.iter_pages()`, sorts by page number, and passes `Vec<SnapshotPage>` to `SnapshotWriter::write_table`. The snapshot manager writes page-numbered table files and does not decide which tables are live.

`checkpoint.rs` exposes component-level APIs, not query-service APIs:

```rust
pub struct CheckpointState {
    pub last_checkpoint_lsn: AtomicU64,
    pub commits_since_checkpoint: AtomicU64,
}

pub fn run_checkpoint(components: &ServerComponents) -> Result<()>;
pub fn record_commit_and_maybe_checkpoint(components: &ServerComponents) -> Result<()>;
```

`run_checkpoint` resets `last_checkpoint_lsn` to the checkpoint LSN and `commits_since_checkpoint` to `0` only after the snapshot manifest and WAL checkpoint metadata are durable. `record_commit_and_maybe_checkpoint` is called after each successful write statement, after the statement write guard has been dropped. It increments `commits_since_checkpoint` and triggers `run_checkpoint` when either `commits_since_checkpoint >= config.checkpoint_every_n_commits` or `wal.bytes_after(last_checkpoint_lsn)? >= config.checkpoint_wal_bytes`. If checkpoint fails, leave the counters unchanged except for the recorded commit so a later write can retry.

## Connection Handling

For each accepted TCP connection:

1. Create protocol codec and connection state.
2. Read bytes from socket.
3. Decode client messages.
4. Handle startup/SSL/terminate through protocol state.
5. For query messages, run `QueryService` using the blocking thread pool.
6. Encode and write server messages.
7. On query execution errors, send `ErrorResponse` and `ReadyForQuery` and keep the connection open.
8. On protocol decode errors, send `ErrorResponse` and `ReadyForQuery`, then close the connection because the codec buffer state may be unrecoverable.
9. On Terminate or unrecoverable IO error, close connection.

## Graceful Shutdown

`ServerComponents` owns a `shutdown: Arc<ShutdownState>` used by the listener and connection tasks. `ShutdownState` tracks whether the server is still accepting new work and counts in-flight query executions. A query increments the count before entering `spawn_blocking` and decrements after its response is encoded or an error response is written. If shutdown has begun, `begin_query` returns `ErrorKind::Internal` / `SqlState::InternalError` with message `server is shutting down`.

On SIGINT/SIGTERM:

1. Stop accepting new connections.
2. Wait for in-flight queries up to `Config.shutdown_timeout_ms`.
3. Run checkpoint.
4. Flush WAL.
5. Close files and exit.

If checkpoint fails during shutdown, log the error and exit. WAL durability still preserves committed changes.

## Acceptance Tests

- Startup with no manifest creates empty catalog and empty storage.
- Startup with manifest loads snapshot metadata and catalog.
- Recovery replays only committed records after manifest checkpoint LSN.
- Failed write rolls back buffer pages and does not append commit.
- Successful write appends commit, flushes WAL, commits buffer before returning.
- Checkpoint creates new snapshot and advances manifest checkpoint LSN.
- Protocol startup and simple query work over a loopback TCP connection.
- Graceful shutdown runs checkpoint after in-flight query completes.
