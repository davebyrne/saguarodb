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
- `compress` — constructs the shared `CompressionRegistry` and `DictStore` at startup and injects them into `storage`/the heap page store (`docs/specs/compression.md` §5a/§7)

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
    pub auto_vacuum_dead_rows: u64,
    pub shutdown_timeout_ms: u64,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
}
```

The server fsyncs WAL on every commit. There is no `wal_flush_interval_ms` in the server spec.

Defaults:

- `data_dir = "./data"`
- `port = 5433`
- `buffer_pool_frames = 1024`
- `checkpoint_every_n_commits = 100`
- `checkpoint_wal_bytes = 64 * 1024 * 1024`
- `auto_vacuum_dead_rows = 10000`
- `shutdown_timeout_ms = 30000`
- `tls_cert_file = None`
- `tls_key_file = None`

Binary CLI flags:

- `--data-dir <PATH>` sets `Config.data_dir`; default `./data`.
- `--port <PORT>` sets `Config.port`; default `5433`.
- `--buffer-pool-frames <N>` sets `Config.buffer_pool_frames`; default `1024`.
- `--checkpoint-every-n-commits <N>` sets `Config.checkpoint_every_n_commits`; default `100`.
- `--checkpoint-wal-bytes <BYTES>` sets `Config.checkpoint_wal_bytes`; default `67108864`.
- `--auto-vacuum-dead-rows <N>` sets `Config.auto_vacuum_dead_rows`; default `10000`. When at least this many committed dead versions have accumulated since the last auto-prune, the next checkpoint folds a VACUUM pass over every user table into itself (Milestone F4b, `mvcc.md` §9). `0` disables auto-prune (space is then bounded only by explicit `VACUUM`); unlike the other numeric flags, `0` is accepted here.
- `--shutdown-timeout-ms <MS>` sets `Config.shutdown_timeout_ms`; default `30000`.
- `--tls-cert-file <PATH>` sets `Config.tls_cert_file`; PEM certificate chain. Optional; defaults to disabled.
- `--tls-key-file <PATH>` sets `Config.tls_key_file`; PEM private key. Optional; defaults to disabled.
- `--help` prints usage and exits with code `0`.

The binary parses flags with `std::env::args`; do not add a CLI parser dependency. `--port` accepts `1..=65535`; all other numeric flags must be positive nonzero integers. Unknown flags, missing values, non-numeric numeric values, or out-of-range numeric values print usage to stderr and exit with code `2`. TLS is enabled only when both `--tls-cert-file` and `--tls-key-file` are supplied; supplying exactly one is an error that prints usage to stderr and exits with code `2`.

## Startup Sequence

1. Load configuration.
2. Construct the shared compression state (`docs/specs/compression.md` §5a/§7): one `compress::CompressionRegistry` instance and one `compress::DictStore` (over `<data>/dicts`, created if absent). Initialize the control store (`FileControlStore`) and the heap page store — `HeapPageStore::open_with_compression(<data>/heap, compression.clone())` — sharing that SAME registry instance so a file's at-rest config is consulted consistently by the heap store here and by storage's WAL-FPI path (step 8).
3. Initialize the WAL manager.
4. Initialize the buffer pool with the configured frame count, the `WalFlushPolicy`, and the heap page store as its `PageStore`. `WalFlushPolicy::can_flush` admits a dirty page iff it is **WAL-durable** (`page_lsn ≤ wal.flushed_lsn()`); the earlier committedness gate is dropped (Milestone D1, `mvcc.md` §8), so uncommitted/aborted dirty pages may be flushed/evicted (hidden by the CLOG). `WalFlushPolicy::ensure_durable` (called by the buffer pool's steal path before writing a stolen page) flushes the WAL, giving write-ahead logging for the now-possibly-uncommitted stolen page.
5. Enable eviction-flush-on-steal (`buffer_pool.enable_stealing()`), immediately after constructing the pool and before loading the control record. The durable on-disk index means recovery rebuilds nothing in memory, so redo may spill and the recovery working set is not bounded by the pool size.
6. Load the control record (`control.load()`): the redo boundary `checkpoint_lsn` and catalog bytes (none if no control record exists yet).
7. Initialize catalog from the control catalog bytes, or empty catalog if no control record exists.
8. Initialize storage engine in recovery mode with `PageBackedStorageEngine::open_with_compression(buffer_pool.clone(), wal.clone(), StorageMode::Recovery, compression.clone())`, sharing the same `CompressionRegistry` instance constructed in step 2.
9. Call `storage.install_schemas(catalog.list_tables()?)`, `storage.install_index_schemas(indexes)`, and `storage.install_sequences(catalog.list_sequences()?)`, where `indexes` is gathered via `catalog.list_indexes_for_table` for each table, so recovery replay and later DML maintain secondary indexes and runtime sequence state. Installing schemas also registers each table's/index's compression config into the shared registry (heap = the table's codec + trained dictionary, index files = the same codec but always dict-less — `docs/specs/compression.md` §4, §5a).
10. Seed the dictionary resolver from the durable dictionary files, **before redo runs**: for every `(dict_id, table_id, bytes)` returned by `dict_store.load_all()`, call `compression.register_dictionary(dict_id, &bytes)`, then advance the catalog's dictionary-id allocator past the highest loaded id (`catalog.reserve_dictionary_id`). This must precede step 11 so a dict-compressed page envelope or WAL FPI replayed there can already resolve its `dict_id`. An orphaned dictionary file (a crash between the file becoming durable and its `CreateDictionary` WAL record's commit) is loaded and registered the same way — harmless — and its id is burned regardless, so a later allocation never collides with it (`docs/specs/compression.md` §7).
10a. Validate referenced dictionaries (fail fast): for every table returned by `catalog.list_tables()` whose `active_dict_id` or `toast.active_dict_id` is `Some(id)`, check `compression.has_dictionary(id)`; if `id` is not registered by step 10's seeding, return a structured internal `DbError` naming the table, dict field, and dict id instead of proceeding. This first boot-time check runs after step 10's seeding and before step 11's replay, catching a deleted/missing `.dict` file from the checkpointed catalog loudly and immediately rather than as a later, confusing decode error on first read of a dict-compressed page or TOAST value (`docs/specs/compression.md` §7). It validates only each table's CURRENT active dict fields; a historical dict id referenced by an older `FullPageImageCompressed` WAL record is unchecked but always present too, since dict files are never deleted in v1.
11. Redo-all: replay every record with `LSN > checkpoint_lsn` (`WalManager::replay_from`) via `storage::apply_physical_redo` (PageLSN-gated; torn/missing pages are zeroed so a `FullPageImage`/`HeapInit` re-establishes them), regardless of the dirtying transaction's outcome — the CLOG (rebuilt at WAL open) decides visibility afterward, and an aborted/in-flight transaction's replayed versions are invisible (`mvcc.md` §8). Heap, primary-key-index, and secondary-index pages replay the same way. Before dispatch, `apply_redo` normalizes a `FullPageImageCompressed` record to a decompressed raw `FullPageImage` (`compression.decompress_fpi(codec, dict_id, payload, PAGE_SIZE)`, resolving `dict_id` against the resolver seeded in step 10) so `storage::apply_physical_redo` only ever handles the raw variant; an unresolvable `dict_id` at this point is a fatal structured recovery error (a normal crash never removes a referenced dictionary file — see `docs/specs/compression.md` §7's durability ordering). The `Commit`/`Abort`/`Checkpoint` markers are skipped (they are not page mutations). DDL records (`CreateTable`/`DropTable`/`CreateIndex`/`DropIndex`/`CreateSequence`/`DropSequence`) replay **only when their transaction is committed** (they mutate the durable catalog directly, not idempotent PageLSN-gated pages, so an aborted DDL's catalog change must not take effect). Table/index/sequence DDL records also replay through `RecoveryOperations`. `SequenceAdvance` and `SetSequenceValue` replay unconditionally into storage's sequence state because sequence values are non-transactional. `CreateDictionary`, `AlterTableCompression`, and `AlterTableToast` are classified alongside the other DDL records (`is_logical_catalog_record`) and replay under the same committed-only gate: a committed `CreateDictionary` (idempotently) re-saves the dictionary file and re-registers it; a committed `AlterTableCompression` applies `catalog.set_table_compression` then `storage.apply_set_table_compression`; and a committed `AlterTableToast` applies `catalog.set_table_toast_metadata` then `storage.apply_set_table_toast_metadata`. For skipped aborted/in-flight create records, recovery still reserves the table/index/sequence/dictionary ID so any replayed orphan page files or catalog IDs cannot be reused by a later object. If replay applied records, recovery repeats the dictionary-reference validation after step 11 so a committed metadata record replayed after the checkpoint cannot introduce an unresolved `active_dict_id` or `toast.active_dict_id`.
12. Create `ServerComponents` with catalog, storage, buffer pool, WAL, control store, heap store, the shared `compression` registry and `dict_store` (steps 2/8/10), concurrency controller, shutdown state, checkpoint state initialized from the control `checkpoint_lsn`, `next_txn_id` initialized from the allocator scan over all retained WAL records (`replay_from(0)`, including committed subxids and the `Checkpoint` marker high-water), and an empty `active_txns` registry (the WAL manager reconstructed its CLOG on `open` — seeded from the durable `clog.dat` snapshot when present plus a fold of the post-snapshot `Commit`/`Abort` records, else rebuilt from those records).
13. If records were replayed, run `run_checkpoint(&components)` to persist the redone state to the heap and index and advance the redo boundary.
14. Switch storage engine to normal mode with `storage.set_mode(StorageMode::Normal)`.
15. Construct query service from `components`.
16. Start Tokio runtime and bind listener.

Recovery mode must not append WAL records.

Recovery computes `next_txn_id` from all retained WAL records by calling
`WalManager::replay_from(0)`, not `replay_committed_from`. This intentionally scans
records below the control record's `checkpoint_lsn` when they are still retained:
that covers the crash window where the manifest and CLOG snapshot are durable but
the `Checkpoint` marker carrying the transaction-id high-water has not yet been
appended/flushed. After a completed checkpoint truncates below the boundary, the
retained `Checkpoint` marker carries the high-water mark so the allocator boundary
is still recovered even when every data record below the checkpoint was removed.
Include committed operations, uncommitted operations, `Commit` records, committed
subxids in `CommitWithSubxids`, and the `Checkpoint` marker's high-water; ignore
only records with `txn_id = 0`.
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
(see `wal.md` "Implicit-committed floor" and `mvcc.md` §5.4). When the CLOG was
seeded from a durable `clog.dat` snapshot the loaded `committed_floor` is
authoritative, so this call is a **no-op** — re-deriving the floor from the
unconditionally-truncated WAL could float it past an un-vacuumed aborted
transaction whose tuples survive (corruption). It re-derives the floor only in the
**no-snapshot fallback** (fresh database, or a pre-durable-CLOG data directory):
the oldest transaction in the retained WAL whose CLOG status is not `Committed`
(aborted or in-flight), or the allocation boundary if every retained transaction is
committed — never crossing a non-committed transaction. That fallback is safe
because the older build conservatively truncated the WAL, guaranteeing every
transaction dropped below the oldest non-committed one was committed.

## Query Service Wiring

The concrete `QueryService` in `crates/server/src/query.rs` performs:

```text
parse(sql)
bind(statement, catalog)
logical_plan(bound)
physical_plan(logical, catalog)
engine.execute(execution_context, physical)
```

The server constructs `ExecutionContext { statement, catalog, storage, schema_ops, cancel }` for each physical plan. The `StatementContext` carries the server-allocated transaction id, snapshot, lock/SSI/cancel handles, the concrete storage engine as `Arc<dyn SequenceManager>`, and the connection-owned session sequence state for `currval`. The `QueryEngine` receives that context and never allocates transaction IDs, appends commit records, flushes WAL, or calls storage/buffer commit or rollback.

For autocommit write and DDL statements, `bind(statement, catalog)` runs after
the statement has acquired its statement guard: the shared writer guard for DML,
or the exclusive checkpoint guard for DDL. Read statements still bind lock-free,
then the server inspects the bound tree; if it contains `nextval` or `setval`,
it is re-routed through the shared writer path so sequence advancement is WAL
logged and committed like other writes. This keeps write-side catalog resolution
inside the same exclusion window as planning and execution.

### Transaction lifecycle (Milestone C)

The query path is a real transaction lifecycle; autocommit is an implicit single-statement transaction routed through the same machinery. A simple query carries the connection's transaction slot (`Option<Transaction>`, held on the `Session`) and its session-default isolation (`Session.default_isolation`, see below) into `QueryService::execute_simple(sql, slot, default_isolation, cancel)`, which returns the (possibly mutated) slot **and** the (possibly updated) session default. The connection derives its `ReadyForQuery` byte from the returned slot (`I`/`T`/`E`) and persists the returned default on the `Session`.

- **BEGIN**: allocate a `txn_id` (and register it active) atomically under the registry latch, set the slot to an open `InTransaction` (`'T'`) at the requested isolation level (`BEGIN [TRANSACTION] ISOLATION LEVEL <level>` / `START TRANSACTION ISOLATION LEVEL <level>`; `Transaction.isolation`). A `BEGIN` with **no** explicit level inherits the session default (`begin_transaction(isolation.unwrap_or(session_default))`); an explicit level overrides it for that one transaction. Inheritance precedence: explicit `BEGIN` level > `SET TRANSACTION` > session default > Read Committed (`mvcc.md` §10 Milestone G2). `BEGIN` inside an open block is a no-op warning that stays `'T'` (Postgres-compatible) and ignores any requested level. DDL inside a block is rejected (`FeatureNotSupported`); DDL is non-transactional.
- **SET TRANSACTION** (`SET TRANSACTION ISOLATION LEVEL <level>`): set the **current** transaction's `Transaction.isolation`, valid only **before its first query** (`Transaction.first_statement_ran`, set when a statement captures the transaction snapshot). After the first query it errors (`FeatureNotSupported`, "must be called before any query") and poisons the block to `'E'`. Inside an already-failed (`'E'`) block it is rejected with `25P02` like any non-COMMIT/ROLLBACK statement and stays `'E'`. With no open transaction it is a no-op success that stays `Idle`. The four SQL levels map onto three (`READ UNCOMMITTED`/`READ COMMITTED` → Read Committed; `REPEATABLE READ`/`SNAPSHOT` → Repeatable Read; `SERIALIZABLE` → Serializable / SSI, see `docs/specs/ssi.md`); `READ WRITE` is accepted-and-ignored and `READ ONLY` is rejected at parse time (`mvcc.md` §10 Milestone G1). Activating Repeatable Read is just this wiring: the per-transaction snapshot / advertisement / write-conflict machinery already exists (C–F).
- **SET SESSION CHARACTERISTICS** (`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`): set the **per-connection default** isolation (`Session.default_isolation`, default Read Committed) used by FUTURE transactions, threaded in/out of `execute_simple` beside the transaction slot. It does **not** change an already-open transaction's `Transaction.isolation` (unlike `SET TRANSACTION`, it has no before-first-query rule and is allowed inside a block — Postgres-compatible). With no isolation-level mode (e.g. `READ WRITE` only) it is a no-op success. Inside an already-failed (`'E'`) block it is rejected with `25P02` like any non-COMMIT/ROLLBACK statement, leaving the default unchanged. Inside a healthy transaction block, the new default is visible to `SHOW default_transaction_isolation` immediately but persists only if that block commits; rollback or failed-block `COMMIT` discards it. The default persists across committed transactions on the connection and resets to Read Committed per new connection (the field is per-`Session`). Same level mapping and access-mode handling as `SET TRANSACTION`. Command tag `SET` (`mvcc.md` §10 Milestone G2).
- **Session configuration GUCs** (`SET`/`SHOW`/`RESET` and `DISCARD ALL`): the server owns a per-connection accept-all `SessionGucs` store for driver compatibility. `SET <name> = <value>` records arbitrary parameters and `SHOW <name>` returns stored or built-in values; SHOW of a never-seen unknown parameter returns `UndefinedObject` (`42704`). `SET LOCAL` for ordinary stored driver GUCs is accepted as session-scoped, a documented compatibility divergence; PostgreSQL-special isolation GUCs preserve `LOCAL` semantics. `SHOW ALL` returns `name`, `setting`, and empty `description` columns sorted by name. `RESET <name>` restores the session-start value if one exists, or removes a custom parameter; `RESET ALL` restores the GUC store and resets `default_transaction_isolation` to Read Committed, using the same transaction-scoped persistence rules as setting that parameter directly. `DISCARD ALL` is rejected inside any transaction block with `FeatureNotSupported` and poisons the block; outside a block it performs the same configuration reset, clears session sequence `currval` memory, deallocates all prepared statements and portals on the connection, and returns command tag `DISCARD ALL`. `application_name` changes are re-reported to the client with `ParameterStatus`; other startup-reported parameters are fixed. `transaction_isolation` is not an inert stored string: `SHOW transaction_isolation` reports the open transaction's isolation, otherwise the session default, and `SET transaction_isolation = <level>` is equivalent to `SET TRANSACTION ISOLATION LEVEL <level>` with the same no-open-transaction no-op and before-first-query rule. `default_transaction_isolation` controls only the session default for future transactions, equivalent to `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`: inside an open transaction, a regular `SET` is visible immediately and applied to the session only at commit, while `SET LOCAL` is visible only until transaction end and has no effect outside a transaction. `ROLLBACK TO SAVEPOINT` restores the isolation-default state captured when the savepoint was created; `RELEASE SAVEPOINT` keeps later changes merged into the parent transaction. Supported text values are PostgreSQL spellings, case-insensitive: `read uncommitted` (strengthened to Read Committed), `read committed`, `repeatable read`, and `serializable`; `DEFAULT` resets to Read Committed for the default GUC and to the currently visible default for `transaction_isolation`. Session-configuration statements are accepted through both the simple and extended query protocols: `Parse` classifies them without binding, `Describe` reports the row shape for `SHOW` and `NoData` for `SET`/`RESET`/`DISCARD ALL`, and `Execute` routes through the session path so GUC state, transaction-state gating, and `DISCARD ALL` cleanup match simple-query execution.
- **Statements inside the block** share the transaction's `txn_id`; writes are stamped with it; reads use the transaction's snapshot (per isolation, below). A Repeatable Read transaction whose write targets a row another transaction changed and committed after its snapshot surfaces `40001` (`SerializationFailure`) via the existing first-updater-wins detection.
- **COMMIT**: append `Commit` → `flush` (fsync) → `CLOG=Committed` (set inside `flush`) → `storage.commit_txn`/`buffer_pool.commit` cleanup → deregister → release the write guard → best-effort `record_commit_and_maybe_checkpoint`. The slot returns to `Idle` (`'I'`). A read-only explicit transaction (no write guard, no writes) commits with no WAL record. If the post-commit checkpoint trigger fails, the transaction remains committed, the slot still returns to `Idle`, and the client still receives commit success; the checkpoint error is logged and a later write/shutdown checkpoint can retry.
- **ROLLBACK** (or any statement error): append `Abort` → `CLOG=Aborted` → deregister → metadata/bookkeeping cleanup → release the write guard → `Idle`. The transaction is deregistered only after the Abort append succeeds; if that append fails, the rollback path is treated as a pre-durable cleanup failure (fatal on normal query paths, returned by direct internal/test calls) and the transaction remains active. Abort is **status-based** (Milestone D1, `mvcc.md` §4 Decision 3): there is no page undo. The transaction's modified tuples stay in the heap, hidden by the CLOG and reclaimed by VACUUM. The `storage.rollback_txn`/`buffer_pool.rollback` calls still run after deregistration, but `storage.rollback_txn` only restores engine-owned DDL metadata (table/index schema shadow state) and `buffer_pool.rollback` is now a bookkeeping clear that reclaims no pages. If that cleanup fails, normal query paths exit fatally rather than returning to service with uncertain metadata; direct internal/test calls surface the error. Abort is not fsync-gated (a transaction with no durable `Commit` is recovered as aborted regardless).
- **Failed (`'E'`) state**: any statement error inside an explicit block poisons it to `'E'` and does **not** end it. While `'E'`, every statement except `COMMIT`/`ROLLBACK` is rejected with `SqlState::InFailedSqlTransaction` (SQLSTATE `25P02`). `COMMIT` of an `'E'` block issues `ROLLBACK` (returns `Idle`). `COMMIT`/`ROLLBACK` with no open block are no-op warnings that stay `Idle`.
- **Autocommit**: a data/DDL statement with no open block runs as an implicit `BEGIN…COMMIT` around the one statement (allocate, snapshot, execute, commit-or-abort), preserving the prior external behavior exactly. A single autocommit statement has exactly one snapshot, so Read Committed vs Repeatable Read is functionally moot for it; the session default is **not** threaded into the autocommit single-statement snapshot path.
- **Maintenance (`VACUUM [table]`, `ALTER TABLE <t> SET (compression = ...)`, and `ALTER TABLE <t> SET (toast...)`)**: classified `StatementClass::Maintenance` (`statement_class`); they do not bind or plan, and are rejected inside an explicit transaction block — the rejection message is the generalized `"maintenance commands cannot run inside a transaction block"` (`FeatureNotSupported`), not a VACUUM-specific string; the block is poisoned to `'E'` exactly like a DDL-in-block rejection. `dispatch` (simple-query path) and the prepared-statement dispatch (`prepare_sql`/`execute_prepared_*`, which carries the raw parsed `Statement` unbound as `PreparedStatement.maintenance` since maintenance takes no parameters) both route through the single shared router `QueryService::run_maintenance(statement)`, which matches on the statement kind:
  - `Statement::Vacuum { .. }` → `run_vacuum`, which resolves the target table(s) (`VACUUM` = every user table, excluding hidden TOAST relations as top-level targets; `VACUUM t` = just `t`, error if it does not exist), acquires the **exclusive** checkpoint guard (`begin_checkpoint`) for the whole pass, captures `gc_horizon()` **once after the guard is held**, and vacuums each target. For TOAST-enabled tables it first asks storage for external value ids owned by parent tuples that full VACUUM would prune. If any visible hidden chunks exist, the server allocates and registers a real maintenance transaction id, deletes those chunks through normal storage deletes, appends and flushes a `Commit`, runs storage/buffer post-commit cleanup, deregisters the xid, and wakes row-lock waiters. This maintenance commit deliberately does **not** call `record_commit_and_maybe_checkpoint_after_durable_commit`, avoiding recursive checkpoint auto-prune while already inside VACUUM/checkpoint maintenance. If the chunk delete or pre-durable commit fails, the server aborts that maintenance xid and does not prune the parent table in that pass; if post-durable cleanup fails, the server exits fatally, matching normal durable-commit cleanup failures. After the cleanup check succeeds, it calls `engine.vacuum_after_toast_cleanup(schema, horizon)` for the parent (heap-prune → index-vacuum → line-pointer-reclaim) and then `engine.vacuum_hidden_toast_relation(schema, toast_horizon)`, where `toast_horizon = max(horizon, cleanup_xid + 1)` when a cleanup xid committed. If no visible chunks were deleted because the pending chunks are aborted, the same coordinated parent prune is still used and hidden-relation VACUUM reclaims the aborted chunks by their own MVCC headers. Command tag `VACUUM`. Safe because no writer runs under the exclusive guard and the horizon accounts for active reader snapshots, so VACUUM never reclaims a version any live snapshot needs (`mvcc.md` §9/§10 Milestone F4a). **F4c — vacuum floor:** a FULL pass (`VACUUM` with no table) captures `B = next_txn_id` under the guard *before* the pass and calls `wal.set_vacuum_floor(B)` *after* it (the shared `full_vacuum_pass` helper, also used by the checkpoint auto-prune), so WAL truncation may later drop the now-reclaimed aborted transactions `< B`. Maintenance TOAST cleanup xids allocated during the pass are committed and are at or above `B`, so the floor advance remains safe. A single-table `VACUUM t` does **not** advance the floor (other tables' aborted tuples survive) — see `wal.md`/`mvcc.md` §9 F4c.
  - `Statement::AlterTableSetCompression { .. }` → `run_alter_table_compression` (below).
  - `Statement::AlterTableSetOptions { .. }` → `run_alter_table_toast_options` (below).
- **`ALTER TABLE <t> SET (compression = 'none' | 'zstd')`** (`docs/specs/compression.md` §8): `run_alter_table_compression` runs the whole statement under the **exclusive** checkpoint guard (drains writers, like `VACUUM`/`CREATE INDEX` backfill), in this load-bearing order. Step 4's `wal.flush()` is the **durable commit point** (mirrors `autocommit_bound_write_with_guard`): steps 1-4 propagate an error normally as a statement error (nothing has committed yet — table resolution's `UndefinedTable` and a dict-training failure both land here). Steps 5-8 are **post-durable-commit cleanup**: any error there is routed to `fatal_after_durable_commit` (logs, best-effort WAL flush, `process::exit`) instead of being returned as a statement error, because the DDL already committed and misreporting it as failed would be worse than crashing. Step 9 (the checkpoint-account trigger) runs after the guard releases and is best-effort — `record_commit_and_maybe_checkpoint_after_durable_commit` logs any error rather than returning or crashing, exactly like every other commit path.
  1. Acquire the exclusive guard; resolve the table by name (`SqlState::UndefinedTable` if it does not exist) and allocate a fresh `txn_id`.
  2. **Train.** If the target setting is `Zstd`, sample the table's current heap pages (`storage.sample_heap_pages`, capped at 4096 pages) and attempt `compress::train_dictionary`; a `None` (too small a corpus) leaves `active_dict_id = None` — not an error, the table proceeds dict-less.
  3. **Dict file, if trained.** Allocate a dictionary id (`catalog.allocate_dictionary_id`), persist the dictionary file (`dict_store.save`, durable **before** anything can reference it), and register it with the in-memory resolver (`compression.register_dictionary`) — all before any WAL record names the id (`compression.md` §7's durability order).
  4. **WAL + flush (durable commit point).** Append `CreateDictionary` (only if step 3 trained one), then `AlterTableCompression { table_id, compression, active_dict_id }`, then `Commit` — one combined `wal.flush()` after all three (immediate-commit DDL, like other DDL).
  5. **Catalog + registry.** Install the new setting into the catalog (`catalog.set_table_compression`, which also reserves `active_dict_id` if `Some`) and into the storage engine's file-compression registry (`storage.set_table_compression`) — heap file plus every live index file of the table.
  6. **Rewrite (an FPI per page).** `storage.rewrite_table_pages(&schema)` re-encodes every initialized page of the heap, primary-key index, and every secondary index (`docs/specs/crates/storage.md`, At-Rest Page Compression): for each page it logs a single unconditional `FullPageImage`/`FullPageImageCompressed` under `VACUUM_TXN` and stamps the FPI's assigned LSN as the page's new PageLSN — exactly the `vacuum_heap`/`reclaim_line_pointers` pattern. Logical bytes are unchanged; only the PageLSN (and checksum) advances.
  7. **WAL flush (write-ahead of the page flush).** `wal.flush()` makes every rewrite FPI from step 6 durable before any of those pages are written back. This is required because `buffer_pool.flush_dirty_pages()` does not gate on PageLSN at all — it assumes the caller already flushed the WAL — so skipping this step would not produce a loud error; it would let a torn page write precede its FPI being durable, i.e. silent corruption on recovery.
  8. **Flush, sync, mark clean.** `buffer_pool.flush_dirty_pages()` then `store.sync_all()` re-encodes every dirtied page under the new config and makes it durable; then `buffer_pool.mark_all_clean()` — `flush_dirty_pages` does not itself mark frames clean (the caller fsyncs via the store and only then calls `mark_all_clean`), so skipping this would not lose data but would leave the rewrite's pages dirty and get them redundantly re-written at the next checkpoint.
  9. **Release, then checkpoint-account.** The exclusive guard releases (it is scoped over steps 1-8) strictly **before** the next call, because `record_commit_and_maybe_checkpoint_after_durable_commit` acquires its own exclusive guard and would deadlock against one this same statement still held. That call runs immediately after release, so the rewrite's (potentially large) WAL activity counts toward `--checkpoint-wal-bytes` right away rather than waiting for an unrelated later commit to notice it. Command tag `ALTER TABLE`.

  Crash behavior mirrors other immediate-commit DDL: a crash before step 4's flush leaves the DDL uncommitted (CLOG-gated on replay, so it is skipped — see `docs/specs/crates/wal.md`); a persisted-but-unreferenced dictionary file from step 3 is orphaned but harmless. A crash during/after the rewrite leaves the catalog change durable and the files holding a self-describing mix of old- and new-encoding pages; a page torn mid-write during step 8's flush is **repaired by redo** replaying that page's FPI from step 6, exactly like any other page-write path — recovery does not depend on the `ALTER` being re-run. The rewrite as a whole is still **not** auto-resumed past whatever page range it reached — re-running the same `ALTER` completes an interrupted (cleanly mixed-encoding) rewrite (`compression.md` §8).
- **`ALTER TABLE <t> SET (toast = ..., toast_tuple_target = ..., toast_min_value_size = ..., toast_compression = ...)`**: `run_alter_table_toast_options` runs under the exclusive checkpoint guard but is **future-write-only**. It updates the table's durable TOAST policy and optional active value dictionary; it does not rewrite existing parent rows and does not rewrite existing hidden TOAST chunks. Existing rows remain readable because every inline-compressed or external value carries its own physical codec, dictionary id, raw length, and CRC metadata.
  1. Reject a mixed page-compression change (`compression = ...` together with any TOAST option) with `FeatureNotSupported`; page compression has the full-rewrite contract above and is not combined with the future-write-only TOAST ALTER in one statement.
  2. Acquire the exclusive guard, resolve the user table by name (`UndefinedTable` if absent), reject hidden-relation targets, and allocate/register a maintenance transaction id.
  3. Merge the `ToastOptionPatch` into the current `ToastOptions`. `toast = aggressive` with omitted `toast_min_value_size` applies the aggressive default; explicit `toast_compression` clears any old active dictionary before optional retraining.
  4. If `toast_compression = zstd_dict` is explicit, sample visible logical `TEXT`/`BYTEA` values through `storage.sample_toast_values` (bounded by sample count and bytes, detoasting existing values through the normal read materialization path). If `compress::train_dictionary` returns bytes, persist the dictionary file, register it, append `CreateDictionary`, and set `toast.active_dict_id = Some(dict_id)`. A tiny corpus simply leaves `active_dict_id = None`; future writes then fall back to plain zstd for TOAST value compression.
  5. If the table has toastable columns but no hidden TOAST relation (legacy catalog), reserve a new table id and create the hidden storage relation before the DDL commit. This logs the hidden relation's logical `CreateTable` plus empty primary-key B-tree page images before the `Commit`, so a crash after commit can recover the hidden relation. The catalog does not expose the hidden relation and the base schema does not reference it until after the commit flush. Failed pre-commit work aborts the maintenance xid and rolls back the storage metadata while leaving burned ids and orphan dictionary files harmless.
  6. Append `AlterTableToast { table_id, toast, toast_table_id }`, append `Commit`, and flush. This is the durable commit point.
  7. Post-durable cleanup installs any newly-created hidden relation in the catalog, installs the updated base-table TOAST metadata in the catalog and storage engine, runs storage/buffer commit cleanup for the maintenance xid, deregisters the xid, wakes waiters, releases the exclusive guard, then calls `record_commit_and_maybe_checkpoint_after_durable_commit`. Errors before the commit flush are statement errors after rollback; errors after the flush are routed to `fatal_after_durable_commit`.
  8. Recovery replays committed `CreateDictionary`, hidden-relation `CreateTable`, and `AlterTableToast` records in WAL order. Existing rows written before the ALTER decode under their old per-value metadata; rows written after the ALTER use the new policy.
- **COPY (`COPY <table> [(cols)] FROM STDIN | TO STDOUT`)**: classified `StatementClass::Copy(direction)`. `dispatch` binds it (resolve table + columns) and returns `ExecutionResult::BeginCopyIn`/`BeginCopyOut` — it does **not** execute — leaving the transaction slot unchanged. The connection loop then drives the COPY sub-protocol (see `docs/specs/copy.md` §5); COPY is rejected in the extended query protocol (`prepare_sql`). For **COPY FROM**, the loop sends `CopyInResponse`, spawns a blocking task (`run_copy_in_stream`) that owns the transaction — a fresh autocommit write transaction (mirrors `autocommit_write`: guard → register → snapshot → insert via `executor::CopyIn` → commit) or, inside a `BEGIN` block, the open transaction with no commit (mirrors `run_bound_in_transaction`) — and forwards `CopyData` into a bounded channel; `CopyDone` commits, while `CopyFail`/a row error/disconnect abort (status-based, like any other failure). For **COPY TO**, a blocking producer (`run_copy_out_stream`) scans under a read snapshot (autocommit) or the transaction's snapshot and streams `CopyData` frames out, then `CopyDone`. Command tag `COPY n`; a mid-stream error sends `ErrorResponse` (no `CopyDone`). `ReadyForQuery` is emitted only after the inbound stream is drained to its terminator, so the protocol stays in sync.
- **Disconnect**: an open transaction held on a dropped `Session` is aborted (status-based: `Abort` record + `CLOG=Aborted` + write-guard release + deregister, no page undo), so a client that disconnects mid-transaction leaks neither the guard nor a registry entry. A disconnect mid-`COPY FROM` drops the channel, so the blocking task sees no `Done` and aborts (no partial commit).

### Concurrency — Stage 2 (concurrent readers AND writers; Milestone E)

As of Milestone E2b the global writer lock is **inverted** into a shared-writer / exclusive-checkpoint guard (`common.md`, `mvcc.md` §10 E2b), so write-transactions now run concurrently.

- **Readers run lock-free.** A read-only statement/transaction takes **no** `ConcurrencyController` guard. It captures its snapshot under the active-transaction-registry latch and reads via the buffer pool's per-frame latches, so it runs concurrently with in-flight writers and skips their uncommitted versions by MVCC visibility. (Unchanged from Stage 1.)
- **Writers run concurrently.** A write transaction acquires the **SHARED** writer guard (`begin_writer`) **lazily** on its first write statement and holds the owned guard on the `Session` for the whole write-transaction, releasing it at COMMIT/ROLLBACK/disconnect. Many writers hold it at once; write-write safety comes from per-row conflict detection (E1: first-updater-wins `40001`) and the per-index / per-heap structural latches in `storage` (E2a), not from this lock. Autocommit DML = acquire the shared guard for the one statement, release at the implicit commit.
- **DDL runs alone.** DDL is non-transactional, rejected inside an explicit transaction block, and acquires the **EXCLUSIVE** checkpoint guard (`begin_checkpoint`) for the whole autocommit statement. This covers table, index, and sequence DDL. The guard prevents catalog rollback from restoring a stale whole-catalog snapshot over another committed DDL change, and gives `CREATE INDEX` the stable physical chain view its HOT broken-chain backfill check requires.
- **Checkpoint excludes writers.** `run_checkpoint` takes the **EXCLUSIVE** checkpoint guard (`begin_checkpoint`), which drains all in-flight writers and then runs alone — preserving the "no in-flight writer at checkpoint" invariant verbatim (so every transaction below the truncation boundary is settled and captured by `persist_clog`'s snapshot, keeping recovery correct without a fuzzy checkpoint). The `acquire-at-most-one-writer-guard-per-transaction` reentrancy tripwire is now a cheap correctness assertion (the shared guard is re-entrant), not a deadlock guard.

Deferred from Milestone E (`mvcc.md` §12): a fully-concurrent / B-link writer protocol (so E2a takes per-index latches instead), blocking + deadlock detection (instead of fail-fast `40001`), and a fuzzy checkpoint (checkpointing with writers in flight).

### Snapshot capture (per isolation)

Snapshot capture (`capture_snapshot(own_txn)`) builds the `Snapshot` consistently with the registry and the id allocator under one registry latch (`ActiveTxnRegistry::capture`): it reads the active set, then reads `next_txn_id` as `xmax`, so a concurrently-begun writer can never be both absent from `xip` and `< xmax`. `xip = active_ids` minus `own_txn` (own writes are seen via the predicate's `current_txn` path), and `xmin = oldest active id` or `xmax` if none are active. A read uses `own_txn = 0`. Id allocation and registration are done together under the latch (`register_allocated`) to close the same torn-snapshot window. In the **same** latched section, capture advertises the snapshot's `xmin` to the GC horizon and returns an RAII `AdvertisedSnapshot` guard alongside the `Arc<Snapshot>`; the caller holds the guard for exactly the snapshot's usable lifetime (`mvcc.md` §9). The snapshot is shared via `Arc<Snapshot>` (`StatementContext.snapshot`), so the executor clones a `StatementContext` per scan operator by bumping a refcount rather than deep-cloning the now-possibly-non-empty `xip` vector. Isolation is the capture-timing knob: **Read Committed** (default) captures a fresh snapshot per statement (its advertisement released at statement end); **Repeatable Read** captures one snapshot at the transaction's first statement and reuses it (its advertisement held on the `Transaction` and released at commit/abort). The autocommit read and write paths each advertise their snapshot across the statement's execution; the autocommit read in particular **must** advertise, since it is not its own transaction and so is otherwise invisible to the horizon.

`QueryService::execute_sql`/`execute_prepared` run with no cancellation and default session identity; the connection uses `execute_simple_streamed` for simple queries and `execute_prepared_in_session_streamed`/`execute_prepared_cancelable_streamed` for extended `Execute` (in-transaction vs. autocommit, respectively). These entry points take a `QuerySessionContext` bundling the connection's persistent `SessionSequenceState`, `SessionInfo` (startup user/database plus BackendKeyData process id), `SessionGucs`, and shared cancellation flag (an `Arc<AtomicBool>`) used as `ExecutionContext.cancel`. The flag is cleared before each query and set when a `CancelRequest` for that backend arrives, so the in-flight query aborts with `SqlState::QueryCanceled` (SQLSTATE `57014`).

`EXPLAIN` is a query-service exception to the uniform execution path. For `BoundStatement::Explain(inner)`, `QueryService` plans `inner` to a `PhysicalPlan`, calls planner `format_explain(&physical)`, and returns `ExecutionResult::Explanation { text }` without calling `QueryEngine::execute`.

Statement guard policy:

- No guard: SELECT and EXPLAIN that do not mutate sequences (lock-free readers), and a read-only explicit transaction.
- Shared writer guard (`begin_writer`, held for the whole write-transaction, many concurrent): INSERT, UPDATE, DELETE, `SELECT` statements whose bound expressions contain `nextval` or `setval`, and an explicit transaction once its first write runs. Acquired lazily.
- Exclusive checkpoint guard (`begin_checkpoint`, per statement): CREATE TABLE,
  DROP TABLE, CREATE INDEX, DROP INDEX, CREATE SEQUENCE, DROP SEQUENCE (DDL is
  non-transactional and rejected inside a block).
- Exclusive checkpoint guard (`begin_checkpoint`, drains all writers, runs alone): checkpoint, `VACUUM`, and `ALTER TABLE ... SET (compression = ...)` (both maintenance commands run with no concurrent writer; readers stay lock-free).

Bind and plan run under the same statement guard as execution (for writers) so catalog state cannot change between name resolution and execution.

Write statement protocol (autocommit; an explicit write transaction is the same but the guard spans all its statements and the commit/abort happens at COMMIT/ROLLBACK):

1. Acquire the shared writer guard (lazily, on the first write in an explicit transaction; concurrent with other writers).
2. Allocate `txn_id` and register it active (atomically under the registry latch).
3. Execute storage/catalog operations.
4. If execution fails, append `Abort` (`CLOG=Aborted`) before deregistering the txn, then run `storage.rollback_txn(txn_id)` (DDL-metadata restore only), `buffer_pool.rollback(txn_id)` (bookkeeping clear; no page undo), and catalog `restore` when needed. Abort is status-based — the failed statement's heap versions stay invisible via the CLOG, not undone. If the Abort append fails, the transaction remains active and normal query paths treat it as fatal rather than returning to service with a deregistered in-progress CLOG entry. If post-abort cleanup fails, normal query paths also exit fatally rather than returning with uncertain DDL metadata. In an explicit transaction the statement error instead poisons the block to `'E'` and the abort runs at ROLLBACK.
5. Append WAL `Commit`.
6. Flush WAL.
7. The statement/transaction is now durable and must not be rolled back or reported as a normal SQL failure.
8. Call `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)` to discard in-memory rollback metadata.
9. Release the shared writer guard.
10. Call best-effort `record_commit_and_maybe_checkpoint(&components)`. A failure here is logged after the commit is already durable and cleaned up; it is not returned as a normal SQL error and is not a rollback signal.
11. Return success.

DDL follows the same allocate/execute/commit-or-abort sequence as an autocommit write, but it acquires the exclusive checkpoint guard instead of the shared writer guard. Catalog and storage mutations are part of the same statement-level commit. `CREATE TABLE` with `SERIAL` creates the owned sequences before the table and stores ordinary `ColumnDefault::Nextval` defaults. `CREATE TABLE` with a `TEXT` or `BYTEA` column creates a hidden TOAST relation in the catalog and installs both the base and hidden schemas in storage under the same statement context; the hidden relation is not resolvable by user table name. `DROP TABLE` emits sibling `DropSequence` records for owned sequences and cascades catalog/storage metadata to the linked hidden TOAST relation. `CreateTable`, `DropTable`, `CreateSequence`, and `DropSequence` WAL replay update both catalog and storage; `CreateIndex` and `DropIndex` update catalog, storage metadata, and index pages through their normal recovery paths. Normal DDL execution must restore the previous catalog state if storage mutation, WAL append, or WAL flush fails before the commit record is durable; DML rollback does not restore a catalog snapshot.

If `storage.rollback_txn`, `buffer_pool.rollback`, or catalog `restore` fails before the commit record is durable, the server treats that as fatal. It logs the rollback failure, attempts to flush WAL, and exits instead of returning to service with possibly visible partial statement state.

`storage.commit_txn` and `buffer_pool.commit` are cleanup-only in-memory operations and must not perform I/O. For a valid `txn_id`, they should not fail. If either returns an error after WAL flush through the `Commit` record succeeded, the server must not call rollback and must not restore the catalog. Treat it as a fatal internal error: log it, flush WAL, and terminate the process because recovery will replay the durable commit.

Checkpoint may run after successful writes according to configured thresholds. It is called after the statement/transaction guard is released because `run_checkpoint` acquires the exclusive checkpoint guard, which must drain all writers (including this connection's, were it still held). If the triggered checkpoint fails, the write remains committed and the query/COPY/COMMIT path still returns success; surfacing the failure as a normal SQL error would invite clients to retry a transaction that already committed. The server logs the checkpoint failure and leaves the commit accounting in a state that lets a later write retry the checkpoint.

`ServerComponents.storage` is the concrete `Arc<PageBackedStorageEngine>`. Startup uses it for `install_schemas`, `install_index_schemas`, `install_sequences`, and `set_mode`. Query execution passes `components.storage.as_ref()` to `ExecutionContext.storage` as `&dyn StorageEngine`, to `ExecutionContext.schema_ops` as `&dyn SchemaOperations`, and to `StatementContext.sequence_manager` as `Arc<dyn SequenceManager>`. Recovery passes the same concrete value as `&dyn RecoveryOperations`.

## Query Results

A `SELECT` streams its rows through a bounded channel (`docs/specs/streaming.md`). The connection creates an `mpsc` channel and calls `execute_simple_streamed`, whose `spawn_blocking` producer owns the `PlanExecutor` and pushes a `StreamMessage::Start { columns }` then `StreamMessage::Rows` batches into it (via a `ChannelRowSink` implementing `executor::RowSink`); the async task drains the channel — emitting `RowDescription` from `Start` and `DataRow`s from each batch — concurrently while the producer runs, then finishes with `CommandComplete("SELECT n")` (n is the producer's authoritative row count, carried on `StreamOutcome::Streamed { count }`) and `ReadyForQuery`. The producer returns a `StreamOutcome`: `Streamed` for a SELECT, `Direct(ExecutionResult)` for ordinary non-streamed statements, or `SessionReset(ExecutionResult)` for `DISCARD ALL` so the connection clears prepared statements and portals through a typed signal. The producer holds the snapshot's GC-horizon advertisement and any transaction guard for the whole stream and returns the transaction slot only when it finishes, so MVCC and transaction semantics are unchanged. `blocking_send` provides backpressure; a dropped receiver (client gone) turns the next push into a graceful stop. The extended-protocol `Execute` streams identically through `execute_prepared_cancelable_streamed` / `execute_prepared_in_session_streamed`, differing only in that its `RowDescription` comes from `Describe` (so `Start` is consumed without emitting one) and its `ReadyForQuery` comes from `Sync` (see Connection Handling). Streaming alters neither protocol message encoding nor physical operator semantics; the materializing path (`execute_simple_with_session_sequences` and the `execute_sql` / `execute_prepared` convenience helpers, used by tests) shares the same executor drive.

A DML statement with a `RETURNING` clause yields `ExecutionResult::ModifiedReturning { command, count, columns, rows }`. The simple-query writer sends `RowDescription` (the `columns`), one `DataRow` per returned row, and then `CommandComplete` carrying the **DML** command tag (e.g. `INSERT 0 n` / `UPDATE n` / `DELETE n`, from `count`) — not `SELECT n`. Over the extended protocol the `RowDescription` comes from `Describe` (`result_columns` returns the `RETURNING` projection schema for an `Insert`/`Update`/`Delete` whose `returning` is `Some`), and `Execute` streams the `DataRow`s followed by the DML `CommandComplete`. `RETURNING` rows count toward the auto-prune dead-version accounting exactly like the equivalent plain `UPDATE`/`DELETE`.

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
    /// Shared compression state (`docs/specs/compression.md` §5a): the SAME
    /// instance is injected into both `store` (at-rest envelopes) and
    /// `storage` (WAL FPIs), so a file's config is consulted consistently by
    /// both.
    pub compression: Arc<compress::CompressionRegistry>,
    /// Durable home for trained per-table dictionaries (`compression.md` §7),
    /// seeded into `compression` at startup and appended to on
    /// `CreateDictionary` (a live `ALTER` training a dictionary, or replay).
    pub dict_store: Arc<compress::DictStore>,
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

`active_txns` is the active-transaction registry: an `ActiveTxnRegistry` over a
shared `Mutex` holding both a `BTreeSet<TxnId>` of currently in-progress
transaction ids (with an `O(log n)` minimum) and a refcounted
`BTreeMap<TxnId, usize>` multiset of the `xmin`s advertised by currently-live
snapshots (`xmin → count`, an `O(log n)` minimum). The lifecycle registers a
`txn_id` when it is allocated (`register_allocated`, which advances `next_txn_id`
and inserts the id under the same latch) and deregisters it on commit or rollback.
With concurrent readers and **concurrent** writers (Stage 2, E2b), several write
transactions may be registered at once, and a read's snapshot capture may observe
any of them; the set is no longer always empty between statements. Snapshot capture
(`capture_snapshot` via `ActiveTxnRegistry::capture`) reads `active_ids()` for `xip`
(excluding the statement's own txn) and the minimum for `xmin`, taking the registry
latch across the active-set read and the `next_txn_id` read so the snapshot is not
torn relative to a concurrent `BEGIN`; in that **same** latched section it publishes
the snapshot's `xmin` into the advertised-`xmin` multiset and returns an RAII
`AdvertisedSnapshot` guard (whose `Drop` releases the advertisement under the latch).

The **GC horizon** (`ServerComponents::gc_horizon`, Milestone F1) is the **minimum
advertised snapshot `xmin`** (`active_txns.oldest_xmin()`) under the registry's brief
latch, or — when no snapshot is advertised — `next_txn_id` (loaded `Acquire`). It is
**not** the oldest active transaction id (`oldest()`): a snapshot freezes its `xmin`
at capture for its whole life, so the active-id minimum can advance above a still-live
snapshot's `xmin`, and an autocommit `SELECT` is not its own transaction and never
registers at all — using `oldest()` could reclaim a version such a snapshot still sees
live (data loss). The advertised min is always `<= oldest()`, so it is strictly safer.
Below it no live snapshot can see a committed delete as undone, so a version with
`xmax < horizon` is reclaimable (`common::is_dead_to_all`, `mvcc.md` §9). Every
snapshot — including autocommit reads — advertises its `xmin` under the capture latch
for the snapshot's exact usable lifetime; the same-latch publish (read `active` and
`xmins[xmin]++` in one critical section, read by `oldest_xmin()` under the same latch)
makes the capture-vs-horizon path race-free (`mvcc.md` §9). The horizon is captured
once per VACUUM pass and only advances as snapshots are released; `QueryService::run_vacuum`
captures it **after** acquiring the exclusive guard so it cannot be advanced by a
concurrent writer and accounts for every reader advertised at that instant (Milestone
F4a). The CLOG that records settled transaction outcomes
lives in the WAL manager (`Clog`, rebuilt from `Commit`/`Abort` records; see
`docs/specs/crates/wal.md`), separate from this registry of still-running
transactions.

Checkpoint flushes dirty pages in place to the heap and advances the redo
boundary; its cost is O(pages changed), not O(database size). Driven by the
server under the **exclusive checkpoint guard** (E2b), which drains all in-flight
shared writers and runs alone:

1. Acquire the exclusive checkpoint guard (`begin_checkpoint`) — waits for all in-flight writers to drain, then holds off any new writer until the checkpoint returns.
1a. **Auto-prune (Milestone F4b, `mvcc.md` §9).** If `config.auto_vacuum_dead_rows` is non-zero and `dead_rows_since_vacuum >= config.auto_vacuum_dead_rows`, capture `horizon = gc_horizon()` **under the guard just acquired** and run a FULL VACUUM pass (`full_vacuum_pass`) over every user table — the same F4a orchestration the on-demand `VACUUM` uses, which ALSO advances the **vacuum floor** to `B = next_txn_id` captured under the guard (F4c, bounding `clog.dat` pruning) — then reset `dead_rows_since_vacuum` to `0`. This runs at the very start of the checkpoint body, **before** `flush_dirty_pages` (3), so the pages the vacuum dirties and the `FullPageImage` records it logs are flushed and made durable by **this** checkpoint and their WAL records precede the truncation in (7). Skipped (no vacuum) when the count is below the threshold. **No data loss — identical safety to on-demand `VACUUM`:** the horizon is captured under the exclusive guard, so no writer runs and the horizon is the minimum `xmin` advertised by any live snapshot (including lock-free readers); every reclaimed version has `xmax < horizon`, so no live snapshot can see it. Recovery is unaffected: the vacuum's FPIs sit below `checkpoint_lsn` and their pages are flushed before the control record, so a crash before the control record simply replays the previous redo boundary (the vacuum did not happen). **F4c durability ordering:** because this auto-prune runs before `flush_dirty_pages`/`store.sync_all` (3) while the `persist_clog` that consults the vacuum floor runs in (6b), the reclamation is fsynced into the heap before the snapshot drops any reclaimed transaction's explicit `Aborted` entry.
2. `wal.flush()` (a page's redo must be durable before the page is written).
3. `buffer_pool.flush_dirty_pages()` — write every flushable dirty page to the heap `PageStore`. With the relaxed flush gate (Milestone D1, `mvcc.md` §8) this spills committed, aborted, and — under Stage 2 — in-flight dirty pages alike; all are WAL-durable after (2), and the CLOG hides the non-committed tuples.
4. `store.sync_all()` — fsync the heap before advancing the redo boundary.
5. `checkpoint_lsn = wal.flushed_lsn()`.
6. `control.store(checkpoint_lsn, sorted_table_ids, catalog_bytes)` — the durable commit point. Before serializing `catalog_bytes`, checkpoint overlays storage's live sequence `(last_value, is_called)` values into the catalog snapshot so the control record contains the current sequence baseline.
6b. `wal.persist_clog(checkpoint_lsn)` — write the durable CLOG snapshot `clog.dat` (every transaction outcome plus both floors) **before** truncating, so it remembers every outcome the truncation is about to drop (`mvcc.md` §5.4).
7. Append the `Checkpoint { redo_lsn }` WAL marker stamped with the transaction-id high-water mark (`txn_id = next_txn_id - 1`, so the allocator boundary survives truncation; see `wal.md`), `wal.flush()`, `wal.truncate_before(checkpoint_lsn)`. Truncation is **unconditional**: it drops every record below `checkpoint_lsn`. It is safe because `persist_clog` (6b) durably recorded every aborted outcome, and under the exclusive guard no writer is in flight, so all transactions below `checkpoint_lsn` are settled and captured by the snapshot (`wal.md`, `mvcc.md` §5.4/§8). **F4c:** the **vacuum floor** (advanced by the full VACUUM in 1a) bounds `clog.dat` pruning — `persist_clog` drops the explicit `Aborted` entry of a reclaimed aborted transaction below the floor; WAL truncation does not consult it.
8. `buffer_pool.mark_all_clean()` (clears dirty flags, re-arms `needs_fpi`).
9. Release the exclusive checkpoint guard.

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
pub fn record_commit_and_maybe_checkpoint_after_durable_commit(components: &ServerComponents);
```

`run_checkpoint` resets `last_checkpoint_lsn` to the checkpoint LSN and `commits_since_checkpoint` to `0` after the control record and WAL checkpoint marker are durable. `record_commit_and_maybe_checkpoint` is called after each successful write statement, after the statement write guard has been dropped. It increments `commits_since_checkpoint` and triggers `run_checkpoint` when either `commits_since_checkpoint >= config.checkpoint_every_n_commits` or `wal.bytes_after(last_checkpoint_lsn)? >= config.checkpoint_wal_bytes`. If checkpoint fails, leave the counters unchanged except for the recorded commit so a later write can retry.

**Auto-prune threshold metric (F4b).** `ServerComponents.dead_rows_since_vacuum: AtomicU64` tracks committed dead versions since the last auto-prune. Each committed `DELETE` row and each committed `UPDATE` row creates one dead version, so the commit paths add the affected-row count from a `DELETE`/`UPDATE` result to this counter **only on a successful, durable commit** (`ServerComponents::add_dead_versions`): the autocommit-write path adds it before `record_commit_and_maybe_checkpoint`; an explicit transaction accumulates each statement's count on the `Transaction` and folds the total in on `COMMIT` (never on `ROLLBACK`/abort — a rolled-back delete/update produces no committed dead version). The counter is purely additive and never requires a scan to decide whether to prune; over-counting (e.g. a version a live snapshot still pins, so not yet reclaimable) only triggers an extra, harmless pass. The checkpoint reads and resets it in step (1a) above.

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
thread pool; a SELECT streams its `DataRow`s through the same bounded-channel
bridge as the simple-query path (`docs/specs/streaming.md`) in the requested
result formats, followed by `CommandComplete` (no `RowDescription`, that came from
`Describe`; no `ReadyForQuery`, that comes from `Sync`); every other statement is
returned whole as `StreamOutcome::Direct` or, for `DISCARD ALL`,
`StreamOutcome::SessionReset`, and written as before. `max_rows` is
treated as all rows. `Execute` participates in the session's CURRENT transaction:
when an explicit transaction is open on the session (`Session.txn` is `Some`), the
portal runs *inside* that transaction via `QueryService::execute_prepared_in_session_streamed`
(a thin wrapper over `…_with_context` that installs the channel sink),
which routes through the same in-transaction machinery the simple-query path uses —
the session's single write guard is reused (or lazily acquired once on the first
write), the transaction's snapshot/isolation applies, the `'E'` failed-state gate
rejects non-control statements with `25P02`, and a transaction-control portal
(BEGIN/COMMIT/ROLLBACK/SET TRANSACTION/SET SESSION CHARACTERISTICS) is dispatched
through `handle_transaction_control` so it affects `Session.txn` and
`Session.default_isolation` exactly like a simple-query control statement.
Session-configuration portals (`SET`/`SHOW`/`RESET`/`DISCARD ALL`) also route
through this path, even with no open transaction, so the connection's
`SessionGucs`, session sequence state, `default_transaction_isolation`, and
`transaction_isolation` rules match the simple-query path. With no open
transaction (`Session.txn` is `None`), a data `Execute` is
its own autocommit unit via `QueryService::execute_prepared_cancelable_streamed`. Routing both protocols through the one
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
- Recovery redoes every record after the control record's checkpoint LSN regardless of transaction outcome, and the CLOG (rebuilt from `Commit`/`Abort` records) decides visibility; a transaction in-flight at crash is recovered as aborted.
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
- `CREATE TABLE ... WITH (compression = 'zstd')` then insert then restart then select round-trips correctly.
- `CREATE TABLE ... WITH (toast...)` persists the resolved TOAST options and installs the hidden TOAST relation for `TEXT`/`BYTEA` tables without making it visible through user table-name lookup.
- `ALTER TABLE ... SET (compression = ...)` rewrites a table in both directions (`none → zstd`, `zstd → none`) with correctness preserved across a restart; a crash simulated mid-rewrite recovers with a self-describing mix of old/new-encoded pages still readable, and re-running the same `ALTER` completes the rewrite.
- `ALTER TABLE` (like `VACUUM`) is rejected inside an explicit transaction block with the generalized maintenance-in-block message, and is rejected via the extended query protocol's prepared-maintenance path the same way.
- Recovery resolves a dictionary created (via `ALTER`) after the last checkpoint, both from the replayed `CreateDictionary` WAL record and from the durable dictionary file seeded before redo.
- `VACUUM` still runs correctly on a compressed table.
