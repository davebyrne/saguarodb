# `server` Crate Specification

**Date:** 2026-07-12
**Status:** Living crate contract

The server turns the session `search_path` GUC into schema ids against the catalog
visible to the current transaction and supplies them at every simple, prepared,
cursor, COPY, and extended-Parse bind site. Missing path entries (including a
missing `$user` schema) are skipped; explicit missing qualifiers are errors. Schema
DDL and schema-qualified relation DDL use the transaction catalog overlay and
schema/name locks, so they remain private until durable commit and roll back with
transactions and savepoints.

`CREATE TABLE` foreign-key parents participate in prepared schema identities and
object-lock convergence. Existing parents are held with `AccessShare` through
the CREATE statement; self references need no pre-existing relation lock. The
transaction overlay publishes or rolls back the final FK-bearing schema together
with its table, indexes, TOAST relation, and owned sequences.

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
- `spill` — supplies the session `work_mem` budget and `<data-dir>/tmp` spill
  configuration captured by each opened query

No library crate depends on `server`.

## Modules

`app` (component bundle + `AppState`), `cancel` (`BackendKey { process_id,
secret_key }` and the process-wide `CancelRegistry`), `checkpoint`, `config`,
`connection`, `lock_manager`, `query`, `recovery`, `registry`,
`session_registry`, `shutdown`, `ssi_manager`, and `tls` (`build_acceptor`).

## Configuration

```rust
pub struct Config {
    pub data_dir: PathBuf,
    pub port: u16,
    pub buffer_pool_frames: usize,
    pub checkpoint_every_n_commits: u64,
    pub checkpoint_wal_bytes: u64,
    pub auto_vacuum_dead_rows: u64,
    pub auto_analyze_changed_rows: u64,
    pub shutdown_timeout_ms: u64,
    pub deadlock_timeout_ms: u64,
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
- `auto_analyze_changed_rows = 10000`
- `shutdown_timeout_ms = 30000`
- `deadlock_timeout_ms = 1000`
- `tls_cert_file = None`
- `tls_key_file = None`

Binary CLI flags:

- `--data-dir <PATH>` sets `Config.data_dir`; default `./data`.
- `--port <PORT>` sets `Config.port`; default `5433`.
- `--buffer-pool-frames <N>` sets `Config.buffer_pool_frames`; default `1024`.
- `--checkpoint-every-n-commits <N>` sets `Config.checkpoint_every_n_commits`; default `100`.
- `--checkpoint-wal-bytes <BYTES>` sets `Config.checkpoint_wal_bytes`; default `67108864`.
- `--auto-analyze-changed-rows <N>` sets `Config.auto_analyze_changed_rows`; default `10000`. When at least this many committed changed rows (`INSERT`/`UPDATE`/`DELETE`/`COPY FROM`, counted on durable commit only) have accumulated since the last auto-analyze, the next checkpoint re-collects statistics for every user table under its exclusive guard with the built-in default statistics target, as one committed maintenance transaction whose single generic catalog change precedes the checkpoint's WAL flush (so the manifest carries the fresh statistics and truncating the record is safe), then resets the accumulator (`docs/specs/statistics.md` §10). A manual full `ANALYZE` (no table) also resets it. `0` disables auto-analyze; like `--auto-vacuum-dead-rows`, `0` is accepted.
- `--auto-vacuum-dead-rows <N>` sets `Config.auto_vacuum_dead_rows`; default `10000`. When at least this many committed dead versions have accumulated since the last auto-prune, the next checkpoint folds a VACUUM pass over every user table into itself (Milestone F4b, `mvcc.md` §9). `0` disables auto-prune (space is then bounded only by explicit `VACUUM`); unlike the other numeric flags, `0` is accepted here.
- `--shutdown-timeout-ms <MS>` sets `Config.shutdown_timeout_ms`; default `30000`.
- `--deadlock-timeout-ms <MS>` sets `Config.deadlock_timeout_ms`; default `1000`.
  This is how long a transaction blocked on a row, table, or sequence lock waits
  before the shared deadlock detector checks the wait-for graph. The value must
  be positive and nonzero.
- `--tls-cert-file <PATH>` sets `Config.tls_cert_file`; PEM certificate chain. Optional; defaults to disabled.
- `--tls-key-file <PATH>` sets `Config.tls_key_file`; PEM private key. Optional; defaults to disabled.
- `--help` prints usage and exits with code `0`.

The binary parses flags with `std::env::args`; do not add a CLI parser dependency. `--port` accepts `1..=65535`; both automatic-maintenance thresholds accept `0` to disable their pass, and every other numeric flag must be positive and nonzero. Unknown flags, missing values, non-numeric numeric values, or out-of-range numeric values print usage to stderr and exit with code `2`. TLS is enabled only when both `--tls-cert-file` and `--tls-key-file` are supplied; supplying exactly one is an error that prints usage to stderr and exits with code `2`.

## Startup Sequence

1. Load configuration.
2. Construct the shared compression state (`docs/specs/compression.md` §5a/§7): one `compress::CompressionRegistry` instance and one `compress::DictStore` (over `<data>/dicts`, created if absent). Initialize the control store (`FileControlStore`) and the heap page store — `HeapPageStore::open_with_compression(<data>/heap, compression.clone())` — sharing that SAME registry instance so a file's at-rest config is consulted consistently by the heap store here and by storage's WAL-FPI path (step 8). Create `<data-dir>/tmp` and verify an anonymous temporary file can be created there; failure is a startup `IoError`. Spill files are ephemeral, never fsynced/WAL-logged, and require no recovery cleanup.
3. Initialize the WAL manager.
4. Initialize the buffer pool with the configured frame count, the `WalFlushPolicy`, and the heap page store as its `PageStore`. `WalFlushPolicy::can_flush` admits a dirty page iff it is **WAL-durable** (`page_lsn ≤ wal.flushed_lsn()`); the earlier committedness gate is dropped (Milestone D1, `mvcc.md` §8), so uncommitted/aborted dirty pages may be flushed/evicted (hidden by the CLOG). `WalFlushPolicy::ensure_durable` (called by the buffer pool's steal path before writing a stolen page) flushes the WAL, giving write-ahead logging for the now-possibly-uncommitted stolen page.
5. Enable eviction-flush-on-steal (`buffer_pool.enable_stealing()`), immediately after constructing the pool and before loading the control record. The durable on-disk index means recovery rebuilds nothing in memory, so redo may spill and the recovery working set is not bounded by the pool size.
6. Load the control record (`control.load()`): the redo boundary `checkpoint_lsn` and catalog bytes (none if no control record exists yet).
7. Initialize catalog from the control catalog bytes, or empty catalog if no control record exists.
8. Initialize storage engine in recovery mode with `PageBackedStorageEngine::open_with_compression(buffer_pool.clone(), wal.clone(), StorageMode::Recovery, compression.clone())`, sharing the same `CompressionRegistry` instance constructed in step 2.
9. Call `storage.install_schemas(catalog.list_tables()?)`, `storage.install_index_schemas(indexes)`, and `storage.install_sequences(catalog.list_sequences()?)`, where `indexes` is gathered via `catalog.list_indexes_for_table` for each table, so recovery replay and later DML maintain catalog indexes and runtime sequence state. Installing schemas also registers each table's/index's compression config into the shared registry (heap = the table's codec + trained dictionary, index files = the same codec but always dict-less — `docs/specs/compression.md` §4, §5a).
10. Seed the dictionary resolver from the durable dictionary files, **before redo runs**: for every `(dict_id, table_id, bytes)` returned by `dict_store.load_all()`, call `compression.register_dictionary(dict_id, &bytes)`, then advance the catalog's dictionary-id allocator past the highest loaded id (`catalog.reserve_dictionary_id`). This must precede step 11 so a dict-compressed page envelope or WAL FPI replayed there can already resolve its `dict_id`. An orphaned dictionary file (a crash between the file becoming durable and its `CreateDictionary` WAL record's commit) is loaded and registered the same way — harmless — and its id is burned regardless, so a later allocation never collides with it (`docs/specs/compression.md` §7).
10a. Validate referenced dictionaries (fail fast): for every table returned by `catalog.list_tables()` whose `active_dict_id` or `toast.active_dict_id` is `Some(id)`, check `compression.has_dictionary(id)`; if `id` is not registered by step 10's seeding, return a structured internal `DbError` naming the table, dict field, and dict id instead of proceeding. This first boot-time check runs after step 10's seeding and before step 11's replay, catching a deleted/missing `.dict` file from the checkpointed catalog loudly and immediately rather than as a later, confusing decode error on first read of a dict-compressed page or TOAST value (`docs/specs/compression.md` §7). It validates only each table's CURRENT active dict fields; a historical dict id referenced by an older `FullPageImageCompressed` WAL record is unchecked but always present too, since dict files are never deleted in v1.
11. Redo-all: replay every record with `LSN > checkpoint_lsn` (`WalManager::replay_from`). First pre-scan every generic `CatalogChange`, including aborted/in-flight transactions, to merge all carried allocator high-water marks and register every table/index physical generation's compression configuration. Physical page records then replay under PageLSN gating regardless of transaction outcome; the CLOG hides aborted/in-flight versions. Before dispatch, `apply_redo` normalizes compressed FPIs to raw `FullPageImage` records using the dictionary resolver seeded in step 10. Apply committed catalog changes in LSN order, atomically validating the complete resulting snapshot and reflecting table/index/sequence objects through `RecoveryOperations`; skip aborted/in-flight metadata but apply each skipped change's allocator reservation immediately so a later exact table before-image observes earlier burned column/FK IDs. Reapply the merged reservation after replay so sparse high-water for a relation created by a later committed change is retained. `CreateDictionary` remains a separate physical prerequisite and is commit-gated. Sequence value records replay unconditionally because sequence advancement is non-transactional. These reservations prevent orphan files or catalog IDs from being reused. Primary-key table-object replacement defers its derived identity-tree rebuild until replay and crashed-writer abort resolution finish. If replay applied records, repeat dictionary-reference validation so committed metadata cannot introduce an unresolved active dictionary.
12. Create `ServerComponents` with catalog, storage, buffer pool, WAL, control store, heap store, the shared `compression` registry and `dict_store` (steps 2/8/10), concurrency controller, shutdown state, checkpoint state initialized from the control `checkpoint_lsn`, `next_txn_id` initialized from the allocator scan over all retained WAL records (`replay_from(0)`, including committed subxids and the `Checkpoint` marker high-water), and an empty `active_txns` registry (the WAL manager reconstructed its CLOG on `open` — seeded from the durable `clog.dat` snapshot when present plus a fold of the post-snapshot `Commit`/`Abort` records, else rebuilt from those records).
13. If records were replayed, run `run_checkpoint(&components)` to persist the redone state to the heap and index and advance the redo boundary.
14. Run relation-generation cleanup once while still in startup: retired generations produced by truncate/drop replay and unreferenced orphan files from aborted prepares are removed only after buffer pin checks, and no user reader can exist yet.
15. Switch storage engine to normal mode with `storage.set_mode(StorageMode::Normal)`.
16. Construct query service from `components`.
17. Start Tokio runtime and bind listener.

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

The server constructs `ExecutionContext { statement, catalog, storage, schema_ops, cancel }` for each physical plan. The `StatementContext` carries the server-allocated transaction id, snapshot, conflict-wait/tuple-lock/SSI/cancel handles, the concrete storage engine as `Arc<dyn SequenceManager>`, and the connection-owned session sequence state for `currval`. The tuple-lock handle is the same `LockManager` used for catalog-object and direct xid waits, so future row-lock queues cannot hide mixed deadlocks. The `QueryEngine` receives that context and never allocates transaction IDs, appends commit records, flushes WAL, or calls storage/buffer commit or rollback.

Every statement initially binds under the shared catalog publication gate far
enough to discover logical object ids, releases that gate, acquires its object
locks, then reacquires the shared gate to fully rebind/revalidate object identity,
schema version, and storage-generation ids before capturing the storage
relation-generation snapshot used by execution. `capture_consistent_snapshots`
continues to pair a newly captured MVCC snapshot with relation `Arc`s under
`relation_publish_gate`; Repeatable Read/Serializable reuse their retained MVCC
snapshot but capture fresh relation `Arc`s after locking for every statement.
Relation snapshots are never retained across statements. The transaction-retained
table locks, rather than an eager all-table relation snapshot, pin generations for
relations actually referenced by the transaction. Relation-swap publication and
rollback take the gate's write side. A conflicting session therefore waits before
capturing relation state and sees either the committed replacement or restored
original, never an uncommitted replacement. After locking, a schema mismatch
retries an unprepared bind or returns the cached-plan-reprepare error. An explicit
transaction's binder overlays its pending transactional-TRUNCATE schemas on the
committed catalog.

For autocommit write and DDL statements, binding/preflight first discovers table
ids without mutating state. The server then allocates/registers the xid, acquires
the shared writer guard, acquires table locks, and revalidates before snapshot
capture or mutation. DDL then takes the exclusive catalog publication gate around
repeated preflight/mutation/undo and Commit/rollback; it never holds the gate while
waiting for an object lock. After taking the exclusive gate it verifies that its
grants still cover the current name-resolved request set; an uncovered target
causes the gate to be released and full ordered lock convergence to repeat before
mutation. Ordinary binders/system-catalog capture take the gate's shared side, so
provisional catalog state is not observable.

Statements that evaluate virtual system scans or catalog-introspection functions
receive an immutable catalog/provider snapshot captured under the shared
publication gate after lock-set convergence. Execution does not consult the live
catalog for those rows/functions. Statements that do not use this surface avoid a
catalog-sized clone. DDL expressions use the pre-mutation snapshot, avoiding
shared-gate re-entry while the statement holds the exclusive side.
Read statements bind without a writer guard, acquire statement-owned table locks,
then capture their snapshot. If the bound tree
contains `nextval` or `setval`, it is re-routed through the shared writer path so
sequence advancement is WAL logged and committed like other writes. This keeps
write-side catalog resolution inside the same exclusion window as planning and
execution.

### Transaction lifecycle (Milestone C)

The query path is a real transaction lifecycle; autocommit is an implicit single-statement transaction routed through the same machinery. A simple-query connection carries the transaction slot (`Option<Transaction>`, held on the `Session`) and its session-default isolation (`Session.default_isolation`, see below) into `QueryService::execute_simple_streamed`, which returns the (possibly mutated) slot **and** the (possibly updated) session default after the bounded row stream finishes. Direct internal/test callers can use `execute_simple` for the same lifecycle with a materialized result. The connection derives its `ReadyForQuery` byte from the returned slot (`I`/`T`/`E`) and persists the returned default on the `Session`.

- **BEGIN**: allocate a `txn_id` (and register it active) atomically under the registry latch, set the slot to an open `InTransaction` (`'T'`) at the requested isolation level (`BEGIN [TRANSACTION] ISOLATION LEVEL <level>` / `START TRANSACTION ISOLATION LEVEL <level>`; `Transaction.isolation`). A `BEGIN` with **no** explicit level inherits the session default (`begin_transaction(isolation.unwrap_or(session_default))`); an explicit level overrides it for that one transaction. Inheritance precedence: explicit `BEGIN` level > `SET TRANSACTION` > session default > Read Committed (`mvcc.md` §10 Milestone G2). `BEGIN` inside an open block is a no-op warning that stays `'T'` (Postgres-compatible) and ignores any requested level. DDL uses the transaction-local catalog/storage journals and publishes only after durable top-level commit.
- **SET TRANSACTION** (`SET TRANSACTION ISOLATION LEVEL <level>`): set the **current** transaction's `Transaction.isolation`, valid only **before its first query** (`Transaction.first_statement_ran`, set when a statement captures the transaction snapshot). After the first query it errors (`FeatureNotSupported`, "must be called before any query") and poisons the block to `'E'`. Inside an already-failed (`'E'`) block it is rejected with `25P02` like any non-COMMIT/ROLLBACK statement and stays `'E'`. With no open transaction it is a no-op success that stays `Idle`. The four SQL levels map onto three (`READ UNCOMMITTED`/`READ COMMITTED` → Read Committed; `REPEATABLE READ`/`SNAPSHOT` → Repeatable Read; `SERIALIZABLE` → Serializable / SSI, see `docs/specs/ssi.md`); `READ WRITE` is accepted-and-ignored and `READ ONLY` is rejected at parse time (`mvcc.md` §10 Milestone G1). Activating Repeatable Read is just this wiring: the per-transaction snapshot / advertisement / write-conflict machinery already exists (C–F).
- **SET SESSION CHARACTERISTICS** (`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`): set the **per-connection default** isolation (`Session.default_isolation`, default Read Committed) used by FUTURE transactions, threaded through query execution beside the transaction slot. It does **not** change an already-open transaction's `Transaction.isolation` (unlike `SET TRANSACTION`, it has no before-first-query rule and is allowed inside a block — Postgres-compatible). With no isolation-level mode (e.g. `READ WRITE` only) it is a no-op success. Inside an already-failed (`'E'`) block it is rejected with `25P02` like any non-COMMIT/ROLLBACK statement, leaving the default unchanged. Inside a healthy transaction block, the new default is visible to `SHOW default_transaction_isolation` immediately but persists only if that block commits; rollback or failed-block `COMMIT` discards it. The default persists across committed transactions on the connection and resets to Read Committed per new connection (the field is per-`Session`). Same level mapping and access-mode handling as `SET TRANSACTION`. Command tag `SET` (`mvcc.md` §10 Milestone G2).
- **Session configuration GUCs** (`SET`/`SHOW`/`RESET` and `DISCARD ALL`): the server owns a per-connection accept-all `SessionGucs` store for driver compatibility. `SET <name> = <value>` records arbitrary parameters and `SHOW <name>` returns stored or built-in values; SHOW of a never-seen unknown parameter returns `UndefinedObject` (`42704`). `SET LOCAL` for ordinary stored driver GUCs is accepted as session-scoped, a documented compatibility divergence; PostgreSQL-special isolation GUCs and `statement_timeout` preserve `LOCAL` semantics. `SHOW ALL` returns `name`, `setting`, and empty `description` columns sorted by name. `SessionGucs` also backs the default virtual-catalog state provider for `pg_settings` and `current_setting(text)`: rows expose the current setting, boot/reset values, and a `source` of `default` or `session`, with `default_transaction_isolation` derived from the currently visible session default and `transaction_isolation` derived from the open transaction's isolation or, outside a transaction block, that session default. `default_statistics_target` is a recognized integer GUC with default `100` and range `1..=1000`; it scales the ANALYZE sample (`300 ×` its value, `docs/specs/statistics.md` §6), values are validated on `SET` (`InvalidParameterValue`, `22023`, outside the range or non-integer), and it has no special `LOCAL` semantics. `statement_timeout` is a recognized integer-time GUC with default `0` (disabled) and range `0..=2147483647` milliseconds. It accepts PostgreSQL numeric syntax (including fractional/exponent, octal, and hexadecimal integer forms) plus the case-sensitive explicit units `us`, `ms`, `s`, `min`, `h`, and `d`; unit conversion and integer rounding follow PostgreSQL and invalid values return `InvalidParameterValue` (`22023`). The session store and `pg_settings.setting` retain canonical integer milliseconds, while `SHOW`, `SHOW ALL`, and `current_setting` render the largest exact time unit (for example `1000` as `1s` and `1500` as `1500ms`; disabled remains `0`). A regular `SET statement_timeout` inside a transaction is visible immediately and reaches the session only on commit; rollback discards it. `SET LOCAL statement_timeout` lasts only until transaction end and is a no-op outside a transaction. Savepoint rollback restores the captured timeout override, while release keeps the later value. `current_setting(NULL)` returns `NULL`; `current_setting` of an unknown parameter returns `UndefinedObject` (`42704`) like `SHOW`. `RESET <name>` restores the session-start value if one exists, or removes a custom parameter; `RESET ALL` restores the GUC store and resets the recognized transactional defaults using the same commit/rollback rules as setting them directly. `DISCARD ALL` is rejected inside any transaction block with `FeatureNotSupported` and poisons the block; outside a block it performs the same configuration reset, clears session sequence `currval` memory, deallocates all prepared statements, portals, and SQL cursors on the connection, and returns command tag `DISCARD ALL`. `application_name` changes are re-reported to the client with `ParameterStatus`; other startup-reported parameters are fixed. `transaction_isolation` is not an inert stored string: `SHOW transaction_isolation` reports the open transaction's isolation, otherwise the session default, and `SET transaction_isolation = <level>` is equivalent to `SET TRANSACTION ISOLATION LEVEL <level>` with the same no-open-transaction no-op and before-first-query rule. `default_transaction_isolation` controls only the session default for future transactions, equivalent to `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`: inside an open transaction, a regular `SET` is visible immediately and applied to the session only at commit, while `SET LOCAL` is visible only until transaction end and has no effect outside a transaction. `ROLLBACK TO SAVEPOINT` restores the recognized transactional GUC state captured when the savepoint was created; `RELEASE SAVEPOINT` keeps later changes merged into the parent transaction. Supported isolation text values are PostgreSQL spellings, case-insensitive: `read uncommitted` (strengthened to Read Committed), `read committed`, `repeatable read`, and `serializable`; `DEFAULT` resets to Read Committed for the default isolation GUC, to the currently visible default for `transaction_isolation`, and to `0` for `statement_timeout`. Session-configuration statements are accepted through both the simple and extended query protocols: `Parse` classifies them without binding, `Describe` reports the row shape for `SHOW` and `NoData` for `SET`/`RESET`/`DISCARD ALL`, and `Execute` routes through the session path so GUC state, transaction-state gating, and `DISCARD ALL` cleanup match simple-query execution.
  `work_mem` is a recognized memory-size GUC with default `4096` KiB and range `64..=2147483647` KiB. It accepts PostgreSQL numeric syntax plus the case-sensitive binary units `B`, `kB`, `MB`, `GB`, and `TB`; unitless values are KiB, fractional results round to the nearest KiB with ties-to-even, and invalid values return `InvalidParameterValue` (`22023`). The session store and `pg_settings.setting` retain canonical integer KiB, while `SHOW`, `SHOW ALL`, and `current_setting` render the largest exact unit (the default is `4MB`). Its regular `SET`, `SET LOCAL`, savepoint, commit, rollback, `RESET`, `RESET ALL`, and `DISCARD ALL` behavior matches `statement_timeout`; `DEFAULT` maps to `4096` KiB. Each opened query or portal captures the effective value in its `ExecutionContext`, so later session changes do not alter an execution already in progress.
- **Statements inside the block** share the transaction's `txn_id`; writes are stamped with it; reads use the transaction's snapshot (per isolation, below). A Repeatable Read transaction whose write targets a row another transaction changed and committed after its snapshot surfaces `40001` (`SerializationFailure`) when executor tuple-chain resolution observes advancement after any in-progress blocker has settled.
- **Deadlock victim inside a block:** before reporting `40P01`, immediately run the
  top-level Abort/storage/buffer/SSI cleanup and release all object/shared guards.
  Replace the live transaction with a failed shell so protocol state remains `E`
  until ROLLBACK (or failed-block COMMIT), but no locks depend on client cleanup.
- **COMMIT**: append `Commit` → `flush` (fsync) → `CLOG=Committed` (set inside `flush`) → `storage.commit_txn`/`buffer_pool.commit` cleanup → deregister → release object locks and the shared checkpoint-participant guard → best-effort checkpoint accounting. A read-only explicit transaction still emits no WAL Commit; if it accessed an object, it simply releases its retained shared guard and locks. The slot returns to `Idle` (`'I'`).
- **ROLLBACK** (or any statement error): append `Abort` → `CLOG=Aborted` → deregister → metadata/bookkeeping cleanup → release the write guard → `Idle`. The transaction is deregistered only after the Abort append succeeds; if that append fails, the rollback path is treated as a pre-durable cleanup failure (fatal on normal query paths, returned by direct internal/test calls) and the transaction remains active. Abort is **status-based** (Milestone D1, `mvcc.md` §4 Decision 3): there is no page undo. The transaction's modified tuples stay in the heap, hidden by the CLOG and reclaimed by VACUUM. The `storage.rollback_txn`/`buffer_pool.rollback` calls still run after deregistration; `storage.rollback_txn` restores engine-owned DDL metadata, may remove unpublished truncate replacement files that were never committed, and retires rollback-removed published generations until relation snapshots drain, while `buffer_pool.rollback` is a bookkeeping clear that reclaims no row pages. If that cleanup fails, normal query paths exit fatally rather than returning to service with uncertain metadata; direct internal/test calls surface the error. Abort is not fsync-gated (a transaction with no durable `Commit` is recovered as aborted regardless).
- **Failed (`'E'`) state**: any statement error inside an explicit block poisons it to `'E'` and does **not** end it. While `'E'`, every statement except `COMMIT`/`ROLLBACK` is rejected with `SqlState::InFailedSqlTransaction` (SQLSTATE `25P02`). `COMMIT` of an `'E'` block issues `ROLLBACK` (returns `Idle`). `COMMIT`/`ROLLBACK` with no open block are no-op warnings that stay `Idle`.
- **Autocommit**: a data/DDL statement with no open block runs as an implicit `BEGIN…COMMIT` around the one statement (allocate, snapshot, execute, commit-or-abort), preserving the prior external behavior exactly. A single autocommit statement has exactly one snapshot, so Read Committed vs Repeatable Read is functionally moot for it; the session default is **not** threaded into the autocommit single-statement snapshot path.

`VACUUM ANALYZE` and `VACUUM ANALYZE <table>` run the ordinary reclamation pass
and then the statistics-collection pass (`run_analyze_pass`) over the same
targets (`docs/specs/statistics.md` §7). These forms return the ordinary
`VACUUM` command tag and retain the same maintenance-command transaction
restrictions.

Table access and relation-changing operations additionally use transaction-owned
locks defined by `docs/specs/table-locks.md`. Plain reads use `AccessShare`, a
locking SELECT uses `RowShare` on its target, DML uses
`RowExclusive` on targets plus `AccessShare` on foreign-key parents/children
needed by enforcement, CREATE INDEX/VACUUM use the initial safe `Share` mode,
and TRUNCATE/DROP/ALTER use `AccessExclusive`. Explicit transactions retain locks
through top-level completion except that `ROLLBACK TO SAVEPOINT` restores the
captured grant set, releasing later acquisitions and downgrading later upgrades;
`RELEASE SAVEPOINT` retains them. Autocommit/read statements retain RAII locks
through stream completion. Relation waits share the row-lock deadlock graph.

Unlike other maintenance commands, TRUNCATE may execute in a healthy explicit
transaction, including beneath savepoints. Its target locks, catalog undo, and storage
generation before-images remain transaction-owned; COMMIT retains the replacements
and ROLLBACK restores the originals. See `docs/specs/table-locks.md` §8.

- **Maintenance (`VACUUM [ANALYZE] [table]`, `ANALYZE [table]`, `TRUNCATE [TABLE] <name> [, ...]`, `ALTER TABLE <t> SET (...)`, primary-key ALTER, and foreign-key ADD/generic constraint DROP)**: classified `StatementClass::Maintenance` (`statement_class`) and not bound/planned relationally. VACUUM, ANALYZE, and ALTER maintenance forms remain rejected inside explicit transaction blocks with `FeatureNotSupported`; transactional TRUNCATE is the documented exception and routes through the open transaction. Outside a block, `dispatch` and prepared execution carry the raw parsed statement through `QueryService::run_maintenance`:
  - `Statement::Vacuum { .. }` → `run_vacuum`, which resolves the targets, allocates/registers one maintenance xid, acquires the shared writer guard and xid-owned `Share` on every target, revalidates, and captures the GC horizon (plus the bounded full-pass floor). Full-target revalidation and floor capture occur in one shared catalog-publication critical section, preventing a newly published table with an older unvacuumed aborted xid from falling below the floor. It first identifies and deletes visible hidden-TOAST chunks using that same xid, then appends/flushes exactly one Commit even when no chunks were deleted. Only after the delete is durable does it prune each parent and hidden relation in the required order. Storage/buffer transaction cleanup and xid deregistration happen after commit, but the relation-lock owner token deliberately retains the same xid grants through the nontransactional prune phase and releases them only when the VACUUM statement ends. Thus table and row waits use one graph node and no target writer enters between cleanup and prune. Pre-commit failure aborts and skips parent pruning; physical-prune failure after the cleanup commit is safe to retry. The maintenance commit does not recursively trigger auto-prune. Command tag `VACUUM`. **F4c:** a FULL pass captures `B = min(next_txn_id, oldest_active_xid)` after locks, while its maintenance xid is active, and advances the floor only after pruning. A single-table VACUUM does not advance the floor.
  - `Statement::Analyze { table }` → `run_analyze_pass` (`crates/server/src/query/analyze.rs`, `docs/specs/statistics.md` §5): resolves one named live user table (a hidden/non-user relation reads as undefined) or every user table sorted by id, allocates/registers one maintenance xid, acquires the shared writer guard and xid-owned `AccessShare` on every target with the same acquire-and-revalidate loop as VACUUM (writers keep flowing — only `AccessExclusive` DDL conflicts), captures a registered reader snapshot (its advertised xmin holds back concurrent VACUUM's GC horizon), collects per-table statistics via the streaming sampler with the session's `default_statistics_target`, then — under the catalog publication gate — appends one generic catalog change containing all target statistics before the in-memory catalog update, appends exactly one `Commit`, and holds the gate through its WAL flush. This prevents concurrent ANALYZE from capturing an uncommitted statistics image as its durable before-state; a crash mid-pass applies none of the targets. Statistics updates do not bump `schema_version`, so cached prepared plans stay valid. Command tag `ANALYZE`. `Statement::Vacuum { analyze: true, .. }` chains this pass after `run_vacuum` with the `VACUUM` tag.
  - `Statement::Truncate { .. }` → `run_truncate`, which resolves the complete ordered user-table target list, allocates/registers one maintenance `txn_id`, acquires the shared writer guard, takes xid-owned `AccessExclusive` on every target plus `AccessShare` on incoming FK children, and revalidates before burning storage ids. An incoming child outside the target set is a `2BP01` dependency error; child-only, self, cyclic, and complete parent/child target sets are allowed. The catalog builds every post-truncate schema without publishing; the server appends one generic catalog change for the complete replacement set before storage initializes every empty replacement heap/primary-index/secondary-index file. Before appending/flushing one `Commit`, the server takes the catalog publication gate and `relation_publish_gate` write side and holds both until catalog/storage publish the complete batch, so neither catalog rollback nor snapshot capture can observe a partial publication. Pre-commit failure rolls back every prepared storage generation. After durable batch publication, storage/buffer commit cleanup runs once, the xid is deregistered, lock waiters are awakened, checkpoint accounting runs once, and one best-effort retired-generation cleanup pass follows. Post-commit publication or cleanup failure is fatal because recovery will replay the committed batch. Command tag `TRUNCATE TABLE`.
  - `Statement::AlterTableSetCompression { .. }` → `run_alter_table_compression` (below).
  - `Statement::AlterTableSetOptions { .. }` → `run_alter_table_toast_options` (below).
  - `Statement::AlterTableAddPrimaryKey { .. }` / `Statement::AlterTableDropPrimaryKey { .. }` → primary-key ALTER handlers (below).
  - `Statement::AlterTableAddForeignKey { .. }` / `Statement::AlterTableDropConstraint { .. }` → standalone FK maintenance; generic DROP routes a matching PK name to the primary-key handler.
- **Schema-evolution DDL (`ALTER TABLE ... ADD/DROP/RENAME COLUMN`, `ALTER TABLE ... ALTER [COLUMN] ... [SET DATA] TYPE`, `ALTER TABLE ... RENAME TO`)**: classified as `StatementClass::Ddl`, transactional inside explicit blocks, and executed with the shared writer guard, retained schema/name locks, and `AccessExclusive` on the target in autocommit/prepared execution. Bound/prepared ALTER carries the target table id plus schema version; execution rejects the cached plan if the table is dropped, recreated, or otherwise schema-changed before execution. Metadata-only renames increment `schema_version` and update catalog/storage metadata through a generic catalog change. ADD/DROP/TYPE statements first run catalog preflight; no-op forms and dependency errors return before xid/lock acquisition. `AccessExclusive` drains target relation users before a real rewrite, replacing the former global snapshot-exclusion fence. DROP COLUMN additionally takes `SequenceLockMode::Exclusive` when its Auto drop closure contains a SERIAL-owned sequence, preventing concurrent sequence access from racing physical removal. The executor captures the old relation generation after locking, applies the catalog schema change, allocates fresh storage ids through the catalog update helpers, appends the generic change before `SchemaOperations::update_table_schema` initializes empty replacement files, then streams visible old rows into the replacement relation. ALTER TYPE explicitly casts each value/default and rebuilds indexes; `USING` is unsupported. Command tag `ALTER TABLE`.
- **View DDL (`CREATE [OR REPLACE] VIEW`, `DROP VIEW`)**: classified as `StatementClass::Ddl`, transactional inside explicit blocks through the normal DDL-in-block path, and executed with the shared writer guard plus retained schema/name/object locks in autocommit or prepared execution. `CREATE VIEW` binds once and persists canonical SQL, output columns, and resolved `StoredQueryV1` through generic catalog WAL; dependency edges are extracted from the IR. `DROP VIEW [IF EXISTS]` resolves the view at execution time and includes its removal in the generic change. Later references lower stored IR directly without parsing SQL; cached prepared plans record schema versions for referenced catalog views/tables so replacing a view or changing an underlying table forces a reprepare.

Every DDL/maintenance guard list below is governed by one acquisition order:
shared writer, schema/name locks, all table locks, all sequence locks, catalog publication gate, then
storage latches. The gate is never held while waiting for an object lock.

- **`ALTER TABLE <t> SET (compression = 'none' | 'zstd')`** (`docs/specs/compression.md` §8): `run_alter_table_compression` runs under the shared writer guard, target `AccessExclusive` lock, then catalog publication gate, in this load-bearing order. Step 4's `wal.flush()` is the **durable commit point** (mirrors `autocommit_bound_write_with_guard`): steps 1-4 propagate an error normally as a statement error. Steps 5-8 are post-durable cleanup and therefore fatal on failure. Step 9 triggers best-effort checkpoint accounting only after all three guards release.
  1. Resolve/preflight the table, allocate/register a fresh `txn_id`, acquire the shared writer guard and target `AccessExclusive`, then revalidate under the catalog publication gate.
  2. **Train.** If the target setting is `Zstd`, sample the table's current heap pages (`storage.sample_heap_pages`, capped at 4096 pages) and attempt `compress::train_dictionary`; a `None` (too small a corpus) leaves `active_dict_id = None` — not an error, the table proceeds dict-less.
  3. **Dict file, if trained.** Allocate a dictionary id (`catalog.allocate_dictionary_id`), persist the dictionary file (`dict_store.save`, durable **before** anything can reference it), and register it with the in-memory resolver (`compression.register_dictionary`) — all before any WAL record names the id (`compression.md` §7's durability order).
  4. **WAL + flush (durable commit point).** Append `CreateDictionary` (only if step 3 trained one), then the generic catalog change carrying the table metadata, then `Commit` — one combined `wal.flush()` after all three (immediate-commit maintenance DDL). The maintenance xid is registered while these records are prepared. Any error or statement cancellation before the commit flush appends `Abort`, rolls back storage/buffer bookkeeping, deregisters the xid, and removes the prepared dictionary from both the resolver and durable dictionary store; its allocated id remains burned.
  5. **Catalog + registry.** Install the new setting into the catalog (`catalog.set_table_compression`, which also reserves `active_dict_id` if `Some`) and into the storage engine's file-compression registry (`storage.set_table_compression`) — heap file plus every live index file of the table.
  6. **Rewrite (an FPI per page).** `storage.rewrite_table_pages(&schema)` re-encodes every initialized target heap/index page, logs/stamps one FPI per page, and returns the deduplicated target file-id set with the touched count.
  7. **WAL flush (write-ahead of the page flush).** `wal.flush()` makes every rewrite FPI from step 6 durable before any of those pages are written back. This is required because `buffer_pool.flush_dirty_pages()` does not gate on PageLSN at all — it assumes the caller already flushed the WAL — so skipping this step would not produce a loud error; it would let a torn page write precede its FPI being durable, i.e. silent corruption on recovery.
  8. **Target-file flush, sync, mark clean.** Call
     `flush_dirty_pages_for_files(file_ids)`, `store.sync_files(file_ids)`, then
     `mark_files_clean(file_ids)`. Target `AccessExclusive` makes that set stable;
     unrelated writer frames are never flushed or cleared. Do not mark after error.
  9. **Release, then checkpoint-account.** The table lock, catalog publication gate, and shared writer guard release strictly before `record_commit_and_maybe_checkpoint_after_durable_commit`, which may acquire the exclusive checkpoint guard. The rewrite's WAL activity therefore counts toward `--checkpoint-wal-bytes` immediately. Command tag `ALTER TABLE`.

  Crash behavior mirrors other immediate-commit DDL: a crash before step 4's flush leaves the DDL uncommitted (CLOG-gated on replay, so it is skipped — see `docs/specs/crates/wal.md`); a persisted-but-unreferenced dictionary file from step 3 is orphaned but harmless. A returned pre-commit error is different from a crash: orderly rollback removes that file and its in-memory registration. A crash during/after the rewrite leaves the catalog change durable and the files holding a self-describing mix of old- and new-encoding pages; a page torn mid-write during step 8's flush is **repaired by redo** replaying that page's FPI from step 6, exactly like any other page-write path — recovery does not depend on the `ALTER` being re-run. The rewrite as a whole is still **not** auto-resumed past whatever page range it reached — re-running the same `ALTER` completes an interrupted (cleanly mixed-encoding) rewrite (`compression.md` §8).
- **`ALTER TABLE <t> SET (toast = ..., toast_tuple_target = ..., toast_min_value_size = ..., toast_compression = ...)`**: `run_alter_table_toast_options` runs with the shared writer guard, catalog publication gate, and target `AccessExclusive` lock but is **future-write-only**. It updates the table's durable TOAST policy and optional active value dictionary; it does not rewrite existing parent rows and does not rewrite existing hidden TOAST chunks. Existing rows remain readable because every inline-compressed or external value carries its own physical codec, dictionary id, raw length, and CRC metadata.
  1. Reject a mixed page-compression change (`compression = ...` together with any TOAST option) with `FeatureNotSupported`; page compression has the full-rewrite contract above and is not combined with the future-write-only TOAST ALTER in one statement.
  2. Resolve the user table (`UndefinedTable` if absent), reject hidden targets, allocate/register a maintenance xid, acquire the shared writer guard and target `AccessExclusive`, then revalidate under the catalog publication gate.
  3. Merge the `ToastOptionPatch` into the current `ToastOptions`. `toast = aggressive` with omitted `toast_min_value_size` applies the aggressive default; explicit `toast_compression` clears any old active dictionary before optional retraining.
  4. If `toast_compression = zstd_dict` is explicit, sample visible logical `TEXT`/`BYTEA` values through `storage.sample_toast_values` (bounded by sample count and bytes; compressed/external values larger than the remaining byte budget are skipped before detoasting). If `compress::train_dictionary` returns bytes, persist the dictionary file, register it, append `CreateDictionary`, and set `toast.active_dict_id = Some(dict_id)`. A tiny corpus simply leaves `active_dict_id = None`; future writes then fall back to plain zstd for TOAST value compression.
  5. Append the generic catalog change, append `Commit`, and flush. This is the durable commit point. Failed or canceled pre-commit work aborts the maintenance xid, rolls back storage metadata, and removes a prepared dictionary from the resolver and durable dictionary store; allocated ids remain burned. A process crash may instead leave an unreferenced dictionary file, which startup treats as a harmless orphan and reserves its id.
  6. Post-durable cleanup installs the updated base-table TOAST metadata in the catalog and storage engine, runs storage/buffer commit cleanup for the maintenance xid, deregisters the xid, wakes waiters, releases its table lock/shared writer guard/catalog publication gate, then calls `record_commit_and_maybe_checkpoint_after_durable_commit`. Errors before the commit flush are statement errors after rollback; errors after the flush are routed to `fatal_after_durable_commit`.
  7. Recovery replays committed `CreateDictionary` and the generic catalog change in WAL order. Existing rows written before the ALTER decode under their old per-value metadata; rows written after the ALTER use the new policy.
- **`ALTER TABLE <t> ADD [CONSTRAINT name] PRIMARY KEY (cols...)` / `DROP PRIMARY KEY` / `DROP CONSTRAINT <pkey>`**: primary-key ALTER runs with the shared writer guard, catalog publication gate, and target `AccessExclusive` lock as immediate-commit maintenance DDL. `ADD` requires the table to be a user table with no current primary key, resolves and de-duplicates the named columns, validates existing rows for NULLs, duplicate live keys, and live HOT-chain key divergence, reserves a catalog index id, appends one generic catalog change containing both table and backing index, and builds the storage index before commit without publishing catalog metadata to readers. After the commit is durable, SSI tuple reads and tuple writes retained for the table are conservatively promoted to relation-granularity tracking, storage rebuilds the table identity B-tree from heap rows with full-page-image redo and flushes that WAL, and only then the catalog publishes the staged snapshot. `DROP` carries both the table replacement and backing-index removal in one generic change before storage cleanup. In both directions, failures before the commit flush abort the maintenance xid and restore the catalog snapshot; failures after the flush are fatal post-commit cleanup errors. Recovery detects the primary-key projection change from the table before/after values, then rebuilds the identity tree after replay and crashed-writer abort resolution without appending recovery WAL. `DROP PRIMARY KEY` does not restore prior column nullability. Command tag `ALTER TABLE`.
- **`ALTER TABLE <child> ADD [CONSTRAINT name] FOREIGN KEY ...` / `DROP CONSTRAINT [IF EXISTS] name [RESTRICT]`**: FK ALTER allocates a maintenance xid and shared writer guard, then uses that xid's single deadlock-graph owner to converge schema/name/relation locks before taking the publication gate. ADD holds child `AccessExclusive` and parent `Share`; it re-resolves ordered columns and the declared PK/UNIQUE target, installs one first-class constraint under the gate, scans every existing child row through the executor's shared outgoing validator, and persists the constraint plus rebuilt dependency graph through generic catalog WAL. DROP resolves the table-local name at execution, routes a matching PK name above, otherwise holds child `AccessExclusive` and parent `AccessShare` and removes only that constraint without rewinding the global ID allocator. `IF EXISTS` suppresses only a missing named constraint; `CASCADE` is unsupported. Prepared ADD/DROP identities include both child and known parent and are revalidated after those relation locks converge under the publication gate. Any pre-flush validation, cancellation, storage, or WAL failure aborts and restores catalog/storage state; committed recovery applies the complete change. Command tag `ALTER TABLE`.
- **COPY (`COPY <table> [(cols)] FROM STDIN | TO STDOUT`)**: classified `StatementClass::Copy(direction)`. `dispatch` binds the statement and resolves its target. COPY FROM then allocates/registers its autocommit xid (or uses the open transaction xid), acquires/carries the shared writer guard, takes xid-owned `RowExclusive`, and revalidates; COPY TO takes statement- or transaction-owned `AccessShare`. Only then does either direction capture the MVCC/relation snapshot and an immutable catalog/introspection snapshot before returning `BeginCopyIn`/`BeginCopyOut`; all three cross the protocol boundary and remain fixed through stream completion. Autocommit COPY carries its table-lock guard through the streaming worker; an explicit transaction retains its lock through top-level completion unless `ROLLBACK TO` restores a savepoint predating its acquisition. The bound `CopyJob` carries the resolved `TableSchema`, so the worker does not re-read live catalog column/type/default metadata. COPY remains simple-query only. COPY FROM streams into the transaction and commits on `CopyDone`; `CopyFail`, row error, or disconnect aborts. COPY TO scans under its captured statement relation snapshot and autocommit or retained transaction MVCC snapshot. Command tag `COPY n`; a mid-stream error sends `ErrorResponse` without `CopyDone`, and `ReadyForQuery` waits until inbound COPY is drained to its terminator.
- **Disconnect**: an open transaction held on a dropped `Session` is aborted (status-based: `Abort` record + `CLOG=Aborted` + write-guard release + deregister, no page undo), so a client that disconnects mid-transaction leaks neither the guard nor a registry entry. A disconnect mid-`COPY FROM` drops the channel, so the blocking task sees no `Done` and aborts (no partial commit).

### Concurrency — Stage 2 (concurrent readers AND writers; Milestone E)

As of Milestone E2b the global writer lock is **inverted** into a shared-writer / exclusive-checkpoint guard (`common.md`, `mvcc.md` §10 E2b), so write-transactions now run concurrently.

- **Readers use table locks.** An autocommit plain read takes no controller guard. Before an explicit transaction's first object lock, even for a read, it acquires and retains the shared checkpoint-participant guard. Plain reads take `AccessShare`; a locking SELECT is promoted through the transaction-owned path, takes `RowShare`, and retains tuple locks until statement commit or explicit top-level completion.
- **Writers run concurrently.** Autocommit writes acquire the shared writer guard before object locks. An explicit transaction already holds that same shared side before any retained object lock and reuses it if a later statement writes. It releases the guard at top-level completion. Write-write safety comes from table/row conflicts and storage structural latches.
- **DDL is catalog-serialized and relation-scoped.** DDL is transactional and
  uses a transaction-local catalog overlay and the shared writer guard,
  the documented object locks, and only then a catalog publication gate so whole-catalog
  rollback cannot overlap another DDL. CREATE INDEX's `Share` lock provides its stable
  target-table chain view; unrelated table readers and writers remain concurrent.
- **Checkpoint excludes participants.** `run_checkpoint` takes the **EXCLUSIVE** checkpoint guard (`begin_checkpoint`), which drains all in-flight writers plus explicit transactions that retained the shared participant before an object lock, then runs alone — preserving the "no in-flight writer at checkpoint" invariant verbatim (so every transaction below the truncation boundary is settled and captured by `persist_clog`'s snapshot, keeping recovery correct without a fuzzy checkpoint). The `acquire-at-most-one-writer-guard-per-transaction` reentrancy tripwire is now a cheap correctness assertion (the shared guard is re-entrant), not a deadlock guard.

Deferred from Milestone E (`mvcc.md` §12): a fully-concurrent / B-link writer protocol and a fuzzy checkpoint (checkpointing with writers in flight).

### Snapshot capture (per isolation)

Snapshot capture (`capture_snapshot(own_txn)`) builds the `Snapshot` consistently with the registry and the id allocator under one registry latch (`ActiveTxnRegistry::capture`): it reads the active set, then reads `next_txn_id` as `xmax`, so a concurrently-begun writer can never be both absent from `xip` and `< xmax`. `xip = active_ids` minus `own_txn` (own writes are seen via the predicate's `current_txn` path), and `xmin = oldest active id` or `xmax` if none are active. A read uses `own_txn = 0`. Id allocation and registration are done together under the latch (`register_allocated`) to close the same torn-snapshot window. In the **same** latched section, capture advertises the snapshot's `xmin` to the GC horizon and returns an RAII `AdvertisedSnapshot` guard alongside the `Arc<Snapshot>`; the caller holds the guard for exactly the snapshot's usable lifetime (`mvcc.md` §9). The snapshot is shared via `Arc<Snapshot>` (`StatementContext.snapshot`), so the executor clones a `StatementContext` per scan operator by bumping a refcount rather than deep-cloning the now-possibly-non-empty `xip` vector. Isolation is the capture-timing knob: **Read Committed** (default) captures a fresh snapshot per statement (its advertisement released at statement end); **Repeatable Read** captures one snapshot at the transaction's first statement and reuses it (its advertisement held on the `Transaction` and released at commit/abort). The autocommit read and write paths each advertise their snapshot across the statement's execution; the autocommit read in particular **must** advertise, since it is not its own transaction and so is otherwise invisible to the horizon.

`QueryService::execute_sql`/`execute_prepared` run with no cancellation and default session identity; the connection uses `execute_simple_streamed` for simple queries and `execute_prepared_in_session_streamed`/`execute_prepared_cancelable_streamed` for extended `Execute` (in-transaction vs. autocommit, respectively). These entry points take a `QuerySessionContext` bundling the connection's persistent `SessionSequenceState`, `SessionInfo` (startup user/database plus BackendKeyData process id), `SessionGucs`, an optional `SystemStateProvider` override for virtual system-catalog session data, an optional catalog-introspection provider override, an optional `SessionRegistry` handle, and shared cancellation token (an `Arc<QueryCancel>`) used as `ExecutionContext.cancel`. Without a system-state override, each statement runtime builds a provider from the current `SessionGucs` plus the effective `default_transaction_isolation`, `transaction_isolation`, and the live registry rows. Query entry points, including the COPY streaming drivers, also install a catalog-backed introspection provider from `ServerComponents.catalog` and the session identity unless the session supplied an explicit override; it resolves `to_regclass`, `pg_table_is_visible`, `pg_get_userbyid`, `pg_get_serial_sequence`, `pg_get_indexdef`, and `pg_get_constraintdef` against real catalog/session state while other unresolved definition renderers return `NULL` until implemented. For `pg_get_indexdef(oid, column, pretty)`, column `0` renders the full definition, positive values render the indexed key column, and out-of-range nonzero values return `NULL`. `pg_table_is_visible` follows SaguaroDB's effective search path (`public` plus implicit `pg_catalog`): normal user tables, their indexes, public sequences, and unshadowed `pg_catalog` system views are visible; a public relation with the same name shadows a `pg_catalog` view, and hidden TOAST relations and `information_schema` views are not visible through this predicate. Non-connection helper entry points omit the registry, so `pg_stat_activity` is empty there. The token is reset before each query; a `CancelRequest` records `CancelReason::UserRequest`, so the in-flight query aborts with `SqlState::QueryCanceled` (SQLSTATE `57014`) and the matching message. The first cancellation reason wins until reset. Schema-qualified SQL names that reference an unknown schema map to `SqlState::InvalidSchemaName` (SQLSTATE `3F000`).

`pg_get_constraintdef` renders primary-key, UNIQUE, CHECK, and foreign-key definitions.
Foreign-key rendering resolves durable child/parent column IDs against the
current catalog, is therefore rename-aware, omits implicit `NO ACTION`, and
includes explicit `ON UPDATE RESTRICT`/`ON DELETE RESTRICT`. Unknown or
non-constraint OIDs return `NULL`.

`SessionRegistry` is process-local state on `ServerComponents` backing
`pg_stat_activity`. A session registers after startup once `BackendKeyData`,
`SessionInfo`, and `SessionGucs` exist, and deregisters the exact registered
record handle on disconnect. Each record has its own small mutex for dynamic
activity state (`state`, `query`, `query_start`, `xact_start`, and
`state_change`), while the registry membership mutex is held only to copy record
handles. Stored query text is truncated to 1024 bytes on a UTF-8 character
boundary before retention, matching PostgreSQL's bounded activity-query behavior
without exposing a configurable `track_activity_query_size` GUC. Simple query and
extended `Execute` mark the record `active` before entering `spawn_blocking`,
keep it active while streaming rows or COPY data, and mark it `idle`, `idle in
transaction`, or `idle in transaction (aborted)` after the session's transaction
slot has been restored. Parse/Bind/Describe are not activity-tracked;
`client_addr`, `client_port`, and wait-event columns are reported as `NULL`.

`EXPLAIN` converges in `run_plan` after normal snapshot, relation-snapshot, object-lock, SSI, cancellation, and timeout setup. For `BoundStatement::Explain { analyze, statement }`, `QueryService` plans the inner SELECT. Plain mode calls the fallible planner `format_explain`; analyzed mode drains it exactly once through `QueryEngine::analyze_query` and calls the fallible `format_explain_analyze`. Plan/layout invariant failures propagate as structured errors. Successful formatting returns `StreamOutcome::Direct(ExecutionResult::Explanation)` and ignores a connection-provided SELECT row sink, so no inner rows escape and extended `max_rows` cannot suspend the result. Errors use the same panic firewall and explicit-transaction poisoning as SELECT.

When the inner query has a row-locking clause, plain EXPLAIN remains non-executing
and takes only its ordinary read locks. EXPLAIN ANALYZE is promoted to the locking
transaction lifecycle, upgrades the target to `RowShare`, and retains tuple locks
exactly as executing the locking SELECT would.

Statement guard policy:

- No `ConcurrencyController` guard: autocommit plain SELECT, plain EXPLAIN, and analyzed
  EXPLAIN that does not mutate sequences. Explicit transactions take the shared side before their first
  object lock. Both take `AccessShare` and may wait for relation-changing work.
- Shared writer/participant guard (`begin_writer`, many concurrent): all writes,
  locking SELECT, DDL, and WAL-writing maintenance, including analyzed EXPLAIN
  whose SELECT calls `nextval` or `setval`; plus an explicit transaction before
  its first object lock even when that statement is read-only. DDL takes the
  catalog publication gate only after object locks. Plain EXPLAIN never advances
  sequences.
- Exclusive checkpoint guard (`begin_checkpoint`, drains all writers, runs alone): actual checkpoint and its internal auto-prune only. Existing readers take no controller guard; relation locks separately exclude conflicting table access.

Simple-query bind discovers relation ids before snapshot capture; execution then
acquires table locks, revalidates the bound schema, and captures the statement's
relation snapshot. Writer guards still span execution as documented. Extended
`Parse` records the schema version of every referenced user table and user view;
`Execute` acquires locks and rejects the cached plan with `FeatureNotSupported`
("cached plan must be reprepared after schema change") if any version changed or
relation disappeared. Prepared schema-rewrite ADD/DROP COLUMN performs catalog
preflight before xid/guard/lock acquisition; execute-time revalidation under the
catalog publication gate and target `AccessExclusive` lock then decides whether the
statement is still a no-op or must rewrite. Prepared `DROP TABLE IF EXISTS`/`DROP VIEW IF EXISTS`
therefore still carries the normalized relation name and resolves it during
initial preflight, then revalidates under its object locks and catalog publication gate,
including `WrongObjectType` checks when the
shared table/view namespace contains the wrong relation kind. Prepared `CREATE
TABLE IF NOT EXISTS` performs the duplicate-table no-op check under that same
guard but still returns `DuplicateTable` when the name belongs to a view.

Write statement protocol (autocommit; an explicit write transaction is the same but the guard spans all its statements and the commit/abort happens at COMMIT/ROLLBACK):

1. Bind/preflight to discover referenced table ids without mutation.
2. Allocate `txn_id` and register it active (atomically under the registry latch); an explicit transaction already has its top-level xid.
3. Acquire the shared writer guard (an explicit transaction may already retain it from an earlier object access), then acquire xid-owned schema/name/table/sequence locks in stable order and revalidate.
4. Capture the statement snapshot and execute storage/catalog operations.
5. If execution fails, append `Abort` (`CLOG=Aborted`) before deregistering the txn, then run `storage.rollback_txn(txn_id)` (DDL-metadata restore, deletion of unpublished truncate replacement files, and retired-generation protection for rollback-removed published generations), `buffer_pool.rollback(txn_id)` (bookkeeping clear; no page undo), and catalog `restore` when needed. Abort is status-based — the failed statement's heap versions stay invisible via the CLOG, not undone. If the Abort append fails, the transaction remains active and normal query paths treat it as fatal rather than returning to service with a deregistered in-progress CLOG entry. If post-abort cleanup fails, normal query paths also exit fatally rather than returning with uncertain DDL metadata. In an explicit transaction the statement error instead poisons the block to `'E'` and the abort runs at ROLLBACK.
6. Append WAL `Commit`.
7. Flush WAL.
8. The statement/transaction is now durable and must not be rolled back or reported as a normal SQL failure.
9. Call `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)` to discard in-memory rollback metadata.
10. Release table locks, deregister the xid/wake waiters, and release the shared writer guard.
11. Call best-effort `record_commit_and_maybe_checkpoint(&components)`. A failure here is logged after the commit is already durable and cleaned up; it is not returned as a normal SQL error and is not a rollback signal.
12. Return success.

DDL follows the same allocate/shared-writer/execute/commit-or-abort sequence as an autocommit write and holds the catalog publication gate across catalog preflight, mutation, and possible restore. Catalog and storage mutations are part of the same statement-level commit. `CREATE TABLE IF NOT EXISTS` still validates the requested table definition shape, then returns the normal command tag without catalog/storage/WAL mutation when the table already exists. Prepared `CREATE TABLE` records every stable sequence ID embedded in its typed defaults and CHECKs, takes the corresponding sequence access locks at execution, and requests reprepare if any referenced ID disappeared. `CREATE TABLE ... WITH (fillfactor = 10..100)` accepts and validates the PostgreSQL compatibility option but intentionally does not persist it or change page occupancy. Multi-target `DROP TABLE [IF EXISTS]` resolves and validates the complete ordered target list, takes `AccessExclusive` on every present target plus `AccessShare` on discovered incoming FK children, revalidates, and applies every drop under one statement transaction; an incoming child outside the set is rejected with `2BP01`, while self, cyclic, and wholly internal dependencies are allowed. Absent targets are skipped only with `IF EXISTS`. Every statement computes one deterministic object-sorted `CatalogChange` from its validated before/after snapshots and appends it before dependent physical mutations. CREATE TABLE can therefore carry its owned sequences, base/TOAST relations, constraints, and declared indexes atomically; DROP TABLE carries the corresponding graph-derived removal. `CREATE INDEX` takes `Share` on its table and publishes storage metadata only after the secondary tree is initialized and backfilled. Schema-evolution ALTER TABLE takes target `AccessExclusive`; ADD/DROP column rewrites switch to fresh storage ids and reinsert transformed visible rows, while renames update metadata only. DROP COLUMN and ALTER COLUMN TYPE query the dependency graph by stable column identity: dependencies on the exact target block with `2BP01`, dependencies on other columns do not, and DROP removes the target default plus an Auto-owned sequence. Identical type requests and ID-backed table/column renames retain their existing behavior. Recovery pre-reserves allocator high-water from all changes, then atomically applies only committed changes in LSN order and reflects changed table/index/sequence objects into storage. Normal DDL execution must restore the previous catalog state if storage mutation, WAL append, or WAL flush fails before the commit record is durable; DML rollback does not restore a catalog snapshot.

Before appending that change or starting dependent physical DDL, execution
atomically claims its global and per-relation allocator high-water in the shared
catalog. Claims are never rewound, including when backfill or rewrite later
fails. Recovery passes each committed complete change set through the single
storage reconciliation boundary; the server does not redispatch its individual
table/index/sequence mutation forms.

If `storage.rollback_txn`, `buffer_pool.rollback`, or catalog `restore` fails before the commit record is durable, the server treats that as fatal. It logs the rollback failure, attempts to flush WAL, and exits instead of returning to service with possibly visible partial statement state.

`storage.commit_txn` and `buffer_pool.commit` are cleanup-only in-memory operations and must not perform I/O. For a valid `txn_id`, they should not fail. If either returns an error after WAL flush through the `Commit` record succeeded, the server must not call rollback and must not restore the catalog. Treat it as a fatal internal error: log it, flush WAL, and terminate the process because recovery will replay the durable commit.

Checkpoint may run after successful writes according to configured thresholds. It is called after the statement/transaction guard is released because `run_checkpoint` acquires the exclusive checkpoint guard, which must drain all writers (including this connection's, were it still held). If the triggered checkpoint fails, the write remains committed and the query/COPY/COMMIT path still returns success; surfacing the failure as a normal SQL error would invite clients to retry a transaction that already committed. The server logs the checkpoint failure and leaves the commit accounting in a state that lets a later write retry the checkpoint.

`ServerComponents.storage` is the concrete `Arc<PageBackedStorageEngine>`. Startup uses it for `install_schemas`, `install_index_schemas`, `install_sequences`, and `set_mode`. Query execution passes `components.storage.as_ref()` to `ExecutionContext.storage` as `&dyn StorageEngine`, to `ExecutionContext.schema_ops` as `&dyn SchemaOperations`, and to `StatementContext.sequence_manager` as `Arc<dyn SequenceManager>`. Recovery passes the same concrete value as `&dyn RecoveryOperations`.

## Query Results

Here `SELECT` means a plain non-locking SELECT. A locking SELECT is materialized
on the transaction-owned path so its implicit transaction retains tuple locks
until the result is complete.

A `SELECT` streams its rows through a bounded channel (`docs/specs/streaming.md`). The connection creates an `mpsc` channel and calls `execute_simple_streamed`, whose `spawn_blocking` producer owns the `PlanExecutor` and pushes a `StreamMessage::Start { columns }` then `StreamMessage::Rows` batches into it (via a `ChannelRowSink` implementing `executor::RowSink`); the async task drains the channel — emitting `RowDescription` from `Start` and `DataRow`s from each batch — concurrently while the producer runs, then finishes with `CommandComplete("SELECT n")` (n is the producer's authoritative row count, carried on `StreamOutcome::Streamed { count }`) and `ReadyForQuery`. The producer returns a `StreamOutcome`: `Streamed` for a SELECT, `Direct(ExecutionResult)` for an ordinary cancelable non-streamed result, `Durable(ExecutionResult)` after an autocommit or completed irreversible session/transaction-state boundary (including savepoint commands), or `SessionReset(ExecutionResult)` for completed `DISCARD ALL` so the connection clears prepared statements and portals through a typed signal. The producer holds the snapshot's GC-horizon advertisement and any transaction guard for the whole stream and returns the transaction slot only when it finishes, so MVCC and transaction semantics are unchanged. A retrying `try_send` loop provides backpressure while polling the statement cancellation token; a dropped receiver (client gone) turns the next push into a graceful stop. The extended-protocol `Execute` streams identically through `execute_prepared_cancelable_streamed` / `execute_prepared_in_session_streamed`, differing only in that its `RowDescription` comes from `Describe` (so `Start` is consumed without emitting one) and its `ReadyForQuery` comes from `Sync` (see Connection Handling). Streaming alters neither protocol message encoding nor physical operator semantics; the materializing path (`execute_simple_with_session_sequences` and the `execute_sql` / `execute_prepared` convenience helpers, used by tests) shares the same executor drive.

Once an autocommit operation has crossed its durable commit boundary, a later timeout or `CancelRequest` cannot replace its terminal success with an `ErrorResponse`. A completed terminal frame wins a simultaneous cancellation race. If the terminal write is still pending when cancellation is observed, however, the connection closes because the socket may contain a partial frame; the durable database outcome remains successful. This applies to simple and extended execution, completed session resets, and successful autocommit `COPY FROM`; direct or explicit-transaction outcomes remain cancelable until their terminal response.

COPY IN/OUT retains its in-flight shutdown guard while its blocking worker owns database work, then normally releases the guard immediately after joining the worker and restoring transaction state, before terminal socket responses. If a restored explicit transaction still owns a writer guard, the in-flight guard is retained through the response/drain boundary so graceful shutdown times out safely instead of entering a checkpoint that would block behind that transaction. Thus a non-reading client cannot make shutdown skip its final checkpoint after settled autocommit/read-only COPY work or bypass the timeout gate while a write transaction remains open.

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
    pub lock_manager: Arc<LockManager>,
    /// Shared for all catalog binding/introspection capture; exclusive for DDL
    /// provisional mutation through Commit/rollback.
    pub catalog_publication_gate: Arc<RwLock<()>>,
    pub relation_publish_gate: Arc<RwLock<()>>,
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
captures it **after** acquiring `Share` on every target, so no target writer can
advance it, and accounts for every reader advertised at that instant (Milestone
F4a). The CLOG that records settled transaction outcomes
lives in the WAL manager (`Clog`, seeded from `clog.dat` and folded forward
with later `Commit`/`Abort` records, or rebuilt from retained WAL when no
snapshot exists; see `docs/specs/crates/wal.md`), separate from this registry
of still-running transactions.

Checkpoint flushes dirty pages in place to the heap and advances the redo
boundary; its cost is O(pages changed), not O(database size). Driven by the
server under the **exclusive checkpoint guard** (E2b), which drains all in-flight
shared writers and runs alone:

1. Acquire the exclusive checkpoint guard (`begin_checkpoint`) — waits for all shared writer/explicit-transaction participants to drain, then holds off new participants until the checkpoint returns.
1a. **Auto-prune (Milestone F4b/F4c, `mvcc.md` §9).** When the dead-row threshold is reached, capture `horizon = gc_horizon()` under the checkpoint guard and run the full pass before flushing dirty pages. Capture the vacuum boundary as `B = min(next_txn_id, oldest_active_xid)`, treating no active xid as `next_txn_id`; an xid registered before blocking on this checkpoint therefore holds the floor back. Advance the floor and reset the counter only after the pass. The same checkpoint flushes/fsyncs the vacuum pages before `persist_clog` may prune aborted entries below `B`, preserving the F4c durability order.
2. `wal.flush()` (a page's redo must be durable before the page is written).
3. `buffer_pool.flush_dirty_pages()` — write every flushable dirty page to the heap `PageStore`. With the relaxed flush gate (Milestone D1, `mvcc.md` §8) this spills committed, aborted, and — under Stage 2 — in-flight dirty pages alike; all are WAL-durable after (2), and the CLOG hides the non-committed tuples.
4. `store.sync_all()` — fsync the heap before advancing the redo boundary.
5. `checkpoint_lsn = wal.flushed_lsn()`.
6. `control.store(checkpoint_lsn, sorted_table_ids, catalog_bytes)` — the durable commit point. Before serializing `catalog_bytes`, checkpoint overlays storage's live sequence `(last_value, is_called)` values into the catalog snapshot so the control record contains the current sequence baseline.
6b. `wal.persist_clog(checkpoint_lsn)` — write the durable CLOG snapshot `clog.dat` (every transaction outcome plus both floors) **before** truncating, so it remembers every outcome the truncation is about to drop (`mvcc.md` §5.4).
7. Append the `Checkpoint { redo_lsn }` WAL marker stamped with the transaction-id high-water mark (`txn_id = next_txn_id - 1`, so the allocator boundary survives truncation; see `wal.md`), `wal.flush()`, `wal.truncate_before(checkpoint_lsn)`. Truncation is **unconditional**: it drops every record below `checkpoint_lsn`. It is safe because `persist_clog` (6b) durably recorded every aborted outcome, and under the exclusive guard no writer is in flight, so all transactions below `checkpoint_lsn` are settled and captured by the snapshot (`wal.md`, `mvcc.md` §5.4/§8). **F4c:** the **vacuum floor** (advanced by the full VACUUM in 1a) bounds `clog.dat` pruning — `persist_clog` drops the explicit `Aborted` entry of a reclaimed aborted transaction below the floor; WAL truncation does not consult it.
8. `buffer_pool.mark_all_clean()` (clears dirty flags, re-arms `needs_fpi`).
9. Relation-generation cleanup: storage attempts to remove truncate/drop-retired generations whose `Arc` snapshots are no longer referenced and untracked orphan files whose buffer frames are not pinned or in transition. Files still tracked by live storage metadata, rollback metadata, or pending retired generations are protected; dropped metadata is not live protection once commit has queued its retired generation.
10. Release the exclusive checkpoint guard.

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
5. For simple `Query` messages, run `QueryService::execute_simple_streamed` using the blocking thread pool and a bounded row channel.
6. Encode and write server messages.
7. On query execution errors, send `ErrorResponse` and `ReadyForQuery` and keep the connection open.
8. On protocol decode errors, send `ErrorResponse` and `ReadyForQuery`, then close the connection because the codec buffer state may be unrecoverable.
9. On Terminate or unrecoverable IO error, close connection.

For simple-query SQL cursors, the connection owns `Session.cursors`, a
transaction-scoped registry separate from extended-protocol portals. `DECLARE
<name> CURSOR FOR SELECT ...` requires a healthy explicit transaction, rejects
duplicates with `42P03`, starts a parked read-only cursor worker, and returns
`DECLARE CURSOR`. `FETCH` resolves the name, emits `RowDescription` and text
`DataRow`s, returns `FETCH n`, and leaves an exhausted cursor open so later
fetches return zero rows until `CLOSE` or transaction cleanup. `CLOSE` removes
the cursor and returns `CLOSE CURSOR`; missing cursor names return `34000`.
`COMMIT`, `ROLLBACK`, successful `ROLLBACK TO SAVEPOINT`, `DISCARD ALL`, and
disconnect close SQL cursors. SQL cursor statements are rejected in the extended
query protocol.

The connection also serves the extended query protocol, holding per-connection
prepared-statement and portal maps (named and unnamed). `Parse` calls
`QueryService::prepare_sql` (mapping the declared parameter type OIDs, `0` =
unspecified, including PostgreSQL `oid` OID 26 as SaguaroDB integer semantics,
while retaining the declared `PgType`, or an unambiguous catalog-function
argument `PgType` for unspecified parameters, for `ParameterDescription` and
selected parameter result metadata), caches referenced table/view schema
versions for bound data statements, and replies `ParseComplete`. Cancellation
is checked during the blocking-pool schema-guard wait and binding work, then again
before publishing the named statement. `Bind` decodes
each parameter value (text or binary, per the Bind format codes, via the
declared `PgType`) into a portal
and checks cancellation before publishing it, then replies `BindComplete`.
`Describe` builds its metadata and checks cancellation before replying with `ParameterDescription` +
`RowDescription`/`NoData` for a statement, or `RowDescription`/`NoData` in the
portal's result formats for a portal. Requested binary result formats are
preserved for supported scalar wire types, but virtual-catalog vector/array
columns (`int2vector`, `oidvector`, `int2[]`, `oid[]`) are reported and encoded
as text because SaguaroDB has no binary array/vector value representation yet.
`Execute` runs the portal on the blocking
thread pool; a SELECT streams its `DataRow`s through the same bounded-channel
bridge as the simple-query path (`docs/specs/streaming.md`) in the requested
result formats, followed by `CommandComplete` (no `RowDescription`, that came from
`Describe`; no `ReadyForQuery`, that comes from `Sync`). For SELECT portals with
`max_rows == 0`, `Execute` drains the query to completion. For read-only SELECT
portals with `max_rows > 0`, `Execute` starts or resumes a server-local cursor
worker backed by `executor::OpenQuery`, sends at most `max_rows` rows, and either
sends `PortalSuspended` when rows remain or `CommandComplete("SELECT n")` when
the portal is exhausted; `n` is the cumulative row count across every fetch of
that portal. A suspended portal created outside an explicit transaction may be
resumed only before the next `Sync` or simple `Query`; either closes any
still-suspended autocommit portal before reporting `ReadyForQuery`. A suspended
portal created inside an explicit transaction may survive `Sync`, but is closed
when it is exhausted, explicitly closed, replaced by `Bind`, discarded by
`DISCARD ALL`, invalidated by a successful `ROLLBACK TO SAVEPOINT` that changes
the transaction's live subxid set, when the transaction ends, or when the
connection closes. Every other statement is returned whole as
`StreamOutcome::Direct`, `StreamOutcome::Durable` after an autocommit
durable/irreversible boundary, or, for `DISCARD ALL`,
`StreamOutcome::SessionReset`, and
`max_rows` does not limit it. `Execute` participates in the session's CURRENT transaction:
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
`CloseComplete`. `Close` is not a timed statement, so stale cancellation recorded
while the backend was idle does not interrupt its response. An error inside an extended sequence sends `ErrorResponse` and then
skips every message except `Sync`/`Terminate`; only `Sync` clears that aborted
state, so a simple `Query` arriving first is discarded.

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

Query cancellation uses a process-wide `CancelRegistry` on `ServerComponents` mapping a per-connection `BackendKey { process_id, secret_key }` to that connection's `QueryCancel` token plus an async protocol-loop wakeup. At startup the server allocates a key (a counter-based `process_id` and a random `secret_key`), registers the target, and sends `BackendKeyData` after the `ParameterStatus` messages and before `ReadyForQuery`. A `CancelRequest` arrives on its own connection (handled during negotiation, before startup): the server looks up the `BackendKey`, records `CancelReason::UserRequest` if the token has no earlier reason, wakes an idle protocol loop (notably COPY FROM waiting for input), and closes without any reply; an unknown or stale key is ignored. The wakeup is honored only while a statement lifecycle is active, so canceling an idle backend produces no spurious response. The connection deregisters its key on disconnect. See the cancellation-token plumbing under Connection Handling.

Each session also owns a race-safe statement timer driven by the effective
transaction/session `statement_timeout`. Arming a genuinely new statement first
aborts and joins any prior timer task, then resets the shared `QueryCancel`, so an
expired timer from old idle work cannot cancel later work on a reused connection.
Restarting the timer for another message in the same extended-query lifecycle
aborts/joins the old task but does **not** reset cancellation: a timeout or
`CancelRequest` racing Parse/Bind/Describe/Execute remains pending and enters
skip-until-Sync instead of being erased. Expiry records
`CancelReason::StatementTimeout`; the token's first reason wins if a concurrent
`CancelRequest` races it. The simple-query timer starts when the `Query` message is
handled and is disarmed only after the statement's terminal response (or retained
for an active COPY stream). For extended query, each non-failed `Parse`, `Bind`,
`Describe`, and `Execute` arrival arms/restarts the timer; completion of `Execute`
or receipt of `Sync` disarms it. Expiry while the connection is waiting between
extended messages emits `QueryCanceled` (`57014`, `canceling statement due to
statement timeout`), enters skip-until-Sync mode, and leaves `ReadyForQuery` to the
following `Sync`. An extended-message error also disarms the timer. A value of `0`
creates no timer task but still resets cancellation state for the new statement.
If COPY FROM is canceled while waiting for client input, timer expiry or the
`CancelRegistry` wakeup makes the connection send `Fail` to abort the worker,
emit exactly one reason-specific `ErrorResponse`, and enter
a lightweight drain state: later `CopyData` is discarded until `CopyDone` or
`CopyFail`, which emits the sole `ReadyForQuery` without a second error.
The worker is joined before entering that drain state. Its in-flight shutdown
guard is normally released then, but remains installed when the restored explicit
transaction still owns a writer guard, so shutdown cannot bypass its timeout gate
and block indefinitely in the final checkpoint.

Foreground statement waits are cancellation-aware beyond executor row polling:
writer/exclusive concurrency guards, relation-publication gates, and snapshot-exclusion waits use short timed
lock waits, bounded producer channels retry nonblocking sends, COPY FROM polls its
input channel, and the async socket side races channel receives and writes against
the same cancellation token. Foreground VACUUM, primary-key validation/backfill,
TOAST scans, and compression sampling check cancellation while waiting for
exclusion and at page/leaf boundaries. ZDICT training runs as a bounded,
side-effect-free helper job while foreground DDL polls cancellation; post-durable rewrites deliberately remain
uninterruptible. Autocommit DML/DDL/COPY write paths check once more immediately
before appending the statement's durable commit record; cancellation before that
boundary rolls back (or poisons an explicit transaction), while cancellation after
that statement commit cannot turn it into an error and cleanup runs to completion.
VACUUM is nontransactional and may durably commit a hidden-TOAST cleanup
subtransaction, then observe cancellation before parent/hidden physical vacuum;
that restart-safe partial maintenance is reported as `QueryCanceled`, and a later
VACUUM resumes the remaining work. After joining a blocking producer, the connection
therefore preserves an explicit `StreamOutcome::Durable` or completed
`SessionReset` if the async channel wait observed a late cancellation. Session
mutations check cancellation before changing state, then use that completed
boundary so the protocol cannot report a canceled `SET`/`RESET`/`DISCARD ALL`
whose effects remain applied. `Direct`, interrupted stream/COPY, and still-open
explicit-transaction data outcomes remain cancelable at that boundary. Channel
receive/write waits check cancellation both before selection and after their future
becomes ready, so simultaneous channel closure cannot hide expiration. Successful
terminal response writes (including COPY completion) use the same cancellation
race through `CommandComplete`/`ReadyForQuery`; if cancellation interrupts a
possibly partial terminal frame, the connection closes instead of appending an
error to corrupt framing. A completed transaction-ending `ROLLBACK`, like COMMIT,
is an irreversible `Durable` outcome, so late consumer cancellation cannot report
failure after the transaction has already been removed. Once an
autocommit xid has been registered, every fallible snapshot
or execution-context setup path uses the normal pre-durable rollback so timeout
cannot leave an `InProgress` xid pinning snapshots or GC.

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
- Recovery redoes every record after the control record's checkpoint LSN
  regardless of transaction outcome, and the CLOG seeded/folded at WAL open
  decides visibility; without `clog.dat`, retained WAL is the reconstruction
  fallback. A transaction in-flight at crash is recovered as aborted.
- Failed write rolls back buffer pages and does not append commit.
- Successful write appends commit, flushes WAL, commits buffer before returning.
- Checkpoint flushes dirty pages to the heap and advances the control checkpoint LSN.
- Protocol startup and simple query work over a loopback TCP connection.
- An extended-protocol Parse/Bind/Describe/Execute/Sync sequence runs a parameterized query over a loopback connection with both text and binary parameter and result encodings.
- An error inside an extended sequence is reported and the following messages are skipped until Sync, after which the connection is reusable.
- Startup sends `BackendKeyData`, and a `CancelRequest` carrying a registered backend key records user-request cancellation on that backend's token (and is ignored for an unknown key).
- With TLS disabled, an `SSLRequest` is rejected with `N` and the same connection then completes a plaintext startup.
- With TLS enabled, an `SSLRequest` is accepted with `S`, the TLS handshake completes, and a simple query runs over the encrypted stream.
- Supplying exactly one of `--tls-cert-file`/`--tls-key-file` is rejected during config parsing.
- A `GSSENCRequest` is declined with `N`; the client may then negotiate SSL or start in plaintext on the same connection.
- Graceful shutdown runs checkpoint after in-flight query completes.
- `CREATE TABLE ... WITH (compression = 'zstd')` then insert then restart then select round-trips correctly.
- The exact pgbench multi-table DROP statement succeeds; a late invalid target
  leaves every earlier table intact. `CREATE TABLE ... WITH (fillfactor=100)`
  succeeds without persisting a storage option.
- `CREATE TABLE ... WITH (toast...)` persists the resolved TOAST options and installs the hidden TOAST relation for tables with `TEXT`, `BYTEA`, or array columns without making it visible through user table-name lookup.
- `ALTER TABLE ... SET (compression = ...)` rewrites a table in both directions (`none → zstd`, `zstd → none`) with correctness preserved across a restart; a crash simulated mid-rewrite recovers with a self-describing mix of old/new-encoded pages still readable, and re-running the same `ALTER` completes the rewrite.
- `ALTER TABLE ... ADD PRIMARY KEY` validates and enforces the key on an existing heap-identity table, rejects duplicate/NULL existing rows without mutating the table, and survives restart; `ALTER TABLE ... DROP PRIMARY KEY` removes the primary-key constraint index, rebuilds hidden heap identity, allows duplicate key values, and survives restart.
- Standalone FK ADD validates existing simple/composite/self-referencing rows,
  generated/named constraints, declared UNIQUE targets, simple and extended
  execution, and restart recovery; failed validation publishes no FK. Generic
  DROP handles FK, PK, `IF EXISTS`, restart, and explicit-transaction rejection.
- `VACUUM` and maintenance `ALTER TABLE` remain rejected inside an explicit
  transaction block. `TRUNCATE` is the exception: it may run in a healthy
  explicit transaction and normally retains `AccessExclusive` through transaction
  end. `ROLLBACK TO SAVEPOINT` instead restores the captured lock mode and
  generation/catalog state; top-level completion publishes or restores the final
  generation swap as specified in `docs/specs/table-locks.md`.
  Autocommit and extended prepared-maintenance `TRUNCATE` retain the same
  relation-generation behavior and command tag `TRUNCATE TABLE`.
- Schema-evolution `ALTER TABLE` participates in explicit transactions, executes in simple autocommit and extended prepared execution, rewrites rows for ADD/DROP, preserves secondary indexes, and survives restart.
- View DDL participates in explicit transactions, executes in simple autocommit and extended prepared execution, invalidates stale prepared plans, appears in `pg_class`/`pg_attribute` and `information_schema`, and survives restart.
- Autocommit `TRUNCATE` performs a relation-generation swap, clears heap, primary-key index, secondary-index, and hidden TOAST storage, permits reinserting prior keys, survives restart, and returns command tag `TRUNCATE TABLE`.
- Multi-table autocommit TRUNCATE publishes no target before commit, rolls back
  every target on a pre-commit preparation failure, publishes the complete batch
  after commit, and recovery before checkpoint empties every target including
  secondary-index and hidden-TOAST generations.
- `TRUNCATE a, missing` and a list with a late wrong-object target fail during
  initial catalog preflight before allocating an xid or storage ids and leave every table
  intact. A successful N-table batch produces exactly one xid/commit, one
  storage/buffer cleanup, one deregistration/wakeup, one checkpoint-accounting
  event, and one retired-generation cleanup pass.
- Recovery resolves a dictionary created (via `ALTER`) after the last checkpoint, both from the replayed `CreateDictionary` WAL record and from the durable dictionary file seeded before redo.
- `VACUUM` still runs correctly on a compressed table.
- `VACUUM ANALYZE` and `VACUUM ANALYZE <table>` run the ordinary VACUUM pass,
  then the ANALYZE pass, return the `VACUUM` command tag, and persist
  statistics for the targets (`docs/specs/statistics.md`).
