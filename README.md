# SaguaroDB

SaguaroDB is a SQL-compatible relational database written in Rust. It runs as a
standalone server, accepts client connections over the PostgreSQL wire protocol,
executes SQL through a parse/bind/plan/execute pipeline, and stores data in
page-oriented heap and index files with write-ahead-log recovery.

SaguaroDB is a compact, trait-boundary database with PostgreSQL-style
multi-version concurrency control (MVCC): snapshot-isolated reads, concurrent
writers, savepoints, Serializable Snapshot Isolation (SSI), `VACUUM`, HOT
updates, compression, and TOAST, while keeping the major subsystems clear behind
trait seams.

## What Works

- Standalone multi-client Tokio server. It accepts PostgreSQL startup,
  cancellation, simple-query, extended-query, and COPY messages; authentication
  is intentionally not implemented, so all users/databases are accepted.
- PostgreSQL simple query and extended query (`Parse`/`Bind`/`Describe`/
  `Execute`/`Close`/`Sync`), prepared statements, portals, and SELECT portal
  suspension for extended-query `max_rows`.
- Optional TLS/SSL server connections when both a PEM certificate chain and
  private key are configured.
- DDL and maintenance for `CREATE`/`DROP SCHEMA`, `CREATE`/`DROP TABLE`, `CREATE`/`DROP VIEW`,
  `CREATE`/`DROP SEQUENCE`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`, `TRUNCATE`,
  `VACUUM`, table compression/TOAST option changes, primary-key add/drop,
  standalone foreign-key add/drop, and
  schema evolution for add/drop/rename/type-change columns and table renames.
  One- and two-part user object names resolve through the session `search_path`.
  `CREATE TABLE` accepts column- and table-level foreign keys referencing declared
  primary-key/UNIQUE constraints with immediate `NO ACTION`/`RESTRICT` enforcement.
  Existing tables may add the same constraints with `ALTER TABLE ... ADD
  [CONSTRAINT name] FOREIGN KEY`; `DROP CONSTRAINT [IF EXISTS] name [RESTRICT]`
  removes a foreign key or routes a primary-key constraint name to the existing
  primary-key drop behavior.
- DML for `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` (including
  `UPDATE ... FROM`), `DELETE` (including `DELETE ... USING`), `RETURNING`,
  primary-key `ON CONFLICT DO NOTHING` / `DO UPDATE`, and
  `COPY ... FROM STDIN` / `COPY ... TO STDOUT` in text or CSV format.
- `SELECT` support includes FROM-less projections, `VALUES`, views,
  non-recursive CTEs, derived tables, set operations, `DISTINCT`, `WHERE`,
  inner/cross/left/right/full joins, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`,
  `OFFSET`, scalar / `[NOT] IN` / `[NOT] EXISTS` subqueries — correlated in
  `WHERE`, the select list, and `HAVING`, with equality shapes decorrelated to
  hash semi/anti joins — and `LATERAL` derived tables
  (`docs/specs/subqueries.md`). `unnest(array)` and integer
  `generate_series(...)` are implicitly lateral table functions.
- Data types include integer widths and serial families, boolean, text and
  bounded character types, date/time/timestamp/timestamptz/interval, bytea,
  uuid, floating point, numeric, rectangular arrays, and null values. Arrays
  support constructors, casts, comparisons, subscripts, `op ANY(array)`, text
  and binary protocol formats, COPY fields, and `array_agg`.
- Column defaults, `nextval` defaults, non-constant expression defaults,
  unnamed `CHECK` constraints, sequence functions, PostgreSQL-compatible system
  information functions, and catalog/probe functions used by common clients.
- Multi-statement transactions, autocommit, transaction-scoped and
  session-scoped isolation settings, savepoints, forward-only read-only SQL
  cursors, `statement_timeout`, `work_mem`, and `DISCARD ALL`.
- MVCC tuple visibility, concurrent writers, locking SELECT with four PostgreSQL
  tuple-lock strengths plus NOWAIT/SKIP LOCKED, Read Committed, Repeatable Read,
  and Serializable Snapshot Isolation.
- Garbage collection via `VACUUM [table]`, coordinated TOAST cleanup, and
  checkpoint auto-pruning (`--auto-vacuum-dead-rows`).
- Optimizer statistics via `ANALYZE [table]` / `VACUUM ANALYZE` (sampled row
  counts, null fractions, n_distinct, most-common values, histograms), exposed
  through `pg_class.reltuples`/`relpages` and `pg_stats`, refreshed
  automatically at checkpoints (`--auto-analyze-changed-rows`).
- HOT (heap-only tuples): eligible same-page updates that do not change indexed
  columns skip secondary-index maintenance, and dead HOT chains are pruned in
  place.
- Rule-based planning with table scans, primary-key and secondary-index scans,
  hash joins for inner equi-joins, streaming SELECT execution, and materialized
  blocking operators where needed. ANALYZE statistics feed cardinality
  estimates (shown as `rows=` in `EXPLAIN`) and the first cost-based
  decisions: hash-join build-side choice and seq-vs-index scan selection.
  SELECT-only `EXPLAIN ANALYZE` adds per-node actual time, rows, and loops while
  discarding query rows; `EXPLAIN (ANALYZE FALSE)` remains planner-only.
- Page-backed MVCC storage with heap files, durable non-clustered
  storage-identity B-trees, secondary B-tree indexes, TOAST relations for large
  values, at-rest page compression, dictionary-backed zstd payload compression,
  and WAL full-page-image compression.
- Physiological redo WAL with full-page-image torn-page protection, manifest
  checkpoints, WAL truncation, and crash recovery.

SaguaroDB deliberately does not implement authentication, replication, a custom
wire protocol, mutual TLS/client-certificate authentication, or time-travel
queries. Important follow-on areas include a fuller cost-based optimizer (join
reordering, multi-column index ranges), recursive queries, window functions,
advanced index options
(partial/expression/concurrent/include indexes), and more complete sequence and
constraint DDL.

## Quick Start

Prerequisites:

- Rust stable. The repository includes `rust-toolchain.toml` with `rustfmt` and
  `clippy` components.
- Optional: `psql` for interactive SQL testing.

Build the whole workspace:

```bash
cargo build --workspace
```

Run tests:

```bash
cargo test --workspace
```

Run the server on port `5433` with a disposable data directory:

```bash
cargo run -p saguarodb-server --bin saguarodb -- --data-dir /tmp/saguarodb-dev --port 5433
```

The server runs in the foreground and listens on `0.0.0.0:<port>`. It does not
print a startup banner, so a quiet terminal usually means it is running. Stop it
with `Ctrl-C` or SIGTERM to trigger graceful shutdown.

Connect with `psql`:

```bash
psql "host=127.0.0.1 port=5433 user=saguarodb dbname=saguarodb sslmode=disable"
```

Try a small SQL session:

```sql
create table users (
  id serial primary key,
  name text not null,
  active boolean default true
);

insert into users (name) values ('Ada'), ('Grace') returning id, name;
create index users_active_idx on users (active);

begin isolation level serializable;
savepoint before_update;
update users set active = false where name = 'Grace' returning id, active;
rollback to savepoint before_update;
commit;

explain select id, name from users where active = true order by id;
```

## Server Options

The server accepts:

```text
--data-dir <PATH>                  default ./data
--port <PORT>                      default 5433
--buffer-pool-frames <N>           default 1024
--checkpoint-every-n-commits <N>   default 100
--checkpoint-wal-bytes <BYTES>     default 67108864
--auto-vacuum-dead-rows <N>        default 10000 (0 disables auto-prune)
--auto-analyze-changed-rows <N>    default 10000 (0 disables auto-analyze)
--shutdown-timeout-ms <MS>         default 30000
--deadlock-timeout-ms <MS>         default 1000
--tls-cert-file <PATH>             PEM cert chain; enables TLS (needs --tls-key-file)
--tls-key-file <PATH>              PEM private key; enables TLS (needs --tls-cert-file)
--help                             print usage and exit 0
```

For local development, prefer a data directory outside the repository or an
ignored directory:

```bash
cargo run -p saguarodb-server --bin saguarodb -- --data-dir /tmp/saguarodb-dev
```

## Transactions & Concurrency

SaguaroDB uses PostgreSQL-style MVCC: an `UPDATE`/`DELETE` writes a new in-heap
row version instead of overwriting, and each statement reads against a snapshot
that hides versions it should not see. Aborts are status-based: a rolled-back
transaction's versions are left in place, made invisible through the commit-status
map, and reclaimed later by VACUUM.

- **Transactions.** `BEGIN`/`START TRANSACTION` ... `COMMIT`/`ROLLBACK` group
  statements into a unit; a standalone statement runs in its own autocommit
  transaction. A statement that errors puts the transaction into a failed state
  that accepts only `ROLLBACK`, `ROLLBACK TO SAVEPOINT`, or `COMMIT`, which
  rolls back a failed top-level transaction.
- **Isolation levels.** Read Committed (the default) takes a fresh snapshot per
  statement; Repeatable Read takes one snapshot at the first statement and reuses
  it. Choose the level per transaction (`BEGIN ISOLATION LEVEL ...`,
  `SET TRANSACTION ISOLATION LEVEL ...`) or as the per-connection default
  (`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`).
  `SERIALIZABLE` uses Serializable Snapshot Isolation and can abort unsafe
  dependency cycles with serialization error `40001`.
- **Savepoints.** `SAVEPOINT`, `RELEASE SAVEPOINT`, and
  `ROLLBACK TO SAVEPOINT` create nested subtransactions. Rolling back to a
  savepoint marks that subtransaction's row versions aborted, releases object
  locks acquired since the savepoint, restores upgraded locks to their earlier
  modes, and preserves the outer transaction. Releasing a savepoint keeps its
  changes and locks merged into the parent.
- **Concurrency.** Plain MVCC tuple reads take no row locks and do not conflict
  with ordinary DML writers. A top-level SELECT over one base table can use
  `FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, or `FOR KEY SHARE`, optionally
  with `NOWAIT` or `SKIP LOCKED`; it locks and rechecks the latest row version.
  Read Committed UPDATE/DELETE likewise resolve a concurrently updated row,
  rerun qualification, and mutate the latest version when it still matches;
  retained-snapshot isolation returns `40001` instead.
  SQL statements also take transaction- or statement-owned
  table locks, so a reader can wait behind an `AccessExclusive` operation such as
  `TRUNCATE` or table-rewrite DDL. Writers run concurrently, coordinated by table
  and row locks plus per-index and per-heap structural latches; a writer blocked
  on a lock invokes deadlock detection after `--deadlock-timeout-ms`. Conflicts
  can return deadlock error `40P01`, unique violation `23505`, or serialization
  error `40001`, depending on the committed state and isolation level. Writes,
  DDL, and WAL-writing maintenance share the checkpoint-participant guard; only
  checkpoint takes it exclusively and drains page/WAL writers.
- **Garbage collection.** Dead row versions are reclaimed by `VACUUM [table]` and
  by automatic pruning at checkpoint (`--auto-vacuum-dead-rows`). For tables with
  TOAST storage, the server coordinates parent-row pruning and hidden TOAST-chunk
  cleanup. HOT (heap-only tuples) lets a same-page `UPDATE` that changes no
  indexed column skip secondary-index work and collapses dead version chains in
  place.

## Architecture

SaguaroDB is organized as a Rust workspace. Each major subsystem owns a crate,
and the public contracts are documented in `docs/specs/`.

```text
                 +-------------------+
psql / client -> | server            |
                 | accept loop       |
                 | QueryService      |
                 +---------+---------+
                           |
                           v
    +----------+     +-----+-----+     +----------+     +----------+
    | protocol |     | parser    | --> | planner  | --> | executor |
    | codec    |     | SQL -> AST|     | bind/plan|     | operators|
    +----------+     +-----------+     +----------+     +-----+----+
                                                              |
                                                              v
                +----------+     +---------+     +------+     +-------+
                | catalog  |     | storage | --> |buffer|     |control|
                | schemas  |     | tables  |     |pages |     |record |
                +----------+     +----+----+     +------+     +-------+
                                      |
                                      v
                              +-------+--------+
                              | WAL | compress |
                              +-----+----------+
```

The executor also uses the one-way `spill` crate for per-operator `work_mem`
accounting, rewindable temporary tapes, and external sorting under
`<data-dir>/tmp`.

Crate dependency flow:

```text
server
  -> protocol, parser, planner, executor, control, storage, buffer, wal,
     catalog, compress, spill, common

executor -> planner, storage, catalog, spill, common
planner  -> parser, catalog, common
storage  -> buffer, wal, compress, common
control  -> common
protocol -> common
parser   -> common
buffer   -> common
wal      -> common
catalog  -> common
compress -> common
spill    -> common

`common` is the leaf crate for shared database types. `compress` depends only on
`common` plus codec support, and `spill` depends only on `common` plus
`tempfile`.
No library crate depends on server.
```

Workspace layout:

```text
crates/
  common/    shared IDs, values, rows, errors, contexts, traits
  spill/     query-local memory accounting, spill tapes, external sorting
  compress/  compression codecs, page envelopes, TOAST helpers, dictionaries
  parser/    SQL text to SaguaroDB AST
  catalog/   table metadata, dense ordinals + stable column IDs, schema snapshots
  planner/   binding, logical plans, physical plans, EXPLAIN formatting
  executor/  expression evaluation and volcano-style operators
  storage/   page-backed MVCC storage, B-tree indexes, TOAST, VACUUM, recovery
  buffer/    page cache, frame latches, dirty tracking, in-place flushing
  wal/       physiological redo WAL, commit/abort records, transaction-status (CLOG), replay
  control/   manifest.dat control record: redo boundary plus catalog snapshot
  protocol/  PostgreSQL wire codec and connection state
  server/    binary, startup/recovery, TCP listener, query orchestration
```

## Query Path

Most SQL flows through the same parse, bind, plan, and execute pipeline. Plain read
statements take an MVCC snapshot and `AccessShare` table locks but no row locks;
locking SELECT takes `RowShare`, a transaction ID, and selected tuple locks;
data-changing statements run under a shared writer guard and receive a
transaction ID plus a snapshot for visibility, WAL, table/row locks, and
conflict detection. DDL and maintenance also use the shared writer guard plus
relation-specific locks and the catalog publication gate. Checkpoint alone takes
the exclusive checkpoint guard and drains page/WAL writers.

```text
client query
    |
    v
protocol codec
    |
    v
QueryService
    |
    +--> parse SQL
    |
    +--> classify statement
    |       |
    |       +-- plain SELECT / EXPLAIN: MVCC snapshot + AccessShare table locks
    |       +-- locking SELECT: txn_id + RowShare + tuple locks + latest-row recheck
    |       +-- DML / COPY FROM:  shared writer guard + txn_id + table locks + snapshot
    |       +-- DDL / maintenance:
    |                            shared writer guard + object locks/catalog gate
    |       +-- checkpoint:      exclusive checkpoint guard (drains writers)
    |       +-- transaction, savepoint, SET/SHOW/RESET, DISCARD:
    |                            handled by server session state
    |
    +--> bind names and types against catalog
    |
    +--> logical plan
    |
    +--> physical plan
    |
    +--> executor
            |
            +-- SELECT: scans storage and returns rows
            +-- DML:    mutates storage pages and appends WAL operation records
            +-- DDL:    mutates catalog/storage and appends WAL operation records
            +-- COPY:   uses COPY sub-protocol encoders/decoders around storage
```

For successful writes, the server appends a WAL `Commit` record, fsyncs the WAL,
then performs in-memory commit cleanup. A failed or rolled-back transaction is
aborted by status: the server appends an `Abort` record and marks the transaction
aborted in the commit-status map (CLOG). Its row versions are not undone in place
and are invisible under MVCC until VACUUM reclaims them. Storage rollback
metadata may clean up unpublished relation-generation files or retire removed
generations until snapshots drain, but heap/index page bytes are not undone.

## Data Files

The data directory contains the write-ahead log, durable CLOG snapshot, control
record, dictionary files, ephemeral query-spill directory, and physical
heap/index relation files. `manifest.dat` is the control record: the redo
boundary (`checkpoint_lsn`), the live table ids, and the serialized catalog,
written atomically as a single CRC-checked envelope.

Each live table generation has a `storage_id`. The row heap is stored at
`<storage_id>.heap`, the reserved storage-identity B-tree is stored at
`<storage_id>.idx`, and each secondary-index generation is stored at
`<storage_id>.sidx`. Logical table and index IDs stay stable across operations
such as `TRUNCATE` and rewrite-style schema evolution; `storage_id` names the
current physical files. TOAST-enabled tables also have hidden TOAST relation
generations with the same heap/index file pattern.

```text
data/
  wal.dat
  clog.dat
  manifest.dat
  manifest.dat.tmp
  tmp/                       ephemeral query spill files
  dicts/
    <dict-id>.dict
  heap/
    <storage-id>.heap
    <storage-id>.idx
    <storage-id>.sidx
```

At runtime, the buffer pool holds clean pages loaded from the heap and index
files plus dirty pages created by committed and in-flight statements. Dirty
pages are flushed in place to their home files once their page-LSN is
WAL-durable. MVCC visibility is decided by the CLOG, so flushed pages may
contain committed, aborted, or still-in-flight row versions.

```text
             normal operation

        committed SQL writes
                |
                v
        +-----------------+
        | redo WAL        |  fsynced on every commit
        | data/wal.dat    |
        +-----------------+
                |
                | records page changes since the redo boundary
                v
        +-----------------+
        | buffer pool     |  dirty pages flushed in place
        +-----------------+
                |
                | checkpoint flushes dirty pages, advances the redo boundary
                v
        +-----------------+
        | heap/ files     |  heap + index pages, written in place
        +-----------------+
                |
                v
        +-----------------+
        | manifest.dat    |  checkpoint_lsn + catalog snapshot
        +-----------------+
```

## Checkpointing

Checkpoints are triggered after a configured number of committed statements, a
configured amount of WAL growth, or graceful shutdown. A checkpoint flushes the
dirty pages changed since the last one, so its cost is O(pages changed), not
O(database size).

```text
checkpoint
    |
    +-- take global write guard
    |
    +-- flush WAL so every page it describes is durable
    |
    +-- flush WAL-durable dirty pages in place to heap/index files
    |
    +-- fsync the heap/index files
    |
    +-- choose checkpoint_lsn from the WAL high-water mark
    |
    +-- write manifest.dat.tmp (checkpoint_lsn + table ids + catalog)
    |
    +-- fsync manifest.dat.tmp
    |
    +-- rename manifest.dat.tmp -> manifest.dat
    |
    +-- fsync data directory
    |
    +-- persist clog.dat through checkpoint_lsn (atomic write + fsync)
    |
    +-- append WAL Checkpoint metadata record and fsync
    |
    +-- truncate WAL records before checkpoint_lsn
    |
    +-- mark buffer pages clean
```

The control record is the commit point: it is written only after the heap and
index pages it describes are durable. Before the WAL prefix is truncated, the
server also persists `clog.dat` through the new boundary so every transaction
outcome removed from WAL remains durable. If the server crashes mid-checkpoint,
recovery falls back to the previous redo boundary, where this cycle's full-page
images repair any torn page writes.

## Recovery

Startup opens the WAL and its durable `clog.dat` snapshot, enables buffer
stealing, reads the control record for the redo boundary and catalog, validates
the dictionary store, then replays WAL records after `checkpoint_lsn` onto heap
and index pages (redo-all). The CLOG is seeded from `clog.dat` and folded forward
with later `Commit`/`Abort` records, including subtransaction commit records; if
the snapshot is absent, it is rebuilt from retained WAL records.

```text
server startup
    |
    +-- open control store, dictionary store, heap page store, and data/wal.dat
    |
    +-- enable buffer stealing; load clog.dat when present
    |
    +-- read manifest.dat
    |       |
    |       +-- absent: fresh empty database
    |       +-- present: load checkpoint_lsn and the serialized catalog
    |
    +-- install table, hidden TOAST, secondary-index, and sequence schemas
    |       into storage
    |
    +-- replay WAL records with LSN > checkpoint_lsn (redo-all); use the CLOG
    |       seeded/folded at WAL open for transaction visibility
    |       page-LSN gating makes redo idempotent; torn or missing pages are
    |       zeroed so a FullPageImage / FullPageImageCompressed / HeapInit
    |       re-establishes them
    |
    +-- if replay changed state, run a checkpoint
    |
    +-- reseed TOAST value-id allocators, then switch storage from recovery mode
    |       to normal mode
    |
    +-- bind TCP listener
```

Recovery replays redo records onto heap and index pages and applies catalog,
schema, sequence, compression, TOAST, and dictionary records without appending
new WAL records. The storage-identity index is an on-disk B-tree recovered
through the same redo path, so there is no in-memory directory to rebuild.
Normal storage operations append WAL after startup switches to normal mode.

## Development

Common checks:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run a narrower package test while working:

```bash
cargo test -p saguarodb-planner
cargo test -p saguarodb-server
```

The main project documentation lives in:

```text
docs/specs/overview.md
docs/specs/rust-style.md
docs/specs/crates/*.md
```

If behavior and specs disagree, resolve the mismatch deliberately instead of
silently drifting the implementation away from the documented contracts.

## License

SaguaroDB is licensed under the GNU General Public License version 3 or later.
See [COPYING](COPYING) for the full license text.
