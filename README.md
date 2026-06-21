# SaguaroDB

SaguaroDB is a SQL-compatible relational database written in Rust. It runs as a
standalone server, accepts client connections over the PostgreSQL simple-query
wire protocol, executes SQL through a parse/bind/plan/execute pipeline, and
stores data in page-oriented per-table files with write-ahead-log recovery.

The current implementation is SaguaroDB v1: a compact, trait-boundary database
intended to keep the major subsystems clear while leaving room for future MVCC
and richer SQL support.

## What Works

- Standalone multi-client server using Tokio.
- PostgreSQL simple query protocol, usable from `psql` with SSL disabled.
- SQL support for `CREATE TABLE`, `DROP TABLE`, `INSERT ... VALUES`, `SELECT`,
  `UPDATE`, `DELETE`, and `EXPLAIN`.
- `SELECT` supports `WHERE`, inner/cross/left/right/full joins, `GROUP BY`,
  `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET`.
- Data types: `INTEGER` (`i64`), `TEXT`, `BOOLEAN`, and `NULL`.
- Autocommit statement execution.
- Rule-based planning with primary-key, secondary-index, and table-scan access
  paths.
- Page-backed storage with an on-disk B-tree primary-key index.
- `CREATE [UNIQUE] INDEX` and `DROP INDEX` with a secondary-index access path.
- Query cancellation (PostgreSQL `CancelRequest` / `BackendKeyData`).
- TLS/SSL connections, prepared statements, and the PostgreSQL extended query
  protocol (`Parse`/`Bind`/`Describe`/`Execute`).
- Physiological redo WAL with full-page-image torn-page protection,
  in-place checkpointing, and crash recovery.

V1 deliberately does not implement authentication, multi-statement
transactions, MVCC, replication, or a custom wire protocol.

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
create table users (id integer primary key, name text, active boolean);
insert into users (id, name, active) values (1, 'Ada', true);
insert into users (id, name, active) values (2, 'Grace', false);
select id, name from users where active = true;
explain select name from users where id = 1;
```

## Server Options

The server accepts:

```text
--data-dir <PATH>                  default ./data
--port <PORT>                      default 5433
--buffer-pool-frames <N>           default 1024
--checkpoint-every-n-commits <N>   default 100
--checkpoint-wal-bytes <BYTES>     default 67108864
--shutdown-timeout-ms <MS>         default 30000
--tls-cert-file <PATH>             PEM cert chain; enables TLS (needs --tls-key-file)
--tls-key-file <PATH>              PEM private key; enables TLS (needs --tls-cert-file)
--help                             print usage and exit 0
```

For local development, prefer a data directory outside the repository or an
ignored directory:

```bash
cargo run -p saguarodb-server --bin saguarodb -- --data-dir /tmp/saguarodb-dev
```

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
                                   +-----+
                                   | WAL |
                                   +-----+
```

Crate dependency flow:

```text
server
  -> protocol, parser, planner, executor, control, storage, buffer, wal,
     catalog, common

executor -> planner, storage, catalog, common
planner  -> parser, catalog, common
storage  -> buffer, wal, common
control  -> common
protocol -> common
parser   -> common
buffer   -> common
wal      -> common
catalog  -> common

common is the leaf crate.
No library crate depends on server.
```

Workspace layout:

```text
crates/
  common/    shared IDs, values, rows, errors, contexts, traits
  parser/    SQL text to SaguaroDB AST
  catalog/   table metadata, stable IDs, schema snapshots
  planner/   binding, logical plans, physical plans, EXPLAIN formatting
  executor/  expression evaluation and volcano-style operators
  storage/   page-backed table storage, on-disk B-tree index, recovery
  buffer/    page cache, latches, dirty tracking, rollback, in-place flushing
  wal/       physiological redo write-ahead log, commit records, replay iterators
  control/   manifest.dat control record: redo boundary plus catalog snapshot
  protocol/  PostgreSQL wire codec (simple and extended query) and connection state
  server/    binary, startup/recovery, TCP listener, query orchestration
```

## Query Path

Reads and writes flow through the same SQL pipeline, but write statements run
under an exclusive statement guard and receive a transaction ID for WAL and
rollback tracking.

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
    |       +-- SELECT / EXPLAIN: read guard
    |       +-- DDL / DML:       write guard + txn_id
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
```

For successful writes, the server appends a WAL `Commit` record, fsyncs the WAL,
then performs in-memory commit cleanup. If a write fails before durable commit,
the server rolls back storage, buffer, and catalog state for that statement.

## Data Files

The data directory contains one write-ahead-log file, the control record, and
per-table heap and index files. `manifest.dat` is the control record: the redo
boundary (`checkpoint_lsn`), the live table ids, and the serialized catalog,
written atomically as a single CRC-checked envelope. Each table persists in
place to its own files under `heap/`: the row heap at `<TableId>.heap`, the
primary-key B-tree at `<TableId>.idx`, and any secondary index at
`<IndexId>.sidx`.

```text
data/
  wal.dat
  manifest.dat
  manifest.dat.tmp
  heap/
    <TableId>.heap
    <TableId>.idx
    <IndexId>.sidx
```

At runtime, the buffer pool holds clean pages loaded from the heap and index
files plus dirty pages created by committed and in-flight statements. Dirty
pages are flushed in place to their home files once their dirtying transaction
has committed and its page-LSN is WAL-durable.

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
        | heap/ files     |  per-table heap + index pages, written in place
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
    +-- flush committed dirty pages in place to heap/index files
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
    +-- append WAL Checkpoint metadata record and fsync
    |
    +-- truncate WAL records before checkpoint_lsn
    |
    +-- mark buffer pages clean
```

The control record is the commit point: it is written only after the heap and
index pages it describes are durable, and the WAL prefix is truncated only after
the control record is durable. If the server crashes mid-checkpoint, recovery
falls back to the previous redo boundary, where this cycle's full-page images
repair any torn page writes.

## Recovery

Startup reads the control record for the redo boundary and catalog, then replays
committed WAL records after `checkpoint_lsn` onto the heap and index pages.

```text
server startup
    |
    +-- open control store, heap page store, and data/wal.dat
    |
    +-- read manifest.dat
    |       |
    |       +-- absent: fresh empty database
    |       +-- present: load checkpoint_lsn and the serialized catalog
    |
    +-- install table and secondary-index schemas into storage
    |
    +-- replay committed WAL records with LSN > checkpoint_lsn
    |       page-LSN gating makes redo idempotent; torn or missing pages are
    |       zeroed so a FullPageImage / HeapInit re-establishes them
    |
    +-- if replay changed state, run a checkpoint
    |
    +-- switch storage from recovery mode to normal mode
    |
    +-- bind TCP listener
```

Recovery replays redo records onto heap and index pages, and applies catalog and
schema records, without appending new WAL records. The primary-key index is an
on-disk B-tree recovered through the same redo path, so there is no in-memory
directory to rebuild. Normal storage operations append WAL after startup switches
to normal mode.

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
