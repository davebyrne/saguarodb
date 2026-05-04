# SaguaroDB

SaguaroDB is a SQL-compatible relational database written in Rust. It runs as a
standalone server, accepts client connections over the PostgreSQL simple-query
wire protocol, executes SQL through a parse/bind/plan/execute pipeline, and
stores data in page-oriented table snapshots with logical WAL recovery.

The current implementation is SaguaroDB v1: a compact, trait-boundary database
intended to keep the major subsystems clear while leaving room for future MVCC,
secondary indexes, physical WAL, and richer protocol support.

## What Works

- Standalone multi-client server using Tokio.
- PostgreSQL simple query protocol, usable from `psql` with SSL disabled.
- SQL support for `CREATE TABLE`, `DROP TABLE`, `INSERT ... VALUES`, `SELECT`,
  `UPDATE`, `DELETE`, and `EXPLAIN`.
- `SELECT` supports `WHERE`, inner/cross/left/right/full joins, `GROUP BY`,
  `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET`.
- Data types: `INTEGER` (`i64`), `TEXT`, `BOOLEAN`, and `NULL`.
- Autocommit statement execution.
- Rule-based planning with a primary-key access path and table scans.
- Page-backed storage with an in-memory primary-key directory.
- Logical WAL, full manifest snapshots, checkpointing, and crash recovery.

V1 deliberately does not implement authentication, SSL/TLS, prepared statements,
the PostgreSQL extended query protocol, multi-statement transactions, MVCC,
secondary indexes, replication, or a custom wire protocol.

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
                | catalog  |     | storage | --> |buffer| <-> |snapshot|
                | schemas  |     | tables  |     |pages |     |files  |
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
  -> protocol, parser, planner, executor, snapshot, storage, buffer, wal,
     catalog, common

executor -> planner, storage, catalog, common
planner  -> parser, catalog, common
storage  -> buffer, wal, common
snapshot -> buffer, common
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
  storage/   page-backed table storage and recovery operations
  buffer/    page cache, latches, dirty tracking, rollback, snapshot iteration
  wal/       logical write-ahead log, commit records, replay iterators
  snapshot/  manifest, snapshot generation directories, table/catalog files
  protocol/  PostgreSQL simple-query codec and connection state
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

The data directory contains one logical WAL file and full snapshot generations.
`manifest.dat` is the source of truth for the current snapshot.

```text
data/
  wal.dat
  manifest.dat
  manifest.dat.tmp
  snap_<generation>/
    catalog.dat
    table_<TableId>.tbl
```

At runtime, the buffer pool holds clean pages loaded from the current snapshot
plus dirty pages created by committed and in-flight statements. V1 does not
flush dirty pages individually. Dirty table pages reach disk only through a full
snapshot checkpoint.

```text
             normal operation

        committed SQL writes
                |
                v
        +-----------------+
        | logical WAL     |  fsynced on every commit
        | data/wal.dat    |
        +-----------------+
                |
                | records changes since last snapshot
                v
        +-----------------+
        | buffer pool     |  dirty pages stay in memory
        +-----------------+
                |
                | checkpoint writes complete snapshot
                v
        +-----------------+
        | snap_<gen>/     |  table files + catalog.dat
        +-----------------+
                |
                v
        +-----------------+
        | manifest.dat    |  current generation + checkpoint_lsn
        +-----------------+
```

## Checkpointing

Checkpoints are triggered after a configured number of committed statements, a
configured amount of WAL growth, or graceful shutdown.

```text
checkpoint
    |
    +-- take global write guard
    |
    +-- choose checkpoint_lsn from WAL high-water mark
    |
    +-- compose table pages
    |       clean pages from current snapshot
    |       overlaid with dirty pages from buffer pool
    |
    +-- write new snap_<generation>/
    |       catalog.dat
    |       table_<TableId>.tbl
    |
    +-- fsync snapshot files and directory
    |
    +-- write manifest.dat.tmp
    |
    +-- fsync manifest.dat.tmp
    |
    +-- rename manifest.dat.tmp -> manifest.dat
    |
    +-- fsync data directory
    |
    +-- mark buffer pages clean
    |
    +-- append WAL Checkpoint metadata record
    |
    +-- fsync WAL and truncate records before checkpoint_lsn
```

The previous snapshot is not deleted until the new manifest is durable. If the
server crashes mid-checkpoint, recovery uses whichever manifest survived and
cleans orphan snapshot directories on the next startup.

## Recovery

Startup always begins from the manifest snapshot and replays committed WAL
records after the manifest's `checkpoint_lsn`.

```text
server startup
    |
    +-- open snapshot manager
    |
    +-- open data/wal.dat
    |
    +-- read manifest.dat
    |       |
    |       +-- absent: fresh empty database
    |       +-- present: load snap_<generation>/
    |
    +-- load catalog.dat into catalog
    |
    +-- load table pages into buffer pool
    |
    +-- install schemas into storage
    |
    +-- rebuild in-memory primary-key directories from pages
    |
    +-- replay committed WAL records with LSN > checkpoint_lsn
    |
    +-- if replay changed state, run a checkpoint
    |
    +-- switch storage from recovery mode to normal mode
    |
    +-- bind TCP listener
```

Recovery operations update catalog, storage pages, and in-memory primary-key
directories without appending new WAL records. Normal storage operations append
WAL after startup switches to normal mode.

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
