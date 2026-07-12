# SaguaroDB Crate Specs

**Date:** 2026-05-03
**Status:** Living crate contract index

This directory decomposes the overview spec into crate-level contracts for the implementation. The goal is to preserve the architecture while allowing naive implementations behind stable boundaries.

## Project Guidelines

- Rust style and implementation conventions: [../rust-style.md](../rust-style.md)

## Crates

| Crate | Spec | Responsibility |
|---|---|---|
| `common` | [common.md](common.md) | Shared IDs, values, rows, errors, execution envelopes, cross-crate traits, and the scalar function registry |
| `spill` | [spill.md](spill.md) | Query-local memory accounting, ephemeral row tapes/codecs, and stable external sorting |
| `compress` | [compress.md](compress.md) | Compression codecs, at-rest page envelope, dictionary training/store, shared `CompressionRegistry` |
| `parser` | [parser.md](parser.md) | SQL text to SaguaroDB AST |
| `catalog` | [catalog.md](catalog.md) | Table metadata, stable relation IDs, schema snapshots |
| `planner` | [planner.md](planner.md) | Bind, logical plan, physical plan |
| `executor` | [executor.md](executor.md) | Volcano operators, expression evaluation, DML/DDL execution |
| `storage` | [storage.md](storage.md) | Page-backed table storage, primary-key B-tree index, row serialization, recovery operations |
| `buffer` | [buffer.md](buffer.md) | Page cache, RAII guards, dirty tracking, rollback, in-place page flushing |
| `wal` | [wal.md](wal.md) | Physiological redo WAL, commit/checkpoint records, replay iterator |
| `control` | [control.md](control.md) | Durable control record (checkpoint commit point): redo boundary, table ids, catalog |
| `protocol` | [protocol.md](protocol.md) | PostgreSQL wire codec and connection state for startup, cancellation, simple query, extended query, and COPY messages |
| `server` | [server.md](server.md) | Binary wiring, startup/recovery, Tokio listener, blocking query execution |

## Cross-Crate Rules

- Parser output may contain user-facing names. All phases after binding use IDs and slot indices.
- `common` and `compress` are leaf crates (`compress` depends on `common` only). No crate may depend on `server`.
- `spill` depends only on `common` and `tempfile`; it must not depend on executor, planner, storage, or server so executor and future storage/index builds can reuse it.
- `compress` is consumed by `storage` (at-rest page envelopes, WAL full-page-image compression) and `server` (constructs and shares the `CompressionRegistry`/`DictStore`); `wal` does not depend on `compress` — its compression-related record types carry plain codec/dict-id fields and bytes (see `docs/specs/compression.md`, `wal.md`).
- Cargo package names use the `saguarodb-*` prefix, but internal `Cargo.toml` dependencies use short aliases such as `common`, `storage`, and `wal`.
- `storage` must not depend on `planner`; shared access types such as `KeyRange` live in `common`.
- Normal storage operations append WAL records. Recovery operations must not append WAL records.
- Eviction can steal any WAL-durable dirty page (flush, then evict) once stealing is enabled (the server enables it at startup, before redo); checkpoint also flushes dirty pages in place to the heap. The CLOG hides uncommitted or aborted versions that reach disk.
- SaguaroDB uses a physiological redo WAL with per-page LSNs, in-place heap files, and eviction-flush-on-steal, with PostgreSQL-style in-heap MVCC layered on top (snapshot isolation, concurrent readers and writers, VACUUM; see `../mvcc.md`).

## Test Strategy

Each crate owns focused unit tests for its public contract. Cross-crate behavior is covered by integration tests at the server/workspace level:

- SQL pipeline: parse, bind, plan, explain.
- Execution: SELECT, INSERT, UPDATE, DELETE against in-memory storage.
- Durability: commit, rollback, checkpoint, recovery replay.
- Protocol: startup, SSL/GSS negotiation, cancellation, simple-query response
  shape, extended-query response shape, portal suspension, and COPY sub-protocol
  messages.
- Virtual system catalogs: `pg_catalog`/`information_schema` driver query
  shapes, `RowDescription` wire metadata, shadowing rules, read-only error
  paths, `pg_settings`, and live `pg_stat_activity` state.
