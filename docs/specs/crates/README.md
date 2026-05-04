# SaguaroDB Crate Specs

**Date:** 2026-05-03
**Status:** Draft

This directory decomposes the overview spec into crate-level contracts for v1 implementation. The goal is to preserve the architecture while allowing naive v1 implementations behind stable boundaries.

## Project Guidelines

- Rust style and implementation conventions: [../rust-style.md](../rust-style.md)

## Crates

| Crate | Spec | Responsibility |
|---|---|---|
| `common` | [common.md](common.md) | Shared IDs, values, rows, errors, execution envelopes, and cross-crate traits |
| `parser` | [parser.md](parser.md) | SQL text to SaguaroDB AST |
| `catalog` | [catalog.md](catalog.md) | Table metadata, stable IDs, schema snapshots |
| `planner` | [planner.md](planner.md) | Bind, logical plan, physical plan |
| `executor` | [executor.md](executor.md) | Volcano operators, expression evaluation, DML/DDL execution |
| `storage` | [storage.md](storage.md) | Page-backed table storage, row serialization, recovery operations |
| `buffer` | [buffer.md](buffer.md) | Page cache, RAII guards, dirty tracking, rollback, snapshot iteration |
| `wal` | [wal.md](wal.md) | Logical WAL, commit records, replay iterator |
| `snapshot` | [snapshot.md](snapshot.md) | Manifest, snapshot generation directories, checkpoint loading/writing |
| `protocol` | [protocol.md](protocol.md) | PostgreSQL simple query codec and connection state |
| `server` | [server.md](server.md) | Binary wiring, startup/recovery, Tokio listener, blocking query execution |

## Cross-Crate Rules

- Parser output may contain user-facing names. All phases after binding use IDs and slot indices.
- `common` is the only leaf crate. No crate may depend on `server`.
- Cargo package names use the `saguarodb-*` prefix, but internal `Cargo.toml` dependencies use short aliases such as `common`, `storage`, and `wal`.
- `storage` must not depend on `planner`; shared access types such as `KeyRange` live in `common`.
- Normal storage operations append WAL records. Recovery operations must not append WAL records.
- V1 never evicts dirty pages. Dirty pages become clean only after a successful manifest-based snapshot checkpoint.
- V1 uses logical WAL and full snapshots. Physical WAL and incremental flushing are future work behind existing traits.

## V1 Test Strategy

Each crate owns focused unit tests for its public contract. Cross-crate behavior is covered by integration tests at the server/workspace level:

- SQL pipeline: parse, bind, plan, explain.
- Execution: SELECT, INSERT, UPDATE, DELETE against in-memory storage.
- Durability: commit, rollback, snapshot, recovery replay.
- Protocol: startup, SSL rejection, simple query response shape.
