# Agent Instructions

These instructions apply to the whole repository. Direct user instructions take
precedence.

## Project Context

- SaguaroDB v1 is implemented as a Rust workspace with a PostgreSQL simple-query
  server, SQL parse/bind/plan/execute pipeline, page-backed storage, logical
  WAL, manifest snapshots, and crash recovery.
- The old task-by-task implementation plan is historical and is not a source of
  truth. Do not depend on `docs/superpowers/**`; those files are not project
  documentation in git and may be absent.

## Authoritative Documentation

- Treat `docs/specs/overview.md` as the system-level specification.
- Treat `docs/specs/crates/*.md` as the crate-level API and behavior contracts.
- Treat `docs/specs/rust-style.md` as the Rust style, testing, and durability
  convention guide.
- If code and specs disagree, stop and surface the mismatch before changing
  behavior. Do not silently update code or specs to paper over the conflict.
- Update the relevant spec in the same change when intentionally changing a
  public contract, SQL behavior, durable format, startup option, or crate
  responsibility.

## Repository Workflow

- Run commands from the repository root.
- Work from `develop` unless the user asks otherwise.
- Keep changes scoped to the requested behavior. Avoid unrelated refactors,
  formatting churn, and cleanup outside touched areas.
- Preserve user changes already present in the worktree. Do not revert files you
  did not intentionally edit.
- Keep `Cargo.lock` committed when any Cargo manifest changes.
- Keep root `Cargo.toml` workspace membership in sync when adding, removing, or
  renaming crates.
- Use leading-underscore parameter or binding names only for intentionally
  unused values. Do not accept `_name` and immediately shadow it as `name`;
  name used parameters directly.
- Runtime data belongs in ignored directories such as `data/` or `/tmp`, not in
  git.

## Workspace And Crate Boundaries

- Crates and responsibilities are documented in `docs/specs/crates/README.md`.
- Cargo package names use the `saguarodb-*` prefix. Internal dependencies should
  use short aliases such as `common`, `storage`, and `wal`.
- Keep dependency edges aligned with `docs/specs/overview.md`.
- `common` is the leaf crate for shared IDs, values, rows, errors, execution
  context, and cross-crate traits.
- `server` is the binary/root wiring crate. No library crate may depend on
  `server`.
- Do not let `parser` depend on `catalog`.
- Do not let `planner` depend on `storage`.
- Do not let `storage` depend on `planner`.
- `planner` may depend on `parser` for internal AST types.
- Normal storage operations append WAL records. Recovery operations must not
  append WAL records.

## SQL And Durability Rules

- Preserve the v1 SQL subset unless the specs are intentionally updated:
  `CREATE TABLE`, `DROP TABLE`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`,
  `INSERT ... VALUES`, `SELECT` with v1 clauses and joins, `UPDATE`, `DELETE`,
  `EXPLAIN`, and the maintenance command `VACUUM [table]` (non-relational: it does
  not bind/plan, takes the exclusive guard, and is rejected inside a transaction
  block).
- Unsupported parsed forms should be rejected by the binder or server with
  structured `common::DbError` values and accurate SQLSTATE codes.
- Do not introduce implicit casts. Type mismatches return
  `SqlState::DatatypeMismatch`, except `NULL` is valid where the target
  expression or column is nullable.
- Normalize unquoted SQL identifiers to lowercase. Quoted identifiers remain
  unsupported in v1 unless the specs change.
- Preserve autocommit semantics. The server owns statement guards, transaction
  ID allocation, WAL commit records, WAL flush, rollback before durable commit,
  cleanup after durable commit, and checkpoint triggering.
- Preserve fsync-sensitive ordering for WAL flush, snapshot writes, manifest
  swap, WAL checkpoint records, WAL truncation, and graceful shutdown.
- Be conservative with durable formats. WAL, manifest, snapshot, and page/row
  encodings need versioning/checksum behavior consistent with their specs.

## Testing And Verification

- Prefer focused tests in the crate that owns the behavior.
- Use server integration tests for cross-crate SQL, protocol, checkpoint, and
  recovery behavior.
- Run narrow package tests first for the crate you changed, then broaden as risk
  increases.
- Before handing off substantial changes, run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

- If a verification command cannot run, record the exact command and reason.
- Do not claim a fix is complete until relevant verification has run or the
  limitation is explicitly documented.

## Running The Server

- Start the server from the repository root with:

```bash
cargo run -p saguarodb-server --bin saguarodb -- --data-dir /tmp/saguarodb-dev --port 5433
```

- Defaults are `--data-dir ./data`, `--port 5433`,
  `--buffer-pool-frames 1024`, `--checkpoint-every-n-commits 100`,
  `--checkpoint-wal-bytes 67108864`, and `--shutdown-timeout-ms 30000`.
- TLS is off by default. Pass both `--tls-cert-file <PATH>` (PEM cert chain) and
  `--tls-key-file <PATH>` (PEM private key) to enable it; setting only one is an error.
- The server listens on `0.0.0.0:<port>` and runs in the foreground. Stop it with
  `Ctrl-C` or SIGTERM for graceful shutdown.
- Connect with `psql` using SSL disabled, for example:

```bash
psql "host=127.0.0.1 port=5433 user=saguarodb dbname=saguarodb sslmode=disable"
```
