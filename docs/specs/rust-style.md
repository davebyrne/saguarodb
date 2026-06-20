# SaguaroDB Rust Style Guidelines

**Date:** 2026-05-03
**Status:** Draft

This document defines Rust style and engineering conventions for SaguaroDB. It complements the crate specs in `docs/specs/crates/` and applies to all Rust workspace crates unless a crate spec explicitly says otherwise.

## Goals

- Keep crate boundaries clear and public APIs stable.
- Prefer simple, readable Rust over clever lifetime or generic machinery.
- Make failure modes explicit through `common::DbError`.
- Keep durable formats versioned, checksummed, and recoverable.
- Make tests deterministic and focused on observable behavior.

## Toolchain and Formatting

- Use Rust 2024 edition for all crates.
- Add `rust-toolchain.toml` in Task 1 and pin to stable Rust with `rustfmt` and `clippy` components.
- Commit `Cargo.lock`; this workspace includes the `saguarodb-server` binary.
- Use default `rustfmt` formatting. Do not add `rustfmt.toml` unless a concrete project need appears.
- Run `cargo fmt --all` before every checkpoint commit.
- Run narrow package tests first, then broader workspace tests at vertical-slice checkpoints.

Recommended checkpoint commands:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

If clippy warns on code that is clearer as written, add the narrowest possible `#[allow(...)]` with a short reason. Do not add crate-wide allows for convenience.

## Unsafe, Panics, and Assertions

- Do not use `unsafe` in v1 implementation. If a future change needs `unsafe`, isolate it in a small module, document the safety invariants, and add targeted tests.
- Library crates must not panic for expected runtime errors. Return `common::Result<T>`.
- Avoid `unwrap()` and `expect()` in production code. Use structured error conversion instead.
- `panic!`, `unwrap()`, and `expect()` are acceptable in tests.
- `debug_assert!` is acceptable for internal invariants that should be impossible if earlier validation worked.
- Use `unreachable!()` only for genuinely impossible states, not as a substitute for error handling.

## Errors

- All public fallible APIs in SaguaroDB crates return `common::Result<T>`.
- Prefer error constructors such as `DbError::parse(...)`, `DbError::plan(...)`, `DbError::storage(...)`, and `DbError::internal(...)` over ad hoc struct literals.
- Preserve SQLSTATE accuracy. Examples:
  - syntax and unsupported parser syntax: `SqlState::SyntaxError`
  - unknown table: `SqlState::UndefinedTable`
  - unknown or ambiguous column: `SqlState::UndefinedColumn`
  - type mismatch: `SqlState::DatatypeMismatch`
  - division by zero: `SqlState::DivisionByZero`
  - integer overflow: `SqlState::NumericValueOutOfRange`
  - null constraint violation: `SqlState::NotNullViolation`
  - duplicate table: `SqlState::DuplicateTable`
  - duplicate primary key: `SqlState::UniqueViolation`
- Map low-level IO errors to `ErrorKind::Io` and `SqlState::IoError` at the boundary where context is available.
- Include concise, user-facing `message` text. Use `detail` and `hint` only when they add actionable context.

## Public APIs and Visibility

- Keep modules private by default. Expose crate contracts through curated `pub use` exports in each crate's `lib.rs`.
- Public types and traits that cross crate boundaries need concise rustdoc comments.
- Do not expose implementation-only structs just to make tests easier. Use test-only modules, dev-dependencies, or `test-support` features when shared helpers are needed.
- Keep public APIs aligned with the crate specs. Internal algorithms may be simple if the trait contract remains stable.
- Avoid large generic APIs unless they remove real duplication. Prefer concrete types and trait objects at subsystem boundaries.

## Ownership and Data Shapes

- Use owned `String`, `Vec<T>`, `Row`, `TableSchema`, and plan nodes across crate boundaries. Avoid lifetime-heavy public APIs in v1.
- It is acceptable to clone schema and row metadata at crate boundaries for clarity.
- Avoid cloning page data or row values inside hot loops unless it keeps ownership clear and tests remain fast enough.
- Use `&str` for lookup inputs, such as catalog name lookup, when the callee does not need ownership.
- Use the spec-defined ID aliases (`TableId`, `ColumnId`, `BindingId`, `FileId`, `PageNum`, `Lsn`) rather than introducing new ID wrappers in v1.
- Normalize unquoted SQL identifiers to lowercase before catalog lookup or catalog creation.

## Concurrency and Async Boundaries

- Parser, planner, executor, storage, buffer, WAL, control, and catalog crates are synchronous.
- Tokio belongs at the server and connection boundary.
- Use `std::sync::{Arc, Mutex, RwLock}` in core crates by default.
- Use `parking_lot` where SaguaroDB needs owned lock guards, such as `ReadGuard`, `WriteGuard`, `PageReadGuard`, and `PageWriteGuard`. `std::sync` guard lifetimes are not suitable for these object-safe owned guard types.
- Use Tokio synchronization primitives only where async code would otherwise block the runtime.
- Server shared components should be held as `Arc<dyn Trait + Send + Sync>` when they are accessed across owned services or tasks.
- Prefer references to trait objects for short-lived API calls, such as `&dyn CatalogManager`.
- Do not hold locks across blocking IO or long-running calls unless the relevant spec requires statement-level serialization.
- Keep lock acquisition order simple and documented where multiple locks are unavoidable.

## Serialization and Durable Formats

- Derive `Serialize` and `Deserialize` only for data that intentionally crosses WAL, control-file, or test fixture boundaries.
- Durable files must use explicit versioned envelopes where compatibility matters.
- WAL and manifest data must be checksummed according to their specs.
- Do not serialize arbitrary public structs directly as a durable format unless the codec wraps them in a versioned record.
- Be conservative when changing serialized structs. Add fields in a way that existing control or WAL records can be rejected clearly or migrated deliberately.
- Keep WAL payload encoding behind the WAL codec and storage row encoding behind the storage codec.

## SQL Semantics in Rust Code

- Do not implement implicit casts. Type mismatches return `SqlState::DatatypeMismatch`.
- `NULL` may be accepted where the target column or expression is nullable.
- SQL three-valued logic belongs in executor expression evaluation, not in `Value` ordering.
- `Value::Ord` is storage key ordering only: `Null < Boolean < Integer < Text`.
- Composite primary key APIs stay in place, but v1 implementation may assume a single-column primary key unless a task explicitly expands scope.

## Modules and File Size

- Split files by responsibility, matching the file layout in the implementation plan.
- Keep public contract types near the crate root or in clearly named modules.
- Keep codec, recovery, and test-support code separate from runtime operator logic.
- Avoid unrelated refactors while implementing a task branch.
- When a module grows large enough that tests and implementation are hard to navigate, split by behavior rather than by type category.

## Imports, Naming, and Comments

- Let `rustfmt` organize whitespace; keep import lists straightforward.
- Prefer importing crate-local public types from the crate root when that matches the public API.
- Use clear domain names: `catalog`, `schema`, `row`, `key`, `txn_id`, `checkpoint_lsn`.
- Avoid abbreviations except established database terms such as `LSN`, `DDL`, `DML`, and `WAL`.
- Use leading-underscore names only for values that are intentionally unused. If a function
  parameter is read, name it directly; do not accept `_name` and immediately rebind it as
  `name`.
- Comments should explain non-obvious invariants, recovery ordering, lock ordering, or wire-format details.
- Do not comment obvious assignments or restate type names.

## Tests

- Write focused unit tests in the crate that owns the behavior.
- Use integration tests for cross-crate behavior and server/protocol flows.
- Tests should assert observable behavior and important invariants, not incidental implementation detail.
- Test names should describe behavior, for example `rollback_restores_original_before_image_even_after_multiple_writes`.
- Use `tempfile` for filesystem tests.
- Use production codecs in durability tests; do not duplicate serialization logic in test helpers.
- Prefer deterministic data, deterministic ordering, and explicit assertions.
- Test helpers may live in `#[cfg(test)]` modules for same-crate tests.
- For integration tests or downstream crate tests, expose helpers through dev-dependencies or an explicit `test-support` feature.
- Do not use sleeps for synchronization unless testing time behavior directly. Prefer explicit signals or blocking joins.

## Cargo and Dependencies

- Define shared dependency versions in root `[workspace.dependencies]`.
- Crate manifests should use workspace dependencies where possible.
- Workspace package names use the project prefix, such as `saguarodb-common`, `saguarodb-storage`, and `saguarodb-server`.
- Internal crate dependencies use short aliases in `Cargo.toml`, not the full package-derived Rust crate name. For example:

```toml
[dependencies]
common = { package = "saguarodb-common", path = "../common" }
storage = { package = "saguarodb-storage", path = "../storage" }
```

- Internal Rust code imports those aliases:

```rust
use common::{DbError, Result};
use storage::StorageEngine;
```

- Avoid writing `saguarodb_common::DbError` in internal code unless there is a specific reason to bypass the alias.
- Reserve a future `saguarodb::common::DbError` facade for external consumers if SaguaroDB later exposes a public library API. Internal crates should not depend on that facade.
- Keep dependency edges aligned with `docs/specs/overview.md` and crate specs.
- Do not add dependencies for small helpers that are easy to express with the standard library.
- Dependencies used only in tests belong in `[dev-dependencies]`.
- Feature flags should be explicit and named by behavior, such as `test-support`.
- Do not enable broad dependency features unless the crate needs them.

## Checkpoint Discipline

- Each implementation task happens on its own branch according to the implementation plan.
- Keep commits focused on one task or accepted subtask.
- Before asking for review, run the checkpoint commands required by the task.
- If a verification command cannot run, record the exact reason in the handoff.
- Do not commit ignored planning files unless explicitly requested.
