# SaguaroDB Rust Style Guidelines

**Date:** 2026-07-04
**Status:** Living style and testing contract

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

- Do not use `unsafe`. Every workspace package, including any newly added
  package, must inherit the workspace-level `unsafe_code = "forbid"` lint via
  `[lints] workspace = true`, so this is compiler-enforced for repository
  libraries, binaries, and tests. A future need for `unsafe` requires an
  explicit policy and lint change; isolate it in a small module, document the
  safety invariants, and add targeted tests.
- Production code must not intentionally panic. Return `common::Result<T>` for
  expected runtime failures and unexpected invariant violations alike.
- Do not use `unwrap()`, `expect()`, `panic!`, `unreachable!`, `todo!`,
  `unimplemented!`, `assert!`, `assert_eq!`, `assert_ne!`, or `debug_assert*`
  in production code. Replace invariant assertions with a structured error at
  the nearest fallible boundary.
- Every library and binary crate root must retain the production-only Clippy
  denial for `unwrap_used`, `expect_used`, `panic`, `unreachable`, `todo`,
  `unimplemented`, and `disallowed_macros`. `clippy.toml` disallows assertion
  and debug-assertion macros for those production targets. The `not(test)` gate
  and workspace-level `disallowed_macros = "allow"` default permit normal
  assertion ergonomics in unit and integration tests; each production crate
  root overrides that default with `deny`.
- Fixed-width decoders validate length before conversion and propagate failed
  `try_into`; they never use `try_into().unwrap()` or `try_into().expect()`.
- Infallible trait methods and `Drop` implementations must be total. Recover
  explicitly where safe or defer/report failure at a later fallible boundary.
- Mutex poisoning is handled deliberately by returning a structured error or by
  explicitly recovering the guard where continuation is the documented policy.
- Runtime-controlled indexing and slicing requires checked access or a prior
  bounds/length validation; invariant failure still becomes an error rather than
  an assertion.
- Panic helpers and assertions are acceptable only in `#[cfg(test)]` code and
  integration-test targets that cannot enter a production execution path.

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

## Checked Boundaries, Arithmetic, and Allocation

- Decode untrusted wire data and durable bytes through
  `common::CheckedSliceReader` or a format-specific wrapper around it. Do not
  maintain an ad hoc mutable offset and combine `offset + length` with direct
  indexing or slicing. Map `SliceReadError` to the format's structured error at
  that boundary.
- Validate raw lengths, counts, offsets, and identifiers once, then carry a
  private validated type when the invariant is reused. For example, protocol
  frame lengths are represented by a private type only after minimum, maximum,
  sign, and conversion checks. Do not implement infallible arithmetic traits or
  `Index` for a wrapper when the operation can fail; expose fallible methods.
- Use `checked_add`, `checked_sub`, and `checked_mul` for arithmetic involving
  input-derived lengths, counts, offsets, page positions, and allocation sizes.
  Use saturating arithmetic only when saturation is the domain's documented
  behavior, not to conceal invalid input or a broken invariant.
- Convert integer widths with `From` for widening and `TryFrom` for narrowing or
  signed-to-unsigned conversion. A failed conversion must be mapped to the
  boundary's error; do not use `as` to truncate or reinterpret runtime values.
- Apply a format-defined hard limit before allocating from an external count.
  Reserve with `try_reserve` or `try_reserve_exact` and propagate allocation
  failure instead of relying on `Vec::with_capacity` for input-controlled
  capacity.
- New modules that decode wire or durable formats must enable these
  production-only lints, and existing boundary modules should adopt them when
  touched:

```rust
#![cfg_attr(
    not(test),
    deny(
        clippy::arithmetic_side_effects,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::indexing_slicing
    )
)]
```

  A targeted `allow` requires a nearby explanation of the fixed-layout or
  otherwise proven invariant. The workspace denies `clippy::unused_io_amount`;
  callers must handle partial reads and writes with `read_exact`, `write_all`,
  or an explicit retry/partial-progress loop.

## Public APIs and Visibility

- Keep modules private by default. Expose crate contracts through curated `pub use` exports in each crate's `lib.rs`.
- Public types and traits that cross crate boundaries need concise rustdoc comments.
- Do not expose implementation-only structs just to make tests easier. Use test-only modules, dev-dependencies, or `test-support` features when shared helpers are needed.
- Keep public APIs aligned with the crate specs. Internal algorithms may be simple if the trait contract remains stable.
- Avoid large generic APIs unless they remove real duplication. Prefer concrete types and trait objects at subsystem boundaries.

## Ownership and Data Shapes

- Use owned `String`, `Vec<T>`, `Row`, `TableSchema`, and plan nodes across crate boundaries. Avoid lifetime-heavy public APIs.
- It is acceptable to clone schema and row metadata at crate boundaries for clarity.
- Avoid cloning page data or row values inside hot loops unless it keeps ownership clear and tests remain fast enough.
- Use `&str` for lookup inputs, such as catalog name lookup, when the callee does not need ownership.
- Use the spec-defined ID aliases (`TableId`, `ColumnId`, `BindingId`, `FileId`, `PageNum`, `Lsn`) rather than introducing new ID wrappers.
- Normalize unquoted SQL identifiers to lowercase before catalog lookup or catalog creation.

## Concurrency and Async Boundaries

- Parser, planner, executor, storage, buffer, WAL, control, and catalog crates are synchronous.
- Tokio belongs at the server and connection boundary.
- Use `std::sync::{Arc, Mutex, RwLock}` in core crates by default.
- Use `parking_lot` where SaguaroDB needs owned lock guards, such as `WriteGuard`, `CheckpointGuard`, `PageReadGuard`, and `PageWriteGuard`. `std::sync` guard lifetimes are not suitable for these object-safe owned guard types.
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
- `common::Value` and `common::DataType` variant order is a durable contract: `Value`'s derived `Ord` is the on-disk B-tree key ordering (the index compares decoded `Key(Vec<Value>)` values directly). Append new variants at the end of these enums; never insert or reorder mid-enum without deliberately revisiting and migrating the key ordering/encoding.
- Keep WAL payload encoding behind the WAL codec and storage row encoding behind the storage codec.

## SQL Semantics in Rust Code

- Do not implement implicit casts. Type mismatches return `SqlState::DatatypeMismatch`.
- Foreign-key compatibility is declared-type identity: corresponding columns
  have equal `DataType`, concrete `ColumnDef::wire_type()`, and length/type
  modifier metadata. This is the deliberate case where persisted PostgreSQL
  type identity participates in semantic validation.
- `NULL` may be accepted where the target column or expression is nullable.
- SQL three-valued logic belongs in executor expression evaluation, not in `Value` ordering.
- `Value::Ord` is storage key ordering only, derived from `common::Value`'s declaration order: `Null < Boolean < Integer < Float < Real < Numeric < Text < Date < Timestamp < Time < TimestampTz < Interval < Bytes < Uuid < Array`, with natural ordering inside each variant. `SqlArray` orders by element type, row-major elements, cardinality, and dimensional metadata.
- Composite (multi-column) primary keys are supported end to end: the catalog records the ordered key column list, the storage key encoding (`Key(Vec<Value>)`) covers all key columns, and a leading-column equality uses the prefix-matching primary-key range scan.

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
