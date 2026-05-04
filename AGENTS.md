# Agent Instructions

These instructions apply to the whole repository. Direct user instructions take
precedence.

## Project Context

- The project is SaguaroDB, even though the checkout directory may be named
  `clementedb`.
- Implement SaguaroDB v1 according to
  `docs/superpowers/plans/2026-05-03-saguarodb-v1-implementation.md`.
- Treat `docs/specs/overview.md` and `docs/specs/crates/*.md` as authoritative
  source specs. If the plan and specs disagree, stop and resolve the mismatch
  before implementing.

## Implementation Workflow

- Run commands from the repository root.
- When implementing the v1 plan, use `superpowers:subagent-driven-development`
  or `superpowers:executing-plans` and execute the plan task by task.
- Create one branch per plan task, starting from the latest `develop`, using a
  name such as `task-01-common` or `task-09-page-backed-storage`.
- Use a dedicated fresh agent context per task when available, to keep task
  context bounded.
- Use each task's listed checkpoint commands and commit message.
- Review the branch with a fresh reviewer context that reads this file, the
  implementation plan, relevant specs, and the branch diff.
- The review should look for correctness bugs, architectural drift, dependency
  boundary violations, maintenance problems, missing tests, and unnecessary
  complexity.
- Fix all non-pedantic review findings, rerun verification, and repeat review
  until there are no blocking findings.
- Document any accepted residual minor issues before review acceptance.
- After a task branch is reviewed and accepted, merge it into `develop` before
  starting the next task branch.

## Rust Workspace Rules

- Keep public APIs aligned with the crate specs.
- When a task creates a crate, add it to root `Cargo.toml` workspace members
  before running that task's first Cargo command.
- Include `Cargo.toml` in checkpoints when workspace membership changes.
- Include `Cargo.lock` whenever any Cargo manifest changes.
- Run `cargo fmt --all` before each checkpoint.
- Run the narrow package test first, then broader workspace tests after each
  vertical slice.
- `cargo clippy --workspace --all-targets -- -D warnings` is recommended before
  review or merge.

## Dependency Boundaries

- Do not let `parser` depend on `catalog`.
- Do not let `planner` depend on `storage`.
- Do not let `storage` depend on `planner`.
- Do not let any library crate depend on `server`.
- `planner` may depend on `parser` for internal AST types.

## Durability And SQL Semantics

- Commit `rust-toolchain.toml` in Task 1.
- Commit `Cargo.lock`; this workspace includes the `saguarodb-server` binary.
- Implement fsync behavior from the start, including WAL flush, snapshot commit,
  manifest swap, and checkpoint paths.
- Do not implement implicit casts. Type mismatches return
  `SqlState::DatatypeMismatch`, except `NULL` is valid where the target
  expression or column is nullable.
- Normalize unquoted SQL identifiers to lowercase. Reject quoted identifiers in
  v1 with `ErrorKind::Parse` and `SqlState::SyntaxError`.
