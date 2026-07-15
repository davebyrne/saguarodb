# Foreign Keys

**Date:** 2026-07-13
**Status:** Living feature contract

## Scope

SaguaroDB supports immediately enforced foreign keys declared by `CREATE TABLE`
in column- or table-level form. Source and target lists are non-empty, ordered,
equal-length, and contain no duplicates. Omitting the target list selects the
referenced table's primary key. An explicit list must exactly match, in order, a
declared primary-key or `UNIQUE` constraint. A standalone unique index is not an
eligible referenced key. Paired columns must have identical `DataType`, concrete
`PgType`, and declared length/type-modifier metadata; nullability may differ.

An omitted name becomes `<child>_<source-columns-joined-by-underscore>_fkey`;
collisions receive the smallest positive numeric suffix. Constraint names are
table-local. Identifiers follow the ordinary unquoted lowercase normalization.

Only implicit `MATCH SIMPLE` is supported: if any source value is `NULL`, the row
passes without a parent probe. `NO ACTION` is the default. `NO ACTION` and
`RESTRICT` are immediate per-row checks with identical enforcement. `CASCADE`,
`SET NULL`, `SET DEFAULT`, explicit `MATCH`, `DEFERRABLE`, `NOT VALID`, and other
constraint characteristics return `0A000`. No child index is created.

Standalone `ALTER TABLE <child> ADD [CONSTRAINT name] FOREIGN KEY (...) REFERENCES
<parent> [(...)] [ON UPDATE ...] [ON DELETE ...]` and `ALTER TABLE <child> DROP
CONSTRAINT [IF EXISTS] name [RESTRICT]` are supported. `CASCADE` is rejected.
Generic DROP resolves the table-local name at execution and routes the primary-key
name through the existing primary-key maintenance path; otherwise it removes only
the named foreign key. Both forms are standalone maintenance and return `0A000`
inside an explicit transaction block.

## Binding and creation

The parser stores `ParsedForeignKey` entries in declaration order, including
interleaved column- and table-level forms. The binder
resolves source columns against the proposed table. Existing parents become
stable table/column IDs; self references retain proposed target names until the
new table ID exists. Binding validates the eligible referenced constraint and
exact declared types. Unqualified targets follow `search_path`; the proposed
table is considered for self-reference only at its own schema position. Missing
tables/columns return `42P01`/`42703`, incompatible
types return `42804`, and an ineligible referenced key returns `42830`.

`CREATE TABLE IF NOT EXISTS` validates FK definitions, including generated and
explicit constraint-name collisions and allocator capacity, before deciding that
an existing table makes the statement a no-op. Existing parents are prepared-plan
schema identities and are held with `AccessShare` while CREATE publishes. The
table and PK/UNIQUE indexes are installed first; then the complete resolved FK
batch is attached atomically, and `UpdateTableSchema` persists the final schema
with the current index list. Pre-commit errors roll back the table, indexes,
TOAST relation, owned sequences, and storage metadata through transactional DDL.
CREATE remains transaction- and savepoint-aware.

Standalone ADD resolves both relations again after taking `AccessExclusive` on
the child and `Share` on the parent. It constructs the proposed FK-bearing schema
under the catalog publication gate, validates every existing child row through
the shared executor enforcement service, and persists the complete schema and
current index list with `UpdateTableSchema`. DROP takes `AccessExclusive` on the
child and `AccessShare` on the parent, resolves the name after locking, preserves
the monotonic allocator, and uses the same durable schema record. Pre-commit
validation, cancellation, WAL, or storage failures restore catalog/storage state.
`IF EXISTS` suppresses only a genuinely absent name; a matching declared UNIQUE
constraint is recognized and returns `0A000` because UNIQUE DROP is not supported.
Prepared forms record both child and parent schema identities when the parent is
known at prepare time and revalidate them only after the same maintenance xid owns
the converged schema/name/relation lock set and the publication gate is held.

## Enforcement

The shared executor service enforces installed FKs for INSERT, UPDATE, DELETE,
COPY FROM, and effective upsert branches. Local row constraints run before
outgoing probes. A self-referencing resulting row may satisfy its own key. There
is no deferred statement queue, so a forward reference to a later row in a
multi-row statement is not specially accepted.

Parent probes use current liveness, wait for in-progress creators, retain
`KeyShare` on the actual row, and recheck update/HOT chains. Dependent probes use
an exact existing child index or heap scan, wait/restart around in-progress child
changes, and exclude the current identity for self-references. Read Committed
accepts the current committed result after waiting; Repeatable Read and
Serializable return `40001` when it lies outside their retained snapshot. For a
dependent probe, that includes a committed post-snapshot child update or delete
that makes the previously matching row stop referencing the parent.

Violations return `23503` with PostgreSQL-style child and parent messages.
Related parent/child relations are discovered during lock convergence and held
with `AccessShare`; the DML target retains `RowExclusive`. PK probes record an
exact tuple SIREAD, secondary-UNIQUE probes a conservative parent relation
SIREAD, and dependent scans a child relation SIREAD.

## Durability and dependencies

`TableSchema.foreign_keys` and `next_foreign_key_id` are durable in catalog v3.
Each constraint durably stores the exact declared
PK/UNIQUE constraint-index ID selected at attachment, so duplicate eligible keys
cannot change its identity and that index cannot be dropped while referenced.
Older catalog formats are rejected rather than normalized. IDs are monotonic
`u16` values `0..=4095`; `4096` means exhausted and
dropped IDs are never reused. Recovery installs the complete schema from
committed `UpdateTableSchema`.

## Catalog introspection

Each foreign key has a deterministic virtual OID derived from its child table ID
and monotonic foreign-key ID. `pg_constraint` exposes it with `contype = 'f'`,
the child and parent relation OIDs, the referenced declared PK/UNIQUE constraint
index OID, ordered child/parent attnum arrays, `MATCH SIMPLE`, immediate validated
flags, and `a`/`r` action codes for `NO ACTION`/`RESTRICT`. Unsupported operator
arrays remain `NULL`.

`pg_depend` records the foreign key's dependencies on its child table and source
columns, parent table and referenced columns, and referenced constraint index.
`pg_get_constraintdef` resolves current relation and column names, so table and
column renames preserve the OID while changing rendered text. It omits default
`NO ACTION` clauses and emits explicit `ON UPDATE RESTRICT` and `ON DELETE
RESTRICT` clauses. Foreign keys add no new `information_schema` views in v1.

DROP COLUMN and non-no-op ALTER COLUMN TYPE reject source/referenced columns with
`2BP01`. A referenced PK cannot be changed/dropped. DROP TABLE and TRUNCATE require
every surviving incoming child in the target set; self references, cycles, and
dependencies wholly inside the set are allowed. Component details live in the
catalog, storage, executor, server, recovery, SSI, and table-lock specifications.
