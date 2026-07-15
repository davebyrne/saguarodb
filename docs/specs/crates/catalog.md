# `catalog` Crate Specification

**Date:** 2026-05-03
**Status:** Living crate contract

## Purpose

`catalog` owns schema metadata, stable table/column IDs, name-to-ID resolution, generic catalog snapshot diffs, and atomic change-set application. Its persisted form is included in the control record and updated by replaying committed `CatalogChange` WAL records after the checkpoint.

The catalog's internal lock protects data-structure consistency. Separately, the
server wraps SQL binding/name lookup and system-catalog capture in the shared
catalog publication gate. DDL stages object replacements and tombstones in a
transaction-local `CatalogOverlay`; its own binding sees that overlay while other
sessions continue to see the public catalog. After object locking, DDL takes the
exclusive side briefly for revalidation/materialization. Top-level commit takes
it again to publish the overlay atomically after the Commit record is durable;
rollback discards the overlay. Startup/recovery precedes user access and does not
need the gate.

`CatalogOverlay` stores a journal of generic, versioned change sets rather than
per-kind delta maps. Materialization starts from the current live catalog and
applies each journal entry with exact `before` matching, so unrelated commits
remain visible while conflicting object changes fail. Savepoints capture the
journal position and allocator state. Object/storage allocator reservations
advance live high-water immediately and are never rewound; objects remain private
until `publish` validates and installs the materialized snapshot under the gate.

## Depends On

- `common`

## Data Model

```rust
pub struct Catalog {
    schemas_by_name: HashMap<String, SchemaId>,
    schemas_by_id: HashMap<SchemaId, NamespaceSchema>,
    next_schema_id: SchemaId,
    tables_by_name: HashMap<String, TableId>,
    tables_by_id: HashMap<TableId, TableSchema>,
    next_table_id: TableId,
    views_by_name: HashMap<String, TableId>,
    views_by_id: HashMap<TableId, ViewSchema>,
    indexes_by_name: HashMap<String, IndexId>,
    indexes_by_id: HashMap<IndexId, IndexSchema>,
    next_index_id: IndexId,
    sequences_by_name: HashMap<String, SequenceId>,
    sequences_by_id: HashMap<SequenceId, SequenceSchema>,
    next_sequence_id: SequenceId,
    // Dictionary-id allocator for trained compression dictionaries (`docs/specs/compression.md` §7).
    next_dictionary_id: u32,
    // Physical relation-generation ids for heaps and indexes.
    next_storage_id: FileId,
}

pub struct CatalogSnapshot {
    pub schemas_by_name: HashMap<String, SchemaId>,
    pub schemas_by_id: HashMap<SchemaId, NamespaceSchema>,
    pub next_schema_id: SchemaId,
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub next_table_id: TableId,
    pub views_by_name: HashMap<String, TableId>,
    pub views_by_id: HashMap<TableId, ViewSchema>,
    pub indexes_by_name: HashMap<String, IndexId>,
    pub indexes_by_id: HashMap<IndexId, IndexSchema>,
    pub next_index_id: IndexId,
    pub sequences_by_name: HashMap<String, SequenceId>,
    pub sequences_by_id: HashMap<SequenceId, SequenceSchema>,
    pub next_sequence_id: SequenceId,
    pub next_dictionary_id: u32,
    pub next_storage_id: FileId,
    // Optimizer statistics per analyzed user table (`docs/specs/statistics.md`).
    pub statistics: HashMap<TableId, TableStatistics>,
}
```

`NamespaceSchema`, `TableSchema`, `ColumnDef`, `ColumnDefault`, `DataType`, `IndexSchema`,
`ViewColumn`, `ViewDependency`, `ViewSchema`, and `SequenceSchema` live in
`common`. `TableSchema` additionally carries `schema_version: u64`,
`compression: CompressionSetting`, and `active_dict_id: Option<u32>` (see
"Compression" below). `TableSchema.storage_id` and `IndexSchema.storage_id` are
physical relation-generation ids used by storage file-id derivation; the logical
table/index/view ids remain stable catalog identities.

User `TableSchema` values also carry durable outgoing foreign keys and a
per-table `next_foreign_key_id`. IDs are allocated from zero, never reused, and
limited to `0..=4095`; `4096` is the exhausted sentinel. Constraints preserve
the declared child/parent column order and immediate `NO ACTION`/`RESTRICT`
actions, plus the exact declared parent constraint-index ID selected when the
foreign key is attached. Hidden TOAST relations carry an empty list and allocator zero.
`CREATE TABLE` installs its table and declared PK/UNIQUE indexes before calling
`attach_foreign_keys` once with the fully bound batch. The catalog allocates names
and IDs atomically, validates the resulting complete snapshot, bumps the child
schema version, and returns the schema included in the statement's generic
catalog change. Failed
attachment publishes none of the batch; server/executor rollback removes the
earlier table/index objects. No supporting child index is created implicitly.

Every persisted relation-like object carries a `schema_id`. `ViewSchema` also
captures the schema-id search path used to bind its stored definition. Schema id
`1` is the built-in mutable `public` namespace; user schema allocation begins at
`2`. Schema ids are monotonic and never reused.

`CatalogManager` exposes schema-scoped lookup and creation operations for tables,
indexes, sequences, and views. The legacy bare-name lookup and creation methods
remain compatibility conveniences for `public`. Scoped index creation identifies
its target table by stable `TableId`, avoiding ambiguous bare names. Scoped view
creation records the exact schema-id search path used to bind its definition.
Relation-name collision checks are per schema, while tables, views, indexes,
sequences, and generated primary-key index names share one namespace within each
schema. `create_schema` allocates a monotonic user schema id. `drop_schema` has
RESTRICT semantics: it rejects a schema containing objects or referenced by a
stored view definition search path.

The catalog JSON payload has its own format version, independently of the outer
control-record version. Version 3 stores schemas, tables, views, indexes, and
sequences as id-sorted arrays plus allocator high-water marks, stable column
identities, and typed stored CHECK/default expressions; runtime name maps are
rebuilt while decoding. Unversioned, version-2, and unknown payloads are rejected.

Table IDs, index IDs, sequence IDs, and storage IDs are independent namespaces;
all are monotonically increasing and never reused. `next_index_id` starts at
`PRIMARY_KEY_INDEX_ID + 1`, because index id `0` is reserved for storage's
per-table identity index and is never assigned to a catalog index.
`next_sequence_id` starts at `1`. `next_dictionary_id` starts at `1` (dictionary id `0` is
reserved to mean "no dictionary", never assigned to a real dictionary).
`next_storage_id` starts at `1`; storage id `0` is the legacy/missing sentinel,
and ids with storage file-kind high bits set are invalid. The index, sequence,
dictionary-id, storage-id, and statistics fields deserialize with defaults
(empty maps and initial allocator values). A persisted user table that declares
a primary key must have a matching primary-key constraint index; manifests from
the older implicit-primary-key-index format are rejected rather than migrated.

Statistics are advisory and follow three rules (`docs/specs/statistics.md`):
`get_table_statistics`/`set_table_statistics` read and replace one live user
table's entry without bumping `schema_version` (set rejects unknown tables,
non-user relations, unknown column ids, and statistics containing non-finite
numbers — the JSON manifest payload cannot round-trip NaN/Infinity, so
accepting one would make the next startup unable to load the catalog); every
**column-changing** schema replacement (live DDL and recovery replay) funnels
through one reconciliation — statistics survive only when each prior column is
unchanged (same id, name, and type — pure ADD COLUMN or metadata-only updates,
including table rename); any other column change clears the per-column map but
keeps the row/page counts, because column ids are dense and shift on DROP
COLUMN. (The primary-key/compression/TOAST setters and the truncate apply
paths — which only swap storage ids — bypass the funnel and never change
column shape.) `DROP TABLE` removes the entry. Snapshot load *prunes*
orphan statistics (missing table, non-user relation, or unknown column id)
instead of rejecting them — a stale advisory entry must never block startup.
The transactional-TRUNCATE overlay reads statistics from the base catalog
unchanged and rejects writes.

The crate also exposes a static virtual system-catalog registry. This registry
describes view names, schemas, columns, and deterministic virtual OIDs for the
driver-oriented system-catalog surface; it is not part of `CatalogSnapshot`, WAL,
manifest state, heap storage, or `RelationKind`. Virtual rows are built later by
the executor from ordinary catalog/session/server state.

```rust
pub enum SystemSchema {
    PgCatalog,
    InformationSchema,
}

pub enum SystemView {
    PgNamespace,
    PgClass,
    PgAttribute,
    PgType,
    PgIndex,
    PgProc,
    PgConstraint,
    PgAttrdef,
    PgDepend,
    PgDatabase,
    PgRoles,
    PgSettings,
    PgStatActivity,
    PgStats,
    InformationSchemaSchemata,
    InformationSchemaTables,
    InformationSchemaColumns,
}

pub fn resolve_system_view(schema: Option<&str>, name: &str) -> Option<SystemView>;
pub fn is_system_schema(name: &str) -> bool;
```

`resolve_system_view(None, name)` searches only `pg_catalog`, matching the binder's
bare-name fallback rule. Qualified `pg_catalog.<view>` and
`information_schema.<view>` names resolve only within their named virtual schema.
`public` is not a system schema.

Virtual OIDs are deterministic and derived rather than persisted. User-object
OIDs use tagged 32-bit-compatible spaces (`tag << 28 | payload`) so catalog OID
columns can report PostgreSQL `oid` (OID 26) and still encode in binary protocol
as unsigned 32-bit values:

- schemas: `pg_catalog = 11`, `public = 2200`, `information_schema = 13000`;
- user tables: tag `1`;
- user indexes, including primary-key and unique constraint indexes: tag `2`;
- user sequences: tag `3`;
- fallback synthetic primary-key indexes: tag `4`;
- user schemas: tag `7`;
- foreign-key constraints: tag `8`, with an injective compound payload over
  `(child_table_id, foreign_key_id)`;
- derived constraints and column defaults use separate deterministic tagged
  spaces over `(table_id, local_object_id)`;
- core system views use stable PostgreSQL OIDs where practical, otherwise
  project-reserved constants.

The tag scheme reserves 28 payload bits. The table, index, sequence, schema,
synthetic-primary-key-index, constraint, and attribute-default OID helpers are
fallible and return `InternalError` for object IDs above their payload ranges
instead of truncating or panicking. Derived constraint/default OIDs split the
payload into a table-id portion and a local-object-id portion; catalog validation
rejects objects outside that deterministic injective range rather than hashing
or masking IDs into colliding OIDs. System-catalog row construction uses the
fallible `try_check_constraint_oid` helper for enumerated CHECK constraints; it
validates the `usize`-to-`u16` conversion, the primary-key-reserved sub-ID, and
the table-ID range before constructing an OID, returning `InternalError` for
invalid catalog metadata instead of truncating, aliasing, or panicking.

OID-like catalog columns use existing integer semantic types with PostgreSQL
`oid` wire presentation. `name`/`char` values still use text semantics; vector
and array catalog fields such as `pg_index.indkey`, `pg_proc.proargtypes`, and
`pg_constraint.conkey` use text storage with PostgreSQL-compatible wire identities
(`int2vector`, `oidvector`, `int2[]`, `oid[]`) where SaguaroDB has no first-class
array/vector value type yet.

Foreign-key rows use `foreign_key_constraint_oid(child_table_id,
foreign_key_id)`. Their `conindid` is the OID of the exact durable declared
primary-key or UNIQUE constraint-index ID selected at attachment. The executor
also derives `pg_depend` edges from the durable table/column/index IDs, so
renames and duplicate eligible keys do not change object identity.

## Public API

```rust
pub trait CatalogManager: Send + Sync {
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>>;
    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>>;
    fn list_tables(&self) -> Result<Vec<TableSchema>>;
    fn get_view_by_name(&self, name: &str) -> Result<Option<ViewSchema>>;
    fn get_view(&self, id: TableId) -> Result<Option<ViewSchema>>;
    fn list_views(&self) -> Result<Vec<ViewSchema>>;
    fn snapshot(&self) -> Result<CatalogSnapshot>;
    fn restore(&self, snapshot: CatalogSnapshot) -> Result<()>;
    fn reserve_table_id(&self, id: TableId) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_table_schema(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_table_and_index_schemas(
        &self,
        schema: TableSchema,
        indexes: &[IndexSchema],
    ) -> Result<()>;
    fn apply_drop_table(&self, id: TableId) -> Result<()>;
    fn attach_foreign_keys(
        &self,
        table: TableId,
        foreign_keys: Vec<ResolvedForeignKey>,
    ) -> Result<TableSchema>;
    fn reserve_foreign_key_allocator(
        &self,
        table: TableId,
        next_id: u32,
    ) -> Result<()>;
    fn drop_foreign_key(
        &self,
        table: TableId,
        name: &str,
        if_exists: bool,
    ) -> Result<Option<TableSchema>>;
    fn list_incoming_foreign_keys(
        &self,
        referenced_table: TableId,
    ) -> Result<Vec<(TableSchema, ForeignKeyConstraint)>>;
    fn resolve_foreign_key_index(
        &self,
        referenced_table: TableId,
        referenced_columns: &[ColumnId],
    ) -> Result<Option<IndexId>>;
    fn find_foreign_key_supporting_index(
        &self,
        child_table: TableId,
        columns: &[ColumnId],
    ) -> Result<Option<IndexId>>;
    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        compression: CompressionSetting,
    ) -> Result<TableSchema>;
    fn create_table_with_options(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        compression: CompressionSetting,
        toast: ToastOptions,
        checks: Vec<StoredExpression>,
    ) -> Result<TableSchema>;
    fn drop_table(&self, id: TableId) -> Result<()>;
    fn rename_table(&self, id: TableId, new_name: String) -> Result<TableSchema>;
    fn preflight_add_table_column(
        &self,
        id: TableId,
        if_not_exists: bool,
        column: &ParsedColumnDef,
    ) -> Result<TableColumnAlteration>;
    fn add_table_column(&self, id: TableId, column: ParsedColumnDef) -> Result<TableSchema>;
    fn preflight_drop_table_column(
        &self,
        id: TableId,
        if_exists: bool,
        column: &str,
    ) -> Result<TableColumnAlteration>;
    fn drop_table_column(&self, id: TableId, column: &str) -> Result<TableSchema>;
    fn rename_table_column(
        &self,
        id: TableId,
        old_name: &str,
        new_name: String,
    ) -> Result<TableSchema>;
    /// Applies an ALTER (or replays one during recovery): locates the live
    /// table by id and mutates its `compression`/`active_dict_id` in place,
    /// returning the updated clone. Also high-water-reserves `active_dict_id`
    /// when `Some` (see "Compression" below).
    fn set_table_compression(
        &self,
        table: TableId,
        compression: CompressionSetting,
        active_dict_id: Option<u32>,
    ) -> Result<TableSchema>;
    fn set_table_toast_metadata(
        &self,
        table: TableId,
        toast: ToastOptions,
        toast_table_id: Option<TableId>,
    ) -> Result<TableSchema>;
    fn set_table_primary_key(&self, table: TableId, primary_key: Vec<ColumnId>)
        -> Result<TableSchema>;
    fn add_table_primary_key_index(
        &self,
        table: TableId,
        primary_key: Vec<ColumnId>,
        index: IndexSchema,
    ) -> Result<TableSchema>;
    fn drop_table_primary_key_index(&self, table: TableId, index: IndexId) -> Result<TableSchema>;
    /// Allocates the next dictionary id (monotonic; `0` is reserved to mean
    /// "no dictionary").
    fn allocate_dictionary_id(&self) -> Result<u32>;
    /// Advances the dictionary-id allocator's high-water mark past `id`
    /// (WAL replay and orphan-dictionary-file recovery); never rewinds it.
    fn reserve_dictionary_id(&self, id: u32) -> Result<()>;
    /// Allocates the next physical storage-generation id shared by table heaps,
    /// hidden TOAST heaps, and secondary indexes.
    fn allocate_storage_id(&self) -> Result<FileId>;
    /// Advances the storage-id allocator high-water mark past `id` without
    /// installing a schema.
    fn reserve_storage_id(&self, id: FileId) -> Result<()>;
    fn prepare_truncate_table(&self, table: TableId) -> Result<TruncateTablePlan>;
    fn build_truncate_table_update(
        &self,
        plan: &TruncateTablePlan,
    ) -> Result<TruncateCatalogUpdate>;
    fn apply_truncate_table(&self, plan: &TruncateTablePlan) -> Result<TruncateCatalogUpdate>;
    fn apply_truncate_tables(
        &self,
        plans: &[TruncateTablePlan],
    ) -> Result<Vec<TruncateCatalogUpdate>>;
    fn apply_truncate_updates(&self, updates: &[TruncateCatalogUpdate]) -> Result<()>;

    fn get_index_by_name(&self, name: &str) -> Result<Option<IndexSchema>>;
    fn get_index(&self, id: IndexId) -> Result<Option<IndexSchema>>;
    fn list_indexes_for_table(&self, table: TableId) -> Result<Vec<IndexSchema>>;
    fn reserve_index_id(&self, id: IndexId) -> Result<()>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_update_index_schema(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, id: IndexId) -> Result<()>;
    fn create_index(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<IndexSchema>;
    fn create_index_with_constraint(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
        constraint: IndexConstraintKind,
    ) -> Result<IndexSchema>;
    fn drop_index(&self, id: IndexId) -> Result<()>;

    fn get_sequence_by_name(&self, name: &str) -> Result<Option<SequenceSchema>>;
    fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceSchema>>;
    fn list_sequences(&self) -> Result<Vec<SequenceSchema>>;
    fn reserve_sequence_id(&self, id: SequenceId) -> Result<()>;
    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()>;
    fn apply_drop_sequence(&self, id: SequenceId) -> Result<()>;
    fn create_sequence(
        &self,
        name: String,
        options: SequenceOptions,
        owned: bool,
    ) -> Result<SequenceSchema>;
    fn drop_sequence(&self, id: SequenceId) -> Result<()>;

    fn apply_create_view(&self, schema: ViewSchema) -> Result<()>;
    fn apply_replace_view(&self, schema: ViewSchema) -> Result<()>;
    fn apply_drop_view(&self, id: TableId) -> Result<()>;
    fn create_view(
        &self,
        name: String,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema>;
    fn replace_view(
        &self,
        id: TableId,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema>;
    fn drop_view(&self, id: TableId) -> Result<()>;
}
```

Methods return owned schema copies. The catalog is stored behind an `RwLock`.
Normal transactional DDL uses `CatalogOverlay` journaling rather than restoring a
public before-image. `catalog_change_set_between` converts complete snapshots to
deterministic object-ID-sorted mutations and allocator high-water;
`apply_catalog_change_set` verifies every current object equals its carried
`before`, applies all mutations to a candidate, preserves existing allocator
high-water, validates the complete snapshot, and only then returns it for atomic
publication. Live publication and recovery share this implementation.

The concrete implementation is `MemoryCatalog`. It is constructed with `MemoryCatalog::empty()` (or the equivalent `Default`) for a fresh database, or `MemoryCatalog::try_from_snapshot(snapshot)` to load a persisted snapshot through the validated path; the unchecked `from_snapshot` constructor is crate-internal.

User tables, user views, user-visible indexes (secondary, unique constraint, and
primary-key constraint indexes), public sequences, and primary-key auto-names
(`<relation>_pkey`) share the public relation-name namespace exposed through
`pg_class` and `to_regclass`. Creating or applying any one of those objects
rejects names already held by any other public relation kind with
`SqlState::DuplicateTable`. User views share the table-id relation namespace
with tables but are stored in separate `views_by_*` maps so storage startup
installs only physical relations. Hidden TOAST relations are installed by ID only
and are outside that user-visible namespace.

`attach_foreign_keys` validates and publishes a resolved batch atomically,
generating omitted names and allocating consecutive IDs under the same catalog
write lock. Names use `<child>_<source-columns>_fkey` and then the smallest
positive suffix avoiding table-local PK/UNIQUE/FK names. Drop preserves the ID
allocator. Incoming lookup is ordered by child table/FK ID. Referenced-key
resolution accepts an exact ordered primary key or exact ordered declared
UNIQUE constraint; a standalone unique index is ineligible. Child supporting
indexes are optional and require an exact ordered column match. Attachment
stores the selected constraint index's actual nonzero catalog ID; dependency
checks reject dropping that exact object even when another eligible constraint
has the same columns.
CREATE and standalone ALTER share these mutation primitives. ALTER ADD attaches
one proposed constraint while the server holds the publication gate, validates
existing rows before commit, and persists the returned schema through a generic
catalog change; ALTER DROP removes the execution-time-resolved name and
leaves `next_foreign_key_id` unchanged.
Catalog restore preserves the greatest per-table FK allocator high-water for
tables present in both states, just as it preserves global allocators. Recovery
can advance that same high-water from a skipped `CatalogChange` without
installing its aborted FK metadata. Table and
declared-UNIQUE drops reject surviving incoming dependencies. Constraint-index
creation/update consults the same table-local constraint-name namespace.
Dense column rewrites reject an actually referenced/source column and remap FK
column IDs above an unrelated dropped slot within the rewritten table (including
self-references). A parent rewrite that would require renumbering an external
child's incoming-reference metadata is rejected until the DDL publication path
can durably carry both schemas as one atomic update.
Changing or dropping a referenced primary key or declared UNIQUE constraint is
rejected even when another eligible constraint covers the same columns. Type
rewrites validate all incoming/outgoing FK declared types before allocating
replacement TOAST/catalog state. Recovery
`apply_create_table` validates any embedded FK metadata in a candidate catalog
before publication.

`apply_create_table` and `apply_drop_table` are recovery-only APIs.
`apply_create_table` inserts a fully assigned historical `TableSchema`, rejects
conflicting IDs and public relation names for user tables, rejects duplicate
live table/TOAST `storage_id`s, adds only user tables to the name map, and
advances `next_table_id` to at least `schema.id + 1` and `next_storage_id` past
`schema.storage_id`. Hidden TOAST relations are installed by ID only and never
inserted into `tables_by_name`. `reserve_table_id(id)` advances `next_table_id`
to at least `id + 1` without installing a schema. Neither method reassigns table
or column IDs. `apply_drop_table` removes an existing schema by ID without
assigning IDs; dropping a user table also removes its linked hidden TOAST
relation metadata and that relation's indexes, while directly dropping a linked
hidden TOAST relation is rejected as catalog corruption. A missing ID returns
`SqlState::UndefinedTable`. Normal multi-table drop uses `preflight_drop_tables`
and `drop_tables`: both resolve the complete target set, reject an incoming
foreign key whose child is outside that set with
`SqlState::DependentObjectsStillExist`, and permit self-references, cycles, and
dependencies wholly inside the set. `drop_tables` validates and publishes the
complete removal atomically. The statement's single `CatalogChange` contains
every table, index, hidden relation, and owned-sequence removal, so recovery
validates and applies the complete committed result atomically.

Schema-evolution helpers are catalog operations used by `ALTER TABLE` DDL execution. `add_table_column` assigns both the next dense `ColumnId` and the table's next never-reused `ColumnObjectId`. `drop_table_column` may renumber dense ordinals but preserves every surviving stable ID; a CHECK blocks the drop only when its typed tree references the target stable column. Existing index, FK, view, primary-key, and owned-sequence restrictions remain. Table and column renames change names without rewriting stored CHECK/default IR. Public changes increment `schema_version`, and preflight is repeated under the publication and relation locks.

`create_view` assigns dense and stable output-column identities. A `replace_view` position preserves its `ColumnObjectId` only when its name, logical type, nullability, length, and PostgreSQL type metadata remain compatible. Incompatible or newly permitted output positions allocate monotonically, and removed IDs are never reused. The relation ID/name and existing dependency/search-path behavior remain unchanged.

`create_index` resolves the table and column names, assigns an `IndexId` plus a
fresh `storage_id`, and returns the stored `IndexSchema`; duplicate names
include names already used by any public relation kind and names reserved for
primary-key auto-names exposed through PostgreSQL-compatible catalogs. Creating
or applying a table with a primary key is likewise rejected when its
`<relation>_pkey` auto-name would collide with any public relation. `drop_index`
removes an index by ID, returning `SqlState::UndefinedTable` for a missing ID
(indexes share the relation namespace, so there is no dedicated SQLSTATE).
`apply_create_index` and `apply_drop_index` are the matching recovery-only APIs:
`apply_create_index` inserts a fully assigned historical `IndexSchema`, rejects
conflicting public relation names, IDs, or live secondary-index `storage_id`s,
and advances `next_index_id` to at least `schema.id + 1` and `next_storage_id`
past `schema.storage_id`;
`reserve_index_id(id)` advances `next_index_id` to at least `id + 1` without
installing a schema; `apply_drop_index` removes an existing index by ID.
`list_indexes_for_table` returns a table's indexes ordered by ID and is how
storage learns which indexes to maintain on DML.

`create_sequence` validates and normalizes sequence options, assigns a
`SequenceId`, stores a `SequenceSchema`, and returns it. A duplicate public
relation name or duplicate sequence ID returns `SqlState::DuplicateTable`; a
missing sequence on drop returns `SqlState::UndefinedTable`; dropping a sequence
still referenced by a column `ColumnDefault::Nextval` returns
`SqlState::DependentObjectsStillExist` (`2BP01`).
`apply_create_sequence` / `apply_drop_sequence` are the recovery-only APIs for
historical sequence schemas, and
`reserve_sequence_id(id)` advances `next_sequence_id` to at least `id + 1`
without installing a schema.

`set_table_compression(table, compression, active_dict_id)` resolves the live
table by id first (a missing/dropped table has no side effect on the
dictionary-id allocator), then updates the schema's `compression` and
`active_dict_id` in place and returns the updated clone. When `active_dict_id`
is `Some(id)`, it **also** reserves that id (`reserve_id` against
`next_dictionary_id`) exactly like every other `apply_*` path advances its own
id allocator past an installed id — this covers both a fresh allocation on the
live `ALTER` path and a replayed id during recovery, so the allocator
high-water mark never lags an id a schema now references.
`allocate_dictionary_id` returns `next_dictionary_id` and advances it by one
(overflow-guarded, `SqlState`-free `DbError::internal` on exhaustion, like the
other id allocators); `reserve_dictionary_id(id)` advances the same high-water
mark to at least `id + 1` without allocating, for WAL replay and orphaned
dictionary files discovered at startup.

`allocate_storage_id` returns `next_storage_id` and advances it by one,
overflow-guarded and rejecting ids that collide with the file-kind high-bit
space. `reserve_storage_id(id)` advances the same high-water mark past `id`
without installing a schema. Storage ids are used by table heaps, hidden TOAST
heaps, and secondary indexes. Fresh allocation uses one high-water mark and
therefore avoids raw cross-kind collisions. Catalog formats older than v3 are
rejected rather than migrated; file-kind high bits keep actual file ids
distinct.

`prepare_truncate_table(table)` validates that `table` exists and is a user
table, allocates fresh storage ids for the base table, its hidden TOAST relation
when present, and each secondary index currently on the table, and returns a
`TruncateTablePlan`. It does not publish those ids into schemas; allocated ids
remain burned if the statement later aborts. `build_truncate_table_update(plan)`
validates the plan against the current catalog object set and returns the
post-truncate schemas without mutating the catalog, so storage can prepare empty
files before the commit record is durable. `apply_truncate_table(plan)`
revalidates the same plan, updates only the `storage_id` fields on the base
table, hidden TOAST table, and secondary indexes, reserves every planned storage
id, and returns `TruncateCatalogUpdate` for storage publication after durable
commit. `apply_truncate_tables(plans)` rejects duplicate logical targets and any
replacement storage id reused anywhere across the batch, validates every plan
against one catalog state, and applies the complete batch under one catalog
write lock. Normal multi-table TRUNCATE uses the batch method;
recovery keeps applying individual committed logical WAL records whose shared
transaction outcome makes the batch durable together.

`apply_truncate_updates(updates)` is the top-level commit publication path for
transactional TRUNCATE. It accepts prebuilt overlay schemas, reconstructs and
validates the equivalent plans against one current catalog state (including exact
base/TOAST/index identities and storage-id uniqueness), reserves every replacement
storage id, and publishes the complete batch under one catalog write lock. A
validation or allocator failure publishes nothing.

Transactional TRUNCATE validates the same batch but stores its replacement base,
hidden TOAST, and secondary-index schemas in a transaction-local catalog overlay;
public catalog maps remain unchanged. The read-only overlay stores only replacement
schemas and falls back to the live catalog for unrelated objects, so later statements
do not clone the full catalog and can resolve unrelated committed DDL. Owner binding
resolves overlay entries before the committed catalog. Top-level commit publishes
the complete overlay under the server catalog gate and one catalog write lock;
rollback discards it. Allocated ids remain burned. Repeated truncates replace the
overlay entry with the newest schema while storage retains first-before-images.
Each savepoint captures the overlay journal position; `ROLLBACK TO` restores that
position together with storage generation before-images, so TRUNCATE is supported
beneath savepoints without publishing intermediate catalog state.

Schema-evolution helpers are catalog operations used by `ALTER TABLE`
execution. `rename_table`, `add_table_column`, `drop_table_column`, and
`rename_table_column` require a user table and increment
`TableSchema.schema_version` on changes. ADD/DROP column preflight helpers run
the same no-op/dependency checks without mutating state so the server can avoid
snapshot fencing for harmless conditional statements. ADD/DROP column rewrites
use `add_table_column`/`drop_table_column` to allocate fresh `storage_id`s as
part of the logical schema change. The caller appends the matching generic
catalog change before storage initializes physical replacements. Renames are
metadata-only and keep existing storage
ids. Table/column renames reject dependent views. Table and column renames allow
stored CHECK constraints because typed stable-column references, rather than
canonical SQL, are execution authority. A column drop is blocked only by CHECKs
that reference that stable column. Dropping a column also rejects primary-key,
indexed, view-dependent, and owned-sequence-default columns.

## Create Table Rules

- Table name must be unique across the public relation namespace (user tables,
  secondary indexes, sequences, and synthetic primary-key index rows); a duplicate
  name returns `SqlState::DuplicateTable`.
- Column names must be unique within table; duplicate column definitions return `SqlState::SyntaxError`.
- A primary key is optional. If present, primary-key column names must exist.
- Duplicate primary-key column names return `SqlState::SyntaxError`.
- Composite (multi-column) primary keys are supported — every named column must exist, in declared order, and uniqueness is enforced over the whole tuple at the storage layer.
- Primary key columns are implicitly non-null.
- `ColumnId`s are assigned in declared column order starting at zero. Tables and
  views support at most 65,536 output columns; CREATE/ADD operations beyond that
  `u16` ID space return `SqlState::ProgramLimitExceeded` (`54000`).
- A table supports at most 4,095 stored CHECK constraints, because compound
  virtual OID sub-ID zero is reserved for its primary-key constraint. CREATE
  beyond that limit returns `SqlState::ProgramLimitExceeded` (`54000`). Invalid
  persisted snapshots continue to return `InternalError` as catalog corruption.
- A column's `max_length` (the `VARCHAR(n)`/`CHAR(n)` length constraint) is copied from `ParsedColumnDef` to the stored `ColumnDef` unchanged. The catalog does not enforce it; the executor enforces it at write time.
- A column's `default` is converted from `ParsedDefault` on `ParsedColumnDef` to `ColumnDefault` on the stored `ColumnDef`. `ParsedDefault::Const(Value)` becomes `ColumnDefault::Const(Value)`. User-written `ParsedDefault::Nextval(name)` resolves `name` through the current sequence registry and becomes `ColumnDefault::Nextval(SequenceId)`, but cannot reference a sequence marked `owned`. Internal `ParsedDefault::OwnedNextval(name)` is accepted only for an owned sequence created by `SERIAL` desugaring. A remaining `ParsedDefault::Serial` marker is an internal error because execution must replace it before calling the catalog. The binder type-checks defaults before the catalog sees them; the executor applies them to omitted columns at write time. Every constant default and literal nested in a stored default/CHECK tree must be **finite**: a non-finite `DOUBLE PRECISION`/`REAL` value (for example `DEFAULT 1e400` or `DEFAULT abs(1e400)`) is rejected with `SqlState::NumericValueOutOfRange`, because the JSON catalog/WAL encodings cannot round-trip NaN/±Infinity — an accepted one would make the next startup unable to load the catalog.
- Empty catalogs start with `next_table_id = 1` and `next_storage_id = 1`;
  `TableId` is assigned from `next_table_id`, and a user table's physical
  generation is assigned from `next_storage_id`.
- `PRIMARY KEY` and `UNIQUE` column / table constraints are represented by
  catalog indexes created by the executor immediately after the table. The
  primary-key constraint index uses the PostgreSQL-style auto name
  `<table>_pkey`; unique constraints use `<table>_<col...>_key`. Both reuse the
  normal create-index orchestration. One generic catalog change carries the
  table, hidden relation, declared indexes, owned sequences, and attached FK
  metadata atomically before physical relation/index initialization.
- `create_table_with_options` is the SQL DDL path. Its `compression: CompressionSetting` parameter (binder-resolved from optional `CREATE TABLE ... WITH (compression = ...)`, defaulting to `CompressionSetting::None`) is stored verbatim as `TableSchema.compression`; `active_dict_id` starts `None` — a freshly created `zstd` table is dict-less until an `ALTER` trains a dictionary (`docs/specs/compression.md` §4, §7). Its `toast: ToastOptions` parameter is stored verbatim on the user table after catalog validation. If the user table has at least one `TEXT` or `BYTEA` column, the catalog allocates a second `TableId` and a distinct storage id, stores the table id as `TableSchema.toast_table_id`, and creates a hidden TOAST relation by ID only. The hidden relation name is `"\0toast_<base_table_id>"`; columns are `(value_id BIGINT, seq INTEGER, data BYTEA)` with primary key `(value_id, seq)`; `compression = none`; `toast = ToastOptions::legacy_catalog_default()`; `toast_table_id = None`; `relation_kind = Toast { base_table }`. The hidden relation is not inserted into the user table name map.
- `create_table` is a compatibility helper that delegates to `create_table_with_options` with `ToastOptions::legacy_catalog_default()`. New SQL DDL should use `create_table_with_options`.
- `validate_create_table_definition(name, columns, primary_key, unique)` performs
  the catalog-owned table-shape validation used by table creation (duplicate
  columns, primary-key references, and unique-constraint column references)
  without reading or mutating live catalog state. The executor uses it before
  suppressing a duplicate-table error for `CREATE TABLE IF NOT EXISTS`, so invalid
  table definitions are still rejected even when the named table already exists.
- `set_table_toast_metadata(table, toast, toast_table_id)` validates the target is a user table, validates TOAST bounds, validates any supplied hidden relation cross-link, updates `toast` and `toast_table_id` atomically in the catalog snapshot, and reserves `toast.active_dict_id` when present.
- `set_table_primary_key(table, primary_key)` validates the target is a user table, validates that every `ColumnId` exists and appears once, replaces `TableSchema.primary_key`, increments `TableSchema.schema_version`, and marks those columns non-null. Clearing the primary-key list does not restore earlier nullability. Recovery applies the table replacement from generic catalog WAL; normal runtime `ADD PRIMARY KEY` uses the atomic helper below so readers do not observe a table primary key without its backing constraint index.
- `add_table_primary_key_index(table, primary_key, index)` atomically installs `TableSchema.primary_key` and the backing primary-key constraint index in the same catalog snapshot. It validates the target is a user table with no current primary key, validates the primary-key columns, validates the supplied index name/id/table/columns/constraint metadata, marks key columns non-null, increments `TableSchema.schema_version`, and advances the index allocator.
- `drop_table_primary_key_index(table, index)` atomically clears `TableSchema.primary_key`, increments `TableSchema.schema_version`, and removes the named primary-key constraint index from the same catalog snapshot. It is the runtime `ALTER TABLE ... DROP PRIMARY KEY` path, so readers never observe a table with `primary_key = []` while the old primary-key constraint index is still catalog-visible. Former primary-key columns remain non-null.

## Create Sequence Rules

- Sequence name must be unique across the public relation namespace; a duplicate
  returns `SqlState::DuplicateTable`.
- `increment` must be nonzero.
- For ascending sequences (`increment > 0`), omitted `MINVALUE` defaults to `1`,
  omitted `MAXVALUE` defaults to `i64::MAX`, and omitted `START` defaults to the
  resolved minimum.
- For descending sequences (`increment < 0`), omitted `MINVALUE` defaults to
  `i64::MIN`, omitted `MAXVALUE` defaults to `-1`, and omitted `START` defaults
  to the resolved maximum.
- `INCREMENT BY 0`, `MINVALUE > MAXVALUE`, and `START` outside the effective
  min/max range are rejected with `SqlState::InvalidParameterValue` (`22023`).
- `last_value` is initialized to `START` and `is_called` to `false`.
- `CACHE` is parser input only and is ignored by the catalog.
- `owned = true` is used only for sequences created by `SERIAL` desugaring.
  `DROP SEQUENCE` rejects owned sequences with
  `SqlState::DependentObjectsStillExist`; `DROP TABLE` removes the table and its
  owned sequences in the same statement.
- `DROP SEQUENCE` also rejects a sequence referenced anywhere in a typed
  expression default or CHECK tree, not only a direct `ColumnDefault::Nextval`.

## Create Index Rules

- Index name must be unique across the public relation namespace; a duplicate
  index name returns `SqlState::DuplicateTable`, the same code reused for the
  shared relation namespace.
- The target table must exist; otherwise `SqlState::UndefinedTable`.
- Index column names must exist on the target table; otherwise `SqlState::UndefinedColumn`.
- Duplicate index column names and an empty column list return `SqlState::SyntaxError`.
- Index columns keep the order written.
- `IndexId` is assigned from `next_index_id`, starting at `PRIMARY_KEY_INDEX_ID + 1`; `storage_id` is assigned independently from `next_storage_id`.
- The `unique` flag and constraint kind (`None`, `Unique`, or `PrimaryKey`) are recorded here; duplicate-value rejection for unique indexes happens at the storage layer, not in the catalog. A primary-key constraint index must be unique and must cover exactly the table's primary-key columns.
- `drop_index` rejects primary-key constraint indexes with `SqlState::DependentObjectsStillExist`; dropping the primary key itself is an `ALTER TABLE` operation.
- Dropping a table cascades in the catalog to remove every index on that table and, when the table has a hidden TOAST relation, the hidden relation metadata and its indexes. Owned SERIAL sequences are removed in the same catalog mutation. The durable generic change carries the complete object removal and recovery applies it atomically.

## Catalog Persistence

The catalog snapshot is deterministic JSON format version `3` inside the control record. Unversioned, older, and pre-foundation v3 layouts without the required typed-catalog allocator fields are rejected explicitly; development data directories are rebuilt rather than migrated. Catalog input and output are capped at 64 MiB. Transaction-local catalog mutation rejects growth past that durable limit with `ProgramLimitExceeded` before recording or publishing its overlay, so every successful catalog mutation remains checkpointable and reopenable. Validation requires unique nonzero stable column IDs below each relation's allocator, validates every stored-expression version/shape/reference and its bounded lists, requires each constant or stored-expression default to match its owning column type, and rejects unknown column, function, or sequence identities as catalog corruption. Canonical expression SQL is retained but is not execution authority.

Snapshot validation requires `public` to retain its fixed id/name, schema
name/id maps to be bidirectionally consistent with unique names and ids, and
`next_schema_id` to be greater than every installed schema id.

`TableSchema.foreign_keys` and `next_foreign_key_id` remain durable fields in
catalog v3. Older payload compatibility is intentionally not provided. New
metadata always persists the nonzero actual referenced constraint-index ID,
including for primary keys.

On startup:

1. The control store loads the current catalog bytes from the control record.
2. Catalog deserializes into memory.
3. Recovery pre-scans all post-checkpoint `CatalogChange` records to merge
   their global and per-relation stable-column/legacy-FK allocator high-water,
   regardless of commit state. During LSN-order replay, skipped changes install
   their reservations immediately so later exact before-images observe burned
   IDs; only committed object mutations are applied. The merged reservation is
   reapplied after replay for relations created after an earlier sparse entry.

Transactional catalog mutations update the private overlay immediately; only
allocator high-water reservations update the live catalog before commit. WAL
records provide durability until the next checkpoint, and durable top-level
commit precedes public overlay publication.

Snapshot validation also traverses every stored default/CHECK tree and rejects
non-finite literals; this extends the constant-default finiteness rule described
below to all durable scalar IR.

`restore` and startup loading must validate catalog snapshots before installing them. Public construction from persisted snapshots must use the validated path; unchecked snapshot installation is an implementation detail internal to the crate. Validation requires every table name index entry to point at an existing user-table schema with the same name and ID, every user-table schema to have a reverse name index entry, every hidden TOAST relation to be stored by ID only (not in the name index), every view name index entry to point at an existing view schema with the same name and ID, no table and view to share a relation name or relation id, nonzero table/view schema versions, column IDs assigned in declared order starting at zero, unique column IDs, unique column names, every primary-key column ID to exist, every primary-key column to be non-null, no duplicate primary-key column, every constant column default to be finite (non-finite constants cannot round-trip the JSON encodings; unreachable from any real durable artifact for the same reason), table IDs/default-column IDs/CHECK counts to fit the virtual catalog OID encoding limits, and `next_table_id >= max(table_or_view_id) + 1`. View validation additionally requires at least one output column, dense output column IDs, unique output column names, no column defaults on view output columns, non-empty SQL definition text, no self-dependency, no duplicate dependency entries, every dependency relation to exist, no dependency on a view or hidden TOAST relation, no dependency with both `all_columns = true` and named columns, and every named dependency column to exist. TOAST policy validation requires every table's `toast.tuple_target` to be in `ToastOptions::MIN_TOAST_TUPLE_TARGET..=ToastOptions::MAX_TOAST_TUPLE_TARGET`, `toast.min_value_size >= ToastOptions::MIN_TOAST_MIN_VALUE_SIZE`, every user table with TOAST enabled to name an existing hidden TOAST relation, every hidden TOAST relation to point back to the owning user table without recursively naming another TOAST relation, and every hidden TOAST relation to match `common::toast_schema(base, toast_id)` exactly except for its storage id (name, columns, primary key, compression disabled, no nested TOAST metadata). Index validation additionally requires every index name entry to point at an existing index with the same name and ID, every index schema to have a reverse name entry, the index ID to differ from the reserved `PRIMARY_KEY_INDEX_ID`, the referenced table to exist, a non-empty column list, every index column to exist on the referenced table, unique index column IDs, primary-key constraint indexes to be unique and to match the table's primary-key column list, every user table with a non-empty primary key to have exactly one primary-key constraint index, and `next_index_id >= max(index_id) + 1`. Storage-id validation requires every live table and secondary index to have a nonzero storage id, no storage id to contain file-kind high bits, no duplicate storage ids among live table/TOAST relations, no duplicate storage ids among live secondary indexes, and `next_storage_id` to be greater than every live storage id (or equal to the one-past-end sentinel after exhaustion). Sequence validation requires every sequence name entry to point at an existing sequence with the same name and ID, every sequence schema to have a reverse name entry, a nonzero increment, `MINVALUE <= MAXVALUE`, `START` and `last_value` within range, and `next_sequence_id >= max(sequence_id) + 1`. After per-kind validation succeeds, the same public relation namespace rule used by runtime DDL is enforced across user table names, user view names, user-visible index names, public sequence names, and primary-key auto-names; hidden TOAST names and their helper names remain outside that namespace. **Dictionary-id validation** (`validate_dictionary_ids`) requires `next_dictionary_id >= 1` (dictionary id `0` is reserved to mean "no dictionary" and is never a valid high-water mark) and, for every table with `active_dict_id = Some(id)` or `toast.active_dict_id = Some(id)`, both `id != 0` (a table must never name the reserved sentinel — use `None` instead) and `id < next_dictionary_id`. Invalid loaded snapshots return `InternalError` because they represent durable catalog corruption. Rollback `restore` validates the supplied snapshot and then preserves allocator monotonicity by taking the max of the restored and current `next_*_id` values (including `next_dictionary_id` and `next_storage_id`); startup uses `try_from_snapshot` instead, so persisted high-water marks load exactly as validated.

Foreign-key snapshot validation additionally requires unique IDs and
table-local constraint names, an allocator above every live ID and no greater
than 4096, equal non-empty ordered column lists without duplicates, existing
user child/parent tables and columns, exact `DataType`/concrete `PgType`/length
metadata equality (nullability may differ), and an exact declared PK/UNIQUE
target whose actual constraint-index ID equals the durable referenced index.
Hidden TOAST relations must have no FK metadata. Violations in persisted
metadata are `InternalError` corruption.

## WAL Interaction

All catalog-changing SQL uses `WalRecordKind::CatalogChange`; specialized
schema/table/view/index/sequence/statistics/ALTER metadata records do not exist.
The statement captures its transaction catalog before state, performs existing
typed catalog validation, diffs the result, appends the change set before
dependent physical work, and publishes only after durable commit. Dictionary
bytes, page redo, transaction markers, and non-transactional sequence values
remain separate WAL responsibilities. The exact record contract is authoritative
in `docs/specs/crates/wal.md`.

If normal DDL fails after staging catalog changes, an explicit transaction enters
the failed state. Transaction rollback discards all staged state; an explicit
`ROLLBACK TO SAVEPOINT` restores the overlay/storage journal position captured by
that savepoint. Neither path replaces the public catalog with a whole-catalog
before-image.

Recovery apply methods must update catalog state consistently with storage state.

## Invariants

- Name map and ID map are consistent, for tables, indexes, and sequences.
- IDs are never reused after drop.
- Table, index, sequence, and dictionary ID assignment is overflow-guarded:
  rather than wrap or reuse, an exhausted allocator returns
  `SqlState::InternalError`/`DbError::internal`. User-requested table/view column
  assignment beyond the `ColumnId` space instead returns
  `SqlState::ProgramLimitExceeded`; an out-of-range column ID in a persisted
  snapshot remains `InternalError` corruption.
- Storage-id assignment is overflow-guarded and rejects ids in the file-kind
  high-bit range. Freshly allocated table, TOAST, and secondary-index physical
  objects have distinct raw storage ids. Catalog formats older than v3 are
  rejected rather than migrated.
- Index id `PRIMARY_KEY_INDEX_ID` is reserved for storage's per-table identity index and never assigned to a catalog index.
- Dictionary id `0` is reserved to mean "no dictionary" and is never assigned to a real dictionary or accepted as a table's `active_dict_id`.
- Every secondary index references an existing table and existing columns on it; dropping a table removes its indexes.
- Every view dependency references an existing relation and existing columns when
  the dependency names columns. Dependent views block table/view drops. Column
  DDL is blocked for named dependency columns and for dependencies marked
  `all_columns`; relation-only dependencies block only relation drops/renames.
- Binder is the only consumer that resolves table, column, and index names for
  query planning. `DROP SEQUENCE` intentionally carries the sequence name
  through planning and resolves it at execution time so extended-protocol
  prepared statements do not bake in stale `IF EXISTS` absence.
- Executor/storage should otherwise use `TableId`, `ColumnId`, `IndexId`, and
  `SequenceId` after binding.

`preflight_alter_table_column_type` returns a no-op for an identical `PgType` and
rejects dependencies that cannot safely be rebound. `alter_table_column_type`
preserves the column ID while replacing its logical/wire type and converted
default; the executor assigns fresh table, TOAST, and index storage generations.

## Acceptance Tests

- Create table assigns table and column IDs.
- Duplicate table is rejected.
- Duplicate column is rejected.
- Primary key on missing column is rejected.
- Drop removes name and ID lookup.
- Batch truncate apply rejects duplicate targets and cross-plan replacement
  storage-id collisions before atomically swapping base-table, secondary-index,
  and hidden-TOAST storage ids; a late collision publishes no target.
- Serialization round-trip preserves `next_table_id`.
- Recovery create/drop updates catalog without name leaks into executor.
- Create index resolves columns and assigns monotonically increasing index IDs.
- Duplicate index name, missing table, missing column, and duplicate/empty columns are rejected with the documented SQLSTATEs.
- Dropping a table cascades to its indexes.
- Serialization round-trip preserves indexes and `next_index_id`; a no-primary-key snapshot without index fields loads as an empty index set.
- Snapshot validation rejects an index that references a missing table, uses the reserved storage identity index ID, has invalid primary-key constraint metadata, a primary-key table without exactly one matching primary-key constraint index, or a stale `next_index_id`.
- Create/drop sequence assigns monotonically increasing sequence IDs, validates
  sequence options, rejects drops while a column default references the sequence
  or the sequence is owned by `SERIAL`, rejects explicit defaults that borrow an
  owned sequence, persists through snapshot round-trip, and a snapshot without
  sequence fields loads as an empty sequence set.
- `create_table` stores the requested `compression` setting and starts
  `active_dict_id` at `None`.
- `set_table_compression` updates and persists a table's `compression` and
  `active_dict_id`, and reserving a fresh `active_dict_id` advances
  `next_dictionary_id` past it.
- Dictionary ids allocate monotonically (`allocate_dictionary_id`) and survive
  `reserve_dictionary_id` (a reserve above the current high-water mark advances
  it; a reserve below a value already allocated is a no-op).
- A snapshot without the dictionary-id field defaults `next_dictionary_id` to
  `1`, and the first allocation from that defaulted state still returns `1`.
