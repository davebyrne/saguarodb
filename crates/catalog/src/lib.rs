#![cfg_attr(
    not(test),
    deny(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

mod catalog_overlay;
mod memory;
mod serialize;
pub mod system;
mod truncate_overlay;

pub use catalog_overlay::{CatalogOverlay, CatalogOverlaySavepoint};
pub use memory::{CatalogSnapshot, MemoryCatalog, validate_create_table_definition};
pub use serialize::{deserialize_catalog, serialize_catalog};
pub use system::{
    INFORMATION_SCHEMA_OID, PG_CATALOG_SCHEMA_OID, PUBLIC_SCHEMA_OID, SystemSchema, SystemView,
    attrdef_oid, check_constraint_oid, foreign_key_constraint_oid, index_oid, is_system_schema,
    primary_key_constraint_oid, resolve_system_view, schema_oid, sequence_oid,
    synthetic_primary_key_oid, table_oid, try_check_constraint_oid,
};
pub use truncate_overlay::TruncateCatalogOverlay;

use common::{
    ColumnDefault, ColumnId, CompressionSetting, DataType, DbError, FileId, ForeignKeyAction,
    ForeignKeyConstraint, IndexConstraintKind, IndexId, IndexSchema, NamespaceSchema,
    ParsedColumnDef, PgType, Result, SchemaId, SequenceId, SequenceOptions, SequenceSchema,
    SqlState, TableId, TableSchema, TableStatistics, ToastOptions, TruncateCatalogUpdate,
    TruncateTablePlan, ViewColumn, ViewDependency, ViewSchema,
};

/// A name-optional but otherwise fully resolved foreign-key definition passed
/// to the catalog for atomic name/id allocation and attachment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedForeignKey {
    pub name: Option<String>,
    pub columns: Vec<ColumnId>,
    pub referenced_table: TableId,
    pub referenced_columns: Vec<ColumnId>,
    pub on_update: ForeignKeyAction,
    pub on_delete: ForeignKeyAction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableColumnAlteration {
    Noop,
    Rewrite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogAllocatorState {
    pub next_schema_id: SchemaId,
    pub next_table_id: TableId,
    pub next_index_id: IndexId,
    pub next_sequence_id: SequenceId,
    pub next_dictionary_id: u32,
    pub next_storage_id: FileId,
}

impl CatalogAllocatorState {
    pub fn from_snapshot(snapshot: &CatalogSnapshot) -> Self {
        Self {
            next_schema_id: snapshot.next_schema_id,
            next_table_id: snapshot.next_table_id,
            next_index_id: snapshot.next_index_id,
            next_sequence_id: snapshot.next_sequence_id,
            next_dictionary_id: snapshot.next_dictionary_id,
            next_storage_id: snapshot.next_storage_id,
        }
    }

    fn is_at_least(self, other: Self) -> bool {
        self.next_schema_id >= other.next_schema_id
            && self.next_table_id >= other.next_table_id
            && self.next_index_id >= other.next_index_id
            && self.next_sequence_id >= other.next_sequence_id
            && self.next_dictionary_id >= other.next_dictionary_id
            && self.next_storage_id >= other.next_storage_id
    }
}

impl TableColumnAlteration {
    pub fn rewrites_storage(self) -> bool {
        matches!(self, Self::Rewrite)
    }
}

pub trait CatalogManager: Send + Sync {
    fn claim_allocators(
        &self,
        _expected: CatalogAllocatorState,
        _desired: CatalogAllocatorState,
    ) -> Result<bool> {
        Err(DbError::internal(
            "catalog does not support allocator claims",
        ))
    }
    fn get_schema_by_name(&self, name: &str) -> Result<Option<NamespaceSchema>> {
        let snapshot = self.snapshot()?;
        Ok(snapshot
            .schemas_by_name
            .get(name)
            .and_then(|id| snapshot.schemas_by_id.get(id))
            .cloned())
    }
    fn get_schema(&self, id: SchemaId) -> Result<Option<NamespaceSchema>> {
        Ok(self.snapshot()?.schemas_by_id.get(&id).cloned())
    }
    fn list_schemas(&self) -> Result<Vec<NamespaceSchema>> {
        let mut schemas: Vec<_> = self.snapshot()?.schemas_by_id.into_values().collect();
        schemas.sort_by_key(|schema| schema.id);
        Ok(schemas)
    }
    fn reserve_schema_id(&self, _id: SchemaId) -> Result<()> {
        Err(DbError::internal(
            "catalog does not support schema mutation",
        ))
    }
    fn apply_create_schema(&self, _schema: NamespaceSchema) -> Result<()> {
        Err(DbError::internal(
            "catalog does not support schema mutation",
        ))
    }
    fn create_schema(&self, _name: String) -> Result<NamespaceSchema> {
        Err(DbError::internal(
            "catalog does not support schema mutation",
        ))
    }
    fn apply_drop_schema(&self, _id: SchemaId) -> Result<()> {
        Err(DbError::internal(
            "catalog does not support schema mutation",
        ))
    }
    fn drop_schema(&self, id: SchemaId) -> Result<()> {
        self.apply_drop_schema(id)
    }
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>>;
    fn get_table_in_schema(&self, schema: SchemaId, name: &str) -> Result<Option<TableSchema>> {
        Ok(self
            .snapshot()?
            .tables_by_id
            .into_values()
            .find(|table| table.schema_id == schema && table.name == name))
    }
    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>>;
    fn list_tables(&self) -> Result<Vec<TableSchema>>;
    fn get_view_by_name(&self, name: &str) -> Result<Option<ViewSchema>>;
    fn get_view_in_schema(&self, schema: SchemaId, name: &str) -> Result<Option<ViewSchema>> {
        Ok(self
            .snapshot()?
            .views_by_id
            .into_values()
            .find(|view| view.schema_id == schema && view.name == name))
    }
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
    /// Recovery-only single-record drop with the complete committed statement's
    /// table set. Implementations may remove FK edges between still-present batch
    /// members so cyclic drops replay in WAL-record order.
    fn apply_drop_table_in_batch(&self, id: TableId, batch: &[TableId]) -> Result<()>;
    /// Atomically attaches a batch of fully ID/column-resolved foreign keys to
    /// one child table. IDs must continue that table's monotonic allocator.
    fn attach_foreign_keys(
        &self,
        _table: TableId,
        _foreign_keys: Vec<ResolvedForeignKey>,
    ) -> Result<TableSchema> {
        Err(DbError::internal(
            "catalog does not support foreign-key mutation",
        ))
    }
    /// Removes one foreign key by its table-local constraint name. A missing
    /// name returns `None` only when `if_exists` is true.
    fn drop_foreign_key(
        &self,
        _table: TableId,
        _name: &str,
        _if_exists: bool,
    ) -> Result<Option<TableSchema>> {
        Err(DbError::internal(
            "catalog does not support foreign-key mutation",
        ))
    }
    /// Lists child tables and constraints that reference `referenced_table`.
    fn list_incoming_foreign_keys(
        &self,
        referenced_table: TableId,
    ) -> Result<Vec<(TableSchema, ForeignKeyConstraint)>> {
        let mut incoming = Vec::new();
        for table in self.list_tables()? {
            for foreign_key in &table.foreign_keys {
                if foreign_key.referenced_table == referenced_table {
                    incoming.push((table.clone(), foreign_key.clone()));
                }
            }
        }
        incoming.sort_by_key(|(table, foreign_key)| (table.id, foreign_key.id));
        Ok(incoming)
    }
    /// Resolves an eligible referenced PK/UNIQUE constraint to its storage
    /// access index. Primary keys use the reserved storage identity index id.
    fn resolve_foreign_key_index(
        &self,
        referenced_table: TableId,
        referenced_columns: &[ColumnId],
    ) -> Result<Option<IndexId>> {
        let Some(table) = self.get_table(referenced_table)? else {
            return Ok(None);
        };
        if !referenced_columns.is_empty() && table.primary_key == referenced_columns {
            return Ok(Some(common::PRIMARY_KEY_INDEX_ID));
        }
        Ok(self
            .list_indexes_for_table(referenced_table)?
            .into_iter()
            .filter(|index| {
                index.constraint == IndexConstraintKind::Unique
                    && index.columns == referenced_columns
            })
            .min_by_key(|index| index.id)
            .map(|index| index.id))
    }
    /// Finds an exact-column child-side access index, if one exists.
    fn find_foreign_key_supporting_index(
        &self,
        child_table: TableId,
        columns: &[ColumnId],
    ) -> Result<Option<IndexId>> {
        let Some(table) = self.get_table(child_table)? else {
            return Ok(None);
        };
        if !columns.is_empty() && table.primary_key == columns {
            return Ok(Some(common::PRIMARY_KEY_INDEX_ID));
        }
        Ok(self
            .list_indexes_for_table(child_table)?
            .into_iter()
            .filter(|index| {
                index.constraint != IndexConstraintKind::PrimaryKey && index.columns == columns
            })
            .min_by_key(|index| index.id)
            .map(|index| index.id))
    }
    /// Generates PostgreSQL's conventional table/column-based FK name with the
    /// smallest positive suffix needed to avoid a table-local constraint name.
    fn generate_foreign_key_name(&self, _table: TableId, _columns: &[ColumnId]) -> Result<String> {
        Err(DbError::internal(
            "catalog does not support foreign-key name generation",
        ))
    }
    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        compression: CompressionSetting,
    ) -> Result<TableSchema> {
        self.create_table_with_options(
            name,
            columns,
            primary_key,
            compression,
            ToastOptions::legacy_catalog_default(),
            Vec::new(),
        )
    }
    fn create_table_with_options(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        compression: CompressionSetting,
        toast: ToastOptions,
        checks: Vec<String>,
    ) -> Result<TableSchema> {
        self.create_table_in_schema_with_options(
            common::PUBLIC_SCHEMA_ID,
            name,
            columns,
            primary_key,
            compression,
            toast,
            checks,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn create_table_in_schema_with_options(
        &self,
        schema: SchemaId,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        compression: CompressionSetting,
        toast: ToastOptions,
        checks: Vec<String>,
    ) -> Result<TableSchema>;
    fn drop_table(&self, id: TableId) -> Result<()>;
    /// Validate a complete DROP TABLE target set without mutating it.
    fn preflight_drop_tables(&self, tables: &[TableId]) -> Result<()>;
    /// Atomically remove a complete DROP TABLE target set.
    fn drop_tables(&self, tables: &[TableId]) -> Result<()>;
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
    fn preflight_alter_table_column_type(
        &self,
        _id: TableId,
        _column: &str,
        _pg_type: &PgType,
    ) -> Result<TableColumnAlteration> {
        Err(DbError::internal(
            "catalog does not support ALTER COLUMN TYPE",
        ))
    }
    fn alter_table_column_type(
        &self,
        _id: TableId,
        _column: &str,
        _data_type: DataType,
        _pg_type: PgType,
        _converted_default: Option<ColumnDefault>,
    ) -> Result<TableSchema> {
        Err(DbError::internal(
            "catalog does not support ALTER COLUMN TYPE",
        ))
    }
    fn rename_table_column(
        &self,
        id: TableId,
        old_name: &str,
        new_name: String,
    ) -> Result<TableSchema>;
    /// Applies an ALTER (or replays one during recovery): locates the live
    /// table by id and mutates its compression setting and active dictionary
    /// id in place, returning the updated clone.
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
    /// Applies an ALTER (or replays one during recovery): locates the live user
    /// table by id and replaces its primary-key column list. Adding a primary key
    /// marks those columns not-null in catalog metadata; dropping one does not
    /// restore prior nullability.
    fn set_table_primary_key(
        &self,
        table: TableId,
        primary_key: Vec<ColumnId>,
    ) -> Result<TableSchema>;
    /// Validates a primary-key metadata change without publishing it. A key
    /// referenced by an installed foreign key cannot be changed or removed.
    fn preflight_table_primary_key_change(
        &self,
        table: TableId,
        primary_key: &[ColumnId],
    ) -> Result<()> {
        let schema = self.get_table(table)?.ok_or_else(|| {
            DbError::plan(
                SqlState::UndefinedTable,
                format!("table id {table} does not exist"),
            )
        })?;
        if schema.primary_key.is_empty() || schema.primary_key == primary_key {
            return Ok(());
        }
        if let Some((child, foreign_key)) = self
            .list_incoming_foreign_keys(table)?
            .into_iter()
            .find(|(_, foreign_key)| foreign_key.referenced_columns == schema.primary_key)
        {
            return Err(DbError::plan(
                SqlState::DependentObjectsStillExist,
                format!(
                    "cannot change primary key because constraint {} on table {} depends on it",
                    foreign_key.name, child.name
                ),
            ));
        }
        Ok(())
    }
    /// Atomically installs a live user table's primary-key metadata and the
    /// backing primary-key constraint index. Adding a primary key marks those
    /// columns not-null.
    fn add_table_primary_key_index(
        &self,
        table: TableId,
        primary_key: Vec<ColumnId>,
        index: IndexSchema,
    ) -> Result<TableSchema>;
    /// Atomically clears a live user table's primary-key metadata and removes the
    /// backing primary-key constraint index. Dropping a primary key does not restore
    /// prior nullability on the former key columns.
    fn drop_table_primary_key_index(&self, table: TableId, index: IndexId) -> Result<TableSchema>;
    /// Optimizer statistics for a live user table, if it has been analyzed
    /// (`docs/specs/statistics.md`). Statistics are advisory: absent means
    /// "never analyzed (or cleared by a schema change)".
    fn get_table_statistics(&self, table: TableId) -> Result<Option<TableStatistics>>;
    /// Replaces a live user table's optimizer statistics (ANALYZE, or
    /// recovery replay of a committed `UpdateTableStatistics` record —
    /// `docs/specs/statistics.md` §4; replay skips records whose table no
    /// longer exists). Statistics changes do not bump `schema_version`.
    /// Rejects unknown tables, non-user relations, statistics that reference
    /// column ids the table does not have, and non-finite numbers (the JSON
    /// manifest payload cannot round-trip them).
    fn set_table_statistics(&self, table: TableId, statistics: TableStatistics) -> Result<()>;
    /// Allocates the next dictionary id (monotonic; `0` is reserved to mean
    /// "no dictionary").
    fn allocate_dictionary_id(&self) -> Result<u32>;
    /// Advances the dictionary id allocator's high-water mark past `id`
    /// (replay and orphan-dictionary-file recovery); never rewinds it.
    fn reserve_dictionary_id(&self, id: u32) -> Result<()>;
    /// Allocates a physical storage-generation id. Storage ids are shared by
    /// user-table heap/primary-index pairs, hidden TOAST heap/primary-index
    /// pairs, and secondary indexes; they are never reused.
    fn allocate_storage_id(&self) -> Result<FileId>;
    /// Advances the storage-id allocator's high-water mark past `id` without
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
    fn get_index_in_schema(&self, schema: SchemaId, name: &str) -> Result<Option<IndexSchema>> {
        Ok(self
            .snapshot()?
            .indexes_by_id
            .into_values()
            .find(|index| index.schema_id == schema && index.name == name))
    }
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
    ) -> Result<IndexSchema> {
        self.create_index_with_constraint(name, table, columns, unique, IndexConstraintKind::None)
    }
    fn create_index_with_constraint(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
        constraint: IndexConstraintKind,
    ) -> Result<IndexSchema> {
        let table = self.get_table_by_name(table)?.ok_or_else(|| {
            DbError::plan(
                common::SqlState::UndefinedTable,
                format!("table {table} does not exist"),
            )
        })?;
        self.create_index_in_schema_with_constraint(
            common::PUBLIC_SCHEMA_ID,
            name,
            table.id,
            columns,
            unique,
            constraint,
        )
    }
    fn create_index_in_schema_with_constraint(
        &self,
        schema: SchemaId,
        name: String,
        table: TableId,
        columns: &[String],
        unique: bool,
        constraint: IndexConstraintKind,
    ) -> Result<IndexSchema>;
    fn drop_index(&self, id: IndexId) -> Result<()>;

    fn get_sequence_by_name(&self, name: &str) -> Result<Option<SequenceSchema>>;
    fn get_sequence_in_schema(
        &self,
        schema: SchemaId,
        name: &str,
    ) -> Result<Option<SequenceSchema>> {
        Ok(self
            .snapshot()?
            .sequences_by_id
            .into_values()
            .find(|sequence| sequence.schema_id == schema && sequence.name == name))
    }
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
    ) -> Result<SequenceSchema> {
        self.create_sequence_in_schema(common::PUBLIC_SCHEMA_ID, name, options, owned)
    }
    fn create_sequence_in_schema(
        &self,
        schema: SchemaId,
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
    ) -> Result<ViewSchema> {
        self.create_view_in_schema(
            common::PUBLIC_SCHEMA_ID,
            name,
            columns,
            definition,
            dependencies,
            vec![common::PUBLIC_SCHEMA_ID],
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn create_view_in_schema(
        &self,
        schema: SchemaId,
        name: String,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
        definition_search_path: Vec<SchemaId>,
    ) -> Result<ViewSchema>;
    fn replace_view(
        &self,
        id: TableId,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema>;
    fn replace_view_with_search_path(
        &self,
        id: TableId,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
        definition_search_path: Vec<SchemaId>,
    ) -> Result<ViewSchema>;
    fn drop_view(&self, id: TableId) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common::{
        ColumnDef, ColumnDefault, CompressionSetting, DataType, ErrorKind, ForeignKeyAction,
        ForeignKeyConstraint, IndexConstraintKind, IndexSchema, ParsedColumnDef, PgType,
        RelationKind, SequenceOptions, SequenceSchema, SqlState, TableSchema, ToastCompression,
        ToastMode, ToastOptions, ViewColumn, ViewDependency, toast_schema,
    };

    use crate::{
        CatalogManager, CatalogSnapshot, MemoryCatalog, ResolvedForeignKey, check_constraint_oid,
        deserialize_catalog, foreign_key_constraint_oid, serialize_catalog,
        system::{MAX_COMPOUND_OID_TABLE_ID, MAX_VIRTUAL_OID_PAYLOAD},
        validate_create_table_definition,
    };

    fn id_column(nullable: bool) -> ParsedColumnDef {
        ParsedColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable,
            max_length: None,
            default: None,
            pg_type: None,
        }
    }

    fn view_column(name: &str, data_type: DataType, nullable: bool) -> ViewColumn {
        ViewColumn {
            name: name.to_string(),
            data_type,
            nullable,
            pg_type: None,
        }
    }

    fn stored_id_table(id: u32, name: &str) -> TableSchema {
        TableSchema {
            id,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: id,
            name: name.to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: Vec::new(),
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        }
    }

    fn foreign_key_catalog() -> (MemoryCatalog, u32, u32) {
        let catalog = MemoryCatalog::empty();
        let parent = catalog
            .create_table(
                "parents".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        catalog
            .create_index_with_constraint(
                "parents_pkey".to_string(),
                "parents",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();
        let child = catalog
            .create_table(
                "children".to_string(),
                vec![ParsedColumnDef {
                    name: "parent_id".to_string(),
                    data_type: DataType::Integer,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                }],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        (catalog, parent.id, child.id)
    }

    fn resolved_foreign_key(name: Option<&str>, referenced_table: u32) -> ResolvedForeignKey {
        ResolvedForeignKey {
            name: name.map(str::to_string),
            columns: vec![0],
            referenced_table,
            referenced_columns: vec![0],
            on_update: ForeignKeyAction::NoAction,
            on_delete: ForeignKeyAction::Restrict,
        }
    }

    /// A `users(id INTEGER PRIMARY KEY, name TEXT)` table for index tests.
    fn catalog_with_users() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        catalog
    }

    /// A `users(id INTEGER NOT NULL, name TEXT)` table for schema/view tests that
    /// are not exercising primary-key constraint-index metadata.
    fn catalog_with_users_without_primary_key() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();
        catalog
    }

    #[test]
    fn create_table_assigns_stable_table_and_column_ids() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();

        assert_eq!(schema.id, 1);
        assert_eq!(schema.columns[0].id, 0);
        assert!(!schema.columns[0].nullable);
        assert_eq!(schema.primary_key, vec![0]);
    }

    #[test]
    fn duplicate_table_is_rejected() {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::DuplicateTable);
    }

    #[test]
    fn relation_names_are_unique_across_tables_indexes_sequences_and_synthetic_pkeys() {
        let catalog = catalog_with_users();
        let index_err = catalog
            .create_index("users".to_string(), "users", &["name".to_string()], false)
            .unwrap_err();
        assert_eq!(index_err.code, SqlState::DuplicateTable);
        let sequence_err = catalog
            .create_sequence("users".to_string(), SequenceOptions::default(), false)
            .unwrap_err();
        assert_eq!(sequence_err.code, SqlState::DuplicateTable);
        let synthetic_err = catalog
            .create_table(
                "users_pkey".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();
        assert_eq!(synthetic_err.code, SqlState::DuplicateTable);

        let catalog = MemoryCatalog::empty();
        catalog
            .create_sequence("accounts".to_string(), SequenceOptions::default(), false)
            .unwrap();
        let table_err = catalog
            .create_table(
                "accounts".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();
        assert_eq!(table_err.code, SqlState::DuplicateTable);

        let catalog = catalog_with_users();
        catalog
            .create_sequence("users_name".to_string(), SequenceOptions::default(), false)
            .unwrap();
        let index_err = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(index_err.code, SqlState::DuplicateTable);
    }

    #[test]
    fn hidden_toast_synthetic_primary_key_names_do_not_reserve_public_names() {
        let catalog = MemoryCatalog::empty();
        let base = catalog
            .create_table_with_options(
                "docs".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "body".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                CompressionSetting::None,
                ToastOptions::default_new_table(),
                Vec::new(),
            )
            .unwrap();
        assert!(base.toast_table_id.is_some());

        let public = catalog
            .create_table(
                "pg_toast_1_pkey".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        assert_eq!(public.name, "pg_toast_1_pkey");
    }

    #[test]
    fn public_names_do_not_block_hidden_toast_synthetic_primary_key_names() {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_sequence(
                "pg_toast_1_pkey".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();

        let base = catalog
            .create_table_with_options(
                "docs".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "body".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                CompressionSetting::None,
                ToastOptions::default_new_table(),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(base.toast_table_id, Some(2));
    }

    #[test]
    fn snapshot_validation_rejects_public_relation_name_collisions() {
        let users = stored_id_table(1, "users");
        let duplicate_sequence = SequenceSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            name: "users".to_string(),
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            start: 1,
            cycle: false,
            owned: false,
            last_value: 1,
            is_called: false,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, users.clone())]),
            next_table_id: 2,
            sequences_by_name: HashMap::from([("users".to_string(), 1)]),
            sequences_by_id: HashMap::from([(1, duplicate_sequence)]),
            next_sequence_id: 2,
            ..CatalogSnapshot::default()
        };
        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("public relation name users"));

        let duplicate_index = IndexSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 1,
            table: users.id,
            name: "users".to_string(),
            columns: vec![0],
            unique: false,
            constraint: IndexConstraintKind::None,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, users)]),
            next_table_id: 2,
            indexes_by_name: HashMap::from([("users".to_string(), 1)]),
            indexes_by_id: HashMap::from([(1, duplicate_index)]),
            next_index_id: 2,
            next_storage_id: 2,
            ..CatalogSnapshot::default()
        };
        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("public relation name users"));
    }

    #[test]
    fn validate_create_table_definition_rejects_catalog_owned_shape_errors() {
        let duplicate_column = validate_create_table_definition(
            "t",
            &[
                id_column(false),
                ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            &["id".to_string()],
            &[],
        )
        .unwrap_err();
        assert_eq!(duplicate_column.code, SqlState::SyntaxError);

        let missing_unique_column = validate_create_table_definition(
            "t",
            &[
                id_column(false),
                ParsedColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            &["id".to_string()],
            &[vec!["missing".to_string()]],
        )
        .unwrap_err();
        assert_eq!(missing_unique_column.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn create_table_carries_column_defaults_into_schema() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "t".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "n".to_string(),
                        data_type: DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: Some(common::ParsedDefault::Const(common::Value::Integer(42))),
                        pg_type: None,
                    },
                ],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();

        assert_eq!(schema.columns[0].default, None);
        assert_eq!(
            schema.columns[1].default,
            Some(common::ColumnDefault::Const(common::Value::Integer(42)))
        );

        // The default survives a serialize/restore round trip.
        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();
        let restored_schema = restored.get_table_by_name("t").unwrap().unwrap();
        assert_eq!(
            restored_schema.columns[1].default,
            Some(common::ColumnDefault::Const(common::Value::Integer(42)))
        );
    }

    #[test]
    fn duplicate_column_is_rejected() {
        let catalog = MemoryCatalog::empty();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn restore_does_not_rewind_allocators() {
        let catalog = catalog_with_users();
        catalog
            .create_index_with_constraint(
                "users_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();
        let before_failed_ddl = catalog.snapshot().unwrap();

        let failed_table = catalog
            .create_table(
                "orders".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        let failed_index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        catalog.restore(before_failed_ddl).unwrap();
        assert_eq!(catalog.get_table_by_name("orders").unwrap(), None);
        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);

        let recreated_table = catalog
            .create_table(
                "orders".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        let recreated_index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        assert_eq!(recreated_table.id, failed_table.id + 1);
        assert_eq!(recreated_index.id, failed_index.id + 1);
    }

    #[test]
    fn table_schema_ddl_helpers_update_schema_versions() {
        let catalog = catalog_with_users_without_primary_key();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        assert_eq!(users.schema_version, common::INITIAL_SCHEMA_VERSION);

        let with_email = catalog
            .add_table_column(
                users.id,
                ParsedColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: Some(PgType::Text),
                },
            )
            .unwrap();
        assert_eq!(with_email.schema_version, users.schema_version + 1);
        assert_eq!(with_email.columns[2].id, 2);

        let renamed_column = catalog
            .rename_table_column(users.id, "email", "contact_email".to_string())
            .unwrap();
        assert_eq!(renamed_column.schema_version, with_email.schema_version + 1);
        assert_eq!(renamed_column.columns[2].name, "contact_email");

        let dropped_column = catalog
            .drop_table_column(users.id, "contact_email")
            .unwrap();
        assert_eq!(
            dropped_column.schema_version,
            renamed_column.schema_version + 1
        );
        assert_eq!(dropped_column.columns.len(), 2);

        let renamed_table = catalog
            .rename_table(users.id, "people".to_string())
            .unwrap();
        assert_eq!(
            renamed_table.schema_version,
            dropped_column.schema_version + 1
        );
        assert!(catalog.get_table_by_name("users").unwrap().is_none());
        assert_eq!(
            catalog.get_table_by_name("people").unwrap().unwrap().id,
            users.id
        );
    }

    #[test]
    fn rename_table_allows_stored_check_constraints() {
        let catalog = MemoryCatalog::empty();
        let accounts = catalog
            .create_table_with_options(
                "accounts".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
                ToastOptions::legacy_catalog_default(),
                vec!["id > 0".to_string()],
            )
            .unwrap();

        let renamed = catalog
            .rename_table(accounts.id, "customers".to_string())
            .unwrap();

        assert_eq!(renamed.name, "customers");
        assert_eq!(renamed.checks, vec!["id > 0".to_string()]);
        assert!(catalog.get_table_by_name("accounts").unwrap().is_none());
        assert_eq!(
            catalog.get_table_by_name("customers").unwrap().unwrap().id,
            accounts.id
        );
    }

    #[test]
    fn views_share_relation_namespace_and_persist() {
        let catalog = catalog_with_users_without_primary_key();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let view = catalog
            .create_view(
                "active_users".to_string(),
                vec![
                    view_column("id", DataType::Integer, false),
                    view_column("name", DataType::Text, true),
                ],
                "SELECT id, name FROM users".to_string(),
                vec![ViewDependency {
                    relation: users.id,
                    columns: vec![0, 1],
                    all_columns: false,
                }],
            )
            .unwrap();

        // `users(name text)` owns a hidden TOAST relation, so the next relation id
        // after the user table is already burned before the view is created.
        assert_eq!(view.id, users.id + 2);
        assert_eq!(view.schema_version, common::INITIAL_SCHEMA_VERSION);
        assert!(!view.columns[0].nullable);
        assert!(view.columns[1].nullable);
        assert_eq!(
            catalog
                .get_view_by_name("active_users")
                .unwrap()
                .unwrap()
                .id,
            view.id
        );

        let duplicate = catalog
            .create_table(
                "active_users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();
        assert_eq!(duplicate.code, SqlState::DuplicateTable);

        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();
        let restored_view = restored.get_view(view.id).unwrap().unwrap();
        assert_eq!(restored_view.name, "active_users");
        assert_eq!(restored_view.dependencies[0].relation, users.id);

        let replacement = restored
            .replace_view(
                view.id,
                vec![view_column("id", DataType::Integer, false)],
                "SELECT id FROM users".to_string(),
                vec![ViewDependency {
                    relation: users.id,
                    columns: vec![0],
                    all_columns: false,
                }],
            )
            .unwrap();
        assert_eq!(replacement.id, view.id);
        assert_eq!(replacement.schema_version, view.schema_version + 1);
        assert_eq!(replacement.columns.len(), 1);
    }

    #[test]
    fn view_dependencies_block_unsafe_relation_and_column_changes() {
        let catalog = catalog_with_users_without_primary_key();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let view = catalog
            .create_view(
                "user_names".to_string(),
                vec![view_column("name", DataType::Text, true)],
                "SELECT name FROM users".to_string(),
                vec![ViewDependency {
                    relation: users.id,
                    columns: vec![1],
                    all_columns: false,
                }],
            )
            .unwrap();

        let drop_table = catalog.drop_table(users.id).unwrap_err();
        assert_eq!(drop_table.code, SqlState::DependentObjectsStillExist);

        let rename_table = catalog
            .rename_table(users.id, "people".to_string())
            .unwrap_err();
        assert_eq!(rename_table.code, SqlState::DependentObjectsStillExist);

        let drop_column = catalog.drop_table_column(users.id, "name").unwrap_err();
        assert_eq!(drop_column.code, SqlState::DependentObjectsStillExist);

        let rename_column = catalog
            .rename_table_column(users.id, "name", "full_name".to_string())
            .unwrap_err();
        assert_eq!(rename_column.code, SqlState::DependentObjectsStillExist);

        catalog.drop_view(view.id).unwrap();
        catalog
            .rename_table_column(users.id, "name", "full_name".to_string())
            .unwrap();
    }

    #[test]
    fn view_on_view_dependencies_are_rejected_for_now() {
        let catalog = catalog_with_users_without_primary_key();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let base_view = catalog
            .create_view(
                "base_user_names".to_string(),
                vec![view_column("name", DataType::Text, true)],
                "SELECT name FROM users".to_string(),
                vec![ViewDependency {
                    relation: users.id,
                    columns: vec![1],
                    all_columns: false,
                }],
            )
            .unwrap();

        let err = catalog
            .create_view(
                "derived_user_names".to_string(),
                vec![view_column("name", DataType::Text, true)],
                "SELECT name FROM base_user_names".to_string(),
                vec![ViewDependency {
                    relation: base_view.id,
                    columns: vec![0],
                    all_columns: false,
                }],
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(
            catalog
                .get_view_by_name("derived_user_names")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn apply_update_table_schema_remaps_view_dependencies_by_column_identity() {
        let catalog = MemoryCatalog::empty();
        let accounts = catalog
            .create_table(
                "accounts".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "legacy_score".to_string(),
                        data_type: DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: Some(PgType::Int8),
                    },
                    ParsedColumnDef {
                        name: "balance".to_string(),
                        data_type: DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: Some(PgType::Int8),
                    },
                ],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();
        let view = catalog
            .create_view(
                "account_balances".to_string(),
                vec![view_column("balance", DataType::Integer, true)],
                "SELECT balance FROM accounts".to_string(),
                vec![ViewDependency {
                    relation: accounts.id,
                    columns: vec![2],
                    all_columns: false,
                }],
            )
            .unwrap();

        let mut replayed_schema = accounts;
        replayed_schema.columns.remove(1);
        replayed_schema.columns[1].id = 1;
        replayed_schema.schema_version += 1;
        catalog.apply_update_table_schema(replayed_schema).unwrap();

        let stored_view = catalog.get_view(view.id).unwrap().unwrap();
        assert_eq!(stored_view.dependencies[0].columns, vec![1]);
    }

    #[test]
    fn apply_update_schemas_reserve_replayed_storage_ids() {
        let catalog = catalog_with_users_without_primary_key();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        let mut replayed_index = index.clone();
        replayed_index.storage_id = 50;
        catalog.apply_update_index_schema(replayed_index).unwrap();
        assert_eq!(catalog.snapshot().unwrap().next_storage_id, 51);

        let mut replayed_table = users;
        replayed_table.storage_id = 75;
        replayed_table.schema_version += 1;
        catalog.apply_update_table_schema(replayed_table).unwrap();
        assert_eq!(catalog.snapshot().unwrap().next_storage_id, 76);
    }

    #[test]
    fn apply_update_table_and_index_schemas_validates_primary_key_against_replayed_table() {
        let catalog = MemoryCatalog::empty();
        let accounts = catalog
            .create_table(
                "accounts".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    id_column(false),
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        let primary_key = catalog
            .create_index_with_constraint(
                "accounts_pkey".to_string(),
                "accounts",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();
        assert_eq!(primary_key.columns, vec![1]);

        let mut replayed_table = accounts;
        replayed_table.columns.remove(0);
        replayed_table.columns[0].id = 0;
        replayed_table.primary_key = vec![0];
        replayed_table.schema_version += 1;
        replayed_table.storage_id = 50;

        let mut replayed_primary_key = primary_key;
        replayed_primary_key.columns = vec![0];
        replayed_primary_key.storage_id = 51;

        catalog
            .apply_update_table_and_index_schemas(replayed_table.clone(), &[replayed_primary_key])
            .unwrap();

        let stored_table = catalog.get_table(replayed_table.id).unwrap().unwrap();
        let stored_primary_key = catalog.get_index_by_name("accounts_pkey").unwrap().unwrap();
        assert_eq!(stored_table.primary_key, vec![0]);
        assert_eq!(stored_primary_key.columns, vec![0]);
        assert_eq!(catalog.snapshot().unwrap().next_storage_id, 52);
    }

    #[test]
    fn relation_wide_view_dependency_blocks_add_column() {
        let catalog = catalog_with_users_without_primary_key();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        catalog
            .create_view(
                "all_users".to_string(),
                vec![
                    view_column("id", DataType::Integer, false),
                    view_column("name", DataType::Text, true),
                ],
                "SELECT * FROM users".to_string(),
                vec![ViewDependency {
                    relation: users.id,
                    columns: Vec::new(),
                    all_columns: true,
                }],
            )
            .unwrap();

        let err = catalog
            .add_table_column(
                users.id,
                ParsedColumnDef {
                    name: "active".to_string(),
                    data_type: DataType::Boolean,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: Some(PgType::Bool),
                },
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
    }

    #[test]
    fn add_toastable_column_allocates_hidden_toast_relation() {
        let catalog = MemoryCatalog::empty();
        let table = catalog
            .create_table(
                "numbers".to_string(),
                vec![id_column(false)],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();

        let updated = catalog
            .add_table_column(
                table.id,
                ParsedColumnDef {
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: Some(PgType::Text),
                },
            )
            .unwrap();

        let toast_id = updated.toast_table_id.expect("hidden TOAST relation id");
        let toast = catalog
            .get_table(toast_id)
            .unwrap()
            .expect("hidden TOAST relation exists");
        assert_eq!(
            toast.relation_kind,
            RelationKind::Toast {
                base_table: updated.id
            }
        );
        assert_eq!(catalog.get_table_by_name(&toast.name).unwrap(), None);
    }

    #[test]
    fn dropping_owned_sequence_default_column_is_rejected() {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_sequence(
                "serial_col_seq".to_string(),
                SequenceOptions::default(),
                true,
            )
            .unwrap();
        let table = catalog
            .create_table(
                "t".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "serial_col".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: Some(common::ParsedDefault::OwnedNextval(
                            "serial_col_seq".to_string(),
                        )),
                        pg_type: Some(PgType::Int8),
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();

        let err = catalog
            .drop_table_column(table.id, "serial_col")
            .unwrap_err();

        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
        assert!(
            catalog
                .get_sequence_by_name("serial_col_seq")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn reserve_ids_advance_allocators_without_installing_objects() {
        let catalog = MemoryCatalog::empty();

        catalog.reserve_table_id(9).unwrap();
        catalog.reserve_index_id(42).unwrap();
        catalog.reserve_sequence_id(11).unwrap();

        assert!(
            catalog.list_tables().unwrap().is_empty(),
            "reserving a table id must not install a table"
        );
        let table = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        assert_eq!(table.id, 10);

        let index = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(index.id, 43);

        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();
        assert_eq!(sequence.id, 12);
    }

    #[test]
    fn table_toast_and_index_storage_ids_are_distinct() {
        let catalog = MemoryCatalog::empty();
        let table = catalog
            .create_table_with_options(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "bio".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                CompressionSetting::None,
                ToastOptions::default_new_table(),
                Vec::new(),
            )
            .unwrap();
        let toast = catalog
            .get_table(table.toast_table_id.unwrap())
            .unwrap()
            .unwrap();
        let index = catalog
            .create_index(
                "users_bio".to_string(),
                "users",
                &["bio".to_string()],
                false,
            )
            .unwrap();

        assert_ne!(table.storage_id, toast.storage_id);
        assert_ne!(table.storage_id, index.storage_id);
        assert_ne!(toast.storage_id, index.storage_id);
    }

    #[test]
    fn legacy_snapshot_missing_storage_ids_preserves_file_ids_by_kind() {
        let json = r#"{
            "tables_by_name": {"users": 1},
            "tables_by_id": {"1": {
                "id": 1,
                "name": "users",
                "columns": [{"id": 0, "name": "id", "data_type": "Integer", "nullable": false}],
                "primary_key": []
            }},
            "next_table_id": 2,
            "indexes_by_name": {"users_id": 1},
            "indexes_by_id": {"1": {
                "id": 1,
                "table": 1,
                "name": "users_id",
                "columns": [0],
                "unique": true
            }},
            "next_index_id": 2
        }"#;

        let catalog =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(json.as_bytes()).unwrap())
                .unwrap();

        let table = catalog.get_table_by_name("users").unwrap().unwrap();
        let index = catalog.get_index_by_name("users_id").unwrap().unwrap();
        assert_eq!(table.storage_id, 1);
        assert_eq!(index.storage_id, 1);
        assert_eq!(catalog.snapshot().unwrap().next_storage_id, 2);
    }

    #[test]
    fn prepare_truncate_allocates_without_publishing_storage_ids() {
        let catalog = catalog_with_users();
        let table = catalog.get_table_by_name("users").unwrap().unwrap();
        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        let plan = catalog.prepare_truncate_table(table.id).unwrap();

        assert_eq!(plan.table_id, table.id);
        assert_ne!(plan.new_table_storage_id, table.storage_id);
        assert_eq!(plan.new_index_storage_ids.len(), 1);
        assert_eq!(plan.new_index_storage_ids[0].0, index.id);
        assert_ne!(plan.new_index_storage_ids[0].1, index.storage_id);
        assert_eq!(
            catalog.get_table(table.id).unwrap().unwrap().storage_id,
            table.storage_id
        );
        assert_eq!(
            catalog
                .get_index_by_name("users_name")
                .unwrap()
                .unwrap()
                .storage_id,
            index.storage_id
        );

        let next = catalog.allocate_storage_id().unwrap();
        assert!(next > plan.new_index_storage_ids[0].1);
    }

    #[test]
    fn build_truncate_update_does_not_publish_storage_ids() {
        let catalog = catalog_with_users();
        let table = catalog.get_table_by_name("users").unwrap().unwrap();
        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();
        let plan = catalog.prepare_truncate_table(table.id).unwrap();

        let update = catalog.build_truncate_table_update(&plan).unwrap();

        assert_eq!(update.table.storage_id, plan.new_table_storage_id);
        assert_eq!(
            update.indexes,
            vec![IndexSchema {
                storage_id: plan.new_index_storage_ids[0].1,
                ..index.clone()
            }]
        );
        assert_eq!(
            catalog.get_table(table.id).unwrap().unwrap().storage_id,
            table.storage_id
        );
        assert_eq!(
            catalog
                .get_index_by_name("users_name")
                .unwrap()
                .unwrap()
                .storage_id,
            index.storage_id
        );
    }

    #[test]
    fn truncate_update_rejects_reusing_current_storage_ids() {
        let catalog = catalog_with_users();
        let table = catalog.get_table_by_name("users").unwrap().unwrap();
        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        let mut plan = catalog.prepare_truncate_table(table.id).unwrap();
        plan.new_table_storage_id = table.storage_id;
        assert!(catalog.build_truncate_table_update(&plan).is_err());

        let mut plan = catalog.prepare_truncate_table(table.id).unwrap();
        plan.new_index_storage_ids = vec![(index.id, index.storage_id)];
        assert!(catalog.build_truncate_table_update(&plan).is_err());
    }

    #[test]
    fn apply_truncate_swaps_only_storage_ids() {
        let catalog = MemoryCatalog::empty();
        let table = catalog
            .create_table_with_options(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "bio".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                CompressionSetting::None,
                ToastOptions::default_new_table(),
                Vec::new(),
            )
            .unwrap();
        let toast = catalog
            .get_table(table.toast_table_id.unwrap())
            .unwrap()
            .unwrap();
        let index = catalog
            .create_index("users_bio".to_string(), "users", &["bio".to_string()], true)
            .unwrap();
        let plan = catalog.prepare_truncate_table(table.id).unwrap();

        let update = catalog.apply_truncate_table(&plan).unwrap();

        let mut expected_table = table.clone();
        expected_table.storage_id = plan.new_table_storage_id;
        assert_eq!(update.table, expected_table);
        assert_eq!(catalog.get_table(table.id).unwrap(), Some(expected_table));

        let (toast_id, new_toast_storage_id) = plan.new_toast_storage_id.unwrap();
        assert_eq!(toast_id, toast.id);
        let mut expected_toast = toast.clone();
        expected_toast.storage_id = new_toast_storage_id;
        assert_eq!(update.toast_table, Some(expected_toast.clone()));
        assert_eq!(catalog.get_table(toast.id).unwrap(), Some(expected_toast));

        let (index_id, new_index_storage_id) = plan.new_index_storage_ids[0];
        assert_eq!(index_id, index.id);
        let mut expected_index = index.clone();
        expected_index.storage_id = new_index_storage_id;
        assert_eq!(update.indexes, vec![expected_index.clone()]);
        assert_eq!(
            catalog.get_index_by_name("users_bio").unwrap(),
            Some(expected_index)
        );
    }

    #[test]
    fn apply_prebuilt_truncate_update_validates_then_publishes() {
        let catalog = catalog_with_users();
        let original = catalog.get_table_by_name("users").unwrap().unwrap();
        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();
        let plan = catalog.prepare_truncate_table(original.id).unwrap();
        let update = catalog.build_truncate_table_update(&plan).unwrap();

        let mut malformed = update.clone();
        malformed.table.name = "renamed_without_ddl".to_string();
        assert!(catalog.apply_truncate_updates(&[malformed]).is_err());
        assert_eq!(catalog.get_table(original.id).unwrap(), Some(original));
        assert_eq!(catalog.get_index(index.id).unwrap(), Some(index));

        catalog
            .apply_truncate_updates(std::slice::from_ref(&update))
            .unwrap();
        assert_eq!(
            catalog.get_table(update.table.id).unwrap(),
            Some(update.table)
        );
        assert_eq!(
            catalog.get_index(update.indexes[0].id).unwrap(),
            Some(update.indexes[0].clone())
        );
    }

    #[test]
    fn apply_truncate_tables_publishes_the_complete_batch() {
        let catalog = MemoryCatalog::empty();
        let first = catalog
            .create_table(
                "first".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        let second = catalog
            .create_table(
                "second".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        let plans = [
            catalog.prepare_truncate_table(first.id).unwrap(),
            catalog.prepare_truncate_table(second.id).unwrap(),
        ];

        let updates = catalog.apply_truncate_tables(&plans).unwrap();

        assert_eq!(updates.len(), 2);
        assert_eq!(
            catalog.get_table(first.id).unwrap().unwrap().storage_id,
            plans[0].new_table_storage_id
        );
        assert_eq!(
            catalog.get_table(second.id).unwrap().unwrap().storage_id,
            plans[1].new_table_storage_id
        );
    }

    #[test]
    fn apply_truncate_tables_rejects_batch_collisions_without_publishing() {
        let catalog = MemoryCatalog::empty();
        let first = catalog
            .create_table(
                "first".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        let second = catalog
            .create_table(
                "second".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        let first_plan = catalog.prepare_truncate_table(first.id).unwrap();
        let mut second_plan = catalog.prepare_truncate_table(second.id).unwrap();
        second_plan.new_table_storage_id = first_plan.new_table_storage_id;

        let err = catalog
            .apply_truncate_tables(&[first_plan.clone(), second_plan])
            .unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(catalog.get_table(first.id).unwrap(), Some(first.clone()));
        assert_eq!(catalog.get_table(second.id).unwrap(), Some(second.clone()));

        let err = catalog
            .apply_truncate_tables(&[first_plan.clone(), first_plan])
            .unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(catalog.get_table(first.id).unwrap(), Some(first));
        assert_eq!(catalog.get_table(second.id).unwrap(), Some(second));
    }

    #[test]
    fn create_sequence_assigns_defaults_and_drop_removes_it() {
        let catalog = MemoryCatalog::empty();

        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();

        assert_eq!(sequence.id, 1);
        assert_eq!(sequence.increment, 1);
        assert_eq!(sequence.min_value, 1);
        assert_eq!(sequence.max_value, i64::MAX);
        assert_eq!(sequence.start, 1);
        assert_eq!(sequence.last_value, 1);
        assert!(!sequence.is_called);
        assert!(!sequence.cycle);
        assert!(!sequence.owned);
        assert_eq!(
            catalog
                .get_sequence_by_name("users_id_seq")
                .unwrap()
                .unwrap()
                .id,
            sequence.id
        );
        assert_eq!(catalog.list_sequences().unwrap().len(), 1);

        catalog.drop_sequence(sequence.id).unwrap();
        assert!(catalog.get_sequence(sequence.id).unwrap().is_none());
        assert!(catalog.list_sequences().unwrap().is_empty());
    }

    #[test]
    fn create_sequence_normalizes_descending_defaults() {
        let catalog = MemoryCatalog::empty();

        let sequence = catalog
            .create_sequence(
                "descending_seq".to_string(),
                SequenceOptions {
                    increment: -5,
                    start: None,
                    min_value: None,
                    max_value: None,
                    cycle: true,
                },
                false,
            )
            .unwrap();

        assert_eq!(sequence.increment, -5);
        assert_eq!(sequence.min_value, i64::MIN);
        assert_eq!(sequence.max_value, -1);
        assert_eq!(sequence.start, -1);
        assert_eq!(sequence.last_value, -1);
        assert!(sequence.cycle);
    }

    #[test]
    fn create_sequence_rejects_invalid_options() {
        let catalog = MemoryCatalog::empty();

        for options in [
            SequenceOptions {
                increment: 0,
                ..SequenceOptions::default()
            },
            SequenceOptions {
                min_value: Some(10),
                max_value: Some(5),
                ..SequenceOptions::default()
            },
            SequenceOptions {
                start: Some(99),
                max_value: Some(10),
                ..SequenceOptions::default()
            },
        ] {
            let err = catalog
                .create_sequence("bad_seq".to_string(), options, false)
                .unwrap_err();
            assert_eq!(err.code, SqlState::InvalidParameterValue);
        }
    }

    #[test]
    fn sequence_snapshot_round_trips_and_preserves_allocator() {
        let catalog = MemoryCatalog::empty();
        let first = catalog
            .create_sequence(
                "s".to_string(),
                SequenceOptions {
                    increment: 2,
                    start: Some(5),
                    min_value: Some(1),
                    max_value: Some(100),
                    cycle: true,
                },
                false,
            )
            .unwrap();

        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();

        assert_eq!(
            restored.get_sequence_by_name("s").unwrap().unwrap(),
            SequenceSchema {
                id: first.id,
                schema_id: common::PUBLIC_SCHEMA_ID,
                name: "s".to_string(),
                increment: 2,
                min_value: 1,
                max_value: 100,
                start: 5,
                cycle: true,
                owned: false,
                last_value: 5,
                is_called: false,
            }
        );
        let next = restored
            .create_sequence("next_s".to_string(), SequenceOptions::default(), false)
            .unwrap();
        assert_eq!(next.id, first.id + 1);
    }

    #[test]
    fn restore_rejects_name_index_without_table() {
        let catalog = MemoryCatalog::empty();
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("ghost".to_string(), 7)]),
            tables_by_id: HashMap::new(),
            next_table_id: 1,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = catalog.restore(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("catalog snapshot"));
    }

    #[test]
    fn try_from_snapshot_rejects_next_table_id_that_reuses_existing_id() {
        let schema = TableSchema {
            id: 3,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: Vec::new(),
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            schemas_by_name: HashMap::from([("public".to_string(), common::PUBLIC_SCHEMA_ID)]),
            schemas_by_id: HashMap::from([(
                common::PUBLIC_SCHEMA_ID,
                common::NamespaceSchema {
                    id: common::PUBLIC_SCHEMA_ID,
                    name: "public".to_string(),
                },
            )]),
            next_schema_id: common::FIRST_USER_SCHEMA_ID,
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 3,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("next_table_id"));
    }

    #[test]
    fn validate_rejects_table_ids_that_cannot_have_unique_virtual_oids() {
        let bad_id = MAX_COMPOUND_OID_TABLE_ID + 1;
        let schema = stored_id_table(bad_id, "too_many_tables");
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("too_many_tables".to_string(), bad_id)]),
            tables_by_id: HashMap::from([(bad_id, schema)]),
            next_table_id: bad_id + 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("virtual OID payload limit"));
    }

    #[test]
    fn create_paths_reject_ids_that_cannot_have_unique_virtual_oids() {
        let table_catalog = MemoryCatalog::empty();
        table_catalog
            .reserve_table_id(MAX_COMPOUND_OID_TABLE_ID)
            .unwrap();
        let err = table_catalog
            .create_table(
                "too_many_tables".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap_err();
        assert!(err.message.contains("virtual OID payload limit"));

        let index_catalog = catalog_with_users();
        index_catalog
            .reserve_index_id(MAX_VIRTUAL_OID_PAYLOAD)
            .unwrap();
        let err = index_catalog
            .create_index(
                "too_many_indexes".to_string(),
                "users",
                &["id".to_string()],
                false,
            )
            .unwrap_err();
        assert!(err.message.contains("virtual OID payload limit"));

        let sequence_catalog = MemoryCatalog::empty();
        sequence_catalog
            .reserve_sequence_id(MAX_VIRTUAL_OID_PAYLOAD)
            .unwrap();
        let err = sequence_catalog
            .create_sequence(
                "too_many_sequences".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap_err();
        assert!(err.message.contains("virtual OID payload limit"));
    }

    #[test]
    fn try_from_snapshot_accepts_valid_sequence_default() {
        let sequence = SequenceSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            name: "users_id_seq".to_string(),
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            start: 1,
            cycle: false,
            owned: false,
            last_value: 1,
            is_called: false,
        };
        let schema = TableSchema {
            id: 3,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: Some(ColumnDefault::Nextval(1)),
                pg_type: None,
            }],
            primary_key: Vec::new(),
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            schemas_by_name: HashMap::from([("public".to_string(), common::PUBLIC_SCHEMA_ID)]),
            schemas_by_id: HashMap::from([(
                common::PUBLIC_SCHEMA_ID,
                common::NamespaceSchema {
                    id: common::PUBLIC_SCHEMA_ID,
                    name: "public".to_string(),
                },
            )]),
            next_schema_id: common::FIRST_USER_SCHEMA_ID,
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            sequences_by_name: HashMap::from([("users_id_seq".to_string(), 1)]),
            sequences_by_id: HashMap::from([(1, sequence)]),
            next_sequence_id: 2,
            next_dictionary_id: 1,
            next_storage_id: 4,
            views_by_name: HashMap::new(),
            views_by_id: HashMap::new(),
            statistics: HashMap::new(),
        };

        let catalog = MemoryCatalog::try_from_snapshot(snapshot).unwrap();
        assert_eq!(
            catalog.get_table_by_name("users").unwrap().unwrap().columns[0].default,
            Some(ColumnDefault::Nextval(1))
        );
    }

    #[test]
    fn apply_create_table_rejects_missing_sequence_default() {
        let catalog = MemoryCatalog::empty();
        let schema = TableSchema {
            id: 3,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: Some(ColumnDefault::Nextval(1)),
                pg_type: None,
            }],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };

        let err = catalog.apply_create_table(schema).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("references missing sequence"));
        assert_eq!(catalog.get_table_by_name("users").unwrap(), None);
    }

    #[test]
    fn create_table_resolves_parsed_nextval_default() {
        let catalog = MemoryCatalog::empty();
        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: Some(common::ParsedDefault::Nextval("users_id_seq".to_string())),
                    pg_type: None,
                }],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();

        assert_eq!(
            schema.columns[0].default,
            Some(ColumnDefault::Nextval(sequence.id))
        );
    }

    #[test]
    fn drop_sequence_rejects_referenced_default() {
        let catalog = MemoryCatalog::empty();
        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();
        catalog
            .create_table(
                "users".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: Some(common::ParsedDefault::Nextval("users_id_seq".to_string())),
                    pg_type: None,
                }],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();

        let err = catalog.drop_sequence(sequence.id).unwrap_err();

        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
        assert!(catalog.get_sequence(sequence.id).unwrap().is_some());
    }

    #[test]
    fn owned_sequence_drop_and_explicit_default_are_rejected() {
        let catalog = MemoryCatalog::empty();
        let sequence = catalog
            .create_sequence("users_id_seq".to_string(), SequenceOptions::default(), true)
            .unwrap();

        let err = catalog.drop_sequence(sequence.id).unwrap_err();
        assert_eq!(err.code, SqlState::DependentObjectsStillExist);

        let err = catalog
            .create_table(
                "borrower".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: Some(common::ParsedDefault::Nextval("users_id_seq".to_string())),
                    pg_type: None,
                }],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
    }

    #[test]
    fn create_table_accepts_internal_owned_nextval_default() {
        let catalog = MemoryCatalog::empty();
        let sequence = catalog
            .create_sequence("users_id_seq".to_string(), SequenceOptions::default(), true)
            .unwrap();
        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: Some(common::ParsedDefault::OwnedNextval(
                        "users_id_seq".to_string(),
                    )),
                    pg_type: None,
                }],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();

        assert_eq!(
            schema.columns[0].default,
            Some(ColumnDefault::Nextval(sequence.id))
        );
    }

    #[test]
    fn try_from_snapshot_accepts_composite_primary_key() {
        let schema = TableSchema {
            id: 3,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 3,
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 1,
                    name: "tenant".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            primary_key: vec![0, 1],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::from([("users_pkey".to_string(), 1)]),
            indexes_by_id: HashMap::from([(
                1,
                IndexSchema {
                    id: 1,
                    schema_id: common::PUBLIC_SCHEMA_ID,
                    storage_id: 4,
                    table: 3,
                    name: "users_pkey".to_string(),
                    columns: vec![0, 1],
                    unique: true,
                    constraint: common::IndexConstraintKind::PrimaryKey,
                },
            )]),
            next_index_id: 2,
            ..CatalogSnapshot::default()
        };

        let catalog = MemoryCatalog::try_from_snapshot(snapshot).unwrap();
        assert_eq!(
            catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap()
                .primary_key,
            vec![0, 1]
        );
    }

    #[test]
    fn try_from_snapshot_rejects_user_primary_key_without_constraint_index() {
        let schema = TableSchema {
            id: 3,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("no primary-key constraint index"));
    }

    #[test]
    fn try_from_snapshot_rejects_nullable_primary_key_column() {
        let schema = TableSchema {
            id: 3,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("primary key"));
    }

    #[test]
    fn try_from_snapshot_rejects_non_contiguous_column_ids() {
        let schema = TableSchema {
            id: 3,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: vec![1],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("column id"));
    }

    #[test]
    fn duplicate_primary_key_column_is_rejected_with_syntax_error() {
        let catalog = MemoryCatalog::empty();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string(), "id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn create_table_accepts_empty_primary_key() {
        let catalog = MemoryCatalog::empty();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec![],
                CompressionSetting::None,
            )
            .unwrap();

        assert!(schema.primary_key.is_empty());
    }

    #[test]
    fn set_table_primary_key_updates_columns_and_can_clear_key() {
        let catalog = MemoryCatalog::empty();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(true)],
                vec![],
                CompressionSetting::None,
            )
            .unwrap();
        assert!(schema.columns[0].nullable);

        let updated = catalog.set_table_primary_key(schema.id, vec![0]).unwrap();
        assert_eq!(updated.primary_key, vec![0]);
        assert!(!updated.columns[0].nullable);
        assert_eq!(updated.schema_version, schema.schema_version + 1);

        let cleared = catalog
            .set_table_primary_key(schema.id, Vec::new())
            .unwrap();
        assert!(cleared.primary_key.is_empty());
        assert_eq!(cleared.schema_version, updated.schema_version + 1);
        assert!(
            !cleared.columns[0].nullable,
            "dropping a primary key does not restore prior nullability"
        );
    }

    #[test]
    fn add_table_primary_key_index_sets_key_and_installs_constraint_index() {
        let catalog = MemoryCatalog::empty();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(true)],
                vec![],
                CompressionSetting::None,
            )
            .unwrap();
        let index = IndexSchema {
            id: 7,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 7,
            table: schema.id,
            name: "users_pkey".to_string(),
            columns: vec![0],
            unique: true,
            constraint: IndexConstraintKind::PrimaryKey,
        };

        let updated = catalog
            .add_table_primary_key_index(schema.id, vec![0], index.clone())
            .unwrap();

        assert_eq!(updated.primary_key, vec![0]);
        assert!(!updated.columns[0].nullable);
        assert_eq!(updated.schema_version, schema.schema_version + 1);
        assert_eq!(catalog.get_index(index.id).unwrap(), Some(index));
        assert_eq!(
            catalog.get_table(schema.id).unwrap().unwrap().primary_key,
            vec![0]
        );
    }

    #[test]
    fn add_table_primary_key_index_rejects_plain_index_metadata() {
        let catalog = MemoryCatalog::empty();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(true)],
                vec![],
                CompressionSetting::None,
            )
            .unwrap();
        let index = IndexSchema {
            id: 7,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 7,
            table: schema.id,
            name: "users_pkey".to_string(),
            columns: vec![0],
            unique: true,
            constraint: IndexConstraintKind::None,
        };

        let err = catalog
            .add_table_primary_key_index(schema.id, vec![0], index)
            .unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(
            catalog
                .get_table(schema.id)
                .unwrap()
                .unwrap()
                .primary_key
                .is_empty()
        );
        assert!(catalog.get_index_by_name("users_pkey").unwrap().is_none());
    }

    #[test]
    fn drop_table_primary_key_index_clears_key_and_removes_constraint_index() {
        let catalog = MemoryCatalog::empty();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        let index = catalog
            .create_index_with_constraint(
                "users_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();

        let updated = catalog
            .drop_table_primary_key_index(schema.id, index.id)
            .unwrap();

        assert!(updated.primary_key.is_empty());
        assert_eq!(updated.schema_version, schema.schema_version + 1);
        assert!(
            !updated.columns[0].nullable,
            "dropping a primary key does not restore prior nullability"
        );
        assert!(catalog.get_index(index.id).unwrap().is_none());
        assert!(
            catalog
                .get_table(schema.id)
                .unwrap()
                .unwrap()
                .primary_key
                .is_empty()
        );
    }

    #[test]
    fn create_table_accepts_composite_primary_key() {
        let catalog = MemoryCatalog::empty();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "tenant".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string(), "tenant".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();

        // Both columns are recorded as the composite key, in declared order, and
        // each is forced non-null.
        assert_eq!(schema.primary_key, vec![0, 1]);
        assert!(!schema.columns[0].nullable);
        assert!(!schema.columns[1].nullable);
    }

    #[test]
    fn primary_key_on_missing_column_is_rejected() {
        let catalog = MemoryCatalog::empty();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["missing".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn drop_removes_name_and_id_lookup_without_reusing_id() {
        let catalog = MemoryCatalog::empty();
        let users = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();

        catalog.drop_table(users.id).unwrap();

        assert_eq!(catalog.get_table(users.id).unwrap(), None);
        assert_eq!(catalog.get_table_by_name("users").unwrap(), None);

        let accounts = catalog
            .create_table(
                "accounts".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        assert_eq!(accounts.id, users.id + 1);
    }

    #[test]
    fn serialize_round_trip_preserves_next_table_id() {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();

        let snapshot = catalog.snapshot().unwrap();
        let bytes = serialize_catalog(&snapshot).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();

        restored
            .create_table(
                "accounts".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();

        assert_eq!(
            restored.get_table_by_name("accounts").unwrap().unwrap().id,
            2
        );
    }

    #[test]
    fn recovery_create_and_drop_update_catalog_by_ids() {
        let catalog = MemoryCatalog::empty();
        let schema = TableSchema {
            id: 7,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 7,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };

        catalog.apply_create_table(schema.clone()).unwrap();
        assert_eq!(catalog.get_table_by_name("users").unwrap(), Some(schema));

        catalog.apply_drop_table(7).unwrap();
        assert_eq!(catalog.get_table_by_name("users").unwrap(), None);
        assert_eq!(catalog.get_table(7).unwrap(), None);

        let next = catalog
            .create_table(
                "accounts".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        assert_eq!(next.id, 8);
    }

    #[test]
    fn create_index_resolves_columns_and_assigns_ids() {
        let catalog = catalog_with_users();
        let table = catalog.get_table_by_name("users").unwrap().unwrap();

        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        assert_eq!(index.id, 1);
        assert_eq!(index.table, table.id);
        assert_eq!(index.columns, vec![1]);
        assert!(!index.unique);

        let second = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], true)
            .unwrap();
        assert_eq!(second.id, 2);
        assert!(second.unique);
        assert_eq!(second.columns, vec![0]);
    }

    #[test]
    fn duplicate_index_name_is_rejected() {
        let catalog = catalog_with_users();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        let err = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["id".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::DuplicateTable);
    }

    #[test]
    fn synthetic_primary_key_index_name_is_reserved() {
        let catalog = catalog_with_users();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();

        let err = catalog
            .create_index(
                "users_pkey".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::DuplicateTable);

        let schema = IndexSchema {
            id: 99,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 99,
            table: users.id,
            name: "users_pkey".to_string(),
            columns: vec![1],
            unique: false,
            constraint: IndexConstraintKind::None,
        };
        let err = catalog.apply_create_index(schema).unwrap_err();
        assert_eq!(err.code, SqlState::DuplicateTable);
    }

    #[test]
    fn table_create_rejects_synthetic_primary_key_name_used_by_existing_index() {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "accounts".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        catalog
            .create_index(
                "users_pkey".to_string(),
                "accounts",
                &["name".to_string()],
                false,
            )
            .unwrap();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::DuplicateTable);

        let mut schema = stored_id_table(99, "users");
        schema.primary_key = vec![0];
        let err = catalog.apply_create_table(schema).unwrap_err();
        assert_eq!(err.code, SqlState::DuplicateTable);
    }

    #[test]
    fn create_index_on_missing_table_is_rejected() {
        let catalog = catalog_with_users();
        let err = catalog
            .create_index("ghost".to_string(), "ghost", &["id".to_string()], false)
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn create_index_on_missing_column_is_rejected() {
        let catalog = catalog_with_users();
        let err = catalog
            .create_index(
                "users_missing".to_string(),
                "users",
                &["missing".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn create_index_rejects_duplicate_and_empty_columns() {
        let catalog = catalog_with_users();

        let duplicate = catalog
            .create_index(
                "dup".to_string(),
                "users",
                &["id".to_string(), "id".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(duplicate.code, SqlState::SyntaxError);

        let empty = catalog
            .create_index("empty".to_string(), "users", &[], false)
            .unwrap_err();
        assert_eq!(empty.code, SqlState::SyntaxError);
    }

    #[test]
    fn get_index_by_name_returns_schema_or_none() {
        let catalog = catalog_with_users();
        let created = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        assert_eq!(
            catalog.get_index_by_name("users_name").unwrap(),
            Some(created)
        );
        assert_eq!(catalog.get_index_by_name("absent").unwrap(), None);
    }

    #[test]
    fn list_indexes_for_table_filters_and_sorts_by_id() {
        let catalog = catalog_with_users();
        let accounts = catalog
            .create_table(
                "accounts".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();
        catalog
            .create_index(
                "accounts_id".to_string(),
                "accounts",
                &["id".to_string()],
                false,
            )
            .unwrap();
        catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();

        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let listed = catalog.list_indexes_for_table(users.id).unwrap();
        let ids: Vec<_> = listed.iter().map(|index| index.id).collect();
        let names: Vec<_> = listed.iter().map(|index| index.name.as_str()).collect();
        assert_eq!(ids, vec![1, 3]);
        assert_eq!(names, vec!["users_name", "users_id"]);

        assert_eq!(
            catalog.list_indexes_for_table(accounts.id).unwrap().len(),
            1
        );
    }

    #[test]
    fn drop_index_removes_lookups_without_reusing_id() {
        let catalog = catalog_with_users();
        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        catalog.drop_index(index.id).unwrap();
        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);

        let next = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(next.id, index.id + 1);
    }

    #[test]
    fn drop_index_on_missing_id_is_rejected() {
        let catalog = catalog_with_users();
        let err = catalog.drop_index(42).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn drop_index_rejects_primary_key_constraint_index() {
        let catalog = catalog_with_users();
        let index = catalog
            .create_index_with_constraint(
                "users_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();

        let err = catalog.drop_index(index.id).unwrap_err();
        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
        assert!(catalog.get_index(index.id).unwrap().is_some());
    }

    #[test]
    fn create_index_with_constraint_rejects_invalid_constraint_metadata() {
        let catalog = catalog_with_users();

        let not_unique = catalog
            .create_index_with_constraint(
                "bad_pkey".to_string(),
                "users",
                &["id".to_string()],
                false,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap_err();
        assert_eq!(not_unique.code, SqlState::InternalError);
        assert_eq!(catalog.get_index_by_name("bad_pkey").unwrap(), None);

        let wrong_columns = catalog
            .create_index_with_constraint(
                "wrong_pkey".to_string(),
                "users",
                &["name".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap_err();
        assert_eq!(wrong_columns.code, SqlState::InternalError);
        assert_eq!(catalog.get_index_by_name("wrong_pkey").unwrap(), None);

        let unique_constraint_not_unique = catalog
            .create_index_with_constraint(
                "bad_unique".to_string(),
                "users",
                &["name".to_string()],
                false,
                IndexConstraintKind::Unique,
            )
            .unwrap_err();
        assert_eq!(unique_constraint_not_unique.code, SqlState::InternalError);
        assert_eq!(catalog.get_index_by_name("bad_unique").unwrap(), None);
    }

    #[test]
    fn create_index_with_constraint_rejects_duplicate_primary_key_constraint() {
        let catalog = catalog_with_users();
        catalog
            .create_index_with_constraint(
                "users_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();

        let err = catalog
            .create_index_with_constraint(
                "users_second_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(
            catalog.get_index_by_name("users_second_pkey").unwrap(),
            None
        );
    }

    #[test]
    fn drop_table_cascades_to_its_indexes() {
        let catalog = catalog_with_users();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();
        catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();

        catalog.drop_table(users.id).unwrap();

        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);
        assert_eq!(catalog.get_index_by_name("users_id").unwrap(), None);
        assert!(catalog.list_indexes_for_table(users.id).unwrap().is_empty());
    }

    #[test]
    fn apply_create_and_drop_index_drive_recovery_by_id() {
        let catalog = catalog_with_users();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let schema = IndexSchema {
            id: 5,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 5,
            table: users.id,
            name: "users_name".to_string(),
            columns: vec![1],
            unique: false,
            constraint: common::IndexConstraintKind::None,
        };

        catalog.apply_create_index(schema.clone()).unwrap();
        assert_eq!(
            catalog.get_index_by_name("users_name").unwrap(),
            Some(schema.clone())
        );

        let duplicate = catalog.apply_create_index(schema).unwrap_err();
        assert_eq!(duplicate.code, SqlState::DuplicateTable);

        // next_index_id advanced past the replayed id, so a fresh create skips it.
        let next = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(next.id, 6);

        catalog.apply_drop_index(5).unwrap();
        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);
    }

    #[test]
    fn apply_create_index_rejects_invalid_constraint_metadata() {
        let catalog = catalog_with_users();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let schema = IndexSchema {
            id: 5,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 5,
            table: users.id,
            name: "bad_pkey".to_string(),
            columns: vec![1],
            unique: true,
            constraint: common::IndexConstraintKind::PrimaryKey,
        };

        let err = catalog.apply_create_index(schema).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(catalog.get_index_by_name("bad_pkey").unwrap(), None);
    }

    #[test]
    fn apply_create_index_rejects_duplicate_primary_key_constraint() {
        let catalog = catalog_with_users();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        catalog
            .create_index_with_constraint(
                "users_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();

        let schema = IndexSchema {
            id: 5,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 5,
            table: users.id,
            name: "users_second_pkey".to_string(),
            columns: vec![0],
            unique: true,
            constraint: common::IndexConstraintKind::PrimaryKey,
        };

        let err = catalog.apply_create_index(schema).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(
            catalog.get_index_by_name("users_second_pkey").unwrap(),
            None
        );
    }

    #[test]
    fn serialize_round_trip_preserves_indexes() {
        let catalog = catalog_with_users();
        catalog
            .create_index_with_constraint(
                "users_pkey".to_string(),
                "users",
                &["id".to_string()],
                true,
                IndexConstraintKind::PrimaryKey,
            )
            .unwrap();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                true,
            )
            .unwrap();

        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();

        let index = restored.get_index_by_name("users_name").unwrap().unwrap();
        assert!(index.unique);
        assert_eq!(index.columns, vec![1]);

        // next_index_id survives the round trip, so ids keep climbing.
        let next = restored
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(next.id, 3);
    }

    #[test]
    fn validate_rejects_index_referencing_missing_table() {
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::new(),
            tables_by_id: HashMap::new(),
            next_table_id: 1,
            indexes_by_name: HashMap::from([("orphan".to_string(), 1)]),
            indexes_by_id: HashMap::from([(
                1,
                IndexSchema {
                    id: 1,
                    schema_id: common::PUBLIC_SCHEMA_ID,
                    storage_id: 1,
                    table: 7,
                    name: "orphan".to_string(),
                    columns: vec![0],
                    unique: false,
                    constraint: common::IndexConstraintKind::None,
                },
            )]),
            next_index_id: 2,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("missing table"));
    }

    #[test]
    fn validate_rejects_reserved_primary_key_index_id() {
        let table = TableSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 1,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, table)]),
            next_table_id: 2,
            indexes_by_name: HashMap::from([("bad".to_string(), 0)]),
            indexes_by_id: HashMap::from([(
                0,
                IndexSchema {
                    id: 0,
                    schema_id: common::PUBLIC_SCHEMA_ID,
                    storage_id: 2,
                    table: 1,
                    name: "bad".to_string(),
                    columns: vec![0],
                    unique: false,
                    constraint: common::IndexConstraintKind::None,
                },
            )]),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("reserved storage identity index id"));
    }

    #[test]
    fn validate_rejects_next_index_id_that_reuses_existing_id() {
        let table = TableSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 1,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, table)]),
            next_table_id: 2,
            indexes_by_name: HashMap::from([("users_id".to_string(), 1)]),
            indexes_by_id: HashMap::from([(
                1,
                IndexSchema {
                    id: 1,
                    schema_id: common::PUBLIC_SCHEMA_ID,
                    storage_id: 2,
                    table: 1,
                    name: "users_id".to_string(),
                    columns: vec![0],
                    unique: false,
                    constraint: common::IndexConstraintKind::None,
                },
            )]),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("next_index_id"));
    }

    #[test]
    fn snapshot_without_index_fields_deserializes_to_empty_indexes() {
        // A catalog persisted before secondary indexes and sequences existed.
        let json = r#"{
            "tables_by_name": {"users": 1},
            "tables_by_id": {"1": {
                "id": 1,
                "name": "users",
                "columns": [{"id": 0, "name": "id", "data_type": "Integer", "nullable": false}],
                "primary_key": []
            }},
            "next_table_id": 2
        }"#;

        let snapshot = deserialize_catalog(json.as_bytes()).unwrap();
        assert!(snapshot.indexes_by_id.is_empty());
        assert!(snapshot.indexes_by_name.is_empty());
        assert_eq!(snapshot.next_index_id, 1);
        assert!(snapshot.sequences_by_id.is_empty());
        assert!(snapshot.sequences_by_name.is_empty());
        assert_eq!(snapshot.next_sequence_id, 1);

        // A column persisted before the pg_type field loads as unlabeled and
        // resolves to the collapsed default wire type (Integer => int8).
        let column = &snapshot.tables_by_id[&1].columns[0];
        assert_eq!(column.pg_type, None);
        assert_eq!(column.wire_type(), PgType::Int8);

        // The validated load path accepts it.
        MemoryCatalog::try_from_snapshot(snapshot).unwrap();
    }

    #[test]
    fn create_table_stores_compression_setting() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::Zstd,
            )
            .unwrap();
        assert_eq!(schema.compression, CompressionSetting::Zstd);
        assert_eq!(schema.active_dict_id, None);
    }

    #[test]
    fn create_table_with_options_stores_toast_options_and_hidden_relation() {
        let catalog = MemoryCatalog::empty();
        let mut toast = ToastOptions::default_new_table();
        toast.tuple_target = 4096;
        let schema = catalog
            .create_table_with_options(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "bio".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                Vec::new(),
                CompressionSetting::None,
                toast.clone(),
                Vec::new(),
            )
            .unwrap();

        assert_eq!(schema.toast, toast);
        assert_eq!(schema.toast_table_id, Some(2));
        let hidden = catalog.get_table(2).unwrap().unwrap();
        assert_eq!(hidden.name, "\0toast_1");
        assert_eq!(hidden.relation_kind, RelationKind::Toast { base_table: 1 });
        assert_eq!(hidden.compression, CompressionSetting::None);
        assert_eq!(hidden.toast, ToastOptions::legacy_catalog_default());
        assert_eq!(hidden.primary_key, vec![0, 1]);
        assert_eq!(
            hidden
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["value_id", "seq", "data"]
        );
        assert_eq!(catalog.get_table_by_name("\0toast_1").unwrap(), None);

        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();
        assert_eq!(
            restored
                .get_table_by_name("users")
                .unwrap()
                .unwrap()
                .toast
                .tuple_target,
            4096
        );
        assert_eq!(
            restored.get_table(2).unwrap().unwrap().relation_kind,
            RelationKind::Toast { base_table: 1 }
        );
    }

    #[test]
    fn create_table_with_options_skips_hidden_relation_without_toastable_columns() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table_with_options(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
                ToastOptions::default_new_table(),
                Vec::new(),
            )
            .unwrap();

        assert_eq!(schema.toast, ToastOptions::default_new_table());
        assert_eq!(schema.toast_table_id, None);
        assert_eq!(catalog.get_table(2).unwrap(), None);
    }

    #[test]
    fn set_table_compression_updates_and_persists() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let updated = catalog
            .set_table_compression(schema.id, CompressionSetting::Zstd, Some(3))
            .unwrap();
        assert_eq!(updated.compression, CompressionSetting::Zstd);
        assert_eq!(updated.active_dict_id, Some(3));
        // Round-trips through the snapshot.
        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();
        assert_eq!(
            restored
                .get_table(schema.id)
                .unwrap()
                .unwrap()
                .active_dict_id,
            Some(3)
        );
    }

    #[test]
    fn set_table_toast_metadata_updates_options_and_reserves_dictionary_id() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
                CompressionSetting::None,
            )
            .unwrap();
        let mut toast = ToastOptions::default_new_table();
        toast.compression = ToastCompression::ZstdDict;
        toast.active_dict_id = Some(7);

        let updated = catalog
            .set_table_toast_metadata(schema.id, toast.clone(), None)
            .unwrap();
        assert_eq!(updated.toast, toast);

        let next = catalog.allocate_dictionary_id().unwrap();
        assert_eq!(next, 8);
    }

    #[test]
    fn validate_rejects_invalid_toast_option_bounds() {
        let mut table = stored_id_table(1, "users");
        table.toast.tuple_target = ToastOptions::MIN_TOAST_TUPLE_TARGET - 1;
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, table)]),
            next_table_id: 2,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("toast tuple_target"));
    }

    #[test]
    fn validate_rejects_invalid_toast_dictionary_id() {
        let mut table = stored_id_table(1, "users");
        table.toast.compression = ToastCompression::ZstdDict;
        table.toast.active_dict_id = Some(2);
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, table)]),
            next_table_id: 2,
            next_dictionary_id: 2,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("toast active_dict_id"));
    }

    #[test]
    fn validate_rejects_invalid_toast_relation_link() {
        let mut base = stored_id_table(1, "users");
        base.toast = ToastOptions::default_new_table();
        base.toast.mode = ToastMode::Auto;
        base.toast_table_id = Some(2);
        let unrelated = stored_id_table(2, "other");
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1), ("other".to_string(), 2)]),
            tables_by_id: HashMap::from([(1, base), (2, unrelated)]),
            next_table_id: 3,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("non-matching TOAST relation"));
    }

    #[test]
    fn validate_accepts_hidden_toast_relation_without_name_index() {
        let mut base = stored_id_table(1, "users");
        base.toast_table_id = Some(2);
        let toast = toast_schema(&base, 2);
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, base), (2, toast)]),
            next_table_id: 3,
            ..CatalogSnapshot::default()
        };

        MemoryCatalog::try_from_snapshot(snapshot).unwrap();
    }

    #[test]
    fn validate_accepts_hidden_toast_storage_id_change() {
        let mut base = stored_id_table(1, "users");
        base.toast_table_id = Some(2);
        let mut toast = toast_schema(&base, 2);
        toast.storage_id = 7;
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, base), (2, toast)]),
            next_table_id: 3,
            next_storage_id: 8,
            ..CatalogSnapshot::default()
        };

        MemoryCatalog::try_from_snapshot(snapshot).unwrap();
    }

    #[test]
    fn validate_rejects_malformed_hidden_toast_schema() {
        let mut base = stored_id_table(1, "users");
        base.toast_table_id = Some(2);
        let mut toast = toast_schema(&base, 2);
        toast.columns[2].data_type = DataType::Text;
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, base), (2, toast)]),
            next_table_id: 3,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("required internal schema"));
    }

    #[test]
    fn apply_create_table_installs_hidden_toast_relation_by_id_only() {
        let catalog = MemoryCatalog::empty();
        let mut base = stored_id_table(1, "users");
        base.toast_table_id = Some(2);
        let toast = toast_schema(&base, 2);

        catalog.apply_create_table(base).unwrap();
        catalog.apply_create_table(toast).unwrap();

        let snapshot = catalog.snapshot().unwrap();
        assert_eq!(
            snapshot.tables_by_name,
            HashMap::from([("users".to_string(), 1)])
        );
        MemoryCatalog::try_from_snapshot(snapshot).unwrap();
    }

    #[test]
    fn apply_drop_table_cascades_to_hidden_toast_relation() {
        let catalog = MemoryCatalog::empty();
        let mut base = stored_id_table(1, "users");
        base.toast_table_id = Some(2);
        let toast = toast_schema(&base, 2);

        catalog.apply_create_table(base).unwrap();
        catalog.apply_create_table(toast).unwrap();
        catalog.apply_drop_table(1).unwrap();

        let snapshot = catalog.snapshot().unwrap();
        assert!(snapshot.tables_by_name.is_empty());
        assert!(snapshot.tables_by_id.is_empty());
        MemoryCatalog::try_from_snapshot(snapshot).unwrap();
    }

    #[test]
    fn apply_drop_table_rejects_direct_hidden_toast_drop_while_base_links_it() {
        let catalog = MemoryCatalog::empty();
        let mut base = stored_id_table(1, "users");
        base.toast_table_id = Some(2);
        let toast = toast_schema(&base, 2);

        catalog.apply_create_table(base).unwrap();
        catalog.apply_create_table(toast).unwrap();
        let err = catalog.apply_drop_table(2).unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("cannot drop hidden TOAST relation"));
        MemoryCatalog::try_from_snapshot(catalog.snapshot().unwrap()).unwrap();
    }

    #[test]
    fn dictionary_ids_allocate_monotonically_and_survive_reserve() {
        let catalog = MemoryCatalog::empty();
        assert_eq!(catalog.allocate_dictionary_id().unwrap(), 1);
        assert_eq!(catalog.allocate_dictionary_id().unwrap(), 2);
        catalog.reserve_dictionary_id(10).unwrap();
        assert_eq!(catalog.allocate_dictionary_id().unwrap(), 11);
        // Reserving below the mark never rewinds it.
        catalog.reserve_dictionary_id(3).unwrap();
        assert_eq!(catalog.allocate_dictionary_id().unwrap(), 12);
    }

    #[test]
    fn snapshot_without_dictionary_field_defaults_next_id_to_one() {
        // Mirror snapshot_without_index_fields_deserializes_to_empty_indexes:
        // a catalog persisted before compression/dictionary ids existed.
        let json = r#"{
            "tables_by_name": {"users": 1},
            "tables_by_id": {"1": {
                "id": 1,
                "name": "users",
                "columns": [{"id": 0, "name": "id", "data_type": "Integer", "nullable": false}],
                "primary_key": []
            }},
            "next_table_id": 2
        }"#;

        let snapshot = deserialize_catalog(json.as_bytes()).unwrap();
        assert_eq!(snapshot.next_dictionary_id, 1);

        let catalog = MemoryCatalog::try_from_snapshot(snapshot).unwrap();
        assert_eq!(catalog.allocate_dictionary_id().unwrap(), 1);
    }

    #[test]
    fn catalog_v2_persists_schemas_deterministically_and_migrates_legacy_objects() {
        let catalog = MemoryCatalog::empty();
        catalog
            .apply_create_schema(common::NamespaceSchema {
                id: common::FIRST_USER_SCHEMA_ID,
                name: "app".to_string(),
            })
            .unwrap();
        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["version"], 2);
        assert_eq!(value["schemas"][0]["name"], "public");
        assert_eq!(value["schemas"][1]["name"], "app");
        assert_eq!(
            bytes,
            serialize_catalog(&catalog.snapshot().unwrap()).unwrap()
        );

        let legacy = serde_json::json!({
            "tables_by_name": {},
            "tables_by_id": {},
            "next_table_id": 1
        });
        let migrated = deserialize_catalog(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert_eq!(
            migrated.schemas_by_name.get("public"),
            Some(&common::PUBLIC_SCHEMA_ID)
        );
        assert_eq!(migrated.next_schema_id, common::FIRST_USER_SCHEMA_ID);
    }

    #[test]
    fn schema_catalog_rejects_duplicates_and_public_drop() {
        let catalog = MemoryCatalog::empty();
        let app = common::NamespaceSchema {
            id: common::FIRST_USER_SCHEMA_ID,
            name: "app".to_string(),
        };
        catalog.apply_create_schema(app.clone()).unwrap();
        assert_eq!(
            catalog.get_schema_by_name("app").unwrap(),
            Some(app.clone())
        );
        let duplicate = catalog.apply_create_schema(common::NamespaceSchema {
            id: app.id + 1,
            name: app.name,
        });
        assert_eq!(duplicate.unwrap_err().code, SqlState::DuplicateSchema);
        assert_eq!(
            catalog
                .apply_drop_schema(common::PUBLIC_SCHEMA_ID)
                .unwrap_err()
                .code,
            SqlState::InsufficientPrivilege
        );
        catalog.apply_drop_schema(app.id).unwrap();
        assert!(catalog.get_schema(app.id).unwrap().is_none());
    }

    #[test]
    fn same_relation_name_is_valid_in_distinct_schemas() {
        let catalog = MemoryCatalog::empty();
        catalog
            .apply_create_schema(common::NamespaceSchema {
                id: common::FIRST_USER_SCHEMA_ID,
                name: "app".to_string(),
            })
            .unwrap();
        let public_table = stored_id_table(1, "items");
        let mut app_table = stored_id_table(2, "items");
        app_table.schema_id = common::FIRST_USER_SCHEMA_ID;
        catalog.apply_create_table(public_table).unwrap();
        catalog.apply_create_table(app_table).unwrap();

        let restored = MemoryCatalog::try_from_snapshot(
            deserialize_catalog(&serialize_catalog(&catalog.snapshot().unwrap()).unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(restored.list_tables().unwrap().len(), 2);
        assert_eq!(restored.get_table_by_name("items").unwrap().unwrap().id, 1);
    }

    #[test]
    fn catalog_v2_rejects_duplicate_object_ids() {
        let snapshot = MemoryCatalog::empty().snapshot().unwrap();
        let bytes = serialize_catalog(&snapshot).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["schemas"] = serde_json::json!([
            {"id": 1, "name": "public"},
            {"id": 1, "name": "duplicate"}
        ]);
        assert!(deserialize_catalog(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn schema_validation_rejects_reserved_and_oversized_ids() {
        let catalog = MemoryCatalog::empty();
        for id in [0, MAX_VIRTUAL_OID_PAYLOAD + 1] {
            assert!(
                catalog
                    .apply_create_schema(common::NamespaceSchema {
                        id,
                        name: format!("schema_{id}"),
                    })
                    .is_err()
            );
        }
    }

    #[test]
    fn apply_paths_reject_missing_object_schemas_and_view_moves() {
        let catalog = MemoryCatalog::empty();
        let missing_schema = 99;

        let mut table = stored_id_table(1, "items");
        table.schema_id = missing_schema;
        assert!(catalog.apply_create_table(table).is_err());

        let sequence = SequenceSchema {
            id: 1,
            schema_id: missing_schema,
            name: "items_seq".to_string(),
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            start: 1,
            cycle: false,
            owned: false,
            last_value: 1,
            is_called: false,
        };
        assert!(catalog.apply_create_sequence(sequence).is_err());

        catalog
            .create_view(
                "items_view".to_string(),
                vec![view_column("id", DataType::Integer, true)],
                "select 1".to_string(),
                Vec::new(),
            )
            .unwrap();
        let mut view = catalog.get_view_by_name("items_view").unwrap().unwrap();
        view.schema_id = missing_schema;
        let mut new_view = view.clone();
        new_view.id += 1;
        assert!(catalog.apply_create_view(new_view).is_err());
        assert!(catalog.apply_replace_view(view).is_err());
    }

    #[test]
    fn batch_schema_update_cannot_reassign_an_existing_index() {
        let catalog = MemoryCatalog::empty();
        let first = catalog
            .create_table(
                "first".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let second = catalog
            .create_table(
                "second".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let mut index = catalog
            .create_index(
                "second_id_idx".to_string(),
                "second",
                &["id".to_string()],
                false,
            )
            .unwrap();
        assert_eq!(index.table, second.id);
        index.table = first.id;
        assert!(
            catalog
                .apply_update_table_and_index_schemas(first, &[index])
                .is_err()
        );
    }

    #[test]
    fn schema_scoped_creation_and_lookups_allow_matching_names() {
        let catalog = MemoryCatalog::empty();
        let app = catalog.create_schema("app".to_string()).unwrap();
        let public_table = catalog
            .create_table(
                "items".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let app_table = catalog
            .create_table_in_schema_with_options(
                app.id,
                "items".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
                ToastOptions::legacy_catalog_default(),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(
            catalog.get_table_by_name("items").unwrap(),
            Some(public_table)
        );
        assert_eq!(
            catalog.get_table_in_schema(app.id, "items").unwrap(),
            Some(app_table.clone())
        );

        let public_sequence = catalog
            .create_sequence("item_ids".to_string(), SequenceOptions::default(), false)
            .unwrap();
        let app_sequence = catalog
            .create_sequence_in_schema(
                app.id,
                "item_ids".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();
        assert_eq!(
            catalog.get_sequence_by_name("item_ids").unwrap(),
            Some(public_sequence)
        );
        assert_eq!(
            catalog.get_sequence_in_schema(app.id, "item_ids").unwrap(),
            Some(app_sequence)
        );

        let app_index = catalog
            .create_index_in_schema_with_constraint(
                app.id,
                "items_id_idx".to_string(),
                app_table.id,
                &["id".to_string()],
                false,
                IndexConstraintKind::None,
            )
            .unwrap();
        assert_eq!(
            catalog.get_index_in_schema(app.id, "items_id_idx").unwrap(),
            Some(app_index)
        );
        assert!(catalog.get_index_by_name("items_id_idx").unwrap().is_none());

        catalog
            .create_view_in_schema(
                app.id,
                "item_view".to_string(),
                vec![view_column("id", DataType::Integer, false)],
                "SELECT id FROM items".to_string(),
                vec![ViewDependency {
                    relation: app_table.id,
                    columns: vec![0],
                    all_columns: false,
                }],
                vec![app.id, common::PUBLIC_SCHEMA_ID],
            )
            .unwrap();
        assert!(
            catalog
                .get_view_in_schema(app.id, "item_view")
                .unwrap()
                .is_some()
        );
        assert!(catalog.get_view_by_name("item_view").unwrap().is_none());
    }

    #[test]
    fn drop_schema_restricts_owned_objects_and_view_search_path_dependencies() {
        let catalog = MemoryCatalog::empty();
        let app = catalog.create_schema("app".to_string()).unwrap();
        let table = catalog
            .create_table_in_schema_with_options(
                app.id,
                "items".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
                ToastOptions::legacy_catalog_default(),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(
            catalog.drop_schema(app.id).unwrap_err().code,
            SqlState::DependentObjectsStillExist
        );
        catalog.drop_table(table.id).unwrap();

        let view = catalog
            .create_view_in_schema(
                common::PUBLIC_SCHEMA_ID,
                "uses_app_path".to_string(),
                vec![view_column("id", DataType::Integer, false)],
                "SELECT 1 AS id".to_string(),
                Vec::new(),
                vec![app.id, common::PUBLIC_SCHEMA_ID],
            )
            .unwrap();
        assert_eq!(
            catalog.drop_schema(app.id).unwrap_err().code,
            SqlState::DependentObjectsStillExist
        );
        catalog.drop_view(view.id).unwrap();
        catalog.drop_schema(app.id).unwrap();
    }

    #[test]
    fn foreign_keys_attach_generate_names_list_incoming_and_drop_without_reusing_ids() {
        let (catalog, parent, child) = foreign_key_catalog();
        let schema = catalog
            .attach_foreign_keys(
                child,
                vec![
                    resolved_foreign_key(None, parent),
                    resolved_foreign_key(None, parent),
                ],
            )
            .unwrap();
        assert_eq!(schema.next_foreign_key_id, 2);
        assert_eq!(schema.foreign_keys[0].id, 0);
        assert_eq!(schema.foreign_keys[0].name, "children_parent_id_fkey");
        assert_eq!(schema.foreign_keys[1].id, 1);
        assert_eq!(schema.foreign_keys[1].name, "children_parent_id_fkey1");
        assert_eq!(
            catalog.resolve_foreign_key_index(parent, &[0]).unwrap(),
            Some(common::PRIMARY_KEY_INDEX_ID)
        );
        let incoming = catalog.list_incoming_foreign_keys(parent).unwrap();
        assert_eq!(incoming.len(), 2);
        assert_eq!(incoming[0].0.id, child);
        assert_eq!(incoming[0].1.id, 0);

        let dropped = catalog
            .drop_foreign_key(child, "children_parent_id_fkey", false)
            .unwrap()
            .unwrap();
        assert_eq!(dropped.next_foreign_key_id, 2);
        assert_eq!(dropped.foreign_keys.len(), 1);
        assert!(
            catalog
                .drop_foreign_key(child, "missing", true)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            catalog
                .drop_foreign_key(child, "missing", false)
                .unwrap_err()
                .code,
            SqlState::UndefinedObject
        );
    }

    #[test]
    fn foreign_key_attach_is_atomic_and_validates_types_columns_names_and_allocator() {
        let (catalog, parent, child) = foreign_key_catalog();
        let mut invalid = resolved_foreign_key(Some("bad"), parent);
        invalid.columns = vec![9];
        let err = catalog
            .attach_foreign_keys(
                child,
                vec![resolved_foreign_key(Some("valid"), parent), invalid],
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
        let unchanged = catalog.get_table(child).unwrap().unwrap();
        assert!(unchanged.foreign_keys.is_empty());
        assert_eq!(unchanged.next_foreign_key_id, 0);

        let mut snapshot = catalog.snapshot().unwrap();
        snapshot
            .tables_by_id
            .get_mut(&child)
            .unwrap()
            .next_foreign_key_id = 4095;
        let exhausted = MemoryCatalog::try_from_snapshot(snapshot).unwrap();
        let last = exhausted
            .attach_foreign_keys(child, vec![resolved_foreign_key(Some("last"), parent)])
            .unwrap();
        assert_eq!(last.foreign_keys[0].id, 4095);
        assert_eq!(last.next_foreign_key_id, 4096);
        assert_eq!(
            exhausted
                .attach_foreign_keys(child, vec![resolved_foreign_key(Some("too_many"), parent)])
                .unwrap_err()
                .code,
            SqlState::ProgramLimitExceeded
        );
    }

    #[test]
    fn foreign_key_resolution_accepts_declared_unique_only_and_finds_exact_child_index() {
        let catalog = MemoryCatalog::empty();
        let parent = catalog
            .create_table(
                "parents".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        catalog
            .create_index(
                "parents_id_standalone".to_string(),
                "parents",
                &["id".to_string()],
                true,
            )
            .unwrap();
        assert_eq!(
            catalog.resolve_foreign_key_index(parent.id, &[0]).unwrap(),
            None
        );
        let unique = catalog
            .create_index_with_constraint(
                "parents_id_key".to_string(),
                "parents",
                &["id".to_string()],
                true,
                IndexConstraintKind::Unique,
            )
            .unwrap();
        assert_eq!(
            catalog.resolve_foreign_key_index(parent.id, &[0]).unwrap(),
            Some(unique.id)
        );

        let child = catalog
            .create_table(
                "children".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let supporting = catalog
            .create_index(
                "children_id_idx".to_string(),
                "children",
                &["id".to_string()],
                false,
            )
            .unwrap();
        assert_eq!(
            catalog
                .find_foreign_key_supporting_index(child.id, &[0])
                .unwrap(),
            Some(supporting.id)
        );
        assert_eq!(
            catalog
                .find_foreign_key_supporting_index(child.id, &[])
                .unwrap(),
            None
        );
    }

    #[test]
    fn malformed_foreign_key_snapshot_is_rejected_and_legacy_metadata_defaults_empty() {
        let (catalog, parent, child) = foreign_key_catalog();
        catalog
            .attach_foreign_keys(
                child,
                vec![resolved_foreign_key(Some("child_parent"), parent)],
            )
            .unwrap();
        let mut malformed = catalog.snapshot().unwrap();
        malformed
            .tables_by_id
            .get_mut(&child)
            .unwrap()
            .next_foreign_key_id = 0;
        let err = MemoryCatalog::try_from_snapshot(malformed).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);

        let legacy = MemoryCatalog::empty();
        legacy
            .create_table(
                "legacy".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let bytes = serialize_catalog(&legacy.snapshot().unwrap()).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let table = value
            .get_mut("tables")
            .and_then(serde_json::Value::as_array_mut)
            .and_then(|tables| tables.first_mut())
            .and_then(serde_json::Value::as_object_mut)
            .unwrap();
        table.remove("foreign_keys");
        table.remove("next_foreign_key_id");
        let decoded = deserialize_catalog(&serde_json::to_vec(&value).unwrap()).unwrap();
        let loaded = MemoryCatalog::try_from_snapshot(decoded).unwrap();
        let schema = loaded.get_table_by_name("legacy").unwrap().unwrap();
        assert!(schema.foreign_keys.is_empty());
        assert_eq!(schema.next_foreign_key_id, 0);
    }

    #[test]
    fn foreign_key_oid_uses_a_dedicated_stable_namespace() {
        let oid = foreign_key_constraint_oid(42, 7).unwrap();
        assert_eq!(oid, foreign_key_constraint_oid(42, 7).unwrap());
        assert_ne!(oid, check_constraint_oid(42, 7).unwrap());
        assert_ne!(oid, crate::primary_key_constraint_oid(42).unwrap());
        assert!(foreign_key_constraint_oid(MAX_COMPOUND_OID_TABLE_ID + 1, 0).is_err());
    }

    #[test]
    fn restore_preserves_foreign_key_allocator_high_water() {
        let (catalog, parent, child) = foreign_key_catalog();
        let before = catalog.snapshot().unwrap();
        catalog
            .attach_foreign_keys(child, vec![resolved_foreign_key(Some("first"), parent)])
            .unwrap();
        catalog.drop_foreign_key(child, "first", false).unwrap();
        catalog.restore(before).unwrap();
        let restored = catalog.get_table(child).unwrap().unwrap();
        assert_eq!(restored.next_foreign_key_id, 1);
        let attached = catalog
            .attach_foreign_keys(child, vec![resolved_foreign_key(Some("second"), parent)])
            .unwrap();
        assert_eq!(attached.foreign_keys[0].id, 1);
    }

    #[test]
    fn referenced_parent_table_and_unique_constraint_index_cannot_be_dropped() {
        let catalog = MemoryCatalog::empty();
        let parent = catalog
            .create_table(
                "parents".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let unique = catalog
            .create_index_with_constraint(
                "parents_id_key".to_string(),
                "parents",
                &["id".to_string()],
                true,
                IndexConstraintKind::Unique,
            )
            .unwrap();
        let child = catalog
            .create_table(
                "children".to_string(),
                vec![id_column(false)],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        catalog
            .attach_foreign_keys(
                child.id,
                vec![resolved_foreign_key(Some("children_parent"), parent.id)],
            )
            .unwrap();
        assert_eq!(
            catalog.drop_index(unique.id).unwrap_err().code,
            SqlState::DependentObjectsStillExist
        );
        assert_eq!(
            catalog.drop_table(parent.id).unwrap_err().code,
            SqlState::DependentObjectsStillExist
        );
        assert!(catalog.get_index(unique.id).unwrap().is_some());
        assert!(catalog.get_table(parent.id).unwrap().is_some());
    }

    #[test]
    fn batch_drop_allows_internal_foreign_keys_and_rejects_external_dependents_atomically() {
        let catalog = MemoryCatalog::empty();
        let make_table = |name: &str| {
            let table = catalog
                .create_table(
                    name.to_string(),
                    vec![id_column(false)],
                    Vec::new(),
                    CompressionSetting::None,
                )
                .unwrap();
            catalog
                .create_index_with_constraint(
                    format!("{name}_key"),
                    name,
                    &["id".to_string()],
                    true,
                    IndexConstraintKind::Unique,
                )
                .unwrap();
            catalog.get_table(table.id).unwrap().unwrap()
        };
        let first = make_table("drop_first");
        let second = make_table("drop_second");
        let outside = make_table("drop_outside");
        let fk = |name: &str, referenced_table| ResolvedForeignKey {
            name: Some(name.to_string()),
            columns: vec![0],
            referenced_table,
            referenced_columns: vec![0],
            on_update: ForeignKeyAction::NoAction,
            on_delete: ForeignKeyAction::NoAction,
        };
        catalog
            .attach_foreign_keys(first.id, vec![fk("first_second", second.id)])
            .unwrap();
        catalog
            .attach_foreign_keys(second.id, vec![fk("second_first", first.id)])
            .unwrap();
        catalog
            .attach_foreign_keys(outside.id, vec![fk("outside_first", first.id)])
            .unwrap();

        let before = catalog.snapshot().unwrap();
        let err = catalog.drop_tables(&[first.id, second.id]).unwrap_err();
        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
        let after = catalog.snapshot().unwrap();
        assert_eq!(after.tables_by_id, before.tables_by_id);
        assert_eq!(after.indexes_by_id, before.indexes_by_id);

        catalog
            .drop_tables(&[first.id, second.id, outside.id])
            .unwrap();
        assert!(catalog.get_table(first.id).unwrap().is_none());
        assert!(catalog.get_table(second.id).unwrap().is_none());
        assert!(catalog.get_table(outside.id).unwrap().is_none());
    }

    #[test]
    fn recovery_batch_drop_removes_cycle_edges_in_wal_record_order() {
        let catalog = MemoryCatalog::empty();
        let make_table = |name: &str| {
            let table = catalog
                .create_table(
                    name.to_string(),
                    vec![id_column(false)],
                    Vec::new(),
                    CompressionSetting::None,
                )
                .unwrap();
            catalog
                .create_index_with_constraint(
                    format!("{name}_key"),
                    name,
                    &["id".to_string()],
                    true,
                    IndexConstraintKind::Unique,
                )
                .unwrap();
            catalog.get_table(table.id).unwrap().unwrap()
        };
        let first = make_table("replay_first");
        let second = make_table("replay_second");
        let fk = |name: &str, referenced_table| ResolvedForeignKey {
            name: Some(name.to_string()),
            columns: vec![0],
            referenced_table,
            referenced_columns: vec![0],
            on_update: ForeignKeyAction::NoAction,
            on_delete: ForeignKeyAction::NoAction,
        };
        catalog
            .attach_foreign_keys(first.id, vec![fk("first_second", second.id)])
            .unwrap();
        catalog
            .attach_foreign_keys(second.id, vec![fk("second_first", first.id)])
            .unwrap();

        let batch = [first.id, second.id];
        catalog.apply_drop_table_in_batch(first.id, &batch).unwrap();
        assert!(catalog.get_table(first.id).unwrap().is_none());
        assert!(
            catalog
                .get_table(second.id)
                .unwrap()
                .unwrap()
                .foreign_keys
                .is_empty()
        );
        catalog
            .apply_drop_table_in_batch(second.id, &batch)
            .unwrap();
        assert!(catalog.get_table(second.id).unwrap().is_none());
    }

    #[test]
    fn recovery_batch_first_drop_removes_every_internal_cycle_edge() {
        let catalog = MemoryCatalog::empty();
        let make_table = |name: &str| {
            let table = catalog
                .create_table(
                    name.to_string(),
                    vec![id_column(false)],
                    Vec::new(),
                    CompressionSetting::None,
                )
                .unwrap();
            catalog
                .create_index_with_constraint(
                    format!("{name}_key"),
                    name,
                    &["id".to_string()],
                    true,
                    IndexConstraintKind::Unique,
                )
                .unwrap();
            catalog.get_table(table.id).unwrap().unwrap()
        };
        let tables = [
            make_table("cycle_a1"),
            make_table("cycle_a2"),
            make_table("cycle_b1"),
            make_table("cycle_b2"),
        ];
        let fk = |name: &str, referenced_table| ResolvedForeignKey {
            name: Some(name.to_string()),
            columns: vec![0],
            referenced_table,
            referenced_columns: vec![0],
            on_update: ForeignKeyAction::NoAction,
            on_delete: ForeignKeyAction::NoAction,
        };
        for (child, parent, name) in [
            (0, 1, "a1_a2"),
            (1, 0, "a2_a1"),
            (2, 3, "b1_b2"),
            (3, 2, "b2_b1"),
        ] {
            catalog
                .attach_foreign_keys(tables[child].id, vec![fk(name, tables[parent].id)])
                .unwrap();
        }

        let batch = tables.iter().map(|table| table.id).collect::<Vec<_>>();
        catalog
            .apply_drop_table_in_batch(tables[0].id, &batch)
            .unwrap();
        for table in &tables[1..] {
            assert!(
                catalog
                    .get_table(table.id)
                    .unwrap()
                    .unwrap()
                    .foreign_keys
                    .is_empty()
            );
            catalog.apply_drop_table(table.id).unwrap();
        }
    }

    #[test]
    fn dropping_earlier_child_column_remaps_source_but_parent_renumbering_is_rejected() {
        let catalog = MemoryCatalog::empty();
        let two_columns = || {
            vec![
                ParsedColumnDef {
                    name: "unused".to_string(),
                    data_type: DataType::Integer,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ParsedColumnDef {
                    name: "key".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ]
        };
        let parent = catalog
            .create_table(
                "parents".to_string(),
                two_columns(),
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        catalog
            .create_index_with_constraint(
                "parents_key_key".to_string(),
                "parents",
                &["key".to_string()],
                true,
                IndexConstraintKind::Unique,
            )
            .unwrap();
        let child = catalog
            .create_table(
                "children".to_string(),
                two_columns(),
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let mut definition = resolved_foreign_key(Some("children_parent"), parent.id);
        definition.columns = vec![1];
        definition.referenced_columns = vec![1];
        catalog
            .attach_foreign_keys(child.id, vec![definition])
            .unwrap();

        catalog.drop_table_column(child.id, "unused").unwrap();
        assert_eq!(
            catalog.get_table(child.id).unwrap().unwrap().foreign_keys[0].columns,
            vec![0]
        );
        let child_before_parent_drop = catalog.get_table(child.id).unwrap().unwrap();
        let err = catalog.drop_table_column(parent.id, "unused").unwrap_err();
        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
        assert_eq!(
            catalog.get_table(child.id).unwrap().unwrap(),
            child_before_parent_drop
        );
    }

    #[test]
    fn declared_constraint_index_cannot_reuse_a_foreign_key_name() {
        let (catalog, parent, child) = foreign_key_catalog();
        catalog
            .attach_foreign_keys(
                child,
                vec![resolved_foreign_key(Some("children_parent_key"), parent)],
            )
            .unwrap();
        let err = catalog
            .create_index_with_constraint(
                "children_parent_key".to_string(),
                "children",
                &["parent_id".to_string()],
                true,
                IndexConstraintKind::Unique,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::DuplicateObject);
        assert!(
            catalog
                .get_index_by_name("children_parent_key")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn referenced_primary_key_cannot_be_changed_or_dropped() {
        let (catalog, parent, child) = foreign_key_catalog();
        catalog
            .attach_foreign_keys(
                child,
                vec![resolved_foreign_key(Some("children_parent"), parent)],
            )
            .unwrap();
        assert_eq!(
            catalog
                .preflight_table_primary_key_change(parent, &[])
                .unwrap_err()
                .code,
            SqlState::DependentObjectsStillExist
        );
        assert_eq!(
            catalog
                .set_table_primary_key(parent, Vec::new())
                .unwrap_err()
                .code,
            SqlState::DependentObjectsStillExist
        );
        let primary_index = catalog
            .list_indexes_for_table(parent)
            .unwrap()
            .into_iter()
            .find(|index| index.constraint == IndexConstraintKind::PrimaryKey)
            .unwrap();
        assert_eq!(
            catalog
                .drop_table_primary_key_index(parent, primary_index.id)
                .unwrap_err()
                .code,
            SqlState::DependentObjectsStillExist
        );
        assert_eq!(
            catalog.get_table(parent).unwrap().unwrap().primary_key,
            vec![0]
        );
    }

    #[test]
    fn recovery_create_rejects_malformed_embedded_foreign_key_metadata_atomically() {
        let catalog = MemoryCatalog::empty();
        let mut schema = stored_id_table(99, "bad_child");
        schema.foreign_keys.push(ForeignKeyConstraint {
            id: 0,
            name: "bad_child_parent_fkey".to_string(),
            columns: vec![0],
            referenced_table: 999,
            referenced_columns: vec![0],
            on_update: ForeignKeyAction::NoAction,
            on_delete: ForeignKeyAction::NoAction,
        });
        schema.next_foreign_key_id = 1;
        let before = catalog.snapshot().unwrap();
        let err = catalog.apply_create_table(schema).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert_eq!(
            catalog.snapshot().unwrap().tables_by_id,
            before.tables_by_id
        );
    }

    #[test]
    fn foreign_key_column_type_alter_is_dependency_error_and_does_not_allocate_toast_metadata() {
        let (catalog, parent, child) = foreign_key_catalog();
        catalog
            .attach_foreign_keys(
                child,
                vec![resolved_foreign_key(Some("children_parent"), parent)],
            )
            .unwrap();
        let before = catalog.snapshot().unwrap();
        let err = catalog
            .alter_table_column_type(child, "parent_id", DataType::Text, PgType::Text, None)
            .unwrap_err();
        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
        let after = catalog.snapshot().unwrap();
        assert_eq!(after.next_table_id, before.next_table_id);
        assert_eq!(after.next_storage_id, before.next_storage_id);
        assert_eq!(after.tables_by_id, before.tables_by_id);
    }
}
