use std::collections::{HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use common::{
    ColumnDef, ColumnDefault, ColumnId, CompressionSetting, DataType, DbError, FileId,
    IndexConstraintKind, IndexId, IndexSchema, PRIMARY_KEY_INDEX_ID, ParsedColumnDef,
    ParsedDefault, RelationKind, Result, SequenceId, SequenceOptions, SequenceSchema, SqlState,
    TableId, TableSchema, ToastMode, ToastOptions, TruncateCatalogUpdate, TruncateTablePlan,
    ViewColumn, ViewDependency, ViewSchema, needs_toast_relation, toast_schema,
};

use crate::{
    CatalogManager, TableColumnAlteration,
    system::{MAX_COMPOUND_OID_SUB_ID, MAX_COMPOUND_OID_TABLE_ID, MAX_VIRTUAL_OID_PAYLOAD},
};

const STORAGE_ID_KIND_BITS: FileId = 0xC000_0000;
const MAX_STORAGE_ID: FileId = !STORAGE_ID_KIND_BITS;
const STORAGE_ID_EXHAUSTED: FileId = MAX_STORAGE_ID + 1;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CatalogSnapshot {
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub next_table_id: TableId,
    #[serde(default)]
    pub views_by_name: HashMap<String, TableId>,
    #[serde(default)]
    pub views_by_id: HashMap<TableId, ViewSchema>,
    // Secondary-index fields default so catalogs written before secondary
    // indexes existed still deserialize (no indexes, ids start after the
    // reserved primary-key id).
    #[serde(default)]
    pub indexes_by_name: HashMap<String, IndexId>,
    #[serde(default)]
    pub indexes_by_id: HashMap<IndexId, IndexSchema>,
    #[serde(default = "default_next_index_id")]
    pub next_index_id: IndexId,
    #[serde(default)]
    pub sequences_by_name: HashMap<String, SequenceId>,
    #[serde(default)]
    pub sequences_by_id: HashMap<SequenceId, SequenceSchema>,
    #[serde(default = "default_next_sequence_id")]
    pub next_sequence_id: SequenceId,
    // Dictionary-id allocator for trained compression dictionaries. Defaults
    // so catalogs written before compression existed still deserialize (no
    // dictionaries yet, first allocation is 1; 0 is reserved to mean "no
    // dictionary").
    #[serde(default = "default_next_dictionary_id")]
    pub next_dictionary_id: u32,
    // Physical storage-generation id allocator. Defaults so catalogs written
    // before relation generations existed migrate from logical ids.
    #[serde(default = "default_next_storage_id")]
    pub next_storage_id: FileId,
}

impl Default for CatalogSnapshot {
    fn default() -> Self {
        Self {
            tables_by_name: HashMap::new(),
            tables_by_id: HashMap::new(),
            next_table_id: 1,
            views_by_name: HashMap::new(),
            views_by_id: HashMap::new(),
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: default_next_index_id(),
            sequences_by_name: HashMap::new(),
            sequences_by_id: HashMap::new(),
            next_sequence_id: default_next_sequence_id(),
            next_dictionary_id: default_next_dictionary_id(),
            next_storage_id: default_next_storage_id(),
        }
    }
}

fn default_next_index_id() -> IndexId {
    PRIMARY_KEY_INDEX_ID + 1
}

fn default_next_sequence_id() -> SequenceId {
    1
}

fn default_next_dictionary_id() -> u32 {
    1
}

fn default_next_storage_id() -> FileId {
    1
}

#[derive(Debug)]
pub struct MemoryCatalog {
    snapshot: RwLock<CatalogSnapshot>,
}

impl MemoryCatalog {
    pub fn empty() -> Self {
        Self::from_snapshot(CatalogSnapshot {
            tables_by_name: HashMap::new(),
            tables_by_id: HashMap::new(),
            next_table_id: 1,
            ..CatalogSnapshot::default()
        })
    }

    fn from_snapshot(snapshot: CatalogSnapshot) -> Self {
        Self {
            snapshot: RwLock::new(snapshot),
        }
    }

    pub fn try_from_snapshot(mut snapshot: CatalogSnapshot) -> Result<Self> {
        normalize_snapshot_storage_ids(&mut snapshot)?;
        validate_snapshot(&snapshot)?;
        Ok(Self::from_snapshot(snapshot))
    }

    fn read_snapshot(&self) -> Result<RwLockReadGuard<'_, CatalogSnapshot>> {
        self.snapshot
            .read()
            .map_err(|_| DbError::internal("catalog read lock poisoned"))
    }

    fn write_snapshot(&self) -> Result<RwLockWriteGuard<'_, CatalogSnapshot>> {
        self.snapshot
            .write()
            .map_err(|_| DbError::internal("catalog write lock poisoned"))
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::empty()
    }
}

impl CatalogManager for MemoryCatalog {
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>> {
        let snapshot = self.read_snapshot()?;
        Ok(snapshot
            .tables_by_name
            .get(name)
            .and_then(|id| snapshot.tables_by_id.get(id))
            .cloned())
    }

    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>> {
        Ok(self.read_snapshot()?.tables_by_id.get(&id).cloned())
    }

    fn list_tables(&self) -> Result<Vec<TableSchema>> {
        let mut tables: Vec<_> = self
            .read_snapshot()?
            .tables_by_id
            .values()
            .cloned()
            .collect();
        tables.sort_by_key(|table| table.id);
        Ok(tables)
    }

    fn get_view_by_name(&self, name: &str) -> Result<Option<ViewSchema>> {
        let snapshot = self.read_snapshot()?;
        Ok(snapshot
            .views_by_name
            .get(name)
            .and_then(|id| snapshot.views_by_id.get(id))
            .cloned())
    }

    fn get_view(&self, id: TableId) -> Result<Option<ViewSchema>> {
        Ok(self.read_snapshot()?.views_by_id.get(&id).cloned())
    }

    fn list_views(&self) -> Result<Vec<ViewSchema>> {
        let mut views: Vec<_> = self
            .read_snapshot()?
            .views_by_id
            .values()
            .cloned()
            .collect();
        views.sort_by_key(|view| view.id);
        Ok(views)
    }

    fn snapshot(&self) -> Result<CatalogSnapshot> {
        Ok(self.read_snapshot()?.clone())
    }

    fn restore(&self, mut snapshot: CatalogSnapshot) -> Result<()> {
        normalize_snapshot_storage_ids(&mut snapshot)?;
        validate_snapshot(&snapshot)?;
        let mut current = self.write_snapshot()?;
        snapshot.next_table_id = snapshot.next_table_id.max(current.next_table_id);
        snapshot.next_index_id = snapshot.next_index_id.max(current.next_index_id);
        snapshot.next_sequence_id = snapshot.next_sequence_id.max(current.next_sequence_id);
        snapshot.next_dictionary_id = snapshot.next_dictionary_id.max(current.next_dictionary_id);
        snapshot.next_storage_id = snapshot.next_storage_id.max(current.next_storage_id);
        *current = snapshot;
        Ok(())
    }

    fn reserve_table_id(&self, id: TableId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reserve_id(&mut snapshot.next_table_id, id, "table")
    }

    fn apply_create_table(&self, mut schema: TableSchema) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        normalize_table_storage_id(&mut schema, &snapshot)?;
        validate_schema(&schema, &snapshot.sequences_by_id)?;
        if schema.relation_kind == RelationKind::User {
            reject_duplicate_relation_name(&snapshot, "table", &schema.name)?;
        }
        reject_index_name_matching_synthetic_primary_key(&snapshot, &schema)?;
        reject_duplicate_relation_id(&snapshot, schema.id)?;
        validate_storage_id("table", schema.storage_id)?;
        reject_duplicate_table_storage_id(&snapshot, schema.storage_id, "table storage id")?;

        let next_after_schema = schema.id.checked_add(1).ok_or_else(|| {
            DbError::internal("catalog table id overflow while applying create table")
        })?;

        if schema.relation_kind == RelationKind::User {
            snapshot
                .tables_by_name
                .insert(schema.name.clone(), schema.id);
        }
        snapshot.next_table_id = snapshot.next_table_id.max(next_after_schema);
        reserve_storage_id_value(&mut snapshot.next_storage_id, schema.storage_id)?;
        snapshot.tables_by_id.insert(schema.id, schema);
        Ok(())
    }

    fn apply_update_table_schema(&self, schema: TableSchema) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reserve_storage_id_value(&mut snapshot.next_storage_id, schema.storage_id)?;
        replace_table_schema(&mut snapshot, schema)
    }

    fn apply_update_table_and_index_schemas(
        &self,
        schema: TableSchema,
        indexes: &[IndexSchema],
    ) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reserve_storage_id_value(&mut snapshot.next_storage_id, schema.storage_id)?;
        for index in indexes {
            reserve_storage_id_value(&mut snapshot.next_storage_id, index.storage_id)?;
        }
        replace_table_and_index_schemas(&mut snapshot, schema, indexes)
    }

    fn apply_drop_table(&self, id: TableId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        let schema = snapshot
            .tables_by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;

        if let RelationKind::Toast { base_table } = schema.relation_kind
            && snapshot
                .tables_by_id
                .get(&base_table)
                .is_some_and(|base| base.toast_table_id == Some(id))
        {
            return Err(DbError::internal(format!(
                "cannot drop hidden TOAST relation {} while base table {} still references it",
                schema.name, base_table
            )));
        }

        reject_dependent_views(&snapshot, id, None)?;

        let mut table_ids = vec![id];
        if let RelationKind::User = schema.relation_kind
            && let Some(toast_table_id) = schema.toast_table_id
        {
            if toast_table_id == id {
                return Err(DbError::internal(format!(
                    "catalog table {} references itself as a TOAST relation",
                    schema.name
                )));
            }
            if let Some(toast_schema) = snapshot.tables_by_id.get(&toast_table_id) {
                if toast_schema.relation_kind
                    != (RelationKind::Toast {
                        base_table: schema.id,
                    })
                {
                    return Err(DbError::internal(format!(
                        "catalog table {} references non-matching TOAST relation {}",
                        schema.name, toast_table_id
                    )));
                }
                table_ids.push(toast_table_id);
            }
        }

        for table_id in table_ids {
            let schema = snapshot
                .tables_by_id
                .remove(&table_id)
                .ok_or_else(|| undefined_table(format!("table id {table_id} does not exist")))?;
            if snapshot.tables_by_name.get(&schema.name) == Some(&table_id) {
                snapshot.tables_by_name.remove(&schema.name);
            }
            drop_indexes_for_table(&mut snapshot, table_id);
        }
        Ok(())
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
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_relation_name(&snapshot, "table", &name)?;

        let table_id = snapshot.next_table_id;
        reject_duplicate_relation_id(&snapshot, table_id)?;
        let table_storage_id = snapshot.next_storage_id;
        let mut next_storage_id =
            next_storage_id_after(table_storage_id, "catalog storage id overflow")?;
        let mut next_table_id = table_id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog table id overflow"))?;
        let mut schema = build_schema(
            &snapshot,
            BuildSchemaInput {
                table_id,
                storage_id: table_storage_id,
                name,
                columns,
                primary_key,
                compression,
                toast,
                checks,
            },
        )?;
        validate_schema(&schema, &snapshot.sequences_by_id)?;
        validate_toast_options(&schema)?;
        let hidden_toast = if needs_toast_relation(&schema) {
            let toast_id = next_table_id;
            reject_duplicate_relation_id(&snapshot, toast_id)?;
            next_table_id = toast_id
                .checked_add(1)
                .ok_or_else(|| DbError::internal("catalog table id overflow"))?;
            let toast_storage_id = next_storage_id;
            next_storage_id =
                next_storage_id_after(toast_storage_id, "catalog storage id overflow")?;
            schema.toast_table_id = Some(toast_id);
            let mut hidden_toast = toast_schema(&schema, toast_id);
            hidden_toast.storage_id = toast_storage_id;
            validate_schema(&hidden_toast, &snapshot.sequences_by_id)?;
            Some(hidden_toast)
        } else {
            None
        };
        reject_index_name_matching_synthetic_primary_key(&snapshot, &schema)?;

        snapshot
            .tables_by_name
            .insert(schema.name.clone(), schema.id);
        if let Some(hidden_toast) = hidden_toast {
            snapshot.tables_by_id.insert(hidden_toast.id, hidden_toast);
        }
        snapshot.tables_by_id.insert(schema.id, schema.clone());
        snapshot.next_table_id = next_table_id;
        snapshot.next_storage_id = next_storage_id;
        Ok(schema)
    }

    fn drop_table(&self, id: TableId) -> Result<()> {
        self.apply_drop_table(id)
    }

    fn rename_table(&self, id: TableId, new_name: String) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;
        ensure_user_table(&schema)?;
        if schema.name == new_name {
            return Ok(schema);
        }
        reject_duplicate_relation_name(&snapshot, "table", &new_name)?;
        reject_dependent_views(&snapshot, id, None)?;
        schema.name = new_name;
        bump_schema_version(&mut schema.schema_version)?;
        replace_table_schema(&mut snapshot, schema.clone())?;
        Ok(schema)
    }

    fn preflight_add_table_column(
        &self,
        id: TableId,
        if_not_exists: bool,
        column: &ParsedColumnDef,
    ) -> Result<TableColumnAlteration> {
        let snapshot = self.read_snapshot()?;
        let schema = snapshot
            .tables_by_id
            .get(&id)
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;
        match validate_add_table_column(&snapshot, schema, column, if_not_exists)? {
            AddColumnValidation::Noop => Ok(TableColumnAlteration::Noop),
            AddColumnValidation::Rewrite { .. } => Ok(TableColumnAlteration::Rewrite),
        }
    }

    fn add_table_column(&self, id: TableId, column: ParsedColumnDef) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;
        let AddColumnValidation::Rewrite {
            column,
            toast_table_id,
        } = validate_add_table_column(&snapshot, &schema, &column, false)?
        else {
            return Err(DbError::internal(
                "ADD COLUMN unexpectedly validated as a no-op",
            ));
        };
        schema.columns.push(column);
        bump_schema_version(&mut schema.schema_version)?;
        let hidden_toast = if let Some(toast_id) = toast_table_id {
            snapshot.next_table_id = toast_id
                .checked_add(1)
                .ok_or_else(|| DbError::internal("catalog table id overflow"))?;
            let toast_storage_id = allocate_storage_id_from_snapshot(&mut snapshot)?;
            schema.toast_table_id = Some(toast_id);
            let mut hidden_toast = toast_schema(&schema, toast_id);
            hidden_toast.storage_id = toast_storage_id;
            Some(hidden_toast)
        } else {
            None
        };
        if let Some(hidden_toast) = hidden_toast {
            snapshot.tables_by_id.insert(hidden_toast.id, hidden_toast);
        }
        replace_table_schema(&mut snapshot, schema.clone())?;
        Ok(schema)
    }

    fn preflight_drop_table_column(
        &self,
        id: TableId,
        if_exists: bool,
        column: &str,
    ) -> Result<TableColumnAlteration> {
        let snapshot = self.read_snapshot()?;
        let schema = snapshot
            .tables_by_id
            .get(&id)
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;
        match validate_drop_table_column(&snapshot, schema, column, if_exists)? {
            DropColumnValidation::Noop => Ok(TableColumnAlteration::Noop),
            DropColumnValidation::Rewrite { .. } => Ok(TableColumnAlteration::Rewrite),
        }
    }

    fn drop_table_column(&self, id: TableId, column: &str) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;
        let DropColumnValidation::Rewrite {
            position,
            column_id,
        } = validate_drop_table_column(&snapshot, &schema, column, false)?
        else {
            return Err(DbError::internal(
                "DROP COLUMN unexpectedly validated as a no-op",
            ));
        };

        schema.columns.remove(position);
        remap_columns_after_drop(&mut schema, column_id);
        remap_indexes_after_drop(&mut snapshot, id, column_id);
        bump_schema_version(&mut schema.schema_version)?;
        replace_table_schema(&mut snapshot, schema.clone())?;
        Ok(schema)
    }

    fn rename_table_column(
        &self,
        id: TableId,
        old_name: &str,
        new_name: String,
    ) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;
        ensure_user_table(&schema)?;
        if schema.columns.iter().any(|column| column.name == new_name) {
            return Err(DbError::plan(
                SqlState::DuplicateTable,
                format!("column {new_name} already exists"),
            ));
        }
        let column = schema
            .columns
            .iter_mut()
            .find(|column| column.name == old_name)
            .ok_or_else(|| {
                DbError::plan(
                    SqlState::UndefinedColumn,
                    format!("column {old_name} does not exist"),
                )
            })?;
        if !schema.checks.is_empty() {
            return Err(DbError::plan(
                SqlState::DependentObjectsStillExist,
                format!(
                    "cannot rename column {old_name} because table {} has CHECK constraints",
                    schema.name
                ),
            ));
        }
        reject_view_column_dependency(&snapshot, id, column.id, "rename")?;
        column.name = new_name;
        bump_schema_version(&mut schema.schema_version)?;
        replace_table_schema(&mut snapshot, schema.clone())?;
        Ok(schema)
    }

    fn set_table_compression(
        &self,
        table: TableId,
        compression: CompressionSetting,
        active_dict_id: Option<u32>,
    ) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        // Resolve the live table first so a failed call (table absent/dropped)
        // has no side effects on the dictionary id allocator.
        let schema = snapshot
            .tables_by_id
            .get_mut(&table)
            .ok_or_else(|| DbError::internal(format!("table id {table} does not exist")))?;
        schema.compression = compression;
        schema.active_dict_id = active_dict_id;
        let schema = schema.clone();
        // An externally-supplied dictionary id (a fresh allocation on the live
        // ALTER path, or a replayed id during recovery) must never be at or
        // past the allocator's high-water mark, so bump it the same way every
        // other apply_* path advances its id allocator past an installed id.
        if let Some(id) = active_dict_id {
            reserve_id(&mut snapshot.next_dictionary_id, id, "dictionary")?;
        }
        Ok(schema)
    }

    fn set_table_toast_metadata(
        &self,
        table: TableId,
        toast: ToastOptions,
        toast_table_id: Option<TableId>,
    ) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&table)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("table id {table} does not exist")))?;
        if schema.relation_kind != RelationKind::User {
            return Err(DbError::internal(format!(
                "cannot set TOAST metadata on hidden relation {}",
                schema.name
            )));
        }
        schema.toast = toast;
        schema.toast_table_id = toast_table_id;
        validate_toast_options(&schema)?;
        if let Some(toast_table_id) = toast_table_id {
            let toast_schema = snapshot.tables_by_id.get(&toast_table_id).ok_or_else(|| {
                DbError::internal(format!(
                    "table id {table} references missing TOAST relation {toast_table_id}"
                ))
            })?;
            if toast_schema.relation_kind != (RelationKind::Toast { base_table: table }) {
                return Err(DbError::internal(format!(
                    "table id {table} references non-matching TOAST relation {toast_table_id}"
                )));
            }
        }
        if let Some(id) = schema.toast.active_dict_id {
            reserve_id(&mut snapshot.next_dictionary_id, id, "dictionary")?;
        }
        snapshot.tables_by_id.insert(table, schema.clone());
        Ok(schema)
    }

    fn set_table_primary_key(
        &self,
        table: TableId,
        primary_key: Vec<ColumnId>,
    ) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&table)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("table id {table} does not exist")))?;
        if schema.relation_kind != RelationKind::User {
            return Err(DbError::internal(format!(
                "cannot set primary key metadata on hidden relation {}",
                schema.name
            )));
        }

        set_primary_key_columns(&mut schema, primary_key)?;
        validate_schema(&schema, &snapshot.sequences_by_id)?;
        bump_schema_version(&mut schema.schema_version)?;
        snapshot.tables_by_id.insert(table, schema.clone());
        Ok(schema)
    }

    fn add_table_primary_key_index(
        &self,
        table: TableId,
        primary_key: Vec<ColumnId>,
        index: IndexSchema,
    ) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&table)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("table id {table} does not exist")))?;
        if schema.relation_kind != RelationKind::User {
            return Err(DbError::internal(format!(
                "cannot add primary key metadata on hidden relation {}",
                schema.name
            )));
        }
        if !schema.primary_key.is_empty() {
            return Err(DbError::internal(format!(
                "table {} already has primary key metadata",
                schema.name
            )));
        }

        set_primary_key_columns(&mut schema, primary_key)?;
        validate_schema(&schema, &snapshot.sequences_by_id)?;
        bump_schema_version(&mut schema.schema_version)?;
        if index.table != table {
            return Err(DbError::internal(format!(
                "primary-key index {} references table {}, expected {table}",
                index.name, index.table
            )));
        }
        if index.constraint != IndexConstraintKind::PrimaryKey || !index.unique {
            return Err(DbError::internal(format!(
                "primary-key index {} must be a unique primary-key constraint index",
                index.name
            )));
        }
        reject_duplicate_index_name(&snapshot, &index.name)?;
        reject_duplicate_index_id(&snapshot, index.id)?;
        validate_index_schema_for_table(&index, &schema)?;
        reject_duplicate_primary_key_constraint_index(&snapshot, &index)?;

        let next_after_index = index.id.checked_add(1).ok_or_else(|| {
            DbError::internal("catalog index id overflow while applying primary key index")
        })?;
        snapshot.tables_by_id.insert(table, schema.clone());
        snapshot
            .indexes_by_name
            .insert(index.name.clone(), index.id);
        snapshot.next_index_id = snapshot.next_index_id.max(next_after_index);
        snapshot.indexes_by_id.insert(index.id, index);
        Ok(schema)
    }

    fn drop_table_primary_key_index(&self, table: TableId, index: IndexId) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        let mut schema = snapshot
            .tables_by_id
            .get(&table)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("table id {table} does not exist")))?;
        if schema.relation_kind != RelationKind::User {
            return Err(DbError::internal(format!(
                "cannot drop primary key metadata on hidden relation {}",
                schema.name
            )));
        }
        let index_schema = snapshot
            .indexes_by_id
            .get(&index)
            .cloned()
            .ok_or_else(|| undefined_index(format!("index id {index} does not exist")))?;
        if index_schema.table != table || index_schema.constraint != IndexConstraintKind::PrimaryKey
        {
            return Err(DbError::internal(format!(
                "index {} is not the primary-key constraint index for table {}",
                index_schema.name, schema.name
            )));
        }

        schema.primary_key.clear();
        validate_schema(&schema, &snapshot.sequences_by_id)?;
        bump_schema_version(&mut schema.schema_version)?;
        snapshot.indexes_by_id.remove(&index);
        snapshot.indexes_by_name.remove(&index_schema.name);
        snapshot.tables_by_id.insert(table, schema.clone());
        Ok(schema)
    }

    fn allocate_dictionary_id(&self) -> Result<u32> {
        let mut snapshot = self.write_snapshot()?;
        let id = snapshot.next_dictionary_id;
        snapshot.next_dictionary_id = id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog dictionary id overflow"))?;
        Ok(id)
    }

    fn reserve_dictionary_id(&self, id: u32) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reserve_id(&mut snapshot.next_dictionary_id, id, "dictionary")
    }

    fn allocate_storage_id(&self) -> Result<FileId> {
        let mut snapshot = self.write_snapshot()?;
        allocate_storage_id_from_snapshot(&mut snapshot)
    }

    fn reserve_storage_id(&self, id: FileId) -> Result<()> {
        validate_storage_id("storage", id)?;
        let mut snapshot = self.write_snapshot()?;
        reserve_storage_id_value(&mut snapshot.next_storage_id, id)
    }

    fn prepare_truncate_table(&self, table: TableId) -> Result<TruncateTablePlan> {
        let mut snapshot = self.write_snapshot()?;
        let schema = snapshot
            .tables_by_id
            .get(&table)
            .cloned()
            .ok_or_else(|| undefined_table(format!("table id {table} does not exist")))?;
        if schema.relation_kind != RelationKind::User {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                format!("cannot truncate hidden TOAST relation {}", schema.name),
            ));
        }

        let new_table_storage_id = allocate_storage_id_from_snapshot(&mut snapshot)?;
        let new_toast_storage_id = match schema.toast_table_id {
            Some(toast_table_id) => {
                let toast_schema = snapshot.tables_by_id.get(&toast_table_id).ok_or_else(|| {
                    DbError::internal(format!(
                        "catalog table {} references missing TOAST relation {}",
                        schema.name, toast_table_id
                    ))
                })?;
                if toast_schema.relation_kind
                    != (RelationKind::Toast {
                        base_table: schema.id,
                    })
                {
                    return Err(DbError::internal(format!(
                        "catalog table {} references non-matching TOAST relation {}",
                        schema.name, toast_table_id
                    )));
                }
                Some((
                    toast_table_id,
                    allocate_storage_id_from_snapshot(&mut snapshot)?,
                ))
            }
            None => None,
        };

        let mut indexes = snapshot
            .indexes_by_id
            .values()
            .filter(|index| index.table == table)
            .map(|index| index.id)
            .collect::<Vec<_>>();
        indexes.sort_unstable();
        let mut new_index_storage_ids = Vec::with_capacity(indexes.len());
        for index_id in indexes {
            new_index_storage_ids
                .push((index_id, allocate_storage_id_from_snapshot(&mut snapshot)?));
        }

        Ok(TruncateTablePlan {
            table_id: table,
            new_table_storage_id,
            new_toast_storage_id,
            new_index_storage_ids,
        })
    }

    fn build_truncate_table_update(
        &self,
        plan: &TruncateTablePlan,
    ) -> Result<TruncateCatalogUpdate> {
        let snapshot = self.read_snapshot()?;
        build_truncate_catalog_update(&snapshot, plan)
    }

    fn apply_truncate_table(&self, plan: &TruncateTablePlan) -> Result<TruncateCatalogUpdate> {
        let mut snapshot = self.write_snapshot()?;
        let update = build_truncate_catalog_update(&snapshot, plan)?;

        reserve_storage_id_value(&mut snapshot.next_storage_id, update.table.storage_id)?;
        snapshot
            .tables_by_id
            .insert(update.table.id, update.table.clone());
        if let Some(toast) = &update.toast_table {
            reserve_storage_id_value(&mut snapshot.next_storage_id, toast.storage_id)?;
            snapshot.tables_by_id.insert(toast.id, toast.clone());
        }
        for index in &update.indexes {
            reserve_storage_id_value(&mut snapshot.next_storage_id, index.storage_id)?;
            snapshot.indexes_by_id.insert(index.id, index.clone());
        }

        Ok(update)
    }

    fn get_index_by_name(&self, name: &str) -> Result<Option<IndexSchema>> {
        let snapshot = self.read_snapshot()?;
        Ok(snapshot
            .indexes_by_name
            .get(name)
            .and_then(|id| snapshot.indexes_by_id.get(id))
            .cloned())
    }

    fn get_index(&self, id: IndexId) -> Result<Option<IndexSchema>> {
        Ok(self.read_snapshot()?.indexes_by_id.get(&id).cloned())
    }

    fn list_indexes_for_table(&self, table: TableId) -> Result<Vec<IndexSchema>> {
        let mut indexes: Vec<_> = self
            .read_snapshot()?
            .indexes_by_id
            .values()
            .filter(|index| index.table == table)
            .cloned()
            .collect();
        indexes.sort_by_key(|index| index.id);
        Ok(indexes)
    }

    fn reserve_index_id(&self, id: IndexId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reserve_id(&mut snapshot.next_index_id, id, "index")
    }

    fn apply_create_index(&self, mut schema: IndexSchema) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        normalize_index_storage_id(&mut schema, &snapshot)?;
        reject_duplicate_index_id(&snapshot, schema.id)?;
        validate_storage_id("index", schema.storage_id)?;
        reject_duplicate_index_storage_id(&snapshot, schema.storage_id, "index storage id")?;
        validate_index_schema(&schema, &snapshot.tables_by_id)?;
        reject_duplicate_index_name_for_schema(&snapshot, &schema)?;
        reject_duplicate_primary_key_constraint_index(&snapshot, &schema)?;

        let next_after_schema = schema.id.checked_add(1).ok_or_else(|| {
            DbError::internal("catalog index id overflow while applying create index")
        })?;

        snapshot
            .indexes_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.next_index_id = snapshot.next_index_id.max(next_after_schema);
        reserve_storage_id_value(&mut snapshot.next_storage_id, schema.storage_id)?;
        snapshot.indexes_by_id.insert(schema.id, schema);
        Ok(())
    }

    fn apply_update_index_schema(&self, schema: IndexSchema) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        let old = snapshot
            .indexes_by_id
            .get(&schema.id)
            .cloned()
            .ok_or_else(|| undefined_index(format!("index id {} does not exist", schema.id)))?;
        if old.name != schema.name {
            reject_duplicate_index_name(&snapshot, &schema.name)?;
            snapshot.indexes_by_name.remove(&old.name);
            snapshot
                .indexes_by_name
                .insert(schema.name.clone(), schema.id);
        }
        validate_index_schema(&schema, &snapshot.tables_by_id)?;
        reserve_storage_id_value(&mut snapshot.next_storage_id, schema.storage_id)?;
        snapshot.indexes_by_id.insert(schema.id, schema);
        Ok(())
    }

    fn apply_drop_index(&self, id: IndexId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        let schema = snapshot
            .indexes_by_id
            .remove(&id)
            .ok_or_else(|| undefined_index(format!("index id {id} does not exist")))?;
        snapshot.indexes_by_name.remove(&schema.name);
        Ok(())
    }

    fn create_index_with_constraint(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
        constraint: IndexConstraintKind,
    ) -> Result<IndexSchema> {
        let mut snapshot = self.write_snapshot()?;

        let index_id = snapshot.next_index_id;
        let next_index_id = index_id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog index id overflow"))?;
        let storage_id = snapshot.next_storage_id;
        let next_storage_id = next_storage_id_after(storage_id, "catalog storage id overflow")?;

        let schema = {
            let table_schema = snapshot
                .tables_by_name
                .get(table)
                .and_then(|id| snapshot.tables_by_id.get(id))
                .ok_or_else(|| undefined_table(format!("table {table} does not exist")))?;
            build_index_schema(
                index_id,
                storage_id,
                name,
                table_schema,
                columns,
                unique,
                constraint,
            )?
        };
        validate_index_schema(&schema, &snapshot.tables_by_id)?;
        reject_duplicate_index_name_for_schema(&snapshot, &schema)?;
        reject_duplicate_primary_key_constraint_index(&snapshot, &schema)?;

        snapshot
            .indexes_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.indexes_by_id.insert(schema.id, schema.clone());
        snapshot.next_index_id = next_index_id;
        snapshot.next_storage_id = next_storage_id;
        Ok(schema)
    }

    fn drop_index(&self, id: IndexId) -> Result<()> {
        if self
            .get_index(id)?
            .is_some_and(|index| index.constraint == IndexConstraintKind::PrimaryKey)
        {
            return Err(DbError::plan(
                SqlState::DependentObjectsStillExist,
                "cannot drop index backing a primary key constraint",
            ));
        }
        self.apply_drop_index(id)
    }

    fn get_sequence_by_name(&self, name: &str) -> Result<Option<SequenceSchema>> {
        let snapshot = self.read_snapshot()?;
        Ok(snapshot
            .sequences_by_name
            .get(name)
            .and_then(|id| snapshot.sequences_by_id.get(id))
            .cloned())
    }

    fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceSchema>> {
        Ok(self.read_snapshot()?.sequences_by_id.get(&id).cloned())
    }

    fn list_sequences(&self) -> Result<Vec<SequenceSchema>> {
        let mut sequences: Vec<_> = self
            .read_snapshot()?
            .sequences_by_id
            .values()
            .cloned()
            .collect();
        sequences.sort_by_key(|sequence| sequence.id);
        Ok(sequences)
    }

    fn reserve_sequence_id(&self, id: SequenceId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reserve_id(&mut snapshot.next_sequence_id, id, "sequence")
    }

    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()> {
        validate_sequence_schema(&schema)?;
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_sequence_name(&snapshot, &schema.name)?;
        reject_duplicate_sequence_id(&snapshot, schema.id)?;
        let next_after_schema = schema.id.checked_add(1).ok_or_else(|| {
            DbError::internal("catalog sequence id overflow while applying create sequence")
        })?;
        snapshot
            .sequences_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.next_sequence_id = snapshot.next_sequence_id.max(next_after_schema);
        snapshot.sequences_by_id.insert(schema.id, schema);
        Ok(())
    }

    fn apply_drop_sequence(&self, id: SequenceId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        let schema = snapshot
            .sequences_by_id
            .remove(&id)
            .ok_or_else(|| undefined_sequence_id(id))?;
        snapshot.sequences_by_name.remove(&schema.name);
        Ok(())
    }

    fn create_sequence(
        &self,
        name: String,
        options: SequenceOptions,
        owned: bool,
    ) -> Result<SequenceSchema> {
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_sequence_name(&snapshot, &name)?;
        let id = snapshot.next_sequence_id;
        reject_duplicate_sequence_id(&snapshot, id)?;
        let next_sequence_id = id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog sequence id overflow"))?;
        let schema = build_sequence_schema(id, name, options, owned)?;
        validate_sequence_schema(&schema)?;
        snapshot
            .sequences_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.sequences_by_id.insert(schema.id, schema.clone());
        snapshot.next_sequence_id = next_sequence_id;
        Ok(schema)
    }

    fn drop_sequence(&self, id: SequenceId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        let schema = snapshot
            .sequences_by_id
            .get(&id)
            .ok_or_else(|| undefined_sequence_id(id))?;
        if schema.owned {
            return Err(DbError::plan(
                SqlState::DependentObjectsStillExist,
                format!("cannot drop owned sequence {}", schema.name),
            ));
        }
        reject_referenced_sequence(&snapshot, id)?;
        let schema = snapshot
            .sequences_by_id
            .remove(&id)
            .ok_or_else(|| undefined_sequence_id(id))?;
        snapshot.sequences_by_name.remove(&schema.name);
        Ok(())
    }

    fn apply_create_view(&self, schema: ViewSchema) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        validate_view_schema(&schema, &snapshot)?;
        reject_duplicate_relation_name(&snapshot, "view", &schema.name)?;
        reject_duplicate_relation_id(&snapshot, schema.id)?;
        let next_after_schema = schema.id.checked_add(1).ok_or_else(|| {
            DbError::internal("catalog view id overflow while applying create view")
        })?;
        snapshot
            .views_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.next_table_id = snapshot.next_table_id.max(next_after_schema);
        snapshot.views_by_id.insert(schema.id, schema);
        Ok(())
    }

    fn apply_replace_view(&self, schema: ViewSchema) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        validate_view_schema(&schema, &snapshot)?;
        let old = snapshot
            .views_by_id
            .get(&schema.id)
            .cloned()
            .ok_or_else(|| undefined_view(format!("view id {} does not exist", schema.id)))?;
        if old.name != schema.name {
            reject_duplicate_relation_name(&snapshot, "view", &schema.name)?;
            snapshot.views_by_name.remove(&old.name);
            snapshot
                .views_by_name
                .insert(schema.name.clone(), schema.id);
        }
        snapshot.views_by_id.insert(schema.id, schema);
        Ok(())
    }

    fn apply_drop_view(&self, id: TableId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reject_dependent_views(&snapshot, id, Some(id))?;
        let schema = snapshot
            .views_by_id
            .remove(&id)
            .ok_or_else(|| undefined_view(format!("view id {id} does not exist")))?;
        snapshot.views_by_name.remove(&schema.name);
        Ok(())
    }

    fn create_view(
        &self,
        name: String,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema> {
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_relation_name(&snapshot, "view", &name)?;
        let id = snapshot.next_table_id;
        reject_duplicate_relation_id(&snapshot, id)?;
        let next_table_id = id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog view id overflow"))?;
        let schema = build_view_schema(id, name, columns, definition, dependencies)?;
        validate_live_view_schema(&schema, &snapshot)?;
        snapshot
            .views_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.views_by_id.insert(schema.id, schema.clone());
        snapshot.next_table_id = next_table_id;
        Ok(schema)
    }

    fn replace_view(
        &self,
        id: TableId,
        columns: Vec<ViewColumn>,
        definition: String,
        dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema> {
        let mut snapshot = self.write_snapshot()?;
        let old = snapshot
            .views_by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| undefined_view(format!("view id {id} does not exist")))?;
        let mut schema = build_view_schema(id, old.name, columns, definition, dependencies)?;
        schema.schema_version = old.schema_version;
        bump_schema_version(&mut schema.schema_version)?;
        validate_live_view_schema(&schema, &snapshot)?;
        snapshot.views_by_id.insert(schema.id, schema.clone());
        Ok(schema)
    }

    fn drop_view(&self, id: TableId) -> Result<()> {
        self.apply_drop_view(id)
    }
}

/// Advance a monotonic id allocator's high-water mark past `id`, so a later
/// allocation never reuses it (the `reserve_*_id` path, used when recovery
/// replays a create). `kind` names the object for the overflow error.
fn reserve_id(next: &mut u32, id: u32, kind: &str) -> Result<()> {
    let next_after_id = id.checked_add(1).ok_or_else(|| {
        DbError::internal(format!("catalog {kind} id overflow while reserving id"))
    })?;
    *next = (*next).max(next_after_id);
    Ok(())
}

fn build_truncate_catalog_update(
    snapshot: &CatalogSnapshot,
    plan: &TruncateTablePlan,
) -> Result<TruncateCatalogUpdate> {
    validate_truncate_plan_storage_ids(plan)?;

    let mut table = snapshot
        .tables_by_id
        .get(&plan.table_id)
        .cloned()
        .ok_or_else(|| undefined_table(format!("table id {} does not exist", plan.table_id)))?;
    if table.relation_kind != RelationKind::User {
        return Err(DbError::plan(
            SqlState::FeatureNotSupported,
            format!("cannot truncate hidden TOAST relation {}", table.name),
        ));
    }

    let toast_table = match (table.toast_table_id, plan.new_toast_storage_id) {
        (Some(expected_id), Some((toast_id, storage_id))) if expected_id == toast_id => {
            let mut toast = snapshot
                .tables_by_id
                .get(&toast_id)
                .cloned()
                .ok_or_else(|| {
                    DbError::internal(format!(
                        "catalog table {} references missing TOAST relation {}",
                        table.name, toast_id
                    ))
                })?;
            if toast.relation_kind
                != (RelationKind::Toast {
                    base_table: table.id,
                })
            {
                return Err(DbError::internal(format!(
                    "catalog table {} references non-matching TOAST relation {}",
                    table.name, toast_id
                )));
            }
            toast.storage_id = storage_id;
            Some(toast)
        }
        (None, None) => None,
        (Some(expected_id), Some((toast_id, _))) => {
            return Err(DbError::internal(format!(
                "truncate plan toast table {toast_id} does not match catalog toast table {expected_id}"
            )));
        }
        (Some(expected_id), None) => {
            return Err(DbError::internal(format!(
                "truncate plan missing toast storage id for table {} toast relation {}",
                table.name, expected_id
            )));
        }
        (None, Some((toast_id, _))) => {
            return Err(DbError::internal(format!(
                "truncate plan names toast relation {toast_id} for table {} without one",
                table.name
            )));
        }
    };

    let live_index_ids = snapshot
        .indexes_by_id
        .values()
        .filter(|index| index.table == table.id)
        .map(|index| index.id)
        .collect::<HashSet<_>>();
    let planned_index_ids = plan
        .new_index_storage_ids
        .iter()
        .map(|(id, _)| *id)
        .collect::<HashSet<_>>();
    if live_index_ids != planned_index_ids {
        return Err(DbError::internal(format!(
            "truncate plan index set does not match catalog indexes for table {}",
            table.name
        )));
    }

    validate_truncate_storage_ids_available(snapshot, plan)?;

    table.storage_id = plan.new_table_storage_id;
    let mut indexes = Vec::with_capacity(plan.new_index_storage_ids.len());
    for (index_id, storage_id) in &plan.new_index_storage_ids {
        let mut index = snapshot
            .indexes_by_id
            .get(index_id)
            .cloned()
            .ok_or_else(|| undefined_index(format!("index id {index_id} does not exist")))?;
        if index.table != table.id {
            return Err(DbError::internal(format!(
                "truncate plan index {} belongs to table {}, expected {}",
                index.id, index.table, table.id
            )));
        }
        index.storage_id = *storage_id;
        indexes.push(index);
    }
    indexes.sort_by_key(|index| index.id);

    Ok(TruncateCatalogUpdate {
        table,
        toast_table,
        indexes,
    })
}

fn allocate_storage_id_from_snapshot(snapshot: &mut CatalogSnapshot) -> Result<FileId> {
    let id = snapshot.next_storage_id;
    validate_storage_id("storage", id)?;
    snapshot.next_storage_id = next_storage_id_after(id, "catalog storage id overflow")?;
    Ok(id)
}

fn reserve_storage_id_value(next: &mut FileId, id: FileId) -> Result<()> {
    validate_storage_id("storage", id)?;
    *next = (*next).max(next_storage_id_after(
        id,
        "catalog storage id overflow while reserving id",
    )?);
    Ok(())
}

fn next_storage_id_after(id: FileId, overflow_message: &'static str) -> Result<FileId> {
    validate_storage_id("storage", id)?;
    id.checked_add(1)
        .filter(|next| *next <= STORAGE_ID_EXHAUSTED)
        .ok_or_else(|| DbError::internal(overflow_message))
}

fn validate_storage_id(kind: &str, id: FileId) -> Result<()> {
    if id == 0 {
        return Err(DbError::internal(format!(
            "catalog {kind} storage id 0 is reserved for legacy missing ids"
        )));
    }
    if id & STORAGE_ID_KIND_BITS != 0 {
        return Err(DbError::internal(format!(
            "catalog {kind} storage id {id} contains file-kind high bits"
        )));
    }
    Ok(())
}

fn normalize_snapshot_storage_ids(snapshot: &mut CatalogSnapshot) -> Result<()> {
    let mut table_assigned = explicit_table_storage_ids(snapshot);
    let mut index_assigned = explicit_index_storage_ids(snapshot);
    let mut assigned = table_assigned
        .iter()
        .chain(index_assigned.iter())
        .copied()
        .collect::<HashSet<_>>();
    let mut next_storage_id = snapshot
        .next_storage_id
        .max(default_next_storage_id())
        .max(next_after_max_storage_id(&assigned)?);

    let mut table_ids = snapshot.tables_by_id.keys().copied().collect::<Vec<_>>();
    table_ids.sort_unstable();
    for table_id in table_ids {
        let table = snapshot
            .tables_by_id
            .get_mut(&table_id)
            .expect("table id came from map keys");
        if table.storage_id == 0 {
            table.storage_id =
                legacy_or_fresh_storage_id(table.id, &mut next_storage_id, &mut table_assigned)?;
            assigned.insert(table.storage_id);
        } else {
            table_assigned.insert(table.storage_id);
            assigned.insert(table.storage_id);
        }
    }

    let mut index_ids = snapshot.indexes_by_id.keys().copied().collect::<Vec<_>>();
    index_ids.sort_unstable();
    for index_id in index_ids {
        let index = snapshot
            .indexes_by_id
            .get_mut(&index_id)
            .expect("index id came from map keys");
        if index.storage_id == 0 {
            index.storage_id =
                legacy_or_fresh_storage_id(index.id, &mut next_storage_id, &mut index_assigned)?;
            assigned.insert(index.storage_id);
        } else {
            index_assigned.insert(index.storage_id);
            assigned.insert(index.storage_id);
        }
    }

    snapshot.next_storage_id = next_storage_id.max(next_after_max_storage_id(&assigned)?);
    Ok(())
}

fn explicit_table_storage_ids(snapshot: &CatalogSnapshot) -> HashSet<FileId> {
    snapshot
        .tables_by_id
        .values()
        .filter_map(|table| (table.storage_id != 0).then_some(table.storage_id))
        .collect()
}

fn explicit_index_storage_ids(snapshot: &CatalogSnapshot) -> HashSet<FileId> {
    snapshot
        .indexes_by_id
        .values()
        .filter_map(|index| (index.storage_id != 0).then_some(index.storage_id))
        .collect()
}

fn legacy_or_fresh_storage_id(
    preferred: FileId,
    next_storage_id: &mut FileId,
    assigned: &mut HashSet<FileId>,
) -> Result<FileId> {
    if preferred != 0 && preferred & STORAGE_ID_KIND_BITS == 0 && !assigned.contains(&preferred) {
        assigned.insert(preferred);
        return Ok(preferred);
    }
    loop {
        let candidate = *next_storage_id;
        *next_storage_id = next_storage_id_after(candidate, "catalog storage id overflow")?;
        if !assigned.contains(&candidate) {
            assigned.insert(candidate);
            return Ok(candidate);
        }
    }
}

fn next_after_max_storage_id(ids: &HashSet<FileId>) -> Result<FileId> {
    match ids.iter().copied().max() {
        Some(max) => next_storage_id_after(max, "catalog storage id overflow"),
        None => Ok(default_next_storage_id()),
    }
}

fn normalize_table_storage_id(schema: &mut TableSchema, snapshot: &CatalogSnapshot) -> Result<()> {
    if schema.storage_id != 0 {
        return Ok(());
    }
    let mut assigned = live_table_storage_ids(snapshot);
    let all_assigned = live_storage_ids(snapshot);
    let mut next_storage_id = snapshot
        .next_storage_id
        .max(next_after_max_storage_id(&all_assigned)?);
    schema.storage_id = legacy_or_fresh_storage_id(schema.id, &mut next_storage_id, &mut assigned)?;
    Ok(())
}

fn normalize_index_storage_id(schema: &mut IndexSchema, snapshot: &CatalogSnapshot) -> Result<()> {
    if schema.storage_id != 0 {
        return Ok(());
    }
    let mut assigned = live_index_storage_ids(snapshot);
    let all_assigned = live_storage_ids(snapshot);
    let mut next_storage_id = snapshot
        .next_storage_id
        .max(next_after_max_storage_id(&all_assigned)?);
    schema.storage_id = legacy_or_fresh_storage_id(schema.id, &mut next_storage_id, &mut assigned)?;
    Ok(())
}

fn live_table_storage_ids(snapshot: &CatalogSnapshot) -> HashSet<FileId> {
    snapshot
        .tables_by_id
        .values()
        .map(|table| table.storage_id)
        .filter(|id| *id != 0)
        .collect()
}

fn live_index_storage_ids(snapshot: &CatalogSnapshot) -> HashSet<FileId> {
    snapshot
        .indexes_by_id
        .values()
        .map(|index| index.storage_id)
        .filter(|id| *id != 0)
        .collect()
}

fn live_storage_ids(snapshot: &CatalogSnapshot) -> HashSet<FileId> {
    snapshot
        .tables_by_id
        .values()
        .map(|table| table.storage_id)
        .chain(
            snapshot
                .indexes_by_id
                .values()
                .map(|index| index.storage_id),
        )
        .filter(|id| *id != 0)
        .collect()
}

fn validate_storage_ids(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut seen_tables = HashMap::<FileId, String>::new();
    let mut seen_indexes = HashMap::<FileId, String>::new();
    let mut max_storage_id = 0;
    for table in snapshot.tables_by_id.values() {
        validate_storage_id("table", table.storage_id)?;
        reject_seen_storage_id(
            &mut seen_tables,
            table.storage_id,
            format!("table {}", table.name),
        )?;
        max_storage_id = max_storage_id.max(table.storage_id);
    }
    for index in snapshot.indexes_by_id.values() {
        validate_storage_id("index", index.storage_id)?;
        reject_seen_storage_id(
            &mut seen_indexes,
            index.storage_id,
            format!("index {}", index.name),
        )?;
        max_storage_id = max_storage_id.max(index.storage_id);
    }

    let required_next = if max_storage_id == 0 {
        default_next_storage_id()
    } else {
        next_storage_id_after(max_storage_id, "catalog storage id overflow")?
    };
    if snapshot.next_storage_id < required_next {
        return Err(DbError::internal(format!(
            "catalog snapshot next_storage_id {} is less than required {required_next}",
            snapshot.next_storage_id
        )));
    }
    if snapshot.next_storage_id > STORAGE_ID_EXHAUSTED {
        return Err(DbError::internal(format!(
            "catalog snapshot next_storage_id {} exceeds the storage-id space",
            snapshot.next_storage_id
        )));
    }
    Ok(())
}

fn reject_seen_storage_id(
    seen: &mut HashMap<FileId, String>,
    storage_id: FileId,
    owner: String,
) -> Result<()> {
    if let Some(existing) = seen.insert(storage_id, owner.clone()) {
        return Err(DbError::internal(format!(
            "catalog storage id {storage_id} is used by both {existing} and {owner}"
        )));
    }
    Ok(())
}

fn reject_duplicate_table_storage_id(
    snapshot: &CatalogSnapshot,
    storage_id: FileId,
    owner: &str,
) -> Result<()> {
    for table in snapshot.tables_by_id.values() {
        if table.storage_id == storage_id {
            return Err(DbError::internal(format!(
                "{owner} {storage_id} collides with table {}",
                table.name
            )));
        }
    }
    Ok(())
}

fn reject_duplicate_index_storage_id(
    snapshot: &CatalogSnapshot,
    storage_id: FileId,
    owner: &str,
) -> Result<()> {
    for index in snapshot.indexes_by_id.values() {
        if index.storage_id == storage_id {
            return Err(DbError::internal(format!(
                "{owner} {storage_id} collides with index {}",
                index.name
            )));
        }
    }
    Ok(())
}

fn validate_truncate_plan_storage_ids(plan: &TruncateTablePlan) -> Result<()> {
    let mut ids = HashSet::new();
    validate_truncate_plan_storage_id(plan.new_table_storage_id, &mut ids)?;
    if let Some((_, storage_id)) = plan.new_toast_storage_id {
        validate_truncate_plan_storage_id(storage_id, &mut ids)?;
    }
    for (_, storage_id) in &plan.new_index_storage_ids {
        validate_truncate_plan_storage_id(*storage_id, &mut ids)?;
    }
    Ok(())
}

fn validate_truncate_plan_storage_id(storage_id: FileId, ids: &mut HashSet<FileId>) -> Result<()> {
    validate_storage_id("truncate", storage_id)?;
    if !ids.insert(storage_id) {
        return Err(DbError::internal(format!(
            "truncate plan repeats storage id {storage_id}"
        )));
    }
    Ok(())
}

fn validate_truncate_storage_ids_available(
    snapshot: &CatalogSnapshot,
    plan: &TruncateTablePlan,
) -> Result<()> {
    let plan_ids = std::iter::once(plan.new_table_storage_id)
        .chain(plan.new_toast_storage_id.map(|(_, id)| id))
        .chain(plan.new_index_storage_ids.iter().map(|(_, id)| *id))
        .collect::<HashSet<_>>();

    for table in snapshot.tables_by_id.values() {
        if plan_ids.contains(&table.storage_id) {
            return Err(DbError::internal(format!(
                "truncate plan storage id collides with table {}",
                table.name
            )));
        }
    }
    for index in snapshot.indexes_by_id.values() {
        if plan_ids.contains(&index.storage_id) {
            return Err(DbError::internal(format!(
                "truncate plan storage id collides with index {}",
                index.name
            )));
        }
    }
    Ok(())
}

struct BuildSchemaInput {
    table_id: TableId,
    storage_id: FileId,
    name: String,
    columns: Vec<ParsedColumnDef>,
    primary_key: Vec<String>,
    compression: CompressionSetting,
    toast: ToastOptions,
    checks: Vec<String>,
}

fn build_schema(snapshot: &CatalogSnapshot, input: BuildSchemaInput) -> Result<TableSchema> {
    let BuildSchemaInput {
        table_id,
        storage_id,
        name,
        columns,
        primary_key,
        compression,
        toast,
        checks,
    } = input;

    let mut seen_names = HashSet::new();
    let mut column_ids_by_name = HashMap::new();
    let mut assigned_columns = Vec::with_capacity(columns.len());

    for (index, column) in columns.into_iter().enumerate() {
        if !seen_names.insert(column.name.clone()) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!("duplicate column {}", column.name),
            ));
        }

        let column_id: ColumnId = index
            .try_into()
            .map_err(|_| DbError::internal("catalog column id overflow"))?;
        column_ids_by_name.insert(column.name.clone(), column_id);
        let default = convert_column_default(snapshot, column.default)?;
        if matches!(default, Some(ColumnDefault::Nextval(_)))
            && column.data_type != DataType::Integer
        {
            return Err(DbError::plan(
                SqlState::DatatypeMismatch,
                format!(
                    "DEFAULT nextval for column {} requires INTEGER, got {:?}",
                    column.name, column.data_type
                ),
            ));
        }
        assigned_columns.push(ColumnDef {
            id: column_id,
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
            max_length: column.max_length,
            default,
            pg_type: column.pg_type,
        });
    }

    let mut primary_key_ids = Vec::with_capacity(primary_key.len());
    let mut seen_primary_key_names = HashSet::new();
    for primary_key_name in primary_key {
        if !seen_primary_key_names.insert(primary_key_name.clone()) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!("duplicate primary key column {primary_key_name}"),
            ));
        }

        let column_id = *column_ids_by_name.get(&primary_key_name).ok_or_else(|| {
            DbError::plan(
                SqlState::UndefinedColumn,
                format!("primary key column {primary_key_name} does not exist"),
            )
        })?;
        if let Some(column) = assigned_columns
            .iter_mut()
            .find(|column| column.id == column_id)
        {
            column.nullable = false;
        }
        primary_key_ids.push(column_id);
    }

    Ok(TableSchema {
        id: table_id,
        storage_id,
        name,
        columns: assigned_columns,
        primary_key: primary_key_ids,
        schema_version: common::INITIAL_SCHEMA_VERSION,
        compression,
        active_dict_id: None,
        toast,
        toast_table_id: None,
        relation_kind: RelationKind::User,
        checks,
    })
}

fn convert_column_default(
    snapshot: &CatalogSnapshot,
    default: Option<ParsedDefault>,
) -> Result<Option<ColumnDefault>> {
    match default {
        Some(ParsedDefault::Const(value)) => Ok(Some(ColumnDefault::Const(value))),
        Some(ParsedDefault::Serial) => Err(DbError::internal(
            "unresolved SERIAL default reached catalog create_table",
        )),
        Some(ParsedDefault::Nextval(name)) => resolve_sequence_default(snapshot, name, false),
        Some(ParsedDefault::OwnedNextval(name)) => resolve_sequence_default(snapshot, name, true),
        // A non-constant expression default is stored as canonical SQL text; the
        // binder validated it against the column at CREATE TABLE time.
        Some(ParsedDefault::Expr(text)) => Ok(Some(ColumnDefault::Expr(text))),
        None => Ok(None),
    }
}

enum AddColumnValidation {
    Noop,
    Rewrite {
        column: ColumnDef,
        toast_table_id: Option<TableId>,
    },
}

fn validate_add_table_column(
    snapshot: &CatalogSnapshot,
    schema: &TableSchema,
    column: &ParsedColumnDef,
    if_not_exists: bool,
) -> Result<AddColumnValidation> {
    ensure_user_table(schema)?;
    if schema
        .columns
        .iter()
        .any(|existing| existing.name == column.name)
    {
        if if_not_exists {
            return Ok(AddColumnValidation::Noop);
        }
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("column {} already exists", column.name),
        ));
    }

    reject_relation_wide_view_dependency(snapshot, schema.id, "add column")?;
    let column_id: ColumnId = schema
        .columns
        .len()
        .try_into()
        .map_err(|_| DbError::internal("catalog column id overflow"))?;
    let default = convert_column_default(snapshot, column.default.clone())?;
    if matches!(default, Some(ColumnDefault::Nextval(_))) && column.data_type != DataType::Integer {
        return Err(DbError::plan(
            SqlState::DatatypeMismatch,
            format!(
                "DEFAULT nextval for column {} requires INTEGER, got {:?}",
                column.name, column.data_type
            ),
        ));
    }

    let column = ColumnDef {
        id: column_id,
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        max_length: column.max_length,
        default,
        pg_type: column.pg_type.clone(),
    };
    let mut schema_after = schema.clone();
    schema_after.columns.push(column.clone());
    let new_column_is_toastable = matches!(&column.data_type, DataType::Text | DataType::Bytea);
    let toast_table_id = if new_column_is_toastable
        && schema.toast_table_id.is_none()
        && needs_toast_relation(&schema_after)
    {
        let toast_id = snapshot.next_table_id;
        reject_duplicate_relation_id(snapshot, toast_id)?;
        toast_id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog table id overflow"))?;
        Some(toast_id)
    } else {
        None
    };
    Ok(AddColumnValidation::Rewrite {
        column,
        toast_table_id,
    })
}

enum DropColumnValidation {
    Noop,
    Rewrite {
        position: usize,
        column_id: ColumnId,
    },
}

fn validate_drop_table_column(
    snapshot: &CatalogSnapshot,
    schema: &TableSchema,
    column: &str,
    if_exists: bool,
) -> Result<DropColumnValidation> {
    ensure_user_table(schema)?;
    let Some(position) = schema
        .columns
        .iter()
        .position(|existing| existing.name == column)
    else {
        if if_exists {
            return Ok(DropColumnValidation::Noop);
        }
        return Err(DbError::plan(
            SqlState::UndefinedColumn,
            format!("column {column} does not exist"),
        ));
    };
    let column_id = schema.columns[position].id;
    if schema.primary_key.contains(&column_id) {
        return Err(DbError::plan(
            SqlState::DependentObjectsStillExist,
            format!("cannot drop primary key column {column}"),
        ));
    }
    if !schema.checks.is_empty() {
        return Err(DbError::plan(
            SqlState::DependentObjectsStillExist,
            format!(
                "cannot drop column {column} because table {} has CHECK constraints",
                schema.name
            ),
        ));
    }
    reject_index_dependency(snapshot, schema.id, column_id, "drop")?;
    reject_view_column_dependency(snapshot, schema.id, column_id, "drop")?;
    reject_owned_sequence_default_drop(snapshot, &schema.columns[position])?;
    Ok(DropColumnValidation::Rewrite {
        position,
        column_id,
    })
}

fn build_view_schema(
    id: TableId,
    name: String,
    columns: Vec<ViewColumn>,
    definition: String,
    dependencies: Vec<ViewDependency>,
) -> Result<ViewSchema> {
    let mut assigned_columns = Vec::with_capacity(columns.len());
    let mut seen_names = HashSet::new();
    for (index, column) in columns.into_iter().enumerate() {
        if !seen_names.insert(column.name.clone()) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!("duplicate view column {}", column.name),
            ));
        }
        let column_id: ColumnId = index
            .try_into()
            .map_err(|_| DbError::internal("catalog view column id overflow"))?;
        assigned_columns.push(ColumnDef {
            id: column_id,
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
            max_length: None,
            default: None,
            pg_type: column.pg_type,
        });
    }
    if assigned_columns.is_empty() {
        return Err(DbError::plan(
            SqlState::SyntaxError,
            "view requires at least one output column",
        ));
    }
    Ok(ViewSchema {
        id,
        name,
        columns: assigned_columns,
        definition,
        dependencies,
        schema_version: common::INITIAL_SCHEMA_VERSION,
    })
}

fn resolve_sequence_default(
    snapshot: &CatalogSnapshot,
    name: String,
    allow_owned: bool,
) -> Result<Option<ColumnDefault>> {
    let id = snapshot.sequences_by_name.get(&name).ok_or_else(|| {
        DbError::plan(
            SqlState::UndefinedTable,
            format!("sequence {name} does not exist"),
        )
    })?;
    let sequence = snapshot.sequences_by_id.get(id).ok_or_else(|| {
        DbError::internal(format!(
            "catalog sequence name {name} points to missing sequence id {id}",
        ))
    })?;
    if sequence.owned && !allow_owned {
        return Err(DbError::plan(
            SqlState::DependentObjectsStillExist,
            format!("sequence {name} is owned by a SERIAL column"),
        ));
    }
    if allow_owned && !sequence.owned {
        return Err(DbError::internal(format!(
            "SERIAL default {name} resolved to a non-owned sequence"
        )));
    }
    Ok(Some(ColumnDefault::Nextval(*id)))
}

fn reject_referenced_sequence(snapshot: &CatalogSnapshot, sequence: SequenceId) -> Result<()> {
    for table in snapshot.tables_by_id.values() {
        for column in &table.columns {
            if matches!(&column.default, Some(ColumnDefault::Nextval(id)) if *id == sequence) {
                return Err(DbError::plan(
                    SqlState::DependentObjectsStillExist,
                    format!(
                        "cannot drop sequence {sequence} because table {} column {} depends on it",
                        table.name, column.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn build_sequence_schema(
    id: SequenceId,
    name: String,
    options: SequenceOptions,
    owned: bool,
) -> Result<SequenceSchema> {
    if options.increment == 0 {
        return Err(DbError::plan(
            SqlState::InvalidParameterValue,
            "INCREMENT BY 0 is not allowed",
        ));
    }

    let descending = options.increment < 0;
    let min_value = options
        .min_value
        .unwrap_or(if descending { i64::MIN } else { 1 });
    let max_value = options
        .max_value
        .unwrap_or(if descending { -1 } else { i64::MAX });
    if min_value > max_value {
        return Err(DbError::plan(
            SqlState::InvalidParameterValue,
            "MINVALUE cannot be greater than MAXVALUE",
        ));
    }

    let start = options
        .start
        .unwrap_or(if descending { max_value } else { min_value });
    if start < min_value || start > max_value {
        return Err(DbError::plan(
            SqlState::InvalidParameterValue,
            "START value must be between MINVALUE and MAXVALUE",
        ));
    }

    Ok(SequenceSchema {
        id,
        name,
        increment: options.increment,
        min_value,
        max_value,
        start,
        cycle: options.cycle,
        owned,
        last_value: start,
        is_called: false,
    })
}

pub fn validate_create_table_definition(
    name: &str,
    columns: &[ParsedColumnDef],
    primary_key: &[String],
    unique: &[Vec<String>],
) -> Result<()> {
    let columns_for_shape = columns
        .iter()
        .cloned()
        .map(|mut column| {
            column.default = None;
            column
        })
        .collect();
    let schema = build_schema(
        &CatalogSnapshot::default(),
        BuildSchemaInput {
            table_id: 0,
            storage_id: 1,
            name: name.to_string(),
            columns: columns_for_shape,
            primary_key: primary_key.to_vec(),
            compression: CompressionSetting::None,
            toast: ToastOptions::legacy_catalog_default(),
            checks: Vec::new(),
        },
    )?;
    let mut generated_unique_names = HashSet::new();
    for columns in unique {
        let index_name = format!("{}_{}_key", name, columns.join("_"));
        if !generated_unique_names.insert(index_name.clone()) {
            return Err(DbError::plan(
                SqlState::DuplicateTable,
                format!("index {index_name} already exists"),
            ));
        }
        build_index_schema(
            0,
            2,
            index_name,
            &schema,
            columns,
            true,
            IndexConstraintKind::None,
        )?;
    }
    Ok(())
}

fn validate_snapshot(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut max_relation_id = 0;
    validate_sequences(snapshot)?;

    for (name, id) in &snapshot.tables_by_name {
        let schema = snapshot.tables_by_id.get(id).ok_or_else(|| {
            DbError::internal(format!(
                "catalog snapshot name index {name} points to missing table id {id}",
            ))
        })?;
        if schema.relation_kind != RelationKind::User {
            return Err(DbError::internal(format!(
                "catalog snapshot name index {name} points to hidden TOAST relation id {id}",
            )));
        }
        if &schema.name != name || schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot name/id mismatch for table {name}",
            )));
        }
    }
    for (name, id) in &snapshot.views_by_name {
        let schema = snapshot.views_by_id.get(id).ok_or_else(|| {
            DbError::internal(format!(
                "catalog snapshot view name index {name} points to missing view id {id}",
            ))
        })?;
        if &schema.name != name || schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot name/id mismatch for view {name}",
            )));
        }
        if snapshot.tables_by_name.contains_key(name) {
            return Err(DbError::internal(format!(
                "catalog snapshot relation name {name} is used by both a table and a view",
            )));
        }
    }

    for (id, schema) in &snapshot.tables_by_id {
        if schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot table id key {id} does not match schema id {}",
                schema.id
            )));
        }
        match schema.relation_kind {
            RelationKind::User => {
                if snapshot.tables_by_name.get(&schema.name) != Some(id) {
                    return Err(DbError::internal(format!(
                        "catalog snapshot table {} is missing from name index",
                        schema.name
                    )));
                }
            }
            RelationKind::Toast { .. } => {
                if snapshot.tables_by_name.contains_key(&schema.name) {
                    return Err(DbError::internal(format!(
                        "catalog snapshot hidden TOAST relation {} must not be in the name index",
                        schema.name
                    )));
                }
            }
        }
        validate_schema(schema, &snapshot.sequences_by_id)?;
        max_relation_id = max_relation_id.max(*id);
    }

    for (id, schema) in &snapshot.views_by_id {
        if schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot view id key {id} does not match schema id {}",
                schema.id
            )));
        }
        if snapshot.views_by_name.get(&schema.name) != Some(id) {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} is missing from name index",
                schema.name
            )));
        }
        if snapshot.tables_by_id.contains_key(id) {
            return Err(DbError::internal(format!(
                "catalog snapshot relation id {id} is used by both a table and a view",
            )));
        }
        validate_view_schema(schema, snapshot)?;
        max_relation_id = max_relation_id.max(*id);
    }

    let required_next = max_relation_id
        .checked_add(1)
        .ok_or_else(|| DbError::internal("catalog snapshot table id overflow"))?;
    if snapshot.next_table_id < required_next {
        return Err(DbError::internal(format!(
            "catalog snapshot next_table_id {} is less than required {required_next}",
            snapshot.next_table_id
        )));
    }

    validate_indexes(snapshot)?;
    validate_public_relation_namespace(snapshot)?;
    validate_dictionary_ids(snapshot)?;
    validate_storage_ids(snapshot)?;
    validate_toast_relations(snapshot)?;
    Ok(())
}

fn validate_public_relation_namespace(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut seen = HashMap::new();
    for table in snapshot.tables_by_id.values() {
        if table.relation_kind != RelationKind::User {
            continue;
        }
        record_public_relation_name(&mut seen, "table", &table.name)?;
        let has_matching_primary_key_index = snapshot.indexes_by_id.values().any(|index| {
            index.table == table.id && is_matching_primary_key_constraint_index(snapshot, index)
        });
        if !has_matching_primary_key_index && !table.primary_key.is_empty() {
            record_public_relation_name(
                &mut seen,
                "synthetic primary-key index",
                &synthetic_primary_key_index_name(table),
            )?;
        }
    }
    for index in snapshot.indexes_by_id.values() {
        record_public_relation_name(&mut seen, "index", &index.name)?;
    }
    for sequence in snapshot.sequences_by_id.values() {
        record_public_relation_name(&mut seen, "sequence", &sequence.name)?;
    }
    Ok(())
}

fn record_public_relation_name(
    seen: &mut HashMap<String, &'static str>,
    kind: &'static str,
    name: &str,
) -> Result<()> {
    if let Some(existing_kind) = seen.insert(name.to_string(), kind) {
        return Err(DbError::internal(format!(
            "catalog snapshot public relation name {name} is used by both {existing_kind} and {kind}"
        )));
    }
    Ok(())
}

fn validate_dictionary_ids(snapshot: &CatalogSnapshot) -> Result<()> {
    if snapshot.next_dictionary_id < 1 {
        return Err(DbError::internal(format!(
            "catalog snapshot next_dictionary_id {} must be at least 1 (0 is reserved for \"no dictionary\")",
            snapshot.next_dictionary_id
        )));
    }
    for schema in snapshot.tables_by_id.values() {
        validate_dictionary_ref(
            &schema.name,
            "active_dict_id",
            schema.active_dict_id,
            snapshot.next_dictionary_id,
        )?;
        validate_dictionary_ref(
            &schema.name,
            "toast active_dict_id",
            schema.toast.active_dict_id,
            snapshot.next_dictionary_id,
        )?;
    }
    Ok(())
}

fn validate_dictionary_ref(
    table_name: &str,
    field_name: &str,
    active_dict_id: Option<u32>,
    next_dictionary_id: u32,
) -> Result<()> {
    let Some(active_dict_id) = active_dict_id else {
        return Ok(());
    };
    if active_dict_id == 0 {
        return Err(DbError::internal(format!(
            "catalog snapshot table {table_name} {field_name} is reserved value 0 (0 means \"no dictionary\"; use None instead)"
        )));
    }
    if active_dict_id >= next_dictionary_id {
        return Err(DbError::internal(format!(
            "catalog snapshot table {table_name} {field_name} {active_dict_id} is not less than next_dictionary_id {next_dictionary_id}"
        )));
    }
    Ok(())
}

fn validate_toast_relations(snapshot: &CatalogSnapshot) -> Result<()> {
    for schema in snapshot.tables_by_id.values() {
        match schema.relation_kind {
            RelationKind::User => {
                if let Some(toast_table_id) = schema.toast_table_id {
                    if toast_table_id == schema.id {
                        return Err(DbError::internal(format!(
                            "catalog snapshot table {} references itself as a TOAST relation",
                            schema.name
                        )));
                    }
                    let hidden_schema =
                        snapshot.tables_by_id.get(&toast_table_id).ok_or_else(|| {
                            DbError::internal(format!(
                                "catalog snapshot table {} references missing TOAST relation {}",
                                schema.name, toast_table_id
                            ))
                        })?;
                    if hidden_schema.relation_kind
                        != (RelationKind::Toast {
                            base_table: schema.id,
                        })
                    {
                        return Err(DbError::internal(format!(
                            "catalog snapshot table {} references non-matching TOAST relation {}",
                            schema.name, toast_table_id
                        )));
                    }
                    validate_hidden_toast_schema(schema, hidden_schema)?;
                }
            }
            RelationKind::Toast { base_table } => {
                if base_table == schema.id {
                    return Err(DbError::internal(format!(
                        "catalog snapshot TOAST relation {} references itself as base table",
                        schema.name
                    )));
                }
                if schema.toast_table_id.is_some() {
                    return Err(DbError::internal(format!(
                        "catalog snapshot TOAST relation {} must not have its own toast_table_id",
                        schema.name
                    )));
                }
                if schema.toast.mode != ToastMode::Off {
                    return Err(DbError::internal(format!(
                        "catalog snapshot TOAST relation {} must have toast mode Off",
                        schema.name
                    )));
                }
                let base_schema = snapshot.tables_by_id.get(&base_table).ok_or_else(|| {
                    DbError::internal(format!(
                        "catalog snapshot TOAST relation {} references missing base table {}",
                        schema.name, base_table
                    ))
                })?;
                if base_schema.relation_kind != RelationKind::User {
                    return Err(DbError::internal(format!(
                        "catalog snapshot TOAST relation {} references non-user base table {}",
                        schema.name, base_table
                    )));
                }
                if base_schema.toast_table_id != Some(schema.id) {
                    return Err(DbError::internal(format!(
                        "catalog snapshot TOAST relation {} is not linked from base table {}",
                        schema.name, base_table
                    )));
                }
                validate_hidden_toast_schema(base_schema, schema)?;
            }
        }
    }
    Ok(())
}

fn validate_hidden_toast_schema(base: &TableSchema, hidden: &TableSchema) -> Result<()> {
    let mut expected = toast_schema(base, hidden.id);
    expected.storage_id = hidden.storage_id;
    if hidden != &expected {
        return Err(DbError::internal(format!(
            "catalog snapshot TOAST relation {} does not match the required internal schema",
            hidden.name
        )));
    }
    Ok(())
}

fn validate_indexes(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut max_index_id = 0;

    for (name, id) in &snapshot.indexes_by_name {
        let schema = snapshot.indexes_by_id.get(id).ok_or_else(|| {
            DbError::internal(format!(
                "catalog snapshot index name {name} points to missing index id {id}",
            ))
        })?;
        if &schema.name != name || schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot index name/id mismatch for index {name}",
            )));
        }
    }

    for (id, schema) in &snapshot.indexes_by_id {
        if schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot index id key {id} does not match schema id {}",
                schema.id
            )));
        }
        if snapshot.indexes_by_name.get(&schema.name) != Some(id) {
            return Err(DbError::internal(format!(
                "catalog snapshot index {} is missing from name index",
                schema.name
            )));
        }
        validate_index_schema(schema, &snapshot.tables_by_id)?;
        max_index_id = max_index_id.max(*id);
    }
    for table in snapshot.tables_by_id.values() {
        validate_no_index_name_matching_synthetic_primary_key(snapshot, table)?;
    }

    let required_next = max_index_id
        .checked_add(1)
        .ok_or_else(|| DbError::internal("catalog snapshot index id overflow"))?;
    if snapshot.next_index_id < required_next {
        return Err(DbError::internal(format!(
            "catalog snapshot next_index_id {} is less than required {required_next}",
            snapshot.next_index_id
        )));
    }
    validate_user_primary_key_indexes(snapshot)?;

    Ok(())
}

fn validate_index_schema(
    schema: &IndexSchema,
    tables_by_id: &HashMap<TableId, TableSchema>,
) -> Result<()> {
    if schema.id == PRIMARY_KEY_INDEX_ID {
        return Err(DbError::internal(
            "catalog snapshot uses the reserved storage identity index id for a catalog index",
        ));
    }
    validate_virtual_oid_id("index", &schema.name, schema.id, MAX_VIRTUAL_OID_PAYLOAD)?;
    let table = tables_by_id.get(&schema.table).ok_or_else(|| {
        DbError::internal(format!(
            "catalog snapshot index {} references missing table {}",
            schema.name, schema.table
        ))
    })?;

    validate_index_schema_for_table(schema, table)
}

fn validate_index_schema_for_table(schema: &IndexSchema, table: &TableSchema) -> Result<()> {
    if schema.columns.is_empty() {
        return Err(DbError::internal(format!(
            "catalog index {} has no columns",
            schema.name
        )));
    }
    if matches!(
        schema.constraint,
        IndexConstraintKind::Unique | IndexConstraintKind::PrimaryKey
    ) && !schema.unique
    {
        return Err(DbError::internal(format!(
            "catalog constraint index {} is not unique",
            schema.name
        )));
    }
    if schema.constraint == IndexConstraintKind::PrimaryKey && schema.columns != table.primary_key {
        return Err(DbError::internal(format!(
            "catalog primary-key index {} does not match table {} primary key",
            schema.name, table.name
        )));
    }

    let mut seen = HashSet::new();
    for column_id in &schema.columns {
        if !table.columns.iter().any(|column| column.id == *column_id) {
            return Err(DbError::internal(format!(
                "catalog index {} references missing column {} on table {}",
                schema.name, column_id, schema.table
            )));
        }
        if !seen.insert(*column_id) {
            return Err(DbError::internal(format!(
                "catalog index {} has duplicate column {}",
                schema.name, column_id
            )));
        }
    }

    Ok(())
}

fn validate_user_primary_key_indexes(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut counts_by_table: HashMap<TableId, usize> = HashMap::new();
    for index in snapshot.indexes_by_id.values() {
        if index.constraint == IndexConstraintKind::PrimaryKey {
            *counts_by_table.entry(index.table).or_default() += 1;
        }
    }

    for table in snapshot.tables_by_id.values() {
        if table.relation_kind != RelationKind::User || table.primary_key.is_empty() {
            continue;
        }
        match counts_by_table.get(&table.id).copied().unwrap_or(0) {
            0 => {
                return Err(DbError::internal(format!(
                    "catalog snapshot table {} has a primary key but no primary-key constraint index",
                    table.name
                )));
            }
            1 => {}
            count => {
                return Err(DbError::internal(format!(
                    "catalog snapshot table {} has {count} primary-key constraint indexes",
                    table.name
                )));
            }
        }
    }

    Ok(())
}

fn validate_sequences(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut max_sequence_id = 0;

    for (name, id) in &snapshot.sequences_by_name {
        let schema = snapshot.sequences_by_id.get(id).ok_or_else(|| {
            DbError::internal(format!(
                "catalog snapshot sequence name {name} points to missing sequence id {id}",
            ))
        })?;
        if &schema.name != name || schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot sequence name/id mismatch for sequence {name}",
            )));
        }
    }

    for (id, schema) in &snapshot.sequences_by_id {
        if schema.id != *id {
            return Err(DbError::internal(format!(
                "catalog snapshot sequence id key {id} does not match schema id {}",
                schema.id
            )));
        }
        if snapshot.sequences_by_name.get(&schema.name) != Some(id) {
            return Err(DbError::internal(format!(
                "catalog snapshot sequence {} is missing from name index",
                schema.name
            )));
        }
        validate_sequence_schema(schema)?;
        max_sequence_id = max_sequence_id.max(*id);
    }

    let required_next = max_sequence_id
        .checked_add(1)
        .ok_or_else(|| DbError::internal("catalog snapshot sequence id overflow"))?;
    if snapshot.next_sequence_id < required_next {
        return Err(DbError::internal(format!(
            "catalog snapshot next_sequence_id {} is less than required {required_next}",
            snapshot.next_sequence_id
        )));
    }

    Ok(())
}

fn validate_sequence_schema(schema: &SequenceSchema) -> Result<()> {
    validate_virtual_oid_id("sequence", &schema.name, schema.id, MAX_VIRTUAL_OID_PAYLOAD)?;

    if schema.increment == 0 {
        return Err(DbError::internal(format!(
            "catalog snapshot sequence {} has zero increment",
            schema.name
        )));
    }
    if schema.min_value > schema.max_value {
        return Err(DbError::internal(format!(
            "catalog snapshot sequence {} has MINVALUE greater than MAXVALUE",
            schema.name
        )));
    }
    if schema.start < schema.min_value || schema.start > schema.max_value {
        return Err(DbError::internal(format!(
            "catalog snapshot sequence {} has START outside MINVALUE/MAXVALUE",
            schema.name
        )));
    }
    if schema.last_value < schema.min_value || schema.last_value > schema.max_value {
        return Err(DbError::internal(format!(
            "catalog snapshot sequence {} has last_value outside MINVALUE/MAXVALUE",
            schema.name
        )));
    }
    Ok(())
}

fn validate_schema(
    schema: &TableSchema,
    sequences_by_id: &HashMap<SequenceId, SequenceSchema>,
) -> Result<()> {
    if schema.schema_version == 0 {
        return Err(DbError::internal(format!(
            "catalog snapshot table {} has schema_version 0",
            schema.name
        )));
    }
    validate_toast_options(schema)?;
    validate_table_virtual_oids(schema)?;

    let mut column_ids = HashSet::new();
    let mut column_names = HashSet::new();
    for (expected_id, column) in schema.columns.iter().enumerate() {
        let expected_id: ColumnId = expected_id
            .try_into()
            .map_err(|_| DbError::internal("catalog snapshot column id overflow"))?;
        if column.id != expected_id {
            return Err(DbError::internal(format!(
                "catalog snapshot table {} has column id {} at position {}, expected {}",
                schema.name,
                column.id,
                usize::from(expected_id),
                expected_id
            )));
        }
        if !column_ids.insert(column.id) {
            return Err(DbError::internal(format!(
                "catalog snapshot table {} has duplicate column id {}",
                schema.name, column.id
            )));
        }
        if !column_names.insert(column.name.clone()) {
            return Err(DbError::internal(format!(
                "catalog snapshot table {} has duplicate column {}",
                schema.name, column.name
            )));
        }
        validate_column_default(&schema.name, column, sequences_by_id)?;
    }

    let mut primary_key_ids = HashSet::new();
    for column_id in &schema.primary_key {
        let Some(column) = schema.columns.iter().find(|column| column.id == *column_id) else {
            return Err(DbError::internal(format!(
                "catalog snapshot table {} primary key references missing column {}",
                schema.name, column_id
            )));
        };
        if column.nullable {
            return Err(DbError::internal(format!(
                "catalog snapshot table {} primary key column {} is nullable",
                schema.name, column_id
            )));
        }
        if !primary_key_ids.insert(*column_id) {
            return Err(DbError::internal(format!(
                "catalog snapshot table {} has duplicate primary key column {}",
                schema.name, column_id
            )));
        }
    }

    Ok(())
}

fn set_primary_key_columns(schema: &mut TableSchema, primary_key: Vec<ColumnId>) -> Result<()> {
    let mut seen = HashSet::new();
    for column_id in &primary_key {
        if !seen.insert(*column_id) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!("duplicate primary key column id {column_id}"),
            ));
        }
        let column = schema
            .columns
            .iter_mut()
            .find(|column| column.id == *column_id)
            .ok_or_else(|| {
                DbError::plan(
                    SqlState::UndefinedColumn,
                    format!("primary key column id {column_id} does not exist"),
                )
            })?;
        column.nullable = false;
    }
    schema.primary_key = primary_key;
    Ok(())
}

fn validate_view_schema(schema: &ViewSchema, snapshot: &CatalogSnapshot) -> Result<()> {
    if schema.schema_version == 0 {
        return Err(DbError::internal(format!(
            "catalog snapshot view {} has schema_version 0",
            schema.name
        )));
    }
    if schema.definition.trim().is_empty() {
        return Err(DbError::internal(format!(
            "catalog snapshot view {} has an empty definition",
            schema.name
        )));
    }
    validate_view_columns(schema)?;
    validate_view_dependencies(schema, snapshot)
}

fn validate_live_view_schema(schema: &ViewSchema, snapshot: &CatalogSnapshot) -> Result<()> {
    if schema.definition.trim().is_empty() {
        return Err(DbError::plan(
            SqlState::SyntaxError,
            format!("view {} requires a non-empty definition", schema.name),
        ));
    }
    validate_view_columns(schema)?;
    validate_live_view_dependencies(schema, snapshot)
}

fn validate_view_columns(schema: &ViewSchema) -> Result<()> {
    if schema.columns.is_empty() {
        return Err(DbError::internal(format!(
            "catalog snapshot view {} must have at least one column",
            schema.name
        )));
    }
    let mut seen_names = HashSet::new();
    for (expected_id, column) in schema.columns.iter().enumerate() {
        let expected_id: ColumnId = expected_id
            .try_into()
            .map_err(|_| DbError::internal("catalog snapshot view column id overflow"))?;
        if column.id != expected_id {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} has column id {} at position {}, expected {}",
                schema.name,
                column.id,
                usize::from(expected_id),
                expected_id
            )));
        }
        if !seen_names.insert(column.name.clone()) {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} has duplicate column {}",
                schema.name, column.name
            )));
        }
        if column.default.is_some() {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} column {} has a column default",
                schema.name, column.name
            )));
        }
    }
    Ok(())
}

fn validate_table_virtual_oids(schema: &TableSchema) -> Result<()> {
    validate_virtual_oid_id("table", &schema.name, schema.id, MAX_VIRTUAL_OID_PAYLOAD)?;
    validate_virtual_oid_id("table", &schema.name, schema.id, MAX_COMPOUND_OID_TABLE_ID)?;

    let max_default_column_id = schema
        .columns
        .iter()
        .filter(|column| column.default.is_some())
        .map(|column| column.id)
        .max();
    if let Some(column_id) = max_default_column_id
        && column_id > MAX_COMPOUND_OID_SUB_ID
    {
        return Err(DbError::internal(format!(
            "catalog snapshot table {} column id {column_id} exceeds compound virtual OID sub-id limit {}",
            schema.name, MAX_COMPOUND_OID_SUB_ID
        )));
    }

    if schema.checks.len() > usize::from(MAX_COMPOUND_OID_SUB_ID) {
        return Err(DbError::internal(format!(
            "catalog snapshot table {} has {} CHECK constraints, exceeding compound virtual OID sub-id limit {}",
            schema.name,
            schema.checks.len(),
            MAX_COMPOUND_OID_SUB_ID
        )));
    }
    Ok(())
}

fn validate_live_view_dependencies(schema: &ViewSchema, snapshot: &CatalogSnapshot) -> Result<()> {
    let mut seen = HashSet::new();
    for dependency in &schema.dependencies {
        if dependency.relation == schema.id {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                format!("view {} cannot depend on itself", schema.name),
            ));
        }
        if dependency.all_columns && !dependency.columns.is_empty() {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!(
                    "view {} dependency on relation {} cannot be both column-specific and all-column",
                    schema.name, dependency.relation
                ),
            ));
        }
        if !seen.insert((
            dependency.relation,
            dependency.columns.clone(),
            dependency.all_columns,
        )) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!(
                    "view {} has duplicate dependency on relation {}",
                    schema.name, dependency.relation
                ),
            ));
        }
        if snapshot.views_by_id.contains_key(&dependency.relation) {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                format!(
                    "view {} cannot depend on view relation {} yet",
                    schema.name, dependency.relation
                ),
            ));
        }
        let table = snapshot
            .tables_by_id
            .get(&dependency.relation)
            .ok_or_else(|| {
                undefined_table(format!("relation {} does not exist", dependency.relation))
            })?;
        if table.relation_kind != RelationKind::User {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                format!(
                    "view {} cannot depend on hidden relation {}",
                    schema.name, dependency.relation
                ),
            ));
        }
        let mut seen_columns = HashSet::new();
        for column_id in &dependency.columns {
            if !table.columns.iter().any(|column| column.id == *column_id) {
                return Err(DbError::plan(
                    SqlState::UndefinedColumn,
                    format!(
                        "view {} references missing column {} on relation {}",
                        schema.name, column_id, dependency.relation
                    ),
                ));
            }
            if !seen_columns.insert(*column_id) {
                return Err(DbError::plan(
                    SqlState::SyntaxError,
                    format!(
                        "view {} has duplicate dependency column {} on relation {}",
                        schema.name, column_id, dependency.relation
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_view_dependencies(schema: &ViewSchema, snapshot: &CatalogSnapshot) -> Result<()> {
    let mut seen = HashSet::new();
    for dependency in &schema.dependencies {
        if dependency.relation == schema.id {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} depends on itself",
                schema.name
            )));
        }
        if dependency.all_columns && !dependency.columns.is_empty() {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} dependency on relation {} is both column-specific and all-column",
                schema.name, dependency.relation
            )));
        }
        if !seen.insert((
            dependency.relation,
            dependency.columns.clone(),
            dependency.all_columns,
        )) {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} has duplicate dependency on relation {}",
                schema.name, dependency.relation
            )));
        }
        if snapshot.views_by_id.contains_key(&dependency.relation) {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} references view relation {}",
                schema.name, dependency.relation
            )));
        }
        if let Some(table) = snapshot.tables_by_id.get(&dependency.relation)
            && table.relation_kind != RelationKind::User
        {
            return Err(DbError::internal(format!(
                "catalog snapshot view {} references hidden relation {}",
                schema.name, dependency.relation
            )));
        }
        let columns = relation_columns(snapshot, dependency.relation).ok_or_else(|| {
            DbError::internal(format!(
                "catalog snapshot view {} references missing relation {}",
                schema.name, dependency.relation
            ))
        })?;
        let mut seen_columns = HashSet::new();
        for column_id in &dependency.columns {
            if !columns.iter().any(|column| column.id == *column_id) {
                return Err(DbError::internal(format!(
                    "catalog snapshot view {} references missing column {} on relation {}",
                    schema.name, column_id, dependency.relation
                )));
            }
            if !seen_columns.insert(*column_id) {
                return Err(DbError::internal(format!(
                    "catalog snapshot view {} has duplicate dependency column {} on relation {}",
                    schema.name, column_id, dependency.relation
                )));
            }
        }
    }
    Ok(())
}

fn validate_virtual_oid_id(kind: &str, name: &str, id: u32, max: u32) -> Result<()> {
    if id > max {
        return Err(DbError::internal(format!(
            "catalog snapshot {kind} {name} id {id} exceeds virtual OID payload limit {max}"
        )));
    }
    Ok(())
}

fn validate_toast_options(schema: &TableSchema) -> Result<()> {
    if !(ToastOptions::MIN_TOAST_TUPLE_TARGET..=ToastOptions::MAX_TOAST_TUPLE_TARGET)
        .contains(&schema.toast.tuple_target)
    {
        return Err(DbError::internal(format!(
            "catalog snapshot table {} toast tuple_target {} is outside {}..={}",
            schema.name,
            schema.toast.tuple_target,
            ToastOptions::MIN_TOAST_TUPLE_TARGET,
            ToastOptions::MAX_TOAST_TUPLE_TARGET
        )));
    }
    if schema.toast.min_value_size < ToastOptions::MIN_TOAST_MIN_VALUE_SIZE {
        return Err(DbError::internal(format!(
            "catalog snapshot table {} toast min_value_size {} is below {}",
            schema.name,
            schema.toast.min_value_size,
            ToastOptions::MIN_TOAST_MIN_VALUE_SIZE
        )));
    }
    Ok(())
}

fn validate_column_default(
    table_name: &str,
    column: &ColumnDef,
    sequences_by_id: &HashMap<SequenceId, SequenceSchema>,
) -> Result<()> {
    match &column.default {
        Some(ColumnDefault::Nextval(_)) if column.data_type != DataType::Integer => {
            Err(DbError::internal(format!(
                "catalog snapshot table {table_name} column {} has sequence default on non-INTEGER column",
                column.name
            )))
        }
        Some(ColumnDefault::Nextval(sequence)) if !sequences_by_id.contains_key(sequence) => {
            Err(DbError::internal(format!(
                "catalog snapshot table {table_name} column {} references missing sequence {}",
                column.name, sequence
            )))
        }
        _ => Ok(()),
    }
}

fn relation_columns(snapshot: &CatalogSnapshot, relation: TableId) -> Option<&[ColumnDef]> {
    snapshot
        .tables_by_id
        .get(&relation)
        .map(|schema| schema.columns.as_slice())
        .or_else(|| {
            snapshot
                .views_by_id
                .get(&relation)
                .map(|schema| schema.columns.as_slice())
        })
}

fn ensure_user_table(schema: &TableSchema) -> Result<()> {
    if schema.relation_kind != RelationKind::User {
        return Err(DbError::plan(
            SqlState::UndefinedTable,
            format!("table id {} is not a user table", schema.id),
        ));
    }
    Ok(())
}

fn replace_table_schema(snapshot: &mut CatalogSnapshot, schema: TableSchema) -> Result<()> {
    replace_table_and_index_schemas(snapshot, schema, &[])
}

fn replace_table_and_index_schemas(
    snapshot: &mut CatalogSnapshot,
    schema: TableSchema,
    indexes: &[IndexSchema],
) -> Result<()> {
    let old = snapshot
        .tables_by_id
        .get(&schema.id)
        .cloned()
        .ok_or_else(|| undefined_table(format!("table id {} does not exist", schema.id)))?;
    if old.relation_kind != schema.relation_kind {
        return Err(DbError::internal(format!(
            "cannot change relation kind for table id {}",
            schema.id
        )));
    }
    validate_schema(&schema, &snapshot.sequences_by_id)?;

    let mut candidate = snapshot.clone();
    carry_view_dependencies_for_table_update(&old, &schema, &mut candidate.views_by_id)?;
    if old.relation_kind == RelationKind::User {
        if old.name != schema.name {
            reject_duplicate_relation_name(snapshot, "table", &schema.name)?;
            candidate.tables_by_name.remove(&old.name);
            candidate
                .tables_by_name
                .insert(schema.name.clone(), schema.id);
        } else if candidate.tables_by_name.get(&schema.name) != Some(&schema.id) {
            return Err(DbError::internal(format!(
                "catalog table {} is missing from name index",
                schema.name
            )));
        }
    }
    candidate.tables_by_id.insert(schema.id, schema);
    for index in indexes {
        let old_index = snapshot
            .indexes_by_id
            .get(&index.id)
            .cloned()
            .ok_or_else(|| undefined_index(format!("index id {} does not exist", index.id)))?;
        if index.table != old.id {
            return Err(DbError::internal(format!(
                "catalog index {} references table {}, expected {}",
                index.name, index.table, old.id
            )));
        }
        if old_index.name != index.name {
            if candidate
                .indexes_by_name
                .get(&index.name)
                .is_some_and(|existing| *existing != index.id)
            {
                return Err(DbError::plan(
                    SqlState::DuplicateTable,
                    format!("index {} already exists", index.name),
                ));
            }
            candidate.indexes_by_name.remove(&old_index.name);
            candidate
                .indexes_by_name
                .insert(index.name.clone(), index.id);
        } else if candidate.indexes_by_name.get(&index.name) != Some(&index.id) {
            return Err(DbError::internal(format!(
                "catalog index {} is missing from name index",
                index.name
            )));
        }
        candidate.indexes_by_id.insert(index.id, index.clone());
    }
    validate_snapshot(&candidate)?;
    *snapshot = candidate;
    Ok(())
}

fn carry_view_dependencies_for_table_update(
    old: &TableSchema,
    new: &TableSchema,
    views: &mut HashMap<TableId, ViewSchema>,
) -> Result<()> {
    let columns_changed =
        !table_columns_equivalent_for_view_dependencies(&old.columns, &new.columns);
    for view in views.values_mut() {
        for dependency in &mut view.dependencies {
            if dependency.relation != old.id {
                continue;
            }
            if old.name != new.name {
                return Err(DbError::internal(format!(
                    "cannot apply table rename for {} because view {} depends on it",
                    old.name, view.name
                )));
            }
            if dependency.all_columns {
                if columns_changed {
                    return Err(DbError::internal(format!(
                        "cannot apply table schema update for {} because view {} depends on all columns",
                        old.name, view.name
                    )));
                }
                continue;
            }
            if dependency.columns.is_empty() {
                continue;
            }

            let mut remapped = Vec::with_capacity(dependency.columns.len());
            let mut seen = HashSet::new();
            for old_column_id in &dependency.columns {
                let old_column = old
                    .columns
                    .iter()
                    .find(|column| column.id == *old_column_id)
                    .ok_or_else(|| {
                        DbError::internal(format!(
                            "view {} dependency references missing column {} on old table {}",
                            view.name, old_column_id, old.name
                        ))
                    })?;
                let new_column = new
                    .columns
                    .iter()
                    .find(|column| column.name == old_column.name)
                    .ok_or_else(|| {
                        DbError::internal(format!(
                            "cannot apply table schema update for {} because view {} depends on removed or renamed column {}",
                            old.name, view.name, old_column.name
                        ))
                    })?;
                if !view_dependency_column_compatible(old_column, new_column) {
                    return Err(DbError::internal(format!(
                        "cannot apply table schema update for {} because view {} depends on changed column {}",
                        old.name, view.name, old_column.name
                    )));
                }
                if !seen.insert(new_column.id) {
                    return Err(DbError::internal(format!(
                        "table schema update for {} maps duplicate view dependency column {} in view {}",
                        old.name, new_column.id, view.name
                    )));
                }
                remapped.push(new_column.id);
            }
            dependency.columns = remapped;
        }
    }
    Ok(())
}

fn table_columns_equivalent_for_view_dependencies(left: &[ColumnDef], right: &[ColumnDef]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right.iter()).all(|(left, right)| {
            left.id == right.id && view_dependency_column_compatible(left, right)
        })
}

fn view_dependency_column_compatible(left: &ColumnDef, right: &ColumnDef) -> bool {
    left.name == right.name
        && left.data_type == right.data_type
        && left.max_length == right.max_length
        && left.pg_type == right.pg_type
}

fn bump_schema_version(version: &mut u64) -> Result<()> {
    *version = version
        .checked_add(1)
        .ok_or_else(|| DbError::internal("catalog schema version overflow"))?;
    Ok(())
}

fn reject_dependent_views(
    snapshot: &CatalogSnapshot,
    relation: TableId,
    excluding_view: Option<TableId>,
) -> Result<()> {
    for view in snapshot.views_by_id.values() {
        if Some(view.id) == excluding_view {
            continue;
        }
        if view
            .dependencies
            .iter()
            .any(|dependency| dependency.relation == relation)
        {
            return Err(DbError::plan(
                SqlState::DependentObjectsStillExist,
                format!(
                    "cannot drop relation {relation} because view {} depends on it",
                    view.name
                ),
            ));
        }
    }
    Ok(())
}

fn reject_relation_wide_view_dependency(
    snapshot: &CatalogSnapshot,
    relation: TableId,
    action: &str,
) -> Result<()> {
    for view in snapshot.views_by_id.values() {
        if view
            .dependencies
            .iter()
            .any(|dependency| dependency.relation == relation && dependency.all_columns)
        {
            return Err(DbError::plan(
                SqlState::DependentObjectsStillExist,
                format!(
                    "cannot {action} on relation {relation} because view {} depends on all columns",
                    view.name
                ),
            ));
        }
    }
    Ok(())
}

fn reject_view_column_dependency(
    snapshot: &CatalogSnapshot,
    relation: TableId,
    column: ColumnId,
    action: &str,
) -> Result<()> {
    for view in snapshot.views_by_id.values() {
        for dependency in &view.dependencies {
            if dependency.relation == relation
                && (dependency.all_columns || dependency.columns.contains(&column))
            {
                return Err(DbError::plan(
                    SqlState::DependentObjectsStillExist,
                    format!(
                        "cannot {action} column {column} because view {} depends on it",
                        view.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn reject_owned_sequence_default_drop(
    snapshot: &CatalogSnapshot,
    column: &ColumnDef,
) -> Result<()> {
    let Some(ColumnDefault::Nextval(sequence_id)) = column.default.as_ref() else {
        return Ok(());
    };
    let sequence = snapshot.sequences_by_id.get(sequence_id).ok_or_else(|| {
        DbError::internal(format!(
            "column {} references missing sequence {sequence_id}",
            column.name
        ))
    })?;
    if sequence.owned {
        return Err(DbError::plan(
            SqlState::DependentObjectsStillExist,
            format!(
                "cannot drop column {} because it owns sequence {}",
                column.name, sequence.name
            ),
        ));
    }
    Ok(())
}

fn reject_index_dependency(
    snapshot: &CatalogSnapshot,
    table: TableId,
    column: ColumnId,
    action: &str,
) -> Result<()> {
    for index in snapshot.indexes_by_id.values() {
        if index.table == table && index.columns.contains(&column) {
            return Err(DbError::plan(
                SqlState::DependentObjectsStillExist,
                format!(
                    "cannot {action} column {column} because index {} depends on it",
                    index.name
                ),
            ));
        }
    }
    Ok(())
}

fn remap_columns_after_drop(schema: &mut TableSchema, dropped: ColumnId) {
    for column in &mut schema.columns {
        if column.id > dropped {
            column.id -= 1;
        }
    }
    for column_id in &mut schema.primary_key {
        if *column_id > dropped {
            *column_id -= 1;
        }
    }
}

fn remap_indexes_after_drop(snapshot: &mut CatalogSnapshot, table: TableId, dropped: ColumnId) {
    for index in snapshot.indexes_by_id.values_mut() {
        if index.table != table {
            continue;
        }
        for column_id in &mut index.columns {
            if *column_id > dropped {
                *column_id -= 1;
            }
        }
    }
}

fn reject_duplicate_relation_id(snapshot: &CatalogSnapshot, id: TableId) -> Result<()> {
    if snapshot.tables_by_id.contains_key(&id) || snapshot.views_by_id.contains_key(&id) {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("relation id {id} already exists"),
        ));
    }
    Ok(())
}

fn reject_duplicate_sequence_name(snapshot: &CatalogSnapshot, name: &str) -> Result<()> {
    reject_duplicate_relation_name(snapshot, "sequence", name)
}

fn reject_duplicate_sequence_id(snapshot: &CatalogSnapshot, id: SequenceId) -> Result<()> {
    if snapshot.sequences_by_id.contains_key(&id) {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("sequence id {id} already exists"),
        ));
    }
    Ok(())
}

fn undefined_table(message: String) -> DbError {
    DbError::plan(SqlState::UndefinedTable, message)
}

fn undefined_view(message: String) -> DbError {
    DbError::plan(SqlState::UndefinedTable, message)
}

fn undefined_index(message: String) -> DbError {
    // Indexes share the relation namespace; v1 has no dedicated SQLSTATE.
    DbError::plan(SqlState::UndefinedTable, message)
}

fn undefined_sequence_id(id: SequenceId) -> DbError {
    DbError::plan(
        SqlState::UndefinedTable,
        format!("sequence id {id} not found"),
    )
}

fn build_index_schema(
    index_id: IndexId,
    storage_id: FileId,
    name: String,
    table: &TableSchema,
    columns: &[String],
    unique: bool,
    constraint: IndexConstraintKind,
) -> Result<IndexSchema> {
    if columns.is_empty() {
        return Err(DbError::plan(
            SqlState::SyntaxError,
            "index requires at least one column",
        ));
    }

    let mut column_ids = Vec::with_capacity(columns.len());
    let mut seen_names = HashSet::new();
    for column_name in columns {
        if !seen_names.insert(column_name.clone()) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!("duplicate index column {column_name}"),
            ));
        }

        let column = table
            .columns
            .iter()
            .find(|column| &column.name == column_name)
            .ok_or_else(|| {
                DbError::plan(
                    SqlState::UndefinedColumn,
                    format!("index column {column_name} does not exist"),
                )
            })?;
        column_ids.push(column.id);
    }

    let schema = IndexSchema {
        id: index_id,
        storage_id,
        table: table.id,
        name,
        columns: column_ids,
        unique,
        constraint,
    };
    validate_index_schema_for_table(&schema, table)?;
    Ok(schema)
}

fn drop_indexes_for_table(snapshot: &mut CatalogSnapshot, table: TableId) {
    let dropped: Vec<IndexId> = snapshot
        .indexes_by_id
        .iter()
        .filter(|(_, schema)| schema.table == table)
        .map(|(id, _)| *id)
        .collect();
    for id in dropped {
        if let Some(schema) = snapshot.indexes_by_id.remove(&id) {
            snapshot.indexes_by_name.remove(&schema.name);
        }
    }
}

fn reject_duplicate_index_name(snapshot: &CatalogSnapshot, name: &str) -> Result<()> {
    reject_duplicate_relation_name(snapshot, "index", name)
}

fn reject_duplicate_index_name_for_schema(
    snapshot: &CatalogSnapshot,
    schema: &IndexSchema,
) -> Result<()> {
    if snapshot.tables_by_name.contains_key(&schema.name)
        || snapshot.sequences_by_name.contains_key(&schema.name)
        || snapshot.indexes_by_name.contains_key(&schema.name)
        || (synthetic_primary_key_index_name_conflict_by_name(snapshot, &schema.name)
            && !is_matching_primary_key_constraint_index(snapshot, schema))
    {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("index {} already exists", schema.name),
        ));
    }
    Ok(())
}

fn is_matching_primary_key_constraint_index(
    snapshot: &CatalogSnapshot,
    schema: &IndexSchema,
) -> bool {
    if schema.constraint != IndexConstraintKind::PrimaryKey || !schema.unique {
        return false;
    }
    snapshot
        .tables_by_id
        .get(&schema.table)
        .is_some_and(|table| {
            table.relation_kind == RelationKind::User
                && !table.primary_key.is_empty()
                && schema.columns == table.primary_key
                && schema.name == synthetic_primary_key_index_name(table)
        })
}

fn reject_duplicate_relation_name(
    snapshot: &CatalogSnapshot,
    kind: &str,
    name: &str,
) -> Result<()> {
    if snapshot.tables_by_name.contains_key(name)
        || snapshot.views_by_name.contains_key(name)
        || snapshot.sequences_by_name.contains_key(name)
        || snapshot.indexes_by_name.contains_key(name)
        || synthetic_primary_key_index_name_conflict_by_name(snapshot, name)
    {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("{kind} {name} already exists"),
        ));
    }
    Ok(())
}

fn reject_index_name_matching_synthetic_primary_key(
    snapshot: &CatalogSnapshot,
    table: &TableSchema,
) -> Result<()> {
    if table.relation_kind == RelationKind::User && !table.primary_key.is_empty() {
        reject_duplicate_relation_name(
            snapshot,
            "index",
            &synthetic_primary_key_index_name(table),
        )?;
    }
    Ok(())
}

fn synthetic_primary_key_index_name_conflict_by_name(
    snapshot: &CatalogSnapshot,
    name: &str,
) -> bool {
    snapshot.tables_by_id.values().any(|table| {
        matches!(table.relation_kind, RelationKind::User)
            && !table.primary_key.is_empty()
            && synthetic_primary_key_index_name(table) == name
    })
}

fn validate_no_index_name_matching_synthetic_primary_key(
    snapshot: &CatalogSnapshot,
    table: &TableSchema,
) -> Result<()> {
    if let Some(name) = synthetic_primary_key_index_name_conflict(snapshot, table) {
        return Err(DbError::internal(format!(
            "catalog snapshot table {} synthetic primary-key index name {name} conflicts with a secondary index",
            table.name
        )));
    }
    Ok(())
}

fn synthetic_primary_key_index_name_conflict(
    snapshot: &CatalogSnapshot,
    table: &TableSchema,
) -> Option<String> {
    if table.relation_kind != RelationKind::User || table.primary_key.is_empty() {
        return None;
    }
    let name = synthetic_primary_key_index_name(table);
    let index = snapshot
        .indexes_by_name
        .get(&name)
        .and_then(|id| snapshot.indexes_by_id.get(id))?;
    (!is_matching_primary_key_constraint_index(snapshot, index)).then_some(name)
}

fn synthetic_primary_key_index_name(table: &TableSchema) -> String {
    match table.relation_kind {
        RelationKind::User => format!("{}_pkey", table.name),
        RelationKind::Toast { base_table } => format!("pg_toast_{base_table}_pkey"),
    }
}

fn reject_duplicate_index_id(snapshot: &CatalogSnapshot, id: IndexId) -> Result<()> {
    if snapshot.indexes_by_id.contains_key(&id) {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("index id {id} already exists"),
        ));
    }
    Ok(())
}

fn reject_duplicate_primary_key_constraint_index(
    snapshot: &CatalogSnapshot,
    schema: &IndexSchema,
) -> Result<()> {
    if schema.constraint != IndexConstraintKind::PrimaryKey {
        return Ok(());
    }
    if let Some(existing) = snapshot.indexes_by_id.values().find(|index| {
        index.id != schema.id
            && index.table == schema.table
            && index.constraint == IndexConstraintKind::PrimaryKey
    }) {
        let table = snapshot
            .tables_by_id
            .get(&schema.table)
            .map(|table| table.name.as_str())
            .unwrap_or("<missing>");
        return Err(DbError::internal(format!(
            "catalog table {table} already has primary-key constraint index {}",
            existing.name
        )));
    }
    Ok(())
}
