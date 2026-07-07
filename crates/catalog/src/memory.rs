use std::collections::{HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use common::{
    ColumnDef, ColumnDefault, ColumnId, CompressionSetting, DataType, DbError, FileId, IndexId,
    IndexSchema, PRIMARY_KEY_INDEX_ID, ParsedColumnDef, ParsedDefault, RelationKind, Result,
    SequenceId, SequenceOptions, SequenceSchema, SqlState, TableId, TableSchema, ToastMode,
    ToastOptions, TruncateCatalogUpdate, TruncateTablePlan, needs_toast_relation, toast_schema,
};

use crate::CatalogManager;

const STORAGE_ID_KIND_BITS: FileId = 0xC000_0000;
const MAX_STORAGE_ID: FileId = !STORAGE_ID_KIND_BITS;
const STORAGE_ID_EXHAUSTED: FileId = MAX_STORAGE_ID + 1;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CatalogSnapshot {
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub next_table_id: TableId,
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
            reject_duplicate_table_name(&snapshot, &schema.name)?;
        }
        reject_duplicate_table_id(&snapshot, schema.id)?;
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
        reject_duplicate_table_name(&snapshot, &name)?;

        let table_id = snapshot.next_table_id;
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
        validate_toast_options(&schema)?;
        let hidden_toast = if needs_toast_relation(&schema) {
            let toast_id = next_table_id;
            next_table_id = toast_id
                .checked_add(1)
                .ok_or_else(|| DbError::internal("catalog table id overflow"))?;
            let toast_storage_id = next_storage_id;
            next_storage_id =
                next_storage_id_after(toast_storage_id, "catalog storage id overflow")?;
            schema.toast_table_id = Some(toast_id);
            let mut hidden_toast = toast_schema(&schema, toast_id);
            hidden_toast.storage_id = toast_storage_id;
            Some(hidden_toast)
        } else {
            None
        };

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
        reject_duplicate_index_name(&snapshot, &schema.name)?;
        reject_duplicate_index_id(&snapshot, schema.id)?;
        validate_storage_id("index", schema.storage_id)?;
        reject_duplicate_index_storage_id(&snapshot, schema.storage_id, "index storage id")?;

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

    fn apply_drop_index(&self, id: IndexId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        let schema = snapshot
            .indexes_by_id
            .remove(&id)
            .ok_or_else(|| undefined_index(format!("index id {id} does not exist")))?;
        snapshot.indexes_by_name.remove(&schema.name);
        Ok(())
    }

    fn create_index(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<IndexSchema> {
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_index_name(&snapshot, &name)?;

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
            build_index_schema(index_id, storage_id, name, table_schema, columns, unique)?
        };

        snapshot
            .indexes_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.indexes_by_id.insert(schema.id, schema.clone());
        snapshot.next_index_id = next_index_id;
        snapshot.next_storage_id = next_storage_id;
        Ok(schema)
    }

    fn drop_index(&self, id: IndexId) -> Result<()> {
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

    if primary_key_ids.is_empty() {
        return Err(DbError::plan(
            SqlState::DatatypeMismatch,
            "a table requires a primary key",
        ));
    }

    Ok(TableSchema {
        id: table_id,
        storage_id,
        name,
        columns: assigned_columns,
        primary_key: primary_key_ids,
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
        build_index_schema(0, 2, index_name, &schema, columns, true)?;
    }
    Ok(())
}

fn validate_snapshot(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut max_table_id = 0;
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
        max_table_id = max_table_id.max(*id);
    }

    let required_next = max_table_id
        .checked_add(1)
        .ok_or_else(|| DbError::internal("catalog snapshot table id overflow"))?;
    if snapshot.next_table_id < required_next {
        return Err(DbError::internal(format!(
            "catalog snapshot next_table_id {} is less than required {required_next}",
            snapshot.next_table_id
        )));
    }

    validate_indexes(snapshot)?;
    validate_dictionary_ids(snapshot)?;
    validate_storage_ids(snapshot)?;
    validate_toast_relations(snapshot)?;
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
        if *id == PRIMARY_KEY_INDEX_ID {
            return Err(DbError::internal(
                "catalog snapshot uses the reserved primary-key index id for a secondary index",
            ));
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

    let required_next = max_index_id
        .checked_add(1)
        .ok_or_else(|| DbError::internal("catalog snapshot index id overflow"))?;
    if snapshot.next_index_id < required_next {
        return Err(DbError::internal(format!(
            "catalog snapshot next_index_id {} is less than required {required_next}",
            snapshot.next_index_id
        )));
    }

    Ok(())
}

fn validate_index_schema(
    schema: &IndexSchema,
    tables_by_id: &HashMap<TableId, TableSchema>,
) -> Result<()> {
    let table = tables_by_id.get(&schema.table).ok_or_else(|| {
        DbError::internal(format!(
            "catalog snapshot index {} references missing table {}",
            schema.name, schema.table
        ))
    })?;

    if schema.columns.is_empty() {
        return Err(DbError::internal(format!(
            "catalog snapshot index {} has no columns",
            schema.name
        )));
    }

    let mut seen = HashSet::new();
    for column_id in &schema.columns {
        if !table.columns.iter().any(|column| column.id == *column_id) {
            return Err(DbError::internal(format!(
                "catalog snapshot index {} references missing column {} on table {}",
                schema.name, column_id, schema.table
            )));
        }
        if !seen.insert(*column_id) {
            return Err(DbError::internal(format!(
                "catalog snapshot index {} has duplicate column {}",
                schema.name, column_id
            )));
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
    validate_toast_options(schema)?;

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

    if schema.primary_key.is_empty() {
        return Err(DbError::internal(format!(
            "catalog snapshot table {} must have a primary key",
            schema.name
        )));
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

fn reject_duplicate_table_name(snapshot: &CatalogSnapshot, name: &str) -> Result<()> {
    if snapshot.tables_by_name.contains_key(name) {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("table {name} already exists"),
        ));
    }
    Ok(())
}

fn reject_duplicate_table_id(snapshot: &CatalogSnapshot, id: TableId) -> Result<()> {
    if snapshot.tables_by_id.contains_key(&id) {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("table id {id} already exists"),
        ));
    }
    Ok(())
}

fn reject_duplicate_sequence_name(snapshot: &CatalogSnapshot, name: &str) -> Result<()> {
    if snapshot.sequences_by_name.contains_key(name) {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("sequence {name} already exists"),
        ));
    }
    Ok(())
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

    Ok(IndexSchema {
        id: index_id,
        storage_id,
        table: table.id,
        name,
        columns: column_ids,
        unique,
    })
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
    if snapshot.indexes_by_name.contains_key(name) {
        return Err(DbError::plan(
            SqlState::DuplicateTable,
            format!("index {name} already exists"),
        ));
    }
    Ok(())
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
