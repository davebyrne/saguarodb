use std::collections::BTreeMap;
use std::sync::Arc;

use common::{
    ColumnId, CompressionSetting, DbError, FileId, IndexConstraintKind, IndexId, IndexSchema,
    ParsedColumnDef, Result, SequenceId, SequenceOptions, SequenceSchema, TableId, TableSchema,
    TableStatistics, ToastOptions, TruncateCatalogUpdate, TruncateTablePlan, ViewColumn,
    ViewDependency, ViewSchema,
};

use crate::{CatalogManager, CatalogSnapshot, MemoryCatalog, TableColumnAlteration};

/// Read-only catalog view that overlays transaction-local TRUNCATE generations
/// on the live catalog. Unrelated objects continue to come from `base`, avoiding
/// a full catalog clone for every later statement in the transaction.
pub struct TruncateCatalogOverlay {
    base: Arc<dyn CatalogManager>,
    tables: BTreeMap<TableId, TableSchema>,
    indexes: BTreeMap<IndexId, IndexSchema>,
}

impl TruncateCatalogOverlay {
    pub fn new(
        base: Arc<dyn CatalogManager>,
        updates: impl IntoIterator<Item = TruncateCatalogUpdate>,
    ) -> Self {
        let mut tables = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        for update in updates {
            tables.insert(update.table.id, update.table);
            if let Some(toast) = update.toast_table {
                tables.insert(toast.id, toast);
            }
            for index in update.indexes {
                indexes.insert(index.id, index);
            }
        }
        Self {
            base,
            tables,
            indexes,
        }
    }

    fn read_only<T>() -> Result<T> {
        Err(DbError::internal(
            "transactional TRUNCATE catalog overlay is read-only",
        ))
    }

    fn materialized(&self) -> Result<MemoryCatalog> {
        MemoryCatalog::try_from_snapshot(self.snapshot()?)
    }
}

impl CatalogManager for TruncateCatalogOverlay {
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>> {
        let Some(base) = self.base.get_table_by_name(name)? else {
            return Ok(None);
        };
        Ok(self.tables.get(&base.id).cloned().or(Some(base)))
    }

    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>> {
        if let Some(table) = self.tables.get(&id) {
            return Ok(Some(table.clone()));
        }
        self.base.get_table(id)
    }

    fn list_tables(&self) -> Result<Vec<TableSchema>> {
        let mut tables = self.base.list_tables()?;
        for table in &mut tables {
            if let Some(replacement) = self.tables.get(&table.id) {
                *table = replacement.clone();
            }
        }
        Ok(tables)
    }

    fn get_view_by_name(&self, name: &str) -> Result<Option<ViewSchema>> {
        self.base.get_view_by_name(name)
    }

    fn get_view(&self, id: TableId) -> Result<Option<ViewSchema>> {
        self.base.get_view(id)
    }

    fn list_views(&self) -> Result<Vec<ViewSchema>> {
        self.base.list_views()
    }

    fn snapshot(&self) -> Result<CatalogSnapshot> {
        let mut snapshot = self.base.snapshot()?;
        for table in self.tables.values() {
            snapshot.tables_by_id.insert(table.id, table.clone());
        }
        for index in self.indexes.values() {
            snapshot.indexes_by_id.insert(index.id, index.clone());
        }
        Ok(snapshot)
    }

    fn restore(&self, _snapshot: CatalogSnapshot) -> Result<()> {
        Self::read_only()
    }

    fn reserve_table_id(&self, _id: TableId) -> Result<()> {
        Self::read_only()
    }

    fn apply_create_table(&self, _schema: TableSchema) -> Result<()> {
        Self::read_only()
    }

    fn apply_update_table_schema(&self, _schema: TableSchema) -> Result<()> {
        Self::read_only()
    }

    fn apply_update_table_and_index_schemas(
        &self,
        _schema: TableSchema,
        _indexes: &[IndexSchema],
    ) -> Result<()> {
        Self::read_only()
    }

    fn apply_drop_table(&self, _id: TableId) -> Result<()> {
        Self::read_only()
    }

    fn create_table_with_options(
        &self,
        _name: String,
        _columns: Vec<ParsedColumnDef>,
        _primary_key: Vec<String>,
        _compression: CompressionSetting,
        _toast: ToastOptions,
        _checks: Vec<String>,
    ) -> Result<TableSchema> {
        Self::read_only()
    }

    fn drop_table(&self, _id: TableId) -> Result<()> {
        Self::read_only()
    }

    fn rename_table(&self, _id: TableId, _new_name: String) -> Result<TableSchema> {
        Self::read_only()
    }

    fn preflight_add_table_column(
        &self,
        _id: TableId,
        _if_not_exists: bool,
        _column: &ParsedColumnDef,
    ) -> Result<TableColumnAlteration> {
        Self::read_only()
    }

    fn add_table_column(&self, _id: TableId, _column: ParsedColumnDef) -> Result<TableSchema> {
        Self::read_only()
    }

    fn preflight_drop_table_column(
        &self,
        _id: TableId,
        _if_exists: bool,
        _column: &str,
    ) -> Result<TableColumnAlteration> {
        Self::read_only()
    }

    fn drop_table_column(&self, _id: TableId, _column: &str) -> Result<TableSchema> {
        Self::read_only()
    }

    fn rename_table_column(
        &self,
        _id: TableId,
        _old_name: &str,
        _new_name: String,
    ) -> Result<TableSchema> {
        Self::read_only()
    }

    fn set_table_compression(
        &self,
        _table: TableId,
        _compression: CompressionSetting,
        _active_dict_id: Option<u32>,
    ) -> Result<TableSchema> {
        Self::read_only()
    }

    fn set_table_toast_metadata(
        &self,
        _table: TableId,
        _toast: ToastOptions,
        _toast_table_id: Option<TableId>,
    ) -> Result<TableSchema> {
        Self::read_only()
    }

    fn set_table_primary_key(
        &self,
        _table: TableId,
        _primary_key: Vec<ColumnId>,
    ) -> Result<TableSchema> {
        Self::read_only()
    }

    fn add_table_primary_key_index(
        &self,
        _table: TableId,
        _primary_key: Vec<ColumnId>,
        _index: IndexSchema,
    ) -> Result<TableSchema> {
        Self::read_only()
    }

    fn drop_table_primary_key_index(
        &self,
        _table: TableId,
        _index: IndexId,
    ) -> Result<TableSchema> {
        Self::read_only()
    }

    // Transactional TRUNCATE leaves statistics untouched (they go stale until
    // the next ANALYZE), so the overlay reads them straight from the base.
    fn get_table_statistics(&self, table: TableId) -> Result<Option<TableStatistics>> {
        self.base.get_table_statistics(table)
    }

    fn set_table_statistics(&self, _table: TableId, _statistics: TableStatistics) -> Result<()> {
        Self::read_only()
    }

    fn allocate_dictionary_id(&self) -> Result<u32> {
        Self::read_only()
    }

    fn reserve_dictionary_id(&self, _id: u32) -> Result<()> {
        Self::read_only()
    }

    fn allocate_storage_id(&self) -> Result<FileId> {
        Self::read_only()
    }

    fn reserve_storage_id(&self, _id: FileId) -> Result<()> {
        Self::read_only()
    }

    fn prepare_truncate_table(&self, _table: TableId) -> Result<TruncateTablePlan> {
        Self::read_only()
    }

    fn build_truncate_table_update(
        &self,
        plan: &TruncateTablePlan,
    ) -> Result<TruncateCatalogUpdate> {
        self.materialized()?.build_truncate_table_update(plan)
    }

    fn apply_truncate_table(&self, _plan: &TruncateTablePlan) -> Result<TruncateCatalogUpdate> {
        Self::read_only()
    }

    fn apply_truncate_tables(
        &self,
        _plans: &[TruncateTablePlan],
    ) -> Result<Vec<TruncateCatalogUpdate>> {
        Self::read_only()
    }

    fn apply_truncate_updates(&self, _updates: &[TruncateCatalogUpdate]) -> Result<()> {
        Self::read_only()
    }

    fn get_index_by_name(&self, name: &str) -> Result<Option<IndexSchema>> {
        let Some(base) = self.base.get_index_by_name(name)? else {
            return Ok(None);
        };
        Ok(self.indexes.get(&base.id).cloned().or(Some(base)))
    }

    fn get_index(&self, id: IndexId) -> Result<Option<IndexSchema>> {
        if let Some(index) = self.indexes.get(&id) {
            return Ok(Some(index.clone()));
        }
        self.base.get_index(id)
    }

    fn list_indexes_for_table(&self, table: TableId) -> Result<Vec<IndexSchema>> {
        let mut indexes = self.base.list_indexes_for_table(table)?;
        for index in &mut indexes {
            if let Some(replacement) = self.indexes.get(&index.id) {
                *index = replacement.clone();
            }
        }
        Ok(indexes)
    }

    fn reserve_index_id(&self, _id: IndexId) -> Result<()> {
        Self::read_only()
    }

    fn apply_create_index(&self, _schema: IndexSchema) -> Result<()> {
        Self::read_only()
    }

    fn apply_update_index_schema(&self, _schema: IndexSchema) -> Result<()> {
        Self::read_only()
    }

    fn apply_drop_index(&self, _id: IndexId) -> Result<()> {
        Self::read_only()
    }

    fn create_index_with_constraint(
        &self,
        _name: String,
        _table: &str,
        _columns: &[String],
        _unique: bool,
        _constraint: IndexConstraintKind,
    ) -> Result<IndexSchema> {
        Self::read_only()
    }

    fn drop_index(&self, _id: IndexId) -> Result<()> {
        Self::read_only()
    }

    fn get_sequence_by_name(&self, name: &str) -> Result<Option<SequenceSchema>> {
        self.base.get_sequence_by_name(name)
    }

    fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceSchema>> {
        self.base.get_sequence(id)
    }

    fn list_sequences(&self) -> Result<Vec<SequenceSchema>> {
        self.base.list_sequences()
    }

    fn reserve_sequence_id(&self, _id: SequenceId) -> Result<()> {
        Self::read_only()
    }

    fn apply_create_sequence(&self, _schema: SequenceSchema) -> Result<()> {
        Self::read_only()
    }

    fn apply_drop_sequence(&self, _id: SequenceId) -> Result<()> {
        Self::read_only()
    }

    fn create_sequence(
        &self,
        _name: String,
        _options: SequenceOptions,
        _owned: bool,
    ) -> Result<SequenceSchema> {
        Self::read_only()
    }

    fn drop_sequence(&self, _id: SequenceId) -> Result<()> {
        Self::read_only()
    }

    fn apply_create_view(&self, _schema: ViewSchema) -> Result<()> {
        Self::read_only()
    }

    fn apply_replace_view(&self, _schema: ViewSchema) -> Result<()> {
        Self::read_only()
    }

    fn apply_drop_view(&self, _id: TableId) -> Result<()> {
        Self::read_only()
    }

    fn create_view(
        &self,
        _name: String,
        _columns: Vec<ViewColumn>,
        _definition: String,
        _dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema> {
        Self::read_only()
    }

    fn replace_view(
        &self,
        _id: TableId,
        _columns: Vec<ViewColumn>,
        _definition: String,
        _dependencies: Vec<ViewDependency>,
    ) -> Result<ViewSchema> {
        Self::read_only()
    }

    fn drop_view(&self, _id: TableId) -> Result<()> {
        Self::read_only()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{CompressionSetting, DataType, ParsedColumnDef};

    use super::*;

    #[test]
    fn overlays_only_truncated_generations_and_keeps_live_fallback() {
        let base = Arc::new(MemoryCatalog::empty());
        let table = base
            .create_table(
                "target".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                }],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let plan = base.prepare_truncate_table(table.id).unwrap();
        let update = base.build_truncate_table_update(&plan).unwrap();
        let overlay = TruncateCatalogOverlay::new(base.clone(), [update.clone()]);

        assert_eq!(
            overlay.get_table(table.id).unwrap().unwrap().storage_id,
            update.table.storage_id
        );

        let later = base
            .create_table(
                "later".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                }],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        assert_eq!(overlay.get_table_by_name("later").unwrap(), Some(later));
        assert!(
            overlay.prepare_truncate_table(table.id).is_err(),
            "the read-only view must not allocate ids on a disposable snapshot"
        );
    }
}
