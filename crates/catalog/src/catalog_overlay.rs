use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

use common::{
    DbError, IndexId, IndexSchema, NamespaceSchema, PUBLIC_SCHEMA_ID, RelationKind, Result,
    SchemaId, SequenceId, SequenceSchema, TableId, TableSchema, ViewSchema,
};

use crate::{CatalogAllocatorState, CatalogManager, CatalogSnapshot, MemoryCatalog};

#[derive(Default)]
struct CatalogDelta {
    schemas: BTreeMap<SchemaId, Option<NamespaceSchema>>,
    tables: BTreeMap<TableId, Option<TableSchema>>,
    views: BTreeMap<TableId, Option<ViewSchema>>,
    indexes: BTreeMap<IndexId, Option<IndexSchema>>,
    sequences: BTreeMap<SequenceId, Option<SequenceSchema>>,
    next_schema_id: SchemaId,
    next_table_id: TableId,
    next_index_id: IndexId,
    next_sequence_id: SequenceId,
    next_dictionary_id: u32,
    next_storage_id: u32,
}

/// Writable transaction-local catalog state.
///
/// The overlay stores only objects changed by the transaction. Each read starts
/// from the current live catalog and reapplies those replacements/tombstones, so
/// unrelated catalog commits remain visible. Callers must hold the server's
/// catalog publication gate while publishing the resulting snapshot.
pub struct CatalogOverlay {
    base: Arc<dyn CatalogManager>,
    delta: RwLock<CatalogDelta>,
}

impl CatalogOverlay {
    pub fn new(base: Arc<dyn CatalogManager>) -> Self {
        Self {
            base,
            delta: RwLock::new(CatalogDelta::default()),
        }
    }

    pub fn snapshot(&self) -> Result<CatalogSnapshot> {
        let delta = self
            .delta
            .read()
            .map_err(|_| DbError::internal("catalog overlay read lock poisoned"))?;
        materialize(self.base.snapshot()?, &delta)
    }

    pub fn catalog(&self) -> Result<MemoryCatalog> {
        MemoryCatalog::try_from_snapshot(self.snapshot()?)
    }

    pub fn apply<T>(&self, mutation: impl FnOnce(&MemoryCatalog) -> Result<T>) -> Result<T> {
        let mut delta = self
            .delta
            .write()
            .map_err(|_| DbError::internal("catalog overlay write lock poisoned"))?;
        let before = materialize(self.base.snapshot()?, &delta)?;
        let expected = CatalogAllocatorState::from_snapshot(&before);
        let catalog = MemoryCatalog::try_from_snapshot(before.clone())?;
        let result = mutation(&catalog)?;
        let after = catalog.snapshot()?;
        let desired = CatalogAllocatorState::from_snapshot(&after);
        if !self.base.claim_allocators(expected, desired)? {
            return Err(DbError::plan(
                common::SqlState::SerializationFailure,
                "catalog allocators changed concurrently; retry the statement",
            ));
        }
        record_changes(&mut delta, &before, &after);
        Ok(result)
    }

    pub fn publish(&self) -> Result<()> {
        self.base.restore(self.snapshot()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        let delta = self
            .delta
            .read()
            .map_err(|_| DbError::internal("catalog overlay read lock poisoned"))?;
        Ok(delta.schemas.is_empty()
            && delta.tables.is_empty()
            && delta.views.is_empty()
            && delta.indexes.is_empty()
            && delta.sequences.is_empty())
    }
}

fn materialize(mut snapshot: CatalogSnapshot, delta: &CatalogDelta) -> Result<CatalogSnapshot> {
    apply_objects(&mut snapshot.schemas_by_id, &delta.schemas);
    apply_objects(&mut snapshot.tables_by_id, &delta.tables);
    apply_objects(&mut snapshot.views_by_id, &delta.views);
    apply_objects(&mut snapshot.indexes_by_id, &delta.indexes);
    apply_objects(&mut snapshot.sequences_by_id, &delta.sequences);

    snapshot.next_schema_id = snapshot.next_schema_id.max(delta.next_schema_id);
    snapshot.next_table_id = snapshot.next_table_id.max(delta.next_table_id);
    snapshot.next_index_id = snapshot.next_index_id.max(delta.next_index_id);
    snapshot.next_sequence_id = snapshot.next_sequence_id.max(delta.next_sequence_id);
    snapshot.next_dictionary_id = snapshot.next_dictionary_id.max(delta.next_dictionary_id);
    snapshot.next_storage_id = snapshot.next_storage_id.max(delta.next_storage_id);
    rebuild_name_indexes(&mut snapshot);
    MemoryCatalog::try_from_snapshot(snapshot.clone())?;
    Ok(snapshot)
}

fn apply_objects<K, V>(target: &mut HashMap<K, V>, changes: &BTreeMap<K, Option<V>>)
where
    K: Copy + Eq + std::hash::Hash + Ord,
    V: Clone,
{
    for (id, value) in changes {
        match value {
            Some(value) => {
                target.insert(*id, value.clone());
            }
            None => {
                target.remove(id);
            }
        }
    }
}

fn rebuild_name_indexes(snapshot: &mut CatalogSnapshot) {
    snapshot.schemas_by_name = snapshot
        .schemas_by_id
        .values()
        .map(|schema| (schema.name.clone(), schema.id))
        .collect();
    snapshot.tables_by_name = snapshot
        .tables_by_id
        .values()
        .filter(|table| {
            table.schema_id == PUBLIC_SCHEMA_ID && table.relation_kind == RelationKind::User
        })
        .map(|table| (table.name.clone(), table.id))
        .collect();
    snapshot.views_by_name = snapshot
        .views_by_id
        .values()
        .filter(|view| view.schema_id == PUBLIC_SCHEMA_ID)
        .map(|view| (view.name.clone(), view.id))
        .collect();
    snapshot.indexes_by_name = snapshot
        .indexes_by_id
        .values()
        .filter(|index| index.schema_id == PUBLIC_SCHEMA_ID)
        .map(|index| (index.name.clone(), index.id))
        .collect();
    snapshot.sequences_by_name = snapshot
        .sequences_by_id
        .values()
        .filter(|sequence| sequence.schema_id == PUBLIC_SCHEMA_ID)
        .map(|sequence| (sequence.name.clone(), sequence.id))
        .collect();
}

fn record_changes(delta: &mut CatalogDelta, before: &CatalogSnapshot, after: &CatalogSnapshot) {
    diff_objects(
        &mut delta.schemas,
        &before.schemas_by_id,
        &after.schemas_by_id,
    );
    diff_objects(&mut delta.tables, &before.tables_by_id, &after.tables_by_id);
    diff_objects(&mut delta.views, &before.views_by_id, &after.views_by_id);
    diff_objects(
        &mut delta.indexes,
        &before.indexes_by_id,
        &after.indexes_by_id,
    );
    diff_objects(
        &mut delta.sequences,
        &before.sequences_by_id,
        &after.sequences_by_id,
    );
    delta.next_schema_id = delta.next_schema_id.max(after.next_schema_id);
    delta.next_table_id = delta.next_table_id.max(after.next_table_id);
    delta.next_index_id = delta.next_index_id.max(after.next_index_id);
    delta.next_sequence_id = delta.next_sequence_id.max(after.next_sequence_id);
    delta.next_dictionary_id = delta.next_dictionary_id.max(after.next_dictionary_id);
    delta.next_storage_id = delta.next_storage_id.max(after.next_storage_id);
}

fn diff_objects<K, V>(
    delta: &mut BTreeMap<K, Option<V>>,
    before: &HashMap<K, V>,
    after: &HashMap<K, V>,
) where
    K: Copy + Eq + std::hash::Hash + Ord,
    V: Clone + PartialEq,
{
    for (id, value) in after {
        if before.get(id) != Some(value) {
            delta.insert(*id, Some(value.clone()));
        }
    }
    for id in before.keys() {
        if !after.contains_key(id) {
            delta.insert(*id, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use common::{CompressionSetting, DataType, ParsedColumnDef, ViewColumn};

    use super::*;

    fn column() -> ParsedColumnDef {
        ParsedColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            max_length: None,
            default: None,
            pg_type: None,
        }
    }

    #[test]
    fn local_changes_are_isolated_until_atomic_publish() {
        let base: Arc<dyn CatalogManager> = Arc::new(MemoryCatalog::empty());
        let overlay = CatalogOverlay::new(base.clone());
        overlay
            .apply(|catalog| {
                catalog.create_table(
                    "local".to_string(),
                    vec![column()],
                    Vec::new(),
                    CompressionSetting::None,
                )
            })
            .unwrap();

        assert!(base.get_table_by_name("local").unwrap().is_none());
        assert!(
            overlay
                .catalog()
                .unwrap()
                .get_table_by_name("local")
                .unwrap()
                .is_some()
        );
        overlay.publish().unwrap();
        assert!(base.get_table_by_name("local").unwrap().is_some());
    }

    #[test]
    fn unrelated_live_changes_remain_visible_beneath_local_delta() {
        let concrete = Arc::new(MemoryCatalog::empty());
        let base: Arc<dyn CatalogManager> = concrete.clone();
        let overlay = CatalogOverlay::new(base);
        overlay
            .apply(|catalog| {
                catalog.create_table(
                    "local".to_string(),
                    vec![column()],
                    Vec::new(),
                    CompressionSetting::None,
                )
            })
            .unwrap();
        concrete
            .create_table(
                "concurrent".to_string(),
                vec![column()],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();

        let visible = overlay.catalog().unwrap();
        assert!(visible.get_table_by_name("local").unwrap().is_some());
        assert!(visible.get_table_by_name("concurrent").unwrap().is_some());
    }

    #[test]
    fn local_tombstone_wins_over_live_base() {
        let concrete = Arc::new(MemoryCatalog::empty());
        let table = concrete
            .create_table(
                "gone".to_string(),
                vec![column()],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let base: Arc<dyn CatalogManager> = concrete;
        let overlay = CatalogOverlay::new(base);
        overlay
            .apply(|catalog| catalog.drop_table(table.id))
            .unwrap();
        assert!(
            overlay
                .catalog()
                .unwrap()
                .get_table(table.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn local_view_reserves_the_shared_relation_id() {
        let concrete = Arc::new(MemoryCatalog::empty());
        let base: Arc<dyn CatalogManager> = concrete.clone();
        let overlay = CatalogOverlay::new(base);
        let view = overlay
            .apply(|catalog| {
                catalog.create_view(
                    "local_view".to_string(),
                    vec![ViewColumn {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        pg_type: None,
                    }],
                    "select 1".to_string(),
                    Vec::new(),
                )
            })
            .unwrap();
        let table = concrete
            .create_table(
                "concurrent".to_string(),
                vec![column()],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();

        assert_ne!(view.id, table.id);
        assert!(overlay.catalog().is_ok());
    }

    #[test]
    fn competing_overlays_claim_distinct_ids_and_publish_without_replacement() {
        let base: Arc<dyn CatalogManager> = Arc::new(MemoryCatalog::empty());
        let first = Arc::new(CatalogOverlay::new(base.clone()));
        let second = Arc::new(CatalogOverlay::new(base.clone()));
        let barrier = Arc::new(Barrier::new(3));
        let first_task = {
            let first = first.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                first.apply(|catalog| {
                    catalog.create_table(
                        "first".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
            })
        };
        let second_task = {
            let second = second.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                second.apply(|catalog| {
                    catalog.create_table(
                        "second".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
            })
        };
        barrier.wait();
        let first_result = first_task.join().unwrap();
        let second_result = second_task.join().unwrap();
        let first_table = match first_result {
            Ok(table) => table,
            Err(error) if error.code == common::SqlState::SerializationFailure => first
                .apply(|catalog| {
                    catalog.create_table(
                        "first".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
                .unwrap(),
            Err(error) => panic!("unexpected first overlay error: {error}"),
        };
        let second_table = match second_result {
            Ok(table) => table,
            Err(error) if error.code == common::SqlState::SerializationFailure => second
                .apply(|catalog| {
                    catalog.create_table(
                        "second".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
                .unwrap(),
            Err(error) => panic!("unexpected second overlay error: {error}"),
        };
        assert_ne!(first_table.id, second_table.id);
        assert_ne!(first_table.storage_id, second_table.storage_id);

        first.publish().unwrap();
        second.publish().unwrap();
        assert!(base.get_table_by_name("first").unwrap().is_some());
        assert!(base.get_table_by_name("second").unwrap().is_some());
    }
}
