use std::collections::BTreeMap;

use common::{
    CatalogAllocatorHighWater, CatalogChangeSet, CatalogObject, CatalogObjectId, DbError, Result,
    SqlState,
};

use crate::dependencies::reconcile_constraints_and_dependencies;
use crate::{CatalogSnapshot, MemoryCatalog};

pub fn catalog_change_set_between(
    before: &CatalogSnapshot,
    after: &CatalogSnapshot,
) -> CatalogChangeSet {
    CatalogChangeSet::between(
        &snapshot_objects(before),
        &snapshot_objects(after),
        allocator_high_water_between(before, after),
    )
}

pub fn apply_catalog_change_set(
    snapshot: &CatalogSnapshot,
    change_set: &CatalogChangeSet,
) -> Result<CatalogSnapshot> {
    change_set
        .validate_shape()
        .map_err(|message| DbError::internal(format!("invalid catalog change set: {message}")))?;
    let existing_high_water = snapshot_allocator_high_water(snapshot);
    let mut result = snapshot.clone();
    for mutation in &change_set.mutations {
        let id = mutation
            .id()
            .ok_or_else(|| DbError::internal("catalog mutation has no object identity"))?;
        let current = object(&result, id);
        if current.as_ref() != mutation.before.as_ref() {
            return Err(DbError::plan(
                SqlState::SerializationFailure,
                format!("catalog object {id:?} changed concurrently"),
            ));
        }
        apply_object(
            &mut result,
            mutation.before.as_ref(),
            mutation.after.as_ref(),
        )?;
    }
    reserve_change_allocators(&mut result, &change_set.allocator_high_water);
    reserve_change_allocators(&mut result, &existing_high_water);
    rebuild_name_indexes(&mut result);
    reconcile_constraints_and_dependencies(&mut result)?;
    MemoryCatalog::try_from_snapshot(result.clone())?;
    Ok(result)
}

pub fn reserve_change_allocators(
    snapshot: &mut CatalogSnapshot,
    high_water: &CatalogAllocatorHighWater,
) {
    snapshot.next_schema_id = snapshot.next_schema_id.max(high_water.next_schema_id);
    snapshot.next_table_id = snapshot.next_table_id.max(high_water.next_table_id);
    snapshot.next_index_id = snapshot.next_index_id.max(high_water.next_index_id);
    snapshot.next_sequence_id = snapshot.next_sequence_id.max(high_water.next_sequence_id);
    snapshot.next_dictionary_id = snapshot
        .next_dictionary_id
        .max(high_water.next_dictionary_id);
    snapshot.next_storage_id = snapshot.next_storage_id.max(high_water.next_storage_id);
    snapshot.next_constraint_id = snapshot
        .next_constraint_id
        .max(high_water.next_constraint_id);
    for (relation, next_column) in &high_water.next_column_object_ids {
        if let Some(table) = snapshot.tables_by_id.get_mut(relation) {
            table.next_column_object_id = table.next_column_object_id.max(*next_column);
        }
        if let Some(view) = snapshot.views_by_id.get_mut(relation) {
            view.next_column_object_id = view.next_column_object_id.max(*next_column);
        }
    }
}

pub fn merge_allocator_high_water(
    target: &mut CatalogAllocatorHighWater,
    source: &CatalogAllocatorHighWater,
) {
    target.next_schema_id = target.next_schema_id.max(source.next_schema_id);
    target.next_table_id = target.next_table_id.max(source.next_table_id);
    target.next_index_id = target.next_index_id.max(source.next_index_id);
    target.next_sequence_id = target.next_sequence_id.max(source.next_sequence_id);
    target.next_dictionary_id = target.next_dictionary_id.max(source.next_dictionary_id);
    target.next_storage_id = target.next_storage_id.max(source.next_storage_id);
    target.next_constraint_id = target.next_constraint_id.max(source.next_constraint_id);
    for (relation, next_column) in &source.next_column_object_ids {
        let reserved = target
            .next_column_object_ids
            .entry(*relation)
            .or_insert(*next_column);
        *reserved = (*reserved).max(*next_column);
    }
}

fn snapshot_objects(snapshot: &CatalogSnapshot) -> BTreeMap<CatalogObjectId, CatalogObject> {
    let mut objects = BTreeMap::new();
    objects.extend(snapshot.schemas_by_id.values().cloned().map(|schema| {
        (
            CatalogObjectId::Schema(schema.id),
            CatalogObject::Schema(schema),
        )
    }));
    objects.extend(snapshot.tables_by_id.values().cloned().map(|schema| {
        (
            CatalogObjectId::Table(schema.id),
            CatalogObject::Table(schema),
        )
    }));
    objects.extend(snapshot.views_by_id.values().cloned().map(|schema| {
        (
            CatalogObjectId::View(schema.id),
            CatalogObject::View(schema),
        )
    }));
    objects.extend(snapshot.indexes_by_id.values().cloned().map(|schema| {
        (
            CatalogObjectId::Index(schema.id),
            CatalogObject::Index(schema),
        )
    }));
    objects.extend(snapshot.sequences_by_id.values().cloned().map(|schema| {
        (
            CatalogObjectId::Sequence(schema.id),
            CatalogObject::Sequence(schema),
        )
    }));
    objects.extend(snapshot.constraints_by_id.values().cloned().map(|schema| {
        (
            CatalogObjectId::Constraint(schema.id),
            CatalogObject::Constraint(schema),
        )
    }));
    objects.extend(snapshot.statistics.iter().map(|(table, statistics)| {
        (
            CatalogObjectId::Statistics(*table),
            CatalogObject::Statistics {
                table: *table,
                statistics: statistics.clone(),
            },
        )
    }));
    objects
}

fn snapshot_allocator_high_water(snapshot: &CatalogSnapshot) -> CatalogAllocatorHighWater {
    let next_column_object_ids = snapshot
        .tables_by_id
        .iter()
        .map(|(id, table)| (*id, table.next_column_object_id))
        .chain(
            snapshot
                .views_by_id
                .iter()
                .map(|(id, view)| (*id, view.next_column_object_id)),
        )
        .collect();
    CatalogAllocatorHighWater {
        next_schema_id: snapshot.next_schema_id,
        next_table_id: snapshot.next_table_id,
        next_index_id: snapshot.next_index_id,
        next_sequence_id: snapshot.next_sequence_id,
        next_dictionary_id: snapshot.next_dictionary_id,
        next_storage_id: snapshot.next_storage_id,
        next_constraint_id: snapshot.next_constraint_id,
        next_column_object_ids,
    }
}

fn allocator_high_water_between(
    before: &CatalogSnapshot,
    after: &CatalogSnapshot,
) -> CatalogAllocatorHighWater {
    let next_column_object_ids = after
        .tables_by_id
        .iter()
        .filter(|(id, table)| {
            before
                .tables_by_id
                .get(id)
                .is_none_or(|old| old.next_column_object_id < table.next_column_object_id)
        })
        .map(|(id, table)| (*id, table.next_column_object_id))
        .chain(
            after
                .views_by_id
                .iter()
                .filter(|(id, view)| {
                    before
                        .views_by_id
                        .get(id)
                        .is_none_or(|old| old.next_column_object_id < view.next_column_object_id)
                })
                .map(|(id, view)| (*id, view.next_column_object_id)),
        )
        .collect();
    CatalogAllocatorHighWater {
        next_schema_id: after.next_schema_id,
        next_table_id: after.next_table_id,
        next_index_id: after.next_index_id,
        next_sequence_id: after.next_sequence_id,
        next_dictionary_id: after.next_dictionary_id,
        next_storage_id: after.next_storage_id,
        next_constraint_id: after.next_constraint_id,
        next_column_object_ids,
    }
}

fn object(snapshot: &CatalogSnapshot, id: CatalogObjectId) -> Option<CatalogObject> {
    match id {
        CatalogObjectId::Schema(id) => snapshot
            .schemas_by_id
            .get(&id)
            .cloned()
            .map(CatalogObject::Schema),
        CatalogObjectId::Table(id) => snapshot
            .tables_by_id
            .get(&id)
            .cloned()
            .map(CatalogObject::Table),
        CatalogObjectId::View(id) => snapshot
            .views_by_id
            .get(&id)
            .cloned()
            .map(CatalogObject::View),
        CatalogObjectId::Index(id) => snapshot
            .indexes_by_id
            .get(&id)
            .cloned()
            .map(CatalogObject::Index),
        CatalogObjectId::Sequence(id) => snapshot
            .sequences_by_id
            .get(&id)
            .cloned()
            .map(CatalogObject::Sequence),
        CatalogObjectId::Statistics(table) => snapshot
            .statistics
            .get(&table)
            .cloned()
            .map(|statistics| CatalogObject::Statistics { table, statistics }),
        CatalogObjectId::Constraint(id) => snapshot
            .constraints_by_id
            .get(&id)
            .cloned()
            .map(CatalogObject::Constraint),
        CatalogObjectId::Function(_)
        | CatalogObjectId::SystemRelation(_)
        | CatalogObjectId::Column { .. }
        | CatalogObjectId::ColumnDefault { .. } => None,
    }
}

fn apply_object(
    snapshot: &mut CatalogSnapshot,
    before: Option<&CatalogObject>,
    after: Option<&CatalogObject>,
) -> Result<()> {
    let object = after
        .or(before)
        .ok_or_else(|| DbError::internal("empty catalog mutation"))?;
    match object.id() {
        CatalogObjectId::Schema(id) => {
            replace(
                &mut snapshot.schemas_by_id,
                id,
                after,
                |object| match object {
                    CatalogObject::Schema(value) => Some(value.clone()),
                    _ => None,
                },
            )
        }
        CatalogObjectId::Table(id) => {
            replace(
                &mut snapshot.tables_by_id,
                id,
                after,
                |object| match object {
                    CatalogObject::Table(value) => Some(value.clone()),
                    _ => None,
                },
            )
        }
        CatalogObjectId::View(id) => {
            replace(
                &mut snapshot.views_by_id,
                id,
                after,
                |object| match object {
                    CatalogObject::View(value) => Some(value.clone()),
                    _ => None,
                },
            )
        }
        CatalogObjectId::Index(id) => {
            replace(
                &mut snapshot.indexes_by_id,
                id,
                after,
                |object| match object {
                    CatalogObject::Index(value) => Some(value.clone()),
                    _ => None,
                },
            )
        }
        CatalogObjectId::Sequence(id) => replace(
            &mut snapshot.sequences_by_id,
            id,
            after,
            |object| match object {
                CatalogObject::Sequence(value) => Some(value.clone()),
                _ => None,
            },
        ),
        CatalogObjectId::Statistics(id) => apply_statistics(snapshot, id, after),
        CatalogObjectId::Constraint(id) => replace(
            &mut snapshot.constraints_by_id,
            id,
            after,
            |object| match object {
                CatalogObject::Constraint(value) => Some(value.clone()),
                _ => None,
            },
        ),
        CatalogObjectId::Function(_)
        | CatalogObjectId::SystemRelation(_)
        | CatalogObjectId::Column { .. }
        | CatalogObjectId::ColumnDefault { .. } => Err(DbError::internal(
            "catalog change references a non-replaceable object",
        )),
    }
}

fn apply_statistics(
    snapshot: &mut CatalogSnapshot,
    table: common::TableId,
    after: Option<&CatalogObject>,
) -> Result<()> {
    let Some(after) = after else {
        snapshot.statistics.remove(&table);
        return Ok(());
    };
    let CatalogObject::Statistics { statistics, .. } = after else {
        return Err(DbError::internal(
            "catalog statistics object variant does not match its id",
        ));
    };
    let Some(schema) = snapshot.tables_by_id.get(&table) else {
        snapshot.statistics.remove(&table);
        return Ok(());
    };
    if schema.relation_kind != common::RelationKind::User {
        snapshot.statistics.remove(&table);
        return Ok(());
    }
    if let Some(column) = statistics.columns.keys().find(|column| {
        !schema
            .columns
            .iter()
            .any(|candidate| candidate.id == **column)
    }) {
        return Err(DbError::internal(format!(
            "statistics for table {table} reference unknown column id {column}"
        )));
    }
    snapshot.statistics.insert(table, statistics.clone());
    Ok(())
}

fn replace<K, V>(
    map: &mut std::collections::HashMap<K, V>,
    id: K,
    after: Option<&CatalogObject>,
    value: impl FnOnce(&CatalogObject) -> Option<V>,
) -> Result<()>
where
    K: Eq + std::hash::Hash,
{
    if let Some(after) = after {
        let value = value(after)
            .ok_or_else(|| DbError::internal("catalog object variant does not match its id"))?;
        map.insert(id, value);
    } else {
        map.remove(&id);
    }
    Ok(())
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
            table.schema_id == common::PUBLIC_SCHEMA_ID
                && table.relation_kind == common::RelationKind::User
        })
        .map(|table| (table.name.clone(), table.id))
        .collect();
    snapshot.views_by_name = snapshot
        .views_by_id
        .values()
        .filter(|view| view.schema_id == common::PUBLIC_SCHEMA_ID)
        .map(|view| (view.name.clone(), view.id))
        .collect();
    snapshot.indexes_by_name = snapshot
        .indexes_by_id
        .values()
        .filter(|index| index.schema_id == common::PUBLIC_SCHEMA_ID)
        .map(|index| (index.name.clone(), index.id))
        .collect();
    snapshot.sequences_by_name = snapshot
        .sequences_by_id
        .values()
        .filter(|sequence| sequence.schema_id == common::PUBLIC_SCHEMA_ID)
        .map(|sequence| (sequence.name.clone(), sequence.id))
        .collect();
}

#[cfg(test)]
mod tests {
    use common::{
        CompressionSetting, DataType, FIRST_USER_SCHEMA_ID, NamespaceSchema, ParsedColumnDef,
        SqlState,
    };

    use super::*;
    use crate::{CatalogManager, serialize_catalog};

    #[test]
    fn between_is_deterministic_and_applies_atomically() {
        let before = MemoryCatalog::empty().snapshot().unwrap();
        let staged = MemoryCatalog::try_from_snapshot(before.clone()).unwrap();
        staged
            .apply_create_schema(NamespaceSchema {
                id: FIRST_USER_SCHEMA_ID,
                name: "app".to_string(),
            })
            .unwrap();
        let after = staged.snapshot().unwrap();

        let first = catalog_change_set_between(&before, &after);
        let second = catalog_change_set_between(&before, &after);
        assert_eq!(first, second);
        assert_eq!(first.mutations.len(), 1);
        let applied = apply_catalog_change_set(&before, &first).unwrap();
        assert_eq!(
            serialize_catalog(&applied).unwrap(),
            serialize_catalog(&after).unwrap()
        );

        let error = apply_catalog_change_set(&after, &first).unwrap_err();
        assert_eq!(error.code, SqlState::SerializationFailure);
        assert_eq!(after.schemas_by_id.len(), 2);
    }

    #[test]
    fn allocator_only_change_burns_ids() {
        let before = MemoryCatalog::empty().snapshot().unwrap();
        let mut high_water = snapshot_allocator_high_water(&before);
        high_water.next_table_id = 100;
        high_water.next_constraint_id = 200;
        let change_set = CatalogChangeSet::between(&BTreeMap::new(), &BTreeMap::new(), high_water);

        let after = apply_catalog_change_set(&before, &change_set).unwrap();
        assert_eq!(after.next_table_id, 100);
        assert_eq!(after.next_constraint_id, 200);
    }

    #[test]
    fn change_set_carries_only_advanced_per_relation_allocators() {
        let catalog = MemoryCatalog::empty();
        let column = |name: &str| ParsedColumnDef {
            name: name.to_string(),
            data_type: DataType::Integer,
            nullable: true,
            max_length: None,
            default: None,
            pg_type: None,
        };
        let changed = catalog
            .create_table(
                "changed".to_string(),
                vec![column("id")],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let unchanged = catalog
            .create_table(
                "unchanged".to_string(),
                vec![column("id")],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let before = catalog.snapshot().unwrap();
        catalog
            .add_table_column(changed.id, column("added"))
            .unwrap();
        let after = catalog.snapshot().unwrap();

        let change_set = catalog_change_set_between(&before, &after);
        assert_eq!(
            change_set
                .allocator_high_water
                .next_column_object_ids
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![changed.id]
        );
        assert_eq!(unchanged.next_column_object_id, 2);
    }
}
