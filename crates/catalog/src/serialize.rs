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

use std::{
    collections::{BTreeSet, HashMap},
    fmt::Display,
    hash::Hash,
};

use common::{
    ConstraintSchema, DbError, DependencyEdge, FileId, IndexId, IndexSchema, NamespaceSchema,
    RelationKind, Result, SchemaId, SequenceId, SequenceSchema, TableId, TableSchema,
    TableStatistics, ViewSchema,
};
use serde::{Deserialize, Deserializer, Serialize};

use crate::CatalogSnapshot;

const CATALOG_FORMAT_VERSION: u32 = 3;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_OBJECTS_PER_KIND: usize = 65_536;
const MAX_CATALOG_DEPENDENCIES: usize = 1_000_000;

#[derive(Serialize)]
struct CatalogV3<'a> {
    version: u32,
    schemas: Vec<&'a NamespaceSchema>,
    tables: Vec<&'a TableSchema>,
    views: Vec<&'a ViewSchema>,
    indexes: Vec<&'a IndexSchema>,
    sequences: Vec<&'a SequenceSchema>,
    constraints: Vec<&'a ConstraintSchema>,
    dependencies: Vec<&'a DependencyEdge>,
    statistics: Vec<(TableId, &'a TableStatistics)>,
    next_schema_id: SchemaId,
    next_table_id: TableId,
    next_index_id: IndexId,
    next_sequence_id: SequenceId,
    next_dictionary_id: u32,
    next_storage_id: FileId,
    next_constraint_id: common::ConstraintId,
}

#[derive(Deserialize)]
struct OwnedCatalogV3 {
    #[serde(rename = "version")]
    _version: u32,
    #[serde(deserialize_with = "deserialize_bounded_objects")]
    schemas: Vec<NamespaceSchema>,
    #[serde(deserialize_with = "deserialize_bounded_objects")]
    tables: Vec<TableSchema>,
    #[serde(deserialize_with = "deserialize_bounded_objects")]
    views: Vec<ViewSchema>,
    #[serde(deserialize_with = "deserialize_bounded_objects")]
    indexes: Vec<IndexSchema>,
    #[serde(deserialize_with = "deserialize_bounded_objects")]
    sequences: Vec<SequenceSchema>,
    #[serde(deserialize_with = "deserialize_bounded_objects")]
    constraints: Vec<ConstraintSchema>,
    #[serde(deserialize_with = "deserialize_bounded_dependencies")]
    dependencies: Vec<DependencyEdge>,
    #[serde(deserialize_with = "deserialize_bounded_objects")]
    statistics: Vec<(TableId, TableStatistics)>,
    next_schema_id: SchemaId,
    next_table_id: TableId,
    next_index_id: IndexId,
    next_sequence_id: SequenceId,
    next_dictionary_id: u32,
    next_storage_id: FileId,
    next_constraint_id: common::ConstraintId,
}

#[derive(Deserialize)]
struct CatalogHeader {
    version: Option<u32>,
    next_constraint_id: Option<common::ConstraintId>,
}

pub fn serialize_catalog(snapshot: &CatalogSnapshot) -> Result<Vec<u8>> {
    ensure_collection_limit(
        snapshot.schemas_by_id.len(),
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog schema collection",
    )?;
    ensure_collection_limit(
        snapshot.tables_by_id.len(),
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog table collection",
    )?;
    ensure_collection_limit(
        snapshot.views_by_id.len(),
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog view collection",
    )?;
    ensure_collection_limit(
        snapshot.indexes_by_id.len(),
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog index collection",
    )?;
    ensure_collection_limit(
        snapshot.sequences_by_id.len(),
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog sequence collection",
    )?;
    ensure_collection_limit(
        snapshot.constraints_by_id.len(),
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog constraint collection",
    )?;
    ensure_collection_limit(
        snapshot.statistics.len(),
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog statistics collection",
    )?;
    ensure_collection_limit(
        snapshot.dependencies.len(),
        MAX_CATALOG_DEPENDENCIES,
        "catalog dependency collection",
    )?;

    let mut schemas: Vec<_> = snapshot.schemas_by_id.values().collect();
    schemas.sort_by_key(|schema| schema.id);
    let mut tables: Vec<_> = snapshot.tables_by_id.values().collect();
    tables.sort_by_key(|table| table.id);
    let mut views: Vec<_> = snapshot.views_by_id.values().collect();
    views.sort_by_key(|view| view.id);
    let mut indexes: Vec<_> = snapshot.indexes_by_id.values().collect();
    indexes.sort_by_key(|index| index.id);
    let mut sequences: Vec<_> = snapshot.sequences_by_id.values().collect();
    sequences.sort_by_key(|sequence| sequence.id);
    let mut constraints: Vec<_> = snapshot.constraints_by_id.values().collect();
    constraints.sort_by_key(|constraint| constraint.id);
    let dependencies: Vec<_> = snapshot.dependencies.iter().collect();
    let mut statistics: Vec<_> = snapshot
        .statistics
        .iter()
        .map(|(table, statistics)| (*table, statistics))
        .collect();
    statistics.sort_by_key(|(table, _)| *table);

    let bytes = serde_json::to_vec(&CatalogV3 {
        version: CATALOG_FORMAT_VERSION,
        schemas,
        tables,
        views,
        indexes,
        sequences,
        constraints,
        dependencies,
        statistics,
        next_schema_id: snapshot.next_schema_id,
        next_table_id: snapshot.next_table_id,
        next_index_id: snapshot.next_index_id,
        next_sequence_id: snapshot.next_sequence_id,
        next_dictionary_id: snapshot.next_dictionary_id,
        next_storage_id: snapshot.next_storage_id,
        next_constraint_id: snapshot.next_constraint_id,
    })
    .map_err(|err| DbError::internal(format!("failed to serialize catalog: {err}")))?;
    if bytes.len() > MAX_CATALOG_BYTES {
        return Err(DbError::plan(
            common::SqlState::ProgramLimitExceeded,
            "catalog snapshot exceeds the 64 MiB durable size limit",
        ));
    }
    Ok(bytes)
}

fn ensure_collection_limit(count: usize, limit: usize, description: &str) -> Result<()> {
    if count > limit {
        return Err(DbError::plan(
            common::SqlState::ProgramLimitExceeded,
            format!("{description} exceeds {limit} entries"),
        ));
    }
    Ok(())
}

pub fn deserialize_catalog(bytes: &[u8]) -> Result<CatalogSnapshot> {
    if bytes.len() > MAX_CATALOG_BYTES {
        return Err(DbError::internal("catalog snapshot exceeds size limit"));
    }
    let header: CatalogHeader = serde_json::from_slice(bytes)
        .map_err(|err| DbError::internal(format!("failed to deserialize catalog: {err}")))?;
    let Some(version) = header.version else {
        return Err(DbError::internal("unsupported unversioned catalog format"));
    };
    if version != CATALOG_FORMAT_VERSION {
        return Err(DbError::internal(format!(
            "unsupported catalog format version {}",
            version
        )));
    }
    if header.next_constraint_id.is_none() {
        return Err(DbError::internal(
            "unsupported pre-foundation catalog v3 layout",
        ));
    }
    let catalog: OwnedCatalogV3 = serde_json::from_slice(bytes)
        .map_err(|err| DbError::internal(format!("failed to deserialize catalog: {err}")))?;

    let schemas_by_name = collect_pairs(
        catalog
            .schemas
            .iter()
            .map(|schema| (schema.name.clone(), schema.id)),
        catalog.schemas.len(),
        "catalog schema name index",
    )?;
    let schemas_by_id = collect_unique(catalog.schemas, |schema| schema.id, "schema")?;
    let tables_by_name = collect_pairs(
        catalog
            .tables
            .iter()
            .filter(|table| table.relation_kind == RelationKind::User)
            .filter(|table| table.schema_id == common::PUBLIC_SCHEMA_ID)
            .map(|table| (table.name.clone(), table.id)),
        catalog.tables.len(),
        "catalog table name index",
    )?;
    let tables_by_id = collect_unique(catalog.tables, |table| table.id, "table")?;
    let views_by_name = collect_pairs(
        catalog
            .views
            .iter()
            .filter(|view| view.schema_id == common::PUBLIC_SCHEMA_ID)
            .map(|view| (view.name.clone(), view.id)),
        catalog.views.len(),
        "catalog view name index",
    )?;
    let views_by_id = collect_unique(catalog.views, |view| view.id, "view")?;
    let indexes_by_name = collect_pairs(
        catalog
            .indexes
            .iter()
            .filter(|index| index.schema_id == common::PUBLIC_SCHEMA_ID)
            .map(|index| (index.name.clone(), index.id)),
        catalog.indexes.len(),
        "catalog index name index",
    )?;
    let indexes_by_id = collect_unique(catalog.indexes, |index| index.id, "index")?;
    let sequences_by_name = collect_pairs(
        catalog
            .sequences
            .iter()
            .filter(|sequence| sequence.schema_id == common::PUBLIC_SCHEMA_ID)
            .map(|sequence| (sequence.name.clone(), sequence.id)),
        catalog.sequences.len(),
        "catalog sequence name index",
    )?;
    let sequences_by_id = collect_unique(catalog.sequences, |sequence| sequence.id, "sequence")?;
    let constraints_by_id = collect_unique(
        catalog.constraints,
        |constraint| constraint.id,
        "constraint",
    )?;
    let statistics_with_ids =
        collect_unique(catalog.statistics, |(table, _)| *table, "statistics")?;
    let mut statistics = HashMap::new();
    statistics
        .try_reserve(statistics_with_ids.len())
        .map_err(|_| DbError::internal("cannot allocate catalog statistics map"))?;
    for (table, (_, table_statistics)) in statistics_with_ids {
        statistics.insert(table, table_statistics);
    }

    let mut dependencies = BTreeSet::new();
    for dependency in catalog.dependencies {
        if !dependencies.insert(dependency) {
            return Err(DbError::internal(
                "catalog contains a duplicate dependency edge",
            ));
        }
    }

    Ok(CatalogSnapshot {
        schemas_by_name,
        schemas_by_id,
        next_schema_id: catalog.next_schema_id,
        tables_by_name,
        tables_by_id,
        next_table_id: catalog.next_table_id,
        views_by_name,
        views_by_id,
        indexes_by_name,
        indexes_by_id,
        next_index_id: catalog.next_index_id,
        sequences_by_name,
        sequences_by_id,
        next_sequence_id: catalog.next_sequence_id,
        next_dictionary_id: catalog.next_dictionary_id,
        next_storage_id: catalog.next_storage_id,
        next_constraint_id: catalog.next_constraint_id,
        constraints_by_id,
        dependencies,
        statistics,
    })
}

fn collect_unique<T, K>(values: Vec<T>, key: impl Fn(&T) -> K, kind: &str) -> Result<HashMap<K, T>>
where
    K: Copy + Eq + Hash + Display,
{
    let mut result = HashMap::new();
    result
        .try_reserve(values.len())
        .map_err(|_| DbError::internal(format!("cannot allocate catalog {kind} map")))?;
    for value in values {
        let id = key(&value);
        if result.insert(id, value).is_some() {
            return Err(DbError::internal(format!(
                "catalog contains duplicate {kind} id {id}"
            )));
        }
    }
    Ok(result)
}

fn collect_pairs<K, V>(
    pairs: impl IntoIterator<Item = (K, V)>,
    capacity: usize,
    description: &str,
) -> Result<HashMap<K, V>>
where
    K: Eq + Hash,
{
    let mut result = HashMap::new();
    result
        .try_reserve(capacity)
        .map_err(|_| DbError::internal(format!("cannot allocate {description}")))?;
    for (key, value) in pairs {
        result.insert(key, value);
    }
    Ok(result)
}

fn deserialize_bounded_objects<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    common::deserialize_bounded_vec_named(
        deserializer,
        MAX_CATALOG_OBJECTS_PER_KIND,
        "catalog object collection",
    )
}

fn deserialize_bounded_dependencies<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    common::deserialize_bounded_vec_named(
        deserializer,
        MAX_CATALOG_DEPENDENCIES,
        "catalog dependency collection",
    )
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{
        MAX_CATALOG_OBJECTS_PER_KIND, deserialize_bounded_objects, ensure_collection_limit,
    };

    #[derive(Debug, Deserialize)]
    struct BoundedValues {
        #[serde(deserialize_with = "deserialize_bounded_objects")]
        values: Vec<u8>,
    }

    #[test]
    fn bounded_catalog_collection_rejects_an_extra_item() {
        let json = serde_json::to_vec(&serde_json::json!({
            "values": vec![0_u8; MAX_CATALOG_OBJECTS_PER_KIND + 1]
        }))
        .unwrap();
        let error = serde_json::from_slice::<BoundedValues>(&json).unwrap_err();
        assert!(error.to_string().contains("exceeds 65536 items"));
    }

    #[test]
    fn bounded_catalog_collection_accepts_its_limit() {
        let json = serde_json::to_vec(&serde_json::json!({
            "values": vec![0_u8; MAX_CATALOG_OBJECTS_PER_KIND]
        }))
        .unwrap();
        let values: BoundedValues = serde_json::from_slice(&json).unwrap();
        assert_eq!(values.values.len(), MAX_CATALOG_OBJECTS_PER_KIND);
    }

    #[test]
    fn catalog_encoder_rejects_a_collection_above_the_decoder_limit() {
        ensure_collection_limit(
            MAX_CATALOG_OBJECTS_PER_KIND,
            MAX_CATALOG_OBJECTS_PER_KIND,
            "catalog schema collection",
        )
        .unwrap();
        let error = ensure_collection_limit(
            MAX_CATALOG_OBJECTS_PER_KIND + 1,
            MAX_CATALOG_OBJECTS_PER_KIND,
            "catalog schema collection",
        )
        .unwrap_err();
        assert_eq!(error.code, common::SqlState::ProgramLimitExceeded);
        assert!(error.message.contains("exceeds 65536 entries"));
    }
}
