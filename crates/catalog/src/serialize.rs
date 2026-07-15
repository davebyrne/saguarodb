use common::{
    DbError, FileId, IndexId, IndexSchema, NamespaceSchema, RelationKind, Result, SchemaId,
    SequenceId, SequenceSchema, TableId, TableSchema, TableStatistics, ViewSchema,
};
use serde::{Deserialize, Serialize};

use crate::CatalogSnapshot;

const CATALOG_FORMAT_VERSION: u32 = 3;
const MAX_CATALOG_BYTES: usize = 64 * 1024 * 1024;

#[derive(Serialize)]
struct CatalogV3<'a> {
    version: u32,
    schemas: Vec<&'a NamespaceSchema>,
    tables: Vec<&'a TableSchema>,
    views: Vec<&'a ViewSchema>,
    indexes: Vec<&'a IndexSchema>,
    sequences: Vec<&'a SequenceSchema>,
    statistics: Vec<(TableId, &'a TableStatistics)>,
    next_schema_id: SchemaId,
    next_table_id: TableId,
    next_index_id: IndexId,
    next_sequence_id: SequenceId,
    next_dictionary_id: u32,
    next_storage_id: FileId,
}

#[derive(Deserialize)]
struct OwnedCatalogV3 {
    #[serde(rename = "version")]
    _version: u32,
    schemas: Vec<NamespaceSchema>,
    tables: Vec<TableSchema>,
    views: Vec<ViewSchema>,
    indexes: Vec<IndexSchema>,
    sequences: Vec<SequenceSchema>,
    #[serde(default)]
    statistics: Vec<(TableId, TableStatistics)>,
    next_schema_id: SchemaId,
    next_table_id: TableId,
    next_index_id: IndexId,
    next_sequence_id: SequenceId,
    next_dictionary_id: u32,
    next_storage_id: FileId,
}

#[derive(Deserialize)]
struct CatalogHeader {
    version: Option<u32>,
}

pub fn serialize_catalog(snapshot: &CatalogSnapshot) -> Result<Vec<u8>> {
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
        statistics,
        next_schema_id: snapshot.next_schema_id,
        next_table_id: snapshot.next_table_id,
        next_index_id: snapshot.next_index_id,
        next_sequence_id: snapshot.next_sequence_id,
        next_dictionary_id: snapshot.next_dictionary_id,
        next_storage_id: snapshot.next_storage_id,
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
    let catalog: OwnedCatalogV3 = serde_json::from_slice(bytes)
        .map_err(|err| DbError::internal(format!("failed to deserialize catalog: {err}")))?;

    let schemas_by_name = catalog
        .schemas
        .iter()
        .map(|schema| (schema.name.clone(), schema.id))
        .collect();
    let schemas_by_id = collect_unique(catalog.schemas, |schema| schema.id, "schema")?;
    let tables_by_name = catalog
        .tables
        .iter()
        .filter(|table| table.relation_kind == RelationKind::User)
        .filter(|table| table.schema_id == common::PUBLIC_SCHEMA_ID)
        .map(|table| (table.name.clone(), table.id))
        .collect();
    let tables_by_id = collect_unique(catalog.tables, |table| table.id, "table")?;
    let views_by_name = catalog
        .views
        .iter()
        .filter(|view| view.schema_id == common::PUBLIC_SCHEMA_ID)
        .map(|view| (view.name.clone(), view.id))
        .collect();
    let views_by_id = collect_unique(catalog.views, |view| view.id, "view")?;
    let indexes_by_name = catalog
        .indexes
        .iter()
        .filter(|index| index.schema_id == common::PUBLIC_SCHEMA_ID)
        .map(|index| (index.name.clone(), index.id))
        .collect();
    let indexes_by_id = collect_unique(catalog.indexes, |index| index.id, "index")?;
    let sequences_by_name = catalog
        .sequences
        .iter()
        .filter(|sequence| sequence.schema_id == common::PUBLIC_SCHEMA_ID)
        .map(|sequence| (sequence.name.clone(), sequence.id))
        .collect();
    let sequences_by_id = collect_unique(catalog.sequences, |sequence| sequence.id, "sequence")?;
    let statistics = collect_unique(catalog.statistics, |(table, _)| *table, "statistics")?
        .into_iter()
        .map(|(table, (_, statistics))| (table, statistics))
        .collect();

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
        statistics,
    })
}

fn collect_unique<T, K>(values: Vec<T>, key: impl Fn(&T) -> K, kind: &str) -> Result<HashMap<K, T>>
where
    K: Copy + Eq + Hash + Display,
{
    let mut result = HashMap::with_capacity(values.len());
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
use std::{collections::HashMap, fmt::Display, hash::Hash};
