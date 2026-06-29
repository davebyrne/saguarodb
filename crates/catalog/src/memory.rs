use std::collections::{HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use common::{
    ColumnDef, ColumnDefault, ColumnId, DbError, IndexId, IndexSchema, PRIMARY_KEY_INDEX_ID,
    ParsedColumnDef, ParsedDefault, Result, SequenceId, SequenceOptions, SequenceSchema, SqlState,
    TableId, TableSchema,
};

use crate::CatalogManager;

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
        }
    }
}

fn default_next_index_id() -> IndexId {
    PRIMARY_KEY_INDEX_ID + 1
}

fn default_next_sequence_id() -> SequenceId {
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

    pub fn try_from_snapshot(snapshot: CatalogSnapshot) -> Result<Self> {
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
        validate_snapshot(&snapshot)?;
        let mut current = self.write_snapshot()?;
        snapshot.next_table_id = snapshot.next_table_id.max(current.next_table_id);
        snapshot.next_index_id = snapshot.next_index_id.max(current.next_index_id);
        snapshot.next_sequence_id = snapshot.next_sequence_id.max(current.next_sequence_id);
        *current = snapshot;
        Ok(())
    }

    fn reserve_table_id(&self, id: TableId) -> Result<()> {
        let next_after_id = id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog table id overflow while reserving id"))?;
        let mut snapshot = self.write_snapshot()?;
        snapshot.next_table_id = snapshot.next_table_id.max(next_after_id);
        Ok(())
    }

    fn apply_create_table(&self, schema: TableSchema) -> Result<()> {
        validate_schema(&schema)?;
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_table_name(&snapshot, &schema.name)?;
        reject_duplicate_table_id(&snapshot, schema.id)?;

        let next_after_schema = schema.id.checked_add(1).ok_or_else(|| {
            DbError::internal("catalog table id overflow while applying create table")
        })?;

        snapshot
            .tables_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.next_table_id = snapshot.next_table_id.max(next_after_schema);
        snapshot.tables_by_id.insert(schema.id, schema);
        Ok(())
    }

    fn apply_drop_table(&self, id: TableId) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        let schema = snapshot
            .tables_by_id
            .remove(&id)
            .ok_or_else(|| undefined_table(format!("table id {id} does not exist")))?;
        snapshot.tables_by_name.remove(&schema.name);
        drop_indexes_for_table(&mut snapshot, id);
        Ok(())
    }

    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<TableSchema> {
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_table_name(&snapshot, &name)?;

        let table_id = snapshot.next_table_id;
        let next_table_id = table_id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog table id overflow"))?;
        let schema = build_schema(table_id, name, columns, primary_key)?;

        snapshot
            .tables_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.tables_by_id.insert(schema.id, schema.clone());
        snapshot.next_table_id = next_table_id;
        Ok(schema)
    }

    fn drop_table(&self, id: TableId) -> Result<()> {
        self.apply_drop_table(id)
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
        let next_after_id = id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog index id overflow while reserving id"))?;
        let mut snapshot = self.write_snapshot()?;
        snapshot.next_index_id = snapshot.next_index_id.max(next_after_id);
        Ok(())
    }

    fn apply_create_index(&self, schema: IndexSchema) -> Result<()> {
        let mut snapshot = self.write_snapshot()?;
        reject_duplicate_index_name(&snapshot, &schema.name)?;
        reject_duplicate_index_id(&snapshot, schema.id)?;

        let next_after_schema = schema.id.checked_add(1).ok_or_else(|| {
            DbError::internal("catalog index id overflow while applying create index")
        })?;

        snapshot
            .indexes_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.next_index_id = snapshot.next_index_id.max(next_after_schema);
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

        let schema = {
            let table_schema = snapshot
                .tables_by_name
                .get(table)
                .and_then(|id| snapshot.tables_by_id.get(id))
                .ok_or_else(|| undefined_table(format!("table {table} does not exist")))?;
            build_index_schema(index_id, name, table_schema, columns, unique)?
        };

        snapshot
            .indexes_by_name
            .insert(schema.name.clone(), schema.id);
        snapshot.indexes_by_id.insert(schema.id, schema.clone());
        snapshot.next_index_id = next_index_id;
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
        let next_after_id = id
            .checked_add(1)
            .ok_or_else(|| DbError::internal("catalog sequence id overflow while reserving id"))?;
        let mut snapshot = self.write_snapshot()?;
        snapshot.next_sequence_id = snapshot.next_sequence_id.max(next_after_id);
        Ok(())
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
        self.apply_drop_sequence(id)
    }
}

fn build_schema(
    table_id: TableId,
    name: String,
    columns: Vec<ParsedColumnDef>,
    primary_key: Vec<String>,
) -> Result<TableSchema> {
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
        assigned_columns.push(ColumnDef {
            id: column_id,
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
            max_length: column.max_length,
            default: convert_column_default(column.default)?,
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
        name,
        columns: assigned_columns,
        primary_key: primary_key_ids,
    })
}

fn convert_column_default(default: Option<ParsedDefault>) -> Result<Option<ColumnDefault>> {
    match default {
        Some(ParsedDefault::Const(value)) => Ok(Some(ColumnDefault::Const(value))),
        Some(ParsedDefault::Nextval(_)) => Err(DbError::plan(
            SqlState::FeatureNotSupported,
            "sequence defaults are not supported yet",
        )),
        None => Ok(None),
    }
}

fn build_sequence_schema(
    id: SequenceId,
    name: String,
    options: SequenceOptions,
    owned: bool,
) -> Result<SequenceSchema> {
    if options.increment == 0 {
        return Err(DbError::plan(
            SqlState::DatatypeMismatch,
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
            SqlState::DatatypeMismatch,
            "MINVALUE cannot be greater than MAXVALUE",
        ));
    }

    let start = options
        .start
        .unwrap_or(if descending { max_value } else { min_value });
    if start < min_value || start > max_value {
        return Err(DbError::plan(
            SqlState::DatatypeMismatch,
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

fn validate_snapshot(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut max_table_id = 0;

    for (name, id) in &snapshot.tables_by_name {
        let schema = snapshot.tables_by_id.get(id).ok_or_else(|| {
            DbError::internal(format!(
                "catalog snapshot name index {name} points to missing table id {id}",
            ))
        })?;
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
        if snapshot.tables_by_name.get(&schema.name) != Some(id) {
            return Err(DbError::internal(format!(
                "catalog snapshot table {} is missing from name index",
                schema.name
            )));
        }
        validate_schema(schema)?;
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
    validate_sequences(snapshot)?;

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

fn validate_schema(schema: &TableSchema) -> Result<()> {
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
        validate_column_default(&schema.name, column)?;
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

fn validate_column_default(table_name: &str, column: &ColumnDef) -> Result<()> {
    match &column.default {
        Some(ColumnDefault::Nextval(_)) => Err(DbError::internal(format!(
            "catalog snapshot table {table_name} column {} has unsupported sequence default",
            column.name
        ))),
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
