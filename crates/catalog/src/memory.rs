use std::collections::{HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use common::{
    ColumnDef, ColumnId, DbError, ParsedColumnDef, Result, SqlState, TableId, TableSchema,
};

use crate::CatalogManager;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CatalogSnapshot {
    pub tables_by_name: HashMap<String, TableId>,
    pub tables_by_id: HashMap<TableId, TableSchema>,
    pub next_table_id: TableId,
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
        })
    }

    pub fn from_snapshot(snapshot: CatalogSnapshot) -> Self {
        Self {
            snapshot: RwLock::new(snapshot),
        }
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

    fn restore(&self, snapshot: CatalogSnapshot) -> Result<()> {
        *self.write_snapshot()? = snapshot;
        Ok(())
    }

    fn apply_create_table(&self, schema: TableSchema) -> Result<()> {
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
                SqlState::DatatypeMismatch,
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
        });
    }

    let mut primary_key_ids = Vec::with_capacity(primary_key.len());
    let mut seen_primary_key_names = HashSet::new();
    for primary_key_name in primary_key {
        if !seen_primary_key_names.insert(primary_key_name.clone()) {
            return Err(DbError::plan(
                SqlState::DatatypeMismatch,
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
        name,
        columns: assigned_columns,
        primary_key: primary_key_ids,
    })
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

fn undefined_table(message: String) -> DbError {
    DbError::plan(SqlState::UndefinedTable, message)
}
