mod memory;
mod serialize;

pub use memory::{CatalogSnapshot, MemoryCatalog};
pub use serialize::{deserialize_catalog, serialize_catalog};

use common::{ParsedColumnDef, Result, TableId, TableSchema};

pub trait CatalogManager: Send + Sync {
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>>;
    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>>;
    fn list_tables(&self) -> Result<Vec<TableSchema>>;
    fn snapshot(&self) -> Result<CatalogSnapshot>;
    fn restore(&self, snapshot: CatalogSnapshot) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, id: TableId) -> Result<()>;
    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<TableSchema>;
    fn drop_table(&self, id: TableId) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use common::{ColumnDef, DataType, ErrorKind, ParsedColumnDef, SqlState, TableSchema};

    use crate::{CatalogManager, MemoryCatalog, deserialize_catalog, serialize_catalog};

    fn id_column(nullable: bool) -> ParsedColumnDef {
        ParsedColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable,
        }
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
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                    },
                ],
                vec!["id".to_string()],
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
                vec!["id".to_string()],
            )
            .unwrap();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::DuplicateTable);
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
                    },
                ],
                vec!["id".to_string()],
            )
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn primary_key_on_missing_column_is_rejected() {
        let catalog = MemoryCatalog::empty();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["missing".to_string()],
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
                vec!["id".to_string()],
            )
            .unwrap();

        let snapshot = catalog.snapshot().unwrap();
        let bytes = serialize_catalog(&snapshot).unwrap();
        let restored = MemoryCatalog::from_snapshot(deserialize_catalog(&bytes).unwrap());

        restored
            .create_table(
                "accounts".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
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
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            }],
            primary_key: vec![0],
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
            )
            .unwrap();
        assert_eq!(next.id, 8);
    }
}
