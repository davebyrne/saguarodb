mod memory;
mod serialize;

pub use memory::{CatalogSnapshot, MemoryCatalog};
pub use serialize::{deserialize_catalog, serialize_catalog};

use common::{
    IndexId, IndexSchema, ParsedColumnDef, Result, SequenceId, SequenceOptions, SequenceSchema,
    TableId, TableSchema,
};

pub trait CatalogManager: Send + Sync {
    fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>>;
    fn get_table(&self, id: TableId) -> Result<Option<TableSchema>>;
    fn list_tables(&self) -> Result<Vec<TableSchema>>;
    fn snapshot(&self) -> Result<CatalogSnapshot>;
    fn restore(&self, snapshot: CatalogSnapshot) -> Result<()>;
    fn reserve_table_id(&self, id: TableId) -> Result<()>;
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_drop_table(&self, id: TableId) -> Result<()>;
    fn create_table(
        &self,
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    ) -> Result<TableSchema>;
    fn drop_table(&self, id: TableId) -> Result<()>;

    fn get_index_by_name(&self, name: &str) -> Result<Option<IndexSchema>>;
    fn list_indexes_for_table(&self, table: TableId) -> Result<Vec<IndexSchema>>;
    fn reserve_index_id(&self, id: IndexId) -> Result<()>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, id: IndexId) -> Result<()>;
    fn create_index(
        &self,
        name: String,
        table: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<IndexSchema>;
    fn drop_index(&self, id: IndexId) -> Result<()>;

    fn get_sequence_by_name(&self, name: &str) -> Result<Option<SequenceSchema>>;
    fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceSchema>>;
    fn list_sequences(&self) -> Result<Vec<SequenceSchema>>;
    fn reserve_sequence_id(&self, id: SequenceId) -> Result<()>;
    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()>;
    fn apply_drop_sequence(&self, id: SequenceId) -> Result<()>;
    fn create_sequence(
        &self,
        name: String,
        options: SequenceOptions,
        owned: bool,
    ) -> Result<SequenceSchema>;
    fn drop_sequence(&self, id: SequenceId) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common::{
        ColumnDef, ColumnDefault, DataType, ErrorKind, IndexSchema, ParsedColumnDef,
        SequenceOptions, SequenceSchema, SqlState, TableSchema,
    };

    use crate::{
        CatalogManager, CatalogSnapshot, MemoryCatalog, deserialize_catalog, serialize_catalog,
    };

    fn id_column(nullable: bool) -> ParsedColumnDef {
        ParsedColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable,
            max_length: None,
            default: None,
        }
    }

    /// A `users(id INTEGER PRIMARY KEY, name TEXT)` table for index tests.
    fn catalog_with_users() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        catalog
            .create_table(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
                    },
                ],
                vec!["id".to_string()],
            )
            .unwrap();
        catalog
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
                        max_length: None,
                        default: None,
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: true,
                        max_length: None,
                        default: None,
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
    fn create_table_carries_column_defaults_into_schema() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "t".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "n".to_string(),
                        data_type: DataType::Integer,
                        nullable: true,
                        max_length: None,
                        default: Some(common::ParsedDefault::Const(common::Value::Integer(42))),
                    },
                ],
                vec!["id".to_string()],
            )
            .unwrap();

        assert_eq!(schema.columns[0].default, None);
        assert_eq!(
            schema.columns[1].default,
            Some(common::ColumnDefault::Const(common::Value::Integer(42)))
        );

        // The default survives a serialize/restore round trip.
        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();
        let restored_schema = restored.get_table_by_name("t").unwrap().unwrap();
        assert_eq!(
            restored_schema.columns[1].default,
            Some(common::ColumnDefault::Const(common::Value::Integer(42)))
        );
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
                        max_length: None,
                        default: None,
                    },
                ],
                vec!["id".to_string()],
            )
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn restore_does_not_rewind_allocators() {
        let catalog = catalog_with_users();
        let before_failed_ddl = catalog.snapshot().unwrap();

        let failed_table = catalog
            .create_table(
                "orders".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
            )
            .unwrap();
        let failed_index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        catalog.restore(before_failed_ddl).unwrap();
        assert_eq!(catalog.get_table_by_name("orders").unwrap(), None);
        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);

        let recreated_table = catalog
            .create_table(
                "orders".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
            )
            .unwrap();
        let recreated_index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        assert_eq!(recreated_table.id, failed_table.id + 1);
        assert_eq!(recreated_index.id, failed_index.id + 1);
    }

    #[test]
    fn reserve_ids_advance_allocators_without_installing_objects() {
        let catalog = MemoryCatalog::empty();

        catalog.reserve_table_id(9).unwrap();
        catalog.reserve_index_id(42).unwrap();
        catalog.reserve_sequence_id(11).unwrap();

        assert!(
            catalog.list_tables().unwrap().is_empty(),
            "reserving a table id must not install a table"
        );
        let table = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
            )
            .unwrap();
        assert_eq!(table.id, 10);

        let index = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(index.id, 43);

        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();
        assert_eq!(sequence.id, 12);
    }

    #[test]
    fn create_sequence_assigns_defaults_and_drop_removes_it() {
        let catalog = MemoryCatalog::empty();

        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();

        assert_eq!(sequence.id, 1);
        assert_eq!(sequence.increment, 1);
        assert_eq!(sequence.min_value, 1);
        assert_eq!(sequence.max_value, i64::MAX);
        assert_eq!(sequence.start, 1);
        assert_eq!(sequence.last_value, 1);
        assert!(!sequence.is_called);
        assert!(!sequence.cycle);
        assert!(!sequence.owned);
        assert_eq!(
            catalog
                .get_sequence_by_name("users_id_seq")
                .unwrap()
                .unwrap()
                .id,
            sequence.id
        );
        assert_eq!(catalog.list_sequences().unwrap().len(), 1);

        catalog.drop_sequence(sequence.id).unwrap();
        assert!(catalog.get_sequence(sequence.id).unwrap().is_none());
        assert!(catalog.list_sequences().unwrap().is_empty());
    }

    #[test]
    fn create_sequence_normalizes_descending_defaults() {
        let catalog = MemoryCatalog::empty();

        let sequence = catalog
            .create_sequence(
                "descending_seq".to_string(),
                SequenceOptions {
                    increment: -5,
                    start: None,
                    min_value: None,
                    max_value: None,
                    cycle: true,
                },
                false,
            )
            .unwrap();

        assert_eq!(sequence.increment, -5);
        assert_eq!(sequence.min_value, i64::MIN);
        assert_eq!(sequence.max_value, -1);
        assert_eq!(sequence.start, -1);
        assert_eq!(sequence.last_value, -1);
        assert!(sequence.cycle);
    }

    #[test]
    fn create_sequence_rejects_invalid_options() {
        let catalog = MemoryCatalog::empty();

        for options in [
            SequenceOptions {
                increment: 0,
                ..SequenceOptions::default()
            },
            SequenceOptions {
                min_value: Some(10),
                max_value: Some(5),
                ..SequenceOptions::default()
            },
            SequenceOptions {
                start: Some(99),
                max_value: Some(10),
                ..SequenceOptions::default()
            },
        ] {
            let err = catalog
                .create_sequence("bad_seq".to_string(), options, false)
                .unwrap_err();
            assert_eq!(err.code, SqlState::InvalidParameterValue);
        }
    }

    #[test]
    fn sequence_snapshot_round_trips_and_preserves_allocator() {
        let catalog = MemoryCatalog::empty();
        let first = catalog
            .create_sequence(
                "s".to_string(),
                SequenceOptions {
                    increment: 2,
                    start: Some(5),
                    min_value: Some(1),
                    max_value: Some(100),
                    cycle: true,
                },
                false,
            )
            .unwrap();

        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();

        assert_eq!(
            restored.get_sequence_by_name("s").unwrap().unwrap(),
            SequenceSchema {
                id: first.id,
                name: "s".to_string(),
                increment: 2,
                min_value: 1,
                max_value: 100,
                start: 5,
                cycle: true,
                owned: false,
                last_value: 5,
                is_called: false,
            }
        );
        let next = restored
            .create_sequence("next_s".to_string(), SequenceOptions::default(), false)
            .unwrap();
        assert_eq!(next.id, first.id + 1);
    }

    #[test]
    fn restore_rejects_name_index_without_table() {
        let catalog = MemoryCatalog::empty();
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("ghost".to_string(), 7)]),
            tables_by_id: HashMap::new(),
            next_table_id: 1,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = catalog.restore(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("catalog snapshot"));
    }

    #[test]
    fn try_from_snapshot_rejects_next_table_id_that_reuses_existing_id() {
        let schema = TableSchema {
            id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
            }],
            primary_key: vec![0],
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 3,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("next_table_id"));
    }

    #[test]
    fn try_from_snapshot_accepts_valid_sequence_default() {
        let sequence = SequenceSchema {
            id: 1,
            name: "users_id_seq".to_string(),
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            start: 1,
            cycle: false,
            owned: false,
            last_value: 1,
            is_called: false,
        };
        let schema = TableSchema {
            id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: Some(ColumnDefault::Nextval(1)),
            }],
            primary_key: vec![0],
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            sequences_by_name: HashMap::from([("users_id_seq".to_string(), 1)]),
            sequences_by_id: HashMap::from([(1, sequence)]),
            next_sequence_id: 2,
        };

        let catalog = MemoryCatalog::try_from_snapshot(snapshot).unwrap();
        assert_eq!(
            catalog.get_table_by_name("users").unwrap().unwrap().columns[0].default,
            Some(ColumnDefault::Nextval(1))
        );
    }

    #[test]
    fn apply_create_table_rejects_missing_sequence_default() {
        let catalog = MemoryCatalog::empty();
        let schema = TableSchema {
            id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: Some(ColumnDefault::Nextval(1)),
            }],
            primary_key: vec![0],
        };

        let err = catalog.apply_create_table(schema).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("references missing sequence"));
        assert_eq!(catalog.get_table_by_name("users").unwrap(), None);
    }

    #[test]
    fn create_table_resolves_parsed_nextval_default() {
        let catalog = MemoryCatalog::empty();
        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: Some(common::ParsedDefault::Nextval("users_id_seq".to_string())),
                }],
                vec!["id".to_string()],
            )
            .unwrap();

        assert_eq!(
            schema.columns[0].default,
            Some(ColumnDefault::Nextval(sequence.id))
        );
    }

    #[test]
    fn drop_sequence_rejects_referenced_default() {
        let catalog = MemoryCatalog::empty();
        let sequence = catalog
            .create_sequence(
                "users_id_seq".to_string(),
                SequenceOptions::default(),
                false,
            )
            .unwrap();
        catalog
            .create_table(
                "users".to_string(),
                vec![ParsedColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: Some(common::ParsedDefault::Nextval("users_id_seq".to_string())),
                }],
                vec!["id".to_string()],
            )
            .unwrap();

        let err = catalog.drop_sequence(sequence.id).unwrap_err();

        assert_eq!(err.code, SqlState::DependentObjectsStillExist);
        assert!(catalog.get_sequence(sequence.id).unwrap().is_some());
    }

    #[test]
    fn try_from_snapshot_accepts_composite_primary_key() {
        let schema = TableSchema {
            id: 3,
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                },
                ColumnDef {
                    id: 1,
                    name: "tenant".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                },
            ],
            primary_key: vec![0, 1],
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let catalog = MemoryCatalog::try_from_snapshot(snapshot).unwrap();
        assert_eq!(
            catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap()
                .primary_key,
            vec![0, 1]
        );
    }

    #[test]
    fn try_from_snapshot_rejects_nullable_primary_key_column() {
        let schema = TableSchema {
            id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                max_length: None,
                default: None,
            }],
            primary_key: vec![0],
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("primary key"));
    }

    #[test]
    fn try_from_snapshot_rejects_non_contiguous_column_ids() {
        let schema = TableSchema {
            id: 3,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
            }],
            primary_key: vec![1],
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 3)]),
            tables_by_id: HashMap::from([(3, schema)]),
            next_table_id: 4,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("column id"));
    }

    #[test]
    fn duplicate_primary_key_column_is_rejected_with_syntax_error() {
        let catalog = MemoryCatalog::empty();

        let err = catalog
            .create_table(
                "users".to_string(),
                vec![id_column(false)],
                vec!["id".to_string(), "id".to_string()],
            )
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::SyntaxError);
    }

    #[test]
    fn create_table_rejects_empty_primary_key() {
        let catalog = MemoryCatalog::empty();

        let err = catalog
            .create_table("users".to_string(), vec![id_column(false)], vec![])
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Plan);
        assert_eq!(err.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn create_table_accepts_composite_primary_key() {
        let catalog = MemoryCatalog::empty();

        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![
                    id_column(false),
                    ParsedColumnDef {
                        name: "tenant".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                    },
                ],
                vec!["id".to_string(), "tenant".to_string()],
            )
            .unwrap();

        // Both columns are recorded as the composite key, in declared order, and
        // each is forced non-null.
        assert_eq!(schema.primary_key, vec![0, 1]);
        assert!(!schema.columns[0].nullable);
        assert!(!schema.columns[1].nullable);
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
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();

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
                max_length: None,
                default: None,
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

    #[test]
    fn create_index_resolves_columns_and_assigns_ids() {
        let catalog = catalog_with_users();
        let table = catalog.get_table_by_name("users").unwrap().unwrap();

        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        assert_eq!(index.id, 1);
        assert_eq!(index.table, table.id);
        assert_eq!(index.columns, vec![1]);
        assert!(!index.unique);

        let second = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], true)
            .unwrap();
        assert_eq!(second.id, 2);
        assert!(second.unique);
        assert_eq!(second.columns, vec![0]);
    }

    #[test]
    fn duplicate_index_name_is_rejected() {
        let catalog = catalog_with_users();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        let err = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["id".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::DuplicateTable);
    }

    #[test]
    fn create_index_on_missing_table_is_rejected() {
        let catalog = catalog_with_users();
        let err = catalog
            .create_index("ghost".to_string(), "ghost", &["id".to_string()], false)
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn create_index_on_missing_column_is_rejected() {
        let catalog = catalog_with_users();
        let err = catalog
            .create_index(
                "users_missing".to_string(),
                "users",
                &["missing".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedColumn);
    }

    #[test]
    fn create_index_rejects_duplicate_and_empty_columns() {
        let catalog = catalog_with_users();

        let duplicate = catalog
            .create_index(
                "dup".to_string(),
                "users",
                &["id".to_string(), "id".to_string()],
                false,
            )
            .unwrap_err();
        assert_eq!(duplicate.code, SqlState::SyntaxError);

        let empty = catalog
            .create_index("empty".to_string(), "users", &[], false)
            .unwrap_err();
        assert_eq!(empty.code, SqlState::SyntaxError);
    }

    #[test]
    fn get_index_by_name_returns_schema_or_none() {
        let catalog = catalog_with_users();
        let created = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        assert_eq!(
            catalog.get_index_by_name("users_name").unwrap(),
            Some(created)
        );
        assert_eq!(catalog.get_index_by_name("absent").unwrap(), None);
    }

    #[test]
    fn list_indexes_for_table_filters_and_sorts_by_id() {
        let catalog = catalog_with_users();
        let accounts = catalog
            .create_table(
                "accounts".to_string(),
                vec![id_column(false)],
                vec!["id".to_string()],
            )
            .unwrap();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();
        catalog
            .create_index(
                "accounts_id".to_string(),
                "accounts",
                &["id".to_string()],
                false,
            )
            .unwrap();
        catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();

        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let listed = catalog.list_indexes_for_table(users.id).unwrap();
        let ids: Vec<_> = listed.iter().map(|index| index.id).collect();
        let names: Vec<_> = listed.iter().map(|index| index.name.as_str()).collect();
        assert_eq!(ids, vec![1, 3]);
        assert_eq!(names, vec!["users_name", "users_id"]);

        assert_eq!(
            catalog.list_indexes_for_table(accounts.id).unwrap().len(),
            1
        );
    }

    #[test]
    fn drop_index_removes_lookups_without_reusing_id() {
        let catalog = catalog_with_users();
        let index = catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();

        catalog.drop_index(index.id).unwrap();
        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);

        let next = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(next.id, index.id + 1);
    }

    #[test]
    fn drop_index_on_missing_id_is_rejected() {
        let catalog = catalog_with_users();
        let err = catalog.drop_index(42).unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn drop_table_cascades_to_its_indexes() {
        let catalog = catalog_with_users();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                false,
            )
            .unwrap();
        catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();

        catalog.drop_table(users.id).unwrap();

        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);
        assert_eq!(catalog.get_index_by_name("users_id").unwrap(), None);
        assert!(catalog.list_indexes_for_table(users.id).unwrap().is_empty());
    }

    #[test]
    fn apply_create_and_drop_index_drive_recovery_by_id() {
        let catalog = catalog_with_users();
        let users = catalog.get_table_by_name("users").unwrap().unwrap();
        let schema = IndexSchema {
            id: 5,
            table: users.id,
            name: "users_name".to_string(),
            columns: vec![1],
            unique: false,
        };

        catalog.apply_create_index(schema.clone()).unwrap();
        assert_eq!(
            catalog.get_index_by_name("users_name").unwrap(),
            Some(schema.clone())
        );

        let duplicate = catalog.apply_create_index(schema).unwrap_err();
        assert_eq!(duplicate.code, SqlState::DuplicateTable);

        // next_index_id advanced past the replayed id, so a fresh create skips it.
        let next = catalog
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(next.id, 6);

        catalog.apply_drop_index(5).unwrap();
        assert_eq!(catalog.get_index_by_name("users_name").unwrap(), None);
    }

    #[test]
    fn serialize_round_trip_preserves_indexes() {
        let catalog = catalog_with_users();
        catalog
            .create_index(
                "users_name".to_string(),
                "users",
                &["name".to_string()],
                true,
            )
            .unwrap();

        let bytes = serialize_catalog(&catalog.snapshot().unwrap()).unwrap();
        let restored =
            MemoryCatalog::try_from_snapshot(deserialize_catalog(&bytes).unwrap()).unwrap();

        let index = restored.get_index_by_name("users_name").unwrap().unwrap();
        assert!(index.unique);
        assert_eq!(index.columns, vec![1]);

        // next_index_id survives the round trip, so ids keep climbing.
        let next = restored
            .create_index("users_id".to_string(), "users", &["id".to_string()], false)
            .unwrap();
        assert_eq!(next.id, 2);
    }

    #[test]
    fn validate_rejects_index_referencing_missing_table() {
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::new(),
            tables_by_id: HashMap::new(),
            next_table_id: 1,
            indexes_by_name: HashMap::from([("orphan".to_string(), 1)]),
            indexes_by_id: HashMap::from([(
                1,
                IndexSchema {
                    id: 1,
                    table: 7,
                    name: "orphan".to_string(),
                    columns: vec![0],
                    unique: false,
                },
            )]),
            next_index_id: 2,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("missing table"));
    }

    #[test]
    fn validate_rejects_reserved_primary_key_index_id() {
        let table = TableSchema {
            id: 1,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
            }],
            primary_key: vec![0],
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, table)]),
            next_table_id: 2,
            indexes_by_name: HashMap::from([("bad".to_string(), 0)]),
            indexes_by_id: HashMap::from([(
                0,
                IndexSchema {
                    id: 0,
                    table: 1,
                    name: "bad".to_string(),
                    columns: vec![0],
                    unique: false,
                },
            )]),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("reserved primary-key index id"));
    }

    #[test]
    fn validate_rejects_next_index_id_that_reuses_existing_id() {
        let table = TableSchema {
            id: 1,
            name: "users".to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
            }],
            primary_key: vec![0],
        };
        let snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("users".to_string(), 1)]),
            tables_by_id: HashMap::from([(1, table)]),
            next_table_id: 2,
            indexes_by_name: HashMap::from([("users_id".to_string(), 1)]),
            indexes_by_id: HashMap::from([(
                1,
                IndexSchema {
                    id: 1,
                    table: 1,
                    name: "users_id".to_string(),
                    columns: vec![0],
                    unique: false,
                },
            )]),
            next_index_id: 1,
            ..CatalogSnapshot::default()
        };

        let err = MemoryCatalog::try_from_snapshot(snapshot).unwrap_err();
        assert!(err.message.contains("next_index_id"));
    }

    #[test]
    fn snapshot_without_index_fields_deserializes_to_empty_indexes() {
        // A catalog persisted before secondary indexes and sequences existed.
        let json = r#"{
            "tables_by_name": {"users": 1},
            "tables_by_id": {"1": {
                "id": 1,
                "name": "users",
                "columns": [{"id": 0, "name": "id", "data_type": "Integer", "nullable": false}],
                "primary_key": [0]
            }},
            "next_table_id": 2
        }"#;

        let snapshot = deserialize_catalog(json.as_bytes()).unwrap();
        assert!(snapshot.indexes_by_id.is_empty());
        assert!(snapshot.indexes_by_name.is_empty());
        assert_eq!(snapshot.next_index_id, 1);
        assert!(snapshot.sequences_by_id.is_empty());
        assert!(snapshot.sequences_by_name.is_empty());
        assert_eq!(snapshot.next_sequence_id, 1);

        // The validated load path accepts it.
        MemoryCatalog::try_from_snapshot(snapshot).unwrap();
    }
}
