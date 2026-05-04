mod codec;
mod engine;
mod page;
mod recovery;
mod traits;

pub use codec::{decode_row, encode_row};
pub use engine::{PageBackedStorageEngine, StorageMode};
pub use traits::{RecoveryOperations, RowIterator, SchemaOperations, StorageEngine};

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use buffer::{BufferPool, MemoryBufferPool, PageData};
    use common::{
        ColumnDef, DataType, Key, KeyRange, Lsn, Result, Row, SqlState, StatementContext,
        TableSchema, Value,
    };
    use wal::{WalManager, WalRecord};

    use crate::{
        PageBackedStorageEngine, RecoveryOperations, SchemaOperations, StorageEngine, StorageMode,
        decode_row, encode_row,
    };

    #[test]
    fn storage_traits_are_object_safe() {
        fn assert_engine<T: StorageEngine + ?Sized>() {}
        fn assert_schema<T: SchemaOperations + ?Sized>() {}
        fn assert_recovery<T: RecoveryOperations + ?Sized>() {}

        assert_engine::<dyn StorageEngine>();
        assert_schema::<dyn SchemaOperations>();
        assert_recovery::<dyn RecoveryOperations>();
    }

    #[test]
    fn row_codec_round_trips_all_v1_types_and_nulls() {
        let schema = users_schema();
        let row = Row {
            values: vec![
                Value::Integer(7),
                Value::Text("Ada".to_string()),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let bytes = encode_row(&schema, &row).unwrap();
        let decoded = decode_row(&schema, &bytes).unwrap();

        assert_eq!(decoded, row);
    }

    #[test]
    fn insert_get_update_delete_round_trip_through_pages() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        harness.create_users_table(&ctx).unwrap();

        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        assert_eq!(
            harness
                .storage
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );

        harness
            .storage
            .update(
                &ctx,
                1,
                &Key(vec![Value::Integer(1)]),
                user_row(1, "Lovelace", false),
            )
            .unwrap();

        assert_eq!(
            harness
                .storage
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Lovelace", false))
        );

        assert!(
            harness
                .storage
                .delete(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap()
        );
        assert_eq!(
            harness
                .storage
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            None
        );
    }

    #[test]
    fn recovery_apply_does_not_append_wal() {
        let harness = StorageHarness::new();
        harness.storage.apply_create_table(users_schema()).unwrap();

        harness
            .storage
            .apply_insert(1, Key(vec![Value::Integer(1)]), user_row(1, "Ada", true))
            .unwrap();

        assert_eq!(harness.wal.record_count(), 0);
    }

    #[test]
    fn recovery_update_delete_and_drop_do_not_append_wal() {
        let harness = StorageHarness::new();
        let key = Key(vec![Value::Integer(1)]);
        harness.storage.apply_create_table(users_schema()).unwrap();
        harness
            .storage
            .apply_insert(1, key.clone(), user_row(1, "Ada", true))
            .unwrap();

        harness
            .storage
            .apply_update(1, key.clone(), user_row(1, "Lovelace", false))
            .unwrap();
        assert_eq!(
            harness
                .storage
                .get(&StatementContext { txn_id: 0 }, 1, &key)
                .unwrap(),
            Some(user_row(1, "Lovelace", false))
        );

        harness.storage.apply_delete(1, key.clone()).unwrap();
        assert_eq!(
            harness
                .storage
                .get(&StatementContext { txn_id: 0 }, 1, &key)
                .unwrap(),
            None
        );

        harness
            .storage
            .apply_insert(1, key.clone(), user_row(1, "Ada", true))
            .unwrap();
        harness.storage.apply_drop_table(1).unwrap();
        let err = match harness.storage.scan(&StatementContext { txn_id: 0 }, 1) {
            Ok(_) => panic!("expected dropped table to be unavailable"),
            Err(err) => err,
        };
        assert_eq!(err.code, SqlState::UndefinedTable);
        assert_eq!(harness.wal.record_count(), 0);
    }

    #[test]
    fn duplicate_insert_returns_unique_violation_without_replacing_row() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        let err = harness
            .storage
            .insert(&ctx, 1, user_row(1, "Grace", false))
            .unwrap_err();

        assert_eq!(err.code, SqlState::UniqueViolation);
        assert_eq!(
            harness
                .storage
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
    }

    #[test]
    fn scan_range_walks_primary_key_directory_in_key_order() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(3, "Three", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "One", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Two", true))
            .unwrap();

        let mut iter = harness.storage.scan_range(&ctx, 1, &KeyRange::All).unwrap();
        let mut keys = Vec::new();
        while let Some(row) = iter.next().unwrap() {
            keys.push(row.key);
        }

        assert_eq!(
            keys,
            vec![
                Key(vec![Value::Integer(1)]),
                Key(vec![Value::Integer(2)]),
                Key(vec![Value::Integer(3)]),
            ]
        );
    }

    #[test]
    fn scan_returns_stored_row_identity_and_bounded_range() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "One", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Two", false))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(3, "Three", true))
            .unwrap();

        let mut iter = harness
            .storage
            .scan_range(
                &ctx,
                1,
                &KeyRange::Range {
                    start: Bound::Included(Key(vec![Value::Integer(2)])),
                    end: Bound::Excluded(Key(vec![Value::Integer(3)])),
                },
            )
            .unwrap();

        let row = iter.next().unwrap().unwrap();
        assert_eq!(row.key, Key(vec![Value::Integer(2)]));
        assert_eq!(row.row_id.page_num, 0);
        assert_eq!(row.row_id.slot_num, 1);
        assert_eq!(row.row, user_row(2, "Two", false));
        assert_eq!(iter.next().unwrap(), None);
    }

    #[test]
    fn rebuild_directories_restores_lookup_from_table_pages() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        let reopened = PageBackedStorageEngine::open(
            harness.buffer.clone(),
            harness.wal.clone(),
            StorageMode::Normal,
        )
        .unwrap();
        reopened.install_schemas(vec![users_schema()]).unwrap();
        reopened.rebuild_directories().unwrap();

        assert_eq!(
            reopened
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
    }

    #[test]
    fn rollback_txn_restores_storage_owned_directory_metadata() {
        let harness = StorageHarness::new();
        harness
            .create_users_table(&StatementContext { txn_id: 0 })
            .unwrap();
        let ctx = StatementContext { txn_id: 1 };
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        harness.storage.rollback_txn(ctx.txn_id).unwrap();
        harness.buffer.rollback(ctx.txn_id).unwrap();

        assert_eq!(
            harness
                .storage
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            None
        );
    }

    #[test]
    fn rollback_txn_restores_update_and_drop_metadata() {
        let harness = StorageHarness::new();
        harness
            .create_users_table(&StatementContext { txn_id: 0 })
            .unwrap();
        let insert_ctx = StatementContext { txn_id: 1 };
        harness
            .storage
            .insert(&insert_ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness.storage.commit_txn(insert_ctx.txn_id).unwrap();
        harness.buffer.commit(insert_ctx.txn_id).unwrap();

        let update_ctx = StatementContext { txn_id: 2 };
        harness
            .storage
            .update(
                &update_ctx,
                1,
                &Key(vec![Value::Integer(1)]),
                user_row(1, "Lovelace", false),
            )
            .unwrap();
        harness.storage.rollback_txn(update_ctx.txn_id).unwrap();
        harness.buffer.rollback(update_ctx.txn_id).unwrap();

        assert_eq!(
            harness
                .storage
                .get(&update_ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );

        let drop_ctx = StatementContext { txn_id: 3 };
        harness.storage.drop_table(&drop_ctx, 1).unwrap();
        harness.storage.rollback_txn(drop_ctx.txn_id).unwrap();

        assert_eq!(
            harness
                .storage
                .get(&drop_ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
    }

    #[test]
    fn rollback_after_insert_on_new_page_removes_directory_and_buffer_page() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let large_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("x".repeat(8159)),
                Value::Null,
            ],
        };
        harness.storage.insert(&ctx, 2, large_row).unwrap();
        harness.storage.commit_txn(ctx.txn_id).unwrap();
        harness.buffer.commit(ctx.txn_id).unwrap();

        let failed_ctx = StatementContext { txn_id: 2 };
        harness
            .storage
            .insert(&failed_ctx, 2, small_big_text_row(2))
            .unwrap();
        harness.storage.rollback_txn(failed_ctx.txn_id).unwrap();
        harness.buffer.rollback(failed_ctx.txn_id).unwrap();

        assert_eq!(
            harness
                .storage
                .get(&failed_ctx, 2, &Key(vec![Value::Integer(2)]))
                .unwrap(),
            None
        );
        assert!(
            harness
                .buffer
                .iter_pages()
                .unwrap()
                .all(|page| page.file_id != 2 || page.page_num != 1)
        );
    }

    #[test]
    fn insert_accepts_row_that_fills_single_page_payload_capacity() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("x".repeat(8159)),
                Value::Null,
            ],
        };

        let row_id = harness.storage.insert(&ctx, 2, row.clone()).unwrap();

        assert_eq!(row_id.page_num, 0);
        assert_eq!(
            harness
                .storage
                .get(&ctx, 2, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(row)
        );
    }

    #[test]
    fn insert_rejects_row_too_large_before_allocating_page() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("x".repeat(8160)),
                Value::Null,
            ],
        };

        let err = harness.storage.insert(&ctx, 2, row).unwrap_err();

        assert!(err.message.contains("row is too large for a data page"));
        assert!(
            harness
                .buffer
                .iter_pages()
                .unwrap()
                .all(|page| page.file_id != 2)
        );
    }

    #[test]
    fn page_reports_no_space_when_only_existing_slot_boundary_remains() {
        let mut page = PageData::default();
        crate::page::init_page(&mut page.0, 0);
        crate::page::insert_row(&mut page.0, &vec![0; 8172]).unwrap();

        assert!(!crate::page::has_space_for(&page.0, 1).unwrap());
    }

    #[test]
    fn insert_allocates_new_page_when_first_page_has_no_next_slot_space() {
        let harness = StorageHarness::new();
        let ctx = StatementContext { txn_id: 1 };
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let large_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("x".repeat(8159)),
                Value::Null,
            ],
        };
        harness.storage.insert(&ctx, 2, large_row.clone()).unwrap();

        let row_id = harness
            .storage
            .insert(&ctx, 2, small_big_text_row(2))
            .unwrap();

        assert_eq!(row_id.page_num, 1);
        assert_eq!(
            harness
                .storage
                .get(&ctx, 2, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(large_row)
        );
        assert_eq!(
            harness
                .storage
                .get(&ctx, 2, &Key(vec![Value::Integer(2)]))
                .unwrap(),
            Some(small_big_text_row(2))
        );
    }

    struct StorageHarness {
        storage: PageBackedStorageEngine,
        buffer: Arc<MemoryBufferPool>,
        wal: Arc<CountingWal>,
    }

    impl StorageHarness {
        fn new() -> Self {
            let buffer = Arc::new(MemoryBufferPool::empty(64));
            let wal = Arc::new(CountingWal::default());
            let storage =
                PageBackedStorageEngine::open(buffer.clone(), wal.clone(), StorageMode::Normal)
                    .unwrap();
            Self {
                storage,
                buffer,
                wal,
            }
        }

        fn create_users_table(&self, ctx: &StatementContext) -> Result<()> {
            self.storage.create_table(ctx, &users_schema())
        }
    }

    #[derive(Default)]
    struct CountingWal {
        count: AtomicUsize,
    }

    impl CountingWal {
        fn record_count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl WalManager for CountingWal {
        fn append(&self, _record: WalRecord) -> Result<Lsn> {
            Ok(self.count.fetch_add(1, Ordering::SeqCst) as Lsn + 1)
        }

        fn flush(&self) -> Result<Lsn> {
            Ok(self.count.load(Ordering::SeqCst) as Lsn)
        }

        fn replay_from(&self, _lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>> {
            Ok(Box::new(std::iter::empty()))
        }

        fn replay_committed_from(
            &self,
            _lsn: Lsn,
        ) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>> {
            Ok(Box::new(std::iter::empty()))
        }

        fn truncate_before(&self, _lsn: Lsn) -> Result<()> {
            Ok(())
        }

        fn is_committed(&self, _txn_id: u64) -> bool {
            false
        }

        fn flushed_lsn(&self) -> Lsn {
            0
        }

        fn bytes_after(&self, _lsn: Lsn) -> Result<u64> {
            Ok(0)
        }
    }

    fn users_schema() -> TableSchema {
        TableSchema {
            id: 1,
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
                ColumnDef {
                    id: 2,
                    name: "active".to_string(),
                    data_type: DataType::Boolean,
                    nullable: true,
                },
                ColumnDef {
                    id: 3,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn user_row(id: i64, name: &str, active: bool) -> Row {
        Row {
            values: vec![
                Value::Integer(id),
                Value::Text(name.to_string()),
                Value::Boolean(active),
                Value::Null,
            ],
        }
    }

    fn big_text_schema() -> TableSchema {
        TableSchema {
            id: 2,
            name: "big_text".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    id: 1,
                    name: "payload".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
                ColumnDef {
                    id: 2,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn small_big_text_row(id: i64) -> Row {
        Row {
            values: vec![
                Value::Integer(id),
                Value::Text("small".to_string()),
                Value::Null,
            ],
        }
    }
}
