mod btree;
mod codec;
mod engine;
mod heap;
mod index_page;
mod page;
mod recovery;
mod redo;
mod toast;
mod traits;

pub use codec::{DecodedRow, decode_row, encode_row};
pub use engine::{PageBackedStorageEngine, RewriteTablePages, StorageMode};
pub use heap::HeapPageStore;
pub use page::is_valid as page_is_valid;
pub use redo::apply_physical_redo;
pub use traits::{
    RecoveryOperations, RelationSnapshot, RowIterator, SchemaOperations, StorageEngine,
};

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use buffer::{BufferPool, MemoryBufferPool, PAGE_SIZE, PageData};
    use common::{
        CancelReason, ColumnDef, CompressionSetting, DataType, DbError, FileId, INVALID_XID,
        IndexSchema, Key, KeyRange, Lsn, RelationKind, Result, Row, SequenceManager,
        SequenceSchema, SqlState, StatementContext, TableSchema, ToastCompression, ToastMode,
        ToastOptions, TxnId, TxnStatus, TxnStatusView, Value, toast_schema,
    };
    use wal::{WalManager, WalRecord, WalRecordKind};

    use crate::{
        PageBackedStorageEngine, RecoveryOperations, RowIterator, SchemaOperations, StorageEngine,
        StorageMode, apply_physical_redo,
        btree::BTree,
        codec::{
            DecodedPhysicalValue, HEAP_ONLY, HOT_UPDATED, MvccHeader, PreparedColumnValue,
            ToastPointer, V2_MVCC_HEADER_LEN, VarlenaPhysical, decode_physical_row,
            encode_row_v3_prepared, null_bitmap_len,
        },
        decode_row, encode_row,
        engine::RowLocation,
        heap::{heap_file_id, primary_index_file_id, secondary_index_file_id},
        toast::{TOAST_CHUNK_PAYLOAD, build_external_stream},
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

        let bytes = encode_row(&schema, &row, 1).unwrap();
        let decoded = decode_row(&schema, &bytes).unwrap();

        assert_eq!(decoded.row, row);
    }

    #[test]
    fn insert_get_update_delete_round_trip_through_pages() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
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
    fn recovery_apply_ddl_does_not_append_wal() {
        let harness = StorageHarness::new();
        harness.storage.apply_create_table(users_schema()).unwrap();
        harness
            .storage
            .apply_create_index(name_index(false))
            .unwrap();
        harness
            .storage
            .apply_create_sequence(sequence_schema(7, "users_id_seq", 1, 1, 10, 1, false))
            .unwrap();
        harness.storage.apply_sequence_advance(7, 3).unwrap();
        harness
            .storage
            .apply_set_sequence_value(7, 5, false)
            .unwrap();
        harness.storage.apply_drop_index(1).unwrap();
        harness.storage.apply_drop_sequence(7).unwrap();
        harness.storage.apply_drop_table(1).unwrap();

        assert_eq!(harness.wal.record_count(), 0);
    }

    #[test]
    fn nextval_logs_advances_and_keeps_gaps_after_rollback() {
        let harness = StorageHarness::new();
        harness
            .storage
            .create_sequence(
                &StatementContext::new(1),
                &sequence_schema(7, "users_id_seq", 1, 1, 3, 1, false),
            )
            .unwrap();

        assert_eq!(harness.wal.record_count(), 1);
        assert_eq!(harness.storage.nextval(2, 7).unwrap(), 1);
        assert_eq!(harness.storage.nextval(2, 7).unwrap(), 2);

        harness.storage.rollback_txn(2).unwrap();

        assert_eq!(harness.storage.nextval(3, 7).unwrap(), 3);
        assert_eq!(harness.wal.record_count(), 4);
    }

    #[test]
    fn nextval_flushes_sequence_wal_before_returning() {
        let harness = StorageHarness::new();
        harness
            .storage
            .create_sequence(
                &StatementContext::new(1),
                &sequence_schema(7, "users_id_seq", 1, 1, 10, 1, false),
            )
            .unwrap();

        assert_eq!(harness.wal.flush_count(), 0);
        assert_eq!(harness.storage.nextval(2, 7).unwrap(), 1);

        assert_eq!(harness.wal.flush_count(), 1);
        assert_eq!(harness.wal.flushed_lsn(), harness.wal.record_count() as Lsn);
    }

    #[test]
    fn sequence_wal_flush_does_not_hold_global_storage_lock() {
        let buffer = Arc::new(MemoryBufferPool::empty(64));
        let wal = Arc::new(CountingWal::default());
        let storage = Arc::new(
            PageBackedStorageEngine::open(buffer, wal.clone(), StorageMode::Normal).unwrap(),
        );
        storage
            .create_sequence(
                &StatementContext::new(1),
                &sequence_schema(7, "users_id_seq", 1, 1, 10, 1, false),
            )
            .unwrap();
        let (flush_entered, release_flush) = wal.block_next_flush();

        let nextval_storage = storage.clone();
        let nextval = std::thread::spawn(move || nextval_storage.nextval(2, 7));
        flush_entered
            .recv_timeout(Duration::from_secs(1))
            .expect("nextval did not reach the sequence WAL flush");

        let (metadata_done_tx, metadata_done_rx) = mpsc::channel();
        let metadata_storage = storage.clone();
        let metadata = std::thread::spawn(move || {
            metadata_storage.set_mode(StorageMode::Normal).unwrap();
            metadata_done_tx.send(()).unwrap();
        });
        metadata_done_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("sequence WAL flush held the global storage lock");

        release_flush.send(()).unwrap();
        assert_eq!(nextval.join().unwrap().unwrap(), 1);
        metadata.join().unwrap();
    }

    #[test]
    fn sequences_enforce_bounds_cycle_and_setval_false() {
        let harness = StorageHarness::new();
        harness
            .storage
            .create_sequence(
                &StatementContext::new(1),
                &sequence_schema(8, "bounded", 1, 1, 2, 1, false),
            )
            .unwrap();
        assert_eq!(harness.storage.nextval(2, 8).unwrap(), 1);
        assert_eq!(harness.storage.nextval(2, 8).unwrap(), 2);
        let err = harness.storage.nextval(2, 8).unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);

        harness
            .storage
            .create_sequence(
                &StatementContext::new(3),
                &sequence_schema(9, "cycling", 1, 1, 2, 1, true),
            )
            .unwrap();
        assert_eq!(harness.storage.nextval(4, 9).unwrap(), 1);
        assert_eq!(harness.storage.nextval(4, 9).unwrap(), 2);
        assert_eq!(harness.storage.nextval(4, 9).unwrap(), 1);

        harness
            .storage
            .create_sequence(
                &StatementContext::new(5),
                &sequence_schema(10, "settable", 5, 1, 20, 1, false),
            )
            .unwrap();
        assert_eq!(harness.storage.setval(6, 10, 11, false).unwrap(), 11);
        assert_eq!(harness.storage.nextval(7, 10).unwrap(), 11);
        assert_eq!(harness.storage.nextval(7, 10).unwrap(), 12);

        let settable = harness
            .storage
            .sequence_schemas_for_checkpoint()
            .unwrap()
            .into_iter()
            .find(|sequence| sequence.id == 10)
            .unwrap();
        assert_eq!(settable.last_value, 12);
        assert!(settable.is_called);
    }

    #[test]
    fn failed_heap_insert_wal_append_does_not_leave_tuple_bytes() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();

        // A first insert into an empty heap appends HeapInit, initializes the page,
        // inserts the tuple bytes, then appends HeapInsert. Fail that second append:
        // the correct behavior is that the failed statement leaves no tuple behind.
        harness
            .wal
            .fail_on_append_number(harness.wal.record_count() + 2);

        let err = harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );

        let page = harness.buffer.read_page(1, 0).unwrap();
        assert_eq!(
            crate::page::next_slot(page.data()).unwrap(),
            0,
            "failed WAL append left an unlogged tuple slot on the heap page"
        );
    }

    #[test]
    fn failed_heap_init_append_does_not_leave_dirty_zero_page() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness.buffer.mark_all_clean().unwrap();

        harness
            .wal
            .fail_on_append_number(harness.wal.record_count() + 1);
        let err = harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );

        let dirty_zero_pages: Vec<_> = harness
            .buffer
            .iter_pages()
            .unwrap()
            .filter(|page| {
                page.file_id == 1 && page.is_dirty && !crate::page::is_valid(&page.data.0)
            })
            .map(|page| page.page_num)
            .collect();
        assert!(
            dirty_zero_pages.is_empty(),
            "failed HeapInit append left dirty zero heap pages: {dirty_zero_pages:?}"
        );
        assert_eq!(
            harness.buffer.page_count(1).unwrap(),
            0,
            "failed HeapInit append advertised a heap page with no redo base"
        );
        assert_eq!(
            harness.storage.vacuum(&users_schema(), 100).unwrap(),
            0,
            "VACUUM should ignore the abandoned failed-allocation page"
        );

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
    }

    #[test]
    fn failed_heap_insert_fpi_append_restores_page_and_fpi_flag() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness.buffer.mark_all_clean().unwrap();

        harness.wal.fail_next_full_page_image();
        let err = harness
            .storage
            .insert(&ctx, 1, user_row(2, "Grace", true))
            .unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );
        let page = harness.buffer.read_page(1, 0).unwrap();
        assert_eq!(
            crate::page::next_slot(page.data()).unwrap(),
            1,
            "failed FPI append left the second tuple on the heap page"
        );
        drop(page);

        harness
            .storage
            .insert(&ctx, 1, user_row(3, "Hopper", true))
            .unwrap();
        assert_eq!(
            harness.wal.full_page_image_count(1),
            1,
            "failed FPI append consumed the page's first-touch FPI flag"
        );
    }

    #[test]
    fn failed_xmax_fpi_append_does_not_stamp_header_and_restores_fpi_flag() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        harness.create_users_table(&insert_ctx).unwrap();
        harness
            .storage
            .insert(&insert_ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness.buffer.mark_all_clean().unwrap();

        harness.wal.fail_next_full_page_image();
        let delete_ctx = StatementContext::new(2);
        let err = harness
            .storage
            .delete(&delete_ctx, 1, &Key(vec![Value::Integer(1)]))
            .unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );

        let page = harness.buffer.read_page(1, 0).unwrap();
        let tuple = crate::page::read_row(page.data(), 0)
            .unwrap()
            .expect("original tuple remains");
        let decoded = decode_row(&users_schema(), &tuple).unwrap();
        assert_eq!(
            decoded.xmax, INVALID_XID,
            "failed FPI append left an unlogged xmax stamp in the heap tuple"
        );
        drop(page);

        let retry_ctx = StatementContext::new(3);
        assert!(
            harness
                .storage
                .delete(&retry_ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap()
        );
        assert_eq!(
            harness.wal.full_page_image_count(1),
            1,
            "failed xmax FPI append consumed the page's first-touch FPI flag"
        );
    }

    #[test]
    fn failed_xmax_delta_append_does_not_stamp_header() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        harness.create_users_table(&insert_ctx).unwrap();
        harness
            .storage
            .insert(&insert_ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness
            .storage
            .insert(&insert_ctx, 1, user_row(2, "Grace", true))
            .unwrap();

        harness.wal.fail_next_heap_update_header();
        let delete_ctx = StatementContext::new(2);
        let err = harness
            .storage
            .delete(&delete_ctx, 1, &Key(vec![Value::Integer(1)]))
            .unwrap_err();
        assert!(
            err.message.contains("injected WAL append failure"),
            "unexpected error: {err:?}"
        );

        let page = harness.buffer.read_page(1, 0).unwrap();
        let tuple = crate::page::read_row(page.data(), 0)
            .unwrap()
            .expect("original tuple remains");
        let decoded = decode_row(&users_schema(), &tuple).unwrap();
        assert_eq!(
            decoded.xmax, INVALID_XID,
            "failed HeapUpdateHeader append left an unlogged xmax stamp in the heap tuple"
        );
    }

    #[test]
    fn failed_xmax_preflight_restores_fpi_flag_without_wal() {
        let harness = StorageHarness::new();
        let setup_ctx = StatementContext::new(1);
        harness.create_users_table(&setup_ctx).unwrap();

        let mut page = harness.buffer.new_page(1, setup_ctx.txn_id).unwrap();
        let page_num = page.page_num();
        crate::page::init_page(page.data_mut(), page_num);
        let slot =
            crate::page::insert_row(page.data_mut(), &legacy_user_row(1, "Ada", true)).unwrap();
        let location = crate::engine::RowLocation {
            file_id: 1,
            page_num,
            slot_num: slot,
        };
        drop(page);
        let registry = compress::CompressionRegistry::new();
        crate::btree::BTree::new(
            harness.buffer.as_ref(),
            harness.wal.as_ref(),
            crate::heap::primary_index_file_id(1),
            &registry,
        )
        .insert(setup_ctx.txn_id, &Key(vec![Value::Integer(1)]), &location)
        .unwrap();
        harness.buffer.mark_all_clean().unwrap();

        let before = harness.wal.record_count();
        let err = harness
            .storage
            .delete(&StatementContext::new(2), 1, &Key(vec![Value::Integer(1)]))
            .unwrap_err();
        assert!(
            err.message.contains("cannot mutate header"),
            "unexpected error: {err:?}"
        );
        assert_eq!(
            harness.wal.record_count(),
            before,
            "legacy-tuple header preflight failure appended WAL"
        );

        harness
            .storage
            .insert(&StatementContext::new(3), 1, user_row(2, "Grace", true))
            .unwrap();
        assert_eq!(
            harness.wal.full_page_image_count(1),
            1,
            "failed header preflight consumed the page's first-touch FPI flag"
        );
    }

    #[test]
    fn create_index_logs_a_create_index_record() {
        let dir = tempfile::tempdir().unwrap();
        let buffer = Arc::new(MemoryBufferPool::empty(64));
        let wal = Arc::new(wal::FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let storage =
            PageBackedStorageEngine::open(buffer, wal.clone(), StorageMode::Normal).unwrap();
        let ctx = StatementContext::new(1);
        storage.create_table(&ctx, &users_schema()).unwrap();
        storage.create_index(&ctx, &name_index(false), 0).unwrap();
        wal.flush().unwrap();

        let logged = wal
            .replay_from(0)
            .unwrap()
            .filter_map(|record| record.ok())
            .any(|record| matches!(record.kind, wal::WalRecordKind::CreateIndex { .. }));
        assert!(logged, "create_index should log a CreateIndex WAL record");
    }

    #[test]
    fn storage_routes_files_by_storage_id_not_logical_id() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let mut schema = users_schema();
        schema.id = 2;
        schema.storage_id = 42;
        schema.name = "accounts".to_string();
        let index = IndexSchema {
            id: 7,
            storage_id: 43,
            table: schema.id,
            name: "accounts_name".to_string(),
            columns: vec![1],
            unique: false,
            constraint: common::IndexConstraintKind::None,
        };

        harness.storage.create_table(&ctx, &schema).unwrap();
        harness.storage.create_index(&ctx, &index, 0).unwrap();
        harness
            .storage
            .insert(&ctx, schema.id, user_row(1, "Ada", true))
            .unwrap();

        assert_eq!(
            harness
                .storage
                .get(&ctx, schema.id, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
        assert_eq!(
            harness
                .storage
                .index_scan(
                    &ctx,
                    schema.id,
                    index.id,
                    &KeyRange::Exact(Key(vec![Value::Text("Ada".to_string())])),
                )
                .unwrap()
                .next()
                .unwrap()
                .map(|stored| stored.row),
            Some(user_row(1, "Ada", true))
        );

        assert!(
            harness
                .buffer
                .page_count(heap_file_id(schema.storage_id))
                .unwrap()
                > 0
        );
        assert!(
            harness
                .buffer
                .page_count(primary_index_file_id(schema.storage_id))
                .unwrap()
                > 0
        );
        assert!(
            harness
                .buffer
                .page_count(secondary_index_file_id(index.storage_id))
                .unwrap()
                > 0
        );
        assert_eq!(
            harness.buffer.page_count(heap_file_id(schema.id)).unwrap(),
            0
        );
        assert_eq!(
            harness
                .buffer
                .page_count(primary_index_file_id(schema.id))
                .unwrap(),
            0
        );
        assert_eq!(
            harness
                .buffer
                .page_count(secondary_index_file_id(index.id))
                .unwrap(),
            0
        );
    }

    #[test]
    fn duplicate_insert_returns_unique_violation_without_replacing_row() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
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
    fn primary_key_null_is_rejected_by_storage_insert() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness
            .storage
            .create_table(&ctx, &nullable_primary_key_users_schema())
            .unwrap();

        let err = harness
            .storage
            .insert(&ctx, 1, user_row_null_id("Ada", true))
            .unwrap_err();

        assert_eq!(err.code, SqlState::NotNullViolation);
    }

    #[test]
    fn primary_key_null_is_rejected_by_storage_update() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness
            .storage
            .create_table(&ctx, &nullable_primary_key_users_schema())
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        let err = harness
            .storage
            .update(
                &ctx,
                1,
                &Key(vec![Value::Integer(1)]),
                user_row_null_id("Ada", true),
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::NotNullViolation);
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
        let ctx = StatementContext::new(1);
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
    fn set_table_primary_key_rebuilds_storage_identity() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let mut heap_schema = users_schema();
        heap_schema.primary_key.clear();
        harness.storage.create_table(&ctx, &heap_schema).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Grace", true))
            .unwrap();

        let mut pk_schema = heap_schema;
        pk_schema.primary_key = vec![0];
        harness
            .storage
            .validate_table_primary_key_change(&ctx, &pk_schema, u64::MAX)
            .unwrap();
        harness
            .storage
            .set_table_primary_key(&pk_schema, u64::MAX)
            .unwrap();

        assert_eq!(
            harness
                .storage
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
        let err = harness
            .storage
            .insert(&ctx, 1, user_row(1, "Duplicate", true))
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
    }

    #[test]
    fn set_table_primary_key_rebuilds_current_storage_generation() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let mut heap_schema = users_schema();
        heap_schema.storage_id = 42;
        heap_schema.primary_key.clear();
        harness.storage.create_table(&ctx, &heap_schema).unwrap();
        harness
            .storage
            .insert(&ctx, heap_schema.id, user_row(1, "Ada", true))
            .unwrap();

        let mut pk_schema = heap_schema.clone();
        pk_schema.primary_key = vec![0];
        harness
            .storage
            .validate_table_primary_key_change(&ctx, &pk_schema, u64::MAX)
            .unwrap();
        harness
            .storage
            .set_table_primary_key(&pk_schema, u64::MAX)
            .unwrap();

        assert_eq!(
            harness
                .storage
                .get(&ctx, pk_schema.id, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
        let err = harness
            .storage
            .insert(&ctx, pk_schema.id, user_row(1, "Duplicate", true))
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
    }

    #[test]
    fn logged_set_table_primary_key_emits_identity_index_fpis() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let mut heap_schema = users_schema();
        heap_schema.primary_key.clear();
        harness.storage.create_table(&ctx, &heap_schema).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Grace", true))
            .unwrap();

        let before = harness
            .wal
            .full_page_image_count(primary_index_file_id(heap_schema.storage_id));
        let mut pk_schema = heap_schema;
        pk_schema.primary_key = vec![0];
        harness
            .storage
            .set_table_primary_key_logged(&pk_schema, u64::MAX, 9)
            .unwrap();
        let after = harness
            .wal
            .full_page_image_count(primary_index_file_id(pk_schema.storage_id));

        assert!(
            after >= before + 2,
            "logged identity rebuild must emit at least root and metapage FPIs"
        );
    }

    #[test]
    fn set_table_primary_key_allows_same_key_across_non_hot_versions() {
        let harness = StorageHarness::new();
        let mut heap_schema = users_schema();
        heap_schema.primary_key.clear();
        let setup = StatementContext::new(1);
        harness.storage.create_table(&setup, &heap_schema).unwrap();
        harness
            .storage
            .create_index(&setup, &name_index(false), 0)
            .unwrap();
        harness
            .storage
            .insert(&setup, 1, user_row(1, "Ada", true))
            .unwrap();

        let mut scan = harness.storage.scan(&setup, 1).unwrap();
        let hidden_key = scan.next().unwrap().unwrap().key;
        assert!(scan.next().unwrap().is_none());

        let update = StatementContext::new(2);
        harness
            .storage
            .update(&update, 1, &hidden_key, user_row(1, "Lovelace", true))
            .unwrap();

        let mut pk_schema = heap_schema;
        pk_schema.primary_key = vec![0];
        harness
            .storage
            .validate_table_primary_key_change(&StatementContext::new(3), &pk_schema, 1)
            .unwrap();
        harness
            .storage
            .set_table_primary_key(&pk_schema, 1)
            .unwrap();

        assert_eq!(
            harness
                .storage
                .get(&StatementContext::new(4), 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Lovelace", true))
        );
    }

    #[test]
    fn set_table_primary_key_rejects_duplicate_live_rows() {
        let harness = StorageHarness::new();
        let mut heap_schema = users_schema();
        heap_schema.primary_key.clear();
        let ctx = StatementContext::new(1);
        harness.storage.create_table(&ctx, &heap_schema).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Grace", false))
            .unwrap();

        let mut pk_schema = heap_schema;
        pk_schema.primary_key = vec![0];
        let err = harness
            .storage
            .validate_table_primary_key_change(&ctx, &pk_schema, u64::MAX)
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
    }

    #[test]
    fn scan_returns_stored_row_identity_and_bounded_range() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
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
    fn reopened_engine_reads_rows_through_durable_index() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        // A second engine over the same buffer pool finds the row through the
        // durable on-disk index — there is no in-memory directory to rebuild.
        let reopened = PageBackedStorageEngine::open(
            harness.buffer.clone(),
            harness.wal.clone(),
            StorageMode::Normal,
        )
        .unwrap();
        reopened.install_schemas(vec![users_schema()]).unwrap();

        assert_eq!(
            reopened
                .get(&ctx, 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
    }

    #[test]
    fn aborted_insert_is_invisible_after_rollback() {
        // Status-based abort (`docs/specs/mvcc.md` §4 Decision 3, Milestone D1): the
        // inserted row is NOT physically removed by rollback; it is hidden by the
        // CLOG (here modelled via `mark_aborted`). A later reader (own writes
        // excluded — query under txn 0) does not see it. (Before D1 this test
        // asserted physical absence via buffer before-image undo; updated to assert
        // VISIBILITY, the new abort contract.)
        let harness = StorageHarness::new();
        harness
            .create_users_table(&StatementContext::new(0))
            .unwrap();
        let ctx = StatementContext::new(1);
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        harness.storage.rollback_txn(ctx.txn_id).unwrap();
        harness.buffer.rollback(ctx.txn_id).unwrap();
        harness.wal.mark_aborted(ctx.txn_id);

        // A reader other than the aborted txn sees nothing (the row is invisible).
        assert_eq!(
            harness
                .storage
                .get(&StatementContext::new(0), 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            None
        );
    }

    #[test]
    fn aborted_update_leaves_old_version_visible_and_rollback_restores_drop_metadata() {
        // An aborted UPDATE writes a new version (xmin = aborter) and stamps the old
        // version's xmax = aborter. Under status-based abort (`docs/specs/mvcc.md`
        // §4 Decision 3) neither is undone; with the aborter hidden by the CLOG, the
        // new version is invisible and the old version's xmax does not hide it, so a
        // reader still sees the OLD value. (Before D1 the buffer before-image
        // physically restored the page; updated to assert VISIBILITY.)
        //
        // DROP metadata rollback is unchanged: `drop_table` only flips the engine's
        // shadow `dropped` flag, which `rollback_txn` restores (this is engine DDL
        // metadata, not before-image page undo, so it survives D1).
        let harness = StorageHarness::new();
        harness
            .create_users_table(&StatementContext::new(0))
            .unwrap();
        let insert_ctx = StatementContext::new(1);
        harness
            .storage
            .insert(&insert_ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness.storage.commit_txn(insert_ctx.txn_id).unwrap();
        harness.buffer.commit(insert_ctx.txn_id).unwrap();

        let update_ctx = StatementContext::new(2);
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
        harness.wal.mark_aborted(update_ctx.txn_id);

        // A reader other than the aborter sees the original committed value.
        assert_eq!(
            harness
                .storage
                .get(&StatementContext::new(0), 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );

        let drop_ctx = StatementContext::new(3);
        harness.storage.drop_table(&drop_ctx, 1).unwrap();
        harness.storage.rollback_txn(drop_ctx.txn_id).unwrap();

        assert_eq!(
            harness
                .storage
                .get(&StatementContext::new(0), 1, &Key(vec![Value::Integer(1)]))
                .unwrap(),
            Some(user_row(1, "Ada", true))
        );
    }

    #[test]
    fn aborted_insert_on_new_page_keeps_the_page_and_hides_the_row() {
        // An INSERT that needs a fresh heap page, then aborts: under status-based
        // abort (`docs/specs/mvcc.md` §4 Decision 3, Milestone D1) the freshly
        // allocated page is NOT reclaimed — it stays resident (and would be replayed
        // by redo-all recovery), with its tuple hidden by the CLOG. (Before D1 the
        // buffer pool removed the new page on rollback; updated to assert the page
        // REMAINS and the row is invisible, matching the recovered state.)
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let large_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(single_page_capacity_text()),
                Value::Null,
            ],
        };
        harness.storage.insert(&ctx, 2, large_row).unwrap();
        harness.storage.commit_txn(ctx.txn_id).unwrap();
        harness.buffer.commit(ctx.txn_id).unwrap();

        let failed_ctx = StatementContext::new(2);
        // This row needs a second heap page (page 1 of file 2): the first page is
        // full of the committed large row.
        harness
            .storage
            .insert(&failed_ctx, 2, small_big_text_row(2))
            .unwrap();
        harness.storage.rollback_txn(failed_ctx.txn_id).unwrap();
        harness.buffer.rollback(failed_ctx.txn_id).unwrap();
        harness.wal.mark_aborted(failed_ctx.txn_id);

        // The aborted row is invisible to a reader other than the aborter.
        assert_eq!(
            harness
                .storage
                .get(&StatementContext::new(0), 2, &Key(vec![Value::Integer(2)]))
                .unwrap(),
            None
        );
        // The freshly allocated page (file 2, page 1) is still resident, not
        // reclaimed.
        assert!(
            harness
                .buffer
                .iter_pages()
                .unwrap()
                .any(|page| page.file_id == 2 && page.page_num == 1)
        );
    }

    #[test]
    fn insert_accepts_row_that_fills_single_page_payload_capacity() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(single_page_capacity_text()),
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
        let ctx = StatementContext::new(1);
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(single_page_capacity_text() + "x"),
                Value::Null,
            ],
        };

        let err = harness.storage.insert(&ctx, 2, row).unwrap_err();

        assert_eq!(err.code, SqlState::ProgramLimitExceeded);
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
        crate::page::insert_row(
            &mut page.0,
            &vec![0; PAGE_SIZE - crate::page::HEADER_LEN - crate::page::SLOT_LEN],
        )
        .unwrap();

        assert!(!crate::page::has_space_for(&page.0, 1).unwrap());
    }

    #[test]
    fn insert_allocates_new_page_when_first_page_has_no_next_slot_space() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let schema = big_text_schema();
        harness.storage.create_table(&ctx, &schema).unwrap();
        let large_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(single_page_capacity_text()),
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

    #[test]
    fn create_index_backfills_existing_rows() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Grace", true))
            .unwrap();

        // Build the index after the rows exist; backfill must pick them up.
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();

        let rows = collect(
            harness
                .storage
                .index_scan(&ctx, 1, 1, &name_eq("Ada"))
                .unwrap(),
        );
        assert_eq!(rows, vec![user_row(1, "Ada", true)]);
    }

    #[test]
    fn dml_keeps_secondary_index_in_sync() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();

        // Insert is reflected in the index.
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        assert_eq!(index_ids(&harness, "Ada"), vec![1]);

        // Updating the indexed column moves the entry.
        harness
            .storage
            .update(&ctx, 1, &pk(1), user_row(1, "Lovelace", true))
            .unwrap();
        assert!(index_ids(&harness, "Ada").is_empty());
        assert_eq!(index_ids(&harness, "Lovelace"), vec![1]);

        // Delete hides the row from the index scan. The entry is now *retained*
        // internally (MVCC delete stamps xmax in place; VACUUM reclaims it), but
        // the deleted version is invisible, so an index scan returns no id.
        harness.storage.delete(&ctx, 1, &pk(1)).unwrap();
        assert!(index_ids(&harness, "Lovelace").is_empty());
    }

    #[test]
    fn non_unique_index_returns_every_row_for_a_value() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Sam", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Sam", false))
            .unwrap();

        let mut ids = index_ids(&harness, "Sam");
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn index_scan_returns_a_range_in_index_order() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        for (id, name) in [(1, "Ada"), (2, "Cleo"), (3, "Bob")] {
            harness
                .storage
                .insert(&ctx, 1, user_row(id, name, true))
                .unwrap();
        }

        let rows = collect(
            harness
                .storage
                .index_scan(
                    &ctx,
                    1,
                    1,
                    &KeyRange::Range {
                        start: Bound::Included(Key(vec![Value::Text("Bob".to_string())])),
                        end: Bound::Unbounded,
                    },
                )
                .unwrap(),
        );
        let names: Vec<_> = rows.into_iter().map(row_name).collect();
        assert_eq!(names, vec!["Bob".to_string(), "Cleo".to_string()]);
    }

    #[test]
    fn unique_index_rejects_duplicate_value_on_insert() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(true), 0)
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        let err = harness
            .storage
            .insert(&ctx, 1, user_row(2, "Ada", false))
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
    }

    #[test]
    fn unique_index_backfill_rejects_duplicate_existing_values() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Ada", false))
            .unwrap();

        let err = harness
            .storage
            .create_index(&ctx, &name_index(true), 0)
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
    }

    #[test]
    fn unique_index_allows_multiple_nulls() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(true), 0)
            .unwrap();

        // name is nullable; SQL treats NULLs as distinct, so two are allowed.
        harness
            .storage
            .insert(&ctx, 1, user_row_null_name(1))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row_null_name(2))
            .unwrap();

        assert!(harness.storage.get(&ctx, 1, &pk(1)).unwrap().is_some());
        assert!(harness.storage.get(&ctx, 1, &pk(2)).unwrap().is_some());
    }

    #[test]
    fn secondary_scan_resolves_heap_tids_directly() {
        // Secondary entries now store the heap TID directly (not the primary key),
        // so a scan reads the heap at that TID with no primary-key indirection.
        // Updating a row keeps its indexed value but relocates the heap tuple to a
        // new TID; the secondary entry must follow to the new TID, and a point scan
        // must return the row's current contents — which only holds if the entry's
        // value is the heap TID, not the (unchanged) primary key.
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        // Update a non-indexed column (active true -> false); the name is unchanged
        // but the heap row relocates to a fresh slot.
        harness
            .storage
            .update(&ctx, 1, &pk(1), user_row(1, "Ada", false))
            .unwrap();

        let rows = collect(
            harness
                .storage
                .index_scan(&ctx, 1, 1, &name_eq("Ada"))
                .unwrap(),
        );
        // The scan resolves to the relocated tuple's current contents.
        assert_eq!(rows, vec![user_row(1, "Ada", false)]);
    }

    #[test]
    fn secondary_point_scan_returns_all_rows_for_a_value() {
        // A non-unique secondary point scan returns every row sharing the indexed
        // value, each resolved straight to its heap TID.
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        for id in [1, 2, 3] {
            harness
                .storage
                .insert(&ctx, 1, user_row(id, "Sam", id % 2 == 0))
                .unwrap();
        }

        let mut ids = index_ids(&harness, "Sam");
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn unique_index_keeps_and_scans_multiple_null_rows() {
        // Multiple rows whose indexed value is NULL coexist under a UNIQUE
        // secondary index (SQL NULLs are distinct), now disambiguated by their
        // differing heap TIDs rather than an embedded primary key. A scan of the
        // NULL key returns every such row.
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(true), 0)
            .unwrap();

        for id in [1, 2, 3] {
            harness
                .storage
                .insert(&ctx, 1, user_row_null_name(id))
                .unwrap();
        }

        // All three NULL-name rows are present and the unique index did not reject
        // them; scanning the NULL key returns all three.
        let rows = collect(
            harness
                .storage
                .index_scan(&ctx, 1, 1, &KeyRange::Exact(Key(vec![Value::Null])))
                .unwrap(),
        );
        let mut ids: Vec<i64> = rows
            .into_iter()
            .map(|row| match row.values[0] {
                Value::Integer(id) => id,
                ref other => panic!("expected integer id, got {other:?}"),
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn unique_index_rejects_duplicate_non_null_value() {
        // A duplicate non-NULL value under a UNIQUE secondary index is rejected by
        // the temporary presence-probe, even though the key no longer embeds the
        // primary key as a tiebreaker.
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(true), 0)
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        let err = harness
            .storage
            .insert(&ctx, 1, user_row(2, "Ada", false))
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
    }

    #[test]
    fn dropped_index_is_not_maintained_but_existing_entries_remain_scannable() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(true), 0)
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        harness.storage.drop_index(&ctx, 1).unwrap();

        // The dropped unique index no longer rejects the duplicate value...
        harness
            .storage
            .insert(&ctx, 1, user_row(2, "Ada", false))
            .unwrap();
        // ...and is no longer maintained, but the retained physical entries remain
        // scan-readable for statements planned before the catalog drop.
        let rows = collect(
            harness
                .storage
                .index_scan(&ctx, 1, 1, &name_eq("Ada"))
                .unwrap(),
        );
        assert_eq!(rows, vec![user_row(1, "Ada", true)]);

        let rows = collect(
            harness
                .storage
                .index_scan(&ctx, 1, 1, &name_eq("Missing"))
                .unwrap(),
        );
        assert!(rows.is_empty());
    }

    #[test]
    fn rollback_removes_a_created_index() {
        let harness = StorageHarness::new();
        harness
            .create_users_table(&StatementContext::new(0))
            .unwrap();
        let ctx = StatementContext::new(5);
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();

        harness.storage.rollback_txn(ctx.txn_id).unwrap();
        harness.buffer.rollback(ctx.txn_id).unwrap();

        let err = harness
            .storage
            .index_scan(&StatementContext::new(6), 1, 1, &KeyRange::All)
            .err()
            .expect("rolled-back index should not be scannable");
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn drop_table_cascades_to_its_secondary_indexes() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        harness.create_users_table(&ctx).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        harness
            .storage
            .insert(&ctx, 1, user_row(1, "Ada", true))
            .unwrap();

        // Dropping the table cascades to its index state; the table and its
        // index are both gone.
        harness.storage.drop_table(&ctx, 1).unwrap();

        let err = harness
            .storage
            .index_scan(&ctx, 1, 1, &name_eq("Ada"))
            .err()
            .expect("a dropped table's index should not be scannable");
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn drop_table_cascades_to_hidden_toast_relation_metadata() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let mut base = users_schema();
        base.toast_table_id = Some(2);
        let mut toast = users_schema();
        toast.id = 2;
        toast.storage_id = 2;
        toast.name = "\0toast_1".to_string();
        toast.relation_kind = RelationKind::Toast { base_table: 1 };
        harness.storage.install_schemas(vec![base, toast]).unwrap();

        harness.storage.drop_table(&ctx, 1).unwrap();

        let err = harness
            .storage
            .get(&ctx, 2, &Key(vec![Value::Integer(1)]))
            .expect_err("a dropped table's hidden TOAST relation should not be readable");
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn recovery_toast_metadata_update_is_wal_free_and_drives_drop_cascade() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let base = users_schema();
        let mut toast_relation = users_schema();
        toast_relation.id = 2;
        toast_relation.storage_id = 2;
        toast_relation.name = "\0toast_1".to_string();
        toast_relation.relation_kind = RelationKind::Toast { base_table: 1 };
        harness
            .storage
            .install_schemas(vec![base.clone(), toast_relation])
            .unwrap();

        let mut updated = base;
        updated.toast_table_id = Some(2);
        updated.toast.mode = ToastMode::Aggressive;
        updated.toast.min_value_size = 512;
        updated.toast.compression = ToastCompression::Zstd;
        harness
            .storage
            .apply_set_table_toast_metadata(updated)
            .unwrap();
        assert_eq!(
            harness.wal.record_count(),
            0,
            "recovery metadata apply must not append WAL"
        );

        harness.storage.apply_drop_table(1).unwrap();
        assert_eq!(
            harness.wal.record_count(),
            0,
            "recovery drop apply must remain WAL-free"
        );
        let err = harness
            .storage
            .get(&ctx, 2, &Key(vec![Value::Integer(1)]))
            .expect_err("updated TOAST link should cascade-drop the hidden relation");
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[test]
    fn toast_value_id_allocator_starts_at_one_for_empty_relation() {
        let harness = StorageHarness::new();
        let (base, toast) = base_and_toast_schema();
        harness.storage.install_schemas(vec![base, toast]).unwrap();

        assert_eq!(harness.storage.alloc_toast_value_id(2).unwrap(), 1);
    }

    #[test]
    fn toast_value_id_allocator_is_monotonic_in_memory() {
        let harness = StorageHarness::new();
        let (base, toast) = base_and_toast_schema();
        harness.storage.install_schemas(vec![base, toast]).unwrap();

        assert_eq!(harness.storage.alloc_toast_value_id(2).unwrap(), 1);
        assert_eq!(harness.storage.alloc_toast_value_id(2).unwrap(), 2);
        assert_eq!(harness.storage.alloc_toast_value_id(2).unwrap(), 3);
    }

    #[test]
    fn toast_value_id_allocator_seeds_from_physical_chunk_rows() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .insert(&ctx, 2, toast_chunk_row(4, 0, b"chunk-a"))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 2, toast_chunk_row(9, 0, b"chunk-b"))
            .unwrap();

        let reopened = PageBackedStorageEngine::open(
            harness.buffer.clone(),
            harness.wal.clone(),
            StorageMode::Normal,
        )
        .unwrap();
        let (base, toast) = base_and_toast_schema();
        reopened.install_schemas(vec![base, toast]).unwrap();

        assert_eq!(reopened.alloc_toast_value_id(2).unwrap(), 10);
    }

    #[test]
    fn toast_value_id_allocator_reseeds_when_recovery_enters_normal_mode() {
        let buffer = Arc::new(MemoryBufferPool::empty(64));
        let wal = Arc::new(CountingWal::default());
        let recovery =
            PageBackedStorageEngine::open(buffer.clone(), wal, StorageMode::Recovery).unwrap();
        let (base, toast) = base_and_toast_schema();
        recovery.install_schemas(vec![base, toast]).unwrap();

        redo_toast_chunk(&recovery, 19, 88, 1, b"redone").unwrap();
        recovery.set_mode(StorageMode::Normal).unwrap();

        assert_eq!(recovery.alloc_toast_value_id(2).unwrap(), 20);
    }

    #[test]
    fn toast_value_id_allocator_keeps_recovery_seed_if_row_removed_before_allocation() {
        let buffer = Arc::new(MemoryBufferPool::empty(64));
        let wal = Arc::new(CountingWal::default());
        let recovery = PageBackedStorageEngine::open(buffer, wal, StorageMode::Recovery).unwrap();
        let (base, toast) = base_and_toast_schema();
        recovery.install_schemas(vec![base, toast]).unwrap();

        redo_toast_chunk(&recovery, 19, 88, 1, b"redone-then-removed").unwrap();
        recovery.set_mode(StorageMode::Normal).unwrap();
        {
            let mut guard = recovery.buffer_pool.fetch_for_redo(2, 0).unwrap();
            apply_physical_redo(
                guard.data_mut(),
                3,
                &WalRecordKind::HeapDelete {
                    file_id: 2,
                    page_num: 0,
                    slot: 0,
                },
            )
            .unwrap();
        }

        assert_eq!(recovery.alloc_toast_value_id(2).unwrap(), 20);
    }

    #[test]
    fn toast_value_id_allocator_seeds_from_aborted_physical_chunk_rows() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(77);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .insert(&ctx, 2, toast_chunk_row(12, 0, b"aborted"))
            .unwrap();
        harness.wal.mark_aborted(ctx.txn_id);

        let reopened = PageBackedStorageEngine::open(
            harness.buffer.clone(),
            harness.wal.clone(),
            StorageMode::Normal,
        )
        .unwrap();
        let (base, toast) = base_and_toast_schema();
        reopened.install_schemas(vec![base, toast]).unwrap();

        assert_eq!(reopened.alloc_toast_value_id(2).unwrap(), 13);
    }

    #[test]
    fn toast_value_id_allocator_rejects_ids_past_i64_max() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .insert(&ctx, 2, toast_chunk_row(i64::MAX, 0, b"last"))
            .unwrap();

        let reopened = PageBackedStorageEngine::open(
            harness.buffer.clone(),
            harness.wal.clone(),
            StorageMode::Normal,
        )
        .unwrap();
        let (base, toast) = base_and_toast_schema();
        reopened.install_schemas(vec![base, toast]).unwrap();

        let err = reopened.alloc_toast_value_id(2).unwrap_err();
        assert_eq!(err.code, SqlState::ProgramLimitExceeded);
    }

    #[test]
    fn write_toast_stream_creates_expected_chunks() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let relations = harness
            .storage
            .capture_pagebacked_relation_snapshot()
            .unwrap();
        let raw = vec![b'x'; TOAST_CHUNK_PAYLOAD * 2 + 37];
        let stream =
            build_external_stream(compress::CODEC_NONE, None, crc32fast::hash(&raw), &raw).unwrap();

        let pointer = harness
            .storage
            .write_toast_stream(
                &ctx,
                &relations,
                &base,
                raw.len() as u32,
                compress::CODEC_NONE,
                &stream,
            )
            .unwrap();

        assert_eq!(pointer.value_id, 1);
        assert_eq!(pointer.raw_len, raw.len() as u32);
        assert_eq!(pointer.stored_len, stream.len() as u32);
        assert_eq!(pointer.codec, compress::CODEC_NONE);
        assert_eq!(
            visible_toast_chunk_sizes(&harness, &ctx, pointer.value_id),
            vec![TOAST_CHUNK_PAYLOAD, TOAST_CHUNK_PAYLOAD, 41]
        );
    }

    #[test]
    fn read_toast_stream_reconstructs_exact_stream() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let relations = harness
            .storage
            .capture_pagebacked_relation_snapshot()
            .unwrap();
        let raw = b"external logical bytes";
        let stream = build_external_stream(
            compress::CODEC_ZSTD,
            None,
            crc32fast::hash(raw),
            b"zstd-payload",
        )
        .unwrap();
        let pointer = harness
            .storage
            .write_toast_stream(
                &ctx,
                &relations,
                &base,
                raw.len() as u32,
                compress::CODEC_ZSTD,
                &stream,
            )
            .unwrap();

        let read = harness
            .storage
            .read_toast_stream(&ctx, &relations, &base, &pointer)
            .unwrap();

        assert_eq!(read, stream);
    }

    #[test]
    fn read_toast_stream_rejects_missing_sequence() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .insert(&ctx, 2, toast_chunk_row(1, 0, b"abcd"))
            .unwrap();
        harness
            .storage
            .insert(&ctx, 2, toast_chunk_row(1, 2, b"efgh"))
            .unwrap();
        let relations = harness
            .storage
            .capture_pagebacked_relation_snapshot()
            .unwrap();
        let pointer = crate::codec::ToastPointer {
            value_id: 1,
            raw_len: 4,
            stored_len: 8,
            codec: compress::CODEC_NONE,
        };

        let err = harness
            .storage
            .read_toast_stream(&ctx, &relations, &base, &pointer)
            .unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("missing, duplicate, or out of order"));
    }

    #[test]
    fn read_toast_stream_rejects_duplicate_sequence() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let row_id = harness
            .storage
            .insert(&ctx, 2, toast_chunk_row(1, 0, b"abcd"))
            .unwrap();
        let btree = BTree::new(
            harness.storage.buffer_pool.as_ref(),
            harness.storage.wal.as_ref(),
            primary_index_file_id(2),
            harness.storage.compression.as_ref(),
        );
        btree
            .insert(
                ctx.txn_id,
                &Key(vec![Value::Integer(1), Value::Integer(0)]),
                &RowLocation {
                    file_id: 2,
                    page_num: row_id.page_num,
                    slot_num: row_id.slot_num,
                },
            )
            .unwrap();
        let relations = harness
            .storage
            .capture_pagebacked_relation_snapshot()
            .unwrap();
        let pointer = crate::codec::ToastPointer {
            value_id: 1,
            raw_len: 4,
            stored_len: 8,
            codec: compress::CODEC_NONE,
        };

        let err = harness
            .storage
            .read_toast_stream(&ctx, &relations, &base, &pointer)
            .unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("duplicate seq"));
    }

    #[test]
    fn read_toast_stream_rejects_wrong_stored_length() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let relations = harness
            .storage
            .capture_pagebacked_relation_snapshot()
            .unwrap();
        let raw = b"raw";
        let stream =
            build_external_stream(compress::CODEC_NONE, None, crc32fast::hash(raw), raw).unwrap();
        let mut pointer = harness
            .storage
            .write_toast_stream(
                &ctx,
                &relations,
                &base,
                raw.len() as u32,
                compress::CODEC_NONE,
                &stream,
            )
            .unwrap();
        pointer.stored_len += 1;

        let err = harness
            .storage
            .read_toast_stream(&ctx, &relations, &base, &pointer)
            .unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("expected"));
    }

    #[test]
    fn write_toast_stream_requires_hidden_toast_relation() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let base = users_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        let relations = harness
            .storage
            .capture_pagebacked_relation_snapshot()
            .unwrap();
        let stream = build_external_stream(compress::CODEC_NONE, None, 0, b"raw").unwrap();

        let err = harness
            .storage
            .write_toast_stream(&ctx, &relations, &base, 3, compress::CODEC_NONE, &stream)
            .unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("hidden TOAST relation"));
    }

    #[test]
    fn prepare_row_under_target_with_no_eligible_values_stays_plain() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(b"small".to_vec())],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        assert_eq!(
            values[1],
            DecodedPhysicalValue::Value(Value::Bytes(b"small".to_vec()))
        );
        assert!(visible_toast_chunk_sizes(&harness, &ctx, 1).is_empty());
    }

    #[test]
    fn prepare_hidden_toast_relation_does_not_recurse() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let data = vec![42; 3000];
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Integer(0),
                Value::Bytes(data.clone()),
            ],
        };

        let values = prepare_physical_row(&harness, &ctx, &toast, &row);

        assert_eq!(values[2], DecodedPhysicalValue::Value(Value::Bytes(data)));
    }

    #[test]
    fn prepare_legacy_table_without_toast_relation_stays_plain() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, _) = base_and_toast_schema();
        base.toast_table_id = None;
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::Zstd;
        harness.storage.create_table(&ctx, &base).unwrap();
        let raw = "a".repeat(1500);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(raw.clone()),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        assert_eq!(values[1], DecodedPhysicalValue::Value(Value::Text(raw)));
    }

    #[test]
    fn prepare_medium_compressible_text_under_target_becomes_inline_compressed() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::Zstd;
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = "a".repeat(1500);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(raw.clone()),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        match &values[1] {
            DecodedPhysicalValue::Compressed {
                codec,
                dict_id,
                raw_len,
                raw_crc32,
                ..
            } => {
                assert_eq!(*codec, compress::CODEC_ZSTD);
                assert_eq!(*dict_id, None);
                assert_eq!(*raw_len, raw.len() as u32);
                assert_eq!(*raw_crc32, crc32fast::hash(raw.as_bytes()));
            }
            other => panic!("expected inline compressed text, got {other:?}"),
        }
    }

    #[test]
    fn prepare_default_auto_uses_1024_min_value_size() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = "a".repeat(512);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(raw.clone()),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        assert_eq!(values[1], DecodedPhysicalValue::Value(Value::Text(raw)));
    }

    #[test]
    fn prepare_aggressive_uses_256_min_value_size() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.mode = ToastMode::Aggressive;
        base.toast.min_value_size = ToastOptions::AGGRESSIVE_TOAST_MIN_VALUE_SIZE;
        base.toast.compression = ToastCompression::Zstd;
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = "a".repeat(512);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(raw),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        assert!(matches!(
            values[1],
            DecodedPhysicalValue::Compressed {
                codec: compress::CODEC_ZSTD,
                ..
            }
        ));
    }

    #[test]
    fn prepare_incompressible_large_bytea_externalizes_raw_stream() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = entropy_bytes(9000);
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(raw.clone())],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        let pointer = match &values[1] {
            DecodedPhysicalValue::External { pointer, .. } => pointer,
            other => panic!("expected external bytea, got {other:?}"),
        };
        assert_eq!(pointer.codec, compress::CODEC_NONE);
        assert_eq!(pointer.raw_len, raw.len() as u32);
        let stream = harness
            .storage
            .read_toast_stream(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &base,
                pointer,
            )
            .unwrap();
        let (dict_id, raw_crc32, payload) =
            crate::toast::parse_external_stream(pointer.codec, &stream).unwrap();
        assert_eq!(dict_id, None);
        assert_eq!(raw_crc32, crc32fast::hash(&raw));
        assert_eq!(payload, raw.as_slice());
    }

    #[test]
    fn prepare_zstd_dict_inline_records_dictionary_id() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::ZstdDict;
        base.toast.active_dict_id = Some(7);
        register_test_dictionary(&harness, 7);
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = dict_value(99, 1800);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(String::from_utf8(raw.clone()).unwrap()),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        match &values[1] {
            DecodedPhysicalValue::Compressed {
                codec,
                dict_id,
                raw_len,
                raw_crc32,
                ..
            } => {
                assert_eq!(*codec, compress::CODEC_ZSTD_DICT);
                assert_eq!(*dict_id, Some(7));
                assert_eq!(*raw_len, raw.len() as u32);
                assert_eq!(*raw_crc32, crc32fast::hash(&raw));
            }
            other => panic!("expected zstd-dict inline value, got {other:?}"),
        }
    }

    #[test]
    fn prepare_zstd_dict_external_stream_records_dictionary_id_and_crc() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = dict_external_base_and_toast_schema();
        base.toast.compression = ToastCompression::ZstdDict;
        base.toast.active_dict_id = Some(7);
        register_test_dictionary(&harness, 7);
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = dict_value(77, 12_000);
        let mut values = vec![Value::Integer(1)];
        values.extend((0..40).map(Value::Integer));
        values.push(Value::Text(String::from_utf8(raw.clone()).unwrap()));
        let row = Row { values };

        let physical = prepare_physical_row(&harness, &ctx, &base, &row);

        let pointer = match physical.last().unwrap() {
            DecodedPhysicalValue::External { pointer, .. } => pointer,
            other => panic!("expected external zstd-dict text, got {other:?}"),
        };
        assert_eq!(pointer.codec, compress::CODEC_ZSTD_DICT);
        let stream = harness
            .storage
            .read_toast_stream(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &base,
                pointer,
            )
            .unwrap();
        let (dict_id, raw_crc32, payload) =
            crate::toast::parse_external_stream(pointer.codec, &stream).unwrap();
        assert_eq!(dict_id, Some(7));
        assert_eq!(raw_crc32, crc32fast::hash(&raw));
        assert!(!payload.is_empty());
    }

    #[test]
    fn sample_toast_values_skips_invisible_tuple_before_varlena_decode() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let sample_ctx = StatementContext::new(2);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("discarded".to_string()),
                Value::Boolean(true),
                Value::Null,
            ],
        };
        harness.storage.insert(&insert_ctx, 1, row).unwrap();
        harness.wal.mark_aborted(insert_ctx.txn_id);

        {
            let mut guard = harness
                .storage
                .buffer_pool
                .fetch_for_redo(base.id, 0)
                .unwrap();
            let row_bytes = crate::page::read_row(guard.data(), 0).unwrap().unwrap();
            let row_start = guard
                .data()
                .windows(row_bytes.len())
                .position(|window| window == row_bytes.as_slice())
                .expect("inserted row bytes are on page");
            let text_len_offset =
                row_start + 1 + V2_MVCC_HEADER_LEN + null_bitmap_len(base.columns.len()) + 8;
            guard.data_mut()[text_len_offset + 3] |= 0xC0;
            crate::page::set_page_lsn(guard.data_mut(), 99);
        }

        let samples = harness
            .storage
            .sample_toast_values(&sample_ctx, &base, 16, 1024)
            .unwrap();
        assert!(samples.is_empty());
    }

    #[test]
    fn sample_toast_values_observes_statement_cancellation() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let sample_ctx = StatementContext::new(2);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        harness
            .storage
            .insert(
                &insert_ctx,
                base.id,
                Row {
                    values: vec![
                        Value::Integer(1),
                        Value::Text("sample".to_string()),
                        Value::Boolean(true),
                        Value::Null,
                    ],
                },
            )
            .unwrap();
        sample_ctx.cancel.request(CancelReason::StatementTimeout);

        let err = harness
            .storage
            .sample_toast_values(&sample_ctx, &base, 16, 1024)
            .unwrap_err();
        assert_eq!(err.code, SqlState::QueryCanceled);
    }

    #[test]
    fn sample_toast_values_skips_oversized_inline_compressed_before_decompress() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let sample_ctx = StatementContext::new(2);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let row_bytes = encode_row_v3_prepared(
            &base,
            &MvccHeader::fresh(ctx.txn_id, 0),
            &[
                PreparedColumnValue::Value(Value::Integer(1)),
                PreparedColumnValue::Varlena(VarlenaPhysical::Compressed {
                    codec: compress::CODEC_ZSTD,
                    dict_id: None,
                    raw_len: 2048,
                    raw_crc32: 0,
                    payload: b"not-a-zstd-payload".to_vec(),
                }),
                PreparedColumnValue::Value(Value::Boolean(true)),
                PreparedColumnValue::Null,
            ],
        )
        .unwrap();
        seed_parent_tuple(
            &harness,
            &ctx,
            &base,
            &row_bytes,
            &Key(vec![Value::Integer(1)]),
        );

        let samples = harness
            .storage
            .sample_toast_values(&sample_ctx, &base, 16, 1024)
            .unwrap();

        assert!(samples.is_empty());
    }

    #[test]
    fn sample_toast_values_skips_oversized_external_before_reading_chunks() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let sample_ctx = StatementContext::new(2);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let row_bytes = encode_row_v3_prepared(
            &base,
            &MvccHeader::fresh(ctx.txn_id, 0),
            &[
                PreparedColumnValue::Value(Value::Integer(1)),
                PreparedColumnValue::Varlena(VarlenaPhysical::External(ToastPointer {
                    value_id: 1,
                    raw_len: 2048,
                    stored_len: 4,
                    codec: compress::CODEC_NONE,
                })),
                PreparedColumnValue::Value(Value::Boolean(true)),
                PreparedColumnValue::Null,
            ],
        )
        .unwrap();
        seed_parent_tuple(
            &harness,
            &ctx,
            &base,
            &row_bytes,
            &Key(vec![Value::Integer(1)]),
        );

        let samples = harness
            .storage
            .sample_toast_values(&sample_ctx, &base, 16, 1024)
            .unwrap();

        assert!(samples.is_empty());
    }

    #[test]
    fn prepare_externalizes_largest_value_first() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = two_bytea_base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let smaller = entropy_bytes(3000);
        let larger = entropy_bytes(6000);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Bytes(smaller),
                Value::Bytes(larger),
            ],
        };

        let values = prepare_physical_row(&harness, &ctx, &base, &row);

        let smaller_pointer = match &values[1] {
            DecodedPhysicalValue::External { pointer, .. } => pointer,
            other => panic!("expected smaller value externalized, got {other:?}"),
        };
        let larger_pointer = match &values[2] {
            DecodedPhysicalValue::External { pointer, .. } => pointer,
            other => panic!("expected larger value externalized, got {other:?}"),
        };
        assert_eq!(larger_pointer.value_id, 1);
        assert_eq!(smaller_pointer.value_id, 2);
    }

    #[test]
    fn prepare_non_toastable_row_over_page_limit_is_rejected() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let schema = integer_heavy_schema(1100);
        harness.storage.create_table(&ctx, &schema).unwrap();
        let row = Row {
            values: (0..1100).map(Value::Integer).collect(),
        };

        let err = harness
            .storage
            .prepare_row_for_storage(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &schema,
                &MvccHeader::fresh(ctx.txn_id, 0),
                &row,
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::ProgramLimitExceeded);
    }

    #[test]
    fn prepare_toast_off_rejects_row_that_requires_externalization() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = bytea_base_and_toast_schema();
        base.toast.mode = ToastMode::Off;
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9000))],
        };

        let err = harness
            .storage
            .prepare_row_for_storage(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &base,
                &MvccHeader::fresh(ctx.txn_id, 0),
                &row,
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::ProgramLimitExceeded);
        assert!(visible_toast_chunk_sizes(&harness, &ctx, 1).is_empty());
    }

    #[test]
    fn prepare_rejects_after_all_candidates_externalized_without_chunks() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = wide_bytea_base_and_toast_schema(1100);
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let mut values: Vec<Value> = (0..1100).map(Value::Integer).collect();
        values.push(Value::Bytes(entropy_bytes(9000)));
        let row = Row { values };

        let err = harness
            .storage
            .prepare_row_for_storage(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &base,
                &MvccHeader::fresh(ctx.txn_id, 0),
                &row,
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::ProgramLimitExceeded);
        assert!(visible_toast_chunk_sizes(&harness, &ctx, 1).is_empty());
    }

    #[test]
    fn prepare_oversized_indexed_text_rejects_before_writing_chunks() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("x".repeat(PAGE_SIZE)),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let err = harness
            .storage
            .prepare_row_for_storage(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &base,
                &MvccHeader::fresh(ctx.txn_id, 0),
                &row,
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::ProgramLimitExceeded);
        assert!(visible_toast_chunk_sizes(&harness, &ctx, 1).is_empty());
    }

    #[test]
    fn read_materializes_inline_compressed_text_for_get_scan_and_index_scan() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::Zstd;
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        let name = "read-materialized-name-".repeat(80);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(name.clone()),
                Value::Boolean(true),
                Value::Null,
            ],
        };
        let row_bytes = harness
            .storage
            .prepare_row_for_storage(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &base,
                &MvccHeader::fresh(ctx.txn_id, 0),
                &row,
            )
            .unwrap();
        assert!(matches!(
            decode_physical_row(&base, &row_bytes).unwrap().values[1],
            DecodedPhysicalValue::Compressed { .. }
        ));
        let location = seed_parent_tuple(&harness, &ctx, &base, &row_bytes, &pk(1));
        let name_index_schema = name_index(false);
        seed_secondary_entry(
            &harness,
            &ctx,
            &name_index_schema,
            &Key(vec![Value::Text(name.clone())]),
            &location,
        );

        assert_eq!(
            harness.storage.get(&ctx, 1, &pk(1)).unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            collect(harness.storage.scan_range(&ctx, 1, &KeyRange::All).unwrap()),
            vec![row.clone()]
        );
        assert_eq!(
            collect(
                harness
                    .storage
                    .index_scan(&ctx, 1, name_index(false).id, &name_eq(&name))
                    .unwrap()
            ),
            vec![row]
        );
    }

    #[test]
    fn read_materializes_external_bytea_for_get_and_scan() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = entropy_bytes(9000);
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(raw)],
        };
        let row_bytes = harness
            .storage
            .prepare_row_for_storage(
                &ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                &base,
                &MvccHeader::fresh(ctx.txn_id, 0),
                &row,
            )
            .unwrap();
        assert!(matches!(
            decode_physical_row(&base, &row_bytes).unwrap().values[1],
            DecodedPhysicalValue::External { .. }
        ));
        seed_parent_tuple(&harness, &ctx, &base, &row_bytes, &pk(1));

        assert_eq!(
            harness.storage.get(&ctx, 1, &pk(1)).unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            collect(harness.storage.scan_range(&ctx, 1, &KeyRange::All).unwrap()),
            vec![row]
        );
    }

    #[test]
    fn read_visible_external_value_with_missing_chunks_errors() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let row_bytes = external_name_parent_bytes(&base, ctx.txn_id, 1, 99);
        seed_parent_tuple(&harness, &ctx, &base, &row_bytes, &pk(1));

        let err = harness.storage.get(&ctx, 1, &pk(1)).unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("expected"));
    }

    #[test]
    fn read_skips_invisible_external_value_without_touching_missing_chunks() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(10);
        let read_ctx = StatementContext::new(20);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let row_bytes = external_name_parent_bytes(&base, insert_ctx.txn_id, 1, 99);
        seed_parent_tuple(&harness, &insert_ctx, &base, &row_bytes, &pk(1));
        harness.wal.mark_aborted(insert_ctx.txn_id);

        assert_eq!(harness.storage.get(&read_ctx, 1, &pk(1)).unwrap(), None);
        assert!(
            collect(
                harness
                    .storage
                    .scan_range(&read_ctx, 1, &KeyRange::All)
                    .unwrap()
            )
            .is_empty()
        );
    }

    #[test]
    fn read_rejects_inline_crc_mismatch() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = b"crc-checked inline text".repeat(80);
        let payload = compress::compress_value_zstd(&raw).unwrap();
        let row_bytes = encode_row_v3_prepared(
            &base,
            &MvccHeader::fresh(ctx.txn_id, 0),
            &[
                PreparedColumnValue::Value(Value::Integer(1)),
                PreparedColumnValue::Varlena(VarlenaPhysical::Compressed {
                    codec: compress::CODEC_ZSTD,
                    dict_id: None,
                    raw_len: raw.len() as u32,
                    raw_crc32: crc32fast::hash(&raw) ^ 1,
                    payload,
                }),
                PreparedColumnValue::Value(Value::Boolean(true)),
                PreparedColumnValue::Null,
            ],
        )
        .unwrap();
        seed_parent_tuple(&harness, &ctx, &base, &row_bytes, &pk(1));

        let err = harness.storage.get(&ctx, 1, &pk(1)).unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("CRC32"));
    }

    #[test]
    fn insert_large_text_writes_toast_and_reads_logical_value() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::None;
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();
        let name = "insert-large-text-name-".repeat(140);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(name.clone()),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        harness.storage.insert(&ctx, 1, row.clone()).unwrap();

        assert_eq!(
            harness.storage.get(&ctx, 1, &pk(1)).unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            collect(harness.storage.scan_range(&ctx, 1, &KeyRange::All).unwrap()),
            vec![row.clone()]
        );
        assert_eq!(
            collect(
                harness
                    .storage
                    .index_scan(&ctx, 1, name_index(false).id, &name_eq(&name))
                    .unwrap()
            ),
            vec![row]
        );
        assert_eq!(
            external_value_ids_for_key(&harness, &base, &pk(1), 1),
            vec![1]
        );
        assert!(!visible_toast_chunk_sizes(&harness, &ctx, 1).is_empty());
    }

    #[test]
    fn insert_large_bytea_round_trips() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9000))],
        };

        harness.storage.insert(&ctx, 1, row.clone()).unwrap();

        assert_eq!(harness.storage.get(&ctx, 1, &pk(1)).unwrap(), Some(row));
        assert_eq!(
            external_value_ids_for_key(&harness, &base, &pk(1), 1),
            vec![1]
        );
    }

    #[test]
    fn primary_key_conflict_ignores_toast_physical_pointer() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        harness
            .storage
            .insert(
                &ctx,
                1,
                Row {
                    values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9000))],
                },
            )
            .unwrap();

        let err = harness
            .storage
            .insert(
                &ctx,
                1,
                Row {
                    values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(16))],
                },
            )
            .unwrap_err();

        assert_eq!(err.code, SqlState::UniqueViolation);
    }

    #[test]
    fn create_index_backfills_toasted_logical_values() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::None;
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let name = "create-index-toasted-name-".repeat(120);
        let row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text(name.clone()),
                Value::Boolean(true),
                Value::Null,
            ],
        };
        harness.storage.insert(&ctx, 1, row.clone()).unwrap();

        harness
            .storage
            .create_index(&ctx, &name_index(false), 0)
            .unwrap();

        assert_eq!(
            collect(
                harness
                    .storage
                    .index_scan(&ctx, 1, name_index(false).id, &name_eq(&name))
                    .unwrap()
            ),
            vec![row]
        );
    }

    #[test]
    fn create_index_skips_aborted_toasted_rows_before_detoast() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let index_ctx = StatementContext::new(2);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::None;
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let name = "aborted-toasted-name-".repeat(120);
        harness
            .storage
            .insert(
                &insert_ctx,
                1,
                Row {
                    values: vec![
                        Value::Integer(1),
                        Value::Text(name.clone()),
                        Value::Boolean(true),
                        Value::Null,
                    ],
                },
            )
            .unwrap();
        harness.wal.mark_aborted(insert_ctx.txn_id);

        harness
            .storage
            .create_index(&index_ctx, &name_index(false), 0)
            .unwrap();

        assert!(
            collect(
                harness
                    .storage
                    .index_scan(&index_ctx, 1, name_index(false).id, &name_eq(&name))
                    .unwrap()
            )
            .is_empty()
        );
    }

    #[test]
    fn update_toasted_value_creates_new_value_id() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let update_ctx = StatementContext::new(2);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let first = Row {
            values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9000))],
        };
        let second = Row {
            values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9500))],
        };
        harness.storage.insert(&insert_ctx, 1, first).unwrap();

        assert!(
            harness
                .storage
                .update(&update_ctx, 1, &pk(1), second.clone())
                .unwrap()
        );

        assert_eq!(
            harness.storage.get(&update_ctx, 1, &pk(1)).unwrap(),
            Some(second)
        );
        assert_eq!(
            external_value_ids_for_key(&harness, &base, &pk(1), 1),
            vec![1, 2]
        );
    }

    #[test]
    fn update_non_toast_column_retoasts_owned_value_and_old_snapshot_reads_old_value() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let update_ctx = StatementContext::new(2);
        let old_snapshot_ctx = StatementContext::with_snapshot(
            10,
            Arc::new(common::Snapshot {
                xmin: 1,
                xmax: 2,
                xip: Vec::new(),
            }),
        );
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::None;
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let old_note = "old toasted note ".repeat(600);
        let new_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(false),
                Value::Text(old_note.clone()),
            ],
        };
        let old_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(true),
                Value::Text(old_note),
            ],
        };
        harness
            .storage
            .insert(&insert_ctx, 1, old_row.clone())
            .unwrap();

        assert!(
            harness
                .storage
                .update(&update_ctx, 1, &pk(1), new_row.clone())
                .unwrap()
        );

        assert_eq!(
            harness.storage.get(&update_ctx, 1, &pk(1)).unwrap(),
            Some(new_row)
        );
        assert_eq!(
            harness.storage.get(&old_snapshot_ctx, 1, &pk(1)).unwrap(),
            Some(old_row)
        );
        assert_eq!(
            external_value_ids_for_key(&harness, &base, &pk(1), 3),
            vec![1, 2]
        );
    }

    #[test]
    fn update_inline_toastable_text_uses_hot() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let update_ctx = StatementContext::new(2);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.compression = ToastCompression::None;
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        harness
            .storage
            .create_index(&insert_ctx, &name_index(false), 0)
            .unwrap();
        let old_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(true),
                Value::Text("inline note".to_string()),
            ],
        };
        let new_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(false),
                Value::Text("inline note".to_string()),
            ],
        };
        harness.storage.insert(&insert_ctx, 1, old_row).unwrap();

        assert!(
            harness
                .storage
                .update(&update_ctx, 1, &pk(1), new_row.clone())
                .unwrap()
        );

        let pk_tids = pk_index_tids_for_key(&harness, &base, &pk(1));
        assert_eq!(pk_tids.len(), 1, "HOT update should not add a PK entry");
        assert_eq!(
            secondary_index_tids_for_text(&harness, &name_index(false), "Ada").len(),
            1,
            "HOT update should not add a secondary index entry"
        );
        let root = physical_row_at(&harness, &base, pk_tids[0]);
        assert_ne!(root.header.infomask & HOT_UPDATED, 0);
        let successor = physical_row_at(
            &harness,
            &base,
            RowLocation {
                file_id: base.id,
                page_num: root.header.t_ctid.0,
                slot_num: root.header.t_ctid.1,
            },
        );
        assert_ne!(successor.header.infomask & HEAP_ONLY, 0);
        assert_eq!(
            harness.storage.get(&update_ctx, 1, &pk(1)).unwrap(),
            Some(new_row)
        );
        assert!(visible_toast_chunk_sizes(&harness, &update_ctx, 1).is_empty());
    }

    #[test]
    fn update_inline_compressed_toastable_text_uses_hot() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let update_ctx = StatementContext::new(2);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::Zstd;
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        harness
            .storage
            .create_index(&insert_ctx, &name_index(false), 0)
            .unwrap();
        let note = "compressible inline note ".repeat(80);
        let old_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(true),
                Value::Text(note.clone()),
            ],
        };
        let new_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(false),
                Value::Text(note),
            ],
        };
        harness.storage.insert(&insert_ctx, 1, old_row).unwrap();

        assert!(
            harness
                .storage
                .update(&update_ctx, 1, &pk(1), new_row.clone())
                .unwrap()
        );

        let pk_tids = pk_index_tids_for_key(&harness, &base, &pk(1));
        assert_eq!(pk_tids.len(), 1, "HOT update should not add a PK entry");
        let root = physical_row_at(&harness, &base, pk_tids[0]);
        let successor = physical_row_at(
            &harness,
            &base,
            RowLocation {
                file_id: base.id,
                page_num: root.header.t_ctid.0,
                slot_num: root.header.t_ctid.1,
            },
        );
        assert_ne!(successor.header.infomask & HEAP_ONLY, 0);
        assert!(matches!(
            &successor.values[3],
            DecodedPhysicalValue::Compressed { .. }
        ));
        assert_eq!(
            harness.storage.get(&update_ctx, 1, &pk(1)).unwrap(),
            Some(new_row)
        );
        assert!(visible_toast_chunk_sizes(&harness, &update_ctx, 1).is_empty());
    }

    #[test]
    fn update_external_toastable_text_falls_back_to_normal_update() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let update_ctx = StatementContext::new(2);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::None;
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        harness
            .storage
            .create_index(&insert_ctx, &name_index(false), 0)
            .unwrap();
        let note = "external note ".repeat(700);
        let old_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(true),
                Value::Text(note.clone()),
            ],
        };
        let new_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(false),
                Value::Text(note),
            ],
        };
        harness.storage.insert(&insert_ctx, 1, old_row).unwrap();

        assert!(
            harness
                .storage
                .update(&update_ctx, 1, &pk(1), new_row.clone())
                .unwrap()
        );

        let pk_tids = pk_index_tids_for_key(&harness, &base, &pk(1));
        assert_eq!(
            pk_tids.len(),
            2,
            "external TOAST owner should use the normal indexed update path"
        );
        assert_eq!(
            secondary_index_tids_for_text(&harness, &name_index(false), "Ada").len(),
            2,
            "normal update keeps one secondary entry per version"
        );
        for location in pk_tids {
            let physical = physical_row_at(&harness, &base, location);
            assert_eq!(physical.header.infomask & HEAP_ONLY, 0);
        }
        assert_eq!(
            external_value_ids_for_key(&harness, &base, &pk(1), 3),
            vec![1, 2]
        );
        assert_eq!(
            harness.storage.get(&update_ctx, 1, &pk(1)).unwrap(),
            Some(new_row)
        );
    }

    #[test]
    fn update_inline_to_external_toastable_text_falls_back_to_normal_update() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let update_ctx = StatementContext::new(2);
        let (mut base, toast) = base_and_toast_schema();
        base.toast = ToastOptions::default_new_table();
        base.toast.min_value_size = 128;
        base.toast.compression = ToastCompression::None;
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        harness
            .storage
            .create_index(&insert_ctx, &name_index(false), 0)
            .unwrap();
        let old_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(true),
                Value::Text("inline note".to_string()),
            ],
        };
        let new_row = Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Ada".to_string()),
                Value::Boolean(true),
                Value::Text("external note ".repeat(700)),
            ],
        };
        harness.storage.insert(&insert_ctx, 1, old_row).unwrap();

        assert!(
            harness
                .storage
                .update(&update_ctx, 1, &pk(1), new_row.clone())
                .unwrap()
        );

        let pk_tids = pk_index_tids_for_key(&harness, &base, &pk(1));
        assert_eq!(
            pk_tids.len(),
            2,
            "would-be external successor should use the normal indexed update path"
        );
        assert_eq!(
            secondary_index_tids_for_text(&harness, &name_index(false), "Ada").len(),
            2,
            "normal update keeps one secondary entry per version"
        );
        for location in pk_tids {
            let physical = physical_row_at(&harness, &base, location);
            assert_eq!(physical.header.infomask & HEAP_ONLY, 0);
        }
        assert_eq!(
            external_value_ids_present_for_key(&harness, &base, &pk(1), 3),
            vec![1]
        );
        assert_eq!(
            harness.storage.get(&update_ctx, 1, &pk(1)).unwrap(),
            Some(new_row)
        );
    }

    #[test]
    fn update_toast_enabled_row_above_target_without_candidates_uses_hot() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let update_ctx = StatementContext::new(2);
        let integer_columns = 270;
        let (base, toast) = wide_bytea_base_and_toast_schema(integer_columns);
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let wide_row = |second_value: i64| {
            let mut values = Vec::with_capacity(integer_columns + 1);
            for index in 0..integer_columns {
                let value = match index {
                    0 => 1,
                    1 => second_value,
                    _ => index as i64,
                };
                values.push(Value::Integer(value));
            }
            values.push(Value::Bytes(vec![7; 8]));
            Row { values }
        };
        let old_row = wide_row(10);
        let new_row = wide_row(11);
        harness.storage.insert(&insert_ctx, 1, old_row).unwrap();

        assert!(
            harness
                .storage
                .update(&update_ctx, 1, &pk(1), new_row.clone())
                .unwrap()
        );

        let pk_tids = pk_index_tids_for_key(&harness, &base, &pk(1));
        assert_eq!(
            pk_tids.len(),
            1,
            "inline row above toast_tuple_target but with no externalization candidates should still HOT-update"
        );
        let root = physical_row_at(&harness, &base, pk_tids[0]);
        assert_ne!(root.header.infomask & HOT_UPDATED, 0);
        let successor = physical_row_at(
            &harness,
            &base,
            RowLocation {
                file_id: base.id,
                page_num: root.header.t_ctid.0,
                slot_num: root.header.t_ctid.1,
            },
        );
        assert_ne!(successor.header.infomask & HEAP_ONLY, 0);
        assert!(visible_toast_chunk_sizes(&harness, &update_ctx, 1).is_empty());
        assert_eq!(
            harness.storage.get(&update_ctx, 1, &pk(1)).unwrap(),
            Some(new_row)
        );
    }

    #[test]
    fn vacuum_rejects_toast_parent_when_chunk_cleanup_pending() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let delete_ctx = StatementContext::new(2);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9000))],
        };
        harness.storage.insert(&insert_ctx, 1, row).unwrap();
        assert!(harness.storage.delete(&delete_ctx, 1, &pk(1)).unwrap());

        assert_eq!(
            harness
                .storage
                .toast_value_ids_pending_vacuum(&base, 10)
                .unwrap(),
            vec![1]
        );
        let err = harness.storage.vacuum(&base, 10).unwrap_err();
        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("TOAST-enabled parent vacuum"));
    }

    #[test]
    fn vacuum_toast_cleanup_deletes_chunks_before_parent_prune() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let delete_ctx = StatementContext::new(2);
        let cleanup_ctx = StatementContext::new(3);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9000))],
        };
        harness.storage.insert(&insert_ctx, 1, row).unwrap();
        assert!(!visible_toast_chunk_sizes(&harness, &insert_ctx, 1).is_empty());
        assert!(harness.storage.delete(&delete_ctx, 1, &pk(1)).unwrap());

        assert_eq!(
            harness
                .storage
                .toast_value_ids_pending_vacuum(&base, 10)
                .unwrap(),
            vec![1]
        );
        let deleted = harness
            .storage
            .delete_toast_values(&cleanup_ctx, &base, &[1])
            .unwrap();
        assert!(deleted > 0);
        assert!(visible_toast_chunk_sizes(&harness, &cleanup_ctx, 1).is_empty());

        assert!(
            harness
                .storage
                .vacuum_after_toast_cleanup(&base, 10)
                .unwrap()
                > 0
        );
        assert!(
            harness
                .storage
                .vacuum_hidden_toast_relation(&base, cleanup_ctx.txn_id + 1)
                .unwrap()
                > 0
        );
        assert!(visible_toast_chunk_sizes(&harness, &cleanup_ctx, 1).is_empty());
        assert!(
            harness
                .storage
                .toast_value_ids_pending_vacuum(&base, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn vacuum_hidden_toast_relation_reclaims_aborted_chunks() {
        let harness = StorageHarness::new();
        let insert_ctx = StatementContext::new(1);
        let read_ctx = StatementContext::new(2);
        let (base, toast) = bytea_base_and_toast_schema();
        harness.storage.create_table(&insert_ctx, &base).unwrap();
        harness.storage.create_table(&insert_ctx, &toast).unwrap();
        let row = Row {
            values: vec![Value::Integer(1), Value::Bytes(entropy_bytes(9000))],
        };
        harness.storage.insert(&insert_ctx, 1, row).unwrap();
        harness.wal.mark_aborted(insert_ctx.txn_id);

        assert!(visible_toast_chunk_sizes(&harness, &read_ctx, 1).is_empty());
        assert_eq!(
            harness
                .storage
                .toast_value_ids_pending_vacuum(&base, 10)
                .unwrap(),
            vec![1]
        );
        assert!(
            harness
                .storage
                .vacuum_after_toast_cleanup(&base, 10)
                .unwrap()
                > 0
        );
        assert!(
            harness
                .storage
                .vacuum_hidden_toast_relation(&base, 10)
                .unwrap()
                > 0
        );
        assert!(
            harness
                .storage
                .toast_value_ids_pending_vacuum(&base, 10)
                .unwrap()
                .is_empty()
        );
    }

    /// A secondary index on the `name` column (column id 1) of `users`.
    fn name_index(unique: bool) -> IndexSchema {
        IndexSchema {
            id: 1,
            storage_id: 101,
            table: 1,
            name: "users_name".to_string(),
            columns: vec![1],
            unique,
            constraint: common::IndexConstraintKind::None,
        }
    }

    fn pk(id: i64) -> Key {
        Key(vec![Value::Integer(id)])
    }

    fn name_eq(name: &str) -> KeyRange {
        KeyRange::Exact(Key(vec![Value::Text(name.to_string())]))
    }

    fn collect(mut iter: Box<dyn RowIterator>) -> Vec<Row> {
        let mut rows = Vec::new();
        while let Some(stored) = iter.next().unwrap() {
            rows.push(stored.row);
        }
        rows
    }

    fn row_name(row: Row) -> String {
        match &row.values[1] {
            Value::Text(name) => name.clone(),
            other => panic!("expected text name, got {other:?}"),
        }
    }

    /// The primary-key ids of the rows the `name` index returns for `name`.
    fn index_ids(harness: &StorageHarness, name: &str) -> Vec<i64> {
        let iter = harness
            .storage
            .index_scan(&StatementContext::new(1), 1, 1, &name_eq(name))
            .unwrap();
        collect(iter)
            .into_iter()
            .map(|row| match row.values[0] {
                Value::Integer(id) => id,
                ref other => panic!("expected integer id, got {other:?}"),
            })
            .collect()
    }

    fn external_value_ids_for_key(
        harness: &StorageHarness,
        schema: &TableSchema,
        key: &Key,
        column: usize,
    ) -> Vec<u64> {
        let btree: BTree<'_, RowLocation> = BTree::new(
            harness.storage.buffer_pool.as_ref(),
            harness.storage.wal.as_ref(),
            primary_index_file_id(schema.storage_id),
            harness.storage.compression.as_ref(),
        );
        let mut value_ids = Vec::new();
        for location in btree.scan_key(key).unwrap() {
            let readable = harness
                .storage
                .buffer_pool
                .read_page(location.file_id, location.page_num)
                .unwrap();
            let bytes = crate::page::read_row(readable.data(), location.slot_num)
                .unwrap()
                .unwrap();
            let physical = decode_physical_row(schema, &bytes).unwrap();
            match &physical.values[column] {
                DecodedPhysicalValue::External { pointer, .. } => {
                    value_ids.push(pointer.value_id);
                }
                other => panic!("expected external value at column {column}, got {other:?}"),
            }
        }
        value_ids.sort_unstable();
        value_ids
    }

    fn external_value_ids_present_for_key(
        harness: &StorageHarness,
        schema: &TableSchema,
        key: &Key,
        column: usize,
    ) -> Vec<u64> {
        let mut value_ids = Vec::new();
        for location in pk_index_tids_for_key(harness, schema, key) {
            let physical = physical_row_at(harness, schema, location);
            if let DecodedPhysicalValue::External { pointer, .. } = &physical.values[column] {
                value_ids.push(pointer.value_id);
            }
        }
        value_ids.sort_unstable();
        value_ids
    }

    fn pk_index_tids_for_key(
        harness: &StorageHarness,
        schema: &TableSchema,
        key: &Key,
    ) -> Vec<RowLocation> {
        let btree: BTree<'_, RowLocation> = BTree::new(
            harness.storage.buffer_pool.as_ref(),
            harness.storage.wal.as_ref(),
            primary_index_file_id(schema.storage_id),
            harness.storage.compression.as_ref(),
        );
        btree.scan_key(key).unwrap()
    }

    fn secondary_index_tids_for_text(
        harness: &StorageHarness,
        index: &IndexSchema,
        value: &str,
    ) -> Vec<RowLocation> {
        let btree: BTree<'_, RowLocation> = BTree::new(
            harness.storage.buffer_pool.as_ref(),
            harness.storage.wal.as_ref(),
            secondary_index_file_id(index.storage_id),
            harness.storage.compression.as_ref(),
        );
        btree
            .scan_key(&Key(vec![Value::Text(value.to_string())]))
            .unwrap()
    }

    fn physical_row_at(
        harness: &StorageHarness,
        schema: &TableSchema,
        location: RowLocation,
    ) -> crate::codec::DecodedPhysicalRow {
        let readable = harness
            .storage
            .buffer_pool
            .read_page(location.file_id, location.page_num)
            .unwrap();
        let bytes = crate::page::read_row(readable.data(), location.slot_num)
            .unwrap()
            .unwrap();
        decode_physical_row(schema, &bytes).unwrap()
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
        flushed: AtomicUsize,
        flushes: AtomicUsize,
        fail_at: AtomicUsize,
        block_next_flush: std::sync::Mutex<Option<FlushGate>>,
        fail_next_fpi: std::sync::atomic::AtomicBool,
        fail_next_heap_update_header: std::sync::atomic::AtomicBool,
        fpi_count_by_file: std::sync::Mutex<std::collections::HashMap<FileId, usize>>,
        /// Transactions the test has explicitly aborted. Status-based abort
        /// (`docs/specs/mvcc.md` §4 Decision 3) hides a rolled-back txn's rows via
        /// the CLOG rather than by physical undo, so a test that rolls a txn back
        /// marks it here to model `CLOG[txn] = Aborted` and assert invisibility.
        aborted: std::sync::Mutex<std::collections::HashSet<TxnId>>,
    }

    impl CountingWal {
        fn record_count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }

        fn flush_count(&self) -> usize {
            self.flushes.load(Ordering::SeqCst)
        }

        fn fail_on_append_number(&self, append_number: usize) {
            self.fail_at.store(append_number, Ordering::SeqCst);
        }

        fn block_next_flush(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            *self.block_next_flush.lock().unwrap() = Some(FlushGate {
                entered: entered_tx,
                release: release_rx,
            });
            (entered_rx, release_tx)
        }

        fn fail_next_full_page_image(&self) {
            self.fail_next_fpi.store(true, Ordering::SeqCst);
        }

        fn full_page_image_count(&self, file_id: FileId) -> usize {
            self.fpi_count_by_file
                .lock()
                .unwrap()
                .get(&file_id)
                .copied()
                .unwrap_or(0)
        }

        fn fail_next_heap_update_header(&self) {
            self.fail_next_heap_update_header
                .store(true, Ordering::SeqCst);
        }

        /// Model `CLOG[txn] = Aborted` so the visibility predicate hides the txn's
        /// (physically retained, no-undo) rows.
        fn mark_aborted(&self, txn_id: TxnId) {
            self.aborted.lock().unwrap().insert(txn_id);
        }
    }

    struct FlushGate {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl WalManager for CountingWal {
        fn append(&self, record: WalRecord) -> Result<Lsn> {
            let next = self.count.load(Ordering::SeqCst) + 1;
            if self.fail_at.load(Ordering::SeqCst) == next {
                self.fail_at.store(0, Ordering::SeqCst);
                return Err(DbError::io("injected WAL append failure"));
            }
            // Compression (Task 7) attempts zstd on EVERY full-page image, even
            // under a registry with no file configured, so a compressible test
            // page now logs `FullPageImageCompressed` instead of the raw
            // variant. Treat either as "an FPI" for failure injection and
            // counting.
            let is_fpi = matches!(
                record.kind,
                WalRecordKind::FullPageImage { .. } | WalRecordKind::FullPageImageCompressed { .. }
            );
            if is_fpi && self.fail_next_fpi.swap(false, Ordering::SeqCst) {
                return Err(DbError::io("injected WAL append failure"));
            }
            if matches!(record.kind, WalRecordKind::HeapUpdateHeader { .. })
                && self
                    .fail_next_heap_update_header
                    .swap(false, Ordering::SeqCst)
            {
                return Err(DbError::io("injected WAL append failure"));
            }
            let fpi_file_id = match &record.kind {
                WalRecordKind::FullPageImage { file_id, .. }
                | WalRecordKind::FullPageImageCompressed { file_id, .. } => Some(*file_id),
                _ => None,
            };
            if let Some(file_id) = fpi_file_id {
                *self
                    .fpi_count_by_file
                    .lock()
                    .unwrap()
                    .entry(file_id)
                    .or_insert(0) += 1;
            }
            Ok(self.count.fetch_add(1, Ordering::SeqCst) as Lsn + 1)
        }

        fn flush(&self) -> Result<Lsn> {
            if let Some(gate) = self.block_next_flush.lock().unwrap().take() {
                let _ = gate.entered.send(());
                let _ = gate.release.recv();
            }
            self.flushes.fetch_add(1, Ordering::SeqCst);
            let count = self.count.load(Ordering::SeqCst);
            self.flushed.store(count, Ordering::SeqCst);
            Ok(count as Lsn)
        }

        fn replay_from(&self, _lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>> {
            Ok(Box::new(std::iter::empty()))
        }

        fn truncate_before(&self, _lsn: Lsn) -> Result<()> {
            Ok(())
        }

        fn flushed_lsn(&self) -> Lsn {
            self.flushed.load(Ordering::SeqCst) as Lsn
        }

        fn bytes_after(&self, _lsn: Lsn) -> Result<u64> {
            Ok(0)
        }

        fn establish_recovery_committed_floor(&self, _allocation_boundary: u64) -> Result<()> {
            Ok(())
        }

        fn resolve_in_flight_as_aborted(
            &self,
            _writer_xids: &std::collections::HashSet<u64>,
        ) -> Result<()> {
            Ok(())
        }

        fn set_vacuum_floor(&self, _boundary: TxnId) -> Result<()> {
            Ok(())
        }

        fn persist_clog(&self, _clog_lsn: Lsn) -> Result<()> {
            Ok(())
        }
    }

    impl TxnStatusView for CountingWal {
        // The harness models committed autocommit units: every statement here
        // commits (via `commit_txn`/`buffer.commit`) and is read back as committed,
        // EXCEPT txns the test explicitly aborted. Under status-based abort
        // (`docs/specs/mvcc.md` §4 Decision 3) a rolled-back txn's rows are retained
        // physically (no before-image undo) and hidden by this `Aborted` status, so
        // the harness must report it to make rollback tests assert invisibility.
        fn status(&self, txn_id: TxnId) -> TxnStatus {
            if self.aborted.lock().unwrap().contains(&txn_id) {
                TxnStatus::Aborted
            } else {
                TxnStatus::Committed
            }
        }
    }

    fn users_schema() -> TableSchema {
        TableSchema {
            id: 1,
            storage_id: 1,
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 2,
                    name: "active".to_string(),
                    data_type: DataType::Boolean,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 3,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            schema_version: common::INITIAL_SCHEMA_VERSION,
            checks: Vec::new(),
        }
    }

    fn nullable_primary_key_users_schema() -> TableSchema {
        let mut schema = users_schema();
        schema.columns[0].nullable = true;
        schema
    }

    fn sequence_schema(
        id: u32,
        name: &str,
        start: i64,
        min_value: i64,
        max_value: i64,
        increment: i64,
        cycle: bool,
    ) -> SequenceSchema {
        SequenceSchema {
            id,
            name: name.to_string(),
            increment,
            min_value,
            max_value,
            start,
            cycle,
            owned: false,
            last_value: start,
            is_called: false,
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

    fn user_row_null_name(id: i64) -> Row {
        Row {
            values: vec![
                Value::Integer(id),
                Value::Null,
                Value::Boolean(true),
                Value::Null,
            ],
        }
    }

    fn user_row_null_id(name: &str, active: bool) -> Row {
        Row {
            values: vec![
                Value::Null,
                Value::Text(name.to_string()),
                Value::Boolean(active),
                Value::Null,
            ],
        }
    }

    fn legacy_user_row(id: i64, name: &str, active: bool) -> Vec<u8> {
        let mut bytes = vec![1u8, 1 << 3];
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(u8::from(active));
        bytes
    }

    fn big_text_schema() -> TableSchema {
        TableSchema {
            id: 2,
            storage_id: 2,
            name: "big_text".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 1,
                    name: "payload".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 2,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            schema_version: common::INITIAL_SCHEMA_VERSION,
            checks: Vec::new(),
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

    fn base_and_toast_schema() -> (TableSchema, TableSchema) {
        let mut base = users_schema();
        base.toast_table_id = Some(2);
        let toast = toast_schema(&base, 2);
        (base, toast)
    }

    fn bytea_base_and_toast_schema() -> (TableSchema, TableSchema) {
        let mut base = TableSchema {
            id: 1,
            storage_id: 1,
            name: "bytea_base".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 1,
                    name: "payload".to_string(),
                    data_type: DataType::Bytea,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::default_new_table(),
            toast_table_id: Some(2),
            relation_kind: RelationKind::User,
            schema_version: common::INITIAL_SCHEMA_VERSION,
            checks: Vec::new(),
        };
        base.toast.min_value_size = 128;
        let toast = toast_schema(&base, 2);
        (base, toast)
    }

    fn two_bytea_base_and_toast_schema() -> (TableSchema, TableSchema) {
        let (mut base, _) = bytea_base_and_toast_schema();
        base.columns.push(ColumnDef {
            id: 2,
            name: "payload2".to_string(),
            data_type: DataType::Bytea,
            nullable: true,
            max_length: None,
            default: None,
            pg_type: None,
        });
        let toast = toast_schema(&base, 2);
        (base, toast)
    }

    fn integer_heavy_schema(column_count: usize) -> TableSchema {
        let mut columns = Vec::with_capacity(column_count);
        for index in 0..column_count {
            columns.push(ColumnDef {
                id: index as u16,
                name: format!("c{index}"),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            });
        }
        TableSchema {
            id: 1,
            storage_id: 1,
            name: "wide_fixed".to_string(),
            columns,
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::default_new_table(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            schema_version: common::INITIAL_SCHEMA_VERSION,
            checks: Vec::new(),
        }
    }

    fn wide_bytea_base_and_toast_schema(integer_column_count: usize) -> (TableSchema, TableSchema) {
        let mut columns = Vec::with_capacity(integer_column_count + 1);
        for index in 0..integer_column_count {
            columns.push(ColumnDef {
                id: index as u16,
                name: format!("c{index}"),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            });
        }
        columns.push(ColumnDef {
            id: integer_column_count as u16,
            name: "payload".to_string(),
            data_type: DataType::Bytea,
            nullable: false,
            max_length: None,
            default: None,
            pg_type: None,
        });
        let mut base = TableSchema {
            id: 1,
            storage_id: 1,
            name: "wide_bytea".to_string(),
            columns,
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::default_new_table(),
            toast_table_id: Some(2),
            relation_kind: RelationKind::User,
            schema_version: common::INITIAL_SCHEMA_VERSION,
            checks: Vec::new(),
        };
        base.toast.min_value_size = 128;
        let toast = toast_schema(&base, 2);
        (base, toast)
    }

    fn dict_external_base_and_toast_schema() -> (TableSchema, TableSchema) {
        let mut columns = vec![ColumnDef {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            max_length: None,
            default: None,
            pg_type: None,
        }];
        for index in 0..40 {
            columns.push(ColumnDef {
                id: index + 1,
                name: format!("fixed{index}"),
                data_type: DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            });
        }
        columns.push(ColumnDef {
            id: 41,
            name: "body".to_string(),
            data_type: DataType::Text,
            nullable: false,
            max_length: None,
            default: None,
            pg_type: None,
        });
        let mut base = TableSchema {
            id: 1,
            storage_id: 1,
            name: "dict_external".to_string(),
            columns,
            primary_key: vec![0],
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::default_new_table(),
            toast_table_id: Some(2),
            relation_kind: RelationKind::User,
            schema_version: common::INITIAL_SCHEMA_VERSION,
            checks: Vec::new(),
        };
        base.toast.tuple_target = ToastOptions::MIN_TOAST_TUPLE_TARGET;
        base.toast.min_value_size = 128;
        let toast = toast_schema(&base, 2);
        (base, toast)
    }

    fn toast_chunk_row(value_id: i64, seq: i64, data: &[u8]) -> Row {
        Row {
            values: vec![
                Value::Integer(value_id),
                Value::Integer(seq),
                Value::Bytes(data.to_vec()),
            ],
        }
    }

    fn visible_toast_chunk_sizes(
        harness: &StorageHarness,
        ctx: &StatementContext,
        value_id: u64,
    ) -> Vec<usize> {
        let mut iter = harness
            .storage
            .scan_range(
                ctx,
                2,
                &KeyRange::Exact(Key(vec![Value::Integer(value_id as i64)])),
            )
            .unwrap();
        let mut sizes = Vec::new();
        while let Some(stored) = iter.next().unwrap() {
            match stored.row.values.get(2) {
                Some(Value::Bytes(data)) => sizes.push(data.len()),
                other => panic!("expected TOAST chunk BYTEA, got {other:?}"),
            }
        }
        sizes
    }

    fn prepare_physical_row(
        harness: &StorageHarness,
        ctx: &StatementContext,
        schema: &TableSchema,
        row: &Row,
    ) -> Vec<DecodedPhysicalValue> {
        let bytes = harness
            .storage
            .prepare_row_for_storage(
                ctx,
                &harness
                    .storage
                    .capture_pagebacked_relation_snapshot()
                    .unwrap(),
                schema,
                &MvccHeader::fresh(ctx.txn_id, 0),
                row,
            )
            .unwrap();
        decode_physical_row(schema, &bytes).unwrap().values
    }

    fn seed_parent_tuple(
        harness: &StorageHarness,
        ctx: &StatementContext,
        schema: &TableSchema,
        row_bytes: &[u8],
        key: &Key,
    ) -> RowLocation {
        let location = RowLocation {
            file_id: heap_file_id(schema.storage_id),
            page_num: 0,
            slot_num: 0,
        };
        {
            let mut guard = harness
                .storage
                .buffer_pool
                .fetch_for_redo(heap_file_id(schema.storage_id), 0)
                .unwrap();
            apply_physical_redo(
                guard.data_mut(),
                10,
                &WalRecordKind::HeapInit {
                    file_id: heap_file_id(schema.storage_id),
                    page_num: 0,
                },
            )
            .unwrap();
        }
        {
            let mut guard = harness
                .storage
                .buffer_pool
                .fetch_for_redo(heap_file_id(schema.storage_id), 0)
                .unwrap();
            apply_physical_redo(
                guard.data_mut(),
                11,
                &WalRecordKind::HeapInsert {
                    file_id: heap_file_id(schema.storage_id),
                    page_num: 0,
                    slot: 0,
                    row_bytes: row_bytes.to_vec(),
                },
            )
            .unwrap();
        }
        let btree = BTree::new(
            harness.storage.buffer_pool.as_ref(),
            harness.storage.wal.as_ref(),
            primary_index_file_id(schema.storage_id),
            harness.storage.compression.as_ref(),
        );
        btree.insert(ctx.txn_id, key, &location).unwrap();
        location
    }

    fn seed_secondary_entry(
        harness: &StorageHarness,
        ctx: &StatementContext,
        index: &IndexSchema,
        key: &Key,
        location: &RowLocation,
    ) {
        let btree = BTree::new(
            harness.storage.buffer_pool.as_ref(),
            harness.storage.wal.as_ref(),
            secondary_index_file_id(index.storage_id),
            harness.storage.compression.as_ref(),
        );
        btree.insert(ctx.txn_id, key, location).unwrap();
    }

    fn external_name_parent_bytes(
        schema: &TableSchema,
        txn_id: TxnId,
        row_id: i64,
        value_id: u64,
    ) -> Vec<u8> {
        encode_row_v3_prepared(
            schema,
            &MvccHeader::fresh(txn_id, 0),
            &[
                PreparedColumnValue::Value(Value::Integer(row_id)),
                PreparedColumnValue::Varlena(VarlenaPhysical::External(ToastPointer {
                    value_id,
                    raw_len: 4,
                    stored_len: 8,
                    codec: compress::CODEC_NONE,
                })),
                PreparedColumnValue::Value(Value::Boolean(true)),
                PreparedColumnValue::Null,
            ],
        )
        .unwrap()
    }

    fn entropy_bytes(len: usize) -> Vec<u8> {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x & 0xFF) as u8
            })
            .collect()
    }

    fn dict_value(seed: u8, len: usize) -> Vec<u8> {
        let chunk = format!("customer-{seed:03}-email-bio-location-plan-status-lifecycle-event;");
        chunk.as_bytes().iter().copied().cycle().take(len).collect()
    }

    fn register_test_dictionary(harness: &StorageHarness, dict_id: u32) {
        let samples: Vec<Vec<u8>> = (0..64).map(|seed| dict_value(seed, 2048)).collect();
        let dict = compress::train_dictionary(&samples).expect("dictionary corpus is large enough");
        harness
            .storage
            .compression
            .register_dictionary(dict_id, &dict)
            .unwrap();
    }

    fn redo_toast_chunk(
        storage: &PageBackedStorageEngine,
        value_id: i64,
        txn_id: TxnId,
        first_lsn: Lsn,
        data: &[u8],
    ) -> Result<()> {
        let (_, toast) = base_and_toast_schema();
        let row_bytes = encode_row(&toast, &toast_chunk_row(value_id, 0, data), txn_id)?;
        {
            let mut guard = storage.buffer_pool.fetch_for_redo(2, 0)?;
            apply_physical_redo(
                guard.data_mut(),
                first_lsn,
                &WalRecordKind::HeapInit {
                    file_id: 2,
                    page_num: 0,
                },
            )?;
        }
        {
            let mut guard = storage.buffer_pool.fetch_for_redo(2, 0)?;
            apply_physical_redo(
                guard.data_mut(),
                first_lsn + 1,
                &WalRecordKind::HeapInsert {
                    file_id: 2,
                    page_num: 0,
                    slot: 0,
                    row_bytes,
                },
            )?;
        }
        Ok(())
    }

    /// Longest `big_text` payload whose encoded row exactly fills one data page,
    /// derived from the page header/slot overhead so it tracks format changes.
    fn single_page_capacity_text() -> String {
        let schema = big_text_schema();
        let base = encode_row(
            &schema,
            &Row {
                values: vec![Value::Integer(1), Value::Text(String::new()), Value::Null],
            },
            1,
        )
        .unwrap()
        .len();
        let capacity = PAGE_SIZE - crate::page::HEADER_LEN - crate::page::SLOT_LEN;
        "x".repeat(capacity - base)
    }
}
