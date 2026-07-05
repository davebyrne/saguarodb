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
pub use engine::{PageBackedStorageEngine, StorageMode};
pub use heap::HeapPageStore;
pub use page::is_valid as page_is_valid;
pub use redo::apply_physical_redo;
pub use traits::{RecoveryOperations, RowIterator, SchemaOperations, StorageEngine};

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use buffer::{BufferPool, MemoryBufferPool, PAGE_SIZE, PageData};
    use common::{
        ColumnDef, CompressionSetting, DataType, DbError, FileId, INVALID_XID, IndexSchema, Key,
        KeyRange, Lsn, RelationKind, Result, Row, SequenceManager, SequenceSchema, SqlState,
        StatementContext, TableSchema, ToastCompression, ToastMode, ToastOptions, TxnId, TxnStatus,
        TxnStatusView, Value, toast_schema,
    };
    use wal::{WalManager, WalRecord, WalRecordKind};

    use crate::{
        PageBackedStorageEngine, RecoveryOperations, RowIterator, SchemaOperations, StorageEngine,
        StorageMode, apply_physical_redo,
        btree::BTree,
        decode_row, encode_row,
        engine::RowLocation,
        heap::index_file_id,
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
            crate::heap::index_file_id(1),
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
    fn dropped_index_is_not_maintained_or_scannable() {
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
        // ...and can no longer be scanned.
        let err = harness
            .storage
            .index_scan(&ctx, 1, 1, &name_eq("Ada"))
            .err()
            .expect("dropped index should not be scannable");
        assert_eq!(err.code, SqlState::UndefinedTable);
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
        let raw = vec![b'x'; TOAST_CHUNK_PAYLOAD * 2 + 37];
        let stream =
            build_external_stream(compress::CODEC_NONE, None, crc32fast::hash(&raw), &raw).unwrap();

        let pointer = harness
            .storage
            .write_toast_stream(&ctx, &base, raw.len() as u32, compress::CODEC_NONE, &stream)
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
            .write_toast_stream(&ctx, &base, raw.len() as u32, compress::CODEC_ZSTD, &stream)
            .unwrap();

        let read = harness
            .storage
            .read_toast_stream(&ctx, &base, &pointer)
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
        let pointer = crate::codec::ToastPointer {
            value_id: 1,
            raw_len: 4,
            stored_len: 8,
            codec: compress::CODEC_NONE,
        };

        let err = harness
            .storage
            .read_toast_stream(&ctx, &base, &pointer)
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
            index_file_id(2),
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
        let pointer = crate::codec::ToastPointer {
            value_id: 1,
            raw_len: 4,
            stored_len: 8,
            codec: compress::CODEC_NONE,
        };

        let err = harness
            .storage
            .read_toast_stream(&ctx, &base, &pointer)
            .unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("missing, duplicate, or out of order"));
    }

    #[test]
    fn read_toast_stream_rejects_wrong_stored_length() {
        let harness = StorageHarness::new();
        let ctx = StatementContext::new(1);
        let (base, toast) = base_and_toast_schema();
        harness.storage.create_table(&ctx, &base).unwrap();
        harness.storage.create_table(&ctx, &toast).unwrap();
        let raw = b"raw";
        let stream =
            build_external_stream(compress::CODEC_NONE, None, crc32fast::hash(raw), raw).unwrap();
        let mut pointer = harness
            .storage
            .write_toast_stream(&ctx, &base, raw.len() as u32, compress::CODEC_NONE, &stream)
            .unwrap();
        pointer.stored_len += 1;

        let err = harness
            .storage
            .read_toast_stream(&ctx, &base, &pointer)
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
        let stream = build_external_stream(compress::CODEC_NONE, None, 0, b"raw").unwrap();

        let err = harness
            .storage
            .write_toast_stream(&ctx, &base, 3, compress::CODEC_NONE, &stream)
            .unwrap_err();

        assert_eq!(err.code, SqlState::InternalError);
        assert!(err.message.contains("hidden TOAST relation"));
    }

    /// A secondary index on the `name` column (column id 1) of `users`.
    fn name_index(unique: bool) -> IndexSchema {
        IndexSchema {
            id: 1,
            table: 1,
            name: "users_name".to_string(),
            columns: vec![1],
            unique,
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
        }
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
