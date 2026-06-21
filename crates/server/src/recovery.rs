use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use catalog::{CatalogManager, MemoryCatalog, deserialize_catalog};
use common::{DbError, FlushPolicy, PageFlushInfo, Result, RwLockConcurrencyController};
use control::{ControlStore, FileControlStore};
use storage::{HeapPageStore, PageBackedStorageEngine, RecoveryOperations, StorageMode};
use wal::{FileWalManager, WalManager, WalRecordKind};

use crate::app::{AppState, ServerComponents};
use crate::checkpoint::{CheckpointState, run_checkpoint};
use crate::config::Config;
use crate::query::QueryService;
use crate::shutdown::ShutdownState;

pub fn open_app(config: Config) -> Result<AppState> {
    let control: Arc<dyn ControlStore> = Arc::new(FileControlStore::open(&config.data_dir)?);
    let store: Arc<dyn PageStore> = Arc::new(HeapPageStore::open(config.data_dir.join("heap"))?);
    let wal: Arc<dyn WalManager> = Arc::new(FileWalManager::open(config.data_dir.join("wal.dat"))?);
    let buffer_pool: Arc<dyn BufferPool> = Arc::new(MemoryBufferPool::new(
        config.buffer_pool_frames,
        Box::new(WalFlushPolicy { wal: wal.clone() }),
        store.clone(),
    ));
    // The durable on-disk primary-key index means recovery never rebuilds an
    // in-memory directory, so redo may flush+evict committed pages to the heap and
    // index files. Allow stealing from the start: the recovery working set is no
    // longer bounded by the buffer pool size.
    buffer_pool.enable_stealing();

    // The control record is the redo boundary plus the catalog snapshot.
    let loaded = control.load()?;
    let checkpoint_lsn = loaded
        .as_ref()
        .map(|control| control.checkpoint_lsn)
        .unwrap_or(0);
    let catalog: Arc<dyn CatalogManager> = match &loaded {
        Some(control) => Arc::new(MemoryCatalog::try_from_snapshot(deserialize_catalog(
            &control.catalog,
        )?)?),
        None => Arc::new(MemoryCatalog::empty()),
    };

    let storage = Arc::new(PageBackedStorageEngine::open(
        buffer_pool.clone(),
        wal.clone(),
        StorageMode::Recovery,
    )?);
    // Install both table and secondary-index schemas from the loaded catalog so
    // recovery replay and later DML maintain the indexes.
    let tables = catalog.list_tables()?;
    let mut indexes = Vec::new();
    for table in &tables {
        indexes.extend(catalog.list_indexes_for_table(table.id)?);
    }
    storage.install_schemas(tables)?;
    storage.install_index_schemas(indexes)?;

    // Redo: replay committed records after the checkpoint LSN onto the heap and
    // index pages. PageLSN gating makes this idempotent; torn/missing pages are
    // zeroed so a FullPageImage/HeapInit re-establishes them. The durable B-tree
    // is recovered the same way (its mutations are full-page-image records).
    let mut replay_applied = false;
    for record in wal.replay_committed_from(checkpoint_lsn)? {
        let record = record?;
        apply_redo(
            catalog.as_ref(),
            storage.as_ref(),
            buffer_pool.as_ref(),
            record.lsn,
            record.kind,
        )?;
        replay_applied = true;
    }

    let next_txn_id = next_txn_id(wal.as_ref(), checkpoint_lsn)?;
    let tls = match config.tls_files().map_err(DbError::io)? {
        Some((cert, key)) => Some(crate::tls::build_acceptor(cert, key)?),
        None => None,
    };
    let components = Arc::new(ServerComponents {
        config,
        catalog,
        storage,
        buffer_pool,
        wal,
        control,
        store,
        concurrency: Arc::new(RwLockConcurrencyController::new()),
        checkpoint: CheckpointState {
            last_checkpoint_lsn: AtomicU64::new(checkpoint_lsn),
            commits_since_checkpoint: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
        },
        shutdown: Arc::new(ShutdownState::new()),
        next_txn_id: AtomicU64::new(next_txn_id),
        active_txns: crate::registry::ActiveTxnRegistry::new(),
        tls,
        cancel_registry: crate::cancel::CancelRegistry::new(),
    });

    // Persist the redone state to the heap/index and advance the redo boundary.
    if replay_applied {
        run_checkpoint(&components)?;
    }
    components.storage.set_mode(StorageMode::Normal)?;

    Ok(AppState {
        components: components.clone(),
        query_service: Arc::new(QueryService::new(components)),
    })
}

#[allow(dead_code)]
pub fn data_dir_for_test(path: &Path) -> Config {
    Config {
        data_dir: path.to_path_buf(),
        ..Config::default()
    }
}

/// Flush policy for in-place dirty-page flushing: a page is flushable once its
/// dirtying transaction is committed (or it is recovery-written, txn 0) and its
/// page-LSN is WAL-durable.
struct WalFlushPolicy {
    wal: Arc<dyn WalManager>,
}

impl FlushPolicy for WalFlushPolicy {
    fn can_flush(&self, info: &PageFlushInfo) -> bool {
        let committed = info.dirty_txn_id == 0 || self.wal.is_committed(info.dirty_txn_id);
        let durable = info
            .page_lsn
            .is_none_or(|lsn| lsn <= self.wal.flushed_lsn());
        committed && durable
    }
}

fn apply_redo(
    catalog: &dyn CatalogManager,
    storage: &dyn RecoveryOperations,
    buffer_pool: &dyn BufferPool,
    lsn: u64,
    kind: WalRecordKind,
) -> Result<()> {
    match &kind {
        WalRecordKind::CreateTable { schema } => {
            catalog.apply_create_table(schema.clone())?;
            storage.apply_create_table(schema.clone())
        }
        WalRecordKind::DropTable { table } => {
            catalog.apply_drop_table(*table)?;
            storage.apply_drop_table(*table)
        }
        WalRecordKind::CreateIndex { schema } => {
            catalog.apply_create_index(schema.clone())?;
            storage.apply_create_index(schema.clone())
        }
        WalRecordKind::DropIndex { index } => {
            catalog.apply_drop_index(*index)?;
            storage.apply_drop_index(*index)
        }
        WalRecordKind::HeapInit { file_id, page_num }
        | WalRecordKind::HeapInsert {
            file_id, page_num, ..
        }
        | WalRecordKind::HeapDelete {
            file_id, page_num, ..
        }
        | WalRecordKind::FullPageImage {
            file_id, page_num, ..
        } => {
            let mut guard = buffer_pool.fetch_for_redo(*file_id, *page_num)?;
            // A torn or never-initialized page cannot be trusted for PageLSN
            // gating; zero it so the first FullPageImage / HeapInit rebuilds it.
            if !storage::page_is_valid(guard.data()) {
                guard.data_mut().fill(0);
            }
            storage::apply_physical_redo(guard.data_mut(), lsn, &kind)?;
            Ok(())
        }
        WalRecordKind::Commit | WalRecordKind::Abort | WalRecordKind::Checkpoint { .. } => Err(
            DbError::internal("recovery replay received an unexpected WAL record"),
        ),
    }
}

fn next_txn_id(wal: &dyn WalManager, checkpoint_lsn: u64) -> Result<u64> {
    let mut max_txn_id = 0;
    for record in wal.replay_from(checkpoint_lsn)? {
        let txn_id = record?.txn_id;
        if txn_id != 0 {
            max_txn_id = max_txn_id.max(txn_id);
        }
    }
    let next = max_txn_id
        .checked_add(1)
        .ok_or_else(|| DbError::wal(common::SqlState::InternalError, "transaction id overflow"))?;
    // Floor the allocator at FIRST_NORMAL_XID so real transactions never stamp
    // tuple headers with a reserved xid. On a fresh database max_txn_id is 0, so
    // an unfloored seed would hand out 1 and 2 (== FROZEN_XID), persisting rows
    // that later visibility code would treat as frozen/always-visible.
    Ok(next.max(common::FIRST_NORMAL_XID))
}

#[cfg(test)]
mod tests {
    use crate::app::AppState;
    use crate::checkpoint::run_checkpoint;
    use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

    #[tokio::test]
    async fn recovery_replays_committed_records_after_snapshot_lsn() {
        let dir = tempfile::tempdir().unwrap();
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (1, 'Ada')")
                .unwrap();
            run_checkpoint(&app.components).unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (2, 'Grace')")
                .unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let result = reopened
            .query_service
            .execute_sql("select id, name from users order by id")
            .unwrap();

        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn recovery_replays_create_index_and_rebuilds_the_secondary_tree() {
        use common::{Key, KeyRange, StatementContext, Value};
        use storage::StorageEngine;

        let dir = tempfile::tempdir().unwrap();
        let table_id;
        let index_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            for sql in [
                "create table users (id integer primary key, name text)",
                "insert into users (id, name) values (1, 'Ada')",
                "insert into users (id, name) values (2, 'Grace')",
                "create index users_name on users (name)",
            ] {
                app.query_service.execute_sql(sql).unwrap();
            }
            // No checkpoint happened (few commits), so recovery must replay the
            // CreateIndex record rather than load it from the snapshot.
            let index = app
                .components
                .catalog
                .get_index_by_name("users_name")
                .unwrap()
                .unwrap();
            table_id = index.table;
            index_id = index.id;
        }

        // Reopen: recovery replays the CreateIndex record into both catalog and
        // storage and rebuilds the secondary tree from its full-page images.
        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let comps = &reopened.components;
        assert!(
            comps
                .catalog
                .get_index_by_name("users_name")
                .unwrap()
                .is_some()
        );

        // Scan the rebuilt secondary tree directly to prove its pages recovered.
        let ctx = StatementContext::new(0);
        let mut iter = comps
            .storage
            .index_scan(
                &ctx,
                table_id,
                index_id,
                &KeyRange::Exact(Key(vec![Value::Text("Ada".to_string())])),
            )
            .unwrap();
        let row = iter
            .next()
            .unwrap()
            .expect("Ada should be found through the recovered index");
        assert_eq!(row.row.values[0], Value::Integer(1));
        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn next_txn_id_rejects_retained_u64_max_txn_id() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: u64::MAX,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.flush().unwrap();

        let err = super::next_txn_id(&wal, 0).unwrap_err();
        assert!(err.message.contains("transaction id overflow"));
    }
}
