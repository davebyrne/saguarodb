use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use buffer::{MemoryBufferPool, PageData, PageLoader};
use catalog::{CatalogManager, MemoryCatalog, deserialize_catalog};
use common::{
    DbError, FileId, FlushPolicy, PageFlushInfo, PageNum, Result, RwLockConcurrencyController,
};
use snapshot::{FileSnapshotManager, SnapshotManager};
use storage::{PageBackedStorageEngine, RecoveryOperations, StorageMode};
use wal::{FileWalManager, WalManager, WalRecordKind};

use crate::app::{AppState, ServerComponents};
use crate::checkpoint::{CheckpointState, run_checkpoint};
use crate::config::Config;
use crate::query::QueryService;
use crate::shutdown::ShutdownState;

pub fn open_app(config: Config) -> Result<AppState> {
    let snapshot_manager: Arc<dyn SnapshotManager> =
        Arc::new(FileSnapshotManager::open(&config.data_dir)?);
    let page_loader: Arc<dyn PageLoader> = Arc::new(SnapshotPageLoader {
        snapshot_manager: snapshot_manager.clone(),
    });
    let buffer_pool: Arc<dyn buffer::BufferPool> = Arc::new(MemoryBufferPool::new(
        config.buffer_pool_frames,
        Box::new(NeverFlush),
        page_loader,
    ));
    let wal: Arc<dyn WalManager> = Arc::new(FileWalManager::open(config.data_dir.join("wal.dat"))?);

    let loaded = snapshot_manager.load_current(buffer_pool.as_ref())?;
    let checkpoint_lsn = loaded
        .as_ref()
        .map(|snapshot| snapshot.metadata.checkpoint_lsn)
        .unwrap_or(0);
    let catalog: Arc<dyn CatalogManager> = match loaded {
        Some(snapshot) => Arc::new(MemoryCatalog::try_from_snapshot(deserialize_catalog(
            &snapshot.catalog_bytes,
        )?)?),
        None => Arc::new(MemoryCatalog::empty()),
    };

    let storage = Arc::new(PageBackedStorageEngine::open(
        buffer_pool.clone(),
        wal.clone(),
        StorageMode::Recovery,
    )?);
    storage.install_schemas(catalog.list_tables()?)?;
    storage.rebuild_directories()?;

    let mut replay_applied = false;
    for record in wal.replay_committed_from(checkpoint_lsn)? {
        let record = record?;
        apply_record(catalog.as_ref(), storage.as_ref(), record.kind)?;
        replay_applied = true;
    }

    let next_txn_id = next_txn_id(wal.as_ref(), checkpoint_lsn)?;
    let components = Arc::new(ServerComponents {
        config,
        catalog,
        storage,
        buffer_pool,
        wal,
        snapshot_manager,
        concurrency: Arc::new(RwLockConcurrencyController::new()),
        checkpoint: CheckpointState {
            last_checkpoint_lsn: AtomicU64::new(checkpoint_lsn),
            commits_since_checkpoint: AtomicU64::new(0),
        },
        shutdown: Arc::new(ShutdownState::new()),
        next_txn_id: AtomicU64::new(next_txn_id),
    });

    components.snapshot_manager.cleanup_old_snapshots()?;
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

struct NeverFlush;

impl FlushPolicy for NeverFlush {
    fn can_flush(&self, _info: &PageFlushInfo) -> bool {
        false
    }
}

struct SnapshotPageLoader {
    snapshot_manager: Arc<dyn SnapshotManager>,
}

impl PageLoader for SnapshotPageLoader {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>> {
        let pages = self.snapshot_manager.current_table_pages(file_id)?;
        Ok(pages
            .into_iter()
            .find(|page| page.page_num == page_num)
            .map(|page| page.data))
    }
}

fn apply_record(
    catalog: &dyn CatalogManager,
    storage: &dyn RecoveryOperations,
    kind: WalRecordKind,
) -> Result<()> {
    match kind {
        WalRecordKind::Insert { table, key, row } => storage.apply_insert(table, key, row),
        WalRecordKind::Update { table, key, row } => storage.apply_update(table, key, row),
        WalRecordKind::Delete { table, key } => storage.apply_delete(table, key),
        WalRecordKind::CreateTable { schema } => {
            catalog.apply_create_table(schema.clone())?;
            storage.apply_create_table(schema)
        }
        WalRecordKind::DropTable { table } => {
            catalog.apply_drop_table(table)?;
            storage.apply_drop_table(table)
        }
        WalRecordKind::Commit | WalRecordKind::Checkpoint { .. } => Err(DbError::internal(
            "recovery replay received non-logical WAL record",
        )),
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
    Ok(max_txn_id + 1)
}

#[cfg(test)]
mod tests {
    use crate::app::AppState;
    use crate::checkpoint::run_checkpoint;

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
}
