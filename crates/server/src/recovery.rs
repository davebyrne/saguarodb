use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use buffer::{BufferPool, MemoryBufferPool, PageStore};
use catalog::{CatalogManager, MemoryCatalog, deserialize_catalog};
use common::{
    DbError, FileId, FlushPolicy, PageFlushInfo, PageNum, Result, RwLockConcurrencyController,
    SqlState, TableId,
};
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
    storage.install_schemas(catalog.list_tables()?)?;

    // Load the checkpointed heap pages so redo and the directory rebuild see the
    // full working set. (V1 requires the working set to fit in the buffer pool.)
    let table_ids: Vec<TableId> = catalog
        .list_tables()?
        .iter()
        .map(|table| table.id)
        .collect();
    let preloaded = preload_heap_pages(store.as_ref(), buffer_pool.as_ref(), &table_ids)?;

    // Redo: replay committed records after the checkpoint LSN. PageLSN gating
    // makes this idempotent; torn/missing pages are zeroed so a FullPageImage
    // re-establishes them.
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

    // The directory rebuild scans resident pages only. If preload or redo evicted
    // any checkpointed page (buffer pool too small for the working set), fail
    // loudly rather than silently rebuild a partial directory.
    verify_pages_resident(buffer_pool.as_ref(), &preloaded)?;
    storage.rebuild_directories()?;

    let next_txn_id = next_txn_id(wal.as_ref(), checkpoint_lsn)?;
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
    });

    // Persist the redone state to the heap and advance the redo boundary.
    if replay_applied {
        run_checkpoint(&components)?;
    }
    // Recovery (preload, redo, directory rebuild) ran with stealing disabled so a
    // too-small buffer fails loudly instead of silently dropping pages. Normal
    // operation may now flush+evict committed dirty pages to bound memory use.
    components.buffer_pool.enable_stealing();
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

/// Load every heap page of each table into the buffer pool so that redo and the
/// in-memory directory rebuild operate over the complete checkpointed state.
/// Returns the loaded `(file_id, page_num)` keys for the post-redo residency check.
fn preload_heap_pages(
    store: &dyn PageStore,
    buffer_pool: &dyn BufferPool,
    tables: &[TableId],
) -> Result<Vec<(FileId, PageNum)>> {
    let mut loaded = Vec::new();
    for &table in tables {
        let mut page_num = 0;
        while let Some(data) = store.load_page(table, page_num)? {
            buffer_pool.load_page(table, page_num, data)?;
            loaded.push((table, page_num));
            page_num += 1;
        }
    }
    Ok(loaded)
}

/// Verify every checkpointed page is still resident after preload + redo. A
/// missing page means eviction occurred because the buffer pool cannot hold the
/// working set, which would silently drop rows from the rebuilt directory.
fn verify_pages_resident(
    buffer_pool: &dyn BufferPool,
    expected: &[(FileId, PageNum)],
) -> Result<()> {
    let resident: HashSet<(FileId, PageNum)> = buffer_pool
        .iter_pages()?
        .map(|page| (page.file_id, page.page_num))
        .collect();
    for key in expected {
        if !resident.contains(key) {
            return Err(DbError::storage(
                SqlState::InternalError,
                "buffer pool is too small to hold the recovery working set",
            ));
        }
    }
    Ok(())
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
        WalRecordKind::Commit | WalRecordKind::Checkpoint { .. } => Err(DbError::internal(
            "recovery replay received an unexpected WAL record",
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
    max_txn_id
        .checked_add(1)
        .ok_or_else(|| DbError::wal(common::SqlState::InternalError, "transaction id overflow"))
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
