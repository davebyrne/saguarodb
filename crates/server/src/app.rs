use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use buffer::BufferPool;
use catalog::CatalogManager;
use common::ConcurrencyController;
use snapshot::SnapshotManager;
use storage::PageBackedStorageEngine;
use wal::WalManager;

use crate::checkpoint::CheckpointState;
use crate::config::Config;
use crate::query::QueryService;
use crate::shutdown::ShutdownState;

pub struct ServerComponents {
    pub config: Config,
    pub catalog: Arc<dyn CatalogManager>,
    pub storage: Arc<PageBackedStorageEngine>,
    pub buffer_pool: Arc<dyn BufferPool>,
    pub wal: Arc<dyn WalManager>,
    pub snapshot_manager: Arc<dyn SnapshotManager>,
    pub concurrency: Arc<dyn ConcurrencyController>,
    pub checkpoint: CheckpointState,
    pub shutdown: Arc<ShutdownState>,
    pub next_txn_id: AtomicU64,
}

pub struct AppState {
    pub components: Arc<ServerComponents>,
    pub query_service: Arc<QueryService>,
}

#[cfg(test)]
impl AppState {
    pub fn open_for_test(_path: &std::path::Path) -> common::Result<Self> {
        let config = crate::recovery::data_dir_for_test(_path);
        crate::recovery::open_app(config)
    }

    pub fn checkpoint_count_for_test(&self) -> usize {
        std::fs::read_dir(&self.components.config.data_dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false)
                            && entry
                                .file_name()
                                .to_str()
                                .map(|name| name.starts_with("snap_"))
                                .unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn wal_flushed_for_test(&self) -> bool {
        self.components.wal.flushed_lsn() > 0
    }
}
