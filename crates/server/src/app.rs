use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use buffer::{BufferPool, PageStore};
use catalog::CatalogManager;
use common::ConcurrencyController;
use control::ControlStore;
use storage::PageBackedStorageEngine;
use tokio_rustls::TlsAcceptor;
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
    pub control: Arc<dyn ControlStore>,
    pub store: Arc<dyn PageStore>,
    pub concurrency: Arc<dyn ConcurrencyController>,
    pub checkpoint: CheckpointState,
    pub shutdown: Arc<ShutdownState>,
    pub next_txn_id: AtomicU64,
    /// TLS acceptor when the server is configured for SSL, else `None`.
    pub tls: Option<TlsAcceptor>,
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
        self.components
            .checkpoint
            .checkpoints
            .load(std::sync::atomic::Ordering::Acquire) as usize
    }

    pub fn wal_flushed_for_test(&self) -> bool {
        self.components.wal.flushed_lsn() > 0
    }
}
