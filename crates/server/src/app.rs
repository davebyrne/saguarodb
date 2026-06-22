use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use buffer::{BufferPool, PageStore};
use catalog::CatalogManager;
use common::ConcurrencyController;
use control::ControlStore;
use storage::PageBackedStorageEngine;
use tokio_rustls::TlsAcceptor;
use wal::WalManager;

use crate::cancel::CancelRegistry;
use crate::checkpoint::CheckpointState;
use crate::config::Config;
use crate::query::QueryService;
use crate::registry::ActiveTxnRegistry;
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
    /// In-progress transaction ids. The CLOG (in the WAL manager) records settled
    /// outcomes; this registry tracks which transactions are still running, for
    /// snapshot capture (B3/C3) and the GC horizon (Milestone F).
    pub active_txns: ActiveTxnRegistry,
    /// TLS acceptor when the server is configured for SSL, else `None`.
    pub tls: Option<TlsAcceptor>,
    /// Per-connection cancellation keys, used to act on `CancelRequest`.
    pub cancel_registry: CancelRegistry,
}

impl ServerComponents {
    /// The GC horizon (`docs/specs/mvcc.md` §9): the oldest still-running
    /// transaction id, below which no live snapshot can see a committed delete as
    /// undone — so a committed-deleted version with `xmax < horizon` is dead to
    /// everyone (see [`common::is_dead_to_all`]). It is captured **once** at the
    /// start of a VACUUM pass.
    ///
    /// When no transaction is active there is nothing to protect, so the horizon is
    /// the next id to be assigned — nothing older than the future can be needed.
    /// Reads the registry minimum (`oldest`) under its brief latch, then loads
    /// `next_txn_id` with [`Ordering::Acquire`], matching `capture_snapshot` /
    /// `register_allocated`. The horizon may only *advance* as transactions finish;
    /// a concurrent BEGIN can only register a *newer* (larger) id, so it never lowers
    /// the captured horizon.
    #[allow(dead_code, reason = "consumed by VACUUM in F2/F4")]
    pub fn gc_horizon(&self) -> u64 {
        self.active_txns
            .oldest()
            .unwrap_or_else(|| self.next_txn_id.load(Ordering::Acquire))
    }
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

#[cfg(test)]
mod tests {
    use super::AppState;

    /// `gc_horizon` is the oldest active xid, or `next_txn_id` when the registry is
    /// empty, and advances as the oldest transaction deregisters (`mvcc.md` §9).
    #[test]
    fn gc_horizon_tracks_oldest_active_else_next_id() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        let components = &app.components;

        // No active txns ⇒ horizon is the next id to be assigned (nothing older
        // than the future can be needed).
        components
            .next_txn_id
            .store(42, std::sync::atomic::Ordering::Release);
        assert!(components.active_txns.active_ids().is_empty());
        assert_eq!(components.gc_horizon(), 42);

        // With active txns ⇒ horizon is the oldest, regardless of next_txn_id.
        components.active_txns.register(30);
        components.active_txns.register(50);
        assert_eq!(components.gc_horizon(), 30);

        // After the oldest deregisters the horizon advances to the next-oldest.
        components.active_txns.deregister(30);
        assert_eq!(components.gc_horizon(), 50);

        // After the last deregisters it falls back to next_txn_id again.
        components.active_txns.deregister(50);
        assert_eq!(components.gc_horizon(), 42);
    }
}
