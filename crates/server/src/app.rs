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
    /// Dead MVCC versions produced by committed statements since the last
    /// auto-prune (`docs/specs/mvcc.md` §9, Milestone F4b). Each committed `DELETE`
    /// row and each committed `UPDATE` row creates one dead version; this counts
    /// those (incremented only on a successful commit, never on abort). When a
    /// checkpoint runs and this reaches `config.auto_vacuum_dead_rows`, the
    /// checkpoint vacuums every user table under its exclusive guard and resets this
    /// to 0. A purely additive proxy: it never needs a scan to decide whether to
    /// prune, and over-counting (e.g. a version a live snapshot still pins, so it is
    /// not yet reclaimable) only triggers an extra, harmless pass.
    pub dead_rows_since_vacuum: AtomicU64,
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
    /// The GC horizon (`docs/specs/mvcc.md` §9): the **minimum `xmin` advertised by
    /// any currently-live snapshot**, below which no live snapshot can see a
    /// committed delete as undone — so a committed-deleted version with
    /// `xmax < horizon` is dead to everyone (see [`common::is_dead_to_all`]). It is
    /// captured **once** at the start of a VACUUM pass.
    ///
    /// This is **not** the oldest active transaction id. Every snapshot freezes its
    /// `xmin` at capture (`xmin = oldest active id then`, or `next_txn_id` if none),
    /// and that frozen `xmin` can be *below* the oldest active id once the
    /// then-oldest transaction finishes; an autocommit `SELECT` is not even its own
    /// transaction, so it never appears in the active set at all. A version a still-
    /// live snapshot sees as live (its committed deleter `xmax` is in that
    /// snapshot's `xip`) must not be reclaimed. Using `oldest()` (the active-id min)
    /// could exceed a live snapshot's frozen `xmin` and reclaim such a version
    /// (data loss). The min advertised `xmin` is always `<= oldest()`, so this is
    /// strictly safer and never reclaims anything the old rule retained.
    ///
    /// When no snapshot is advertised there is nothing to protect, so the horizon
    /// is the next id to be assigned — nothing older than the future can be needed.
    /// Reads the min advertised `xmin` ([`ActiveTxnRegistry::oldest_xmin`]) under
    /// the registry's brief latch, else loads `next_txn_id` with
    /// [`Ordering::Acquire`], matching `capture` / `register_allocated`.
    ///
    /// **Race-freedom** (`docs/specs/mvcc.md` §9): at the instant this reads the min
    /// advertised `xmin` `H` under the registry latch, every snapshot that is live
    /// OR being captured has `xmin >= H` or is not-yet-usable. A capture publishes
    /// `xmins[xmin]++` in the *same* latched critical section that reads the active
    /// set ([`ActiveTxnRegistry::capture`]), and its snapshot is not returned/usable
    /// until that section completes; this reads `oldest_xmin()` under that same
    /// latch, so the mutex total order leaves no window where the horizon exceeds a
    /// usable snapshot's `xmin`. A snapshot published *after* this read derives its
    /// `xmin` from an `active`/`next_txn_id` state in which any already-finished
    /// transaction — including any committed deleter this horizon could reclaim — is
    /// settled-past, so that later snapshot's `xmin` is above any reclaimed `xmax`
    /// and it cannot see a reclaimed version live.
    ///
    /// [`ActiveTxnRegistry::oldest_xmin`]: crate::registry::ActiveTxnRegistry::oldest_xmin
    /// [`ActiveTxnRegistry::capture`]: crate::registry::ActiveTxnRegistry::capture
    pub fn gc_horizon(&self) -> u64 {
        self.active_txns
            .oldest_xmin()
            .unwrap_or_else(|| self.next_txn_id.load(Ordering::Acquire))
    }

    /// Add `count` dead MVCC versions to the auto-prune accumulator
    /// (`dead_rows_since_vacuum`, `docs/specs/mvcc.md` §9, Milestone F4b). Called by
    /// the commit paths only on a successful, durable commit (an aborted statement
    /// never reaches the call), so the counter reflects committed dead versions. A
    /// zero `count` is a cheap no-op.
    pub fn add_dead_versions(&self, count: u64) {
        if count != 0 {
            self.dead_rows_since_vacuum
                .fetch_add(count, Ordering::Relaxed);
        }
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
    use common::{Snapshot, TxnStatus, TxnStatusView};

    use super::AppState;

    /// `gc_horizon` is the **minimum advertised snapshot `xmin`**, or `next_txn_id`
    /// when no snapshot is advertised — NOT the oldest active id (`mvcc.md` §9).
    /// Crucially, it stays pinned at a live snapshot's frozen `xmin` even after the
    /// then-oldest transaction deregisters and the active-id minimum advances above
    /// it; it only advances when the snapshot's advertisement is released.
    #[test]
    fn gc_horizon_tracks_min_advertised_xmin_else_next_id() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        let components = &app.components;

        // No advertised snapshot ⇒ horizon is the next id to be assigned (nothing
        // older than the future can be needed).
        components
            .next_txn_id
            .store(42, std::sync::atomic::Ordering::Release);
        assert!(components.active_txns.active_ids().is_empty());
        assert_eq!(components.gc_horizon(), 42);

        // Two transactions are active; a snapshot captured now freezes xmin = 50.
        components.active_txns.register(50);
        components.active_txns.register(70);
        let (_active, _xmax, guard) = components.active_txns.capture(|| 90);
        assert_eq!(guard.xmin(), 50);
        assert_eq!(components.gc_horizon(), 50);

        // The then-oldest active id (50) deregisters: the ACTIVE-ID minimum advances
        // to 70, but the horizon must STAY at 50 — the live snapshot still has
        // xmin = 50 and would lose a row if VACUUM advanced past it. This is the
        // exact difference between the old (buggy) `oldest()` rule and the fix.
        components.active_txns.deregister(50);
        assert_eq!(components.active_txns.oldest(), Some(70));
        assert_eq!(
            components.gc_horizon(),
            50,
            "horizon is pinned by the live snapshot's frozen xmin, not the active-id min"
        );

        // Dropping the snapshot's advertisement releases the pin; with no snapshot
        // and id 70 still active, the horizon falls back to next_txn_id (42).
        drop(guard);
        assert_eq!(components.gc_horizon(), 42);
    }

    /// A mock CLOG view that reports a fixed committed set, otherwise InProgress,
    /// honouring the reserved `< FIRST_NORMAL_XID ⇒ Committed` rule.
    struct CommittedView(Vec<u64>);

    impl TxnStatusView for CommittedView {
        fn status(&self, xid: u64) -> TxnStatus {
            if xid < common::ids::FIRST_NORMAL_XID || self.0.contains(&xid) {
                TxnStatus::Committed
            } else {
                TxnStatus::InProgress
            }
        }
    }

    /// The data-loss bug, end-to-end at the horizon/predicate level (Path A): a
    /// long-lived snapshot S with xmin = X and a committed deleter Y ∈ S.xip sees
    /// the deleted version LIVE. While S is advertised, `is_dead_to_all(.., xmax=Y,
    /// gc_horizon())` MUST be false (the version is retained). Only after S's
    /// advertisement is released does the horizon advance past Y and the version
    /// become reclaimable.
    ///
    /// Under the old `oldest()` rule, once the snapshot-owning/older active ids
    /// finished, `gc_horizon()` would jump to `next_txn_id` (> Y) while S is still
    /// live, so `is_dead_to_all` would return true and VACUUM would reclaim a row S
    /// sees live — the data loss this fix prevents.
    #[test]
    fn live_snapshot_pins_committed_delete_against_reclaim() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        let components = &app.components;

        // The deleter Y = 50 is committed; the deleted version has xmin = 10
        // (committed creator), xmax = 50 (committed delete). No infomask hints.
        let creator = 10u64;
        let deleter = 50u64;
        let view = CommittedView(vec![creator, deleter]);

        // A long-lived reader captures a snapshot WHILE Y is still active, so its
        // xmin freezes at 50 and Y ∈ xip ⇒ the reader sees the version live.
        components.active_txns.register(deleter);
        let (active, xmax, snapshot_guard) = components.active_txns.capture(|| 100);
        let snapshot = Snapshot {
            xmin: active.first().copied().unwrap_or(xmax),
            xmax,
            xip: active,
        };
        assert_eq!(snapshot.xmin, 50);
        assert!(
            snapshot.xip.contains(&deleter),
            "the reader sees Y in-flight"
        );
        // The reader genuinely sees the row as live (the delete is not effective).
        assert!(
            common::is_visible(creator, deleter, 0, &snapshot, &[], &view),
            "the deleted version is live to the long-lived snapshot"
        );

        // Y now commits its delete and deregisters. next_txn_id has advanced (a few
        // later transactions ran), so the active-id minimum / next_txn_id is well
        // above Y — exactly the condition under which the OLD horizon rule would
        // reclaim the row.
        components.active_txns.deregister(deleter);
        components
            .next_txn_id
            .store(200, std::sync::atomic::Ordering::Release);

        // While the snapshot is advertised, the horizon is pinned at 50, so the
        // committed delete (xmax = 50) is NOT below the horizon (50 < 50 is false)
        // ⇒ the version is retained. The row the snapshot sees live survives.
        let horizon = components.gc_horizon();
        assert_eq!(horizon, 50, "live snapshot pins the horizon");
        assert!(
            !common::is_dead_to_all(creator, deleter, 0, horizon, &view),
            "a version a live snapshot sees live must NOT be reclaimable"
        );

        // Once the snapshot is done (advertisement released), the horizon advances
        // past the deleter and the version finally becomes reclaimable.
        drop(snapshot_guard);
        let horizon = components.gc_horizon();
        assert_eq!(horizon, 200, "no snapshot ⇒ horizon is next_txn_id");
        assert!(
            common::is_dead_to_all(creator, deleter, 0, horizon, &view),
            "after the snapshot ends the committed delete is reclaimable"
        );
    }
}
