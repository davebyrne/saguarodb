//! Active-transaction registry.
//!
//! Tracks the set of currently in-progress transaction ids. It feeds two later
//! consumers (see `docs/specs/mvcc.md` §5.5, §9):
//!
//! - **Snapshot capture** (Milestones B3/C3) reads the active set to compute a
//!   snapshot's `xmin`/`xip`.
//! - **The GC horizon** (Milestone F) is the oldest active `xmin`, so the set is
//!   backed by a [`BTreeSet`] for an `O(log n)` minimum.
//!
//! In Milestone A it is wired into the autocommit lifecycle for bookkeeping
//! (insert on begin, remove on commit/rollback) but is not yet consulted, so the
//! external behavior is unchanged.

use std::collections::BTreeSet;
use std::sync::Mutex;

use common::TxnId;

/// A concurrent set of in-progress transaction ids with a cheap minimum.
#[derive(Debug, Default)]
pub struct ActiveTxnRegistry {
    active: Mutex<BTreeSet<TxnId>>,
}

impl ActiveTxnRegistry {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(BTreeSet::new()),
        }
    }

    /// Register `txn_id` as in-progress. Called when an autocommit unit begins.
    pub fn register(&self, txn_id: TxnId) {
        self.lock().insert(txn_id);
    }

    /// Allocate a transaction id and register it as in-progress atomically under
    /// the registry latch (`docs/specs/mvcc.md` §7.1).
    ///
    /// `allocate` is invoked while the latch is held; it must advance the id
    /// allocator (e.g. `next_txn_id.fetch_add(1)`) and return the new id. Doing
    /// the increment and the registration under one latch closes the torn-snapshot
    /// window: a concurrent [`snapshot_with_boundary`](Self::snapshot_with_boundary),
    /// which also takes the latch, can never observe the advanced allocator
    /// boundary without also observing this transaction in the active set. Without
    /// the shared latch a reader could read `xmax` after the increment but the
    /// active set before the insert, wrongly treating the new writer as a settled
    /// past transaction.
    pub fn register_allocated<F>(&self, allocate: F) -> TxnId
    where
        F: FnOnce() -> TxnId,
    {
        let mut guard = self.lock();
        let txn_id = allocate();
        guard.insert(txn_id);
        txn_id
    }

    /// Deregister `txn_id`. Called on commit or rollback.
    pub fn deregister(&self, txn_id: TxnId) {
        self.lock().remove(&txn_id);
    }

    /// The oldest in-progress transaction id, or `None` if none are active.
    ///
    /// This is the GC horizon source for Milestone F: it backs
    /// [`ServerComponents::gc_horizon`](crate::app::ServerComponents::gc_horizon),
    /// and is the reason the registry is an ordered set.
    pub fn oldest(&self) -> Option<TxnId> {
        self.lock().iter().next().copied()
    }

    /// A snapshot of the currently active ids, ascending.
    pub fn active_ids(&self) -> Vec<TxnId> {
        self.lock().iter().copied().collect()
    }

    /// Capture the active set together with an allocator boundary computed under
    /// the registry latch, so snapshot capture is not torn relative to a
    /// concurrent `register` (`docs/specs/mvcc.md` §7.1).
    ///
    /// `boundary` is invoked while the latch is held, *after* the active set is
    /// read; the caller passes a closure that loads `next_txn_id`. Holding the
    /// latch across both reads guarantees that any transaction registered before
    /// the boundary is observed is also present in the returned active set — so a
    /// concurrently-begun writer can never be both absent from `xip` and `< xmax`
    /// (which would wrongly make its uncommitted writes visible). Reading the
    /// active set first and the boundary second keeps every active id `< boundary`
    /// (the allocator only grows).
    pub fn snapshot_with_boundary<F>(&self, boundary: F) -> (Vec<TxnId>, TxnId)
    where
        F: FnOnce() -> TxnId,
    {
        let guard = self.lock();
        let active: Vec<TxnId> = guard.iter().copied().collect();
        let xmax = boundary();
        (active, xmax)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeSet<TxnId>> {
        // A poisoned registry mutex means a panic left the active set possibly
        // inconsistent; recovering the guard is the least-bad option (the set is
        // advisory bookkeeping, not a durability structure).
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::ActiveTxnRegistry;

    #[test]
    fn register_and_deregister_track_membership() {
        let registry = ActiveTxnRegistry::new();
        registry.register(5);
        registry.register(3);
        assert_eq!(registry.active_ids(), vec![3, 5]);
        assert_eq!(registry.oldest(), Some(3));

        registry.deregister(3);
        assert_eq!(registry.active_ids(), vec![5]);
        assert_eq!(registry.oldest(), Some(5));

        registry.deregister(5);
        assert!(registry.active_ids().is_empty());
        assert_eq!(registry.oldest(), None);
    }
}
