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

    /// Deregister `txn_id`. Called on commit or rollback.
    pub fn deregister(&self, txn_id: TxnId) {
        self.lock().remove(&txn_id);
    }

    /// The oldest in-progress transaction id, or `None` if none are active.
    ///
    /// This is the GC horizon source for Milestone F; it has no consumer in
    /// Milestone A but is the reason the registry is an ordered set.
    #[allow(dead_code, reason = "GC horizon consumer arrives in Milestone F")]
    pub fn oldest(&self) -> Option<TxnId> {
        self.lock().iter().next().copied()
    }

    /// A snapshot of the currently active ids, ascending.
    ///
    /// Snapshot capture (B3/C3) will read this to populate `xip`; unused until
    /// then.
    #[allow(dead_code, reason = "snapshot capture consumer arrives in B3/C3")]
    pub fn active_ids(&self) -> Vec<TxnId> {
        self.lock().iter().copied().collect()
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
