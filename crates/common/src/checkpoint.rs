use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::{DbError, Lsn, Result, TxnId};

/// Redo pins for generic catalog WAL records whose transaction has not finished
/// publishing (or restoring) its catalog/storage state.
#[derive(Clone, Debug, Default)]
pub struct CatalogRedoTracker {
    pending: Arc<Mutex<BTreeMap<TxnId, Lsn>>>,
}

impl CatalogRedoTracker {
    pub fn register(&self, txn_id: TxnId, replay_from: Lsn) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| DbError::internal("catalog redo tracker lock was poisoned"))?;
        pending
            .entry(txn_id)
            .and_modify(|current| *current = (*current).min(replay_from))
            .or_insert(replay_from);
        Ok(())
    }

    pub fn resolve(&self, txn_id: TxnId) -> Result<()> {
        self.pending
            .lock()
            .map_err(|_| DbError::internal("catalog redo tracker lock was poisoned"))?
            .remove(&txn_id);
        Ok(())
    }

    pub fn oldest_pending(&self) -> Result<Option<Lsn>> {
        Ok(self
            .pending
            .lock()
            .map_err(|_| DbError::internal("catalog redo tracker lock was poisoned"))?
            .values()
            .copied()
            .min())
    }
}

#[cfg(test)]
mod tests {
    use super::CatalogRedoTracker;

    #[test]
    fn retains_each_transactions_earliest_boundary() {
        let tracker = CatalogRedoTracker::default();
        tracker.register(7, 40).unwrap();
        tracker.register(7, 60).unwrap();
        tracker.register(7, 20).unwrap();
        tracker.register(8, 30).unwrap();

        assert_eq!(tracker.oldest_pending().unwrap(), Some(20));
        tracker.resolve(7).unwrap();
        assert_eq!(tracker.oldest_pending().unwrap(), Some(30));
        tracker.resolve(8).unwrap();
        assert_eq!(tracker.oldest_pending().unwrap(), None);
    }

    #[test]
    fn clones_share_pending_state() {
        let tracker = CatalogRedoTracker::default();
        let clone = tracker.clone();
        tracker.register(9, 11).unwrap();
        assert_eq!(clone.oldest_pending().unwrap(), Some(11));
        clone.resolve(9).unwrap();
        assert_eq!(tracker.oldest_pending().unwrap(), None);
    }
}
