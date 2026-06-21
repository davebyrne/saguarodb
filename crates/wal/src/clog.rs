//! CLOG — the in-memory transaction-status map.
//!
//! The CLOG records, for each transaction id, whether it is `InProgress`,
//! `Committed`, or `Aborted` (see `docs/specs/mvcc.md` §5.4). It is the
//! authoritative transaction-status source, superseding the single-bit
//! `committed_txns` set that previously lived in [`crate::file`].
//!
//! For the A–D MVP the CLOG is kept **in memory** and rebuilt at recovery by
//! scanning `Commit`/`Abort` WAL records — those durable records remain the
//! source of truth for transaction outcome. A standalone durable CLOG file and
//! its truncation are only needed for GC (Milestone F) and to bound recovery
//! scans, so they are deferred to F; nothing reads a durable CLOG yet because
//! recovery rebuilds the map from the WAL regardless.

use std::collections::HashMap;

use common::{FIRST_NORMAL_XID, TxnId, TxnStatus};

/// In-memory map `txn_id -> TxnStatus`.
///
/// Reserved transaction ids below [`FIRST_NORMAL_XID`] (`INVALID_XID`, the
/// frozen marker, and the gap between them) read as [`TxnStatus::Committed`]:
/// the allocator never hands these out to real transactions, frozen tuples must
/// be visible to every snapshot, and pre-MVCC (row format v1) tuples decode with
/// `xmin = FROZEN_XID`. Any other unrecorded id reads as
/// [`TxnStatus::InProgress`] (it is in flight, or aborted-but-never-recorded —
/// indistinguishable, and treated as not-yet-committed either way).
#[derive(Debug, Default)]
pub struct Clog {
    statuses: HashMap<TxnId, TxnStatus>,
}

impl Clog {
    pub fn new() -> Self {
        Self {
            statuses: HashMap::new(),
        }
    }

    /// The status of `txn_id`. Reserved ids (`< FIRST_NORMAL_XID`) are always
    /// committed; an unrecorded normal id is `InProgress`.
    pub fn status(&self, txn_id: TxnId) -> TxnStatus {
        if txn_id < FIRST_NORMAL_XID {
            return TxnStatus::Committed;
        }
        self.statuses
            .get(&txn_id)
            .copied()
            .unwrap_or(TxnStatus::InProgress)
    }

    /// Whether `txn_id` is committed. Equivalent to
    /// `self.status(txn_id) == TxnStatus::Committed`; the redo flush gate uses
    /// this in place of the retired `committed_txns` set.
    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.status(txn_id) == TxnStatus::Committed
    }

    /// Record `txn_id` as in-progress (statement/transaction begin).
    pub fn set_in_progress(&mut self, txn_id: TxnId) {
        self.statuses.insert(txn_id, TxnStatus::InProgress);
    }

    /// Record `txn_id` as committed (durable `Commit`).
    pub fn set_committed(&mut self, txn_id: TxnId) {
        self.statuses.insert(txn_id, TxnStatus::Committed);
    }

    /// Record `txn_id` as aborted (`Abort` / rollback).
    pub fn set_aborted(&mut self, txn_id: TxnId) {
        self.statuses.insert(txn_id, TxnStatus::Aborted);
    }
}

#[cfg(test)]
mod tests {
    use common::{FIRST_NORMAL_XID, FROZEN_XID, INVALID_XID, TxnStatus};

    use super::Clog;

    #[test]
    fn reserved_ids_read_as_committed() {
        let clog = Clog::new();
        assert_eq!(clog.status(INVALID_XID), TxnStatus::Committed);
        assert_eq!(clog.status(FROZEN_XID), TxnStatus::Committed);
        assert_eq!(clog.status(FIRST_NORMAL_XID - 1), TxnStatus::Committed);
        assert!(clog.is_committed(FROZEN_XID));
    }

    #[test]
    fn unknown_normal_id_is_in_progress() {
        let clog = Clog::new();
        assert_eq!(clog.status(FIRST_NORMAL_XID), TxnStatus::InProgress);
        assert!(!clog.is_committed(FIRST_NORMAL_XID));
    }

    #[test]
    fn recorded_status_is_returned() {
        let mut clog = Clog::new();
        clog.set_in_progress(10);
        assert_eq!(clog.status(10), TxnStatus::InProgress);
        clog.set_committed(10);
        assert_eq!(clog.status(10), TxnStatus::Committed);
        assert!(clog.is_committed(10));
        clog.set_aborted(11);
        assert_eq!(clog.status(11), TxnStatus::Aborted);
        assert!(!clog.is_committed(11));
    }
}
