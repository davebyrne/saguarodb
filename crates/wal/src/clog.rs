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

use common::{FIRST_NORMAL_XID, TxnId, TxnStatus, TxnStatusView};

/// In-memory map `txn_id -> TxnStatus`.
///
/// Reserved transaction ids below [`FIRST_NORMAL_XID`] (`INVALID_XID`, the
/// frozen marker, and the gap between them) read as [`TxnStatus::Committed`]:
/// the allocator never hands these out to real transactions, frozen tuples must
/// be visible to every snapshot, and pre-MVCC (row format v1) tuples decode with
/// `xmin = FROZEN_XID`. Any other unrecorded id reads as
/// [`TxnStatus::InProgress`] (it is in flight, or aborted-but-never-recorded —
/// indistinguishable, and treated as not-yet-committed either way).
///
/// **Implicit-committed floor (recovery).** Transactions whose `Commit`/`Abort`
/// records were truncated by a checkpoint are no longer in the rebuilt map, yet
/// their flushed tuples survive in the heap. Per `docs/specs/mvcc.md` §5.4
/// ("transactions older than the horizon are implicitly committed") and the
/// Milestone B flush-gate invariant (uncommitted pages are never flushed, so a
/// surviving tuple is committed), any unrecorded normal id **below**
/// `committed_floor` reads as [`TxnStatus::Committed`]. The floor is set at
/// recovery to the oldest transaction id still present in the (un-truncated) WAL;
/// at runtime it stays `FIRST_NORMAL_XID` (no truncation gap), so live behavior is
/// unchanged.
#[derive(Debug)]
pub struct Clog {
    statuses: HashMap<TxnId, TxnStatus>,
    committed_floor: TxnId,
}

impl Default for Clog {
    fn default() -> Self {
        Self::new()
    }
}

impl Clog {
    pub fn new() -> Self {
        Self {
            statuses: HashMap::new(),
            committed_floor: FIRST_NORMAL_XID,
        }
    }

    /// Raise the implicit-committed floor: unrecorded normal ids strictly below
    /// the floor read as committed (see the type docs). Monotonic — the floor only
    /// ever advances, so a later checkpoint truncation cannot un-settle a
    /// transaction an earlier one already covered. Set at recovery to the
    /// transaction-id allocation boundary, and advanced past each checkpoint's
    /// truncated transactions at runtime.
    pub fn set_committed_floor(&mut self, floor: TxnId) {
        self.committed_floor = self.committed_floor.max(floor).max(FIRST_NORMAL_XID);
    }

    /// The current implicit-committed floor.
    pub fn committed_floor(&self) -> TxnId {
        self.committed_floor
    }

    /// The status of `txn_id`. Reserved ids (`< FIRST_NORMAL_XID`) are always
    /// committed; an unrecorded normal id below the implicit-committed floor is
    /// committed (its `Commit` record was truncated by a checkpoint); any other
    /// unrecorded normal id is `InProgress`.
    pub fn status(&self, txn_id: TxnId) -> TxnStatus {
        if txn_id < FIRST_NORMAL_XID {
            return TxnStatus::Committed;
        }
        if let Some(status) = self.statuses.get(&txn_id) {
            return *status;
        }
        if txn_id < self.committed_floor {
            return TxnStatus::Committed;
        }
        TxnStatus::InProgress
    }

    /// Whether `txn_id` is committed. Equivalent to
    /// `self.status(txn_id) == TxnStatus::Committed`; the redo flush gate uses
    /// this in place of the retired `committed_txns` set.
    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.status(txn_id) == TxnStatus::Committed
    }

    /// Whether `txn_id` is recorded as `Aborted`. Equivalent to
    /// `self.status(txn_id) == TxnStatus::Aborted`. Used by the F4c truncation
    /// relaxation, which floors past an aborted transaction only when it is BELOW
    /// the vacuum floor — never an unrecorded/in-progress id (which is not
    /// `Aborted` here).
    pub fn is_aborted(&self, txn_id: TxnId) -> bool {
        self.status(txn_id) == TxnStatus::Aborted
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

/// The CLOG is the canonical [`TxnStatusView`] for the visibility predicate
/// (`docs/specs/mvcc.md` §6): it answers `status(xid)` with the same reserved-id
/// rule it uses everywhere. This is the impl injected into the storage engine
/// (via the WAL manager handle) so snapshot-aware scans (Milestone B3.6) can probe
/// transaction status per tuple.
impl TxnStatusView for Clog {
    fn status(&self, xid: TxnId) -> TxnStatus {
        Clog::status(self, xid)
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
    fn committed_floor_treats_truncated_ids_as_committed() {
        let mut clog = Clog::new();
        // No floor yet: an unrecorded normal id is in-progress.
        assert_eq!(clog.status(10), TxnStatus::InProgress);

        // Raise the floor to 10: ids below it (whose Commit records were truncated)
        // read as committed; ids at/above stay in-progress.
        clog.set_committed_floor(10);
        assert_eq!(clog.status(9), TxnStatus::Committed);
        assert_eq!(clog.status(10), TxnStatus::InProgress);

        // An explicit Abort below the floor still wins (recorded status is checked
        // before the floor), so a recorded aborted txn is never falsely committed.
        clog.set_aborted(8);
        assert_eq!(clog.status(8), TxnStatus::Aborted);
    }

    #[test]
    fn committed_floor_is_monotonic() {
        let mut clog = Clog::new();
        clog.set_committed_floor(20);
        // A lower floor never lowers the boundary.
        clog.set_committed_floor(5);
        assert_eq!(clog.committed_floor(), 20);
        assert_eq!(clog.status(15), TxnStatus::Committed);
        // Never drops below FIRST_NORMAL_XID.
        let mut fresh = Clog::new();
        fresh.set_committed_floor(0);
        assert_eq!(fresh.committed_floor(), FIRST_NORMAL_XID);
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
