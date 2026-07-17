//! CLOG — the in-memory transaction-status map.
//!
//! The CLOG records, for each transaction id, whether it is `InProgress`,
//! `Committed`, or `Aborted` (see `docs/specs/mvcc.md` §5.4). It is the
//! authoritative transaction-status source, superseding the single-bit
//! `committed_txns` set that previously lived in [`crate::file`].
//!
//! The CLOG is held in memory at runtime, but its outcomes (and floors) are made
//! durable by the CLOG snapshot ([`crate::clog_file`], `clog.dat`): at recovery the
//! map is seeded from that snapshot via [`Clog::from_snapshot`] and brought current
//! by folding the post-snapshot `Commit`/`Abort` records, and [`Clog::live_snapshot`]
//! / [`Clog::prune_to`] produce the next snapshot at each checkpoint. When no
//! snapshot exists on a fresh replay-floor-zero WAL, the map is rebuilt from its
//! retained status records. After recycling, the snapshot is required.

use std::collections::HashMap;

use common::{FIRST_NORMAL_XID, Lsn, TxnId, TxnStatus, TxnStatusView};

use crate::clog_file::ClogSnapshot;

/// Trigger full checkpoint maintenance before either durable status list can
/// approach its one-million-entry format cap.
const CLOG_MAINTENANCE_STATUS_COUNT: usize = 750_000;

fn status_pressure_needs_maintenance(status_count: usize, floor_is_pinned: bool) -> bool {
    status_count >= CLOG_MAINTENANCE_STATUS_COUNT && floor_is_pinned
}

fn status_pressure_blocks_new_writer(
    status_count: usize,
    writer_is_known: bool,
    active_writer_exists: bool,
) -> bool {
    status_count >= CLOG_MAINTENANCE_STATUS_COUNT && !writer_is_known && active_writer_exists
}

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
/// **Implicit-committed floor.** Transactions whose status records are logically
/// below the replay floor (or pruned from the CLOG snapshot) are no longer in the
/// map, yet their flushed tuples survive in the heap. Per `docs/specs/mvcc.md` §5.4
/// ("transactions older than the horizon are implicitly committed"), every
/// unreclaimed abort retains an explicit entry, so any unrecorded normal id **below**
/// `committed_floor` reads as [`TxnStatus::Committed`]. The floor is loaded from the
/// durable CLOG snapshot at recovery (or re-established from an unrecycled fresh WAL)
/// and advances monotonically when a checkpoint prunes the CLOG ([`Clog::prune_to`]).
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
    /// ever advances, so later replay-floor advancement cannot un-settle a
    /// transaction an earlier durable snapshot already covered. It is established
    /// conservatively during recovery and persisted in each CLOG snapshot.
    pub fn set_committed_floor(&mut self, floor: TxnId) {
        self.committed_floor = self.committed_floor.max(floor).max(FIRST_NORMAL_XID);
    }

    /// The current implicit-committed floor.
    pub fn committed_floor(&self) -> TxnId {
        self.committed_floor
    }

    pub(crate) fn needs_maintenance(&self, vacuum_floor: TxnId) -> bool {
        let floor_is_pinned = self.statuses.iter().any(|(id, status)| match status {
            TxnStatus::Aborted => *id >= vacuum_floor,
            TxnStatus::InProgress => true,
            TxnStatus::Committed => false,
        });
        status_pressure_needs_maintenance(self.statuses.len(), floor_is_pinned)
    }

    pub(crate) fn must_backpressure_new_writer(&self, txn_id: TxnId) -> bool {
        status_pressure_blocks_new_writer(
            self.statuses.len(),
            self.statuses.contains_key(&txn_id),
            self.statuses
                .values()
                .any(|status| *status == TxnStatus::InProgress),
        )
    }

    /// The status of `txn_id`. Reserved ids (`< FIRST_NORMAL_XID`) are always
    /// committed; an unrecorded normal id below the implicit-committed floor is
    /// committed (its outcome is covered by the durable CLOG floor even if its old
    /// WAL segment remains physically present); any other
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
    /// `self.status(txn_id) == TxnStatus::Aborted`. Note an unrecorded/in-progress id
    /// is NOT `Aborted` here, so the F4c snapshot pruning (which drops only *recorded*
    /// aborts below the vacuum floor) never mistakes one for a reclaimed abort.
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

    pub fn resolve_all_in_progress_as_aborted(&mut self) {
        for status in self.statuses.values_mut() {
            if *status == TxnStatus::InProgress {
                *status = TxnStatus::Aborted;
            }
        }
    }

    /// Seed a fresh CLOG from a durable [`ClogSnapshot`] loaded at recovery: the
    /// persisted floor plus the live-window statuses. The caller then folds the
    /// post-`clog_lsn` `Commit`/`Abort` records on top (`docs/specs/mvcc.md` §5.4).
    pub fn from_snapshot(snapshot: &ClogSnapshot) -> Self {
        let mut statuses =
            HashMap::with_capacity(snapshot.committed.len() + snapshot.aborted.len());
        for &id in &snapshot.committed {
            statuses.insert(id, TxnStatus::Committed);
        }
        for &id in &snapshot.aborted {
            statuses.insert(id, TxnStatus::Aborted);
        }
        for &id in &snapshot.in_progress {
            statuses.insert(id, TxnStatus::InProgress);
        }
        Self {
            statuses,
            committed_floor: snapshot.committed_floor.max(FIRST_NORMAL_XID),
        }
    }

    /// Compute the durable [`ClogSnapshot`] for the live window **without mutating**.
    ///
    /// The implicit-committed floor it reports advances to the oldest transaction
    /// whose status must stay explicit: the smallest **un-reclaimed** aborted id
    /// (`>= vacuum_floor`), or one past the latest settled status when every aborted
    /// id is reclaimed. Everything below that floor is implicit-committed — genuinely
    /// committed, or an aborted transaction whose on-disk versions a full VACUUM
    /// reclaimed (`docs/specs/mvcc.md` §5.4) — so it is omitted. The floor is
    /// monotonic, so a smaller `vacuum_floor` never lowers it. `clog_lsn` records how
    /// far the persisted statuses have absorbed the WAL. `Committed`, `Aborted`,
    /// and captured `InProgress` entries are persisted explicitly in CLOG v3.
    ///
    /// The caller persists this snapshot and then applies [`Clog::prune_to`] — so a
    /// failed durable write never leaves the in-memory floor advanced past on-disk.
    ///
    /// Captured active IDs and recorded in-progress statuses pin the floor, so this
    /// snapshot is valid while write transactions remain in flight.
    pub fn live_snapshot(
        &self,
        clog_lsn: Lsn,
        authorized_replay_floor: Lsn,
        vacuum_floor: TxnId,
        captured_active: &[TxnId],
        allocation_boundary: TxnId,
    ) -> ClogSnapshot {
        let oldest_status_requiring_explicit_entry = self
            .statuses
            .iter()
            .filter_map(|(id, status)| match status {
                TxnStatus::Aborted if *id >= vacuum_floor => Some(*id),
                TxnStatus::InProgress => Some(*id),
                TxnStatus::Committed | TxnStatus::Aborted => None,
            })
            .min();
        let settled_boundary = self
            .statuses
            .keys()
            .copied()
            .max()
            .map_or(self.committed_floor, |id| id.saturating_add(1));
        let oldest_active = captured_active.iter().copied().min();
        let floor = oldest_status_requiring_explicit_entry
            .into_iter()
            .chain(oldest_active)
            .min()
            .unwrap_or(settled_boundary)
            .min(allocation_boundary)
            .max(self.committed_floor)
            .max(FIRST_NORMAL_XID);

        let mut committed = Vec::new();
        let mut aborted = Vec::new();
        let mut in_progress = Vec::new();
        for (id, status) in &self.statuses {
            if *id < floor {
                continue;
            }
            match status {
                TxnStatus::Committed => committed.push(*id),
                TxnStatus::Aborted => aborted.push(*id),
                TxnStatus::InProgress => in_progress.push(*id),
            }
        }
        for &id in captured_active {
            if id >= floor && self.status(id) == TxnStatus::InProgress && !in_progress.contains(&id)
            {
                in_progress.push(id);
            }
        }
        committed.sort_unstable();
        aborted.sort_unstable();
        in_progress.sort_unstable();

        ClogSnapshot {
            clog_lsn,
            authorized_replay_floor,
            committed_floor: floor,
            vacuum_floor,
            committed,
            aborted,
            in_progress,
        }
    }

    /// Apply the prune reported by [`Clog::live_snapshot`]: advance the (monotonic)
    /// implicit-committed floor and drop every entry below it. Call only after the
    /// snapshot is durable.
    pub fn prune_to(&mut self, committed_floor: TxnId) {
        self.set_committed_floor(committed_floor);
        let floor = self.committed_floor;
        self.statuses.retain(|id, _| *id >= floor);
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
    use common::{FIRST_NORMAL_XID, FROZEN_XID, INVALID_XID, Lsn, TxnId, TxnStatus};

    use super::{
        CLOG_MAINTENANCE_STATUS_COUNT, Clog, ClogSnapshot, status_pressure_blocks_new_writer,
        status_pressure_needs_maintenance,
    };

    #[test]
    fn requests_maintenance_only_when_status_pressure_has_a_floor_pin() {
        assert!(!status_pressure_needs_maintenance(
            CLOG_MAINTENANCE_STATUS_COUNT - 1,
            true,
        ));
        assert!(!status_pressure_needs_maintenance(
            CLOG_MAINTENANCE_STATUS_COUNT,
            false,
        ));
        assert!(status_pressure_needs_maintenance(
            CLOG_MAINTENANCE_STATUS_COUNT,
            true,
        ));
    }

    #[test]
    fn pressure_blocks_only_new_writers_while_an_active_writer_pins_the_floor() {
        assert!(!status_pressure_blocks_new_writer(
            CLOG_MAINTENANCE_STATUS_COUNT - 1,
            false,
            true,
        ));
        assert!(status_pressure_blocks_new_writer(
            CLOG_MAINTENANCE_STATUS_COUNT,
            false,
            true,
        ));
        assert!(!status_pressure_blocks_new_writer(
            CLOG_MAINTENANCE_STATUS_COUNT,
            true,
            true,
        ));
        assert!(!status_pressure_blocks_new_writer(
            CLOG_MAINTENANCE_STATUS_COUNT,
            false,
            false,
        ));
    }

    #[test]
    fn commit_only_pressure_does_not_request_vacuum_but_an_unreclaimed_abort_does() {
        let mut clog = Clog::new();
        let limit = u64::try_from(CLOG_MAINTENANCE_STATUS_COUNT).unwrap();
        for xid in FIRST_NORMAL_XID..FIRST_NORMAL_XID + limit {
            clog.set_committed(xid);
        }
        assert!(!clog.needs_maintenance(FIRST_NORMAL_XID));

        clog.set_aborted(FIRST_NORMAL_XID);
        assert!(clog.needs_maintenance(FIRST_NORMAL_XID));
        assert!(!clog.needs_maintenance(FIRST_NORMAL_XID + 1));
    }

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
    fn committed_floor_treats_covered_ids_as_committed() {
        let mut clog = Clog::new();
        // No floor yet: an unrecorded normal id is in-progress.
        assert_eq!(clog.status(10), TxnStatus::InProgress);

        // Raise the floor to 10: ids below it (whose outcomes are durably covered)
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

    /// Compute a snapshot then apply its prune, mirroring how `checkpoint_clog`
    /// write-then-mutates. Returns the snapshot.
    fn snapshot_and_prune(clog: &mut Clog, clog_lsn: Lsn, vacuum_floor: TxnId) -> ClogSnapshot {
        let snapshot = clog.live_snapshot(clog_lsn, clog_lsn, vacuum_floor, &[], u64::MAX);
        clog.prune_to(snapshot.committed_floor);
        snapshot
    }

    #[test]
    fn live_snapshot_does_not_mutate() {
        let mut clog = Clog::new();
        clog.set_committed(10);
        clog.set_committed(20);
        let snapshot = clog.live_snapshot(99, 99, 15, &[], u64::MAX);
        // The floor it reports is not yet applied: the in-memory floor is unchanged
        // and id 10 still reads from its explicit entry, not the implicit floor.
        assert_eq!(snapshot.committed_floor, 21);
        assert_eq!(clog.committed_floor(), FIRST_NORMAL_XID);
        assert_eq!(clog.status(10), TxnStatus::Committed);
    }

    #[test]
    fn prune_with_no_aborts_advances_past_all_settled_commits() {
        let mut clog = Clog::new();
        clog.set_committed(10);
        clog.set_committed(20);

        // No aborts: every settled id can become implicit-committed, independent of
        // the vacuum floor, so commit-only workloads keep a bounded live window.
        let snapshot = snapshot_and_prune(&mut clog, 99, 15);
        assert_eq!(snapshot.committed_floor, 21);
        assert_eq!(snapshot.vacuum_floor, 15);
        assert_eq!(snapshot.clog_lsn, 99);
        assert!(snapshot.committed.is_empty());
        assert!(snapshot.aborted.is_empty());
        // Both ids now read implicit-committed.
        assert_eq!(clog.status(10), TxnStatus::Committed);
        assert_eq!(clog.status(20), TxnStatus::Committed);
    }

    #[test]
    fn commit_only_snapshot_has_a_bounded_live_window_without_vacuum() {
        let mut clog = Clog::new();
        for xid in FIRST_NORMAL_XID..10_003 {
            clog.set_committed(xid);
        }

        let snapshot = snapshot_and_prune(&mut clog, 99, FIRST_NORMAL_XID);
        assert_eq!(snapshot.committed_floor, 10_003);
        assert!(snapshot.committed.is_empty());
        assert!(snapshot.aborted.is_empty());
        assert_eq!(clog.status(10_002), TxnStatus::Committed);
    }

    #[test]
    fn prune_pins_floor_at_oldest_unreclaimed_abort() {
        let mut clog = Clog::new();
        clog.set_committed(10);
        clog.set_aborted(12); // unreclaimed (>= vacuum floor)
        clog.set_aborted(14);
        clog.set_committed(16);

        // vacuum floor 11: aborts 12 and 14 are un-reclaimed, so the floor stops at 12.
        let snapshot = snapshot_and_prune(&mut clog, 50, 11);
        assert_eq!(snapshot.committed_floor, 12);
        assert_eq!(snapshot.committed, vec![16]); // 10 dropped (below floor)
        assert_eq!(snapshot.aborted, vec![12, 14]);
        assert_eq!(clog.status(10), TxnStatus::Committed); // implicit
        assert_eq!(clog.status(12), TxnStatus::Aborted); // explicit, must survive
    }

    #[test]
    fn prune_drops_reclaimed_aborts_below_vacuum_floor() {
        let mut clog = Clog::new();
        clog.set_aborted(8); // reclaimed (< vacuum floor 10)
        clog.set_aborted(13); // un-reclaimed
        clog.set_committed(15);

        let snapshot = snapshot_and_prune(&mut clog, 50, 10);
        // Reclaimed abort 8 is gone; the floor stops at the un-reclaimed abort 13.
        assert_eq!(snapshot.committed_floor, 13);
        assert_eq!(snapshot.aborted, vec![13]);
        assert_eq!(snapshot.committed, vec![15]);
        // The reclaimed abort now reads implicit-committed (vacuously correct).
        assert_eq!(clog.status(8), TxnStatus::Committed);
    }

    #[test]
    fn prune_keeps_floor_monotonic() {
        let mut clog = Clog::new();
        clog.set_committed_floor(30);
        // A smaller vacuum floor must not lower the established floor.
        let snapshot = snapshot_and_prune(&mut clog, 1, 5);
        assert_eq!(snapshot.committed_floor, 30);
        assert_eq!(clog.committed_floor(), 30);
    }

    #[test]
    fn from_snapshot_restores_statuses_and_floor() {
        let mut source = Clog::new();
        source.set_committed(20);
        source.set_aborted(22);
        let snapshot = snapshot_and_prune(&mut source, 7, 18);

        let restored = Clog::from_snapshot(&snapshot);
        assert_eq!(restored.committed_floor(), snapshot.committed_floor);
        assert_eq!(restored.status(20), TxnStatus::Committed);
        assert_eq!(restored.status(22), TxnStatus::Aborted);
        // Below the floor reads implicit-committed, same as the source after pruning.
        assert_eq!(restored.status(5), TxnStatus::Committed);
        // An id at/above the floor that was not recorded is in-progress.
        assert_eq!(restored.status(25), TxnStatus::InProgress);
    }

    #[test]
    fn snapshot_retains_in_progress_entries() {
        let mut clog = Clog::new();
        clog.set_in_progress(40);
        clog.set_committed(41);
        let snapshot = snapshot_and_prune(&mut clog, 1, 39);
        // Recovery must resolve every captured in-progress transaction to aborted,
        // including one whose physical records fall below the page redo boundary.
        assert!(!snapshot.committed.contains(&40));
        assert!(!snapshot.aborted.contains(&40));
        assert_eq!(snapshot.in_progress, vec![40]);
        assert_eq!(snapshot.committed, vec![41]);
    }
}
