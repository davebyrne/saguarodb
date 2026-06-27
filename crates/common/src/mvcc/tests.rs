use std::cell::Cell;
use std::collections::HashMap;

use crate::ids::{FIRST_NORMAL_XID, FROZEN_XID, INVALID_XID, TxnId};

use super::{
    IsolationLevel, Snapshot, TxnStatus, TxnStatusView, UniqueConflict, WriteConflict,
    XMAX_ABORTED, XMAX_COMMITTED, XMIN_ABORTED, XMIN_COMMITTED, classify_unique_conflict,
    is_dead_to_all, is_visible, version_conflicts, write_conflict,
};

#[test]
fn empty_snapshot_is_a_degenerate_non_capture() {
    let snap = Snapshot::empty();
    assert_eq!(snap.xmin, 0);
    assert_eq!(snap.xmax, 0);
    assert!(snap.xip.is_empty());
    assert_eq!(Snapshot::default(), snap);
}

#[test]
fn snapshot_equality_compares_all_fields() {
    let a = Snapshot {
        xmin: 3,
        xmax: 7,
        xip: vec![4, 5],
    };
    let b = a.clone();
    assert_eq!(a, b);
    assert_ne!(a, Snapshot::empty());
}

#[test]
fn isolation_level_default_is_read_committed() {
    assert_eq!(IsolationLevel::default(), IsolationLevel::ReadCommitted);
    assert_ne!(
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead
    );
}

#[test]
fn txn_status_variants_are_distinct() {
    assert_ne!(TxnStatus::InProgress, TxnStatus::Committed);
    assert_ne!(TxnStatus::Committed, TxnStatus::Aborted);
    assert_eq!(TxnStatus::Aborted, TxnStatus::Aborted);
}

/// A mock [`TxnStatusView`] backed by an explicit map, honouring the reserved
/// `< FIRST_NORMAL_XID` ⇒ `Committed` rule (so frozen/reserved ids resolve the
/// same way the CLOG-backed impl does). It records every probe so tests can
/// assert that hint-bit short-circuiting avoids the probe entirely.
struct MockStatus {
    statuses: HashMap<TxnId, TxnStatus>,
    probes: Cell<usize>,
}

impl MockStatus {
    fn new(entries: &[(TxnId, TxnStatus)]) -> Self {
        Self {
            statuses: entries.iter().copied().collect(),
            probes: Cell::new(0),
        }
    }

    fn probe_count(&self) -> usize {
        self.probes.get()
    }
}

impl TxnStatusView for MockStatus {
    fn status(&self, xid: TxnId) -> TxnStatus {
        self.probes.set(self.probes.get() + 1);
        if xid < FIRST_NORMAL_XID {
            return TxnStatus::Committed;
        }
        self.statuses
            .get(&xid)
            .copied()
            .unwrap_or(TxnStatus::InProgress)
    }
}

/// A status view that panics if consulted — used to prove the hint-bit path
/// short-circuits the CLOG probe entirely.
struct PanicStatus;

impl TxnStatusView for PanicStatus {
    fn status(&self, xid: TxnId) -> TxnStatus {
        panic!("status() must not be consulted; xid={xid}");
    }
}

/// `{xmin: 5, xmax: 20, xip: [8, 12]}`: ids 8 and 12 are in-progress, 20+ is
/// the future, everything below 20 and outside `xip` is settled via the view.
fn snapshot() -> Snapshot {
    Snapshot {
        xmin: 5,
        xmax: 20,
        xip: vec![8, 12],
    }
}

#[test]
fn creator_committed_and_in_past_is_visible_when_not_deleted() {
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    assert!(is_visible(7, INVALID_XID, 0, &snapshot(), 100, &view));
}

#[test]
fn creator_in_progress_to_me_is_invisible() {
    // 8 ∈ xip ⇒ in-progress at snapshot capture, regardless of CLOG.
    let view = MockStatus::new(&[(8, TxnStatus::Committed)]);
    assert!(!is_visible(8, INVALID_XID, 0, &snapshot(), 100, &view));
}

#[test]
fn creator_in_the_future_is_invisible() {
    // 20 == snapshot.xmax (the future); 25 > xmax. Both invisible.
    let view = MockStatus::new(&[(20, TxnStatus::Committed), (25, TxnStatus::Committed)]);
    assert!(!is_visible(20, INVALID_XID, 0, &snapshot(), 100, &view));
    assert!(!is_visible(25, INVALID_XID, 0, &snapshot(), 100, &view));
}

#[test]
fn creator_aborted_is_invisible() {
    let view = MockStatus::new(&[(7, TxnStatus::Aborted)]);
    assert!(!is_visible(7, INVALID_XID, 0, &snapshot(), 100, &view));
}

#[test]
fn creator_in_progress_unrecorded_is_invisible() {
    // 7 < xmax and ∉ xip, but the view reports InProgress (unrecorded).
    let view = MockStatus::new(&[]);
    assert!(!is_visible(7, INVALID_XID, 0, &snapshot(), 100, &view));
}

#[test]
fn own_write_is_visible_even_if_uncommitted() {
    // current_txn creates the row; CLOG would say InProgress, but it's mine.
    let view = MockStatus::new(&[]);
    let current = 30;
    assert!(is_visible(
        current,
        INVALID_XID,
        0,
        &snapshot(),
        current,
        &view
    ));
}

#[test]
fn deleter_invalid_does_not_hide() {
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    assert!(is_visible(7, INVALID_XID, 0, &snapshot(), 100, &view));
}

#[test]
fn deleter_committed_and_visible_hides_the_row() {
    // Creator committed-past (visible); deleter 9 committed-past (visible) ⇒
    // the delete is effective ⇒ the row is hidden.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Committed)]);
    assert!(!is_visible(7, 9, 0, &snapshot(), 100, &view));
}

#[test]
fn deleter_aborted_does_not_hide() {
    // Deleter 9 aborted ⇒ the delete never happened ⇒ row still visible.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Aborted)]);
    assert!(is_visible(7, 9, 0, &snapshot(), 100, &view));
}

#[test]
fn deleter_in_progress_to_others_does_not_hide() {
    // Deleter 8 ∈ xip (in-progress to me) ⇒ delete not effective ⇒ visible.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (8, TxnStatus::Committed)]);
    assert!(is_visible(7, 8, 0, &snapshot(), 100, &view));
}

#[test]
fn deleter_in_the_future_does_not_hide() {
    // Deleter 25 is in the future ⇒ delete not effective ⇒ visible.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (25, TxnStatus::Committed)]);
    assert!(is_visible(7, 25, 0, &snapshot(), 100, &view));
}

#[test]
fn deleter_is_me_hides_the_row() {
    // I deleted it (xmax == current_txn) ⇒ hidden from me (no command ids yet;
    // the RC "delete happened earlier in my history" nuance is deferred to G).
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    let current = 30;
    assert!(!is_visible(7, current, 0, &snapshot(), current, &view));
}

#[test]
fn frozen_and_reserved_xmin_is_visible() {
    // Reserved ids (< FIRST_NORMAL_XID) read as Committed via the view's rule.
    let view = MockStatus::new(&[]);
    assert!(is_visible(
        FROZEN_XID,
        INVALID_XID,
        0,
        &snapshot(),
        100,
        &view
    ));
    assert!(is_visible(
        FIRST_NORMAL_XID - 1,
        INVALID_XID,
        0,
        &snapshot(),
        100,
        &view
    ));
}

#[test]
fn xmin_committed_hint_short_circuits_clog_probe() {
    // Hint says committed; the view must NOT be consulted for xmin. 7 < xmax
    // and ∉ xip, so only the settled-status decision differs — and the hint
    // supplies it. PanicStatus proves status() is never called.
    assert!(is_visible(
        7,
        INVALID_XID,
        XMIN_COMMITTED,
        &snapshot(),
        100,
        &PanicStatus
    ));
}

#[test]
fn xmin_aborted_hint_short_circuits_clog_probe() {
    assert!(!is_visible(
        7,
        INVALID_XID,
        XMIN_ABORTED,
        &snapshot(),
        100,
        &PanicStatus
    ));
}

#[test]
fn xmax_committed_hint_short_circuits_clog_probe() {
    // xmin committed via hint, xmax committed via hint ⇒ deleter visible ⇒
    // hidden. No CLOG probe for either xid.
    assert!(!is_visible(
        7,
        9,
        XMIN_COMMITTED | XMAX_COMMITTED,
        &snapshot(),
        100,
        &PanicStatus
    ));
}

#[test]
fn xmax_aborted_hint_short_circuits_clog_probe() {
    // Deleter aborted via hint ⇒ not hidden. xmin committed via hint. No probe.
    assert!(is_visible(
        7,
        9,
        XMIN_COMMITTED | XMAX_ABORTED,
        &snapshot(),
        100,
        &PanicStatus
    ));
}

#[test]
fn hint_matches_clog_answer_and_avoids_probe() {
    // The hinted result must equal the CLOG-probe result for the same state.
    let snap = snapshot();
    // creator committed, deleter committed ⇒ hidden, by CLOG:
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Committed)]);
    let by_clog = is_visible(7, 9, 0, &snap, 100, &view);
    assert_eq!(view.probe_count(), 2, "both xids probed without hints");
    // same answer, with hints, never touching the view:
    let by_hint = is_visible(
        7,
        9,
        XMIN_COMMITTED | XMAX_COMMITTED,
        &snap,
        100,
        &PanicStatus,
    );
    assert_eq!(by_clog, by_hint);
    assert!(!by_hint);
}

// --- version_conflicts (uniqueness liveness check) ---

const CURRENT_TXN: TxnId = 100;

#[test]
fn conflict_committed_live_version_conflicts() {
    // Creator committed, no deleter ⇒ alive ⇒ conflict.
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    assert!(version_conflicts(7, INVALID_XID, 0, CURRENT_TXN, &view));
}

#[test]
fn conflict_aborted_creator_does_not_conflict() {
    // Creator aborted ⇒ the version never really existed ⇒ no conflict.
    let view = MockStatus::new(&[(7, TxnStatus::Aborted)]);
    assert!(!version_conflicts(7, INVALID_XID, 0, CURRENT_TXN, &view));
}

#[test]
fn conflict_committed_deleted_version_does_not_conflict() {
    // Creator committed but a committed delete removed it ⇒ no conflict.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Committed)]);
    assert!(!version_conflicts(7, 9, 0, CURRENT_TXN, &view));
}

#[test]
fn conflict_aborted_delete_still_conflicts() {
    // Creator committed; the delete aborted (the row is still alive) ⇒ conflict.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Aborted)]);
    assert!(version_conflicts(7, 9, 0, CURRENT_TXN, &view));
}

#[test]
fn conflict_in_progress_delete_still_conflicts() {
    // Creator committed; another in-progress txn is deleting it but has not
    // committed (it may yet roll back) ⇒ still potentially-alive ⇒ conflict.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::InProgress)]);
    assert!(version_conflicts(7, 9, 0, CURRENT_TXN, &view));
}

#[test]
fn conflict_in_progress_creator_conflicts() {
    // A concurrent inserter's in-flight version (creator in-progress, not me)
    // is potentially-alive ⇒ conflict.
    let view = MockStatus::new(&[(7, TxnStatus::InProgress)]);
    assert!(version_conflicts(7, INVALID_XID, 0, CURRENT_TXN, &view));
}

#[test]
fn conflict_own_live_write_conflicts() {
    // A live version I created myself still occupies the key ⇒ conflict.
    let view = MockStatus::new(&[]);
    assert!(version_conflicts(
        CURRENT_TXN,
        INVALID_XID,
        0,
        CURRENT_TXN,
        &view
    ));
}

#[test]
fn conflict_deleted_by_me_does_not_conflict() {
    // I created and then deleted this version earlier in my own txn; with no
    // command ids, deleted-by-me counts as gone ⇒ I may re-insert the key.
    let view = MockStatus::new(&[]);
    assert!(!version_conflicts(
        CURRENT_TXN,
        CURRENT_TXN,
        0,
        CURRENT_TXN,
        &view
    ));
}

#[test]
fn conflict_honours_hint_bits_without_probing() {
    // Aborted-creator hint ⇒ no conflict, no CLOG probe.
    assert!(!version_conflicts(
        7,
        INVALID_XID,
        XMIN_ABORTED,
        CURRENT_TXN,
        &PanicStatus
    ));
    // The aborted-delete hint must NOT short-circuit to "no conflict": an
    // aborted delete leaves the row alive. The creator status is still probed,
    // so use a live view rather than PanicStatus here.
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    assert!(version_conflicts(7, 9, XMAX_ABORTED, CURRENT_TXN, &view));
}

#[test]
fn conflict_committed_delete_hint_short_circuits() {
    // Committed-delete hint ⇒ no conflict without probing the deleter; the
    // creator status is still consulted (committed here).
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    assert!(!version_conflicts(7, 9, XMAX_COMMITTED, CURRENT_TXN, &view));
}

#[test]
fn conflict_is_not_snapshot_relative() {
    // A version whose creator is in the "future" relative to some snapshot
    // (id >= a snapshot's xmax) still conflicts if it is committed/alive —
    // proving the check is liveness-based, not snapshot-relative. (There is no
    // snapshot argument to pass; this documents the contract.)
    let view = MockStatus::new(&[(50, TxnStatus::Committed)]);
    assert!(version_conflicts(50, INVALID_XID, 0, CURRENT_TXN, &view));
}

// --- classify_unique_conflict (3-way concurrent-inserter resolution, §7.3) ---
//
// The boolean `version_conflicts` is just `classify != None`; these tests pin
// the Violation-vs-InFlight split it cannot express. A creator that is committed
// / own / frozen is a definite duplicate (Violation ⇒ 23505); a creator that is
// ANOTHER still-running txn is undecidable (InFlight ⇒ 40001).

#[test]
fn classify_aborted_creator_is_none() {
    // Creator aborted (CLOG) ⇒ dead ⇒ no conflict.
    let view = MockStatus::new(&[(7, TxnStatus::Aborted)]);
    assert_eq!(
        classify_unique_conflict(7, INVALID_XID, 0, CURRENT_TXN, &view),
        UniqueConflict::None
    );
}

#[test]
fn classify_aborted_creator_hint_is_none_without_probe() {
    // XMIN_ABORTED hint ⇒ dead, no CLOG probe.
    assert_eq!(
        classify_unique_conflict(7, INVALID_XID, XMIN_ABORTED, CURRENT_TXN, &PanicStatus),
        UniqueConflict::None
    );
}

#[test]
fn classify_committed_deleted_is_none() {
    // Committed creator, committed delete ⇒ the row is gone ⇒ no conflict.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Committed)]);
    assert_eq!(
        classify_unique_conflict(7, 9, 0, CURRENT_TXN, &view),
        UniqueConflict::None
    );
}

#[test]
fn classify_own_deleted_old_version_is_none() {
    // An UPDATE's superseded old version: created by me, deleted by me
    // (xmax == current_txn) ⇒ own-deleted ⇒ dead ⇒ no false self-conflict.
    // PanicStatus proves the own-write fast paths avoid the CLOG.
    assert_eq!(
        classify_unique_conflict(CURRENT_TXN, CURRENT_TXN, 0, CURRENT_TXN, &PanicStatus),
        UniqueConflict::None
    );
}

#[test]
fn classify_committed_live_is_violation() {
    // Committed creator, no deleter ⇒ alive AND definitely a duplicate ⇒ 23505.
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    assert_eq!(
        classify_unique_conflict(7, INVALID_XID, 0, CURRENT_TXN, &view),
        UniqueConflict::Violation
    );
}

#[test]
fn classify_own_alive_version_is_violation() {
    // A live version I created myself (xmin == current_txn) still occupies the
    // key ⇒ a real duplicate within my own txn ⇒ Violation, no CLOG probe.
    assert_eq!(
        classify_unique_conflict(CURRENT_TXN, INVALID_XID, 0, CURRENT_TXN, &PanicStatus),
        UniqueConflict::Violation
    );
}

#[test]
fn classify_frozen_creator_is_violation() {
    // A frozen/reserved creator reads Committed via the status rule ⇒ a live
    // pre-MVCC/frozen tuple is a definite duplicate.
    let view = MockStatus::new(&[]);
    assert_eq!(
        classify_unique_conflict(FROZEN_XID, INVALID_XID, 0, CURRENT_TXN, &view),
        UniqueConflict::Violation
    );
    assert_eq!(
        classify_unique_conflict(FIRST_NORMAL_XID - 1, INVALID_XID, 0, CURRENT_TXN, &view),
        UniqueConflict::Violation
    );
}

#[test]
fn classify_committed_hint_is_violation_without_probe() {
    // XMIN_COMMITTED hint ⇒ Violation without probing the creator.
    assert_eq!(
        classify_unique_conflict(7, INVALID_XID, XMIN_COMMITTED, CURRENT_TXN, &PanicStatus),
        UniqueConflict::Violation
    );
}

#[test]
fn classify_another_in_progress_creator_is_in_flight() {
    // The key is held only by ANOTHER txn's still-running, uncommitted insert
    // (creator in-progress, not me, not committed) ⇒ uniqueness undecidable ⇒
    // 40001 (it may yet abort).
    let view = MockStatus::new(&[(7, TxnStatus::InProgress)]);
    assert_eq!(
        classify_unique_conflict(7, INVALID_XID, 0, CURRENT_TXN, &view),
        UniqueConflict::InFlight
    );
}

#[test]
fn classify_another_in_progress_creator_unrecorded_is_in_flight() {
    // An unrecorded creator reads InProgress (the CLOG default) ⇒ InFlight.
    let view = MockStatus::new(&[]);
    assert_eq!(
        classify_unique_conflict(7, INVALID_XID, 0, CURRENT_TXN, &view),
        UniqueConflict::InFlight
    );
}

#[test]
fn classify_in_progress_creator_with_aborted_delete_is_in_flight() {
    // Another txn's in-progress creator whose (in-progress) delete aborted is
    // still alive-but-undecidable ⇒ InFlight, not Violation. Guards against the
    // aborted-delete branch reclassifying the creator.
    let view = MockStatus::new(&[(7, TxnStatus::InProgress), (9, TxnStatus::Aborted)]);
    assert_eq!(
        classify_unique_conflict(7, 9, 0, CURRENT_TXN, &view),
        UniqueConflict::InFlight
    );
}

#[test]
fn classify_committed_creator_with_in_progress_delete_is_violation() {
    // Committed creator whose delete is only in-progress (may roll back) is still
    // alive ⇒ a definite duplicate of a committed version ⇒ Violation.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::InProgress)]);
    assert_eq!(
        classify_unique_conflict(7, 9, 0, CURRENT_TXN, &view),
        UniqueConflict::Violation
    );
}

#[test]
fn classify_matches_version_conflicts_boolean() {
    // The boolean `version_conflicts` must agree with `classify != None` across
    // the representative states (the documented relationship).
    let view = MockStatus::new(&[
        (7, TxnStatus::Committed),
        (8, TxnStatus::InProgress),
        (9, TxnStatus::Aborted),
    ]);
    for (xmin, xmax) in [
        (7u64, INVALID_XID), // committed-live  -> Violation -> conflict
        (8, INVALID_XID),    // in-progress     -> InFlight  -> conflict
        (9, INVALID_XID),    // aborted creator -> None      -> no conflict
        (7, 9),              // committed, aborted delete -> Violation -> conflict
    ] {
        let classified =
            classify_unique_conflict(xmin, xmax, 0, CURRENT_TXN, &view) != UniqueConflict::None;
        let boolean = version_conflicts(xmin, xmax, 0, CURRENT_TXN, &view);
        assert_eq!(classified, boolean, "xmin={xmin} xmax={xmax}");
    }
}

// --- is_dead_to_all (VACUUM reclaimability, §9) ---
//
// The horizon is the oldest still-running xid. A version is reclaimable iff its
// creator aborted (ANY age — no horizon gate) OR it is committed-deleted with
// `xmax < horizon` (strict). These pin the asymmetry and the strict boundary.

const HORIZON: u64 = 50;

#[test]
fn dead_aborted_creator_below_horizon_is_reclaimable() {
    // Creator aborted (CLOG); xmin < horizon. No snapshot can see an aborted
    // creator ⇒ reclaimable regardless of the deleter.
    let view = MockStatus::new(&[(7, TxnStatus::Aborted)]);
    assert!(is_dead_to_all(7, INVALID_XID, 0, HORIZON, &view));
}

#[test]
fn dead_aborted_creator_above_horizon_is_reclaimable() {
    // Creator aborted but xmin > horizon: proves there is NO age requirement
    // for an aborted creator — it is dead to everyone the instant the abort is
    // settled, whatever its relation to the horizon.
    // xmin = 80 > HORIZON (50).
    let view = MockStatus::new(&[(80, TxnStatus::Aborted)]);
    assert!(is_dead_to_all(80, INVALID_XID, 0, HORIZON, &view));
}

#[test]
fn dead_committed_delete_below_horizon_is_reclaimable() {
    // Committed creator, committed delete with xmax = 9 < HORIZON (50) ⇒ every
    // live snapshot considers the delete settled-and-past ⇒ reclaimable.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Committed)]);
    assert!(is_dead_to_all(7, 9, 0, HORIZON, &view));
}

#[test]
fn dead_committed_delete_above_horizon_is_not_reclaimable() {
    // Committed delete but xmax = 80 > HORIZON (50): some live snapshot may still
    // place the deleter in its future/in-progress set and see the row as live ⇒
    // NOT reclaimable.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (80, TxnStatus::Committed)]);
    assert!(!is_dead_to_all(7, 80, 0, HORIZON, &view));
}

#[test]
fn dead_committed_delete_at_horizon_is_not_reclaimable() {
    // Boundary: xmax == horizon. The gate is `< horizon` (strict), so the
    // snapshot whose xmax == horizon still sees the row as live ⇒ NOT yet
    // reclaimable.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (HORIZON, TxnStatus::Committed)]);
    assert!(!is_dead_to_all(7, HORIZON, 0, HORIZON, &view));
}

#[test]
fn dead_live_committed_version_is_not_reclaimable() {
    // Committed creator, no deleter (xmax == INVALID) ⇒ alive ⇒ never
    // reclaimable, however old.
    let view = MockStatus::new(&[(7, TxnStatus::Committed)]);
    assert!(!is_dead_to_all(7, INVALID_XID, 0, HORIZON, &view));
}

#[test]
fn dead_aborted_deleter_is_not_reclaimable() {
    // The delete aborted ⇒ the row is still alive ⇒ not reclaimable (even
    // though the deleter's xmax = 9 < HORIZON).
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::Aborted)]);
    assert!(!is_dead_to_all(7, 9, 0, HORIZON, &view));
}

#[test]
fn dead_in_progress_deleter_is_not_reclaimable() {
    // The delete is only in-progress (may yet roll back) ⇒ the row is still
    // alive ⇒ not reclaimable, even with xmax = 9 < HORIZON.
    let view = MockStatus::new(&[(7, TxnStatus::Committed), (9, TxnStatus::InProgress)]);
    assert!(!is_dead_to_all(7, 9, 0, HORIZON, &view));
}

#[test]
fn dead_aborted_creator_hint_short_circuits_clog_probe() {
    // XMIN_ABORTED hint ⇒ reclaimable without consulting the view. PanicStatus
    // proves status() is never called.
    assert!(is_dead_to_all(
        7,
        INVALID_XID,
        XMIN_ABORTED,
        HORIZON,
        &PanicStatus
    ));
}

#[test]
fn dead_committed_delete_hint_short_circuits_clog_probe() {
    // XMAX_COMMITTED hint with xmax < horizon ⇒ reclaimable without any probe.
    // The committed-delete branch is evaluated before the `xmin` CLOG fallback,
    // so a hint-settled committed delete reclaims regardless of `xmin`'s status
    // (both branches mean "dead"). PanicStatus proves status() is never called
    // for either xid. (xmax = 9 < HORIZON.)
    assert!(is_dead_to_all(7, 9, XMAX_COMMITTED, HORIZON, &PanicStatus));
}

// --- write_conflict (write-write row-lock check, first-updater-wins) ---

#[test]
fn write_conflict_invalid_xmax_proceeds() {
    // No deleter stamped ⇒ the row is unlocked ⇒ I am the first updater.
    let view = MockStatus::new(&[]);
    assert_eq!(
        write_conflict(INVALID_XID, 0, CURRENT_TXN, &view),
        WriteConflict::Proceed
    );
}

#[test]
fn write_conflict_self_lock_proceeds() {
    // I already locked/deleted it earlier in my own txn ⇒ proceed. The view
    // would report InProgress for me, so PanicStatus proves it is not probed.
    assert_eq!(
        write_conflict(CURRENT_TXN, 0, CURRENT_TXN, &PanicStatus),
        WriteConflict::Proceed
    );
}

#[test]
fn write_conflict_deleter_aborted_proceeds() {
    // The lock holder aborted ⇒ its delete never happened ⇒ the row is free.
    let view = MockStatus::new(&[(9, TxnStatus::Aborted)]);
    assert_eq!(
        write_conflict(9, 0, CURRENT_TXN, &view),
        WriteConflict::Proceed
    );
}

#[test]
fn write_conflict_deleter_committed_conflicts() {
    // The lock holder committed its delete ⇒ another txn beat me ⇒ conflict.
    let view = MockStatus::new(&[(9, TxnStatus::Committed)]);
    assert_eq!(
        write_conflict(9, 0, CURRENT_TXN, &view),
        WriteConflict::Conflict
    );
}

#[test]
fn write_conflict_deleter_in_progress_conflicts() {
    // Another live writer holds the row lock (delete not yet committed) ⇒
    // fail-fast conflict (no blocking).
    let view = MockStatus::new(&[(9, TxnStatus::InProgress)]);
    assert_eq!(
        write_conflict(9, 0, CURRENT_TXN, &view),
        WriteConflict::Conflict
    );
}

#[test]
fn write_conflict_aborted_hint_short_circuits_clog_probe() {
    // XMAX_ABORTED hint ⇒ Proceed without probing the deleter. PanicStatus
    // proves status() is never consulted.
    assert_eq!(
        write_conflict(9, XMAX_ABORTED, CURRENT_TXN, &PanicStatus),
        WriteConflict::Proceed
    );
}

#[test]
fn write_conflict_committed_hint_short_circuits_clog_probe() {
    // XMAX_COMMITTED hint ⇒ Conflict without probing the deleter. The aborted
    // check is `XMAX_ABORTED` (unset) OR `status == Aborted`; the committed
    // hint means status would return Committed, so the predicate must reach
    // its fall-through Conflict WITHOUT a probe. PanicStatus proves it.
    assert_eq!(
        write_conflict(9, XMAX_COMMITTED, CURRENT_TXN, &PanicStatus),
        WriteConflict::Conflict
    );
}

#[test]
fn write_conflict_reserved_frozen_xmax_conflicts() {
    // Edge case, not a real runtime value (a real `xmax` is 0 or a normal
    // xid): a reserved/frozen `xmax` reads Committed via the status rule, so
    // it classifies as Conflict. Documents the fall-through is correct.
    let view = MockStatus::new(&[]);
    assert_eq!(
        write_conflict(FROZEN_XID, 0, CURRENT_TXN, &view),
        WriteConflict::Conflict
    );
    assert_eq!(
        write_conflict(FIRST_NORMAL_XID - 1, 0, CURRENT_TXN, &view),
        WriteConflict::Conflict
    );
}
