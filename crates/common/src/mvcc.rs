//! Runtime MVCC types shared across crates.
//!
//! These types are scaffolding for the MVCC effort (see `docs/specs/mvcc.md`).
//! They are defined now (Milestone A2) so later milestones can build on a stable
//! shape, but they are not yet consulted by any visibility or transaction logic.
//! The snapshot capture (B3/C3) and CLOG-backed status (A3) wire them in.
//!
//! These are deliberately runtime-only: no `serde`/durable-encoding derives. The
//! CLOG on-disk representation of transaction status is a later task (A3).

use crate::ids::{INVALID_XID, TxnId};

/// `infomask` hint bits that cache settled CLOG status in the v2 tuple header, so
/// the visibility predicate can skip a CLOG probe (see `docs/specs/mvcc.md` §6).
///
/// These four bits are the **single source of truth** shared between the storage
/// tuple codec (`crates/storage/src/codec.rs`, which writes/reads the header) and
/// the [`is_visible`] predicate (which honours them). The HOT-reserved bits
/// (`HEAP_ONLY`, `HOT_UPDATED`) stay storage-private because the predicate never
/// consults them.
///
/// ```text
/// bit 0: XMIN_COMMITTED  bit 1: XMIN_ABORTED
/// bit 2: XMAX_COMMITTED  bit 3: XMAX_ABORTED
/// ```
///
/// `xmin` is settled-committed (skip the CLOG probe for the creator).
pub const XMIN_COMMITTED: u16 = 1 << 0;
/// `xmin` is settled-aborted (the creator's version is never visible).
pub const XMIN_ABORTED: u16 = 1 << 1;
/// `xmax` is settled-committed (the deleter committed; the version may be hidden).
pub const XMAX_COMMITTED: u16 = 1 << 2;
/// `xmax` is settled-aborted (the delete never happened; it cannot hide the row).
pub const XMAX_ABORTED: u16 = 1 << 3;

/// A read-only view of transaction status, used by [`is_visible`] without
/// `common` depending on `wal`. The CLOG-backed implementation lives in
/// `crates/wal` (`impl TxnStatusView for Clog`); a mock implementation backs the
/// predicate's unit tests.
///
/// Reserved ids below [`FIRST_NORMAL_XID`](crate::ids::FIRST_NORMAL_XID)
/// (including [`FROZEN_XID`](crate::ids::FROZEN_XID)) must read as
/// [`TxnStatus::Committed`], consistent with the CLOG (§5.4): the allocator never
/// hands them out, frozen tuples must be visible to every snapshot, and pre-MVCC
/// (row format v1) tuples decode with `xmin = FROZEN_XID`.
pub trait TxnStatusView {
    /// The status of `xid` (`Committed`/`Aborted`/`InProgress`).
    fn status(&self, xid: TxnId) -> TxnStatus;

    /// Whether `xid` is committed. Convenience over [`TxnStatusView::status`].
    fn is_committed(&self, xid: TxnId) -> bool {
        self.status(xid) == TxnStatus::Committed
    }

    /// Whether `xid` is aborted. Convenience over [`TxnStatusView::status`].
    fn is_aborted(&self, xid: TxnId) -> bool {
        self.status(xid) == TxnStatus::Aborted
    }
}

/// Whether transaction `xid` that created (or deleted) a version is **visible** to
/// the transaction holding `snapshot` — i.e. its effect is settled-and-in-the-past
/// from that snapshot's perspective. This is the shared "creator is visible" test
/// of `docs/specs/mvcc.md` §6, applied to both `xmin` (creator) and `xmax`
/// (deleter):
///
/// `xid` is visible iff it is `current_txn` itself (an own write), **or**
/// `xid < snapshot.xmax` (not in the future) **and** `xid ∉ snapshot.xip` (not
/// in-progress at snapshot capture) **and** `status(xid) == Committed`.
///
/// `committed_hint`/`aborted_hint` are the already-resolved infomask hint bits for
/// `xid` (the caller checks `XMIN_*`/`XMAX_*` as appropriate): a settled hint
/// short-circuits the [`TxnStatusView`] probe. At most one hint is ever set for a
/// given xid.
fn txn_effect_visible(
    xid: TxnId,
    snapshot: &Snapshot,
    current_txn: TxnId,
    status: &dyn TxnStatusView,
    committed_hint: bool,
    aborted_hint: bool,
) -> bool {
    // Own write: my uncommitted effects are visible to me regardless of CLOG.
    if xid == current_txn {
        return true;
    }
    // The future (allocated at or after my snapshot) is never visible.
    if xid >= snapshot.xmax {
        return false;
    }
    // In-progress at snapshot capture: not yet committed from my perspective.
    if snapshot.xip.contains(&xid) {
        return false;
    }
    // Settled-status decision: prefer the infomask hint, else probe the CLOG.
    // (Reserved/frozen xids resolve to Committed via the status view's rule.)
    if committed_hint {
        return true;
    }
    if aborted_hint {
        return false;
    }
    status.status(xid) == TxnStatus::Committed
}

/// The pure tuple-visibility predicate of `docs/specs/mvcc.md` §6.
///
/// A version with creator `xmin` and deleter `xmax` is visible to the transaction
/// `current_txn` holding `snapshot` iff:
///
/// 1. **Creator is visible** — `xmin` is `current_txn` (own write), or it is
///    settled-committed and in the snapshot's past (`xmin < snapshot.xmax`,
///    `xmin ∉ snapshot.xip`, `status(xmin) == Committed`). If the creator is
///    in-progress, aborted, or in the future, the version is invisible.
/// 2. **Deleter does not hide it** — `xmax` is invalid/zero (the version is live),
///    or `xmax` is *not* visible by the same test (the delete is in the future,
///    in-progress to others, or aborted). If `xmax` **is** visible (a committed,
///    past delete) the version is hidden.
///
/// **Own-delete (`xmax == current_txn`):** because `txn_effect_visible` treats an
/// own write as visible, a delete I performed *does* hide the row from me. This is
/// the simplification the spec allows here: with no command ids yet, we model
/// "`xmax == current_txn` ⇒ hidden" unconditionally and defer the Read-Committed
/// command-visibility nuance ("the delete happened *earlier* in my own history")
/// to Milestone G. See `docs/specs/mvcc.md` §6.
///
/// **Hint bits** short-circuit the CLOG probe: if `infomask` records
/// `XMIN_COMMITTED`/`XMIN_ABORTED` (resp. `XMAX_*`), that settled status is used
/// instead of calling [`TxnStatusView::status`] for that xid.
pub fn is_visible(
    xmin: TxnId,
    xmax: TxnId,
    infomask: u16,
    snapshot: &Snapshot,
    current_txn: TxnId,
    status: &dyn TxnStatusView,
) -> bool {
    // 1. Creator must be visible, else the version never existed for me.
    let creator_visible = txn_effect_visible(
        xmin,
        snapshot,
        current_txn,
        status,
        infomask & XMIN_COMMITTED != 0,
        infomask & XMIN_ABORTED != 0,
    );
    if !creator_visible {
        return false;
    }

    // 2. A live row (no deleter) is visible. Otherwise the delete hides the row
    //    exactly when the deleter itself is visible (settled-committed in my past,
    //    or my own delete).
    if xmax == INVALID_XID {
        return true;
    }
    let deleter_visible = txn_effect_visible(
        xmax,
        snapshot,
        current_txn,
        status,
        infomask & XMAX_COMMITTED != 0,
        infomask & XMAX_ABORTED != 0,
    );
    !deleter_visible
}

/// A point-in-time view of which transactions are visible, in the Postgres
/// `{xmin, xmax, xip}` style (see `docs/specs/mvcc.md` §5.5, §6).
///
/// A transaction id is settled (committed or aborted via CLOG) below `xmin`,
/// invisible at or above `xmax` (the future), and in-progress if it appears in
/// `xip`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Lowest still-running xid; below this, status is settled via CLOG.
    pub xmin: TxnId,
    /// Next xid to be assigned; `>= xmax` is invisible (the future).
    pub xmax: TxnId,
    /// In-progress xids in `[xmin, xmax)` at snapshot capture.
    pub xip: Vec<TxnId>,
}

impl Snapshot {
    /// A degenerate, non-capture placeholder snapshot (`xmin = xmax = 0`, no
    /// in-progress xids). It is not a real captured snapshot; it exists so that
    /// pre-MVCC call sites can construct a [`StatementContext`](crate::StatementContext)
    /// before snapshot capture is wired in (B3/C3).
    pub fn empty() -> Self {
        Self {
            xmin: 0,
            xmax: 0,
            xip: Vec::new(),
        }
    }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::empty()
    }
}

/// The committed/aborted/in-progress status of a transaction, as recorded by the
/// CLOG (see `docs/specs/mvcc.md` §5.4). Consulted by the visibility predicate in
/// later milestones (B3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnStatus {
    InProgress,
    Committed,
    Aborted,
}

/// Transaction isolation level. `RepeatableRead` is snapshot isolation: one
/// snapshot captured at the first statement and reused (see `docs/specs/mvcc.md`
/// §6, Milestone G).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    /// = snapshot isolation.
    RepeatableRead,
}

impl Default for IsolationLevel {
    /// Postgres' default; the only level honored until Milestone G.
    fn default() -> Self {
        Self::ReadCommitted
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use crate::ids::{FIRST_NORMAL_XID, FROZEN_XID, INVALID_XID, TxnId};

    use super::{
        IsolationLevel, Snapshot, TxnStatus, TxnStatusView, XMAX_ABORTED, XMAX_COMMITTED,
        XMIN_ABORTED, XMIN_COMMITTED, is_visible,
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
}
