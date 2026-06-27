//! Tuple-visibility predicate (`is_visible`) and the `infomask` hint-bit
//! constants it consults.

use crate::ids::{INVALID_XID, TxnId};

use super::{Snapshot, TxnStatus, TxnStatusView};

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
