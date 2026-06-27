//! VACUUM reclaimability oracle (`is_dead_to_all`).

use crate::ids::INVALID_XID;

use super::{TxnStatus, TxnStatusView, XMAX_COMMITTED, XMIN_ABORTED};

/// The pure VACUUM **reclaimability** predicate of `docs/specs/mvcc.md` §9: is a
/// version with creator `xmin` and deleter `xmax` *dead to every possible
/// snapshot* — and therefore safe to physically reclaim — given the GC `horizon`
/// (the minimum `xmin` advertised by any live snapshot, `mvcc.md` §9; equivalently,
/// no version with `xmax < horizon` is seen live by any current snapshot)?
///
/// This is the **sibling of [`is_visible`]** but asks a different question.
/// [`is_visible`] answers "is this version visible to **my** snapshot?" — a
/// snapshot-relative read. `is_dead_to_all` answers "is this version invisible to
/// **everyone**, now and for the rest of its on-disk life?" — the VACUUM oracle.
/// A version that `is_dead_to_all` may be pruned, its index entries vacuumed, and
/// its line pointer reclaimed without any live or future snapshot ever missing it.
///
/// A version is reclaimable iff **either**:
///
/// - **its creator aborted** — `XMIN_ABORTED` hint, or `status(xmin) == Aborted`.
///   An aborted creator's version was never visible to any snapshot (no snapshot
///   can ever see an aborted creator), so it is dead to everyone the instant the
///   abort is settled. **There is no age requirement here**: unlike a committed
///   delete, the creator's `xmin` need not be below the horizon — reclaimability
///   does not depend on `xmin`'s relation to any live snapshot, because *no*
///   snapshot (past, present, or future) could see it. **or**
/// - **it is committed-deleted below the horizon** — `xmax != INVALID_XID` **and**
///   the delete is settled-committed (`XMAX_COMMITTED` hint, or
///   `status(xmax) == Committed`) **and** `xmax < horizon` (strict). The
///   `< horizon` gate is **required**: a delete with `xmax >= horizon` may still
///   fall in some live snapshot's future/in-progress set, so that snapshot still
///   sees the row as *live* and the version must be retained.
///
/// **The asymmetry** (aborted-creator needs no age; committed-delete needs
/// `< horizon`) follows from *which* xid a live snapshot could care about. An
/// aborted creator is universally invisible — no snapshot's `xmin`/`xip`/`xmax`
/// can resurrect it — so age is irrelevant. A committed delete only hides the row
/// from snapshots that consider the deleter settled-and-past; a snapshot taken at
/// or before `xmax` still sees the pre-delete row, and the horizon is the oldest
/// such snapshot still alive, so the delete is universally effective only once
/// `xmax < horizon`.
///
/// Everything else is **not (yet) reclaimable**: a live committed version
/// (`xmax == INVALID_XID`) is never reclaimable; an aborted-deleter or
/// in-progress-deleter (the delete did not commit) leaves the row alive; and a
/// committed delete with `xmax >= horizon` is not *yet* reclaimable.
///
/// **Hint bits** short-circuit the CLOG probe exactly as in [`is_visible`]:
/// `XMIN_ABORTED` settles the aborted-creator branch and `XMAX_COMMITTED` settles
/// the committed-delete branch, each without calling [`TxnStatusView::status`].
///
/// Pure: no [`Snapshot`] (the single scalar `horizon` summarizes every live
/// snapshot), no I/O beyond whatever the caller's [`TxnStatusView`] probe takes.
pub fn is_dead_to_all(
    xmin: u64,
    xmax: u64,
    infomask: u16,
    horizon: u64,
    status: &dyn TxnStatusView,
) -> bool {
    // Aborted-creator branch (no horizon gate): an aborted creator is invisible to
    // every snapshot regardless of age. The `XMIN_ABORTED` hint settles it without
    // a CLOG probe.
    if infomask & XMIN_ABORTED != 0 {
        return true;
    }
    // Committed-delete branch: a delete settled-committed and strictly below the
    // horizon is universally effective — every live snapshot considers the deleter
    // settled-and-past, so none still sees the row as live. A delete at or above
    // the horizon may still be live to some snapshot ⇒ retain. Evaluated before the
    // (potential) `xmin` CLOG probe so a `XMAX_COMMITTED` hint short-circuits the
    // whole predicate without ever touching the view: both branches return "dead",
    // so a hint-settled committed delete reclaims regardless of `xmin`'s status.
    if xmax != INVALID_XID
        && xmax < horizon
        && (infomask & XMAX_COMMITTED != 0 || status.status(xmax) == TxnStatus::Committed)
    {
        return true;
    }
    // No settled hint for either branch and the delete did not reclaim: fall back
    // to the CLOG for the aborted-creator branch.
    status.status(xmin) == TxnStatus::Aborted
}
