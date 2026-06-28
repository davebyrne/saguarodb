//! Write-conflict and uniqueness-conflict classifiers.

use crate::ids::{INVALID_XID, TxnId};

use super::{TxnStatus, TxnStatusView, XMAX_ABORTED, XMAX_COMMITTED, XMIN_ABORTED, XMIN_COMMITTED};

/// The visibility-aware **uniqueness conflict** predicate of `docs/specs/mvcc.md`
/// §6/§7.3: whether a candidate index version with creator `xmin` and deleter
/// `xmax` is **alive or potentially-alive** and therefore conflicts with an
/// inserting transaction `current_txn` trying to claim the same unique key.
///
/// This is a **liveness ("dirty") check, not a snapshot-relative read**. Unique
/// enforcement must consider concurrently in-flight and already-committed state —
/// not just what `current_txn`'s snapshot sees — so it takes **no [`Snapshot`]**
/// and decides committed/aborted purely from the [`TxnStatusView`] (CLOG) and the
/// `infomask` hint bits. Do not route it through [`is_visible`].
///
/// A candidate is **definitively dead ⇒ no conflict** iff either:
/// - its creator is **aborted** (`status(xmin) == Aborted`, or `XMIN_ABORTED`) —
///   the row never really existed; or
/// - it is **committed-deleted**: `xmax` is set (`!= INVALID_XID`) and the delete
///   is settled — either `current_txns.contains(&xmax)` (deleted by me earlier in this
///   txn; with no command ids yet this counts as deleted, mirroring `is_visible`'s
///   own-delete handling, so I may re-insert the key within my own txn) or the
///   deleter committed (`status(xmax) == Committed`, or `XMAX_COMMITTED`).
///
/// Otherwise the candidate is **alive or potentially-alive ⇒ conflict**: the
/// creator is committed, in-progress, or `current_txn`, and the row is not
/// committed-deleted (`xmax == INVALID_XID`, the deleter is aborted, or the
/// deleter is another in-progress txn — an in-flight delete that may yet roll
/// back, so it still blocks).
pub fn version_conflicts(
    xmin: TxnId,
    xmax: TxnId,
    infomask: u16,
    current_txns: &[TxnId],
    status: &dyn TxnStatusView,
) -> bool {
    classify_unique_conflict(xmin, xmax, infomask, current_txns, status) != UniqueConflict::None
}

/// The three-way outcome of the concurrent-inserter uniqueness check of
/// `docs/specs/mvcc.md` §7.3 ("concurrent inserts of the same unique key are
/// resolved by the same status check"). It refines the boolean
/// [`version_conflicts`] by distinguishing a *definite* duplicate from one that is
/// only *potentially* a duplicate because the conflicting version's creator is
/// another still-running transaction that may yet abort.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UniqueConflict {
    /// The candidate version is **dead** (its creator aborted, or it is
    /// committed-deleted / deleted-by-me) ⇒ it does not occupy the key. The
    /// inserter may proceed.
    None,
    /// The candidate version is **alive and definitely a duplicate**: its creator
    /// is committed, is `current_txn` itself (I already hold this key), or is
    /// frozen/reserved. The inserter must fail with a
    /// [`SqlState::UniqueViolation`](crate::SqlState::UniqueViolation) (`23505`).
    Violation,
    /// The candidate version is **alive but only potentially a duplicate**: its
    /// creator is **another in-progress transaction** (not me, not committed) that
    /// has not yet committed or aborted, so uniqueness is undecidable. The inserter
    /// **blocks** on that creator (`docs/specs/deadlock.md`) and re-checks when it
    /// finishes (committed ⇒ `23505`; aborted ⇒ no conflict). Carries the creator's
    /// xid for the waiter / deadlock detector.
    WouldBlock(TxnId),
}

/// The three-way **concurrent-inserter uniqueness** classifier of
/// `docs/specs/mvcc.md` §7.3. Given a candidate index version with creator `xmin`
/// and deleter `xmax`, decide whether it blocks `current_txn` claiming the same
/// unique key, and if so whether the conflict is **definite** ([`UniqueConflict::Violation`])
/// or **in-flight** ([`UniqueConflict::WouldBlock`]).
///
/// This builds on the liveness logic of [`version_conflicts`] (the same "dirty",
/// non-snapshot CLOG + hint-bit check; do not route it through [`is_visible`]) and
/// then, for an alive candidate, classifies its creator:
///
/// - **Dead ⇒ [`UniqueConflict::None`]:** the creator aborted (`XMIN_ABORTED`, or
///   `status(xmin) == Aborted`), **or** the version is committed-deleted —
///   `xmax` is set and either the delete is by me (`current_txns.contains(&xmax)`, e.g. an
///   UPDATE's own superseded old version, so an UPDATE does not false-conflict on
///   the row it supersedes), the deleter committed (`XMAX_COMMITTED`, or
///   `status(xmax) == Committed`). The candidate does not occupy the key.
/// - **Alive, creator settled-as-mine-or-committed ⇒ [`UniqueConflict::Violation`]:**
///   the creator is `current_txn` (a live version I created myself still occupies
///   the key — a real duplicate within my own txn), or it is committed
///   (`XMIN_COMMITTED`, or `status(xmin) == Committed`, which the reserved/frozen
///   `< FIRST_NORMAL_XID ⇒ Committed` rule also covers). Definitely a duplicate.
/// - **Alive, creator is another in-progress txn ⇒ [`UniqueConflict::WouldBlock`]:**
///   the creator is neither me nor committed (`status(xmin) == InProgress`). It may
///   yet abort, so uniqueness is undecidable; block on the creator (`xmin`) and
///   re-check when it finishes (committed ⇒ `23505`; aborted ⇒ no conflict).
///
/// **Hint bits** short-circuit the CLOG probe exactly as in [`version_conflicts`]:
/// `XMIN_ABORTED` settles a dead creator, `XMIN_COMMITTED` settles a `Violation`
/// creator, and the `XMAX_*` bits settle the committed-deleted check — all without
/// calling [`TxnStatusView::status`].
pub fn classify_unique_conflict(
    xmin: TxnId,
    xmax: TxnId,
    infomask: u16,
    current_txns: &[TxnId],
    status: &dyn TxnStatusView,
) -> UniqueConflict {
    // Resolve the creator's settled status once (hint bits short-circuit the CLOG
    // probe; my own write is always treated as committed-to-me). `current_txn`
    // takes precedence so PanicStatus is never consulted for an own write.
    let creator = if current_txns.contains(&xmin) || infomask & XMIN_COMMITTED != 0 {
        TxnStatus::Committed
    } else if infomask & XMIN_ABORTED != 0 {
        TxnStatus::Aborted
    } else {
        status.status(xmin)
    };

    // Creator aborted ⇒ the version is definitively dead; never conflicts.
    if creator == TxnStatus::Aborted {
        return UniqueConflict::None;
    }
    // Committed-deleted (including deleted-by-me, e.g. an UPDATE's superseded old
    // version) ⇒ the row is gone; no conflict.
    if xmax != INVALID_XID
        && (current_txns.contains(&xmax)
            || infomask & XMAX_COMMITTED != 0
            || status.status(xmax) == TxnStatus::Committed)
    {
        return UniqueConflict::None;
    }
    // Alive: classify the creator. A committed creator (or my own live version, or
    // reserved/frozen via the status rule) is a definite duplicate; only ANOTHER
    // txn's still-running creator is in-flight — it may yet abort, so uniqueness is
    // undecidable ⇒ block on it (WouldBlock) and re-check when it finishes.
    if creator == TxnStatus::Committed {
        UniqueConflict::Violation
    } else {
        UniqueConflict::WouldBlock(xmin)
    }
}

/// The outcome of the write-write conflict check of `docs/specs/mvcc.md` §7.3:
/// whether a writer may claim a target version's row lock (its `xmax`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteConflict {
    /// No live lock stands in the way; the writer may stamp `xmax = my_txn` and
    /// supersede this version (it is the first updater, or it already owns the
    /// lock, or the prior lock evaporated when its holder aborted).
    Proceed,
    /// Another transaction **committed** a delete/update of this version since the
    /// writer's snapshot, so the row changed under it ⇒
    /// [`SqlState::SerializationFailure`](crate::SqlState::SerializationFailure)
    /// (`40001`).
    Conflict,
    /// Another **in-progress** writer holds the version's `xmax` row lock. The
    /// writer must **block** on that holder (`docs/specs/deadlock.md`) and re-check
    /// once it finishes (aborted ⇒ `Proceed`; committed ⇒ `Conflict`). Carries the
    /// holder's xid for the waiter / deadlock detector.
    WouldBlock(TxnId),
}

/// The pure **write-write conflict** predicate of `docs/specs/mvcc.md` §7.3:
/// given the deleter `xmax` already stamped on a target version's *physical*
/// tuple header, may `current_txn` claim that version's row lock (stamp
/// `xmax = current_txn` to update/delete it)?
///
/// `xmax` doubles as the row lock. The engine (E1b) re-reads the version's
/// physical header immediately before stamping and feeds that just-read `xmax`
/// here; this predicate is pure (no [`Snapshot`]) because the row lock is an
/// **actual-status** check against the live CLOG, not a snapshot-relative read.
///
/// Rule (blocking + deadlock detection — §7.3, `docs/specs/deadlock.md`):
/// - `xmax == INVALID_XID` ⇒ [`WriteConflict::Proceed`]: no one has locked the
///   row; this writer is the first updater.
/// - `current_txns.contains(&xmax)` ⇒ [`WriteConflict::Proceed`]: this writer already
///   locked/deleted the row itself earlier in the same transaction.
/// - the deleter **aborted** (`XMAX_ABORTED` hint, or `status(xmax) == Aborted`)
///   ⇒ [`WriteConflict::Proceed`]: the other lock evaporated — its delete never
///   happened — so the row is free to claim.
/// - the deleter **committed** (`XMAX_COMMITTED` hint, or `status == Committed`) ⇒
///   [`WriteConflict::Conflict`]: the row changed since the writer's snapshot ⇒
///   `40001`.
/// - the deleter is **in-progress** (another live writer holds the lock) ⇒
///   [`WriteConflict::WouldBlock(xmax)`]: the writer blocks on that holder and
///   re-checks when it finishes (no fail-fast).
///
/// **Hint bits** short-circuit the CLOG probe exactly as in [`is_visible`] and
/// [`version_conflicts`]: a settled `XMAX_ABORTED`/`XMAX_COMMITTED` bit decides
/// the deleter's fate without calling [`TxnStatusView::status`].
///
/// **Relationship to [`version_conflicts`].** They are siblings, not duplicates:
/// [`version_conflicts`] answers "is *some* version with this key alive?"
/// (uniqueness enforcement, keyed off the candidate's *creator*); `write_conflict`
/// answers "may I lock *this* version, or did another txn beat me to its `xmax`?"
/// (first-updater-wins, keyed off the candidate's *deleter*).
pub fn write_conflict(
    xmax: u64,
    infomask: u16,
    current_txns: &[TxnId],
    status: &dyn TxnStatusView,
) -> WriteConflict {
    // No deleter: the row is unlocked ⇒ I am the first updater.
    if xmax == INVALID_XID {
        return WriteConflict::Proceed;
    }
    // I already hold the lock (locked/deleted it earlier in my own txn).
    if current_txns.contains(&xmax) {
        return WriteConflict::Proceed;
    }
    // Settled hint bits decide the deleter's fate without a CLOG probe (mirrors
    // `txn_effect_visible`): an aborted delete frees the row, a committed delete
    // means another txn beat me. At most one hint is ever set for a given xid.
    if infomask & XMAX_ABORTED != 0 {
        return WriteConflict::Proceed;
    }
    if infomask & XMAX_COMMITTED != 0 {
        return WriteConflict::Conflict;
    }
    // No hint: probe the CLOG. An aborted lock holder evaporated ⇒ the row is free;
    // a committed one beat me ⇒ conflict (`40001`); an in-progress one ⇒ I block on
    // it and re-check when it finishes (`docs/specs/deadlock.md`). (Reserved/frozen
    // xids never appear here: a real `xmax` is 0 or a normal xid, and the status
    // view's "< FIRST_NORMAL_XID ⇒ Committed" rule maps such a value to Conflict.)
    match status.status(xmax) {
        TxnStatus::Aborted => WriteConflict::Proceed,
        TxnStatus::Committed => WriteConflict::Conflict,
        TxnStatus::InProgress => WriteConflict::WouldBlock(xmax),
    }
}
