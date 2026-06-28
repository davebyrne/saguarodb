//! Runtime MVCC types shared across crates.
//!
//! These types are scaffolding for the MVCC effort (see `docs/specs/mvcc.md`).
//! They are defined now (Milestone A2) so later milestones can build on a stable
//! shape, but they are not yet consulted by any visibility or transaction logic.
//! The snapshot capture (B3/C3) and CLOG-backed status (A3) wire them in.
//!
//! These are deliberately runtime-only: no `serde`/durable-encoding derives. The
//! CLOG on-disk representation of transaction status is a later task (A3).

use crate::ids::TxnId;

mod conflict;
mod reclaim;
mod visibility;

#[cfg(test)]
mod tests;

pub use conflict::{
    UniqueConflict, WriteConflict, classify_unique_conflict, version_conflicts, write_conflict,
};
pub use reclaim::is_dead_to_all;
pub use visibility::{XMAX_ABORTED, XMAX_COMMITTED, XMIN_ABORTED, XMIN_COMMITTED, is_visible};

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
    ///
    /// Because `xmax = 0`, every transaction id is "in the future" and therefore
    /// invisible under [`is_visible`]; this snapshot sees nothing. Call sites that
    /// must see committed rows (the server's autocommit paths, and the pre-capture
    /// placeholder used by [`StatementContext::new`](crate::StatementContext::new))
    /// use [`Snapshot::sees_all_committed`] instead.
    pub fn empty() -> Self {
        Self {
            xmin: 0,
            xmax: 0,
            xip: Vec::new(),
        }
    }

    /// The degenerate "sees all committed" snapshot used by single-writer
    /// autocommit before real per-transaction snapshots arrive (Milestone C):
    /// `xmax = u64::MAX` (no transaction is in the future), no in-progress xids,
    /// so every committed transaction — and the reader's own writes via the
    /// predicate's `current_txn` path — is visible. This is the placeholder that
    /// [`StatementContext::new`](crate::StatementContext::new) carries so the
    /// snapshot-aware read paths behave as if no version is filtered.
    pub fn sees_all_committed() -> Self {
        Self {
            xmin: TxnId::MAX,
            xmax: TxnId::MAX,
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
/// §6). `Serializable` adds SSI on top of that same snapshot (`docs/specs/ssi.md`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    /// = snapshot isolation.
    RepeatableRead,
    /// Snapshot isolation plus Serializable Snapshot Isolation (SSI): rw-conflict
    /// tracking and dangerous-structure detection on top of the Repeatable Read
    /// snapshot (`docs/specs/ssi.md`). No longer an alias for Repeatable Read.
    Serializable,
}

impl Default for IsolationLevel {
    /// Postgres' default; the only level honored until Milestone G.
    fn default() -> Self {
        Self::ReadCommitted
    }
}
