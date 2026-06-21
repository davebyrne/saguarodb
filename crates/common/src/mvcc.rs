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
    use super::{IsolationLevel, Snapshot, TxnStatus};

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
}
