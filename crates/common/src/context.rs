use std::sync::Arc;

use crate::mvcc::{IsolationLevel, Snapshot};

/// Per-statement execution context threaded into every storage operation.
///
/// `snapshot` is the visibility snapshot threaded into the storage engine's read
/// paths (`docs/specs/mvcc.md` §5.5, §6). The server's transaction paths capture a
/// real snapshot via [`StatementContext::with_snapshot`]; [`StatementContext::new`]
/// fills it with the equivalent [`Snapshot::sees_all_committed`] placeholder so
/// pre-capture call sites (tests, recovery scaffolding) see every committed row and
/// own write.
///
/// The snapshot is held behind an [`Arc`] so the executor can clone a
/// `StatementContext` per scan operator (`crates/executor/src/query.rs`) by bumping
/// a refcount rather than deep-cloning the `xip` vector. With concurrent
/// transactions (Milestone C) `xip` is no longer always empty, so the share matters
/// (`docs/specs/mvcc.md` §10 C3). `isolation` is honored by the server's snapshot
/// capture from Milestone C (Read Committed = fresh per statement, Repeatable Read =
/// captured once); the storage engine does not consult it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Arc<Snapshot>,
    pub isolation: IsolationLevel,
}

impl StatementContext {
    /// Construct a context for `txn_id` carrying the degenerate "sees all
    /// committed" snapshot ([`Snapshot::sees_all_committed`]) and the default
    /// isolation level. This is the placeholder used before real snapshot capture:
    /// every committed row and own write is visible, so the snapshot-aware read
    /// paths filter nothing.
    pub fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            snapshot: Arc::new(Snapshot::sees_all_committed()),
            isolation: IsolationLevel::default(),
        }
    }

    /// Construct a context for `txn_id` carrying a shared, already-captured
    /// `snapshot` and the default isolation level. Used by the server's
    /// transaction read/write paths to thread the visibility snapshot into the
    /// storage engine; the shared `Arc` is cloned cheaply per scan operator.
    pub fn with_snapshot(txn_id: u64, snapshot: Arc<Snapshot>) -> Self {
        Self {
            txn_id,
            snapshot,
            isolation: IsolationLevel::default(),
        }
    }

    /// Like [`StatementContext::with_snapshot`] but also carries an explicit
    /// `isolation` level (the server sets this from the active transaction).
    pub fn with_snapshot_and_isolation(
        txn_id: u64,
        snapshot: Arc<Snapshot>,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            txn_id,
            snapshot,
            isolation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IsolationLevel, Snapshot, StatementContext};

    #[test]
    fn new_sets_txn_id_and_placeholder_fields() {
        let ctx = StatementContext::new(42);
        assert_eq!(ctx.txn_id, 42);
        // The placeholder is the degenerate "sees all committed" snapshot, not the
        // empty (sees-nothing) one, so pre-capture reads return committed rows.
        assert_eq!(*ctx.snapshot, Snapshot::sees_all_committed());
        assert_eq!(ctx.isolation, IsolationLevel::ReadCommitted);
    }

    #[test]
    fn contexts_with_same_txn_id_are_equal() {
        assert_eq!(StatementContext::new(7), StatementContext::new(7));
        assert_ne!(StatementContext::new(7), StatementContext::new(8));
    }
}
