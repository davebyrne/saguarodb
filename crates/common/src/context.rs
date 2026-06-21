use crate::mvcc::{IsolationLevel, Snapshot};

/// Per-statement execution context threaded into every storage operation.
///
/// `snapshot` is the visibility snapshot threaded into the storage engine's read
/// paths (`docs/specs/mvcc.md` §5.5, §6). The server's autocommit paths capture a
/// real (degenerate, single-writer) snapshot via `StatementContext::with_snapshot`;
/// [`StatementContext::new`] fills it with the equivalent
/// [`Snapshot::sees_all_committed`] placeholder so pre-capture call sites (tests,
/// recovery scaffolding) see every committed row and own write. Real
/// per-transaction snapshots arrive in Milestone C. `isolation` is carried but not
/// yet consulted (honored from Milestone G).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Snapshot,
    pub isolation: IsolationLevel,
}

impl StatementContext {
    /// Construct a context for `txn_id` carrying the degenerate "sees all
    /// committed" snapshot ([`Snapshot::sees_all_committed`]) and the default
    /// isolation level. This is the single-writer placeholder used before real
    /// per-transaction snapshot capture (Milestone C): every committed row and own
    /// write is visible, so the snapshot-aware read paths filter nothing.
    pub fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            snapshot: Snapshot::sees_all_committed(),
            isolation: IsolationLevel::default(),
        }
    }

    /// Construct a context for `txn_id` carrying a captured `snapshot` and the
    /// default isolation level. Used by the server's autocommit read/write paths
    /// (B3) to thread the visibility snapshot into the storage engine.
    pub fn with_snapshot(txn_id: u64, snapshot: Snapshot) -> Self {
        Self {
            txn_id,
            snapshot,
            isolation: IsolationLevel::default(),
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
        assert_eq!(ctx.snapshot, Snapshot::sees_all_committed());
        assert_eq!(ctx.isolation, IsolationLevel::ReadCommitted);
    }

    #[test]
    fn contexts_with_same_txn_id_are_equal() {
        assert_eq!(StatementContext::new(7), StatementContext::new(7));
        assert_ne!(StatementContext::new(7), StatementContext::new(8));
    }
}
