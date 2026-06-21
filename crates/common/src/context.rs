use crate::mvcc::{IsolationLevel, Snapshot};

/// Per-statement execution context threaded into every storage operation.
///
/// `snapshot` and `isolation` are MVCC scaffolding (see `docs/specs/mvcc.md`
/// §5.5): they are carried but not yet consulted. Real snapshot capture per
/// isolation level is wired in by later milestones (B3/C3). Until then,
/// [`StatementContext::new`] fills them with a degenerate placeholder snapshot
/// and the default isolation level.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Snapshot,
    pub isolation: IsolationLevel,
}

impl StatementContext {
    /// Construct a context for `txn_id` with a placeholder snapshot
    /// ([`Snapshot::empty`]) and the default isolation level. The placeholder is
    /// not a captured snapshot; capture is a later milestone (B3/C3).
    pub fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            snapshot: Snapshot::empty(),
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
        assert_eq!(ctx.snapshot, Snapshot::empty());
        assert_eq!(ctx.isolation, IsolationLevel::ReadCommitted);
    }

    #[test]
    fn contexts_with_same_txn_id_are_equal() {
        assert_eq!(StatementContext::new(7), StatementContext::new(7));
        assert_ne!(StatementContext::new(7), StatementContext::new(8));
    }
}
