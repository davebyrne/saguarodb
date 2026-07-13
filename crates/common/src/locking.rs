use std::any::Any;

use crate::{Key, QueryCancel, Result, TableId, TxnId};

/// Logical identity of a row lock. The key is the table's primary key or its
/// stable hidden heap identity, so it survives non-key updates and HOT moves.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TupleLockTag {
    pub table: TableId,
    pub key: Key,
}

/// PostgreSQL-compatible tuple lock strengths, ordered from weakest to strongest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TupleLockMode {
    KeyShare,
    Share,
    NoKeyUpdate,
    Update,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TupleLockWaitPolicy {
    Block,
    NoWait,
    SkipLocked,
}

/// Reversible change made while acquiring one tuple grant. Successor traversal
/// uses these receipts to undo only grants from an ultimately skipped row.
pub struct TupleLockGrantChange(Box<dyn Any + Send + Sync>);

impl std::fmt::Debug for TupleLockGrantChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TupleLockGrantChange(..)")
    }
}

impl TupleLockGrantChange {
    /// Lock-manager implementation hook. Callers receive receipts from
    /// `acquire_tuple`; they must not manufacture them.
    #[doc(hidden)]
    pub fn manager_receipt<T: Any + Send + Sync>(payload: T) -> Self {
        Self(Box::new(payload))
    }

    #[doc(hidden)]
    pub fn manager_payload<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

#[derive(Debug)]
pub enum TupleLockAcquire {
    Acquired(TupleLockGrantChange),
    Skipped,
}

/// Transaction-owned tuple locking. Implementations must share their wait graph
/// with catalog-object and uniqueness waits so mixed deadlocks remain visible.
pub trait TupleLockManager: Send + Sync + std::fmt::Debug {
    fn acquire_tuple(
        &self,
        xid: TxnId,
        tag: &TupleLockTag,
        mode: TupleLockMode,
        wait_policy: TupleLockWaitPolicy,
        cancel: &QueryCancel,
    ) -> Result<TupleLockAcquire>;

    fn restore_tuple_grants(&self, xid: TxnId, changes: Vec<TupleLockGrantChange>) -> Result<()>;
}
