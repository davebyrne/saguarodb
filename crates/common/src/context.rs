use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::error::{DbError, Result};
use crate::mvcc::{IsolationLevel, Snapshot};

/// Blocks a writer that hit an in-progress row-lock conflict until the holder
/// finishes, so the writer can re-check (`docs/specs/deadlock.md`). The storage
/// engine's write paths call this at a conflict point (after dropping the page
/// latch); the server's lock manager implements it. `wait_for` returns `Ok` once
/// `blocker` is no longer active (the caller then re-checks the row), or `Err` with
/// `DeadlockDetected` (`40P01`) if waiting would deadlock, or `QueryCanceled`
/// (`57014`) if `cancel` is set.
pub trait ConflictWaiter: Send + Sync + std::fmt::Debug {
    fn wait_for(&self, waiter: u64, blocker: u64, cancel: &AtomicBool) -> Result<()>;
}

/// The default `ConflictWaiter` for read-only / test contexts. A real `WouldBlock`
/// only arises at a storage write-conflict point, which always carries the server's
/// real lock manager, so this is never legitimately reached. It **errors loudly**
/// rather than returning `Ok` — returning `Ok` would make a mis-wired write path
/// spin forever (`WouldBlock → wait → Ok → re-attempt → WouldBlock → …`).
#[derive(Debug)]
struct NoConflictWaiter;

impl ConflictWaiter for NoConflictWaiter {
    fn wait_for(&self, waiter: u64, blocker: u64, _cancel: &AtomicBool) -> Result<()> {
        Err(DbError::internal(format!(
            "no conflict waiter configured: a write path reached a row-lock conflict \
             (waiter={waiter}, blocker={blocker}) without a lock manager"
        )))
    }
}

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
/// `PartialEq`/`Eq` are hand-rolled below (not derived) because `conflict_waiter`
/// and `cancel` are not comparable; equality compares the value fields only.
#[derive(Clone, Debug)]
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Arc<Snapshot>,
    pub isolation: IsolationLevel,
    /// Blocks this statement when it hits an in-progress row-lock conflict, until
    /// the holder finishes (`docs/specs/deadlock.md`). The default
    /// ([`NoConflictWaiter`]) errors if ever asked to wait; the server installs the
    /// real lock manager on write-capable contexts.
    pub conflict_waiter: Arc<dyn ConflictWaiter>,
    /// The per-statement cancel flag, shared with the connection (set by a client
    /// `CancelRequest`). Threaded to the storage conflict point so a blocked writer
    /// can be interrupted (`docs/specs/deadlock.md` §5). Defaults to a never-set flag.
    pub cancel: Arc<AtomicBool>,
    /// The reading/writing transaction's **live (sub)xid set** — `txn_id` plus any
    /// not-rolled-back savepoint subxids (`docs/specs/savepoints.md` §4). A tuple
    /// whose `xmin`/`xmax` is in this set is the transaction's own (uncommitted)
    /// effect, visible to it and not a self-conflict. Defaults to just `[txn_id]`
    /// (no savepoints); the server widens it for a transaction with open/released
    /// savepoints. `Arc`-shared so the executor clones a context per scan operator
    /// cheaply, like `snapshot`.
    pub live_txns: Arc<[u64]>,
    /// The GC horizon (minimum advertised snapshot `xmin`) the server captured for
    /// this statement. Consumed ONLY by the storage engine's HOT update-path prune
    /// (`docs/specs/mvcc.md` §10 Milestone H3): when a same-page HOT update has no
    /// room, the engine collapses that page's committed-dead HOT prefixes to reclaim
    /// space before falling back. A stale/smaller horizon only prunes less, never
    /// unsafely, so it is captured before execution under the shared writer guard.
    /// Defaults to `0` (prune nothing committed-dead) for pre-capture / read / test
    /// contexts; the server sets it on write paths via [`StatementContext::with_gc_horizon`].
    pub gc_horizon: u64,
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
            gc_horizon: 0,
            live_txns: Arc::from([txn_id]),
            conflict_waiter: Arc::new(NoConflictWaiter),
            cancel: Arc::new(AtomicBool::new(false)),
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
            gc_horizon: 0,
            live_txns: Arc::from([txn_id]),
            conflict_waiter: Arc::new(NoConflictWaiter),
            cancel: Arc::new(AtomicBool::new(false)),
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
            gc_horizon: 0,
            live_txns: Arc::from([txn_id]),
            conflict_waiter: Arc::new(NoConflictWaiter),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the GC horizon for this statement (the H3 update-path prune; see the field
    /// doc). Builder-style so the server threads it after constructing the context.
    #[must_use]
    pub fn with_gc_horizon(mut self, gc_horizon: u64) -> Self {
        self.gc_horizon = gc_horizon;
        self
    }

    /// Set the transaction's live (sub)xid set (the server uses this for a
    /// transaction with savepoints; see the `live_txns` field). Builder-style.
    #[must_use]
    pub fn with_live_txns(mut self, live_txns: Arc<[u64]>) -> Self {
        self.live_txns = live_txns;
        self
    }

    /// Install the real conflict waiter (the server's lock manager) and the
    /// connection's cancel flag for a write-capable statement (`docs/specs/deadlock.md`).
    /// Builder-style.
    #[must_use]
    pub fn with_conflict_waiter(
        mut self,
        conflict_waiter: Arc<dyn ConflictWaiter>,
        cancel: Arc<AtomicBool>,
    ) -> Self {
        self.conflict_waiter = conflict_waiter;
        self.cancel = cancel;
        self
    }
}

// Hand-rolled to exclude `conflict_waiter` and `cancel` (neither is comparable);
// two contexts are equal when their value fields match, as before the deadlock
// waiter/cancel fields were added.
impl PartialEq for StatementContext {
    fn eq(&self, other: &Self) -> bool {
        self.txn_id == other.txn_id
            && self.snapshot == other.snapshot
            && self.isolation == other.isolation
            && self.live_txns == other.live_txns
            && self.gc_horizon == other.gc_horizon
    }
}

impl Eq for StatementContext {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::{IsolationLevel, Snapshot, StatementContext};

    #[test]
    fn default_conflict_waiter_errors_rather_than_spinning() {
        // A read/test context's waiter must never be asked to wait (a real
        // WouldBlock only arises on a write path with the real lock manager). If it
        // ever is, it errors loudly instead of returning Ok and spinning forever.
        let ctx = StatementContext::new(7);
        let err = ctx
            .conflict_waiter
            .wait_for(7, 8, &AtomicBool::new(false))
            .expect_err("default waiter must error, not return Ok");
        assert!(err.message.contains("no conflict waiter configured"));
    }

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
