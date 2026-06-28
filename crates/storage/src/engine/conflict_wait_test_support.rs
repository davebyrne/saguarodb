//! Test-only `ConflictWaiter`s for exercising the engine's blocking conflict path
//! (`docs/specs/deadlock.md`) without the server's real lock manager.
//!
//! When a writer hits an in-progress row/key lock the engine returns `WouldBlock`
//! and calls `ConflictWaiter::wait_for`; the real server blocks until the holder
//! finishes, then the engine re-checks. These doubles simulate the holder finishing
//! *during the wait* — committing or aborting it in the shared WAL/CLOG — so a
//! single-threaded (or racing) unit test observes the resolved outcome the engine
//! produces on retry (committed ⇒ `23505`/`40001`; aborted ⇒ the write proceeds).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::{ConflictWaiter, Result, StatementContext};
use wal::{WalManager, WalRecord, WalRecordKind};

/// On `wait_for`, settle the blocker in the WAL (so the engine's retry sees a
/// settled conflict), simulating the holder finishing while the waiter was parked.
pub(super) struct SettleBlockerOnWait {
    wal: Arc<dyn WalManager>,
    commit: bool,
}

impl std::fmt::Debug for SettleBlockerOnWait {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettleBlockerOnWait")
            .field("commit", &self.commit)
            .finish()
    }
}

impl ConflictWaiter for SettleBlockerOnWait {
    fn wait_for(&self, _waiter: u64, blocker: u64, _cancel: &AtomicBool) -> Result<()> {
        let kind = if self.commit {
            WalRecordKind::Commit
        } else {
            WalRecordKind::Abort
        };
        self.wal.append(WalRecord {
            lsn: 0,
            txn_id: blocker,
            kind,
        })?;
        self.wal.flush()?;
        Ok(())
    }
}

/// `ctx` with a waiter that **commits** any in-progress blocker on wait — the engine
/// retries and observes a committed conflict (`23505` for a unique key, `40001` for
/// a row lock). Models "blocked, then the holder committed".
pub(super) fn committing_blocker(
    ctx: StatementContext,
    wal: Arc<dyn WalManager>,
) -> StatementContext {
    ctx.with_conflict_waiter(
        Arc::new(SettleBlockerOnWait { wal, commit: true }),
        Arc::new(AtomicBool::new(false)),
    )
}

/// `ctx` with a waiter that **aborts** any in-progress blocker on wait — the engine
/// retries and the write proceeds (the lock evaporated). Models "blocked, then the
/// holder aborted".
pub(super) fn aborting_blocker(
    ctx: StatementContext,
    wal: Arc<dyn WalManager>,
) -> StatementContext {
    ctx.with_conflict_waiter(
        Arc::new(SettleBlockerOnWait { wal, commit: false }),
        Arc::new(AtomicBool::new(false)),
    )
}
