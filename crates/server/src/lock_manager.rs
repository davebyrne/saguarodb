//! Row-lock wait coordination + timeout-based deadlock detection
//! (`docs/specs/deadlock.md`).
//!
//! When a writer hits an in-progress row-lock holder it calls [`LockManager::
//! wait_for`] (via the [`ConflictWaiter`] trait threaded onto `StatementContext`),
//! which parks the writer's `spawn_blocking` thread until the holder finishes, then
//! returns so the caller re-checks the row. A waiter that sits longer than
//! `deadlock_timeout` runs wait-for-graph cycle detection and, if it is in a cycle,
//! aborts itself with `DeadlockDetected` (`40P01`).
//!
//! The wait-for graph is keyed by **top-level** transaction ids (a deadlock is
//! between transactions; with savepoints both the writing xid and a stamped `xmax`
//! are subxids), canonicalized via [`ActiveTxnRegistry::top_of`] at insert. The
//! per-blocker liveness re-check stays keyed on the specific blocker subxid, so a
//! partial `ROLLBACK TO` that deregisters only that subxid frees its waiter.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use common::{ConflictWaiter, DbError, Result, SqlState, TxnId};

use crate::registry::ActiveTxnRegistry;

/// How often a parked waiter wakes to re-check its blocker's liveness and the
/// cancel flag. Bounds cancel latency; deadlock detection runs only at the full
/// `deadlock_timeout`.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Coordinates blocking writers and detects deadlocks (`docs/specs/deadlock.md`).
#[derive(Debug)]
pub struct LockManager {
    /// `top → top` wait-for edges (one outgoing edge per waiting transaction). Guards
    /// the condvar; `on_txn_finished` takes this same lock before notifying, which
    /// closes the lost-wakeup window.
    waits_for_top: Mutex<HashMap<TxnId, TxnId>>,
    cond: Condvar,
    registry: ActiveTxnRegistry,
    deadlock_timeout: Duration,
}

impl LockManager {
    pub fn new(registry: ActiveTxnRegistry, deadlock_timeout: Duration) -> Self {
        Self {
            waits_for_top: Mutex::new(HashMap::new()),
            cond: Condvar::new(),
            registry,
            deadlock_timeout,
        }
    }

    /// Wake every parked waiter so it re-checks its blocker's liveness. Called after
    /// a (sub)xid is deregistered on commit / abort / rollback / partial `ROLLBACK
    /// TO`. Taking the lock before `notify_all` is load-bearing: a waiter holds this
    /// lock across its `is_active` check and `wait_timeout`, so a finishing
    /// transaction cannot slip its wakeup into that window.
    pub fn on_txn_finished(&self) {
        let _guard = self.lock();
        self.cond.notify_all();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TxnId, TxnId>> {
        self.waits_for_top
            .lock()
            .expect("lock manager mutex poisoned")
    }

    /// Is `start` on a cycle in `graph`? Each transaction has at most one outgoing
    /// edge, so the walk is a simple chain; a revisit of `start` is a cycle. Bounded
    /// by the graph size so an unrelated chain cannot loop forever.
    fn on_cycle(graph: &HashMap<TxnId, TxnId>, start: TxnId) -> bool {
        let mut current = start;
        for _ in 0..graph.len() {
            match graph.get(&current) {
                Some(&next) if next == start => return true,
                Some(&next) => current = next,
                None => return false,
            }
        }
        false
    }
}

impl ConflictWaiter for LockManager {
    fn wait_for(&self, waiter_subxid: u64, blocker_subxid: u64, cancel: &AtomicBool) -> Result<()> {
        let waiter_top = self.registry.top_of(waiter_subxid);
        let blocker_top = self.registry.top_of(blocker_subxid);

        let mut graph = self.lock();
        graph.insert(waiter_top, blocker_top);
        let mut last_detection = Instant::now();

        let result = loop {
            // The blocker finished (committed/aborted, or its subxid was rolled
            // back) ⇒ proceed; the caller re-checks the row's status.
            if !self.registry.is_active(blocker_subxid) {
                break Ok(());
            }
            if cancel.load(Ordering::Relaxed) {
                break Err(DbError::execute(
                    SqlState::QueryCanceled,
                    "canceling statement due to user request",
                ));
            }
            let (next_graph, timed_out) = self
                .cond
                .wait_timeout(graph, POLL_INTERVAL)
                .expect("lock manager mutex poisoned");
            graph = next_graph;
            if timed_out.timed_out() && last_detection.elapsed() >= self.deadlock_timeout {
                last_detection = Instant::now();
                if Self::on_cycle(&graph, waiter_top) {
                    // Drop the victim's edge in the SAME critical section as detection
                    // so a concurrent detector sees a broken chain and stays waiting —
                    // exactly one victim per cycle.
                    graph.remove(&waiter_top);
                    return Err(DbError::execute(
                        SqlState::DeadlockDetected,
                        "deadlock detected",
                    ));
                }
            }
        };

        graph.remove(&waiter_top);
        result
    }
}
