use std::sync::Arc;
use std::time::Duration;

use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};

use crate::{QueryCancel, Result};

const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// The writer-vs-checkpoint coordination primitive (`docs/specs/mvcc.md` §7.1
/// Stage 2, §10 E2b). The lock is **inverted** relative to Stage 1:
///
/// - **Writers take a SHARED guard** ([`begin_writer`](ConcurrencyController::begin_writer)),
///   so many write-transactions run concurrently. The shared guard is held on the
///   `Session`/`Transaction` for the whole write-transaction; per-row conflict
///   detection (E1) and the per-index / per-heap structural latches (E2a) — not
///   this lock — provide write-write safety.
/// - **The checkpoint takes the EXCLUSIVE guard**
///   ([`begin_checkpoint`](ConcurrencyController::begin_checkpoint)), which drains
///   every in-flight shared writer and then runs alone. This preserves the
///   "no in-flight writer during a checkpoint" invariant verbatim, so every
///   transaction below the truncation boundary is settled and captured by
///   `persist_clog`'s snapshot, keeping recovery correct without a fuzzy
///   checkpoint (`docs/specs/mvcc.md` §5.4, §8, §12).
/// - **Readers take no guard at all** and run lock-free; they are unaffected by
///   this lock and never call into it.
pub trait ConcurrencyController: Send + Sync {
    /// Acquire the SHARED writer guard. Many concurrent writers hold it
    /// simultaneously; it only blocks while a checkpoint holds the exclusive guard.
    fn begin_writer(&self) -> Result<WriteGuard>;

    /// Cancelable form used by foreground SQL statements. Implementations that
    /// can poll lock acquisition should override this; the default preserves
    /// compatibility for test/custom controllers.
    fn begin_writer_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard> {
        cancel.check()?;
        self.begin_writer()
    }

    /// Acquire the EXCLUSIVE checkpoint guard. Blocks until every in-flight shared
    /// writer has released its guard, then holds off any new writer until released,
    /// so the checkpoint body runs with no concurrent writer.
    fn begin_checkpoint(&self) -> Result<CheckpointGuard>;

    /// Cancelable form used by foreground maintenance and DDL statements.
    fn begin_checkpoint_cancelable(&self, cancel: &QueryCancel) -> Result<CheckpointGuard> {
        cancel.check()?;
        self.begin_checkpoint()
    }

    /// Acquire the SHARED guard for a non-writing exclusion participant (e.g. a
    /// test or a future drain point). Shares with writers; only the checkpoint
    /// excludes it. Used where a caller needs to observe writer concurrency without
    /// being one.
    fn begin_shared(&self) -> Result<WriteGuard> {
        self.begin_writer()
    }
}

#[derive(Debug)]
pub struct RwLockConcurrencyController {
    lock: Arc<RwLock<()>>,
}

impl RwLockConcurrencyController {
    pub fn new() -> Self {
        Self {
            lock: Arc::new(RwLock::new(())),
        }
    }
}

impl Default for RwLockConcurrencyController {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrencyController for RwLockConcurrencyController {
    fn begin_writer(&self) -> Result<WriteGuard> {
        // Shared (`read_arc`): writers do NOT exclude each other here. The inversion
        // (E2b) makes the read side of the `RwLock` the writer side: any number of
        // write-transactions hold this guard at once and rely on E1 conflict
        // detection + the E2a structural latches for safety. A shared acquire blocks
        // only while a checkpoint holds the exclusive (`write_arc`) guard, which is
        // exactly the writer-vs-checkpoint exclusion we want. `parking_lot`'s
        // `RwLock` read side IS re-entrant (a thread may hold multiple read guards),
        // so the same connection re-acquiring this guard cannot self-deadlock; the
        // "at most one writer guard per transaction" rule is now a cheap correctness
        // assertion at the transaction layer, not a deadlock guard.
        Ok(WriteGuard {
            _guard: self.lock.read_arc(),
        })
    }

    fn begin_writer_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard> {
        loop {
            cancel.check()?;
            if let Some(guard) = self.lock.try_read_arc_for(CANCEL_POLL_INTERVAL) {
                return Ok(WriteGuard { _guard: guard });
            }
        }
    }

    fn begin_checkpoint(&self) -> Result<CheckpointGuard> {
        // Exclusive (`write_arc`): blocks until all shared writers have drained and
        // then holds off any new writer, so the checkpoint runs alone. This is the
        // load-bearing "no in-flight writer at checkpoint" invariant
        // (`docs/specs/mvcc.md` §5.4, §8).
        Ok(CheckpointGuard {
            _guard: self.lock.write_arc(),
        })
    }

    fn begin_checkpoint_cancelable(&self, cancel: &QueryCancel) -> Result<CheckpointGuard> {
        loop {
            cancel.check()?;
            if let Some(guard) = self.lock.try_write_arc_for(CANCEL_POLL_INTERVAL) {
                return Ok(CheckpointGuard { _guard: guard });
            }
        }
    }
}

/// The SHARED writer guard. Held on the `Transaction`/`Session` for the whole
/// write-transaction; many exist concurrently. Named `WriteGuard` for continuity
/// with the call sites, though it is now the *shared* side of the lock.
pub struct WriteGuard {
    _guard: ArcRwLockReadGuard<RawRwLock, ()>,
}

/// The EXCLUSIVE checkpoint guard. Only the checkpointer holds it; it drains all
/// shared writers and runs alone.
pub struct CheckpointGuard {
    _guard: ArcRwLockWriteGuard<RawRwLock, ()>,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use super::{CheckpointGuard, ConcurrencyController, RwLockConcurrencyController, WriteGuard};
    use crate::{CancelReason, QueryCancel, SqlState};

    #[test]
    fn guards_satisfy_thread_safety_contract() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<WriteGuard>();
        assert_sync::<WriteGuard>();
        assert_send::<CheckpointGuard>();
    }

    /// Writers are concurrent: a second writer acquires the shared guard WHILE the
    /// first still holds it (it does not block, it does not error). This is the
    /// inversion — the whole point of E2b.
    #[test]
    fn many_writers_share_the_guard_concurrently() {
        let controller = Arc::new(RwLockConcurrencyController::new());
        let first = controller.begin_writer().expect("first writer acquires");

        let acquired = Arc::new(AtomicBool::new(false));
        let controller2 = controller.clone();
        let acquired2 = acquired.clone();
        let second = thread::spawn(move || {
            // Must NOT block behind the first writer: shared guards coexist.
            let _guard = controller2.begin_writer().expect("second writer acquires");
            acquired2.store(true, Ordering::Release);
        });
        second.join().expect("second writer thread finished");
        assert!(
            acquired.load(Ordering::Acquire),
            "a second writer acquires the shared guard while the first still holds it"
        );
        drop(first);
    }

    /// A checkpoint takes the exclusive guard: while a writer holds the shared guard
    /// the checkpoint BLOCKS (waits, does not error), proceeding only once every
    /// writer has drained. This is the preserved "no in-flight writer at checkpoint"
    /// invariant.
    #[test]
    fn checkpoint_blocks_until_all_writers_drain() {
        let controller = Arc::new(RwLockConcurrencyController::new());
        let writer = controller.begin_writer().expect("writer acquires");

        let started = Arc::new(AtomicBool::new(false));
        let acquired = Arc::new(AtomicBool::new(false));
        let controller2 = controller.clone();
        let started2 = started.clone();
        let acquired2 = acquired.clone();
        let checkpoint = thread::spawn(move || {
            started2.store(true, Ordering::Release);
            // Blocks here until the writer releases; must return Ok, not Err.
            let _guard = controller2.begin_checkpoint().expect("checkpoint acquires");
            acquired2.store(true, Ordering::Release);
        });

        while !started.load(Ordering::Acquire) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(50));
        assert!(
            !acquired.load(Ordering::Acquire),
            "the checkpoint must wait for the in-flight writer to drain"
        );

        drop(writer);
        checkpoint.join().expect("checkpoint thread finished");
        assert!(
            acquired.load(Ordering::Acquire),
            "the checkpoint acquires once every writer has drained"
        );
    }

    /// While a checkpoint holds the exclusive guard, a new writer BLOCKS (waits,
    /// does not error) until the checkpoint releases. This is the other half of the
    /// invariant: no writer slips in during the checkpoint body.
    #[test]
    fn writer_blocks_while_checkpoint_holds_the_exclusive_guard() {
        let controller = Arc::new(RwLockConcurrencyController::new());
        let checkpoint = controller.begin_checkpoint().expect("checkpoint acquires");

        let started = Arc::new(AtomicBool::new(false));
        let acquired = Arc::new(AtomicBool::new(false));
        let controller2 = controller.clone();
        let started2 = started.clone();
        let acquired2 = acquired.clone();
        let writer = thread::spawn(move || {
            started2.store(true, Ordering::Release);
            let _guard = controller2.begin_writer().expect("writer acquires");
            acquired2.store(true, Ordering::Release);
        });

        while !started.load(Ordering::Acquire) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(50));
        assert!(
            !acquired.load(Ordering::Acquire),
            "a writer must wait while the checkpoint holds the exclusive guard"
        );

        drop(checkpoint);
        writer.join().expect("writer thread finished");
        assert!(acquired.load(Ordering::Acquire));
    }

    #[test]
    fn foreground_guard_waits_are_cancelable() {
        let controller = Arc::new(RwLockConcurrencyController::new());
        let checkpoint = controller.begin_checkpoint().unwrap();
        let cancel = Arc::new(QueryCancel::new());
        let waiter = {
            let controller = controller.clone();
            let cancel = cancel.clone();
            thread::spawn(move || controller.begin_writer_cancelable(cancel.as_ref()))
        };
        thread::sleep(Duration::from_millis(25));
        cancel.request(CancelReason::StatementTimeout);
        let err = match waiter.join().unwrap() {
            Err(err) => err,
            Ok(_) => panic!("writer guard unexpectedly acquired"),
        };
        assert_eq!(err.code, SqlState::QueryCanceled);
        drop(checkpoint);

        let writer = controller.begin_writer().unwrap();
        cancel.reset();
        let waiter = {
            let controller = controller.clone();
            let cancel = cancel.clone();
            thread::spawn(move || controller.begin_checkpoint_cancelable(cancel.as_ref()))
        };
        thread::sleep(Duration::from_millis(25));
        cancel.request(CancelReason::StatementTimeout);
        let err = match waiter.join().unwrap() {
            Err(err) => err,
            Ok(_) => panic!("checkpoint guard unexpectedly acquired"),
        };
        assert_eq!(err.code, SqlState::QueryCanceled);
        drop(writer);
    }

    /// The shared writer guard is re-entrant on one thread: a connection re-acquiring
    /// it cannot self-deadlock (so the transaction-layer tripwire is a correctness
    /// assertion, not a deadlock guard).
    #[test]
    fn shared_writer_guard_is_reentrant_on_one_thread() {
        let controller = Arc::new(RwLockConcurrencyController::new());
        let first = controller.begin_writer().expect("first acquire");
        let second = controller
            .begin_writer()
            .expect("re-acquire on the same thread");
        drop(second);
        drop(first);
    }

    /// Many concurrent writers all make progress and then a checkpoint drains them:
    /// a stress check that the shared/exclusive coordination has no lost wakeup or
    /// starvation under contention.
    ///
    /// Both observed properties are made DETERMINISTIC (no scheduler/`sleep`
    /// dependence) by a [`Barrier`](std::sync::Barrier):
    /// - **Concurrency:** every writer acquires the shared guard and increments
    ///   `in_flight` BEFORE the barrier, so when the barrier releases all `WRITERS`
    ///   are provably in flight at once; `max_seen >= WRITERS` therefore always
    ///   holds (the sibling `many_writers_share_the_guard_concurrently` proves the
    ///   same non-blocking property in the simplest two-writer form).
    /// - **Drain:** the writers release only after the barrier, and the subsequent
    ///   exclusive `begin_checkpoint` blocks until every shared writer has drained,
    ///   so it observes `in_flight == 0`.
    #[test]
    fn concurrent_writers_then_checkpoint_drains_them() {
        const WRITERS: usize = 8;
        const ROUNDS: usize = 200;

        let controller = Arc::new(RwLockConcurrencyController::new());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        // One extra party so the main thread can release each round in lockstep with
        // the writers, keeping the rounds contended without any `sleep`.
        let barrier = Arc::new(Barrier::new(WRITERS + 1));

        let mut handles = Vec::new();
        for _ in 0..WRITERS {
            let controller = controller.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..ROUNDS {
                    let guard = controller.begin_writer().expect("writer acquires");
                    // Become in-flight, then wait. Every writer reaches the barrier
                    // only while holding the shared guard, so once it releases all
                    // `WRITERS` are provably in flight simultaneously — this is the
                    // deterministic concurrency observation.
                    in_flight.fetch_add(1, Ordering::AcqRel);
                    barrier.wait();
                    // Now all `WRITERS` are in flight; record the peak after release.
                    max_seen.fetch_max(in_flight.load(Ordering::Acquire), Ordering::AcqRel);
                    // Release the round in lockstep before dropping the guard.
                    barrier.wait();
                    in_flight.fetch_sub(1, Ordering::AcqRel);
                    drop(guard);
                }
            }));
        }
        for _ in 0..ROUNDS {
            // Release the writers into their in-flight window, then let them drain.
            barrier.wait();
            barrier.wait();
        }
        for handle in handles {
            handle.join().expect("writer thread finished");
        }

        // Drain (deterministic): the exclusive guard blocks until every shared writer
        // has released, so it observes nothing in flight.
        let _checkpoint = controller.begin_checkpoint().expect("checkpoint acquires");
        assert_eq!(in_flight.load(Ordering::Acquire), 0);
        // Concurrency (deterministic): the barrier guaranteed all `WRITERS` held the
        // shared guard at once, so the peak is exactly `WRITERS`.
        assert_eq!(
            max_seen.load(Ordering::Acquire),
            WRITERS,
            "all writers provably held the shared guard at the same instant"
        );
    }
}
