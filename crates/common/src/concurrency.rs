use std::sync::Arc;

use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};

use crate::Result;

pub trait ConcurrencyController: Send + Sync {
    fn begin_read(&self) -> Result<ReadGuard>;
    fn begin_write(&self) -> Result<WriteGuard>;
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
    fn begin_read(&self) -> Result<ReadGuard> {
        Ok(ReadGuard {
            _guard: self.lock.read_arc(),
        })
    }

    fn begin_write(&self) -> Result<WriteGuard> {
        // `parking_lot::RwLock` is non-reentrant: a holder that re-acquires
        // `write_arc()` self-deadlocks. Blocking here is correct: the write path is
        // structured so a single connection acquires the guard at most once (the
        // open transaction holds its one `WriteGuard` for the whole
        // write-transaction), so this blocks only on a *different* connection's
        // writer — exactly the writer-vs-writer serialization Stage 1 wants. The
        // "acquire at most once per connection" invariant (and its defensive
        // reentrancy tripwire) lives at the transaction layer in
        // `crates/server/src/query.rs`, where connection identity is known; a
        // controller-level flag cannot tell a reentrant self-acquire from a
        // legitimately contended second connection that must wait.
        Ok(WriteGuard {
            _guard: self.lock.write_arc(),
        })
    }
}

pub struct ReadGuard {
    _guard: ArcRwLockReadGuard<RawRwLock, ()>,
}

pub struct WriteGuard {
    _guard: ArcRwLockWriteGuard<RawRwLock, ()>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::{ConcurrencyController, ReadGuard, RwLockConcurrencyController, WriteGuard};

    #[test]
    fn guards_satisfy_thread_safety_contract() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<ReadGuard>();
        assert_sync::<ReadGuard>();
        assert_send::<WriteGuard>();
    }

    /// Two distinct writers serialize: while the first holds the exclusive guard,
    /// a second writer on another thread BLOCKS (it waits, it does NOT error), and
    /// proceeds only once the first guard drops. This is the writer-vs-writer
    /// serialization the reentrancy tripwire (at the transaction layer) must not
    /// weaken.
    #[test]
    fn second_writer_blocks_until_first_releases_and_does_not_error() {
        let controller = Arc::new(RwLockConcurrencyController::new());

        let first = controller.begin_write().expect("first writer acquires");

        let started = Arc::new(AtomicBool::new(false));
        let acquired = Arc::new(AtomicBool::new(false));
        let controller2 = controller.clone();
        let started2 = started.clone();
        let acquired2 = acquired.clone();
        let second = thread::spawn(move || {
            started2.store(true, Ordering::Release);
            // Blocks here until the first writer releases; must return Ok, not Err.
            let _guard = controller2.begin_write().expect("second writer acquires");
            acquired2.store(true, Ordering::Release);
        });

        // Give the second writer time to reach (and block on) the guard.
        while !started.load(Ordering::Acquire) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(50));
        assert!(
            !acquired.load(Ordering::Acquire),
            "the second writer must wait behind the first, not error or proceed"
        );

        // Release the first guard; the second writer now proceeds.
        drop(first);
        second.join().expect("second writer thread finished");
        assert!(
            acquired.load(Ordering::Acquire),
            "the second writer acquires once the first releases"
        );
    }
}
