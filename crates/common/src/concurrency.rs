use crate::{QueryCancel, Result};

/// Compatibility boundary for write-unit lifetime tracking.
///
/// Checkpoints no longer participate in this controller: fuzzy checkpoints use
/// page latches, publication gates, and the buffer-owned checkpoint fence. The
/// returned token remains in transaction state so existing write-unit cleanup
/// continues to have one explicit lifetime marker while that plumbing is retired.
pub trait ConcurrencyController: Send + Sync {
    fn begin_writer(&self) -> Result<WriteGuard>;

    fn begin_writer_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard> {
        cancel.check()?;
        self.begin_writer()
    }

    fn begin_shared(&self) -> Result<WriteGuard> {
        self.begin_writer()
    }

    fn begin_shared_cancelable(&self, cancel: &QueryCancel) -> Result<WriteGuard> {
        self.begin_writer_cancelable(cancel)
    }
}

#[derive(Debug, Default)]
pub struct RwLockConcurrencyController;

impl RwLockConcurrencyController {
    pub fn new() -> Self {
        Self
    }
}

impl ConcurrencyController for RwLockConcurrencyController {
    fn begin_writer(&self) -> Result<WriteGuard> {
        Ok(WriteGuard { _private: () })
    }
}

/// A non-blocking write-unit lifetime token. It never excludes a checkpoint or
/// another writer.
pub struct WriteGuard {
    _private: (),
}

impl Drop for WriteGuard {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::{ConcurrencyController, RwLockConcurrencyController, WriteGuard};
    use crate::{CancelReason, QueryCancel, SqlState};

    #[test]
    fn guard_satisfies_thread_safety_contract() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<WriteGuard>();
        assert_sync::<WriteGuard>();
    }

    #[test]
    fn concurrent_writers_do_not_block_each_other() {
        let controller = Arc::new(RwLockConcurrencyController::new());
        let _first = controller.begin_writer().expect("first writer acquires");
        let second_controller = Arc::clone(&controller);
        thread::spawn(move || second_controller.begin_writer())
            .join()
            .expect("writer thread finished")
            .expect("second writer acquires");
    }

    #[test]
    fn cancelable_acquisition_checks_pending_cancellation() {
        let controller = RwLockConcurrencyController::new();
        let cancel = QueryCancel::new();
        cancel.request(CancelReason::StatementTimeout);
        let error = match controller.begin_writer_cancelable(&cancel) {
            Ok(_) => panic!("canceled writer unexpectedly acquired a token"),
            Err(error) => error,
        };
        assert_eq!(error.code, SqlState::QueryCanceled);
    }
}
