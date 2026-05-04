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
    use super::{ReadGuard, WriteGuard};

    #[test]
    fn guards_satisfy_thread_safety_contract() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<ReadGuard>();
        assert_sync::<ReadGuard>();
        assert_send::<WriteGuard>();
    }
}
