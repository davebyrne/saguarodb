use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

/// Identifies a backend for query cancellation. Sent to the client at startup as
/// `BackendKeyData` and presented back, on a separate connection, in a
/// `CancelRequest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BackendKey {
    pub process_id: i32,
    pub secret_key: i32,
}

/// Maps each connected backend's key to its cancellation flag, so a
/// `CancelRequest` arriving on a separate connection can signal the in-flight
/// query to abort. The flag is the same `Arc<AtomicBool>` the running query
/// shares through its `ExecutionContext`.
pub struct CancelRegistry {
    next_process_id: AtomicI32,
    flags: Mutex<HashMap<BackendKey, Arc<AtomicBool>>>,
}

impl Default for CancelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelRegistry {
    pub fn new() -> Self {
        Self {
            next_process_id: AtomicI32::new(1),
            flags: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate a fresh key (counter-based process id, random secret) for a
    /// connection and register its cancellation flag.
    pub fn register(&self, cancel: Arc<AtomicBool>) -> BackendKey {
        let key = BackendKey {
            process_id: self.next_process_id.fetch_add(1, Ordering::Relaxed),
            secret_key: random_secret(),
        };
        if let Ok(mut flags) = self.flags.lock() {
            flags.insert(key, cancel);
        }
        key
    }

    /// Drop a connection's key when it disconnects.
    pub fn deregister(&self, key: BackendKey) {
        if let Ok(mut flags) = self.flags.lock() {
            flags.remove(&key);
        }
    }

    /// Signal the backend identified by `key` to cancel its in-flight query.
    /// An unknown or stale key is ignored, matching PostgreSQL (cancellation is
    /// best-effort and unauthenticated).
    pub fn request_cancel(&self, key: BackendKey) {
        if let Ok(flags) = self.flags.lock()
            && let Some(flag) = flags.get(&key)
        {
            flag.store(true, Ordering::Relaxed);
        }
    }
}

fn random_secret() -> i32 {
    let mut buf = [0u8; 4];
    getrandom::getrandom(&mut buf).expect("OS random number generator unavailable");
    i32::from_ne_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_cancel_sets_the_registered_flag() {
        let registry = CancelRegistry::new();
        let flag = Arc::new(AtomicBool::new(false));
        let key = registry.register(flag.clone());

        registry.request_cancel(key);

        assert!(flag.load(Ordering::Relaxed));
    }

    #[test]
    fn unknown_key_is_ignored() {
        let registry = CancelRegistry::new();
        let flag = Arc::new(AtomicBool::new(false));
        let key = registry.register(flag.clone());

        registry.request_cancel(BackendKey {
            process_id: key.process_id,
            secret_key: key.secret_key.wrapping_add(1),
        });

        assert!(!flag.load(Ordering::Relaxed));
    }

    #[test]
    fn deregistered_key_is_no_longer_cancelable() {
        let registry = CancelRegistry::new();
        let flag = Arc::new(AtomicBool::new(false));
        let key = registry.register(flag.clone());

        registry.deregister(key);
        registry.request_cancel(key);

        assert!(!flag.load(Ordering::Relaxed));
    }

    #[test]
    fn each_backend_gets_a_distinct_process_id() {
        let registry = CancelRegistry::new();
        let first = registry.register(Arc::new(AtomicBool::new(false)));
        let second = registry.register(Arc::new(AtomicBool::new(false)));
        assert_ne!(first.process_id, second.process_id);
    }
}
