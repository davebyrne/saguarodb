use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::{DbError, Result};

use crate::app::AppState;
use crate::checkpoint::run_checkpoint;

#[cfg(test)]
type WaitForIdleHook = Arc<dyn Fn() + Send + Sync + 'static>;

pub struct ShutdownState {
    pub(crate) accepting: AtomicBool,
    pub(crate) in_flight: AtomicUsize,
    pub(crate) notify_idle: tokio::sync::Notify,
    #[cfg(test)]
    wait_for_idle_before_wait_hook: std::sync::Mutex<Option<WaitForIdleHook>>,
}

pub struct InFlightQueryGuard {
    state: Arc<ShutdownState>,
}

impl ShutdownState {
    pub fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            in_flight: AtomicUsize::new(0),
            notify_idle: tokio::sync::Notify::new(),
            #[cfg(test)]
            wait_for_idle_before_wait_hook: std::sync::Mutex::new(None),
        }
    }

    pub fn begin_query(self: &Arc<Self>) -> Result<InFlightQueryGuard> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(DbError::internal("server is shutting down"));
        }
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) {
            if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.notify_idle.notify_one();
            }
            return Err(DbError::internal("server is shutting down"));
        }
        Ok(InFlightQueryGuard {
            state: self.clone(),
        })
    }

    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
        if self.in_flight.load(Ordering::Acquire) == 0 {
            self.notify_idle.notify_one();
        }
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    pub async fn wait_for_idle(&self, _timeout: Duration) -> Result<()> {
        let wait = async {
            loop {
                if self.in_flight.load(Ordering::Acquire) == 0 {
                    return;
                }
                #[cfg(test)]
                self.run_wait_for_idle_before_wait_hook();
                self.notify_idle.notified().await;
            }
        };
        tokio::time::timeout(_timeout, wait)
            .await
            .map_err(|_| DbError::internal("timed out waiting for in-flight queries"))?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_wait_for_idle_before_wait_hook(&self, hook: Option<WaitForIdleHook>) {
        *self.wait_for_idle_before_wait_hook.lock().unwrap() = hook;
    }

    #[cfg(test)]
    fn run_wait_for_idle_before_wait_hook(&self) {
        let hook = self.wait_for_idle_before_wait_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }
}

impl Default for ShutdownState {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for InFlightQueryGuard {
    fn drop(&mut self) {
        if self.state.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.notify_idle.notify_one();
        }
    }
}

pub async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|err| DbError::io(format!("failed to install SIGTERM handler: {err}")))?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.map_err(|err| DbError::io(format!("failed to wait for ctrl-c: {err}")))?;
            }
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|err| DbError::io(format!("failed to wait for ctrl-c: {err}")))?;
    }
    Ok(())
}

pub async fn run_graceful_shutdown(_app: Arc<AppState>) -> Result<()> {
    let app = _app;
    app.components.shutdown.stop_accepting();
    let timeout = Duration::from_millis(app.components.config.shutdown_timeout_ms);
    let idle_result = app.components.shutdown.wait_for_idle(timeout).await;
    if idle_result.is_ok() {
        if let Err(err) = run_checkpoint(&app.components) {
            eprintln!("checkpoint failed during shutdown: {err}");
        }
    } else {
        eprintln!("shutdown timed out waiting for in-flight queries; skipping checkpoint");
    }
    app.components.wal.flush()?;
    idle_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use crate::app::AppState;
    use crate::config::Config;
    use crate::shutdown::{ShutdownState, run_graceful_shutdown};

    #[tokio::test]
    async fn graceful_shutdown_waits_for_in_flight_query_before_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let app = Arc::new(AppState::open_for_test(dir.path()).unwrap());
        let guard = app.components.shutdown.begin_query().unwrap();

        let shutdown = {
            let app = app.clone();
            tokio::spawn(async move { run_graceful_shutdown(app).await })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(app.checkpoint_count_for_test(), 0);

        drop(guard);
        shutdown.await.unwrap().unwrap();

        assert_eq!(app.checkpoint_count_for_test(), 1);
        assert!(app.wal_flushed_for_test());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_idle_handles_guard_drop_between_count_check_and_wait_registration() {
        let state = Arc::new(ShutdownState::new());
        let guard = state.begin_query().unwrap();
        let entered_hook = Arc::new((Mutex::new(false), Condvar::new()));
        let release_hook = Arc::new((Mutex::new(false), Condvar::new()));

        let hook_entered = entered_hook.clone();
        let hook_release = release_hook.clone();
        state.set_wait_for_idle_before_wait_hook(Some(Arc::new(move || {
            let (lock, condvar) = &*hook_entered;
            *lock.lock().unwrap() = true;
            condvar.notify_one();

            let (lock, condvar) = &*hook_release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = condvar.wait(released).unwrap();
            }
        })));

        let waiter = {
            let state = state.clone();
            tokio::spawn(async move { state.wait_for_idle(Duration::from_millis(25)).await })
        };

        {
            let (lock, condvar) = &*entered_hook;
            let mut entered = lock.lock().unwrap();
            while !*entered {
                entered = condvar.wait(entered).unwrap();
            }
        }

        drop(guard);

        {
            let (lock, condvar) = &*release_hook;
            *lock.lock().unwrap() = true;
            condvar.notify_one();
        }

        waiter.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_flushes_without_checkpoint_after_idle_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            data_dir: dir.path().to_path_buf(),
            shutdown_timeout_ms: 1,
            ..Config::default()
        };
        let app = Arc::new(crate::recovery::open_app(config).unwrap());
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        let _guard = app.components.shutdown.begin_query().unwrap();

        let err = run_graceful_shutdown(app.clone()).await.unwrap_err();

        assert!(err.message.contains("timed out waiting"));
        assert_eq!(app.checkpoint_count_for_test(), 0);
        assert!(app.wal_flushed_for_test());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn graceful_shutdown_timeout_does_not_block_on_statement_guard() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            data_dir: dir.path().to_path_buf(),
            shutdown_timeout_ms: 1,
            ..Config::default()
        };
        let app = Arc::new(crate::recovery::open_app(config).unwrap());
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        let _in_flight = app.components.shutdown.begin_query().unwrap();
        let statement_guard = app.components.concurrency.begin_read().unwrap();

        let shutdown = {
            let app = app.clone();
            tokio::spawn(async move { run_graceful_shutdown(app).await })
        };

        let timed = tokio::time::timeout(Duration::from_millis(50), shutdown).await;
        drop(statement_guard);

        let result = timed.expect("shutdown blocked on checkpoint behind statement guard");
        let err = result.unwrap().unwrap_err();
        assert!(err.message.contains("timed out waiting"));
        assert!(app.wal_flushed_for_test());
    }
}
