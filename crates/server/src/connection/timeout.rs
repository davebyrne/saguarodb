use std::sync::Arc;
use std::time::Duration;

use common::{CancelReason, QueryCancel};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TimerState {
    Idle,
    Armed,
    Expired,
}

/// One connection's statement timer. Disarming aborts and joins the old task
/// before a later arm resets the shared cancellation token, so a stale timer can
/// never cancel a subsequent statement.
pub(super) struct StatementTimer {
    state_tx: watch::Sender<TimerState>,
    task: Option<JoinHandle<()>>,
}

impl StatementTimer {
    pub(super) fn new() -> Self {
        let (state_tx, _) = watch::channel(TimerState::Idle);
        Self {
            state_tx,
            task: None,
        }
    }

    pub(super) async fn arm(&mut self, timeout: Duration, cancel: Arc<QueryCancel>) {
        self.disarm().await;
        cancel.reset();
        if timeout.is_zero() {
            return;
        }

        self.state_tx.send_replace(TimerState::Armed);
        let state_tx = self.state_tx.clone();
        self.task = Some(tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            cancel.request(CancelReason::StatementTimeout);
            state_tx.send_replace(TimerState::Expired);
        }));
    }

    pub(super) async fn disarm(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        self.state_tx.send_replace(TimerState::Idle);
    }

    pub(super) fn is_expired(&self) -> bool {
        *self.state_tx.borrow() == TimerState::Expired
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<TimerState> {
        self.state_tx.subscribe()
    }

    pub(super) fn receiver_is_expired(receiver: &watch::Receiver<TimerState>) -> bool {
        *receiver.borrow() == TimerState::Expired
    }
}

impl Drop for StatementTimer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rearming_cannot_leak_an_old_timeout_into_new_work() {
        let cancel = Arc::new(QueryCancel::new());
        let mut timer = StatementTimer::new();

        timer.arm(Duration::from_millis(10), cancel.clone()).await;
        timer.arm(Duration::from_millis(100), cancel.clone()).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(cancel.reason().is_none());
        assert!(!timer.is_expired());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cancel.reason(), Some(CancelReason::StatementTimeout));
        assert!(timer.is_expired());
    }

    #[tokio::test]
    async fn disabled_or_disarmed_timer_does_not_cancel() {
        let cancel = Arc::new(QueryCancel::new());
        let mut timer = StatementTimer::new();

        timer.arm(Duration::ZERO, cancel.clone()).await;
        assert!(cancel.reason().is_none());

        timer.arm(Duration::from_millis(10), cancel.clone()).await;
        timer.disarm().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cancel.reason().is_none());
        assert!(!timer.is_expired());
    }

    #[tokio::test]
    async fn subscriber_created_after_expiry_observes_current_state() {
        let cancel = Arc::new(QueryCancel::new());
        let mut timer = StatementTimer::new();
        timer.arm(Duration::from_millis(1), cancel.clone()).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !timer.is_expired() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let receiver = timer.subscribe();
        assert!(StatementTimer::receiver_is_expired(&receiver));
        assert_eq!(cancel.reason(), Some(CancelReason::StatementTimeout));
    }
}
