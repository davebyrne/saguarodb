use std::sync::atomic::{AtomicU8, Ordering};

use crate::{DbError, Result, SqlState};

const NOT_CANCELED: u8 = 0;

/// Why the currently executing statement was canceled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CancelReason {
    UserRequest = 1,
    StatementTimeout = 2,
}

/// Per-connection cancellation state shared by the protocol, server, storage
/// wait paths, and executor. The first request wins until the connection resets
/// the token for the next statement.
#[derive(Debug, Default)]
pub struct QueryCancel {
    reason: AtomicU8,
}

impl QueryCancel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request(&self, reason: CancelReason) {
        let _ = self.reason.compare_exchange(
            NOT_CANCELED,
            reason as u8,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    pub fn reset(&self) {
        self.reason.store(NOT_CANCELED, Ordering::Relaxed);
    }

    pub fn reason(&self) -> Option<CancelReason> {
        match self.reason.load(Ordering::Relaxed) {
            NOT_CANCELED => None,
            value if value == CancelReason::UserRequest as u8 => Some(CancelReason::UserRequest),
            value if value == CancelReason::StatementTimeout as u8 => {
                Some(CancelReason::StatementTimeout)
            }
            _ => None,
        }
    }

    pub fn check(&self) -> Result<()> {
        let Some(reason) = self.reason() else {
            return Ok(());
        };
        let message = match reason {
            CancelReason::UserRequest => "canceling statement due to user request",
            CancelReason::StatementTimeout => "canceling statement due to statement timeout",
        };
        Err(DbError::execute(SqlState::QueryCanceled, message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_cancellation_reason_wins_until_reset() {
        let cancel = QueryCancel::new();
        cancel.request(CancelReason::StatementTimeout);
        cancel.request(CancelReason::UserRequest);
        assert_eq!(cancel.reason(), Some(CancelReason::StatementTimeout));
        assert!(
            cancel
                .check()
                .unwrap_err()
                .message
                .contains("statement timeout")
        );

        cancel.reset();
        assert_eq!(cancel.reason(), None);
        assert!(cancel.check().is_ok());
    }
}
