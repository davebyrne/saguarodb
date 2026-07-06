use std::sync::{Arc, Mutex};

use common::datetime::now_micros;
use common::{SessionActivityRow, SessionInfo, SessionState};

use crate::query::SessionGucs;

const DATABASE_OID: i32 = 1;
const USER_OID: i32 = 10;
const ACTIVITY_QUERY_MAX_BYTES: usize = 1024;

#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: Mutex<Vec<Arc<SessionActivityRecord>>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        session_info: Arc<SessionInfo>,
        gucs: Arc<SessionGucs>,
    ) -> Arc<SessionActivityRecord> {
        let record = Arc::new(SessionActivityRecord::new(session_info, gucs));
        self.lock().push(record.clone());
        record
    }

    pub fn deregister(&self, record: &Arc<SessionActivityRecord>) {
        let mut sessions = self.lock();
        if let Some(index) = sessions
            .iter()
            .position(|candidate| Arc::ptr_eq(candidate, record))
        {
            sessions.swap_remove(index);
        }
    }

    pub fn sessions(&self) -> Vec<SessionActivityRow> {
        let records = self.lock().clone();
        records.into_iter().map(|record| record.row()).collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Arc<SessionActivityRecord>>> {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug)]
pub struct SessionActivityRecord {
    session_info: Arc<SessionInfo>,
    gucs: Arc<SessionGucs>,
    backend_start: i64,
    state: Mutex<ActivityState>,
}

impl SessionActivityRecord {
    fn new(session_info: Arc<SessionInfo>, gucs: Arc<SessionGucs>) -> Self {
        let now = now_micros();
        Self {
            session_info,
            gucs,
            backend_start: now,
            state: Mutex::new(ActivityState {
                xact_start: None,
                query_start: None,
                state_change: Some(now),
                state: SessionState::Idle,
                query: String::new(),
            }),
        }
    }

    pub fn begin_statement(&self, query: &str) {
        let now = now_micros();
        let mut state = self.lock();
        if state.xact_start.is_none() {
            state.xact_start = Some(now);
        }
        state.query_start = Some(now);
        state.state_change = Some(now);
        state.state = SessionState::Active;
        state.query = truncate_activity_query(query);
    }

    pub fn end_statement(&self, next_state: SessionState) {
        let now = now_micros();
        let mut state = self.lock();
        match next_state {
            SessionState::Idle => state.xact_start = None,
            SessionState::IdleInTransaction | SessionState::IdleInTransactionAborted => {}
            SessionState::Active => {}
        }
        state.state = next_state;
        state.state_change = Some(now);
    }

    fn row(&self) -> SessionActivityRow {
        let state = self.lock().clone();
        SessionActivityRow {
            datid: DATABASE_OID,
            datname: self.session_info.database.clone(),
            pid: self.session_info.backend_pid,
            usesysid: USER_OID,
            usename: self.session_info.user.clone(),
            application_name: self.gucs.application_name(),
            backend_start: self.backend_start,
            xact_start: state.xact_start,
            query_start: state.query_start,
            state_change: state.state_change,
            state: state.state,
            query: state.query,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ActivityState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Clone, Debug)]
struct ActivityState {
    xact_start: Option<i64>,
    query_start: Option<i64>,
    state_change: Option<i64>,
    state: SessionState,
    query: String,
}

fn truncate_activity_query(query: &str) -> String {
    if query.len() <= ACTIVITY_QUERY_MAX_BYTES {
        return query.to_string();
    }
    let mut end = ACTIVITY_QUERY_MAX_BYTES;
    while !query.is_char_boundary(end) {
        end -= 1;
    }
    query[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_info(pid: i32) -> Arc<SessionInfo> {
        Arc::new(SessionInfo {
            user: "saguarodb".to_string(),
            database: "saguarodb".to_string(),
            backend_pid: pid,
        })
    }

    #[test]
    fn activity_query_is_capped_on_utf8_boundary() {
        let query = format!("select '{}'", "\u{00e9}".repeat(ACTIVITY_QUERY_MAX_BYTES));

        let truncated = truncate_activity_query(&query);

        assert!(truncated.len() <= ACTIVITY_QUERY_MAX_BYTES);
        assert!(query.starts_with(&truncated));
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn deregister_removes_only_the_exact_record_handle() {
        let registry = SessionRegistry::new();
        let first = registry.register(session_info(7), Arc::new(SessionGucs::new("first".into())));
        let _second =
            registry.register(session_info(7), Arc::new(SessionGucs::new("second".into())));

        registry.deregister(&first);

        let applications = registry
            .sessions()
            .into_iter()
            .map(|row| row.application_name)
            .collect::<Vec<_>>();
        assert_eq!(applications, vec!["second".to_string()]);
    }
}
