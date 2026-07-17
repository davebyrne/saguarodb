use std::sync::{Arc, Condvar, Mutex, Weak};

use common::{CancelReason, DbError, QueryCancel, Result};

use crate::app::ServerComponents;

#[derive(Default)]
struct MaintenanceState {
    vacuum_requested: bool,
    analyze_requested: bool,
    running: bool,
    stop_requested: bool,
    last_error: Option<DbError>,
    active_cancel: Option<Arc<QueryCancel>>,
    last_job_was_vacuum: bool,
}

#[derive(Default)]
pub struct MaintenanceCoordinator {
    state: Mutex<MaintenanceState>,
    changed: Condvar,
}

impl MaintenanceCoordinator {
    pub fn request_vacuum(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.vacuum_requested = true;
        self.changed.notify_all();
    }

    pub fn request_analyze(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.analyze_requested = true;
        self.changed.notify_all();
    }

    pub fn stop(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.stop_requested = true;
        if let Some(cancel) = &state.active_cancel {
            cancel.request(CancelReason::UserRequest);
        }
        self.changed.notify_all();
    }
}

pub fn spawn_maintenance_worker(
    components: &Arc<ServerComponents>,
) -> Result<std::thread::JoinHandle<()>> {
    let weak = Arc::downgrade(components);
    std::thread::Builder::new()
        .name("saguarodb-maintenance".to_string())
        .spawn(move || maintenance_worker(weak))
        .map_err(|err| DbError::io(format!("failed to start maintenance worker: {err}")))
}

fn maintenance_worker(components: Weak<ServerComponents>) {
    let Some(initial) = components.upgrade() else {
        return;
    };
    let coordinator = Arc::clone(&initial.maintenance_coordinator);
    drop(initial);
    loop {
        let mut state = match coordinator.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        while !state.stop_requested && !state.vacuum_requested && !state.analyze_requested {
            state = match coordinator.changed.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        if state.stop_requested {
            return;
        }
        let (vacuum, analyze) = choose_maintenance_job(
            state.vacuum_requested,
            state.analyze_requested,
            state.last_job_was_vacuum,
        );
        state.last_job_was_vacuum = vacuum;
        let cancel = Arc::new(QueryCancel::new());
        state.active_cancel = Some(Arc::clone(&cancel));
        state.running = true;
        drop(state);

        let Some(components) = components.upgrade() else {
            return;
        };
        let result = if vacuum {
            crate::query::run_automatic_vacuum(Arc::clone(&components), cancel)
        } else if analyze {
            crate::query::run_automatic_analyze(Arc::clone(&components), cancel)
        } else {
            Ok(())
        };
        let mut state = match coordinator.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.running = false;
        state.active_cancel = None;
        if state.stop_requested {
            coordinator.changed.notify_all();
            return;
        }
        match result {
            Ok(()) => {
                state.last_error = None;
                if vacuum {
                    let remaining = components
                        .dead_rows_since_vacuum
                        .load(std::sync::atomic::Ordering::Acquire);
                    state.vacuum_requested = components.config.auto_vacuum_dead_rows != 0
                        && remaining >= components.config.auto_vacuum_dead_rows;
                } else if analyze {
                    let remaining = components
                        .rows_changed_since_analyze
                        .load(std::sync::atomic::Ordering::Acquire);
                    state.analyze_requested = components.config.auto_analyze_changed_rows != 0
                        && remaining >= components.config.auto_analyze_changed_rows;
                }
            }
            Err(err) => {
                eprintln!("automatic maintenance failed: {err}");
                state.last_error = Some(err);
                let (next, _) = match coordinator
                    .changed
                    .wait_timeout(state, std::time::Duration::from_secs(1))
                {
                    Ok(pair) => pair,
                    Err(poisoned) => poisoned.into_inner(),
                };
                state = next;
            }
        }
        coordinator.changed.notify_all();
        drop(state);
    }
}

fn choose_maintenance_job(
    vacuum_requested: bool,
    analyze_requested: bool,
    last_job_was_vacuum: bool,
) -> (bool, bool) {
    let vacuum = vacuum_requested && (!analyze_requested || !last_job_was_vacuum);
    let analyze = analyze_requested && !vacuum;
    (vacuum, analyze)
}

#[cfg(test)]
mod tests {
    #[test]
    fn pending_vacuum_and_analyze_alternate() {
        assert_eq!(
            super::choose_maintenance_job(true, true, false),
            (true, false)
        );
        assert_eq!(
            super::choose_maintenance_job(true, true, true),
            (false, true)
        );
    }
}
