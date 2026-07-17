use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

use buffer::PAGE_SIZE;
use catalog::{reconcile_snapshot_derived_metadata, serialize_catalog};
use common::{DbError, Result, TableId};
use control::ControlData;
use wal::{WalRecord, WalRecordKind};

use crate::app::ServerComponents;

#[derive(Default)]
struct CoordinatorState {
    requested_generation: u64,
    completed_generation: u64,
    running: bool,
    stop_requested: bool,
    last_error: Option<DbError>,
    busy_retry_ms: u64,
}

#[derive(Default)]
pub struct CheckpointCoordinator {
    state: Mutex<CoordinatorState>,
    changed: Condvar,
}

impl CheckpointCoordinator {
    pub fn request(&self) -> u64 {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.requested_generation == state.completed_generation {
            state.requested_generation = state.requested_generation.saturating_add(1);
        }
        let generation = state.requested_generation;
        self.changed.notify_all();
        generation
    }

    pub fn stop(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.stop_requested = true;
        self.changed.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn is_stopped(&self) -> bool {
        match self.state.lock() {
            Ok(state) => state.stop_requested,
            Err(poisoned) => poisoned.into_inner().stop_requested,
        }
    }

    pub fn checkpoint_now_and_wait(&self, deadline: std::time::Instant) -> Result<()> {
        let target = self.request();
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("checkpoint coordinator lock was poisoned"))?;
        while state.completed_generation < target {
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(state.last_error.clone().unwrap_or_else(|| {
                    DbError::internal("timed out waiting for checkpoint completion")
                }));
            }
            let wait = deadline.saturating_duration_since(now);
            let (next, timeout) = self
                .changed
                .wait_timeout(state, wait)
                .map_err(|_| DbError::internal("checkpoint coordinator lock was poisoned"))?;
            state = next;
            if timeout.timed_out() && state.completed_generation < target {
                return Err(state.last_error.clone().unwrap_or_else(|| {
                    DbError::internal("timed out waiting for checkpoint completion")
                }));
            }
        }
        Ok(())
    }
}

pub fn spawn_checkpoint_worker(
    components: &Arc<ServerComponents>,
) -> Result<std::thread::JoinHandle<()>> {
    let weak = Arc::downgrade(components);
    std::thread::Builder::new()
        .name("saguarodb-checkpointer".to_string())
        .spawn(move || checkpoint_worker(weak))
        .map_err(|err| DbError::io(format!("failed to start checkpoint worker: {err}")))
}

fn checkpoint_worker(components: Weak<ServerComponents>) {
    let Some(initial) = components.upgrade() else {
        return;
    };
    let coordinator = Arc::clone(&initial.checkpoint_coordinator);
    drop(initial);
    loop {
        let mut state = match coordinator.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        while !state.stop_requested && state.requested_generation == state.completed_generation {
            state = match coordinator.changed.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        if state.stop_requested {
            return;
        }
        let target = state.requested_generation;
        state.running = true;
        drop(state);

        let Some(components) = components.upgrade() else {
            return;
        };
        let result = run_checkpoint(&components);
        let mut state = match coordinator.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut busy_retry = None;
        state.running = false;
        match result {
            Ok(()) => {
                state.completed_generation = state.completed_generation.max(target);
                state.last_error = None;
                let last = components
                    .checkpoint
                    .wal_trigger_lsn
                    .load(Ordering::Acquire);
                let commits = components
                    .checkpoint
                    .commits_since_checkpoint
                    .load(Ordering::Acquire);
                let skipped = components
                    .checkpoint
                    .last_busy_or_skipped
                    .load(Ordering::Acquire);
                if skipped > 0 {
                    state.busy_retry_ms = next_busy_retry_ms(state.busy_retry_ms, true);
                    busy_retry = Some(state.busy_retry_ms);
                } else {
                    state.busy_retry_ms = 0;
                }
                let wal_pressure = components
                    .wal
                    .bytes_after(last)
                    .is_ok_and(|bytes| bytes >= components.config.checkpoint_wal_bytes);
                if checkpoint_followup_required(
                    commits,
                    components.config.checkpoint_every_n_commits,
                    wal_pressure,
                    skipped,
                ) && state.requested_generation == state.completed_generation
                {
                    state.requested_generation = state.requested_generation.saturating_add(1);
                }
            }
            Err(err) => {
                eprintln!("background checkpoint failed: {err}");
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
        if let Some(delay_ms) = busy_retry {
            state = wait_for_busy_retry(&coordinator, state, delay_ms);
            if state.stop_requested {
                return;
            }
        }
        drop(state);
    }
}

fn checkpoint_followup_required(
    commits: u64,
    commit_threshold: u64,
    wal_pressure: bool,
    skipped: u64,
) -> bool {
    commits >= commit_threshold || wal_pressure || skipped > 0
}

fn next_busy_retry_ms(current: u64, skipped: bool) -> u64 {
    if !skipped {
        0
    } else if current == 0 {
        100
    } else {
        current.saturating_mul(2).min(5_000)
    }
}

fn wait_for_busy_retry<'a>(
    coordinator: &'a CheckpointCoordinator,
    state: std::sync::MutexGuard<'a, CoordinatorState>,
    delay_ms: u64,
) -> std::sync::MutexGuard<'a, CoordinatorState> {
    if state.stop_requested {
        return state;
    }
    match coordinator
        .changed
        .wait_timeout(state, std::time::Duration::from_millis(delay_ms))
    {
        Ok((state, _)) => state,
        Err(poisoned) => poisoned.into_inner().0,
    }
}

pub struct CheckpointState {
    pub last_checkpoint_end_lsn: AtomicU64,
    /// WAL position after checkpoint-generated records, used only for workload
    /// pressure accounting so a checkpoint marker cannot retrigger itself.
    pub wal_trigger_lsn: AtomicU64,
    pub commits_since_checkpoint: AtomicU64,
    pub checkpoints: AtomicU64,
    pub last_busy_or_skipped: AtomicU64,
    run_lock: Mutex<()>,
}

impl CheckpointState {
    pub fn new(last_checkpoint_end_lsn: u64) -> Self {
        Self {
            last_checkpoint_end_lsn: AtomicU64::new(last_checkpoint_end_lsn),
            wal_trigger_lsn: AtomicU64::new(last_checkpoint_end_lsn),
            commits_since_checkpoint: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
            last_busy_or_skipped: AtomicU64::new(0),
            run_lock: Mutex::new(()),
        }
    }
}

/// Run one fuzzy checkpoint. Writers continue throughout page and metadata I/O;
/// only the final in-memory snapshot uses short publication read gates and the
/// buffer-owned checkpoint fence.
pub fn run_checkpoint(components: &ServerComponents) -> Result<()> {
    let _single_runner = components
        .checkpoint
        .run_lock
        .lock()
        .map_err(|_| DbError::internal("checkpoint runner lock was poisoned"))?;
    let represented_commits = components
        .checkpoint
        .commits_since_checkpoint
        .load(Ordering::Acquire);
    if components.wal.needs_clog_maintenance()? {
        components.maintenance_coordinator.request_vacuum();
    }
    let batch_pages =
        validated_checkpoint_batch_pages(components.config.checkpoint_flush_batch_pages)?;
    let candidates = components.buffer_pool.checkpoint_dirty_keys()?;
    let mut busy_or_skipped = 0_u64;
    for batch in candidates.chunks(batch_pages) {
        let stats = components.buffer_pool.flush_checkpoint_batch(batch)?;
        busy_or_skipped = busy_or_skipped.saturating_add(stats.busy_or_skipped as u64);
        std::thread::yield_now();
    }

    let catalog_read = components
        .catalog_publication_gate
        .read()
        .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
    let relation_read = components
        .relation_publish_gate
        .read()
        .map_err(|_| DbError::internal("relation publish gate poisoned"))?;
    let fence = components.buffer_pool.checkpoint_fence();
    let fence_exclusive = fence.exclusive();

    let dirty_pages = components.buffer_pool.dirty_page_table()?;
    let mut tables: Vec<TableId> = components
        .catalog
        .list_tables()?
        .iter()
        .map(|table| table.id)
        .collect();
    tables.sort_unstable();
    let mut snapshot = components.catalog.snapshot()?;
    for live in components.storage.sequence_schemas_for_checkpoint()? {
        match snapshot.sequences_by_id.get_mut(&live.id) {
            Some(sequence) => {
                sequence.last_value = live.last_value;
                sequence.is_called = live.is_called;
            }
            None => {
                snapshot
                    .sequences_by_name
                    .insert(live.name.clone(), live.id);
                snapshot.next_sequence_id =
                    snapshot.next_sequence_id.max(live.id.saturating_add(1));
                snapshot.sequences_by_id.insert(live.id, live);
            }
        }
    }
    let catalog_redo_pin = components.storage.catalog_redo_tracker().oldest_pending()?;
    let (active_ids, allocation_boundary) = components
        .active_txns
        .checkpoint_snapshot(|| components.next_txn_id.load(Ordering::Acquire));
    let checkpoint_end_lsn = components.wal.written_lsn()?;
    let page_redo_lsn = dirty_pages
        .iter()
        .map(|entry| entry.rec_lsn)
        .min()
        .unwrap_or(checkpoint_end_lsn);
    let catalog_redo_lsn = catalog_redo_pin.unwrap_or(checkpoint_end_lsn);
    let replay_floor = page_redo_lsn.min(catalog_redo_lsn);

    drop(fence_exclusive);
    drop(relation_read);
    drop(catalog_read);

    reconcile_snapshot_derived_metadata(&mut snapshot)?;
    let catalog = serialize_catalog(&snapshot)?;
    let control = ControlData {
        checkpoint_end_lsn,
        page_redo_lsn,
        catalog_redo_lsn,
        dirty_pages,
        tables,
        catalog,
        page_size: PAGE_SIZE as u32,
    };

    let txn_high_water = allocation_boundary.saturating_sub(1);
    let checkpoint_marker = components.wal.append_positioned(WalRecord {
        lsn: 0,
        txn_id: txn_high_water,
        kind: WalRecordKind::Checkpoint {
            checkpoint_end_lsn,
            page_redo_lsn,
            catalog_redo_lsn,
        },
    })?;
    components
        .wal
        .checkpoint_clog(replay_floor, &active_ids, allocation_boundary)?;
    components.control.store(control)?;
    components.wal.recycle_through(replay_floor)?;

    if let Err(err) = cleanup_relation_generation_files(components) {
        eprintln!("post-checkpoint relation cleanup failed: {err}");
    }
    components
        .checkpoint
        .last_checkpoint_end_lsn
        .store(checkpoint_end_lsn, Ordering::Release);
    components
        .checkpoint
        .wal_trigger_lsn
        .store(checkpoint_marker.record_lsn, Ordering::Release);
    let _ = components.checkpoint.commits_since_checkpoint.fetch_update(
        Ordering::AcqRel,
        Ordering::Acquire,
        |current| Some(current.saturating_sub(represented_commits)),
    );
    components
        .checkpoint
        .checkpoints
        .fetch_add(1, Ordering::AcqRel);
    components
        .checkpoint
        .last_busy_or_skipped
        .store(busy_or_skipped, Ordering::Release);
    if busy_or_skipped > 0 {
        eprintln!("checkpoint skipped {busy_or_skipped} busy page candidates; follow-up requested");
    }
    Ok(())
}

fn validated_checkpoint_batch_pages(batch_pages: usize) -> Result<usize> {
    if !(1..=1_000_000).contains(&batch_pages) {
        return Err(DbError::internal(
            "checkpoint flush batch pages must be in 1..=1000000",
        ));
    }
    Ok(batch_pages)
}

pub fn cleanup_relation_generation_files(components: &ServerComponents) -> Result<()> {
    components.storage.try_cleanup_retired_generations()?;
    components.storage.cleanup_orphan_files()?;
    Ok(())
}

pub fn record_commit_and_maybe_checkpoint(components: &ServerComponents) -> Result<()> {
    let commits = components
        .checkpoint
        .commits_since_checkpoint
        .fetch_add(1, Ordering::AcqRel)
        .saturating_add(1);
    let last_checkpoint_end_lsn = components
        .checkpoint
        .wal_trigger_lsn
        .load(Ordering::Acquire);
    if commits >= components.config.checkpoint_every_n_commits
        || components.wal.bytes_after(last_checkpoint_end_lsn)?
            >= components.config.checkpoint_wal_bytes
    {
        components.checkpoint_coordinator.request();
    }
    Ok(())
}

pub fn record_commit_and_maybe_checkpoint_after_durable_commit(components: &ServerComponents) {
    if let Err(err) = record_commit_and_maybe_checkpoint(components) {
        eprintln!("checkpoint trigger failed after durable commit: {err}");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[test]
    fn programmatic_zero_checkpoint_batch_is_rejected_without_panicking() {
        let error = super::validated_checkpoint_batch_pages(0).unwrap_err();
        assert!(error.message.contains("1..=1000000"));
    }

    #[test]
    fn skipped_page_requests_a_followup_without_other_pressure() {
        assert!(super::checkpoint_followup_required(0, 100, false, 1));
        assert!(!super::checkpoint_followup_required(0, 100, false, 0));
    }

    #[test]
    fn busy_retry_uses_bounded_exponential_backoff() {
        let mut delay = 0;
        for expected in [100, 200, 400, 800, 1_600, 3_200, 5_000, 5_000] {
            delay = super::next_busy_retry_ms(delay, true);
            assert_eq!(delay, expected);
        }
        assert_eq!(super::next_busy_retry_ms(delay, false), 0);
    }

    #[test]
    fn stop_interrupts_maximum_busy_retry() {
        let coordinator = Arc::new(super::CheckpointCoordinator::default());
        let waiting_coordinator = Arc::clone(&coordinator);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let state = waiting_coordinator.state.lock().unwrap();
            entered_tx.send(()).unwrap();
            let state = super::wait_for_busy_retry(&waiting_coordinator, state, 5_000);
            assert!(state.stop_requested);
        });

        entered_rx.recv().unwrap();
        let started = std::time::Instant::now();
        coordinator.stop();
        waiter.join().unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}
