//! Serializable Snapshot Isolation (SSI) conflict tracking (`docs/specs/ssi.md`).
//!
//! The real [`common::SsiTracker`] for `SERIALIZABLE` transactions. It records what
//! each serializable transaction reads — **SIREAD locks** at tuple granularity for
//! point reads and relation granularity for scans — and (from Milestone 5) the
//! rw-antidependency edges between serializable transactions, so a dangerous
//! structure can be detected and a victim aborted with `40001`.
//!
//! All state is **in-memory and transient**, like the deadlock wait-for graph: a
//! crash aborts every in-flight transaction, so there is nothing to persist. Keys are
//! **top-level** transaction ids (a savepoint subxid canonicalizes to its top via
//! [`ActiveTxnRegistry::top_of`]).
//!
//! **Lock order:** a method canonicalizes through the registry *before* taking the
//! manager lock, so the manager lock is never held across a registry call (manager →
//! registry, never the reverse).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use common::{Key, Result, Snapshot, SsiTracker, TableId, TxnId};

use crate::registry::ActiveTxnRegistry;

/// How many distinct tuple SIREAD locks one transaction may hold on a single table
/// before they collapse into one relation lock (the memory safety valve,
/// `docs/specs/ssi.md` §5.4). Collapsing is always safe — a relation lock is strictly
/// more conservative than the tuple locks it replaces.
const SSI_TUPLE_LOCK_CAP: usize = 64;

/// Tracks SIREAD locks and (later) rw-conflict edges for `SERIALIZABLE` transactions
/// (`docs/specs/ssi.md`). Sibling of [`crate::lock_manager::LockManager`].
#[derive(Debug)]
pub struct SerializableConflictManager {
    state: Mutex<SsiState>,
    registry: ActiveTxnRegistry,
}

#[derive(Debug, Default)]
struct SsiState {
    /// `table → serializable readers` holding a relation-granularity SIREAD lock.
    relation_readers: HashMap<TableId, HashSet<TxnId>>,
    /// `(table, key) → serializable readers` holding a tuple-granularity SIREAD lock.
    tuple_readers: HashMap<(TableId, Key), HashSet<TxnId>>,
    /// Per top-level serializable transaction.
    txns: HashMap<TxnId, TxnSsi>,
}

/// Per-transaction SSI state, kept until the GC horizon releases its SIREAD locks.
#[derive(Debug)]
struct TxnSsi {
    /// The transaction's snapshot, for the lifetime test and (Milestone 5) the
    /// reader/writer concurrency test.
    snapshot: Arc<Snapshot>,
    /// Tables this transaction holds a relation SIREAD lock on (reverse index for
    /// cleanup).
    relation_locks: HashSet<TableId>,
    /// Keys this transaction holds a tuple SIREAD lock on, grouped by table (reverse
    /// index for cleanup and the per-table cap).
    tuple_locks: HashMap<TableId, HashSet<Key>>,
    /// Set once the transaction has committed or aborted. SIREAD locks outlive the
    /// transaction (a later concurrent writer can still form an edge); they are
    /// released only when the GC horizon passes the reader (`release_up_to`).
    finished: bool,
}

impl SerializableConflictManager {
    pub fn new(registry: ActiveTxnRegistry) -> Self {
        Self {
            state: Mutex::new(SsiState::default()),
            registry,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SsiState> {
        self.state.lock().expect("ssi manager mutex poisoned")
    }

    /// Begin tracking the serializable transaction `txn_id` under `snapshot` (called
    /// when it captures its first-statement snapshot). `txn_id` may be a savepoint
    /// subxid; it is canonicalized to its top-level id. Idempotent: a repeat keeps the
    /// existing (immutable) snapshot and locks.
    pub fn register(&self, txn_id: TxnId, snapshot: Arc<Snapshot>) {
        let top = self.registry.top_of(txn_id);
        let mut st = self.lock();
        st.txns.entry(top).or_insert_with(|| TxnSsi {
            snapshot,
            relation_locks: HashSet::new(),
            tuple_locks: HashMap::new(),
            finished: false,
        });
    }

    /// Mark `top` finished (committed or aborted). Its SIREAD locks are retained
    /// until `release_up_to` drops them. A no-op for an unregistered (non-tracked)
    /// transaction.
    pub fn finished(&self, top: TxnId) {
        let mut st = self.lock();
        if let Some(txn) = st.txns.get_mut(&top) {
            txn.finished = true;
        }
    }

    /// Release the SIREAD locks of every **finished** reader whose snapshot can no
    /// longer be concurrent with any live transaction — i.e. whose `snapshot.xmax`
    /// (the first txn id outside its view) is at or below the GC horizon
    /// `gc_horizon` (the minimum advertised snapshot `xmin`). Once that holds, every
    /// live snapshot began after the reader's view, so no future write can be
    /// concurrent with it and its locks can form no further edges
    /// (`docs/specs/ssi.md` §5.3).
    pub fn release_up_to(&self, gc_horizon: TxnId) {
        let mut st = self.lock();
        let releasable: Vec<TxnId> = st
            .txns
            .iter()
            .filter(|(_, t)| t.finished && t.snapshot.xmax <= gc_horizon)
            .map(|(&id, _)| id)
            .collect();
        for top in releasable {
            let txn = st.txns.remove(&top).expect("just collected");
            for table in txn.relation_locks {
                remove_reader(&mut st.relation_readers, &table, top);
            }
            for (table, keys) in txn.tuple_locks {
                for key in keys {
                    remove_reader(&mut st.tuple_readers, &(table, key), top);
                }
            }
        }
    }

    /// Observability for metrics and tests: `(tracked transactions, relation SIREAD
    /// lock memberships, tuple SIREAD lock memberships)`.
    pub fn tracking_counts(&self) -> (usize, usize, usize) {
        let st = self.lock();
        let relation = st.relation_readers.values().map(HashSet::len).sum();
        let tuple = st.tuple_readers.values().map(HashSet::len).sum();
        (st.txns.len(), relation, tuple)
    }

    /// Record `top`'s relation SIREAD lock on `table` (under the manager lock).
    fn add_relation_lock(st: &mut SsiState, top: TxnId, table: TableId) {
        let Some(txn) = st.txns.get_mut(&top) else {
            return; // not a tracked (registered) serializable transaction
        };
        if txn.relation_locks.insert(table) {
            st.relation_readers.entry(table).or_default().insert(top);
        }
    }
}

/// Drop `reader` from the reader set at `key`, removing the now-empty entry.
fn remove_reader<K: std::hash::Hash + Eq>(
    map: &mut HashMap<K, HashSet<TxnId>>,
    key: &K,
    reader: TxnId,
) {
    if let Some(set) = map.get_mut(key) {
        set.remove(&reader);
        if set.is_empty() {
            map.remove(key);
        }
    }
}

impl SsiTracker for SerializableConflictManager {
    fn record_tuple_read(&self, reader: TxnId, table: TableId, key: &Key) {
        let top = self.registry.top_of(reader);
        let mut st = self.lock();
        let Some(txn) = st.txns.get_mut(&top) else {
            return; // not a tracked serializable transaction
        };
        let table_keys = txn.tuple_locks.entry(table).or_default();
        if !table_keys.insert(key.clone()) {
            return; // already held
        }
        if table_keys.len() > SSI_TUPLE_LOCK_CAP {
            // Safety valve: collapse this table's tuple locks into one relation lock
            // (`docs/specs/ssi.md` §5.4). Strictly more conservative, so correct.
            let keys = std::mem::take(table_keys);
            txn.tuple_locks.remove(&table);
            for k in keys {
                remove_reader(&mut st.tuple_readers, &(table, k), top);
            }
            Self::add_relation_lock(&mut st, top, table);
            return;
        }
        st.tuple_readers
            .entry((table, key.clone()))
            .or_default()
            .insert(top);
    }

    fn record_relation_read(&self, reader: TxnId, table: TableId) {
        let top = self.registry.top_of(reader);
        let mut st = self.lock();
        Self::add_relation_lock(&mut st, top, table);
    }

    fn note_write(&self, _writer: TxnId, _table: TableId, _key: &Key) -> Result<()> {
        // rw-edge formation + detection land in Milestones 5–6; until then a
        // serializable write forms no edges and never fails an SSI check.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn snapshot(xmax: TxnId) -> Arc<Snapshot> {
        Arc::new(Snapshot {
            xmin: 1,
            xmax,
            xip: vec![],
        })
    }

    fn manager() -> SerializableConflictManager {
        SerializableConflictManager::new(ActiveTxnRegistry::new())
    }

    fn relation_has(mgr: &SerializableConflictManager, table: TableId, reader: TxnId) -> bool {
        mgr.lock()
            .relation_readers
            .get(&table)
            .is_some_and(|s| s.contains(&reader))
    }

    fn tuple_has(
        mgr: &SerializableConflictManager,
        table: TableId,
        key: &Key,
        reader: TxnId,
    ) -> bool {
        mgr.lock()
            .tuple_readers
            .get(&(table, key.clone()))
            .is_some_and(|s| s.contains(&reader))
    }

    fn key(n: i64) -> Key {
        Key(vec![common::Value::Integer(n)])
    }

    #[test]
    fn records_relation_and_tuple_reads() {
        let mgr = manager();
        mgr.register(10, snapshot(20));
        mgr.record_relation_read(10, 1);
        mgr.record_tuple_read(10, 2, &key(5));
        assert!(relation_has(&mgr, 1, 10));
        assert!(tuple_has(&mgr, 2, &key(5), 10));
    }

    #[test]
    fn unregistered_reads_are_ignored() {
        // A read by a transaction never registered (e.g. non-serializable, which uses
        // NoSsiTracker anyway) leaves the table untouched — no orphan reader entry.
        let mgr = manager();
        mgr.record_relation_read(99, 1);
        assert!(!relation_has(&mgr, 1, 99));
    }

    #[test]
    fn release_drops_finished_readers_below_horizon() {
        let mgr = manager();
        mgr.register(10, snapshot(50));
        mgr.record_relation_read(10, 1);
        mgr.record_tuple_read(10, 2, &key(5));

        // Still active (not finished): not released even past the horizon.
        mgr.release_up_to(50);
        assert!(relation_has(&mgr, 1, 10));

        // Finished but horizon not yet past its snapshot.xmax (50): retained.
        mgr.finished(10);
        mgr.release_up_to(49);
        assert!(relation_has(&mgr, 1, 10));

        // Finished and horizon reached its snapshot.xmax: released everywhere.
        mgr.release_up_to(50);
        assert!(!relation_has(&mgr, 1, 10));
        assert!(!tuple_has(&mgr, 2, &key(5), 10));
        assert!(!mgr.lock().txns.contains_key(&10));
    }

    #[test]
    fn tuple_locks_collapse_to_relation_lock_past_cap() {
        let mgr = manager();
        mgr.register(10, snapshot(20));
        for k in 0..=(SSI_TUPLE_LOCK_CAP as i64) {
            mgr.record_tuple_read(10, 7, &key(k));
        }
        let st = mgr.lock();
        // Collapsed: no per-table tuple locks remain, and one relation lock stands in.
        assert!(!st.txns[&10].tuple_locks.contains_key(&7));
        assert!(st.relation_readers.get(&7).is_some_and(|s| s.contains(&10)));
        // The earlier tuple-reader entries for table 7 were cleared.
        assert!(!st.tuple_readers.contains_key(&(7, key(0))));
    }
}
