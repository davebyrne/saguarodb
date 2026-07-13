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

use common::{DbError, Key, Result, Snapshot, SqlState, SsiTracker, TableId, TxnId};

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
    /// `table → serializable writers` of any row in the table — the dual of
    /// `relation_readers`, consulted at read time for conflict-out (`docs/specs/ssi.md`
    /// §6). Populated by every `note_write`.
    relation_writers: HashMap<TableId, HashSet<TxnId>>,
    /// `table → serializable whole-relation writers`. Unlike ordinary row writers,
    /// these conflict with a later tuple read regardless of its key.
    whole_relation_writers: HashMap<TableId, HashSet<TxnId>>,
    /// `(table, key) → serializable writers` of that row — the dual of `tuple_readers`.
    tuple_writers: HashMap<(TableId, Key), HashSet<TxnId>>,
    /// `table → writers` whose tuple-write keys were invalidated by a primary-key
    /// DDL identity rewrite. Later exact reads on that table must conservatively
    /// check these writers at relation granularity until the writer's SSI state is
    /// released.
    promoted_tuple_writers: HashMap<TableId, HashSet<TxnId>>,
    /// Per top-level serializable transaction.
    txns: HashMap<TxnId, TxnSsi>,
    /// Monotonic commit-sequence counter assigned to a serializable transaction when
    /// it passes its commit-time SSI check, so detection can ask "did `T_out` commit
    /// first?" (`docs/specs/ssi.md` §7). Process-local, not a durable commit timestamp.
    next_commit_seq: u64,
}

/// Per-transaction SSI state, kept until the GC horizon releases its SIREAD locks.
#[derive(Debug)]
struct TxnSsi {
    /// The transaction's snapshot, for the lifetime test and the reader/writer
    /// concurrency test (`docs/specs/ssi.md` §6).
    snapshot: Arc<Snapshot>,
    /// Tables this transaction holds a relation SIREAD lock on (reverse index for
    /// cleanup).
    relation_locks: HashSet<TableId>,
    /// Keys this transaction holds a tuple SIREAD lock on, grouped by table (reverse
    /// index for cleanup and the per-table cap).
    tuple_locks: HashMap<TableId, HashSet<Key>>,
    /// Tables this transaction wrote a row in (reverse index into `relation_writers`).
    relation_writes: HashSet<TableId>,
    /// Tables this transaction replaced wholesale (reverse index into
    /// `whole_relation_writers`).
    whole_relation_writes: HashSet<TableId>,
    /// Keys this transaction wrote, grouped by table (reverse index into
    /// `tuple_writers`).
    tuple_writes: HashMap<TableId, HashSet<Key>>,
    /// rw-antidependency successors: `W` such that `self →rw W` (self read an item `W`
    /// then overwrote). Non-empty ⇒ this transaction has an *outgoing* conflict.
    out_edges: HashSet<TxnId>,
    /// rw-antidependency predecessors: `V` such that `V →rw self` (V read an item this
    /// transaction then overwrote). Non-empty ⇒ this transaction has an *incoming*
    /// conflict. A transaction with both an incoming and an outgoing edge is a pivot.
    in_edges: HashSet<TxnId>,
    /// Assigned (from `next_commit_seq`) when this transaction passes its commit-time
    /// SSI check; `None` while it is still in progress. Used to order commits for the
    /// "`T_out` commits first" condition (`docs/specs/ssi.md` §7).
    commit_seq: Option<u64>,
    /// Set once the transaction has committed or aborted. SIREAD locks and edges
    /// outlive the transaction (a later concurrent writer can still form an edge);
    /// they are released only when the GC horizon passes the reader (`release_up_to`).
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
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
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
            relation_writes: HashSet::new(),
            whole_relation_writes: HashSet::new(),
            tuple_writes: HashMap::new(),
            out_edges: HashSet::new(),
            in_edges: HashSet::new(),
            commit_seq: None,
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
            if let Some(txn) = st.txns.remove(&top) {
                purge(&mut st, top, txn);
            }
        }
    }

    /// Drop all SSI state for an **aborted** transaction immediately: an aborted
    /// transaction's reads and writes never happened, so its SIREAD locks and rw-edges
    /// are void and need not wait for the GC horizon (`docs/specs/ssi.md` §8). A no-op
    /// for an unregistered (non-tracked) transaction.
    pub fn aborted(&self, top: TxnId) {
        let mut st = self.lock();
        if let Some(txn) = st.txns.remove(&top) {
            purge(&mut st, top, txn);
        }
    }

    /// The commit-time SSI check for a serializable transaction `top`, run **before**
    /// the WAL `Commit` is flushed (`docs/specs/ssi.md` §7). Returns `Err(40001)` when
    /// committing `top` would complete a dangerous structure; otherwise it stamps
    /// `top` with its commit sequence and returns `Ok`. The acting transaction (`top`)
    /// is always the participant aborted, so the abort is synchronous on its own
    /// thread. A no-op `Ok` for an unregistered transaction.
    pub fn commit_check(&self, top: TxnId) -> Result<()> {
        let mut st = self.lock();
        if !st.txns.contains_key(&top) {
            return Ok(());
        }
        // (a) `top` is itself a pivot whose outbound neighbor committed first.
        if is_doomed_pivot(&st, top) {
            return Err(serialization_failure());
        }
        // (b) `top` is the `T_out` whose committing-first dooms an active in-neighbor
        //     pivot `V` (`V →rw top`, V still in progress and itself having an inbound
        //     edge). Breaking the `V → top` edge by aborting `top` breaks the structure.
        let in_edges: Vec<TxnId> = st.txns[&top].in_edges.iter().copied().collect();
        let dooms_in_neighbor = in_edges.iter().any(|v| {
            st.txns
                .get(v)
                .is_some_and(|vt| vt.commit_seq.is_none() && !vt.in_edges.is_empty())
        });
        if dooms_in_neighbor {
            return Err(serialization_failure());
        }
        // Passed: stamp the commit sequence so later checks can order this commit.
        let seq = st.next_commit_seq;
        st.next_commit_seq = seq
            .checked_add(1)
            .ok_or_else(|| DbError::internal("SSI commit sequence exhausted"))?;
        let txn = st
            .txns
            .get_mut(&top)
            .ok_or_else(|| DbError::internal("SSI transaction disappeared during commit check"))?;
        txn.commit_seq = Some(seq);
        Ok(())
    }

    /// Observability for metrics and tests: `(tracked transactions, relation SIREAD
    /// lock memberships, tuple SIREAD lock memberships)`.
    pub fn tracking_counts(&self) -> (usize, usize, usize) {
        let st = self.lock();
        let relation = st.relation_readers.values().map(HashSet::len).sum();
        let tuple = st.tuple_readers.values().map(HashSet::len).sum();
        (st.txns.len(), relation, tuple)
    }

    /// Promote tuple-granularity SSI state on `table` to relation granularity.
    ///
    /// `ALTER TABLE ... ADD/DROP PRIMARY KEY` changes the table's storage identity
    /// keyspace. Existing tuple locks and tuple-write records are keyed by the old
    /// identity, so future writes/reads under the new identity would miss them.
    /// Relation-granularity tracking is keyspace-neutral and strictly more
    /// conservative.
    pub fn promote_table_identity_locks_to_relation(&self, table: TableId) {
        let mut st = self.lock();
        let readers: Vec<TxnId> = st
            .txns
            .iter()
            .filter(|(_, txn)| txn.tuple_locks.contains_key(&table))
            .map(|(&txn_id, _)| txn_id)
            .collect();
        let writers: Vec<TxnId> = st
            .relation_writers
            .get(&table)
            .map_or_else(Vec::new, |writers| writers.iter().copied().collect());
        if !writers.is_empty() {
            st.promoted_tuple_writers
                .entry(table)
                .or_default()
                .extend(writers.iter().copied());
        }

        for reader in readers {
            let keys = st
                .txns
                .get_mut(&reader)
                .and_then(|txn| txn.tuple_locks.remove(&table))
                .unwrap_or_default();
            for key in keys {
                remove_reader(&mut st.tuple_readers, &(table, key), reader);
            }
            for writer in &writers {
                form_rw_edge(&mut st, reader, *writer);
            }
            Self::add_relation_lock(&mut st, reader, table);
        }
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

/// Fully remove `top` (already taken out of `st.txns`) from the graph: its SIREAD
/// lock memberships in the reader maps and its edges in every neighbor's edge set.
fn purge(st: &mut SsiState, top: TxnId, txn: TxnSsi) {
    for table in txn.relation_locks {
        remove_reader(&mut st.relation_readers, &table, top);
    }
    for (table, keys) in txn.tuple_locks {
        for key in keys {
            remove_reader(&mut st.tuple_readers, &(table, key), top);
        }
    }
    for table in txn.relation_writes {
        remove_reader(&mut st.relation_writers, &table, top);
    }
    for table in txn.whole_relation_writes {
        remove_reader(&mut st.whole_relation_writers, &table, top);
    }
    for (table, keys) in txn.tuple_writes {
        for key in keys {
            remove_reader(&mut st.tuple_writers, &(table, key), top);
        }
    }
    let mut empty_promoted = Vec::new();
    for (table, writers) in &mut st.promoted_tuple_writers {
        writers.remove(&top);
        if writers.is_empty() {
            empty_promoted.push(*table);
        }
    }
    for table in empty_promoted {
        st.promoted_tuple_writers.remove(&table);
    }
    // `top →rw w` ⟹ `w.in_edges` holds `top`; `v →rw top` ⟹ `v.out_edges` holds `top`.
    for w in txn.out_edges {
        if let Some(wt) = st.txns.get_mut(&w) {
            wt.in_edges.remove(&top);
        }
    }
    for v in txn.in_edges {
        if let Some(vt) = st.txns.get_mut(&v) {
            vt.out_edges.remove(&top);
        }
    }
}

/// Whether `writer` is concurrent with the reader snapshot `s` — i.e. the reader did
/// NOT see the writer's effect (the writer is in the reader's future, or was
/// in-progress at the reader's snapshot). This is the condition for a relevant
/// rw-antidependency edge `reader →rw writer` (`docs/specs/ssi.md` §6); if the reader
/// had seen the writer's version there would be no antidependency.
fn concurrent(s: &Snapshot, writer: TxnId) -> bool {
    writer >= s.xmax || s.xip.contains(&writer)
}

/// Whether `t` is a pivot whose outbound rw-neighbor already committed first: `t` has
/// both an inbound and an outbound edge, and some `t →rw W` target has committed
/// (`W.commit_seq` is set) while `t` is still in progress — the dangerous structure of
/// `docs/specs/ssi.md` §7.
fn is_doomed_pivot(st: &SsiState, t: TxnId) -> bool {
    let Some(txn) = st.txns.get(&t) else {
        return false;
    };
    if txn.in_edges.is_empty() || txn.out_edges.is_empty() {
        return false;
    }
    txn.out_edges
        .iter()
        .any(|w| st.txns.get(w).is_some_and(|o| o.commit_seq.is_some()))
}

/// Form the rw-antidependency edge `reader →rw writer` if relevant: distinct
/// transactions, the writer is a tracked serializable transaction, and the writer is
/// concurrent with the reader (the reader did not see the writer's version, §6). The
/// shared core of both conflict-in (`note_write`) and conflict-out (`record_*`).
fn form_rw_edge(st: &mut SsiState, reader: TxnId, writer: TxnId) {
    if reader == writer || !st.txns.contains_key(&writer) {
        return;
    }
    let concurrent = match st.txns.get(&reader) {
        Some(rt) => concurrent(&rt.snapshot, writer),
        None => return,
    };
    if !concurrent {
        return;
    }
    if let Some(reader_txn) = st.txns.get_mut(&reader) {
        reader_txn.out_edges.insert(writer);
    }
    if let Some(writer_txn) = st.txns.get_mut(&writer) {
        writer_txn.in_edges.insert(reader);
    }
}

/// The `40001` raised when an SSI check aborts a transaction (matches PostgreSQL's
/// message and reuses the `SerializationFailure` SQLSTATE — `docs/specs/ssi.md` §3).
fn serialization_failure() -> DbError {
    DbError::execute(
        SqlState::SerializationFailure,
        "could not serialize access due to read/write dependencies among transactions",
    )
}

impl SsiTracker for SerializableConflictManager {
    fn record_tuple_read(&self, reader: TxnId, table: TableId, key: &Key) {
        let top = self.registry.top_of(reader);
        let mut st = self.lock();
        if !st.txns.contains_key(&top) {
            return; // not a tracked serializable transaction
        }
        // Conflict-out (§6): a concurrent writer may have already written this exact
        // row, so form `reader →rw writer` for each recorded writer of (table, key).
        let mut writers: HashSet<TxnId> = st
            .tuple_writers
            .get(&(table, key.clone()))
            .map_or_else(HashSet::new, |s| s.iter().copied().collect());
        if let Some(promoted) = st.promoted_tuple_writers.get(&table) {
            writers.extend(promoted.iter().copied());
        }
        if let Some(whole_relation) = st.whole_relation_writers.get(&table) {
            writers.extend(whole_relation.iter().copied());
        }
        for w in writers {
            form_rw_edge(&mut st, top, w);
        }
        // Record the tuple SIREAD lock (with the per-table cap collapse). Each tuple
        // read already formed its own conflict-out above, so a later collapse to a
        // relation lock loses no conflict-out edge (it only coarsens future conflict-in).
        let Some(txn) = st.txns.get_mut(&top) else {
            return;
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
        if !st.txns.contains_key(&top) {
            return;
        }
        // Conflict-out (§6): a concurrent writer may have already written some row in
        // this table, which a full scan read; form `reader →rw writer` for each.
        let writers: Vec<TxnId> = st
            .relation_writers
            .get(&table)
            .map_or_else(Vec::new, |s| s.iter().copied().collect());
        for w in writers {
            form_rw_edge(&mut st, top, w);
        }
        Self::add_relation_lock(&mut st, top, table);
    }

    fn note_write(&self, writer: TxnId, table: TableId, key: &Key) -> Result<()> {
        let writer_top = self.registry.top_of(writer);
        let mut st = self.lock();
        if !st.txns.contains_key(&writer_top) {
            return Ok(()); // not a tracked serializable writer
        }
        // Conflict-in (§6): form `reader →rw writer` for each already-recorded SIREAD
        // holder of this item (relation readers of the table, tuple readers of the key).
        let readers: Vec<TxnId> = st
            .relation_readers
            .get(&table)
            .into_iter()
            .chain(st.tuple_readers.get(&(table, key.clone())))
            .flat_map(|s| s.iter().copied())
            .collect();
        for reader in readers {
            form_rw_edge(&mut st, reader, writer_top);
        }
        // Record this write in the writer tables (both grains) + reverse index, so a
        // concurrent reader that reads the item LATER forms the conflict-out edge (§6).
        let txn = st.txns.get_mut(&writer_top).ok_or_else(|| {
            DbError::internal("SSI writer disappeared while recording tuple write")
        })?;
        txn.relation_writes.insert(table);
        txn.tuple_writes
            .entry(table)
            .or_default()
            .insert(key.clone());
        st.relation_writers
            .entry(table)
            .or_default()
            .insert(writer_top);
        st.tuple_writers
            .entry((table, key.clone()))
            .or_default()
            .insert(writer_top);
        // Edge-time detection (§7): the new inbound edges can make the writer a pivot
        // if it already had an outbound edge whose target committed first. The acting
        // writer is the participant aborted.
        if is_doomed_pivot(&st, writer_top) {
            return Err(serialization_failure());
        }
        Ok(())
    }

    fn note_relation_write(&self, writer: TxnId, table: TableId) -> Result<()> {
        let writer_top = self.registry.top_of(writer);
        let mut st = self.lock();
        if !st.txns.contains_key(&writer_top) {
            return Ok(());
        }
        let readers = st
            .relation_readers
            .get(&table)
            .into_iter()
            .chain(
                st.tuple_readers
                    .iter()
                    .filter_map(|((read_table, _), readers)| {
                        (*read_table == table).then_some(readers)
                    }),
            )
            .flat_map(|readers| readers.iter().copied())
            .collect::<HashSet<_>>();
        for reader in readers {
            form_rw_edge(&mut st, reader, writer_top);
        }
        let txn = st.txns.get_mut(&writer_top).ok_or_else(|| {
            DbError::internal("SSI writer disappeared while recording relation write")
        })?;
        txn.relation_writes.insert(table);
        txn.whole_relation_writes.insert(table);
        st.relation_writers
            .entry(table)
            .or_default()
            .insert(writer_top);
        st.whole_relation_writers
            .entry(table)
            .or_default()
            .insert(writer_top);
        if is_doomed_pivot(&st, writer_top) {
            return Err(serialization_failure());
        }
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

    #[test]
    fn primary_key_ddl_promotion_keeps_future_writes_conservative() {
        let mgr = manager();
        mgr.register(10, snapshot(20));
        mgr.record_tuple_read(10, 7, &key(1));

        mgr.promote_table_identity_locks_to_relation(7);
        assert!(!tuple_has(&mgr, 7, &key(1), 10));
        assert!(relation_has(&mgr, 7, 10));

        mgr.register(25, snapshot(30));
        mgr.note_write(25, 7, &key(999)).unwrap();
        let st = mgr.lock();
        assert!(
            st.txns[&10].out_edges.contains(&25),
            "promoted relation lock catches writes under a different identity key"
        );
        assert!(st.txns[&25].in_edges.contains(&10));
    }

    #[test]
    fn primary_key_ddl_promotion_keeps_prior_writes_conservative() {
        let mgr = manager();
        mgr.register(25, snapshot_excluding(40, &[10]));
        mgr.note_write(25, 7, &key(1)).unwrap();
        mgr.commit_check(25).unwrap();
        mgr.finished(25);

        mgr.promote_table_identity_locks_to_relation(7);

        mgr.register(10, snapshot_excluding(40, &[25]));
        mgr.record_tuple_read(10, 7, &key(999));
        let st = mgr.lock();
        assert!(
            st.txns[&10].out_edges.contains(&25),
            "exact reads under a new identity key must see writers retained from the old keyspace"
        );
        assert!(st.txns[&25].in_edges.contains(&10));
    }

    /// A snapshot that excludes `others` (lists them in `xip`), so a writer among them
    /// is concurrent with this reader.
    fn snapshot_excluding(xmax: TxnId, others: &[TxnId]) -> Arc<Snapshot> {
        Arc::new(Snapshot {
            xmin: 1,
            xmax,
            xip: others.to_vec(),
        })
    }

    #[test]
    fn conflict_out_tuple_read_after_concurrent_write_forms_edge() {
        // Write-before-read ordering: the writer wrote the row BEFORE the reader read
        // it, so the edge can only form at the read (conflict-out, `docs/specs/ssi.md` §6).
        let mgr = manager();
        mgr.register(10, snapshot_excluding(30, &[20])); // writer W=10
        mgr.note_write(10, 1, &key(5)).unwrap();
        mgr.register(20, snapshot_excluding(30, &[10])); // reader R=20, concurrent with W
        mgr.record_tuple_read(20, 1, &key(5)); // reads the row W superseded
        let st = mgr.lock();
        assert!(
            st.txns[&20].out_edges.contains(&10),
            "conflict-out edge R→W formed at read time"
        );
        assert!(st.txns[&10].in_edges.contains(&20));
    }

    #[test]
    fn conflict_out_relation_read_after_concurrent_write_forms_edge() {
        let mgr = manager();
        mgr.register(10, snapshot_excluding(30, &[20]));
        mgr.note_write(10, 7, &key(5)).unwrap(); // W wrote some row in table 7
        mgr.register(20, snapshot_excluding(30, &[10]));
        mgr.record_relation_read(20, 7); // a full scan reads the row W superseded
        assert!(
            mgr.lock().txns[&20].out_edges.contains(&10),
            "relation read forms the conflict-out edge against a prior concurrent writer"
        );
    }

    #[test]
    fn note_write_forms_edge_for_a_concurrent_reader_only() {
        let mgr = manager();
        mgr.register(10, snapshot(20)); // reader R; writers >= 20 are concurrent
        mgr.record_relation_read(10, 1);
        mgr.register(25, snapshot(30)); // writer W = 25 (>= R.xmax 20 ⇒ concurrent)
        assert!(mgr.note_write(25, 1, &key(0)).is_ok());
        let st = mgr.lock();
        assert!(st.txns[&10].out_edges.contains(&25), "edge R→W formed");
        assert!(st.txns[&25].in_edges.contains(&10));
    }

    #[test]
    fn relation_write_conflicts_with_prior_and_later_reads_at_both_grains() {
        let mgr = manager();
        mgr.register(10, snapshot(20));
        mgr.record_relation_read(10, 7);
        mgr.register(11, snapshot(20));
        mgr.record_tuple_read(11, 7, &key(1));
        mgr.register(25, snapshot(30));

        mgr.note_relation_write(25, 7).unwrap();
        {
            let st = mgr.lock();
            assert!(st.txns[&10].out_edges.contains(&25));
            assert!(st.txns[&11].out_edges.contains(&25));
            assert!(st.txns[&25].in_edges.contains(&10));
            assert!(st.txns[&25].in_edges.contains(&11));
        }

        mgr.register(30, snapshot_excluding(40, &[25]));
        mgr.record_relation_read(30, 7);
        mgr.register(31, snapshot_excluding(40, &[25]));
        mgr.record_tuple_read(31, 7, &key(999));
        let st = mgr.lock();
        assert!(st.txns[&30].out_edges.contains(&25));
        assert!(st.txns[&31].out_edges.contains(&25));
    }

    #[test]
    fn note_write_skips_self_and_non_concurrent_readers() {
        // Self: a transaction reading then writing the same item forms no edge.
        let mgr = manager();
        mgr.register(10, snapshot(20));
        mgr.record_relation_read(10, 1);
        assert!(mgr.note_write(10, 1, &key(0)).is_ok());
        assert!(mgr.lock().txns[&10].in_edges.is_empty());

        // Non-concurrent: the reader's snapshot already saw the writer (writer < xmax,
        // not in xip), so no antidependency.
        let mgr = manager();
        mgr.register(30, snapshot(40)); // R sees everything < 40 (empty xip)
        mgr.record_relation_read(30, 1);
        mgr.register(25, snapshot(50));
        assert!(mgr.note_write(25, 1, &key(0)).is_ok());
        assert!(
            !mgr.lock().txns[&30].out_edges.contains(&25),
            "R saw W ⇒ no edge"
        );
    }

    #[test]
    fn write_skew_makes_a_pivot_and_one_commit_aborts() {
        let mgr = manager();
        // Two mutually-concurrent transactions (each lists the other in xip).
        mgr.register(10, snapshot_excluding(12, &[11]));
        mgr.register(11, snapshot_excluding(12, &[10]));
        mgr.record_relation_read(10, 1); // T1 reads table 1
        mgr.record_relation_read(11, 2); // T2 reads table 2
        assert!(mgr.note_write(10, 2, &key(0)).is_ok()); // T1 writes table 2 (T2 read it)
        assert!(mgr.note_write(11, 1, &key(0)).is_ok()); // T2 writes table 1 (T1 read it)
        // Both are pivots; the first to commit is the victim.
        assert_eq!(
            mgr.commit_check(10).unwrap_err().code,
            SqlState::SerializationFailure
        );
        mgr.aborted(10);
        // The survivor commits cleanly (its edges to the aborted pivot were purged).
        assert!(mgr.commit_check(11).is_ok());
    }

    #[test]
    fn edge_time_abort_once_out_neighbor_committed() {
        let mgr = manager();
        // Pivot reads tables 1 and 3; both 10 and 30 are concurrent with it.
        mgr.register(20, snapshot_excluding(25, &[10, 30]));
        mgr.record_relation_read(20, 1);
        mgr.record_relation_read(20, 3);
        // T_out=10 writes table 1 → edge pivot→T_out, then T_out commits first.
        mgr.register(10, snapshot(25));
        assert!(mgr.note_write(10, 1, &key(0)).is_ok());
        assert!(mgr.commit_check(10).is_ok());
        // R_in=30 reads table 3; the pivot writes table 3 → gains an in-edge, becoming
        // a pivot whose out-neighbor already committed ⇒ edge-time abort.
        mgr.register(30, snapshot_excluding(25, &[10, 20]));
        mgr.record_relation_read(30, 3);
        assert_eq!(
            mgr.note_write(20, 3, &key(0)).unwrap_err().code,
            SqlState::SerializationFailure
        );
    }

    #[test]
    fn pivot_committing_before_its_out_neighbor_proceeds() {
        let mgr = manager();
        mgr.register(20, snapshot_excluding(25, &[10, 30]));
        mgr.record_relation_read(20, 1); // pivot's future out-edge target (table 1)
        // R_in=30 reads table 3; pivot writes table 3 → in-edge for the pivot.
        mgr.register(30, snapshot_excluding(25, &[10, 20]));
        mgr.record_relation_read(30, 3);
        assert!(mgr.note_write(20, 3, &key(0)).is_ok());
        // T_out=10 writes table 1 → pivot gains its out-edge, but T_out has NOT
        // committed, so no edge-time abort.
        mgr.register(10, snapshot(25));
        assert!(mgr.note_write(10, 1, &key(0)).is_ok());
        // The pivot commits BEFORE its out-neighbor ⇒ it is not the dangerous pivot.
        assert!(mgr.commit_check(20).is_ok());
    }
}
