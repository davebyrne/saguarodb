//! Active-transaction registry.
//!
//! Tracks the set of currently in-progress transaction ids together with the set
//! of `xmin`s currently advertised by live snapshots. It feeds two consumers (see
//! `docs/specs/mvcc.md` §5.5, §9):
//!
//! - **Snapshot capture** (Milestones B3/C3) reads the active set to compute a
//!   snapshot's `xmin`/`xip`, and — atomically in the same latched critical
//!   section — advertises that `xmin` so it pins the GC horizon for the snapshot's
//!   lifetime.
//! - **The GC horizon** (Milestone F) is the *minimum advertised snapshot `xmin`*
//!   (`oldest_xmin`), not the oldest active id. A still-live snapshot (especially a
//!   long autocommit `SELECT`, which is not its own transaction and so never
//!   appears in `active`) may hold a frozen `xmin` *below* the oldest active id;
//!   VACUUM must not reclaim a version that snapshot still sees live. Publishing
//!   the `xmin` under the capture latch makes it visible to the horizon.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use common::TxnId;

/// The latched state of the registry: the in-progress transaction ids and a
/// refcounted multiset of `xmin`s advertised by currently-live snapshots.
///
/// Both live under the **same** `Mutex` so a snapshot capture can read the active
/// set and publish its advertised `xmin` in one critical section — this is
/// load-bearing for the capture-vs-horizon race (see `oldest_xmin`).
#[derive(Debug, Default)]
struct RegistryState {
    /// Currently in-progress transaction ids. A [`BTreeSet`] gives an `O(log n)`
    /// minimum for snapshot `xmin` capture.
    active: BTreeSet<TxnId>,
    /// `xmin → count`: the multiset of `xmin`s advertised by live snapshots. A
    /// [`BTreeMap`] gives an `O(log n)` minimum (the GC horizon) and shares one key
    /// across the many snapshots that captured the same `xmin`.
    xmins: BTreeMap<TxnId, usize>,
    /// `subxid → top-level txn id` for currently-active savepoint subtransactions
    /// (`docs/specs/deadlock.md` §4). Populated when a savepoint subxid is allocated
    /// and pruned on deregister. Used only by the deadlock detector to canonicalize
    /// wait-for edges to transaction granularity; it is an in-memory, active-only
    /// map, distinct from a durable `pg_subtrans`, and the visibility path never
    /// consults it.
    subtrans: HashMap<TxnId, TxnId>,
    /// True while a schema rewrite is waiting to publish, or has published, a new
    /// physical table generation. New snapshot captures wait while this is set so
    /// no statement can bind/execute against a generation that is being swapped.
    snapshot_exclusion: bool,
}

#[derive(Debug, Default)]
struct RegistryShared {
    state: Mutex<RegistryState>,
    cvar: Condvar,
}

/// A concurrent set of in-progress transaction ids plus the advertised-`xmin`
/// multiset, with cheap minimums for both.
///
/// The state is behind an `Arc<Mutex<…>>` so an [`AdvertisedSnapshot`] guard can
/// hold a cheap handle and release its advertised `xmin` from `Drop` without a
/// back-reference to [`ServerComponents`](crate::app::ServerComponents).
#[derive(Debug, Default, Clone)]
pub struct ActiveTxnRegistry {
    shared: Arc<RegistryShared>,
}

impl ActiveTxnRegistry {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(RegistryShared::default()),
        }
    }

    /// Register `txn_id` as in-progress. Called when an autocommit unit begins.
    pub fn register(&self, txn_id: TxnId) {
        self.lock().active.insert(txn_id);
    }

    /// Allocate a transaction id and register it as in-progress atomically under
    /// the registry latch (`docs/specs/mvcc.md` §7.1).
    ///
    /// `allocate` is invoked while the latch is held; it must advance the id
    /// allocator (e.g. `next_txn_id.fetch_add(1)`) and return the new id. Doing
    /// the increment and the registration under one latch closes the torn-snapshot
    /// window: a concurrent [`capture`](Self::capture), which also takes the latch,
    /// can never observe the advanced allocator boundary without also observing
    /// this transaction in the active set. Without the shared latch a reader could
    /// read `xmax` after the increment but the active set before the insert,
    /// wrongly treating the new writer as a settled past transaction.
    pub fn register_allocated<F>(&self, allocate: F) -> TxnId
    where
        F: FnOnce() -> TxnId,
    {
        let mut guard = self.lock();
        let txn_id = allocate();
        guard.active.insert(txn_id);
        txn_id
    }

    /// Allocate and register a savepoint **subxid** owned by top-level `top`,
    /// recording the subxid→top mapping for deadlock-detection canonicalization
    /// (`docs/specs/deadlock.md` §4). Like [`register_allocated`](Self::register_allocated)
    /// the allocate-and-insert is atomic under the latch.
    pub fn register_subxid_allocated<F>(&self, top: TxnId, allocate: F) -> TxnId
    where
        F: FnOnce() -> TxnId,
    {
        let mut guard = self.lock();
        let subxid = allocate();
        guard.active.insert(subxid);
        guard.subtrans.insert(subxid, top);
        subxid
    }

    /// Deregister `txn_id`. Called on commit or rollback.
    pub fn deregister(&self, txn_id: TxnId) {
        let mut guard = self.lock();
        guard.active.remove(&txn_id);
        guard.subtrans.remove(&txn_id);
    }

    /// Atomically deregister every id in `txn_ids` under one latch. Used at a
    /// top-level COMMIT/ROLLBACK of a transaction with savepoint subtransactions to
    /// remove the whole family `{top} ∪ subxids` in a single critical section
    /// (`docs/specs/savepoints.md` §3, §6): a per-id `deregister` loop would let a
    /// concurrent [`capture`](Self::capture) observe a partially-settled family
    /// (e.g. a released subxid already visible while the parent still appears
    /// in-progress). One latched batch makes `capture` see the family either
    /// all-present (all invisible) or all-absent (all settled).
    pub fn deregister_all(&self, txn_ids: &[TxnId]) {
        let mut guard = self.lock();
        for id in txn_ids {
            guard.active.remove(id);
            guard.subtrans.remove(id);
        }
    }

    /// Whether `xid` (a top-level txn or a subxid) is currently in-progress. Used by
    /// the lock manager to decide when a blocked writer's blocker has finished
    /// (`docs/specs/deadlock.md` §4) — keyed on the specific (sub)xid, so a partial
    /// `ROLLBACK TO` that deregisters only a subxid frees its waiters.
    pub fn is_active(&self, xid: TxnId) -> bool {
        self.lock().active.contains(&xid)
    }

    /// The top-level transaction id owning `xid`: the subxid→top mapping for an
    /// active savepoint subxid, or `xid` itself for a top-level id (or any id with no
    /// recorded parent). Used to canonicalize wait-for edges to transaction
    /// granularity for deadlock detection (`docs/specs/deadlock.md` §4).
    pub fn top_of(&self, xid: TxnId) -> TxnId {
        self.lock().subtrans.get(&xid).copied().unwrap_or(xid)
    }

    /// The oldest in-progress transaction id, or `None` if none are active.
    ///
    /// This is the active-id minimum, used for a snapshot's `xmin` (via
    /// [`capture`](Self::capture)), **not** the GC horizon — the horizon is the
    /// minimum *advertised* `xmin` ([`oldest_xmin`](Self::oldest_xmin)), which is
    /// always `<= oldest()`.
    #[allow(
        dead_code,
        reason = "active-id minimum; retained for snapshot xmin reasoning and tests, \
                  superseded as the horizon source by oldest_xmin"
    )]
    pub fn oldest(&self) -> Option<TxnId> {
        self.lock().active.iter().next().copied()
    }

    /// A snapshot of the currently active ids, ascending.
    pub fn active_ids(&self) -> Vec<TxnId> {
        self.lock().active.iter().copied().collect()
    }

    /// The minimum `xmin` advertised by any currently-live snapshot, or `None` if
    /// no snapshot is advertised.
    ///
    /// This is the GC horizon source (`docs/specs/mvcc.md` §9): the horizon is
    /// `oldest_xmin().unwrap_or(next_txn_id)`. It is computed under the same latch
    /// that [`capture`](Self::capture) publishes an `xmin` in, which is what makes
    /// the capture-vs-horizon race safe — see the module docs and
    /// [`ServerComponents::gc_horizon`](crate::app::ServerComponents::gc_horizon).
    pub fn oldest_xmin(&self) -> Option<TxnId> {
        self.lock().xmins.keys().next().copied()
    }

    /// Advertise an already-captured snapshot `xmin`, returning an RAII guard that
    /// releases that advertisement on drop. This is for cursor/portal execution
    /// that keeps using a transaction snapshot independently of the transaction
    /// object that originally captured it.
    pub(crate) fn advertise_xmin(&self, xmin: TxnId) -> AdvertisedSnapshot {
        let mut guard = self.lock();
        *guard.xmins.entry(xmin).or_insert(0) += 1;
        AdvertisedSnapshot {
            shared: Arc::clone(&self.shared),
            xmin,
        }
    }

    /// Capture the data for a visibility snapshot — the active set (as `xip`
    /// source), the allocator boundary (`xmax`), and the snapshot's `xmin` — and
    /// **advertise** that `xmin` to the GC horizon, all under one acquisition of
    /// the registry latch (`docs/specs/mvcc.md` §7.1, §9).
    ///
    /// This supersedes the older `snapshot_with_boundary`: it does everything that
    /// did (read the active set, then read the allocator boundary, so the snapshot
    /// is not torn relative to a concurrent `BEGIN` — see below) **and** publishes
    /// the snapshot's `xmin` into the advertised-`xmin` multiset in the *same*
    /// critical section. Publishing under the same latch that reads `active` is
    /// what closes the capture-vs-horizon race: a concurrent
    /// [`oldest_xmin`](Self::oldest_xmin) (the GC horizon) takes the same latch, so
    /// it can never read a horizon above an `xmin` whose snapshot is already usable
    /// (the snapshot is not returned until this section completes).
    ///
    /// `boundary` is invoked while the latch is held, *after* the active set is
    /// read; the caller passes a closure that loads `next_txn_id`. Holding the
    /// latch across both reads guarantees that any transaction registered before
    /// the boundary is observed is also present in the returned active set — so a
    /// concurrently-begun writer can never be both absent from `xip` and `< xmax`
    /// (which would wrongly make its uncommitted writes visible). Reading the
    /// active set first and the boundary second keeps every active id `< boundary`
    /// (the allocator only grows).
    ///
    /// Returns the active set, the `xmax` boundary, and an [`AdvertisedSnapshot`]
    /// guard whose `xmin` field is the snapshot's `xmin` and whose `Drop` releases
    /// the advertisement. The caller MUST hold the guard for exactly as long as the
    /// snapshot can still be used to read; dropping it sooner reintroduces the
    /// data-loss bug, holding it longer over-pins the horizon.
    pub fn capture<F>(&self, boundary: F) -> (Vec<TxnId>, TxnId, AdvertisedSnapshot)
    where
        F: FnOnce() -> TxnId,
    {
        self.capture_with_exclusion_bypass(false, boundary)
    }

    /// Like [`capture`](Self::capture), but lets a caller that already holds a
    /// writer guard bypass a pending schema-rewrite fence. That narrow exception
    /// prevents deadlocks with explicit write transactions: a schema rewrite holds
    /// the snapshot fence while waiting for the checkpoint guard, and the writer
    /// must be able to finish statements so it can release the writer guard the
    /// checkpoint is waiting on. Read-only transactions pass `false` and wait so
    /// they cannot bind one schema generation and scan another.
    pub fn capture_with_exclusion_bypass<F>(
        &self,
        bypass_snapshot_exclusion: bool,
        boundary: F,
    ) -> (Vec<TxnId>, TxnId, AdvertisedSnapshot)
    where
        F: FnOnce() -> TxnId,
    {
        let mut guard = self.lock();
        while guard.snapshot_exclusion && !bypass_snapshot_exclusion {
            guard = self.wait(guard);
        }
        let active: Vec<TxnId> = guard.active.iter().copied().collect();
        let xmax = boundary();
        let xmin = active.first().copied().unwrap_or(xmax);
        *guard.xmins.entry(xmin).or_insert(0) += 1;
        drop(guard);
        (
            active,
            xmax,
            AdvertisedSnapshot {
                shared: Arc::clone(&self.shared),
                xmin,
            },
        )
    }

    /// Non-blocking form of [`capture_with_exclusion_bypass`](Self::capture_with_exclusion_bypass).
    /// Returns `None` when a schema-rewrite snapshot fence is active and the caller
    /// is not allowed to bypass it. This lets callers drop unrelated guards before
    /// waiting on the fence, avoiding lock-order cycles.
    pub fn try_capture_with_exclusion_bypass<F>(
        &self,
        bypass_snapshot_exclusion: bool,
        boundary: F,
    ) -> Option<(Vec<TxnId>, TxnId, AdvertisedSnapshot)>
    where
        F: FnOnce() -> TxnId,
    {
        let mut guard = self.lock();
        if guard.snapshot_exclusion && !bypass_snapshot_exclusion {
            return None;
        }
        let active: Vec<TxnId> = guard.active.iter().copied().collect();
        let xmax = boundary();
        let xmin = active.first().copied().unwrap_or(xmax);
        *guard.xmins.entry(xmin).or_insert(0) += 1;
        drop(guard);
        Some((
            active,
            xmax,
            AdvertisedSnapshot {
                shared: Arc::clone(&self.shared),
                xmin,
            },
        ))
    }

    /// Wait until a schema-rewrite snapshot fence is no longer active, without
    /// capturing or advertising a snapshot.
    pub(crate) fn wait_for_snapshot_exclusion_clear(&self) {
        let mut guard = self.lock();
        while guard.snapshot_exclusion {
            guard = self.wait(guard);
        }
    }

    /// Capture the active set and allocator boundary without advertising the
    /// snapshot's `xmin`, and without waiting on snapshot exclusion. This is only
    /// for schema-rewrite DDL after [`begin_snapshot_exclusion`](Self::begin_snapshot_exclusion)
    /// has drained advertised snapshots and while the exclusive checkpoint guard
    /// prevents VACUUM from advancing the GC horizon.
    pub(crate) fn capture_unadvertised<F>(&self, boundary: F) -> (Vec<TxnId>, TxnId)
    where
        F: FnOnce() -> TxnId,
    {
        let guard = self.lock();
        let active: Vec<TxnId> = guard.active.iter().copied().collect();
        let xmax = boundary();
        (active, xmax)
    }

    /// Block new snapshot captures and wait for already-advertised snapshots to
    /// drain. The returned guard keeps the exclusion active until it is dropped.
    ///
    /// Schema rewrites that publish a new storage generation must acquire this
    /// before taking the exclusive checkpoint guard. That order avoids deadlocking
    /// around open transactions: the DDL waits for already-advertised snapshots first,
    /// while transactions that already hold a writer guard may still capture the
    /// statement snapshots they need to finish and release that guard.
    pub(crate) fn begin_snapshot_exclusion(&self) -> SnapshotExclusionGuard {
        let mut guard = self.lock();
        while guard.snapshot_exclusion {
            guard = self.wait(guard);
        }
        guard.snapshot_exclusion = true;
        while !guard.xmins.is_empty() {
            guard = self.wait(guard);
        }
        SnapshotExclusionGuard {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Release one advertisement of `xmin`: decrement its count and drop the key at
    /// zero. Called only from [`AdvertisedSnapshot::drop`].
    fn release_advertised(&self, xmin: TxnId) {
        let mut guard = self.lock();
        if let std::collections::btree_map::Entry::Occupied(mut entry) = guard.xmins.entry(xmin) {
            let count = entry.get_mut();
            *count -= 1;
            if *count == 0 {
                entry.remove();
            }
        } else {
            debug_assert!(
                false,
                "released an advertised xmin={xmin} that was not advertised"
            );
        }
        if guard.xmins.is_empty() {
            self.shared.cvar.notify_all();
        }
    }

    fn lock(&self) -> MutexGuard<'_, RegistryState> {
        // A poisoned registry mutex means a panic left the state possibly
        // inconsistent; recovering the guard is the least-bad option (the registry
        // is advisory bookkeeping, not a durability structure).
        self.shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn wait<'a>(&self, guard: MutexGuard<'a, RegistryState>) -> MutexGuard<'a, RegistryState> {
        self.shared
            .cvar
            .wait(guard)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// An RAII handle to a live snapshot's advertised `xmin`. While it is alive the
/// `xmin` pins the GC horizon (`oldest_xmin` can never exceed it), so VACUUM cannot
/// reclaim a version this snapshot still sees live. `Drop` releases the
/// advertisement under the registry latch, so "stop pinning at end of life" is
/// automatic and panic-safe.
///
/// Hold this for exactly the snapshot's usable lifetime (see
/// [`ActiveTxnRegistry::capture`]). It is intentionally **not** `Clone`: each
/// advertisement is released exactly once.
#[derive(Debug)]
pub struct AdvertisedSnapshot {
    shared: Arc<RegistryShared>,
    xmin: TxnId,
}

impl AdvertisedSnapshot {
    /// The advertised `xmin` (this snapshot's lower visibility bound).
    pub fn xmin(&self) -> TxnId {
        self.xmin
    }
}

impl Drop for AdvertisedSnapshot {
    fn drop(&mut self) {
        // Reconstruct a registry handle over the shared state to reuse the
        // poison-recovering `release_advertised`. `Drop` takes the registry latch;
        // callers must never hold that latch across a guard drop (capture/release
        // each take and release it within their own scope, so no re-entrancy).
        let registry = ActiveTxnRegistry {
            shared: Arc::clone(&self.shared),
        };
        registry.release_advertised(self.xmin);
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotExclusionGuard {
    shared: Arc<RegistryShared>,
}

impl Drop for SnapshotExclusionGuard {
    fn drop(&mut self) {
        let mut guard = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.snapshot_exclusion = false;
        drop(guard);
        self.shared.cvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::ActiveTxnRegistry;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn register_and_deregister_track_membership() {
        let registry = ActiveTxnRegistry::new();
        registry.register(5);
        registry.register(3);
        assert_eq!(registry.active_ids(), vec![3, 5]);
        assert_eq!(registry.oldest(), Some(3));

        registry.deregister(3);
        assert_eq!(registry.active_ids(), vec![5]);
        assert_eq!(registry.oldest(), Some(5));

        registry.deregister(5);
        assert!(registry.active_ids().is_empty());
        assert_eq!(registry.oldest(), None);
    }

    #[test]
    fn capture_advertises_xmin_and_guard_release_clears_it() {
        let registry = ActiveTxnRegistry::new();
        registry.register(50);
        registry.register(70);

        // No snapshot advertised yet.
        assert_eq!(registry.oldest_xmin(), None);

        // Capture with boundary 100: xmin is the oldest active id (50).
        let (active, xmax, guard) = registry.capture(|| 100);
        assert_eq!(active, vec![50, 70]);
        assert_eq!(xmax, 100);
        assert_eq!(guard.xmin(), 50);
        assert_eq!(registry.oldest_xmin(), Some(50));

        // The oldest active id advancing does NOT advance the advertised xmin: the
        // frozen snapshot still pins 50 (this is the bug the horizon must respect).
        registry.deregister(50);
        assert_eq!(registry.oldest(), Some(70));
        assert_eq!(registry.oldest_xmin(), Some(50));

        // Dropping the guard releases the advertisement; the horizon is free again.
        drop(guard);
        assert_eq!(registry.oldest_xmin(), None);
    }

    #[test]
    fn advertised_xmins_are_a_refcounted_multiset() {
        let registry = ActiveTxnRegistry::new();
        registry.register(30);

        // Two snapshots share xmin=30.
        let (_a, _x, g1) = registry.capture(|| 50);
        let (_a, _x, g2) = registry.capture(|| 50);
        assert_eq!(g1.xmin(), 30);
        assert_eq!(g2.xmin(), 30);
        assert_eq!(registry.oldest_xmin(), Some(30));

        // Dropping one still leaves 30 advertised by the other.
        drop(g1);
        assert_eq!(registry.oldest_xmin(), Some(30));

        // Dropping the last clears it.
        drop(g2);
        assert_eq!(registry.oldest_xmin(), None);
    }

    #[test]
    fn oldest_xmin_is_the_minimum_over_advertised_snapshots() {
        let registry = ActiveTxnRegistry::new();
        registry.register(40);
        let (_a, _x, g_low) = registry.capture(|| 100); // xmin = 40

        registry.register(60);
        // A later snapshot derives a higher xmin (oldest active is still 40 here),
        // so capture again after the 40-txn leaves to get a distinct higher xmin.
        drop(g_low);
        let (_a, _x, g40) = registry.capture(|| 100); // xmin = 40 (40 still active)
        registry.deregister(40);
        let (_a, _x, g60) = registry.capture(|| 100); // xmin = 60
        assert_eq!(registry.oldest_xmin(), Some(40), "the min over {{40, 60}}");
        drop(g40);
        assert_eq!(registry.oldest_xmin(), Some(60), "now only 60 remains");
        drop(g60);
        assert_eq!(registry.oldest_xmin(), None);
    }

    #[test]
    fn snapshot_exclusion_waits_for_advertised_snapshots_to_drain() {
        let registry = ActiveTxnRegistry::new();
        let (_active, _xmax, advertised) = registry.capture(|| 10);
        let (tx, rx) = mpsc::channel();

        let waiter = {
            let registry = registry.clone();
            thread::spawn(move || {
                let _guard = registry.begin_snapshot_exclusion();
                tx.send(()).unwrap();
            })
        };

        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "schema exclusion must wait while a snapshot is advertised"
        );
        drop(advertised);
        rx.recv_timeout(Duration::from_secs(2))
            .expect("schema exclusion should acquire after the snapshot drains");
        waiter.join().unwrap();
    }

    #[test]
    fn pending_snapshot_exclusion_blocks_new_snapshots_while_draining() {
        let registry = ActiveTxnRegistry::new();
        let (_active, _xmax, advertised) = registry.capture(|| 10);
        let (exclude_tx, exclude_rx) = mpsc::channel();
        let (capture_tx, capture_rx) = mpsc::channel();

        let exclusion = {
            let registry = registry.clone();
            thread::spawn(move || {
                let guard = registry.begin_snapshot_exclusion();
                exclude_tx.send(()).unwrap();
                thread::sleep(Duration::from_millis(100));
                drop(guard);
            })
        };

        thread::sleep(Duration::from_millis(50));
        let capture = {
            let registry = registry.clone();
            thread::spawn(move || {
                let (_active, xmax, _advertised) = registry.capture(|| 42);
                capture_tx.send(xmax).unwrap();
            })
        };

        assert!(
            capture_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "a pending schema exclusion must fence new snapshot captures"
        );
        drop(advertised);
        exclude_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("schema exclusion should acquire after old snapshots drain");
        assert_eq!(
            capture_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            42,
            "new snapshot should resume after the exclusion guard drops"
        );
        exclusion.join().unwrap();
        capture.join().unwrap();
    }

    #[test]
    fn pending_snapshot_exclusion_allows_writer_guarded_transactions_to_finish() {
        let registry = ActiveTxnRegistry::new();
        registry.register(7);
        let (_active, _xmax, advertised) = registry.capture_with_exclusion_bypass(true, || 10);
        let (exclude_tx, exclude_rx) = mpsc::channel();

        let exclusion = {
            let registry = registry.clone();
            thread::spawn(move || {
                let _guard = registry.begin_snapshot_exclusion();
                exclude_tx.send(()).unwrap();
            })
        };

        thread::sleep(Duration::from_millis(50));
        let (_active, xmax, own_advertised) = registry.capture_with_exclusion_bypass(true, || 20);
        assert_eq!(xmax, 20);
        assert!(
            exclude_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "the exclusion still waits for the writer-guarded transaction's snapshots"
        );
        drop(advertised);
        drop(own_advertised);
        exclude_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("schema exclusion should acquire after active txn snapshots drain");
        exclusion.join().unwrap();
    }

    #[test]
    fn pending_snapshot_exclusion_blocks_active_transactions_without_writer_guard() {
        let registry = ActiveTxnRegistry::new();
        registry.register(7);
        let guard = registry.begin_snapshot_exclusion();
        let (tx, rx) = mpsc::channel();

        let waiter = {
            let registry = registry.clone();
            thread::spawn(move || {
                let (_active, xmax, _advertised) =
                    registry.capture_with_exclusion_bypass(false, || 42);
                tx.send(xmax).unwrap();
            })
        };

        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "an active read-only transaction must still wait behind schema exclusion"
        );
        drop(guard);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            42,
            "active read-only transaction capture should resume after exclusion drops"
        );
        waiter.join().unwrap();
    }

    #[test]
    fn try_capture_reports_snapshot_exclusion_without_blocking() {
        let registry = ActiveTxnRegistry::new();
        let guard = registry.begin_snapshot_exclusion();

        assert!(
            registry
                .try_capture_with_exclusion_bypass(false, || 42)
                .is_none(),
            "non-bypassing try-capture should report the fence instead of waiting"
        );
        let (_active, xmax, advertised) = registry
            .try_capture_with_exclusion_bypass(true, || 43)
            .expect("writer-guarded try-capture may bypass the fence");
        assert_eq!(xmax, 43);
        drop(advertised);

        drop(guard);
        let (_active, xmax, advertised) = registry
            .try_capture_with_exclusion_bypass(false, || 44)
            .expect("try-capture should succeed after the fence clears");
        assert_eq!(xmax, 44);
        drop(advertised);
    }

    #[test]
    fn capture_waits_while_snapshot_exclusion_is_active() {
        let registry = ActiveTxnRegistry::new();
        let guard = registry.begin_snapshot_exclusion();
        let (tx, rx) = mpsc::channel();

        let waiter = {
            let registry = registry.clone();
            thread::spawn(move || {
                let (_active, xmax, _advertised) = registry.capture(|| 42);
                tx.send(xmax).unwrap();
            })
        };

        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "snapshot capture must wait while schema exclusion is active"
        );
        drop(guard);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            42,
            "snapshot capture should resume after exclusion drops"
        );
        waiter.join().unwrap();
    }
}
