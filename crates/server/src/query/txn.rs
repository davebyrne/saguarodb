use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{DbError, IsolationLevel, Result, Snapshot, SqlState, StatementContext};
use executor::{ExecutionContext, ExecutionResult};
use storage::StorageEngine;
use wal::{WalRecord, WalRecordKind};

use super::{
    QueryService, Transaction, TransactionControl, begin_complete, commit_complete,
    rollback_complete, set_complete,
};
use crate::checkpoint::record_commit_and_maybe_checkpoint;
use crate::registry::AdvertisedSnapshot;

impl QueryService {
    /// Handle BEGIN/COMMIT/ROLLBACK/SET TRANSACTION/SET SESSION CHARACTERISTICS
    /// against the session's transaction `slot` and `default_isolation` (the session
    /// default, in/out). Only `Begin` reads the default and only
    /// `SetSessionCharacteristics` updates it; every other arm returns it unchanged.
    pub(super) fn handle_transaction_control(
        &self,
        kind: TransactionControl,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        _cancel: &AtomicBool,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        match kind {
            TransactionControl::Begin(isolation) => match slot {
                // Postgres: BEGIN inside a transaction is a warning + no-op that
                // stays 'T'. We keep the open transaction (and its existing
                // isolation) and report success; the requested level is ignored,
                // matching Postgres' "there is already a transaction in progress".
                Some(txn) => (Some(txn), default_isolation, Ok(begin_complete())),
                // No explicit level INHERITS the session default (`docs/specs/mvcc.md`
                // §10 G2: explicit BEGIN level > SET TRANSACTION > session default >
                // Read Committed). An explicit `ISOLATION LEVEL` overrides it for this
                // one transaction.
                None => match self.begin_transaction(isolation.unwrap_or(default_isolation)) {
                    Ok(txn) => (Some(txn), default_isolation, Ok(begin_complete())),
                    Err(err) => (None, default_isolation, Err(err)),
                },
            },
            TransactionControl::SetTransaction(isolation) => {
                let (slot, result) = self.handle_set_transaction(isolation, slot);
                (slot, default_isolation, result)
            }
            TransactionControl::SetSessionCharacteristics(isolation) => {
                self.handle_set_session_characteristics(isolation, slot, default_isolation)
            }
            TransactionControl::Commit => match slot {
                // COMMIT of a healthy transaction commits durably.
                Some(txn) if !txn.failed => {
                    let result = self.commit_transaction(txn).map(|()| commit_complete());
                    (None, default_isolation, result)
                }
                // COMMIT of a failed transaction issues ROLLBACK (Postgres
                // behavior), returning to Idle.
                Some(txn) => {
                    self.abort_transaction(txn);
                    // Postgres tags this `ROLLBACK`, the actual action taken.
                    (None, default_isolation, Ok(rollback_complete()))
                }
                // COMMIT with no open transaction is a no-op warning, stays Idle.
                None => (None, default_isolation, Ok(commit_complete())),
            },
            TransactionControl::Rollback => match slot {
                Some(txn) => {
                    self.abort_transaction(txn);
                    (None, default_isolation, Ok(rollback_complete()))
                }
                // ROLLBACK with no open transaction is a no-op warning, stays Idle.
                None => (None, default_isolation, Ok(rollback_complete())),
            },
        }
    }

    /// Handle `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`
    /// (`docs/specs/mvcc.md` §10 Milestone G2). Postgres semantics: it sets the
    /// per-connection DEFAULT isolation for FUTURE transactions and does NOT change
    /// an already-open transaction's level; it is allowed inside a transaction block
    /// (unlike `SET TRANSACTION`, it has no before-first-query rule) and persists
    /// across transactions on this connection.
    ///
    /// - With an isolation-level mode, update `default_isolation` to the mapped level
    ///   and leave the open transaction (if any) untouched.
    /// - With no isolation-level mode (e.g. `READ WRITE` only) it is a no-op success
    ///   that leaves the default unchanged.
    /// - Inside an already-failed (`'E'`) block it is rejected with `25P02` like any
    ///   other non-COMMIT/ROLLBACK statement, leaving the default unchanged.
    fn handle_set_session_characteristics(
        &self,
        isolation: Option<IsolationLevel>,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        if let Some(txn) = &slot
            && txn.failed
        {
            // A failed block rejects everything but COMMIT/ROLLBACK with `25P02` and
            // stays 'E'; the session default is unchanged.
            return (
                slot,
                default_isolation,
                Err(DbError::execute(
                    SqlState::InFailedSqlTransaction,
                    "current transaction is aborted, commands ignored until end of transaction block",
                )),
            );
        }
        // Update the session default only when a level was given; otherwise it is a
        // no-op success. The open transaction (if any) is returned UNCHANGED — this
        // statement never mutates the current transaction's isolation, matching
        // Postgres (it affects only future transactions).
        let updated = isolation.unwrap_or(default_isolation);
        (slot, updated, Ok(set_complete()))
    }

    /// Handle `SET TRANSACTION ISOLATION LEVEL <level>` against the session's
    /// transaction `slot` (`docs/specs/mvcc.md` §10 Milestone G). Postgres
    /// semantics:
    ///
    /// - **Failed ('E') block**: rejected with `25P02` like any non-COMMIT/ROLLBACK
    ///   statement (the block must be ended first).
    /// - **Open transaction, before its first query** (`!first_statement_ran`): set
    ///   the current transaction's isolation level. A `SET TRANSACTION` with no
    ///   isolation-level mode is a successful no-op.
    /// - **Open transaction, after its first query**: error
    ///   (`SET TRANSACTION ... must be called before any query`), which — like any
    ///   statement error inside a block — poisons it to 'E'.
    /// - **No open transaction** (autocommit): a no-op success. A bare
    ///   `SET TRANSACTION` runs as its own implicit single-statement transaction
    ///   that does no query, so there is nothing for the level to affect; Postgres
    ///   treats it as a no-op (and warns), which we mirror as a plain success.
    fn handle_set_transaction(
        &self,
        isolation: Option<IsolationLevel>,
        slot: Option<Transaction>,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
        match slot {
            Some(txn) if txn.failed => {
                // A failed block rejects everything but COMMIT/ROLLBACK with `25P02`
                // and stays 'E', matching the data-statement gate in
                // `run_bound_in_transaction`.
                (
                    Some(txn),
                    Err(DbError::execute(
                        SqlState::InFailedSqlTransaction,
                        "current transaction is aborted, commands ignored until end of transaction block",
                    )),
                )
            }
            Some(mut txn) => {
                if txn.first_statement_ran {
                    txn.failed = true;
                    return (
                        Some(txn),
                        Err(DbError::execute(
                            SqlState::FeatureNotSupported,
                            "SET TRANSACTION ISOLATION LEVEL must be called before any query",
                        )),
                    );
                }
                if let Some(level) = isolation {
                    txn.isolation = level;
                }
                (Some(txn), Ok(set_complete()))
            }
            // No open transaction: a no-op success (autocommit).
            None => (None, Ok(set_complete())),
        }
    }

    /// Allocate a transaction id, register it active, and build the explicit
    /// transaction at `isolation`. The write guard is acquired lazily on the first
    /// write; the snapshot is captured on the first statement (per isolation).
    fn begin_transaction(&self, isolation: IsolationLevel) -> Result<Transaction> {
        let txn_id = self.register_active_txn();
        Ok(Transaction {
            txn_id,
            isolation,
            first_statement_ran: false,
            failed: false,
            write_guard: None,
            rr_snapshot: None,
            rr_advertised: None,
            dead_versions_pending: 0,
        })
    }

    /// Acquire the SHARED writer guard for an explicit transaction's first write,
    /// holding it on `txn` for the whole write-transaction. The guard is shared
    /// (E2b lock inversion, `docs/specs/mvcc.md` §10 E2b): acquiring it does not
    /// block on another connection's writer — only on a checkpoint holding the
    /// exclusive guard.
    ///
    /// Correctness assertion (no longer a deadlock guard): a transaction must hold
    /// AT MOST ONE writer guard. The shared guard IS re-entrant — re-acquiring it on
    /// the same thread cannot self-deadlock — so this is a cheap invariant check
    /// ("one writer guard per transaction"), not a hang-prevention measure. It
    /// catches a routing regression that would leak a second guard, which would keep
    /// a writer in flight past commit/abort and could stall a checkpoint waiting to
    /// drain.
    pub(super) fn acquire_write_guard(&self, txn: &mut Transaction) -> Result<()> {
        if txn.write_guard.is_some() {
            debug_assert!(
                false,
                "duplicate write-guard acquisition: this transaction already holds \
                 a writer guard"
            );
            return Err(DbError::internal(
                "duplicate write-guard acquisition (transaction already holds a writer guard)",
            ));
        }
        let guard = self.components.concurrency.begin_writer()?;
        txn.write_guard = Some(guard);
        Ok(())
    }

    /// Commit an explicit transaction: append a `Commit` record, flush (fsync),
    /// set `CLOG=Committed` (done at flush), run post-durable-commit cleanup, and
    /// deregister. Releasing the write guard happens when `txn` is dropped after
    /// this returns.
    fn commit_transaction(&self, txn: Transaction) -> Result<()> {
        let txn_id = txn.txn_id;
        let dead_versions = txn.dead_versions_pending;
        // A read-only explicit transaction (no write guard, no writes) has nothing
        // durable to commit: just deregister and return. Appending a `Commit` for
        // it is harmless but unnecessary; skip it so a pure-reader transaction
        // never touches the WAL.
        if txn.write_guard.is_none() {
            self.components.active_txns.deregister(txn_id);
            return Ok(());
        }

        if let Err(err) = self.append_and_flush_commit(txn_id) {
            // The commit is not durable: abort instead (append `Abort` +
            // CLOG=Aborted, clear per-txn bookkeeping, restore DDL metadata)
            // so the transaction's effects are hidden by the CLOG. Abort is
            // status-based — no page-content undo (`docs/specs/mvcc.md` §4
            // Decision 3, Milestone D1).
            self.rollback_pre_durable_or_die(txn_id, None);
            // `txn` (and its write guard) drops here, releasing the guard.
            return Err(err);
        }

        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        self.components.active_txns.deregister(txn_id);
        // `txn` drops here, releasing the exclusive write guard.
        drop(txn);

        // Fold the committed transaction's dead versions into the auto-prune counter
        // BEFORE the checkpoint trigger (`docs/specs/mvcc.md` §9, F4b): only a durable
        // commit reaches here, so an aborted transaction never advances the counter.
        self.components.add_dead_versions(dead_versions);

        if let Err(err) = record_commit_and_maybe_checkpoint(&self.components) {
            eprintln!("checkpoint failed after committed transaction: {err}");
        }
        Ok(())
    }

    /// Abort an explicit transaction: append an `Abort` record, set `CLOG=Aborted`,
    /// clear per-txn bookkeeping, restore DDL metadata, and deregister. Abort is
    /// status-based (`docs/specs/mvcc.md` §4 Decision 3,
    /// Milestone D1): the transaction's modified tuples stay in the heap, hidden by
    /// the CLOG and reclaimed by VACUUM — there is NO before-image page undo.
    /// Dropping `txn` releases the write guard. A pre-durable rollback failure is
    /// fatal (the engine cannot guarantee consistency), matching the autocommit
    /// path.
    pub(super) fn abort_transaction(&self, txn: Transaction) {
        let txn_id = txn.txn_id;
        if txn.write_guard.is_none() {
            // A read-only transaction wrote nothing: no Abort record, no cleanup,
            // just deregister.
            self.components.active_txns.deregister(txn_id);
            return;
        }
        self.rollback_pre_durable_or_die(txn_id, None);
        // `txn` drops here, releasing the exclusive write guard.
        drop(txn);
    }

    /// Allocate the next transaction id and register it active atomically under
    /// the registry latch (`docs/specs/mvcc.md` §7.1), so a concurrent reader's
    /// snapshot capture never observes the advanced allocator boundary without
    /// also observing this transaction in `xip`.
    pub(super) fn register_active_txn(&self) -> u64 {
        self.components
            .active_txns
            .register_allocated(|| self.components.next_txn_id.fetch_add(1, Ordering::AcqRel))
    }

    /// The snapshot a statement of `txn` reads with, per isolation level
    /// (`docs/specs/mvcc.md` §6, §9), together with the per-statement GC-horizon
    /// advertisement the caller must hold for the statement's execution.
    ///
    /// - **Read Committed** captures a fresh snapshot each statement (seeing other
    ///   transactions' commits between statements). Its advertisement is returned as
    ///   `Some(guard)` so the caller drops it at statement end, releasing the
    ///   previous statement's pinned `xmin`.
    /// - **Repeatable Read** captures one snapshot at the first statement and reuses
    ///   it for the whole transaction. Its advertisement is held on `txn`
    ///   (`rr_advertised`) for the transaction's life and released when the
    ///   `Transaction` drops at commit/abort, so this returns `None` (no
    ///   per-statement guard) — the snapshot stays pinned across statements.
    pub(super) fn snapshot_for_transaction(
        &self,
        txn: &mut Transaction,
    ) -> (Arc<Snapshot>, Option<AdvertisedSnapshot>) {
        match txn.isolation {
            IsolationLevel::ReadCommitted => {
                let (snapshot, advertised) = self.capture_snapshot(txn.txn_id);
                (snapshot, Some(advertised))
            }
            IsolationLevel::RepeatableRead => {
                if let Some(snapshot) = &txn.rr_snapshot {
                    (snapshot.clone(), None)
                } else {
                    let (snapshot, advertised) = self.capture_snapshot(txn.txn_id);
                    txn.rr_snapshot = Some(snapshot.clone());
                    // Hold the advertisement for the transaction's life (released
                    // when `txn` drops at commit/abort), so the reusable snapshot's
                    // xmin stays pinned across every statement that reuses it.
                    txn.rr_advertised = Some(advertised);
                    (snapshot, None)
                }
            }
        }
    }

    pub(super) fn execution_context<'a>(
        &'a self,
        txn_id: u64,
        snapshot: Arc<Snapshot>,
        isolation: IsolationLevel,
        gc_horizon: u64,
        cancel: &'a AtomicBool,
    ) -> ExecutionContext<'a> {
        ExecutionContext {
            statement: StatementContext::with_snapshot_and_isolation(txn_id, snapshot, isolation)
                .with_gc_horizon(gc_horizon),
            catalog: self.components.catalog.as_ref(),
            storage: self.components.storage.as_ref(),
            schema_ops: self.components.storage.as_ref(),
            gc_horizon,
            cancel,
        }
    }

    /// Capture a visibility snapshot consistently with the active-transaction
    /// registry and the id allocator (`docs/specs/mvcc.md` §5.5, §7.1, §9), and
    /// **advertise its `xmin`** to the GC horizon for the snapshot's lifetime.
    /// Captured under the registry's brief latch (via `capture`) so the snapshot is
    /// not torn relative to `next_txn_id` AND its `xmin` is published in the same
    /// critical section that reads the active set (closing the capture-vs-horizon
    /// race; see [`ActiveTxnRegistry::capture`](crate::registry::ActiveTxnRegistry::capture)):
    ///
    /// - `xmax` is the next id to be assigned; every already-allocated id is below
    ///   it (read after the latched active set so no concurrently-begun writer is
    ///   missed from `xip`).
    /// - `xip` is the currently-active set minus `own_txn` (own writes are seen via
    ///   the predicate's own-write path, not as in-progress). A read passes
    ///   `own_txn = 0`; nothing is excluded.
    /// - `xmin` is the oldest active id, or `xmax` if none are active.
    ///
    /// Returns the `Arc<Snapshot>` (shared by the executor across scan operators
    /// rather than deep-cloning `xip` per operator) together with the
    /// [`AdvertisedSnapshot`] guard. **The caller MUST hold the guard for exactly as
    /// long as the snapshot can still be used to read**: dropping it sooner lets
    /// VACUUM reclaim a version this snapshot sees live (data loss); holding it
    /// longer over-pins the horizon (a space cost only).
    pub(super) fn capture_snapshot(&self, own_txn: u64) -> (Arc<Snapshot>, AdvertisedSnapshot) {
        // Capture the active set and the allocator boundary under one latch so a
        // concurrent BEGIN cannot slip a new writer between reading `xmax` and
        // reading `xip`, and publish the snapshot's `xmin` in the same section.
        // Reading `next_txn_id` first, then the active set, would risk a writer that
        // registered after the `xmax` read being both `>= xmax` (so excluded as
        // "future") and absent from `xip` — but visible. Reading the active set
        // first guarantees any active id is reflected in `xip`, and `xmax` taken
        // after only grows, so every active id stays `< xmax`.
        let (active, xmax, advertised) = self
            .components
            .active_txns
            .capture(|| self.components.next_txn_id.load(Ordering::Acquire));
        let xip: Vec<u64> = active.iter().copied().filter(|&id| id != own_txn).collect();
        let xmin = active.first().copied().unwrap_or(xmax);
        debug_assert_eq!(
            advertised.xmin(),
            xmin,
            "advertised xmin must match the snapshot's xmin"
        );
        (Arc::new(Snapshot { xmin, xmax, xip }), advertised)
    }

    pub(super) fn append_and_flush_commit(&self, txn_id: u64) -> Result<()> {
        self.components.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::Commit,
        })?;
        self.components.wal.flush()?;
        Ok(())
    }

    pub(super) fn rollback_pre_durable_or_die(
        &self,
        txn_id: u64,
        catalog_before: Option<catalog::CatalogSnapshot>,
    ) {
        if let Err(rollback_err) = self.rollback_pre_durable(txn_id, catalog_before) {
            self.fatal_pre_durable_rollback_failure(rollback_err);
        }
    }

    pub(super) fn rollback_pre_durable(
        &self,
        txn_id: u64,
        catalog_before: Option<catalog::CatalogSnapshot>,
    ) -> Result<()> {
        // Record the abort: append an `Abort` record (which sets the CLOG to
        // `Aborted`) and drop the transaction from the active set. The abort is not
        // fsynced here — a transaction with no durable `Commit` is recovered as
        // aborted regardless (redo-all + in-flight = aborted, `docs/specs/mvcc.md`
        // §8). The next checkpoint's `persist_clog` durably records the `Aborted`
        // status in `clog.dat`, so the aborted txn's flushed pages stay hidden across a
        // checkpoint even though truncation drops the `Abort` record (§5.4). A failure
        // to append it is logged but not fatal: the txn is still recovered as aborted.
        if let Err(err) = self.components.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::Abort,
        }) {
            eprintln!("failed to append Abort record for txn {txn_id}: {err}");
        }
        self.components.active_txns.deregister(txn_id);

        // Abort is status-based (`docs/specs/mvcc.md` §4 Decision 3, Milestone D1):
        // there is NO before-image page undo. The transaction's modified tuples
        // stay in the heap, hidden by the CLOG (Aborted) and reclaimed by VACUUM.
        // The two cleanups below are not undo: `storage.rollback_txn` restores the
        // engine's own DDL metadata (table/index schema shadow state, for a failed
        // CREATE/DROP inside the unit), and `buffer_pool.rollback` only clears any
        // per-txn bookkeeping. It does NOT undo or reclaim pages: tuples and pages
        // this transaction modified or freshly allocated stay resident as
        // dirty-but-evictable frames (and page numbers are not reused), matching
        // what redo-all recovery replays and the CLOG then hides.
        if let Err(err) = self.components.storage.rollback_txn(txn_id) {
            return Err(DbError::internal(format!(
                "storage rollback failed for txn {txn_id}: {err}",
            )));
        }
        if let Err(err) = self.components.buffer_pool.rollback(txn_id) {
            return Err(DbError::internal(format!(
                "buffer rollback failed for txn {txn_id}: {err}",
            )));
        }
        if let Some(snapshot) = catalog_before {
            self.components.catalog.restore(snapshot).map_err(|err| {
                DbError::internal(format!("catalog restore failed for txn {txn_id}: {err}"))
            })?;
        }
        Ok(())
    }

    pub(super) fn cleanup_after_durable_commit(&self, txn_id: u64) -> Result<()> {
        self.components.storage.commit_txn(txn_id)?;
        self.components.buffer_pool.commit(txn_id)?;
        Ok(())
    }

    pub(super) fn fatal_after_durable_commit(&self, err: DbError) -> ! {
        eprintln!("fatal cleanup failure after durable commit: {err}");
        let _ = self.components.wal.flush();
        std::process::exit(1);
    }

    fn fatal_pre_durable_rollback_failure(&self, err: DbError) -> ! {
        eprintln!("fatal rollback failure before durable commit: {err}");
        let _ = self.components.wal.flush();
        std::process::exit(1);
    }
}
