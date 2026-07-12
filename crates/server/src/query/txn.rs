use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use catalog::CatalogManager;
use common::{
    CatalogIntrospectionProvider, DbError, IsolationLevel, QueryCancel, Result, SequenceManager,
    SessionInfo, SessionSequenceState, Snapshot, SqlState, StatementContext, SystemStateProvider,
    no_catalog_introspection, no_system_state,
};
use executor::{ExecutionContext, ExecutionResult};
use parser::Statement;
use storage::{RelationSnapshot, StorageEngine};
use wal::{WalRecord, WalRecordKind};

use super::{
    QueryService, SavepointLevel, SessionGucs, Transaction, TransactionControl, begin_complete,
    commit_complete, release_complete, rollback_complete, savepoint_complete, set_complete,
};
use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;
use crate::registry::AdvertisedSnapshot;

pub(super) struct StatementRuntime<'a> {
    pub(super) cancel: &'a Arc<QueryCancel>,
    session_sequences: Arc<SessionSequenceState>,
    session_info: Arc<SessionInfo>,
    search_path_names: Vec<String>,
    system_state: Arc<dyn SystemStateProvider>,
    catalog_introspection: Arc<dyn CatalogIntrospectionProvider>,
    catalog_introspection_is_explicit: bool,
    work_mem_kib: u64,
}

pub(crate) struct CapturedSnapshots {
    pub(super) snapshot: Arc<Snapshot>,
    pub(super) relations: Arc<dyn RelationSnapshot>,
    pub(super) advertised: AdvertisedSnapshot,
}

pub(crate) struct TransactionSnapshots {
    pub(super) snapshot: Arc<Snapshot>,
    pub(super) relations: Arc<dyn RelationSnapshot>,
    pub(super) advertised: Option<AdvertisedSnapshot>,
}

pub(super) struct ExecutionContextInput<'a> {
    pub(super) txn_id: u64,
    pub(super) snapshot: Arc<Snapshot>,
    pub(super) relations: Arc<dyn RelationSnapshot>,
    pub(super) isolation: IsolationLevel,
    pub(super) gc_horizon: u64,
    pub(super) live_txns: Arc<[u64]>,
    pub(super) runtime: StatementRuntime<'a>,
}

impl<'a> StatementRuntime<'a> {
    pub(super) fn new(
        cancel: &'a Arc<QueryCancel>,
        session_sequences: Arc<SessionSequenceState>,
        session_info: Arc<SessionInfo>,
        work_mem_kib: u64,
    ) -> Self {
        Self {
            cancel,
            session_sequences,
            session_info,
            search_path_names: vec!["public".to_string()],
            system_state: no_system_state(),
            catalog_introspection: no_catalog_introspection(),
            catalog_introspection_is_explicit: false,
            work_mem_kib,
        }
    }

    pub(super) fn cancel(&self) -> &QueryCancel {
        self.cancel.as_ref()
    }

    pub(super) fn search_path_names(&self) -> &[String] {
        &self.search_path_names
    }

    #[must_use]
    pub(super) fn with_search_path_names(mut self, search_path_names: Vec<String>) -> Self {
        self.search_path_names = search_path_names;
        self
    }

    #[must_use]
    pub(super) fn with_system_state(mut self, system_state: Arc<dyn SystemStateProvider>) -> Self {
        self.system_state = system_state;
        self
    }

    #[must_use]
    pub(super) fn with_catalog_introspection(
        mut self,
        catalog_introspection: Arc<dyn CatalogIntrospectionProvider>,
        is_explicit: bool,
    ) -> Self {
        self.catalog_introspection = catalog_introspection;
        self.catalog_introspection_is_explicit = is_explicit;
        self
    }
}

/// The `25P02` error for a statement issued in a failed (`'E'`) transaction block
/// (matching `run_bound_in_transaction`'s gate). `SAVEPOINT`/`RELEASE` hit this;
/// `ROLLBACK TO` is the exception that recovers the block instead.
fn failed_block_error() -> DbError {
    DbError::execute(
        SqlState::InFailedSqlTransaction,
        "current transaction is aborted, commands ignored until end of transaction block",
    )
}

/// The `3B001` error for `RELEASE`/`ROLLBACK TO` of a name with no matching live
/// savepoint (`docs/specs/savepoints.md` §2).
fn no_such_savepoint(name: &str) -> DbError {
    DbError::plan(
        SqlState::InvalidSavepointSpecification,
        format!("savepoint \"{name}\" does not exist"),
    )
}

impl QueryService {
    /// Handle BEGIN/COMMIT/ROLLBACK/SET TRANSACTION/SET SESSION CHARACTERISTICS
    /// against the session's transaction `slot` and `default_isolation` (the session
    /// default, in/out). `BEGIN` reads the default; `SET SESSION CHARACTERISTICS`
    /// updates it immediately outside a transaction or stores a pending change inside
    /// one; `COMMIT` promotes a pending change after a healthy transaction commits.
    pub(super) fn handle_transaction_control(
        &self,
        kind: TransactionControl,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        cancel: &Arc<QueryCancel>,
        gucs: &SessionGucs,
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
                Some(mut txn) if !txn.failed => {
                    if let Err(err) = cancel.check() {
                        txn.failed = true;
                        return (Some(txn), default_isolation, Err(err));
                    }
                    let committed_default = txn.committed_default_isolation(default_isolation);
                    let committed_statement_timeout =
                        txn.committed_statement_timeout_ms(gucs.statement_timeout_ms());
                    let committed_work_mem = txn.committed_work_mem_kib(gucs.work_mem_kib());
                    let result = self
                        .commit_transaction(txn, cancel.as_ref())
                        .map(|()| commit_complete());
                    let default_isolation = if result.is_ok() {
                        gucs.set("statement_timeout", committed_statement_timeout.to_string());
                        gucs.set("work_mem", committed_work_mem.to_string());
                        committed_default
                    } else {
                        default_isolation
                    };
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

    /// Handle a savepoint command (`SAVEPOINT` / `RELEASE [SAVEPOINT]` / `ROLLBACK
    /// TO [SAVEPOINT]`) against the session's transaction `slot`
    /// (`docs/specs/savepoints.md`). Savepoints require an open transaction block.
    ///
    /// - `SAVEPOINT s` eagerly allocates a subxid, registers it active, and pushes a
    ///   level (the innermost subxid becomes the writing xid).
    /// - `RELEASE s` is a pure in-memory merge: it pops the named level and any above
    ///   it; the popped subxids stay live and registered (settled only at the
    ///   top-level COMMIT) so a released subtransaction never becomes visible to
    ///   other transactions before the top commits.
    /// - `ROLLBACK TO s` aborts the named level's subxid and every deeper one, drops
    ///   them from the live-set, re-establishes `s` with a fresh subxid for continued
    ///   work, and clears the failed (`'E'`) state — the failed-transaction recovery
    ///   point. `SAVEPOINT`/`RELEASE` in a failed block are rejected (`25P02`).
    pub(super) fn handle_savepoint(
        &self,
        statement: Statement,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        let Some(mut txn) = slot else {
            // No open transaction block: PostgreSQL rejects with 25P01.
            let label = match &statement {
                Statement::ReleaseSavepoint { .. } => "RELEASE SAVEPOINT",
                Statement::RollbackToSavepoint { .. } => "ROLLBACK TO SAVEPOINT",
                _ => "SAVEPOINT",
            };
            let err = DbError::plan(
                SqlState::NoActiveSqlTransaction,
                format!("{label} can only be used in transaction blocks"),
            );
            return (None, default_isolation, Err(err));
        };

        match statement {
            Statement::Savepoint { name } => {
                if txn.failed {
                    return (Some(txn), default_isolation, Err(failed_block_error()));
                }
                let catalog_overlay = match txn.catalog_overlay.savepoint() {
                    Ok(savepoint) => savepoint,
                    Err(err) => return (Some(txn), default_isolation, Err(err)),
                };
                let storage = match self.components.storage.savepoint(txn.txn_id) {
                    Ok(savepoint) => savepoint,
                    Err(err) => return (Some(txn), default_isolation, Err(err)),
                };
                let subxid = self.register_active_subxid(txn.txn_id);
                txn.live_subxids.push(subxid);
                txn.savepoints.push(SavepointLevel {
                    name,
                    subxid,
                    default_isolation_override: txn.default_isolation_override,
                    statement_timeout_override: txn.statement_timeout_override,
                    truncate_updates: txn.truncate_updates.clone(),
                    relation_generation_changed: txn.relation_generation_changed,
                    catalog_overlay,
                    storage,
                    work_mem_override: txn.work_mem_override,
                });
                (Some(txn), default_isolation, Ok(savepoint_complete()))
            }
            Statement::ReleaseSavepoint { name } => {
                if txn.failed {
                    return (Some(txn), default_isolation, Err(failed_block_error()));
                }
                match txn.savepoints.iter().rposition(|level| level.name == name) {
                    // In-memory merge only: pop the named level and any above it. The
                    // popped subxids stay in `live_subxids` (still live) and
                    // registered until the top commits.
                    Some(idx) => {
                        txn.savepoints.truncate(idx);
                        (Some(txn), default_isolation, Ok(release_complete()))
                    }
                    // Unknown savepoint: an error that aborts the block to 'E'
                    // (PostgreSQL raises an ERROR like any statement error).
                    None => {
                        txn.failed = true;
                        (Some(txn), default_isolation, Err(no_such_savepoint(&name)))
                    }
                }
            }
            Statement::RollbackToSavepoint { name } => {
                match txn.savepoints.iter().rposition(|level| level.name == name) {
                    Some(idx) => {
                        // Roll back ALL work done since the named level was
                        // established. Select by subxid VALUE, not stack position:
                        // subxids are allocated monotonically (`register_active_txn`),
                        // so every live subxid `>=` the named level's is work since
                        // that savepoint — INCLUDING a subxid that was `RELEASE`d into
                        // a nested level (popped from the stack but still live). A
                        // by-position slice would miss it and wrongly commit its rows.
                        let level_subxid = txn.savepoints[idx].subxid;
                        let default_isolation_override =
                            txn.savepoints[idx].default_isolation_override;
                        let statement_timeout_override =
                            txn.savepoints[idx].statement_timeout_override;
                        let truncate_updates = txn.savepoints[idx].truncate_updates.clone();
                        let relation_generation_changed =
                            txn.savepoints[idx].relation_generation_changed;
                        let catalog_overlay = txn.savepoints[idx].catalog_overlay.clone();
                        let storage = txn.savepoints[idx].storage.clone();
                        let work_mem_override = txn.savepoints[idx].work_mem_override;
                        let rolled: Vec<u64> = txn
                            .live_subxids
                            .iter()
                            .copied()
                            .filter(|&s| s >= level_subxid)
                            .collect();
                        if let Err(err) = txn.catalog_overlay.rollback_to(&catalog_overlay) {
                            return (Some(txn), default_isolation, Err(err));
                        }
                        let relation_publication = if txn.relation_generation_changed {
                            match self.components.relation_publish_gate.write() {
                                Ok(guard) => Some(guard),
                                Err(_) => {
                                    return (
                                        Some(txn),
                                        default_isolation,
                                        Err(DbError::internal(
                                            "relation publish gate poisoned during savepoint rollback",
                                        )),
                                    );
                                }
                            }
                        } else {
                            None
                        };
                        if let Err(err) = self
                            .components
                            .storage
                            .rollback_to_savepoint(txn.txn_id, &storage)
                        {
                            return (Some(txn), default_isolation, Err(err));
                        }
                        for subxid in &rolled {
                            if let Err(err) = self.components.storage.rollback_txn(*subxid) {
                                return (Some(txn), default_isolation, Err(err));
                            }
                        }
                        drop(relation_publication);
                        self.abort_subxids(&rolled);
                        txn.live_subxids.retain(|&s| s < level_subxid);
                        txn.savepoints.truncate(idx);
                        txn.default_isolation_override = default_isolation_override;
                        txn.statement_timeout_override = statement_timeout_override;
                        txn.truncate_updates = truncate_updates.clone();
                        txn.relation_generation_changed = relation_generation_changed;
                        txn.work_mem_override = work_mem_override;
                        // Re-establish the named level with a fresh subxid so work can
                        // continue under it (PostgreSQL keeps the savepoint active).
                        let fresh = self.register_active_subxid(txn.txn_id);
                        txn.live_subxids.push(fresh);
                        txn.savepoints.push(SavepointLevel {
                            name,
                            subxid: fresh,
                            default_isolation_override,
                            statement_timeout_override,
                            truncate_updates,
                            relation_generation_changed,
                            catalog_overlay,
                            storage,
                            work_mem_override,
                        });
                        // ROLLBACK TO recovers a failed ('E') block to this savepoint.
                        txn.failed = false;
                        (Some(txn), default_isolation, Ok(rollback_complete()))
                    }
                    // Unknown savepoint: an error that aborts the block to 'E'
                    // (PostgreSQL raises an ERROR like any statement error).
                    None => {
                        txn.failed = true;
                        (Some(txn), default_isolation, Err(no_such_savepoint(&name)))
                    }
                }
            }
            other => (
                Some(txn),
                default_isolation,
                Err(DbError::internal(format!(
                    "handle_savepoint received a non-savepoint statement: {other:?}"
                ))),
            ),
        }
    }

    /// Handle `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`
    /// (`docs/specs/mvcc.md` §10 Milestone G2). Postgres semantics: it sets the
    /// per-connection DEFAULT isolation for FUTURE transactions and does NOT change
    /// an already-open transaction's level; inside a transaction block, the new
    /// default is visible immediately but persists only if the block commits.
    ///
    /// - With an isolation-level mode and no open transaction, update
    ///   `default_isolation` to the mapped level.
    /// - With an isolation-level mode inside a healthy transaction, store a pending
    ///   session-default change and leave the open transaction's isolation untouched.
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
            return (
                slot,
                default_isolation,
                Err(DbError::execute(
                    SqlState::InFailedSqlTransaction,
                    "current transaction is aborted, commands ignored until end of transaction block",
                )),
            );
        }

        let Some(level) = isolation else {
            return (slot, default_isolation, Ok(set_complete()));
        };

        match slot {
            Some(mut txn) => {
                txn.set_default_isolation(level);
                (Some(txn), default_isolation, Ok(set_complete()))
            }
            None => (None, level, Ok(set_complete())),
        }
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
    pub(super) fn handle_set_transaction(
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
            default_isolation_override: None,
            statement_timeout_override: None,
            work_mem_override: None,
            first_statement_ran: false,
            failed: false,
            physically_aborted: false,
            object_locks: None,
            write_guard: None,
            has_writes: false,
            rr_snapshot: None,
            rr_advertised: None,
            dead_versions_pending: 0,
            changed_rows_pending: 0,
            savepoints: Vec::new(),
            live_subxids: Vec::new(),
            truncate_updates: std::collections::BTreeMap::new(),
            relation_generation_changed: false,
            catalog_overlay: Arc::new(catalog::CatalogOverlay::new(
                self.components.catalog.clone(),
            )),
        })
    }

    /// Acquire the SHARED checkpoint-participant guard before an explicit
    /// transaction's first retained object lock, holding it through top-level
    /// completion. The guard is shared
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
    pub(super) fn acquire_write_guard(
        &self,
        txn: &mut Transaction,
        cancel: &QueryCancel,
    ) -> Result<()> {
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
        let guard = self
            .components
            .concurrency
            .begin_writer_cancelable(cancel)?;
        txn.write_guard = Some(guard);
        Ok(())
    }

    /// Establish the universal explicit-transaction lock order: retain the shared
    /// checkpoint-participant guard before creating the transaction's single
    /// top-level object-lock owner. The same owner is reused by every statement and
    /// released only with the top-level transaction.
    pub(super) fn ensure_transaction_lock_owner<'a>(
        &self,
        txn: &'a mut Transaction,
        cancel: &QueryCancel,
    ) -> Result<&'a mut crate::lock_manager::ObjectLockGuard> {
        let acquired_participant = txn.write_guard.is_none();
        if acquired_participant {
            self.acquire_write_guard(txn, cancel)?;
        }
        if txn.object_locks.is_none() {
            match self.components.lock_manager.transaction_owner(txn.txn_id) {
                Ok(owner) => txn.object_locks = Some(owner),
                Err(err) => {
                    if acquired_participant {
                        txn.write_guard = None;
                    }
                    return Err(err);
                }
            }
        }
        Ok(txn
            .object_locks
            .as_mut()
            .expect("transaction lock owner installed above"))
    }

    /// Commit an explicit transaction: append a `Commit` record, flush (fsync),
    /// set `CLOG=Committed` (done at flush), run post-durable-commit cleanup, and
    /// deregister. Releasing the write guard happens when `txn` is dropped after
    /// this returns.
    /// Settle a serializable transaction's SSI state at the end of its life. On commit,
    /// mark it finished (its SIREAD locks + rw-edges are retained so concurrent
    /// transactions can still form edges) and release any SIREAD locks the now-advanced
    /// GC horizon permits; on abort, drop its SSI state immediately (its reads/writes
    /// are void). A no-op for non-serializable transactions (`docs/specs/ssi.md` §8).
    pub(super) fn ssi_finish(&self, txn_id: u64, isolation: IsolationLevel, committed: bool) {
        if isolation != IsolationLevel::Serializable {
            return;
        }
        if committed {
            self.components.ssi_manager.finished(txn_id);
            self.components
                .ssi_manager
                .release_up_to(self.components.gc_horizon());
        } else {
            self.components.ssi_manager.aborted(txn_id);
        }
    }

    fn commit_transaction(&self, txn: Transaction, cancel: &QueryCancel) -> Result<()> {
        let txn_id = txn.txn_id;
        let isolation = txn.isolation;
        let dead_versions = txn.dead_versions_pending;
        let changed_rows = txn.changed_rows_pending;
        // The whole family `{top} ∪ subxids` settles together. Compute it before any
        // settle so the atomic family-deregister (`docs/specs/savepoints.md` §3) can
        // run after the CLOG is marked committed.
        let family: Vec<u64> = std::iter::once(txn_id)
            .chain(txn.live_subxids.iter().copied())
            .collect();
        // A read-only explicit transaction (no write guard, no writes by the top or
        // any subxid) has nothing durable to commit: just deregister the family and
        // return. Appending a `Commit` for it is unnecessary; skip the WAL.
        if !txn.has_writes {
            self.components.active_txns.deregister_all(&family);
            self.components.lock_manager.on_txn_finished();
            // A read-only serializable transaction never writes, so it can never be a
            // pivot or a dooming `T_out` (no commit check needed); but its SIREAD locks
            // must persist for concurrent writers, so finish (not abort) it.
            self.ssi_finish(txn_id, isolation, true);
            return Ok(());
        }

        // SSI commit-time check, BEFORE the durable Commit flush, so a serializable
        // transaction that completes a dangerous structure is rolled back and never
        // becomes durable (`docs/specs/ssi.md` §7). The committing transaction is the
        // participant aborted, so this is synchronous.
        if isolation == IsolationLevel::Serializable
            && let Err(err) = self.components.ssi_manager.commit_check(txn_id)
        {
            // Abort the whole family (mirroring the flush-failure path) and drop its
            // SSI state, then surface 40001.
            self.abort_subxids(&txn.live_subxids);
            self.rollback_transaction_pre_durable_or_die(txn_id, txn.relation_generation_changed);
            self.ssi_finish(txn_id, isolation, false);
            return Err(err);
        }

        // The outer transaction-control gate catches already-expired statements;
        // check again after SSI work at the last safe boundary before the durable
        // commit record. Once that record is flushed, cancellation must not turn a
        // committed transaction into an error.
        if let Err(err) = cancel.check() {
            self.abort_subxids(&txn.live_subxids);
            self.rollback_transaction_pre_durable_or_die(txn_id, txn.relation_generation_changed);
            self.ssi_finish(txn_id, isolation, false);
            return Err(err);
        }

        let truncate_updates = txn.truncate_updates.values().cloned().collect::<Vec<_>>();
        let has_catalog_changes = !txn.catalog_overlay.is_empty()?;
        let publication = (|| {
            if truncate_updates.is_empty() && !has_catalog_changes {
                return Ok((None, None));
            }
            let catalog = self
                .components
                .catalog_publication_gate
                .write()
                .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
            let relations = self
                .components
                .relation_publish_gate
                .write()
                .map_err(|_| DbError::internal("relation publish gate poisoned"))?;
            Ok((Some(catalog), Some(relations)))
        })();
        let (catalog_publication, relation_publication) = match publication {
            Ok(guards) => guards,
            Err(err) => {
                self.abort_subxids(&txn.live_subxids);
                self.rollback_transaction_pre_durable_or_die(
                    txn_id,
                    txn.relation_generation_changed,
                );
                self.ssi_finish(txn_id, isolation, false);
                return Err(err);
            }
        };
        let combined_catalog = if has_catalog_changes || !truncate_updates.is_empty() {
            let materialize = || {
                let base: Arc<dyn catalog::CatalogManager> =
                    Arc::new(txn.catalog_overlay.catalog()?);
                if truncate_updates.is_empty() {
                    base.snapshot()
                } else {
                    catalog::TruncateCatalogOverlay::new(base, truncate_updates.iter().cloned())
                        .snapshot()
                }
            };
            match materialize() {
                Ok(snapshot) => Some(snapshot),
                Err(err) => {
                    self.abort_subxids(&txn.live_subxids);
                    self.rollback_transaction_pre_durable_or_die(
                        txn_id,
                        txn.relation_generation_changed,
                    );
                    self.ssi_finish(txn_id, isolation, false);
                    return Err(err);
                }
            }
        } else {
            None
        };

        if let Err(err) = self.append_and_flush_commit(txn_id, &txn.live_subxids) {
            // The commit is not durable: abort the whole family instead (append
            // `Abort` records + CLOG=Aborted, clear per-txn bookkeeping, restore DDL
            // metadata) so its effects are hidden by the CLOG. Abort is status-based
            // — no page-content undo (`docs/specs/mvcc.md` §4 Decision 3).
            self.abort_subxids(&txn.live_subxids);
            self.rollback_pre_durable_or_die(txn_id, None);
            drop(relation_publication);
            drop(catalog_publication);
            if txn.relation_generation_changed {
                super::truncate::best_effort_retired_generation_cleanup(&self.components);
            }
            // The transaction passed its SSI commit check but did not commit durably:
            // drop its (now void) SSI state.
            self.ssi_finish(txn_id, isolation, false);
            // `txn` (and its write guard) drops here, releasing the guard.
            return Err(err);
        }

        if let Some(snapshot) = combined_catalog
            && let Err(err) = self.components.catalog.restore(snapshot)
        {
            self.fatal_after_durable_commit(err);
        }

        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        // The CLOG marked the top + every committed subxid `Committed` at flush;
        // remove the whole family from the active set in ONE latched batch so a
        // concurrent snapshot capture sees the family all-present (all invisible) or
        // all-absent (all settled), never a torn commit (`docs/specs/savepoints.md` §3).
        self.components.active_txns.deregister_all(&family);
        // Wake any writer blocked on this transaction's row locks.
        self.components.lock_manager.on_txn_finished();
        // Finish the serializable transaction's SSI state (retain its SIREAD locks for
        // concurrent transactions; release whatever the advanced GC horizon permits).
        self.ssi_finish(txn_id, isolation, true);
        drop(relation_publication);
        drop(catalog_publication);
        let relation_generation_changed = txn.relation_generation_changed;
        // `txn` drops here, releasing the exclusive write guard.
        drop(txn);

        if relation_generation_changed {
            super::truncate::best_effort_retired_generation_cleanup(&self.components);
        }

        // Fold the committed transaction's dead versions into the auto-prune counter
        // BEFORE the checkpoint trigger (`docs/specs/mvcc.md` §9, F4b): only a durable
        // commit reaches here, so an aborted transaction never advances the counter.
        self.components.add_dead_versions(dead_versions);
        self.components.add_changed_rows(changed_rows);

        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);
        Ok(())
    }

    /// Abort an explicit transaction: append an `Abort` record, set `CLOG=Aborted`,
    /// clear per-txn bookkeeping, restore DDL metadata, and deregister. Abort is
    /// status-based (`docs/specs/mvcc.md` §4 Decision 3,
    /// Milestone D1): the transaction's modified tuples stay in the heap, hidden by
    /// the CLOG and reclaimed by VACUUM — there is NO before-image page undo.
    /// Dropping `txn` releases the write guard. A failed *durable* `Abort` append is
    /// best-effort and logged, not fatal — the in-memory `Aborted` status is still
    /// recorded (`WalManager::append`) and recovery reconstructs the abort. Only a
    /// failure of the engine-state rollback (storage/buffer/catalog) is fatal, since
    /// the engine can then no longer guarantee consistency.
    pub(super) fn abort_transaction(&self, txn: Transaction) {
        if txn.physically_aborted {
            return;
        }
        let txn_id = txn.txn_id;
        let isolation = txn.isolation;
        if !txn.has_writes {
            // A read-only transaction (top + subxids) wrote nothing: no Abort record,
            // no cleanup, just deregister the whole family.
            let family: Vec<u64> = std::iter::once(txn_id)
                .chain(txn.live_subxids.iter().copied())
                .collect();
            self.components.active_txns.deregister_all(&family);
            self.components.lock_manager.on_txn_finished();
            // Drop the serializable transaction's SSI state (its reads are void).
            self.ssi_finish(txn_id, isolation, false);
            return;
        }
        // Abort every not-rolled-back subxid (so its rows are CLOG-hidden and
        // VACUUM-reclaimable), then the top-level transaction.
        self.abort_subxids(&txn.live_subxids);
        self.rollback_transaction_pre_durable_or_die(txn_id, txn.relation_generation_changed);
        // Drop the serializable transaction's SSI state (its reads/writes are void).
        self.ssi_finish(txn_id, isolation, false);
        // `txn` drops here, releasing the exclusive write guard.
        drop(txn);
    }

    pub(super) fn abort_deadlock_victim(&self, txn: &mut Transaction) {
        if txn.physically_aborted {
            txn.failed = true;
            return;
        }
        let family: Vec<u64> = std::iter::once(txn.txn_id)
            .chain(txn.live_subxids.iter().copied())
            .collect();
        if txn.has_writes {
            self.abort_subxids(&txn.live_subxids);
            self.rollback_transaction_pre_durable_or_die(
                txn.txn_id,
                txn.relation_generation_changed,
            );
        } else {
            self.components.active_txns.deregister_all(&family);
            self.components.lock_manager.on_txn_finished();
        }
        self.ssi_finish(txn.txn_id, txn.isolation, false);
        txn.object_locks = None;
        txn.write_guard = None;
        txn.rr_snapshot = None;
        txn.rr_advertised = None;
        txn.savepoints.clear();
        txn.live_subxids.clear();
        txn.has_writes = false;
        txn.failed = true;
        txn.physically_aborted = true;
    }

    /// Abort `subxids` (savepoint subtransactions): append an `Abort` record per
    /// subxid — which sets the in-memory CLOG to `Aborted` so its rows are hidden
    /// and VACUUM-reclaimable — and deregister them from the active set. Not
    /// fsynced: abort durability is not critical (recovery aborts any subxid with
    /// no durable `Commit`/`CommitWithSubxids` anyway). Used by `ROLLBACK TO
    /// SAVEPOINT` and the top-level abort paths (`docs/specs/savepoints.md` §3, §5).
    pub(super) fn abort_subxids(&self, subxids: &[u64]) {
        if subxids.is_empty() {
            return;
        }
        for &subxid in subxids {
            if let Err(err) = self.components.wal.append(WalRecord {
                lsn: 0,
                txn_id: subxid,
                kind: WalRecordKind::Abort,
            }) {
                // Best-effort durable record (recovery aborts any subxid with no
                // durable commit); the in-memory `Aborted` status is recorded by the
                // append itself even on failure, so log and continue rather than
                // taking down the whole server on a transient WAL write error.
                eprintln!("failed to append Abort record for subxid {subxid}: {err}");
            }
        }
        self.components.active_txns.deregister_all(subxids);
        // A partial ROLLBACK TO frees any writer blocked on a rolled-back subxid.
        self.components.lock_manager.on_txn_finished();
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

    /// Allocate and register a savepoint **subxid** owned by top-level `top`,
    /// recording the subxid→top mapping so the deadlock detector can canonicalize
    /// wait-for edges to transaction granularity (`docs/specs/deadlock.md` §4).
    pub(super) fn register_active_subxid(&self, top: u64) -> u64 {
        self.components
            .active_txns
            .register_subxid_allocated(top, || {
                self.components.next_txn_id.fetch_add(1, Ordering::AcqRel)
            })
    }

    /// The MVCC snapshot selected by `txn`'s isolation level and a fresh statement
    /// relation-generation snapshot (`docs/specs/mvcc.md` §6, §9), together with the
    /// per-statement GC-horizon advertisement the caller must hold for the
    /// statement's execution.
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
    pub(super) fn snapshots_for_transaction(
        &self,
        txn: &mut Transaction,
        cancel: &QueryCancel,
    ) -> Result<TransactionSnapshots> {
        match txn.isolation {
            IsolationLevel::ReadCommitted => {
                let CapturedSnapshots {
                    snapshot,
                    relations,
                    advertised,
                } = self.capture_consistent_snapshots_with_exclusion_bypass(
                    txn.txn_id,
                    false,
                    Some(cancel),
                )?;
                Ok(TransactionSnapshots {
                    snapshot,
                    relations,
                    advertised: Some(advertised),
                })
            }
            // Serializable shares Repeatable Read's single per-transaction snapshot;
            // SSI layers rw-conflict tracking on top of it (`docs/specs/ssi.md`).
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                if let Some(snapshot) = &txn.rr_snapshot {
                    Ok(TransactionSnapshots {
                        snapshot: snapshot.clone(),
                        relations: self.capture_statement_relation_snapshot()?,
                        advertised: None,
                    })
                } else {
                    let CapturedSnapshots {
                        snapshot,
                        relations,
                        advertised,
                    } = self.capture_consistent_snapshots_allowing_missing_table_reads(
                        txn.txn_id,
                        Some(cancel),
                    )?;
                    txn.rr_snapshot = Some(snapshot.clone());
                    // Hold the advertisement for the transaction's life (released
                    // when `txn` drops at commit/abort), so the reusable snapshot's
                    // xmin stays pinned across every statement that reuses it.
                    txn.rr_advertised = Some(advertised);
                    Ok(TransactionSnapshots {
                        snapshot,
                        relations,
                        advertised: None,
                    })
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn capture_consistent_snapshots(&self, own_txn: u64) -> Result<CapturedSnapshots> {
        self.capture_consistent_snapshots_with_options(own_txn, false, false, None)
    }

    pub(super) fn capture_consistent_snapshots_cancelable(
        &self,
        own_txn: u64,
        cancel: &QueryCancel,
    ) -> Result<CapturedSnapshots> {
        self.capture_consistent_snapshots_with_options(own_txn, false, false, Some(cancel))
    }

    fn capture_statement_relation_snapshot(&self) -> Result<Arc<dyn RelationSnapshot>> {
        let _publish_read = self
            .components
            .relation_publish_gate
            .read()
            .map_err(|_| DbError::internal("relation publish gate poisoned"))?;
        self.components.storage.capture_relation_snapshot()
    }

    fn capture_consistent_snapshots_allowing_missing_table_reads(
        &self,
        own_txn: u64,
        cancel: Option<&QueryCancel>,
    ) -> Result<CapturedSnapshots> {
        self.capture_consistent_snapshots_with_options(own_txn, false, true, cancel)
    }

    fn capture_consistent_snapshots_with_exclusion_bypass(
        &self,
        own_txn: u64,
        bypass_snapshot_exclusion: bool,
        cancel: Option<&QueryCancel>,
    ) -> Result<CapturedSnapshots> {
        self.capture_consistent_snapshots_with_options(
            own_txn,
            bypass_snapshot_exclusion,
            false,
            cancel,
        )
    }

    fn capture_consistent_snapshots_with_options(
        &self,
        own_txn: u64,
        bypass_snapshot_exclusion: bool,
        missing_table_reads_are_empty: bool,
        cancel: Option<&QueryCancel>,
    ) -> Result<CapturedSnapshots> {
        loop {
            if let Some(cancel) = cancel {
                cancel.check()?;
            }
            let relation_publish_read = if let Some(cancel) = cancel {
                loop {
                    cancel.check()?;
                    match self.components.relation_publish_gate.try_read() {
                        Ok(guard) => break guard,
                        Err(std::sync::TryLockError::WouldBlock) => {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            return Err(DbError::internal("relation publish gate poisoned"));
                        }
                    }
                }
            } else {
                self.components
                    .relation_publish_gate
                    .read()
                    .map_err(|_| DbError::internal("relation publish gate poisoned"))?
            };
            let relation_epoch = self.components.storage.relation_epoch()?;
            let Some((snapshot, advertised)) =
                self.try_capture_snapshot_for_transaction(own_txn, bypass_snapshot_exclusion)
            else {
                drop(relation_publish_read);
                if let Some(cancel) = cancel {
                    self.components
                        .active_txns
                        .wait_for_snapshot_exclusion_clear_cancelable(cancel)?;
                } else {
                    self.components
                        .active_txns
                        .wait_for_snapshot_exclusion_clear();
                }
                continue;
            };
            let relations = match if missing_table_reads_are_empty {
                self.components
                    .storage
                    .capture_relation_snapshot_with_missing_table_reads_empty()
            } else {
                self.components.storage.capture_relation_snapshot()
            } {
                Ok(relations) => relations,
                Err(err) => {
                    drop(advertised);
                    return Err(err);
                }
            };
            if relation_epoch == relations.relation_epoch() {
                return Ok(CapturedSnapshots {
                    snapshot,
                    relations,
                    advertised,
                });
            }
            drop(advertised);
            drop(relation_publish_read);
        }
    }

    pub(super) fn execution_context_under_catalog_gate<'a>(
        &'a self,
        mut input: ExecutionContextInput<'a>,
    ) -> Result<ExecutionContext<'a>> {
        if !input.runtime.catalog_introspection_is_explicit {
            let introspection = self.catalog_introspection_provider_under_gate(
                input.runtime.session_info.clone(),
                input.runtime.search_path_names.clone(),
            )?;
            input.runtime = input
                .runtime
                .with_catalog_introspection(introspection, false);
        }
        self.execution_context_with_catalog(input, self.components.catalog.clone())
    }

    pub(super) fn execution_context_for_bound<'a>(
        &'a self,
        mut input: ExecutionContextInput<'a>,
        bound: &planner::BoundStatement,
    ) -> Result<ExecutionContext<'a>> {
        let (catalog, is_snapshot) = self.statement_catalog(bound)?;
        if is_snapshot && !input.runtime.catalog_introspection_is_explicit {
            let introspection = Arc::new(super::QueryCatalogIntrospection {
                source: super::QueryCatalogSource::Fixed(catalog.clone()),
                session_info: input.runtime.session_info.clone(),
                search_path_names: input.runtime.search_path_names.clone(),
            });
            input.runtime = input
                .runtime
                .with_catalog_introspection(introspection, false);
        }
        self.execution_context_with_catalog(input, catalog)
    }

    pub(super) fn execution_context_with_fixed_catalog<'a>(
        &'a self,
        mut input: ExecutionContextInput<'a>,
        catalog: Arc<dyn catalog::CatalogManager>,
    ) -> Result<ExecutionContext<'a>> {
        if !input.runtime.catalog_introspection_is_explicit
            && !Arc::ptr_eq(&catalog, &self.components.catalog)
        {
            let introspection = Arc::new(super::QueryCatalogIntrospection {
                source: super::QueryCatalogSource::Fixed(catalog.clone()),
                session_info: input.runtime.session_info.clone(),
                search_path_names: input.runtime.search_path_names.clone(),
            });
            input.runtime = input
                .runtime
                .with_catalog_introspection(introspection, false);
        }
        self.execution_context_with_catalog(input, catalog)
    }

    pub(super) fn execution_context_with_selected_catalog<'a>(
        &'a self,
        input: ExecutionContextInput<'a>,
        catalog: Arc<dyn catalog::CatalogManager>,
        is_snapshot: bool,
    ) -> Result<ExecutionContext<'a>> {
        if is_snapshot {
            self.execution_context_with_fixed_catalog(input, catalog)
        } else {
            self.execution_context_with_catalog(input, catalog)
        }
    }

    fn execution_context_with_catalog<'a>(
        &'a self,
        input: ExecutionContextInput<'a>,
        catalog: Arc<dyn catalog::CatalogManager>,
    ) -> Result<ExecutionContext<'a>> {
        let ExecutionContextInput {
            txn_id,
            snapshot,
            relations,
            isolation,
            gc_horizon,
            live_txns,
            runtime,
        } = input;

        // A SERIALIZABLE statement registers its transaction with the SSI manager
        // (idempotent; canonicalized to the top-level id) and installs the real SSI
        // tracker so its reads record SIREAD locks (`docs/specs/ssi.md`). Registering
        // here, in the same place the tracker is installed, guarantees the transaction
        // is registered before any read can record a lock; Read Committed / Repeatable
        // Read keep the no-op tracker and pay nothing (not even the snapshot clone).
        let serializable = isolation == IsolationLevel::Serializable;
        if serializable {
            self.components
                .ssi_manager
                .register(txn_id, snapshot.clone());
        }
        let sequence_manager: Arc<dyn SequenceManager> = self.components.storage.clone();
        let mut statement =
            StatementContext::with_snapshot_and_isolation(txn_id, snapshot, isolation)
                .with_gc_horizon(gc_horizon)
                .with_live_txns(live_txns)
                .with_sequence_manager(sequence_manager)
                .with_session_sequences(runtime.session_sequences)
                .with_session_info(runtime.session_info)
                .with_system_state(runtime.system_state)
                .with_catalog_introspection(runtime.catalog_introspection)
                // Install the lock manager (so an in-progress row-lock conflict blocks
                // instead of failing fast) and the connection's cancel flag (so a
                // blocked writer is interruptible) — `docs/specs/deadlock.md`.
                .with_conflict_waiter(self.components.lock_manager.clone(), runtime.cancel.clone());
        if serializable {
            statement = statement.with_ssi_tracker(self.components.ssi_manager.clone());
        }
        Ok(ExecutionContext {
            statement,
            relations,
            catalog,
            storage: self.components.storage.as_ref(),
            schema_ops: self.components.storage.as_ref(),
            gc_horizon,
            cancel: runtime.cancel.as_ref(),
            spill: spill::SpillConfig::new(
                runtime.work_mem_kib.saturating_mul(1024),
                self.components.config.data_dir.join("tmp"),
            ),
        })
    }

    fn try_capture_snapshot_for_transaction(
        &self,
        own_txn: u64,
        bypass_snapshot_exclusion: bool,
    ) -> Option<(Arc<Snapshot>, AdvertisedSnapshot)> {
        let (active, xmax, advertised) = self
            .components
            .active_txns
            .try_capture_with_exclusion_bypass(bypass_snapshot_exclusion, || {
                self.components.next_txn_id.load(Ordering::Acquire)
            })?;
        let xip = active.iter().copied().filter(|&id| id != own_txn).collect();
        let xmin = active.first().copied().unwrap_or(xmax);
        debug_assert_eq!(advertised.xmin(), xmin);
        Some((Arc::new(Snapshot { xmin, xmax, xip }), advertised))
    }

    /// Capture a visibility snapshot consistently with the active-transaction
    /// registry and the id allocator (`docs/specs/mvcc.md` §5.5, §7.1, §9), and
    /// **advertise its `xmin`** to the GC horizon for the snapshot's lifetime.
    /// Captured under the registry's brief latch so the snapshot
    /// is not torn relative to `next_txn_id` AND its `xmin` is published in the
    /// same critical section that reads the active set (closing the
    /// capture-vs-horizon race).
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
    /// [`AdvertisedSnapshot`] guard. **The caller MUST hold the
    /// guard for exactly as long as the snapshot can still be used to read**:
    /// dropping it sooner lets VACUUM reclaim a version this snapshot sees live
    /// (data loss); holding it longer over-pins the horizon (a space cost only).
    #[allow(dead_code)]
    fn capture_snapshot_for_transaction(
        &self,
        own_txn: u64,
    ) -> (Arc<Snapshot>, AdvertisedSnapshot) {
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

    pub(super) fn append_and_flush_commit(
        &self,
        txn_id: u64,
        committed_subxids: &[u64],
    ) -> Result<()> {
        // A transaction with committed (live or released, not-rolled-back) savepoint
        // subxids records them in one atomic `CommitWithSubxids`; otherwise the plain
        // `Commit` (unchanged format). See `docs/specs/savepoints.md` §5.
        let kind = if committed_subxids.is_empty() {
            WalRecordKind::Commit
        } else {
            WalRecordKind::CommitWithSubxids {
                subxids: committed_subxids.to_vec(),
            }
        };
        self.components.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind,
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

    fn rollback_transaction_pre_durable_or_die(
        &self,
        txn_id: u64,
        has_transactional_truncate: bool,
    ) {
        if !has_transactional_truncate {
            self.rollback_pre_durable_or_die(txn_id, None);
            return;
        }
        let relation_publication = match self.components.relation_publish_gate.write() {
            Ok(guard) => guard,
            Err(_) => self.fatal_pre_durable_rollback_failure(DbError::internal(
                "relation publish gate poisoned during transactional TRUNCATE rollback",
            )),
        };
        self.rollback_pre_durable_or_die(txn_id, None);
        drop(relation_publication);
        super::truncate::best_effort_retired_generation_cleanup(&self.components);
    }

    pub(super) fn rollback_pre_durable(
        &self,
        txn_id: u64,
        catalog_before: Option<catalog::CatalogSnapshot>,
    ) -> Result<()> {
        // Record the abort: append an `Abort` record and drop the transaction from
        // the active set. The abort is not fsynced here — a transaction with no
        // durable `Commit` is recovered as aborted regardless: recovery's
        // `resolve_in_flight_as_aborted` marks every replayed-but-unresolved writer
        // `Aborted` (redo-all + in-flight = aborted, `docs/specs/mvcc.md` §8). A
        // failure to append the durable record is logged but not fatal — the WAL still
        // records the `Aborted` status in the in-memory CLOG before returning the
        // error (`FileWalManager::append`), so the deregistered writer stays hidden
        // and never floats past the implicit-committed floor. Crashing the whole
        // server on a transient WAL write error would drop every other connection.
        if let Err(err) = self.components.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::Abort,
        }) {
            eprintln!("failed to append Abort record for txn {txn_id}: {err}");
        }
        self.components.active_txns.deregister(txn_id);
        // Wake any writer blocked on this aborted transaction's row locks.
        self.components.lock_manager.on_txn_finished();

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

    pub(super) fn fatal_pre_durable_rollback_failure(&self, err: DbError) -> ! {
        eprintln!("fatal rollback failure before durable commit: {err}");
        let _ = self.components.wal.flush();
        std::process::exit(1);
    }
}
