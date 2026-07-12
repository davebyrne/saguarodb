use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, RwLockWriteGuard};

use common::{CopyDirection, DbError, IsolationLevel, QueryCancel, Result, SqlState};
use executor::{CopyJob, ExecutionResult, RowSink};
use parser::Statement;
use planner::{BoundStatement, format_explain, logical_plan, physical_plan};

use super::{
    AutocommitCopyWrite, BindSource, CapturedSnapshots, CopySnapshots, ExecutionContextInput,
    QueryService, QuerySessionContext, StatementClass, StatementRuntime, StreamOutcome,
    Transaction, TransactionSnapshots, WriteUnitGuard, changed_rows_in, classify_bound,
    dead_versions_in, exec_or_stream, mark_failed_on_error, object_lock_requests,
    prepared_schema_versions, run_plan, statement_class,
    validate_prepared_schema_versions_in_catalog,
};
use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;

impl QueryService {
    pub(super) fn catalog_write_after_lock_convergence<'a>(
        &'a self,
        bound: &BoundStatement,
        object_guard: &mut crate::lock_manager::ObjectLockGuard,
        object_baseline: &crate::lock_manager::OwnerGrantSnapshot,
        cancel: &QueryCancel,
    ) -> Result<RwLockWriteGuard<'a, ()>> {
        loop {
            let catalog_guard = self
                .components
                .catalog_publication_gate
                .write()
                .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
            let current = object_lock_requests(bound, self.components.catalog.as_ref())?;
            if object_guard.covers(&current)? {
                return Ok(catalog_guard);
            }
            drop(catalog_guard);
            object_guard.restore(object_baseline)?;
            object_guard.acquire_many(&current, cancel)?;
        }
    }

    /// Route a parsed simple-query statement through the transaction lifecycle.
    /// `default_isolation` is the session default (in/out, like `slot`); only
    /// transaction-control statements read or update it, so the data and maintenance
    /// arms pass it back unchanged.
    pub(super) fn dispatch(
        &self,
        statement: Statement,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        session: &QuerySessionContext,
        // `Some` streams a `SELECT`'s rows into the sink; `None` materializes.
        // Only the data (read/write) arms consult it; every other arm ignores it
        // and returns a non-streamed `StreamOutcome`.
        sink: Option<&mut dyn RowSink>,
    ) -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>) {
        let class = match statement_class(&statement) {
            Ok(class) => class,
            Err(err) => {
                // A parse/classification error inside an open transaction still
                // poisons it to the failed state (matching Postgres).
                let slot = mark_failed_on_error(slot);
                return (slot, default_isolation, Err(err));
            }
        };

        if let StatementClass::TransactionControl(kind) = class {
            let (slot, default_isolation, result) = self.handle_transaction_control(
                kind,
                slot,
                default_isolation,
                session.cancel(),
                session.gucs(),
            );
            return (slot, default_isolation, result.map(StreamOutcome::Direct));
        }

        if let StatementClass::SessionConfig = class {
            if let Err(err) = session.cancel().check() {
                return (mark_failed_on_error(slot), default_isolation, Err(err));
            }
            let resets_session_objects = matches!(statement, Statement::DiscardAll);
            let mutates_session = matches!(
                statement,
                Statement::SetVariable { .. }
                    | Statement::ResetVariable { .. }
                    | Statement::DiscardAll
            );
            let (slot, default_isolation, result) = self.handle_session_config(
                statement,
                slot,
                default_isolation,
                session.gucs(),
                session.session_sequences(),
            );
            let result = result.map(|result| {
                if resets_session_objects {
                    StreamOutcome::SessionReset(result)
                } else if mutates_session {
                    StreamOutcome::Durable(result)
                } else {
                    StreamOutcome::Direct(result)
                }
            });
            return (slot, default_isolation, result);
        }

        // Savepoints (SAVEPOINT / RELEASE / ROLLBACK TO) drive the session's
        // transaction lifecycle like transaction control; the op + name are read
        // from the parsed statement (`docs/specs/savepoints.md`).
        if let StatementClass::Savepoint = class {
            if let Err(err) = session.cancel().check() {
                return (mark_failed_on_error(slot), default_isolation, Err(err));
            }
            let (slot, default_isolation, result) =
                self.handle_savepoint(statement, slot, default_isolation);
            return (slot, default_isolation, result.map(StreamOutcome::Durable));
        }

        if let StatementClass::SqlCursor = class {
            return (
                mark_failed_on_error(slot),
                default_isolation,
                Err(DbError::plan(
                    SqlState::FeatureNotSupported,
                    "SQL cursors require the connection cursor path",
                )),
            );
        }

        // Maintenance commands do not bind/plan, and like DDL are forbidden inside
        // an explicit transaction block (Postgres: "VACUUM cannot run inside a
        // transaction block"). Reject with the open transaction poisoned to the 'E'
        // failed state, matching the DDL-in-block contract.
        if let StatementClass::Maintenance = class {
            if matches!(statement, Statement::Truncate { .. })
                && let Some(mut txn) = slot
            {
                if txn.failed {
                    return (
                        Some(txn),
                        default_isolation,
                        Err(DbError::execute(
                            SqlState::InFailedSqlTransaction,
                            "current transaction is aborted, commands ignored until end of transaction block",
                        )),
                    );
                }
                let statement = match self.transaction_catalog(&txn).and_then(|catalog| {
                    self.qualify_maintenance_statement(
                        statement,
                        catalog.as_ref(),
                        &session.gucs.search_path_names(&session.session_info.user),
                    )
                }) {
                    Ok(statement) => statement,
                    Err(err) => return (Some(txn), default_isolation, Err(err)),
                };
                let result = self
                    .run_truncate_in_transaction(&mut txn, statement, session.cancel().as_ref())
                    .map(StreamOutcome::Direct);
                if let Err(err) = &result {
                    if err.code == SqlState::DeadlockDetected {
                        self.abort_deadlock_victim(&mut txn);
                    } else {
                        txn.failed = true;
                    }
                }
                return (Some(txn), default_isolation, result);
            }
            if let Some(mut txn) = slot {
                txn.failed = true;
                return (
                    Some(txn),
                    default_isolation,
                    Err(DbError::plan(
                        SqlState::FeatureNotSupported,
                        "maintenance commands cannot run inside a transaction block",
                    )),
                );
            }
            let statement = match self.qualify_maintenance_statement(
                statement,
                self.components.catalog.as_ref(),
                &session.gucs.search_path_names(&session.session_info.user),
            ) {
                Ok(statement) => statement,
                Err(err) => return (None, default_isolation, Err(err)),
            };
            return (
                None,
                default_isolation,
                self.run_maintenance(
                    statement,
                    session.cancel(),
                    session.gucs().default_statistics_target(),
                )
                .map(StreamOutcome::Durable),
            );
        }

        // COPY is bound here (resolve table/columns) but not executed: it returns a
        // `BeginCopyIn`/`BeginCopyOut` request that the connection loop drives over
        // the COPY sub-protocol. Object locks are acquired and the statement is
        // rebound/revalidated before its snapshots are captured and carried into
        // the streaming driver.
        if let StatementClass::Copy(direction) = class {
            let (slot, default_isolation, result) =
                self.dispatch_copy(direction, statement, slot, default_isolation, session);
            return (slot, default_isolation, result);
        }

        match slot {
            // A data statement with an open explicit transaction runs inside it.
            Some(txn) => {
                let runtime = session.statement_runtime(
                    txn.current_default_isolation(default_isolation),
                    txn.isolation,
                    txn.current_statement_timeout_ms(session.statement_timeout_ms()),
                );
                let (slot, result) = self.run_in_transaction(txn, class, statement, runtime, sink);
                (slot, default_isolation, result)
            }
            // No open transaction: this is an autocommit unit.
            None => {
                let result =
                    self.run_autocommit(class, statement, session, default_isolation, sink);
                (None, default_isolation, result)
            }
        }
    }

    /// Bind a COPY statement and return the request the connection loop will drive.
    /// Like other statements, a bind error (unknown table/column) poisons an open
    /// transaction; the slot is otherwise returned unchanged.
    fn dispatch_copy(
        &self,
        direction: CopyDirection,
        statement: Statement,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        session: &QuerySessionContext,
    ) -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>) {
        let cancel = session.cancel();
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

        let mut slot = slot;
        if let Err(err) = self.bind_with_object_requests_for_path(
            &statement,
            &session.gucs.search_path_names(&session.session_info.user),
        ) {
            if let Some(txn) = slot.as_mut() {
                txn.failed = true;
            }
            return (slot, default_isolation, Err(err));
        }

        let (bound, snapshots, schema_versions) = match slot.as_mut() {
            Some(txn) => {
                let overlay = txn.catalog_overlay.clone();
                let updates = txn.truncate_updates.clone();
                let locked = match self.ensure_transaction_lock_owner(txn, session.cancel()) {
                    Ok(owner) => self.bind_and_lock_unprepared_in_transaction(
                        &statement,
                        overlay,
                        updates,
                        &session.gucs.search_path_names(&session.session_info.user),
                        owner,
                        session.cancel(),
                    ),
                    Err(err) => Err(err),
                };
                let (bound, schema_versions, validated_catalog) = match locked {
                    Ok(locked) => locked,
                    Err(err) => {
                        if err.code == SqlState::DeadlockDetected {
                            self.abort_deadlock_victim(txn);
                        } else {
                            txn.failed = true;
                        }
                        return (slot, default_isolation, Err(err));
                    }
                };
                txn.has_writes |= direction == CopyDirection::From;
                let snapshots = match self.snapshots_for_transaction(txn, cancel) {
                    Ok(snapshots) => snapshots,
                    Err(err) => {
                        txn.failed = true;
                        return (slot, default_isolation, Err(err));
                    }
                };
                let (catalog, catalog_is_snapshot) = match self
                    .transaction_statement_catalog_from_validated(txn, &bound, validated_catalog)
                {
                    Ok(catalog) => catalog,
                    Err(err) => {
                        txn.failed = true;
                        return (slot, default_isolation, Err(err));
                    }
                };
                let catalog = if catalog_is_snapshot {
                    catalog
                } else {
                    match self.snapshot_catalog_view(catalog.as_ref()) {
                        Ok(catalog) => catalog,
                        Err(err) => {
                            txn.failed = true;
                            return (slot, default_isolation, Err(err));
                        }
                    }
                };
                txn.first_statement_ran = true;
                (
                    bound,
                    CopySnapshots::Transaction {
                        snapshots,
                        catalog,
                        catalog_is_snapshot: true,
                    },
                    schema_versions,
                )
            }
            None if direction == CopyDirection::From => {
                let txn_id = self.register_active_txn();
                let write_guard = match self.components.concurrency.begin_writer_cancelable(cancel)
                {
                    Ok(guard) => guard,
                    Err(err) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return (None, default_isolation, Err(err));
                    }
                };
                let mut object_guard = match self.components.lock_manager.transaction_owner(txn_id)
                {
                    Ok(guard) => guard,
                    Err(err) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return (None, default_isolation, Err(err));
                    }
                };
                let (bound, schema_versions) = match self.bind_and_lock_unprepared_for_path(
                    &statement,
                    &session.gucs.search_path_names(&session.session_info.user),
                    &mut object_guard,
                    cancel,
                ) {
                    Ok(locked) => locked,
                    Err(err) => {
                        drop(object_guard);
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return (None, default_isolation, Err(err));
                    }
                };
                let snapshots = match self.capture_consistent_snapshots_cancelable(txn_id, cancel) {
                    Ok(snapshots) => snapshots,
                    Err(err) => {
                        drop(object_guard);
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return (None, default_isolation, Err(err));
                    }
                };
                let catalog = match self.snapshot_catalog() {
                    Ok(catalog) => catalog,
                    Err(err) => {
                        drop(object_guard);
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return (None, default_isolation, Err(err));
                    }
                };
                (
                    bound,
                    CopySnapshots::Autocommit {
                        snapshots,
                        catalog,
                        write: Some(AutocommitCopyWrite::new(
                            self.components.clone(),
                            txn_id,
                            object_guard,
                            write_guard,
                        )),
                        object_guard: None,
                    },
                    schema_versions,
                )
            }
            None => {
                let mut object_guard = self.components.lock_manager.statement_owner();
                let (bound, schema_versions) = match self.bind_and_lock_unprepared_for_path(
                    &statement,
                    &session.gucs.search_path_names(&session.session_info.user),
                    &mut object_guard,
                    cancel,
                ) {
                    Ok(locked) => locked,
                    Err(err) => return (None, default_isolation, Err(err)),
                };
                let snapshots = match self.capture_consistent_snapshots_cancelable(0, cancel) {
                    Ok(snapshots) => snapshots,
                    Err(err) => return (None, default_isolation, Err(err)),
                };
                let catalog = match self.snapshot_catalog() {
                    Ok(catalog) => catalog,
                    Err(err) => return (None, default_isolation, Err(err)),
                };
                (
                    bound,
                    CopySnapshots::Autocommit {
                        snapshots,
                        catalog,
                        write: None,
                        object_guard: Some(object_guard),
                    },
                    schema_versions,
                )
            }
        };

        let relations = match &snapshots {
            CopySnapshots::Autocommit { snapshots, .. } => snapshots.relations.as_ref(),
            CopySnapshots::Transaction { snapshots, .. } => snapshots.relations.as_ref(),
        };
        if let Err(err) = self.validate_relation_snapshot_schema_versions(
            relations,
            &schema_versions,
            direction == CopyDirection::To,
        ) {
            if let Some(txn) = slot.as_mut() {
                txn.failed = true;
            }
            return (slot, default_isolation, Err(err));
        }
        let BoundStatement::Copy {
            table_schema,
            columns,
            options,
            default_exprs,
            check_exprs,
            ..
        } = bound
        else {
            return (
                slot,
                default_isolation,
                Err(DbError::internal("COPY bound to a non-COPY statement")),
            );
        };
        let job = CopyJob {
            schema: table_schema,
            columns,
            options,
            default_exprs,
            check_exprs,
        };
        let result = match direction {
            CopyDirection::From => Ok(StreamOutcome::BeginCopyIn { job, snapshots }),
            CopyDirection::To => Ok(StreamOutcome::BeginCopyOut { job, snapshots }),
        };
        (slot, default_isolation, result)
    }

    /// Run a simple-query data statement inside an open explicit transaction:
    /// bind it against the catalog, then execute the bound form within `txn`.
    fn run_in_transaction(
        &self,
        txn: Transaction,
        class: StatementClass,
        statement: Statement,
        runtime: StatementRuntime<'_>,
        sink: Option<&mut dyn RowSink>,
    ) -> (Option<Transaction>, Result<StreamOutcome>) {
        self.run_bound_in_transaction(txn, class, BindSource::Unbound(statement), runtime, sink)
    }

    /// Run a data statement inside an open explicit transaction. The statement is
    /// supplied either unbound (simple query; bound here against the live catalog)
    /// or already bound (extended-protocol `Execute`, with parameters
    /// substituted). Both paths reuse the *same* in-transaction machinery: the 'E'
    /// failed-state gate, the DDL rejection, and the single lazily-acquired write
    /// guard — so the open transaction's one `WriteGuard` is acquired at most once.
    pub(super) fn run_bound_in_transaction(
        &self,
        mut txn: Transaction,
        class: StatementClass,
        source: BindSource,
        runtime: StatementRuntime<'_>,
        sink: Option<&mut dyn RowSink>,
    ) -> (Option<Transaction>, Result<StreamOutcome>) {
        // While failed ('E'), reject everything but COMMIT/ROLLBACK (handled in
        // `handle_transaction_control`, never reaching here).
        if txn.failed {
            return (
                Some(txn),
                Err(DbError::execute(
                    SqlState::InFailedSqlTransaction,
                    "current transaction is aborted, commands ignored until end of transaction block",
                )),
            );
        }

        let initial_class = class;
        let locked = match source {
            BindSource::Unbound(statement) => {
                let discovery = self.transaction_catalog(&txn).and_then(|catalog| {
                    let options =
                        self.bind_options(catalog.as_ref(), runtime.search_path_names())?;
                    let bound = planner::bind_with_options(&statement, catalog.as_ref(), &options)?;
                    let requests = object_lock_requests(&bound, catalog.as_ref())?;
                    Ok((bound, requests, catalog))
                });
                match discovery {
                    Ok((bound, requests, catalog)) if requests.is_empty() => {
                        prepared_schema_versions(&bound, catalog.as_ref())
                            .map(|versions| (bound, versions, catalog))
                    }
                    Ok((_bound, _requests, _catalog)) => {
                        let overlay = txn.catalog_overlay.clone();
                        let updates = txn.truncate_updates.clone();
                        match self.ensure_transaction_lock_owner(&mut txn, runtime.cancel()) {
                            Ok(owner) => self.bind_and_lock_unprepared_in_transaction(
                                &statement,
                                overlay,
                                updates,
                                runtime.search_path_names(),
                                owner,
                                runtime.cancel(),
                            ),
                            Err(err) => Err(err),
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            BindSource::Bound {
                bound,
                schema_versions,
            } => {
                let updates = txn.truncate_updates.clone();
                let overlay = txn.catalog_overlay.clone();
                let requests = self
                    .transaction_catalog(&txn)
                    .and_then(|catalog| object_lock_requests(&bound, catalog.as_ref()));
                match requests {
                    Ok(requests) if requests.is_empty() => {
                        self.transaction_catalog(&txn).and_then(|catalog| {
                            validate_prepared_schema_versions_in_catalog(
                                &schema_versions,
                                catalog.as_ref(),
                            )
                            .map(|()| (bound, schema_versions, catalog))
                        })
                    }
                    Ok(_) => match self.ensure_transaction_lock_owner(&mut txn, runtime.cancel()) {
                        Ok(owner) => self
                            .lock_prepared_bound_in_transaction(
                                &bound,
                                &schema_versions,
                                &updates,
                                &overlay,
                                owner,
                                runtime.cancel(),
                            )
                            .map(|catalog| (bound, schema_versions, catalog)),
                        Err(err) => Err(err),
                    },
                    Err(err) => Err(err),
                }
            }
        };
        let (bound, schema_versions, validated_catalog) = match locked {
            Ok(locked) => locked,
            Err(err) => {
                if err.code == SqlState::DeadlockDetected {
                    self.abort_deadlock_victim(&mut txn);
                } else {
                    txn.failed = true;
                }
                return (Some(txn), Err(err));
            }
        };
        let class = classify_bound(initial_class, &bound);

        let is_write = matches!(class, StatementClass::Write | StatementClass::Ddl);
        if matches!(class, StatementClass::Ddl)
            && txn.object_locks.is_none()
            && let Err(err) = self.ensure_transaction_lock_owner(&mut txn, runtime.cancel())
        {
            txn.failed = true;
            return (Some(txn), Err(err));
        }
        txn.has_writes |= is_write;
        let captured_snapshot = match self.snapshots_for_transaction(&mut txn, runtime.cancel()) {
            Ok(snapshots) => snapshots,
            Err(err) => {
                txn.failed = true;
                return (Some(txn), Err(err));
            }
        };
        txn.first_statement_ran = true;
        if let Err(err) = self.validate_relation_snapshot_schema_versions(
            captured_snapshot.relations.as_ref(),
            &schema_versions,
            matches!(class, StatementClass::Read),
        ) {
            txn.failed = true;
            return (Some(txn), Err(err));
        }

        // Capture the snapshot and hold its GC-horizon advertisement across this
        // statement's execution. Under Read Committed `advertised` is the
        // per-statement guard dropped at the end of this call (releasing the prior
        // statement's pinned xmin); under Repeatable Read it is `None` because the
        // reusable snapshot's advertisement lives on `txn` for the whole
        // transaction (`docs/specs/mvcc.md` §9).
        let TransactionSnapshots {
            snapshot,
            relations,
            advertised,
        } = captured_snapshot;
        // This statement has now captured the transaction's snapshot, so the
        // transaction has "run a query": a later `SET TRANSACTION ISOLATION LEVEL`
        // must be rejected (the before-first-query guard). Set here, before
        // `run_plan`, so an execute-time error still counts as a run first command.
        // (A first statement that fails earlier, at bind or write-guard acquisition,
        // returns above with `failed = true` and never reaches here; a following
        // `SET TRANSACTION` is then gated by the 'E' state instead.)
        txn.first_statement_ran = true;
        // The GC horizon is used by the H3 UPDATE update-path prune (`docs/specs/mvcc.md`
        // §10 H3); CREATE INDEX is the other consumer. A stale/smaller horizon only
        // prunes less, never unsafely (the prune reclaims only dead-to-all versions and
        // mutates one latched page).
        let gc_horizon = self.components.gc_horizon();
        let (statement_catalog, catalog_is_snapshot) = match self
            .transaction_statement_catalog_from_validated(&txn, &bound, validated_catalog)
        {
            Ok(catalog) => catalog,
            Err(err) => {
                txn.failed = true;
                return (Some(txn), Err(err));
            }
        };
        // The writing xid is the innermost open savepoint's subxid (or `txn_id`),
        // and the live (sub)xid set is threaded for own-write/own-conflict detection
        // (`docs/specs/savepoints.md` §4).
        let result = (|| {
            let ctx = self.execution_context_with_selected_catalog(
                ExecutionContextInput {
                    txn_id: txn.writing_xid(),
                    snapshot,
                    relations,
                    isolation: txn.isolation,
                    gc_horizon,
                    live_txns: txn.live_txns(),
                    runtime,
                },
                statement_catalog.clone(),
                catalog_is_snapshot,
            )?;

            // Only a read (a plain `SELECT`) streams; a write is materialized, so the
            // sink is withheld from `run_plan` for writes and the executor is never
            // asked to stream a DML plan.
            let read_sink = if is_write { None } else { sink };
            let result = run_plan(
                &self.engine,
                &ctx,
                bound,
                statement_catalog.as_ref(),
                read_sink,
            );
            // The snapshot can no longer be used to read once `run_plan` has returned.
            drop(ctx);
            result
        })();
        // The snapshot can no longer be used to read once `run_plan` has returned;
        // drop the per-statement advertisement now (a no-op under Repeatable Read).
        drop(advertised);
        match result {
            Ok(outcome) => {
                if matches!(class, StatementClass::Ddl) {
                    let snapshot = match statement_catalog.snapshot() {
                        Ok(snapshot) => snapshot,
                        Err(err) => {
                            txn.failed = true;
                            return (Some(txn), Err(err));
                        }
                    };
                    if let Err(err) = txn.catalog_overlay.absorb(snapshot.clone()) {
                        txn.failed = true;
                        return (Some(txn), Err(err));
                    }
                    reconcile_truncate_updates_after_ddl(&mut txn.truncate_updates, &snapshot);
                }
                // Accumulate this statement's dead-version count on the transaction
                // (`docs/specs/mvcc.md` §9, F4b). It is folded into the server-wide
                // auto-prune counter only when the transaction COMMITS durably; on
                // ROLLBACK it is discarded (the dead versions then belong to this
                // transaction's own aborted writes, not to committed deletes/updates).
                // A streamed outcome is always a read, which leaves no dead versions.
                let dead = match &outcome {
                    StreamOutcome::Streamed { .. } => 0,
                    StreamOutcome::Direct(result)
                    | StreamOutcome::Durable(result)
                    | StreamOutcome::SessionReset(result) => dead_versions_in(result),
                    StreamOutcome::BeginCopyIn { .. } | StreamOutcome::BeginCopyOut { .. } => 0,
                };
                txn.dead_versions_pending = txn.dead_versions_pending.saturating_add(dead);
                let changed = match &outcome {
                    StreamOutcome::Streamed { .. } => 0,
                    StreamOutcome::Direct(result)
                    | StreamOutcome::Durable(result)
                    | StreamOutcome::SessionReset(result) => changed_rows_in(result),
                    StreamOutcome::BeginCopyIn { .. } | StreamOutcome::BeginCopyOut { .. } => 0,
                };
                txn.changed_rows_pending = txn.changed_rows_pending.saturating_add(changed);
                (Some(txn), Ok(outcome))
            }
            Err(err) => {
                // Any statement error poisons the transaction: it enters 'E' and
                // must be ended with COMMIT/ROLLBACK. No partial-statement undo is
                // needed — the failed statement's versions stay invisible via the
                // CLOG (the transaction will be marked Aborted on ROLLBACK), and
                // abort is status-based, not before-image undo (`docs/specs/mvcc.md`
                // §4 Decision 3, Milestone D1).
                if err.code == SqlState::DeadlockDetected {
                    self.abort_deadlock_victim(&mut txn);
                } else {
                    txn.failed = true;
                }
                (Some(txn), Err(err))
            }
        }
    }

    /// Run a data/DDL statement as an implicit single-statement transaction
    /// (autocommit): allocate, snapshot, execute, and commit-or-abort.
    fn run_autocommit(
        &self,
        class: StatementClass,
        statement: Statement,
        session: &QuerySessionContext,
        default_isolation: IsolationLevel,
        sink: Option<&mut dyn RowSink>,
    ) -> Result<StreamOutcome> {
        match class {
            StatementClass::Read => {
                let (initial, requests) = self.bind_with_object_requests_for_path(
                    &statement,
                    &session.gucs.search_path_names(&session.session_info.user),
                )?;
                if matches!(classify_bound(class, &initial), StatementClass::Write) {
                    return self
                        .autocommit_write(
                            statement,
                            session.statement_runtime(
                                default_isolation,
                                default_isolation,
                                session.statement_timeout_ms(),
                            ),
                        )
                        .map(StreamOutcome::Direct);
                }
                let (bound, object_guard) = if requests.is_empty() {
                    (initial, None)
                } else {
                    let mut guard = self.components.lock_manager.statement_owner();
                    let (bound, _) = self.bind_and_lock_unprepared_for_path(
                        &statement,
                        &session.gucs.search_path_names(&session.session_info.user),
                        &mut guard,
                        session.cancel().as_ref(),
                    )?;
                    (bound, Some(guard))
                };
                match classify_bound(class, &bound) {
                    StatementClass::Read => {
                        let captured =
                            self.capture_consistent_snapshots_cancelable(0, session.cancel())?;
                        self.autocommit_read_with_snapshot(
                            bound,
                            session.statement_runtime(
                                default_isolation,
                                default_isolation,
                                session.statement_timeout_ms(),
                            ),
                            sink,
                            captured,
                            object_guard,
                        )
                    }
                    // A catalog change during discovery can only add/remove objects
                    // by forcing the bind/lock loop to retry, but keep this branch
                    // total: restart the unprepared statement under its xid owner.
                    StatementClass::Write => {
                        drop(object_guard);
                        self.autocommit_write(
                            statement,
                            session.statement_runtime(
                                default_isolation,
                                default_isolation,
                                session.statement_timeout_ms(),
                            ),
                        )
                        .map(StreamOutcome::Direct)
                    }
                    _ => unreachable!("classify_bound only promotes reads to writes"),
                }
            }
            StatementClass::Write | StatementClass::Ddl => self
                .autocommit_write(
                    statement,
                    session.statement_runtime(
                        default_isolation,
                        default_isolation,
                        session.statement_timeout_ms(),
                    ),
                )
                .map(StreamOutcome::Direct),
            // Maintenance never reaches here: `dispatch` runs it via
            // `run_maintenance` before the autocommit data path.
            StatementClass::Maintenance => Err(DbError::internal(
                "maintenance reached the autocommit data path",
            )),
            // Transaction-control statements never reach here (dispatch routes
            // them through `handle_transaction_control`).
            StatementClass::TransactionControl(_) => Err(DbError::internal(
                "transaction control reached the autocommit data path",
            )),
            // Session configuration is dispatched before the autocommit data path.
            StatementClass::SessionConfig => Err(DbError::internal(
                "session configuration reached the autocommit data path",
            )),
            StatementClass::SqlCursor => Err(DbError::internal(
                "SQL cursor reached the autocommit data path",
            )),
            // COPY is intercepted by `dispatch` (→ `dispatch_copy`) and driven by the
            // connection loop, never the autocommit data path.
            StatementClass::Copy(_) => {
                Err(DbError::internal("COPY reached the autocommit data path"))
            }
            // Savepoints are routed through `handle_savepoint` in `dispatch`, never
            // the autocommit data path.
            StatementClass::Savepoint => Err(DbError::internal(
                "savepoint reached the autocommit data path",
            )),
        }
    }

    /// Execute a read-only statement (SELECT/EXPLAIN) with statement-owned object
    /// locks and a snapshot captured under the registry latch. No
    /// `ConcurrencyController` guard is taken, so autocommit reads still run
    /// concurrently with in-flight writers (`docs/specs/mvcc.md` §7.1).
    pub(super) fn autocommit_read_with_snapshot(
        &self,
        bound: BoundStatement,
        runtime: StatementRuntime<'_>,
        sink: Option<&mut dyn RowSink>,
        captured: CapturedSnapshots,
        _object_guard: Option<crate::lock_manager::ObjectLockGuard>,
    ) -> Result<StreamOutcome> {
        if let BoundStatement::Explain(inner) = &bound {
            return self.explain(inner.as_ref()).map(StreamOutcome::Direct);
        }
        // A read is not its own transaction (txn_id 0 / INVALID_XID), so no own
        // txn is excluded; the snapshot sees all committed rows and skips any
        // in-flight writer's uncommitted versions via MVCC visibility.
        //
        // Advertise the snapshot's `xmin` to the GC horizon and HOLD the guard
        // across the whole scan (`docs/specs/mvcc.md` §9). This is the new behavior
        // reads must gain: an autocommit `SELECT` is not in the active registry, so
        // without advertising its `xmin` the GC horizon would ignore it and VACUUM
        // could reclaim a version this long-lived read still sees live (data loss —
        // the worst path). `_advertised` lives until the end of this function, i.e.
        // exactly the snapshot's usable lifetime.
        let CapturedSnapshots {
            snapshot,
            relations,
            advertised: _advertised,
        } = captured;
        let schema_versions = prepared_schema_versions(&bound, self.components.catalog.as_ref())?;
        self.validate_relation_snapshot_schema_versions(
            relations.as_ref(),
            &schema_versions,
            true,
        )?;
        // A read never runs CREATE INDEX (the only horizon consumer), so the horizon
        // is unused on this path; pass `0`.
        let ctx = self.execution_context_for_bound(
            ExecutionContextInput {
                txn_id: 0,
                snapshot,
                relations,
                isolation: IsolationLevel::default(),
                gc_horizon: 0,
                live_txns: Arc::from([0]),
                runtime,
            },
            &bound,
        )?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        // `_advertised` is held across the drive (including any producer block on a
        // full channel) and dropped when this returns, exactly as on the
        // materializing path (`docs/specs/streaming.md` §5).
        exec_or_stream(&self.engine, &ctx, &physical, sink)
    }

    /// Execute a write/DDL statement as an autocommit unit, committing durably on
    /// success and aborting on error.
    ///
    /// DML and DDL take the SHARED writer guard. Relation locks provide scoped
    /// exclusion, while catalog-changing DDL additionally holds the exclusive
    /// catalog publication gate across mutation, durable commit, and rollback.
    pub(super) fn autocommit_write(
        &self,
        statement: Statement,
        runtime: StatementRuntime<'_>,
    ) -> Result<ExecutionResult> {
        let (_, _, catalog_noop) =
            self.bind_with_object_requests_and_preflight(&statement, runtime.search_path_names())?;
        if catalog_noop {
            return Ok(ExecutionResult::Modified {
                command: "ALTER TABLE".to_string(),
                count: 0,
            });
        }
        let txn_id = self.register_active_txn();
        let guard = match self
            .components
            .concurrency
            .begin_writer_cancelable(runtime.cancel())
        {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let mut object_guard = match self.components.lock_manager.transaction_owner(txn_id) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let object_baseline = object_guard.snapshot();
        let (bound, schema_versions) = match self.bind_and_lock_unprepared_for_path(
            &statement,
            runtime.search_path_names(),
            &mut object_guard,
            runtime.cancel(),
        ) {
            Ok(locked) => locked,
            Err(err) => {
                drop(object_guard);
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        self.autocommit_bound_write_with_guard(
            bound,
            guard,
            runtime,
            Some(&schema_versions),
            (txn_id, object_guard, object_baseline),
        )
    }

    pub(super) fn autocommit_prepared_bound_write(
        &self,
        bound: BoundStatement,
        runtime: StatementRuntime<'_>,
        prepared_schema_versions: Option<&[super::PreparedRelationVersion]>,
    ) -> Result<ExecutionResult> {
        let schema_versions = match prepared_schema_versions {
            Some(versions) => versions.to_vec(),
            None => self.schema_versions_for_bound(&bound)?,
        };
        let schema_alter = matches!(
            &bound,
            BoundStatement::AlterTableAddColumn { .. }
                | BoundStatement::AlterTableDropColumn { .. }
        );
        if schema_alter && self.prepared_catalog_change_is_noop(&bound, &schema_versions)? {
            return Ok(ExecutionResult::Modified {
                command: "ALTER TABLE".to_string(),
                count: 0,
            });
        }
        let txn_id = self.register_active_txn();
        let guard = match self
            .components
            .concurrency
            .begin_writer_cancelable(runtime.cancel())
        {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let mut object_guard = match self.components.lock_manager.transaction_owner(txn_id) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let object_baseline = object_guard.snapshot();
        if let Err(err) = self.lock_prepared_bound(
            &bound,
            &schema_versions,
            &mut object_guard,
            runtime.cancel(),
        ) {
            drop(object_guard);
            self.rollback_pre_durable_or_die(txn_id, None);
            return Err(err);
        }
        self.autocommit_bound_write_with_guard(
            bound,
            guard,
            runtime,
            Some(&schema_versions),
            (txn_id, object_guard, object_baseline),
        )
    }

    fn autocommit_bound_write_with_guard(
        &self,
        bound: BoundStatement,
        guard: WriteUnitGuard,
        runtime: StatementRuntime<'_>,
        prepared_schema_versions: Option<&[super::PreparedRelationVersion]>,
        locked_txn: (
            u64,
            crate::lock_manager::ObjectLockGuard,
            crate::lock_manager::OwnerGrantSnapshot,
        ),
    ) -> Result<ExecutionResult> {
        // The autocommit unit begins: allocate the transaction id and register it
        // active atomically (so a concurrent reader's snapshot is not torn). Its
        // CLOG status is `InProgress` implicitly until a `Commit`/`Abort` record
        // settles it.
        let (txn_id, mut object_guard, object_baseline) = locked_txn;
        let catalog_publication = if bound_mutates_catalog(&bound) {
            match self.catalog_write_after_lock_convergence(
                &bound,
                &mut object_guard,
                &object_baseline,
                runtime.cancel(),
            ) {
                Ok(guard) => Some(guard),
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            }
        } else {
            None
        };
        let logical = match logical_plan(&bound) {
            Ok(logical) => logical,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let physical = match physical_plan(&logical, self.components.catalog.as_ref()) {
            Ok(physical) => physical,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let catalog_before = if bound_mutates_catalog(&bound) {
            match self.components.catalog.snapshot() {
                Ok(snapshot) => Some(snapshot),
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            }
        } else {
            None
        };
        // Capture the snapshot after registering, excluding the own id so own
        // writes are seen via the predicate's `current_txn` path. Advertise its
        // `xmin` to the GC horizon and hold `_advertised` across execution and the
        // commit/rollback that follow (`docs/specs/mvcc.md` §9): it lives until this
        // function returns on every path (success, statement error, panic), exactly
        // bracketing when the snapshot can still be used to read.
        let captured = self
            .capture_consistent_snapshots_cancelable(txn_id, runtime.cancel())
            .map(
                |CapturedSnapshots {
                     snapshot,
                     relations,
                     advertised,
                 }| (snapshot, relations, Some(advertised)),
            );
        let (snapshot, relations, advertised) = match captured {
            Ok(captured) => captured,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, catalog_before.clone());
                return Err(err);
            }
        };
        let schema_versions = match prepared_schema_versions {
            Some(schema_versions) => schema_versions.to_vec(),
            None => match self.schema_versions_for_bound(&bound) {
                Ok(schema_versions) => schema_versions,
                Err(err) => {
                    drop(advertised);
                    self.rollback_pre_durable_or_die(txn_id, catalog_before.clone());
                    return Err(err);
                }
            },
        };
        if let Err(err) = self.validate_relation_snapshot_schema_versions(
            relations.as_ref(),
            &schema_versions,
            false,
        ) {
            drop(advertised);
            self.rollback_pre_durable_or_die(txn_id, catalog_before.clone());
            return Err(err);
        }
        // Capture the GC horizon. CREATE INDEX holds Share on its target, so a target
        // writer cannot create a newer dead chain during backfill; unrelated commits
        // may advance the true horizon, but this captured lower value remains safe.
        // An UPDATE needs it for the H3 update-path prune
        // (`docs/specs/mvcc.md` §10 H3). For an UPDATE under the SHARED writer guard a
        // concurrent writer/commit could advance the true horizon after this read, but
        // a stale/smaller horizon only prunes LESS — never unsafely — so capturing it
        // here (before execution) is sound. Other statements ignore it.
        let gc_horizon = self.components.gc_horizon();
        let context_input = ExecutionContextInput {
            txn_id,
            snapshot,
            relations,
            isolation: IsolationLevel::default(),
            gc_horizon,
            live_txns: Arc::from([txn_id]),
            runtime,
        };
        let context = if catalog_publication.is_some() {
            self.execution_context_under_catalog_gate(context_input)
        } else {
            self.execution_context_for_bound(context_input, &bound)
        };
        let ctx = match context {
            Ok(ctx) => ctx,
            Err(err) => {
                drop(advertised);
                self.rollback_pre_durable_or_die(txn_id, catalog_before.clone());
                return Err(err);
            }
        };

        let result = catch_unwind(AssertUnwindSafe(|| self.engine.execute(&ctx, &physical)));
        drop(ctx);
        drop(advertised);
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                self.rollback_pre_durable_or_die(txn_id, catalog_before.clone());
                return Err(err);
            }
            Err(_) => {
                self.rollback_pre_durable_or_die(txn_id, catalog_before.clone());
                return Err(DbError::internal("statement execution panicked"));
            }
        };

        // An autocommit unit has no savepoints, so no committed subxids.
        if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
            self.rollback_pre_durable_or_die(txn_id, catalog_before);
            return Err(err);
        }

        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        // The commit is durable and cleaned up; the CLOG already recorded it
        // `Committed` (set inside `wal.flush`). Drop it from the active set and wake
        // any writer blocked on its row locks.
        self.components.active_txns.deregister(txn_id);
        self.components.lock_manager.on_txn_finished();
        drop(catalog_publication);
        drop(object_guard);
        drop(guard);

        // Account this committed statement's dead versions toward the auto-prune
        // threshold BEFORE the checkpoint trigger, so a checkpoint fired by this same
        // commit observes the updated count (`docs/specs/mvcc.md` §9, F4b). Only a
        // durable commit reaches here; an aborted statement returned above without
        // counting.
        self.components.add_dead_versions(dead_versions_in(&result));
        self.components.add_changed_rows(changed_rows_in(&result));

        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);

        Ok(result)
    }

    fn explain(&self, inner: &BoundStatement) -> Result<ExecutionResult> {
        if !matches!(inner, BoundStatement::Query(_)) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                "EXPLAIN supports SELECT only in v1",
            ));
        }
        let logical = logical_plan(inner)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        Ok(ExecutionResult::Explanation {
            text: format_explain(&physical, self.components.catalog.as_ref()),
        })
    }
}

fn bound_mutates_catalog(bound: &BoundStatement) -> bool {
    matches!(
        bound,
        BoundStatement::CreateTable { .. }
            | BoundStatement::DropTable { .. }
            | BoundStatement::AlterTableAddColumn { .. }
            | BoundStatement::AlterTableDropColumn { .. }
            | BoundStatement::AlterTableRenameColumn { .. }
            | BoundStatement::AlterTableRenameTable { .. }
            | BoundStatement::CreateIndex { .. }
            | BoundStatement::DropIndex { .. }
            | BoundStatement::CreateSequence { .. }
            | BoundStatement::DropSequence { .. }
            | BoundStatement::CreateView { .. }
            | BoundStatement::DropView { .. }
    )
}

fn reconcile_truncate_updates_after_ddl(
    updates: &mut std::collections::BTreeMap<common::TableId, common::TruncateCatalogUpdate>,
    snapshot: &catalog::CatalogSnapshot,
) {
    updates.retain(|table_id, update| {
        let Some(table) = snapshot.tables_by_id.get(table_id) else {
            return false;
        };
        update.table = table.clone();
        update.toast_table = table
            .toast_table_id
            .and_then(|toast_id| snapshot.tables_by_id.get(&toast_id).cloned());
        let mut indexes = snapshot
            .indexes_by_id
            .values()
            .filter(|index| index.table == *table_id)
            .cloned()
            .collect::<Vec<_>>();
        indexes.sort_unstable_by_key(|index| index.id);
        update.indexes = indexes;
        true
    });
}
