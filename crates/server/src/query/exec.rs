use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use catalog::CatalogManager;
use common::{CopyDirection, DbError, IsolationLevel, QueryCancel, Result, SqlState, TableId};
use executor::{CopyJob, ExecutionResult, RowSink};
use parser::Statement;
use planner::{BoundStatement, bind, format_explain, logical_plan, physical_plan};
use storage::StorageEngine;

use super::{
    BindSource, CapturedSnapshots, CopySnapshots, ExecutionContextInput, QueryService,
    QuerySessionContext, StatementClass, StatementRuntime, StreamOutcome, Transaction,
    TransactionSnapshots, WriteUnitGuard, classify_bound, dead_versions_in, exec_or_stream,
    mark_failed_on_error, prepared_schema_versions, run_plan, statement_class,
    transaction_control_is_irreversible,
};
use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;
use crate::registry::SnapshotExclusionGuard;

impl QueryService {
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
            if let Err(err) = session.cancel().check() {
                return (mark_failed_on_error(slot), default_isolation, Err(err));
            }
            let had_txn = slot.is_some();
            let durable = transaction_control_is_irreversible(kind, had_txn);
            let (slot, default_isolation, result) = self.handle_transaction_control(
                kind,
                slot,
                default_isolation,
                session.cancel(),
                session.gucs(),
            );
            let result = result.map(|result| {
                if durable {
                    StreamOutcome::Durable(result)
                } else {
                    StreamOutcome::Direct(result)
                }
            });
            return (slot, default_isolation, result);
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
            return (slot, default_isolation, result.map(StreamOutcome::Direct));
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
            return (
                None,
                default_isolation,
                self.run_maintenance(statement, session.cancel())
                    .map(StreamOutcome::Durable),
            );
        }

        // COPY is bound here (resolve table/columns) but not executed: it returns a
        // `BeginCopyIn`/`BeginCopyOut` request that the connection loop drives over
        // the COPY sub-protocol. Its snapshot is captured before binding and then
        // carried into the streaming driver so a schema rewrite cannot publish a
        // different relation generation between COPY bind and COPY execution.
        if let StatementClass::Copy(direction) = class {
            let (slot, default_isolation, result) = self.dispatch_copy(
                direction,
                statement,
                slot,
                default_isolation,
                session.cancel(),
            );
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
        cancel: &Arc<QueryCancel>,
    ) -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>) {
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
        let mut snapshots = match slot.as_mut() {
            Some(txn) => {
                // Capture before a first-write COPY acquires the writer guard. A
                // transaction that already held the guard may bypass a pending
                // schema-rewrite fence so it can finish and release that guard.
                let snapshots = match self.snapshots_for_transaction(txn, cancel.as_ref()) {
                    Ok(snapshots) => snapshots,
                    Err(err) => {
                        txn.failed = true;
                        return (slot, default_isolation, Err(err));
                    }
                };
                txn.first_statement_ran = true;
                CopySnapshots::Transaction(snapshots)
            }
            None => match self.capture_consistent_snapshots_cancelable(0, cancel.as_ref()) {
                Ok(snapshots) => CopySnapshots::Autocommit {
                    snapshots,
                    write_guard: None,
                },
                Err(err) => return (None, default_isolation, Err(err)),
            },
        };

        let bound = match bind(&statement, self.components.catalog.as_ref()) {
            Ok(bound) => bound,
            Err(err) => {
                if let Some(txn) = slot.as_mut() {
                    txn.failed = true;
                }
                return (slot, default_isolation, Err(err));
            }
        };
        let schema_versions =
            match prepared_schema_versions(&bound, self.components.catalog.as_ref()) {
                Ok(schema_versions) => schema_versions,
                Err(err) => {
                    if let Some(txn) = slot.as_mut() {
                        txn.failed = true;
                    }
                    return (slot, default_isolation, Err(err));
                }
            };
        {
            let relations = match &snapshots {
                CopySnapshots::Autocommit { snapshots, .. } => snapshots.relations.as_ref(),
                CopySnapshots::Transaction(snapshots) => snapshots.relations.as_ref(),
            };
            let allow_missing_tables = matches!(direction, CopyDirection::To);
            if let Err(err) = self.validate_relation_snapshot_schema_versions(
                relations,
                &schema_versions,
                allow_missing_tables,
            ) {
                if let Some(txn) = slot.as_mut() {
                    txn.failed = true;
                }
                return (slot, default_isolation, Err(err));
            }
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
        if matches!(direction, CopyDirection::From) {
            if let Some(txn) = slot.as_mut() {
                let acquired_guard = if txn.write_guard.is_none() {
                    if let Err(err) = self.acquire_write_guard(txn, cancel.as_ref()) {
                        txn.failed = true;
                        return (slot, default_isolation, Err(err));
                    }
                    true
                } else {
                    false
                };
                let relations = match &snapshots {
                    CopySnapshots::Autocommit { snapshots, .. } => snapshots.relations.as_ref(),
                    CopySnapshots::Transaction(snapshots) => snapshots.relations.as_ref(),
                };
                if let Err(err) = self
                    .validate_current_schema_versions(&schema_versions)
                    .and_then(|()| self.validate_relation_snapshot_current_for_write(relations))
                {
                    if acquired_guard {
                        txn.write_guard = None;
                    }
                    txn.failed = true;
                    return (slot, default_isolation, Err(err));
                }
            } else if let CopySnapshots::Autocommit {
                snapshots,
                write_guard,
            } = &mut snapshots
            {
                let guard = match self
                    .components
                    .concurrency
                    .begin_writer_cancelable(cancel.as_ref())
                {
                    Ok(guard) => WriteUnitGuard::Shared(guard),
                    Err(err) => return (slot, default_isolation, Err(err)),
                };
                if let Err(err) = self
                    .validate_current_schema_versions(&schema_versions)
                    .and_then(|()| {
                        self.validate_relation_snapshot_current_for_write(
                            snapshots.relations.as_ref(),
                        )
                    })
                {
                    drop(guard);
                    return (slot, default_isolation, Err(err));
                }
                *write_guard = Some(guard);
            }
        }
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
        let cancel = runtime.cancel.clone();
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

        // DDL is non-transactional (`docs/specs/mvcc.md` §4 Decision 6): reject it
        // inside an explicit transaction block. The transaction stays healthy and
        // open (this is a plain statement error, not a poisoning one in Postgres —
        // but per our semantics any statement error poisons the block; do that for
        // consistency with the 'E' gating contract).
        if matches!(class, StatementClass::Ddl) {
            txn.failed = true;
            return (
                Some(txn),
                Err(DbError::plan(
                    SqlState::FeatureNotSupported,
                    "DDL is not allowed inside a transaction block",
                )),
            );
        }

        let initial_class = class;
        let had_write_guard = txn.write_guard.is_some();
        // Data statements capture before waiting for a writer guard. Schema
        // rewrites fence new snapshot captures before waiting for the checkpoint
        // guard, so this ordering prevents a transaction from holding a writer
        // guard while blocked on the rewrite fence.
        let captured_snapshot =
            match self.snapshots_for_transaction(&mut txn, runtime.cancel.as_ref()) {
                Ok(snapshots) => snapshots,
                Err(err) => {
                    txn.failed = true;
                    return (Some(txn), Err(err));
                }
            };
        txn.first_statement_ran = true;

        let acquired_for_syntactic_write =
            matches!(initial_class, StatementClass::Write) && !had_write_guard;
        if acquired_for_syntactic_write
            && let Err(err) = self.acquire_write_guard(&mut txn, runtime.cancel.as_ref())
        {
            txn.failed = true;
            return (Some(txn), Err(err));
        }

        let prepared_schema_versions = match &source {
            BindSource::Unbound(_) => None,
            BindSource::Bound {
                schema_versions, ..
            } => Some(schema_versions.clone()),
        };
        if let Some(schema_versions) = prepared_schema_versions.as_deref()
            && let Err(err) = self.validate_prepared_schema_versions(schema_versions)
        {
            if acquired_for_syntactic_write {
                txn.write_guard = None;
            }
            txn.failed = true;
            return (Some(txn), Err(err));
        }

        let bound = match source {
            BindSource::Unbound(statement) => {
                match bind(&statement, self.components.catalog.as_ref()) {
                    Ok(bound) => bound,
                    Err(err) => {
                        if acquired_for_syntactic_write {
                            txn.write_guard = None;
                        }
                        txn.failed = true;
                        return (Some(txn), Err(err));
                    }
                }
            }
            BindSource::Bound { bound, .. } => bound,
        };
        let class = classify_bound(initial_class, &bound);
        let schema_versions = match prepared_schema_versions {
            Some(schema_versions) => schema_versions,
            None => match super::prepared_schema_versions(&bound, self.components.catalog.as_ref())
            {
                Ok(schema_versions) => schema_versions,
                Err(err) => {
                    if acquired_for_syntactic_write {
                        txn.write_guard = None;
                    }
                    txn.failed = true;
                    return (Some(txn), Err(err));
                }
            },
        };
        if let Err(err) = self.validate_relation_snapshot_schema_versions(
            captured_snapshot.relations.as_ref(),
            &schema_versions,
            matches!(class, StatementClass::Read),
        ) {
            if acquired_for_syntactic_write {
                txn.write_guard = None;
            }
            txn.failed = true;
            return (Some(txn), Err(err));
        }

        let is_write = matches!(class, StatementClass::Write);
        if is_write && txn.write_guard.is_none() {
            // A syntactic read can promote to a write after binding (for example
            // `SELECT nextval(...)`). The snapshot was already advertised before
            // bind, and schema rewrites wait for advertised snapshots before taking
            // the checkpoint guard, so acquiring the writer guard here does not
            // create a DDL deadlock.
            if let Err(err) = self.acquire_write_guard(&mut txn, runtime.cancel.as_ref()) {
                txn.failed = true;
                return (Some(txn), Err(err));
            }
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
        // §10 H3); CREATE INDEX (the other consumer) is non-transactional and rejected
        // inside an explicit block (above), so only UPDATE reads it here. A stale/smaller
        // horizon only prunes less, never unsafely (the prune reclaims only dead-to-all
        // versions and mutates one latched page).
        let gc_horizon = self.components.gc_horizon();
        // The writing xid is the innermost open savepoint's subxid (or `txn_id`),
        // and the live (sub)xid set is threaded for own-write/own-conflict detection
        // (`docs/specs/savepoints.md` §4).
        let result = (|| {
            let ctx = self.execution_context(ExecutionContextInput {
                txn_id: txn.writing_xid(),
                snapshot,
                relations,
                isolation: txn.isolation,
                gc_horizon,
                live_txns: txn.live_txns(),
                runtime,
            })?;

            // Only a read (a plain `SELECT`) streams; a write is materialized, so the
            // sink is withheld from `run_plan` for writes and the executor is never
            // asked to stream a DML plan.
            let read_sink = if is_write { None } else { sink };
            let result = run_plan(
                &self.engine,
                &ctx,
                bound,
                self.components.catalog.as_ref(),
                read_sink,
            );
            // The snapshot can no longer be used to read once `run_plan` has returned.
            drop(ctx);
            result
        })();
        // The snapshot can no longer be used to read once `run_plan` has returned;
        // drop the per-statement advertisement now (a no-op under Repeatable Read).
        drop(advertised);
        let result = result.and_then(|outcome| {
            cancel.check()?;
            Ok(outcome)
        });
        match result {
            Ok(outcome) => {
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
                (Some(txn), Ok(outcome))
            }
            Err(err) => {
                // Any statement error poisons the transaction: it enters 'E' and
                // must be ended with COMMIT/ROLLBACK. No partial-statement undo is
                // needed — the failed statement's versions stay invisible via the
                // CLOG (the transaction will be marked Aborted on ROLLBACK), and
                // abort is status-based, not before-image undo (`docs/specs/mvcc.md`
                // §4 Decision 3, Milestone D1).
                txn.failed = true;
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
                let captured =
                    self.capture_consistent_snapshots_cancelable(0, session.cancel().as_ref())?;
                let bound = bind(&statement, self.components.catalog.as_ref())?;
                match classify_bound(class, &bound) {
                    StatementClass::Read => self.autocommit_read_with_snapshot(
                        bound,
                        session.statement_runtime(
                            default_isolation,
                            default_isolation,
                            session.statement_timeout_ms(),
                        ),
                        sink,
                        captured,
                    ),
                    // A read promoted to a write (e.g. `SELECT nextval(...)`) is
                    // materialized, not streamed. Reuse the advertised snapshot so
                    // schema rewrites wait for this already-bound statement.
                    StatementClass::Write => self
                        .autocommit_prepared_bound_write_with_snapshot(
                            bound,
                            session.statement_runtime(
                                default_isolation,
                                default_isolation,
                                session.statement_timeout_ms(),
                            ),
                            None,
                            captured,
                        )
                        .map(StreamOutcome::Durable),
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
                .map(StreamOutcome::Durable),
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

    /// Execute a read-only statement (SELECT/EXPLAIN) lock-free: capture a
    /// snapshot under the registry latch and read via the buffer pool's per-frame
    /// latches. No `ConcurrencyController` guard is taken, so reads run
    /// concurrently with an in-flight writer (`docs/specs/mvcc.md` §7.1).
    pub(super) fn autocommit_read_with_snapshot(
        &self,
        bound: BoundStatement,
        runtime: StatementRuntime<'_>,
        sink: Option<&mut dyn RowSink>,
        captured: CapturedSnapshots,
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
        let ctx = self.execution_context(ExecutionContextInput {
            txn_id: 0,
            snapshot,
            relations,
            isolation: IsolationLevel::default(),
            gc_horizon: 0,
            live_txns: Arc::from([0]),
            runtime,
        })?;
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
    /// DML takes the SHARED writer guard (E2b, `docs/specs/mvcc.md` §10 E2b),
    /// running concurrently with other writers. DDL takes the EXCLUSIVE guard (like
    /// VACUUM): catalog rollback restores whole object maps, so no other DDL may
    /// commit between the rollback snapshot and restore; CREATE INDEX also needs a
    /// stable physical chain view with no concurrent writer (HOT updates) mutating a
    /// chain mid-scan, which its HOT broken-chain safety check requires
    /// (`docs/specs/mvcc.md` §10 Milestone H2). The GC horizon, threaded into the
    /// backfill for that check, is captured ONCE under the exclusive guard (so a
    /// writer cannot advance it mid-build), mirroring `run_vacuum`.
    pub(super) fn autocommit_write(
        &self,
        statement: Statement,
        runtime: StatementRuntime<'_>,
    ) -> Result<ExecutionResult> {
        let cancel = runtime.cancel.clone();
        if statement_is_syntactic_dml(&statement) {
            // Advertise the statement snapshot before waiting for the writer guard.
            // Schema rewrites fence snapshot capture before waiting for the
            // checkpoint guard; taking the snapshot first keeps data writers from
            // holding a writer guard while blocked on that fence.
            let captured = self.capture_consistent_snapshots_cancelable(0, cancel.as_ref())?;
            let guard = WriteUnitGuard::Shared(
                self.components
                    .concurrency
                    .begin_writer_cancelable(cancel.as_ref())?,
            );
            let bound = bind(&statement, self.components.catalog.as_ref())?;
            return self.autocommit_bound_write_with_guard(
                bound,
                guard,
                runtime,
                None,
                None,
                Some(captured),
            );
        }
        let schema_snapshot_guard = if statement_may_rewrite_table_storage(&statement) {
            Some(
                self.components
                    .active_txns
                    .begin_snapshot_exclusion_cancelable(cancel.as_ref())?,
            )
        } else {
            None
        };
        let needs_exclusive = statement_needs_exclusive_guard(&statement);
        let guard = if needs_exclusive {
            WriteUnitGuard::Exclusive(
                self.components
                    .concurrency
                    .begin_checkpoint_cancelable(cancel.as_ref())?,
            )
        } else {
            WriteUnitGuard::Shared(
                self.components
                    .concurrency
                    .begin_writer_cancelable(cancel.as_ref())?,
            )
        };
        let bound = bind(&statement, self.components.catalog.as_ref())?;
        self.autocommit_bound_write_with_guard(
            bound,
            guard,
            runtime,
            None,
            schema_snapshot_guard,
            None,
        )
    }

    pub(super) fn autocommit_prepared_bound_write(
        &self,
        bound: BoundStatement,
        runtime: StatementRuntime<'_>,
        prepared_schema_versions: Option<&[(TableId, u64)]>,
    ) -> Result<ExecutionResult> {
        let cancel = runtime.cancel.clone();
        if let Some(schema_versions) = prepared_schema_versions {
            self.validate_prepared_schema_versions(schema_versions)?;
        }
        if !bound_needs_exclusive_guard(&bound) {
            let captured = self.capture_consistent_snapshots_cancelable(0, cancel.as_ref())?;
            return self.autocommit_prepared_bound_write_with_snapshot(
                bound,
                runtime,
                prepared_schema_versions,
                captured,
            );
        }
        // Prepared statements are already bound; no catalog name resolution happens
        // here. Acquire the write/checkpoint guard before planning/execution.
        let schema_snapshot_guard =
            if bound_rewrites_table_storage(&bound, self.components.catalog.as_ref())? {
                Some(
                    self.components
                        .active_txns
                        .begin_snapshot_exclusion_cancelable(cancel.as_ref())?,
                )
            } else {
                None
            };
        let needs_exclusive = bound_needs_exclusive_guard(&bound);
        let guard = if needs_exclusive {
            WriteUnitGuard::Exclusive(
                self.components
                    .concurrency
                    .begin_checkpoint_cancelable(cancel.as_ref())?,
            )
        } else {
            WriteUnitGuard::Shared(
                self.components
                    .concurrency
                    .begin_writer_cancelable(cancel.as_ref())?,
            )
        };
        self.autocommit_bound_write_with_guard(
            bound,
            guard,
            runtime,
            prepared_schema_versions,
            schema_snapshot_guard,
            None,
        )
    }

    pub(super) fn autocommit_prepared_bound_write_with_snapshot(
        &self,
        bound: BoundStatement,
        runtime: StatementRuntime<'_>,
        prepared_schema_versions: Option<&[(TableId, u64)]>,
        captured: CapturedSnapshots,
    ) -> Result<ExecutionResult> {
        let guard = WriteUnitGuard::Shared(
            self.components
                .concurrency
                .begin_writer_cancelable(runtime.cancel.as_ref())?,
        );
        self.autocommit_bound_write_with_guard(
            bound,
            guard,
            runtime,
            prepared_schema_versions,
            None,
            Some(captured),
        )
    }

    fn autocommit_bound_write_with_guard(
        &self,
        bound: BoundStatement,
        guard: WriteUnitGuard,
        runtime: StatementRuntime<'_>,
        prepared_schema_versions: Option<&[(TableId, u64)]>,
        schema_snapshot_guard: Option<SnapshotExclusionGuard>,
        snapshot_override: Option<CapturedSnapshots>,
    ) -> Result<ExecutionResult> {
        let cancel = runtime.cancel.clone();
        if let Some(schema_versions) = prepared_schema_versions {
            self.validate_prepared_schema_versions(schema_versions)?;
        }
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        // The autocommit unit begins: allocate the transaction id and register it
        // active atomically (so a concurrent reader's snapshot is not torn). Its
        // CLOG status is `InProgress` implicitly until a `Commit`/`Abort` record
        // settles it.
        let txn_id = self.register_active_txn();
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
        let rewrite_snapshot = schema_snapshot_guard.is_some();
        let snapshots = (|| {
            if let Some(CapturedSnapshots {
                snapshot,
                relations,
                advertised,
            }) = snapshot_override
            {
                Ok((snapshot, relations, Some(advertised)))
            } else if rewrite_snapshot {
                Ok((
                    self.capture_unadvertised_snapshot(txn_id),
                    self.components.storage.capture_relation_snapshot()?,
                    None,
                ))
            } else {
                let CapturedSnapshots {
                    snapshot,
                    relations,
                    advertised,
                } = self.capture_consistent_snapshots_cancelable(txn_id, cancel.as_ref())?;
                Ok((snapshot, relations, Some(advertised)))
            }
        })();
        let (snapshot, relations, advertised) = match snapshots {
            Ok(snapshots) => snapshots,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, catalog_before);
                return Err(err);
            }
        };
        let schema_versions = match prepared_schema_versions {
            Some(schema_versions) => schema_versions.to_vec(),
            None => match super::prepared_schema_versions(&bound, self.components.catalog.as_ref())
            {
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
        // Capture the GC horizon. CREATE INDEX needs it for its broken-chain check
        // (captured AFTER the exclusive guard, so no writer can advance it, exactly as
        // `run_vacuum` does); an UPDATE needs it for the H3 update-path prune
        // (`docs/specs/mvcc.md` §10 H3). For an UPDATE under the SHARED writer guard a
        // concurrent writer/commit could advance the true horizon after this read, but
        // a stale/smaller horizon only prunes LESS — never unsafely — so capturing it
        // here (before execution) is sound. Other statements ignore it.
        let gc_horizon = self.components.gc_horizon();
        let ctx = match self.execution_context(ExecutionContextInput {
            txn_id,
            snapshot,
            relations,
            isolation: IsolationLevel::default(),
            gc_horizon,
            live_txns: Arc::from([txn_id]),
            runtime,
        }) {
            Ok(ctx) => ctx,
            Err(err) => {
                drop(advertised);
                self.rollback_pre_durable_or_die(txn_id, catalog_before);
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

        if let Err(err) = cancel.check() {
            self.rollback_pre_durable_or_die(txn_id, catalog_before);
            return Err(err);
        }

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
        drop(guard);
        drop(schema_snapshot_guard);

        // Account this committed statement's dead versions toward the auto-prune
        // threshold BEFORE the checkpoint trigger, so a checkpoint fired by this same
        // commit observes the updated count (`docs/specs/mvcc.md` §9, F4b). Only a
        // durable commit reaches here; an aborted statement returned above without
        // counting.
        self.components.add_dead_versions(dead_versions_in(&result));

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
            text: format_explain(&physical),
        })
    }
}

fn statement_needs_exclusive_guard(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable { .. }
            | Statement::DropTable { .. }
            | Statement::AlterTableAddColumn { .. }
            | Statement::AlterTableDropColumn { .. }
            | Statement::AlterTableRenameColumn { .. }
            | Statement::AlterTableRenameTable { .. }
            | Statement::CreateIndex { .. }
            | Statement::DropIndex { .. }
            | Statement::CreateSequence { .. }
            | Statement::DropSequence { .. }
            | Statement::CreateView { .. }
            | Statement::DropView { .. }
    )
}

fn statement_is_syntactic_dml(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. }
    )
}

fn statement_may_rewrite_table_storage(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::AlterTableAddColumn { .. } | Statement::AlterTableDropColumn { .. }
    )
}

fn bound_needs_exclusive_guard(bound: &BoundStatement) -> bool {
    bound_mutates_catalog(bound)
}

fn bound_rewrites_table_storage(
    bound: &BoundStatement,
    catalog: &dyn CatalogManager,
) -> Result<bool> {
    match bound {
        BoundStatement::AlterTableAddColumn {
            table,
            if_not_exists,
            column,
            ..
        } => preflight_add_column_rewrite_by_id(catalog, *table, *if_not_exists, column),
        BoundStatement::AlterTableDropColumn {
            table,
            if_exists,
            column,
            ..
        } => preflight_drop_column_rewrite_by_id(catalog, *table, *if_exists, column),
        _ => Ok(false),
    }
}

fn preflight_add_column_rewrite_by_id(
    catalog: &dyn CatalogManager,
    table: TableId,
    if_not_exists: bool,
    column: &common::ParsedColumnDef,
) -> Result<bool> {
    Ok(catalog
        .preflight_add_table_column(table, if_not_exists, column)?
        .rewrites_storage())
}

fn preflight_drop_column_rewrite_by_id(
    catalog: &dyn CatalogManager,
    table: TableId,
    if_exists: bool,
    column: &str,
) -> Result<bool> {
    Ok(catalog
        .preflight_drop_table_column(table, if_exists, column)?
        .rewrites_storage())
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::MemoryCatalog;
    use common::{CancelReason, DataType, ParsedColumnDef, SessionInfo, SessionSequenceState};

    use crate::app::AppState;

    use super::*;

    fn parsed_column(name: &str) -> ParsedColumnDef {
        ParsedColumnDef {
            name: name.to_string(),
            data_type: DataType::Integer,
            nullable: true,
            max_length: None,
            default: None,
            pg_type: None,
        }
    }

    #[test]
    fn conditional_add_drop_column_are_classified_as_potential_rewrites() {
        let add = Statement::AlterTableAddColumn {
            table: "t".to_string(),
            if_not_exists: true,
            column: parsed_column("c"),
        };
        assert!(statement_may_rewrite_table_storage(&add));

        let drop = Statement::AlterTableDropColumn {
            table: "t".to_string(),
            if_exists: true,
            column: "c".to_string(),
        };
        assert!(statement_may_rewrite_table_storage(&drop));
    }

    #[test]
    fn bound_conditional_add_drop_column_detect_actual_rewrites() {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "t".to_string(),
                vec![parsed_column("id")],
                Vec::new(),
                common::CompressionSetting::None,
            )
            .unwrap();
        let add = BoundStatement::AlterTableAddColumn {
            table: schema.id,
            table_name: "t".to_string(),
            if_not_exists: true,
            column: parsed_column("c"),
        };
        assert!(bound_rewrites_table_storage(&add, &catalog).unwrap());

        let drop = BoundStatement::AlterTableDropColumn {
            table: schema.id,
            table_name: "t".to_string(),
            if_exists: true,
            column: "c".to_string(),
        };
        assert!(!bound_rewrites_table_storage(&drop, &catalog).unwrap());
    }

    #[test]
    fn canceled_snapshot_capture_after_registration_rolls_back_xid() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        let statement = parser::parse("create table t (id integer primary key)").unwrap();
        let bound = bind(&statement, app.components.catalog.as_ref()).unwrap();
        let guard =
            WriteUnitGuard::Exclusive(app.components.concurrency.begin_checkpoint().unwrap());
        let cancel = Arc::new(QueryCancel::new());
        cancel.request(CancelReason::StatementTimeout);
        let runtime = StatementRuntime::new(
            &cancel,
            Arc::new(SessionSequenceState::new()),
            Arc::new(SessionInfo::default()),
        );

        let err = app
            .query_service
            .autocommit_bound_write_with_guard(bound, guard, runtime, None, None, None)
            .unwrap_err();

        assert_eq!(err.code, SqlState::QueryCanceled);
        assert!(app.components.active_txns.active_ids().is_empty());
    }
}
