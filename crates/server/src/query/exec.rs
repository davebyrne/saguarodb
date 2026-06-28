use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::{CopyDirection, DbError, IsolationLevel, Result, SqlState};
use executor::{CopyJob, ExecutionResult};
use parser::Statement;
use planner::{BoundStatement, bind, format_explain, logical_plan, physical_plan};

use super::{
    BindSource, QueryService, StatementClass, Transaction, WriteUnitGuard, dead_versions_in,
    mark_failed_on_error, run_plan, statement_class,
};
use crate::checkpoint::record_commit_and_maybe_checkpoint;

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
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
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
            return self.handle_transaction_control(kind, slot, default_isolation, cancel);
        }

        // Savepoints (SAVEPOINT / RELEASE / ROLLBACK TO) drive the session's
        // transaction lifecycle like transaction control; the op + name are read
        // from the parsed statement (`docs/specs/savepoints.md`).
        if let StatementClass::Savepoint = class {
            return self.handle_savepoint(statement, slot, default_isolation);
        }

        // VACUUM is a maintenance command: it does not bind/plan, and like DDL it is
        // forbidden inside an explicit transaction block (Postgres: "VACUUM cannot run
        // inside a transaction block"). Reject it with the open transaction poisoned to
        // the 'E' failed state, matching the DDL-in-block contract.
        if let StatementClass::Maintenance = class {
            if let Some(mut txn) = slot {
                txn.failed = true;
                return (
                    Some(txn),
                    default_isolation,
                    Err(DbError::plan(
                        SqlState::FeatureNotSupported,
                        "VACUUM cannot run inside a transaction block",
                    )),
                );
            }
            return (None, default_isolation, self.run_vacuum(statement));
        }

        // COPY is bound here (resolve table/columns) but not executed: it returns a
        // `BeginCopyIn`/`BeginCopyOut` request that the connection loop drives over
        // the COPY sub-protocol. The transaction slot passes through unchanged; the
        // COPY's own transaction work happens in the streaming driver.
        if let StatementClass::Copy(direction) = class {
            return self.dispatch_copy(direction, statement, slot, default_isolation);
        }

        match slot {
            // A data statement with an open explicit transaction runs inside it.
            Some(txn) => {
                let (slot, result) = self.run_in_transaction(txn, class, statement, cancel);
                (slot, default_isolation, result)
            }
            // No open transaction: this is an autocommit unit.
            None => {
                let result = self.run_autocommit(class, statement, cancel);
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
        let bound = match bind(&statement, self.components.catalog.as_ref()) {
            Ok(bound) => bound,
            Err(err) => return (mark_failed_on_error(slot), default_isolation, Err(err)),
        };
        let BoundStatement::Copy {
            table,
            columns,
            options,
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
            table,
            columns,
            options,
        };
        let result = match direction {
            CopyDirection::From => Ok(ExecutionResult::BeginCopyIn(job)),
            CopyDirection::To => Ok(ExecutionResult::BeginCopyOut(job)),
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
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
        self.run_bound_in_transaction(txn, class, BindSource::Unbound(statement), cancel)
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
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
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

        let bound = match source {
            BindSource::Unbound(statement) => {
                match bind(&statement, self.components.catalog.as_ref()) {
                    Ok(bound) => bound,
                    Err(err) => {
                        txn.failed = true;
                        return (Some(txn), Err(err));
                    }
                }
            }
            BindSource::Bound(bound) => bound,
        };

        let is_write = matches!(class, StatementClass::Write);
        if is_write && txn.write_guard.is_none() {
            // Lazily acquire the exclusive write guard on the first write of the
            // transaction; hold it for the whole write-transaction.
            if let Err(err) = self.acquire_write_guard(&mut txn) {
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
        let (snapshot, advertised) = self.snapshot_for_transaction(&mut txn);
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
        let ctx = self.execution_context(
            txn.writing_xid(),
            snapshot,
            txn.isolation,
            gc_horizon,
            txn.live_txns(),
            cancel,
        );

        let result = run_plan(&self.engine, &ctx, bound, self.components.catalog.as_ref());
        // The snapshot can no longer be used to read once `run_plan` has returned;
        // drop the per-statement advertisement now (a no-op under Repeatable Read).
        drop(ctx);
        drop(advertised);
        match result {
            Ok(result) => {
                // Accumulate this statement's dead-version count on the transaction
                // (`docs/specs/mvcc.md` §9, F4b). It is folded into the server-wide
                // auto-prune counter only when the transaction COMMITS durably; on
                // ROLLBACK it is discarded (the dead versions then belong to this
                // transaction's own aborted writes, not to committed deletes/updates).
                txn.dead_versions_pending = txn
                    .dead_versions_pending
                    .saturating_add(dead_versions_in(&result));
                (Some(txn), Ok(result))
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
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        let bound = bind(&statement, self.components.catalog.as_ref())?;
        match class {
            StatementClass::Read => self.autocommit_read(bound, cancel),
            StatementClass::Write | StatementClass::Ddl => self.autocommit_write(bound, cancel),
            // Maintenance (VACUUM) never reaches here: `dispatch` runs it via
            // `run_vacuum` before the autocommit data path.
            StatementClass::Maintenance => Err(DbError::internal(
                "maintenance reached the autocommit data path",
            )),
            // Transaction-control statements never reach here (dispatch routes
            // them through `handle_transaction_control`).
            StatementClass::TransactionControl(_) => Err(DbError::internal(
                "transaction control reached the autocommit data path",
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
    pub(super) fn autocommit_read(
        &self,
        bound: BoundStatement,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        if let BoundStatement::Explain(inner) = &bound {
            return self.explain(inner.as_ref());
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
        let (snapshot, _advertised) = self.capture_snapshot(0);
        // A read never runs CREATE INDEX (the only horizon consumer), so the horizon
        // is unused on this path; pass `0`.
        let ctx = self.execution_context(
            0,
            snapshot,
            IsolationLevel::default(),
            0,
            Arc::from([0]),
            cancel,
        );
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        self.engine.execute(&ctx, &physical)
    }

    /// Execute a write/DDL statement as an autocommit unit, committing durably on
    /// success and aborting on error.
    ///
    /// Most writes (and most DDL) take the SHARED writer guard (E2b,
    /// `docs/specs/mvcc.md` §10 E2b), running concurrently with other writers; only a
    /// checkpoint (the exclusive guard) excludes them. **CREATE INDEX is the
    /// exception:** it takes the EXCLUSIVE guard (like VACUUM) so its backfill sees a
    /// stable physical chain view with no concurrent writer (HOT updates) mutating a
    /// chain mid-scan, which its HOT broken-chain safety check requires
    /// (`docs/specs/mvcc.md` §10 Milestone H2). The GC horizon, threaded into the
    /// backfill for that check, is captured ONCE under the exclusive guard (so a
    /// writer cannot advance it mid-build), mirroring `run_vacuum`.
    pub(super) fn autocommit_write(
        &self,
        bound: BoundStatement,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        // CREATE INDEX takes the exclusive guard (stable chain view for the HOT
        // broken-chain check); every other write/DDL takes the shared writer guard.
        let needs_exclusive = matches!(bound, BoundStatement::CreateIndex { .. });
        let guard = if needs_exclusive {
            WriteUnitGuard::Exclusive(self.components.concurrency.begin_checkpoint()?)
        } else {
            WriteUnitGuard::Shared(self.components.concurrency.begin_writer()?)
        };
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        // The autocommit unit begins: allocate the transaction id and register it
        // active atomically (so a concurrent reader's snapshot is not torn). Its
        // CLOG status is `InProgress` implicitly until a `Commit`/`Abort` record
        // settles it.
        let txn_id = self.register_active_txn();
        let catalog_before = self.components.catalog.snapshot()?;
        // Capture the snapshot after registering, excluding the own id so own
        // writes are seen via the predicate's `current_txn` path. Advertise its
        // `xmin` to the GC horizon and hold `_advertised` across execution and the
        // commit/rollback that follow (`docs/specs/mvcc.md` §9): it lives until this
        // function returns on every path (success, statement error, panic), exactly
        // bracketing when the snapshot can still be used to read.
        let (snapshot, _advertised) = self.capture_snapshot(txn_id);
        // Capture the GC horizon. CREATE INDEX needs it for its broken-chain check
        // (captured AFTER the exclusive guard, so no writer can advance it, exactly as
        // `run_vacuum` does); an UPDATE needs it for the H3 update-path prune
        // (`docs/specs/mvcc.md` §10 H3). For an UPDATE under the SHARED writer guard a
        // concurrent writer/commit could advance the true horizon after this read, but
        // a stale/smaller horizon only prunes LESS — never unsafely — so capturing it
        // here (before execution) is sound. Other statements ignore it.
        let gc_horizon = self.components.gc_horizon();
        let ctx = self.execution_context(
            txn_id,
            snapshot,
            IsolationLevel::default(),
            gc_horizon,
            Arc::from([txn_id]),
            cancel,
        );

        let result = catch_unwind(AssertUnwindSafe(|| self.engine.execute(&ctx, &physical)));
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
                return Err(err);
            }
            Err(_) => {
                self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
                return Err(DbError::internal("statement execution panicked"));
            }
        };

        // An autocommit unit has no savepoints, so no committed subxids.
        if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
            self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
            return Err(err);
        }

        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        // The commit is durable and cleaned up; the CLOG already recorded it
        // `Committed` (set inside `wal.flush`). Drop it from the active set.
        self.components.active_txns.deregister(txn_id);
        drop(guard);

        // Account this committed statement's dead versions toward the auto-prune
        // threshold BEFORE the checkpoint trigger, so a checkpoint fired by this same
        // commit observes the updated count (`docs/specs/mvcc.md` §9, F4b). Only a
        // durable commit reaches here; an aborted statement returned above without
        // counting.
        self.components.add_dead_versions(dead_versions_in(&result));

        if let Err(err) = record_commit_and_maybe_checkpoint(&self.components) {
            eprintln!("checkpoint failed after committed statement: {err}");
        }

        Ok(result)
    }

    fn explain(&self, inner: &BoundStatement) -> Result<ExecutionResult> {
        if !matches!(inner, BoundStatement::Select(_)) {
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
