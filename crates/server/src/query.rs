use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{
    ColumnInfo, DataType, DbError, IsolationLevel, Result, Snapshot, SqlState, StatementContext,
    Value, WriteGuard,
};
use executor::{ExecutionContext, ExecutionResult, QueryEngine};
use parser::Statement;
use planner::{
    BoundStatement, bind, bind_parameterized, format_explain, logical_plan, physical_plan,
    substitute_params,
};
use storage::StorageEngine;
use wal::{WalRecord, WalRecordKind};

use crate::app::ServerComponents;
use crate::checkpoint::record_commit_and_maybe_checkpoint;

pub struct QueryService {
    components: Arc<ServerComponents>,
    engine: QueryEngine,
}

/// The transaction-block status a session reports to the protocol layer after a
/// statement runs. Mirrors PostgreSQL's `ReadyForQuery` status byte; the
/// connection translates it to `b'I'`/`b'T'`/`b'E'`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionTxnStatus {
    /// No transaction block is open (autocommit).
    Idle,
    /// A transaction block is open and healthy.
    InTransaction,
    /// A transaction block is open but failed; only COMMIT/ROLLBACK are accepted.
    Failed,
}

/// An open explicit transaction's runtime state, held on the connection `Session`
/// across statements (`docs/specs/mvcc.md` §7.2). It owns the exclusive write
/// guard (acquired lazily on the first write) for the whole write-transaction so
/// concurrent readers stay lock-free while at most one writer runs.
pub struct Transaction {
    txn_id: u64,
    isolation: IsolationLevel,
    /// `true` once any statement has entered the `Failed` ('E') state. While set,
    /// every statement except COMMIT/ROLLBACK is rejected with `25P02`.
    failed: bool,
    /// The exclusive write guard, acquired lazily on the first write statement and
    /// held until COMMIT/ROLLBACK. A read-only transaction never acquires it.
    write_guard: Option<WriteGuard>,
    /// The Repeatable Read snapshot: captured once at the first statement and
    /// reused. `None` under Read Committed (a fresh snapshot is captured per
    /// statement).
    rr_snapshot: Option<Arc<Snapshot>>,
}

impl Transaction {
    fn status(&self) -> SessionTxnStatus {
        if self.failed {
            SessionTxnStatus::Failed
        } else {
            SessionTxnStatus::InTransaction
        }
    }
}

impl QueryService {
    pub fn new(components: Arc<ServerComponents>) -> Self {
        Self {
            components,
            engine: QueryEngine,
        }
    }

    /// Execute a simple-protocol SQL string against the session's transaction
    /// `slot`, returning the (possibly mutated) slot alongside the result. The
    /// slot carries the open explicit transaction across statements; autocommit
    /// statements run with `slot == None`.
    pub fn execute_simple(
        &self,
        sql: &str,
        slot: Option<Transaction>,
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
        let parsed = match parser::parse(sql) {
            Ok(parsed) => parsed,
            // A syntax error inside an open transaction poisons the block to the
            // failed state, matching PostgreSQL (the block must be ended before any
            // further command is accepted). Autocommit (`None`) is unaffected.
            Err(err) => return (mark_failed_on_error(slot), Err(err)),
        };
        self.dispatch(parsed, slot, cancel)
    }

    /// Backwards-compatible autocommit entry point: run one SQL string with no
    /// surrounding transaction. Used by the prepared-statement path and by tests.
    pub fn execute_sql(&self, sql: &str) -> Result<ExecutionResult> {
        self.execute_sql_cancelable(sql, &AtomicBool::new(false))
    }

    /// Like `execute_sql`, but aborts with `QueryCanceled` if `cancel` becomes
    /// set (from another connection's `CancelRequest`) while the query runs. This
    /// is the autocommit path: no transaction is carried across the call.
    pub fn execute_sql_cancelable(
        &self,
        sql: &str,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        let (_slot, result) = self.execute_simple(sql, None, cancel);
        result
    }

    /// Parse and bind a (possibly parameterized) statement for the extended
    /// query protocol, resolving parameter types from the declared OIDs or by
    /// inference. The result can be executed repeatedly with different values.
    pub fn prepare_sql(
        &self,
        sql: &str,
        declared_param_types: &[Option<DataType>],
    ) -> Result<PreparedStatement> {
        let statement = parser::parse(sql)?;
        let class = statement_class(&statement)?;
        if let StatementClass::TransactionControl(_) = class {
            // BEGIN/COMMIT/ROLLBACK take no parameters and produce no rows; they do
            // not bind. Carry the prepared statement with a no-op bound payload so
            // an extended-protocol `Execute` can route it through the session's
            // transaction lifecycle (`handle_transaction_control`) exactly like the
            // simple-query path, rather than as an independent autocommit unit.
            return Ok(PreparedStatement {
                class,
                bound: None,
                param_types: Vec::new(),
                result_columns: None,
            });
        }
        let (bound, param_types) = bind_parameterized(
            &statement,
            self.components.catalog.as_ref(),
            declared_param_types,
        )?;
        let result_columns = result_columns(&bound);
        Ok(PreparedStatement {
            class,
            bound: Some(bound),
            param_types,
            result_columns,
        })
    }

    /// Execute a prepared statement with one value per parameter, in order. Each
    /// call is its own autocommit unit, like a simple query.
    pub fn execute_prepared(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<ExecutionResult> {
        self.execute_prepared_cancelable(prepared, params, &AtomicBool::new(false))
    }

    /// Like `execute_prepared`, but cancelable mid-flight via `cancel`. Runs as an
    /// autocommit unit: the caller has no open explicit transaction (the session's
    /// transaction slot is `None`), so each `Execute` is its own implicit
    /// `BEGIN…COMMIT`. When a transaction IS open, the connection routes through
    /// `execute_prepared_in_session` instead, so the autocommit write path here is
    /// never reached while the session already holds the write guard.
    pub fn execute_prepared_cancelable(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        let bound = self.substitute_prepared_params(prepared, params)?;
        match prepared.class {
            StatementClass::Read => self.autocommit_read(bound, cancel),
            StatementClass::Write | StatementClass::Ddl => self.autocommit_write(bound, cancel),
            StatementClass::TransactionControl(_) => Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "transaction control statements require the simple query protocol",
            )),
        }
    }

    /// Execute a prepared statement against the session's open explicit
    /// transaction `slot`, returning the (possibly mutated) slot alongside the
    /// result. This is the extended-protocol counterpart of `execute_simple`: it
    /// routes a data statement through the SAME in-transaction machinery the simple
    /// path uses (`run_bound_in_transaction`), so the open transaction's single
    /// write guard is reused — never re-acquired — and the transaction's
    /// snapshot/isolation and 'E' failed-state gating apply. Transaction-control
    /// statements are dispatched through `handle_transaction_control`, exactly like
    /// a simple `BEGIN`/`COMMIT`/`ROLLBACK`.
    ///
    /// Precondition: `slot` is `Some` (the connection only calls this with an open
    /// transaction; with no open transaction it uses the autocommit
    /// `execute_prepared_cancelable`).
    pub fn execute_prepared_in_session(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        slot: Option<Transaction>,
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
        if let StatementClass::TransactionControl(kind) = prepared.class {
            return self.handle_transaction_control(kind, slot, cancel);
        }

        let bound = match self.substitute_prepared_params(prepared, params) {
            Ok(bound) => bound,
            // A parameter-count/substitution error inside an open transaction
            // poisons it to the failed state, matching the simple-query path.
            Err(err) => return (mark_failed_on_error(slot), Err(err)),
        };

        match slot {
            Some(txn) => {
                self.run_bound_in_transaction(txn, prepared.class, BindSource::Bound(bound), cancel)
            }
            // No open transaction: fall back to an autocommit unit (the connection
            // routes here only when a transaction is open, but keep this total so
            // the contract holds regardless of caller).
            None => {
                let result = match prepared.class {
                    StatementClass::Read => self.autocommit_read(bound, cancel),
                    StatementClass::Write | StatementClass::Ddl => {
                        self.autocommit_write(bound, cancel)
                    }
                    StatementClass::TransactionControl(_) => {
                        unreachable!("transaction control is dispatched above before substitution")
                    }
                };
                (None, result)
            }
        }
    }

    /// Validate the parameter count and substitute `params` into a prepared
    /// statement's bound payload. Transaction-control statements carry no bound
    /// payload, so substitution is only valid for data statements.
    fn substitute_prepared_params(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<BoundStatement> {
        if params.len() != prepared.param_types.len() {
            return Err(DbError::protocol(
                SqlState::SyntaxError,
                format!(
                    "prepared statement requires {} parameter(s), but {} were supplied",
                    prepared.param_types.len(),
                    params.len()
                ),
            ));
        }
        let bound = prepared.bound.as_ref().ok_or_else(|| {
            DbError::internal("prepared transaction-control statement has no bound payload")
        })?;
        substitute_params(bound, params)
    }
}

impl QueryService {
    /// Route a parsed simple-query statement through the transaction lifecycle.
    fn dispatch(
        &self,
        statement: Statement,
        slot: Option<Transaction>,
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
        let class = match statement_class(&statement) {
            Ok(class) => class,
            Err(err) => {
                // A parse/classification error inside an open transaction still
                // poisons it to the failed state (matching Postgres).
                let slot = mark_failed_on_error(slot);
                return (slot, Err(err));
            }
        };

        if let StatementClass::TransactionControl(kind) = class {
            return self.handle_transaction_control(kind, slot, cancel);
        }

        match slot {
            // A data statement with an open explicit transaction runs inside it.
            Some(txn) => self.run_in_transaction(txn, class, statement, cancel),
            // No open transaction: this is an autocommit unit.
            None => {
                let result = self.run_autocommit(class, statement, cancel);
                (None, result)
            }
        }
    }

    /// Handle BEGIN/COMMIT/ROLLBACK against the session's transaction `slot`.
    fn handle_transaction_control(
        &self,
        kind: TransactionControl,
        slot: Option<Transaction>,
        _cancel: &AtomicBool,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
        match kind {
            TransactionControl::Begin => match slot {
                // Postgres: BEGIN inside a transaction is a warning + no-op that
                // stays 'T'. We keep the open transaction and report success.
                Some(txn) => (Some(txn), Ok(begin_complete())),
                None => match self.begin_transaction() {
                    Ok(txn) => (Some(txn), Ok(begin_complete())),
                    Err(err) => (None, Err(err)),
                },
            },
            TransactionControl::Commit => match slot {
                // COMMIT of a healthy transaction commits durably.
                Some(txn) if !txn.failed => {
                    let result = self.commit_transaction(txn).map(|()| commit_complete());
                    (None, result)
                }
                // COMMIT of a failed transaction issues ROLLBACK (Postgres
                // behavior), returning to Idle.
                Some(txn) => {
                    self.abort_transaction(txn);
                    // Postgres tags this `ROLLBACK`, the actual action taken.
                    (None, Ok(rollback_complete()))
                }
                // COMMIT with no open transaction is a no-op warning, stays Idle.
                None => (None, Ok(commit_complete())),
            },
            TransactionControl::Rollback => match slot {
                Some(txn) => {
                    self.abort_transaction(txn);
                    (None, Ok(rollback_complete()))
                }
                // ROLLBACK with no open transaction is a no-op warning, stays Idle.
                None => (None, Ok(rollback_complete())),
            },
        }
    }

    /// Allocate a transaction id, register it active, and build the explicit
    /// transaction. The write guard is acquired lazily on the first write.
    fn begin_transaction(&self) -> Result<Transaction> {
        let txn_id = self.register_active_txn();
        Ok(Transaction {
            txn_id,
            isolation: IsolationLevel::default(),
            failed: false,
            write_guard: None,
            rr_snapshot: None,
        })
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
    fn run_bound_in_transaction(
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

        let snapshot = self.snapshot_for_transaction(&mut txn);
        let ctx = self.execution_context(txn.txn_id, snapshot, txn.isolation, cancel);

        let result = run_plan(&self.engine, &ctx, bound, self.components.catalog.as_ref());
        match result {
            Ok(result) => (Some(txn), Ok(result)),
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

    /// Acquire the exclusive write guard for an explicit transaction's first
    /// write, holding it on `txn` for the whole write-transaction.
    ///
    /// Reentrancy tripwire: a connection must acquire the write guard AT MOST ONCE.
    /// `parking_lot::RwLock` is non-reentrant, so re-acquiring a held guard would
    /// block this connection forever (and wedge every other writer, since the guard
    /// is process-wide). The routing in this module guarantees a single acquire per
    /// connection; this assertion converts a would-be self-deadlock from a future
    /// regression into a clear error instead of a silent hang. It does NOT weaken
    /// writer-vs-writer serialization: a *different* connection's writer (whose
    /// `txn` has no guard) still blocks and waits inside `begin_write`.
    fn acquire_write_guard(&self, txn: &mut Transaction) -> Result<()> {
        if txn.write_guard.is_some() {
            debug_assert!(
                false,
                "reentrant write-guard acquisition: this transaction already holds \
                 the exclusive write guard"
            );
            return Err(DbError::internal(
                "reentrant write-guard acquisition (transaction already holds the write guard)",
            ));
        }
        // Only reached when this transaction holds no guard, so the blocking acquire
        // can wait only on another connection's writer, never on ourselves.
        let guard = self.components.concurrency.begin_write()?;
        txn.write_guard = Some(guard);
        Ok(())
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
            // Transaction-control statements never reach here (dispatch routes
            // them through `handle_transaction_control`).
            StatementClass::TransactionControl(_) => Err(DbError::internal(
                "transaction control reached the autocommit data path",
            )),
        }
    }

    /// Execute a read-only statement (SELECT/EXPLAIN) lock-free: capture a
    /// snapshot under the registry latch and read via the buffer pool's per-frame
    /// latches. No `ConcurrencyController` guard is taken, so reads run
    /// concurrently with an in-flight writer (`docs/specs/mvcc.md` §7.1).
    fn autocommit_read(
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
        let snapshot = self.capture_snapshot(0);
        let ctx = self.execution_context(0, snapshot, IsolationLevel::default(), cancel);
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        self.engine.execute(&ctx, &physical)
    }

    /// Execute a write/DDL statement as an autocommit unit under the exclusive
    /// write guard, committing durably on success and aborting on error.
    fn autocommit_write(
        &self,
        bound: BoundStatement,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        let guard = self.components.concurrency.begin_write()?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        // The autocommit unit begins: allocate the transaction id and register it
        // active atomically (so a concurrent reader's snapshot is not torn). Its
        // CLOG status is `InProgress` implicitly until a `Commit`/`Abort` record
        // settles it.
        let txn_id = self.register_active_txn();
        let catalog_before = self.components.catalog.snapshot()?;
        // Capture the snapshot after registering, excluding the own id so own
        // writes are seen via the predicate's `current_txn` path.
        let snapshot = self.capture_snapshot(txn_id);
        let ctx = self.execution_context(txn_id, snapshot, IsolationLevel::default(), cancel);

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

        if let Err(err) = self.append_and_flush_commit(txn_id) {
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

        if let Err(err) = record_commit_and_maybe_checkpoint(&self.components) {
            eprintln!("checkpoint failed after committed statement: {err}");
        }

        Ok(result)
    }

    /// Commit an explicit transaction: append a `Commit` record, flush (fsync),
    /// set `CLOG=Committed` (done at flush), run post-durable-commit cleanup, and
    /// deregister. Releasing the write guard happens when `txn` is dropped after
    /// this returns.
    fn commit_transaction(&self, txn: Transaction) -> Result<()> {
        let txn_id = txn.txn_id;
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
    fn abort_transaction(&self, txn: Transaction) {
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

    /// Allocate the next transaction id and register it active atomically under
    /// the registry latch (`docs/specs/mvcc.md` §7.1), so a concurrent reader's
    /// snapshot capture never observes the advanced allocator boundary without
    /// also observing this transaction in `xip`.
    fn register_active_txn(&self) -> u64 {
        self.components
            .active_txns
            .register_allocated(|| self.components.next_txn_id.fetch_add(1, Ordering::AcqRel))
    }

    /// The snapshot a statement of `txn` reads with, per isolation level
    /// (`docs/specs/mvcc.md` §6): Read Committed captures a fresh snapshot each
    /// statement (seeing other transactions' commits between statements);
    /// Repeatable Read captures one snapshot at the first statement and reuses it.
    fn snapshot_for_transaction(&self, txn: &mut Transaction) -> Arc<Snapshot> {
        match txn.isolation {
            IsolationLevel::ReadCommitted => self.capture_snapshot(txn.txn_id),
            IsolationLevel::RepeatableRead => {
                if let Some(snapshot) = &txn.rr_snapshot {
                    snapshot.clone()
                } else {
                    let snapshot = self.capture_snapshot(txn.txn_id);
                    txn.rr_snapshot = Some(snapshot.clone());
                    snapshot
                }
            }
        }
    }

    fn execution_context<'a>(
        &'a self,
        txn_id: u64,
        snapshot: Arc<Snapshot>,
        isolation: IsolationLevel,
        cancel: &'a AtomicBool,
    ) -> ExecutionContext<'a> {
        ExecutionContext {
            statement: StatementContext::with_snapshot_and_isolation(txn_id, snapshot, isolation),
            catalog: self.components.catalog.as_ref(),
            storage: self.components.storage.as_ref(),
            schema_ops: self.components.storage.as_ref(),
            cancel,
        }
    }

    /// Capture a visibility snapshot consistently with the active-transaction
    /// registry and the id allocator (`docs/specs/mvcc.md` §5.5, §7.1). Held under
    /// the registry's brief latch (via `active_snapshot`) so the snapshot is not
    /// torn relative to `next_txn_id`:
    ///
    /// - `xmax` is the next id to be assigned; every already-allocated id is below
    ///   it (read after the latched active set so no concurrently-begun writer is
    ///   missed from `xip`).
    /// - `xip` is the currently-active set minus `own_txn` (own writes are seen via
    ///   the predicate's own-write path, not as in-progress). A read passes
    ///   `own_txn = 0`; nothing is excluded.
    /// - `xmin` is the oldest active id, or `xmax` if none are active.
    ///
    /// Returned behind an `Arc` so the executor shares it across scan operators
    /// rather than deep-cloning the `xip` vector per operator.
    fn capture_snapshot(&self, own_txn: u64) -> Arc<Snapshot> {
        // Snapshot the active set and the allocator boundary under one latch so a
        // concurrent BEGIN cannot slip a new writer between reading `xmax` and
        // reading `xip`. Reading `next_txn_id` first, then the active set, would
        // risk a writer that registered after the `xmax` read being both `>= xmax`
        // (so excluded as "future") and absent from `xip` — but visible. Reading
        // the active set first guarantees any active id is reflected in `xip`, and
        // `xmax` taken after only grows, so every active id stays `< xmax`.
        let (active, xmax) = self
            .components
            .active_txns
            .snapshot_with_boundary(|| self.components.next_txn_id.load(Ordering::Acquire));
        let xip: Vec<u64> = active.iter().copied().filter(|&id| id != own_txn).collect();
        let xmin = active.iter().copied().next().unwrap_or(xmax);
        Arc::new(Snapshot { xmin, xmax, xip })
    }

    fn append_and_flush_commit(&self, txn_id: u64) -> Result<()> {
        self.components.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::Commit,
        })?;
        self.components.wal.flush()?;
        Ok(())
    }

    fn rollback_pre_durable_or_die(
        &self,
        txn_id: u64,
        catalog_before: Option<catalog::CatalogSnapshot>,
    ) {
        if let Err(rollback_err) = self.rollback_pre_durable(txn_id, catalog_before) {
            self.fatal_pre_durable_rollback_failure(rollback_err);
        }
    }

    fn rollback_pre_durable(
        &self,
        txn_id: u64,
        catalog_before: Option<catalog::CatalogSnapshot>,
    ) -> Result<()> {
        // Record the abort: append an `Abort` record (which sets the CLOG to
        // `Aborted`) and drop the transaction from the active set. The abort is not
        // fsynced here — a transaction with no durable `Commit` is recovered as
        // aborted regardless (redo-all + in-flight = aborted, `docs/specs/mvcc.md`
        // §8). The unflushed `Abort` is still durable by the next checkpoint, whose
        // `wal.flush` makes it so before truncation, where it pins conservative WAL
        // truncation (an aborted txn's flushed pages must stay hidden across a
        // checkpoint, §5.4). A failure to append it is logged but not fatal: the
        // txn is still recovered as aborted.
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

    fn cleanup_after_durable_commit(&self, txn_id: u64) -> Result<()> {
        self.components.storage.commit_txn(txn_id)?;
        self.components.buffer_pool.commit(txn_id)?;
        Ok(())
    }

    fn fatal_after_durable_commit(&self, err: DbError) -> ! {
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

/// Abort and discard a transaction held on the session, e.g. when a client
/// disconnects mid-transaction. Releases the write guard and clears the registry
/// entry so neither is leaked. Standalone so the connection layer can call it on
/// disconnect without holding a `&QueryService` borrow across the blocking task.
pub fn abort_session_transaction(components: &Arc<ServerComponents>, txn: Transaction) {
    let service = QueryService::new(components.clone());
    service.abort_transaction(txn);
}

/// The session-facing status of a transaction slot after a statement.
pub fn slot_status(slot: &Option<Transaction>) -> SessionTxnStatus {
    match slot {
        Some(txn) => txn.status(),
        None => SessionTxnStatus::Idle,
    }
}

/// Plan and execute a fully bound data statement under `ctx`.
fn run_plan(
    engine: &QueryEngine,
    ctx: &ExecutionContext<'_>,
    bound: BoundStatement,
    catalog: &dyn catalog::CatalogManager,
) -> Result<ExecutionResult> {
    if let BoundStatement::Explain(inner) = &bound {
        if !matches!(inner.as_ref(), BoundStatement::Select(_)) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                "EXPLAIN supports SELECT only in v1",
            ));
        }
        let logical = logical_plan(inner.as_ref())?;
        let physical = physical_plan(&logical, catalog)?;
        return Ok(ExecutionResult::Explanation {
            text: format_explain(&physical),
        });
    }
    let logical = logical_plan(&bound)?;
    let physical = physical_plan(&logical, catalog)?;
    let result = catch_unwind(AssertUnwindSafe(|| engine.execute(ctx, &physical)));
    match result {
        Ok(result) => result,
        Err(_) => Err(DbError::internal("statement execution panicked")),
    }
}

/// Poison an open transaction's slot to the failed state on a statement error
/// (parse/classification before the lifecycle handler runs). Autocommit
/// (`None`) is unaffected.
fn mark_failed_on_error(slot: Option<Transaction>) -> Option<Transaction> {
    slot.map(|mut txn| {
        txn.failed = true;
        txn
    })
}

fn begin_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "BEGIN".to_string(),
        count: 0,
    }
}

fn commit_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "COMMIT".to_string(),
        count: 0,
    }
}

fn rollback_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "ROLLBACK".to_string(),
        count: 0,
    }
}

/// The statement supplied to the in-transaction execution path: either an
/// unbound AST (simple query, bound here against the live catalog) or an
/// already-bound statement (extended-protocol `Execute`, with its parameters
/// already substituted).
enum BindSource {
    Unbound(Statement),
    Bound(BoundStatement),
}

#[derive(Clone, Copy)]
enum TransactionControl {
    Begin,
    Commit,
    Rollback,
}

#[derive(Clone, Copy)]
enum StatementClass {
    Read,
    Write,
    Ddl,
    TransactionControl(TransactionControl),
}

/// A parsed and bound extended-protocol statement that can be executed
/// repeatedly with different parameter values. `bound` is `None` only for
/// transaction-control statements (BEGIN/COMMIT/ROLLBACK), which carry no bound
/// payload and are dispatched through the session's transaction lifecycle.
pub struct PreparedStatement {
    class: StatementClass,
    bound: Option<BoundStatement>,
    param_types: Vec<DataType>,
    result_columns: Option<Vec<ColumnInfo>>,
}

impl PreparedStatement {
    /// Resolved parameter types, by position.
    pub fn param_types(&self) -> &[DataType] {
        &self.param_types
    }

    /// Whether this is a transaction-control statement (BEGIN/COMMIT/ROLLBACK).
    /// The connection routes such an `Execute` through the session's transaction
    /// lifecycle even with no transaction open, so it drives `Session.txn` rather
    /// than running as an autocommit unit.
    pub fn is_transaction_control(&self) -> bool {
        matches!(self.class, StatementClass::TransactionControl(_))
    }

    /// Result column metadata, or `None` for a statement that returns no rows.
    pub fn result_columns(&self) -> Option<&[ColumnInfo]> {
        self.result_columns.as_deref()
    }
}

fn result_columns(bound: &BoundStatement) -> Option<Vec<ColumnInfo>> {
    match bound {
        BoundStatement::Select(select) => Some(select.output_schema.clone()),
        BoundStatement::Explain(_) => Some(vec![ColumnInfo {
            name: "QUERY PLAN".to_string(),
            data_type: DataType::Text,
            table_id: None,
            column_id: None,
        }]),
        _ => None,
    }
}

fn statement_class(statement: &Statement) -> Result<StatementClass> {
    match statement {
        Statement::Select(_) => Ok(StatementClass::Read),
        Statement::Explain(inner) => match inner.as_ref() {
            Statement::Select(_) => Ok(StatementClass::Read),
            _ => Err(DbError::plan(
                SqlState::SyntaxError,
                "EXPLAIN supports SELECT only in v1",
            )),
        },
        Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
            Ok(StatementClass::Write)
        }
        Statement::CreateTable { .. }
        | Statement::DropTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::DropIndex { .. } => Ok(StatementClass::Ddl),
        Statement::Begin => Ok(StatementClass::TransactionControl(
            TransactionControl::Begin,
        )),
        Statement::Commit => Ok(StatementClass::TransactionControl(
            TransactionControl::Commit,
        )),
        Statement::Rollback => Ok(StatementClass::TransactionControl(
            TransactionControl::Rollback,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;

    use catalog::CatalogSnapshot;
    use common::{DataType, SqlState, Value};

    use super::SessionTxnStatus;
    use crate::app::AppState;

    #[tokio::test]
    async fn execute_sql_aborts_when_cancellation_requested() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id) values (1)")
            .unwrap();

        let cancel = AtomicBool::new(true);
        let err = app
            .query_service
            .execute_sql_cancelable("select id from users", &cancel)
            .unwrap_err();
        assert_eq!(err.code, SqlState::QueryCanceled);
    }

    #[tokio::test]
    async fn begin_insert_select_commit_is_visible_to_a_later_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let cancel = AtomicBool::new(false);
        // BEGIN; INSERT; SELECT (sees own insert); COMMIT;
        let (slot, result) = app.query_service.execute_simple("begin", None, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);

        let (slot, result) = app.query_service.execute_simple(
            "insert into users (id, name) values (1, 'Ada')",
            slot,
            &cancel,
        );
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);

        let (slot, result) =
            app.query_service
                .execute_simple("select id from users", slot, &cancel);
        let rows = match result.unwrap() {
            executor::ExecutionResult::Query { rows, .. } => rows,
            other => panic!("expected query, got {other:?}"),
        };
        assert_eq!(rows.len(), 1, "the open transaction sees its own insert");

        let (slot, result) = app.query_service.execute_simple("commit", slot, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Idle);
        assert!(slot.is_none());

        // A fresh autocommit SELECT now sees the committed row.
        let result = app
            .query_service
            .execute_sql("select id from users")
            .unwrap();
        assert_eq!(result.row_count(), 1);
        assert!(app.components.active_txns.active_ids().is_empty());
    }

    #[tokio::test]
    async fn begin_insert_rollback_is_not_visible() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, result) = app.query_service.execute_simple("begin", None, &cancel);
        result.unwrap();
        let (slot, result) = app.query_service.execute_simple(
            "insert into users (id, name) values (1, 'Ada')",
            slot,
            &cancel,
        );
        result.unwrap();
        let (slot, result) = app.query_service.execute_simple("rollback", slot, &cancel);
        result.unwrap();
        assert!(slot.is_none());

        let result = app
            .query_service
            .execute_sql("select id from users")
            .unwrap();
        assert_eq!(result.row_count(), 0, "rolled-back insert is invisible");
        assert!(app.components.active_txns.active_ids().is_empty());
    }

    #[tokio::test]
    async fn failed_statement_enters_e_state_and_rejects_until_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, result) = app.query_service.execute_simple("begin", None, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);

        // A statement against a missing table errors and poisons the txn to 'E'.
        let (slot, result) =
            app.query_service
                .execute_simple("select id from ghosts", slot, &cancel);
        assert!(result.is_err());
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);

        // While 'E', every statement but COMMIT/ROLLBACK is rejected with 25P02.
        let (slot, result) =
            app.query_service
                .execute_simple("select id from users", slot, &cancel);
        let err = result.unwrap_err();
        assert_eq!(err.code, SqlState::InFailedSqlTransaction);
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);

        // ROLLBACK returns to Idle.
        let (slot, result) = app.query_service.execute_simple("rollback", slot, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Idle);
        assert!(app.components.active_txns.active_ids().is_empty());
    }

    #[tokio::test]
    async fn commit_of_failed_transaction_rolls_back() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, _) = app.query_service.execute_simple("begin", None, &cancel);
        let (slot, _) =
            app.query_service
                .execute_simple("insert into users (id) values (1)", slot, &cancel);
        let (slot, result) =
            app.query_service
                .execute_simple("select id from ghosts", slot, &cancel);
        assert!(result.is_err());
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);

        // COMMIT of an aborted transaction issues ROLLBACK (Postgres behavior).
        let (slot, result) = app.query_service.execute_simple("commit", slot, &cancel);
        result.unwrap();
        assert!(slot.is_none());

        // The insert was rolled back: nothing committed.
        let result = app
            .query_service
            .execute_sql("select id from users")
            .unwrap();
        assert_eq!(result.row_count(), 0);
    }

    #[tokio::test]
    async fn ddl_inside_transaction_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, _) = app.query_service.execute_simple("begin", None, &cancel);
        let (slot, result) = app.query_service.execute_simple(
            "create table users (id integer primary key)",
            slot,
            &cancel,
        );
        let err = result.unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);
        let (_slot, result) = app.query_service.execute_simple("rollback", slot, &cancel);
        result.unwrap();
    }

    #[tokio::test]
    async fn commit_and_rollback_with_no_open_transaction_are_no_ops() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, result) = app.query_service.execute_simple("commit", None, &cancel);
        result.unwrap();
        assert!(slot.is_none());
        let (slot, result) = app.query_service.execute_simple("rollback", None, &cancel);
        result.unwrap();
        assert!(slot.is_none());
    }

    #[tokio::test]
    async fn begin_inside_transaction_is_a_noop_warning_staying_in_t() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, _) = app.query_service.execute_simple("begin", None, &cancel);
        let txn_id_before = app.components.active_txns.active_ids();
        let (slot, result) = app.query_service.execute_simple("begin", slot, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);
        // The second BEGIN did not allocate a new transaction.
        assert_eq!(app.components.active_txns.active_ids(), txn_id_before);
        let (_slot, _) = app.query_service.execute_simple("rollback", slot, &cancel);
    }

    #[tokio::test]
    async fn canceled_write_aborts_and_does_not_commit() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id) values (1)")
            .unwrap();

        let cancel = AtomicBool::new(true);
        let err = app
            .query_service
            .execute_sql_cancelable("insert into users (id) values (2)", &cancel)
            .unwrap_err();
        assert_eq!(err.code, SqlState::QueryCanceled);

        // The canceled write rolled back: the second row was never committed.
        let result = app
            .query_service
            .execute_sql("select id from users")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn failed_write_rolls_back_buffer_and_does_not_commit() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let err = app
            .query_service
            .execute_sql("insert into users (id, name) values (1, 'Duplicate')")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);

        let result = app
            .query_service
            .execute_sql("select id, name from users")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn create_index_executes_and_query_still_returns_rows() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        for sql in [
            "create table users (id integer primary key, name text)",
            "insert into users (id, name) values (1, 'Ada')",
            "insert into users (id, name) values (2, 'Grace')",
            "create index users_name on users (name)",
        ] {
            app.query_service.execute_sql(sql).unwrap();
        }

        let result = app
            .query_service
            .execute_sql("select id from users where name = 'Ada'")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn unique_index_rejects_duplicate_insert() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("create unique index users_name on users (name)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let err = app
            .query_service
            .execute_sql("insert into users (id, name) values (2, 'Ada')")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);

        // The rejected insert left no trace.
        let result = app
            .query_service
            .execute_sql("select id from users")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn create_unique_index_on_duplicate_values_fails() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        for sql in [
            "create table users (id integer primary key, name text)",
            "insert into users (id, name) values (1, 'Ada')",
            "insert into users (id, name) values (2, 'Ada')",
        ] {
            app.query_service.execute_sql(sql).unwrap();
        }

        let err = app
            .query_service
            .execute_sql("create unique index users_name on users (name)")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
        // The rolled-back create left no index behind, so a non-unique one succeeds.
        app.query_service
            .execute_sql("create index users_name on users (name)")
            .unwrap();
    }

    #[tokio::test]
    async fn drop_index_allows_recreate_and_rejects_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("create index users_name on users (name)")
            .unwrap();
        app.query_service
            .execute_sql("drop index users_name")
            .unwrap();
        // Recreating under the same name now succeeds.
        app.query_service
            .execute_sql("create index users_name on users (name)")
            .unwrap();

        let err = app
            .query_service
            .execute_sql("drop index missing")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[tokio::test]
    async fn create_index_rejects_bad_table_column_and_duplicate_name() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let missing_table = app
            .query_service
            .execute_sql("create index i on ghosts (name)")
            .unwrap_err();
        assert_eq!(missing_table.code, SqlState::UndefinedTable);

        let missing_column = app
            .query_service
            .execute_sql("create index i on users (ghost)")
            .unwrap_err();
        assert_eq!(missing_column.code, SqlState::UndefinedColumn);

        app.query_service
            .execute_sql("create index dup on users (name)")
            .unwrap();
        let duplicate = app
            .query_service
            .execute_sql("create index dup on users (id)")
            .unwrap_err();
        assert_eq!(duplicate.code, SqlState::DuplicateTable);
    }

    #[tokio::test]
    async fn select_uses_secondary_index_and_returns_correct_rows() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        for sql in [
            "create table users (id integer primary key, name text)",
            "insert into users (id, name) values (1, 'Ada')",
            "insert into users (id, name) values (2, 'Bob')",
            "insert into users (id, name) values (3, 'Cleo')",
            "create index users_name on users (name)",
        ] {
            app.query_service.execute_sql(sql).unwrap();
        }

        // EXPLAIN shows the secondary index (id 1) is chosen, not a seq scan.
        let executor::ExecutionResult::Explanation { text } = app
            .query_service
            .execute_sql("explain select id from users where name = 'Bob'")
            .unwrap()
        else {
            panic!("expected explanation");
        };
        assert!(text.contains("IndexScan"), "plan was: {text}");
        assert!(text.contains("index=1"), "plan was: {text}");

        // Equality through the secondary index returns exactly the matching row.
        let executor::ExecutionResult::Query { rows, .. } = app
            .query_service
            .execute_sql("select id from users where name = 'Bob'")
            .unwrap()
        else {
            panic!("expected query");
        };
        assert_eq!(
            rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            vec![vec![Value::Integer(2)]]
        );

        // A range over the indexed column returns the matching rows.
        let executor::ExecutionResult::Query { rows, .. } = app
            .query_service
            .execute_sql("select name from users where name >= 'Bob' order by name")
            .unwrap()
        else {
            panic!("expected query");
        };
        assert_eq!(
            rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            vec![
                vec![Value::Text("Bob".to_string())],
                vec![Value::Text("Cleo".to_string())],
            ]
        );
    }

    #[tokio::test]
    async fn overflowing_update_rolls_back_prior_row_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql("create table nums (id integer primary key, val integer)")
            .unwrap();
        app.query_service
            .execute_sql("insert into nums (id, val) values (1, 1)")
            .unwrap();
        app.query_service
            .execute_sql("insert into nums (id, val) values (2, 9223372036854775807)")
            .unwrap();

        let err = app
            .query_service
            .execute_sql("update nums set val = val + 1")
            .unwrap_err();
        assert_eq!(err.code, SqlState::NumericValueOutOfRange);

        let executor::ExecutionResult::Query { rows, .. } = app
            .query_service
            .execute_sql("select id, val from nums order by id")
            .unwrap()
        else {
            panic!("expected query result");
        };
        assert_eq!(
            rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            vec![
                vec![Value::Integer(1), Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(i64::MAX)],
            ]
        );
    }

    #[tokio::test]
    async fn having_without_group_by_is_not_silently_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id) values (1)")
            .unwrap();

        let err = app
            .query_service
            .execute_sql("select id from users having false")
            .unwrap_err();
        assert_eq!(err.code, SqlState::DatatypeMismatch);

        let executor::ExecutionResult::Query { rows, .. } = app
            .query_service
            .execute_sql("select count(*) from users having false")
            .unwrap()
        else {
            panic!("expected query result");
        };
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn rollback_pre_durable_reports_catalog_restore_failure() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        let service = super::QueryService::new(app.components.clone());
        let invalid_snapshot = CatalogSnapshot {
            tables_by_name: HashMap::from([("ghost".to_string(), 7)]),
            tables_by_id: HashMap::new(),
            next_table_id: 1,
            indexes_by_name: HashMap::new(),
            indexes_by_id: HashMap::new(),
            next_index_id: 1,
        };

        let err = service
            .rollback_pre_durable(99, Some(invalid_snapshot))
            .unwrap_err();

        assert!(err.message.contains("catalog restore failed"));
    }

    #[tokio::test]
    async fn autocommit_commit_and_rollback_leave_registry_empty() {
        use wal::WalRecordKind;

        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        // A committed autocommit unit deregisters itself.
        assert!(app.components.active_txns.active_ids().is_empty());

        // A duplicate-key insert fails and rolls back, also leaving the registry
        // empty and appending an Abort record for the failed transaction.
        let err = app
            .query_service
            .execute_sql("insert into users (id, name) values (1, 'Dup')")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
        assert!(app.components.active_txns.active_ids().is_empty());

        let aborted: Vec<_> = app
            .components
            .wal
            .replay_from(0)
            .unwrap()
            .collect::<common::Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .filter(|record| matches!(record.kind, WalRecordKind::Abort))
            .collect();
        assert_eq!(aborted.len(), 1);
        // The failed transaction's id is not committed (it aborted).
        assert!(!app.components.wal.is_committed(aborted[0].txn_id));
    }

    #[tokio::test]
    async fn explain_returns_one_text_row_without_executor() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let executor::ExecutionResult::Explanation { text } = app
            .query_service
            .execute_sql("explain select name from users where id = 1")
            .unwrap()
        else {
            panic!("expected explain result");
        };

        assert!(text.contains("IndexScan"));
        assert!(text.contains("users"));
    }

    #[tokio::test]
    async fn select_materializes_rows_in_projection_order() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let executor::ExecutionResult::Query { rows, .. } = app
            .query_service
            .execute_sql("select name, id from users")
            .unwrap()
        else {
            panic!("expected query result");
        };

        assert_eq!(
            rows[0].values,
            vec![Value::Text("Ada".to_string()), Value::Integer(1)]
        );
    }

    #[tokio::test]
    async fn prepared_select_executes_and_reuses_with_bound_parameter() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (2, 'Bo')")
            .unwrap();

        let prepared = app
            .query_service
            .prepare_sql("select name from users where id = $1", &[])
            .unwrap();
        assert_eq!(prepared.param_types(), &[DataType::Integer]);
        assert_eq!(prepared.result_columns().unwrap().len(), 1);

        for (id, name) in [(2, "Bo"), (1, "Ada")] {
            let executor::ExecutionResult::Query { rows, .. } = app
                .query_service
                .execute_prepared(&prepared, &[Value::Integer(id)])
                .unwrap()
            else {
                panic!("expected query result");
            };
            assert_eq!(rows[0].values, vec![Value::Text(name.to_string())]);
        }
    }

    #[tokio::test]
    async fn prepared_insert_with_parameters_commits() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let prepared = app
            .query_service
            .prepare_sql("insert into users (id, name) values ($1, $2)", &[])
            .unwrap();
        assert_eq!(prepared.param_types(), &[DataType::Integer, DataType::Text]);
        assert!(prepared.result_columns().is_none());

        app.query_service
            .execute_prepared(
                &prepared,
                &[Value::Integer(5), Value::Text("Cy".to_string())],
            )
            .unwrap();

        let result = app
            .query_service
            .execute_sql("select name from users where id = 5")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn execute_prepared_rejects_wrong_parameter_count() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let prepared = app
            .query_service
            .prepare_sql("select name from users where id = $1", &[])
            .unwrap();
        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::SyntaxError);
    }
}
