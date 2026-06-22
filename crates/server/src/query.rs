use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{
    CheckpointGuard, ColumnInfo, DataType, DbError, IsolationLevel, Result, Snapshot, SqlState,
    StatementContext, Value, WriteGuard,
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
use crate::registry::AdvertisedSnapshot;

pub struct QueryService {
    components: Arc<ServerComponents>,
    engine: QueryEngine,
}

/// The concurrency guard an autocommit write/DDL unit holds for its lifetime. Most
/// writes take the SHARED writer guard (concurrent with other writers); CREATE INDEX
/// takes the EXCLUSIVE guard so its HOT broken-chain backfill sees a stable physical
/// view (`docs/specs/mvcc.md` §10 Milestone H2). Dropping this drops the inner guard
/// either way, so the autocommit path holds one variable across execution + commit.
/// The inner guards are held purely for their RAII `Drop` (they own the lock), never
/// read — like `WriteGuard`/`CheckpointGuard`'s own `_guard` fields.
#[allow(dead_code, reason = "guards are held for RAII Drop, never read")]
enum WriteUnitGuard {
    Shared(WriteGuard),
    Exclusive(CheckpointGuard),
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
/// across statements (`docs/specs/mvcc.md` §7.2). It owns the SHARED writer guard
/// (acquired lazily on the first write) for the whole write-transaction. Under the
/// E2b lock inversion (§7.1 Stage 2, §10 E2b) the writer guard is shared, so many
/// write-transactions run concurrently; per-row conflict detection (E1) and the
/// per-index / per-heap structural latches (E2a) provide write-write safety. Only a
/// checkpoint (the exclusive guard) excludes writers. Readers stay lock-free.
pub struct Transaction {
    txn_id: u64,
    /// The transaction's isolation level (`docs/specs/mvcc.md` §6, §10 Milestone
    /// G). Set at BEGIN from an explicit `ISOLATION LEVEL` mode or the default
    /// (Read Committed), and adjustable by `SET TRANSACTION ISOLATION LEVEL`
    /// before the first query. Threaded into `StatementContext.isolation`, which
    /// drives `snapshot_for_transaction`: Read Committed captures a fresh snapshot
    /// per statement, Repeatable Read captures one at the first statement and
    /// reuses it.
    isolation: IsolationLevel,
    /// `true` once the transaction has run its first query/data statement (i.e.
    /// captured its snapshot). `SET TRANSACTION ISOLATION LEVEL` is only valid
    /// while this is `false` (Postgres: "SET TRANSACTION ... must be called before
    /// any query"), so this is the before-first-query guard.
    first_statement_ran: bool,
    /// `true` once any statement has entered the `Failed` ('E') state. While set,
    /// every statement except COMMIT/ROLLBACK is rejected with `25P02`.
    failed: bool,
    /// The SHARED writer guard, acquired lazily on the first write statement and
    /// held until COMMIT/ROLLBACK. A read-only transaction never acquires it. It is
    /// shared (concurrent with other writers); only a checkpoint, holding the
    /// exclusive guard, waits for it to drain.
    write_guard: Option<WriteGuard>,
    /// The Repeatable Read snapshot: captured once at the first statement and
    /// reused. `None` under Read Committed (a fresh snapshot is captured per
    /// statement).
    rr_snapshot: Option<Arc<Snapshot>>,
    /// The advertisement pinning the GC horizon at the snapshot's `xmin` for the
    /// snapshot's usable lifetime (`docs/specs/mvcc.md` §9). Under Repeatable Read
    /// the one `rr_snapshot` is reusable for the whole transaction, so its
    /// advertisement is held here and released when the `Transaction` is dropped at
    /// commit/abort. Under Read Committed each statement captures and drops its own
    /// short-lived advertisement, so this stays `None`.
    rr_advertised: Option<AdvertisedSnapshot>,
    /// Dead MVCC versions this transaction's statements have produced so far
    /// (`docs/specs/mvcc.md` §9, Milestone F4b). Accumulated per write statement, but
    /// folded into the server-wide auto-prune counter ONLY on a durable COMMIT — on
    /// ROLLBACK the transaction's own new versions are the ones that become dead (the
    /// old versions it superseded stay live), so a rolled-back DELETE/UPDATE produces
    /// no committed dead version and this is discarded.
    dead_versions_pending: u64,
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
    ///
    /// `default_isolation` is the session's current default isolation level
    /// (`docs/specs/mvcc.md` §10 G2), threaded in/out by value like `slot`: a
    /// `BEGIN` with no explicit `ISOLATION LEVEL` inherits it, and `SET SESSION
    /// CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` updates it. The
    /// (possibly updated) default is returned so the connection persists it across
    /// statements.
    pub fn execute_simple(
        &self,
        sql: &str,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        let parsed = match parser::parse(sql) {
            Ok(parsed) => parsed,
            // A syntax error inside an open transaction poisons the block to the
            // failed state, matching PostgreSQL (the block must be ended before any
            // further command is accepted). Autocommit (`None`) is unaffected. The
            // session default is unchanged by a failed parse.
            Err(err) => return (mark_failed_on_error(slot), default_isolation, Err(err)),
        };
        self.dispatch(parsed, slot, default_isolation, cancel)
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
        // The autocommit helper has no persistent session: pass the built-in default
        // and discard the returned (possibly updated) default. A bare `SET SESSION
        // CHARACTERISTICS` here is therefore a no-op success with no lasting effect.
        let (_slot, _default, result) =
            self.execute_simple(sql, None, IsolationLevel::default(), cancel);
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
                maintenance: None,
                param_types: Vec::new(),
                result_columns: None,
            });
        }
        if let StatementClass::Maintenance = class {
            // VACUUM takes no parameters, produces no rows, and does not bind/plan.
            // Carry the parsed statement (the target table) so an extended-protocol
            // `Execute` routes it through `run_vacuum`, exactly like the simple path.
            return Ok(PreparedStatement {
                class,
                bound: None,
                maintenance: Some(statement),
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
            maintenance: None,
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
        // Maintenance does not bind/plan; run it before parameter substitution. The
        // connection routes maintenance through `execute_prepared_in_session`, so this
        // arm is reached only if a caller bypasses that routing — keep it total.
        if let StatementClass::Maintenance = prepared.class {
            return self.run_prepared_vacuum(prepared);
        }
        let bound = self.substitute_prepared_params(prepared, params)?;
        match prepared.class {
            StatementClass::Read => self.autocommit_read(bound, cancel),
            StatementClass::Write | StatementClass::Ddl => self.autocommit_write(bound, cancel),
            StatementClass::Maintenance => {
                unreachable!("maintenance is dispatched above before substitution")
            }
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
        default_isolation: IsolationLevel,
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        if let StatementClass::TransactionControl(kind) = prepared.class {
            return self.handle_transaction_control(kind, slot, default_isolation, cancel);
        }

        // VACUUM does not bind/plan: dispatch it before parameter substitution.
        // Inside an open transaction block it is rejected (poisoning it to 'E', like
        // DDL); otherwise it runs as a standalone maintenance unit.
        if let StatementClass::Maintenance = prepared.class {
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
            return (None, default_isolation, self.run_prepared_vacuum(prepared));
        }

        let bound = match self.substitute_prepared_params(prepared, params) {
            Ok(bound) => bound,
            // A parameter-count/substitution error inside an open transaction
            // poisons it to the failed state, matching the simple-query path.
            Err(err) => return (mark_failed_on_error(slot), default_isolation, Err(err)),
        };

        match slot {
            Some(txn) => {
                let (slot, result) = self.run_bound_in_transaction(
                    txn,
                    prepared.class,
                    BindSource::Bound(bound),
                    cancel,
                );
                (slot, default_isolation, result)
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
                    StatementClass::Maintenance => {
                        unreachable!("maintenance is dispatched above before substitution")
                    }
                    StatementClass::TransactionControl(_) => {
                        unreachable!("transaction control is dispatched above before substitution")
                    }
                };
                (None, default_isolation, result)
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
    /// `default_isolation` is the session default (in/out, like `slot`); only
    /// transaction-control statements read or update it, so the data and maintenance
    /// arms pass it back unchanged.
    fn dispatch(
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

    /// Handle BEGIN/COMMIT/ROLLBACK/SET TRANSACTION/SET SESSION CHARACTERISTICS
    /// against the session's transaction `slot` and `default_isolation` (the session
    /// default, in/out). Only `Begin` reads the default and only
    /// `SetSessionCharacteristics` updates it; every other arm returns it unchanged.
    fn handle_transaction_control(
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
        // The GC horizon is consumed only by CREATE INDEX, which is non-transactional
        // and rejected inside an explicit block (above), so it never reaches here; pass
        // `0` (unused on this path).
        let ctx = self.execution_context(txn.txn_id, snapshot, txn.isolation, 0, cancel);

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
    fn acquire_write_guard(&self, txn: &mut Transaction) -> Result<()> {
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
        let ctx = self.execution_context(0, snapshot, IsolationLevel::default(), 0, cancel);
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
    fn autocommit_write(
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
        // Capture the GC horizon for CREATE INDEX's broken-chain check AFTER the
        // exclusive guard is held (so no writer can advance it), exactly as
        // `run_vacuum` does. For non-CREATE-INDEX statements the horizon is unused.
        let gc_horizon = if needs_exclusive {
            self.components.gc_horizon()
        } else {
            0
        };
        let ctx = self.execution_context(
            txn_id,
            snapshot,
            IsolationLevel::default(),
            gc_horizon,
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

    /// Run a prepared (extended-protocol) `VACUUM`. The statement carries no bound
    /// payload — it is the raw `Statement::Vacuum` parsed at `prepare_sql` time.
    fn run_prepared_vacuum(&self, prepared: &PreparedStatement) -> Result<ExecutionResult> {
        let statement = prepared.maintenance.as_ref().ok_or_else(|| {
            DbError::internal("maintenance prepared statement has no carried VACUUM payload")
        })?;
        self.run_vacuum(statement.clone())
    }

    /// Run `VACUUM` (Milestone F4a, `docs/specs/mvcc.md` §9/§10 F): reclaim dead MVCC
    /// versions from one table or every user table, under the EXCLUSIVE checkpoint
    /// guard. Returns a `CommandComplete`-style result tagged `VACUUM`.
    ///
    /// **Concurrency + safety (no data loss — the horizon-under-the-guard argument).**
    /// VACUUM takes the exclusive guard ([`ConcurrencyController::begin_checkpoint`]),
    /// which drains all in-flight writers and holds off new ones, so NO writer runs
    /// during the pass (lock-free readers still run concurrently). The GC horizon is
    /// captured **once, after the guard is held**, as the minimum `xmin` advertised by
    /// any live snapshot — INCLUDING active lock-free readers and autocommit reads,
    /// which advertise their `xmin` ([`ServerComponents::gc_horizon`]). Each phase only
    /// reclaims versions with `xmax < horizon` ([`common::is_dead_to_all`]), i.e.
    /// deletes that committed before every live snapshot's `xmin`; no current snapshot
    /// can see such a version live, and any reader that starts mid-pass freezes
    /// `xmin >= horizon` (the deleter is in its settled past). Capturing the horizon
    /// AFTER acquiring the guard is load-bearing: it cannot then be advanced by a
    /// concurrent writer/commit, and it already accounts for every reader advertised at
    /// that instant. VACUUM therefore never reclaims a version any snapshot needs.
    fn run_vacuum(&self, statement: Statement) -> Result<ExecutionResult> {
        let Statement::Vacuum { table } = statement else {
            return Err(DbError::internal(
                "run_vacuum called with a non-VACUUM statement",
            ));
        };

        // Acquire the EXCLUSIVE guard for the whole pass FIRST: it drains in-flight
        // writers and excludes new ones, so the pass runs with no concurrent writer
        // (readers stay lock-free) and the catalog cannot change under us (DDL takes
        // the shared writer guard, which is excluded here). The guard is released when
        // `_guard` drops at return. Resolving the target table(s) under the guard —
        // like `run_checkpoint` — means the resolved schema is stable for the pass.
        let _guard = self.components.concurrency.begin_checkpoint()?;

        // Capture the horizon ONCE, AFTER the guard is held (see the method doc): it is
        // the min advertised snapshot `xmin`, so no version a live snapshot can see is
        // reclaimable, and it cannot be advanced by a writer while we hold the guard.
        let horizon = self.components.gc_horizon();

        // `VACUUM t` targets just `t` (error if it does not exist); `VACUUM` (no table)
        // is a FULL pass over every user table — and ONLY a full pass advances the
        // vacuum floor (`docs/specs/mvcc.md` §9, F4c), since a single-table pass leaves
        // other tables' aborted-creator tuples on disk.
        match table {
            Some(name) => {
                let schema = self
                    .components
                    .catalog
                    .get_table_by_name(&name)?
                    .ok_or_else(|| {
                        DbError::plan(
                            SqlState::UndefinedTable,
                            format!("table {name} does not exist"),
                        )
                    })?;
                // Single-table pass: reclaim `t`'s dead versions but DO NOT advance the
                // vacuum floor (other tables may still hold aborted-creator tuples).
                vacuum_tables(&self.components, std::slice::from_ref(&schema), horizon)?;
            }
            None => {
                // Full pass: capture the boundary BEFORE the pass and advance the vacuum
                // floor AFTER it (the F4c contract — see `full_vacuum_pass`). The
                // reclamation becomes durable in the NEXT checkpoint, which flushes all
                // dirty pages before its `truncate_before` consults the floor, so no
                // `Abort` is ever dropped while its tuples remain on disk.
                full_vacuum_pass(&self.components, horizon)?;
            }
        }

        Ok(ExecutionResult::Modified {
            command: "VACUUM".to_string(),
            count: 0,
        })
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
    fn snapshot_for_transaction(
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

    fn execution_context<'a>(
        &'a self,
        txn_id: u64,
        snapshot: Arc<Snapshot>,
        isolation: IsolationLevel,
        gc_horizon: u64,
        cancel: &'a AtomicBool,
    ) -> ExecutionContext<'a> {
        ExecutionContext {
            statement: StatementContext::with_snapshot_and_isolation(txn_id, snapshot, isolation),
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
    fn capture_snapshot(&self, own_txn: u64) -> (Arc<Snapshot>, AdvertisedSnapshot) {
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

/// The number of dead MVCC versions a statement's result implies, for the
/// auto-prune threshold (`docs/specs/mvcc.md` §9, Milestone F4b). Each committed
/// `DELETE` row leaves a dead version (the committed-deleted tuple) and each
/// committed `UPDATE` row leaves a dead version (the superseded old tuple); both
/// carry their affected-row count in the `Modified` command tag the executor
/// already produces. `INSERT`, DDL, and read/explain results imply no dead version.
/// Counted only on a successful commit by the callers.
fn dead_versions_in(result: &ExecutionResult) -> u64 {
    match result {
        ExecutionResult::Modified { command, count }
            if command == "DELETE" || command == "UPDATE" =>
        {
            *count
        }
        _ => 0,
    }
}

/// Vacuum each `table` with F4a's three-phase orchestration
/// ([`PageBackedStorageEngine::vacuum`]: heap-prune → index-vacuum →
/// line-pointer-reclaim), reclaiming versions dead to `horizon`. Shared by the
/// on-demand `VACUUM` command and the checkpoint auto-prune so the reclamation logic
/// is defined once (`docs/specs/mvcc.md` §9/§10 F4a/F4b).
///
/// **Caller contract (the no-data-loss safety):** the caller MUST already hold the
/// EXCLUSIVE checkpoint guard ([`ConcurrencyController::begin_checkpoint`]) and MUST
/// have captured `horizon` from [`ServerComponents::gc_horizon`] *after* acquiring
/// that guard. Under the guard no writer runs, and the horizon is the minimum `xmin`
/// advertised by any live snapshot (including lock-free readers), so every reclaimed
/// version (`xmax < horizon`) is one no live snapshot can see — identical safety to
/// the on-demand `VACUUM` (F4a). This helper does not take the guard or capture the
/// horizon itself, precisely so it cannot be misused to vacuum with an
/// outside-the-guard horizon.
fn vacuum_tables(
    components: &ServerComponents,
    tables: &[common::TableSchema],
    horizon: u64,
) -> Result<()> {
    for schema in tables {
        components.storage.vacuum(schema, horizon)?;
    }
    Ok(())
}

/// Run a FULL VACUUM pass over every user table AND advance the WAL **vacuum floor**
/// (`docs/specs/mvcc.md` §5.4, §9, Milestone F4c). Used by the on-demand `VACUUM`
/// (no table) and the checkpoint auto-prune (F4b) — the two full-pass callers.
///
/// The boundary `B = next_txn_id` is captured BEFORE the pass and the floor is
/// advanced to `B` AFTER it. Both reads happen under the exclusive guard the caller
/// holds (same contract as [`vacuum_tables`]: `horizon` was captured under it), so no
/// id is allocated mid-pass and `B` is the exact id high-water at scan time. Because
/// the aborted-creator reclaim has NO age requirement, a full pass reclaims EVERY
/// aborted-creator tuple (heap + index) that exists at scan time across every user
/// table, so every aborted transaction with id `< B` now has no surviving on-disk
/// version. Advancing the floor to `B` is therefore safe: `truncate_before` may drop
/// those aborted txns' `Abort` records and float the implicit-committed floor past
/// them (the catalog is NOT MVCC-versioned, so user-table tuples are the only place
/// aborted-creator versions live).
///
/// **Durability ordering.** The floor is only ever CONSULTED by `truncate_before`,
/// which a checkpoint runs AFTER `flush_dirty_pages` + `store.sync_all` — so by the
/// time the floor is used, every dirty page this pass produced (auto-prune: this same
/// checkpoint; on-demand: a later checkpoint) is fsynced to the heap. No `Abort` is
/// dropped while its reclaimed tuples are still only in memory.
pub(crate) fn full_vacuum_pass(components: &ServerComponents, horizon: u64) -> Result<()> {
    // Capture B BEFORE the pass, under the guard (no concurrent allocation).
    let boundary = components.next_txn_id.load(Ordering::Acquire);
    vacuum_all_user_tables(components, horizon)?;
    // Advance the floor only AFTER the pass has reclaimed every aborted-creator tuple
    // below B. Monotonic; in-memory; reset-at-restart (see `WalManager::set_vacuum_floor`).
    components.wal.set_vacuum_floor(boundary)
}

/// Vacuum every user table in the catalog, for the checkpoint auto-prune path (F4b).
/// Same caller contract as [`vacuum_tables`]: the exclusive guard is held and
/// `horizon` was captured under it. This does NOT advance the vacuum floor; callers
/// that perform a *full* pass and want the floor advanced use [`full_vacuum_pass`].
fn vacuum_all_user_tables(components: &ServerComponents, horizon: u64) -> Result<()> {
    let tables = components.catalog.list_tables()?;
    vacuum_tables(components, &tables, horizon)
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

/// The `SET` command tag, shared by `SET TRANSACTION` and `SET SESSION
/// CHARACTERISTICS` (and a no-op `SET`) — Postgres tags all of them `SET`.
fn set_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "SET".to_string(),
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
    /// `BEGIN`/`START TRANSACTION`, carrying an optional explicit
    /// `ISOLATION LEVEL` (`None` inherits the session default — Read Committed
    /// unless `SET SESSION CHARACTERISTICS` raised it, `docs/specs/mvcc.md` §10 G2).
    Begin(Option<IsolationLevel>),
    Commit,
    Rollback,
    /// `SET TRANSACTION ISOLATION LEVEL <level>`: set the current transaction's
    /// isolation level, valid only before its first query. `None` isolation is a
    /// `SET TRANSACTION` with no level mode (a no-op for v1).
    SetTransaction(Option<IsolationLevel>),
    /// `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`: set the
    /// per-connection DEFAULT isolation for future transactions, without changing an
    /// already-open transaction (`docs/specs/mvcc.md` §10 G2). `None` is a
    /// `SET SESSION CHARACTERISTICS` with no level mode (a no-op success).
    SetSessionCharacteristics(Option<IsolationLevel>),
}

#[derive(Clone, Copy)]
enum StatementClass {
    Read,
    Write,
    Ddl,
    /// A maintenance command (`VACUUM`) — not relational, so it never binds or
    /// plans, and like DDL it is forbidden inside an explicit transaction block.
    Maintenance,
    TransactionControl(TransactionControl),
}

/// A parsed and bound extended-protocol statement that can be executed
/// repeatedly with different parameter values. `bound` is `None` only for
/// transaction-control statements (BEGIN/COMMIT/ROLLBACK), which carry no bound
/// payload and are dispatched through the session's transaction lifecycle.
pub struct PreparedStatement {
    class: StatementClass,
    bound: Option<BoundStatement>,
    /// The parsed maintenance statement (`VACUUM`), carried unbound for the
    /// `StatementClass::Maintenance` case so an extended-protocol `Execute` can run
    /// it through `run_vacuum`. `None` for every other class.
    maintenance: Option<Statement>,
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

    /// Whether this is a maintenance command (`VACUUM`). The connection routes such
    /// an `Execute` through the session path so it is rejected inside an open
    /// transaction block and otherwise runs as a standalone maintenance unit.
    pub fn is_maintenance(&self) -> bool {
        matches!(self.class, StatementClass::Maintenance)
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
        Statement::Begin { isolation } => Ok(StatementClass::TransactionControl(
            TransactionControl::Begin(*isolation),
        )),
        Statement::Commit => Ok(StatementClass::TransactionControl(
            TransactionControl::Commit,
        )),
        Statement::Rollback => Ok(StatementClass::TransactionControl(
            TransactionControl::Rollback,
        )),
        Statement::SetTransaction { isolation } => Ok(StatementClass::TransactionControl(
            TransactionControl::SetTransaction(*isolation),
        )),
        Statement::SetSessionCharacteristics { isolation } => {
            Ok(StatementClass::TransactionControl(
                TransactionControl::SetSessionCharacteristics(*isolation),
            ))
        }
        Statement::Vacuum { .. } => Ok(StatementClass::Maintenance),
    }
}

#[cfg(test)]
impl QueryService {
    /// Test-only thin wrapper over [`QueryService::execute_simple`] that supplies the
    /// built-in default isolation (`ReadCommitted`) and discards the returned
    /// (possibly updated) session default, recovering the pre-G2 `(slot, result)`
    /// shape. Used by transaction-control tests where the session default is
    /// irrelevant; the G2 inheritance tests call `execute_simple` directly to drive
    /// and observe the default.
    fn execute_simple_default(
        &self,
        sql: &str,
        slot: Option<Transaction>,
        cancel: &AtomicBool,
    ) -> (Option<Transaction>, Result<ExecutionResult>) {
        let (slot, _default, result) =
            self.execute_simple(sql, slot, IsolationLevel::default(), cancel);
        (slot, result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;

    use catalog::CatalogSnapshot;
    use common::{DataType, IsolationLevel, Result, SqlState, Value};
    use executor::ExecutionResult;

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
        let (slot, result) = app
            .query_service
            .execute_simple_default("begin", None, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);

        let (slot, result) = app.query_service.execute_simple_default(
            "insert into users (id, name) values (1, 'Ada')",
            slot,
            &cancel,
        );
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);

        let (slot, result) =
            app.query_service
                .execute_simple_default("select id from users", slot, &cancel);
        let rows = match result.unwrap() {
            executor::ExecutionResult::Query { rows, .. } => rows,
            other => panic!("expected query, got {other:?}"),
        };
        assert_eq!(rows.len(), 1, "the open transaction sees its own insert");

        let (slot, result) = app
            .query_service
            .execute_simple_default("commit", slot, &cancel);
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
        let (slot, result) = app
            .query_service
            .execute_simple_default("begin", None, &cancel);
        result.unwrap();
        let (slot, result) = app.query_service.execute_simple_default(
            "insert into users (id, name) values (1, 'Ada')",
            slot,
            &cancel,
        );
        result.unwrap();
        let (slot, result) = app
            .query_service
            .execute_simple_default("rollback", slot, &cancel);
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
        let (slot, result) = app
            .query_service
            .execute_simple_default("begin", None, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);

        // A statement against a missing table errors and poisons the txn to 'E'.
        let (slot, result) =
            app.query_service
                .execute_simple_default("select id from ghosts", slot, &cancel);
        assert!(result.is_err());
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);

        // While 'E', every statement but COMMIT/ROLLBACK is rejected with 25P02.
        let (slot, result) =
            app.query_service
                .execute_simple_default("select id from users", slot, &cancel);
        let err = result.unwrap_err();
        assert_eq!(err.code, SqlState::InFailedSqlTransaction);
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);

        // ROLLBACK returns to Idle.
        let (slot, result) = app
            .query_service
            .execute_simple_default("rollback", slot, &cancel);
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
        let (slot, _) = app
            .query_service
            .execute_simple_default("begin", None, &cancel);
        let (slot, _) = app.query_service.execute_simple_default(
            "insert into users (id) values (1)",
            slot,
            &cancel,
        );
        let (slot, result) =
            app.query_service
                .execute_simple_default("select id from ghosts", slot, &cancel);
        assert!(result.is_err());
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);

        // COMMIT of an aborted transaction issues ROLLBACK (Postgres behavior).
        let (slot, result) = app
            .query_service
            .execute_simple_default("commit", slot, &cancel);
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
        let (slot, _) = app
            .query_service
            .execute_simple_default("begin", None, &cancel);
        let (slot, result) = app.query_service.execute_simple_default(
            "create table users (id integer primary key)",
            slot,
            &cancel,
        );
        let err = result.unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);
        let (_slot, result) = app
            .query_service
            .execute_simple_default("rollback", slot, &cancel);
        result.unwrap();
    }

    #[tokio::test]
    async fn commit_and_rollback_with_no_open_transaction_are_no_ops() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, result) = app
            .query_service
            .execute_simple_default("commit", None, &cancel);
        result.unwrap();
        assert!(slot.is_none());
        let (slot, result) = app
            .query_service
            .execute_simple_default("rollback", None, &cancel);
        result.unwrap();
        assert!(slot.is_none());
    }

    #[tokio::test]
    async fn begin_inside_transaction_is_a_noop_warning_staying_in_t() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, _) = app
            .query_service
            .execute_simple_default("begin", None, &cancel);
        let txn_id_before = app.components.active_txns.active_ids();
        let (slot, result) = app
            .query_service
            .execute_simple_default("begin", slot, &cancel);
        result.unwrap();
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::InTransaction);
        // The second BEGIN did not allocate a new transaction.
        assert_eq!(app.components.active_txns.active_ids(), txn_id_before);
        let (_slot, _) = app
            .query_service
            .execute_simple_default("rollback", slot, &cancel);
    }

    // -- Milestone G2: session-default isolation (SET SESSION CHARACTERISTICS) --

    /// Count the rows a SELECT returns, asserting it succeeded.
    fn row_count(result: Result<ExecutionResult>) -> usize {
        match result.unwrap() {
            ExecutionResult::Query { rows, .. } => rows.len(),
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn session_default_repeatable_read_is_inherited_by_a_new_begin() {
        // The payoff test. After `SET SESSION CHARACTERISTICS ... REPEATABLE READ`, a
        // plain `BEGIN` (no explicit level) defaults to Repeatable Read, so its second
        // SELECT does NOT see a row another connection committed between the two
        // SELECTs. The default (Read Committed) WOULD see it (the contrast case below).
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table t (id integer primary key)")
            .unwrap();

        let cancel = AtomicBool::new(false);

        // Contrast: with the session default Read Committed, the second SELECT in an
        // open transaction sees the concurrently-committed row.
        let (slot, _iso, res) =
            app.query_service
                .execute_simple("begin", None, IsolationLevel::default(), &cancel);
        res.unwrap();
        let (slot, _iso, res) = app.query_service.execute_simple(
            "select id from t",
            slot,
            IsolationLevel::ReadCommitted,
            &cancel,
        );
        assert_eq!(row_count(res), 0);
        // Another connection commits a new row (autocommit = its own implicit txn).
        app.query_service
            .execute_sql("insert into t (id) values (1)")
            .unwrap();
        let (slot, _iso, res) = app.query_service.execute_simple(
            "select id from t",
            slot,
            IsolationLevel::ReadCommitted,
            &cancel,
        );
        assert_eq!(
            row_count(res),
            1,
            "Read Committed sees the concurrently-committed row"
        );
        let (_slot, _iso, res) = app.query_service.execute_simple(
            "commit",
            slot,
            IsolationLevel::ReadCommitted,
            &cancel,
        );
        res.unwrap();
        app.query_service.execute_sql("delete from t").unwrap();

        // Now SET SESSION CHARACTERISTICS ... REPEATABLE READ, then a plain BEGIN
        // inherits Repeatable Read: its second SELECT does NOT see the new row.
        let (slot, default_isolation, res) = app.query_service.execute_simple(
            "set session characteristics as transaction isolation level repeatable read",
            None,
            IsolationLevel::default(),
            &cancel,
        );
        res.unwrap();
        assert_eq!(default_isolation, IsolationLevel::RepeatableRead);
        assert!(slot.is_none(), "SET SESSION CHARACTERISTICS opens no txn");

        let (slot, default_isolation, res) =
            app.query_service
                .execute_simple("begin", None, default_isolation, &cancel);
        res.unwrap();
        let (slot, default_isolation, res) =
            app.query_service
                .execute_simple("select id from t", slot, default_isolation, &cancel);
        assert_eq!(row_count(res), 0);
        app.query_service
            .execute_sql("insert into t (id) values (2)")
            .unwrap();
        let (slot, default_isolation, res) =
            app.query_service
                .execute_simple("select id from t", slot, default_isolation, &cancel);
        assert_eq!(
            row_count(res),
            0,
            "the inherited Repeatable Read txn does NOT see the new row"
        );
        let (_slot, _iso, res) =
            app.query_service
                .execute_simple("commit", slot, default_isolation, &cancel);
        res.unwrap();
    }

    #[tokio::test]
    async fn explicit_begin_level_overrides_session_default() {
        // Precedence: an explicit BEGIN level overrides the session default; a plain
        // BEGIN inherits it. After SET SESSION CHARACTERISTICS ... REPEATABLE READ:
        // `BEGIN ISOLATION LEVEL READ COMMITTED` behaves as Read Committed, while a
        // plain `BEGIN` behaves as Repeatable Read.
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table t (id integer primary key)")
            .unwrap();

        let cancel = AtomicBool::new(false);
        let (_slot, session_default, res) = app.query_service.execute_simple(
            "set session characteristics as transaction isolation level repeatable read",
            None,
            IsolationLevel::default(),
            &cancel,
        );
        res.unwrap();
        assert_eq!(session_default, IsolationLevel::RepeatableRead);

        // Explicit READ COMMITTED on BEGIN overrides the RR session default: the
        // second SELECT sees the concurrently-committed row.
        let (slot, sd, res) = app.query_service.execute_simple(
            "begin isolation level read committed",
            None,
            session_default,
            &cancel,
        );
        res.unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(row_count(res), 0);
        app.query_service
            .execute_sql("insert into t (id) values (1)")
            .unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(
            row_count(res),
            1,
            "explicit READ COMMITTED overrides the RR session default"
        );
        let (_slot, sd, res) = app
            .query_service
            .execute_simple("commit", slot, sd, &cancel);
        res.unwrap();
        app.query_service.execute_sql("delete from t").unwrap();

        // A plain BEGIN still inherits the RR session default: it does not see the
        // concurrently-committed row.
        let (slot, sd, res) = app.query_service.execute_simple("begin", None, sd, &cancel);
        res.unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(row_count(res), 0);
        app.query_service
            .execute_sql("insert into t (id) values (2)")
            .unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(
            row_count(res),
            0,
            "a plain BEGIN inherits the RR session default"
        );
        let (_slot, _sd, res) = app
            .query_service
            .execute_simple("commit", slot, sd, &cancel);
        res.unwrap();
    }

    #[tokio::test]
    async fn session_default_persists_across_transactions() {
        // One SET SESSION CHARACTERISTICS ... REPEATABLE READ makes both of two
        // sequential plain BEGIN…COMMIT transactions on the same connection behave as
        // Repeatable Read (the default persists on the threaded session value).
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table t (id integer primary key)")
            .unwrap();

        let cancel = AtomicBool::new(false);
        let (_slot, sd, res) = app.query_service.execute_simple(
            "set session characteristics as transaction isolation level repeatable read",
            None,
            IsolationLevel::default(),
            &cancel,
        );
        res.unwrap();
        assert_eq!(sd, IsolationLevel::RepeatableRead);

        // Run two transactions in sequence; each must behave as Repeatable Read.
        let mut session_default = sd;
        for round in 0..2 {
            let (slot, sd, res) =
                app.query_service
                    .execute_simple("begin", None, session_default, &cancel);
            res.unwrap();
            let (slot, sd, res) =
                app.query_service
                    .execute_simple("select id from t", slot, sd, &cancel);
            let before = row_count(res);
            // Another connection commits a fresh row.
            app.query_service
                .execute_sql(&format!("insert into t (id) values ({})", round + 1))
                .unwrap();
            let (slot, sd, res) =
                app.query_service
                    .execute_simple("select id from t", slot, sd, &cancel);
            assert_eq!(
                row_count(res),
                before,
                "round {round}: each transaction stays Repeatable Read"
            );
            let (slot, sd, res) = app
                .query_service
                .execute_simple("commit", slot, sd, &cancel);
            res.unwrap();
            assert!(slot.is_none());
            session_default = sd;
            assert_eq!(session_default, IsolationLevel::RepeatableRead);
        }
    }

    #[tokio::test]
    async fn set_session_characteristics_does_not_change_the_open_transaction() {
        // `SET SESSION CHARACTERISTICS` is allowed inside a transaction block but does
        // NOT change the CURRENT transaction's isolation; it only affects FUTURE
        // transactions. An open Read Committed transaction stays Read Committed after
        // the SET, while the NEXT transaction is Repeatable Read.
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table t (id integer primary key)")
            .unwrap();

        let cancel = AtomicBool::new(false);
        // Open an explicit Read Committed transaction and capture its first snapshot.
        let (slot, sd, res) = app.query_service.execute_simple(
            "begin isolation level read committed",
            None,
            IsolationLevel::default(),
            &cancel,
        );
        res.unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(row_count(res), 0);

        // SET SESSION CHARACTERISTICS ... REPEATABLE READ inside the open block:
        // updates the session default but leaves THIS transaction Read Committed.
        let (slot, sd, res) = app.query_service.execute_simple(
            "set session characteristics as transaction isolation level repeatable read",
            slot,
            sd,
            &cancel,
        );
        res.unwrap();
        assert_eq!(sd, IsolationLevel::RepeatableRead);
        assert_eq!(
            super::slot_status(&slot),
            SessionTxnStatus::InTransaction,
            "SET SESSION CHARACTERISTICS does not end or fail the open block"
        );

        // Another connection commits a row; this still-Read-Committed transaction
        // sees it on its next SELECT (proving its isolation was not raised to RR).
        app.query_service
            .execute_sql("insert into t (id) values (1)")
            .unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(
            row_count(res),
            1,
            "the open txn stayed Read Committed; SET SESSION CHARACTERISTICS did not change it"
        );
        let (_slot, sd, res) = app
            .query_service
            .execute_simple("commit", slot, sd, &cancel);
        res.unwrap();

        // The NEXT transaction is Repeatable Read (it inherited the updated default).
        app.query_service.execute_sql("delete from t").unwrap();
        let (slot, sd, res) = app.query_service.execute_simple("begin", None, sd, &cancel);
        res.unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(row_count(res), 0);
        app.query_service
            .execute_sql("insert into t (id) values (2)")
            .unwrap();
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from t", slot, sd, &cancel);
        assert_eq!(
            row_count(res),
            0,
            "the next transaction inherited Repeatable Read"
        );
        let (_slot, _sd, res) = app
            .query_service
            .execute_simple("commit", slot, sd, &cancel);
        res.unwrap();
    }

    #[tokio::test]
    async fn set_session_characteristics_no_level_is_a_noop_success() {
        // `SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE` (no isolation-level
        // mode) is a no-op success that leaves the session default unchanged.
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, sd, res) = app.query_service.execute_simple(
            "set session characteristics as transaction read write",
            None,
            IsolationLevel::RepeatableRead,
            &cancel,
        );
        res.unwrap();
        assert!(slot.is_none());
        assert_eq!(
            sd,
            IsolationLevel::RepeatableRead,
            "a no-level SET SESSION CHARACTERISTICS leaves the default unchanged"
        );
    }

    #[tokio::test]
    async fn set_session_characteristics_in_failed_block_is_rejected() {
        // Inside an already-failed ('E') block, SET SESSION CHARACTERISTICS is rejected
        // with 25P02 like any non-COMMIT/ROLLBACK statement, and the session default is
        // unchanged.
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let cancel = AtomicBool::new(false);
        let (slot, sd, res) =
            app.query_service
                .execute_simple("begin", None, IsolationLevel::default(), &cancel);
        res.unwrap();
        // Poison the block to 'E'.
        let (slot, sd, res) =
            app.query_service
                .execute_simple("select id from ghosts", slot, sd, &cancel);
        assert!(res.is_err());
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);

        let (slot, sd, res) = app.query_service.execute_simple(
            "set session characteristics as transaction isolation level repeatable read",
            slot,
            sd,
            &cancel,
        );
        let err = res.unwrap_err();
        assert_eq!(err.code, SqlState::InFailedSqlTransaction);
        assert_eq!(super::slot_status(&slot), SessionTxnStatus::Failed);
        assert_eq!(
            sd,
            IsolationLevel::ReadCommitted,
            "a rejected SET SESSION CHARACTERISTICS leaves the default unchanged"
        );
        let (_slot, _sd, _res) = app
            .query_service
            .execute_simple("rollback", slot, sd, &cancel);
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
