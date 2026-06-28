use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::{
    CheckpointGuard, ColumnInfo, CopyDirection, DataType, DbError, IsolationLevel, Result,
    Snapshot, SqlState, Value, WriteGuard,
};
use executor::{ExecutionContext, ExecutionResult, QueryEngine};
use parser::Statement;
use planner::{
    BoundStatement, bind_parameterized, format_explain, logical_plan, physical_plan,
    substitute_params,
};

use crate::app::ServerComponents;
use crate::registry::AdvertisedSnapshot;

mod copy;
mod exec;
mod txn;
mod vacuum;

pub use copy::CopyInChunk;
pub(crate) use vacuum::full_vacuum_pass;

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
    /// The OPEN savepoint stack, outermost first (`docs/specs/savepoints.md` §3).
    /// Each level owns a subxid; the innermost level's subxid is the current
    /// writing xid (`writing_xid`), or `txn_id` when the stack is empty. `SAVEPOINT`
    /// pushes, `RELEASE` pops the named level and any above it (a pure in-memory
    /// merge — the popped subxids stay live), `ROLLBACK TO` pops down to the named
    /// level and re-establishes it with a fresh subxid.
    savepoints: Vec<SavepointLevel>,
    /// Every not-rolled-back subxid (open AND released), i.e. the transaction's
    /// live-set minus `txn_id`. This is what the top-level COMMIT records as
    /// committed subxids and, together with `txn_id`, the live (sub)xid set threaded
    /// into each statement's `StatementContext` (`live_txns`). `SAVEPOINT` appends;
    /// `ROLLBACK TO` removes the rolled-back subxids; `RELEASE` leaves it unchanged.
    live_subxids: Vec<u64>,
}

/// One open savepoint: its name and the subxid that owns writes made under it.
struct SavepointLevel {
    name: String,
    subxid: u64,
}

impl Transaction {
    /// The current writing xid: the innermost open savepoint's subxid, or the
    /// top-level `txn_id` when no savepoint is open. New tuples stamp this as `xmin`.
    fn writing_xid(&self) -> u64 {
        self.savepoints
            .last()
            .map(|level| level.subxid)
            .unwrap_or(self.txn_id)
    }

    /// The transaction's live (sub)xid set — `txn_id` plus every not-rolled-back
    /// subxid — for `StatementContext::live_txns` (the "self" set for visibility and
    /// conflict detection; `docs/specs/savepoints.md` §4).
    fn live_txns(&self) -> Arc<[u64]> {
        if self.live_subxids.is_empty() {
            return Arc::from([self.txn_id]);
        }
        let mut ids = Vec::with_capacity(self.live_subxids.len() + 1);
        ids.push(self.txn_id);
        ids.extend_from_slice(&self.live_subxids);
        Arc::from(ids)
    }
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
        cancel: &Arc<AtomicBool>,
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
        self.execute_sql_cancelable(sql, &Arc::new(AtomicBool::new(false)))
    }

    /// Like `execute_sql`, but aborts with `QueryCanceled` if `cancel` becomes
    /// set (from another connection's `CancelRequest`) while the query runs. This
    /// is the autocommit path: no transaction is carried across the call.
    pub fn execute_sql_cancelable(
        &self,
        sql: &str,
        cancel: &Arc<AtomicBool>,
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
        if let StatementClass::Copy(_) = class {
            // COPY needs the simple-query COPY sub-protocol; PostgreSQL likewise
            // rejects it through Parse/Bind/Execute.
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "COPY is not supported in the extended query protocol",
            ));
        }
        if let StatementClass::Savepoint = class {
            // Savepoints are driven through the simple-query transaction lifecycle
            // (`docs/specs/savepoints.md` §2), like transaction control via the
            // extended protocol — rejected here so an Execute never reaches them.
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "savepoints require the simple query protocol",
            ));
        }
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
        self.execute_prepared_cancelable(prepared, params, &Arc::new(AtomicBool::new(false)))
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
        cancel: &Arc<AtomicBool>,
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
            // COPY is rejected at prepare time for the extended protocol, so an
            // already-prepared statement is never COPY; keep the match total.
            StatementClass::Copy(_) => Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "COPY is not supported in the extended query protocol",
            )),
            // Savepoints are likewise rejected at prepare time for the extended
            // protocol, so an already-prepared statement is never a savepoint.
            StatementClass::Savepoint => Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "savepoints require the simple query protocol",
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
        cancel: &Arc<AtomicBool>,
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
                    StatementClass::Copy(_) => {
                        unreachable!("COPY is rejected at prepare time for the extended protocol")
                    }
                    StatementClass::Savepoint => {
                        unreachable!(
                            "savepoints are rejected at prepare time for the extended protocol"
                        )
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
        | ExecutionResult::ModifiedReturning { command, count, .. }
            if command == "DELETE" || command == "UPDATE" =>
        {
            *count
        }
        _ => 0,
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

/// The `SET` command tag, shared by `SET TRANSACTION` and `SET SESSION
/// CHARACTERISTICS` (and a no-op `SET`) — Postgres tags all of them `SET`.
fn set_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "SET".to_string(),
        count: 0,
    }
}

fn savepoint_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "SAVEPOINT".to_string(),
        count: 0,
    }
}

fn release_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "RELEASE".to_string(),
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
    /// `COPY ... FROM STDIN | TO STDOUT` — a bulk-transfer command driven by the
    /// connection loop's COPY sub-protocol, not the normal execute path
    /// (`docs/specs/copy.md`). It binds (resolve table/columns) but is not lowered.
    Copy(CopyDirection),
    TransactionControl(TransactionControl),
    /// `SAVEPOINT` / `RELEASE [SAVEPOINT]` / `ROLLBACK TO [SAVEPOINT]` — driven
    /// through the session's transaction lifecycle like transaction control
    /// (`docs/specs/savepoints.md`); simple-query only. The op + name are read from
    /// the parsed `Statement` in `handle_savepoint` (so this stays a `Copy` marker).
    Savepoint,
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
        // A DML statement with a RETURNING clause produces a result set; its
        // RowDescription is the RETURNING projection schema.
        BoundStatement::Insert { returning, .. }
        | BoundStatement::Update { returning, .. }
        | BoundStatement::Delete { returning, .. } => returning
            .as_ref()
            .map(|returning| returning.output_schema.clone()),
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
        Statement::Copy { direction, .. } => Ok(StatementClass::Copy(*direction)),
        Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::RollbackToSavepoint { .. } => Ok(StatementClass::Savepoint),
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
        cancel: &Arc<AtomicBool>,
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

        let cancel = std::sync::Arc::new(AtomicBool::new(true));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));

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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(false));
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

        let cancel = std::sync::Arc::new(AtomicBool::new(true));
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
