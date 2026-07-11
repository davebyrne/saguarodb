use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use catalog::{
    CatalogManager, check_constraint_oid, index_oid, primary_key_constraint_oid,
    resolve_system_view, sequence_oid, synthetic_primary_key_oid, table_oid,
};
use common::{
    CatalogIntrospectionProvider, CheckpointGuard, ColumnDefault, ColumnInfo, CopyDirection,
    DataType, DbError, GucSetting, IndexConstraintKind, IsolationLevel, PgType, RelationKind,
    Result, SessionActivityRow, SessionInfo, SessionSequenceState, Snapshot, SqlState,
    SystemStateProvider, TableId, Value, WriteGuard,
};
use executor::{ExecutionContext, ExecutionResult, QueryEngine, RowSink};
use parser::Statement;
use planner::{
    BoundDistinct, BoundExpr, BoundFrom, BoundInsertSource, BoundOnConflict, BoundQuery,
    BoundQueryBody, BoundReturning, BoundSelect, BoundStatement, BoundValues,
    bind_parameterized_with_pg_types, collect_param_pg_types, format_explain, logical_plan,
    mutates_sequences, physical_plan, substitute_params,
};
use storage::RelationSnapshot;

use tokio::sync::mpsc;

use crate::app::ServerComponents;
use crate::registry::AdvertisedSnapshot;
use crate::session_registry::SessionRegistry;

mod alter;
mod copy;
mod cursor;
mod exec;
mod gucs;
mod stream;
mod truncate;
mod txn;
mod vacuum;

pub use copy::CopyInChunk;
pub(crate) use cursor::{CursorFetchStatus, QueryCursorHandle, StartedCursor};
pub use gucs::SessionGucs;
use stream::{ChannelRowSink, STREAM_BATCH_ROWS};
pub(crate) use stream::{CopySnapshots, STREAM_CHANNEL_CAPACITY, StreamMessage, StreamOutcome};
use txn::{CapturedSnapshots, ExecutionContextInput, StatementRuntime, TransactionSnapshots};
pub(crate) use vacuum::full_vacuum_pass;

pub struct QueryService {
    components: Arc<ServerComponents>,
    engine: QueryEngine,
}

/// Per-connection state borrowed by query execution for one statement. The
/// transaction slot and default isolation are still threaded separately because
/// statement execution consumes and returns them.
#[derive(Clone)]
pub struct QuerySessionContext {
    cancel: Arc<AtomicBool>,
    session_sequences: Arc<SessionSequenceState>,
    session_info: Arc<SessionInfo>,
    gucs: Arc<SessionGucs>,
    session_registry: Option<Arc<SessionRegistry>>,
    system_state_override: Option<Arc<dyn SystemStateProvider>>,
    catalog_introspection_override: Option<Arc<dyn CatalogIntrospectionProvider>>,
}

impl QuerySessionContext {
    pub fn new(
        cancel: Arc<AtomicBool>,
        session_sequences: Arc<SessionSequenceState>,
        session_info: Arc<SessionInfo>,
        gucs: Arc<SessionGucs>,
    ) -> Self {
        Self {
            cancel,
            session_sequences,
            session_info,
            gucs,
            session_registry: None,
            system_state_override: None,
            catalog_introspection_override: None,
        }
    }

    #[must_use]
    pub(crate) fn with_session_registry(mut self, session_registry: Arc<SessionRegistry>) -> Self {
        self.session_registry = Some(session_registry);
        self
    }

    #[must_use]
    pub fn with_system_state(mut self, system_state: Arc<dyn SystemStateProvider>) -> Self {
        self.system_state_override = Some(system_state);
        self
    }

    #[must_use]
    pub fn with_catalog_introspection(
        mut self,
        catalog_introspection: Arc<dyn CatalogIntrospectionProvider>,
    ) -> Self {
        self.catalog_introspection_override = Some(catalog_introspection);
        self
    }

    fn cancel(&self) -> &Arc<AtomicBool> {
        &self.cancel
    }

    fn session_sequences(&self) -> &SessionSequenceState {
        self.session_sequences.as_ref()
    }

    fn gucs(&self) -> &SessionGucs {
        self.gucs.as_ref()
    }

    fn statement_runtime(
        &self,
        default_isolation: IsolationLevel,
        transaction_isolation: IsolationLevel,
    ) -> StatementRuntime<'_> {
        let system_state = self.system_state_override.clone().unwrap_or_else(|| {
            Arc::new(QuerySystemState {
                gucs: self.gucs.clone(),
                session_registry: self.session_registry.clone(),
                default_isolation,
                transaction_isolation,
            })
        });
        StatementRuntime::new(
            &self.cancel,
            self.session_sequences.clone(),
            self.session_info.clone(),
        )
        .with_system_state(system_state)
        .with_catalog_introspection(
            self.catalog_introspection_override
                .clone()
                .unwrap_or_else(common::no_catalog_introspection),
        )
    }
}

#[derive(Debug)]
struct QuerySystemState {
    gucs: Arc<SessionGucs>,
    session_registry: Option<Arc<SessionRegistry>>,
    default_isolation: IsolationLevel,
    transaction_isolation: IsolationLevel,
}

impl SystemStateProvider for QuerySystemState {
    fn settings(&self) -> Vec<GucSetting> {
        self.gucs
            .settings(self.default_isolation, self.transaction_isolation)
    }

    fn sessions(&self) -> Vec<SessionActivityRow> {
        self.session_registry
            .as_ref()
            .map(|registry| registry.sessions())
            .unwrap_or_default()
    }
}

struct QueryCatalogIntrospection {
    catalog: Arc<dyn CatalogManager>,
    session_info: Arc<SessionInfo>,
}

impl std::fmt::Debug for QueryCatalogIntrospection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCatalogIntrospection")
            .field("session_info", &self.session_info)
            .finish_non_exhaustive()
    }
}

impl QueryCatalogIntrospection {
    fn user_relation_oid(&self, name: &str) -> Result<Option<i64>> {
        if let Some(table) = self.user_table_by_name(name)? {
            return Ok(Some(table_oid(table.id)));
        }
        if let Some(index) = self.catalog.get_index_by_name(name)?
            && self
                .catalog
                .get_table(index.table)?
                .is_some_and(|table| table.relation_kind == RelationKind::User)
        {
            return Ok(Some(index_oid(index.id)));
        }
        for table in self.catalog.list_tables()? {
            if table.relation_kind == RelationKind::User
                && !table.primary_key.is_empty()
                && primary_key_index_name(&table) == name
            {
                return Ok(Some(synthetic_primary_key_oid(table.id)));
            }
        }
        if let Some(sequence) = self.catalog.get_sequence_by_name(name)? {
            return Ok(Some(sequence_oid(sequence.id)));
        }
        Ok(None)
    }

    fn user_table_by_name(&self, name: &str) -> Result<Option<common::TableSchema>> {
        let Some((schema, relation)) = split_relation_name(name) else {
            return Ok(None);
        };
        if !matches!(schema.as_deref(), None | Some("public")) {
            return Ok(None);
        }
        let Some(table) = self.catalog.get_table_by_name(&relation)? else {
            return Ok(None);
        };
        if table.relation_kind == RelationKind::User {
            Ok(Some(table))
        } else {
            Ok(None)
        }
    }

    fn system_relation_oid(&self, schema: Option<&str>, name: &str) -> Option<i64> {
        resolve_system_view(schema, name).map(|view| view.relation_oid())
    }

    fn relation_oid_by_name(&self, name: &str) -> Result<Option<i64>> {
        let Some((schema, relation)) = split_relation_name(name) else {
            return Ok(None);
        };
        match schema.as_deref() {
            Some("public") => self.user_relation_oid(&relation),
            Some("pg_catalog") | Some("information_schema") => {
                Ok(self.system_relation_oid(schema.as_deref(), &relation))
            }
            Some(_) => Ok(None),
            None => self.user_relation_oid(&relation)?.map_or_else(
                || Ok(self.system_relation_oid(None, &relation)),
                |oid| Ok(Some(oid)),
            ),
        }
    }

    fn user_relation_is_visible(&self, relation_oid: i64) -> Result<bool> {
        for table in self.catalog.list_tables()? {
            if table.relation_kind != RelationKind::User {
                continue;
            }
            if table_oid(table.id) == relation_oid {
                return Ok(true);
            }
            let mut has_primary_key_index = false;
            for index in self.catalog.list_indexes_for_table(table.id)? {
                has_primary_key_index |= index.constraint == IndexConstraintKind::PrimaryKey;
                if index_oid(index.id) == relation_oid {
                    return Ok(true);
                }
            }
            if !has_primary_key_index
                && !table.primary_key.is_empty()
                && synthetic_primary_key_oid(table.id) == relation_oid
            {
                return Ok(true);
            }
        }
        for sequence in self.catalog.list_sequences()? {
            if sequence_oid(sequence.id) == relation_oid {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn system_relation_is_visible(&self, relation_oid: i64) -> Result<bool> {
        let Some(view) = catalog::SystemView::ALL.iter().copied().find(|view| {
            view.schema() == catalog::SystemSchema::PgCatalog && view.relation_oid() == relation_oid
        }) else {
            return Ok(false);
        };
        Ok(self.user_relation_oid(view.name())?.is_none())
    }

    fn serial_sequence_name(&self, table: &str, column: &str) -> Result<Option<String>> {
        let Some(table) = self.user_table_by_name(table)? else {
            return Ok(None);
        };
        let column = column.trim().to_ascii_lowercase();
        let Some(sequence_id) = table
            .columns
            .iter()
            .find(|definition| definition.name == column)
            .and_then(|definition| match definition.default {
                Some(ColumnDefault::Nextval(sequence_id)) => Some(sequence_id),
                _ => None,
            })
        else {
            return Ok(None);
        };
        let Some(sequence) = self.catalog.get_sequence(sequence_id)? else {
            return Ok(None);
        };
        Ok(sequence.owned.then_some(sequence.name))
    }

    fn index_definition(
        &self,
        index_oid_value: i64,
        column: Option<i64>,
    ) -> Result<Option<String>> {
        for table in self.catalog.list_tables()? {
            if table.relation_kind != RelationKind::User {
                continue;
            }
            let mut has_primary_key_index = false;
            for index in self.catalog.list_indexes_for_table(table.id)? {
                has_primary_key_index |= index.constraint == IndexConstraintKind::PrimaryKey;
                if index_oid(index.id) == index_oid_value {
                    return Ok(render_index_definition(
                        &index.name,
                        &table,
                        &index.columns,
                        index.unique,
                        column,
                    ));
                }
            }
            if !has_primary_key_index
                && !table.primary_key.is_empty()
                && synthetic_primary_key_oid(table.id) == index_oid_value
            {
                return Ok(render_index_definition(
                    &primary_key_index_name(&table),
                    &table,
                    &table.primary_key,
                    true,
                    column,
                ));
            }
        }
        Ok(None)
    }

    fn constraint_definition(&self, constraint_oid: i64) -> Result<Option<String>> {
        for table in self.catalog.list_tables()? {
            if table.relation_kind != RelationKind::User {
                continue;
            }
            if !table.primary_key.is_empty()
                && primary_key_constraint_oid(table.id) == constraint_oid
            {
                return Ok(Some(format!(
                    "PRIMARY KEY ({})",
                    column_names(&table, &table.primary_key).join(", ")
                )));
            }
            for (index, check) in table.checks.iter().enumerate() {
                let check_index: u16 = index.try_into().unwrap_or(u16::MAX);
                if check_constraint_oid(table.id, check_index) == constraint_oid {
                    return Ok(Some(format!("CHECK ({check})")));
                }
            }
        }
        Ok(None)
    }
}

impl CatalogIntrospectionProvider for QueryCatalogIntrospection {
    fn pg_get_indexdef(
        &self,
        index_oid: i64,
        column: Option<i64>,
        _pretty: bool,
    ) -> Result<Option<String>> {
        self.index_definition(index_oid, column)
    }

    fn pg_get_constraintdef(&self, constraint_oid: i64, _pretty: bool) -> Result<Option<String>> {
        self.constraint_definition(constraint_oid)
    }

    fn pg_get_userbyid(&self, role_oid: i64) -> Result<Option<String>> {
        Ok((role_oid == 10).then(|| self.session_info.user.clone()))
    }

    fn pg_table_is_visible(&self, relation_oid: i64) -> Result<bool> {
        if self.user_relation_is_visible(relation_oid)?
            || self.system_relation_is_visible(relation_oid)?
        {
            return Ok(true);
        }
        Ok(false)
    }

    fn to_regclass(&self, name: &str) -> Result<Option<i64>> {
        self.relation_oid_by_name(name)
    }

    fn pg_get_serial_sequence(&self, table: &str, column: &str) -> Result<Option<String>> {
        self.serial_sequence_name(table, column)
    }
}

fn primary_key_index_name(table: &common::TableSchema) -> String {
    format!("{}_pkey", table.name)
}

fn render_index_definition(
    index_name: &str,
    table: &common::TableSchema,
    columns: &[u16],
    unique: bool,
    column: Option<i64>,
) -> Option<String> {
    let names = column_names(table, columns);
    if let Some(column) = column {
        return match column {
            0 => Some(render_full_index_definition(
                index_name, table, &names, unique,
            )),
            1.. => names.get((column - 1) as usize).cloned(),
            _ => None,
        };
    }
    Some(render_full_index_definition(
        index_name, table, &names, unique,
    ))
}

fn render_full_index_definition(
    index_name: &str,
    table: &common::TableSchema,
    names: &[String],
    unique: bool,
) -> String {
    let unique = if unique { "UNIQUE " } else { "" };
    format!(
        "CREATE {unique}INDEX {index_name} ON public.{} USING btree ({})",
        table.name,
        names.join(", ")
    )
}

fn column_names(table: &common::TableSchema, columns: &[u16]) -> Vec<String> {
    columns
        .iter()
        .filter_map(|column| {
            table
                .columns
                .iter()
                .find(|candidate| candidate.id == *column)
                .map(|definition| definition.name.clone())
        })
        .collect()
}

fn split_relation_name(name: &str) -> Option<(Option<String>, String)> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut parts = trimmed.split('.');
    let first = parts.next()?;
    let Some(second) = parts.next() else {
        return (!first.is_empty()).then_some((None, first.to_ascii_lowercase()));
    };
    if parts.next().is_some() || first.is_empty() || second.is_empty() {
        return None;
    }
    Some((
        Some(first.to_ascii_lowercase()),
        second.to_ascii_lowercase(),
    ))
}

/// The concurrency guard an autocommit write/DDL unit holds for its lifetime. DML
/// takes the SHARED writer guard (concurrent with other writers); DDL takes the
/// EXCLUSIVE guard because catalog rollback restores whole object maps and CREATE
/// INDEX also needs a stable physical view for its HOT broken-chain backfill
/// (`docs/specs/mvcc.md` §10 Milestone H2). Dropping this drops the inner guard
/// either way, so the autocommit path holds one variable across execution + commit.
/// The inner guards are held purely for their RAII `Drop` (they own the lock), never
/// read — like `WriteGuard`/`CheckpointGuard`'s own `_guard` fields.
#[allow(dead_code, reason = "guards are held for RAII Drop, never read")]
pub(crate) enum WriteUnitGuard {
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
    /// drives `snapshots_for_transaction`: Read Committed captures a fresh snapshot
    /// per statement, Repeatable Read captures one at the first statement and
    /// reuses it. The paired relation snapshot follows the same rule so
    /// relation-swap DDL cannot move a stable transaction onto a newer physical
    /// generation.
    isolation: IsolationLevel,
    /// Transactional changes to `default_transaction_isolation`. PostgreSQL makes a
    /// plain `SET` visible immediately but only persists it if the surrounding
    /// transaction commits; `SET LOCAL` is visible only until transaction end.
    default_isolation_override: Option<DefaultIsolationOverride>,
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
    /// The relation-generation snapshot paired with `rr_snapshot`. Relation-swap
    /// DDL such as truncate publishes new storage generations without changing
    /// logical table ids, so Repeatable Read / Serializable must keep resolving
    /// reads against the same relation generations for the whole transaction.
    /// `None` under Read Committed.
    rr_relations: Option<Arc<dyn RelationSnapshot>>,
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

#[derive(Clone, Copy)]
struct DefaultIsolationOverride {
    current: IsolationLevel,
    on_commit: Option<IsolationLevel>,
}

/// One open savepoint: its name and the subxid that owns writes made under it.
struct SavepointLevel {
    name: String,
    subxid: u64,
    default_isolation_override: Option<DefaultIsolationOverride>,
}

impl Transaction {
    fn current_default_isolation(&self, session_default: IsolationLevel) -> IsolationLevel {
        self.default_isolation_override
            .map(|override_state| override_state.current)
            .unwrap_or(session_default)
    }

    fn committed_default_isolation(&self, session_default: IsolationLevel) -> IsolationLevel {
        self.default_isolation_override
            .and_then(|override_state| override_state.on_commit)
            .unwrap_or(session_default)
    }

    fn set_default_isolation(&mut self, level: IsolationLevel) {
        self.default_isolation_override = Some(DefaultIsolationOverride {
            current: level,
            on_commit: Some(level),
        });
    }

    fn set_local_default_isolation(&mut self, level: IsolationLevel) {
        let on_commit = self
            .default_isolation_override
            .and_then(|override_state| override_state.on_commit);
        self.default_isolation_override = Some(DefaultIsolationOverride {
            current: level,
            on_commit,
        });
    }

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
    pub(crate) fn mark_failed(&mut self) {
        self.failed = true;
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.failed
    }

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

    fn catalog_introspection_provider(
        &self,
        session_info: Arc<SessionInfo>,
    ) -> Arc<dyn CatalogIntrospectionProvider> {
        Arc::new(QueryCatalogIntrospection {
            catalog: self.components.catalog.clone(),
            session_info,
        })
    }

    fn with_catalog_introspection(&self, session: QuerySessionContext) -> QuerySessionContext {
        if session.catalog_introspection_override.is_some() {
            return session;
        }
        let session_info = session.session_info.clone();
        session.with_catalog_introspection(self.catalog_introspection_provider(session_info))
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
    ///
    /// This convenience uses a fresh, throwaway [`SessionSequenceState`] per call, so
    /// `currval` is only defined within the single statement that called `nextval` —
    /// there is no cross-statement `currval` memory. It is for autocommit and tests.
    /// A real connection that must persist `currval` across statements calls
    /// [`Self::execute_simple_with_session_sequences`] with its own session state
    /// (see `connection/simple.rs`).
    pub fn execute_simple(
        &self,
        sql: &str,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        cancel: &Arc<AtomicBool>,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        self.execute_simple_with_session_sequences(
            sql,
            slot,
            default_isolation,
            cancel,
            Arc::new(SessionSequenceState::new()),
            Arc::new(SessionGucs::default()),
        )
    }

    pub fn execute_simple_with_session_sequences(
        &self,
        sql: &str,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        cancel: &Arc<AtomicBool>,
        session_sequences: Arc<SessionSequenceState>,
        gucs: Arc<SessionGucs>,
    ) -> (Option<Transaction>, IsolationLevel, Result<ExecutionResult>) {
        let parsed = match parser::parse(sql) {
            Ok(parsed) => parsed,
            // A syntax error inside an open transaction poisons the block to the
            // failed state, matching PostgreSQL (the block must be ended before any
            // further command is accepted). Autocommit (`None`) is unaffected. The
            // session default is unchanged by a failed parse.
            Err(err) => return (mark_failed_on_error(slot), default_isolation, Err(err)),
        };
        // No row sink: every SELECT materializes into `ExecutionResult::Query`, so
        // the outcome is always `Direct`.
        let session = QuerySessionContext::new(
            cancel.clone(),
            session_sequences,
            Arc::new(SessionInfo::default()),
            gucs,
        );
        let session = self.with_catalog_introspection(session);
        let (slot, default_isolation, result) =
            self.dispatch(parsed, slot, default_isolation, &session, None);
        let result = result.and_then(StreamOutcome::into_direct_result);
        let slot = if result.is_err() {
            mark_failed_on_error(slot)
        } else {
            slot
        };
        (slot, default_isolation, result)
    }

    /// The streaming counterpart of [`Self::execute_simple_with_session_sequences`]:
    /// a `SELECT` streams its rows through `row_tx` (as `StreamMessage::Start`
    /// followed by `StreamMessage::Rows` batches) and returns
    /// [`StreamOutcome::Streamed`]; every other statement returns a non-streamed
    /// outcome with its full result. The blocking
    /// producer owns the executor and the channel sender for the whole call, so
    /// the snapshot's GC-horizon advertisement and any transaction guard are held
    /// across the stream, exactly as on the materializing path
    /// (`docs/specs/streaming.md` §4, §5).
    pub(crate) fn execute_simple_streamed(
        &self,
        sql: &str,
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        session: QuerySessionContext,
        row_tx: mpsc::Sender<StreamMessage>,
    ) -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>) {
        let session = self.with_catalog_introspection(session);
        let parsed = match parser::parse(sql) {
            Err(err) => return (mark_failed_on_error(slot), default_isolation, Err(err)),
            Ok(parsed) => parsed,
        };
        // The sink owns `row_tx` for the whole dispatch; when it drops (as this
        // function returns) the channel closes, ending the consumer's drain loop.
        let mut sink = ChannelRowSink::new(row_tx);
        self.dispatch(parsed, slot, default_isolation, &session, Some(&mut sink))
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
        declared_param_types: &[Option<PgType>],
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
        if let StatementClass::SqlCursor = class {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "SQL cursors require the simple query protocol",
            ));
        }
        if let StatementClass::TransactionControl(_) = class {
            // BEGIN/COMMIT/ROLLBACK take no parameters and produce no rows; they do
            // not bind. Carry the prepared statement with a no-op bound payload so
            // an extended-protocol `Execute` can route it through the session's
            // transaction lifecycle (`handle_transaction_control`) exactly like the
            // simple-query path, rather than as an independent autocommit unit.
            return Ok(PreparedStatement {
                sql: sql.to_string(),
                class,
                bound: None,
                schema_versions: Vec::new(),
                maintenance: None,
                session_config: None,
                param_pg_types: Vec::new(),
                result_columns: None,
            });
        }
        if let StatementClass::Maintenance = class {
            // Maintenance commands take no parameters, produce no rows, and do not
            // bind/plan. Carry the parsed statement so an extended-protocol `Execute`
            // routes it through `run_maintenance`, exactly like the simple path.
            return Ok(PreparedStatement {
                sql: sql.to_string(),
                class,
                bound: None,
                schema_versions: Vec::new(),
                maintenance: Some(statement),
                session_config: None,
                param_pg_types: Vec::new(),
                result_columns: None,
            });
        }
        if let StatementClass::SessionConfig = class {
            let result_columns = gucs::session_config_result_columns(&statement);
            return Ok(PreparedStatement {
                sql: sql.to_string(),
                class,
                bound: None,
                schema_versions: Vec::new(),
                maintenance: None,
                session_config: Some(statement),
                param_pg_types: Vec::new(),
                result_columns,
            });
        }
        let _schema_guard = self.components.concurrency.begin_shared()?;
        let (bound, param_types) = bind_parameterized_with_pg_types(
            &statement,
            self.components.catalog.as_ref(),
            declared_param_types,
        )?;
        // Remember each parameter's wire type so `ParameterDescription` echoes the
        // OID the client declared, or the more specific wire type inferred from
        // catalog function arguments.
        let param_pg_types = collect_param_pg_types(&bound, &param_types, declared_param_types)?;
        let schema_versions = prepared_schema_versions(&bound, self.components.catalog.as_ref())?;
        let result_columns = result_columns(&bound);
        Ok(PreparedStatement {
            sql: sql.to_string(),
            class,
            bound: Some(bound),
            schema_versions,
            maintenance: None,
            session_config: None,
            param_pg_types,
            result_columns,
        })
    }

    /// Execute a prepared statement with one value per parameter, in order. Each
    /// call is its own autocommit unit, like a simple query, with a throwaway
    /// session (see [`Self::execute_prepared_cancelable`]).
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
    /// [`Self::execute_prepared_in_session_with_context`] instead, so the
    /// autocommit write path here is never reached while the session already holds
    /// the write guard.
    ///
    /// Like [`Self::execute_simple`], this uses a fresh throwaway
    /// [`SessionSequenceState`] (autocommit/tests). A real connection calls
    /// [`Self::execute_prepared_cancelable_with_session_context`] with its own
    /// session state (see `connection/extended.rs`).
    pub fn execute_prepared_cancelable(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        cancel: &Arc<AtomicBool>,
    ) -> Result<ExecutionResult> {
        let session = QuerySessionContext::new(
            cancel.clone(),
            Arc::new(SessionSequenceState::new()),
            Arc::new(SessionInfo::default()),
            Arc::new(SessionGucs::default()),
        );
        let session = self.with_catalog_introspection(session);
        self.execute_prepared_cancelable_with_session_context(
            prepared,
            params,
            &session,
            IsolationLevel::default(),
            None,
        )
        .and_then(StreamOutcome::into_direct_result)
    }

    pub(crate) fn execute_prepared_cancelable_with_session_context(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        session: &QuerySessionContext,
        default_isolation: IsolationLevel,
        // `Some` streams a SELECT's rows into the sink; `None` materializes.
        sink: Option<&mut dyn RowSink>,
    ) -> Result<StreamOutcome> {
        let session = self.with_catalog_introspection(session.clone());
        // Maintenance does not bind/plan; run it before parameter substitution. The
        // connection routes maintenance through the in-session variant, so this
        // arm is reached only if a caller bypasses that routing — keep it total.
        if let StatementClass::Maintenance = prepared.class {
            return self
                .run_prepared_maintenance(prepared)
                .map(StreamOutcome::Direct);
        }
        if let StatementClass::SessionConfig = prepared.class {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "session configuration statements require session execution context",
            ));
        }
        let bound = self.substitute_prepared_params(prepared, params)?;
        let class = classify_bound(prepared.class, &bound);
        match prepared.class {
            StatementClass::Read => {
                let captured = self.capture_consistent_snapshots(0)?;
                match class {
                    StatementClass::Read => {
                        self.validate_prepared_schema_versions(&prepared.schema_versions)?;
                        self.autocommit_read_with_snapshot(
                            bound,
                            session.statement_runtime(default_isolation, default_isolation),
                            sink,
                            captured,
                        )
                    }
                    // A read promoted to a write (e.g. `SELECT nextval(...)`) is
                    // materialized, not streamed.
                    StatementClass::Write => self
                        .autocommit_prepared_bound_write_with_snapshot(
                            bound,
                            session.statement_runtime(default_isolation, default_isolation),
                            Some(&prepared.schema_versions),
                            captured,
                        )
                        .map(StreamOutcome::Direct),
                    _ => unreachable!("classify_bound only promotes reads to writes"),
                }
            }
            StatementClass::Write | StatementClass::Ddl => self
                .autocommit_prepared_bound_write(
                    bound,
                    session.statement_runtime(default_isolation, default_isolation),
                    Some(&prepared.schema_versions),
                )
                .map(StreamOutcome::Direct),
            StatementClass::Maintenance => {
                unreachable!("maintenance is dispatched above before substitution")
            }
            StatementClass::SessionConfig => {
                unreachable!("session configuration is dispatched above before substitution")
            }
            StatementClass::SqlCursor => Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "SQL cursors require the simple query protocol",
            )),
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

    /// Streaming counterpart of
    /// [`Self::execute_prepared_cancelable_with_session_context`]: a SELECT
    /// streams its rows through `row_tx` and returns [`StreamOutcome::Streamed`];
    /// everything else returns a non-streamed outcome. For the autocommit
    /// extended-protocol `Execute` path (`connection/extended.rs`).
    pub(crate) fn execute_prepared_cancelable_streamed(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        session: QuerySessionContext,
        default_isolation: IsolationLevel,
        row_tx: mpsc::Sender<StreamMessage>,
    ) -> Result<StreamOutcome> {
        let session = self.with_catalog_introspection(session);
        let mut sink = ChannelRowSink::new(row_tx);
        self.execute_prepared_cancelable_with_session_context(
            prepared,
            params,
            &session,
            default_isolation,
            Some(&mut sink),
        )
    }

    /// Execute a prepared statement against the session's open explicit
    /// transaction `slot`, returning the (possibly mutated) slot alongside the
    /// result. This is the extended-protocol counterpart of `execute_simple`: it
    /// routes a data statement through the SAME in-transaction machinery the simple
    /// path uses (`run_bound_in_transaction`), so the open transaction's single
    /// write guard is reused — never re-acquired — and the transaction's
    /// snapshot/isolation and 'E' failed-state gating apply. Transaction-control
    /// statements are dispatched through `handle_transaction_control`, exactly like
    /// a simple `BEGIN`/`COMMIT`/`ROLLBACK`. `session` carries the connection's
    /// persistent `currval`, startup identity, cancellation, and GUC state.
    ///
    /// Precondition: `slot` is `Some` (the connection only calls this with an open
    /// transaction; with no open transaction it uses the autocommit
    /// `execute_prepared_cancelable_with_session_context`).
    pub(crate) fn execute_prepared_in_session_with_context(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        session: &QuerySessionContext,
        // `Some` streams a SELECT's rows into the sink; `None` materializes.
        sink: Option<&mut dyn RowSink>,
    ) -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>) {
        let session = self.with_catalog_introspection(session.clone());
        if let StatementClass::TransactionControl(kind) = prepared.class {
            let (slot, default_isolation, result) =
                self.handle_transaction_control(kind, slot, default_isolation, session.cancel());
            return (slot, default_isolation, result.map(StreamOutcome::Direct));
        }

        if let StatementClass::SessionConfig = prepared.class {
            let statement = match prepared.session_config.clone() {
                Some(statement) => statement,
                None => {
                    return (
                        slot,
                        default_isolation,
                        Err(DbError::internal(
                            "prepared session-configuration statement has no payload",
                        )),
                    );
                }
            };
            let resets_session_objects = matches!(statement, Statement::DiscardAll);
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
                } else {
                    StreamOutcome::Direct(result)
                }
            });
            return (slot, default_isolation, result);
        }

        // Maintenance does not bind/plan: dispatch it before parameter substitution.
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
                        "maintenance commands cannot run inside a transaction block",
                    )),
                );
            }
            return (
                None,
                default_isolation,
                self.run_prepared_maintenance(prepared)
                    .map(StreamOutcome::Direct),
            );
        }

        let bound = match self.substitute_prepared_params(prepared, params) {
            Ok(bound) => bound,
            // A parameter-count/substitution error inside an open transaction
            // poisons it to the failed state, matching the simple-query path.
            Err(err) => return (mark_failed_on_error(slot), default_isolation, Err(err)),
        };

        match slot {
            Some(txn) => {
                let class = classify_bound(prepared.class, &bound);
                let runtime = session.statement_runtime(
                    txn.current_default_isolation(default_isolation),
                    txn.isolation,
                );
                let (slot, result) = self.run_bound_in_transaction(
                    txn,
                    class,
                    BindSource::Bound {
                        bound,
                        schema_versions: prepared.schema_versions.clone(),
                    },
                    runtime,
                    sink,
                );
                (slot, default_isolation, result)
            }
            // No open transaction: fall back to an autocommit unit (the connection
            // routes here only when a transaction is open, but keep this total so
            // the contract holds regardless of caller).
            None => {
                let result = match prepared.class {
                    StatementClass::Read => {
                        let class = classify_bound(prepared.class, &bound);
                        let captured = match self.capture_consistent_snapshots(0) {
                            Ok(captured) => captured,
                            Err(err) => return (None, default_isolation, Err(err)),
                        };
                        match class {
                            StatementClass::Read => {
                                match self
                                    .validate_prepared_schema_versions(&prepared.schema_versions)
                                {
                                    Ok(()) => self.autocommit_read_with_snapshot(
                                        bound,
                                        session.statement_runtime(
                                            default_isolation,
                                            default_isolation,
                                        ),
                                        sink,
                                        captured,
                                    ),
                                    Err(err) => Err(err),
                                }
                            }
                            StatementClass::Write => self
                                .autocommit_prepared_bound_write_with_snapshot(
                                    bound,
                                    session.statement_runtime(default_isolation, default_isolation),
                                    Some(&prepared.schema_versions),
                                    captured,
                                )
                                .map(StreamOutcome::Direct),
                            _ => unreachable!("classify_bound only promotes reads to writes"),
                        }
                    }
                    StatementClass::Write | StatementClass::Ddl => self
                        .autocommit_prepared_bound_write(
                            bound,
                            session.statement_runtime(default_isolation, default_isolation),
                            Some(&prepared.schema_versions),
                        )
                        .map(StreamOutcome::Direct),
                    StatementClass::Maintenance => {
                        unreachable!("maintenance is dispatched above before substitution")
                    }
                    StatementClass::SessionConfig => {
                        unreachable!(
                            "session configuration is dispatched above before substitution"
                        )
                    }
                    StatementClass::TransactionControl(_) => {
                        unreachable!("transaction control is dispatched above before substitution")
                    }
                    StatementClass::SqlCursor => {
                        unreachable!("SQL cursors are rejected at prepare time")
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

    /// Streaming counterpart of
    /// [`Self::execute_prepared_in_session_with_context`]: a SELECT
    /// streams its rows through `row_tx`; every other statement returns
    /// a non-streamed outcome. For the in-transaction extended-protocol
    /// `Execute` path (`connection/extended.rs`).
    pub(crate) fn execute_prepared_in_session_streamed(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        slot: Option<Transaction>,
        default_isolation: IsolationLevel,
        session: QuerySessionContext,
        row_tx: mpsc::Sender<StreamMessage>,
    ) -> (Option<Transaction>, IsolationLevel, Result<StreamOutcome>) {
        let mut sink = ChannelRowSink::new(row_tx);
        self.execute_prepared_in_session_with_context(
            prepared,
            params,
            slot,
            default_isolation,
            &session,
            Some(&mut sink),
        )
    }

    /// Return true only when this prepared statement still classifies as a
    /// read-only SELECT after parameter substitution. If substitution itself
    /// fails, let the normal execute path surface that error instead of changing
    /// error ordering during portal-routing selection.
    pub(crate) fn prepared_supports_read_only_portal_suspension(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> bool {
        if !matches!(prepared.class, StatementClass::Read) {
            return false;
        }
        let Ok(bound) = self.substitute_prepared_params(prepared, params) else {
            return false;
        };
        matches!(bound, BoundStatement::Query(_))
            && matches!(classify_bound(prepared.class, &bound), StatementClass::Read)
    }

    /// Validate the parameter count and substitute `params` into a prepared
    /// statement's bound payload. Transaction-control statements carry no bound
    /// payload, so substitution is only valid for data statements.
    fn substitute_prepared_params(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<BoundStatement> {
        if params.len() != prepared.param_pg_types.len() {
            return Err(DbError::protocol(
                SqlState::SyntaxError,
                format!(
                    "prepared statement requires {} parameter(s), but {} were supplied",
                    prepared.param_pg_types.len(),
                    params.len()
                ),
            ));
        }
        let bound = prepared.bound.as_ref().ok_or_else(|| {
            DbError::internal("prepared transaction-control statement has no bound payload")
        })?;
        substitute_params(bound, params)
    }

    pub(super) fn validate_prepared_schema_versions(
        &self,
        schema_versions: &[(TableId, u64)],
    ) -> Result<()> {
        for (relation, prepared_version) in schema_versions {
            let current_version =
                relation_schema_version(self.components.catalog.as_ref(), *relation)?
                    .ok_or_else(prepared_schema_changed_error)?;
            if current_version != *prepared_version {
                return Err(prepared_schema_changed_error());
            }
        }
        Ok(())
    }

    pub(super) fn validate_relation_snapshot_schema_versions(
        &self,
        relations: &dyn RelationSnapshot,
        schema_versions: &[(TableId, u64)],
        allow_missing_tables: bool,
    ) -> Result<()> {
        for (table_id, bound_version) in schema_versions {
            match relations.table_schema_version(*table_id) {
                Some(snapshot_version) if snapshot_version == *bound_version => {}
                None if self
                    .components
                    .catalog
                    .get_view(*table_id)?
                    .is_some_and(|view| view.schema_version == *bound_version) => {}
                None if allow_missing_tables && relations.missing_tables_are_empty() => {}
                _ => return Err(relation_snapshot_schema_changed_error(*table_id)),
            }
        }
        Ok(())
    }

    pub(super) fn validate_current_schema_versions(
        &self,
        schema_versions: &[(TableId, u64)],
    ) -> Result<()> {
        for (table_id, bound_version) in schema_versions {
            let current_version =
                relation_schema_version(self.components.catalog.as_ref(), *table_id)?
                    .ok_or_else(|| relation_snapshot_schema_changed_error(*table_id))?;
            if current_version != *bound_version {
                return Err(relation_snapshot_schema_changed_error(*table_id));
            }
        }
        Ok(())
    }

    pub(super) fn validate_relation_snapshot_current_for_write(
        &self,
        relations: &dyn RelationSnapshot,
    ) -> Result<()> {
        let current_epoch = self.components.storage.relation_epoch()?;
        if relations.relation_epoch() != current_epoch {
            return Err(DbError::execute(
                SqlState::SerializationFailure,
                "relation generation changed while statement snapshot was captured; retry",
            ));
        }
        Ok(())
    }
}

fn prepared_schema_changed_error() -> DbError {
    DbError::plan(
        SqlState::FeatureNotSupported,
        "cached plan must be reprepared after schema change",
    )
}

fn relation_snapshot_schema_changed_error(table_id: TableId) -> DbError {
    DbError::execute(
        SqlState::SerializationFailure,
        format!("table id {table_id} changed while statement snapshot was captured; retry"),
    )
}

fn prepared_schema_versions(
    bound: &BoundStatement,
    catalog: &dyn catalog::CatalogManager,
) -> Result<Vec<(TableId, u64)>> {
    let mut relations = BTreeMap::new();
    collect_bound_statement_tables(bound, &mut relations)?;
    relations
        .into_iter()
        .map(|(relation, bound_version)| {
            let schema_version = match bound_version {
                Some(schema_version) => schema_version,
                None => relation_schema_version(catalog, relation)?
                    .ok_or_else(prepared_schema_changed_error)?,
            };
            Ok((relation, schema_version))
        })
        .collect()
}

fn relation_schema_version(
    catalog: &dyn catalog::CatalogManager,
    relation: TableId,
) -> Result<Option<u64>> {
    if let Some(table) = catalog.get_table(relation)? {
        return Ok(Some(table.schema_version));
    }
    if let Some(view) = catalog.get_view(relation)? {
        return Ok(Some(view.schema_version));
    }
    Ok(None)
}

type BoundRelationVersions = BTreeMap<TableId, Option<u64>>;

fn record_bound_relation_version(
    relations: &mut BoundRelationVersions,
    relation: TableId,
    schema_version: Option<u64>,
) -> Result<()> {
    let Some(existing) = relations.get_mut(&relation) else {
        relations.insert(relation, schema_version);
        return Ok(());
    };
    match (*existing, schema_version) {
        (Some(existing), Some(incoming)) if existing != incoming => {
            Err(DbError::internal(format!(
                "bound plan references relation id {relation} at conflicting schema versions \
                 {existing} and {incoming}"
            )))
        }
        (None, Some(incoming)) => {
            *existing = Some(incoming);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_bound_statement_tables(
    bound: &BoundStatement,
    relations: &mut BoundRelationVersions,
) -> Result<()> {
    match bound {
        BoundStatement::Query(query) => collect_query_tables(query, relations)?,
        BoundStatement::Insert {
            table,
            source,
            on_conflict,
            returning,
            default_exprs,
            check_exprs,
            ..
        } => {
            record_bound_relation_version(relations, *table, None)?;
            collect_insert_source_tables(source, relations)?;
            if let Some(on_conflict) = on_conflict {
                collect_on_conflict_tables(on_conflict, relations)?;
            }
            if let Some(returning) = returning {
                collect_returning_tables(returning, relations)?;
            }
            for (_, expr) in default_exprs {
                collect_expr_tables(expr, relations)?;
            }
            for expr in check_exprs {
                collect_expr_tables(expr, relations)?;
            }
        }
        BoundStatement::Update {
            table,
            assignments,
            source,
            joined_source: _,
            returning,
            check_exprs,
        } => {
            record_bound_relation_version(relations, *table, None)?;
            for (_, expr) in assignments {
                collect_expr_tables(expr, relations)?;
            }
            collect_select_tables(source, relations)?;
            if let Some(returning) = returning {
                collect_returning_tables(returning, relations)?;
            }
            for expr in check_exprs {
                collect_expr_tables(expr, relations)?;
            }
        }
        BoundStatement::Delete {
            table,
            source,
            joined_source: _,
            returning,
        } => {
            record_bound_relation_version(relations, *table, None)?;
            collect_select_tables(source, relations)?;
            if let Some(returning) = returning {
                collect_returning_tables(returning, relations)?;
            }
        }
        BoundStatement::Explain(inner) => collect_bound_statement_tables(inner, relations)?,
        BoundStatement::AlterTableAddColumn { table, .. }
        | BoundStatement::AlterTableDropColumn { table, .. }
        | BoundStatement::AlterTableRenameColumn { table, .. }
        | BoundStatement::AlterTableRenameTable { table, .. } => {
            record_bound_relation_version(relations, *table, None)?;
        }
        BoundStatement::CreateTable { .. }
        | BoundStatement::DropTable { .. }
        | BoundStatement::CreateIndex { .. }
        | BoundStatement::DropIndex { .. }
        | BoundStatement::CreateSequence { .. }
        | BoundStatement::DropSequence { .. }
        | BoundStatement::DropView { .. } => {}
        BoundStatement::Copy { table, .. } => {
            record_bound_relation_version(relations, *table, None)?;
        }
        BoundStatement::CreateView {
            query,
            dependencies,
            ..
        } => {
            collect_query_tables(query, relations)?;
            for dependency in dependencies {
                record_bound_relation_version(relations, dependency.relation, None)?;
            }
        }
    }
    Ok(())
}

fn collect_query_tables(query: &BoundQuery, relations: &mut BoundRelationVersions) -> Result<()> {
    match &query.body {
        BoundQueryBody::Select(select) => collect_select_tables(select, relations)?,
        BoundQueryBody::Values(values) => collect_values_tables(values, relations)?,
        BoundQueryBody::SetOp(set_op) => {
            collect_query_tables(&set_op.left, relations)?;
            collect_query_tables(&set_op.right, relations)?;
        }
    }
    for order_by in &query.order_by {
        collect_expr_tables(&order_by.expr, relations)?;
    }
    Ok(())
}

fn collect_values_tables(
    values: &BoundValues,
    relations: &mut BoundRelationVersions,
) -> Result<()> {
    for expr in values.rows.iter().flatten() {
        collect_expr_tables(expr, relations)?;
    }
    Ok(())
}

fn collect_select_tables(
    select: &BoundSelect,
    relations: &mut BoundRelationVersions,
) -> Result<()> {
    if let Some(distinct) = &select.distinct {
        collect_distinct_tables(distinct, relations)?;
    }
    for item in &select.columns {
        collect_expr_tables(&item.expr, relations)?;
    }
    if let Some(from) = &select.from {
        collect_from_tables(from, relations)?;
    }
    if let Some(filter) = &select.filter {
        collect_expr_tables(filter, relations)?;
    }
    for expr in &select.group_by {
        collect_expr_tables(expr, relations)?;
    }
    if let Some(having) = &select.having {
        collect_expr_tables(having, relations)?;
    }
    Ok(())
}

fn collect_distinct_tables(
    distinct: &BoundDistinct,
    relations: &mut BoundRelationVersions,
) -> Result<()> {
    match distinct {
        BoundDistinct::All => {}
        BoundDistinct::On(exprs) => {
            for expr in exprs {
                collect_expr_tables(expr, relations)?;
            }
        }
    }
    Ok(())
}

fn collect_from_tables(from: &BoundFrom, relations: &mut BoundRelationVersions) -> Result<()> {
    match from {
        BoundFrom::Table { table, .. } => {
            record_bound_relation_version(relations, *table, None)?;
        }
        BoundFrom::System { .. } => {}
        BoundFrom::Derived { query, .. } => collect_query_tables(query, relations)?,
        BoundFrom::View {
            view,
            schema_version,
            query,
            ..
        } => {
            record_bound_relation_version(relations, *view, Some(*schema_version))?;
            collect_query_tables(query, relations)?;
        }
        BoundFrom::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_from_tables(left, relations)?;
            collect_from_tables(right, relations)?;
            if let Some(condition) = condition {
                collect_expr_tables(condition, relations)?;
            }
        }
    }
    Ok(())
}

fn collect_insert_source_tables(
    source: &BoundInsertSource,
    relations: &mut BoundRelationVersions,
) -> Result<()> {
    match source {
        BoundInsertSource::Values { rows, .. } => {
            for expr in rows.iter().flatten() {
                collect_expr_tables(expr, relations)?;
            }
        }
        BoundInsertSource::Query(query) => collect_query_tables(query, relations)?,
    }
    Ok(())
}

fn collect_on_conflict_tables(
    on_conflict: &BoundOnConflict,
    relations: &mut BoundRelationVersions,
) -> Result<()> {
    match on_conflict {
        BoundOnConflict::DoNothing { .. } => {}
        BoundOnConflict::DoUpdate {
            assignments,
            filter,
            ..
        } => {
            for (_, expr) in assignments {
                collect_expr_tables(expr, relations)?;
            }
            if let Some(filter) = filter {
                collect_expr_tables(filter, relations)?;
            }
        }
    }
    Ok(())
}

fn collect_returning_tables(
    returning: &BoundReturning,
    relations: &mut BoundRelationVersions,
) -> Result<()> {
    for expr in &returning.exprs {
        collect_expr_tables(expr, relations)?;
    }
    Ok(())
}

fn collect_expr_tables(expr: &BoundExpr, relations: &mut BoundRelationVersions) -> Result<()> {
    match expr {
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::AggregateCall { arg: None, .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. } => {}
        BoundExpr::BinaryOp { left, right, .. } => {
            collect_expr_tables(left, relations)?;
            collect_expr_tables(right, relations)?;
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => collect_expr_tables(expr, relations)?,
        BoundExpr::Function { args, .. } => {
            for arg in args {
                collect_expr_tables(arg, relations)?;
            }
        }
        BoundExpr::Setval {
            value, is_called, ..
        } => {
            collect_expr_tables(value, relations)?;
            if let Some(is_called) = is_called {
                collect_expr_tables(is_called, relations)?;
            }
        }
        BoundExpr::AggregateCall { arg: Some(arg), .. } => collect_expr_tables(arg, relations)?,
        BoundExpr::InList { expr, list, .. } => {
            collect_expr_tables(expr, relations)?;
            for item in list {
                collect_expr_tables(item, relations)?;
            }
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_tables(expr, relations)?;
            collect_expr_tables(low, relations)?;
            collect_expr_tables(high, relations)?;
        }
        BoundExpr::Like { expr, pattern, .. } => {
            collect_expr_tables(expr, relations)?;
            collect_expr_tables(pattern, relations)?;
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_tables(operand, relations)?;
            }
            for (when, then) in when_clauses {
                collect_expr_tables(when, relations)?;
                collect_expr_tables(then, relations)?;
            }
            if let Some(else_clause) = else_clause {
                collect_expr_tables(else_clause, relations)?;
            }
        }
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            collect_query_tables(query, relations)?;
        }
        BoundExpr::InSubquery { expr, query, .. } => {
            collect_expr_tables(expr, relations)?;
            collect_query_tables(query, relations)?;
        }
    }
    Ok(())
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
    sink: Option<&mut dyn RowSink>,
) -> Result<StreamOutcome> {
    if let BoundStatement::Explain(inner) = &bound {
        if !matches!(inner.as_ref(), BoundStatement::Query(_)) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                "EXPLAIN supports SELECT only in v1",
            ));
        }
        let logical = logical_plan(inner.as_ref())?;
        let physical = physical_plan(&logical, catalog)?;
        return Ok(StreamOutcome::Direct(ExecutionResult::Explanation {
            text: format_explain(&physical),
        }));
    }
    let logical = logical_plan(&bound)?;
    let physical = physical_plan(&logical, catalog)?;
    // The caller only supplies a sink for a read (a `SELECT`); a write plan is
    // materialized (`sink` is `None`), so `exec_or_stream` never asks the executor
    // to stream a DML plan. The panic firewall wraps both paths.
    let result = catch_unwind(AssertUnwindSafe(|| {
        exec_or_stream(engine, ctx, &physical, sink)
    }));
    match result {
        Ok(result) => result,
        Err(_) => Err(DbError::internal("statement execution panicked")),
    }
}

/// Execute a resolved read plan either by materializing it into an
/// `ExecutionResult::Query` (`sink` is `None`) or by streaming its rows into the
/// sink (`docs/specs/streaming.md` §4.2). The two read-execution sites
/// ([`run_plan`] and `autocommit_read`) share this so the stream/materialize
/// choice lives in exactly one place.
fn exec_or_stream(
    engine: &QueryEngine,
    ctx: &ExecutionContext<'_>,
    physical: &planner::PhysicalPlan,
    sink: Option<&mut dyn RowSink>,
) -> Result<StreamOutcome> {
    match sink {
        Some(sink) => {
            let count = engine.execute_query_streamed(ctx, physical, sink, STREAM_BATCH_ROWS)?;
            Ok(StreamOutcome::Streamed { count })
        }
        None => Ok(StreamOutcome::Direct(engine.execute(ctx, physical)?)),
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

fn reset_complete() -> ExecutionResult {
    ExecutionResult::Modified {
        command: "RESET".to_string(),
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
    Bound {
        bound: BoundStatement,
        schema_versions: Vec<(TableId, u64)>,
    },
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
    /// already-open transaction. Inside a transaction block the change is pending
    /// until commit (`docs/specs/mvcc.md` §10 G2). `None` is a
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
    /// `DECLARE` / `FETCH` / `CLOSE` SQL cursors. These are simple-query,
    /// connection-session operations and never bind as normal relational plans.
    SqlCursor,
    /// `SET`/`RESET`/`SHOW`/`DISCARD ALL` session configuration. These statements
    /// are non-relational and are handled against the connection's GUC/session
    /// state before binding or planning.
    SessionConfig,
}

/// A prepared extended-protocol statement that can be executed repeatedly with
/// different parameter values. Most statements carry a bound relational payload;
/// non-relational statements (transaction control, maintenance, and session
/// configuration) carry their parsed statement/class instead and are dispatched
/// through the session path without binding.
pub struct PreparedStatement {
    sql: String,
    class: StatementClass,
    bound: Option<BoundStatement>,
    /// Table/view schema versions captured at prepare time for cached data plans.
    /// Executing the plan after any referenced relation changes shape is rejected
    /// so stale row slots and RowDescription metadata are never reused silently.
    schema_versions: Vec<(TableId, u64)>,
    /// The parsed maintenance statement, carried unbound for the
    /// `StatementClass::Maintenance` case so an extended-protocol `Execute` can
    /// run it through `run_maintenance`.
    /// `None` for every other class.
    maintenance: Option<Statement>,
    /// The parsed session-configuration statement (`SET`/`RESET`/`SHOW`/
    /// `DISCARD ALL`), carried unbound so an extended-protocol `Execute` routes it
    /// to the connection's GUC/session state. `None` for every other class.
    session_config: Option<Statement>,
    /// Resolved parameter wire types, by position: the client-declared `PgType`
    /// where an OID was given, otherwise the collapsed default inferred by the
    /// binder. Drives both `ParameterDescription` (OID echo) and parameter decode
    /// (via `PgType::data_type`).
    param_pg_types: Vec<PgType>,
    result_columns: Option<Vec<ColumnInfo>>,
}

impl PreparedStatement {
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Resolved parameter wire types, by position.
    pub fn param_pg_types(&self) -> &[PgType] {
        &self.param_pg_types
    }

    /// Whether this is a transaction-control statement (BEGIN/COMMIT/ROLLBACK).
    /// The connection routes such an `Execute` through the session's transaction
    /// lifecycle even with no transaction open, so it drives `Session.txn` rather
    /// than running as an autocommit unit.
    pub fn is_transaction_control(&self) -> bool {
        matches!(self.class, StatementClass::TransactionControl(_))
    }

    /// Whether this is a maintenance command. The connection routes such an
    /// `Execute` through the session path so it is rejected inside an open
    /// transaction block and otherwise runs as a standalone maintenance unit.
    pub fn is_maintenance(&self) -> bool {
        matches!(self.class, StatementClass::Maintenance)
    }

    /// Whether this is a session-configuration statement (`SET`/`RESET`/`SHOW`/
    /// `DISCARD ALL`). The connection routes such an `Execute` through the session
    /// path so the connection's GUC store and transaction state apply.
    pub fn is_session_config(&self) -> bool {
        matches!(self.class, StatementClass::SessionConfig)
    }

    /// Result column metadata, or `None` for a statement that returns no rows.
    pub fn result_columns(&self) -> Option<&[ColumnInfo]> {
        self.result_columns.as_deref()
    }
}

fn result_columns(bound: &BoundStatement) -> Option<Vec<ColumnInfo>> {
    match bound {
        BoundStatement::Query(query) => Some(query.output_schema().to_vec()),
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
            pg_type: None,
        }]),
        _ => None,
    }
}

fn statement_class(statement: &Statement) -> Result<StatementClass> {
    match statement {
        Statement::Query(_) => Ok(StatementClass::Read),
        Statement::Explain(inner) => match inner.as_ref() {
            Statement::Query(_) => Ok(StatementClass::Read),
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
        | Statement::DropIndex { .. }
        | Statement::CreateSequence { .. }
        | Statement::DropSequence { .. }
        | Statement::AlterTableAddColumn { .. }
        | Statement::AlterTableDropColumn { .. }
        | Statement::AlterTableRenameColumn { .. }
        | Statement::AlterTableRenameTable { .. }
        | Statement::CreateView { .. }
        | Statement::DropView { .. } => Ok(StatementClass::Ddl),
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
        Statement::SetVariable { .. }
        | Statement::ResetVariable { .. }
        | Statement::ShowVariable { .. }
        | Statement::DiscardAll => Ok(StatementClass::SessionConfig),
        Statement::Vacuum { .. } | Statement::Truncate { .. } => Ok(StatementClass::Maintenance),
        Statement::AlterTableSetCompression { .. }
        | Statement::AlterTableSetOptions { .. }
        | Statement::AlterTableAddPrimaryKey { .. }
        | Statement::AlterTableDropPrimaryKey { .. } => Ok(StatementClass::Maintenance),
        Statement::Copy { direction, .. } => Ok(StatementClass::Copy(*direction)),
        Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::RollbackToSavepoint { .. } => Ok(StatementClass::Savepoint),
        Statement::DeclareCursor { .. }
        | Statement::FetchCursor { .. }
        | Statement::CloseCursor { .. } => Ok(StatementClass::SqlCursor),
    }
}

fn classify_bound(class: StatementClass, bound: &BoundStatement) -> StatementClass {
    if matches!(class, StatementClass::Read) && mutates_sequences(bound) {
        StatementClass::Write
    } else {
        class
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
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use buffer::{BufferPool, MemoryBufferPool, PageStore};
    use catalog::{
        CatalogManager, CatalogSnapshot, MemoryCatalog, check_constraint_oid, index_oid,
        primary_key_constraint_oid, table_oid,
    };
    use common::{
        CatalogIntrospectionProvider, ConcurrencyController, DbError, FlushPolicy, IndexId,
        IndexSchema, IsolationLevel, Lsn, PageFlushInfo, ParsedColumnDef, PgType, RelationKind,
        Result, RwLockConcurrencyController, SequenceId, SequenceOptions, SequenceSchema,
        SessionInfo, SessionSequenceState, SqlState, TableId, TableSchema, ToastCompression,
        ToastMode, TxnId, TxnStatus, TxnStatusView, Value,
    };
    use control::{ControlData, ControlStore};
    use executor::ExecutionResult;
    use storage::{HeapPageStore, PageBackedStorageEngine, StorageEngine, StorageMode};
    use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

    use super::{CopyInChunk, SessionTxnStatus, slot_status};
    use crate::app::{AppState, ServerComponents};
    use crate::checkpoint::CheckpointState;
    use crate::config::Config;
    use crate::registry::ActiveTxnRegistry;
    use crate::shutdown::ShutdownState;

    struct TestFlushPolicy;

    impl FlushPolicy for TestFlushPolicy {
        fn can_flush(&self, _info: &PageFlushInfo) -> bool {
            true
        }
    }

    fn app_with_parts(
        data_dir: &Path,
        mut config: Config,
        catalog: Arc<dyn CatalogManager>,
        wal: Arc<dyn WalManager>,
        control: Arc<dyn ControlStore>,
        concurrency: Arc<dyn ConcurrencyController>,
    ) -> AppState {
        config.data_dir = data_dir.to_path_buf();
        let compression = Arc::new(compress::CompressionRegistry::new());
        let dict_store = Arc::new(compress::DictStore::open(data_dir.join("dicts")).unwrap());
        let store: Arc<dyn PageStore> = Arc::new(
            HeapPageStore::open_with_compression(data_dir.join("heap"), compression.clone())
                .unwrap(),
        );
        let buffer_pool: Arc<dyn BufferPool> = Arc::new(MemoryBufferPool::new(
            config.buffer_pool_frames,
            Box::new(TestFlushPolicy),
            store.clone(),
        ));
        buffer_pool.enable_stealing();
        let storage = Arc::new(
            PageBackedStorageEngine::open_with_compression(
                buffer_pool.clone(),
                wal.clone(),
                StorageMode::Normal,
                compression.clone(),
            )
            .unwrap(),
        );
        let active_txns = ActiveTxnRegistry::new();
        let lock_manager = Arc::new(crate::lock_manager::LockManager::new(
            active_txns.clone(),
            Duration::from_millis(config.deadlock_timeout_ms),
        ));
        let ssi_manager = Arc::new(crate::ssi_manager::SerializableConflictManager::new(
            active_txns.clone(),
        ));
        let components = Arc::new(ServerComponents {
            config,
            catalog,
            storage,
            buffer_pool,
            wal,
            control,
            store,
            compression,
            dict_store,
            concurrency,
            checkpoint: CheckpointState {
                last_checkpoint_lsn: AtomicU64::new(0),
                commits_since_checkpoint: AtomicU64::new(0),
                checkpoints: AtomicU64::new(0),
            },
            shutdown: Arc::new(ShutdownState::new()),
            next_txn_id: AtomicU64::new(common::ids::FIRST_NORMAL_XID),
            dead_rows_since_vacuum: AtomicU64::new(0),
            active_txns,
            relation_publish_gate: RwLock::new(()),
            lock_manager,
            ssi_manager,
            tls: None,
            cancel_registry: crate::cancel::CancelRegistry::new(),
            session_registry: Arc::new(crate::session_registry::SessionRegistry::new()),
        });
        AppState {
            components: components.clone(),
            query_service: Arc::new(super::QueryService::new(components)),
        }
    }

    struct FailingControlStore {
        fail_store: AtomicBool,
        stored: Mutex<Option<ControlData>>,
    }

    impl FailingControlStore {
        fn fail_store() -> Self {
            Self {
                fail_store: AtomicBool::new(true),
                stored: Mutex::new(None),
            }
        }
    }

    impl ControlStore for FailingControlStore {
        fn load(&self) -> Result<Option<ControlData>> {
            Ok(self.stored.lock().unwrap().clone())
        }

        fn store(&self, checkpoint_lsn: Lsn, tables: &[TableId], catalog: &[u8]) -> Result<()> {
            if self.fail_store.load(Ordering::SeqCst) {
                return Err(DbError::io("injected control store failure"));
            }
            *self.stored.lock().unwrap() = Some(ControlData {
                checkpoint_lsn,
                tables: tables.to_vec(),
                catalog: catalog.to_vec(),
                page_size: buffer::PAGE_SIZE as u32,
            });
            Ok(())
        }
    }

    #[derive(Default)]
    struct FailingAbortWal {
        next_lsn: AtomicU64,
        fail_abort: AtomicBool,
        statuses: Mutex<HashMap<TxnId, TxnStatus>>,
    }

    impl FailingAbortWal {
        fn new_fail_abort() -> Self {
            Self {
                next_lsn: AtomicU64::new(1),
                fail_abort: AtomicBool::new(true),
                statuses: Mutex::new(HashMap::new()),
            }
        }
    }

    impl WalManager for FailingAbortWal {
        fn append(&self, record: WalRecord) -> Result<Lsn> {
            if matches!(record.kind, WalRecordKind::Abort) && self.fail_abort.load(Ordering::SeqCst)
            {
                // Mirror `FileWalManager::append`: the in-memory `Aborted` status is
                // recorded even when the durable write fails, so a rollback whose
                // durable append fails still leaves the writer hidden.
                self.statuses
                    .lock()
                    .unwrap()
                    .insert(record.txn_id, TxnStatus::Aborted);
                return Err(DbError::io("injected abort append failure"));
            }
            match record.kind {
                WalRecordKind::Commit => {
                    self.statuses
                        .lock()
                        .unwrap()
                        .insert(record.txn_id, TxnStatus::Committed);
                }
                WalRecordKind::Abort => {
                    self.statuses
                        .lock()
                        .unwrap()
                        .insert(record.txn_id, TxnStatus::Aborted);
                }
                WalRecordKind::CommitWithSubxids { subxids } => {
                    let mut statuses = self.statuses.lock().unwrap();
                    statuses.insert(record.txn_id, TxnStatus::Committed);
                    for subxid in subxids {
                        statuses.insert(subxid, TxnStatus::Committed);
                    }
                }
                _ => {}
            }
            Ok(self.next_lsn.fetch_add(1, Ordering::SeqCst))
        }

        fn flush(&self) -> Result<Lsn> {
            Ok(self.next_lsn.load(Ordering::SeqCst).saturating_sub(1))
        }

        fn replay_from(&self, _lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>> {
            Ok(Box::new(std::iter::empty()))
        }

        fn truncate_before(&self, _lsn: Lsn) -> Result<()> {
            Ok(())
        }

        fn flushed_lsn(&self) -> Lsn {
            self.next_lsn.load(Ordering::SeqCst).saturating_sub(1)
        }

        fn bytes_after(&self, _lsn: Lsn) -> Result<u64> {
            Ok(0)
        }

        fn persist_clog(&self, _clog_lsn: Lsn) -> Result<()> {
            Ok(())
        }

        fn set_vacuum_floor(&self, _boundary: TxnId) -> Result<()> {
            Ok(())
        }

        fn establish_recovery_committed_floor(&self, _allocation_boundary: u64) -> Result<()> {
            Ok(())
        }

        fn resolve_in_flight_as_aborted(&self, _writer_xids: &HashSet<u64>) -> Result<()> {
            Ok(())
        }
    }

    impl TxnStatusView for FailingAbortWal {
        fn status(&self, txn_id: TxnId) -> TxnStatus {
            if txn_id < common::ids::FIRST_NORMAL_XID {
                return TxnStatus::Committed;
            }
            self.statuses
                .lock()
                .unwrap()
                .get(&txn_id)
                .copied()
                .unwrap_or(TxnStatus::InProgress)
        }
    }

    struct RecordingConcurrency {
        inner: RwLockConcurrencyController,
        begin_writer_calls: Arc<AtomicUsize>,
        begin_checkpoint_calls: Arc<AtomicUsize>,
    }

    impl RecordingConcurrency {
        fn new(
            begin_writer_calls: Arc<AtomicUsize>,
            begin_checkpoint_calls: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                inner: RwLockConcurrencyController::new(),
                begin_writer_calls,
                begin_checkpoint_calls,
            }
        }
    }

    impl ConcurrencyController for RecordingConcurrency {
        fn begin_writer(&self) -> Result<common::WriteGuard> {
            self.begin_writer_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.begin_writer()
        }

        fn begin_checkpoint(&self) -> Result<common::CheckpointGuard> {
            self.begin_checkpoint_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.begin_checkpoint()
        }
    }

    struct RecordingCatalog {
        inner: MemoryCatalog,
        begin_writer_calls: Arc<AtomicUsize>,
        unguarded_lookup: Arc<AtomicBool>,
        restore_calls: Arc<AtomicUsize>,
    }

    impl RecordingCatalog {
        fn new(
            begin_writer_calls: Arc<AtomicUsize>,
            unguarded_lookup: Arc<AtomicBool>,
            restore_calls: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                inner: MemoryCatalog::empty(),
                begin_writer_calls,
                unguarded_lookup,
                restore_calls,
            }
        }
    }

    impl CatalogManager for RecordingCatalog {
        fn get_table_by_name(&self, name: &str) -> Result<Option<TableSchema>> {
            if self.begin_writer_calls.load(Ordering::SeqCst) == 0 {
                self.unguarded_lookup.store(true, Ordering::SeqCst);
            }
            self.inner.get_table_by_name(name)
        }

        fn get_table(&self, id: TableId) -> Result<Option<TableSchema>> {
            self.inner.get_table(id)
        }

        fn list_tables(&self) -> Result<Vec<TableSchema>> {
            self.inner.list_tables()
        }

        fn get_view_by_name(&self, name: &str) -> Result<Option<common::ViewSchema>> {
            self.inner.get_view_by_name(name)
        }

        fn get_view(&self, id: TableId) -> Result<Option<common::ViewSchema>> {
            self.inner.get_view(id)
        }

        fn list_views(&self) -> Result<Vec<common::ViewSchema>> {
            self.inner.list_views()
        }

        fn snapshot(&self) -> Result<CatalogSnapshot> {
            self.inner.snapshot()
        }

        fn restore(&self, snapshot: CatalogSnapshot) -> Result<()> {
            self.restore_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.restore(snapshot)
        }

        fn reserve_table_id(&self, id: TableId) -> Result<()> {
            self.inner.reserve_table_id(id)
        }

        fn apply_create_table(&self, schema: TableSchema) -> Result<()> {
            self.inner.apply_create_table(schema)
        }

        fn apply_update_table_schema(&self, schema: TableSchema) -> Result<()> {
            self.inner.apply_update_table_schema(schema)
        }

        fn apply_update_table_and_index_schemas(
            &self,
            schema: TableSchema,
            indexes: &[IndexSchema],
        ) -> Result<()> {
            self.inner
                .apply_update_table_and_index_schemas(schema, indexes)
        }

        fn apply_drop_table(&self, id: TableId) -> Result<()> {
            self.inner.apply_drop_table(id)
        }

        fn create_table(
            &self,
            name: String,
            columns: Vec<ParsedColumnDef>,
            primary_key: Vec<String>,
            compression: common::CompressionSetting,
        ) -> Result<TableSchema> {
            self.inner
                .create_table(name, columns, primary_key, compression)
        }

        fn create_table_with_options(
            &self,
            name: String,
            columns: Vec<ParsedColumnDef>,
            primary_key: Vec<String>,
            compression: common::CompressionSetting,
            toast: common::ToastOptions,
            checks: Vec<String>,
        ) -> Result<TableSchema> {
            self.inner.create_table_with_options(
                name,
                columns,
                primary_key,
                compression,
                toast,
                checks,
            )
        }

        fn drop_table(&self, id: TableId) -> Result<()> {
            self.inner.drop_table(id)
        }

        fn rename_table(&self, id: TableId, new_name: String) -> Result<TableSchema> {
            self.inner.rename_table(id, new_name)
        }

        fn preflight_add_table_column(
            &self,
            id: TableId,
            if_not_exists: bool,
            column: &ParsedColumnDef,
        ) -> Result<catalog::TableColumnAlteration> {
            self.inner
                .preflight_add_table_column(id, if_not_exists, column)
        }

        fn add_table_column(&self, id: TableId, column: ParsedColumnDef) -> Result<TableSchema> {
            self.inner.add_table_column(id, column)
        }

        fn preflight_drop_table_column(
            &self,
            id: TableId,
            if_exists: bool,
            column: &str,
        ) -> Result<catalog::TableColumnAlteration> {
            self.inner
                .preflight_drop_table_column(id, if_exists, column)
        }

        fn drop_table_column(&self, id: TableId, column: &str) -> Result<TableSchema> {
            self.inner.drop_table_column(id, column)
        }

        fn rename_table_column(
            &self,
            id: TableId,
            old_name: &str,
            new_name: String,
        ) -> Result<TableSchema> {
            self.inner.rename_table_column(id, old_name, new_name)
        }

        fn set_table_compression(
            &self,
            table: TableId,
            compression: common::CompressionSetting,
            active_dict_id: Option<u32>,
        ) -> Result<TableSchema> {
            self.inner
                .set_table_compression(table, compression, active_dict_id)
        }

        fn set_table_toast_metadata(
            &self,
            table: TableId,
            toast: common::ToastOptions,
            toast_table_id: Option<TableId>,
        ) -> Result<TableSchema> {
            self.inner
                .set_table_toast_metadata(table, toast, toast_table_id)
        }

        fn set_table_primary_key(
            &self,
            table: TableId,
            primary_key: Vec<common::ColumnId>,
        ) -> Result<TableSchema> {
            self.inner.set_table_primary_key(table, primary_key)
        }

        fn add_table_primary_key_index(
            &self,
            table: TableId,
            primary_key: Vec<common::ColumnId>,
            index: IndexSchema,
        ) -> Result<TableSchema> {
            self.inner
                .add_table_primary_key_index(table, primary_key, index)
        }

        fn drop_table_primary_key_index(
            &self,
            table: TableId,
            index: IndexId,
        ) -> Result<TableSchema> {
            self.inner.drop_table_primary_key_index(table, index)
        }

        fn allocate_dictionary_id(&self) -> Result<u32> {
            self.inner.allocate_dictionary_id()
        }

        fn reserve_dictionary_id(&self, id: u32) -> Result<()> {
            self.inner.reserve_dictionary_id(id)
        }

        fn allocate_storage_id(&self) -> Result<common::FileId> {
            self.inner.allocate_storage_id()
        }

        fn reserve_storage_id(&self, id: common::FileId) -> Result<()> {
            self.inner.reserve_storage_id(id)
        }

        fn prepare_truncate_table(&self, table: TableId) -> Result<common::TruncateTablePlan> {
            self.inner.prepare_truncate_table(table)
        }

        fn build_truncate_table_update(
            &self,
            plan: &common::TruncateTablePlan,
        ) -> Result<common::TruncateCatalogUpdate> {
            self.inner.build_truncate_table_update(plan)
        }

        fn apply_truncate_table(
            &self,
            plan: &common::TruncateTablePlan,
        ) -> Result<common::TruncateCatalogUpdate> {
            self.inner.apply_truncate_table(plan)
        }

        fn get_index_by_name(&self, name: &str) -> Result<Option<IndexSchema>> {
            self.inner.get_index_by_name(name)
        }

        fn get_index(&self, id: IndexId) -> Result<Option<IndexSchema>> {
            self.inner.get_index(id)
        }

        fn list_indexes_for_table(&self, table: TableId) -> Result<Vec<IndexSchema>> {
            self.inner.list_indexes_for_table(table)
        }

        fn reserve_index_id(&self, id: IndexId) -> Result<()> {
            self.inner.reserve_index_id(id)
        }

        fn apply_create_index(&self, schema: IndexSchema) -> Result<()> {
            self.inner.apply_create_index(schema)
        }

        fn apply_update_index_schema(&self, schema: IndexSchema) -> Result<()> {
            self.inner.apply_update_index_schema(schema)
        }

        fn apply_drop_index(&self, id: IndexId) -> Result<()> {
            self.inner.apply_drop_index(id)
        }

        fn create_index(
            &self,
            name: String,
            table: &str,
            columns: &[String],
            unique: bool,
        ) -> Result<IndexSchema> {
            self.inner.create_index(name, table, columns, unique)
        }

        fn create_index_with_constraint(
            &self,
            name: String,
            table: &str,
            columns: &[String],
            unique: bool,
            constraint: common::IndexConstraintKind,
        ) -> Result<IndexSchema> {
            self.inner
                .create_index_with_constraint(name, table, columns, unique, constraint)
        }

        fn drop_index(&self, id: IndexId) -> Result<()> {
            self.inner.drop_index(id)
        }

        fn get_sequence_by_name(&self, name: &str) -> Result<Option<SequenceSchema>> {
            self.inner.get_sequence_by_name(name)
        }

        fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceSchema>> {
            self.inner.get_sequence(id)
        }

        fn list_sequences(&self) -> Result<Vec<SequenceSchema>> {
            self.inner.list_sequences()
        }

        fn reserve_sequence_id(&self, id: SequenceId) -> Result<()> {
            self.inner.reserve_sequence_id(id)
        }

        fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()> {
            self.inner.apply_create_sequence(schema)
        }

        fn apply_drop_sequence(&self, id: SequenceId) -> Result<()> {
            self.inner.apply_drop_sequence(id)
        }

        fn create_sequence(
            &self,
            name: String,
            options: SequenceOptions,
            owned: bool,
        ) -> Result<SequenceSchema> {
            self.inner.create_sequence(name, options, owned)
        }

        fn drop_sequence(&self, id: SequenceId) -> Result<()> {
            self.inner.drop_sequence(id)
        }

        fn apply_create_view(&self, schema: common::ViewSchema) -> Result<()> {
            self.inner.apply_create_view(schema)
        }

        fn apply_replace_view(&self, schema: common::ViewSchema) -> Result<()> {
            self.inner.apply_replace_view(schema)
        }

        fn apply_drop_view(&self, id: TableId) -> Result<()> {
            self.inner.apply_drop_view(id)
        }

        fn create_view(
            &self,
            name: String,
            columns: Vec<common::ViewColumn>,
            definition: String,
            dependencies: Vec<common::ViewDependency>,
        ) -> Result<common::ViewSchema> {
            self.inner
                .create_view(name, columns, definition, dependencies)
        }

        fn replace_view(
            &self,
            id: TableId,
            columns: Vec<common::ViewColumn>,
            definition: String,
            dependencies: Vec<common::ViewDependency>,
        ) -> Result<common::ViewSchema> {
            self.inner
                .replace_view(id, columns, definition, dependencies)
        }

        fn drop_view(&self, id: TableId) -> Result<()> {
            self.inner.drop_view(id)
        }
    }

    #[test]
    fn relation_publish_gate_blocks_consistent_snapshot_capture() {
        let dir = tempfile::tempdir().unwrap();
        let app = app_with_parts(
            dir.path(),
            Config::default(),
            Arc::new(MemoryCatalog::empty()),
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap()),
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            Arc::new(RwLockConcurrencyController::new()),
        );
        let gate = app.components.relation_publish_gate.write().unwrap();
        let service = app.query_service.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = service.capture_consistent_snapshots(0).is_ok();
            done_tx.send(result).unwrap();
        });

        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("snapshot capture thread did not start");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "snapshot capture completed while relation publication was gated"
        );

        drop(gate);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("snapshot capture did not resume after relation gate released")
            .then_some(())
            .expect("snapshot capture failed after relation gate released");
        handle.join().unwrap();
    }

    #[test]
    fn snapshot_capture_drops_relation_gate_while_waiting_for_schema_exclusion() {
        let dir = tempfile::tempdir().unwrap();
        let app = app_with_parts(
            dir.path(),
            Config::default(),
            Arc::new(MemoryCatalog::empty()),
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap()),
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            Arc::new(RwLockConcurrencyController::new()),
        );
        let exclusion = app.components.active_txns.begin_snapshot_exclusion();
        let service = app.query_service.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = service.capture_consistent_snapshots(0).is_ok();
            done_tx.send(result).unwrap();
        });

        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("snapshot capture thread did not start");
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "snapshot capture should wait while schema exclusion is active"
        );
        let publish_write = app
            .components
            .relation_publish_gate
            .try_write()
            .expect("snapshot waiter must not hold the relation publish read gate");
        drop(publish_write);

        drop(exclusion);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("snapshot capture did not resume after schema exclusion released")
            .then_some(())
            .expect("snapshot capture failed after schema exclusion released");
        handle.join().unwrap();
    }

    #[test]
    fn direct_query_helpers_reject_copy_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table t (id integer primary key)")
            .unwrap();

        let err = app
            .query_service
            .execute_sql("copy t from stdin")
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        assert!(
            err.message.contains("COPY requires"),
            "unexpected error message: {}",
            err.message
        );

        let cancel = Arc::new(AtomicBool::new(false));
        let (slot, begin) = app
            .query_service
            .execute_simple_default("begin", None, &cancel);
        begin.expect("BEGIN should succeed");
        let (slot, err) =
            app.query_service
                .execute_simple_default("copy t from stdin", slot, &cancel);
        assert_eq!(err.unwrap_err().code, SqlState::FeatureNotSupported);
        assert_eq!(slot_status(&slot), SessionTxnStatus::Failed);
    }

    #[test]
    fn rollback_pre_durable_survives_abort_append_failure_without_losing_aborted_status() {
        let dir = tempfile::tempdir().unwrap();
        let wal: Arc<dyn WalManager> = Arc::new(FailingAbortWal::new_fail_abort());
        let app = app_with_parts(
            dir.path(),
            Config::default(),
            Arc::new(MemoryCatalog::empty()),
            wal.clone(),
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            Arc::new(RwLockConcurrencyController::new()),
        );
        let service = super::QueryService::new(app.components.clone());
        let txn_id = 77;
        app.components.active_txns.register(txn_id);

        // A transient failure to append the *durable* Abort record must not take down
        // the whole server: rollback logs it and completes (best-effort durability;
        // recovery reconstructs the abort anyway).
        let result = service.rollback_pre_durable(txn_id, None);
        assert!(
            result.is_ok(),
            "a failed durable Abort append should be logged, not propagated as a fatal rollback error"
        );

        // ...but the transaction must still be recorded `Aborted` in the in-memory CLOG
        // before it is deregistered, so its dirty (rolled-back) versions never float
        // past the implicit-committed floor and read as committed.
        assert!(
            !app.components.active_txns.active_ids().contains(&txn_id),
            "the rolled-back transaction should be deregistered"
        );
        assert_eq!(
            wal.status(txn_id),
            TxnStatus::Aborted,
            "the abort must be recorded in the in-memory CLOG even when the durable append fails"
        );
    }

    #[test]
    fn autocommit_write_does_not_report_post_commit_checkpoint_failure() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            checkpoint_every_n_commits: 1,
            checkpoint_wal_bytes: u64::MAX,
            ..Config::default()
        };
        let wal: Arc<dyn WalManager> =
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let app = app_with_parts(
            dir.path(),
            config,
            Arc::new(MemoryCatalog::empty()),
            wal,
            Arc::new(FailingControlStore::fail_store()),
            Arc::new(RwLockConcurrencyController::new()),
        );

        let result = app
            .query_service
            .execute_sql("create table users (id integer primary key)");

        assert!(
            result.is_ok(),
            "post-commit checkpoint failure was reported as a normal statement error"
        );
        assert!(
            app.components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .is_some(),
            "the committed DDL should remain installed even when its post-commit checkpoint fails"
        );
    }

    #[test]
    fn autocommit_write_binds_after_acquiring_writer_guard() {
        let dir = tempfile::tempdir().unwrap();
        let begin_writer_calls = Arc::new(AtomicUsize::new(0));
        let begin_checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let unguarded_lookup = Arc::new(AtomicBool::new(false));
        let restore_calls = Arc::new(AtomicUsize::new(0));
        let catalog: Arc<dyn CatalogManager> = Arc::new(RecordingCatalog::new(
            begin_writer_calls.clone(),
            unguarded_lookup.clone(),
            restore_calls,
        ));
        let concurrency: Arc<dyn ConcurrencyController> = Arc::new(RecordingConcurrency::new(
            begin_writer_calls.clone(),
            begin_checkpoint_calls,
        ));
        let config = Config {
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..Config::default()
        };
        let wal: Arc<dyn WalManager> =
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let app = app_with_parts(
            dir.path(),
            config,
            catalog,
            wal,
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            concurrency,
        );
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();

        begin_writer_calls.store(0, Ordering::SeqCst);
        unguarded_lookup.store(false, Ordering::SeqCst);
        app.query_service
            .execute_sql("insert into users (id) values (1)")
            .unwrap();

        assert!(
            !unguarded_lookup.load(Ordering::SeqCst),
            "catalog name resolution for a write ran before the writer guard was acquired"
        );
    }

    #[test]
    fn autocommit_copy_from_acquires_writer_guard_before_protocol_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let begin_writer_calls = Arc::new(AtomicUsize::new(0));
        let begin_checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let unguarded_lookup = Arc::new(AtomicBool::new(false));
        let restore_calls = Arc::new(AtomicUsize::new(0));
        let catalog: Arc<dyn CatalogManager> = Arc::new(RecordingCatalog::new(
            begin_writer_calls.clone(),
            unguarded_lookup,
            restore_calls,
        ));
        let concurrency: Arc<dyn ConcurrencyController> = Arc::new(RecordingConcurrency::new(
            begin_writer_calls.clone(),
            begin_checkpoint_calls,
        ));
        let config = Config {
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..Config::default()
        };
        let wal: Arc<dyn WalManager> =
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let app = app_with_parts(
            dir.path(),
            config,
            catalog,
            wal,
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            concurrency,
        );
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();

        begin_writer_calls.store(0, Ordering::SeqCst);
        let session = super::QuerySessionContext::new(
            Arc::new(AtomicBool::new(false)),
            Arc::new(SessionSequenceState::new()),
            Arc::new(SessionInfo::default()),
            Arc::new(super::SessionGucs::default()),
        );
        let statement = parser::parse("copy users from stdin").unwrap();
        let (slot, _, result) = app.query_service.dispatch(
            statement,
            None,
            IsolationLevel::ReadCommitted,
            &session,
            None,
        );

        assert!(
            slot.is_none(),
            "autocommit COPY should not open a session txn"
        );
        assert!(
            matches!(result.unwrap(), super::StreamOutcome::BeginCopyIn { .. }),
            "COPY FROM should enter protocol mode only after validation succeeds"
        );
        assert_eq!(
            begin_writer_calls.load(Ordering::SeqCst),
            1,
            "autocommit COPY FROM must hold the writer guard before CopyInResponse"
        );
    }

    #[test]
    fn autocommit_ddl_uses_exclusive_guard_and_dml_uses_shared_guard() {
        let dir = tempfile::tempdir().unwrap();
        let begin_writer_calls = Arc::new(AtomicUsize::new(0));
        let begin_checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let unguarded_lookup = Arc::new(AtomicBool::new(false));
        let restore_calls = Arc::new(AtomicUsize::new(0));
        let catalog: Arc<dyn CatalogManager> = Arc::new(RecordingCatalog::new(
            begin_writer_calls.clone(),
            unguarded_lookup,
            restore_calls,
        ));
        let concurrency: Arc<dyn ConcurrencyController> = Arc::new(RecordingConcurrency::new(
            begin_writer_calls.clone(),
            begin_checkpoint_calls.clone(),
        ));
        let config = Config {
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..Config::default()
        };
        let wal: Arc<dyn WalManager> =
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let app = app_with_parts(
            dir.path(),
            config,
            catalog,
            wal,
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            concurrency,
        );

        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        assert_eq!(
            begin_checkpoint_calls.load(Ordering::SeqCst),
            1,
            "DDL must take the exclusive guard so catalog rollback cannot race committed DDL"
        );
        assert_eq!(
            begin_writer_calls.load(Ordering::SeqCst),
            0,
            "DDL should not take the shared writer guard"
        );

        begin_checkpoint_calls.store(0, Ordering::SeqCst);
        begin_writer_calls.store(0, Ordering::SeqCst);
        app.query_service
            .execute_sql("insert into users (id) values (1)")
            .unwrap();
        assert_eq!(
            begin_writer_calls.load(Ordering::SeqCst),
            1,
            "DML should still take the shared writer guard"
        );
        assert_eq!(
            begin_checkpoint_calls.load(Ordering::SeqCst),
            0,
            "DML should not serialize through the exclusive guard"
        );
    }

    #[test]
    fn truncate_executes_in_simple_and_prepared_paths() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada'), (2, 'Grace')")
            .unwrap();
        app.query_service
            .execute_sql("alter table users add column active boolean default true")
            .unwrap();
        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("select id, active from users order by id")
            ),
            vec![
                vec![Value::Integer(1), Value::Boolean(true)],
                vec![Value::Integer(2), Value::Boolean(true)],
            ]
        );
        let prepared_alter = app
            .query_service
            .prepare_sql("alter table users rename column active to enabled", &[])
            .unwrap();
        assert_eq!(
            app.query_service
                .execute_prepared(&prepared_alter, &[])
                .unwrap(),
            ExecutionResult::Modified {
                command: "ALTER TABLE".to_string(),
                count: 0,
            }
        );
        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("select enabled from users order by id")
            ),
            vec![vec![Value::Boolean(true)], vec![Value::Boolean(true)]]
        );

        let truncate_sql = "truncate users";
        let simple = app.query_service.execute_sql(truncate_sql).unwrap();
        assert_eq!(
            simple,
            ExecutionResult::Modified {
                command: "TRUNCATE TABLE".to_string(),
                count: 0,
            }
        );
        assert_eq!(
            row_count(app.query_service.execute_sql("select id from users")),
            0
        );

        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Bea')")
            .unwrap();
        let prepared = app
            .query_service
            .prepare_sql(truncate_sql, &[])
            .expect("TRUNCATE prepares as staged maintenance");
        let prepared_result = app.query_service.execute_prepared(&prepared, &[]).unwrap();
        assert_eq!(
            prepared_result,
            ExecutionResult::Modified {
                command: "TRUNCATE TABLE".to_string(),
                count: 0,
            }
        );
        assert_eq!(
            row_count(app.query_service.execute_sql("select id from users")),
            0
        );
    }

    #[test]
    fn views_create_select_replace_drop_and_catalogs() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql("create table users (id integer primary key, name text, active boolean)")
            .unwrap();
        app.query_service
            .execute_sql(
                "insert into users (id, name, active) values \
                 (1, 'Ada', true), (2, 'Grace', false)",
            )
            .unwrap();
        app.query_service
            .execute_sql(
                "create view active_users (uid, uname) as \
                 select id, name from users where active = true",
            )
            .unwrap();
        let create_table_over_view = app
            .query_service
            .execute_sql("create table if not exists active_users (id integer primary key)")
            .unwrap_err();
        assert_eq!(create_table_over_view.code, SqlState::DuplicateTable);
        let drop_table_over_view = app
            .query_service
            .execute_sql("drop table if exists active_users")
            .unwrap_err();
        assert_eq!(drop_table_over_view.code, SqlState::WrongObjectType);
        let plain_drop_table_over_view = app
            .query_service
            .execute_sql("drop table active_users")
            .unwrap_err();
        assert_eq!(plain_drop_table_over_view.code, SqlState::WrongObjectType);
        let drop_view_over_table = app
            .query_service
            .execute_sql("drop view if exists users")
            .unwrap_err();
        assert_eq!(drop_view_over_table.code, SqlState::WrongObjectType);
        let plain_drop_view_over_table = app
            .query_service
            .execute_sql("drop view users")
            .unwrap_err();
        assert_eq!(plain_drop_view_over_table.code, SqlState::WrongObjectType);

        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("select uid, uname from active_users order by uid")
            ),
            vec![vec![Value::Integer(1), Value::Text("Ada".to_string())]]
        );
        app.query_service
            .execute_sql("create table copied_active_users (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("insert into copied_active_users (id) select uid from active_users")
            .unwrap();
        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("select id from copied_active_users")
            ),
            vec![vec![Value::Integer(1)]]
        );
        assert_eq!(
            result_values(app.query_service.execute_sql(
                "select table_type from information_schema.tables \
                 where table_schema = 'public' and table_name = 'active_users'"
            )),
            vec![vec![Value::Text("VIEW".to_string())]]
        );
        assert_eq!(
            result_values(app.query_service.execute_sql(
                "select relkind from pg_catalog.pg_class where relname = 'active_users'"
            )),
            vec![vec![Value::Text("v".to_string())]]
        );
        assert_eq!(
            result_values(app.query_service.execute_sql(
                "select column_name from information_schema.columns \
                 where table_schema = 'public' and table_name = 'active_users' \
                 order by ordinal_position"
            )),
            vec![
                vec![Value::Text("uid".to_string())],
                vec![Value::Text("uname".to_string())],
            ]
        );

        let prepared = app
            .query_service
            .prepare_sql("select uid from active_users order by uid", &[])
            .unwrap();
        assert_eq!(
            result_values(app.query_service.execute_prepared(&prepared, &[])),
            vec![vec![Value::Integer(1)]]
        );

        app.query_service
            .execute_sql(
                "create or replace view active_users (uid) as \
                 select id from users where id > 1",
            )
            .unwrap();
        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("with users (id) as (values (99)) select uid from active_users")
            ),
            vec![vec![Value::Integer(2)]]
        );
        let stale = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(stale.code, SqlState::FeatureNotSupported);
        assert!(stale.message.contains("cached plan must be reprepared"));
        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("select uid from active_users order by uid")
            ),
            vec![vec![Value::Integer(2)]]
        );

        app.query_service
            .execute_sql("drop view active_users")
            .unwrap();
        let missing = app
            .query_service
            .execute_sql("select uid from active_users")
            .unwrap_err();
        assert_eq!(missing.code, SqlState::UndefinedTable);
    }

    #[test]
    fn view_dependencies_block_drop_and_only_wildcards_block_add_column() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("create view user_count as select count(*) from users")
            .unwrap();
        app.query_service
            .execute_sql("alter table users add column active boolean")
            .unwrap();
        app.query_service
            .execute_sql("create view ordered_users as select id from users order by name")
            .unwrap();
        let drop_ordered_column = app
            .query_service
            .execute_sql("alter table users drop column name")
            .unwrap_err();
        assert_eq!(
            drop_ordered_column.code,
            SqlState::DependentObjectsStillExist
        );
        app.query_service
            .execute_sql("drop view ordered_users")
            .unwrap();
        let drop_table = app
            .query_service
            .execute_sql("drop table users")
            .unwrap_err();
        assert_eq!(drop_table.code, SqlState::DependentObjectsStillExist);

        app.query_service
            .execute_sql("drop view user_count")
            .unwrap();
        app.query_service
            .execute_sql("create view all_users as select * from users")
            .unwrap();
        let add_column = app
            .query_service
            .execute_sql("alter table users add column email text")
            .unwrap_err();
        assert_eq!(add_column.code, SqlState::DependentObjectsStillExist);

        app.query_service
            .execute_sql("drop view all_users")
            .unwrap();
        app.query_service
            .execute_sql("create view nested_all_users as select * from (select * from users) d")
            .unwrap();
        let add_nested_column = app
            .query_service
            .execute_sql("alter table users add column email text")
            .unwrap_err();
        assert_eq!(add_nested_column.code, SqlState::DependentObjectsStillExist);

        app.query_service
            .execute_sql("drop view nested_all_users")
            .unwrap();
        app.query_service
            .execute_sql(
                "create view cte_all_users as with d as (select * from users) select * from d",
            )
            .unwrap();
        let add_cte_column = app
            .query_service
            .execute_sql("alter table users add column email text")
            .unwrap_err();
        assert_eq!(add_cte_column.code, SqlState::DependentObjectsStillExist);

        app.query_service
            .execute_sql("create table logs (id integer primary key, message text)")
            .unwrap();
        app.query_service
            .execute_sql(
                "create view unused_cte_view as \
                 with unused as (select * from logs) select 1 as one",
            )
            .unwrap();
        let drop_unused_cte_table = app
            .query_service
            .execute_sql("drop table logs")
            .unwrap_err();
        assert_eq!(
            drop_unused_cte_table.code,
            SqlState::DependentObjectsStillExist
        );

        app.query_service
            .execute_sql("create sequence seq1")
            .unwrap();
        let sequence_view = app
            .query_service
            .execute_sql("create view sequence_view as select nextval('seq1')")
            .unwrap_err();
        assert_eq!(sequence_view.code, SqlState::FeatureNotSupported);
    }

    #[test]
    fn view_output_nullability_accounts_for_outer_join_null_extension() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql("create table left_rows (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("create table right_rows (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("insert into left_rows (id) values (1)")
            .unwrap();
        app.query_service
            .execute_sql(
                "create view nullable_right as \
                 select right_rows.id as maybe_id \
                 from left_rows left join right_rows on false",
            )
            .unwrap();

        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("select maybe_id from nullable_right")
            ),
            vec![vec![Value::Null]]
        );
        assert_eq!(
            result_values(app.query_service.execute_sql(
                "select is_nullable from information_schema.columns \
                 where table_schema = 'public' and table_name = 'nullable_right' \
                 and column_name = 'maybe_id'"
            )),
            vec![vec![Value::Text("YES".to_string())]]
        );
    }

    #[test]
    fn views_recover_from_wal_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (1, 'Ada')")
                .unwrap();
            app.query_service
                .execute_sql("create view user_names as select name from users")
                .unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            result_values(
                reopened
                    .query_service
                    .execute_sql("select name from user_names")
            ),
            vec![vec![Value::Text("Ada".to_string())]]
        );
    }

    #[tokio::test]
    async fn create_table_with_toast_options_installs_hidden_relation() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        app.query_service
            .execute_sql(
                "create table users (id integer primary key, bio text) with \
                 (toast = aggressive, toast_tuple_target = 4096, \
                  toast_min_value_size = 512, toast_compression = zstd)",
            )
            .unwrap();

        let users = app
            .components
            .catalog
            .get_table_by_name("users")
            .unwrap()
            .expect("users table exists");
        assert_eq!(users.toast.mode, ToastMode::Aggressive);
        assert_eq!(users.toast.tuple_target, 4096);
        assert_eq!(users.toast.min_value_size, 512);
        assert_eq!(users.toast.compression, ToastCompression::Zstd);

        let toast_id = users.toast_table_id.expect("hidden TOAST relation id");
        let toast = app
            .components
            .catalog
            .get_table(toast_id)
            .unwrap()
            .expect("hidden TOAST relation exists");
        assert_eq!(
            toast.relation_kind,
            RelationKind::Toast {
                base_table: users.id
            }
        );
        assert_eq!(
            app.components
                .catalog
                .get_table_by_name(&toast.name)
                .unwrap(),
            None
        );
    }

    #[test]
    fn failed_autocommit_dml_does_not_restore_catalog_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let begin_writer_calls = Arc::new(AtomicUsize::new(0));
        let begin_checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let unguarded_lookup = Arc::new(AtomicBool::new(false));
        let restore_calls = Arc::new(AtomicUsize::new(0));
        let catalog: Arc<dyn CatalogManager> = Arc::new(RecordingCatalog::new(
            begin_writer_calls.clone(),
            unguarded_lookup,
            restore_calls.clone(),
        ));
        let concurrency: Arc<dyn ConcurrencyController> = Arc::new(RecordingConcurrency::new(
            begin_writer_calls,
            begin_checkpoint_calls,
        ));
        let config = Config {
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..Config::default()
        };
        let wal: Arc<dyn WalManager> =
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let app = app_with_parts(
            dir.path(),
            config,
            catalog,
            wal,
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            concurrency,
        );

        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id) values (1)")
            .unwrap();
        restore_calls.store(0, Ordering::SeqCst);

        let err = app
            .query_service
            .execute_sql("insert into users (id) values (1)")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UniqueViolation);
        assert_eq!(
            restore_calls.load(Ordering::SeqCst),
            0,
            "DML rollback must not restore a whole catalog snapshot"
        );
    }

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

    fn result_values(result: Result<ExecutionResult>) -> Vec<Vec<Value>> {
        match result.unwrap() {
            ExecutionResult::Query { rows, .. }
            | ExecutionResult::ModifiedReturning { rows, .. } => {
                rows.into_iter().map(|row| row.values).collect()
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn single_integer(result: Result<ExecutionResult>) -> i64 {
        let rows = result_values(result);
        match rows.as_slice() {
            [row] => match row.as_slice() {
                [Value::Integer(value)] => *value,
                other => panic!("expected one integer column, got {other:?}"),
            },
            other => panic!("expected one row, got {other:?}"),
        }
    }

    #[test]
    fn catalog_introspection_provider_is_wired_into_statement_context() {
        let dir = tempfile::tempdir().unwrap();
        let wal: Arc<dyn WalManager> =
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let app = app_with_parts(
            dir.path(),
            Config::default(),
            Arc::new(MemoryCatalog::empty()),
            wal,
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            Arc::new(RwLockConcurrencyController::new()),
        );

        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("create index users_id_lookup on users (id)")
            .unwrap();
        app.query_service
            .execute_sql(
                "create table constrained (\
                 id integer primary key, \
                 check (id > 0))",
            )
            .unwrap();
        app.query_service
            .execute_sql("create table pg_class (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("create table serial_probe (id serial primary key)")
            .unwrap();
        app.query_service
            .execute_sql(
                "create table toast_probe (id integer primary key, body text) \
                 with (toast = aggressive)",
            )
            .unwrap();
        let users = app
            .components
            .catalog
            .get_table_by_name("users")
            .unwrap()
            .unwrap();
        let users_pkey = app
            .components
            .catalog
            .get_index_by_name("users_pkey")
            .unwrap()
            .unwrap();
        let users_pkey_oid = index_oid(users_pkey.id);
        let users_pkey_constraint_oid = primary_key_constraint_oid(users.id);
        let users_id_lookup = app
            .components
            .catalog
            .get_index_by_name("users_id_lookup")
            .unwrap()
            .unwrap();
        let constrained = app
            .components
            .catalog
            .get_table_by_name("constrained")
            .unwrap()
            .unwrap();
        let constrained_check_oid = check_constraint_oid(constrained.id, 0);
        let shadow_pg_class = app
            .components
            .catalog
            .get_table_by_name("pg_class")
            .unwrap()
            .unwrap();
        let toast_probe = app
            .components
            .catalog
            .get_table_by_name("toast_probe")
            .unwrap()
            .unwrap();
        let toast_oid = table_oid(toast_probe.toast_table_id.unwrap());
        let rows = result_values(app.query_service.execute_sql(&format!(
            "select \
             to_regclass('users'), \
             to_regclass('users_pkey'), \
             to_regclass('pg_catalog.pg_class'), \
             to_regclass('pg_class'), \
             pg_table_is_visible(to_regclass('users')), \
             pg_table_is_visible(to_regclass('users_pkey')), \
             pg_table_is_visible({users_pkey_oid}), \
             pg_table_is_visible(to_regclass('pg_catalog.pg_class')), \
             pg_table_is_visible(to_regclass('information_schema.tables')), \
             pg_table_is_visible({toast_oid}), \
             pg_get_userbyid(10), \
             pg_get_serial_sequence('serial_probe', 'id'), \
             pg_get_serial_sequence('users', 'id'), \
             pg_get_indexdef({users_pkey_oid}), \
             pg_get_indexdef({}, 1, true), \
             pg_get_indexdef({}, 99, true), \
             pg_get_indexdef({}, -1, true), \
             pg_get_constraintdef({users_pkey_constraint_oid}), \
             pg_get_constraintdef({constrained_check_oid}), \
             to_regtype('pg_catalog.int4'), \
             to_regclass('missing')",
            index_oid(users_id_lookup.id),
            index_oid(users_id_lookup.id),
            index_oid(users_id_lookup.id)
        )));

        assert_eq!(
            rows,
            vec![vec![
                Value::Integer(table_oid(users.id)),
                Value::Integer(users_pkey_oid),
                Value::Integer(catalog::SystemView::PgClass.relation_oid()),
                Value::Integer(table_oid(shadow_pg_class.id)),
                Value::Boolean(true),
                Value::Boolean(true),
                Value::Boolean(true),
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Text("saguarodb".to_string()),
                Value::Text("serial_probe_id_seq".to_string()),
                Value::Null,
                Value::Text(
                    "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)".to_string(),
                ),
                Value::Text("id".to_string()),
                Value::Null,
                Value::Null,
                Value::Text("PRIMARY KEY (id)".to_string()),
                Value::Text("CHECK (id > 0)".to_string()),
                Value::Integer(23),
                Value::Null,
            ]]
        );
    }

    #[derive(Debug)]
    struct OverrideIntrospection;

    impl CatalogIntrospectionProvider for OverrideIntrospection {
        fn to_regclass(&self, name: &str) -> Result<Option<i64>> {
            Ok((name == "override_target").then_some(123))
        }
    }

    #[test]
    fn explicit_catalog_introspection_provider_override_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        let prepared = app
            .query_service
            .prepare_sql("select to_regclass('override_target')", &[])
            .unwrap();
        let session = super::QuerySessionContext::new(
            Arc::new(AtomicBool::new(false)),
            Arc::new(SessionSequenceState::new()),
            Arc::new(SessionInfo::default()),
            Arc::new(super::SessionGucs::default()),
        )
        .with_catalog_introspection(Arc::new(OverrideIntrospection));
        let result = app
            .query_service
            .execute_prepared_cancelable_with_session_context(
                &prepared,
                &[],
                &session,
                IsolationLevel::default(),
                None,
            )
            .and_then(super::StreamOutcome::into_direct_result);

        assert_eq!(result_values(result), vec![vec![Value::Integer(123)]]);
    }

    #[test]
    fn copy_in_installs_catalog_introspection_for_expression_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table referenced (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql(
                "create table copy_probe (\
                 id integer primary key, \
                 target_oid bigint not null default to_regclass('referenced'))",
            )
            .unwrap();

        let cancel = Arc::new(AtomicBool::new(false));
        let (row_tx, _row_rx) = tokio::sync::mpsc::channel(super::STREAM_CHANNEL_CAPACITY);
        let (slot, _, result) = app.query_service.execute_simple_streamed(
            "copy copy_probe (id) from stdin",
            None,
            IsolationLevel::default(),
            super::QuerySessionContext::new(
                cancel.clone(),
                Arc::new(SessionSequenceState::new()),
                Arc::new(SessionInfo::default()),
                Arc::new(super::SessionGucs::default()),
            ),
            row_tx,
        );
        assert!(slot.is_none());
        let (job, snapshots) = match result.unwrap() {
            super::StreamOutcome::BeginCopyIn { job, snapshots } => (job, snapshots),
            other => panic!("expected COPY FROM request, got {other:?}"),
        };

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.blocking_send(CopyInChunk::Chunk(b"1\n".to_vec()))
            .unwrap();
        tx.blocking_send(CopyInChunk::Done).unwrap();
        drop(tx);
        let session = super::QuerySessionContext::new(
            cancel,
            Arc::new(SessionSequenceState::new()),
            Arc::new(SessionInfo::default()),
            Arc::new(super::SessionGucs::default()),
        );
        let (slot, result) = app
            .query_service
            .run_copy_in_stream(job, slot, session, snapshots, rx);
        assert!(slot.is_none());
        assert_eq!(result.unwrap(), 1);

        let referenced = app
            .components
            .catalog
            .get_table_by_name("referenced")
            .unwrap()
            .unwrap();
        assert_eq!(
            result_values(
                app.query_service
                    .execute_sql("select target_oid from copy_probe where id = 1")
            ),
            vec![vec![Value::Integer(table_oid(referenced.id))]]
        );
    }

    #[tokio::test]
    async fn sequence_functions_use_session_state_and_write_routing() {
        let dir = tempfile::tempdir().unwrap();
        let begin_writer_calls = Arc::new(AtomicUsize::new(0));
        let begin_checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let concurrency: Arc<dyn ConcurrencyController> = Arc::new(RecordingConcurrency::new(
            begin_writer_calls.clone(),
            begin_checkpoint_calls,
        ));
        let config = Config {
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..Config::default()
        };
        let wal: Arc<dyn WalManager> =
            Arc::new(FileWalManager::open(dir.path().join("wal.dat")).unwrap());
        let app = app_with_parts(
            dir.path(),
            config,
            Arc::new(MemoryCatalog::empty()),
            wal,
            Arc::new(
                control::FileControlStore::open(dir.path(), buffer::PAGE_SIZE as u32).unwrap(),
            ),
            concurrency,
        );
        app.query_service
            .execute_sql("create sequence users_id_seq")
            .unwrap();
        assert!(
            app.components
                .catalog
                .get_sequence_by_name("users_id_seq")
                .unwrap()
                .is_some()
        );
        app.query_service
            .execute_sql("create table seq_probe (id integer primary key)")
            .unwrap();
        app.query_service
            .execute_sql("insert into seq_probe (id) values (1)")
            .unwrap();
        begin_writer_calls.store(0, Ordering::SeqCst);

        let cancel = Arc::new(AtomicBool::new(false));
        let session_sequences = Arc::new(SessionSequenceState::new());
        let gucs = Arc::new(super::SessionGucs::default());
        let (_slot, iso, err) = app.query_service.execute_simple_with_session_sequences(
            "select currval('users_id_seq') from seq_probe",
            None,
            IsolationLevel::default(),
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(
            err.unwrap_err().code,
            SqlState::ObjectNotInPrerequisiteState
        );

        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "select nextval('users_id_seq') from seq_probe",
            None,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 1);
        assert_eq!(
            begin_writer_calls.load(Ordering::SeqCst),
            1,
            "SELECT nextval must route through the write guard"
        );

        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "select currval('users_id_seq') from seq_probe",
            None,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 1);
        assert_eq!(
            begin_writer_calls.load(Ordering::SeqCst),
            1,
            "currval is session-local and should not take the write guard"
        );

        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "select setval('users_id_seq', 10, false) from seq_probe",
            None,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 10);
        assert_eq!(begin_writer_calls.load(Ordering::SeqCst), 2);

        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "select currval('users_id_seq') from seq_probe",
            None,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 1);

        let (_slot, _iso, result) = app.query_service.execute_simple_with_session_sequences(
            "select nextval('users_id_seq') from seq_probe",
            None,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 10);

        let fresh_session_sequences = Arc::new(SessionSequenceState::new());
        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "select setval('users_id_seq', 20, false) from seq_probe",
            None,
            iso,
            &cancel,
            fresh_session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 20);

        let (_slot, _iso, err) = app.query_service.execute_simple_with_session_sequences(
            "select currval('users_id_seq') from seq_probe",
            None,
            iso,
            &cancel,
            fresh_session_sequences,
            gucs.clone(),
        );
        assert_eq!(
            err.unwrap_err().code,
            SqlState::ObjectNotInPrerequisiteState
        );
    }

    #[tokio::test]
    async fn default_nextval_fills_omitted_columns_and_keeps_rollback_gap() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create sequence users_id_seq")
            .unwrap();
        app.query_service
            .execute_sql(
                "create table users (\
                 id integer primary key default nextval('users_id_seq'), \
                 name text)",
            )
            .unwrap();

        let cancel = Arc::new(AtomicBool::new(false));
        let session_sequences = Arc::new(SessionSequenceState::new());
        let gucs = Arc::new(super::SessionGucs::default());
        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "insert into users (name) values ('Ada') returning id",
            None,
            IsolationLevel::default(),
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 1);

        let (slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "begin",
            None,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        result.unwrap();
        let (slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "insert into users (name) values ('Rolled') returning id",
            slot,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 2);
        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "rollback",
            slot,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        result.unwrap();

        let (_slot, iso, result) = app.query_service.execute_simple_with_session_sequences(
            "insert into users (name) values ('Grace') returning id",
            None,
            iso,
            &cancel,
            session_sequences.clone(),
            gucs.clone(),
        );
        assert_eq!(single_integer(result), 3);

        let (_slot, _iso, result) = app.query_service.execute_simple_with_session_sequences(
            "select id from users order by id",
            None,
            iso,
            &cancel,
            session_sequences,
            gucs.clone(),
        );
        assert_eq!(
            result_values(result),
            vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
        );
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
        // queues a session-default change for commit but leaves THIS transaction
        // Read Committed.
        let (slot, sd, res) = app.query_service.execute_simple(
            "set session characteristics as transaction isolation level repeatable read",
            slot,
            sd,
            &cancel,
        );
        res.unwrap();
        assert_eq!(
            sd,
            IsolationLevel::ReadCommitted,
            "the session default is not committed until the transaction commits"
        );
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
        assert_eq!(
            sd,
            IsolationLevel::RepeatableRead,
            "COMMIT persists the pending session-default change"
        );

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

        let (slot, sd, res) = app.query_service.execute_simple(
            "set session characteristics as transaction read write",
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
            "a no-level SET SESSION CHARACTERISTICS is still rejected in a failed block"
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

        let executor::ExecutionResult::Query { rows, .. } = app
            .query_service
            .execute_sql("select id from users where name = 'Ada' order by id")
            .unwrap()
        else {
            panic!("expected query");
        };
        assert_eq!(
            rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
        );
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
    async fn drop_index_rejects_primary_key_constraint_index() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let err = app
            .query_service
            .execute_sql("drop index users_pkey")
            .unwrap_err();
        assert_eq!(err.code, SqlState::DependentObjectsStillExist);

        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        let duplicate = app
            .query_service
            .execute_sql("insert into users (id, name) values (1, 'Dup')")
            .unwrap_err();
        assert_eq!(duplicate.code, SqlState::UniqueViolation);
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

        let synthetic_primary_key_name = app
            .query_service
            .execute_sql("create index users_pkey on users (name)")
            .unwrap_err();
        assert_eq!(synthetic_primary_key_name.code, SqlState::DuplicateTable);

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

        // EXPLAIN shows the secondary index (id 2) is chosen, not a seq scan.
        let executor::ExecutionResult::Explanation { text } = app
            .query_service
            .execute_sql("explain select id from users where name = 'Bob'")
            .unwrap()
        else {
            panic!("expected explanation");
        };
        assert!(text.contains("IndexScan"), "plan was: {text}");
        assert!(text.contains("index=2"), "plan was: {text}");

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

        // `val` is BIGINT so it can hold i64::MAX; the overflow under test is the
        // i64 arithmetic `val + 1`, not a column-width range check.
        app.query_service
            .execute_sql("create table nums (id integer primary key, val bigint)")
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
            ..CatalogSnapshot::default()
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
        // No declared OID: the binder infers an integer parameter, which echoes
        // the collapsed default wire type (int8).
        assert_eq!(prepared.param_pg_types(), &[PgType::Int8]);
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
    async fn prepared_data_plan_rejects_schema_change_before_execute() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let prepared_select = app
            .query_service
            .prepare_sql("select name from users where id = $1", &[])
            .unwrap();
        let prepared_insert = app
            .query_service
            .prepare_sql("insert into users (id, name) values ($1, $2)", &[])
            .unwrap();

        app.query_service
            .execute_sql("alter table users add column email text")
            .unwrap();

        let select_err = app
            .query_service
            .execute_prepared(&prepared_select, &[Value::Integer(1)])
            .unwrap_err();
        assert_eq!(select_err.code, SqlState::FeatureNotSupported);
        assert!(
            select_err
                .message
                .contains("cached plan must be reprepared"),
            "message was: {}",
            select_err.message
        );

        let insert_err = app
            .query_service
            .execute_prepared(
                &prepared_insert,
                &[Value::Integer(2), Value::Text("Bo".to_string())],
            )
            .unwrap_err();
        assert_eq!(insert_err.code, SqlState::FeatureNotSupported);
        assert!(
            insert_err
                .message
                .contains("cached plan must be reprepared"),
            "message was: {}",
            insert_err.message
        );
    }

    #[tokio::test]
    async fn prepared_schema_versions_keep_bound_view_version() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, active boolean)")
            .unwrap();
        app.query_service
            .execute_sql("create view active_users as select id from users where active = true")
            .unwrap();

        let statement = parser::parse("select id from active_users").unwrap();
        let bound = planner::bind(&statement, app.components.catalog.as_ref()).unwrap();
        let original_view = app
            .components
            .catalog
            .get_view_by_name("active_users")
            .unwrap()
            .unwrap();

        app.query_service
            .execute_sql("create or replace view active_users as select id from users where id > 0")
            .unwrap();
        let replaced_view = app
            .components
            .catalog
            .get_view_by_name("active_users")
            .unwrap()
            .unwrap();

        let schema_versions =
            super::prepared_schema_versions(&bound, app.components.catalog.as_ref()).unwrap();
        assert!(schema_versions.contains(&(original_view.id, original_view.schema_version)));
        assert!(!schema_versions.contains(&(replaced_view.id, replaced_view.schema_version)));

        let stale = app
            .query_service
            .validate_prepared_schema_versions(&schema_versions)
            .unwrap_err();
        assert_eq!(stale.code, SqlState::FeatureNotSupported);
    }

    #[tokio::test]
    async fn relation_snapshot_schema_mismatch_is_retryable_before_execution() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        let old_relations = app.components.storage.capture_relation_snapshot().unwrap();

        app.query_service
            .execute_sql("alter table users add column name text")
            .unwrap();

        let statement = parser::parse("select name from users").unwrap();
        let bound = planner::bind(&statement, app.components.catalog.as_ref()).unwrap();
        let schema_versions =
            super::prepared_schema_versions(&bound, app.components.catalog.as_ref()).unwrap();
        let err = app
            .query_service
            .validate_relation_snapshot_schema_versions(
                old_relations.as_ref(),
                &schema_versions,
                false,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::SerializationFailure);

        let copy_statement = parser::parse("copy users to stdout").unwrap();
        let copy_bound = planner::bind(&copy_statement, app.components.catalog.as_ref()).unwrap();
        let copy_schema_versions =
            super::prepared_schema_versions(&copy_bound, app.components.catalog.as_ref()).unwrap();
        assert_eq!(
            copy_schema_versions, schema_versions,
            "COPY must validate the target table against the captured relation snapshot"
        );
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
        assert_eq!(prepared.param_pg_types(), &[PgType::Int8, PgType::Text]);
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
    async fn prepared_drop_table_if_exists_resolves_at_execute_time() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        let prepared = app
            .query_service
            .prepare_sql("drop table if exists users", &[])
            .unwrap();

        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        app.query_service.execute_prepared(&prepared, &[]).unwrap();

        let err = app
            .query_service
            .execute_sql("select id from users")
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
    }

    #[tokio::test]
    async fn prepared_plain_drop_table_uses_bound_table_id() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();
        let prepared = app
            .query_service
            .prepare_sql("drop table users", &[])
            .unwrap();

        app.query_service.execute_sql("drop table users").unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key)")
            .unwrap();

        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::UndefinedTable);
        assert!(matches!(
            app.query_service.execute_sql("select id from users"),
            Ok(ExecutionResult::Query { .. })
        ));
    }

    #[tokio::test]
    async fn prepared_schema_alter_rejects_dropped_recreated_target() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();
        let prepared = app
            .query_service
            .prepare_sql("alter table users drop column name", &[])
            .unwrap();

        app.query_service.execute_sql("drop table users").unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text, email text)")
            .unwrap();

        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
        app.query_service
            .execute_sql("insert into users (id, name, email) values (1, 'Ada', 'a@b')")
            .expect("prepared ALTER must not retarget the recreated table");
    }

    #[tokio::test]
    async fn prepared_explicit_on_conflict_rechecks_primary_key_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let prepared = app
            .query_service
            .prepare_sql(
                "insert into users (id, name) values (1, 'Ada') on conflict (id) do nothing",
                &[],
            )
            .unwrap();
        app.query_service
            .execute_sql("alter table users drop primary key")
            .unwrap();

        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[tokio::test]
    async fn prepared_on_conflict_rejects_changed_primary_key_target() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let prepared = app
            .query_service
            .prepare_sql(
                "insert into users (id, name) values (1, 'Ada') \
                 on conflict (id) do update set name = excluded.name",
                &[],
            )
            .unwrap();
        app.query_service
            .execute_sql("alter table users drop primary key")
            .unwrap();
        app.query_service
            .execute_sql("alter table users add primary key (name)")
            .unwrap();

        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[tokio::test]
    async fn prepared_targetless_on_conflict_rejects_changed_primary_key_target() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer primary key, name text)")
            .unwrap();

        let prepared = app
            .query_service
            .prepare_sql(
                "insert into users (id, name) values (1, 'Ada') on conflict do nothing",
                &[],
            )
            .unwrap();
        app.query_service
            .execute_sql("alter table users drop primary key")
            .unwrap();
        app.query_service
            .execute_sql("alter table users add primary key (name)")
            .unwrap();

        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    #[tokio::test]
    async fn prepared_targetless_on_conflict_without_pk_rejects_schema_change() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table users (id integer, name text)")
            .unwrap();
        app.query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();

        let prepared = app
            .query_service
            .prepare_sql(
                "insert into users (id, name) values (1, 'Ada again') on conflict do nothing",
                &[],
            )
            .unwrap();
        app.query_service
            .execute_sql("alter table users add primary key (id)")
            .unwrap();

        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
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

    #[tokio::test]
    async fn stateless_prepared_execution_rejects_session_config() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();

        let prepared = app
            .query_service
            .prepare_sql("set default_transaction_isolation to serializable", &[])
            .unwrap();
        let err = app
            .query_service
            .execute_prepared(&prepared, &[])
            .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);
    }

    /// A gone consumer (client disconnected mid-stream) must stop the streamed
    /// read cleanly rather than hanging or erroring: with the receiver dropped,
    /// `ChannelRowSink::push` sees the closed channel and returns `Break`, so the
    /// drive closes the executor and returns `Streamed`. Exercised without sockets
    /// so it is fully deterministic; more than one batch of rows forces a
    /// mid-drive `push` (and thus the early-stop `Break`).
    #[test]
    fn streamed_select_stops_cleanly_when_receiver_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table t (id integer primary key)")
            .unwrap();
        let values = (1..=200)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        app.query_service
            .execute_sql(&format!("insert into t (id) values {values}"))
            .unwrap();

        // Drop the receiver before any row is drained, as if the client vanished.
        let (row_tx, row_rx) =
            tokio::sync::mpsc::channel::<super::StreamMessage>(super::STREAM_CHANNEL_CAPACITY);
        drop(row_rx);

        let cancel = Arc::new(AtomicBool::new(false));
        let session = super::QuerySessionContext::new(
            cancel,
            Arc::new(SessionSequenceState::new()),
            Arc::new(SessionInfo::default()),
            Arc::new(super::SessionGucs::default()),
        );
        let (slot, _default, outcome) = app.query_service.execute_simple_streamed(
            "select id from t order by id",
            None,
            IsolationLevel::default(),
            session,
            row_tx,
        );

        assert!(
            matches!(outcome, Ok(super::StreamOutcome::Streamed { .. })),
            "a dropped receiver stops the stream cleanly, not with an error or a hang"
        );
        assert!(
            slot.is_none(),
            "an autocommit read leaves no open transaction"
        );
    }
}
