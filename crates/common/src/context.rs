use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::cancel::QueryCancel;
use crate::datetime::now_micros;
use crate::error::{DbError, Result};
use crate::ids::{SequenceId, TableId, TxnId};
use crate::mvcc::{IsolationLevel, Snapshot};
use crate::row::Key;

/// Blocks a writer that hit an in-progress row-lock conflict until the holder
/// finishes, so the writer can re-check (`docs/specs/deadlock.md`). The storage
/// engine's write paths call this at a conflict point (after dropping the page
/// latch); the server's lock manager implements it. `wait_for` returns `Ok` once
/// `blocker` is no longer active (the caller then re-checks the row), or `Err` with
/// `DeadlockDetected` (`40P01`) if waiting would deadlock, or `QueryCanceled`
/// (`57014`) if `cancel` is set.
pub trait ConflictWaiter: Send + Sync + std::fmt::Debug {
    fn wait_for(&self, waiter: u64, blocker: u64, cancel: &QueryCancel) -> Result<()>;
}

/// The default `ConflictWaiter` for read-only / test contexts. A real `WouldBlock`
/// only arises at a storage write-conflict point, which always carries the server's
/// real lock manager, so this is never legitimately reached. It **errors loudly**
/// rather than returning `Ok` — returning `Ok` would make a mis-wired write path
/// spin forever (`WouldBlock → wait → Ok → re-attempt → WouldBlock → …`).
#[derive(Debug)]
struct NoConflictWaiter;

impl ConflictWaiter for NoConflictWaiter {
    fn wait_for(&self, waiter: u64, blocker: u64, _cancel: &QueryCancel) -> Result<()> {
        Err(DbError::internal(format!(
            "no conflict waiter configured: a write path reached a row-lock conflict \
             (waiter={waiter}, blocker={blocker}) without a lock manager"
        )))
    }
}

/// Sequence value runtime used by the executor's `nextval` and `setval` scalar
/// expressions. The storage crate provides the real implementation; the common
/// trait keeps expression evaluation decoupled from storage internals.
pub trait SequenceManager: Send + Sync + std::fmt::Debug {
    fn sequence_exists(&self, sequence: SequenceId) -> Result<bool>;
    fn nextval(&self, txn_id: TxnId, sequence: SequenceId) -> Result<i64>;
    fn setval(
        &self,
        txn_id: TxnId,
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<i64>;
}

#[derive(Debug)]
struct NoSequenceManager;

impl SequenceManager for NoSequenceManager {
    fn sequence_exists(&self, sequence: SequenceId) -> Result<bool> {
        Err(DbError::internal(format!(
            "no sequence manager configured for sequence_exists({sequence})"
        )))
    }

    fn nextval(&self, _txn_id: TxnId, sequence: SequenceId) -> Result<i64> {
        Err(DbError::internal(format!(
            "no sequence manager configured for nextval({sequence})"
        )))
    }

    fn setval(
        &self,
        _txn_id: TxnId,
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<i64> {
        Err(DbError::internal(format!(
            "no sequence manager configured for setval({sequence}, {value}, {is_called})"
        )))
    }
}

/// Per-session sequence state backing PostgreSQL's `currval` semantics. `nextval`
/// and `setval` record the last value seen for a sequence on this connection;
/// `currval` reads it and errors when the sequence has not been used in-session.
#[derive(Debug, Default)]
pub struct SessionSequenceState {
    currvals: Mutex<HashMap<SequenceId, i64>>,
}

impl SessionSequenceState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_currval(&self, sequence: SequenceId, value: i64) -> Result<()> {
        self.currvals
            .lock()
            .map_err(|_| DbError::internal("session sequence state lock poisoned"))?
            .insert(sequence, value);
        Ok(())
    }

    pub fn currval(&self, sequence: SequenceId) -> Result<Option<i64>> {
        Ok(self
            .currvals
            .lock()
            .map_err(|_| DbError::internal("session sequence state lock poisoned"))?
            .get(&sequence)
            .copied())
    }

    pub fn reset_all(&self) -> Result<()> {
        self.currvals
            .lock()
            .map_err(|_| DbError::internal("session sequence state lock poisoned"))?
            .clear();
        Ok(())
    }
}

/// Connection identity reported by system information functions
/// (`current_user`, `current_database()`, `pg_backend_pid()`, ...).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInfo {
    /// The startup `user` parameter.
    pub user: String,
    /// The startup `database` parameter, defaulted to the user name when omitted.
    pub database: String,
    /// The connection's cancellation-key process id (`BackendKeyData`).
    pub backend_pid: i32,
}

impl Default for SessionInfo {
    fn default() -> Self {
        Self {
            user: "saguarodb".to_string(),
            database: "saguarodb".to_string(),
            backend_pid: 0,
        }
    }
}

/// One row from the session configuration source shared by `SHOW ALL` and
/// `pg_catalog.pg_settings`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GucSetting {
    pub name: String,
    pub setting: String,
    pub boot_val: String,
    pub reset_val: String,
    pub source: String,
}

/// Coarse PostgreSQL-compatible activity state for `pg_stat_activity`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Active,
    Idle,
    IdleInTransaction,
    IdleInTransactionAborted,
}

impl SessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionState::Active => "active",
            SessionState::Idle => "idle",
            SessionState::IdleInTransaction => "idle in transaction",
            SessionState::IdleInTransactionAborted => "idle in transaction (aborted)",
        }
    }
}

/// One activity row supplied to the virtual `pg_stat_activity` scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionActivityRow {
    pub datid: i32,
    pub datname: String,
    pub pid: i32,
    pub usesysid: i32,
    pub usename: String,
    pub application_name: String,
    pub backend_start: i64,
    pub xact_start: Option<i64>,
    pub query_start: Option<i64>,
    pub state_change: Option<i64>,
    pub state: SessionState,
    pub query: String,
}

/// Server/session state consumed by virtual system catalogs. Library crates get a
/// no-op provider by default so non-server tests and recovery scaffolding do not
/// need to manufacture session registries.
pub trait SystemStateProvider: Send + Sync + std::fmt::Debug {
    fn settings(&self) -> Vec<GucSetting>;
    fn setting(&self, name: &str) -> Option<String> {
        self.settings()
            .into_iter()
            .find(|setting| setting.name.eq_ignore_ascii_case(name))
            .map(|setting| setting.setting)
    }
    fn sessions(&self) -> Vec<SessionActivityRow>;
}

#[derive(Debug)]
struct NoSystemStateProvider;

impl SystemStateProvider for NoSystemStateProvider {
    fn settings(&self) -> Vec<GucSetting> {
        Vec::new()
    }

    fn sessions(&self) -> Vec<SessionActivityRow> {
        Vec::new()
    }
}

pub fn no_system_state() -> Arc<dyn SystemStateProvider> {
    Arc::new(NoSystemStateProvider)
}

/// Rendered catalog facts consumed by PostgreSQL-compatible introspection
/// functions. The `common` crate owns scalar-function dispatch but must remain a
/// leaf crate, so the real server adapter implements this trait with primitive
/// OID/name inputs rather than exposing catalog crate types here.
pub trait CatalogIntrospectionProvider: Send + Sync + std::fmt::Debug {
    fn pg_get_indexdef(
        &self,
        _index_oid: i64,
        _column: Option<i64>,
        _pretty: bool,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    fn pg_get_constraintdef(&self, _constraint_oid: i64, _pretty: bool) -> Result<Option<String>> {
        Ok(None)
    }

    fn pg_get_expr(&self, expr: &str, _relation_oid: i64, _pretty: bool) -> Result<Option<String>> {
        Ok(Some(expr.to_string()))
    }

    fn pg_get_userbyid(&self, _role_oid: i64) -> Result<Option<String>> {
        Ok(None)
    }

    fn pg_table_is_visible(&self, _relation_oid: i64) -> Result<bool> {
        Ok(false)
    }

    fn to_regclass(&self, _name: &str) -> Result<Option<i64>> {
        Ok(None)
    }

    fn pg_get_serial_sequence(&self, _table: &str, _column: &str) -> Result<Option<String>> {
        Ok(None)
    }
}

#[derive(Debug)]
struct NoCatalogIntrospectionProvider;

impl CatalogIntrospectionProvider for NoCatalogIntrospectionProvider {}

pub fn no_catalog_introspection() -> Arc<dyn CatalogIntrospectionProvider> {
    Arc::new(NoCatalogIntrospectionProvider)
}

/// Records what a `SERIALIZABLE` transaction reads (SIREAD locks) and forms
/// rw-antidependency edges when a write overwrites a concurrent read, so the server's
/// serializable-conflict manager can detect dangerous structures and abort to preserve
/// serializability (`docs/specs/ssi.md`). The executor's scan operators call the
/// `record_*` methods; the storage write paths call `note_write`. Only `SERIALIZABLE`
/// transactions install the real tracker; every other context keeps the no-op
/// [`NoSsiTracker`], so Read Committed / Repeatable Read pay nothing.
pub trait SsiTracker: Send + Sync + std::fmt::Debug {
    /// Record a point read of `(table, key)` by serializable transaction `reader` (an
    /// exact-key index lookup) — recorded even when no row matches, so a later insert
    /// of that key is caught as a phantom.
    fn record_tuple_read(&self, reader: TxnId, table: TableId, key: &Key);
    /// Record a scan / range read of the whole `table` by serializable `reader`.
    fn record_relation_read(&self, reader: TxnId, table: TableId);
    /// A write of `(table, key)` by serializable `writer`: form rw-edges from the
    /// concurrent SIREAD holders of the item to `writer`, then run edge-time
    /// dangerous-structure detection. Returns `Err` with `SerializationFailure`
    /// (`40001`) when `writer` is the SSI victim and must abort; otherwise `Ok`.
    fn note_write(&self, writer: TxnId, table: TableId, key: &Key) -> Result<()>;
}

/// The default no-op `SsiTracker` for non-serializable (Read Committed / Repeatable
/// Read) and pre-capture / test contexts: recording does nothing and `note_write`
/// returns `Ok`. Outside `SERIALIZABLE` there is no read tracking and a write never
/// fails an SSI check, so a silent no-op is correct here — in contrast to
/// [`NoConflictWaiter`], where a no-op would mask a mis-wired write path.
#[derive(Debug)]
struct NoSsiTracker;

impl SsiTracker for NoSsiTracker {
    fn record_tuple_read(&self, _reader: TxnId, _table: TableId, _key: &Key) {}
    fn record_relation_read(&self, _reader: TxnId, _table: TableId) {}
    fn note_write(&self, _writer: TxnId, _table: TableId, _key: &Key) -> Result<()> {
        Ok(())
    }
}

/// Per-statement execution context threaded into every storage operation.
///
/// `snapshot` is the visibility snapshot threaded into the storage engine's read
/// paths (`docs/specs/mvcc.md` §5.5, §6). The server's transaction paths capture a
/// real snapshot via [`StatementContext::with_snapshot`]; [`StatementContext::new`]
/// fills it with the equivalent [`Snapshot::sees_all_committed`] placeholder so
/// pre-capture call sites (tests, recovery scaffolding) see every committed row and
/// own write.
///
/// The snapshot is held behind an [`Arc`] so the executor can clone a
/// `StatementContext` per scan operator (`crates/executor/src/query.rs`) by bumping
/// a refcount rather than deep-cloning the `xip` vector. With concurrent
/// transactions (Milestone C) `xip` is no longer always empty, so the share matters
/// (`docs/specs/mvcc.md` §10 C3). `isolation` is honored by the server's snapshot
/// capture from Milestone C (Read Committed = fresh per statement, Repeatable Read =
/// captured once); the storage engine does not consult it.
/// `PartialEq`/`Eq` are hand-rolled below (not derived) because `conflict_waiter`,
/// `cancel`, `ssi_tracker`, `sequence_manager`, and `session_sequences` are not
/// comparable; equality compares the value fields only.
#[derive(Clone, Debug)]
pub struct StatementContext {
    pub txn_id: u64,
    pub snapshot: Arc<Snapshot>,
    pub isolation: IsolationLevel,
    /// Statement start timestamp as UTC microseconds since the Unix epoch. SQL
    /// clock functions such as `CURRENT_TIMESTAMP` and `now()` read this value so
    /// repeated calls within one statement are stable.
    pub statement_timestamp_micros: i64,
    /// Blocks this statement when it hits an in-progress row-lock conflict, until
    /// the holder finishes (`docs/specs/deadlock.md`). The default
    /// (`NoConflictWaiter`) errors if ever asked to wait; the server installs the
    /// real lock manager on write-capable contexts.
    pub conflict_waiter: Arc<dyn ConflictWaiter>,
    /// Per-statement cancellation state shared with the connection. Threaded to
    /// storage conflict waits so user requests and statement timeouts can interrupt
    /// a blocked writer (`docs/specs/deadlock.md` §5).
    pub cancel: Arc<QueryCancel>,
    /// The reading/writing transaction's **live (sub)xid set** — `txn_id` plus any
    /// not-rolled-back savepoint subxids (`docs/specs/savepoints.md` §4). A tuple
    /// whose `xmin`/`xmax` is in this set is the transaction's own (uncommitted)
    /// effect, visible to it and not a self-conflict. Defaults to just `[txn_id]`
    /// (no savepoints); the server widens it for a transaction with open/released
    /// savepoints. `Arc`-shared so the executor clones a context per scan operator
    /// cheaply, like `snapshot`.
    pub live_txns: Arc<[u64]>,
    /// The GC horizon (minimum advertised snapshot `xmin`) the server captured for
    /// this statement. Consumed ONLY by the storage engine's HOT update-path prune
    /// (`docs/specs/mvcc.md` §10 Milestone H3): when a same-page HOT update has no
    /// room, the engine collapses that page's committed-dead HOT prefixes to reclaim
    /// space before falling back. A stale/smaller horizon only prunes less, never
    /// unsafely, so it is captured before execution under the shared writer guard.
    /// Defaults to `0` (prune nothing committed-dead) for pre-capture / read / test
    /// contexts; the server sets it on write paths via [`StatementContext::with_gc_horizon`].
    pub gc_horizon: u64,
    /// Records SIREAD locks and forms rw-antidependency edges for a `SERIALIZABLE`
    /// transaction (`docs/specs/ssi.md`). The default [`NoSsiTracker`] is a no-op, so
    /// Read Committed / Repeatable Read are untouched; the server installs the real
    /// serializable-conflict manager only on SERIALIZABLE contexts.
    pub ssi_tracker: Arc<dyn SsiTracker>,
    /// Runtime sequence implementation used by `nextval`/`setval`. Defaults to a
    /// loud error so a sequence-mutating expression cannot accidentally run without
    /// the server wiring the storage engine into the context.
    pub sequence_manager: Arc<dyn SequenceManager>,
    /// Per-connection `currval` memory. The server installs a connection-owned
    /// handle; tests and non-session helpers get a fresh empty state by default.
    pub session_sequences: Arc<SessionSequenceState>,
    /// Connection identity for system information functions.
    pub session_info: Arc<SessionInfo>,
    /// Server/session state visible through virtual system catalogs and
    /// `current_setting`.
    pub system_state: Arc<dyn SystemStateProvider>,
    /// Rendered catalog facts for PostgreSQL-compatible introspection functions.
    pub catalog_introspection: Arc<dyn CatalogIntrospectionProvider>,
}

impl StatementContext {
    /// Construct a context for `txn_id` carrying the degenerate "sees all
    /// committed" snapshot ([`Snapshot::sees_all_committed`]) and the default
    /// isolation level. This is the placeholder used before real snapshot capture:
    /// every committed row and own write is visible, so the snapshot-aware read
    /// paths filter nothing.
    pub fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            snapshot: Arc::new(Snapshot::sees_all_committed()),
            isolation: IsolationLevel::default(),
            statement_timestamp_micros: now_micros(),
            gc_horizon: 0,
            live_txns: Arc::from([txn_id]),
            conflict_waiter: Arc::new(NoConflictWaiter),
            cancel: Arc::new(QueryCancel::new()),
            ssi_tracker: Arc::new(NoSsiTracker),
            sequence_manager: Arc::new(NoSequenceManager),
            session_sequences: Arc::new(SessionSequenceState::new()),
            session_info: Arc::new(SessionInfo::default()),
            system_state: no_system_state(),
            catalog_introspection: no_catalog_introspection(),
        }
    }

    /// Construct a context for `txn_id` carrying a shared, already-captured
    /// `snapshot` and the default isolation level. Used by the server's
    /// transaction read/write paths to thread the visibility snapshot into the
    /// storage engine; the shared `Arc` is cloned cheaply per scan operator.
    pub fn with_snapshot(txn_id: u64, snapshot: Arc<Snapshot>) -> Self {
        Self {
            txn_id,
            snapshot,
            isolation: IsolationLevel::default(),
            statement_timestamp_micros: now_micros(),
            gc_horizon: 0,
            live_txns: Arc::from([txn_id]),
            conflict_waiter: Arc::new(NoConflictWaiter),
            cancel: Arc::new(QueryCancel::new()),
            ssi_tracker: Arc::new(NoSsiTracker),
            sequence_manager: Arc::new(NoSequenceManager),
            session_sequences: Arc::new(SessionSequenceState::new()),
            session_info: Arc::new(SessionInfo::default()),
            system_state: no_system_state(),
            catalog_introspection: no_catalog_introspection(),
        }
    }

    /// Like [`StatementContext::with_snapshot`] but also carries an explicit
    /// `isolation` level (the server sets this from the active transaction).
    pub fn with_snapshot_and_isolation(
        txn_id: u64,
        snapshot: Arc<Snapshot>,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            txn_id,
            snapshot,
            isolation,
            statement_timestamp_micros: now_micros(),
            gc_horizon: 0,
            live_txns: Arc::from([txn_id]),
            conflict_waiter: Arc::new(NoConflictWaiter),
            cancel: Arc::new(QueryCancel::new()),
            ssi_tracker: Arc::new(NoSsiTracker),
            sequence_manager: Arc::new(NoSequenceManager),
            session_sequences: Arc::new(SessionSequenceState::new()),
            session_info: Arc::new(SessionInfo::default()),
            system_state: no_system_state(),
            catalog_introspection: no_catalog_introspection(),
        }
    }

    /// Set the GC horizon for this statement (the H3 update-path prune; see the field
    /// doc). Builder-style so the server threads it after constructing the context.
    #[must_use]
    pub fn with_gc_horizon(mut self, gc_horizon: u64) -> Self {
        self.gc_horizon = gc_horizon;
        self
    }

    /// Set the transaction's live (sub)xid set (the server uses this for a
    /// transaction with savepoints; see the `live_txns` field). Builder-style.
    #[must_use]
    pub fn with_live_txns(mut self, live_txns: Arc<[u64]>) -> Self {
        self.live_txns = live_txns;
        self
    }

    /// Override this statement's timestamp. Used by deterministic tests; server
    /// paths rely on the constructor default captured when the context is built.
    #[must_use]
    pub fn with_statement_timestamp_micros(mut self, statement_timestamp_micros: i64) -> Self {
        self.statement_timestamp_micros = statement_timestamp_micros;
        self
    }

    /// Install the real conflict waiter (the server's lock manager) and the
    /// connection's cancel flag for a write-capable statement (`docs/specs/deadlock.md`).
    /// Builder-style.
    #[must_use]
    pub fn with_conflict_waiter(
        mut self,
        conflict_waiter: Arc<dyn ConflictWaiter>,
        cancel: Arc<QueryCancel>,
    ) -> Self {
        self.conflict_waiter = conflict_waiter;
        self.cancel = cancel;
        self
    }

    /// Install the real SSI tracker (the server's serializable-conflict manager) for a
    /// `SERIALIZABLE` statement (`docs/specs/ssi.md`). Builder-style.
    #[must_use]
    pub fn with_ssi_tracker(mut self, ssi_tracker: Arc<dyn SsiTracker>) -> Self {
        self.ssi_tracker = ssi_tracker;
        self
    }

    /// Install the runtime sequence manager used by `nextval`/`setval`.
    #[must_use]
    pub fn with_sequence_manager(mut self, sequence_manager: Arc<dyn SequenceManager>) -> Self {
        self.sequence_manager = sequence_manager;
        self
    }

    /// Install the per-session sequence state used by `currval`.
    #[must_use]
    pub fn with_session_sequences(mut self, session_sequences: Arc<SessionSequenceState>) -> Self {
        self.session_sequences = session_sequences;
        self
    }

    /// Install the connection identity used by system information functions.
    #[must_use]
    pub fn with_session_info(mut self, session_info: Arc<SessionInfo>) -> Self {
        self.session_info = session_info;
        self
    }

    /// Install the server/session provider used by virtual system catalogs and
    /// `current_setting`.
    #[must_use]
    pub fn with_system_state(mut self, system_state: Arc<dyn SystemStateProvider>) -> Self {
        self.system_state = system_state;
        self
    }

    /// Install the provider used by PostgreSQL-compatible catalog introspection
    /// functions.
    #[must_use]
    pub fn with_catalog_introspection(
        mut self,
        catalog_introspection: Arc<dyn CatalogIntrospectionProvider>,
    ) -> Self {
        self.catalog_introspection = catalog_introspection;
        self
    }

    /// Advance `sequence` via [`SequenceManager::nextval`] and record the returned
    /// value as this session's `currval`, returning the new value. Shared by the
    /// `nextval(...)` expression and the INSERT/COPY `DEFAULT nextval` path so both
    /// keep the `currval` bookkeeping identical.
    pub fn nextval_recording_currval(&self, sequence: SequenceId) -> Result<i64> {
        let value = self.sequence_manager.nextval(self.txn_id, sequence)?;
        self.session_sequences.record_currval(sequence, value)?;
        Ok(value)
    }
}

// Hand-rolled to exclude handles that are not comparable; two contexts are equal
// when their value fields match, as before these runtime handles were added.
impl PartialEq for StatementContext {
    fn eq(&self, other: &Self) -> bool {
        self.txn_id == other.txn_id
            && self.snapshot == other.snapshot
            && self.isolation == other.isolation
            && self.live_txns == other.live_txns
            && self.gc_horizon == other.gc_horizon
            && self.session_info == other.session_info
    }
}

impl Eq for StatementContext {}

#[cfg(test)]
mod tests {
    use super::{IsolationLevel, QueryCancel, Snapshot, StatementContext};

    #[test]
    fn default_conflict_waiter_errors_rather_than_spinning() {
        // A read/test context's waiter must never be asked to wait (a real
        // WouldBlock only arises on a write path with the real lock manager). If it
        // ever is, it errors loudly instead of returning Ok and spinning forever.
        let ctx = StatementContext::new(7);
        let err = ctx
            .conflict_waiter
            .wait_for(7, 8, &QueryCancel::new())
            .expect_err("default waiter must error, not return Ok");
        assert!(err.message.contains("no conflict waiter configured"));
    }

    #[test]
    fn new_sets_txn_id_and_placeholder_fields() {
        let ctx = StatementContext::new(42);
        assert_eq!(ctx.txn_id, 42);
        // The placeholder is the degenerate "sees all committed" snapshot, not the
        // empty (sees-nothing) one, so pre-capture reads return committed rows.
        assert_eq!(*ctx.snapshot, Snapshot::sees_all_committed());
        assert_eq!(ctx.isolation, IsolationLevel::ReadCommitted);
    }

    #[test]
    fn contexts_with_same_txn_id_are_equal() {
        assert_eq!(StatementContext::new(7), StatementContext::new(7));
        assert_ne!(StatementContext::new(7), StatementContext::new(8));
    }

    #[test]
    fn statement_timestamp_is_stable_and_not_part_of_context_identity() {
        let ctx = StatementContext::new(7).with_statement_timestamp_micros(123);
        assert_eq!(ctx.statement_timestamp_micros, 123);
        assert_eq!(
            ctx,
            StatementContext::new(7).with_statement_timestamp_micros(456)
        );
    }

    #[test]
    fn default_session_info_names_the_single_database() {
        let ctx = StatementContext::new(1);
        assert_eq!(ctx.session_info.user, "saguarodb");
        assert_eq!(ctx.session_info.database, "saguarodb");
        assert_eq!(ctx.session_info.backend_pid, 0);

        let custom = std::sync::Arc::new(super::SessionInfo {
            user: "ada".to_string(),
            database: "mainframe".to_string(),
            backend_pid: 7,
        });
        let ctx = StatementContext::new(1).with_session_info(custom.clone());
        assert_eq!(ctx.session_info, custom);
    }

    #[test]
    fn default_ssi_tracker_is_a_no_op() {
        // Outside SERIALIZABLE there is no read tracking and a write never fails an
        // SSI check: recording is a no-op and `note_write` returns Ok (unlike the
        // conflict waiter, whose default errors). Equality ignores the tracker.
        let ctx = StatementContext::new(7);
        let key = crate::row::Key(vec![crate::value::Value::Integer(1)]);
        ctx.ssi_tracker.record_tuple_read(7, 1, &key);
        ctx.ssi_tracker.record_relation_read(7, 1);
        assert!(ctx.ssi_tracker.note_write(7, 1, &key).is_ok());
        assert_eq!(ctx, StatementContext::new(7));
    }
}
