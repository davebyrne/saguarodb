use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{ColumnInfo, DataType, DbError, Result, Snapshot, StatementContext, Value};
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

impl QueryService {
    pub fn new(components: Arc<ServerComponents>) -> Self {
        Self {
            components,
            engine: QueryEngine,
        }
    }

    pub fn execute_sql(&self, sql: &str) -> Result<ExecutionResult> {
        self.execute_sql_cancelable(sql, &AtomicBool::new(false))
    }

    /// Like `execute_sql`, but aborts with `QueryCanceled` if `cancel` becomes
    /// set (from another connection's `CancelRequest`) while the query runs.
    pub fn execute_sql_cancelable(
        &self,
        sql: &str,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        let statement = parser::parse(sql)?;
        let class = statement_class(&statement)?;
        let bound = bind(&statement, self.components.catalog.as_ref())?;
        self.execute_bound(class, bound, cancel)
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
        let (bound, param_types) = bind_parameterized(
            &statement,
            self.components.catalog.as_ref(),
            declared_param_types,
        )?;
        let result_columns = result_columns(&bound);
        Ok(PreparedStatement {
            class,
            bound,
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

    /// Like `execute_prepared`, but cancelable mid-flight via `cancel`.
    pub fn execute_prepared_cancelable(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        if params.len() != prepared.param_types.len() {
            return Err(DbError::protocol(
                common::SqlState::SyntaxError,
                format!(
                    "prepared statement requires {} parameter(s), but {} were supplied",
                    prepared.param_types.len(),
                    params.len()
                ),
            ));
        }
        let bound = substitute_params(&prepared.bound, params)?;
        self.execute_bound(prepared.class, bound, cancel)
    }
}

impl QueryService {
    fn execute_bound(
        &self,
        class: StatementClass,
        bound: BoundStatement,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        match class {
            StatementClass::Read => self.execute_read_bound(bound, cancel),
            StatementClass::Write => self.execute_write_bound(bound, cancel),
        }
    }

    fn execute_read_bound(
        &self,
        bound: BoundStatement,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        let _guard = self.components.concurrency.begin_read()?;
        match bound {
            BoundStatement::Explain(inner) => {
                if !matches!(inner.as_ref(), BoundStatement::Select(_)) {
                    return Err(DbError::plan(
                        common::SqlState::SyntaxError,
                        "EXPLAIN supports SELECT only in v1",
                    ));
                }
                let logical = logical_plan(inner.as_ref())?;
                let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
                Ok(ExecutionResult::Explanation {
                    text: format_explain(&physical),
                })
            }
            other => {
                let logical = logical_plan(&other)?;
                let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
                // A read is not its own transaction (txn_id 0 / INVALID_XID), so no
                // own txn is excluded from the snapshot. Under single-writer
                // autocommit, reads and writes are mutually exclusive (shared vs.
                // exclusive concurrency guard), so no write txn is active and `xip`
                // is empty — the snapshot sees all committed rows.
                let snapshot = self.capture_autocommit_snapshot(0);
                let ctx = self.execution_context(0, snapshot, cancel);
                self.engine.execute(&ctx, &physical)
            }
        }
    }

    fn execute_write_bound(
        &self,
        bound: BoundStatement,
        cancel: &AtomicBool,
    ) -> Result<ExecutionResult> {
        let guard = self.components.concurrency.begin_write()?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        let txn_id = self.components.next_txn_id.fetch_add(1, Ordering::AcqRel);
        // The autocommit unit begins: register the transaction active. Its CLOG
        // status is `InProgress` implicitly (the default for any unsettled normal
        // id) until a `Commit`/`Abort` record settles it.
        self.components.active_txns.register(txn_id);
        let catalog_before = self.components.catalog.snapshot()?;
        // Capture the snapshot after registering this txn, excluding its own id so
        // own writes are seen via the predicate's `current_txn` path rather than
        // hidden as "in-progress". Under single-writer autocommit this txn is the
        // only active one, so `xip` is empty and the snapshot sees all committed.
        let snapshot = self.capture_autocommit_snapshot(txn_id);
        let ctx = self.execution_context(txn_id, snapshot, cancel);

        let result = catch_unwind(AssertUnwindSafe(|| self.engine.execute(&ctx, &physical)));
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                if let Err(rollback_err) = self.rollback_pre_durable(txn_id, Some(catalog_before)) {
                    self.fatal_pre_durable_rollback_failure(rollback_err);
                }
                return Err(err);
            }
            Err(_) => {
                if let Err(rollback_err) = self.rollback_pre_durable(txn_id, Some(catalog_before)) {
                    self.fatal_pre_durable_rollback_failure(rollback_err);
                }
                return Err(DbError::internal("statement execution panicked"));
            }
        };

        if let Err(err) = self.append_and_flush_commit(txn_id) {
            if let Err(rollback_err) = self.rollback_pre_durable(txn_id, Some(catalog_before)) {
                self.fatal_pre_durable_rollback_failure(rollback_err);
            }
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

    fn execution_context<'a>(
        &'a self,
        txn_id: u64,
        snapshot: Snapshot,
        cancel: &'a AtomicBool,
    ) -> ExecutionContext<'a> {
        ExecutionContext {
            statement: StatementContext::with_snapshot(txn_id, snapshot),
            catalog: self.components.catalog.as_ref(),
            storage: self.components.storage.as_ref(),
            schema_ops: self.components.storage.as_ref(),
            cancel,
        }
    }

    /// Capture the autocommit snapshot for a statement (`docs/specs/mvcc.md`
    /// §5.5). It sees every already-committed transaction:
    ///
    /// - `xmax` is the next txn id to be assigned, so every allocated (hence
    ///   committed under single-writer autocommit) id is `< xmax`.
    /// - `xip` is the currently-active set minus `own_txn` (the statement's own
    ///   txn is seen via the predicate's own-write path, not as in-progress). A
    ///   read passes `own_txn = 0` (it has no txn); nothing is excluded.
    /// - `xmin` is the oldest active id, or `xmax` if none are active.
    ///
    /// In single-writer mode `xip` is empty (the only active id, if any, is
    /// `own_txn`), making this the degenerate "sees all committed" snapshot — so
    /// read results are unchanged until Milestone C captures real snapshots.
    fn capture_autocommit_snapshot(&self, own_txn: u64) -> Snapshot {
        let xmax = self.components.next_txn_id.load(Ordering::Acquire);
        let xip: Vec<u64> = self
            .components
            .active_txns
            .active_ids()
            .into_iter()
            .filter(|&id| id != own_txn)
            .collect();
        let xmin = self.components.active_txns.oldest().unwrap_or(xmax);
        Snapshot { xmin, xmax, xip }
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

    fn rollback_pre_durable(
        &self,
        txn_id: u64,
        catalog_before: Option<catalog::CatalogSnapshot>,
    ) -> Result<()> {
        // Record the abort: append an `Abort` record (which sets the CLOG to
        // `Aborted`) and drop the transaction from the active set. The abort is
        // not flushed — abort durability is not critical, since a transaction with
        // no durable `Commit` is recovered as aborted regardless. A failure to
        // append it is logged but not fatal: the in-memory rollback below (the
        // mechanism A3 leaves in place until C3) still restores correctness.
        if let Err(err) = self.components.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::Abort,
        }) {
            eprintln!("failed to append Abort record for txn {txn_id}: {err}");
        }
        self.components.active_txns.deregister(txn_id);

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

#[derive(Clone, Copy)]
enum StatementClass {
    Read,
    Write,
}

/// A parsed and bound extended-protocol statement that can be executed
/// repeatedly with different parameter values.
pub struct PreparedStatement {
    class: StatementClass,
    bound: BoundStatement,
    param_types: Vec<DataType>,
    result_columns: Option<Vec<ColumnInfo>>,
}

impl PreparedStatement {
    /// Resolved parameter types, by position.
    pub fn param_types(&self) -> &[DataType] {
        &self.param_types
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
                common::SqlState::SyntaxError,
                "EXPLAIN supports SELECT only in v1",
            )),
        },
        Statement::Insert { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::CreateTable { .. }
        | Statement::DropTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::DropIndex { .. } => Ok(StatementClass::Write),
        // TEMPORARY: replaced by real lifecycle in Milestone C3. BEGIN/COMMIT/
        // ROLLBACK now parse to first-class statements (C1), but the
        // multi-statement transaction lifecycle is not wired yet. Reject here
        // rather than silently no-op a BEGIN (which would falsely tell a client
        // a transaction had started). This is the dispatch seam C3 generalizes
        // into transaction lifecycle handling.
        Statement::Begin | Statement::Commit | Statement::Rollback => Err(DbError::plan(
            common::SqlState::FeatureNotSupported,
            "multi-statement transactions are not yet supported",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;

    use catalog::CatalogSnapshot;
    use common::{DataType, SqlState, Value};

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
    async fn transaction_control_statements_are_temporarily_unsupported() {
        // C1 parses BEGIN/COMMIT/ROLLBACK to first-class statements, but the
        // lifecycle is not wired until C3. Each must fail with a clear,
        // structured feature-not-supported error rather than silently no-op a
        // BEGIN (which would mislead a client into thinking a txn had started).
        // This test marks the exact behavior C3 must replace.
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        for sql in ["begin", "commit", "rollback"] {
            let err = app.query_service.execute_sql(sql).unwrap_err();
            assert_eq!(err.code, SqlState::FeatureNotSupported, "for `{sql}`");
        }
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
