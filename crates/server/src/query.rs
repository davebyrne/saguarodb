use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::{DbError, Result, StatementContext};
use executor::{ExecutionContext, ExecutionResult, QueryEngine};
use parser::Statement;
use planner::{BoundStatement, bind, format_explain, logical_plan, physical_plan};
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

    pub fn execute_sql(&self, _sql: &str) -> Result<ExecutionResult> {
        let statement = parser::parse(_sql)?;
        match statement_class(&statement)? {
            StatementClass::Read => self.execute_read(&statement),
            StatementClass::Write => self.execute_write(&statement),
        }
    }
}

impl QueryService {
    fn execute_read(&self, statement: &Statement) -> Result<ExecutionResult> {
        let _guard = self.components.concurrency.begin_read()?;
        let bound = bind(statement, self.components.catalog.as_ref())?;
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
                let ctx = self.execution_context(0);
                self.engine.execute(&ctx, &physical)
            }
        }
    }

    fn execute_write(&self, statement: &Statement) -> Result<ExecutionResult> {
        let guard = self.components.concurrency.begin_write()?;
        let bound = bind(statement, self.components.catalog.as_ref())?;
        let logical = logical_plan(&bound)?;
        let physical = physical_plan(&logical, self.components.catalog.as_ref())?;
        let txn_id = self.components.next_txn_id.fetch_add(1, Ordering::AcqRel);
        let catalog_before = self.components.catalog.snapshot()?;
        let ctx = self.execution_context(txn_id);

        let result = catch_unwind(AssertUnwindSafe(|| self.engine.execute(&ctx, &physical)));
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                if let Err(rollback_err) =
                    self.rollback_pre_durable(txn_id, Some(catalog_before))
                {
                    self.fatal_pre_durable_rollback_failure(rollback_err);
                }
                return Err(err);
            }
            Err(_) => {
                if let Err(rollback_err) =
                    self.rollback_pre_durable(txn_id, Some(catalog_before))
                {
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
        drop(guard);

        if let Err(err) = record_commit_and_maybe_checkpoint(&self.components) {
            eprintln!("checkpoint failed after committed statement: {err}");
        }

        Ok(result)
    }

    fn execution_context(&self, txn_id: u64) -> ExecutionContext<'_> {
        ExecutionContext {
            statement: StatementContext { txn_id },
            catalog: self.components.catalog.as_ref(),
            storage: self.components.storage.as_ref(),
            schema_ops: self.components.storage.as_ref(),
        }
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

enum StatementClass {
    Read,
    Write,
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
        | Statement::DropTable { .. } => Ok(StatementClass::Write),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use catalog::CatalogSnapshot;
    use common::{SqlState, Value};

    use crate::app::AppState;

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
        };

        let err = service
            .rollback_pre_durable(99, Some(invalid_snapshot))
            .unwrap_err();

        assert!(err.message.contains("catalog restore failed"));
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
}
