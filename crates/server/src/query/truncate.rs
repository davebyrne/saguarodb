use std::sync::{Arc, atomic::Ordering};

use common::{DbError, QueryCancel, RelationKind, Result, SqlState, StatementContext};
use executor::ExecutionResult;
use parser::Statement;

use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;
use crate::lock_manager::{ObjectLockRequest, RelationLockMode};

use super::QueryService;

impl QueryService {
    /// `TRUNCATE [TABLE] <table> [, ...]`: one statement-atomic relation-generation
    /// swap under xid-owned `AccessExclusive` locks. Logical table ids stay stable;
    /// the catalog allocates fresh physical storage ids and storage prepares every
    /// empty replacement before the single durable commit point. Catalog and
    /// storage publish the complete batch while the relation publish gate blocks
    /// new snapshot capture from observing the committed pre-publish gap.
    pub(super) fn run_truncate(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        let Statement::Truncate { tables } = statement else {
            return Err(DbError::internal(
                "run_truncate called with a non-TRUNCATE statement",
            ));
        };
        let components = &self.components;

        {
            let mut schemas = {
                let _catalog_read = components
                    .catalog_publication_gate
                    .read()
                    .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
                resolve_truncate_tables(components, &tables)?
            };
            let txn_id = components
                .active_txns
                .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
            let writer_guard = match components.concurrency.begin_writer() {
                Ok(guard) => guard,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            let mut object_guard = match components.lock_manager.transaction_owner(txn_id) {
                Ok(guard) => guard,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            let baseline = object_guard.snapshot();
            let (catalog_publication, current) = loop {
                let requests = schemas
                    .iter()
                    .map(|schema| {
                        ObjectLockRequest::table(schema.id, RelationLockMode::AccessExclusive)
                    })
                    .collect::<Vec<_>>();
                if let Err(err) = object_guard.acquire_many(&requests, cancel) {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
                let catalog_publication = match components.catalog_publication_gate.write() {
                    Ok(guard) => guard,
                    Err(_) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(DbError::internal("catalog publication gate poisoned"));
                    }
                };
                let current = match resolve_truncate_tables(components, &tables) {
                    Ok(current) => current,
                    Err(err) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(err);
                    }
                };
                if current.iter().map(|schema| schema.id).collect::<Vec<_>>()
                    == schemas.iter().map(|schema| schema.id).collect::<Vec<_>>()
                {
                    break (catalog_publication, current);
                }
                drop(catalog_publication);
                if let Err(err) = object_guard.restore(&baseline) {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
                schemas = current;
            };
            let mut plans = Vec::with_capacity(schemas.len());
            let mut updates = Vec::with_capacity(schemas.len());
            for schema in current {
                let plan = match components.catalog.prepare_truncate_table(schema.id) {
                    Ok(plan) => plan,
                    Err(err) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(err);
                    }
                };
                let update = match components.catalog.build_truncate_table_update(&plan) {
                    Ok(update) => update,
                    Err(err) => {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(err);
                    }
                };
                plans.push(plan);
                updates.push(update);
            }
            let ctx = StatementContext::new(txn_id)
                .with_conflict_waiter(components.lock_manager.clone(), cancel.clone());
            for (plan, update) in plans.iter().zip(&updates) {
                if let Err(err) = components
                    .storage
                    .prepare_truncate_table(&ctx, plan, update)
                {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            }

            let publish_gate = match components.relation_publish_gate.write() {
                Ok(guard) => guard,
                Err(_) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(DbError::internal("relation publish gate poisoned"));
                }
            };
            if let Err(err) = cancel.check() {
                drop(publish_gate);
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
                drop(publish_gate);
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }

            let committed_updates = match components.catalog.apply_truncate_tables(&plans) {
                Ok(updates) => updates,
                Err(err) => self.fatal_after_durable_commit(err),
            };
            if let Err(err) = components
                .storage
                .publish_truncate_tables(committed_updates)
            {
                self.fatal_after_durable_commit(err);
            }
            if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
                self.fatal_after_durable_commit(err);
            }
            components.active_txns.deregister(txn_id);
            components.lock_manager.on_txn_finished();
            drop(publish_gate);
            drop(catalog_publication);
            drop(object_guard);
            drop(writer_guard);
        }

        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);
        best_effort_retired_generation_cleanup(components);

        Ok(ExecutionResult::Modified {
            command: "TRUNCATE TABLE".to_string(),
            count: 0,
        })
    }
}

fn resolve_truncate_tables(
    components: &crate::app::ServerComponents,
    tables: &[String],
) -> Result<Vec<common::TableSchema>> {
    let mut schemas = Vec::with_capacity(tables.len());
    let mut target_ids = HashSet::with_capacity(tables.len());
    for table in tables {
        let schema = match components.catalog.get_table_by_name(table)? {
            Some(schema) => schema,
            None if components.catalog.get_view_by_name(table)?.is_some() => {
                return Err(DbError::plan(
                    SqlState::WrongObjectType,
                    format!("relation {table} is a view, not a table"),
                ));
            }
            None => {
                return Err(DbError::plan(
                    SqlState::UndefinedTable,
                    format!("table {table} does not exist"),
                ));
            }
        };
        if schema.relation_kind != RelationKind::User {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "cannot truncate hidden TOAST relation",
            ));
        }
        if !target_ids.insert(schema.id) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!("duplicate TRUNCATE target {table}"),
            ));
        }
        schemas.push(schema);
    }
    Ok(schemas)
}

fn best_effort_retired_generation_cleanup(components: &crate::app::ServerComponents) {
    if let Err(err) = components.storage.try_cleanup_retired_generations() {
        eprintln!("best-effort relation-generation cleanup after TRUNCATE failed: {err}");
    }
}
use std::collections::HashSet;
