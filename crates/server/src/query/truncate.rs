use std::collections::HashSet;
use std::sync::{Arc, atomic::Ordering};

use catalog::CatalogManager;
use common::{
    DbError, IsolationLevel, PUBLIC_SCHEMA_ID, QualifiedName, QueryCancel, RelationKind, Result,
    SqlState, SsiTracker, StatementContext, TableSchema, TruncateTablePlan,
};
use executor::ExecutionResult;
use parser::Statement;

use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;
use crate::lock_manager::{CatalogLockMode, ObjectLockRequest, RelationLockMode};

use super::{QueryService, Transaction};

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
                resolve_truncate_tables(components.catalog.as_ref(), &tables)?
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
                let mut requests = schemas
                    .iter()
                    .map(|schema| {
                        ObjectLockRequest::schema(schema.schema_id, CatalogLockMode::Access)
                    })
                    .collect::<Vec<_>>();
                requests.extend(
                    schemas
                        .iter()
                        .map(|schema| {
                            ObjectLockRequest::table(schema.id, RelationLockMode::AccessExclusive)
                        })
                        .collect::<Vec<_>>(),
                );
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
                let current = match resolve_truncate_tables(components.catalog.as_ref(), &tables) {
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
            for schema in &current {
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

    pub(super) fn run_truncate_in_transaction(
        &self,
        txn: &mut Transaction,
        statement: Statement,
        cancel: &QueryCancel,
    ) -> Result<ExecutionResult> {
        let Statement::Truncate { tables } = statement else {
            return Err(DbError::internal(
                "run_truncate_in_transaction called with a non-TRUNCATE statement",
            ));
        };
        let updates_before = txn.truncate_updates.clone();
        let mut schemas = {
            let catalog =
                self.transaction_catalog_from_parts(&txn.catalog_overlay, &updates_before)?;
            resolve_truncate_tables(catalog.as_ref(), &tables)?
        };
        let catalog_overlay = txn.catalog_overlay.clone();
        let objects = self.ensure_transaction_lock_owner(txn, cancel)?;
        let baseline = objects.snapshot();
        let (schemas, catalog) = loop {
            let mut requests = schemas
                .iter()
                .map(|schema| ObjectLockRequest::schema(schema.schema_id, CatalogLockMode::Access))
                .collect::<Vec<_>>();
            requests.extend(
                schemas
                    .iter()
                    .map(|schema| {
                        ObjectLockRequest::table(schema.id, RelationLockMode::AccessExclusive)
                    })
                    .collect::<Vec<_>>(),
            );
            objects.acquire_many(&requests, cancel)?;
            let (current, catalog) = {
                let catalog =
                    self.transaction_catalog_from_parts(&catalog_overlay, &updates_before)?;
                let current = resolve_truncate_tables(catalog.as_ref(), &tables)?;
                (current, catalog)
            };
            if current.iter().map(|schema| schema.id).collect::<Vec<_>>()
                == schemas.iter().map(|schema| schema.id).collect::<Vec<_>>()
            {
                break (current, catalog);
            }
            objects.restore(&baseline)?;
            schemas = current;
        };

        let snapshots = self.snapshots_for_transaction(txn, cancel)?;
        if txn.isolation == IsolationLevel::Serializable {
            self.components
                .ssi_manager
                .register(txn.txn_id, snapshots.snapshot.clone());
            for schema in &schemas {
                self.components
                    .ssi_manager
                    .note_relation_write(txn.writing_xid(), schema.id)?;
            }
        }
        drop(snapshots.advertised);
        txn.first_statement_ran = true;

        let mut plans = Vec::with_capacity(schemas.len());
        let mut updates = Vec::with_capacity(schemas.len());
        for schema in &schemas {
            let plan = allocate_transactional_truncate_plan(
                self.components.catalog.as_ref(),
                catalog.as_ref(),
                schema,
            )?;
            let update = catalog.build_truncate_table_update(&plan)?;
            plans.push(plan);
            updates.push(update);
        }
        let ctx = StatementContext::new(txn.writing_xid()).with_conflict_waiter(
            self.components.lock_manager.clone(),
            Arc::new(QueryCancel::new()),
        );
        // Preparation appends WAL and registers replacement files in storage's
        // transaction rollback state. From this point onward even a failed
        // statement must take the write-abort path.
        txn.has_writes = true;
        for (plan, update) in plans.iter().zip(&updates) {
            self.components
                .storage
                .prepare_truncate_table(&ctx, plan, update)?;
        }
        let relation_publish = self
            .components
            .relation_publish_gate
            .write()
            .map_err(|_| DbError::internal("relation publish gate poisoned"))?;
        self.components
            .storage
            .publish_truncate_tables_transactional(txn.txn_id, updates.clone())?;
        drop(relation_publish);
        for update in updates {
            txn.truncate_updates.insert(update.table.id, update);
        }
        txn.relation_generation_changed = true;

        Ok(ExecutionResult::Modified {
            command: "TRUNCATE TABLE".to_string(),
            count: 0,
        })
    }
}

fn allocate_transactional_truncate_plan(
    allocator: &dyn CatalogManager,
    catalog: &dyn CatalogManager,
    schema: &TableSchema,
) -> Result<TruncateTablePlan> {
    let new_table_storage_id = allocator.allocate_storage_id()?;
    let new_toast_storage_id = match schema.toast_table_id {
        Some(toast_id) => Some((toast_id, allocator.allocate_storage_id()?)),
        None => None,
    };
    let mut indexes = catalog.list_indexes_for_table(schema.id)?;
    indexes.sort_unstable_by_key(|index| index.id);
    let new_index_storage_ids = indexes
        .into_iter()
        .map(|index| Ok((index.id, allocator.allocate_storage_id()?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(TruncateTablePlan {
        table_id: schema.id,
        new_table_storage_id,
        new_toast_storage_id,
        new_index_storage_ids,
    })
}

fn resolve_truncate_tables(
    catalog: &dyn CatalogManager,
    tables: &[QualifiedName],
) -> Result<Vec<common::TableSchema>> {
    let mut schemas = Vec::with_capacity(tables.len());
    let mut target_ids = HashSet::with_capacity(tables.len());
    for table in tables {
        let namespace = match &table.schema {
            Some(schema) => catalog
                .get_schema_by_name(schema)?
                .map(|schema| schema.id)
                .ok_or_else(|| {
                    DbError::plan(
                        SqlState::InvalidSchemaName,
                        format!("schema {schema} does not exist"),
                    )
                })?,
            None => PUBLIC_SCHEMA_ID,
        };
        let schema = match catalog.get_table_in_schema(namespace, &table.name)? {
            Some(schema) => schema,
            None if catalog
                .get_view_in_schema(namespace, &table.name)?
                .is_some()
                || catalog
                    .get_index_in_schema(namespace, &table.name)?
                    .is_some()
                || catalog
                    .get_sequence_in_schema(namespace, &table.name)?
                    .is_some() =>
            {
                return Err(DbError::plan(
                    SqlState::WrongObjectType,
                    format!("relation {table} is not a table"),
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

pub(super) fn best_effort_retired_generation_cleanup(components: &crate::app::ServerComponents) {
    if let Err(err) = components.storage.try_cleanup_retired_generations() {
        eprintln!("best-effort relation-generation cleanup after TRUNCATE failed: {err}");
    }
}
