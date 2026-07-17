use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLockReadGuard};

use common::{
    DbError, QualifiedName, QueryCancel, RelationKind, Result, SqlState, StatementContext,
    TableSchema,
};
use executor::ExecutionResult;
use parser::Statement;
use storage::StorageEngine;
use wal::{WalRecord, WalRecordKind};

use super::{PreparedRelationVersion, PreparedStatement, QueryService};
use crate::app::ServerComponents;
use crate::lock_manager::{ObjectLockRequest, RelationLockMode};

impl QueryService {
    pub(super) fn qualify_maintenance_statement(
        &self,
        mut statement: Statement,
        catalog: &dyn catalog::CatalogManager,
        search_path_names: &[String],
    ) -> Result<Statement> {
        let qualify = |name: &mut QualifiedName| -> Result<()> {
            if name.schema.is_some() {
                return Ok(());
            }
            for schema_name in search_path_names {
                let Some(schema) = catalog.get_schema_by_name(schema_name)? else {
                    continue;
                };
                if catalog
                    .get_table_in_schema(schema.id, &name.name)?
                    .is_some()
                    || catalog.get_view_in_schema(schema.id, &name.name)?.is_some()
                    || catalog
                        .get_index_in_schema(schema.id, &name.name)?
                        .is_some()
                    || catalog
                        .get_sequence_in_schema(schema.id, &name.name)?
                        .is_some()
                {
                    name.schema = Some(schema.name);
                    break;
                }
            }
            Ok(())
        };
        match &mut statement {
            Statement::Vacuum {
                table: Some(table), ..
            }
            | Statement::Analyze { table: Some(table) }
            | Statement::AlterTableSetCompression { table, .. }
            | Statement::AlterTableSetOptions { table, .. }
            | Statement::AlterTableAddPrimaryKey { table, .. }
            | Statement::AlterTableDropPrimaryKey { table, .. }
            | Statement::AlterTableDropConstraint { table, .. } => qualify(table)?,
            Statement::AlterTableAddForeignKey { table, foreign_key } => {
                qualify(table)?;
                qualify(&mut foreign_key.referenced_table)?;
            }
            Statement::Truncate { tables } => {
                for table in tables {
                    qualify(table)?;
                }
            }
            _ => {}
        }
        Ok(statement)
    }

    /// Run a prepared (extended-protocol) maintenance command. The statement
    /// carries no bound payload; unqualified targets were resolved against the
    /// effective search path at extended-protocol Parse time.
    pub(super) fn run_prepared_maintenance(
        &self,
        prepared: &PreparedStatement,
        session: &super::QuerySessionContext,
    ) -> Result<ExecutionResult> {
        let statement = prepared.maintenance.as_ref().ok_or_else(|| {
            DbError::internal("maintenance prepared statement has no carried payload")
        })?;
        if matches!(
            statement,
            Statement::AlterTableAddForeignKey { .. } | Statement::AlterTableDropConstraint { .. }
        ) {
            return self.run_maintenance_inner(
                statement.clone(),
                session.cancel(),
                session.gucs().default_statistics_target(),
                Some(&prepared.schema_versions),
            );
        }
        let mut identity_guard = self.components.lock_manager.statement_owner()?;
        let mut identity_requests = Vec::new();
        for name in maintenance_target_names(statement) {
            let Some(schema_name) = name.schema.as_deref() else {
                continue;
            };
            let Some(schema) = self.components.catalog.get_schema_by_name(schema_name)? else {
                continue;
            };
            identity_requests.push(ObjectLockRequest::schema(
                schema.id,
                crate::lock_manager::CatalogLockMode::Access,
            ));
            identity_requests.push(ObjectLockRequest::catalog_name(schema.id, &name.name));
        }
        identity_guard.acquire_many(&identity_requests, session.cancel())?;
        self.validate_prepared_schema_versions(&prepared.schema_versions)?;
        self.run_maintenance_inner(
            statement.clone(),
            session.cancel(),
            session.gucs().default_statistics_target(),
            None,
        )
    }

    /// Shared entry point for every maintenance command: dispatches to the
    /// statement-specific implementation. Both the simple-query and
    /// extended-protocol paths route maintenance through this one router.
    /// `statistics_target` is the session's `default_statistics_target`,
    /// consumed only by the ANALYZE passes.
    pub(super) fn run_maintenance(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
        statistics_target: u32,
    ) -> Result<ExecutionResult> {
        self.run_maintenance_inner(statement, cancel, statistics_target, None)
    }

    fn run_maintenance_inner(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
        statistics_target: u32,
        prepared_versions: Option<&[PreparedRelationVersion]>,
    ) -> Result<ExecutionResult> {
        cancel.check()?;
        match &statement {
            Statement::Vacuum { table, analyze } => {
                let analyze_target = analyze.then(|| table.clone());
                let result = self.run_vacuum(statement.clone(), cancel)?;
                // VACUUM ANALYZE: the statistics pass runs after reclamation
                // over the same targets; the tag stays VACUUM
                // (docs/specs/statistics.md §7).
                if let Some(table) = analyze_target {
                    self.run_analyze_pass(table, cancel, statistics_target)?;
                }
                Ok(result)
            }
            Statement::Analyze { table } => {
                self.run_analyze_pass(table.clone(), cancel, statistics_target)?;
                Ok(ExecutionResult::Modified {
                    command: "ANALYZE".to_string(),
                    count: 0,
                })
            }
            Statement::Truncate { .. } => self.run_truncate(statement, cancel),
            Statement::AlterTableSetCompression { .. } => {
                self.run_alter_table_compression(statement, cancel)
            }
            Statement::AlterTableSetOptions { .. } => {
                self.run_alter_table_toast_options(statement, cancel)
            }
            Statement::AlterTableAddPrimaryKey { .. } => {
                self.run_alter_table_add_primary_key(statement, cancel)
            }
            Statement::AlterTableAddForeignKey { .. } => {
                self.run_alter_table_add_foreign_key(statement, cancel, prepared_versions)
            }
            Statement::AlterTableDropPrimaryKey { .. } => {
                self.run_alter_table_drop_primary_key(statement, cancel)
            }
            Statement::AlterTableDropConstraint { .. } => {
                self.run_alter_table_drop_constraint(statement, cancel, prepared_versions)
            }
            _ => Err(DbError::internal(
                "run_maintenance called with a non-maintenance statement",
            )),
        }
    }

    /// Run `VACUUM` (Milestone F4a, `docs/specs/mvcc.md` §9/§10 F): reclaim dead MVCC
    /// versions from one table or every user table, under xid-owned `Share` locks.
    /// Returns a `CommandComplete`-style result tagged `VACUUM`.
    ///
    /// **Concurrency + safety (no data loss — the horizon-under-the-guard argument).**
    /// VACUUM takes `Share` on every target, which drains target writers while
    /// permitting readers. The GC horizon is captured **once, after the locks are
    /// held**, as the minimum `xmin` advertised by
    /// any live snapshot — INCLUDING active readers and autocommit reads,
    /// which advertise their `xmin` ([`ServerComponents::gc_horizon`]). Each phase only
    /// reclaims versions with `xmax < horizon` ([`common::is_dead_to_all`]), i.e.
    /// deletes that committed before every live snapshot's `xmin`; no current snapshot
    /// can see such a version live, and any reader that starts mid-pass freezes
    /// `xmin >= horizon` (the deleter is in its settled past). Capturing the horizon
    /// AFTER acquiring the locks is load-bearing: a target writer cannot then create
    /// a newly reclaimable version, and the horizon already accounts for every reader
    /// advertised at that instant. VACUUM therefore never reclaims a version any
    /// snapshot needs.
    pub(super) fn run_vacuum(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        let Statement::Vacuum { table, .. } = statement else {
            return Err(DbError::internal(
                "run_vacuum called with a non-VACUUM statement",
            ));
        };

        let components = &self.components;
        let mut discovered = {
            let _catalog_read = components
                .catalog_publication_gate
                .read()
                .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
            resolve_vacuum_tables(components, table.as_ref())?
        };
        let txn_id = components
            .active_txns
            .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
        let writer_guard = match components.concurrency.begin_writer_cancelable(cancel) {
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
        let RevalidatedVacuum {
            catalog_read,
            tables,
            horizon,
            full_boundary,
        } = loop {
            let requests = discovered
                .iter()
                .map(|schema| ObjectLockRequest::table(schema.id, RelationLockMode::Share))
                .collect::<Vec<_>>();
            if let Err(err) = object_guard.acquire_many(&requests, cancel) {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            let revalidated = match revalidate_vacuum_targets(components, table.as_ref()) {
                Ok(state) => state,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            if revalidated
                .tables
                .iter()
                .map(|schema| schema.id)
                .eq(discovered.iter().map(|schema| schema.id))
            {
                break revalidated;
            }
            let RevalidatedVacuum {
                catalog_read,
                tables,
                ..
            } = revalidated;
            drop(catalog_read);
            if let Err(err) = object_guard.restore(&baseline) {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            discovered = tables;
        };
        drop(catalog_read);
        let ctx = StatementContext::new(txn_id)
            .with_tuple_lock_manager(components.lock_manager.clone())
            .with_conflict_waiter(components.lock_manager.clone(), cancel.clone());
        let mut cleaned_toast = Vec::with_capacity(tables.len());
        for schema in &tables {
            match delete_toast_values_for_vacuum_txn(components, &ctx, schema, horizon) {
                Ok(deleted) => cleaned_toast.push(deleted),
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            }
        }
        if let Err(err) = append_and_flush_maintenance_commit(components, txn_id) {
            self.rollback_pre_durable_or_die(txn_id, None);
            return Err(err);
        }
        if let Err(err) = cleanup_after_durable_maintenance_commit(components, txn_id) {
            fatal_after_durable_maintenance_commit(components, err);
        }
        components.active_txns.deregister(txn_id);
        components.lock_manager.on_txn_finished();

        prune_vacuum_tables(components, &tables, &cleaned_toast, horizon, txn_id)?;
        if let Some(boundary) = full_boundary {
            components.wal.set_vacuum_floor(boundary)?;
        }
        drop(object_guard);
        drop(writer_guard);

        Ok(ExecutionResult::Modified {
            command: "VACUUM".to_string(),
            count: 0,
        })
    }
}

fn maintenance_target_names(statement: &Statement) -> Vec<&QualifiedName> {
    match statement {
        Statement::Vacuum {
            table: Some(table), ..
        }
        | Statement::Analyze { table: Some(table) }
        | Statement::AlterTableSetCompression { table, .. }
        | Statement::AlterTableSetOptions { table, .. }
        | Statement::AlterTableAddPrimaryKey { table, .. }
        | Statement::AlterTableDropPrimaryKey { table, .. } => vec![table],
        Statement::AlterTableDropConstraint { table, .. } => vec![table],
        Statement::AlterTableAddForeignKey { table, foreign_key } => {
            vec![table, &foreign_key.referenced_table]
        }
        Statement::Truncate { tables } => tables.iter().collect(),
        _ => Vec::new(),
    }
}

struct RevalidatedVacuum<'a> {
    catalog_read: RwLockReadGuard<'a, ()>,
    tables: Vec<TableSchema>,
    horizon: u64,
    full_boundary: Option<u64>,
}

fn revalidate_vacuum_targets<'a>(
    components: &'a ServerComponents,
    table: Option<&QualifiedName>,
) -> Result<RevalidatedVacuum<'a>> {
    let catalog_read = components
        .catalog_publication_gate
        .read()
        .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
    let current = resolve_vacuum_tables(components, table)?;
    let horizon = components.gc_horizon();
    let full_boundary = table.is_none().then(|| {
        components
            .active_txns
            .oldest()
            .unwrap_or_else(|| components.next_txn_id.load(Ordering::Acquire))
    });
    Ok(RevalidatedVacuum {
        catalog_read,
        tables: current,
        horizon,
        full_boundary,
    })
}

fn resolve_vacuum_tables(
    components: &ServerComponents,
    table: Option<&QualifiedName>,
) -> Result<Vec<TableSchema>> {
    match table {
        Some(name) => {
            let schema = match &name.schema {
                Some(schema) => components
                    .catalog
                    .get_schema_by_name(schema)?
                    .map(|schema| schema.id)
                    .ok_or_else(|| {
                        DbError::plan(
                            SqlState::InvalidSchemaName,
                            format!("schema {schema} does not exist"),
                        )
                    })?,
                None => common::PUBLIC_SCHEMA_ID,
            };
            match components.catalog.get_table_in_schema(schema, &name.name)? {
                Some(table) => Ok(vec![table]),
                None if components
                    .catalog
                    .get_view_in_schema(schema, &name.name)?
                    .is_some()
                    || components
                        .catalog
                        .get_index_in_schema(schema, &name.name)?
                        .is_some()
                    || components
                        .catalog
                        .get_sequence_in_schema(schema, &name.name)?
                        .is_some() =>
                {
                    Err(DbError::plan(
                        SqlState::WrongObjectType,
                        format!("relation {name} is not a table"),
                    ))
                }
                None => Err(DbError::plan(
                    SqlState::UndefinedTable,
                    format!("table {name} does not exist"),
                )),
            }
        }
        None => {
            let mut tables = components
                .catalog
                .list_tables()?
                .into_iter()
                .filter(|schema| schema.relation_kind == RelationKind::User)
                .collect::<Vec<_>>();
            tables.sort_unstable_by_key(|schema| schema.id);
            Ok(tables)
        }
    }
}

fn delete_toast_values_for_vacuum_txn(
    components: &ServerComponents,
    ctx: &StatementContext,
    schema: &TableSchema,
    horizon: u64,
) -> Result<bool> {
    let value_ids = components
        .storage
        .toast_value_ids_pending_vacuum(schema, horizon)?;
    if value_ids.is_empty() {
        return Ok(false);
    }
    Ok(components
        .storage
        .delete_toast_values(ctx, schema, &value_ids)?
        != 0)
}

fn prune_vacuum_tables(
    components: &ServerComponents,
    tables: &[TableSchema],
    cleaned_toast: &[bool],
    horizon: u64,
    cleanup_txn: u64,
) -> Result<()> {
    for (schema, cleaned_toast) in tables.iter().zip(cleaned_toast) {
        if schema.toast_table_id.is_some() {
            components
                .storage
                .vacuum_after_toast_cleanup(schema, horizon)?;
        } else {
            components.storage.vacuum(schema, horizon)?;
        }
        let toast_horizon = if *cleaned_toast {
            horizon.max(cleanup_txn.saturating_add(1))
        } else {
            horizon
        };
        components
            .storage
            .vacuum_hidden_toast_relation(schema, toast_horizon)?;
    }
    Ok(())
}

pub(super) fn append_and_flush_maintenance_commit(
    components: &ServerComponents,
    txn_id: u64,
) -> Result<()> {
    components.wal.append(WalRecord {
        lsn: 0,
        txn_id,
        kind: WalRecordKind::Commit,
    })?;
    if let Err(err) = components.wal.flush() {
        if err.kind == common::ErrorKind::DurabilityOutcomeUnknown {
            fatal_ambiguous_maintenance_commit(err);
        }
        return Err(err);
    }
    Ok(())
}

pub(super) fn append_and_flush_maintenance_catalog_change(
    components: &ServerComponents,
    txn_id: u64,
    change_set: common::CatalogChangeSet,
) -> Result<()> {
    let checkpoint_publication = components.buffer_pool.checkpoint_fence().shared();
    let position = components.wal.append_positioned(WalRecord {
        lsn: 0,
        txn_id,
        kind: WalRecordKind::CatalogChange { change_set },
    })?;
    components
        .storage
        .catalog_redo_tracker()
        .register(txn_id, position.replay_from)?;
    drop(checkpoint_publication);
    if let Err(err) = components.wal.flush() {
        if err.kind == common::ErrorKind::DurabilityOutcomeUnknown {
            fatal_ambiguous_maintenance_commit(err);
        }
        return Err(err);
    }
    Ok(())
}

pub(super) fn cleanup_after_durable_maintenance_commit(
    components: &ServerComponents,
    txn_id: u64,
) -> Result<()> {
    components.storage.commit_txn(txn_id)?;
    components.buffer_pool.commit(txn_id)?;
    Ok(())
}

pub(super) fn fatal_after_durable_maintenance_commit(
    components: &ServerComponents,
    err: DbError,
) -> ! {
    eprintln!("fatal TOAST cleanup failure after durable commit: {err}");
    let _ = components.wal.flush();
    std::process::exit(1);
}

fn fatal_ambiguous_maintenance_commit(err: DbError) -> ! {
    eprintln!("fatal maintenance commit durability outcome unknown: {err}");
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use executor::ExecutionResult;

    use super::{RevalidatedVacuum, revalidate_vacuum_targets};
    use crate::app::AppState;
    use crate::checkpoint::run_checkpoint;

    #[test]
    fn full_vacuum_boundary_excludes_tables_published_after_target_capture() {
        let dir = tempfile::tempdir().unwrap();
        let app = AppState::open_for_test(dir.path()).unwrap();
        app.query_service
            .execute_sql("create table vacuum_anchor (id integer primary key)")
            .unwrap();
        let RevalidatedVacuum {
            catalog_read,
            full_boundary,
            ..
        } = revalidate_vacuum_targets(&app.components, None).unwrap();
        let boundary = full_boundary.expect("full VACUUM captures a floor boundary");

        let creator = app.query_service.clone();
        let create_task = std::thread::spawn(move || {
            creator.execute_sql("create table vacuum_late (id integer primary key)")
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !create_task.is_finished(),
            "catalog publication must wait until the all-table boundary is captured"
        );
        drop(catalog_read);
        create_task.join().unwrap().unwrap();

        let err = app
            .query_service
            .execute_sql("insert into vacuum_late (id) values (1), (1)")
            .unwrap_err();
        assert_eq!(err.code, common::SqlState::UniqueViolation);

        app.components.wal.set_vacuum_floor(boundary).unwrap();
        run_checkpoint(&app.components).unwrap();
        drop(app);

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let result = reopened
            .query_service
            .execute_sql("select count(*) from vacuum_late")
            .unwrap();
        match result {
            ExecutionResult::Query { rows, .. } => {
                assert_eq!(rows[0].values, vec![common::Value::Integer(0)]);
            }
            other => panic!("expected query result, got {other:?}"),
        }
    }
}
