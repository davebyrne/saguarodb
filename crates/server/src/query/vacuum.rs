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

use super::{PreparedStatement, QueryService};
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
            Statement::Vacuum { table: Some(table) }
            | Statement::AlterTableSetCompression { table, .. }
            | Statement::AlterTableSetOptions { table, .. }
            | Statement::AlterTableAddPrimaryKey { table, .. }
            | Statement::AlterTableDropPrimaryKey { table, .. } => qualify(table)?,
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
        let mut identity_guard = self.components.lock_manager.statement_owner();
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
        self.run_maintenance(statement.clone(), session.cancel())
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
            Statement::AlterTableDropPrimaryKey { .. } => {
                self.run_alter_table_drop_primary_key(statement, cancel)
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
        Statement::Vacuum { table: Some(table) }
        | Statement::AlterTableSetCompression { table, .. }
        | Statement::AlterTableSetOptions { table, .. }
        | Statement::AlterTableAddPrimaryKey { table, .. }
        | Statement::AlterTableDropPrimaryKey { table, .. } => vec![table],
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

/// Vacuum each `table` with F4a's three-phase orchestration
/// ([`PageBackedStorageEngine::vacuum`]: heap-prune → index-vacuum →
/// line-pointer-reclaim), reclaiming versions dead to `horizon`. Used by checkpoint
/// auto-prune; on-demand VACUUM uses xid-owned target locks and its one maintenance
/// transaction (`docs/specs/mvcc.md` §9/§10 F4a/F4b).
///
/// **Caller contract (the no-data-loss safety):** the caller MUST already hold the
/// EXCLUSIVE checkpoint guard ([`ConcurrencyController::begin_checkpoint`]) and MUST
/// have captured `horizon` from [`ServerComponents::gc_horizon`] *after* acquiring
/// that guard. Under the guard no writer runs, and the horizon is the minimum `xmin`
/// advertised by any live snapshot (including concurrent readers), so every reclaimed
/// version (`xmax < horizon`) is one no live snapshot can see — identical safety to
/// the on-demand `VACUUM` (F4a). This helper does not take the guard or capture the
/// horizon itself, precisely so it cannot be misused to vacuum with an
/// outside-the-guard horizon.
fn vacuum_tables(
    components: &ServerComponents,
    tables: &[TableSchema],
    horizon: u64,
) -> Result<()> {
    for schema in tables {
        let cleanup_txn = delete_toast_values_pending_parent_vacuum(components, schema, horizon)?;
        if schema.toast_table_id.is_some() {
            components
                .storage
                .vacuum_after_toast_cleanup(schema, horizon)?;
        } else {
            components.storage.vacuum(schema, horizon)?;
        }
        let toast_horizon = cleanup_txn
            .map(|txn_id| horizon.max(txn_id.saturating_add(1)))
            .unwrap_or(horizon);
        components
            .storage
            .vacuum_hidden_toast_relation(schema, toast_horizon)?;
    }
    Ok(())
}

fn delete_toast_values_pending_parent_vacuum(
    components: &ServerComponents,
    schema: &TableSchema,
    horizon: u64,
) -> Result<Option<u64>> {
    let value_ids = components
        .storage
        .toast_value_ids_pending_vacuum(schema, horizon)?;
    if value_ids.is_empty() {
        return Ok(None);
    }

    let txn_id = components
        .active_txns
        .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
    let ctx = StatementContext::new(txn_id).with_conflict_waiter(
        components.lock_manager.clone(),
        Arc::new(QueryCancel::new()),
    );

    let deleted = match components
        .storage
        .delete_toast_values(&ctx, schema, &value_ids)
    {
        Ok(deleted) => deleted,
        Err(err) => {
            rollback_maintenance_txn_or_die(components, txn_id);
            return Err(err);
        }
    };
    if deleted == 0 {
        components.active_txns.deregister(txn_id);
        components.lock_manager.on_txn_finished();
        return Ok(None);
    }

    if let Err(err) = append_and_flush_maintenance_commit(components, txn_id) {
        rollback_maintenance_txn_or_die(components, txn_id);
        return Err(err);
    }
    if let Err(err) = cleanup_after_durable_maintenance_commit(components, txn_id) {
        fatal_after_durable_maintenance_commit(components, err);
    }
    components.active_txns.deregister(txn_id);
    components.lock_manager.on_txn_finished();

    Ok(Some(txn_id))
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
    components.wal.flush()?;
    Ok(())
}

pub(super) fn rollback_maintenance_txn_or_die(components: &ServerComponents, txn_id: u64) {
    if let Err(err) = components.wal.append(WalRecord {
        lsn: 0,
        txn_id,
        kind: WalRecordKind::Abort,
    }) {
        eprintln!("failed to append Abort record for maintenance txn {txn_id}: {err}");
    }
    components.active_txns.deregister(txn_id);
    components.lock_manager.on_txn_finished();
    if let Err(err) = components.storage.rollback_txn(txn_id) {
        fatal_pre_durable_maintenance_rollback(err);
    }
    if let Err(err) = components.buffer_pool.rollback(txn_id) {
        fatal_pre_durable_maintenance_rollback(DbError::internal(format!(
            "buffer rollback failed for maintenance txn {txn_id}: {err}",
        )));
    }
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

fn fatal_pre_durable_maintenance_rollback(err: DbError) -> ! {
    eprintln!("fatal TOAST cleanup rollback failure before durable commit: {err}");
    std::process::exit(1);
}

/// Run a FULL VACUUM pass over every user table AND advance the WAL **vacuum floor**
/// (`docs/specs/mvcc.md` §5.4, §9, Milestone F4c). Used by the on-demand `VACUUM`
/// (no table) and the checkpoint auto-prune (F4b) — the two full-pass callers.
///
/// The boundary `B = next_txn_id` is captured BEFORE the pass and the floor is
/// advanced to `B` AFTER it. The capture happens under the exclusive guard the caller
/// holds (same contract as [`vacuum_tables`]: `horizon` was captured under it), so no
/// user writer can allocate an id below or during the pass. TOAST cleanup may allocate
/// committed maintenance xids during the pass; those ids are `>= B` and are not covered
/// by this floor advance. A full pass leaves EVERY aborted transaction with id `< B`
/// with NO surviving on-disk reference, as creator OR deleter: `vacuum_heap` RECLAIMS
/// every aborted-creator tuple (heap + index; aborted-creator reclaim has NO age
/// requirement) and ABORT-CLEANS every aborted-deleter stamp in place (resetting `xmax →
/// INVALID`, `t_ctid → INVALID`, and un-HOTing an aborted root — the surviving
/// predecessor of an aborted UPDATE/DELETE, which stays live and is NOT reclaimed).
/// Advancing the floor to `B` is therefore safe: the next checkpoint's `persist_clog`
/// may drop those aborted txns' explicit `Aborted` entries from `clog.dat` and let the
/// implicit-committed floor cover them (the catalog is NOT MVCC-versioned, so user-table
/// tuples are the only place an aborted txn's on-disk reference lives). Without the
/// abort-cleanup, an aborted UPDATE/DELETE's surviving predecessor would keep an `xmax =
/// T` that reads as an implicit-committed delete once `T`'s entry is dropped from the
/// snapshot, wrongly removing the row after a crash — the hazard for ALL aborted
/// UPDATE/DELETE, HOT or non-HOT.
///
/// **Durability ordering.** The floor is only ever CONSULTED by `persist_clog`,
/// which a checkpoint runs AFTER `flush_dirty_pages` + `store.sync_all` — so by the
/// time the floor is used, every dirty page this pass produced (auto-prune: this same
/// checkpoint; on-demand: a later checkpoint) is fsynced to the heap. No aborted entry
/// is dropped from the snapshot while its reclaimed tuples are still only in memory.
pub(crate) fn full_vacuum_pass(components: &ServerComponents, horizon: u64) -> Result<()> {
    // Capture B BEFORE the pass. An allocated transaction that has not yet become
    // a checkpoint participant still holds the floor back even though it cannot
    // have touched a relation.
    let boundary = components
        .active_txns
        .oldest()
        .unwrap_or_else(|| components.next_txn_id.load(Ordering::Acquire));
    vacuum_all_user_tables(components, horizon)?;
    // Advance the floor only AFTER the pass has reclaimed every aborted-creator tuple
    // below B. Monotonic; persisted in `clog.dat` and reloaded at open (falls back to the
    // conservative value when no snapshot is present) — see `WalManager::set_vacuum_floor`.
    components.wal.set_vacuum_floor(boundary)
}

/// Vacuum every user table in the catalog, for the checkpoint auto-prune path (F4b).
/// Same caller contract as [`vacuum_tables`]: the exclusive guard is held and
/// `horizon` was captured under it. This does NOT advance the vacuum floor; callers
/// that perform a *full* pass and want the floor advanced use [`full_vacuum_pass`].
fn vacuum_all_user_tables(components: &ServerComponents, horizon: u64) -> Result<()> {
    let tables: Vec<_> = components
        .catalog
        .list_tables()?
        .into_iter()
        .filter(|schema| matches!(&schema.relation_kind, RelationKind::User))
        .collect();
    vacuum_tables(components, &tables, horizon)
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
