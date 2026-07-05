use std::sync::atomic::Ordering;

use common::{DbError, Result, SqlState};
use executor::ExecutionResult;
use parser::Statement;

use super::{PreparedStatement, QueryService};
use crate::app::ServerComponents;

impl QueryService {
    /// Run a prepared (extended-protocol) maintenance command (`VACUUM` or
    /// `ALTER TABLE ... SET (compression = ...)`). The statement carries no bound
    /// payload — it is the raw maintenance `Statement` parsed at `prepare_sql` time.
    pub(super) fn run_prepared_maintenance(
        &self,
        prepared: &PreparedStatement,
    ) -> Result<ExecutionResult> {
        let statement = prepared.maintenance.as_ref().ok_or_else(|| {
            DbError::internal("maintenance prepared statement has no carried payload")
        })?;
        self.run_maintenance(statement.clone())
    }

    /// Shared entry point for every maintenance command: dispatches to the
    /// statement-specific implementation. Both the simple-query and
    /// extended-protocol paths route maintenance through this one router.
    pub(super) fn run_maintenance(&self, statement: Statement) -> Result<ExecutionResult> {
        match &statement {
            Statement::Vacuum { .. } => self.run_vacuum(statement),
            Statement::AlterTableSetCompression { .. } => {
                self.run_alter_table_compression(statement)
            }
            Statement::AlterTableSetOptions { .. } => Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "ALTER TABLE SET TOAST options is not implemented yet",
            )),
            _ => Err(DbError::internal(
                "run_maintenance called with a non-maintenance statement",
            )),
        }
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
    pub(super) fn run_vacuum(&self, statement: Statement) -> Result<ExecutionResult> {
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
                // dirty pages before its `persist_clog` consults the floor, so no
                // aborted entry is dropped from the snapshot while its tuples remain on disk.
                full_vacuum_pass(&self.components, horizon)?;
            }
        }

        Ok(ExecutionResult::Modified {
            command: "VACUUM".to_string(),
            count: 0,
        })
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
/// id is allocated mid-pass and `B` is the exact id high-water at scan time. A full
/// pass leaves EVERY aborted transaction with id `< B` with NO surviving on-disk
/// reference, as creator OR deleter: `vacuum_heap` RECLAIMS every aborted-creator tuple
/// (heap + index; aborted-creator reclaim has NO age requirement) and ABORT-CLEANS every
/// aborted-deleter stamp in place (resetting `xmax → INVALID`, `t_ctid → INVALID`, and
/// un-HOTing an aborted root — the surviving predecessor of an aborted UPDATE/DELETE,
/// which stays live and is NOT reclaimed). Advancing the floor to `B` is therefore safe:
/// the next checkpoint's `persist_clog` may drop those aborted txns' explicit `Aborted`
/// entries from `clog.dat` and let the implicit-committed floor cover them (the catalog
/// is NOT MVCC-versioned, so user-table tuples are the only place an aborted txn's
/// on-disk reference lives). Without the abort-cleanup, an aborted UPDATE/DELETE's
/// surviving predecessor would keep an `xmax = T` that reads as an implicit-committed
/// delete once `T`'s entry is dropped from the snapshot, wrongly removing the row after a
/// crash — the hazard for ALL aborted UPDATE/DELETE, HOT or non-HOT.
///
/// **Durability ordering.** The floor is only ever CONSULTED by `persist_clog`,
/// which a checkpoint runs AFTER `flush_dirty_pages` + `store.sync_all` — so by the
/// time the floor is used, every dirty page this pass produced (auto-prune: this same
/// checkpoint; on-demand: a later checkpoint) is fsynced to the heap. No aborted entry
/// is dropped from the snapshot while its reclaimed tuples are still only in memory.
pub(crate) fn full_vacuum_pass(components: &ServerComponents, horizon: u64) -> Result<()> {
    // Capture B BEFORE the pass, under the guard (no concurrent allocation).
    let boundary = components.next_txn_id.load(Ordering::Acquire);
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
    let tables = components.catalog.list_tables()?;
    vacuum_tables(components, &tables, horizon)
}
