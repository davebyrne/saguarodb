use std::sync::{Arc, RwLockWriteGuard, atomic::Ordering};

use catalog::{CatalogManager, MemoryCatalog, ResolvedForeignKey};
use common::{
    ColumnId, CompressionSetting, DbError, IndexConstraintKind, IndexId, IndexSchema,
    IsolationLevel, QualifiedName, QueryCancel, RelationKind, Result, SqlState, StatementContext,
    TableId, TableOptionPatch, TableSchema, ToastCompression, ToastOptions, WriteGuard,
    needs_toast_relation, toast_schema,
};
use executor::{ExecutionContext, ExecutionResult, validate_existing_foreign_keys};
use parser::{ParsedForeignKey, Statement};
use storage::{RecoveryOperations, SchemaOperations};
use wal::{WalRecord, WalRecordKind};

use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;
use crate::lock_manager::{ObjectLockGuard, ObjectLockRequest, RelationLockMode};

use super::{PreparedRelationVersion, QueryService};

/// How many heap pages to sample for dictionary training (`compression.md`
/// §7: evenly sampled, capped — a 32 MiB corpus at 8 KiB pages).
const DICT_TRAINING_PAGE_CAP: usize = 4096;
/// Logical TOAST value samples used when `ALTER TABLE ... SET
/// (toast_compression = zstd_dict)` trains a value dictionary. The byte cap keeps
/// memory bounded independently of table size.
const TOAST_DICT_MAX_SAMPLES: usize = 4096;
const TOAST_DICT_MAX_BYTES: usize = 32 * 1024 * 1024;

struct ToastAlterPostCommit {
    table_id: TableId,
    toast: ToastOptions,
    toast_table_id: Option<TableId>,
    hidden_schema: Option<TableSchema>,
}

enum PrimaryKeyAlterPostCommit {
    Add {
        txn_id: u64,
        schema: TableSchema,
        index: IndexSchema,
        gc_horizon: u64,
    },
    Drop {
        txn_id: u64,
        schema: TableSchema,
        index_id: IndexId,
        gc_horizon: u64,
    },
}

struct LockedMaintenanceTable<'a> {
    txn_id: u64,
    schema: TableSchema,
    _catalog_publication: RwLockWriteGuard<'a, ()>,
    _objects: ObjectLockGuard,
    _writer: WriteGuard,
}

struct LockedForeignKeyTables<'a> {
    txn_id: u64,
    child: TableSchema,
    parent: TableSchema,
    _catalog_publication: RwLockWriteGuard<'a, ()>,
    _objects: ObjectLockGuard,
    _writer: WriteGuard,
}

#[derive(Clone)]
enum DropConstraintKind {
    PrimaryKey,
    ForeignKey(common::ForeignKeyConstraint),
    UnsupportedUnique,
    Missing,
}

struct LockedDropConstraint<'a> {
    txn_id: u64,
    child: TableSchema,
    parent: Option<TableSchema>,
    kind: DropConstraintKind,
    _catalog_publication: RwLockWriteGuard<'a, ()>,
    _objects: ObjectLockGuard,
    _writer: WriteGuard,
}

impl QueryService {
    fn lock_maintenance_table<'a>(
        &'a self,
        table: &QualifiedName,
        cancel: &QueryCancel,
    ) -> Result<LockedMaintenanceTable<'a>> {
        let components = &self.components;
        let mut discovered = {
            let _catalog_read = components
                .catalog_publication_gate
                .read()
                .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
            self.require_user_table(table)?
        };
        let txn_id = components
            .active_txns
            .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
        let writer = match components.concurrency.begin_writer_cancelable(cancel) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let mut objects = match components.lock_manager.transaction_owner(txn_id) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let baseline = objects.snapshot();
        let (catalog_publication, schema) = loop {
            let requests = [
                ObjectLockRequest::schema(
                    discovered.schema_id,
                    crate::lock_manager::CatalogLockMode::Access,
                ),
                ObjectLockRequest::table(discovered.id, RelationLockMode::AccessExclusive),
            ];
            if let Err(err) = objects.acquire_many(&requests, cancel) {
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
            let current = match self.require_user_table(table) {
                Ok(schema) => schema,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            if current.id == discovered.id {
                break (catalog_publication, current);
            }
            drop(catalog_publication);
            if let Err(err) = objects.restore(&baseline) {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            discovered = current;
        };
        Ok(LockedMaintenanceTable {
            txn_id,
            schema,
            _catalog_publication: catalog_publication,
            _objects: objects,
            _writer: writer,
        })
    }

    fn lock_foreign_key_tables<'a>(
        &'a self,
        child_name: &QualifiedName,
        parent_name: &QualifiedName,
        parent_mode: RelationLockMode,
        cancel: &QueryCancel,
        prepared_versions: Option<&[PreparedRelationVersion]>,
    ) -> Result<LockedForeignKeyTables<'a>> {
        let components = &self.components;
        let (mut child, mut parent) = {
            let _catalog_read = components
                .catalog_publication_gate
                .read()
                .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
            (
                self.require_user_table(child_name)?,
                self.require_user_table(parent_name)?,
            )
        };
        let txn_id = components
            .active_txns
            .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
        let writer = match components.concurrency.begin_writer_cancelable(cancel) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let mut objects = match components.lock_manager.transaction_owner(txn_id) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let baseline = objects.snapshot();
        let (catalog_publication, child, parent) = loop {
            let requests = [
                ObjectLockRequest::schema(
                    child.schema_id,
                    crate::lock_manager::CatalogLockMode::Access,
                ),
                ObjectLockRequest::catalog_name(child.schema_id, &child.name),
                ObjectLockRequest::table(child.id, RelationLockMode::AccessExclusive),
                ObjectLockRequest::schema(
                    parent.schema_id,
                    crate::lock_manager::CatalogLockMode::Access,
                ),
                ObjectLockRequest::catalog_name(parent.schema_id, &parent.name),
                ObjectLockRequest::table(parent.id, parent_mode),
            ];
            if let Err(err) = objects.acquire_many(&requests, cancel) {
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
            let current_child = match self.require_user_table(child_name) {
                Ok(schema) => schema,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            let current_parent = match self.require_user_table(parent_name) {
                Ok(schema) => schema,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            if current_child.id == child.id && current_parent.id == parent.id {
                if let Some(prepared_versions) = prepared_versions
                    && let Err(err) =
                        self.validate_prepared_schema_versions_under_gate(prepared_versions)
                {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
                break (catalog_publication, current_child, current_parent);
            }
            drop(catalog_publication);
            if let Err(err) = objects.restore(&baseline) {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            child = current_child;
            parent = current_parent;
        };
        Ok(LockedForeignKeyTables {
            txn_id,
            child,
            parent,
            _catalog_publication: catalog_publication,
            _objects: objects,
            _writer: writer,
        })
    }

    fn lock_drop_constraint<'a>(
        &'a self,
        child_name: &QualifiedName,
        constraint_name: &str,
        cancel: &QueryCancel,
        prepared_versions: Option<&[PreparedRelationVersion]>,
    ) -> Result<LockedDropConstraint<'a>> {
        let components = &self.components;
        let (mut child, mut kind) = {
            let _catalog_read = components
                .catalog_publication_gate
                .read()
                .map_err(|_| DbError::internal("catalog publication gate poisoned"))?;
            let child = self.require_user_table(child_name)?;
            let kind =
                classify_drop_constraint(components.catalog.as_ref(), &child, constraint_name)?;
            (child, kind)
        };
        let txn_id = components
            .active_txns
            .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
        let writer = match components.concurrency.begin_writer_cancelable(cancel) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let mut objects = match components.lock_manager.transaction_owner(txn_id) {
            Ok(guard) => guard,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let baseline = objects.snapshot();
        loop {
            let discovered_parent = match &kind {
                DropConstraintKind::ForeignKey(foreign_key) => {
                    let parent = match components.catalog.get_table(foreign_key.referenced_table) {
                        Ok(parent) => parent,
                        Err(err) => {
                            self.rollback_pre_durable_or_die(txn_id, None);
                            return Err(err);
                        }
                    };
                    match parent {
                        Some(parent) if parent.relation_kind == RelationKind::User => Some(parent),
                        Some(_) => {
                            self.rollback_pre_durable_or_die(txn_id, None);
                            return Err(DbError::internal(
                                "foreign key references a non-user parent table",
                            ));
                        }
                        None => {
                            self.rollback_pre_durable_or_die(txn_id, None);
                            return Err(DbError::internal(
                                "foreign key references a missing parent table",
                            ));
                        }
                    }
                }
                DropConstraintKind::PrimaryKey
                | DropConstraintKind::UnsupportedUnique
                | DropConstraintKind::Missing => None,
            };
            let mut requests = vec![
                ObjectLockRequest::schema(
                    child.schema_id,
                    crate::lock_manager::CatalogLockMode::Access,
                ),
                ObjectLockRequest::catalog_name(child.schema_id, &child.name),
                ObjectLockRequest::table(child.id, RelationLockMode::AccessExclusive),
            ];
            if let Some(parent) = &discovered_parent {
                requests.push(ObjectLockRequest::schema(
                    parent.schema_id,
                    crate::lock_manager::CatalogLockMode::Access,
                ));
                requests.push(ObjectLockRequest::catalog_name(
                    parent.schema_id,
                    &parent.name,
                ));
                requests.push(ObjectLockRequest::table(
                    parent.id,
                    RelationLockMode::AccessShare,
                ));
            }
            if let Err(err) = objects.acquire_many(&requests, cancel) {
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
            let current_child = match self.require_user_table(child_name) {
                Ok(schema) => schema,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            let current_kind = match classify_drop_constraint(
                components.catalog.as_ref(),
                &current_child,
                constraint_name,
            ) {
                Ok(kind) => kind,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            let current_parent_id = match &current_kind {
                DropConstraintKind::ForeignKey(foreign_key) => Some(foreign_key.referenced_table),
                DropConstraintKind::PrimaryKey
                | DropConstraintKind::UnsupportedUnique
                | DropConstraintKind::Missing => None,
            };
            let locked_parent_id = discovered_parent.as_ref().map(|parent| parent.id);
            if current_child.id == child.id
                && current_parent_id.is_none_or(|parent| Some(parent) == locked_parent_id)
            {
                if let Some(prepared_versions) = prepared_versions
                    && let Err(err) =
                        self.validate_prepared_schema_versions_under_gate(prepared_versions)
                {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
                let parent = match current_parent_id {
                    Some(parent_id) => match components.catalog.get_table(parent_id) {
                        Ok(Some(parent)) => Some(parent),
                        Ok(None) => {
                            self.rollback_pre_durable_or_die(txn_id, None);
                            return Err(DbError::internal(
                                "foreign-key parent disappeared under its lock",
                            ));
                        }
                        Err(err) => {
                            self.rollback_pre_durable_or_die(txn_id, None);
                            return Err(err);
                        }
                    },
                    None => None,
                };
                return Ok(LockedDropConstraint {
                    txn_id,
                    child: current_child,
                    parent,
                    kind: current_kind,
                    _catalog_publication: catalog_publication,
                    _objects: objects,
                    _writer: writer,
                });
            }
            drop(catalog_publication);
            if let Err(err) = objects.restore(&baseline) {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            child = current_child;
            kind = current_kind;
        }
    }

    /// `ALTER TABLE <t> SET (compression = ...)`: immediate-commit DDL under
    /// target `AccessExclusive`, then a full rewrite that logs a FullPageImage per
    /// page (torn-page repair, exactly like VACUUM) (`compression.md` §8).
    ///
    /// Commit boundary (mirrors `exec.rs`'s `autocommit_bound_write_with_guard`):
    /// everything up to and including the `Commit` record's `wal.flush()` below
    /// is pre-durable-commit and propagates `?` normally — a failure there means
    /// nothing committed, so it is a legitimate statement error (table
    /// resolution's `UndefinedTable` and dict training both land here).
    /// Everything AFTER that flush — catalog/registry install, the rewrite, and
    /// the rewrite's own durability — is POST-durable-commit cleanup: like every
    /// other autocommit path, a failure there is routed to
    /// `fatal_after_durable_commit` (process exit), never returned as a
    /// statement error, because the DDL already committed and misreporting it as
    /// failed would be worse than crashing.
    ///
    /// The shared writer, table lock, and catalog publication guards are scoped to
    /// a block covering pre-commit AND post-commit work, then dropped BEFORE
    /// [`record_commit_and_maybe_checkpoint_after_durable_commit`] runs — that
    /// call acquires its own exclusive guard, so calling it while this ALTER
    /// still held one would deadlock. Calling it at all is this fix: unlike the
    /// normal `autocommit_bound_write_with_guard` path, ALTER doesn't go through
    /// that helper, so without this explicit call the rewrite's (potentially
    /// large) FullPageImage bytes would never count toward the WAL-bytes
    /// checkpoint threshold until an unrelated later commit noticed them.
    ///
    /// Ordering is load-bearing: dict file durable → WAL records flushed
    /// (commit point) → catalog/registry updated → rewrite (FPI per page) →
    /// rewrite FPIs flushed (write-ahead) → page flush → fsync → mark clean.
    pub(super) fn run_alter_table_compression(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableSetCompression { table, compression } = statement else {
            return Err(DbError::internal("expected ALTER TABLE statement"));
        };
        let components = &self.components;

        {
            let locked = self.lock_maintenance_table(&table, cancel)?;
            let txn_id = locked.txn_id;
            let schema = locked.schema;

            // 3. Train a dictionary from current heap images (zstd only, and only
            // when the corpus suffices — a tiny/empty table proceeds dict-less).
            // Pre-commit: a failure here is a legitimate statement error, since
            // nothing has committed yet.
            let mut prepared_dict_id = None;
            let pre_commit = (|| {
                let mut active_dict_id = None;
                if compression == CompressionSetting::Zstd {
                    let samples = components.storage.sample_heap_pages_cancelable(
                        &schema,
                        DICT_TRAINING_PAGE_CAP,
                        cancel.as_ref(),
                    )?;
                    if let Some(bytes) =
                        compress::train_dictionary_cancelable(&samples, cancel.as_ref())?
                    {
                        let dict_id = components.catalog.allocate_dictionary_id()?;
                        // Track the id before the durable save so rollback also
                        // removes a temporary file left by a failed save.
                        prepared_dict_id = Some(dict_id);
                        // Durability order: dict file BEFORE any WAL reference (§7).
                        components.dict_store.save(dict_id, schema.id, &bytes)?;
                        components
                            .compression
                            .register_dictionary(dict_id, &bytes)?;
                        components.wal.append(WalRecord {
                            lsn: 0,
                            txn_id,
                            kind: WalRecordKind::CreateDictionary {
                                dict_id,
                                table_id: schema.id,
                                bytes,
                            },
                        })?;
                        active_dict_id = Some(dict_id);
                    }
                }

                // 4. DDL record + immediate commit, flushed durable before any page
                // image can reference the new state. THIS is the durable commit
                // point: everything above (and this block) is rolled back on error.
                components.wal.append(WalRecord {
                    lsn: 0,
                    txn_id,
                    kind: WalRecordKind::AlterTableCompression {
                        table_id: schema.id,
                        compression,
                        active_dict_id,
                    },
                })?;
                cancel.check()?;
                Ok(active_dict_id)
            })();
            let active_dict_id = match pre_commit {
                Ok(active_dict_id) => active_dict_id,
                Err(err) => {
                    self.rollback_prepared_dictionary_or_die(txn_id, prepared_dict_id);
                    return Err(err);
                }
            };
            if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
                self.rollback_prepared_dictionary_or_die(txn_id, prepared_dict_id);
                return Err(err);
            }

            // Post-durable-commit cleanup: install + rewrite + fsync. Any error
            // from here on is fatal (process exit) rather than a statement
            // error — see the doc comment above.
            if let Err(err) = self.finish_alter_table_compression_after_commit(
                schema.id,
                compression,
                active_dict_id,
            ) {
                self.fatal_after_durable_commit(err);
            }
            if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
                self.fatal_after_durable_commit(err);
            }
            components.active_txns.deregister(txn_id);
            components.lock_manager.on_txn_finished();
        }
        // The writer/object/catalog guards dropped when the block above ended.
        // `record_commit_and_maybe_checkpoint_after_durable_commit` acquires its
        // own exclusive guard internally, so it must run only now — calling it
        // while still holding this ALTER's guard would deadlock against itself.
        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);

        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
    }

    /// Post-durable-commit half of [`Self::run_alter_table_compression`]: install
    /// the new compression setting in the catalog/engine, rewrite every page,
    /// and make the rewrite durable. Called only after the DDL's `Commit`
    /// record is flushed durable; the caller routes any `Err` returned here to
    /// `fatal_after_durable_commit` rather than propagating it as a statement
    /// error.
    fn finish_alter_table_compression_after_commit(
        &self,
        table_id: TableId,
        compression: CompressionSetting,
        active_dict_id: Option<u32>,
    ) -> Result<()> {
        let components = &self.components;

        // 5. Install in catalog + engine/registry.
        let schema =
            components
                .catalog
                .set_table_compression(table_id, compression, active_dict_id)?;
        components.storage.set_table_compression(&schema)?;

        // 6-8. Rewrite: re-encode every page, logging a FullPageImage per page
        // and stamping the FPI's LSN as the page's new PageLSN (§8). This flush
        // is load-bearing. `flush_dirty_pages` does NOT gate on PageLSN — it
        // assumes the WAL is already durable — so the rewrite's FPIs must be
        // flushed here first. Removing this flush would let a torn page write
        // precede its FPI being durable (silent corruption on recovery), NOT
        // produce a loud error. A crash mid-rewrite leaves self-describing
        // mixed encodings, and a torn page write is repaired by redo replaying
        // its FPI (§8).
        let rewrite = components.storage.rewrite_table_pages(&schema)?;
        components.wal.flush()?;
        components
            .buffer_pool
            .flush_dirty_pages_for_files(&rewrite.file_ids)?;
        components.store.sync_files(&rewrite.file_ids)?;
        components.buffer_pool.mark_files_clean(&rewrite.file_ids)?;
        Ok(())
    }

    /// `ALTER TABLE <t> SET (toast...)`: future-write-only TOAST policy change
    /// under target `AccessExclusive`. Existing parent rows and existing
    /// TOAST chunks are left byte-for-byte as they are; normal reads keep using the
    /// per-value physical metadata to decode old rows.
    ///
    /// If the ALTER has to create a hidden TOAST relation for a legacy catalog
    /// table, the storage relation is created before the DDL commit so its empty
    /// primary-key B-tree pages are WAL-before-Commit and crash-recoverable. The
    /// catalog does not expose that hidden relation, nor does the base table point
    /// at it, until after the commit record is flushed.
    pub(super) fn run_alter_table_toast_options(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableSetOptions { table, options } = statement else {
            return Err(DbError::internal(
                "expected ALTER TABLE SET options statement",
            ));
        };
        if options.compression.is_some() {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "ALTER TABLE cannot combine page compression and TOAST options yet",
            ));
        }
        if options.toast.is_empty() {
            return Err(DbError::internal(
                "ALTER TABLE SET options carried no TOAST options",
            ));
        }

        let components = &self.components;
        {
            let locked = self.lock_maintenance_table(&table, cancel)?;
            let txn_id = locked.txn_id;
            let schema = locked.schema;
            let mut prepared_dict_id = None;
            let pre_commit = self.prepare_alter_table_toast_commit(
                txn_id,
                &schema,
                &options,
                cancel.clone(),
                &mut prepared_dict_id,
            );
            let post_commit = match pre_commit {
                Ok(post_commit) => post_commit,
                Err(err) => {
                    self.rollback_prepared_dictionary_or_die(txn_id, prepared_dict_id);
                    return Err(err);
                }
            };

            if let Err(err) = components.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::AlterTableToast {
                    table_id: post_commit.table_id,
                    toast: post_commit.toast.clone(),
                    toast_table_id: post_commit.toast_table_id,
                },
            }) {
                self.rollback_prepared_dictionary_or_die(txn_id, prepared_dict_id);
                return Err(err);
            }
            if let Err(err) = cancel.check() {
                self.rollback_prepared_dictionary_or_die(txn_id, prepared_dict_id);
                return Err(err);
            }
            if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
                self.rollback_prepared_dictionary_or_die(txn_id, prepared_dict_id);
                return Err(err);
            }

            if let Err(err) = self.finish_alter_table_toast_after_commit(post_commit) {
                self.fatal_after_durable_commit(err);
            }
            if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
                self.fatal_after_durable_commit(err);
            }
            components.active_txns.deregister(txn_id);
            components.lock_manager.on_txn_finished();
        }
        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);

        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
    }

    fn prepare_alter_table_toast_commit(
        &self,
        txn_id: u64,
        schema: &TableSchema,
        options: &TableOptionPatch,
        cancel: Arc<QueryCancel>,
        prepared_dict_id: &mut Option<u32>,
    ) -> Result<ToastAlterPostCommit> {
        let components = &self.components;
        let ctx = StatementContext::new(txn_id)
            .with_tuple_lock_manager(components.lock_manager.clone())
            .with_conflict_waiter(components.lock_manager.clone(), cancel.clone());

        let mut toast = schema.toast.apply_patch(&options.toast);
        if options.toast.compression == Some(ToastCompression::ZstdDict) {
            let samples = components.storage.sample_toast_values(
                &ctx,
                schema,
                TOAST_DICT_MAX_SAMPLES,
                TOAST_DICT_MAX_BYTES,
            )?;
            if let Some(bytes) = compress::train_dictionary_cancelable(&samples, cancel.as_ref())? {
                let dict_id = components.catalog.allocate_dictionary_id()?;
                *prepared_dict_id = Some(dict_id);
                components.dict_store.save(dict_id, schema.id, &bytes)?;
                components
                    .compression
                    .register_dictionary(dict_id, &bytes)?;
                components.wal.append(WalRecord {
                    lsn: 0,
                    txn_id,
                    kind: WalRecordKind::CreateDictionary {
                        dict_id,
                        table_id: schema.id,
                        bytes,
                    },
                })?;
                toast.active_dict_id = Some(dict_id);
            }
        }

        let hidden_schema = if schema.toast_table_id.is_none() && needs_toast_relation(schema) {
            let toast_table_id = components.catalog.snapshot()?.next_table_id;
            components.catalog.reserve_table_id(toast_table_id)?;
            let toast_storage_id = components.catalog.allocate_storage_id()?;
            let mut base = schema.clone();
            base.toast_table_id = Some(toast_table_id);
            let mut hidden = toast_schema(&base, toast_table_id);
            hidden.storage_id = toast_storage_id;
            components.storage.create_table(&ctx, &hidden)?;
            Some(hidden)
        } else {
            None
        };
        let toast_table_id = hidden_schema
            .as_ref()
            .map(|schema| schema.id)
            .or(schema.toast_table_id);

        Ok(ToastAlterPostCommit {
            table_id: schema.id,
            toast,
            toast_table_id,
            hidden_schema,
        })
    }

    /// Abort an immediate-commit ALTER and remove any dictionary it prepared
    /// before the commit record became durable. Dictionary ids remain burned.
    fn rollback_prepared_dictionary_or_die(&self, txn_id: u64, dict_id: Option<u32>) {
        self.rollback_pre_durable_or_die(txn_id, None);
        if let Some(dict_id) = dict_id {
            self.components.compression.remove_dictionary(dict_id);
            if let Err(err) = self.components.dict_store.remove(dict_id) {
                self.fatal_pre_durable_rollback_failure(err);
            }
        }
    }

    fn finish_alter_table_toast_after_commit(&self, post: ToastAlterPostCommit) -> Result<()> {
        let components = &self.components;
        if let Some(hidden_schema) = post.hidden_schema {
            components.catalog.apply_create_table(hidden_schema)?;
        }
        let schema = components.catalog.set_table_toast_metadata(
            post.table_id,
            post.toast,
            post.toast_table_id,
        )?;
        components.storage.set_table_toast_metadata(&schema)
    }

    /// `ALTER TABLE <t> ADD [CONSTRAINT name] PRIMARY KEY (cols...)`: immediate
    /// commit DDL under target `AccessExclusive`. The normal secondary constraint index
    /// is created before commit (new index files are safe to orphan on abort);
    /// the existing table identity tree is rebuilt only after the DDL commit is
    /// durable, and recovery derives that rebuild from the logical WAL record.
    pub(super) fn run_alter_table_add_primary_key(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableAddPrimaryKey {
            table,
            columns,
            constraint_name,
        } = statement
        else {
            return Err(DbError::internal(
                "expected ALTER TABLE ADD PRIMARY KEY statement",
            ));
        };

        let components = &self.components;
        {
            let locked = self.lock_maintenance_table(&table, cancel)?;
            let txn_id = locked.txn_id;
            let schema = locked.schema;
            let prepared = (|| {
                if !schema.primary_key.is_empty() {
                    return Err(DbError::plan(
                        SqlState::ObjectNotInPrerequisiteState,
                        format!("table {table} already has a primary key"),
                    ));
                }
                let primary_key = primary_key_column_ids(&schema, &columns)?;
                let new_schema = schema_with_primary_key(schema.clone(), primary_key.clone())?;
                let gc_horizon = components.gc_horizon();
                let catalog_snapshot = components.catalog.snapshot()?;
                let index_name = constraint_name
                    .clone()
                    .unwrap_or_else(|| format!("{}_pkey", schema.name));
                if components
                    .catalog
                    .get_index_in_schema(schema.schema_id, &index_name)?
                    .is_some()
                {
                    return Err(DbError::plan(
                        SqlState::DuplicateTable,
                        format!("index {index_name} already exists"),
                    ));
                }
                let index = IndexSchema {
                    id: catalog_snapshot.next_index_id,
                    schema_id: schema.schema_id,
                    storage_id: catalog_snapshot.next_storage_id,
                    table: schema.id,
                    name: index_name,
                    columns: primary_key.clone(),
                    unique: true,
                    constraint: IndexConstraintKind::PrimaryKey,
                };
                Ok::<_, DbError>((primary_key, new_schema, gc_horizon, index, catalog_snapshot))
            })();
            let (primary_key, new_schema, gc_horizon, index, catalog_snapshot) = match prepared {
                Ok(prepared) => prepared,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            let catalog_before = Some(catalog_snapshot);

            let pre_commit = (|| {
                let ctx = maintenance_statement_context(txn_id, self, gc_horizon, cancel.clone());
                components.storage.validate_table_primary_key_change(
                    &ctx,
                    &new_schema,
                    gc_horizon,
                )?;
                components.catalog.reserve_index_id(index.id)?;
                components.catalog.reserve_storage_id(index.storage_id)?;
                components.wal.append(WalRecord {
                    lsn: 0,
                    txn_id,
                    kind: WalRecordKind::AlterTablePrimaryKey {
                        table_id: schema.id,
                        primary_key,
                    },
                })?;
                components.storage.create_index(&ctx, &index, gc_horizon)?;
                Ok::<_, DbError>(PrimaryKeyAlterPostCommit::Add {
                    txn_id,
                    schema: new_schema,
                    index,
                    gc_horizon,
                })
            })();

            let post_commit = match pre_commit {
                Ok(post_commit) => post_commit,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, catalog_before);
                    return Err(err);
                }
            };
            if let Err(err) = cancel.check() {
                self.rollback_pre_durable_or_die(txn_id, catalog_before);
                return Err(err);
            }
            if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
                self.rollback_pre_durable_or_die(txn_id, catalog_before);
                return Err(err);
            }
            if let Err(err) = self.finish_alter_table_primary_key_after_commit(post_commit) {
                self.fatal_after_durable_commit(err);
            }
            if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
                self.fatal_after_durable_commit(err);
            }
            components.active_txns.deregister(txn_id);
            components.lock_manager.on_txn_finished();
        }
        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);

        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
    }

    pub(super) fn run_alter_table_add_foreign_key(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
        prepared_versions: Option<&[PreparedRelationVersion]>,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableAddForeignKey { table, foreign_key } = statement else {
            return Err(DbError::internal(
                "expected ALTER TABLE ADD FOREIGN KEY statement",
            ));
        };
        let components = &self.components;
        {
            let locked = self.lock_foreign_key_tables(
                &table,
                &foreign_key.referenced_table,
                RelationLockMode::Share,
                cancel,
                prepared_versions,
            )?;
            let txn_id = locked.txn_id;
            let catalog_before = match components.catalog.snapshot() {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(err);
                }
            };
            let pre_commit = (|| {
                let resolved =
                    resolve_alter_foreign_key(&locked.child, &locked.parent, foreign_key)?;
                let proposed_catalog =
                    Arc::new(MemoryCatalog::try_from_snapshot(catalog_before.clone())?);
                let proposed = proposed_catalog
                    .attach_foreign_keys(locked.child.id, vec![resolved.clone()])?;
                let indexes = components.catalog.list_indexes_for_table(locked.child.id)?;
                let gc_horizon = components.gc_horizon();
                let captured = self.capture_consistent_snapshots_cancelable(txn_id, cancel)?;
                let statement = StatementContext::with_snapshot_and_isolation(
                    txn_id,
                    captured.snapshot,
                    IsolationLevel::ReadCommitted,
                )
                .with_gc_horizon(gc_horizon)
                .with_tuple_lock_manager(components.lock_manager.clone())
                .with_conflict_waiter(components.lock_manager.clone(), cancel.clone());
                let execution = ExecutionContext {
                    statement,
                    relations: captured.relations,
                    catalog: proposed_catalog,
                    storage: components.storage.as_ref(),
                    schema_ops: components.storage.as_ref(),
                    gc_horizon,
                    cancel: cancel.as_ref(),
                    spill: spill::SpillConfig::new(
                        4096_u64.saturating_mul(1024),
                        components.config.data_dir.join("tmp"),
                    ),
                };
                validate_existing_foreign_keys(&execution, proposed.clone())?;
                let attached = components
                    .catalog
                    .attach_foreign_keys(locked.child.id, vec![resolved])?;
                if attached != proposed {
                    return Err(DbError::internal(
                        "validated foreign-key schema differs from live catalog attachment",
                    ));
                }
                components.storage.update_table_schema(
                    &execution.statement,
                    &attached,
                    &indexes,
                )?;
                Ok::<_, DbError>(())
            })();
            if let Err(err) = pre_commit {
                self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
                return Err(err);
            }
            if let Err(err) = cancel.check() {
                self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
                return Err(err);
            }
            if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
                self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
                return Err(err);
            }
            if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
                self.fatal_after_durable_commit(err);
            }
            components.active_txns.deregister(txn_id);
            components.lock_manager.on_txn_finished();
        }
        record_commit_and_maybe_checkpoint_after_durable_commit(components);
        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
    }

    pub(super) fn run_alter_table_drop_constraint(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
        prepared_versions: Option<&[PreparedRelationVersion]>,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableDropConstraint {
            table,
            constraint_name,
            if_exists,
        } = statement
        else {
            return Err(DbError::internal(
                "expected ALTER TABLE DROP CONSTRAINT statement",
            ));
        };
        let components = &self.components;
        {
            let locked =
                self.lock_drop_constraint(&table, &constraint_name, cancel, prepared_versions)?;
            let txn_id = locked.txn_id;
            match &locked.kind {
                DropConstraintKind::PrimaryKey => self.drop_primary_key_under_locks(
                    txn_id,
                    &locked.child,
                    Some(&constraint_name),
                    cancel,
                )?,
                DropConstraintKind::ForeignKey(foreign_key) => {
                    let Some(parent) = &locked.parent else {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(DbError::internal(
                            "locked foreign-key drop is missing its parent table",
                        ));
                    };
                    if parent.id != foreign_key.referenced_table {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(DbError::internal(
                            "locked foreign-key drop has an inconsistent parent table",
                        ));
                    }
                    self.drop_foreign_key_under_locks(
                        txn_id,
                        &locked.child,
                        &constraint_name,
                        cancel,
                    )?;
                }
                DropConstraintKind::UnsupportedUnique => {
                    self.rollback_pre_durable_or_die(txn_id, None);
                    return Err(DbError::plan(
                        SqlState::FeatureNotSupported,
                        format!("dropping UNIQUE constraint {constraint_name} is not supported",),
                    ));
                }
                DropConstraintKind::Missing => {
                    if !if_exists {
                        self.rollback_pre_durable_or_die(txn_id, None);
                        return Err(DbError::plan(
                            SqlState::UndefinedObject,
                            format!(
                                "constraint {constraint_name} of relation {} does not exist",
                                locked.child.name
                            ),
                        ));
                    }
                    self.commit_empty_maintenance(txn_id, cancel)?;
                }
            }
        }
        record_commit_and_maybe_checkpoint_after_durable_commit(components);
        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
    }

    fn drop_foreign_key_under_locks(
        &self,
        txn_id: u64,
        child: &TableSchema,
        constraint_name: &str,
        cancel: &Arc<QueryCancel>,
    ) -> Result<()> {
        let components = &self.components;
        let catalog_before = match components.catalog.snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let pre_commit = (|| {
            let dropped = components
                .catalog
                .drop_foreign_key(child.id, constraint_name, false)?
                .ok_or_else(|| DbError::internal("locked foreign key disappeared before drop"))?;
            let indexes = components.catalog.list_indexes_for_table(child.id)?;
            let ctx = maintenance_statement_context(
                txn_id,
                self,
                components.gc_horizon(),
                cancel.clone(),
            );
            components
                .storage
                .update_table_schema(&ctx, &dropped, &indexes)
        })();
        if let Err(err) = pre_commit {
            self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
            return Err(err);
        }
        if let Err(err) = cancel.check() {
            self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
            return Err(err);
        }
        if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
            self.rollback_pre_durable_or_die(txn_id, Some(catalog_before));
            return Err(err);
        }
        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        components.active_txns.deregister(txn_id);
        components.lock_manager.on_txn_finished();
        Ok(())
    }

    fn commit_empty_maintenance(&self, txn_id: u64, cancel: &QueryCancel) -> Result<()> {
        if let Err(err) = cancel.check() {
            self.rollback_pre_durable_or_die(txn_id, None);
            return Err(err);
        }
        if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
            self.rollback_pre_durable_or_die(txn_id, None);
            return Err(err);
        }
        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        self.components.active_txns.deregister(txn_id);
        self.components.lock_manager.on_txn_finished();
        Ok(())
    }

    /// `ALTER TABLE <t> DROP PRIMARY KEY` or `DROP CONSTRAINT <pkey>`.
    pub(super) fn run_alter_table_drop_primary_key(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        self.run_alter_table_drop_primary_key_named(statement, None, cancel)
    }

    fn run_alter_table_drop_primary_key_named(
        &self,
        statement: Statement,
        expected_name: Option<&str>,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableDropPrimaryKey { table } = statement else {
            return Err(DbError::internal(
                "expected ALTER TABLE DROP PRIMARY KEY statement",
            ));
        };

        {
            let locked = self.lock_maintenance_table(&table, cancel)?;
            self.drop_primary_key_under_locks(
                locked.txn_id,
                &locked.schema,
                expected_name,
                cancel,
            )?;
        }
        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);

        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
    }

    fn drop_primary_key_under_locks(
        &self,
        txn_id: u64,
        schema: &TableSchema,
        expected_name: Option<&str>,
        cancel: &Arc<QueryCancel>,
    ) -> Result<()> {
        let components = &self.components;
        let prepared = (|| {
            if schema.primary_key.is_empty() {
                return Err(DbError::plan(
                    SqlState::ObjectNotInPrerequisiteState,
                    format!("table {} does not have a primary key", schema.name),
                ));
            }
            let index = primary_key_constraint_index(components.catalog.as_ref(), schema)?;
            if expected_name.is_some_and(|expected_name| expected_name != index.name) {
                return Err(DbError::plan(
                    SqlState::UndefinedObject,
                    format!(
                        "constraint {} of relation {} does not exist",
                        expected_name.unwrap_or_default(),
                        schema.name
                    ),
                ));
            }
            let mut new_schema = schema.clone();
            new_schema.primary_key.clear();
            Ok::<_, DbError>((
                index,
                new_schema,
                components.gc_horizon(),
                components.catalog.snapshot()?,
            ))
        })();
        let (index, new_schema, gc_horizon, catalog_snapshot) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
        };
        let catalog_before = Some(catalog_snapshot);
        let pre_commit = (|| {
            components
                .catalog
                .preflight_table_primary_key_change(schema.id, &new_schema.primary_key)?;
            let ctx = maintenance_statement_context(txn_id, self, gc_horizon, cancel.clone());
            components
                .storage
                .validate_table_primary_key_change(&ctx, &new_schema, gc_horizon)?;
            components.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::AlterTablePrimaryKey {
                    table_id: schema.id,
                    primary_key: Vec::new(),
                },
            })?;
            components.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::DropIndex { index: index.id },
            })?;
            Ok::<_, DbError>(PrimaryKeyAlterPostCommit::Drop {
                txn_id,
                schema: new_schema,
                index_id: index.id,
                gc_horizon,
            })
        })();
        let post_commit = match pre_commit {
            Ok(post_commit) => post_commit,
            Err(err) => {
                self.rollback_pre_durable_or_die(txn_id, catalog_before);
                return Err(err);
            }
        };
        if let Err(err) = cancel.check() {
            self.rollback_pre_durable_or_die(txn_id, catalog_before);
            return Err(err);
        }
        if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
            self.rollback_pre_durable_or_die(txn_id, catalog_before);
            return Err(err);
        }
        if let Err(err) = self.finish_alter_table_primary_key_after_commit(post_commit) {
            self.fatal_after_durable_commit(err);
        }
        if let Err(err) = self.cleanup_after_durable_commit(txn_id) {
            self.fatal_after_durable_commit(err);
        }
        components.active_txns.deregister(txn_id);
        components.lock_manager.on_txn_finished();
        Ok(())
    }

    fn require_user_table(&self, table: &QualifiedName) -> Result<TableSchema> {
        let schema_id = match &table.schema {
            Some(schema) => self
                .components
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
        let schema = match self
            .components
            .catalog
            .get_table_in_schema(schema_id, &table.name)?
        {
            Some(schema) => schema,
            None if self
                .components
                .catalog
                .get_view_in_schema(schema_id, &table.name)?
                .is_some()
                || self
                    .components
                    .catalog
                    .get_index_in_schema(schema_id, &table.name)?
                    .is_some()
                || self
                    .components
                    .catalog
                    .get_sequence_in_schema(schema_id, &table.name)?
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
                "cannot ALTER PRIMARY KEY on a hidden relation",
            ));
        }
        Ok(schema)
    }

    fn finish_alter_table_primary_key_after_commit(
        &self,
        post: PrimaryKeyAlterPostCommit,
    ) -> Result<()> {
        match post {
            PrimaryKeyAlterPostCommit::Add {
                txn_id,
                schema,
                index,
                gc_horizon,
            } => {
                self.components
                    .ssi_manager
                    .promote_table_identity_locks_to_relation(schema.id);
                self.components
                    .storage
                    .set_table_primary_key_logged(&schema, gc_horizon, txn_id)?;
                self.components.wal.flush()?;
                let committed_schema = self.components.catalog.add_table_primary_key_index(
                    schema.id,
                    schema.primary_key.clone(),
                    index,
                )?;
                self.components
                    .storage
                    .set_table_primary_key_metadata(&committed_schema)
            }
            PrimaryKeyAlterPostCommit::Drop {
                txn_id,
                schema,
                index_id,
                gc_horizon,
            } => {
                let committed_schema = self
                    .components
                    .catalog
                    .drop_table_primary_key_index(schema.id, index_id)?;
                self.components
                    .ssi_manager
                    .promote_table_identity_locks_to_relation(schema.id);
                self.components.storage.set_table_primary_key_logged(
                    &committed_schema,
                    gc_horizon,
                    txn_id,
                )?;
                self.components.wal.flush()?;
                self.components.storage.apply_drop_index(index_id)
            }
        }
    }
}

fn maintenance_statement_context(
    txn_id: u64,
    service: &QueryService,
    gc_horizon: u64,
    cancel: Arc<QueryCancel>,
) -> StatementContext {
    StatementContext::new(txn_id)
        .with_gc_horizon(gc_horizon)
        .with_tuple_lock_manager(service.components.lock_manager.clone())
        .with_conflict_waiter(service.components.lock_manager.clone(), cancel)
}

fn resolve_alter_foreign_key(
    child: &TableSchema,
    parent: &TableSchema,
    foreign_key: ParsedForeignKey,
) -> Result<ResolvedForeignKey> {
    if foreign_key.columns.is_empty() {
        return Err(DbError::plan(
            SqlState::SyntaxError,
            "foreign key column list must not be empty",
        ));
    }
    let referenced_names = if foreign_key.referenced_columns.is_empty() {
        if parent.primary_key.is_empty() {
            return Err(DbError::plan(
                SqlState::InvalidForeignKey,
                format!("referenced table {} has no primary key", parent.name),
            ));
        }
        parent
            .primary_key
            .iter()
            .map(|column_id| {
                parent
                    .columns
                    .iter()
                    .find(|column| column.id == *column_id)
                    .map(|column| column.name.clone())
                    .ok_or_else(|| {
                        DbError::internal("primary key references a missing parent column")
                    })
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        foreign_key.referenced_columns
    };
    if foreign_key.columns.len() != referenced_names.len() {
        return Err(DbError::plan(
            SqlState::InvalidForeignKey,
            "foreign key source and referenced column counts do not match",
        ));
    }
    let columns = resolve_foreign_key_column_names(child, &foreign_key.columns)?;
    let referenced_columns = resolve_foreign_key_column_names(parent, &referenced_names)?;
    Ok(ResolvedForeignKey {
        name: foreign_key.name,
        columns,
        referenced_table: parent.id,
        referenced_columns,
        on_update: foreign_key.on_update,
        on_delete: foreign_key.on_delete,
    })
}

fn resolve_foreign_key_column_names(
    schema: &TableSchema,
    names: &[String],
) -> Result<Vec<ColumnId>> {
    let mut seen = std::collections::HashSet::new();
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        if !seen.insert(name.as_str()) {
            return Err(DbError::plan(
                SqlState::InvalidForeignKey,
                format!("foreign key column {name} is specified more than once"),
            ));
        }
        let column = schema
            .columns
            .iter()
            .find(|column| column.name == *name)
            .ok_or_else(|| {
                DbError::plan(
                    SqlState::UndefinedColumn,
                    format!("column {name} of relation {} does not exist", schema.name),
                )
            })?;
        columns.push(column.id);
    }
    Ok(columns)
}

fn primary_key_column_ids(schema: &TableSchema, columns: &[String]) -> Result<Vec<ColumnId>> {
    let mut seen = std::collections::HashSet::new();
    let mut ids = Vec::with_capacity(columns.len());
    for column_name in columns {
        if !seen.insert(column_name.as_str()) {
            return Err(DbError::plan(
                SqlState::SyntaxError,
                format!("duplicate primary key column {column_name}"),
            ));
        }
        let column = schema
            .columns
            .iter()
            .find(|column| column.name == *column_name)
            .ok_or_else(|| {
                DbError::plan(
                    SqlState::UndefinedColumn,
                    format!("primary key column {column_name} does not exist"),
                )
            })?;
        ids.push(column.id);
    }
    Ok(ids)
}

fn schema_with_primary_key(
    mut schema: TableSchema,
    primary_key: Vec<ColumnId>,
) -> Result<TableSchema> {
    for column_id in &primary_key {
        let column = schema
            .columns
            .iter_mut()
            .find(|column| column.id == *column_id)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "primary key column id {column_id} is missing from table {}",
                    schema.name
                ))
            })?;
        column.nullable = false;
    }
    schema.primary_key = primary_key;
    Ok(schema)
}

fn primary_key_constraint_index(
    catalog: &dyn catalog::CatalogManager,
    schema: &TableSchema,
) -> Result<IndexSchema> {
    catalog
        .list_indexes_for_table(schema.id)?
        .into_iter()
        .find(|index| index.constraint == IndexConstraintKind::PrimaryKey)
        .ok_or_else(|| {
            DbError::internal(format!(
                "table {} has primary-key metadata but no primary-key constraint index",
                schema.name
            ))
        })
}

fn classify_drop_constraint(
    catalog: &dyn CatalogManager,
    schema: &TableSchema,
    constraint_name: &str,
) -> Result<DropConstraintKind> {
    let indexes = catalog.list_indexes_for_table(schema.id)?;
    if !schema.primary_key.is_empty() {
        let primary = indexes
            .iter()
            .find(|index| index.constraint == IndexConstraintKind::PrimaryKey)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "table {} has primary-key metadata but no primary-key constraint index",
                    schema.name
                ))
            })?;
        if primary.name == constraint_name {
            return Ok(DropConstraintKind::PrimaryKey);
        }
    }
    if indexes.iter().any(|index| {
        index.constraint == IndexConstraintKind::Unique && index.name == constraint_name
    }) {
        return Ok(DropConstraintKind::UnsupportedUnique);
    }
    Ok(schema
        .foreign_keys
        .iter()
        .find(|foreign_key| foreign_key.name == constraint_name)
        .cloned()
        .map_or(DropConstraintKind::Missing, DropConstraintKind::ForeignKey))
}
