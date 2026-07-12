use std::sync::{Arc, RwLockWriteGuard, atomic::Ordering};

use common::{
    ColumnId, CompressionSetting, DbError, IndexConstraintKind, IndexId, IndexSchema,
    QualifiedName, QueryCancel, RelationKind, Result, SqlState, StatementContext, TableId,
    TableOptionPatch, TableSchema, ToastCompression, ToastOptions, WriteGuard,
    needs_toast_relation, toast_schema,
};
use executor::ExecutionResult;
use parser::Statement;
use storage::{RecoveryOperations, SchemaOperations};
use wal::{WalRecord, WalRecordKind};

use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;
use crate::lock_manager::{ObjectLockGuard, ObjectLockRequest, RelationLockMode};

use super::QueryService;

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

    /// `ALTER TABLE <t> DROP PRIMARY KEY` or `DROP CONSTRAINT <pkey>`.
    pub(super) fn run_alter_table_drop_primary_key(
        &self,
        statement: Statement,
        cancel: &Arc<QueryCancel>,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableDropPrimaryKey {
            table,
            constraint_name,
        } = statement
        else {
            return Err(DbError::internal(
                "expected ALTER TABLE DROP PRIMARY KEY statement",
            ));
        };

        let components = &self.components;
        {
            let locked = self.lock_maintenance_table(&table, cancel)?;
            let txn_id = locked.txn_id;
            let schema = locked.schema;
            let prepared = (|| {
                if schema.primary_key.is_empty() {
                    return Err(DbError::plan(
                        SqlState::ObjectNotInPrerequisiteState,
                        format!("table {table} does not have a primary key"),
                    ));
                }
                let index = primary_key_constraint_index(components.catalog.as_ref(), &schema)?;
                if let Some(name) = &constraint_name
                    && *name != index.name
                {
                    return Err(DbError::plan(
                        SqlState::UndefinedObject,
                        format!("constraint {name} does not exist"),
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
                let ctx = maintenance_statement_context(txn_id, self, gc_horizon, cancel.clone());
                components.storage.validate_table_primary_key_change(
                    &ctx,
                    &new_schema,
                    gc_horizon,
                )?;
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
        }
        record_commit_and_maybe_checkpoint_after_durable_commit(&self.components);

        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
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
        .with_conflict_waiter(service.components.lock_manager.clone(), cancel)
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
