use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use common::{
    CompressionSetting, DbError, RelationKind, Result, SqlState, StatementContext, TableId,
    TableOptionPatch, TableSchema, ToastCompression, ToastOptions, needs_toast_relation,
    toast_schema,
};
use executor::ExecutionResult;
use parser::Statement;
use storage::SchemaOperations;
use wal::{WalRecord, WalRecordKind};

use crate::checkpoint::record_commit_and_maybe_checkpoint_after_durable_commit;

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

impl QueryService {
    /// `ALTER TABLE <t> SET (compression = ...)`: immediate-commit DDL under
    /// the exclusive guard, then a full rewrite that logs a FullPageImage per
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
    /// The exclusive guard is scoped to a block covering pre-commit AND
    /// post-commit work (rewriting every page needs writers drained the whole
    /// time), then dropped BEFORE
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
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableSetCompression { table, compression } = statement else {
            return Err(DbError::internal("expected ALTER TABLE statement"));
        };
        let components = &self.components;

        {
            // 1-2. Bind the table name; take the exclusive guard (drains writers,
            // like VACUUM / CREATE INDEX backfill). Scoped to this block so it is
            // dropped before the checkpoint trigger below runs.
            let _guard = components.concurrency.begin_checkpoint()?;
            let schema = components
                .catalog
                .get_table_by_name(&table)?
                .ok_or_else(|| {
                    DbError::plan(
                        SqlState::UndefinedTable,
                        format!("table {table} does not exist"),
                    )
                })?;

            let txn_id = components.next_txn_id.fetch_add(1, Ordering::AcqRel);

            // 3. Train a dictionary from current heap images (zstd only, and only
            // when the corpus suffices — a tiny/empty table proceeds dict-less).
            // Pre-commit: a failure here is a legitimate statement error, since
            // nothing has committed yet.
            let mut active_dict_id = None;
            if compression == CompressionSetting::Zstd {
                let samples = components
                    .storage
                    .sample_heap_pages(&schema, DICT_TRAINING_PAGE_CAP)?;
                if let Some(bytes) = compress::train_dictionary(&samples) {
                    let dict_id = components.catalog.allocate_dictionary_id()?;
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
            // point: everything above (and this block) propagates `?` normally.
            components.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::AlterTableCompression {
                    table_id: schema.id,
                    compression,
                    active_dict_id,
                },
            })?;
            components.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::Commit,
            })?;
            components.wal.flush()?;

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
        }
        // The exclusive guard dropped when the block above ended.
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
        components.storage.rewrite_table_pages(&schema)?;
        components.wal.flush()?;
        components.buffer_pool.flush_dirty_pages()?;
        components.store.sync_all()?;
        // `flush_dirty_pages` does not mark frames clean (`buffer::BufferPool`'s
        // contract: the caller fsyncs via the store and only then calls
        // `mark_all_clean`). Without this, the rewrite's pages would still be
        // marked dirty and get redundantly re-written at the next checkpoint.
        components.buffer_pool.mark_all_clean()?;
        Ok(())
    }

    /// `ALTER TABLE <t> SET (toast...)`: future-write-only TOAST policy change
    /// under the exclusive maintenance guard. Existing parent rows and existing
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
            let _guard = components.concurrency.begin_checkpoint()?;
            let schema = components
                .catalog
                .get_table_by_name(&table)?
                .ok_or_else(|| {
                    DbError::plan(
                        SqlState::UndefinedTable,
                        format!("table {table} does not exist"),
                    )
                })?;
            if schema.relation_kind != RelationKind::User {
                return Err(DbError::plan(
                    SqlState::FeatureNotSupported,
                    "cannot ALTER TOAST options on a hidden relation",
                ));
            }

            let txn_id = components
                .active_txns
                .register_allocated(|| components.next_txn_id.fetch_add(1, Ordering::AcqRel));
            let pre_commit = self.prepare_alter_table_toast_commit(txn_id, &schema, &options);
            let post_commit = match pre_commit {
                Ok(post_commit) => post_commit,
                Err(err) => {
                    self.rollback_pre_durable_or_die(txn_id, None);
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
                self.rollback_pre_durable_or_die(txn_id, None);
                return Err(err);
            }
            if let Err(err) = self.append_and_flush_commit(txn_id, &[]) {
                self.rollback_pre_durable_or_die(txn_id, None);
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
    ) -> Result<ToastAlterPostCommit> {
        let components = &self.components;
        let ctx = StatementContext::new(txn_id).with_conflict_waiter(
            components.lock_manager.clone(),
            Arc::new(AtomicBool::new(false)),
        );

        let mut toast = schema.toast.apply_patch(&options.toast);
        if options.toast.compression == Some(ToastCompression::ZstdDict) {
            let samples = components.storage.sample_toast_values(
                &ctx,
                schema,
                TOAST_DICT_MAX_SAMPLES,
                TOAST_DICT_MAX_BYTES,
            )?;
            if let Some(bytes) = compress::train_dictionary(&samples) {
                let dict_id = components.catalog.allocate_dictionary_id()?;
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
            let mut base = schema.clone();
            base.toast_table_id = Some(toast_table_id);
            let hidden = toast_schema(&base, toast_table_id);
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
}
