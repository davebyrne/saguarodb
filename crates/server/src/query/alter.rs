use std::sync::atomic::Ordering;

use common::{CompressionSetting, DbError, Result, SqlState};
use executor::ExecutionResult;
use parser::Statement;
use wal::{WalRecord, WalRecordKind};

use super::QueryService;

/// How many heap pages to sample for dictionary training (`compression.md`
/// §7: evenly sampled, capped — a 32 MiB corpus at 8 KiB pages).
const DICT_TRAINING_PAGE_CAP: usize = 4096;

impl QueryService {
    /// `ALTER TABLE <t> SET (compression = ...)`: immediate-commit DDL under
    /// the exclusive guard, then a full rewrite that logs a FullPageImage per
    /// page (torn-page repair, exactly like VACUUM) (`compression.md` §8).
    /// Ordering is load-bearing: dict file durable → WAL records flushed →
    /// catalog/registry updated → rewrite (FPI per page) → rewrite FPIs
    /// flushed (write-ahead) → page flush → fsync.
    pub(super) fn run_alter_table_compression(
        &self,
        statement: Statement,
    ) -> Result<ExecutionResult> {
        let Statement::AlterTableSetCompression { table, compression } = statement else {
            return Err(DbError::internal("expected ALTER TABLE statement"));
        };
        let components = &self.components;

        // 1-2. Bind the table name; take the exclusive guard (drains writers,
        // like VACUUM / CREATE INDEX backfill).
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

        // 4. DDL record + immediate commit, flushed durable before any
        // page image can reference the new state.
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

        // 5. Install in catalog + engine/registry.
        let schema =
            components
                .catalog
                .set_table_compression(schema.id, compression, active_dict_id)?;
        components.storage.set_table_compression(&schema)?;

        // 6-8. Rewrite: re-encode every page, logging a FullPageImage per
        // page and stamping the FPI's LSN as the page's new PageLSN (§8).
        // This flush is load-bearing. `flush_dirty_pages` does NOT gate on
        // PageLSN — it assumes the WAL is already durable — so the
        // rewrite's FPIs must be flushed here first. Removing this flush
        // would let a torn page write precede its FPI being durable
        // (silent corruption on recovery), NOT produce a loud error. A
        // crash mid-rewrite leaves self-describing mixed encodings, and a
        // torn page write is repaired by redo replaying its FPI (§8).
        components.storage.rewrite_table_pages(&schema)?;
        components.wal.flush()?;
        components.buffer_pool.flush_dirty_pages()?;
        components.store.sync_all()?;

        Ok(ExecutionResult::Modified {
            command: "ALTER TABLE".to_string(),
            count: 0,
        })
    }
}
