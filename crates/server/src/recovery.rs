use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use buffer::{BufferPool, MemoryBufferPool, PAGE_SIZE, PageStore};
use catalog::{CatalogManager, MemoryCatalog, deserialize_catalog};
use common::{
    DbError, FlushPolicy, PageFlushInfo, RelationKind, Result, RwLockConcurrencyController,
    TableId, TruncateTablePlan,
};
use control::{ControlStore, FileControlStore};
use storage::{HeapPageStore, PageBackedStorageEngine, RecoveryOperations, StorageMode};
use wal::{FileWalManager, WalManager, WalRecordKind, is_redo_operation};

use crate::app::{AppState, ServerComponents};
use crate::checkpoint::{CheckpointState, cleanup_relation_generation_files, run_checkpoint};
use crate::config::Config;
use crate::query::QueryService;
use crate::shutdown::ShutdownState;

pub fn open_app(config: Config) -> Result<AppState> {
    // Shared compression state (`docs/specs/compression.md` §5a/§7): one registry
    // instance is injected into both the at-rest heap store and the WAL FPI path
    // so a file's config is consulted consistently by both; the dict store is the
    // durable home for trained per-table dictionaries.
    let compression = Arc::new(compress::CompressionRegistry::new());
    let dict_store = Arc::new(compress::DictStore::open(config.data_dir.join("dicts"))?);
    let control: Arc<dyn ControlStore> =
        Arc::new(FileControlStore::open(&config.data_dir, PAGE_SIZE as u32)?);
    let temp_dir = config.data_dir.join("tmp");
    std::fs::create_dir_all(&temp_dir)
        .map_err(|err| DbError::io(format!("failed to create spill directory: {err}")))?;
    tempfile::tempfile_in(&temp_dir)
        .map_err(|err| DbError::io(format!("spill directory is not writable: {err}")))?;
    let store: Arc<dyn PageStore> = Arc::new(HeapPageStore::open_with_compression(
        config.data_dir.join("heap"),
        compression.clone(),
    )?);
    let wal: Arc<dyn WalManager> = Arc::new(FileWalManager::open(config.data_dir.join("wal.dat"))?);
    let buffer_pool: Arc<dyn BufferPool> = Arc::new(MemoryBufferPool::new(
        config.buffer_pool_frames,
        Box::new(WalFlushPolicy { wal: wal.clone() }),
        store.clone(),
    ));
    // The durable on-disk primary-key index means recovery never rebuilds an
    // in-memory directory, so redo may flush+evict committed pages to the heap and
    // index files. Allow stealing from the start: the recovery working set is no
    // longer bounded by the buffer pool size.
    buffer_pool.enable_stealing();

    // The control record is the redo boundary plus the catalog snapshot.
    let loaded = control.load()?;
    let checkpoint_lsn = loaded
        .as_ref()
        .map(|control| control.checkpoint_lsn)
        .unwrap_or(0);
    let catalog: Arc<dyn CatalogManager> = match &loaded {
        Some(control) => Arc::new(MemoryCatalog::try_from_snapshot(deserialize_catalog(
            &control.catalog,
        )?)?),
        None => Arc::new(MemoryCatalog::empty()),
    };

    let storage = Arc::new(PageBackedStorageEngine::open_with_compression(
        buffer_pool.clone(),
        wal.clone(),
        StorageMode::Recovery,
        compression.clone(),
    )?);
    // Install both table and secondary-index schemas from the loaded catalog so
    // recovery replay and later DML maintain the indexes.
    let tables = catalog.list_tables()?;
    let mut indexes = Vec::new();
    for table in &tables {
        indexes.extend(catalog.list_indexes_for_table(table.id)?);
    }
    storage.install_schemas(tables)?;
    storage.install_index_schemas(indexes)?;
    storage.install_sequences(catalog.list_sequences()?)?;

    // Seed the dictionary resolver from the durable dict files so replay can
    // decompress dict-compressed FPIs and at-rest loads resolve dict ids
    // (`compression.md` §7). Orphan files (crash between file-durable and WAL
    // commit) are registered too — harmless — and their ids are burned so a
    // future allocation never collides with an orphan.
    let mut max_dict_id = 0u32;
    for (dict_id, _table_id, bytes) in dict_store.load_all()? {
        compression.register_dictionary(dict_id, &bytes)?;
        max_dict_id = max_dict_id.max(dict_id);
    }
    if max_dict_id > 0 {
        catalog.reserve_dictionary_id(max_dict_id)?;
    }

    validate_referenced_dictionaries(
        catalog.as_ref(),
        compression.as_ref(),
        &config.data_dir.join("dicts"),
    )?;

    // Redo-all (`docs/specs/mvcc.md` §8, Milestone D2): replay every PHYSICAL redo
    // record after the checkpoint LSN onto the heap and index pages, regardless of
    // the dirtying transaction's outcome. PageLSN gating makes this idempotent;
    // torn/missing pages are zeroed so a FullPageImage/HeapInit re-establishes
    // them. The durable B-tree is recovered the same way (its mutations are
    // full-page-image records). Visibility is decided afterwards by the CLOG, which
    // the WAL manager rebuilt from the durable `Commit`/`Abort` records at open: an
    // aborted or in-flight transaction's replayed versions are present in the heap
    // but invisible (and reclaimed by VACUUM in Milestone F). This replaces the old
    // redo-committed-only filter (`replay_committed_from`), which could not handle
    // the flushed-but-uncommitted pages the relaxed flush gate (D1) now admits.
    //
    // LOGICAL CATALOG records (`CreateTable`/`DropTable`/`TruncateTable`/
    // `CreateIndex`/`DropIndex` and sequence DDL) are the exception: they mutate
    // the durable catalog directly (not idempotent PageLSN-gated page bytes), so an
    // aborted DDL's catalog record must NOT take effect. Transactional DDL records
    // replay only for committed transactions; aborted/in-flight records are skipped,
    // gated by the rebuilt CLOG. Skipped CreateTable/CreateIndex/
    // CreateSequence records still reserve their IDs: their index/heap page records
    // may replay as orphan files, or their sequence IDs may have been observed in
    // WAL, and a future object must not reuse those identifiers.
    let mut replay_applied = false;
    // Writers whose page mutations were replayed: any of these left InProgress (no
    // durable Commit/Abort) is a crashed in-flight transaction whose versions are on
    // disk. They are resolved to Aborted below so VACUUM reclaims them before the floor
    // crosses them (`docs/specs/mvcc.md` §8; the FATAL-B resurrection fix).
    let mut writer_xids: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut pending_identity_rebuilds: std::collections::BTreeSet<TableId> =
        std::collections::BTreeSet::new();
    for record in wal.replay_from(checkpoint_lsn)? {
        let record = record?;
        if !is_redo_operation(&record.kind) {
            // `Commit`/`Abort`/`Checkpoint` are metadata markers, not page
            // mutations; the CLOG already absorbed them at WAL open.
            continue;
        }
        if record.txn_id != 0 {
            writer_xids.insert(record.txn_id);
        }
        if is_logical_catalog_record(&record.kind) && !wal.is_committed(record.txn_id) {
            // An aborted/in-flight DDL's catalog mutation must not be applied
            // (redo-all does not hide a non-idempotent catalog change behind the
            // CLOG the way it hides per-tuple versions). Its allocated object ID
            // must still stay burned, because physical page records for that ID
            // may have replayed and future objects map IDs to the same file names.
            if reserve_catalog_id(catalog.as_ref(), &record.kind)? {
                replay_applied = true;
            }
            continue;
        }
        let primary_key_rebuild = match &record.kind {
            WalRecordKind::AlterTablePrimaryKey { table_id, .. } => Some(*table_id),
            _ => None,
        };
        apply_redo(
            catalog.as_ref(),
            storage.as_ref(),
            buffer_pool.as_ref(),
            compression.as_ref(),
            dict_store.as_ref(),
            record.lsn,
            record.kind,
        )?;
        if let Some(table_id) = primary_key_rebuild {
            pending_identity_rebuilds.insert(table_id);
        }
        replay_applied = true;
    }
    if replay_applied {
        validate_referenced_dictionaries(
            catalog.as_ref(),
            compression.as_ref(),
            &config.data_dir.join("dicts"),
        )?;
    }

    let next_txn_id = next_txn_id(wal.as_ref())?;
    // Establish the CLOG implicit-committed floor (`docs/specs/mvcc.md` §5.4, §8).
    // When the WAL loaded a durable `clog.dat` snapshot its floor is authoritative and
    // this is a no-op. Otherwise (no snapshot — a fresh database, or a pre-durable-CLOG
    // data directory whose WAL was conservatively truncated by the older build) the WAL
    // re-derives the floor conservatively: the oldest transaction in the retained WAL
    // whose CLOG status is not `Committed` (aborted or in-flight), or — if every retained
    // transaction is committed — the allocation boundary. That conservatively-truncated
    // WAL guarantees every transaction dropped below the oldest non-committed one was
    // committed, so flooring just under it never marks an aborted/in-flight txn committed.
    wal.establish_recovery_committed_floor(next_txn_id)?;
    // Resolve crashed in-flight writers to Aborted (no-undo MVCC has no undo pass).
    // Must run AFTER the floor is set, so a ghost xid sits at/above the floor and reads
    // InProgress; marking it Aborted lets VACUUM reclaim its on-disk versions and keeps
    // the floor pinned below it until then, instead of floating past it and resurrecting
    // never-committed data as committed (`docs/specs/mvcc.md` §8). Persisted by the
    // recovery checkpoint below via `clog.dat`.
    wal.resolve_in_flight_as_aborted(&writer_xids)?;
    for table_id in pending_identity_rebuilds {
        let Some(schema) = catalog.get_table(table_id)? else {
            continue;
        };
        if schema.relation_kind == RelationKind::User {
            storage.apply_rebuild_table_identity(schema)?;
            replay_applied = true;
        }
    }
    let tls = match config.tls_files().map_err(DbError::io)? {
        Some((cert, key)) => Some(crate::tls::build_acceptor(cert, key)?),
        None => None,
    };
    // The lock manager shares the registry handle (Arc-backed) so it can re-check a
    // blocker's liveness and canonicalize wait-for edges to top-level txn ids.
    let active_txns = crate::registry::ActiveTxnRegistry::new();
    let lock_manager = Arc::new(crate::lock_manager::LockManager::new(
        active_txns.clone(),
        std::time::Duration::from_millis(config.deadlock_timeout_ms),
    ));
    // SSI conflict tracking for SERIALIZABLE transactions (`docs/specs/ssi.md`).
    // Shares the registry handle to canonicalize subxids to top-level ids.
    let ssi_manager = Arc::new(crate::ssi_manager::SerializableConflictManager::new(
        active_txns.clone(),
    ));
    let components = Arc::new(ServerComponents {
        config,
        catalog,
        storage,
        buffer_pool,
        wal,
        control,
        store,
        compression,
        dict_store,
        concurrency: Arc::new(RwLockConcurrencyController::new()),
        checkpoint: CheckpointState {
            last_checkpoint_lsn: AtomicU64::new(checkpoint_lsn),
            commits_since_checkpoint: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
        },
        shutdown: Arc::new(ShutdownState::new()),
        next_txn_id: AtomicU64::new(next_txn_id),
        dead_rows_since_vacuum: AtomicU64::new(0),
        rows_changed_since_analyze: AtomicU64::new(0),
        active_txns,
        catalog_publication_gate: Arc::new(RwLock::new(())),
        relation_publish_gate: RwLock::new(()),
        lock_manager,
        ssi_manager,
        tls,
        cancel_registry: crate::cancel::CancelRegistry::new(),
        session_registry: Arc::new(crate::session_registry::SessionRegistry::new()),
    });

    // Persist the redone state to the heap/index and advance the redo boundary.
    if replay_applied {
        run_checkpoint(&components)?;
    }
    cleanup_relation_generation_files(&components)?;
    components.storage.set_mode(StorageMode::Normal)?;

    Ok(AppState {
        components: components.clone(),
        query_service: Arc::new(QueryService::new(components)),
    })
}

#[allow(dead_code)]
pub fn data_dir_for_test(path: &Path) -> Config {
    Config {
        data_dir: path.to_path_buf(),
        ..Config::default()
    }
}

/// Flush policy for in-place dirty-page flushing: a page is flushable once its
/// page-LSN is WAL-durable, regardless of whether its dirtying transaction has
/// committed (`docs/specs/mvcc.md` §8, Milestone D1).
///
/// The committedness gate that earlier milestones used (refuse to flush a page
/// dirtied by an uncommitted transaction) is retired here: a heap page now holds
/// versions from several transactions (per-version `xmin`/`xmax`), so page-level
/// committedness is incoherent. Uncommitted and aborted dirty pages may now be
/// evicted/flushed — they are hidden by the CLOG (`common::is_visible`) and
/// reclaimed by VACUUM (Milestone F), and redo-all recovery reinstates them under
/// PageLSN gating. The single remaining requirement is WAL-durability: a page may
/// reach the heap only once every WAL record that describes it is fsynced, so a
/// crash can always redo it (write-ahead logging).
struct WalFlushPolicy {
    wal: Arc<dyn WalManager>,
}

impl FlushPolicy for WalFlushPolicy {
    fn can_flush(&self, info: &PageFlushInfo) -> bool {
        info.page_lsn
            .is_none_or(|lsn| lsn <= self.wal.flushed_lsn())
    }

    fn ensure_durable(&self) -> Result<()> {
        // Write-ahead logging for the relaxed steal path: force the WAL so a stolen
        // (possibly uncommitted) page's records are durable before the page reaches
        // the heap. Idempotent — a no-op when the WAL is already flushed.
        self.wal.flush().map(|_| ())
    }
}

/// Whether `kind` is a logical catalog mutation (`CreateTable`/`DropTable`/
/// `CreateIndex`/`DropIndex`/sequence/view DDL). These directly mutate the durable catalog rather
/// than being idempotent PageLSN-gated page writes, so redo-all gates them by
/// transaction outcome (only a committed DDL replays); the physical heap/index
/// page records are not gated.
fn is_logical_catalog_record(kind: &WalRecordKind) -> bool {
    matches!(
        kind,
        WalRecordKind::CreateTable { .. }
            | WalRecordKind::DropTable { .. }
            | WalRecordKind::TruncateTable { .. }
            | WalRecordKind::UpdateTableSchema { .. }
            | WalRecordKind::CreateIndex { .. }
            | WalRecordKind::DropIndex { .. }
            | WalRecordKind::CreateSequence { .. }
            | WalRecordKind::DropSequence { .. }
            | WalRecordKind::CreateView { .. }
            | WalRecordKind::ReplaceView { .. }
            | WalRecordKind::DropView { .. }
            | WalRecordKind::CreateSchema { .. }
            | WalRecordKind::DropSchema { .. }
            | WalRecordKind::CreateDictionary { .. }
            | WalRecordKind::AlterTableCompression { .. }
            | WalRecordKind::AlterTableToast { .. }
            | WalRecordKind::AlterTablePrimaryKey { .. }
            | WalRecordKind::UpdateTableStatistics { .. }
    )
}

fn reserve_catalog_id(catalog: &dyn CatalogManager, kind: &WalRecordKind) -> Result<bool> {
    match kind {
        WalRecordKind::CreateTable { schema } => {
            catalog.reserve_table_id(schema.id)?;
            reserve_storage_id(catalog, schema.storage_id, schema.id)?;
            if let Some(toast_table_id) = schema.toast_table_id {
                catalog.reserve_table_id(toast_table_id)?;
            }
            Ok(true)
        }
        WalRecordKind::CreateIndex { schema } => {
            catalog.reserve_index_id(schema.id)?;
            reserve_storage_id(catalog, schema.storage_id, schema.id)?;
            Ok(true)
        }
        WalRecordKind::CreateSequence { schema } => {
            catalog.reserve_sequence_id(schema.id)?;
            Ok(true)
        }
        WalRecordKind::CreateView { schema } => {
            catalog.reserve_table_id(schema.id)?;
            Ok(true)
        }
        WalRecordKind::CreateSchema { schema } => {
            catalog.reserve_schema_id(schema.id)?;
            Ok(true)
        }
        WalRecordKind::CreateDictionary { dict_id, .. } => {
            catalog.reserve_dictionary_id(*dict_id)?;
            Ok(true)
        }
        WalRecordKind::TruncateTable {
            new_table_storage_id,
            new_toast_storage_id,
            new_index_storage_ids,
            ..
        } => {
            catalog.reserve_storage_id(*new_table_storage_id)?;
            if let Some((_, storage_id)) = new_toast_storage_id {
                catalog.reserve_storage_id(*storage_id)?;
            }
            for (_, storage_id) in new_index_storage_ids {
                catalog.reserve_storage_id(*storage_id)?;
            }
            Ok(true)
        }
        WalRecordKind::UpdateTableSchema { schema, indexes } => {
            reserve_storage_id(catalog, schema.storage_id, schema.id)?;
            for index in indexes {
                reserve_storage_id(catalog, index.storage_id, index.id)?;
            }
            Ok(true)
        }
        WalRecordKind::DropTable { .. }
        | WalRecordKind::DropIndex { .. }
        | WalRecordKind::DropSequence { .. }
        | WalRecordKind::ReplaceView { .. }
        | WalRecordKind::DropView { .. }
        | WalRecordKind::DropSchema { .. }
        | WalRecordKind::AlterTableCompression { .. }
        | WalRecordKind::AlterTableToast { .. }
        | WalRecordKind::AlterTablePrimaryKey { .. }
        | WalRecordKind::UpdateTableStatistics { .. } => Ok(false),
        _ => Ok(false),
    }
}

fn reserve_storage_id(
    catalog: &dyn CatalogManager,
    storage_id: common::FileId,
    legacy_id: common::FileId,
) -> Result<()> {
    catalog.reserve_storage_id(if storage_id == 0 {
        legacy_id
    } else {
        storage_id
    })
}

fn apply_redo(
    catalog: &dyn CatalogManager,
    storage: &dyn RecoveryOperations,
    buffer_pool: &dyn BufferPool,
    compression: &compress::CompressionRegistry,
    dict_store: &compress::DictStore,
    lsn: u64,
    kind: WalRecordKind,
) -> Result<()> {
    // Normalize a dict/codec-compressed FPI to a plain raw `FullPageImage` before
    // the match below: the physical arm's OR-pattern binds `file_id`/`page_num`
    // identically across its member kinds, but `FullPageImageCompressed` carries
    // different fields (`codec`/`dict_id`/`payload`) and cannot join it directly.
    // Decompressing here lets the existing physical-redo path run unchanged.
    let kind = match kind {
        WalRecordKind::FullPageImageCompressed {
            file_id,
            page_num,
            codec,
            dict_id,
            payload,
        } => {
            let image = compression.decompress_fpi(codec, dict_id, &payload, PAGE_SIZE)?;
            WalRecordKind::FullPageImage {
                file_id,
                page_num,
                image,
            }
        }
        other => other,
    };
    match &kind {
        WalRecordKind::CreateTable { schema } => {
            catalog.apply_create_table(schema.clone())?;
            let installed = catalog.get_table(schema.id)?.ok_or_else(|| {
                DbError::internal(format!(
                    "replayed CreateTable for table {} did not install a catalog schema",
                    schema.id
                ))
            })?;
            storage.apply_create_table(installed)
        }
        WalRecordKind::UpdateTableSchema { schema, indexes } => {
            catalog.apply_update_table_and_index_schemas(schema.clone(), indexes)?;
            for index in indexes {
                storage.apply_update_index_schema(index.clone())?;
            }
            storage.apply_update_table_schema(schema.clone())?;
            Ok(())
        }
        WalRecordKind::DropTable { table } => {
            catalog.apply_drop_table(*table)?;
            storage.apply_drop_table(*table)
        }
        WalRecordKind::CreateIndex { schema } => {
            catalog.apply_create_index(schema.clone())?;
            let installed = catalog
                .list_indexes_for_table(schema.table)?
                .into_iter()
                .find(|index| index.id == schema.id)
                .ok_or_else(|| {
                    DbError::internal(format!(
                        "replayed CreateIndex for index {} did not install a catalog schema",
                        schema.id
                    ))
                })?;
            storage.apply_create_index(installed)
        }
        WalRecordKind::DropIndex { index } => {
            catalog.apply_drop_index(*index)?;
            storage.apply_drop_index(*index)
        }
        WalRecordKind::CreateSequence { schema } => {
            catalog.apply_create_sequence(schema.clone())?;
            storage.apply_create_sequence(schema.clone())
        }
        WalRecordKind::DropSequence { sequence } => {
            catalog.apply_drop_sequence(*sequence)?;
            storage.apply_drop_sequence(*sequence)
        }
        WalRecordKind::CreateView { schema } => catalog.apply_create_view(schema.clone()),
        WalRecordKind::ReplaceView { schema } => catalog.apply_replace_view(schema.clone()),
        WalRecordKind::DropView { view } => catalog.apply_drop_view(*view),
        WalRecordKind::CreateSchema { schema } => catalog.apply_create_schema(schema.clone()),
        WalRecordKind::DropSchema { schema } => catalog.apply_drop_schema(*schema),
        WalRecordKind::SequenceAdvance { sequence, value } => {
            storage.apply_sequence_advance(*sequence, *value)
        }
        WalRecordKind::SetSequenceValue {
            sequence,
            value,
            is_called,
        } => storage.apply_set_sequence_value(*sequence, *value, *is_called),
        WalRecordKind::HeapInit { file_id, page_num }
        | WalRecordKind::HeapInsert {
            file_id, page_num, ..
        }
        | WalRecordKind::HeapDelete {
            file_id, page_num, ..
        }
        | WalRecordKind::HeapUpdateHeader {
            file_id, page_num, ..
        }
        | WalRecordKind::FullPageImage {
            file_id, page_num, ..
        } => {
            let mut guard = buffer_pool.fetch_for_redo(*file_id, *page_num)?;
            // A torn or never-initialized page cannot be trusted for PageLSN
            // gating; zero it so the first FullPageImage / HeapInit rebuilds it.
            if !storage::page_is_valid(guard.data()) {
                guard.data_mut().fill(0);
            }
            storage::apply_physical_redo(guard.data_mut(), lsn, &kind)?;
            Ok(())
        }
        WalRecordKind::Commit
        | WalRecordKind::CommitWithSubxids { .. }
        | WalRecordKind::Abort
        | WalRecordKind::Checkpoint { .. } => Err(DbError::internal(
            "recovery replay received an unexpected WAL record",
        )),
        WalRecordKind::CreateDictionary {
            dict_id,
            table_id,
            bytes,
        } => {
            // Recovery apply: durable-file install is idempotent; no WAL appended.
            dict_store.save(*dict_id, *table_id, bytes)?;
            compression.register_dictionary(*dict_id, bytes)?;
            catalog.reserve_dictionary_id(*dict_id)?;
            Ok(())
        }
        WalRecordKind::AlterTableCompression {
            table_id,
            compression: setting,
            active_dict_id,
        } => {
            let schema = catalog.set_table_compression(*table_id, *setting, *active_dict_id)?;
            storage.apply_set_table_compression(schema)
        }
        WalRecordKind::AlterTableToast {
            table_id,
            toast,
            toast_table_id,
        } => {
            let schema =
                catalog.set_table_toast_metadata(*table_id, toast.clone(), *toast_table_id)?;
            storage.apply_set_table_toast_metadata(schema)
        }
        WalRecordKind::TruncateTable {
            table_id,
            new_table_storage_id,
            new_toast_storage_id,
            new_index_storage_ids,
        } => {
            let plan = TruncateTablePlan {
                table_id: *table_id,
                new_table_storage_id: *new_table_storage_id,
                new_toast_storage_id: *new_toast_storage_id,
                new_index_storage_ids: new_index_storage_ids.clone(),
            };
            let update = catalog.apply_truncate_table(&plan)?;
            storage.apply_truncate_table(update)
        }
        WalRecordKind::AlterTablePrimaryKey {
            table_id,
            primary_key,
        } => {
            let schema = catalog.set_table_primary_key(*table_id, primary_key.clone())?;
            storage.apply_set_table_primary_key(schema)
        }
        WalRecordKind::UpdateTableStatistics {
            table_id,
            statistics,
        } => {
            // Catalog-only; storage is untouched. The table may have been
            // dropped later in the log — advisory statistics for it are
            // simply skipped, never an error.
            if catalog.get_table(*table_id)?.is_some() {
                catalog.set_table_statistics(*table_id, statistics.clone())?;
            }
            Ok(())
        }
        // Normalized away above: `FullPageImageCompressed` never reaches this match
        // (it is rewritten to `FullPageImage` before the match runs).
        WalRecordKind::FullPageImageCompressed { .. } => Err(DbError::internal(
            "unreachable: FullPageImageCompressed is normalized before dispatch",
        )),
    }
}

fn next_txn_id(wal: &dyn WalManager) -> Result<u64> {
    let mut max_txn_id = 0;
    // Seed the allocator from every retained WAL record, not only records after the
    // control record's checkpoint LSN. This intentionally covers the crash window
    // where the manifest and CLOG snapshot are durable but the checkpoint marker
    // carrying the transaction-id high-water has not yet been appended/flushed. If a
    // completed checkpoint later truncates below the boundary, the retained
    // Checkpoint marker still carries that high-water mark.
    for record in wal.replay_from(0)? {
        let record = record?;
        if record.txn_id != 0 {
            max_txn_id = max_txn_id.max(record.txn_id);
        }
        // A committed savepoint subxid lives only in the `CommitWithSubxids`
        // payload, not a record header (e.g. a released read-only savepoint).
        // Fold it in so the allocator never reissues a committed subxid. (Records
        // truncated below a completed checkpoint are covered by the retained
        // `Checkpoint` marker's high-water mark, which already includes subxids —
        // they are allocated from the same counter.
        if let WalRecordKind::CommitWithSubxids { subxids } = &record.kind
            && let Some(max_sub) = subxids.iter().copied().max()
        {
            max_txn_id = max_txn_id.max(max_sub);
        }
    }
    let next = max_txn_id
        .checked_add(1)
        .ok_or_else(|| DbError::wal(common::SqlState::InternalError, "transaction id overflow"))?;
    // Floor the allocator at FIRST_NORMAL_XID so real transactions never stamp
    // tuple headers with a reserved xid. On a fresh database max_txn_id is 0, so
    // an unfloored seed would hand out 1 and 2 (== FROZEN_XID), persisting rows
    // that later visibility code would treat as frozen/always-visible.
    Ok(next.max(common::FIRST_NORMAL_XID))
}

fn validate_referenced_dictionaries(
    catalog: &dyn CatalogManager,
    compression: &compress::CompressionRegistry,
    dict_dir: &Path,
) -> Result<()> {
    // Fail fast on a catalog-referenced-but-missing dictionary, rather than a
    // silent write-time dict-less fallback followed by a much later, confusing
    // read-time decode error. Every table whose CURRENT table/TOAST dict field
    // names a dictionary must have had that dictionary registered by the seeding
    // loop; if not, its durable `.dict` file is missing (deleted, corrupted
    // `dicts/` directory, manual tampering) and recovery cannot safely proceed.
    for schema in catalog.list_tables()? {
        validate_dictionary_ref(
            &schema.name,
            schema.id,
            "active dictionary",
            schema.active_dict_id,
            compression,
            dict_dir,
        )?;
        validate_dictionary_ref(
            &schema.name,
            schema.id,
            "TOAST active dictionary",
            schema.toast.active_dict_id,
            compression,
            dict_dir,
        )?;
    }
    Ok(())
}

fn validate_dictionary_ref(
    table_name: &str,
    table_id: common::TableId,
    field_name: &str,
    dict_id: Option<u32>,
    compression: &compress::CompressionRegistry,
    dict_dir: &Path,
) -> Result<()> {
    if let Some(dict_id) = dict_id
        && !compression.has_dictionary(dict_id)
    {
        return Err(DbError::internal(format!(
            "table '{table_name}' (id {table_id}) references {field_name} {dict_id}, but no \
             dictionary file for it was found under {}",
            dict_dir.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::app::AppState;
    use crate::checkpoint::run_checkpoint;
    use catalog::CatalogManager;
    use storage::RecoveryOperations;
    use wal::{FileWalManager, WalManager, WalRecord, WalRecordKind};

    #[test]
    fn startup_creates_a_writable_spill_directory() {
        let dir = tempfile::tempdir().unwrap();
        let app = super::open_app(super::data_dir_for_test(dir.path())).unwrap();
        assert!(dir.path().join("tmp").is_dir());
        tempfile::tempfile_in(dir.path().join("tmp")).unwrap();
        drop(app);
    }

    fn table_schema(id: common::TableId, name: &str) -> common::TableSchema {
        common::TableSchema {
            id,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: id,
            name: name.to_string(),
            columns: vec![common::ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: common::DataType::Integer,
                nullable: false,
                max_length: None,
                default: None,
                pg_type: None,
            }],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: common::CompressionSetting::None,
            active_dict_id: None,
            toast: common::ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: common::RelationKind::User,
            checks: Vec::new(),
        }
    }

    #[derive(Default)]
    struct CapturingRecoveryOps {
        tables: Mutex<Vec<common::TableSchema>>,
        indexes: Mutex<Vec<common::IndexSchema>>,
        truncates: Mutex<Vec<common::TruncateCatalogUpdate>>,
    }

    impl RecoveryOperations for CapturingRecoveryOps {
        fn apply_create_table(&self, schema: common::TableSchema) -> common::Result<()> {
            self.tables.lock().expect("tables lock").push(schema);
            Ok(())
        }

        fn apply_drop_table(&self, _table: common::TableId) -> common::Result<()> {
            Ok(())
        }

        fn apply_create_index(&self, schema: common::IndexSchema) -> common::Result<()> {
            self.indexes.lock().expect("indexes lock").push(schema);
            Ok(())
        }

        fn apply_update_table_schema(&self, schema: common::TableSchema) -> common::Result<()> {
            self.tables.lock().expect("tables lock").push(schema);
            Ok(())
        }

        fn apply_update_index_schema(&self, schema: common::IndexSchema) -> common::Result<()> {
            self.indexes.lock().expect("indexes lock").push(schema);
            Ok(())
        }

        fn apply_drop_index(&self, _index: common::IndexId) -> common::Result<()> {
            Ok(())
        }

        fn apply_create_sequence(&self, _schema: common::SequenceSchema) -> common::Result<()> {
            Ok(())
        }

        fn apply_drop_sequence(&self, _sequence: common::SequenceId) -> common::Result<()> {
            Ok(())
        }

        fn apply_sequence_advance(
            &self,
            _sequence: common::SequenceId,
            _value: i64,
        ) -> common::Result<()> {
            Ok(())
        }

        fn apply_set_sequence_value(
            &self,
            _sequence: common::SequenceId,
            _value: i64,
            _is_called: bool,
        ) -> common::Result<()> {
            Ok(())
        }

        fn apply_set_table_compression(&self, _schema: common::TableSchema) -> common::Result<()> {
            Ok(())
        }

        fn apply_set_table_toast_metadata(
            &self,
            _schema: common::TableSchema,
        ) -> common::Result<()> {
            Ok(())
        }

        fn apply_set_table_primary_key(&self, _schema: common::TableSchema) -> common::Result<()> {
            Ok(())
        }

        fn apply_rebuild_table_identity(&self, _schema: common::TableSchema) -> common::Result<()> {
            Ok(())
        }

        fn apply_truncate_table(
            &self,
            update: common::TruncateCatalogUpdate,
        ) -> common::Result<()> {
            self.truncates.lock().expect("truncates lock").push(update);
            Ok(())
        }
    }

    #[test]
    fn recovery_applies_catalog_normalized_legacy_create_schemas_to_storage() {
        let catalog = catalog::MemoryCatalog::empty();
        let storage = CapturingRecoveryOps::default();
        let buffer_pool = buffer::MemoryBufferPool::empty(1);
        let compression = compress::CompressionRegistry::new();
        let dict_dir = tempfile::tempdir().unwrap();
        let dict_store = compress::DictStore::open(dict_dir.path()).unwrap();

        let mut legacy_table = table_schema(41, "legacy_table");
        legacy_table.storage_id = 0;
        super::apply_redo(
            &catalog,
            &storage,
            &buffer_pool,
            &compression,
            &dict_store,
            1,
            WalRecordKind::CreateTable {
                schema: legacy_table.clone(),
            },
        )
        .unwrap();

        let installed_table = catalog
            .get_table(legacy_table.id)
            .unwrap()
            .expect("table installed in catalog");
        let captured_table = storage
            .tables
            .lock()
            .expect("tables lock")
            .pop()
            .expect("storage saw table create");
        assert_ne!(captured_table.storage_id, 0);
        assert_eq!(captured_table, installed_table);

        let legacy_index = common::IndexSchema {
            id: 7,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 0,
            table: legacy_table.id,
            name: "legacy_table_id_idx".to_string(),
            columns: vec![0],
            unique: false,
            constraint: common::IndexConstraintKind::None,
        };
        super::apply_redo(
            &catalog,
            &storage,
            &buffer_pool,
            &compression,
            &dict_store,
            2,
            WalRecordKind::CreateIndex {
                schema: legacy_index.clone(),
            },
        )
        .unwrap();

        let installed_index = catalog
            .list_indexes_for_table(legacy_table.id)
            .unwrap()
            .into_iter()
            .find(|index| index.id == legacy_index.id)
            .expect("index installed in catalog");
        let captured_index = storage
            .indexes
            .lock()
            .expect("indexes lock")
            .pop()
            .expect("storage saw index create");
        assert_ne!(captured_index.storage_id, 0);
        assert_eq!(captured_index, installed_index);
    }

    #[test]
    fn validate_referenced_dictionaries_checks_toast_active_dict() {
        let catalog = catalog::MemoryCatalog::empty();
        let mut schema = table_schema(1, "logs");
        schema.toast.active_dict_id = Some(7);
        catalog.apply_create_table(schema).unwrap();
        let compression = compress::CompressionRegistry::new();
        let dir = tempfile::tempdir().unwrap();

        let err = super::validate_referenced_dictionaries(&catalog, &compression, dir.path())
            .unwrap_err();

        assert!(
            err.message.contains("TOAST active dictionary 7"),
            "{}",
            err.message
        );
        assert!(err.message.contains("logs"), "{}", err.message);
    }

    #[tokio::test]
    async fn recovery_replays_committed_records_after_snapshot_lsn() {
        let dir = tempfile::tempdir().unwrap();
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (1, 'Ada')")
                .unwrap();
            run_checkpoint(&app.components).unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (2, 'Grace')")
                .unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let result = reopened
            .query_service
            .execute_sql("select id, name from users order by id")
            .unwrap();

        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn recovery_replays_committed_truncate_table_swap() {
        let dir = tempfile::tempdir().unwrap();
        let table_id;
        let new_storage_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            app.query_service
                .execute_sql("create index users_name_idx on users (name)")
                .unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (1, 'Ada'), (2, 'Grace')")
                .unwrap();
            run_checkpoint(&app.components).unwrap();

            let users = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .expect("users table exists");
            table_id = users.id;
            let old_storage_id = users.storage_id;
            let plan = app
                .components
                .catalog
                .prepare_truncate_table(table_id)
                .unwrap();
            new_storage_id = plan.new_table_storage_id;
            let update = app
                .components
                .catalog
                .build_truncate_table_update(&plan)
                .unwrap();

            app.components
                .storage
                .prepare_truncate_table(&common::StatementContext::new(41), &plan, &update)
                .unwrap();
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 41,
                    kind: WalRecordKind::Commit,
                })
                .unwrap();
            app.components.wal.flush().unwrap();
            assert_ne!(old_storage_id, new_storage_id);
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let users = reopened
            .components
            .catalog
            .get_table(table_id)
            .unwrap()
            .expect("users table exists after recovery");
        assert_eq!(users.storage_id, new_storage_id);
        assert_eq!(
            reopened
                .query_service
                .execute_sql("select id, name from users order by id")
                .unwrap()
                .row_count(),
            0
        );
        reopened
            .query_service
            .execute_sql("insert into users (id, name) values (1, 'Ada')")
            .unwrap();
        assert_eq!(
            reopened
                .query_service
                .execute_sql("select id, name from users")
                .unwrap()
                .row_count(),
            1
        );
    }

    #[test]
    fn recovery_applies_committed_alter_table_toast() {
        let dir = tempfile::tempdir().unwrap();
        let table_id;
        let toast_table_id;
        let updated_toast = common::ToastOptions {
            mode: common::ToastMode::Aggressive,
            tuple_target: 4096,
            min_value_size: 512,
            compression: common::ToastCompression::Zstd,
            active_dict_id: None,
        };
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, bio text)")
                .unwrap();
            run_checkpoint(&app.components).unwrap();
            let users = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap();
            table_id = users.id;
            toast_table_id = users.toast_table_id;
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 41,
                    kind: WalRecordKind::AlterTableToast {
                        table_id,
                        toast: updated_toast.clone(),
                        toast_table_id,
                    },
                })
                .unwrap();
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 41,
                    kind: WalRecordKind::Commit,
                })
                .unwrap();
            app.components.wal.flush().unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let users = reopened
            .components
            .catalog
            .get_table(table_id)
            .unwrap()
            .expect("users table exists after recovery");
        assert_eq!(users.toast, updated_toast);
        assert_eq!(users.toast_table_id, toast_table_id);
    }

    #[test]
    fn recovery_skips_uncommitted_alter_table_toast() {
        let dir = tempfile::tempdir().unwrap();
        let table_id;
        let original_toast;
        let original_toast_table_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, bio text)")
                .unwrap();
            run_checkpoint(&app.components).unwrap();
            let users = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap();
            table_id = users.id;
            original_toast = users.toast.clone();
            original_toast_table_id = users.toast_table_id;

            let aborted_toast = common::ToastOptions {
                mode: common::ToastMode::Aggressive,
                tuple_target: 4096,
                min_value_size: 512,
                compression: common::ToastCompression::Zstd,
                active_dict_id: None,
            };
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 42,
                    kind: WalRecordKind::AlterTableToast {
                        table_id,
                        toast: aborted_toast,
                        toast_table_id: original_toast_table_id,
                    },
                })
                .unwrap();
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 42,
                    kind: WalRecordKind::Abort,
                })
                .unwrap();

            let in_flight_toast = common::ToastOptions {
                mode: common::ToastMode::Off,
                tuple_target: 3072,
                min_value_size: 2048,
                compression: common::ToastCompression::None,
                active_dict_id: None,
            };
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 43,
                    kind: WalRecordKind::AlterTableToast {
                        table_id,
                        toast: in_flight_toast,
                        toast_table_id: original_toast_table_id,
                    },
                })
                .unwrap();
            app.components.wal.flush().unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let users = reopened
            .components
            .catalog
            .get_table(table_id)
            .unwrap()
            .expect("users table exists after recovery");
        assert_eq!(users.toast, original_toast);
        assert_eq!(users.toast_table_id, original_toast_table_id);
    }

    #[test]
    fn recovery_validates_replayed_toast_active_dictionary() {
        let dir = tempfile::tempdir().unwrap();
        let table_id;
        let toast_table_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, bio text)")
                .unwrap();
            run_checkpoint(&app.components).unwrap();
            let users = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap();
            table_id = users.id;
            toast_table_id = users.toast_table_id;
            let mut toast = users.toast.clone();
            toast.active_dict_id = Some(99);
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 44,
                    kind: WalRecordKind::AlterTableToast {
                        table_id,
                        toast,
                        toast_table_id,
                    },
                })
                .unwrap();
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 44,
                    kind: WalRecordKind::Commit,
                })
                .unwrap();
            app.components.wal.flush().unwrap();
        }

        let err = AppState::open_for_test(dir.path())
            .err()
            .expect("replayed TOAST dictionary reference should be validated");
        assert!(
            err.message.contains("TOAST active dictionary 99"),
            "{}",
            err.message
        );
        assert!(err.message.contains("users"), "{}", err.message);
    }

    #[test]
    fn recovery_preserves_txn_allocator_when_manifest_lsn_has_no_checkpoint_marker() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let expected_next_txn_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            for id in 0..5 {
                app.query_service
                    .execute_sql(&format!(
                        "insert into users (id, name) values ({id}, 'Ada')"
                    ))
                    .unwrap();
            }
            expected_next_txn_id = app.components.next_txn_id.load(Ordering::Acquire);

            // Simulate the checkpoint crash window: heap pages, manifest/control, and
            // CLOG snapshot are durable at checkpoint_lsn, but the Checkpoint marker
            // carrying the transaction-id high-water mark was never appended/flushed.
            app.components.wal.flush().unwrap();
            app.components.buffer_pool.flush_dirty_pages().unwrap();
            app.components.store.sync_all().unwrap();
            let checkpoint_lsn = app.components.wal.flushed_lsn();
            let mut tables: Vec<_> = app
                .components
                .catalog
                .list_tables()
                .unwrap()
                .iter()
                .map(|table| table.id)
                .collect();
            tables.sort_unstable();
            let catalog_bytes =
                catalog::serialize_catalog(&app.components.catalog.snapshot().unwrap()).unwrap();
            app.components
                .control
                .store(checkpoint_lsn, &tables, &catalog_bytes)
                .unwrap();
            app.components.wal.persist_clog(checkpoint_lsn).unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let recovered_next_txn_id = reopened.components.next_txn_id.load(Ordering::Acquire);
        assert!(
            recovered_next_txn_id >= expected_next_txn_id,
            "recovery reused transaction ids after a manifest/CLOG checkpoint without a retained \
             Checkpoint marker: recovered next={recovered_next_txn_id}, expected at least \
             {expected_next_txn_id}"
        );
    }

    #[test]
    fn recovery_reserves_table_id_from_skipped_create_table_record() {
        let dir = tempfile::tempdir().unwrap();
        let skipped_schema = table_schema(41, "aborted_table");
        {
            let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();
            wal.append(WalRecord {
                lsn: 0,
                txn_id: 3,
                kind: WalRecordKind::CreateTable {
                    schema: skipped_schema.clone(),
                },
            })
            .unwrap();
            wal.append(WalRecord {
                lsn: 0,
                txn_id: 3,
                kind: WalRecordKind::Abort,
            })
            .unwrap();
            wal.flush().unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            reopened
                .components
                .catalog
                .get_table_by_name("aborted_table")
                .unwrap(),
            None,
            "an aborted CreateTable record must not install a catalog table"
        );
        assert_eq!(
            reopened
                .components
                .catalog
                .snapshot()
                .unwrap()
                .next_table_id,
            skipped_schema.id + 1,
            "recovery must still burn the skipped table id so its storage files are never reused"
        );
        assert_eq!(
            reopened
                .components
                .catalog
                .snapshot()
                .unwrap()
                .next_storage_id,
            skipped_schema.storage_id + 1,
            "recovery must still burn the skipped table storage id"
        );

        reopened
            .query_service
            .execute_sql("create table live (id integer primary key)")
            .unwrap();
        let live = reopened
            .components
            .catalog
            .get_table_by_name("live")
            .unwrap()
            .unwrap();
        assert_eq!(live.id, skipped_schema.id + 1);
        assert_eq!(live.storage_id, skipped_schema.storage_id + 1);
    }

    #[test]
    fn recovery_reserves_index_id_from_skipped_create_index_record() {
        let dir = tempfile::tempdir().unwrap();
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (1, 'Ada')")
                .unwrap();
            app.query_service
                .execute_sql("insert into users (id, name) values (2, 'Ada')")
                .unwrap();
            let err = app
                .query_service
                .execute_sql("create unique index users_name on users (name)")
                .unwrap_err();
            assert_eq!(err.code, common::SqlState::UniqueViolation);
            assert_eq!(
                app.components
                    .catalog
                    .get_index_by_name("users_name")
                    .unwrap(),
                None
            );
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            reopened
                .components
                .catalog
                .snapshot()
                .unwrap()
                .next_index_id,
            common::PRIMARY_KEY_INDEX_ID + 3,
            "recovery must burn the aborted index id even though the index catalog record is skipped"
        );
        assert_eq!(
            reopened
                .components
                .catalog
                .snapshot()
                .unwrap()
                .next_storage_id,
            5,
            "recovery must burn the aborted index storage id even though the index catalog record is skipped"
        );

        reopened
            .query_service
            .execute_sql("create index users_id on users (id)")
            .unwrap();
        let index = reopened
            .components
            .catalog
            .get_index_by_name("users_id")
            .unwrap()
            .unwrap();
        assert_eq!(index.id, common::PRIMARY_KEY_INDEX_ID + 3);
        assert_eq!(index.storage_id, 5);
    }

    #[test]
    fn recovery_reserves_storage_ids_from_skipped_update_table_schema_record() {
        let dir = tempfile::tempdir().unwrap();
        let skipped_table_storage_id;
        let skipped_index_storage_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            app.query_service
                .execute_sql("create index users_name on users (name)")
                .unwrap();
            run_checkpoint(&app.components).unwrap();

            let next_storage_id = app.components.catalog.snapshot().unwrap().next_storage_id;
            skipped_table_storage_id = next_storage_id;
            skipped_index_storage_id = next_storage_id + 1;

            let mut replayed_table = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap();
            let old_table_storage_id = replayed_table.storage_id;
            replayed_table.storage_id = skipped_table_storage_id;
            replayed_table.schema_version += 1;

            let mut replayed_index = app
                .components
                .catalog
                .get_index_by_name("users_name")
                .unwrap()
                .unwrap();
            let old_index_storage_id = replayed_index.storage_id;
            replayed_index.storage_id = skipped_index_storage_id;

            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 42,
                    kind: WalRecordKind::UpdateTableSchema {
                        schema: replayed_table,
                        indexes: vec![replayed_index],
                    },
                })
                .unwrap();
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id: 42,
                    kind: WalRecordKind::Abort,
                })
                .unwrap();
            app.components.wal.flush().unwrap();

            assert_ne!(old_table_storage_id, skipped_table_storage_id);
            assert_ne!(old_index_storage_id, skipped_index_storage_id);
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let users = reopened
            .components
            .catalog
            .get_table_by_name("users")
            .unwrap()
            .unwrap();
        let users_name = reopened
            .components
            .catalog
            .get_index_by_name("users_name")
            .unwrap()
            .unwrap();
        assert_ne!(users.storage_id, skipped_table_storage_id);
        assert_ne!(users_name.storage_id, skipped_index_storage_id);
        assert_eq!(
            reopened
                .components
                .catalog
                .snapshot()
                .unwrap()
                .next_storage_id,
            skipped_index_storage_id + 1,
            "recovery must burn storage ids carried by skipped schema rewrites"
        );
    }

    #[test]
    fn recovery_reserves_sequence_id_from_skipped_create_sequence_record() {
        let dir = tempfile::tempdir().unwrap();
        let skipped_schema = common::SequenceSchema {
            id: 41,
            schema_id: common::PUBLIC_SCHEMA_ID,
            name: "aborted_seq".to_string(),
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            start: 1,
            cycle: false,
            owned: false,
            last_value: 1,
            is_called: false,
        };
        {
            let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();
            wal.append(WalRecord {
                lsn: 0,
                txn_id: 3,
                kind: WalRecordKind::CreateSequence {
                    schema: skipped_schema.clone(),
                },
            })
            .unwrap();
            wal.append(WalRecord {
                lsn: 0,
                txn_id: 3,
                kind: WalRecordKind::Abort,
            })
            .unwrap();
            wal.flush().unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            reopened
                .components
                .catalog
                .get_sequence_by_name("aborted_seq")
                .unwrap(),
            None,
            "an aborted CreateSequence record must not install a catalog sequence"
        );
        assert_eq!(
            reopened
                .components
                .catalog
                .snapshot()
                .unwrap()
                .next_sequence_id,
            skipped_schema.id + 1,
            "recovery must still burn the skipped sequence id"
        );

        reopened
            .query_service
            .execute_sql("create sequence live_seq")
            .unwrap();
        let live = reopened
            .components
            .catalog
            .get_sequence_by_name("live_seq")
            .unwrap()
            .unwrap();
        assert_eq!(live.id, skipped_schema.id + 1);
    }

    #[test]
    fn recovery_replays_create_index_and_rebuilds_the_secondary_tree() {
        use common::{Key, KeyRange, StatementContext, Value};

        let dir = tempfile::tempdir().unwrap();
        let table_id;
        let index_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            for sql in [
                "create table users (id integer primary key, name text)",
                "insert into users (id, name) values (1, 'Ada')",
                "insert into users (id, name) values (2, 'Grace')",
                "create index users_name on users (name)",
            ] {
                app.query_service.execute_sql(sql).unwrap();
            }
            // No checkpoint happened (few commits), so recovery must replay the
            // CreateIndex record rather than load it from the snapshot.
            let index = app
                .components
                .catalog
                .get_index_by_name("users_name")
                .unwrap()
                .unwrap();
            table_id = index.table;
            index_id = index.id;
        }

        // Reopen: recovery replays the CreateIndex record into both catalog and
        // storage and rebuilds the secondary tree from its full-page images.
        let reopened = AppState::open_for_test(dir.path()).unwrap();
        let comps = &reopened.components;
        assert!(
            comps
                .catalog
                .get_index_by_name("users_name")
                .unwrap()
                .is_some()
        );

        // Scan the rebuilt secondary tree directly to prove its pages recovered.
        let ctx = StatementContext::new(0);
        let mut iter = comps
            .storage
            .index_scan(
                &ctx,
                table_id,
                index_id,
                &KeyRange::Exact(Key(vec![Value::Text("Ada".to_string())])),
            )
            .unwrap();
        let row = iter
            .next()
            .unwrap()
            .expect("Ada should be found through the recovered index");
        assert_eq!(row.row.values[0], Value::Integer(1));
        assert!(iter.next().unwrap().is_none());
    }

    /// FATAL-B regression: a transaction that crashed in flight (its pages were
    /// stolen to disk, but it never got a durable `Commit` or `Abort`) must NEVER
    /// become visible. Today a later full `VACUUM` followed by a checkpoint floats
    /// the implicit-committed floor *past* the unresolved xid — because nothing
    /// resolves a crashed in-flight transaction to `Aborted` and `vacuum_heap` only
    /// reclaims recorded-aborted creators — so its never-committed rows resurrect as
    /// committed data. See `clog.rs::live_snapshot` (floor pinned only by recorded
    /// aborts) and `query/vacuum.rs::full_vacuum_pass` (floor = `next_txn_id`).
    ///
    /// Fixed by `resolve_in_flight_as_aborted` at recovery (`open_app`): crashed
    /// in-flight writers are marked `Aborted` in the CLOG (persisted via `clog.dat`
    /// by the recovery checkpoint), so VACUUM reclaims their tuples before the floor
    /// crosses them and they never read as committed.
    #[test]
    fn crashed_in_flight_transaction_is_not_resurrected_by_vacuum() {
        use super::open_app;
        use crate::checkpoint::run_checkpoint;
        use common::{IsolationLevel, QueryCancel};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        // Tiny buffer pool forces eviction (steal) of the uncommitted transaction's
        // dirty pages to disk; `auto_vacuum_dead_rows: 0` makes the explicit VACUUM
        // the only thing that advances the vacuum floor, so the two phases below are
        // cleanly separated.
        let config = || crate::config::Config {
            data_dir: dir.path().to_path_buf(),
            buffer_pool_frames: 8,
            auto_vacuum_dead_rows: 0,
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..crate::config::Config::default()
        };

        // Lifetime 1: create + checkpoint a clean baseline, then run an explicit
        // transaction that inserts many rows and NEVER commits. The tiny pool steals
        // its uncommitted heap/index pages to disk (their WAL records fsynced by the
        // steal path). We then "crash" by leaking the open transaction (no Drop-time
        // cleanup) and dropping the app with no commit, no rollback, and no WAL flush
        // — a faithful power-loss mid-transaction.
        {
            let app = open_app(config()).unwrap();
            app.query_service
                .execute_sql("create table ghosts (id integer primary key, payload text)")
                .unwrap();
            run_checkpoint(&app.components).unwrap();

            let cancel = Arc::new(QueryCancel::new());
            let (mut slot, mut iso, res) = app.query_service.execute_simple(
                "begin",
                None,
                IsolationLevel::RepeatableRead,
                &cancel,
            );
            res.unwrap();
            let payload = "x".repeat(300);
            for id in 0..1000 {
                let sql = format!("insert into ghosts (id, payload) values ({id}, '{payload}')");
                let (next, next_iso, res) =
                    app.query_service.execute_simple(&sql, slot, iso, &cancel);
                res.unwrap();
                slot = next;
                iso = next_iso;
            }
            // Simulate the process vanishing: never run the transaction's destructor
            // and never flush. No `Commit`/`Abort` reaches the WAL.
            std::mem::forget(slot);
        }

        // Lifetime 2: recover. The in-flight txn has no Commit/Abort record, so it is
        // InProgress and its replayed rows MUST be invisible. (Sanity check — this
        // holds today.)
        let app = open_app(config()).unwrap();
        let after_crash = app
            .query_service
            .execute_sql("select id from ghosts")
            .unwrap()
            .row_count();
        assert_eq!(
            after_crash, 0,
            "sanity: a crashed in-flight transaction's rows must be invisible right after recovery"
        );

        // Full VACUUM advances the vacuum floor to next_txn_id (above the ghost xid);
        // the checkpoint's `persist_clog` floats the implicit-committed floor up to it.
        // Nothing ever resolved the ghost to Aborted, so it must STILL be invisible.
        app.query_service.execute_sql("vacuum").unwrap();
        run_checkpoint(&app.components).unwrap();

        let after_vacuum = app
            .query_service
            .execute_sql("select id from ghosts")
            .unwrap()
            .row_count();
        assert_eq!(
            after_vacuum, 0,
            "FATAL-B: VACUUM + checkpoint resurrected {after_vacuum} never-committed rows as \
             visible (the implicit-committed floor floated past an unresolved in-flight xid)"
        );
    }

    /// FATAL-B (subtransaction variant): rows written by a crashed transaction's
    /// SAVEPOINT subtransaction are stamped with the *subxid*. The recovery
    /// resolution must abort the subxid too (it must appear in the replayed redo
    /// records' txn ids), or those rows resurrect just like top-level writes.
    #[test]
    fn crashed_subtransaction_writes_are_not_resurrected() {
        use super::open_app;
        use crate::checkpoint::run_checkpoint;
        use common::{IsolationLevel, QueryCancel};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let config = || crate::config::Config {
            data_dir: dir.path().to_path_buf(),
            buffer_pool_frames: 8,
            auto_vacuum_dead_rows: 0,
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..crate::config::Config::default()
        };

        {
            let app = open_app(config()).unwrap();
            app.query_service
                .execute_sql("create table ghosts (id integer primary key, payload text)")
                .unwrap();
            run_checkpoint(&app.components).unwrap();

            let cancel = Arc::new(QueryCancel::new());
            let payload = "x".repeat(300);
            let (mut slot, mut iso, res) = app.query_service.execute_simple(
                "begin",
                None,
                IsolationLevel::RepeatableRead,
                &cancel,
            );
            res.unwrap();
            // One row under the top-level xid, then a savepoint subtransaction that
            // inserts the flood — those rows are stamped with the subxid, and the flood
            // forces their pages to be stolen to disk.
            for stmt in [
                format!("insert into ghosts (id, payload) values (0, '{payload}')"),
                "savepoint s1".to_string(),
            ] {
                let (s, i, res) = app.query_service.execute_simple(&stmt, slot, iso, &cancel);
                res.unwrap();
                slot = s;
                iso = i;
            }
            for id in 1..1000 {
                let sql = format!("insert into ghosts (id, payload) values ({id}, '{payload}')");
                let (s, i, res) = app.query_service.execute_simple(&sql, slot, iso, &cancel);
                res.unwrap();
                slot = s;
                iso = i;
            }
            std::mem::forget(slot);
        }

        let app = open_app(config()).unwrap();
        assert_eq!(
            app.query_service
                .execute_sql("select id from ghosts")
                .unwrap()
                .row_count(),
            0,
            "sanity: crashed subtransaction rows must be invisible right after recovery"
        );

        app.query_service.execute_sql("vacuum").unwrap();
        run_checkpoint(&app.components).unwrap();

        let after_vacuum = app
            .query_service
            .execute_sql("select id from ghosts")
            .unwrap()
            .row_count();
        assert_eq!(
            after_vacuum, 0,
            "FATAL-B (subxid): VACUUM resurrected {after_vacuum} rows from a crashed \
             subtransaction — the subxid creator was not resolved to Aborted at recovery"
        );
    }

    /// FATAL-B (delete face): a crashed in-flight `DELETE` stamps `xmax` on committed
    /// rows. If the ghost later reads as committed, that `xmax` becomes a committed
    /// delete and the rows wrongly disappear. Resolving the ghost to Aborted keeps the
    /// committed rows alive.
    #[test]
    fn crashed_in_flight_delete_does_not_drop_committed_rows() {
        use super::open_app;
        use crate::checkpoint::run_checkpoint;
        use common::{IsolationLevel, QueryCancel};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let config = || crate::config::Config {
            data_dir: dir.path().to_path_buf(),
            buffer_pool_frames: 8,
            auto_vacuum_dead_rows: 0,
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..crate::config::Config::default()
        };

        let base_rows: u32 = 200;
        {
            let app = open_app(config()).unwrap();
            app.query_service
                .execute_sql("create table t (id integer primary key, payload text)")
                .unwrap();
            let payload = "x".repeat(300);
            for id in 0..base_rows {
                app.query_service
                    .execute_sql(&format!(
                        "insert into t (id, payload) values ({id}, '{payload}')"
                    ))
                    .unwrap();
            }
            run_checkpoint(&app.components).unwrap();

            let cancel = Arc::new(QueryCancel::new());
            let (mut slot, mut iso, res) = app.query_service.execute_simple(
                "begin",
                None,
                IsolationLevel::RepeatableRead,
                &cancel,
            );
            res.unwrap();
            // Delete every committed row (stamps xmax in place), then flood inserts so
            // the xmax-stamped base pages are stolen to disk before the crash.
            let (s, i, res) = app
                .query_service
                .execute_simple("delete from t", slot, iso, &cancel);
            res.unwrap();
            slot = s;
            iso = i;
            for id in base_rows..(base_rows + 1000) {
                let sql = format!("insert into t (id, payload) values ({id}, '{payload}')");
                let (s, i, res) = app.query_service.execute_simple(&sql, slot, iso, &cancel);
                res.unwrap();
                slot = s;
                iso = i;
            }
            std::mem::forget(slot);
        }

        let app = open_app(config()).unwrap();
        assert_eq!(
            app.query_service
                .execute_sql("select id from t")
                .unwrap()
                .row_count() as u32,
            base_rows,
            "sanity: committed rows survive a crashed in-flight DELETE right after recovery"
        );

        app.query_service.execute_sql("vacuum").unwrap();
        run_checkpoint(&app.components).unwrap();

        let surviving = app
            .query_service
            .execute_sql("select id from t")
            .unwrap()
            .row_count() as u32;
        assert_eq!(
            surviving, base_rows,
            "FATAL-B (delete): VACUUM + checkpoint left {surviving} of {base_rows} committed rows \
             — a crashed in-flight DELETE's xmax read as a committed delete"
        );
    }

    /// The recovery resolution must be durable: after the recovery checkpoint persists
    /// the aborts to `clog.dat` and truncates the WAL, a SECOND restart (which has no
    /// redo records left for the ghost) plus a VACUUM must still not resurrect it.
    #[test]
    fn resolved_in_flight_abort_survives_restart_and_vacuum() {
        use super::open_app;
        use crate::checkpoint::run_checkpoint;
        use common::{IsolationLevel, QueryCancel};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let config = || crate::config::Config {
            data_dir: dir.path().to_path_buf(),
            buffer_pool_frames: 8,
            auto_vacuum_dead_rows: 0,
            checkpoint_every_n_commits: u64::MAX,
            checkpoint_wal_bytes: u64::MAX,
            ..crate::config::Config::default()
        };

        {
            let app = open_app(config()).unwrap();
            app.query_service
                .execute_sql("create table ghosts (id integer primary key, payload text)")
                .unwrap();
            run_checkpoint(&app.components).unwrap();

            let cancel = Arc::new(QueryCancel::new());
            let payload = "x".repeat(300);
            let (mut slot, mut iso, res) = app.query_service.execute_simple(
                "begin",
                None,
                IsolationLevel::RepeatableRead,
                &cancel,
            );
            res.unwrap();
            for id in 0..1000 {
                let sql = format!("insert into ghosts (id, payload) values ({id}, '{payload}')");
                let (s, i, res) = app.query_service.execute_simple(&sql, slot, iso, &cancel);
                res.unwrap();
                slot = s;
                iso = i;
            }
            std::mem::forget(slot);
        }

        // First restart resolves the ghost to Aborted and the recovery checkpoint
        // persists it to clog.dat (then truncates the WAL).
        drop(open_app(config()).unwrap());

        // Second restart has NO redo records for the ghost; it must rely on the durable
        // clog.dat abort. A VACUUM here must reclaim, not resurrect.
        let app = open_app(config()).unwrap();
        app.query_service.execute_sql("vacuum").unwrap();
        run_checkpoint(&app.components).unwrap();

        let after = app
            .query_service
            .execute_sql("select id from ghosts")
            .unwrap()
            .row_count();
        assert_eq!(
            after, 0,
            "FATAL-B (durability): {after} ghost rows resurrected after a second restart — the \
             recovery abort was not persisted to clog.dat"
        );
    }

    #[test]
    fn next_txn_id_rejects_retained_u64_max_txn_id() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();
        wal.append(WalRecord {
            lsn: 0,
            txn_id: u64::MAX,
            kind: WalRecordKind::Commit,
        })
        .unwrap();
        wal.flush().unwrap();

        let err = super::next_txn_id(&wal).unwrap_err();
        assert!(err.message.contains("transaction id overflow"));
    }

    #[test]
    fn next_txn_id_accounts_for_committed_subxids_in_payload() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWalManager::open(dir.path().join("wal.dat")).unwrap();
        // Top txn 5 commits with a released subxid 9 that did no writes, so 9
        // appears only in the commit payload, not a record header. The allocator
        // must resume at 10 (above 9), or it would reissue the committed subxid.
        wal.append(WalRecord {
            lsn: 0,
            txn_id: 5,
            kind: WalRecordKind::CommitWithSubxids { subxids: vec![9] },
        })
        .unwrap();
        wal.flush().unwrap();

        assert_eq!(super::next_txn_id(&wal).unwrap(), 10);
    }

    /// Statistics matching the `users (id integer primary key, name text)`
    /// shape the statistics-replay tests create via SQL.
    fn users_statistics() -> common::TableStatistics {
        common::TableStatistics {
            row_count: 500,
            page_count: 5,
            columns: std::collections::BTreeMap::from([(
                1,
                common::ColumnStatistics {
                    null_frac: common::OrderedF64::new(0.2),
                    avg_width: 16,
                    n_distinct: common::NDistinct::Count(7),
                    most_common: vec![(
                        common::Value::Text("carol".to_string()),
                        common::OrderedF64::new(0.4),
                    )],
                    histogram_bounds: Vec::new(),
                },
            )]),
        }
    }

    fn append_statistics_record(
        app: &AppState,
        txn_id: u64,
        table_id: common::TableId,
        outcome: Option<WalRecordKind>,
    ) {
        app.components
            .wal
            .append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::UpdateTableStatistics {
                    table_id,
                    statistics: users_statistics(),
                },
            })
            .unwrap();
        if let Some(kind) = outcome {
            app.components
                .wal
                .append(WalRecord {
                    lsn: 0,
                    txn_id,
                    kind,
                })
                .unwrap();
        }
        app.components.wal.flush().unwrap();
    }

    #[test]
    fn recovery_applies_committed_statistics_record() {
        let dir = tempfile::tempdir().unwrap();
        let table_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            table_id = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap()
                .id;
            append_statistics_record(&app, 42, table_id, Some(WalRecordKind::Commit));
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            reopened
                .components
                .catalog
                .get_table_statistics(table_id)
                .unwrap(),
            Some(users_statistics())
        );
    }

    #[test]
    fn recovery_skips_aborted_and_in_flight_statistics_records() {
        let dir = tempfile::tempdir().unwrap();
        let table_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            table_id = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap()
                .id;
            append_statistics_record(&app, 42, table_id, Some(WalRecordKind::Abort));
            append_statistics_record(&app, 43, table_id, None); // in-flight: no outcome
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            reopened
                .components
                .catalog
                .get_table_statistics(table_id)
                .unwrap(),
            None
        );
    }

    #[test]
    fn recovery_skips_committed_statistics_for_missing_table_without_error() {
        let dir = tempfile::tempdir().unwrap();
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            // A committed statistics record whose table id never resolves:
            // advisory data must never fail recovery.
            append_statistics_record(&app, 42, 9999, Some(WalRecordKind::Commit));
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            reopened
                .components
                .catalog
                .get_table_statistics(9999)
                .unwrap(),
            None
        );
    }

    #[test]
    fn statistics_survive_checkpoint_and_wal_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let table_id;
        {
            let app = AppState::open_for_test(dir.path()).unwrap();
            app.query_service
                .execute_sql("create table users (id integer primary key, name text)")
                .unwrap();
            table_id = app
                .components
                .catalog
                .get_table_by_name("users")
                .unwrap()
                .unwrap()
                .id;
            append_statistics_record(&app, 42, table_id, Some(WalRecordKind::Commit));
            // Install in the live catalog too (normal execution does both),
            // then checkpoint: the manifest must carry the statistics after
            // the WAL below the checkpoint is truncated.
            app.components
                .catalog
                .set_table_statistics(table_id, users_statistics())
                .unwrap();
            run_checkpoint(&app.components).unwrap();
        }

        let reopened = AppState::open_for_test(dir.path()).unwrap();
        assert_eq!(
            reopened
                .components
                .catalog
                .get_table_statistics(table_id)
                .unwrap(),
            Some(users_statistics())
        );
    }
}
