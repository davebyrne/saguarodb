use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

use catalog::serialize_catalog;
use common::{Result, TableId};
use snapshot::SnapshotPage;
use wal::{WalRecord, WalRecordKind};

use crate::app::ServerComponents;

pub struct CheckpointState {
    pub last_checkpoint_lsn: AtomicU64,
    pub commits_since_checkpoint: AtomicU64,
}

pub fn run_checkpoint(_components: &ServerComponents) -> Result<()> {
    let components = _components;
    let _guard = components.concurrency.begin_write()?;
    let checkpoint_lsn = components.wal.flushed_lsn();
    let live_tables = components.catalog.list_tables()?;
    let live_table_ids: BTreeSet<TableId> = live_tables.iter().map(|table| table.id).collect();
    let dirty_by_table = dirty_pages_by_table(components, &live_table_ids)?;

    let mut writer = components.snapshot_manager.begin_snapshot()?;
    for table in live_tables {
        let mut pages = BTreeMap::new();
        for page in components.snapshot_manager.current_table_pages(table.id)? {
            pages.insert(page.page_num, page.data);
        }
        if let Some(dirty_pages) = dirty_by_table.get(&table.id) {
            for (page_num, data) in dirty_pages {
                pages.insert(*page_num, data.clone());
            }
        }
        let pages = pages
            .into_iter()
            .map(|(page_num, data)| SnapshotPage { page_num, data })
            .collect::<Vec<_>>();
        writer.write_table(table.id, &pages)?;
    }

    let catalog_bytes = serialize_catalog(&components.catalog.snapshot()?)?;
    writer.write_catalog(&catalog_bytes)?;
    let metadata = components
        .snapshot_manager
        .commit_snapshot(writer, checkpoint_lsn)?;
    components.buffer_pool.mark_all_clean()?;
    components.wal.append(WalRecord {
        lsn: 0,
        txn_id: 0,
        kind: WalRecordKind::Checkpoint {
            generation: metadata.generation,
            checkpoint_lsn,
        },
    })?;
    components.wal.flush()?;
    components.wal.truncate_before(checkpoint_lsn)?;
    components.snapshot_manager.cleanup_old_snapshots()?;

    components
        .checkpoint
        .last_checkpoint_lsn
        .store(checkpoint_lsn, Ordering::Release);
    components
        .checkpoint
        .commits_since_checkpoint
        .store(0, Ordering::Release);
    Ok(())
}

pub fn record_commit_and_maybe_checkpoint(_components: &ServerComponents) -> Result<()> {
    let components = _components;
    let commits = components
        .checkpoint
        .commits_since_checkpoint
        .fetch_add(1, Ordering::AcqRel)
        + 1;
    let last_checkpoint_lsn = components
        .checkpoint
        .last_checkpoint_lsn
        .load(Ordering::Acquire);
    if commits >= components.config.checkpoint_every_n_commits
        || components.wal.bytes_after(last_checkpoint_lsn)?
            >= components.config.checkpoint_wal_bytes
    {
        run_checkpoint(components)?;
    }
    Ok(())
}

fn dirty_pages_by_table(
    components: &ServerComponents,
    live_table_ids: &BTreeSet<TableId>,
) -> Result<BTreeMap<TableId, BTreeMap<u32, buffer::PageData>>> {
    let mut dirty_by_table = BTreeMap::<TableId, BTreeMap<u32, buffer::PageData>>::new();
    for page in components.buffer_pool.iter_pages()? {
        if page.is_dirty && live_table_ids.contains(&page.file_id) {
            dirty_by_table
                .entry(page.file_id)
                .or_default()
                .insert(page.page_num, page.data);
        }
    }
    Ok(dirty_by_table)
}
