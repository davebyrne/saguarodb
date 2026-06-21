use std::sync::atomic::{AtomicU64, Ordering};

use catalog::serialize_catalog;
use common::{Result, TableId};
use wal::{WalRecord, WalRecordKind};

use crate::app::ServerComponents;

pub struct CheckpointState {
    pub last_checkpoint_lsn: AtomicU64,
    pub commits_since_checkpoint: AtomicU64,
    /// Count of completed checkpoints (observability / tests).
    pub checkpoints: AtomicU64,
}

/// Checkpoint by flushing dirty pages in place to the heap and advancing the
/// redo boundary. Cost is O(pages changed), not O(database size).
///
/// Ordering is durability-critical: heap pages are fsynced before the control
/// record (the commit point) is written, which happens before the WAL prefix is
/// truncated. A crash before the control record falls back to the previous redo
/// boundary, where this cycle's full-page images repair any torn heap writes.
pub fn run_checkpoint(components: &ServerComponents) -> Result<()> {
    let _guard = components.concurrency.begin_write()?;

    // The WAL must be durable before any page it describes is written to the heap.
    components.wal.flush()?;
    components.buffer_pool.flush_committed_pages()?;
    components.store.sync_all()?;

    let checkpoint_lsn = components.wal.flushed_lsn();
    let mut tables: Vec<TableId> = components
        .catalog
        .list_tables()?
        .iter()
        .map(|table| table.id)
        .collect();
    tables.sort_unstable();
    let catalog_bytes = serialize_catalog(&components.catalog.snapshot()?)?;
    components
        .control
        .store(checkpoint_lsn, &tables, &catalog_bytes)?;

    // The Checkpoint marker is optional metadata; recovery uses the control
    // record's LSN. Truncating below it reclaims the now-redundant WAL prefix.
    //
    // Stamp the marker with the current transaction-id high-water mark (the
    // highest id allocated so far). The marker survives truncation (its LSN is the
    // retained boundary), so on recovery `next_txn_id`'s max-scan recovers the
    // allocator boundary even when every data record below the checkpoint was
    // truncated. Without this the allocator would restart low and reissue ids that
    // already stamped committed tuples, corrupting visibility. A checkpoint holds
    // the write guard, so no concurrent writer advances the allocator here.
    let txn_high_water = components
        .next_txn_id
        .load(Ordering::Acquire)
        .saturating_sub(1);
    components.wal.append(WalRecord {
        lsn: 0,
        txn_id: txn_high_water,
        kind: WalRecordKind::Checkpoint {
            redo_lsn: checkpoint_lsn,
        },
    })?;
    components.wal.flush()?;
    components.wal.truncate_before(checkpoint_lsn)?;

    components.buffer_pool.mark_all_clean()?;

    components
        .checkpoint
        .last_checkpoint_lsn
        .store(checkpoint_lsn, Ordering::Release);
    components
        .checkpoint
        .commits_since_checkpoint
        .store(0, Ordering::Release);
    components
        .checkpoint
        .checkpoints
        .fetch_add(1, Ordering::AcqRel);
    Ok(())
}

pub fn record_commit_and_maybe_checkpoint(components: &ServerComponents) -> Result<()> {
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
