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
    // Take the EXCLUSIVE checkpoint guard (E2b lock inversion, `docs/specs/mvcc.md`
    // §7.1 Stage 2, §10 E2b). Under concurrent writers (each holding a SHARED writer
    // guard) this blocks until every in-flight writer has drained, then holds off
    // any new writer until the checkpoint returns — so the checkpoint body runs with
    // **no in-flight writer**, exactly as under Stage 1's single exclusive writer.
    // That preserves the Milestone-D recovery / conservative-truncation invariant
    // (no in-flight writer is ever below the truncation boundary — §5.4, §8) without
    // a fuzzy checkpoint, and keeps the `txn_high_water` stamping below correct (no
    // concurrent writer advances `next_txn_id` while we hold the exclusive guard).
    let _guard = components.concurrency.begin_checkpoint()?;

    // The WAL must be durable before any page it describes is written to the heap.
    // With the relaxed flush gate (`docs/specs/mvcc.md` §8, Milestone D1) this
    // spills ALL WAL-durable dirty pages — committed, aborted, and (under Stage-2)
    // in-flight alike — to the heap; the CLOG hides the non-committed tuples and
    // VACUUM (Milestone F) reclaims them. fsync ordering is preserved: WAL flush →
    // flush dirty pages → store fsync → control record → Checkpoint marker → WAL
    // truncation → mark clean.
    components.wal.flush()?;
    components.buffer_pool.flush_dirty_pages()?;
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
