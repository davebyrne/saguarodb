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

    // Auto-prune (Milestone F4b, `docs/specs/mvcc.md` §9/§10 F): when enough dead
    // versions have accumulated since the last auto-prune, fold a VACUUM pass over
    // every user table into THIS checkpoint, under the exclusive guard we already
    // hold. This bounds heap + index space under sustained DELETE/UPDATE churn with
    // no operator `VACUUM`. It runs HERE — at the very start of the checkpoint body,
    // BEFORE `flush_dirty_pages` — so the pages the vacuum dirties and the full-page
    // images it logs are flushed and made durable by THIS checkpoint, and its WAL
    // records precede the WAL truncation below.
    //
    // **No data loss (same safety as on-demand VACUUM, F4a):** the horizon is
    // captured by `gc_horizon()` HERE, *under* the exclusive guard. Under that guard
    // no writer runs, so no committed-deleter appears mid-pass, and the horizon is
    // the minimum `xmin` advertised by any live snapshot — INCLUDING lock-free
    // readers (which advertise their `xmin`). Every reclaimed version has
    // `xmax < horizon`, i.e. its delete committed before every live snapshot's `xmin`,
    // so no current snapshot can see it live. Capturing the horizon under the guard is
    // load-bearing: a concurrent writer/commit cannot then advance it. The auto-prune
    // reclaims only the orchestration's reclaimable versions (F4a) — never a version
    // a live snapshot needs.
    //
    // **Recovery / ordering invariants are intact:** the vacuum appends its
    // `FullPageImage` records BEFORE the `wal.flush()` below, so they are flushed by
    // this checkpoint and sit at LSNs *below* `checkpoint_lsn` (the flushed LSN
    // captured after that flush). The matching dirtied pages are written to the heap
    // by `flush_dirty_pages()` and fsynced by `store.sync_all()` BEFORE the control
    // record (the commit point) — exactly the existing WAL-before-page ordering. So
    // once this checkpoint commits, the vacuum's effects are durably in the heap and
    // the redo boundary advances past them; truncating the vacuum's now-redundant WAL
    // records below `checkpoint_lsn` is therefore safe (their effect is already on
    // disk). A crash BEFORE the control record falls back to the previous redo
    // boundary, where the prior cycle's images repair any torn write and the vacuum
    // simply did not happen. The conservative-truncation guard is unchanged (F4c
    // relaxes it later).
    let threshold = components.config.auto_vacuum_dead_rows;
    if threshold != 0 && components.dead_rows_since_vacuum.load(Ordering::Acquire) >= threshold {
        let horizon = components.gc_horizon();
        // Full pass over every user table, AND advance the vacuum floor (F4c,
        // `docs/specs/mvcc.md` §9): this captures `B = next_txn_id` under the guard,
        // reclaims every aborted-creator tuple below `B`, then sets the floor to `B`.
        // It runs BEFORE `flush_dirty_pages`/`store.sync_all`, so this pass's
        // reclamation is fsynced by THIS checkpoint *before* the `truncate_before`
        // below consults the floor — so an `Abort` is only dropped after its reclaimed
        // tuples are durable (the F4c durability-ordering invariant).
        crate::query::full_vacuum_pass(components, horizon)?;
        // Reset the accumulator: churn from here on counts toward the next auto-prune.
        components
            .dead_rows_since_vacuum
            .store(0, Ordering::Release);
    }

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
