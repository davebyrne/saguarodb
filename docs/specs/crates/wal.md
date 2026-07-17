# `wal` Crate Specification

**Date:** 2026-07-17
**Status:** Living crate contract

## Purpose and Format

`wal` owns logical/physiological redo, transaction status, positioned replay,
segmented retention, and atomic replay-floor advancement. Segmented WAL format
version 3 is current; older formats are rejected without migration. Segments
remain fixed 16 MiB payload streams and `wal.meta` retains the durable end and
replay floor with atomic replacement.

## Positions and API

```rust
pub struct WalPosition {
    pub replay_from: Lsn, // boundary immediately before the encoded record
    pub record_lsn: Lsn,  // boundary immediately after it; stored in record/PageLSN
}

pub struct WalEntry {
    pub replay_from: Lsn,
    pub record: WalRecord,
}

pub trait WalManager: Send + Sync + TxnStatusView {
    fn append_positioned(&self, record: WalRecord) -> Result<WalPosition>;
    fn append(&self, record: WalRecord) -> Result<Lsn>; // default: record_lsn
    fn written_lsn(&self) -> Result<Lsn>;
    fn flushed_lsn(&self) -> Lsn;
    fn flush(&self) -> Result<Lsn>; // durable complete-stream boundary
    fn replay_entries_from(
        &self,
        replay_from: Lsn,
    ) -> Result<Box<dyn Iterator<Item = Result<WalEntry>>>>;
    fn retained_range(&self) -> Result<(Lsn, Lsn)>;
    fn checkpoint_clog(
        &self,
        proposed_replay_floor: Lsn,
        captured_active: &[TxnId],
        allocation_boundary: TxnId,
    ) -> Result<()>;
    fn recycle_through(&self, replay_floor: Lsn) -> Result<()>;
}
```

A replay beginning at boundary `B` includes the record whose
`WalEntry.replay_from == B`. `written_lsn` is the complete current in-process
stream boundary; `flushed_lsn` is the durable boundary. Encoded record headers
continue storing the end LSN and transaction ID. Checksums, bounded decoding,
crash-tail repair, and exact segment-boundary behavior remain mandatory.

## Records

All existing page, generic `CatalogChange`, dictionary, sequence, commit/abort,
and subtransaction records remain. The checkpoint marker is:

```rust
Checkpoint {
    checkpoint_end_lsn: Lsn,
    page_redo_lsn: Lsn,
    catalog_redo_lsn: Lsn,
}
```

Its header transaction ID is the captured allocator high-water mark.

## Commit and Page Durability

Commit appends its marker and `flush` makes the complete stream durable before
the CLOG status becomes visible as committed. A page may reach its home file only
after WAL is durable through its latest PageLSN. Page writers use the positioned
append boundary as the DPT `rec_lsn`, never the stored end LSN.

## CLOG Snapshot v3

`clog.dat` is a CRC-checked, atomically replaced CLOG format v3 snapshot:

```text
clog_lsn
authorized_replay_floor
committed_floor
vacuum_floor
committed[]
aborted[]
in_progress[]
```

All lists are bounded to 1,000,000 entries, strictly sorted, duplicate-free,
mutually disjoint, and contain no ID below `committed_floor`.
`authorized_replay_floor <= clog_lsn`. Captured active IDs, the transaction
allocation boundary, unreclaimed aborts, and existing in-progress statuses pin
the implicit committed floor. In-progress statuses are durable so an active
transaction may span a fuzzy checkpoint without becoming implicitly committed.
If an active writer pins the floor and the explicit status window reaches the
750,000-entry pressure threshold, WAL rejects the first record of a new writer
with `ProgramLimitExceeded` while continuing to accept commit/abort settlement
for existing writers. This bounded backpressure prevents the durable lists from
reaching their one-million-entry cap; writes resume after the pin settles and a
checkpoint/maintenance pass advances the floor.

`checkpoint_clog` first flushes complete WAL records and applies pending commit
statuses, captures the immutable snapshot under the WAL mutex, releases that
mutex while writing/fsyncing/renaming `clog.dat`, then re-locks to prune only
statuses below the durable committed floor and authorize the proposed WAL replay
floor. A failed replacement changes neither in-memory pruning nor recycling
authorization.

## Recycling and Recovery

`recycle_through` atomically replaces `wal.meta` before unlinking wholly obsolete
segments and never rewrites retained segments. It refuses an interior record
boundary or a boundary not authorized by the durable CLOG. Failure after a
durable metadata replacement is an unknown/fatal outcome, while extra retained
segments are harmless.

Open loads `clog.dat`, folds WAL status markers after `clog_lsn`, and records
transaction-bearing non-status records as in-progress until settled. Recovery
then resolves every remaining in-progress status to aborted, including xids whose
physical records fall below the current physical redo boundary. Allocator scans
cover every retained record and checkpoint high-water marker.

## Invariants

- Normal storage operations append WAL; recovery operations do not.
- `replay_from` values are exact record-start boundaries.
- WAL recycling never passes the minimum physical/catalog redo boundary and is
  never authorized before the matching CLOG snapshot is durable.
- Transaction visibility below recycled WAL remains fully determined by CLOG v3.
