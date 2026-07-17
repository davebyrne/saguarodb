# `control` Crate Specification

**Date:** 2026-07-17
**Status:** Living crate contract

## Purpose

`control` owns the atomic manifest that commits a fuzzy checkpoint. Table pages
remain in mutable heap/index files; the manifest records independent physical
and catalog redo boundaries plus the dirty-page table (DPT).

## Durable Record

```rust
pub struct ControlData {
    pub checkpoint_end_lsn: Lsn,
    pub page_redo_lsn: Lsn,
    pub catalog_redo_lsn: Lsn,
    pub dirty_pages: Vec<DirtyPageEntry>,
    pub tables: Vec<TableId>,
    pub catalog: Vec<u8>,
    pub page_size: u32,
}
```

The envelope is magic `SGMF`, manifest format version 6, a little-endian payload
length, a CRC32 of the JSON payload, and the payload. Older versions are rejected;
there is no migration reader. The existing 272 MiB manifest, 64 MiB catalog, and
65,536-table bounds remain. The DPT is capped at 1,000,000 entries and decoding
uses bounded, fallible allocation.

Validation requires:

- sorted, duplicate-free table IDs;
- DPT ordering by `(file_id, page_num)` with no duplicate page key;
- every DPT `rec_lsn <= checkpoint_end_lsn`;
- an empty DPT has `page_redo_lsn == checkpoint_end_lsn`;
- a nonempty DPT has `page_redo_lsn == min(rec_lsn)`;
- `catalog_redo_lsn <= checkpoint_end_lsn`.

The overall retained replay floor is
`min(page_redo_lsn, catalog_redo_lsn)`. `page_size` must match the binary's
configured page size after envelope decoding.

## Public API

```rust
pub trait ControlStore: Send + Sync {
    fn load(&self) -> Result<Option<ControlData>>;
    fn store(&self, control: ControlData) -> Result<()>;
}
```

Accepting the complete value prevents callers from supplying mutually
inconsistent boundaries.

## Commit and Crash Semantics

`store` writes and fsyncs `manifest.dat.tmp`, renames it over `manifest.dat`, and
fsyncs the data directory. The rename plus directory fsync is the checkpoint
commit point. The server persists the matching CLOG snapshot before `store` and
advances/recycles segmented WAL only after it succeeds.

- Before replacement, the previous manifest and WAL replay floor remain
  authoritative. A newly replaced CLOG may be loaded, but WAL retained from that
  old floor includes the VACUUM FPIs that justify any newly implicit transaction
  outcome, so replay cannot resurrect reclaimed aborted data.
- After replacement but before WAL recycling, the new manifest/CLOG are
  authoritative and retaining extra WAL is harmless.
- Both old and new manifests are complete CRC-checked records.

Recovery validates the three boundaries against the retained durable WAL range,
loads the catalog snapshot, and replays positioned records from the lesser redo
boundary according to record class.
