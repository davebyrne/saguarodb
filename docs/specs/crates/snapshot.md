# `snapshot` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`snapshot` owns durable full-snapshot checkpoints. It writes complete table and catalog snapshots to new generation directories, then atomically swaps a manifest. This is the v1 durability anchor that allows logical WAL without page-level redo.

## Depends On

- `common`
- `buffer`

## File Layout

```text
data/
  manifest.dat
  manifest.dat.tmp
  wal.dat
  snap_<generation>/
    catalog.dat
    table_<TableId>.tbl
```

`manifest.dat` is the single source of truth for the current snapshot.

## Manifest

```rust
pub struct SnapshotMetadata {
    pub generation: u64,
    pub checkpoint_lsn: Lsn,
    pub tables: Vec<TableId>,
}

pub struct LoadedSnapshot {
    pub metadata: SnapshotMetadata,
    pub catalog_bytes: Vec<u8>,
}

pub struct SnapshotPage {
    pub page_num: PageNum,
    pub data: PageData,
}
```

Table snapshot files preserve page numbers with this binary layout:

```text
PageCount: 4 bytes
Repeated PageCount times:
  PageNum: 4 bytes
  PageData: 8192 bytes
```

Manifest bytes use a versioned binary envelope:

- magic: `SGMF` (4 bytes)
- version: little-endian `u32`, v1 = `1`
- payload length: little-endian `u32`
- payload checksum: little-endian CRC32 over the exact payload bytes
- payload: UTF-8 JSON containing `generation`, `checkpoint_lsn`, and sorted `tables`

Decode must reject magic mismatch, unsupported versions, length mismatch, checksum mismatch, malformed payload JSON, unsorted table IDs, and duplicate table IDs.

V1 development builds do not migrate the older JSON-object manifest format. A manifest that does not start with `SGMF` is rejected as corrupt, and users must rebuild the data directory from a compatible snapshot/WAL set.

Table file names are deterministic (`table_<TableId>.tbl`) and are not separately exposed in `SnapshotMetadata`. The on-disk manifest may store only table IDs because the file name can be derived from the table ID.

`checkpoint_lsn` is the WAL high-water mark included in the snapshot. Recovery replays committed WAL records with `LSN > checkpoint_lsn`.

## Public API

```rust
pub trait SnapshotManager: Send + Sync {
    fn load_current(&self, buffer_pool: &dyn BufferPool) -> Result<Option<LoadedSnapshot>>;
    fn current_table_pages(&self, table: TableId) -> Result<Vec<SnapshotPage>>;
    fn begin_snapshot(&self) -> Result<SnapshotWriter>;
    fn commit_snapshot(&self, writer: SnapshotWriter, checkpoint_lsn: Lsn) -> Result<SnapshotMetadata>;
    fn cleanup_old_snapshots(&self) -> Result<()>;
}

pub struct FileSnapshotManager { /* data directory backed snapshots */ }

impl FileSnapshotManager {
    pub fn open(data_dir: impl AsRef<std::path::Path>) -> Result<Self>;
}

pub struct SnapshotWriter { /* generation + output dir */ }

impl SnapshotWriter {
    pub fn write_table(&mut self, table: TableId, pages: &[SnapshotPage]) -> Result<()>;
    pub fn write_catalog(&mut self, catalog: &[u8]) -> Result<()>;
}
```

## Snapshot Composition

A full snapshot writes every live page for every live table. The server owns page composition:

- The server starts from `current_table_pages(table)` for each live catalog table.
- The server overlays pages from `BufferPool::iter_pages`; buffer pages win for matching `(table, page_num)`.
- Newly allocated committed pages must be included through `BufferPool::iter_pages`.
- Dropped tables must be omitted because the server iterates live catalog tables only.
- `write_table` sorts pages by `page_num` and rejects duplicate page numbers.

Checkpoint holds the global write guard, so table set and page contents are stable while composing.

## Commit Protocol

`commit_snapshot` performs:

1. fsync every table file and `catalog.dat` in the new generation.
2. fsync the new generation directory.
3. write `manifest.dat.tmp`.
4. fsync `manifest.dat.tmp`.
5. rename `manifest.dat.tmp` to `manifest.dat`.
6. fsync `data/` directory.

Only after `commit_snapshot` succeeds may the caller mark buffer pages clean or truncate WAL. On success, `commit_snapshot` returns the exact `SnapshotMetadata` written to the durable manifest, including `generation`, `checkpoint_lsn`, and `tables`. The server uses this returned metadata when appending the WAL checkpoint metadata record with `txn_id: 0`.

## Recovery Behavior

`load_current`:

- If no manifest exists, returns `Ok(None)`.
- If manifest exists, validates checksum/version.
- Loads page-numbered table files into the buffer pool with `BufferPool::load_page(file_id, page_num, data)`.
- Loads catalog bytes for catalog initialization.
- Returns `LoadedSnapshot` containing `SnapshotMetadata` and catalog bytes.

Orphan generation directories are removed by `cleanup_old_snapshots`.

## Crash Safety

- Crash before manifest rename: old manifest remains current.
- Crash during manifest rename: filesystem yields old or new manifest; both point to complete snapshots.
- Crash after manifest rename: new manifest is current; old snapshots can be cleaned later.
- Previous snapshot is never deleted until new manifest is durable.

## Acceptance Tests

- First snapshot creates `manifest.dat` and `snap_1`.
- Loading manifest returns generation and checkpoint LSN.
- Crash before rename leaves old snapshot current.
- Crash after rename leaves new snapshot current.
- Orphan snapshot directories are cleaned.
- Snapshot composition includes dirty pages and unchanged clean pages.
