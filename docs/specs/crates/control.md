# `control` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`control` owns the durable **control record** — the checkpoint commit point. It
persists, atomically, the redo boundary (`checkpoint_lsn`), the live table ids,
and the catalog snapshot. Table data itself lives in mutable heap files
(`storage::HeapPageStore`) and is flushed in place; this crate no longer writes
whole-table snapshots.

## Depends On

- `common`
- `crc32fast` — payload CRC32 checksum
- `serde`, `serde_json` — JSON payload (de)serialization

## File Layout

```text
data/
  manifest.dat
  manifest.dat.tmp
  wal.dat
  heap/<TableId>.heap   (owned by storage::HeapPageStore)
```

`manifest.dat` is the single source of truth for the current checkpoint.

## Control Record

```rust
pub struct ControlData {
    pub checkpoint_lsn: Lsn,   // redo boundary: heap reflects all committed work <= this LSN
    pub tables: Vec<TableId>,  // sorted, no duplicates
    pub catalog: Vec<u8>,      // serialized catalog snapshot
}
```

The control record uses a versioned binary envelope:

- magic: `SGMF` (4 bytes)
- version: little-endian `u32`, current = `2`
- payload length: little-endian `u32`
- payload checksum: little-endian CRC32 over the exact payload bytes
- payload: UTF-8 JSON containing `checkpoint_lsn`, sorted `tables`, and `catalog`

The four header fields form a fixed 16-byte header (`MANIFEST_HEADER_LEN = 16`)
that precedes the payload.

Decode must reject a file shorter than the 16-byte header, magic mismatch,
unsupported versions (including the legacy full-snapshot manifest, version `1`),
length mismatch, checksum mismatch, malformed payload JSON, unsorted table IDs,
and duplicate table IDs. Development builds do not migrate older formats; an
incompatible or corrupt control file surfaces as `SqlState::InternalError`
(there is no dedicated corruption SQLSTATE) and the data directory must be
rebuilt.

`checkpoint_lsn` is the WAL high-water mark whose effects are reflected in the
heap. Recovery replays committed WAL records with `LSN > checkpoint_lsn`.

## Public API

```rust
pub trait ControlStore: Send + Sync {
    fn load(&self) -> Result<Option<ControlData>>;
    fn store(&self, checkpoint_lsn: Lsn, tables: &[TableId], catalog: &[u8]) -> Result<()>;
}

pub struct FileControlStore { /* data directory */ }

impl FileControlStore {
    pub fn open(data_dir: impl AsRef<std::path::Path>) -> Result<Self>;
}
```

## Commit Protocol

`store` writes the control record atomically:

1. write `manifest.dat.tmp`.
2. fsync `manifest.dat.tmp`.
3. rename `manifest.dat.tmp` to `manifest.dat`.
4. fsync `data/`.

The rename is the checkpoint commit point. The caller (server checkpoint) must
fsync the heap (`PageStore::sync_all`) **before** calling `store`, and must
truncate the WAL only **after** `store` succeeds.

## Recovery Behavior

`load` returns `Ok(None)` when no control file exists, otherwise the validated
`ControlData`. Recovery uses `checkpoint_lsn` as the redo boundary and `catalog`
to initialize the catalog; heap pages are read separately by the buffer pool's
`PageStore`.

## Crash Safety

- Crash before the rename: the previous control record remains current; recovery
  redoes from the previous `checkpoint_lsn`, where this cycle's full-page images
  repair any torn heap writes.
- Crash during the rename: the filesystem yields the old or new control file;
  both are complete, CRC-checked records.
- Crash after the rename: the new control record is current.

## Acceptance Tests

- `store` then `load` round-trips `checkpoint_lsn`, tables, and catalog.
- `load` returns `None` with no control file.
- `store` overwrites the previous control record.
- Decode rejects checksum/version/length tampering and unsorted/duplicate tables.
