# `control` Crate Specification

**Date:** 2026-05-03
**Status:** Living crate contract

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
    pub checkpoint_lsn: Lsn,   // redo boundary: heap reflects flushed page effects <= this LSN
    pub tables: Vec<TableId>,  // sorted, no duplicates
    pub catalog: Vec<u8>,      // serialized catalog snapshot
    pub page_size: u32,        // page size (bytes) the data directory was created with
}
```

With MVCC's relaxed flush gate, the heap may include page effects from committed,
aborted, and in-flight-at-checkpoint transactions at or below the boundary. The
CLOG, persisted separately by the checkpoint before WAL truncation, decides which
versions are visible.

The control record uses a versioned binary envelope:

- magic: `SGMF` (4 bytes)
- version: little-endian `u32`, current = `4`
- payload length: little-endian `u32`
- payload checksum: little-endian CRC32 over the exact payload bytes
- payload: UTF-8 JSON containing `checkpoint_lsn`, sorted `tables`, `catalog`,
  and `page_size`

The opaque `catalog` bytes must contain catalog format v3. Manifest v4 and
catalog v3 are one compatibility boundary: neither decoder supplies missing
typed-catalog fields or migrates an older development data directory.

The four header fields form a fixed 16-byte header (`MANIFEST_HEADER_LEN = 16`)
that precedes the payload.

Decode must reject a file shorter than the 16-byte header, magic mismatch,
unsupported versions (including versions `1` through `3`), length mismatch, checksum
mismatch, malformed payload JSON, unsorted table IDs, and duplicate table IDs.
The manifest envelope is capped at 272 MiB before file materialization; its
opaque catalog field is capped at 64 MiB and its table-id list at 65,536 entries.
Bounded visitors reserve these payload vectors fallibly and reject an extra item
without materializing it. Encoding enforces the same limits.
Development builds do not migrate older formats; an incompatible or corrupt
control file surfaces as `SqlState::InternalError` (there is no dedicated
corruption SQLSTATE) and the data directory must be rebuilt.

`page_size` is forward-compatibility insurance for a future data-dir-creation-
time page size; today every data directory is created with the compile-time
`buffer::PAGE_SIZE` (8192). It is validated separately, *after* a successful
decode (`FileControlStore::open`'s caller supplies the binary's page size, and
`load` compares it against the stored value): a mismatch is a plain, clean
startup error naming both values, not corruption — the envelope itself decoded
fine.

`checkpoint_lsn` is the WAL high-water mark whose effects are reflected in the
heap. Recovery replays every physical WAL record with `LSN > checkpoint_lsn`
using page-LSN idempotence regardless of transaction outcome, then applies only
committed generic `CatalogChange` metadata in LSN order. Non-transactional
sequence-value records are also replayed independently of commit status.

## Public API

```rust
pub trait ControlStore: Send + Sync {
    fn load(&self) -> Result<Option<ControlData>>;
    fn store(&self, checkpoint_lsn: Lsn, tables: &[TableId], catalog: &[u8]) -> Result<()>;
}

pub struct FileControlStore { /* data directory, expected page size */ }

impl FileControlStore {
    pub fn open(data_dir: impl AsRef<std::path::Path>, page_size: u32) -> Result<Self>;
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
`PageStore`. `load` also rejects a `page_size` mismatch between the decoded
control record and the `page_size` passed to `open` (see above).

## Crash Safety

- Crash before the rename: the previous control record remains current; recovery
  redoes from the previous `checkpoint_lsn`, where this cycle's full-page images
  repair any torn heap writes.
- Crash during the rename: the filesystem yields the old or new control file;
  both are complete, CRC-checked records.
- Crash after the rename: the new control record is current.

## Acceptance Tests

- `store` then `load` round-trips `checkpoint_lsn`, tables, catalog, and
  `page_size`.
- `load` returns `None` with no control file.
- `store` overwrites the previous control record.
- Decode rejects checksum/version/length tampering and unsorted/duplicate tables.
- `load` rejects a `page_size` mismatch with a clean startup error naming both
  values (not reported as corruption).
