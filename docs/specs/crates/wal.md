# `wal` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`wal` owns the append-only logical write-ahead log. It records committed statement intent so recovery can replay operations after the latest snapshot checkpoint.

## Depends On

- `common`

## WAL Records

```rust
pub struct WalRecord {
    pub lsn: Lsn,
    pub txn_id: u64,
    pub kind: WalRecordKind,
}

pub enum WalRecordKind {
    Insert { table: TableId, key: Key, row: Row },
    Update { table: TableId, key: Key, row: Row },
    Delete { table: TableId, key: Key },
    CreateTable { schema: TableSchema },
    DropTable { table: TableId },
    Commit,
    Checkpoint { generation: u64, checkpoint_lsn: Lsn },
}
```

`txn_id = 0` is reserved for non-transactional system metadata records. V1 uses it only for `WalRecordKind::Checkpoint`. User statement transaction IDs start at `1`.

On disk:

```text
LSN: 8 bytes
TxnID: 8 bytes
Type: 1 byte
Length: 4 bytes
Payload: variable
CRC32: 4 bytes
```

CRC covers header and payload except the CRC field.

## Public API

```rust
pub struct FileWalManager { /* file-backed WAL */ }

impl FileWalManager {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self>;
}

pub trait WalManager: Send + Sync {
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    fn flush(&self) -> Result<Lsn>;
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    fn replay_committed_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;
    fn is_committed(&self, txn_id: u64) -> bool;
    fn flushed_lsn(&self) -> Lsn;
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;
}
```

`append` always assigns the next monotonically increasing LSN and writes that LSN into the encoded record. Callers may pass `record.lsn = 0`; `append` ignores the caller-provided LSN. `decode_record` and replay preserve the stored LSN from disk. `flush` fsyncs all buffered records and returns the durable high-water mark.

`replay_from(lsn)` and `replay_committed_from(lsn)` are strictly exclusive: both inspect only records whose stored `record.lsn > lsn`. Recovery passes the manifest `checkpoint_lsn`, so replay starts after the last WAL record whose effects are already included in the snapshot. `replay_committed_from` returns committed logical operation records only (`Insert`, `Update`, `Delete`, `CreateTable`, `DropTable`); it never yields `Commit` or `Checkpoint` metadata records.

`truncate_before(lsn)` is strictly exclusive in the opposite direction: it may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`.

`truncate_before` writes retained records to a temporary WAL file, fsyncs the temporary file, renames it over the live WAL, and immediately fsyncs the parent directory. If the parent directory fsync fails, the WAL manager is poisoned and returns the error before reopening the WAL file or mutating retained-record in-memory state. Only after the rename is directory-durable may the manager reopen, seek, and replace in-memory WAL state.

`bytes_after(lsn)` returns the total encoded byte length of retained WAL records whose stored `record.lsn > lsn`. It is used only for server checkpoint threshold accounting. If `lsn` is older than the first retained record after truncation, it returns the total encoded byte length of all retained records.

## Commit Protocol

For a successful write statement:

1. Storage appends logical operation records.
2. Server query orchestration appends `Commit`.
3. Server query orchestration calls `wal.flush()`.
4. The statement is durable and must not be rolled back.
5. Server query orchestration calls cleanup-only `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)`.
6. Success is returned to the client.

If cleanup fails after step 3, the server treats it as fatal and exits after flushing WAL. It must not call rollback because the durable `Commit` record means recovery will replay the statement.

For failed write statements:

1. Server query orchestration does not append `Commit`.
2. Server query orchestration calls `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)`.
3. Uncommitted WAL records remain but are ignored by recovery.

## Checkpoint Interaction

The snapshot manager manifest contains the authoritative `checkpoint_lsn`. WAL `Checkpoint` records are metadata only.

After a snapshot is committed:

1. Append `WalRecord { txn_id: 0, kind: Checkpoint { generation, checkpoint_lsn }, .. }`. Recovery does not depend on this metadata, but v1 writes it for observability and WAL tests.
2. Flush WAL.
3. Call `truncate_before(checkpoint_lsn)`.

`truncate_before` must not remove records needed by the current manifest. It must preserve the relative order and stored LSNs of retained records.

## Replay

Recovery:

- Reads manifest checkpoint LSN.
- Calls `replay_from(checkpoint_lsn)`.
- Builds a set of txn IDs with commit records where `LSN > checkpoint_lsn`.
- Replays only operation records whose txn ID committed. `replay_committed_from(checkpoint_lsn)` provides this filtered committed-record stream through the `WalManager` abstraction.
- Ignores uncommitted records.

The replay iterator stops cleanly at EOF. A partial final record after crash is ignored if CRC/header indicates incomplete trailing write; a corrupt record before EOF returns `ErrorKind::Wal`.

## Invariants

- LSNs are strictly increasing.
- `flush()` only returns after fsync.
- `is_committed(txn_id)` is true only if a commit record exists and is durable or has been read during recovery.
- WAL does not know B-tree/page format.

## Acceptance Tests

- Append and replay records in LSN order.
- Flush advances durable LSN.
- Recovery ignores uncommitted operation records.
- Truncated WAL still replays from manifest checkpoint LSN.
- CRC detects corrupted record.
- Incomplete trailing record after crash is ignored.
