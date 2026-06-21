# `wal` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`wal` owns the append-only write-ahead log. It records committed operations — physiological page redo plus DDL — so recovery can replay them after the latest checkpoint.

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
    // Logical (structured) records, JSON payloads.
    CreateTable { schema: TableSchema },
    DropTable { table: TableId },
    CreateIndex { schema: IndexSchema },
    DropIndex { index: IndexId },
    Commit,
    Abort,
    Checkpoint { redo_lsn: Lsn },
    // Physiological redo records, compact binary payloads.
    HeapInit { file_id: FileId, page_num: PageNum },
    HeapInsert { file_id: FileId, page_num: PageNum, slot: u16, row_bytes: Vec<u8> },
    HeapDelete { file_id: FileId, page_num: PageNum, slot: u16 },
    HeapUpdateHeader { file_id: FileId, page_num: PageNum, slot: u16, xmax: u64, t_ctid: (PageNum, u16), infomask: u16 },
    FullPageImage { file_id: FileId, page_num: PageNum, image: Vec<u8> },
}
```

`txn_id = 0` is reserved for non-transactional system metadata records. V1 uses it only for `WalRecordKind::Checkpoint`. User statement transaction IDs start at `FIRST_NORMAL_XID` (the allocator floors there so real transactions never stamp tuple headers with a reserved xid).

`Commit` and `Abort` carry no payload; the `txn_id` is in the header. `Commit` marks a transaction durably committed; `Abort` marks it aborted. Together they are the durable source of truth for transaction outcome and the input to CLOG reconstruction during recovery (see the MVCC plan, `docs/specs/mvcc.md` §5.4, §8). Under MVCC the CLOG is kept in memory and rebuilt from these records; a durable CLOG file is deferred to Milestone F.

The physiological redo records (`HeapInit`, `HeapInsert`, `HeapDelete`, `HeapUpdateHeader`, `FullPageImage`) describe page-level changes. The storage mutation path produces them (stamping the page-LSN), and recovery replays them PageLSN-gated; `FullPageImage` provides torn-page recovery.

`HeapUpdateHeader` is an in-place mutation of a v2 tuple header — it sets the `xmax`, forward `t_ctid` pointer, and `infomask` of the live tuple at `slot` without relocating it (the three are fixed-width header fields, so the tuple keeps its exact offset and length and the page is not compacted). It is the MVCC substrate for `UPDATE`/`DELETE` version stamping (Milestone B commits 8–9, `docs/specs/mvcc.md` §5.3); the record and its redo handler land first, ahead of engine emission. Recovery replays it PageLSN-gated like the other heap records: it is skipped when `page_lsn >= record.lsn`, otherwise it rewrites the header (via `page::set_tuple_header`) and the primitive stamps `record.lsn`, so replay is idempotent.

On disk:

```text
LSN: 8 bytes
TxnID: 8 bytes
Type: 1 byte
Length: 4 bytes
Payload: variable
CRC32: 4 bytes
```

CRC covers header and payload except the CRC field. Logical records encode their payload as JSON; physiological redo records use compact little-endian binary fields (`FullPageImage` stores the raw page bytes). The `Type` byte is authoritative for binary records.

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

`append` always assigns the next monotonically increasing LSN and writes that LSN into the encoded record. Callers may pass `record.lsn = 0`; `append` ignores the caller-provided LSN. `decode_record` and replay preserve the stored LSN from disk. `decode_record` decodes exactly one record from a buffer: it returns an error on a partial buffer (`"incomplete WAL record"`) and on a buffer with bytes left over after the record (`"WAL buffer contains trailing bytes"`). `flush` fsyncs all buffered records and returns the durable high-water mark.

`replay_from(lsn)` and `replay_committed_from(lsn)` are strictly exclusive: both inspect only records whose stored `record.lsn > lsn`. Recovery passes the control record `checkpoint_lsn`, so replay starts after the last WAL record whose effects are already reflected in the heap. `replay_committed_from` returns committed operation records — every record except the `Commit`, `Abort`, and `Checkpoint` metadata markers — which recovery applies as physiological redo (`HeapInit`/`HeapInsert`/`HeapDelete`/`HeapUpdateHeader`/`FullPageImage`) and DDL replay (`CreateTable`/`DropTable`/`CreateIndex`/`DropIndex`).

`truncate_before(lsn)` is strictly exclusive in the opposite direction: it may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`.

`truncate_before` writes retained records to a temporary WAL file, fsyncs the temporary file, renames it over the live WAL, and immediately fsyncs the parent directory. If the parent-directory fsync — or the subsequent WAL reopen or seek — fails, the WAL manager is poisoned and returns the error before mutating retained-record in-memory state. Only after the rename is directory-durable may the manager reopen, seek, and replace in-memory WAL state.

Poisoning is not limited to truncation: the WAL manager is poisoned whenever it cannot undo a partial mutation — if `append` fails to roll back a partially written record, or if `flush` fails to roll back unflushed bytes. Once poisoned, every subsequent operation returns the poison error.

`bytes_after(lsn)` returns the total encoded byte length of retained WAL records whose stored `record.lsn > lsn`. It is used only for server checkpoint threshold accounting. If `lsn` is older than the first retained record after truncation, it returns the total encoded byte length of all retained records.

## Commit Protocol

For a successful write statement:

1. Storage appends physiological redo records (`HeapInit`/`HeapInsert`/`HeapDelete`, or a `FullPageImage` on the first modification of a page since the last checkpoint); DDL appends `CreateTable`/`DropTable`/`CreateIndex`/`DropIndex`.
2. Server query orchestration appends `Commit`.
3. Server query orchestration calls `wal.flush()`.
4. The statement is durable and must not be rolled back.
5. Server query orchestration calls cleanup-only `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)`.
6. Success is returned to the client.

If cleanup fails after step 3, the server treats it as fatal and exits after flushing WAL. It must not call rollback because the durable `Commit` record means recovery will replay the statement.

For failed write statements:

1. Server query orchestration does not append `Commit`. It appends an `Abort` record (which records the transaction `Aborted` in the CLOG) without flushing — abort durability is not critical, since a transaction with no durable `Commit` is recovered as aborted regardless.
2. Server query orchestration calls `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)`. (The buffer-pool before-image undo is retained in Milestone A; it is retired in Milestone C3 when abort becomes purely status-based.)
3. Uncommitted WAL records remain but are ignored by recovery.

If rollback cleanup fails before the commit record is durable, the server treats the process state as unsafe: it logs the rollback failure, attempts to flush WAL, and exits. Uncommitted WAL records remain ignored by recovery because no durable `Commit` record exists.

## Checkpoint Interaction

The control record (`manifest.dat`) contains the authoritative `checkpoint_lsn` (redo boundary). WAL `Checkpoint` records are metadata only.

After heap pages are flushed + fsynced and the control record is stored:

1. Append `WalRecord { txn_id: 0, kind: Checkpoint { redo_lsn }, .. }`. Recovery does not depend on this metadata; it is written for observability.
2. Flush WAL.
3. Call `truncate_before(checkpoint_lsn)`.

`truncate_before` must not remove records needed by the current control record. It must preserve the relative order and stored LSNs of retained records.

## Replay

Recovery:

- Reads the control record checkpoint LSN.
- Calls `replay_from(checkpoint_lsn)`.
- Rebuilds the CLOG from durable `Commit`/`Abort` records, then replays only operation records whose txn ID committed. `replay_committed_from(checkpoint_lsn)` provides this filtered committed-record stream through the `WalManager` abstraction.
- Ignores uncommitted records (in-flight, or aborted via an `Abort` record).

The replay iterator stops cleanly at EOF. A partial final record after crash is ignored if CRC/header indicates incomplete trailing write; a corrupt record before EOF returns `ErrorKind::Wal`. On `open`, an incomplete trailing record is not merely ignored in memory — the WAL file is physically truncated to the last complete record's end and fsynced, so the torn tail is removed on disk. After such a truncation (and after `truncate_before`), `next_lsn` is derived from the maximum LSN among the retained records, so newly appended records continue monotonically past the highest retained LSN.

## Invariants

- LSNs are strictly increasing.
- `flush()` only returns after fsync.
- `is_committed(txn_id)` consults only durable commits: it is `clog.status(txn_id) == Committed`, which is true once the txn's `Commit` record is flushed. The CLOG (`Clog`, an in-memory `txn_id → TxnStatus` map; supersedes the old single-bit `committed_txns` set) is populated at open by scanning records with `lsn <= flushed_lsn` (`Commit` → `Committed`, `Abort` → `Aborted`) and updated on `flush` (pending commits → `Committed`) and `append` (`Abort` → `Aborted`). A commit that has been appended but not yet flushed is tracked separately as pending and `is_committed` returns false for it until the flush makes it durable. Reserved ids below `FIRST_NORMAL_XID` (including `FROZEN_XID`) read as `Committed`; an unrecorded normal id reads as `InProgress`. The CLOG is in-memory for the MVCC A–D MVP and rebuilt from the WAL at recovery; a durable CLOG file is deferred to Milestone F (see `docs/specs/mvcc.md` §5.4).
- WAL does not know B-tree/page format.

## Acceptance Tests

- Append and replay records in LSN order.
- Flush advances durable LSN.
- Recovery ignores uncommitted operation records.
- Truncated WAL still replays from manifest checkpoint LSN.
- CRC detects corrupted record.
- Incomplete trailing record after crash is ignored.
