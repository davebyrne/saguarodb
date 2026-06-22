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

`txn_id = 0` is reserved for non-transactional system metadata records. The `Checkpoint` marker is the exception that carries a non-zero `txn_id`: it stamps the transaction-id allocation high-water mark so the allocator boundary survives WAL truncation (see Checkpoint Interaction). No consumer treats the marker's `txn_id` as a real transaction (CLOG rebuild and redo key off the record *kind*); only the allocator seed reads it. User statement transaction IDs start at `FIRST_NORMAL_XID` (the allocator floors there so real transactions never stamp tuple headers with a reserved xid).

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

// `TxnStatusView` is a supertrait, so every WAL manager exposes CLOG status
// (`status`/`is_committed`/`is_aborted`) — see "Transaction status" below.
pub trait WalManager: Send + Sync + common::TxnStatusView {
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    fn flush(&self) -> Result<Lsn>;
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;
    fn flushed_lsn(&self) -> Lsn;
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;
    // Establish the CLOG implicit-committed floor at recovery, conservatively
    // (never crossing an aborted/in-flight transaction). See Invariants.
    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()>;
    // Advance the in-memory vacuum floor (Milestone F4c): the boundary below which a
    // full VACUUM pass reclaimed every aborted-creator tuple, so `truncate_before`
    // may drop those aborts' records and float the floor past them. See Invariants.
    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()>;
}
```

The redo-committed-only `replay_committed_from` is **retired** (Milestone D2): recovery uses `replay_from` + the CLOG (redo-all). `is_redo_operation(kind)` (a free function, also re-exported) classifies a record as a replayable page mutation (everything except the `Commit`/`Abort`/`Checkpoint` markers); redo-all applies those and skips the markers.

`append` always assigns the next monotonically increasing LSN and writes that LSN into the encoded record. Callers may pass `record.lsn = 0`; `append` ignores the caller-provided LSN. `decode_record` and replay preserve the stored LSN from disk. `decode_record` decodes exactly one record from a buffer: it returns an error on a partial buffer (`"incomplete WAL record"`) and on a buffer with bytes left over after the record (`"WAL buffer contains trailing bytes"`). `flush` fsyncs all buffered records and returns the durable high-water mark.

`replay_from(lsn)` is strictly exclusive: it inspects only records whose stored `record.lsn > lsn`. Recovery passes the control record `checkpoint_lsn`, so replay starts after the last WAL record whose effects are already reflected in the heap. Recovery (redo-all, Milestone D2) iterates `replay_from(checkpoint_lsn)` and applies every page-mutation record (`is_redo_operation` — `HeapInit`/`HeapInsert`/`HeapDelete`/`HeapUpdateHeader`/`FullPageImage`), skipping the `Commit`/`Abort`/`Checkpoint` markers, and applying DDL records (`CreateTable`/`DropTable`/`CreateIndex`/`DropIndex`) only for committed transactions (the server gates those by the rebuilt CLOG; see `server.md`). The CLOG decides visibility afterward.

`truncate_before(lsn)` is strictly exclusive in the opposite direction: it may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`. **Conservative truncation (Milestone D):** truncation never drops a transaction that is not durably committed — it clamps the effective boundary to the earliest record of the oldest such transaction below `lsn` (`effective_lsn = min(lsn, that record's lsn)`), so an aborted/in-flight transaction's records (notably its `Abort`) are retained. This keeps its on-disk (relaxed-flush) versions hidden across restart (see the implicit-committed floor below and `mvcc.md` §5.4/§8). The extra WAL this retains is bounded and freed once VACUUM reclaims the aborted versions. **F4c relaxation (live):** the pin is `represents_transaction(rec) && !is_committed(txn) && !(is_aborted(txn) && txn < vacuum_floor)` — an aborted transaction with id below the **vacuum floor** (a full VACUUM pass reclaimed every aborted-creator tuple `< vacuum_floor`, made durable before this truncation; see `set_vacuum_floor` in Invariants) no longer pins, and the floor floats past it, because it has no surviving on-disk version. The relaxation is gated strictly on a CLOG-recorded `Aborted` status; an in-flight/un-settled id below the floor still pins.

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

1. Append `WalRecord { txn_id: <txn-id high-water>, kind: Checkpoint { redo_lsn }, .. }`. The marker's `txn_id` carries the transaction-id allocation high-water mark (highest id allocated so far) rather than the usual `0`. The marker survives `truncate_before` (its LSN is the retained boundary), so recovery's allocator seed recovers the boundary even when every data record below the checkpoint was truncated — without it the allocator would restart low and reissue ids that already stamped committed tuples, corrupting MVCC visibility. Recovery still does not *replay* this metadata.
2. Flush WAL.
3. Call `truncate_before(checkpoint_lsn)`.

`truncate_before` must not remove records needed by the current control record. It must preserve the relative order and stored LSNs of retained records. It also advances the CLOG implicit-committed floor (see Invariants) past the (committed) transactions it removes, since their `Commit` records are now gone but their flushed tuples survive — and, per conservative truncation, it never advances past the oldest non-committed transaction it pinned.

## Replay

Recovery (redo-all, Milestone D2):

- Reads the control record checkpoint LSN.
- Calls `replay_from(checkpoint_lsn)` and applies every page-mutation record (`is_redo_operation`) under PageLSN gating, regardless of the transaction's outcome; the `Commit`/`Abort`/`Checkpoint` markers are skipped. DDL records replay only for committed transactions (server-gated by the CLOG).
- Rebuilds the CLOG from durable `Commit`/`Abort` records (done at `open`). The CLOG — not a replay filter — decides visibility: an aborted or in-flight (no `Commit`/`Abort`) transaction's replayed versions are present in the heap but invisible, and reclaimed by VACUUM (Milestone F).

The replay iterator stops cleanly at EOF. A partial final record after crash is ignored if CRC/header indicates incomplete trailing write; a corrupt record before EOF returns `ErrorKind::Wal`. On `open`, an incomplete trailing record is not merely ignored in memory — the WAL file is physically truncated to the last complete record's end and fsynced, so the torn tail is removed on disk. After such a truncation (and after `truncate_before`), `next_lsn` is derived from the maximum LSN among the retained records, so newly appended records continue monotonically past the highest retained LSN.

## Invariants

- LSNs are strictly increasing.
- `flush()` only returns after fsync.
- The WAL manager is the `common::TxnStatusView` for transaction status (a
  supertrait of `WalManager`): `status(txn_id)` returns the CLOG status, and the
  inherited `is_committed(txn_id)`/`is_aborted(txn_id)` convenience methods derive
  from it (replacing the old inherent `WalManager::is_committed`). `is_committed`
  consults only durable commits: it is `clog.status(txn_id) == Committed`, true
  once the txn's `Commit` record is flushed. `Clog` itself implements
  `TxnStatusView` (so the storage engine can probe status per tuple in B3.6 via
  the WAL handle, which trait-upcasts to `&dyn TxnStatusView`). The CLOG (`Clog`,
  an in-memory `txn_id → TxnStatus` map; supersedes the old single-bit
  `committed_txns` set) is populated at open by scanning records with
  `lsn <= flushed_lsn` (`Commit` → `Committed`, `Abort` → `Aborted`) and updated on
  `flush` (pending commits → `Committed`) and `append` (`Abort` → `Aborted`). A
  commit that has been appended but not yet flushed is tracked separately as
  pending and `is_committed` returns false for it until the flush makes it durable.
  `status` takes the WAL state lock briefly per call; the visibility predicate may
  probe it per tuple during scans (B3.6), and lock contention under heavy
  concurrent scanning is a Milestone E concern. Reserved ids below
  `FIRST_NORMAL_XID` (including `FROZEN_XID`) read as `Committed`; an unrecorded
  normal id reads as `InProgress`. The CLOG is in-memory for the MVCC A–D MVP and
  rebuilt from the WAL at recovery; a durable CLOG file is deferred to Milestone F
  (see `docs/specs/mvcc.md` §5.4).
- **Implicit-committed floor (conservative, Milestone D).** The CLOG carries a
  monotonic `committed_floor`: an unrecorded normal id strictly below it reads as
  `Committed` instead of `InProgress`. This covers transactions whose `Commit`
  records were truncated by a checkpoint while their flushed tuples survive in the
  heap (`docs/specs/mvcc.md` §5.4). An explicitly recorded status
  (`Committed`/`Aborted`) always takes precedence over the floor, so a recorded
  abort below the floor is never falsely shown.

  Because the relaxed flush gate (Milestone D1) now lets an aborted/in-flight
  transaction's pages reach the heap, the floor must never cross such a
  transaction (or its on-disk versions would wrongly read as committed —
  corruption). Two coordinated rules enforce this:
  - At recovery, `establish_recovery_committed_floor(allocation_boundary)` sets the
    floor to `min(allocation_boundary, oldest_non_committed_retained_xid)` — never
    above the oldest retained transaction whose CLOG status is not `Committed`.
  - At runtime, `truncate_before` advances the floor only past the (committed)
    transactions it actually removed (it pins the oldest non-committed one, so that
    one is retained, not removed). Before any truncation the floor stays at
    `FIRST_NORMAL_XID`, so live behavior is unchanged.

  Together with conservative truncation (which keeps the pinned transaction's
  `Abort` record), this guarantees an aborted-but-flushed transaction stays
  invisible across a checkpoint and restart. Truncating past an aborted transaction
  (and letting the floor cover it) is safe only once VACUUM has reclaimed its
  versions — which Milestone F4c now tracks via the vacuum floor.
- **Vacuum floor (`set_vacuum_floor`, Milestone F4c).** An in-memory `vacuum_floor`
  (initialized to `FIRST_NORMAL_XID`, monotonic) records the boundary below which a
  FULL VACUUM pass reclaimed every aborted-creator tuple. The server captures
  `B = next_txn_id` at the start of a full pass under the exclusive guard and calls
  `set_vacuum_floor(B)` after it. `truncate_before` then stops pinning — and floats
  the implicit-committed floor past — an aborted transaction with id `< vacuum_floor`,
  because its on-disk versions are reclaimed (so "implicit-committed below floor" is
  vacuously correct). **Durability:** the floor is only consulted by `truncate_before`,
  which a checkpoint runs after `flush_dirty_pages` + `store.sync_all`, so the
  reclamation is fsynced before any `Abort` is dropped. **Reset-at-restart:** the
  floor is NOT durable; it resets to `FIRST_NORMAL_XID` at `open`, so after a crash
  truncation is conservative again until the first post-restart full VACUUM — safe,
  never less correct. A durable CLOG file that would carry the floor across restart
  remains deferred (`docs/specs/mvcc.md` §5.4).
- WAL does not know B-tree/page format.

## Acceptance Tests

- Append and replay records in LSN order.
- Flush advances durable LSN.
- Recovery rebuilds the CLOG from `Commit`/`Abort` records and `replay_from` yields every record (redo-all); visibility is decided by the CLOG, not a replay filter.
- Conservative truncation pins an aborted transaction: a checkpoint that asks to truncate past it retains its `Abort` record, and the recovery floor never marks it committed.
- Vacuum floor (F4c): after `set_vacuum_floor(B)`, truncation drops a reclaimed aborted transaction `< B` (its `Abort` is removed, the WAL shrinks further than the pinned case, and the implicit-committed floor floats past it) — while an aborted transaction `>= B`, or one with no vacuum floor advanced, still pins; the floor resets at reopen (conservative again).
- Truncated WAL still replays from manifest checkpoint LSN.
- CRC detects corrupted record.
- Incomplete trailing record after crash is ignored.
