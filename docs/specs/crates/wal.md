# `wal` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`wal` owns the append-only write-ahead log. It records physiological page redo, logical DDL, and transaction status markers so recovery can replay page changes after the latest checkpoint and use the CLOG to decide visibility.

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
    CreateSequence { schema: SequenceSchema },
    DropSequence { sequence: SequenceId },
    SequenceAdvance { sequence: SequenceId, value: i64 },
    SetSequenceValue { sequence: SequenceId, value: i64, is_called: bool },
    Commit,
    CommitWithSubxids { subxids: Vec<u64> },
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

`Commit` and `Abort` carry no payload; the `txn_id` is in the header. `Commit` marks a transaction durably committed; `Abort` marks it aborted. Together they are the durable source of truth for transaction outcome and the input to CLOG reconstruction during recovery (see the MVCC plan, `docs/specs/mvcc.md` §5.4, §8). The CLOG is reconstructed at recovery; a durable CLOG snapshot (`clog.dat`, Milestone F — see "Durable CLOG snapshot" below) now seeds it and lets recovery fold only the post-snapshot `Commit`/`Abort` records, with the full in-memory rebuild from these records as the no-snapshot fallback.

`CommitWithSubxids` is the commit record for a transaction that had savepoint subtransactions (`docs/specs/savepoints.md` §5). It is identical to `Commit` except it carries the JSON `subxids` payload — the set of committed (live or released, not-rolled-back) subxids. Recovery and the runtime flush mark the header `txn_id` AND every `subxids` entry `Committed`, in one atomic durable record (so a concurrent crash never leaves a released subxid committed while its parent is not). A rolled-back subxid is recorded by its own `Abort` record (header `txn_id` = the subxid; `ROLLBACK TO SAVEPOINT` appends one per rolled-back subxid) and is absent from `subxids`. A no-savepoint commit still uses the plain `Commit` record, so its on-disk format is unchanged. The transaction-id allocator's recovery scan folds in `subxids` too, so a committed read-only subxid (present only in this payload) is never reissued.

`SequenceAdvance` and `SetSequenceValue` are non-transactional logical runtime records produced by `nextval` and `setval`. Recovery replays them unconditionally against the storage sequence runtime, after the checkpoint catalog snapshot has installed the baseline sequence set. They are not CLOG-gated DDL records: a sequence value handed out by an aborted transaction still creates a gap and is not reissued.

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
    // Establish the CLOG implicit-committed floor at recovery (no-op when a durable
    // `clog.dat` snapshot was loaded; conservative re-derivation otherwise). See Invariants.
    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()>;
    // Advance the vacuum floor (Milestone F4c): the boundary below which a full VACUUM
    // pass reclaimed every aborted-creator tuple, so `persist_clog`'s snapshot drops those
    // aborts' explicit entries and floats the floor past them (`truncate_before` does not
    // consult it — it is unconditional). Persisted in `clog.dat`. See Invariants.
    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()>;
    // Persist the durable CLOG snapshot (`clog.dat`) covering records through `clog_lsn`.
    // The checkpoint calls this after the control record is durable and before
    // `truncate_before`. See "Durable CLOG snapshot".
    fn persist_clog(&self, clog_lsn: Lsn) -> Result<()>;
}
```

The redo-committed-only `replay_committed_from` is **retired** (Milestone D2): recovery uses `replay_from` + the CLOG (redo-all). `is_redo_operation(kind)` (a free function, also re-exported) classifies a record as a replayable operation (everything except the `Commit`/`CommitWithSubxids`/`Abort`/`Checkpoint` markers); redo-all applies those and skips the markers.

`append` always assigns the next monotonically increasing LSN and writes that LSN into the encoded record. Callers may pass `record.lsn = 0`; `append` ignores the caller-provided LSN. `decode_record` and replay preserve the stored LSN from disk. `decode_record` decodes exactly one record from a buffer: it returns an error on a partial buffer (`"incomplete WAL record"`) and on a buffer with bytes left over after the record (`"WAL buffer contains trailing bytes"`). `flush` fsyncs all buffered records and returns the durable high-water mark.

`replay_from(lsn)` is strictly exclusive: it inspects only records whose stored `record.lsn > lsn`. Recovery uses it for two separate purposes. Redo passes the control record `checkpoint_lsn`, so page replay starts after the last WAL record whose effects are already reflected in the heap. The transaction-id allocator seed passes `0` and scans every retained record, so it can recover the allocation high-water from pre-boundary records when a crash happens after the manifest/CLOG checkpoint is durable but before the `Checkpoint` marker is appended; after a completed truncation, the retained marker carries the boundary instead. Redo-all (Milestone D2) iterates `replay_from(checkpoint_lsn)` and applies every page-mutation record (`is_redo_operation` — `HeapInit`/`HeapInsert`/`HeapDelete`/`HeapUpdateHeader`/`FullPageImage`), skipping the `Commit`/`CommitWithSubxids`/`Abort`/`Checkpoint` markers. DDL records (`CreateTable`/`DropTable`/`CreateIndex`/`DropIndex`/`CreateSequence`/`DropSequence`) install catalog/storage objects only for committed transactions (the server gates those by the rebuilt CLOG; see `server.md`), while skipped aborted/in-flight create records still reserve their table/index/sequence IDs. The CLOG decides visibility afterward.

`truncate_before(lsn)` is strictly exclusive in the opposite direction: it may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`. **Unconditional truncation:** truncation drops every record below `lsn` — it does NOT pin aborted/in-flight transactions and does NOT touch the in-memory CLOG or floors. It is safe because the checkpoint calls `persist_clog` (which durably records every aborted outcome in `clog.dat`) *before* `truncate_before`, and under the exclusive checkpoint guard no write transaction is in flight, so every transaction below `lsn` is settled and captured by that snapshot (see "Durable CLOG snapshot" and `mvcc.md` §5.4/§8). **Precondition:** a caller must persist the CLOG snapshot covering `lsn` before truncating; the no-snapshot fallback (a pre-durable-CLOG data directory) instead relies on the WAL having been conservatively truncated by the older build.

`truncate_before` writes retained records to a temporary WAL file, fsyncs the temporary file, renames it over the live WAL, and immediately fsyncs the parent directory. If the parent-directory fsync — or the subsequent WAL reopen or seek — fails, the WAL manager is poisoned and returns the error before mutating retained-record in-memory state. Only after the rename is directory-durable may the manager reopen, seek, and replace in-memory WAL state.

Poisoning is not limited to truncation: the WAL manager is poisoned whenever it cannot undo a partial mutation — if `append` fails to roll back a partially written record, or if `flush` fails to roll back unflushed bytes. Once poisoned, every subsequent operation returns the poison error.

`bytes_after(lsn)` returns the total encoded byte length of retained WAL records whose stored `record.lsn > lsn`. It is used only for server checkpoint threshold accounting. If `lsn` is older than the first retained record after truncation, it returns the total encoded byte length of all retained records.

## Commit Protocol

For a successful write statement:

1. Storage appends physiological redo records (`HeapInit`/`HeapInsert`/`HeapDelete`, or a `FullPageImage` on the first modification of a page since the last checkpoint); DDL appends `CreateTable`/`DropTable`/`CreateIndex`/`DropIndex`/`CreateSequence`/`DropSequence`.
2. Server query orchestration appends `Commit`.
3. Server query orchestration calls `wal.flush()`.
4. The statement is durable and must not be rolled back.
5. Server query orchestration calls cleanup-only `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)`.
6. Success is returned to the client.

If cleanup fails after step 3, the server treats it as fatal and exits after flushing WAL. It must not call rollback because the durable `Commit` record means recovery will replay the statement.

For failed write statements:

1. Server query orchestration does not append `Commit`. It appends an `Abort` record (which records the transaction `Aborted` in the CLOG) before deregistering the transaction. Abort is not fsync-gated; a transaction with no durable `Commit` is recovered as aborted regardless.
2. Server query orchestration calls `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)`. These are metadata/bookkeeping cleanup only under status-based abort: heap and index page bytes are not undone.
3. Uncommitted or aborted physical WAL records remain and are still replayed by redo-all recovery. Their versions stay invisible because the CLOG reports the transaction as aborted or in-flight-at-crash. Logical DDL records are the exception: recovery installs them only for committed transactions, but still reserves IDs from skipped aborted/in-flight create records so orphan page files are not reused.

If rollback cleanup fails before the commit record is durable, the server treats the process state as unsafe: it logs the rollback failure, attempts to flush WAL, and exits. Recovery may replay that transaction's physical page records, but no durable `Commit` exists, so the CLOG hides them.

## Checkpoint Interaction

The control record (`manifest.dat`) contains the authoritative `checkpoint_lsn` (redo boundary). WAL `Checkpoint` records are metadata only.

After heap pages are flushed + fsynced and the control record is stored:

1. Call `persist_clog(checkpoint_lsn)` to write the durable CLOG snapshot (see "Durable CLOG snapshot"). This MUST happen before step 3 so the snapshot covers every outcome the truncation is about to drop.
2. Append `WalRecord { txn_id: <txn-id high-water>, kind: Checkpoint { redo_lsn }, .. }`. The marker's `txn_id` carries the transaction-id allocation high-water mark (highest id allocated so far) rather than the usual `0`. The marker survives `truncate_before` (its LSN is the retained boundary), so recovery's allocator seed recovers the boundary even when every data record below the checkpoint was truncated — without it the allocator would restart low and reissue ids that already stamped committed tuples, corrupting MVCC visibility. Recovery still does not *replay* this metadata.
3. Flush WAL.
4. Call `truncate_before(checkpoint_lsn)`.

`truncate_before` must not remove records needed by the current control record. It must preserve the relative order and stored LSNs of retained records. It does NOT touch the CLOG or its floors — `persist_clog` (run first) already pruned them to the live window and owns the durable floor.

## Durable CLOG snapshot

`clog.dat` is a sibling of `wal.dat` (`<data-dir>/clog.dat`) that persists the transaction-status map across restart (`docs/specs/mvcc.md` §5.4). It uses the same versioned + CRC-checked envelope as the control record (`crates/control/src/manifest.rs`): magic `SGCL` + `u32` version + `u32` payload length + `u32` CRC32 over a JSON payload. The payload is the **live window** — the explicit `Committed`/`Aborted` ids at or above the implicit-committed floor — plus `clog_lsn` (the WAL LSN through which the statuses are absorbed), the `committed_floor`, and the `vacuum_floor`. Everything below the floor is implicit-committed (genuinely committed, or a VACUUM-reclaimed abort) and omitted, so the file is `O(live window)`.

- `persist_clog(clog_lsn)` computes the snapshot, writes it atomically (temp file + fsync + rename + parent-directory fsync), then prunes the in-memory CLOG to the same window. The write is **write-then-mutate**: a failed durable write leaves the in-memory floor unchanged, so the next open still reconciles against the previous snapshot. `clog_lsn` is clamped to `flushed_lsn` (the CLOG only records *flushed* commits, so the snapshot must never claim to cover beyond what is durable). A failed `clog.dat` write does **not** poison the WAL — the snapshot is auxiliary, and the durable WAL records remain the source of truth.
- **At `open`**, when `clog.dat` is present the CLOG is seeded from it (statuses + both floors) and only the `Commit`/`Abort` records with `lsn > clog_lsn` are folded on top — bounding the status-rebuild scan to post-snapshot records. When it is **absent** (fresh database, or a pre-durable-CLOG data directory) the CLOG is fully rebuilt from the retained WAL (the historical behavior) — backward compatible, no migration. A **corrupt** `clog.dat` (CRC/version/structure mismatch) is surfaced as an error like a bad `manifest.dat`; the atomic temp+rename means a torn write never occurs, so a mismatch is real corruption.
- Because the snapshot persists the `committed_floor` and `vacuum_floor`, both survive a clean restart (the no-snapshot fallback seeds them conservatively).

## Replay

Recovery (redo-all, Milestone D2):

- Reads the control record checkpoint LSN.
- Calls `replay_from(checkpoint_lsn)` and applies every page-mutation record (`is_redo_operation`) under PageLSN gating, regardless of the transaction's outcome; the `Commit`/`CommitWithSubxids`/`Abort`/`Checkpoint` markers are skipped. DDL records install objects only for committed transactions (server-gated by the CLOG), and skipped aborted/in-flight create records reserve their table/index/sequence IDs.
- Reconstructs the CLOG at `open`: seeded from the durable CLOG snapshot (`clog.dat`) plus a fold of the post-`clog_lsn` `Commit`/`Abort` records, or fully rebuilt from those records when no snapshot exists (see "Durable CLOG snapshot"). The CLOG — not a replay filter — decides visibility: an aborted or in-flight (no `Commit`/`Abort`) transaction's replayed versions are present in the heap but invisible, and reclaimed by VACUUM (Milestone F).
- Seeds `next_txn_id` by scanning `replay_from(0)` over all retained records (including `CommitWithSubxids.subxids` and the `Checkpoint` marker's high-water), not just the post-checkpoint redo range.

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
  `committed_txns` set) is populated at open from the durable CLOG snapshot when one
  exists (seed + fold post-`clog_lsn` records) or, with no snapshot, by scanning
  records with `lsn <= flushed_lsn` (`Commit` → `Committed`, `Abort` → `Aborted`); it
  is updated at runtime on `flush` (pending commits → `Committed`) and `append`
  (`Abort` → `Aborted`). A commit that has been appended but not yet flushed is
  tracked separately as pending and `is_committed` returns false for it until the
  flush makes it durable. `status` takes the WAL state lock briefly per call; the
  visibility predicate may probe it per tuple during scans (B3.6), and lock
  contention under heavy concurrent scanning is a Milestone E concern. Reserved ids
  below `FIRST_NORMAL_XID` (including `FROZEN_XID`) read as `Committed`; an unrecorded
  normal id reads as `InProgress`. The in-memory CLOG is reconstructed at recovery;
  the durable CLOG snapshot (`clog.dat`, Milestone F) seeds and bounds that
  reconstruction (see "Durable CLOG snapshot" and `docs/specs/mvcc.md` §5.4).
- **Implicit-committed floor.** The CLOG carries a
  monotonic `committed_floor`: an unrecorded normal id strictly below it reads as
  `Committed` instead of `InProgress`. This covers transactions whose `Commit`
  records were truncated by a checkpoint while their flushed tuples survive in the
  heap (`docs/specs/mvcc.md` §5.4). An explicitly recorded status
  (`Committed`/`Aborted`) always takes precedence over the floor, so a recorded
  abort below the floor is never falsely shown. The floor is persisted in the durable
  CLOG snapshot (`clog.dat`) and reloaded at open; when no snapshot is present it is
  re-established conservatively from the retained WAL as below.

  Because the relaxed flush gate (Milestone D1) now lets an aborted/in-flight
  transaction's pages reach the heap, the floor must never cross such a
  transaction (or its on-disk versions would wrongly read as committed —
  corruption). The durable CLOG snapshot enforces this:
  - `persist_clog`'s `live_snapshot` advances the floor only up to — never across —
    the oldest **un-reclaimed** aborted id (it keeps that id's explicit `Aborted`
    entry); everything below is committed or a VACUUM-reclaimed abort.
  - At recovery the floor is **loaded from the snapshot**;
    `establish_recovery_committed_floor` is a no-op when a snapshot was loaded. The
    no-snapshot fallback re-derives it as
    `min(allocation_boundary, oldest_non_committed_retained_xid)` — never above the
    oldest retained transaction whose CLOG status is not `Committed`.

  Together this guarantees an aborted-but-flushed transaction stays invisible across a
  checkpoint and restart even though WAL truncation is unconditional. Letting the floor
  cover an aborted transaction (dropping its snapshot entry) is safe only once VACUUM
  has reclaimed its versions — which Milestone F4c tracks via the vacuum floor.
- **Vacuum floor (`set_vacuum_floor`, Milestone F4c).** A `vacuum_floor`
  (monotonic) records the boundary below which a FULL VACUUM pass reclaimed every
  aborted-creator tuple. The server captures `B = next_txn_id` at the start of a full
  pass under the exclusive guard and calls `set_vacuum_floor(B)` after it.
  `persist_clog`'s `live_snapshot` then **drops the explicit entry** of — and floats the
  implicit-committed floor past — an aborted transaction with id `< vacuum_floor`,
  because its on-disk versions are reclaimed (so "implicit-committed below floor" is
  vacuously correct). WAL `truncate_before` does NOT consult the vacuum floor (it is
  unconditional). **Durability:** the floor is consulted by `persist_clog`, which a
  checkpoint runs after `flush_dirty_pages` + `store.sync_all`, so the reclamation is
  fsynced before any entry is dropped. **Persisted across restart:** the floor is written
  to `clog.dat` and reloaded at `open`, so it survives a clean restart; when no snapshot
  is present it falls back to `FIRST_NORMAL_XID`, so the snapshot keeps every aborted
  entry until the first post-restart full VACUUM — safe, never less correct
  (`docs/specs/mvcc.md` §5.4).
- WAL does not know B-tree/page format.

## Acceptance Tests

- Append and replay records in LSN order.
- Flush advances durable LSN.
- Recovery rebuilds the CLOG from `Commit`/`Abort` records and `replay_from` yields every record (redo-all); visibility is decided by the CLOG, not a replay filter.
- Decoupled truncation: `persist_clog` records an un-vacuumed aborted transaction, then unconditional `truncate_before` drops its `Abort` record — yet after reopen the snapshot keeps it `Aborted` (invisible), and repeated checkpoint+recovery cycles (with the recovery floor establisher) never resurrect it.
- Vacuum floor (F4c): after `set_vacuum_floor(B)`, the next `persist_clog` drops a reclaimed aborted transaction `< B` from the snapshot (it reads implicit-committed) — while an aborted transaction `>= B`, or one with no vacuum floor advanced, keeps its explicit `Aborted` entry; with no durable CLOG snapshot the floor falls back to its conservative value at reopen.
- Durable CLOG snapshot: `persist_clog` writes `clog.dat`; reopen seeds the CLOG and both floors from it and folds only the post-`clog_lsn` `Commit`/`Abort` records; an absent snapshot rebuilds from the WAL; a corrupt snapshot (CRC/version/structure) fails open. The snapshot envelope round-trips and rejects tamper/version/length/unsorted/overlapping payloads.
- Truncated WAL still replays from manifest checkpoint LSN.
- CRC detects corrupted record.
- Incomplete trailing record after crash is ignored.
