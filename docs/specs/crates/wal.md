# `wal` Crate Specification

**Date:** 2026-07-12
**Status:** Living crate contract

## Purpose

`wal` owns the append-only write-ahead log. It records physiological page redo, generic catalog change sets, non-transactional sequence values and dictionary bytes, and transaction status markers so recovery can replay changes after the latest checkpoint and use the CLOG to decide visibility.

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
    // Authoritative catalog metadata record, JSON payload.
    CatalogChange { change_set: CatalogChangeSet },
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
    // Compression (`docs/specs/compression.md`), compact binary payloads.
    FullPageImageCompressed { file_id: FileId, page_num: PageNum, codec: u8, dict_id: u32, payload: Vec<u8> },
    CreateDictionary { dict_id: u32, table_id: TableId, bytes: Vec<u8> },
}
```

`CatalogChangeSet` is versioned and contains object-ID-sorted `CatalogMutation { before, after }` entries plus allocator high-water values. Objects cover schemas, tables/hidden relations, views, indexes, sequences, first-class constraints, and statistics. Columns remain nested in relation schemas but have stable `CatalogObjectId::Column { relation, column }` addresses; built-in functions remain virtual and immutable. Global allocator high-water includes `ConstraintId`; per-relation entries carry only stable-column allocators advanced by the change. The codec rejects catalog-change payloads larger than 64 MiB before deserialization and bounds mutation and per-relation allocator counts before growing their collections. Specialized catalog WAL variants are intentionally incompatible with this catalog-format transition and fail with an explicit unsupported-legacy-format error; no data-directory migration is provided.

`txn_id = 0` is reserved for non-transactional system metadata records. The `Checkpoint` marker is the exception that carries a non-zero `txn_id`: it stamps the transaction-id allocation high-water mark so the allocator boundary survives WAL truncation (see Checkpoint Interaction). No consumer treats the marker's `txn_id` as a real transaction (CLOG rebuild and redo key off the record *kind*); only the allocator seed reads it. User statement transaction IDs start at `FIRST_NORMAL_XID` (the allocator floors there so real transactions never stamp tuple headers with a reserved xid).

`Commit` and `Abort` carry no payload; the `txn_id` is in the header. `Commit` marks a transaction durably committed; `Abort` marks it aborted. Together they are the durable source of truth for transaction outcome and the input to CLOG reconstruction during recovery (see the MVCC plan, `docs/specs/mvcc.md` §5.4, §8). The CLOG is reconstructed at recovery; a durable CLOG snapshot (`clog.dat`, Milestone F — see "Durable CLOG snapshot" below) now seeds it and lets recovery fold only the post-snapshot `Commit`/`Abort` records, with the full in-memory rebuild from these records as the no-snapshot fallback.

`CommitWithSubxids` is the commit record for a transaction that had savepoint subtransactions (`docs/specs/savepoints.md` §5). It is identical to `Commit` except it carries the JSON `subxids` payload — the set of committed (live or released, not-rolled-back) subxids. Recovery and the runtime flush mark the header `txn_id` AND every `subxids` entry `Committed`, in one atomic durable record (so a concurrent crash never leaves a released subxid committed while its parent is not). A rolled-back subxid is recorded by its own `Abort` record (header `txn_id` = the subxid; `ROLLBACK TO SAVEPOINT` appends one per rolled-back subxid) and is absent from `subxids`. A no-savepoint commit still uses the plain `Commit` record, so its on-disk format is unchanged. The transaction-id allocator's recovery scan folds in `subxids` too, so a committed read-only subxid (present only in this payload) is never reissued.

`SequenceAdvance` and `SetSequenceValue` are non-transactional logical runtime records produced by `nextval` and `setval`. Recovery replays them unconditionally against the storage sequence runtime, after the checkpoint catalog snapshot has installed the baseline sequence set. They are not CLOG-gated DDL records: a sequence value handed out by an aborted transaction still creates a gap and is not reissued.

The physiological redo records (`HeapInit`, `HeapInsert`, `HeapDelete`, `HeapUpdateHeader`, `FullPageImage`) describe page-level changes. The storage mutation path produces them (stamping the page-LSN), and recovery replays them PageLSN-gated; `FullPageImage` provides torn-page recovery.

`HeapUpdateHeader` is an in-place mutation of a v2 tuple header — it sets the `xmax`, forward `t_ctid` pointer, and `infomask` of the live tuple at `slot` without relocating it (the three are fixed-width header fields, so the tuple keeps its exact offset and length and the page is not compacted). It is the MVCC substrate for `UPDATE`/`DELETE` version stamping (Milestone B commits 8–9, `docs/specs/mvcc.md` §5.3); the record and its redo handler land first, ahead of engine emission. Recovery replays it PageLSN-gated like the other heap records: it is skipped when `page_lsn >= record.lsn`, otherwise it rewrites the header (via `page::set_tuple_header`) and the primitive stamps `record.lsn`, so replay is idempotent.

### Compression records (`docs/specs/compression.md`)

These record kinds support WAL full-page-image compression and dictionary installation. Compression and TOAST catalog metadata are carried only by `CatalogChange`. `wal` has no dependency on `saguarodb-compress`; `storage` and `server` own compression and metadata application.

- **`FullPageImageCompressed { file_id, page_num, codec, dict_id, payload }`** (type byte `18`, compact binary, mirroring `FullPageImage`'s framing: `file_id`(4) + `page_num`(4) + `codec`(1) + `dict_id`(4) + `payload`). `payload` decompresses to exactly `PAGE_SIZE` bytes via the named `codec`/`dict_id`. Emitted by `storage::fpi_record_kind` in place of a plain `FullPageImage` only when it is smaller (unconditional compression attempt, per-record self-describing raw fallback — the WAL never expands, `compression.md` §6). The existing `FullPageImage` record and its redo handling are unchanged; a `FullPageImageCompressed` record is normalized to a decompressed `FullPageImage` by the caller (`server::apply_redo`) before physical redo runs, so `storage::apply_physical_redo` itself never sees the compressed variant (see "Replay" below).
- **`CreateDictionary { dict_id, table_id, bytes }`** (type byte `19`, compact binary: `dict_id`(4) + `table_id`(4) + raw trained-dictionary `bytes`). Installs an immutable per-table zstd dictionary. Appended (and flushed with the creating statement's commit) whenever `ALTER TABLE ... SET (compression = 'zstd')` trains one; the durability order is dictionary file written to `<data>/dicts/` **before** this record is appended, so a page envelope or a WAL FPI can only ever reference a dictionary id that is either already on disk or resolvable from an earlier-LSN `CreateDictionary` record (`compression.md` §7). Replay installs the dictionary file if it is not already present — **idempotent**, since `DictStore::save` is a no-op when the file already exists — and registers it with the in-memory dictionary resolver, so later records can resolve the same `dict_id`. It is classified alongside the other object-creating DDL records: `server` reserves the dictionary id even for a skipped aborted/in-flight record, so a later dictionary never reuses the same id.

All table compression, TOAST, TRUNCATE generation, primary-key, schema-evolution, view, schema, sequence, index, and statistics metadata is represented by the same `CatalogChange` record. Multi-object statements such as `CREATE TABLE` and multi-table TRUNCATE use one atomic change set. Physical page redo, `CreateDictionary`, transaction markers, and sequence value records remain separate.

On disk:

```text
LSN: 8 bytes
TxnID: 8 bytes
Type: 1 byte
Length: 4 bytes
Payload: variable
CRC32: 4 bytes
```

CRC covers header and payload except the CRC field. `CatalogChange` uses JSON; physiological redo uses compact little-endian binary fields. `FullPageImageCompressed` and `CreateDictionary` also use compact binary fields. The type byte is authoritative and must match the decoded variant.

The decoder rejects an oversized length from the record header before waiting
for or materializing the payload. Heap-row and raw/compressed full-page bodies
are capped at 8 KiB, dictionary bodies at 112,640 bytes, and JSON payloads at 64
MiB. Physical byte bodies are copied only after a fallible exact reservation.
`CommitWithSubxids` additionally caps its list at 65,536 entries with a bounded
visitor, so JSON size cannot amplify into an unbounded transaction-id vector.

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

`replay_from(lsn)` is strictly exclusive: it inspects only records whose stored `record.lsn > lsn`. Recovery first pre-scans every retained post-checkpoint `CatalogChange`, regardless of transaction outcome, and merges every carried allocator high-water. It then replays physical page mutations under PageLSN gating, applies committed catalog change sets in LSN order, skips aborted/in-flight catalog publication while immediately applying their allocator reservations in WAL order, and replays sequence-value records unconditionally. In-order reservation is required because a later table before-image can already contain stable-column or FK IDs burned by an earlier aborted change. A complete committed change set is checked against its `before` objects, applied to a candidate snapshot, validated, and only then published; physical records never mutate catalog metadata. The merged high-water is reapplied after replay for sparse reservations whose relation did not exist when an earlier record was skipped.

`truncate_before(lsn)` is strictly exclusive in the opposite direction: it may remove records with `record.lsn < lsn` and must retain records with `record.lsn >= lsn`. Checkpoint calls `truncate_before(checkpoint_lsn)`, which may leave the boundary record in the WAL; recovery still ignores that boundary record because replay is strictly `> checkpoint_lsn`. **Unconditional truncation:** truncation drops every record below `lsn` — it does NOT pin aborted/in-flight transactions and does NOT touch the in-memory CLOG or floors. It is safe because the checkpoint calls `persist_clog` (which durably records every aborted outcome in `clog.dat`) *before* `truncate_before`, and under the exclusive checkpoint guard no write transaction is in flight, so every transaction below `lsn` is settled and captured by that snapshot (see "Durable CLOG snapshot" and `mvcc.md` §5.4/§8). **Precondition:** a caller must persist the CLOG snapshot covering `lsn` before truncating; the no-snapshot fallback (a pre-durable-CLOG data directory) instead relies on the WAL having been conservatively truncated by the older build.

`truncate_before` writes retained records to a temporary WAL file, fsyncs the temporary file, renames it over the live WAL, and immediately fsyncs the parent directory. If the parent-directory fsync — or the subsequent WAL reopen or seek — fails, the WAL manager is poisoned and returns the error before mutating retained-record in-memory state. Only after the rename is directory-durable may the manager reopen, seek, and replace in-memory WAL state.

Poisoning is not limited to truncation: the WAL manager is poisoned whenever it cannot undo a partial mutation — if `append` fails to roll back a partially written record, or if `flush` fails to roll back unflushed bytes. Once poisoned, every subsequent operation returns the poison error.

`bytes_after(lsn)` returns the total encoded byte length of retained WAL records whose stored `record.lsn > lsn`. It is used only for server checkpoint threshold accounting. If `lsn` is older than the first retained record after truncation, it returns the total encoded byte length of all retained records.

## Commit Protocol

For a successful write statement:

1. Catalog-changing statements append one authoritative `CatalogChange` for the
   complete statement mutation before dependent physical work; storage appends
   physiological redo records (`HeapInit`/`HeapInsert`/`HeapDelete`, or a
   `FullPageImage` on first page modification).
2. Server query orchestration appends `Commit`.
3. Server query orchestration calls `wal.flush()`.
4. The statement is durable and must not be rolled back.
5. Server query orchestration calls cleanup-only `storage.commit_txn(txn_id)` and `buffer_pool.commit(txn_id)`.
6. Success is returned to the client.

If cleanup fails after step 3, the server treats it as fatal and exits after flushing WAL. It must not call rollback because the durable `Commit` record means recovery will replay the statement.

For failed write statements:

1. Server query orchestration does not append `Commit`. It appends an `Abort` record (which records the transaction `Aborted` in the CLOG) before deregistering the transaction. Abort is not fsync-gated; a transaction with no durable `Commit` is recovered as aborted regardless.
2. Server query orchestration calls `storage.rollback_txn(txn_id)` and `buffer_pool.rollback(txn_id)`. These are metadata/bookkeeping cleanup under status-based abort, except storage may delete unpublished truncate replacement files that no committed catalog state can reference; heap and index page bytes are not undone.
3. Uncommitted or aborted physical WAL remains replayable and is hidden by the CLOG. Recovery does not publish its `CatalogChange` objects, but its allocator high-water remains burned so object and storage-generation identifiers cannot be reused. The pre-scan registers distinct orphan storage generations for physical redo without allowing an uncommitted metadata-only update to replace the committed compression configuration of a reused generation.

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
- Pre-scans every `CatalogChange` to reserve allocator high-water independent of commit status, then applies only committed change sets in LSN order. Physical page mutations are PageLSN-gated regardless of transaction outcome; sequence-value records replay unconditionally. Primary-key changes are detected from table before/after objects and schedule the derived identity-tree rebuild after replay and crashed-writer abort resolution.
- Before physical redo runs, the server normalizes a `FullPageImageCompressed` record to a decompressed raw `FullPageImage` (resolving `dict_id` against the dictionary resolver seeded from `<data>/dicts/` before redo begins) — an unresolvable `dict_id` at this point is a fatal structured recovery error, since it indicates a deleted/corrupted dictionary file rather than a normal crash state. `storage::apply_physical_redo` itself only ever sees the raw `FullPageImage` variant.
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
  aborted-creator tuple. At full-pass start the server captures
  `B = min(next_txn_id, oldest_active_xid)` after exclusion locks, treating no
  active xid as `next_txn_id`, and calls `set_vacuum_floor(B)` after pruning.
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
- Recovery seeds the CLOG from `clog.dat` and folds post-snapshot
  `Commit`/`Abort` records, rebuilding it from retained WAL only when the
  snapshot is absent. `replay_from` yields every redo record (redo-all), and
  visibility is decided by the CLOG rather than a replay filter.
- Decoupled truncation: `persist_clog` records an un-vacuumed aborted transaction, then unconditional `truncate_before` drops its `Abort` record — yet after reopen the snapshot keeps it `Aborted` (invisible), and repeated checkpoint+recovery cycles (with the recovery floor establisher) never resurrect it.
- Vacuum floor (F4c): after `set_vacuum_floor(B)`, the next `persist_clog` drops a reclaimed aborted transaction `< B` from the snapshot (it reads implicit-committed) — while an aborted transaction `>= B`, or one with no vacuum floor advanced, keeps its explicit `Aborted` entry; with no durable CLOG snapshot the floor falls back to its conservative value at reopen.
- Durable CLOG snapshot: `persist_clog` writes `clog.dat`; reopen seeds the CLOG and both floors from it and folds only the post-`clog_lsn` `Commit`/`Abort` records; an absent snapshot rebuilds from the WAL; a corrupt snapshot (CRC/version/structure) fails open. The snapshot envelope round-trips and rejects tamper/version/length/unsorted/overlapping payloads.
- Truncated WAL still replays from manifest checkpoint LSN.
- CRC detects corrupted record.
- Incomplete trailing record after crash is ignored.
- `FullPageImageCompressed` and `CreateDictionary` round-trip through compact binary encoding, `CatalogChange` round-trips through its bounded JSON decoder, non-finite statistics are rejected, specialized legacy catalog payloads are rejected, and malformed physical payloads fail decoding.
