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

`txn_id = 0` is reserved for non-transactional system metadata records. The `Checkpoint` marker is the exception that carries a non-zero `txn_id`: it stamps the transaction-id allocation high-water mark so the allocator boundary survives WAL recycling (see Checkpoint Interaction). No consumer treats the marker's `txn_id` as a real transaction (CLOG rebuild and redo key off the record *kind*); only the allocator seed reads it. User statement transaction IDs start at `FIRST_NORMAL_XID` (the allocator floors there so real transactions never stamp tuple headers with a reserved xid).

`Commit` and `Abort` carry no payload; the `txn_id` is in the header. `Commit` marks a transaction durably committed; `Abort` marks it aborted. Together they are the durable source of truth for transaction outcome and the input to CLOG reconstruction during recovery (see the MVCC plan, `docs/specs/mvcc.md` §5.4, §8). The CLOG is reconstructed at recovery; a durable CLOG snapshot (`clog.dat`, Milestone F — see "Durable CLOG snapshot" below) seeds it and lets recovery fold only the post-snapshot `Commit`/`Abort` records. A fresh, unrecycled WAL at replay floor zero may rebuild from a lazy retained-WAL scan when no snapshot exists.

`CommitWithSubxids` is the commit record for a transaction that had savepoint subtransactions (`docs/specs/savepoints.md` §5). It is identical to `Commit` except it carries the JSON `subxids` payload — the set of committed (live or released, not-rolled-back) subxids. Recovery and the runtime flush mark the header `txn_id` AND every `subxids` entry `Committed`, in one atomic durable record (so a concurrent crash never leaves a released subxid committed while its parent is not). A rolled-back subxid is recorded by its own `Abort` record (header `txn_id` = the subxid; `ROLLBACK TO SAVEPOINT` appends one per rolled-back subxid) and is absent from `subxids`. A no-savepoint commit still uses the plain `Commit` record, so its on-disk format is unchanged. The transaction-id allocator's recovery scan folds in `subxids` too, so a committed read-only subxid (present only in this payload) is never reissued.

`SequenceAdvance` and `SetSequenceValue` are non-transactional logical runtime records produced by `nextval` and `setval`. Recovery replays them unconditionally against the storage sequence runtime, after the checkpoint catalog snapshot has installed the baseline sequence set. They are not CLOG-gated DDL records: a sequence value handed out by an aborted transaction still creates a gap and is not reissued.

The physiological redo records (`HeapInit`, `HeapInsert`, `HeapDelete`, `HeapUpdateHeader`, `FullPageImage`) describe page-level changes. The storage mutation path produces them (stamping the page-LSN), and recovery replays them PageLSN-gated; `FullPageImage` provides torn-page recovery.

`HeapUpdateHeader` is an in-place mutation of a v2 tuple header — it sets the `xmax`, forward `t_ctid` pointer, and `infomask` of the live tuple at `slot` without relocating it (the three are fixed-width header fields, so the tuple keeps its exact offset and length and the page is not compacted). It is the MVCC substrate for `UPDATE`/`DELETE` version stamping (Milestone B commits 8–9, `docs/specs/mvcc.md` §5.3); the record and its redo handler land first, ahead of engine emission. Recovery replays it PageLSN-gated like the other heap records: it is skipped when `page_lsn >= record.lsn`, otherwise it rewrites the header (via `page::set_tuple_header`) and the primitive stamps `record.lsn`, so replay is idempotent.

### Compression records (`docs/specs/compression.md`)

These record kinds support WAL full-page-image compression and dictionary installation. Compression and TOAST catalog metadata are carried only by `CatalogChange`. `wal` has no dependency on `saguarodb-compress`; `storage` and `server` own compression and metadata application.

- **`FullPageImageCompressed { file_id, page_num, codec, dict_id, payload }`** (type byte `18`, compact binary, mirroring `FullPageImage`'s framing: `file_id`(4) + `page_num`(4) + `codec`(1) + `dict_id`(4) + `payload`). `payload` decompresses to exactly `PAGE_SIZE` bytes via the named `codec`/`dict_id`. Emitted by `storage::fpi_record_kind` in place of a plain `FullPageImage` only when it is smaller (unconditional compression attempt, per-record self-describing raw fallback — the WAL never expands, `compression.md` §6). The existing `FullPageImage` record and its redo handling are unchanged; a `FullPageImageCompressed` record is normalized to a decompressed `FullPageImage` by the caller (`server::apply_redo`) before physical redo runs, so `storage::apply_physical_redo` itself never sees the compressed variant (see "Replay" below).
- **`CreateDictionary { dict_id, table_id, bytes }`** (type byte `19`, compact binary: `dict_id`(4) + `table_id`(4) + raw trained-dictionary `bytes`). Installs an immutable per-table zstd dictionary. Appended (and flushed with the creating statement's commit) whenever `ALTER TABLE ... SET (compression = 'zstd')` trains one; the durability order is dictionary file written to `<data>/dicts/` **before** this record is appended, so a page envelope or a WAL FPI can only ever reference a dictionary id that is either already on disk or resolvable from an earlier-LSN `CreateDictionary` record (`compression.md` §7). Replay installs the dictionary file if it is not already present — **idempotent**, since `DictStore::save` is a no-op when the file already exists — and registers it with the in-memory dictionary resolver, so later records can resolve the same `dict_id`. It is classified alongside the other object-creating DDL records: `server` reserves the dictionary id even for a skipped aborted/in-flight record, so a later dictionary never reuses the same id.

All table compression, TOAST, TRUNCATE generation, primary-key, schema-evolution, view, schema, sequence, index, and statistics metadata is represented by the same `CatalogChange` record. Multi-object statements such as `CREATE TABLE` and multi-table TRUNCATE use one atomic change set. Physical page redo, `CreateDictionary`, transaction markers, and sequence value records remain separate.

The WAL format is version 2. LSNs are logical byte positions in the WAL stream,
and a record's stored LSN is the position immediately after its CRC. Records may
span segment boundaries. On disk, each record frame is:

```text
LSN: 8 bytes
TxnID: 8 bytes
Type: 1 byte
Length: 4 bytes
Payload: variable
CRC32: 4 bytes
```

CRC covers header and payload except the CRC field. `CatalogChange` uses JSON; physiological redo uses compact little-endian binary fields. `FullPageImageCompressed` and `CreateDictionary` also use compact binary fields. The type byte is authoritative and must match the decoded variant.

The stream is stored under `<data-dir>/wal/` in fixed 16 MiB payload segments
named by zero-padded uppercase hexadecimal segment number (`0000000000000000.wal`,
etc.). Every segment has a CRC-checked header containing WAL format version 2,
the segment number, logical start LSN, and payload size; header bytes are not part
of the logical LSN space. `wal.meta` is an atomically replaced, CRC-checked marker
whose envelope version 2 contains both the retained replay floor and the durable
stream end. `flush` fsyncs touched segments before advancing that durable end; a
returned commit is therefore always covered by the marker. The v2 WAL layout
intentionally does not read or migrate legacy `wal.dat` data directories.

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
    pub fn open(data_dir: impl AsRef<std::path::Path>) -> Result<Self>;
}

// `TxnStatusView` is a supertrait, so every WAL manager exposes CLOG status
// (`status`/`is_committed`/`is_aborted`) — see "Transaction status" below.
pub trait WalManager: Send + Sync + common::TxnStatusView {
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    fn flush(&self) -> Result<Lsn>;
    fn replay_from(&self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = Result<WalRecord>>>>;
    fn recycle_through(&self, lsn: Lsn) -> Result<()>;
    fn flushed_lsn(&self) -> Lsn;
    fn retained_range(&self) -> Result<(Lsn, Lsn)>;
    fn needs_clog_maintenance(&self) -> Result<bool>;
    fn bytes_after(&self, lsn: Lsn) -> Result<u64>;
    // Establish the CLOG implicit-committed floor at recovery (no-op when a durable
    // `clog.dat` snapshot was loaded; conservative re-derivation otherwise). See Invariants.
    fn establish_recovery_committed_floor(&self, allocation_boundary: u64) -> Result<()>;
    // Advance the vacuum floor (Milestone F4c): the boundary below which a full VACUUM
    // pass reclaimed every aborted-creator tuple, so `persist_clog`'s snapshot drops those
    // aborts' explicit entries and floats the floor past them (`recycle_through` does not
    // consult it — it is unconditional). Persisted in `clog.dat`. See Invariants.
    fn set_vacuum_floor(&self, boundary: TxnId) -> Result<()>;
    // Persist the durable CLOG snapshot (`clog.dat`) covering records through `clog_lsn`.
    // The checkpoint calls this after the control record is durable and before
    // `recycle_through`. See "Durable CLOG snapshot".
    fn persist_clog(&self, clog_lsn: Lsn) -> Result<()>;
    // Mark crashed writers that remain in-progress after replay as aborted without
    // appending WAL. The recovery checkpoint persists those outcomes in `clog.dat`.
    fn resolve_in_flight_as_aborted(&self, writer_xids: &HashSet<u64>) -> Result<()>;
}
```

`retained_range` returns the inclusive logical `(replay_floor, durable_end)` used
to validate a manifest checkpoint before recovery. `needs_clog_maintenance` lets
checkpoint orchestration force a full maintenance pass before the CLOG live window
approaches its durable format cap; this safety trigger remains active when the
dead-row auto-prune threshold is disabled.

The redo-committed-only `replay_committed_from` is **retired** (Milestone D2): recovery uses `replay_from` + the CLOG (redo-all). `is_redo_operation(kind)` (a free function, also re-exported) classifies a record as a replayable operation (everything except the transaction/checkpoint markers); redo-all applies those and skips the markers.

`append` assigns the next byte-position LSN and writes it into the encoded record.
Callers may pass `record.lsn = 0`; `append` ignores it. Appends write directly to
the active segment and create the next segment when a frame crosses a 16 MiB
boundary. The filled segment is fsynced before the successor's header is durably
installed, so a crash cannot expose a successor behind a torn earlier segment. Payload segments touched
since the prior flush are fsynced together by `flush`, which returns
the durable byte high-water mark.

`replay_from(lsn)` is strictly exclusive: it inspects only records whose stored `record.lsn > lsn`. Recovery first pre-scans every retained post-checkpoint `CatalogChange`, regardless of transaction outcome, and merges every carried allocator high-water. It then replays physical page mutations under PageLSN gating, applies committed catalog change sets in LSN order, skips aborted/in-flight catalog publication while immediately applying their allocator reservations in WAL order, and replays sequence-value records unconditionally. In-order reservation is required because a later table before-image can already contain stable-column or FK IDs burned by an earlier aborted change. A complete committed change set is checked against its `before` objects, applied to a candidate snapshot, validated, and only then published; physical records never mutate catalog metadata. The merged high-water is reapplied after replay for sparse reservations whose relation did not exist when an earlier record was skipped.

`recycle_through(lsn)` atomically advances `wal.meta`'s exclusive replay floor,
then unlinks only segments wholly below that floor. It never rewrites retained
records and does not consult or modify CLOG state. The checkpoint must first
call `persist_clog` at the current durable WAL end; that exact frame boundary is
the only advancing boundary accepted by `recycle_through`. This handshake rejects
mid-frame byte positions without scanning or indexing the retained WAL. A boundary segment remains until
a later checkpoint crosses its end, making reclamation O(the newly obsolete
segments) and independent of retained WAL size.

If append fails after a partial write, the torn stream is truncated back to the
previous byte LSN. If flush reports failure, the complete suffix after the prior
durable LSN is truncated and fsynced before the error is returned. Segment removal
is restart-safe: interruption can expose only a shorter valid stream or a partial
final frame that normal tail repair removes. The live manager is then poisoned so
every concurrent waiter sees failure rather than acknowledging a discarded commit;
reopening resumes from the corrected durable stream.

`wal.meta` replacement has an explicit outcome-unknown boundary: failures before
rename leave the old marker authoritative and the unflushed suffix can be discarded;
a directory-fsync failure after rename may leave either marker durable. The latter
returns `ErrorKind::DurabilityOutcomeUnknown`, poisons the manager, and server commit
orchestration terminates the process without abort cleanup or a success/failure reply.
Replay-floor marker failures likewise poison the manager because continuing with a
possibly advanced on-disk floor would let later flushes diverge from recovery.

`bytes_after(lsn)` is O(1): it reports logical stream bytes after the supplied byte
position, clamped to the retained range (zero at or beyond the current end).

## Commit Protocol

For a successful write statement:

1. Catalog-changing statements append and flush one authoritative `CatalogChange`
   for the complete statement mutation before dependent physical work. The change
   stays uncommitted/CLOG-gated, but allocator reservations survive failure or crash;
   storage then appends
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

1. Call `persist_clog(checkpoint_lsn)` to write the durable CLOG snapshot. This MUST happen before replay-floor advancement so the snapshot covers every outcome that becomes logically excluded.
2. Append `WalRecord { txn_id: <txn-id high-water>, kind: Checkpoint { redo_lsn }, .. }`. The marker's `txn_id` carries the transaction-id allocation high-water mark (highest id allocated so far) rather than the usual `0`, so recovery's allocator seed preserves the boundary after recycling. Recovery does not redo this metadata.
3. Flush WAL.
4. Call `recycle_through(checkpoint_lsn)`.

`recycle_through` must not remove records needed by the current control record. It does NOT touch the CLOG or its floors — `persist_clog` (run first) already pruned them to the live window and owns the durable floor.

## Durable CLOG snapshot

`clog.dat` is a sibling of the `wal/` directory (`<data-dir>/clog.dat`) that persists the transaction-status map across restart (`docs/specs/mvcc.md` §5.4). Its CRC-checked envelope version is 2; its JSON payload is the **live window** — the explicit `Committed`/`Aborted` ids at or above the implicit-committed floor — plus `clog_lsn`, `committed_floor`, and `vacuum_floor`.
The payload is capped at 64 MiB and each status list at 1,000,000 ids on both
encode and bounded decode; open checks the file size before allocating its buffer.

- `persist_clog(clog_lsn)` requires `clog_lsn == flushed_lsn`, computes the snapshot, writes it atomically (temp file + fsync + rename + parent-directory fsync), then prunes the in-memory CLOG to the same window. The write is **write-then-mutate**: a failed durable write leaves the in-memory floor unchanged, so the next open still reconciles against the previous snapshot. The successful snapshot establishes the exact frame boundary accepted by the following `recycle_through`. A failed `clog.dat` write does **not** poison the WAL — the snapshot is auxiliary, and the durable WAL records remain the source of truth.
- **At `open`**, when `clog.dat` is present the CLOG is seeded from it (statuses + both floors) and scanning begins at `clog_lsn`, folding only later `Commit`/`Abort` records. Already absorbed WAL is not decoded merely to ignore it. When the snapshot is absent, a fresh WAL whose replay floor is zero rebuilds from all retained records. If the replay floor has advanced, a missing snapshot is fatal because recycled status records cannot be reconstructed. A corrupt snapshot is likewise fatal.
- The decoder enforces the canonical live-window representation: committed and aborted lists are strictly sorted, duplicate-free, disjoint, bounded to one million entries each, and contain no transaction id below `committed_floor`. When an unreclaimed abort pins the floor, checkpoint requests a full maintenance pass once the in-memory status map reaches 750,000 entries, leaving headroom before either durable-list limit. Commit-only growth is pruned directly by the ordinary CLOG snapshot and does not scan user tables.
- Because the snapshot persists the `committed_floor` and `vacuum_floor`, both survive a clean restart (the no-snapshot fallback seeds them conservatively).

## Replay

Recovery (redo-all, Milestone D2):

- Reads the control record checkpoint LSN.
- Pre-scans every `CatalogChange` to reserve allocator high-water independent of commit status, then applies only committed change sets in LSN order. Physical page mutations are PageLSN-gated regardless of transaction outcome; sequence-value records replay unconditionally. Primary-key changes are detected from table before/after objects and schedule the derived identity-tree rebuild after replay and crashed-writer abort resolution.
- Before physical redo runs, the server normalizes a `FullPageImageCompressed` record to a decompressed raw `FullPageImage` (resolving `dict_id` against the dictionary resolver seeded from `<data>/dicts/` before redo begins) — an unresolvable `dict_id` at this point is a fatal structured recovery error, since it indicates a deleted/corrupted dictionary file rather than a normal crash state. `storage::apply_physical_redo` itself only ever sees the raw `FullPageImage` variant.
- Reconstructs the CLOG at `open`: seeded from `clog.dat` plus post-snapshot status records, or rebuilt from all records only for an unrecycled replay-floor-zero WAL. The CLOG — not a replay filter — decides visibility.
- Seeds `next_txn_id` by scanning `replay_from(0)` over all retained records (including `CommitWithSubxids.subxids` and the `Checkpoint` marker's high-water), not just the post-checkpoint redo range.

Replay lazily decodes one frame at a time from disk and crosses segment headers
without materializing the retained WAL in RAM. On open, every physical byte beyond
`wal.meta`'s durable end is an unacknowledged crash tail and is discarded without
decoding, whether it is partial or full-length/checksum-invalid. Records at or below
the durable end are decoded strictly; incomplete or checksum-invalid durable bytes
are fatal corruption. A short highest segment header created by a crash during
rollover is removed before tail discovery. New appends continue at the durable end.

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
  exists (seed + fold post-`clog_lsn` records) or, for a replay-floor-zero WAL, by scanning
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
  status records are below the replay floor while their flushed tuples survive in the
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
    entry); when no such abort exists it advances past the latest settled status,
    so commit-only workloads retain no growing xid list. Everything below is
    committed or a VACUUM-reclaimed abort.
  - At recovery the floor is **loaded from the snapshot**;
    `establish_recovery_committed_floor` is a no-op when a snapshot was loaded. The
    no-snapshot fallback re-derives it as
    `min(allocation_boundary, oldest_non_committed_retained_xid)` — never above the
    oldest retained transaction whose CLOG status is not `Committed`.

  Together this guarantees an aborted-but-flushed transaction stays invisible across a
  checkpoint and restart even though WAL replay-floor advancement is unconditional with respect to transaction status. Letting the floor
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
  vacuously correct). WAL `recycle_through` does NOT consult the vacuum floor (it is
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
- Decoupled recycling: `persist_clog` records an un-vacuumed aborted transaction, then `recycle_through` advances past its `Abort` record — yet after reopen the snapshot keeps it `Aborted` (invisible), and repeated checkpoint+recovery cycles never resurrect it.
- Vacuum floor (F4c): after `set_vacuum_floor(B)`, the next `persist_clog` drops a reclaimed aborted transaction `< B` from the snapshot (it reads implicit-committed) — while an aborted transaction `>= B`, or one with no vacuum floor advanced, keeps its explicit `Aborted` entry; with no durable CLOG snapshot the floor falls back to its conservative value at reopen.
- Durable CLOG snapshot: `persist_clog` writes `clog.dat`; reopen seeds the CLOG and both floors from it and folds only post-snapshot status records. An absent snapshot rebuilds only at replay floor zero and fails open after recycling; corruption also fails open.
- Recycled WAL still replays from the manifest checkpoint LSN.
- CRC detects corrupted record.
- Incomplete trailing record after crash is ignored.
- `FullPageImageCompressed` and `CreateDictionary` round-trip through compact binary encoding, `CatalogChange` round-trips through its bounded JSON decoder, non-finite statistics are rejected, specialized legacy catalog payloads are rejected, and malformed physical payloads fail decoding.
