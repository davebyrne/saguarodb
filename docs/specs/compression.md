# SaguaroDB Compression and TOAST Specification

**Date:** 2026-07-10
**Status:** Implemented feature specification

## 1. Summary

SaguaroDB implements transparent compression at three points:

1. **Pages at rest** — each 8 KiB page is individually compressed by
   `HeapPageStore` when flushed and decompressed when loaded, with the freed
   tail of the page's fixed 8 KiB file slot returned to the filesystem via
   hole punching. Controlled by a **per-table setting** declared at
   `CREATE TABLE` and changed by a new `ALTER TABLE ... SET (compression)`
   statement that rewrites the table in full.
2. **WAL full-page images** — every FPI payload is compressed
   **unconditionally** (independent of any table setting) before it is
   appended, and decompressed during replay.
3. **TOAST value payloads** — large `TEXT`/`BYTEA` values may be compressed
   inline or stored out of line in a hidden TOAST relation, using the same codec
   and dictionary registry as page/WAL compression.

Both use zstd. Cross-page redundancy is captured with **per-table trained
zstd dictionaries** shared by the at-rest, WAL, and dictionary-backed TOAST
value paths.

Everything happens below the buffer pool and below the logical WAL contract:
the buffer pool, executor, planner, and MVCC code see only uncompressed
8 KiB page images and logical row values. No logical page byte, PageLSN, TID,
or index entry changes meaning.

## 2. Goals and non-goals

**Goals**

- Reduce heap, index, and WAL disk footprint with zero hot-path (buffer-hit)
  cost. Compression CPU is paid on flush/append; decompression on buffer
  miss and replay.
- Store large `TEXT`/`BYTEA` values out of line through hidden TOAST relations
  while preserving MVCC visibility, VACUUM correctness, and logical index keys.
- Preserve every existing durability invariant: WAL-before-data, first-touch
  FPI torn-page repair, PageLSN-gated redo, checkpoint ordering, `page_count`
  semantics, stable `(page, slot)` TIDs.
- Share one codec/dictionary infrastructure across page envelopes, WAL FPI
  compression, and TOAST value payload compression.

**Non-goals (explicit)**

- Multi-page compression groups on the live store (breaks the torn-page
  repair invariant for undirtied group members, or requires COW machinery;
  grouping belongs to future sealed/archival segments).
- A page-map / copy-on-write store (byte-granular savings). The `PageStore`
  trait boundary is unchanged, so this remains a possible later evolution of
  `HeapPageStore` internals; the envelope defined here would be reused as
  the stored extent format.
- Per-table or runtime page size (see §12 for the forward-compatibility
  provisions we do make).
- Compression of non-FPI WAL records. Size-thresholded `HeapInsert` payload
  compression is documented follow-up work; tiny records (`HeapUpdateHeader`,
  `Commit`, `Abort`) lose to per-record framing overhead by construction.
- Dictionary garbage collection. Dictionary files are small (~100 KiB) and
  immutable; v1 never deletes them.
- A global server flag for table compression. The per-table setting is the
  only control surface for at-rest compression. WAL FPI compression has no
  knob at all (always on, with a per-record raw fallback).

## 3. `compress` crate (`saguarodb-compress`)

Leaf library crate wrapping zstd. Depends on `common` only (plus the
external `zstd` crate). Consumed by `storage` and `server`. `wal` does
**not** depend on it (WAL record types carry plain bytes plus codec/dict-id
fields; compression happens at the storage/recovery call sites).

Owns:

- **Codec registry** (`u8` codec ids): `0 = none`, `1 = zstd`,
  `2 = zstd + dictionary`. Unknown ids are structured corruption-class
  errors. Future codecs (e.g. lz4) allocate new ids.
- **Compression levels** (fixed constants in v1): zstd level 1 on the WAL
  append path (inside statement execution), zstd level 3 for pages at rest
  (background flush / eviction / rewrite), and zstd level 3 for TOAST value
  payloads.
- **Dictionary training** (`zstd`'s ZDICT) and dictionary handles for
  compress/decompress.
- **The at-rest page envelope** (§5).
- **TOAST value helpers** that compress/decompress raw value payload bytes
  without page envelopes and without deciding whether compression wins.
- **The dictionary file format** (§7).

The `CompressionSetting` enum (`None | Zstd`) lives in `common` so `catalog`
and `parser`/binder can reference it without depending on `compress`.

## 4. Per-table setting and SQL surface

- `TableSchema` carries `compression: CompressionSetting` and
  `active_dict_id: Option<u32>`.
- **`CREATE TABLE <name> (...) WITH (compression = 'none' | 'zstd')`** —
  optional trailing clause. Unknown option keys are rejected at parse
  time (`SqlState::SyntaxError`) and an unsupported codec value is
  `SqlState::FeatureNotSupported` — there is no bind step. Omitted ⇒ `none`.
  String literal values; unquoted identifiers are also accepted and
  lowercased per the identifier rules.
- **`ALTER TABLE <name> SET (compression = 'none' | 'zstd')`** — the first
  `ALTER` form in the grammar; every other `ALTER` remains rejected.
  Semantics in §8. Classified `StatementClass::Maintenance` and dispatched
  like `VACUUM`: autocommit only, rejected inside a transaction block, runs
  under the exclusive statement guard, and does not bind or plan — the
  binder never sees `AlterTableSetCompression`. The compression value
  (`'none'`/`'zstd'`) is validated at parse time; table existence is
  checked by `run_alter_table_compression` itself once it holds the guard.
- A table's catalog indexes and storage identity index inherit the table's
  setting for their files, but compress **dict-less** (a heap-trained
  dictionary does not fit B-tree node content; per-index dictionaries are
  future work).
- The setting governs **only the at-rest envelope** in the table's files.
  WAL FPI compression is unconditional and independent (§6).

## 5. At-rest page envelope (`HeapPageStore`)

### Format

A compressed page slot begins with an 18-byte envelope header:

```text
[0..4)   magic          = "SGCP" (0x53 0x47 0x43 0x50)
[4]      0xFF           (position of a raw page's PageType; 0xFF is invalid)
[5]      0xFF           (position of a raw page's PageVersion; 0xFF is invalid)
[6]      envelope format version = 1
[7]      codec id       (1 = zstd, 2 = zstd + dictionary; 0 never appears at rest)
[8..12)  dict_id  u32 LE (0 when codec = 1)
[12..14) payload length u16 LE
[14..18) CRC32 over payload, LE
[18..)   compressed payload
```

Detection on load: `bytes[0..6] == [SGCP, 0xFF, 0xFF]` ⇒ envelope; anything
else ⇒ raw page bytes. A valid raw v2 page always carries `PageVersion = 2`
at offset 5 and `PageType ∈ {1, 2}` at offset 4, so no raw page can collide
with the envelope marker. An all-zero slot (sparse hole / never-written) is
not an envelope and falls through to the existing raw-page handling.
Decompressed length must equal `PAGE_SIZE` exactly; anything else is a
corruption-class error. The decompressed image still carries the standard
page checksum, which is verified by the existing page validation — two
independent integrity layers.

`u16` payload length is sufficient: payloads are only stored compressed when
smaller than `PAGE_SIZE`, and the format supports page sizes up to 32 KiB
(§12), so payloads are always < 32768.

### Write path

`write_page(file_id, page_num, data)`:

1. Look up the file's `(codec, dict)` config (§5a). No config or
   `compression = none` ⇒ write the raw 8 KiB image exactly as today.
2. Compress the image. Compute the smallest whole number of filesystem
   blocks (assumed 4096 bytes; see note) that holds envelope + payload.
   If that is fewer blocks than `PAGE_SIZE / 4096`, write the envelope +
   payload **zero-padded out to a full `PAGE_SIZE` slot** at the page's
   normal `page_num * PAGE_SIZE` offset — not a short write of just the
   envelope bytes — and only then punch the trailing blocks with
   `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`. Writing the full
   slot before punching is what keeps `st_size` exact even when this page is
   the file's current tail: a short write there would leave the file shorter
   than `(page_num + 1) * PAGE_SIZE`, under-reporting `page_count` (and so
   under-seeding the allocator and truncating VACUUM's full-extent scan);
   `PUNCH_HOLE | KEEP_SIZE` never changes `st_size`, so writing the full slot
   first and punching after is exact regardless of whether the page is an
   interior page or the tail. Otherwise write the raw image (which naturally
   un-punches any prior hole). At 8 KiB pages this degenerates to: compressed
   slot iff envelope + payload ≤ 4096.
3. `EOPNOTSUPP`/`EINVAL` from `fallocate` ⇒ record the fact once per store
   and skip punching thereafter (correct, merely reclaims nothing).

`KEEP_SIZE` preserves `st_size`, so `page_count` (= `st_size / PAGE_SIZE`)
is untouched — allocator seeding and VACUUM's full-extent scan are
unaffected. Note: the 4096-byte block assumption is conservative; on a
filesystem with larger blocks the punch is a harmless no-op region-wise.

### Read path

`load_page`: read the full 8 KiB slot; if the envelope marker matches,
validate version/codec/CRC, resolve the dictionary (§7), decompress to
exactly `PAGE_SIZE`, return the image. Otherwise return the raw bytes as
today. Mixed encodings within one file are always legal and self-describing
— this is what makes the `ALTER` rewrite crash-tolerant and lets config
changes apply lazily to future writes.

### Corruption semantics

Envelope validation failure (bad version, unknown codec, CRC mismatch,
wrong decompressed length, unresolvable dict) is a **distinct structured
error kind** (corruption-class):

- Normal reads (`read_page`/`write_page` faulting) propagate it — loud,
  like any page corruption.
- `fetch_for_redo` (recovery) maps it to a **zeroed frame**, exactly like a
  missing page, so the post-checkpoint `FullPageImage` re-establishes it.
  This is sound: a torn page was mid-write ⇒ it was dirty ⇒ its first
  post-checkpoint modification logged an FPI that redo will replay. (This
  is strictly better than today's raw-page behavior, where a torn write
  yields garbage bytes whose garbage PageLSN the redo gate trusts.) This
  covers the `ALTER TABLE ... SET (compression)` rewrite too (§8): every
  page it re-encodes is preceded by its own `FullPageImage`, so a page torn
  mid-rewrite is repaired by the same fetch_for_redo → zeroed frame → FPI
  replay path as any other page write.

### 5a. Store configuration

`HeapPageStore` exposes an engine-facing config API (the `PageStore` trait and
the buffer pool are unchanged):

- `set_file_compression(file_id, CodecConfig)` — called by the storage
  engine when schemas are installed at startup/recovery, on `CREATE TABLE`,
  on `CREATE INDEX`, and on `ALTER TABLE ... SET (compression)`. Heap files
  get `(codec, active_dict_id)`; index files get the dict-less variant of
  the table's codec.
- `register_dictionary(dict_id, bytes)` — populates the in-memory
  dictionary resolver. Seeded at store open by scanning `<data>/dicts/`
  (§7) and updated when a dictionary is created.

A file with no registered config writes raw — always correct, since
envelopes are self-describing and mixed encodings are legal.

## 6. WAL full-page-image compression

- Binary WAL record type
  `FullPageImageCompressed { file_id, page_num, codec: u8, dict_id: u32, payload }`.
  The existing `FullPageImage` type remains and remains decodable. The
  record CRC already covers the payload; no envelope or second CRC inside
  the record.
- **Policy: unconditional.** Every FPI append site (first-touch-per-
  checkpoint images, every B-tree node image, VACUUM/prune/reclaim images)
  compresses the 8 KiB image with zstd level 1, using the owning table's
  active dictionary for **heap** pages when one exists and dict-less zstd
  otherwise (index pages: always dict-less). If the compressed payload is
  not smaller than `PAGE_SIZE`, the site emits a plain `FullPageImage`
  instead — per-record, self-describing, the WAL never expands.
- Compression happens in `storage` at record-construction time (storage
  owns the FPI sites and the file→dict mapping); `wal` just stores bytes.
- **Replay:** recovery decompresses the payload back to an exact-`PAGE_SIZE`
  image — resolving `dict_id` against the dictionary resolver — before
  handing it to `apply_physical_redo`, whose `len == PAGE_SIZE` contract
  and PageLSN-gating are unchanged. Resolution order is guaranteed by §7's
  durability rules. A `dict_id` that cannot be resolved during replay is a
  fatal structured recovery error (it indicates deletion/corruption of a
  dictionary file, not a normal crash state).
- `--checkpoint-wal-bytes` continues to measure appended (now smaller)
  bytes; checkpoints trigger correspondingly less often. No semantic
  change.

## 7. Dictionaries

- **Identity:** global monotonic `u32` dict ids allocated by the catalog
  (`next_dict_id` persisted with the catalog; replay advances it past any
  `CreateDictionary` record it sees, mirroring table/index id recovery).
  `0` is reserved for "no dictionary".
- **Artifact:** an immutable file `<data>/dicts/<dict_id>.dict`:

```text
[magic "SGDC"][format version u8][dict_id u32][table_id u32]
[payload length u32][CRC32 over payload][trained dictionary bytes]
```

  Written with the control-file pattern: temp file → fsync → rename →
  fsync directory. Never modified after creation; never deleted in v1.
- **WAL:** a binary logical record
  `CreateDictionary { dict_id, table_id, bytes }` is appended (and flushed
  with the creating statement's commit) so replay can resolve dict ids
  created after the last checkpoint. Replay installs the file if absent
  (recovery operations do not append WAL) and registers it with the
  resolver. The creating statement may be page-compression `ALTER TABLE ...
  SET (compression = 'zstd')` or TOAST value-compression `ALTER TABLE ...
  SET (toast_compression = zstd_dict)`. Like other DDL effects it is gated on
  the creating transaction's committed status via the rebuilt CLOG.
- **Durability order (load-bearing):** dictionary file durable → WAL record
  appended → anything (page envelope, WAL FPI record, or TOAST value payload)
  may reference the dict id. Consequently a referenced dictionary is always
  resolvable: at recovery, dictionaries referenced by post-checkpoint records
  or TOAST metadata/value payloads are either in `<data>/dicts/` already or
  installed by an earlier-LSN
  `CreateDictionary` record.
- **Boot-time validation:** after seeding the dictionary resolver from
  `<data>/dicts/` (and before replay), recovery (`open_app`) checks every
  catalog table whose CURRENT `active_dict_id` or `toast.active_dict_id` is
  `Some(id)` against the resolver; if `id` was not registered, recovery fails
  immediately with a structured internal error naming the table, dict field,
  and dict id, instead of silently proceeding dict-less and surfacing a
  confusing decode error much later on first read of a dict-compressed page or
  TOAST value. Under normal operation this can only fire if a `.dict` file was
  deleted or the `dicts/` directory was otherwise tampered with — the
  durability order above guarantees a reachable dictionary is never
  legitimately missing. The check covers only each table's CURRENT active dict
  fields; a HISTORICAL dict id referenced by an older
  `FullPageImageCompressed` WAL record is unchecked but always present too,
  since dict files are never deleted in v1.
- **Training (v1):** only during `ALTER TABLE ... SET (compression = 'zstd')`
  on a table with data (§8). Training samples are the table's decompressed
  heap page images, sampled evenly across the file, capped at 4096 pages
  (a 32 MiB corpus).
  If the corpus is too small for ZDICT to train, the `ALTER` proceeds
  dict-less — not an error. A freshly created zstd table therefore starts
  dict-less (plain zstd) until an `ALTER` retrains it; re-running the same
  `ALTER` retrains from current data and rewrites. Auto-training (e.g. at
  VACUUM once a table crosses a size threshold) is documented follow-up
  work.

## 8. `ALTER TABLE ... SET (compression = ...)` semantics

Runs autocommit-only under the exclusive statement guard (writers drained,
like `VACUUM` / `CREATE INDEX` backfill). Classified `StatementClass::Maintenance`
and dispatched before binding, exactly like `VACUUM` — the binder never sees
`AlterTableSetCompression`. Ordered steps:

Step 4's commit-record flush is the **durable commit point**. Steps 1-4
propagate an error normally as a statement error (nothing has committed yet).
Steps 5-8 are post-durable-commit cleanup: any error there is fatal
(`fatal_after_durable_commit` — logs, best-effort WAL flush, `process::exit`)
rather than a returned statement error, exactly like every other autocommit
write path (`docs/specs/crates/server.md`), because the DDL already committed
and misreporting it as failed would be worse than crashing. The exclusive
guard covers steps 1-7 and is released before the post-commit checkpoint
trigger runs (`record_commit_and_maybe_checkpoint_after_durable_commit`,
`docs/specs/crates/server.md`) — that call takes its own exclusive guard, so
calling it earlier would deadlock — which is also what makes the rewrite's
WAL activity in step 6 count toward `--checkpoint-wal-bytes` right away
instead of waiting on an unrelated later commit to notice it.

1. Parse: the compression value must be `'none'` or `'zstd'` (checked by the
   parser, `SqlState::FeatureNotSupported` otherwise). Table existence is
   NOT checked here (there is no bind step); `run_alter_table_compression`
   checks it below, once it holds the guard.
2. Take the exclusive guard; look up the table by name
   (`SqlState::UndefinedTable` if it does not exist).
3. If the new setting is `zstd` and the table has data: train a dictionary
   from current heap page images; persist the dict file (§7). If training
   is skipped/fails, `active_dict_id` becomes `None`.
4. Append + flush WAL: `CreateDictionary` (if trained) and a logical
   `AlterTableCompression { table_id, compression, active_dict_id }` DDL
   record, then the commit record (immediate-commit DDL, like other DDL).
   Recovery applies `AlterTableCompression` to the catalog CLOG-gated,
   exactly like other DDL records.
5. Update the in-memory catalog and the store's file configs (heap + all
   index files of the table).
6. **Rewrite pass (an FPI per page — torn-page repair, exactly like
   VACUUM):** for each page `0..page_count` of the heap file, the identity-index
   file, and every catalog-index file (skipping buffer-reported abandoned
   holes and pages that are not yet initialized): take its write guard,
   capture the current image, log it as a single unconditional
   `FullPageImage`/`FullPageImageCompressed` under the maintenance txn id
   (`VACUUM_TXN = 0`), and stamp the FPI's assigned LSN as the page's new
   PageLSN (`rewrite_table_pages`, mirroring `vacuum_heap` /
   `reclaim_line_pointers`). Logical bytes are unchanged — only the
   page-header PageLSN (and its checksum) advances. `wal.flush()` runs
   immediately after this pass and before the page flush (step 7): the
   rewrite FPIs must be durable write-ahead of the pages that now carry a
   higher PageLSN. `flush_dirty_pages` does not gate on PageLSN at all — it
   assumes the caller already flushed the WAL — so skipping this flush
   would not error loudly; it would let a torn page write precede its FPI
   being durable, i.e. silent corruption on recovery. A resident page that
   was ALREADY dirty (from other in-flight work) is likewise FPI-logged and
   re-stamped by this pass.
7. `flush_dirty_pages()` flushes the now-dirty pages through the buffer
   pool: `PageStore::write_page` re-encodes each flushable dirty page under
   the just-installed config — the envelope encode step (§5) runs here.
   Then `store.sync_all()`, then `buffer_pool.mark_all_clean()` —
   `flush_dirty_pages` does not itself mark frames clean (the caller fsyncs
   via the store and only then calls `mark_all_clean`); skipping it would
   not lose data, but would leave the rewrite's pages dirty and get them
   redundantly re-written at the next checkpoint. Release the guard, then
   trigger the post-commit checkpoint accounting, then return command tag
   `ALTER TABLE`.

**Crash behavior:**

- Crash before step 4's flush completes: the DDL did not commit; the CLOG
  marks it aborted/in-flight; replay skips the catalog change. A persisted
  dict file may be orphaned — harmless (unreferenced, small, GC is future
  work). The old setting stands.
- Crash during/after the rewrite: the catalog change is durable; files hold
  a mix of old- and new-encoding slots, every one self-describing and
  readable; subsequent writes follow the new setting. A page torn mid-write
  during the rewrite's own page flush (step 7) is **repaired by redo**
  replaying that page's `FullPageImage` from step 6, exactly like any other
  page-write path (§5) — it is not left corrupt, and recovery does not
  depend on the `ALTER` being re-run. The rewrite as a whole is still **not**
  resumed automatically past whatever page range it reached: re-running the
  same `ALTER` completes an interrupted (cleanly mixed-encoding) rewrite.
  This is documented behavior, not corruption.

`compression = 'none'`: steps 3 is skipped, `active_dict_id` becomes `None`,
and the rewrite writes raw images (un-punching holes as a side effect of
full-slot writes).

## 9. Control file

The control file carries a `page_size` field (added with a format version bump
per the durability rules). It is validated at open: a mismatch between the
binary's compile-time `PAGE_SIZE` and the data directory's recorded size is a
clean startup error naming both values — never reported as page corruption.
Existing data directories without the field are handled per the crate's
existing versioning policy (development builds do not migrate old formats).

## 10. What explicitly does not change

- `PAGE_SIZE` stays 8192, compile-time.
- The `PageStore` / `PageLoader` / `BufferPool` traits, guards, latching,
  eviction-steal, and checkpoint flow. (Steal and checkpoint call
  `write_page`, which now compresses — the WAL-durability-before-page-write
  ordering they already enforce is exactly what makes that safe.)
- Page format, row/tuple format, line pointers, TIDs, index formats, and
  the stable `(page, slot)` contract.
- `apply_physical_redo`'s contract (exact-`PAGE_SIZE` images, PageLSN
  gating).
- Recovery structure: schemas (and now dictionaries) install before redo;
  redo-all with CLOG-decided visibility.
- VACUUM, HOT, MVCC visibility — all operate on logical images above the
  seam.

## 11. Performance expectations (design targets, not promises)

- At-rest ceiling with 8 KiB pages over 4 KiB blocks: 50% per page,
  achieved for every page whose envelope + zstd payload ≤ 4096 bytes.
  Dictionary compression exists precisely to push more pages under that
  bar; B-tree pages (prefix-redundant) generally compress well dict-less.
- **The dictionary's at-rest payoff is page-size-dependent (measured).**
  Hole punching reclaims whole 4 KiB filesystem blocks, so a page occupies
  `ceil((envelope + payload) / 4096)` blocks and can never fall below one
  4 KiB block. A dictionary can only help by *lowering that block count*, so
  it is inert whenever the dict-less compressed page already fits in one
  block — which compressible data does at 8 KiB pages, and usually still does
  at 16 KiB. Larger pages hold more content, so the dict-less compressed form
  is likelier to spill past one block; the dictionary can then collapse it
  back toward a single block and cross a boundary. Throwaway 16/32 KiB builds
  over one page-filling row per page (~0.85 × page of shared,
  not-repeated-within-a-page boilerplate) measured a dictionary gain of
  **+0.0 pp at 8 KiB and 16 KiB, and +12.4 pp at 32 KiB** (75% → 87.5%);
  plain dict-less reduction also rose with page size (50% → 75% → 75%) as the
  fixed 4 KiB minimum became a smaller fraction of the page. Consequence: **at
  the shipped 8 KiB page the per-table dictionary buys essentially nothing at
  rest** — it begins to pay only at a 16/32 KiB build (§12), or for data
  compressible only just past one block. (In the WAL the dictionary is
  likewise marginal for insert-heavy workloads, whose FPI stream is dominated
  by dict-less B-tree node images.)
- WAL: FPI-dominated workloads should see roughly 2–4× fewer WAL bytes,
  with knock-on reduction in checkpoint frequency via
  `--checkpoint-wal-bytes`.
- CPU: zstd-1 ≈ 10–20 µs per 8 KiB image on the WAL/DML path; zstd-3 on
  flush paths only; decompress ≈ 2–5 µs on buffer miss. Buffer-hit reads
  and all in-memory mutation are untouched.

## 12. Forward compatibility: page size

Decisions here deliberately keep a future data-dir-creation-time page size
(8/16/32 KiB) cheap without building it now:

- The control file records `page_size` (§9) — the load-bearing item.
- All new code references `PAGE_SIZE` (never literal 8192/4096-derived
  constants) and expresses hole-punch math generically in whole filesystem
  blocks, which is already correct at 16/32 KiB.
- Larger pages are also what make the per-table **dictionary** earn its
  complexity: at 8 KiB the 4 KiB block quantum pins compressible pages to a
  single block regardless of the dictionary (§11 has the measured numbers), so
  a 16/32 KiB build variant is the first point at which a dictionary reduces
  the at-rest block count. If dictionaries are ever to pay off at rest, they
  are effectively coupled to shipping a larger-page build.
- The envelope is page-size-agnostic (explicit payload length; decompressed
  size must equal the data dir's page size). Dictionaries and WAL records
  are content/length-delimited and equally agnostic.
- Documented ceiling: heap line pointers store `[offset: u16][len: u16]`,
  so the page format supports at most 32 KiB pages; 64 KiB requires page-
  format surgery and is out of consideration.
- No `initdb` concept is needed later: first boot with an absent control
  file already *is* initialization; a future `--page-size` flag consumed
  only at that bootstrap (and validated against the control file thereafter)
  suffices.
- The upgrade ladder: (1) this feature's format insurance; (2) if demanded,
  compile-time 16/32 KiB binary variants (Postgres model) — no format
  changes needed; (3) only if one binary must serve mixed sizes, the
  mechanical `[u8; PAGE_SIZE]` → runtime-length refactor, which costs the
  same then as now and is therefore deferred.

## 13. Verification coverage

The implemented feature is covered at the codec, storage, WAL/recovery, and
server-integration layers. The coverage is intentionally split along the same
crate boundaries as the implementation:

- **`compress`:** compress/decompress roundtrips with and without
  dictionaries; envelope encode/decode/detect (incl. raw-page and all-zero
  non-collision); CRC tamper detection; unknown codec/version rejection;
  dict file encode/decode/CRC; training on small corpora (graceful
  dict-less fallback).
- **`storage`:** mixed raw/compressed files roundtrip through
  `load_page`/`write_page`; incompressible pages stored raw; `page_count`
  invariance under punching; torn/corrupt envelope → structured error on
  normal read, zeroed frame + FPI repair through the redo path; hole
  punching actually reclaims blocks (verified via `SEEK_HOLE`/`SEEK_DATA`,
  skipped when the fs does not support it); store config updates take
  effect on subsequent writes.
- **`wal`:** `FullPageImageCompressed` encode/decode; raw fallback when
  incompressible; replay decompresses (dict and dict-less) before redo;
  `CreateDictionary` replay installs and registers; dict-id high-water
  recovery.
- **`server` integration:** `CREATE TABLE ... WITH (compression = 'zstd')`
  → insert → restart → select roundtrip; `ALTER TABLE` rewrite in both
  directions (`none → zstd`, `zstd → none`) with correctness across
  restart; crash simulated mid-rewrite recovers with mixed encodings
  readable and re-running `ALTER` completes; recovery resolving a
  dictionary created after the last checkpoint; VACUUM on a compressed
  table; `ALTER` rejected inside a transaction block; unknown `WITH`
  options rejected; control-file `page_size` mismatch rejected cleanly;
  `ALTER`'s rewrite counts toward `--checkpoint-wal-bytes` and triggers a
  checkpoint on its own (checkpoint-accounting regression test); recovery
  fails fast, naming the table and dict id, when the catalog's active
  dictionary file is missing (boot-time validation regression test, §7);
  `ALTER TABLE ... SET (toast...)` updates future-write TOAST policy, can
  train a TOAST dictionary when the corpus is sufficient, survives restart,
  creates a hidden TOAST relation for legacy tables when needed, and rejects
  mixed page-compression/TOAST option lists.

## 14. Related crate specs

- `docs/specs/crates/compress.md` owns the codec, envelope, dictionary, and
  `CompressionRegistry` API contract.
- `docs/specs/crates/storage.md` owns the page-store integration, file
  compression config registration, TOAST row preparation/materialization,
  TOAST-aware VACUUM helpers, and `rewrite_table_pages`.
- `docs/specs/crates/wal.md` owns the durable record shapes
  `FullPageImageCompressed`, `CreateDictionary`, `AlterTableCompression`, and
  `AlterTableToast`, plus replay ordering.
- `docs/specs/crates/catalog.md` owns durable table compression metadata,
  TOAST metadata, hidden TOAST relation metadata, and dictionary-id allocation.
- `docs/specs/crates/parser.md` owns `CREATE TABLE ... WITH (...)`,
  `ALTER TABLE ... SET (compression = ...)`, and
  `ALTER TABLE ... SET (toast...)` parsing.
- `docs/specs/crates/server.md` owns maintenance-command dispatch,
  dictionary durability ordering, rewrite orchestration, and recovery
  dictionary seeding/validation.
- `docs/specs/crates/control.md` owns the control-file `page_size` field.
- `docs/specs/overview.md` owns the system-level SQL and storage summary.

## 15. Future work (recorded, not scoped)

- Dictionary GC (safe once no file slot, catalog reference, or retained WAL
  record references a dict id — natural after `ALTER` rewrite + checkpoint
  + truncation).
- Auto-training at VACUUM past a size threshold.
- Size-thresholded `HeapInsert` payload compression.
- Per-index dictionaries.
- lz4 codec id for CPU-constrained deployments.
- Page-map/COW store (byte-granular savings) behind the same trait, reusing
  the envelope as the extent format.
- Sealed multi-page segments for cold data (where multi-page compression
  groups are safe).
