# `compress` Crate Specification

**Date:** 2026-07-04
**Status:** Living crate contract

## Purpose

`compress` owns the compression codecs, the at-rest page envelope, TOAST
value-level compression helpers, per-table dictionary training and the durable
dictionary-file format, and the shared `CompressionRegistry` that the at-rest
heap-page path, WAL full-page-image path, and dictionary-backed TOAST value
path consult (`docs/specs/compression.md`). It knows nothing about pages,
files, WAL records, rows, or the catalog — callers pass raw bytes in and get
raw, compressed, or enveloped bytes back; every policy decision (which file
compresses, which dictionary a file uses, whether a compressed value is worth
storing) lives in the caller's configuration and size checks, not in this crate.

## Depends On

- `common`
- `crc32fast` — envelope and dictionary-file CRC32 checksums
- `zstd` — the codec (bulk compress/decompress, `ZDICT` training, prepared
  encoder/decoder dictionaries)
- `parking_lot` — `CompressionRegistry`'s internal locking

Leaf crate: no dependents besides `common`. Consumed by `storage` (at-rest
`HeapPageStore` envelopes, the engine's WAL-FPI compression, and TOAST value
payload compression/decompression) and `server` (constructs and shares one
`CompressionRegistry` instance, owns the
`DictStore`). `wal` does **not** depend on `compress`: its record types carry
plain codec-id/dict-id fields and already-compressed bytes; compression and
decompression happen at the `storage`/`server` call sites (`docs/specs/crates/wal.md`,
`docs/specs/crates/storage.md`).

## Public API

```rust
// Codec ids shared by page/WAL compression and TOAST value payloads.
// CODEC_NONE is valid only for value payloads; page envelopes and compressed
// WAL FPIs must use a real codec.
pub const CODEC_NONE: u8 = 0;
pub const CODEC_ZSTD: u8 = 1;
pub const CODEC_ZSTD_DICT: u8 = 2;

// Fixed zstd levels: at-rest and TOAST value compression run off the hottest
// DML path, while WAL FPI compression runs inline on the DML path.
pub const LEVEL_AT_REST: i32 = 3;
pub const TOAST_ZSTD_LEVEL: i32 = LEVEL_AT_REST;
pub const LEVEL_WAL: i32 = 1;

// Envelope layout constants (see byte-layout table below).
pub const ENVELOPE_MARKER: [u8; 6] = [b'S', b'G', b'C', b'P', 0xFF, 0xFF];
pub const ENVELOPE_VERSION: u8 = 1;
pub const ENVELOPE_HEADER_LEN: usize = 18;

pub struct Envelope<'a> {
    pub codec: u8,
    pub dict_id: u32,
    pub payload: &'a [u8],
}

pub fn is_envelope(slot: &[u8]) -> bool;
pub fn encode_envelope(codec: u8, dict_id: u32, payload: &[u8]) -> Result<Vec<u8>>;
pub fn decode_envelope(slot: &[u8]) -> Result<Envelope<'_>>;

pub fn compress_value_zstd(raw: &[u8]) -> Result<Vec<u8>>;

/// A table/index file's at-rest compression config (`compression.md` §4/§5a).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FileCompression {
    #[default]
    None,
    Zstd { dict_id: Option<u32> },
}

/// Shared FileId → config map plus dictionary resolver. `Send + Sync`
/// (asserted at the type declaration, so a future non-Send/Sync field fails
/// to compile here rather than downstream): one instance is constructed by
/// the server and injected into both `storage::HeapPageStore` (at-rest) and
/// `storage::PageBackedStorageEngine` (WAL FPIs).
pub struct CompressionRegistry { /* private */ }

impl CompressionRegistry {
    pub fn new() -> Self;

    pub fn set_file_config(&self, file_id: FileId, config: FileCompression);
    pub fn file_config(&self, file_id: FileId) -> FileCompression;

    pub fn register_dictionary(&self, dict_id: u32, bytes: &[u8]) -> Result<()>;
    pub fn has_dictionary(&self, dict_id: u32) -> bool;

    pub fn compress_page_at_rest(&self, file_id: FileId, image: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn decompress_page(&self, slot: &[u8], expected_len: usize) -> Result<Option<Vec<u8>>>;

    pub fn compress_fpi(&self, file_id: FileId, image: &[u8]) -> Option<(u8, u32, Vec<u8>)>;
    pub fn decompress_fpi(
        &self,
        codec: u8,
        dict_id: u32,
        payload: &[u8],
        expected_len: usize,
    ) -> Result<Vec<u8>>;

    pub fn compress_value_zstd_dict(&self, dict_id: u32, raw: &[u8]) -> Result<Vec<u8>>;
    pub fn decompress_value(
        &self,
        codec: u8,
        dict_id: Option<u32>,
        payload: &[u8],
        raw_len: usize,
    ) -> Result<Vec<u8>>;
}

/// Train a zstd dictionary from page-image samples (`compression.md` §7).
pub fn train_dictionary(samples: &[Vec<u8>]) -> Option<Vec<u8>>;
pub fn train_dictionary_cancelable(samples: &[Vec<u8>], cancel: &QueryCancel) -> Result<Option<Vec<u8>>>;

/// Immutable dictionary files under `<data>/dicts/<dict_id>.dict`.
pub struct DictStore { /* private */ }

impl DictStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self>;
    pub fn save(&self, dict_id: u32, table_id: u32, bytes: &[u8]) -> Result<()>;
    pub fn load_all(&self) -> Result<Vec<(u32, u32, Vec<u8>)>>;
}
```

### Envelope functions

- `is_envelope(slot)` — `true` iff `slot` is at least 6 bytes and its first 6
  bytes equal `ENVELOPE_MARKER`. Pure prefix check; does not validate the rest
  of the header.
- `encode_envelope(codec, dict_id, payload)` — validates `codec` is a real
  page-envelope codec (`CODEC_ZSTD` or `CODEC_ZSTD_DICT`) and builds the
  18-byte header plus `payload`. `Err` if `codec` is `CODEC_NONE`/unknown or
  if `payload.len()` does not fit `u16` (the length case is unreachable in
  practice: at-rest payloads are only ever stored compressed when smaller than
  `PAGE_SIZE`, and WAL FPI payloads are checked the same way before this is
  called).
- `decode_envelope(slot)` — validates the marker, format version, codec id,
  declared length against the slice actually present, and the payload CRC, and
  returns the borrowed `Envelope`. `Err` (a structured corruption-class
  `DbError`, `SqlState::InternalError`) on any mismatch: not-an-envelope,
  truncated header, unknown version, unknown codec, value-only `CODEC_NONE`, a
  declared length that would read past the end of `slot`, or a CRC mismatch.
  `decode_envelope` does **not** decompress or check the decompressed length — that is a
  `CompressionRegistry` concern (`decompress_page`/`decompress_fpi`), so this
  function has no dependency on `PAGE_SIZE`.

### TOAST value helpers

- `compress_value_zstd(raw)` — compresses a single value payload with dict-less
  zstd at `TOAST_ZSTD_LEVEL` and returns the bytes. It does **not** compare
  compressed size against raw size and does **not** add a page envelope or any
  TOAST-specific header; storage owns those decisions and wrappers.
- `CompressionRegistry::compress_value_zstd_dict(dict_id, raw)` — same policy
  boundary as `compress_value_zstd`, but uses the registered prepared
  dictionary for `dict_id`. This is a registry method rather than a free
  function because a durable `dict_id` is only meaningful after resolution
  through the registry. An unregistered id is a structured corruption-class
  error.
- `CompressionRegistry::decompress_value(codec, dict_id, payload, raw_len)` —
  decodes a single TOAST value payload to exactly `raw_len` bytes. `CODEC_NONE`
  returns the raw payload after a length check and requires `dict_id == None`;
  `CODEC_ZSTD` uses dict-less zstd and also requires `dict_id == None`;
  `CODEC_ZSTD_DICT` requires `Some(dict_id)` and a registered dictionary.
  Unknown codecs, invalid codec/dictionary pairings, zstd failures, and
  wrong decompressed lengths are structured corruption-class errors.

### `CompressionRegistry`

- `set_file_config`/`file_config` — a plain last-writer-wins `FileId → FileCompression`
  map. A file with **no** entry reads as `FileCompression::None` (raw at rest);
  this is always correct because envelopes are self-describing and mixing raw
  and compressed slots in one file is legal.
- `register_dictionary(dict_id, bytes)` — `Err` if `dict_id == 0` (reserved to
  mean "no dictionary"); otherwise prepares and installs three handles per
  dictionary — an at-rest encoder (`LEVEL_AT_REST`), a WAL encoder
  (`LEVEL_WAL`), and one decoder. TOAST dictionary value compression reuses the
  at-rest encoder because `TOAST_ZSTD_LEVEL == LEVEL_AT_REST` in v1. These
  prepared handles amortize zstd's dictionary-processing
  cost across every later use rather than redoing it per page.
- `has_dictionary(dict_id)` — membership probe (used by `server` recovery when
  reconciling durable dictionary files against WAL `CreateDictionary` records).
- `compress_page_at_rest(file_id, image)` — `Ok(None)` means "store `image`
  raw": either the file's config is `FileCompression::None`, or the encoded
  envelope would not be smaller than `image` (raw always wins a tie).
  `Ok(Some(envelope_bytes))` otherwise, using `CODEC_ZSTD_DICT` plus the
  file's configured dictionary when one is both configured **and** registered,
  else dict-less `CODEC_ZSTD`. `Err` only on an internal zstd failure or the
  `encode_envelope` payload-size guard (unreachable at `PAGE_SIZE`).
- `decompress_page(slot, expected_len)` — `Ok(None)` means "use `slot` as-is":
  it is not an envelope (a raw page, or an all-zero sparse hole). `Ok(Some(image))`
  on a valid envelope, decompressed to exactly `expected_len` bytes. `Err` is
  always corruption-class: a structurally invalid envelope (bad version/codec,
  CRC mismatch, truncated), an unresolvable `dict_id`, or a decompressed
  length that does not equal `expected_len`.
- `compress_fpi(file_id, image)` — **unconditional**: called independent of
  the file's at-rest config (a file with no at-rest config at all still gets
  compressed WAL images). `None` means "append the raw image instead" (the
  compressed payload was not smaller than `image`, or any internal zstd
  failure — compression must never fail a WAL append). `Some((codec, dict_id,
  payload))` otherwise, preferring the file's registered dictionary at
  `LEVEL_WAL` when one is configured and resolvable, dict-less zstd otherwise.
- `decompress_fpi(codec, dict_id, payload, expected_len)` — the shared
  decompress primitive behind both `decompress_page` and WAL FPI replay.
  `Err` on an unknown codec id, an unresolvable `dict_id` (the dictionary is
  not registered), an internal zstd failure, or a decompressed length that
  does not equal `expected_len`.

### Training and the dictionary store

- `train_dictionary(samples)` — `None` when `samples.len() < 8` (too small a
  corpus for `zstd::dict::from_samples` to train usefully) or when ZDICT
  training itself fails. Callers treat both as "proceed dict-less" — training
  failure is never a statement error. Trained dictionaries are capped at
  ~110 KiB (`MAX_DICT_BYTES`, zstd's customary ceiling).
- `train_dictionary_cancelable(samples, cancel)` — the foreground-DDL wrapper.
  ZDICT runs on a side-effect-free helper thread because it has no interruption
  callback; one process-wide permit bounds training to a single job, thread creation
  is fallible, and the caller polls `cancel` every 10 ms. Cancellation may return
  while that one bounded training job finishes in the background.
- `DictStore::open(dir)` — creates `dir` if it does not already exist.
- `DictStore::save(dict_id, table_id, bytes)` — **idempotent**: if
  `<dict_id>.dict` already exists it returns `Ok(())` without touching the
  file, which is exactly what makes replaying a `CreateDictionary` WAL record
  safe at recovery. A fresh save writes the framed bytes to
  `<dict_id>.dict.tmp`, fsyncs the temp file, renames it over `<dict_id>.dict`,
  then fsyncs the directory — the same temp-file/fsync/rename/fsync-directory
  pattern the control record uses.
- `DictStore::load_all()` — reads every `*.dict` file in the directory,
  CRC-validates each, and returns `(dict_id, table_id, bytes)` triples in
  directory-iteration order (callers sort if they need id order). A bad
  magic, unknown format version, truncated payload, or CRC mismatch is a
  structured `Err`.

## Envelope byte layout (`docs/specs/compression.md` §5)

A compressed page slot begins with an 18-byte header:

```text
[0..4)   magic          = "SGCP"
[4]      0xFF              (position of a raw page's PageType; 0xFF is invalid)
[5]      0xFF              (position of a raw page's PageVersion; 0xFF is invalid)
[6]      envelope format version = 1
[7]      codec id       (1 = zstd, 2 = zstd + dictionary; 0 is rejected)
[8..12)  dict_id  u32 LE    (0 when codec = 1)
[12..14) payload length u16 LE
[14..18) CRC32 over payload, LE
[18..)   compressed payload
```

Detection (`is_envelope`) checks only `bytes.starts_with(ENVELOPE_MARKER)`
(`"SGCP"` + `0xFF` + `0xFF`). A valid raw v2 page always carries
`PageVersion = 2` at offset 5 and `PageType ∈ {1, 2}` at offset 4, so no raw
page can collide with the marker; an all-zero slot (a sparse hole) is not an
envelope either and falls through to raw-page handling. `u16` payload length
is sufficient because a page is only ever stored compressed when the envelope
is smaller than the page, and the format supports page sizes up to 32 KiB
(`compression.md` §12), so a stored payload is always `< 32768` bytes.

## Dictionary-file byte layout (`docs/specs/compression.md` §7)

```text
[magic "SGDC"][format version u8][dict_id u32 LE][table_id u32 LE]
[payload length u32 LE][CRC32 over payload, LE][trained dictionary bytes]
```

21-byte header (`4 + 1 + 4 + 4 + 4 + 4`) followed by the `ZDICT`-trained
dictionary payload. Written with the control-file durability pattern (temp
file → fsync → rename → fsync directory). Decode requires the declared payload
length to consume the file exactly and rejects truncation, trailing bytes,
payloads above `MAX_DICT_BYTES`, and CRC mismatch as corruption. The file is
never modified after creation — dictionary files are small and immutable;
garbage collection is future work (`compression.md` §15).

## Acceptance Tests

- Envelope round-trips with and without a dictionary id; detection correctly
  distinguishes a raw v2 page and an all-zero slot from an envelope; CRC
  tamper, version tamper, value-only `CODEC_NONE`, unknown-codec tamper, and truncation are all
  rejected by `decode_envelope`; an oversized payload (`> u16::MAX`) is
  rejected by `encode_envelope`.
- Dictionary files round-trip through durable save/load and reject CRC
  tampering, trailing bytes, and payloads above the supported bound.
- `CompressionRegistry`: a file with no config round-trips raw; a configured
  file's at-rest round-trip through `compress_page_at_rest`/`decompress_page`
  works both dict-less and with a registered dictionary; an incompressible
  (high-entropy) image stays raw at both the at-rest and the unconditional WAL
  path; WAL FPI compression is unconditional — it compresses even a file with
  no at-rest config at all — and prefers a registered dictionary over
  dict-less zstd when one is configured; an unresolvable dictionary id is a
  structured error on decompress; the `CompressionRegistry: Send + Sync`
  assertion holds.
- TOAST value helpers: dict-less zstd value compression round-trips;
  dictionary-backed value compression round-trips with a trained test
  dictionary; `CODEC_NONE` raw value payloads are accepted only through
  `decompress_value`; wrong `raw_len`, unknown codecs, invalid codec/dict-id
  pairings, and page envelopes using `CODEC_NONE` are structured errors.
- `train_dictionary` returns `None` on an empty corpus and on a corpus below
  the minimum sample count.
- `DictStore` save/load round-trips with CRC validation; re-`save`ing an
  already-present dictionary id is a no-op (idempotent replay); a tampered
  dictionary file fails `load_all`.
- A bulk zstd compress/decompress round-trip over a representative 8 KiB page
  image sanity-checks the external zstd API shapes the registry relies on.
