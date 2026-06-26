# SaguaroDB COPY Specification

**Date:** 2026-06-26
**Status:** Draft

## 1. Overview

`COPY` is SaguaroDB's bulk data-transfer command. It moves rows between a table
and the client connection using the PostgreSQL COPY sub-protocol. Two directions
are supported:

- **`COPY <table> [(columns)] FROM STDIN [WITH (...)]`** — bulk *import*: the
  client streams row data to the server.
- **`COPY <table> [(columns)] TO STDOUT [WITH (...)]`** — bulk *export*: the
  server streams the table's rows to the client.

COPY is a *non-relational* utility command in the same family as `VACUUM`: it
does not produce a result set and is only valid through the simple-query
protocol. It reuses the normal MVCC write path (`COPY FROM`) and scan path
(`COPY TO`); it is not a new storage primitive.

### Supported

- `FROM STDIN` and `TO STDOUT` over the wire.
- `text` (default) and `csv` formats.
- Optional explicit column list; default is all table columns in catalog order.
- `WITH` options: `FORMAT`, `DELIMITER`, `NULL`, `HEADER`, and (CSV only)
  `QUOTE` and `ESCAPE`, in both the modern parenthesized form
  (`WITH (FORMAT csv, HEADER true)`) and the legacy bare form (`WITH CSV HEADER`).
- Autocommit (standalone) COPY and COPY inside an explicit `BEGIN`/`COMMIT`
  block.

### Rejected (structured errors, never silent)

- Server-side files: `COPY ... FROM 'filename'` / `TO 'filename'`
  (`FeatureNotSupported`, `0A000`). SaguaroDB has no authentication, so exposing
  the server filesystem is out of scope.
- `COPY (query) TO STDOUT` (`FeatureNotSupported`). Only whole-table export.
- `FORMAT binary` (`FeatureNotSupported`).
- COPY through the extended query protocol (`Parse`/`Bind`/`Execute`)
  (`FeatureNotSupported`) — matches PostgreSQL, which only allows COPY via a
  simple `Query`.
- Unknown table (`UndefinedTable`, `42P01`), unknown / duplicate columns
  (`UndefinedColumn`, `42703`), unknown options or option type errors
  (`SyntaxError`/`FeatureNotSupported`).

## 2. Grammar

```
COPY table_name [ ( column_name [, ...] ) ]
    { FROM STDIN | TO STDOUT }
    [ [ WITH ] ( option [, ...] ) ]
    [ WITH ] ( legacy bare option list )

option:
    FORMAT    { text | csv }
    DELIMITER 'char'
    NULL      'string'
    HEADER    [ boolean ]
    QUOTE     'char'      -- CSV only
    ESCAPE    'char'      -- CSV only
```

Identifiers are normalized to lowercase (quoted identifiers remain unsupported,
per the overview spec). The bound, normalized options are:

```rust
pub struct CopyOptions {
    pub format: CopyFormat,        // Text (default) | Csv
    pub delimiter: char,           // default '\t' (text), ',' (csv)
    pub null_string: String,       // default "\\N" (text), "" (csv)
    pub header: bool,              // default false; valid for text and csv
    pub quote: char,               // csv only, default '"'
    pub escape: char,              // csv only, default = quote
}
```

### Option validation rules

- `FORMAT binary` → `FeatureNotSupported`.
- `DELIMITER`, `QUOTE`, `ESCAPE` must each be a single character.
- `QUOTE` and `ESCAPE` are CSV-only; specifying either with `FORMAT text` →
  `FeatureNotSupported`. `HEADER` is valid for both formats.
- `DELIMITER` must differ from `QUOTE`; neither may be `\r` or `\n`.
- `DELIMITER` may not be a backslash (`\`) in any format — matching PostgreSQL.
  In text format `\` is the escape introducer, so a backslash delimiter would
  make field parsing ambiguous (a silent-corruption hazard rather than a
  structured error).
- Unknown option name → `SyntaxError`.

## 3. Formats

Values are UTF-8 (`client_encoding = UTF8`). The three scalar types map to text
as elsewhere in the system: `Integer` = decimal `i64`, `Boolean` = `t`/`f` on
output, `Text` = raw UTF-8. Field parsing on input:

- `Integer`: parsed as `i64`; a malformed value (e.g. `abc`, `1.5`, overflow)
  → `InvalidTextRepresentation` (`22P02`).
- `Boolean`: accepts `t`/`f`/`true`/`false`/`y`/`n`/`yes`/`no`/`on`/`off`/`1`/`0`
  (case-insensitive), via the shared `common::parse_bool_text` decoder (which the
  extended-protocol parameter path also uses); a `None` result maps to
  `InvalidTextRepresentation` on the COPY path.
- `Text`: taken verbatim after format unescaping.

### 3.1 Text format

- Columns separated by `DELIMITER` (default TAB).
- Rows separated by `\n`; `\r\n` is accepted on input (the trailing `\r` is
  stripped). Output uses `\n`. The final input row need not end with a newline.
- NULL is detected by comparing the **raw (un-de-escaped) field token** to the
  `NULL` option string (default `\N`). `\N` is the default NULL *sentinel*, not a
  character escape: a non-NULL text value of `\N` is written as `\\N` (the
  backslash is escaped) and read back from the raw token `\\N`, which differs
  from the `\N` sentinel and de-escapes to the literal `\N`. With a custom NULL
  string (e.g. `NULL 'NULL'`), the raw token `NULL` is NULL and `\N` is not.
- For a non-NULL field, backslash escapes are recognized on input and emitted on
  output: `\\` → `\`, `\t` → TAB, `\n` → LF, `\r` → CR (and `\b \f \v` are
  decoded on input). On output the backslash, the active delimiter, LF, CR, and
  TAB inside a field are escaped. An unrecognized `\x` decodes to the literal
  character `x` (PostgreSQL behavior).
- `CopyDone` is the sole end-of-data terminator. The legacy `\.` marker is **not**
  treated specially in either format: `psql` consumes it client-side and never
  sends it as data over the v3 protocol, so honoring it would add streaming-drain
  complexity for a case that does not occur.

### 3.2 CSV format

- Columns separated by `DELIMITER` (default `,`).
- Rows separated by `\n` or `\r\n` on input; `\n` on output. The final input row
  need not end with a newline.
- `QUOTE` (default `"`) wraps fields; `ESCAPE` (default = `QUOTE`) escapes an
  embedded quote. A quoted field may contain the delimiter, quote (doubled or
  escaped), CR, and LF.
- NULL is the `NULL` option string, default the empty string. An *unquoted*
  field equal to the NULL string is NULL; a *quoted* empty field (`""`) is an
  empty `Text` value, never NULL.
- Output quotes a field when it contains the delimiter, quote, CR, or LF, or
  when it equals the (unquoted) NULL string. Inside a quoted field, an embedded
  `QUOTE` (and the `ESCAPE` character itself) is prefixed with the active
  `ESCAPE` character — which reduces to doubling the quote in the default
  `ESCAPE` = `QUOTE` case, and round-trips with the input parser.

### 3.3 HEADER

- `COPY TO ... HEADER`: the first emitted line is the column names (in the COPY
  column order), formatted per the active format.
- `COPY FROM ... HEADER`: the first input line is read and discarded without
  validation (no `MATCH` semantics).

## 4. Wire protocol

The `protocol` crate gains COPY messages. Big-endian, one-byte tag plus
four-byte length (length includes itself, not the tag), consistent with §3 of
the overview spec.

### Client → server messages

- `CopyData`: tag `b'd'`, length, raw payload bytes (a chunk of the COPY stream;
  not necessarily aligned to row boundaries). Each frame is bounded by the
  codec's `MAX_FRAME_LEN` (1 MiB). Rows may span multiple frames, so this is a
  client-framing limit, not a row-size limit; `psql` and other line-oriented
  clients stay far below it, and a client using a single very large
  `PQputCopyData` buffer must chunk it under the limit. An over-limit frame is a
  decode error: the server emits `ErrorResponse` and then closes the connection,
  per the codec's existing decode-error policy.
- `CopyDone`: tag `b'c'`, length `4`.
- `CopyFail`: tag `b'f'`, length, `error_message\0`.

### Server → client messages

- `CopyInResponse`: tag `b'G'`, length, `int8 overall_format`,
  `int16 column_count`, then `int16 format_code` per column.
- `CopyOutResponse`: tag `b'H'`, same body shape as `CopyInResponse`.
- `CopyData`: tag `b'd'`, length, payload bytes.
- `CopyDone`: tag `b'c'`, length `4`.

For `text`/`csv`, `overall_format = 0` and every per-column `format_code = 0`.
Server-generated `CopyData` frames for `COPY TO` are batched and kept well under
`MAX_FRAME_LEN`.

## 5. Connection state machine (server)

The per-connection `serve()` loop gains a COPY mode entered only from a simple
`Query` that classifies as COPY. The classification step parses, binds, and
validates the statement on the blocking pool *before* any COPY response is sent,
so an invalid COPY fails like any other query (`ErrorResponse` + `ReadyForQuery`,
connection stays in normal mode).

### 5.1 COPY FROM STDIN (bounded streaming)

1. Classify (blocking): parse → bind → validate. On error, normal error reply.
2. Send `CopyInResponse`. Create a bounded chunk channel (capacity 64) and an
   abort flag.
3. Spawn the insert task (blocking): it takes ownership of the session's
   transaction (autocommit: acquire a write guard, allocate a txn id, capture a
   snapshot; in a `BEGIN` block: reuse the open transaction), then repeatedly
   receives byte chunks from the channel, parses complete rows (buffering a
   partial trailing line across chunks), validates each value
   (`validate_value_type`, `validate_not_null`), and calls `storage.insert`. On a
   clean channel close (`CopyDone`), a non-empty trailing buffer with no final
   newline is flushed as the last row — for both text and CSV, matching
   PostgreSQL; only a still-open CSV quote at end-of-input is `BadCopyFileFormat`.
   The task ends in one of three ways: the channel closes cleanly (all input
   consumed), the abort flag is observed (client `CopyFail`), or a row fails to
   parse/insert. On a row parse/insert failure it records that error, returns,
   and drops the channel receiver; on `CopyFail` it stops without an error of its
   own — the forwarder holds the client's message text and builds the error after
   awaiting the task (step 5).
4. The async forwarder loop reacts both to inbound messages and to the insert
   task:
   - `CopyData` → `channel.send(payload).await` (the `await` provides TCP
     backpressure when the channel is full). **If the send fails** — the receiver
     was dropped because the insert task exited early on a row error — the
     forwarder stops forwarding and switches to drain mode (see step 5).
   - `CopyDone` → close the channel (clean end-of-input) and await the insert
     task.
   - `CopyFail` → set the abort flag, close the channel, and await the insert
     task.
5. Outcome:
   - Clean success, autocommit → append + flush the commit record, then
     `CommandComplete("COPY n")` + `ReadyForQuery('I')`.
   - Clean success, in-transaction → return the transaction to the session (no
     commit), `CommandComplete("COPY n")` + `ReadyForQuery('T')`.
   - Any failure — a row error (insert task exited early), the client `CopyFail`,
     or an in-`BEGIN` error — rolls back (autocommit) or marks the open
     transaction failed (in-transaction). The forwarder then **drains**: it
     discards inbound `CopyData`/`CopyDone`/`CopyFail` until the stream terminator
     (`CopyDone`/`CopyFail`) is consumed, and only then sends `ErrorResponse`
     followed by `ReadyForQuery` — never emitting `ReadyForQuery` while copy-in
     bytes are still in flight (PostgreSQL simple-query COPY error recovery).
     See §7.

### 5.2 COPY TO STDOUT (bounded streaming)

1. Classify (blocking) as above; on error, normal error reply.
2. Send `CopyOutResponse`.
3. Spawn the producer task (blocking): build the read context (snapshot), open a
   pull-based plan executor that scans the table and projects the COPY columns,
   format each row (plus an optional `HEADER` line first), batch the encoded
   bytes into ~`CopyData` frames, and push them over a bounded channel.
4. The async loop writes each `CopyData` frame, then `CopyDone`,
   `CommandComplete("COPY n")`, and `ReadyForQuery`.
5. If the producer errors after `CopyOutResponse` (e.g. a storage read error),
   the server sends `ErrorResponse` then `ReadyForQuery` in place of `CopyDone`/
   `CommandComplete` — a partial export is never finalized with `CopyDone`.

`COPY TO` runs under the reader path: in autocommit it uses a fresh snapshot;
inside a transaction it uses the transaction's snapshot.

## 6. Transaction & durability semantics

- **Autocommit COPY FROM** is one transaction: all-or-nothing. Any row error
  aborts the whole COPY; nothing is committed. Success appends one `Commit` WAL
  record and flushes before reporting `CommandComplete`.
- **COPY FROM in a `BEGIN` block** folds into the open transaction. Inserted
  rows become durable only at `COMMIT`. A row error marks the transaction failed
  (state `E`); subsequent statements are rejected until `ROLLBACK`.
- COPY FROM appends the same per-row WAL records as `INSERT` through the shared
  storage write path; recovery is unchanged.
- COPY TO appends no WAL (read path).

## 7. Errors

- The first failing row aborts the entire COPY (no partial load, no
  error-tolerant mode), with a deterministic SQLSTATE per cause:
  - unparseable field value (bad integer / boolean) → `InvalidTextRepresentation`
    (`22P02`);
  - wrong number of columns in a row, or a structurally malformed row (e.g. an
    unterminated CSV quote) → `BadCopyFileFormat` (`22P04`);
  - `NotNullViolation` (`23502`) and `UniqueViolation` (`23505`), exactly as on
    the normal insert path.
- Empty input yields `COPY 0`.
- `CopyFail` from the client aborts the COPY with the client's message text in
  the error detail.
- COPY FROM error recovery (see §5.1 step 5): the server stops inserting,
  discards inbound `CopyData`/`CopyDone`/`CopyFail` until the stream terminator
  is consumed, then sends `ErrorResponse` followed by `ReadyForQuery` — so
  `ReadyForQuery` is never emitted mid-stream.
- COPY TO error after `CopyOutResponse` (see §5.2 step 5): `ErrorResponse` then
  `ReadyForQuery`, never a partial `CopyDone`/`CommandComplete`.
- Two `common::SqlState` variants are added: `InvalidTextRepresentation`
  (`22P02`) for bad field values and `BadCopyFileFormat` (`22P04`) for malformed
  rows.

## 8. Command tag

`COPY` reports `CommandComplete("COPY n")`, where `n` is the number of rows
transferred, matching PostgreSQL.

## 9. Crate responsibilities

- `common`: add `SqlState::InvalidTextRepresentation` (`22P02`) and
  `SqlState::BadCopyFileFormat` (`22P04`), and the shared
  `parse_bool_text(&str) -> Option<bool>` boolean decoder (reused by `protocol`
  and the COPY import path; the leaf crate is the only place both layers may
  depend on).
- `protocol`: COPY client/server messages and their codec encode/decode.
- `parser`: `Statement::Copy` AST and translation from `sqlparser::Statement::Copy`,
  including option normalization and rejection of unsupported forms.
- `planner`: `BoundStatement::Copy` (binding: resolve table + columns, validate
  options); `COPY TO` carries a bound projection-scan reused by the existing
  logical/physical planner. `COPY` is not lowered by `logical_plan` — the server
  drives it.
- `executor`: a pure text/CSV format module (bytes ↔ `Vec<Value>`), the COPY FROM
  row-insert routine, and the COPY TO row-producer; reuses
  `validate_value_type`/`validate_not_null`, `storage.insert`, and the scan/
  projection operators.
- `server`: the COPY connection state machine, channels, transaction
  integration, command tag, and rejection of unsupported forms / extended-protocol
  COPY.

## 10. Testing

- `protocol`: encode/decode round-trips for all seven COPY messages.
- `parser`: COPY parsing, both option syntaxes, every rejection path.
- `planner`: column defaulting/reordering/validation and option-validation errors.
- `executor`: text/CSV parse↔format round-trips, NULL handling, escaping/quoting,
  `HEADER`, and field type errors.
- `server` integration (simple query, via `psql` or the protocol harness):
  end-to-end `FROM`/`TO` in text and CSV, `HEADER`, `NULL`, a mid-stream error
  that aborts the COPY and resynchronizes the protocol, autocommit vs in-`BEGIN`,
  and each rejection path (file, binary, query, extended protocol).
