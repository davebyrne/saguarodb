# `protocol` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`protocol` implements PostgreSQL simple and extended query protocol encoding, decoding, and connection state. It does not own sockets, Tokio tasks, or storage/query execution.

## Depends On

- `common`

## Public Message Types

```rust
pub enum StatementKind {
    Statement,
    Portal,
}

pub enum ClientMessage {
    Startup { user: String, database: Option<String>, application_name: Option<String> },
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
    Query(String),
    // Extended query protocol.
    Parse { name: String, query: String, param_types: Vec<i32> },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Describe { kind: StatementKind, name: String },
    Execute { portal: String, max_rows: i32 },
    Close { kind: StatementKind, name: String },
    Sync,
    Flush,
    Terminate,
}

pub enum ServerMessage {
    SslAccepted,
    SslRejected,
    AuthenticationOk,
    BackendKeyData { process_id: i32, secret_key: i32 },
    ParameterStatus { key: String, value: String },
    ReadyForQuery(u8),
    RowDescription { columns: Vec<ColumnInfo>, formats: Vec<i16> },
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete(String),
    // Extended query protocol.
    ParseComplete,
    BindComplete,
    CloseComplete,
    ParameterDescription(Vec<i32>),
    NoData,
    ErrorResponse { severity: String, code: String, message: String },
}
```

`RowDescription` carries the column metadata plus a per-field `formats` array
(`0` = text, `1` = binary); a shorter or empty `formats` defaults the remaining
columns to text. `DataRow` carries each value already encoded to its wire bytes
for that format (or `None` for SQL NULL) — not decoded strings. The simple query
path always uses text; the extended-protocol `Bind` carries raw parameter bytes
and the PostgreSQL format-code arrays, and the codec only frames these bytes,
leaving their interpretation to the server. The connection-level choreography of the extended
messages (prepared statements, portals, Describe/Execute/Sync) is owned by
`server`, not the codec; see the server spec.

## Codec API

```rust
pub trait ProtocolCodec: Send {
    fn decode(&mut self, buf: &[u8]) -> Result<Vec<ClientMessage>>;
    fn encode(&self, msg: &ServerMessage) -> Vec<u8>;
}

pub struct PostgresCodec { /* buffered PostgreSQL v3 simple-query codec */ }

impl PostgresCodec {
    pub fn new() -> Self;
}
```

`decode` is stateful and may return zero, one, or many messages. It buffers incomplete input internally. Any startup-style or tagged frame whose declared length exceeds `MAX_FRAME_LEN` (1 MiB) is rejected with a `SyntaxError` rather than buffered, bounding memory use. Per-message decoders enforce full consumption: extended messages reject trailing bytes, and `Query` rejects any payload after its nul terminator.

## Connection State API

```rust
pub trait ConnectionState: Send {
    fn handle_message(&mut self, msg: ClientMessage) -> Result<Vec<ServerMessage>>;
    fn is_terminated(&self) -> bool;
}
```

`ConnectionState` handles non-query messages. Query messages are passed by `server` into the query execution pipeline.

## Startup Flow

1. Client may send SSLRequest. The protocol layer decodes it and encodes the
   negotiation reply the server selects: `SslAccepted` (`S` byte) when the
   server has TLS configured, otherwise `SslRejected` (`N` byte). The protocol
   layer does not perform the TLS handshake; `server` owns it. A client may also
   send a `GSSENCRequest` first; GSSAPI transport encryption is not supported
   and declines it with the same `N` byte, after which the client continues.
2. Client sends StartupMessage.
3. Server sends:
   - `AuthenticationOk`
   - minimal `ParameterStatus` messages
   - `ReadyForQuery`

The server accepts all users/databases and performs no authentication.

## Query Flow

For SELECT:

1. Server executes query through the server-owned query service.
2. Protocol encodes `RowDescription`.
3. Server streams rows as `DataRow`.
4. Protocol encodes `CommandComplete("SELECT n")`.
5. Protocol encodes `ReadyForQuery`.

For DML/DDL:

1. Server executes statement.
2. Protocol encodes `CommandComplete`.
3. Protocol encodes `ReadyForQuery`.

For EXPLAIN:

1. Server executes the planner-only statement.
2. Protocol encodes `RowDescription` with one `TEXT` column named `QUERY PLAN`.
3. Protocol encodes one `DataRow` containing the formatted explanation text.
4. Protocol encodes `CommandComplete("EXPLAIN")`.
5. Protocol encodes `ReadyForQuery`.

On error:

1. Encode `ErrorResponse`.
2. Encode `ReadyForQuery`.
3. Keep connection open unless protocol state is unrecoverable.

The codec may return errors after buffering bytes. The server treats any decode error as connection-fatal: it encodes one `ErrorResponse`, encodes `ReadyForQuery`, and closes the TCP connection instead of attempting to reuse the codec state.

## Type Encoding

A `RowDescription` field's type OID, `type_size`, and `type_modifier`
(`atttypmod`) come from the column's declared PostgreSQL wire type
`common::PgType`, obtained via `ColumnInfo::wire_type()` (an unlabeled column
resolves to the collapsed default from its `DataType`: `Integer` -> `int8`,
`Text` -> `text`). `PgType` reports the exact width, character kind, and length
that `DataType` intentionally collapses:

- integers: `Int2` (`21`, size `2`), `Int4` (`23`, size `4`), `Int8` (`20`, size `8`)
- character: `Text` (`25`), `Varchar` (`1043`), `Bpchar` (`1042`), all size `-1`
- `Bool` (`16`, `1`), `Bytea` (`17`, `-1`), `Uuid` (`2950`, `16`)
- `Float4` (`700`, `4`), `Float8` (`701`, `8`), `Numeric` (`1700`, `-1`)
- temporal: `Date` (`1082`, `4`), `Time` (`1083`, `8`), `Timestamp` (`1114`, `8`),
  `Timestamptz` (`1184`, `8`), `Interval` (`1186`, `16`)

`type_modifier` (`atttypmod`) is `varchar(n)`/`char(n)` -> `n + 4`,
`numeric(p, s)` -> `((p << 16) | s) + 4`, and `-1` otherwise.

On input, an extended-protocol `Parse` may declare each parameter's type OID.
The accepted OIDs are the wire types above plus `0` (unspecified — the server
infers the type): the distinct integer widths `int2` (`21`), `int4` (`23`),
`int8` (`20`) all resolve to the single integer type, and `varchar` (`1043`) /
`bpchar` (`1042`) / `text` (`25`) all resolve to text; any other OID is rejected
with an "unsupported parameter type OID" error. The server remembers each
declared wire type so `ParameterDescription` echoes the exact OID the client
declared (an inferred parameter falls back to the collapsed default). A binary
integer parameter may be bound as 2, 4, or 8 bytes (`int2`/`int4`/`int8`); each
is sign-extended to the internal 64-bit integer.

The simple query path sends text format for all columns. The extended protocol
lets a client request binary format (code `1`) per column via `Bind`;
`RowDescription` then reports the chosen format code per field. Binary integer
output honors the column's declared width: an `int2`/`int4` value is encoded to 2
or 4 big-endian bytes (not the 8-byte `int8` form), via
`encode_value_with_type`. A value that does not fit its declared width — only
reachable for data that predates or bypasses the write-time range check — is
rejected rather than silently truncated.

## PostgreSQL Wire Encoding Details

All integer fields are big-endian. All server messages except `SslAccepted` and `SslRejected` are one-byte tag plus a four-byte length that includes the length field but not the tag. `SslAccepted` is exactly the single byte `b'S'` and `SslRejected` is exactly the single byte `b'N'`.

Client messages:

- `SSLRequest`: startup-style packet with `int32 length = 8`, `int32 code = 80877103`.
- `GSSENCRequest`: startup-style packet with `int32 length = 8`, `int32 code = 80877104`.
- `CancelRequest`: startup-style packet with `int32 length = 16`, `int32 code = 80877102`, `int32 process_id`, `int32 secret_key`. Sent on its own connection; the server sends no reply.
- `Startup`: startup-style packet with `int32 protocol = 196608` for protocol 3.0, followed by nul-terminated key/value strings and a final `\0`. The server reads `user`, optional `database`, and optional `application_name`; other parameters are ignored.
- `Query`: tag `b'Q'`, length, SQL string terminated by `\0`.
- `Parse`: tag `b'P'`, length, `statement_name\0`, `query\0`, `int16 param_type_count`, then that many `int32` parameter type OIDs (`0` = unspecified).
- `Bind`: tag `b'B'`, length, `portal_name\0`, `statement_name\0`, `int16 param_format_count` + that many `int16` format codes, `int16 param_count` + that many parameters (each `int32 length` then `length` bytes, or `int32 -1` for NULL), `int16 result_format_count` + that many `int16` format codes. Format codes are `0` (text) or `1` (binary).
- `Describe`: tag `b'D'`, length, `byte kind` (`b'S'` statement or `b'P'` portal), `name\0`.
- `Execute`: tag `b'E'`, length, `portal_name\0`, `int32 max_rows` (`0` = all rows).
- `Close`: tag `b'C'`, length, `byte kind` (`b'S'`/`b'P'`), `name\0`.
- `Sync`: tag `b'S'`, length `4`.
- `Flush`: tag `b'H'`, length `4`.
- `CopyData` (copy-in): tag `b'd'`, length, raw payload bytes (a stream chunk,
  not necessarily row-aligned).
- `CopyDone` (copy-in): tag `b'c'`, length `4`.
- `CopyFail`: tag `b'f'`, length, `message\0`.
- `Terminate`: tag `b'X'`, length `4`.

Server messages:

- `AuthenticationOk`: tag `b'R'`, length `8`, `int32 auth_code = 0`.
- `BackendKeyData`: tag `b'K'`, length `12`, `int32 process_id`, `int32 secret_key`. Sent at startup so the client can later cancel an in-flight query.
- `ParameterStatus`: tag `b'S'`, length, `key\0value\0`. Startup emits `server_version=16.0`, `server_encoding=UTF8`, `client_encoding=UTF8`, `DateStyle=ISO`, `integer_datetimes=on`, `standard_conforming_strings=on`, `TimeZone=UTC`, and `application_name` echoed from the client's startup parameters (empty when not supplied). The server re-sends `ParameterStatus` for `application_name` whenever `SET`/`RESET`/`DISCARD ALL` changes it (before the terminal `ReadyForQuery`); all other startup-reported parameters are fixed and never re-reported.
- `ReadyForQuery(status)`: tag `b'Z'`, length `5`, transaction-status byte supplied by the caller. The protocol encodes whatever byte it is handed; the server sources it from the session's transaction state (`b'I'` idle, `b'T'` in a transaction block, `b'E'` failed transaction block). Outside an explicit transaction (autocommit) the byte is `b'I'`; inside an open `BEGIN` block it is `b'T'`, and `b'E'` once a statement in that block fails (see `docs/specs/crates/server.md`).
- `RowDescription`: tag `b'T'`, length, `int16 field_count`, then one field entry per column: `name\0`, `int32 table_oid = 0`, `int16 attr_num = 0`, `int32 type_oid`, `int16 type_size`, `int32 type_modifier` (the declared `atttypmod`, or `-1`), `int16 format_code` (`0` text, `1` binary). The OID/size/modifier come from the column's declared `PgType` (see Type Encoding).
- `DataRow`: tag `b'D'`, length, `int16 column_count`, then each value as `int32 byte_length` plus its wire bytes (text or binary per the `RowDescription` format codes), or `int32 -1` for `NULL`.
- `CommandComplete`: tag `b'C'`, length, nul-terminated command tag. Tags include `SELECT n`, `INSERT 0 n`, `UPDATE n`, `DELETE n`, `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`, `CREATE SEQUENCE`, `DROP SEQUENCE`, `ALTER TABLE`, `EXPLAIN`, `BEGIN`, `COMMIT`, `ROLLBACK`, `SET`, `SHOW`, `RESET`, `DISCARD ALL`, `VACUUM`, `TRUNCATE TABLE`, and `COPY n`.
- `ParseComplete`: tag `b'1'`, length `4`.
- `BindComplete`: tag `b'2'`, length `4`.
- `CloseComplete`: tag `b'3'`, length `4`.
- `ParameterDescription`: tag `b't'`, length, `int16 param_count`, then that many `int32` parameter type OIDs.
- `NoData`: tag `b'n'`, length `4`.
- `CopyInResponse`: tag `b'G'`, length, `int8 overall_format`, `int16 column_count`, then one `int16 format_code` per column (`0` = text). Used for `COPY ... FROM STDIN`.
- `CopyOutResponse`: tag `b'H'`, same body shape as `CopyInResponse`. Used for `COPY ... TO STDOUT`.
- `CopyData` (copy-out): tag `b'd'`, length, payload bytes.
- `CopyDone` (copy-out): tag `b'c'`, length `4`.
- `ErrorResponse`: tag `b'E'`, length, fields `b'S' severity\0`, `b'C' sqlstate\0`, `b'M' message\0`, then final `\0`.

The COPY messages are codec-level framing only; the COPY sub-protocol flow,
formats, and error recovery live in `docs/specs/copy.md` §4/§5.

Text value encoding:

- `Integer`: decimal i64 string.
- `Text`: raw UTF-8 string bytes.
- `Boolean`: `t` for true, `f` for false.
- `Date`: `YYYY-MM-DD`.
- `Timestamp`: `YYYY-MM-DD HH:MM:SS[.ffffff]` (fractional seconds only when non-zero).
- `Time`: `HH:MM:SS[.ffffff]` (fractional seconds only when non-zero).
- `TimestampTz`: `YYYY-MM-DD HH:MM:SS[.ffffff]+00` (always UTC).
- `Interval`: PostgreSQL `postgres`-style text (e.g. `1 year 2 mons 3 days 04:05:06`).
- `Bytea`: hex `\x` followed by lowercase hex digits (two per byte).
- `Uuid`: canonical lowercase `8-4-4-4-12` hyphenated form.
- `Float`: round-trippable decimal — fixed-point for moderate magnitudes, `e±NN` scientific for extreme exponents (or `Infinity`/`-Infinity`/`NaN` for non-finite values).
- `Real`: same form as `Float`, single precision.
- `Numeric`: decimal text preserving the value's scale (e.g. `1.50`).
- `NULL`: encoded as a `DataRow` field length of `-1`.

Binary value encoding (extended protocol, format code `1`):

- `Integer`: 8-byte big-endian `int64`.
- `Boolean`: one byte, `0x01` true / `0x00` false.
- `Text`: raw UTF-8 bytes (identical to text format).
- `Date`: 4-byte big-endian `int32` day count from 2000-01-01 (PostgreSQL's date epoch), converted to/from the internal Unix-epoch day count.
- `Timestamp`: 8-byte big-endian `int64` microsecond count from 2000-01-01 00:00:00 (PostgreSQL's timestamp epoch), converted to/from the internal Unix-epoch microsecond count.
- `Time`: 8-byte big-endian `int64` microsecond count since midnight.
- `TimestampTz`: 8-byte big-endian `int64` microsecond count from 2000-01-01 UTC.
- `Interval`: `int64` microseconds, `int32` days, `int32` months (16 bytes, big-endian).
- `Bytea`: the raw bytes (identical to the stored value).
- `Uuid`: the 16 raw bytes.
- `Float`: 8-byte big-endian IEEE 754 binary64.
- `Real`: 4-byte big-endian IEEE 754 binary32.
- `Numeric`: PostgreSQL's base-10000 `NumericVar` format (`int16 ndigits, weight, sign, dscale`, then the digit groups).
- `NULL`: `DataRow` field length of `-1`.

`encode_value`/`decode_value` convert between `common::Value` and these wire
encodings. Parameter decoding accepts text input (`Integer` as a decimal string,
`Boolean` as `t`/`f`/`true`/`false`/`1`/`0`/`yes`/`no`/`on`/`off`, `Text` as raw
bytes) or the binary encodings above, per each `Bind` parameter format code, and
rejects malformed input (e.g. a binary `int8` that is not 8 bytes) and
unsupported format codes.

## Non-Goals

- Authentication.
- Performing the TLS handshake itself. The protocol layer only encodes the
  `SslAccepted`/`SslRejected` negotiation byte; `server` owns the handshake.
- GSSAPI transport encryption (the GSSENCRequest is declined with `N`).
- COPY through the *extended* query protocol (rejected; COPY is simple-query
  only). The COPY sub-protocol itself **is** supported: the codec encodes/decodes
  the `CopyInResponse`/`CopyOutResponse`/`CopyData`/`CopyDone`/`CopyFail` messages.
  Their wire encodings are specified in `docs/specs/copy.md` §4 (the authoritative
  source); the message variants and their acceptance tests are added to this
  crate alongside that code.

## Acceptance Tests

- Decodes SSLRequest and encodes single-byte `N` for `SslRejected`.
- Encodes single-byte `S` for `SslAccepted`.
- Decodes GSSENCRequest and declines it with single-byte `N`.
- Decodes CancelRequest (process id + secret key) and encodes BackendKeyData.
- Decodes StartupMessage (reading `user`, `database`, and `application_name`) and emits expected startup responses.
- Startup echoes `application_name` in a `ParameterStatus`, reporting empty when the client omits it.
- Decodes simple Query.
- Encodes RowDescription for all supported data types with PostgreSQL OIDs, the per-field format code, and `table_oid = 0`.
- Round-trips int8/bool/text values through `encode_value`/`decode_value` in both text and binary formats, and rejects malformed binary input and unsupported format codes.
- Encodes DataRow with `NULL` represented as null field.
- Encodes ReadyForQuery as `b'Z'`, length `5`, status byte equal to the supplied `status` (the server passes `b'I'`/`b'T'`/`b'E'` from the session's transaction state).
- Encodes ErrorResponse with SQLSTATE code.
- Handles Terminate by marking connection terminated.
- Round-trips the COPY messages: decodes `CopyData`/`CopyDone`/`CopyFail` (and an empty `CopyData`), rejects a `CopyDone` with a non-empty body, and encodes `CopyInResponse`/`CopyOutResponse`/`CopyData`/`CopyDone` with the body shapes above.
- A COPY data message reaching the connection state machine (out of an active COPY) is a protocol error.
