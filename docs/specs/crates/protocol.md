# `protocol` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`protocol` implements PostgreSQL simple query protocol encoding, decoding, and connection state. It does not own sockets, Tokio tasks, or storage/query execution.

## Depends On

- `common`

## Public Message Types

```rust
pub enum ClientMessage {
    Startup { user: String, database: Option<String> },
    SslRequest,
    Query(String),
    Terminate,
}

pub enum ServerMessage {
    SslRejected,
    AuthenticationOk,
    ParameterStatus { key: String, value: String },
    ReadyForQuery,
    RowDescription(Vec<ColumnInfo>),
    DataRow(Vec<Option<String>>),
    CommandComplete(String),
    ErrorResponse { severity: String, code: String, message: String },
}
```

All row data is text-format in v1.

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

`decode` is stateful and may return zero, one, or many messages. It buffers incomplete input internally.

## Connection State API

```rust
pub trait ConnectionState: Send {
    fn handle_message(&mut self, msg: ClientMessage) -> Result<Vec<ServerMessage>>;
    fn is_terminated(&self) -> bool;
}
```

`ConnectionState` handles non-query messages. Query messages are passed by `server` into the query execution pipeline.

## Startup Flow

1. Client may send SSLRequest. Server responds with `SslRejected` (`N` byte).
2. Client sends StartupMessage.
3. Server sends:
   - `AuthenticationOk`
   - minimal `ParameterStatus` messages
   - `ReadyForQuery`

V1 accepts all users/databases and performs no authentication.

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

The codec may return errors after buffering bytes. In v1 the server treats any decode error as connection-fatal: it encodes one `ErrorResponse`, encodes `ReadyForQuery`, and closes the TCP connection instead of attempting to reuse the codec state.

## Type Encoding

`ColumnInfo.data_type` maps to PostgreSQL type OIDs:

- `Integer` -> `INT8` (`type_oid = 20`, `type_size = 8`)
- `Text` -> `TEXT` (`type_oid = 25`, `type_size = -1`)
- `Boolean` -> `BOOL` (`type_oid = 16`, `type_size = 1`)

V1 sends text format for all columns.

## PostgreSQL Wire Encoding Details

All integer fields are big-endian. All server messages except `SslRejected` are one-byte tag plus a four-byte length that includes the length field but not the tag. `SslRejected` is exactly the single byte `b'N'`.

Client messages:

- `SSLRequest`: startup-style packet with `int32 length = 8`, `int32 code = 80877103`.
- `Startup`: startup-style packet with `int32 protocol = 196608` for protocol 3.0, followed by nul-terminated key/value strings and a final `\0`. V1 reads `user` and optional `database`; unknown parameters are ignored.
- `Query`: tag `b'Q'`, length, SQL string terminated by `\0`.
- `Terminate`: tag `b'X'`, length `4`.

Server messages:

- `AuthenticationOk`: tag `b'R'`, length `8`, `int32 auth_code = 0`.
- `ParameterStatus`: tag `b'S'`, length, `key\0value\0`. Startup emits at least `server_version=16.0`, `server_encoding=UTF8`, `client_encoding=UTF8`, `DateStyle=ISO`, and `integer_datetimes=on`.
- `ReadyForQuery`: tag `b'Z'`, length `5`, status byte `b'I'`.
- `RowDescription`: tag `b'T'`, length, `int16 field_count`, then one field entry per column: `name\0`, `int32 table_oid = 0`, `int16 attr_num = 0`, mapped `int32 type_oid`, mapped `int16 type_size`, `int32 type_modifier = -1`, `int16 format_code = 0`.
- `DataRow`: tag `b'D'`, length, `int16 column_count`, then each value as `int32 byte_length` plus UTF-8 bytes, or `int32 -1` for `NULL`.
- `CommandComplete`: tag `b'C'`, length, nul-terminated command tag. V1 tags are `SELECT n`, `INSERT 0 n`, `UPDATE n`, `DELETE n`, `CREATE TABLE`, `DROP TABLE`, and `EXPLAIN`.
- `ErrorResponse`: tag `b'E'`, length, fields `b'S' severity\0`, `b'C' sqlstate\0`, `b'M' message\0`, then final `\0`.

Text value encoding:

- `Integer`: decimal i64 string.
- `Text`: raw UTF-8 string bytes.
- `Boolean`: `t` for true, `f` for false.
- `NULL`: encoded as a `DataRow` field length of `-1`.

## Non-Goals

- Extended query protocol.
- Prepared statements.
- Binary row format.
- Authentication.
- TLS beyond explicit SSL rejection.
- CancelRequest.
- COPY.

## Acceptance Tests

- Decodes SSLRequest and encodes single-byte `N`.
- Decodes StartupMessage and emits expected startup responses.
- Decodes simple Query.
- Encodes RowDescription for all v1 data types with PostgreSQL OIDs, text format, and `table_oid = 0`.
- Encodes DataRow with `NULL` represented as null field.
- Encodes ReadyForQuery as `b'Z'`, length `5`, status `b'I'`.
- Encodes ErrorResponse with SQLSTATE code.
- Handles Terminate by marking connection terminated.
