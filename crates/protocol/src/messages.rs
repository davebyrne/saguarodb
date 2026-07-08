use common::ColumnInfo;

/// Target of a Describe/Close message: a prepared statement (`S`) or a portal
/// (`P`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementKind {
    Statement,
    Portal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientMessage {
    Startup {
        user: String,
        database: Option<String>,
        application_name: Option<String>,
    },
    SslRequest,
    GssEncRequest,
    /// Cancel an in-flight query on another backend, identified by the
    /// `BackendKeyData` it was given at startup. Sent on its own short-lived
    /// connection; the server never replies.
    CancelRequest {
        process_id: i32,
        secret_key: i32,
    },
    Query(String),
    /// Extended protocol: prepare a (possibly parameterized) statement.
    Parse {
        name: String,
        query: String,
        param_types: Vec<i32>,
    },
    /// Extended protocol: bind parameter values to a prepared statement,
    /// producing a portal. Parameter values are the raw wire bytes (or `None`
    /// for SQL NULL); `param_formats`/`result_formats` are the PostgreSQL
    /// format-code arrays (`0` = text, `1` = binary) exactly as sent.
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    /// Extended protocol: describe a prepared statement or portal.
    Describe {
        kind: StatementKind,
        name: String,
    },
    /// Extended protocol: execute a portal. `max_rows` of `0` means "all rows".
    Execute {
        portal: String,
        max_rows: i32,
    },
    /// Extended protocol: close a prepared statement or portal.
    Close {
        kind: StatementKind,
        name: String,
    },
    /// Extended protocol: end of an extended-query sequence.
    Sync,
    /// Extended protocol: request the server flush its output buffer.
    Flush,
    /// COPY sub-protocol: a chunk of `COPY ... FROM STDIN` data. Not necessarily
    /// aligned to row boundaries. See `docs/specs/copy.md` §4.
    CopyData(Vec<u8>),
    /// COPY sub-protocol: the client has finished sending copy-in data.
    CopyDone,
    /// COPY sub-protocol: the client aborts the copy-in with this message text.
    CopyFail(String),
    Terminate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerMessage {
    SslAccepted,
    SslRejected,
    AuthenticationOk,
    /// Backend identity for cancellation: the client may later open a separate
    /// connection and send a `CancelRequest` carrying these values.
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    ParameterStatus {
        key: String,
        value: String,
    },
    /// Signals the backend is ready for a new query. The payload is the
    /// transaction-status byte the server supplies from the session's current
    /// transaction state: `b'I'` (idle), `b'T'` (in a transaction block), or
    /// `b'E'` (failed transaction block). The protocol crate only encodes the
    /// byte it is handed; the meaning lives in the server's `TransactionState`.
    ReadyForQuery(u8),
    /// Column metadata plus the wire format code (`0` = text, `1` = binary) the
    /// values will be sent in. `formats` is parallel to `columns`; a shorter (or
    /// empty) `formats` defaults missing columns to text.
    RowDescription {
        columns: Vec<ColumnInfo>,
        formats: Vec<i16>,
    },
    /// One result row: each column is already encoded to its wire bytes (text or
    /// binary per the `RowDescription` format), or `None` for SQL NULL.
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete(String),
    /// Extended protocol: reply to a successful Parse.
    ParseComplete,
    /// Extended protocol: reply to a successful Bind.
    BindComplete,
    /// Extended protocol: reply to a successful Close.
    CloseComplete,
    /// Extended protocol: a portal produced the requested row count and can be
    /// resumed by a later Execute.
    PortalSuspended,
    /// Extended protocol: parameter type OIDs for a described statement.
    ParameterDescription(Vec<i32>),
    /// Extended protocol: a described statement/portal returns no rows.
    NoData,
    /// COPY sub-protocol: the server is ready to receive `COPY ... FROM STDIN`
    /// data. `overall_format` is `0` (text/CSV) or `1` (binary; unused here), and
    /// `column_formats` carries one format code per column. See
    /// `docs/specs/copy.md` §4.
    CopyInResponse {
        overall_format: i8,
        column_formats: Vec<i16>,
    },
    /// COPY sub-protocol: the server is about to send `COPY ... TO STDOUT` data.
    /// Same body shape as `CopyInResponse`.
    CopyOutResponse {
        overall_format: i8,
        column_formats: Vec<i16>,
    },
    /// COPY sub-protocol: a chunk of `COPY ... TO STDOUT` data.
    CopyData(Vec<u8>),
    /// COPY sub-protocol: the server has finished sending copy-out data.
    CopyDone,
    ErrorResponse {
        severity: String,
        code: String,
        message: String,
    },
}
