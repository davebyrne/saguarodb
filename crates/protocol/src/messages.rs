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
    Terminate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerMessage {
    SslAccepted,
    SslRejected,
    AuthenticationOk,
    ParameterStatus {
        key: String,
        value: String,
    },
    ReadyForQuery,
    RowDescription(Vec<ColumnInfo>),
    DataRow(Vec<Option<String>>),
    CommandComplete(String),
    /// Extended protocol: reply to a successful Parse.
    ParseComplete,
    /// Extended protocol: reply to a successful Bind.
    BindComplete,
    /// Extended protocol: reply to a successful Close.
    CloseComplete,
    /// Extended protocol: parameter type OIDs for a described statement.
    ParameterDescription(Vec<i32>),
    /// Extended protocol: a described statement/portal returns no rows.
    NoData,
    ErrorResponse {
        severity: String,
        code: String,
        message: String,
    },
}
