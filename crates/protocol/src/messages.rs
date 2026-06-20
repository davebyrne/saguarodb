use common::ColumnInfo;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientMessage {
    Startup {
        user: String,
        database: Option<String>,
    },
    SslRequest,
    Query(String),
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
    ErrorResponse {
        severity: String,
        code: String,
        message: String,
    },
}
