use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct DbError {
    pub kind: ErrorKind,
    pub code: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    Parse,
    Plan,
    Execute,
    Storage,
    Io,
    Wal,
    Protocol,
    Internal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlState {
    SuccessfulCompletion,
    SyntaxError,
    UndefinedTable,
    UndefinedColumn,
    DuplicateTable,
    DatatypeMismatch,
    DivisionByZero,
    NumericValueOutOfRange,
    NotNullViolation,
    UniqueViolation,
    QueryCanceled,
    FeatureNotSupported,
    /// `25P02`: a statement other than `COMMIT`/`ROLLBACK` was issued inside a
    /// transaction block that has already failed. The block must be ended before
    /// any further command is accepted.
    InFailedSqlTransaction,
    /// `40001`: a write-write conflict was detected — another transaction has
    /// locked or committed-superseded the target version since this writer's
    /// snapshot. SaguaroDB's policy is fail-fast first-updater-wins (no blocking,
    /// no deadlock detection): the losing writer aborts with this code. See
    /// `docs/specs/mvcc.md` §7.3 and `crate::mvcc::write_conflict`.
    SerializationFailure,
    IoError,
    InternalError,
}

pub type Result<T> = std::result::Result<T, DbError>;

impl DbError {
    pub fn parse(code: SqlState, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Parse, code, message)
    }

    pub fn plan(code: SqlState, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Plan, code, message)
    }

    pub fn execute(code: SqlState, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Execute, code, message)
    }

    pub fn storage(code: SqlState, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Storage, code, message)
    }

    pub fn wal(code: SqlState, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Wal, code, message)
    }

    pub fn protocol(code: SqlState, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Protocol, code, message)
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Io, SqlState::IoError, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, SqlState::InternalError, message)
    }

    fn new(kind: ErrorKind, code: SqlState, message: impl Into<String>) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
            detail: None,
            hint: None,
        }
    }
}
