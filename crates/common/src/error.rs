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
    /// `42P10`: a column reference is invalid in its context, e.g. a
    /// `SELECT DISTINCT` query whose `ORDER BY` references an expression that is
    /// not in the select list, or a `DISTINCT ON` whose expressions do not match
    /// the leading `ORDER BY` expressions.
    InvalidColumnReference,
    DuplicateTable,
    DatatypeMismatch,
    DivisionByZero,
    NumericValueOutOfRange,
    /// `22001`: a value is too long for a bounded character type, e.g. a string
    /// longer than `n` assigned to a `VARCHAR(n)` / `CHAR(n)` column.
    StringDataRightTruncation,
    /// `22P02`: a text field could not be parsed into its target type (e.g. a
    /// non-numeric value for an `INTEGER` column in `COPY ... FROM`).
    InvalidTextRepresentation,
    /// `22P04`: a `COPY ... FROM` input row is structurally malformed — the
    /// wrong number of columns, or an unterminated CSV quote.
    BadCopyFileFormat,
    NotNullViolation,
    UniqueViolation,
    QueryCanceled,
    FeatureNotSupported,
    /// `25P02`: a statement other than `COMMIT`/`ROLLBACK` was issued inside a
    /// transaction block that has already failed. The block must be ended before
    /// any further command is accepted.
    InFailedSqlTransaction,
    /// `25P01`: a `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` was issued with no open
    /// transaction block (savepoints are valid only inside `BEGIN`…`COMMIT`).
    NoActiveSqlTransaction,
    /// `3B001`: `RELEASE`/`ROLLBACK TO` named a savepoint that does not exist in
    /// the current transaction. See `docs/specs/savepoints.md` §2.
    InvalidSavepointSpecification,
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
