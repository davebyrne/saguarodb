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
    /// `21000`: a subquery used as an expression returned more than one row where
    /// at most one was expected (a scalar subquery).
    CardinalityViolation,
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
    /// `40001`: a write-write conflict against a **committed**-superseded version —
    /// another transaction updated/deleted the target row since this writer's
    /// snapshot. A conflict against an *in-progress* writer no longer maps here:
    /// SaguaroDB now **blocks** on it (waiting for the holder) and only surfaces
    /// `40001` if the holder turns out to have committed. See `docs/specs/mvcc.md`
    /// §7.3, `docs/specs/deadlock.md`, and `crate::mvcc::write_conflict`.
    SerializationFailure,
    /// `40P01`: a deadlock was detected — two or more transactions are each waiting
    /// for a row lock held by another, forming a cycle. The timeout-based detector
    /// aborts a victim (the detecting waiter) with this code. See
    /// `docs/specs/deadlock.md`.
    DeadlockDetected,
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
