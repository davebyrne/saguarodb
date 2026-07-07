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
    /// `3F000`: a schema name is not recognized.
    InvalidSchemaName,
    UndefinedColumn,
    /// `42704`: an object-like name is not recognized, e.g. a `SHOW` of an
    /// unknown configuration parameter.
    UndefinedObject,
    /// `42P10`: a column reference is invalid in its context, e.g. a
    /// `SELECT DISTINCT` query whose `ORDER BY` references an expression that is
    /// not in the select list, or a `DISTINCT ON` whose expressions do not match
    /// the leading `ORDER BY` expressions.
    InvalidColumnReference,
    DuplicateTable,
    DatatypeMismatch,
    DivisionByZero,
    /// `22023`: a validly typed argument or option has an invalid value, e.g.
    /// `CREATE SEQUENCE INCREMENT BY 0`.
    InvalidParameterValue,
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
    /// `23514`: a row violates a table's `CHECK` constraint — the constraint
    /// expression evaluated to `false` for the proposed row (a `NULL`/unknown
    /// result passes, matching PostgreSQL).
    CheckViolation,
    /// `21000`: a subquery used as an expression returned more than one row where
    /// at most one was expected (a scalar subquery).
    CardinalityViolation,
    /// `2BP01`: an object cannot be dropped because another object depends on it,
    /// e.g. a column default still references a sequence.
    DependentObjectsStillExist,
    /// `55000`: the object is not in the prerequisite state for the requested
    /// operation, e.g. `currval` before this session has called `nextval`/`setval`
    /// for that sequence.
    ObjectNotInPrerequisiteState,
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
    /// `54000`: a statement exceeded an implementation limit, e.g. a row or
    /// logical varlena value cannot fit within the supported storage format.
    ProgramLimitExceeded,
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

impl SqlState {
    pub fn code(self) -> &'static str {
        match self {
            SqlState::SuccessfulCompletion => "00000",
            SqlState::SyntaxError => "42601",
            SqlState::UndefinedTable => "42P01",
            SqlState::InvalidSchemaName => "3F000",
            SqlState::UndefinedColumn => "42703",
            SqlState::UndefinedObject => "42704",
            SqlState::InvalidColumnReference => "42P10",
            SqlState::DuplicateTable => "42P07",
            SqlState::DatatypeMismatch => "42804",
            SqlState::DivisionByZero => "22012",
            SqlState::InvalidParameterValue => "22023",
            SqlState::NumericValueOutOfRange => "22003",
            SqlState::StringDataRightTruncation => "22001",
            SqlState::InvalidTextRepresentation => "22P02",
            SqlState::BadCopyFileFormat => "22P04",
            SqlState::NotNullViolation => "23502",
            SqlState::UniqueViolation => "23505",
            SqlState::CheckViolation => "23514",
            SqlState::CardinalityViolation => "21000",
            SqlState::DependentObjectsStillExist => "2BP01",
            SqlState::ObjectNotInPrerequisiteState => "55000",
            SqlState::QueryCanceled => "57014",
            SqlState::FeatureNotSupported => "0A000",
            SqlState::InFailedSqlTransaction => "25P02",
            SqlState::NoActiveSqlTransaction => "25P01",
            SqlState::InvalidSavepointSpecification => "3B001",
            SqlState::ProgramLimitExceeded => "54000",
            SqlState::SerializationFailure => "40001",
            SqlState::DeadlockDetected => "40P01",
            SqlState::IoError => "58030",
            SqlState::InternalError => "XX000",
        }
    }

    pub fn from_code(code: &str) -> Option<Self> {
        Some(match code {
            "00000" => SqlState::SuccessfulCompletion,
            "42601" => SqlState::SyntaxError,
            "42P01" => SqlState::UndefinedTable,
            "3F000" => SqlState::InvalidSchemaName,
            "42703" => SqlState::UndefinedColumn,
            "42704" => SqlState::UndefinedObject,
            "42P10" => SqlState::InvalidColumnReference,
            "42P07" => SqlState::DuplicateTable,
            "42804" => SqlState::DatatypeMismatch,
            "22012" => SqlState::DivisionByZero,
            "22023" => SqlState::InvalidParameterValue,
            "22003" => SqlState::NumericValueOutOfRange,
            "22001" => SqlState::StringDataRightTruncation,
            "22P02" => SqlState::InvalidTextRepresentation,
            "22P04" => SqlState::BadCopyFileFormat,
            "23502" => SqlState::NotNullViolation,
            "23505" => SqlState::UniqueViolation,
            "23514" => SqlState::CheckViolation,
            "21000" => SqlState::CardinalityViolation,
            "2BP01" => SqlState::DependentObjectsStillExist,
            "55000" => SqlState::ObjectNotInPrerequisiteState,
            "57014" => SqlState::QueryCanceled,
            "0A000" => SqlState::FeatureNotSupported,
            "25P02" => SqlState::InFailedSqlTransaction,
            "25P01" => SqlState::NoActiveSqlTransaction,
            "3B001" => SqlState::InvalidSavepointSpecification,
            "54000" => SqlState::ProgramLimitExceeded,
            "40001" => SqlState::SerializationFailure,
            "40P01" => SqlState::DeadlockDetected,
            "58030" => SqlState::IoError,
            "XX000" => SqlState::InternalError,
            _ => return None,
        })
    }
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

#[cfg(test)]
mod tests {
    use super::SqlState;

    #[test]
    fn sqlstate_code_round_trips_known_codes() {
        for state in [
            SqlState::SuccessfulCompletion,
            SqlState::SyntaxError,
            SqlState::UndefinedTable,
            SqlState::InvalidSchemaName,
            SqlState::UndefinedColumn,
            SqlState::UndefinedObject,
            SqlState::InvalidColumnReference,
            SqlState::DuplicateTable,
            SqlState::DatatypeMismatch,
            SqlState::DivisionByZero,
            SqlState::InvalidParameterValue,
            SqlState::NumericValueOutOfRange,
            SqlState::StringDataRightTruncation,
            SqlState::InvalidTextRepresentation,
            SqlState::BadCopyFileFormat,
            SqlState::NotNullViolation,
            SqlState::UniqueViolation,
            SqlState::CheckViolation,
            SqlState::CardinalityViolation,
            SqlState::DependentObjectsStillExist,
            SqlState::ObjectNotInPrerequisiteState,
            SqlState::QueryCanceled,
            SqlState::FeatureNotSupported,
            SqlState::InFailedSqlTransaction,
            SqlState::NoActiveSqlTransaction,
            SqlState::InvalidSavepointSpecification,
            SqlState::ProgramLimitExceeded,
            SqlState::SerializationFailure,
            SqlState::DeadlockDetected,
            SqlState::IoError,
            SqlState::InternalError,
        ] {
            assert_eq!(SqlState::from_code(state.code()), Some(state));
        }
        assert_eq!(SqlState::from_code("99999"), None);
    }
}
