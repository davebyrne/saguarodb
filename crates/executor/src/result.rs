use common::{ColumnId, ColumnInfo, CopyOptions, Row, TableId};

/// A bound `COPY` request. Binding resolves the table and column list; the server
/// then drives the COPY sub-protocol (the executor's `CopyIn`/`CopyOut` do the
/// actual insert/scan). See `docs/specs/copy.md`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyJob {
    pub table: TableId,
    /// Columns in COPY order (defaulted to all columns in catalog order).
    pub columns: Vec<ColumnId>,
    pub options: CopyOptions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionResult {
    Query {
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
    },
    Modified {
        command: String,
        count: u64,
    },
    Explanation {
        text: String,
    },
    /// The statement is `COPY ... FROM STDIN`: the server must send
    /// `CopyInResponse` and stream the client's data into the table. Carries the
    /// bound request, not a finished result.
    BeginCopyIn(CopyJob),
    /// The statement is `COPY ... TO STDOUT`: the server must send
    /// `CopyOutResponse` and stream the table's rows to the client.
    BeginCopyOut(CopyJob),
}

impl ExecutionResult {
    pub fn row_count(&self) -> usize {
        match self {
            ExecutionResult::Query { rows, .. } => rows.len(),
            ExecutionResult::Modified { count, .. } => *count as usize,
            ExecutionResult::Explanation { .. } => 1,
            // No rows have been transferred yet; the COPY has not run.
            ExecutionResult::BeginCopyIn(_) | ExecutionResult::BeginCopyOut(_) => 0,
        }
    }
}
