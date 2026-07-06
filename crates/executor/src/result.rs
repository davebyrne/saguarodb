use common::{ColumnId, ColumnInfo, CopyOptions, Row, TableId};
use planner::BoundExpr;

/// A bound `COPY` request. Binding resolves the table and column list; the server
/// then drives the COPY sub-protocol (the executor's `CopyIn`/`CopyOut` do the
/// actual insert/scan). See `docs/specs/copy.md`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyJob {
    pub table: TableId,
    /// Columns in COPY order (defaulted to all columns in catalog order).
    pub columns: Vec<ColumnId>,
    pub options: CopyOptions,
    /// Bound expression `DEFAULT`s for columns omitted by `COPY FROM`, evaluated
    /// per row (empty for `COPY TO`). See [`crate::CopyIn`].
    pub default_exprs: Vec<(ColumnId, BoundExpr)>,
    /// The table's bound `CHECK` constraints, enforced per row by `COPY FROM`
    /// (empty for `COPY TO`).
    pub check_exprs: Vec<BoundExpr>,
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
    /// A DML statement (`INSERT`/`UPDATE`/`DELETE`) with a `RETURNING` clause: it
    /// both modifies rows and produces a result set. `count` is the affected-row
    /// count for the `CommandComplete` tag (e.g. `INSERT 0 n`); `columns`/`rows`
    /// are the `RETURNING` projection sent as `RowDescription` + `DataRow`s.
    ModifiedReturning {
        command: String,
        count: u64,
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
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
            ExecutionResult::Modified { count, .. }
            | ExecutionResult::ModifiedReturning { count, .. } => *count as usize,
            ExecutionResult::Explanation { .. } => 1,
            // No rows have been transferred yet; the COPY has not run.
            ExecutionResult::BeginCopyIn(_) | ExecutionResult::BeginCopyOut(_) => 0,
        }
    }
}
