use common::{ColumnInfo, Row};

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
}

impl ExecutionResult {
    pub fn row_count(&self) -> usize {
        match self {
            ExecutionResult::Query { rows, .. } => rows.len(),
            ExecutionResult::Modified { count, .. } => *count as usize,
            ExecutionResult::Explanation { .. } => 1,
        }
    }
}
