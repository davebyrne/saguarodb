use common::{ColumnInfo, DbError, ExecRow, Result, Row, SqlState, StatementContext, Value};
use planner::BoundExpr;

use crate::eval_expr;
use crate::query::PlanExecutor;

pub struct TableFunctionOp {
    ctx: StatementContext,
    name: String,
    args: Vec<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    rows: Vec<Value>,
    index: usize,
}

impl TableFunctionOp {
    pub fn new(
        ctx: StatementContext,
        name: String,
        args: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            ctx,
            name,
            args,
            output_schema,
            rows: Vec::new(),
            index: 0,
        }
    }
}

impl PlanExecutor for TableFunctionOp {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.rows.clear();
        self.index = 0;
        let empty = ExecRow {
            row: Row { values: Vec::new() },
            identity: None,
        };
        let args = self
            .args
            .iter()
            .map(|arg| eval_expr(&self.ctx, arg, &empty))
            .collect::<Result<Vec<_>>>()?;
        match (self.name.as_str(), args.as_slice()) {
            ("unnest", [Value::Array(array)]) => self.rows.extend(array.elements().iter().cloned()),
            ("unnest", [Value::Null]) => {}
            ("generate_series", [Value::Integer(start), Value::Integer(stop)]) => {
                generate_series(*start, *stop, 1, &mut self.rows)?;
            }
            (
                "generate_series",
                [
                    Value::Integer(start),
                    Value::Integer(stop),
                    Value::Integer(step),
                ],
            ) => {
                generate_series(*start, *stop, *step, &mut self.rows)?;
            }
            ("generate_series", values) if values.iter().any(|v| matches!(v, Value::Null)) => {}
            _ => return Err(DbError::internal("invalid bound table-function arguments")),
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let Some(value) = self.rows.get(self.index).cloned() else {
            return Ok(None);
        };
        self.index += 1;
        Ok(Some(ExecRow {
            row: Row {
                values: vec![value],
            },
            identity: None,
        }))
    }

    fn close(&mut self) -> Result<()> {
        self.rows.clear();
        Ok(())
    }
}

fn generate_series(start: i64, stop: i64, step: i64, rows: &mut Vec<Value>) -> Result<()> {
    if step == 0 {
        return Err(DbError::execute(
            SqlState::InvalidParameterValue,
            "step size cannot equal zero",
        ));
    }
    let mut value = start;
    while (step > 0 && value <= stop) || (step < 0 && value >= stop) {
        if rows.len() >= common::MAX_ARRAY_ELEMENTS {
            return Err(DbError::execute(
                SqlState::ProgramLimitExceeded,
                "generated series is too large",
            ));
        }
        rows.push(Value::Integer(value));
        let Some(next) = value.checked_add(step) else {
            break;
        };
        value = next;
    }
    Ok(())
}
