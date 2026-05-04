use common::{DataType, DbError, ExecRow, Result, SqlState, Value};
use planner::{AggregateFunc, BinOp, BoundExpr, UnaryOp};

pub fn eval_expr(expr: &BoundExpr, row: &ExecRow) -> Result<Value> {
    match expr {
        BoundExpr::Literal { value, .. } => Ok(value.clone()),
        BoundExpr::InputRef { slot, .. } | BoundExpr::LocalRef { slot, .. } => row
            .row
            .values
            .get(*slot)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("input slot {slot} is out of bounds"))),
        BoundExpr::BinaryOp {
            left, op, right, ..
        } => eval_binary(left, *op, right, row),
        BoundExpr::UnaryOp { op, expr, .. } => eval_unary(*op, expr, row),
        BoundExpr::Function { name, .. } => Err(DbError::internal(format!(
            "function {name} reached executor scalar evaluation"
        ))),
        BoundExpr::AggregateCall { func, .. } => Err(DbError::internal(format!(
            "aggregate {} reached executor scalar evaluation",
            aggregate_name(*func)
        ))),
        BoundExpr::IsNull { expr, .. } => {
            Ok(Value::Boolean(matches!(eval_expr(expr, row)?, Value::Null)))
        }
        BoundExpr::IsNotNull { expr, .. } => Ok(Value::Boolean(!matches!(
            eval_expr(expr, row)?,
            Value::Null
        ))),
        BoundExpr::InList {
            expr,
            list,
            negated,
            ..
        } => {
            let result = eval_in_list(expr, list, row)?;
            if *negated {
                sql_not(result)
            } else {
                Ok(result)
            }
        }
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            ..
        } => {
            let value = eval_expr(expr, row)?;
            let low = eval_expr(low, row)?;
            let high = eval_expr(high, row)?;
            let lower = compare_values(&value, BinOp::GtEq, &low)?;
            let upper = compare_values(&value, BinOp::LtEq, &high)?;
            let result = sql_and(lower, upper)?;
            if *negated {
                sql_not(result)
            } else {
                Ok(result)
            }
        }
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let value = eval_expr(expr, row)?;
            let pattern = eval_expr(pattern, row)?;
            let result = eval_like(value, pattern)?;
            if *negated {
                sql_not(result)
            } else {
                Ok(result)
            }
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => eval_case(
            operand.as_deref(),
            when_clauses,
            else_clause.as_deref(),
            row,
        ),
        BoundExpr::Cast {
            expr, data_type, ..
        } => cast_value(eval_expr(expr, row)?, data_type),
    }
}

fn eval_binary(left: &BoundExpr, op: BinOp, right: &BoundExpr, row: &ExecRow) -> Result<Value> {
    let left = eval_expr(left, row)?;
    let right = eval_expr(right, row)?;
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            arithmetic_values(left, op, right)
        }
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
            compare_values(&left, op, &right)
        }
        BinOp::And => sql_and(left, right),
        BinOp::Or => sql_or(left, right),
        BinOp::Concat => concat_values(left, right),
    }
}

fn eval_unary(op: UnaryOp, expr: &BoundExpr, row: &ExecRow) -> Result<Value> {
    let value = eval_expr(expr, row)?;
    match op {
        UnaryOp::Neg => match value {
            Value::Null => Ok(Value::Null),
            Value::Integer(value) => value
                .checked_neg()
                .map(Value::Integer)
                .ok_or_else(integer_overflow),
            _ => datatype_mismatch("unary minus requires integer"),
        },
        UnaryOp::Not => sql_not(value),
    }
}

fn arithmetic_values(left: Value, op: BinOp, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(left), Value::Integer(right)) => match op {
            BinOp::Add => checked_integer(left.checked_add(right)),
            BinOp::Sub => checked_integer(left.checked_sub(right)),
            BinOp::Mul => checked_integer(left.checked_mul(right)),
            BinOp::Div if right == 0 => Err(DbError::execute(
                SqlState::DivisionByZero,
                "division by zero",
            )),
            BinOp::Div => checked_integer(left.checked_div(right)),
            BinOp::Mod if right == 0 => Err(DbError::execute(
                SqlState::DivisionByZero,
                "division by zero",
            )),
            BinOp::Mod => checked_integer(left.checked_rem(right)),
            _ => unreachable!(),
        },
        _ => datatype_mismatch("arithmetic operands must be integers"),
    }
}

fn checked_integer(value: Option<i64>) -> Result<Value> {
    value.map(Value::Integer).ok_or_else(integer_overflow)
}

pub(crate) fn compare_values(left: &Value, op: BinOp, right: &Value) -> Result<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let ordering = match (left, right) {
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Integer(left), Value::Integer(right)) => left.cmp(right),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        _ => return datatype_mismatch("comparison operands have different types"),
    };

    let result = match op {
        BinOp::Eq => ordering.is_eq(),
        BinOp::Neq => !ordering.is_eq(),
        BinOp::Lt => ordering.is_lt(),
        BinOp::LtEq => ordering.is_le(),
        BinOp::Gt => ordering.is_gt(),
        BinOp::GtEq => ordering.is_ge(),
        _ => unreachable!(),
    };
    Ok(Value::Boolean(result))
}

fn concat_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(left), Value::Text(right)) => Ok(Value::Text(format!("{left}{right}"))),
        _ => datatype_mismatch("concatenation operands must be text"),
    }
}

pub(crate) fn sql_and(left: Value, right: Value) -> Result<Value> {
    match (
        boolean_or_null(left, "AND")?,
        boolean_or_null(right, "AND")?,
    ) {
        (Some(false), _) | (_, Some(false)) => Ok(Value::Boolean(false)),
        (Some(true), Some(true)) => Ok(Value::Boolean(true)),
        (Some(true), None) | (None, Some(true)) | (None, None) => Ok(Value::Null),
    }
}

pub(crate) fn sql_or(left: Value, right: Value) -> Result<Value> {
    match (boolean_or_null(left, "OR")?, boolean_or_null(right, "OR")?) {
        (Some(true), _) | (_, Some(true)) => Ok(Value::Boolean(true)),
        (Some(false), Some(false)) => Ok(Value::Boolean(false)),
        (Some(false), None) | (None, Some(false)) | (None, None) => Ok(Value::Null),
    }
}

fn boolean_or_null(value: Value, operator: &str) -> Result<Option<bool>> {
    match value {
        Value::Null => Ok(None),
        Value::Boolean(value) => Ok(Some(value)),
        _ => datatype_mismatch(format!("{operator} operands must be boolean")),
    }
}

pub(crate) fn sql_not(value: Value) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Boolean(value) => Ok(Value::Boolean(!value)),
        _ => datatype_mismatch("NOT operand must be boolean"),
    }
}

fn eval_in_list(expr: &BoundExpr, list: &[BoundExpr], row: &ExecRow) -> Result<Value> {
    let left = eval_expr(expr, row)?;
    if matches!(left, Value::Null) {
        return Ok(Value::Null);
    }

    let mut saw_null = false;
    for item in list {
        let right = eval_expr(item, row)?;
        if matches!(right, Value::Null) {
            saw_null = true;
            continue;
        }
        if matches!(
            compare_values(&left, BinOp::Eq, &right)?,
            Value::Boolean(true)
        ) {
            return Ok(Value::Boolean(true));
        }
    }

    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(false))
    }
}

fn eval_like(value: Value, pattern: Value) -> Result<Value> {
    match (value, pattern) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(value), Value::Text(pattern)) => {
            Ok(Value::Boolean(like_matches(&value, &pattern)))
        }
        _ => datatype_mismatch("LIKE operands must be text"),
    }
}

fn like_matches(value: &str, pattern: &str) -> bool {
    #[derive(Clone, Copy)]
    enum Token {
        AnySeq,
        AnyOne,
        Char(char),
    }

    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '%' => tokens.push(Token::AnySeq),
            '_' => tokens.push(Token::AnyOne),
            '\\' => match chars.next() {
                Some(escaped @ ('%' | '_' | '\\')) => tokens.push(Token::Char(escaped)),
                Some(other) => {
                    tokens.push(Token::Char('\\'));
                    tokens.push(Token::Char(other));
                }
                None => tokens.push(Token::Char('\\')),
            },
            other => tokens.push(Token::Char(other)),
        }
    }

    let value: Vec<char> = value.chars().collect();
    let mut memo = vec![vec![None; value.len() + 1]; tokens.len() + 1];
    fn matches_at(
        tokens: &[Token],
        value: &[char],
        token_index: usize,
        value_index: usize,
        memo: &mut [Vec<Option<bool>>],
    ) -> bool {
        if let Some(result) = memo[token_index][value_index] {
            return result;
        }
        let result = if token_index == tokens.len() {
            value_index == value.len()
        } else {
            match tokens[token_index] {
                Token::AnySeq => {
                    matches_at(tokens, value, token_index + 1, value_index, memo)
                        || (value_index < value.len()
                            && matches_at(tokens, value, token_index, value_index + 1, memo))
                }
                Token::AnyOne => {
                    value_index < value.len()
                        && matches_at(tokens, value, token_index + 1, value_index + 1, memo)
                }
                Token::Char(ch) => {
                    value.get(value_index) == Some(&ch)
                        && matches_at(tokens, value, token_index + 1, value_index + 1, memo)
                }
            }
        };
        memo[token_index][value_index] = Some(result);
        result
    }

    matches_at(&tokens, &value, 0, 0, &mut memo)
}

fn eval_case(
    operand: Option<&BoundExpr>,
    when_clauses: &[(BoundExpr, BoundExpr)],
    else_clause: Option<&BoundExpr>,
    row: &ExecRow,
) -> Result<Value> {
    let operand_value = operand.map(|expr| eval_expr(expr, row)).transpose()?;

    for (when, then) in when_clauses {
        let condition = if let Some(operand_value) = &operand_value {
            let when_value = eval_expr(when, row)?;
            compare_values(operand_value, BinOp::Eq, &when_value)?
        } else {
            eval_expr(when, row)?
        };
        if matches!(condition, Value::Boolean(true)) {
            return eval_expr(then, row);
        }
        if !matches!(condition, Value::Boolean(false) | Value::Null) {
            return datatype_mismatch("CASE condition must be boolean");
        }
    }

    match else_clause {
        Some(expr) => eval_expr(expr, row),
        None => Ok(Value::Null),
    }
}

fn cast_value(value: Value, data_type: &DataType) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match (value, data_type) {
        (Value::Integer(value), DataType::Integer) => Ok(Value::Integer(value)),
        (Value::Text(value), DataType::Text) => Ok(Value::Text(value)),
        (Value::Boolean(value), DataType::Boolean) => Ok(Value::Boolean(value)),
        (Value::Integer(value), DataType::Text) => Ok(Value::Text(value.to_string())),
        (Value::Boolean(value), DataType::Text) => Ok(Value::Text(value.to_string())),
        (Value::Text(value), DataType::Integer) => value
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| DbError::execute(SqlState::DatatypeMismatch, "invalid integer cast")),
        (Value::Text(value), DataType::Boolean) => match value.to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Ok(Value::Boolean(true)),
            "false" | "f" | "0" => Ok(Value::Boolean(false)),
            _ => datatype_mismatch("invalid boolean cast"),
        },
        _ => datatype_mismatch("unsupported cast"),
    }
}

fn aggregate_name(func: AggregateFunc) -> &'static str {
    match func {
        AggregateFunc::Count => "COUNT",
        AggregateFunc::Sum => "SUM",
        AggregateFunc::Avg => "AVG",
        AggregateFunc::Min => "MIN",
        AggregateFunc::Max => "MAX",
    }
}

pub(crate) fn datatype_mismatch<T>(message: impl Into<String>) -> Result<T> {
    Err(DbError::execute(SqlState::DatatypeMismatch, message))
}

pub(crate) fn integer_overflow() -> DbError {
    DbError::execute(
        SqlState::NumericValueOutOfRange,
        "integer value out of range",
    )
}
