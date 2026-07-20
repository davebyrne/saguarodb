use common::{
    ArrayDimension, DataType, DbError, ExecRow, PgType, Result, SqlArray, SqlState,
    StatementContext, Value,
};
use planner::{AggregateFunc, BinOp, BoundExpr, UnaryOp};

pub fn eval_expr(ctx: &StatementContext, expr: &BoundExpr, row: &ExecRow) -> Result<Value> {
    eval_expr_inner(ctx, expr, row)
}

fn eval_expr_inner(ctx: &StatementContext, expr: &BoundExpr, row: &ExecRow) -> Result<Value> {
    match expr {
        BoundExpr::Literal { value, .. } => Ok(value.clone()),
        // Parameters are replaced with literals by substitution before execution.
        BoundExpr::Parameter { index, .. } => Err(DbError::internal(format!(
            "unbound parameter ${} reached the executor",
            index + 1
        ))),
        BoundExpr::InputRef { slot, .. } | BoundExpr::LocalRef { slot, .. } => row
            .row
            .values
            .get(*slot)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("input slot {slot} is out of bounds"))),
        BoundExpr::BinaryOp {
            left, op, right, ..
        } => eval_binary(ctx, left, *op, right, row),
        BoundExpr::UnaryOp { op, expr, .. } => eval_unary(ctx, *op, expr, row),
        BoundExpr::Function { name, args, .. } => eval_function(ctx, name, args, row),
        BoundExpr::Array {
            elements,
            dimensions,
            element_type,
            ..
        } => {
            let values = elements
                .iter()
                .map(|element| eval_expr_inner(ctx, element, row))
                .collect::<Result<Vec<_>>>()?;
            let dimensions = dimensions
                .iter()
                .map(|len| ArrayDimension::new(*len, 1))
                .collect();
            Ok(Value::Array(SqlArray::new(
                element_type.clone(),
                dimensions,
                values,
            )?))
        }
        BoundExpr::ArraySubscript {
            array, subscripts, ..
        } => {
            let array = eval_expr_inner(ctx, array, row)?;
            if matches!(array, Value::Null) {
                return Ok(Value::Null);
            }
            let Value::Array(array) = array else {
                return datatype_mismatch("array subscript requires an array operand");
            };
            let mut indexes = Vec::with_capacity(subscripts.len());
            for subscript in subscripts {
                match eval_expr_inner(ctx, subscript, row)? {
                    Value::Integer(index) => indexes.push(index),
                    Value::Null => return Ok(Value::Null),
                    _ => return datatype_mismatch("array subscript must be an integer"),
                }
            }
            Ok(array
                .element_offset(&indexes)
                .and_then(|offset| array.elements().get(offset))
                .cloned()
                .unwrap_or(Value::Null))
        }
        BoundExpr::Any {
            left, op, array, ..
        } => eval_any(ctx, left, *op, array, row),
        BoundExpr::Nextval { sequence, .. } => eval_nextval(ctx, *sequence),
        BoundExpr::Currval { sequence, .. } => eval_currval(ctx, *sequence),
        BoundExpr::Setval {
            sequence,
            value,
            is_called,
            ..
        } => eval_setval(ctx, *sequence, value, is_called.as_deref(), row),
        BoundExpr::AggregateCall { func, .. } => Err(DbError::internal(format!(
            "aggregate {} reached executor scalar evaluation",
            aggregate_name(*func)
        ))),
        BoundExpr::WindowCall { .. } => Err(DbError::internal(
            "window function reached scalar evaluation",
        )),
        BoundExpr::IsNull { expr, .. } => Ok(Value::Boolean(matches!(
            eval_expr_inner(ctx, expr, row)?,
            Value::Null
        ))),
        BoundExpr::IsNotNull { expr, .. } => Ok(Value::Boolean(!matches!(
            eval_expr_inner(ctx, expr, row)?,
            Value::Null
        ))),
        BoundExpr::InList {
            expr,
            list,
            negated,
            ..
        } => {
            let result = eval_in_list(ctx, expr, list, row)?;
            if *negated {
                sql_not(result)
            } else {
                Ok(result)
            }
        }
        BoundExpr::RuntimeInSet {
            expr, set, negated, ..
        } => {
            let operand = eval_expr_inner(ctx, expr, row)?;
            ctx.runtime_value_sets.evaluate(*set, &operand, *negated)
        }
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            ..
        } => {
            let value = eval_expr_inner(ctx, expr, row)?;
            let low = eval_expr_inner(ctx, low, row)?;
            let high = eval_expr_inner(ctx, high, row)?;
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
            case_insensitive,
            escape,
            ..
        } => {
            let value = eval_expr_inner(ctx, expr, row)?;
            let pattern = eval_expr_inner(ctx, pattern, row)?;
            let result = eval_like(value, pattern, *case_insensitive, *escape)?;
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
            ctx,
            operand.as_deref(),
            when_clauses,
            else_clause.as_deref(),
            row,
        ),
        BoundExpr::Cast {
            expr,
            data_type,
            pg_type,
            ..
        } => {
            let value = cast_value(eval_expr_inner(ctx, expr, row)?, data_type)?;
            check_cast_int_width(value, pg_type)
        }
        // Subqueries are resolved to literals (or an `IN` list) by the executor's
        // pre-pass before any row is evaluated; reaching here is a routing bug.
        BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. }
        | BoundExpr::InSubquery { .. } => Err(DbError::internal(
            "subquery expression reached scalar evaluation without being resolved",
        )),
        // A correlated reference is substituted to a literal before its
        // subquery body executes (`docs/specs/subqueries.md` §5.2); reaching
        // here is a routing bug.
        BoundExpr::OuterRef { .. } => Err(DbError::internal(
            "correlated outer reference reached scalar evaluation without being substituted",
        )),
    }
}

fn eval_binary(
    ctx: &StatementContext,
    left: &BoundExpr,
    op: BinOp,
    right: &BoundExpr,
    row: &ExecRow,
) -> Result<Value> {
    let left = eval_expr_inner(ctx, left, row)?;
    let right = eval_expr_inner(ctx, right, row)?;
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
        BinOp::IsDistinctFrom => eval_is_distinct(left, right, false),
        BinOp::IsNotDistinctFrom => eval_is_distinct(left, right, true),
    }
}

fn eval_any(
    ctx: &StatementContext,
    left: &BoundExpr,
    op: BinOp,
    array: &BoundExpr,
    row: &ExecRow,
) -> Result<Value> {
    let left = eval_expr_inner(ctx, left, row)?;
    let array = eval_expr_inner(ctx, array, row)?;
    if matches!(array, Value::Null) {
        return Ok(Value::Null);
    }
    let Value::Array(array) = array else {
        return datatype_mismatch("ANY requires an array operand");
    };
    let mut saw_null = false;
    for element in array.elements() {
        match compare_values(&left, op, element)? {
            Value::Boolean(true) => return Ok(Value::Boolean(true)),
            Value::Boolean(false) => {}
            Value::Null => saw_null = true,
            _ => return Err(DbError::internal("comparison returned a non-boolean value")),
        }
    }
    Ok(if saw_null {
        Value::Null
    } else {
        Value::Boolean(false)
    })
}

/// NULL-safe comparison backing `IS [NOT] DISTINCT FROM`. Two NULLs are *not*
/// distinct; a NULL and a non-NULL are distinct; otherwise it follows ordinary
/// equality. The result is always a boolean, never NULL.
fn eval_is_distinct(left: Value, right: Value, not: bool) -> Result<Value> {
    let equal = match (&left, &right) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => matches!(
            compare_values(&left, BinOp::Eq, &right)?,
            Value::Boolean(true)
        ),
    };
    // `a IS DISTINCT FROM b` is `!equal`; `a IS NOT DISTINCT FROM b` is `equal`.
    Ok(Value::Boolean(if not { equal } else { !equal }))
}

fn eval_unary(
    ctx: &StatementContext,
    op: UnaryOp,
    expr: &BoundExpr,
    row: &ExecRow,
) -> Result<Value> {
    let value = eval_expr_inner(ctx, expr, row)?;
    match op {
        UnaryOp::Neg => match value {
            Value::Null => Ok(Value::Null),
            Value::Integer(value) => value
                .checked_neg()
                .map(Value::Integer)
                .ok_or_else(integer_overflow),
            Value::Float(value) => Ok(Value::Float((-value.0).into())),
            Value::Real(value) => Ok(Value::Real((-value.0).into())),
            Value::Numeric(value) => Ok(Value::Numeric(-value)),
            Value::Interval(value) => value
                .checked_neg()
                .map(Value::Interval)
                .ok_or_else(interval_overflow),
            _ => datatype_mismatch("unary minus requires a numeric or interval operand"),
        },
        UnaryOp::Not => sql_not(value),
    }
}

fn eval_nextval(ctx: &StatementContext, sequence: common::SequenceId) -> Result<Value> {
    Ok(Value::Integer(ctx.nextval_recording_currval(sequence)?))
}

fn eval_currval(ctx: &StatementContext, sequence: common::SequenceId) -> Result<Value> {
    if !ctx.sequence_manager.sequence_exists(sequence)? {
        return Err(DbError::execute(
            SqlState::UndefinedTable,
            format!("sequence id {sequence} does not exist"),
        ));
    }
    let Some(value) = ctx.session_sequences.currval(sequence)? else {
        return Err(DbError::execute(
            SqlState::ObjectNotInPrerequisiteState,
            "currval is not yet defined in this session",
        ));
    };
    Ok(Value::Integer(value))
}

fn eval_setval(
    ctx: &StatementContext,
    sequence: common::SequenceId,
    value: &BoundExpr,
    is_called: Option<&BoundExpr>,
    row: &ExecRow,
) -> Result<Value> {
    let value = eval_expr_inner(ctx, value, row)?;
    let Value::Integer(value) = value else {
        return if matches!(value, Value::Null) {
            Ok(Value::Null)
        } else {
            datatype_mismatch("setval value must be an integer")
        };
    };
    let is_called = match is_called {
        Some(expr) => match eval_expr_inner(ctx, expr, row)? {
            Value::Boolean(value) => value,
            Value::Null => return Ok(Value::Null),
            _ => return datatype_mismatch("setval is_called argument must be boolean"),
        },
        None => true,
    };
    let value = ctx
        .sequence_manager
        .setval(ctx.txn_id, sequence, value, is_called)?;
    if is_called {
        ctx.session_sequences.record_currval(sequence, value)?;
    }
    Ok(Value::Integer(value))
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
            _ => datatype_mismatch("operator is not defined for integers"),
        },
        (Value::Float(left), Value::Float(right)) => {
            let (left, right) = (left.0, right.0);
            let result = match op {
                BinOp::Add => left + right,
                BinOp::Sub => left - right,
                BinOp::Mul => left * right,
                // PostgreSQL raises division by zero for float division too.
                BinOp::Div if right == 0.0 => {
                    return Err(DbError::execute(
                        SqlState::DivisionByZero,
                        "division by zero",
                    ));
                }
                BinOp::Div => left / right,
                // Modulo is rejected for double precision during binding.
                _ => return datatype_mismatch("modulo is not defined for double precision"),
            };
            Ok(Value::Float(result.into()))
        }
        (Value::Real(left), Value::Real(right)) => {
            let (left, right) = (left.0, right.0);
            let result = match op {
                BinOp::Add => left + right,
                BinOp::Sub => left - right,
                BinOp::Mul => left * right,
                BinOp::Div if right == 0.0 => {
                    return Err(DbError::execute(
                        SqlState::DivisionByZero,
                        "division by zero",
                    ));
                }
                BinOp::Div => left / right,
                // Modulo is rejected for floating-point types during binding.
                _ => return datatype_mismatch("modulo is not defined for real"),
            };
            Ok(Value::Real(result.into()))
        }
        (Value::Numeric(left), Value::Numeric(right)) => {
            // Exact decimal arithmetic. `checked_*` avoid rust_decimal's panic-on-
            // overflow; division/modulo by zero error like INTEGER. Result scale
            // follows rust_decimal (add/sub: max scale; mul: sum of scales;
            // div: up to 28 significant digits).
            let result = match op {
                BinOp::Add => left.checked_add(right),
                BinOp::Sub => left.checked_sub(right),
                BinOp::Mul => left.checked_mul(right),
                BinOp::Div if right == common::Decimal::ZERO => {
                    return Err(DbError::execute(
                        SqlState::DivisionByZero,
                        "division by zero",
                    ));
                }
                BinOp::Div => left.checked_div(right),
                BinOp::Mod if right == common::Decimal::ZERO => {
                    return Err(DbError::execute(
                        SqlState::DivisionByZero,
                        "division by zero",
                    ));
                }
                BinOp::Mod => left.checked_rem(right),
                _ => return datatype_mismatch("operator is not defined for numeric values"),
            };
            result.map(Value::Numeric).ok_or_else(numeric_overflow)
        }
        // INTERVAL arithmetic: interval +/- interval, interval * integer.
        (Value::Interval(left), Value::Interval(right)) => match op {
            BinOp::Add => left
                .checked_add(right)
                .map(Value::Interval)
                .ok_or_else(interval_overflow),
            BinOp::Sub => left
                .checked_sub(right)
                .map(Value::Interval)
                .ok_or_else(interval_overflow),
            _ => datatype_mismatch("operator is not defined for two intervals"),
        },
        (Value::Interval(iv), Value::Integer(n)) | (Value::Integer(n), Value::Interval(iv))
            if matches!(op, BinOp::Mul) =>
        {
            iv.checked_mul_int(n)
                .map(Value::Interval)
                .ok_or_else(interval_overflow)
        }
        // <temporal> +/- INTERVAL (and INTERVAL + <temporal>). DATE + INTERVAL
        // yields a TIMESTAMP, matching PostgreSQL.
        (Value::Timestamp(micros), Value::Interval(iv)) => {
            shift_timestamp(micros, iv, op).map(Value::Timestamp)
        }
        (Value::Interval(iv), Value::Timestamp(micros)) if matches!(op, BinOp::Add) => {
            shift_timestamp(micros, iv, BinOp::Add).map(Value::Timestamp)
        }
        (Value::TimestampTz(micros), Value::Interval(iv)) => {
            shift_timestamp(micros, iv, op).map(Value::TimestampTz)
        }
        (Value::Interval(iv), Value::TimestampTz(micros)) if matches!(op, BinOp::Add) => {
            shift_timestamp(micros, iv, BinOp::Add).map(Value::TimestampTz)
        }
        (Value::Date(days), Value::Interval(iv)) => {
            shift_timestamp(date_to_micros(days)?, iv, op).map(Value::Timestamp)
        }
        (Value::Interval(iv), Value::Date(days)) if matches!(op, BinOp::Add) => {
            shift_timestamp(date_to_micros(days)?, iv, BinOp::Add).map(Value::Timestamp)
        }
        (Value::Time(time), Value::Interval(iv)) => shift_time(time, iv, op).map(Value::Time),
        (Value::Interval(iv), Value::Time(time)) if matches!(op, BinOp::Add) => {
            shift_time(time, iv, BinOp::Add).map(Value::Time)
        }
        _ => datatype_mismatch("arithmetic operands must be the same numeric type"),
    }
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Convert a `DATE` (days from epoch) to microseconds-from-epoch.
fn date_to_micros(days: i64) -> Result<i64> {
    days.checked_mul(MICROS_PER_DAY)
        .ok_or_else(datetime_overflow)
}

/// Apply `+/- interval` to a timestamp-like microsecond value (calendar-aware).
fn shift_timestamp(micros: i64, iv: common::Interval, op: BinOp) -> Result<i64> {
    let iv = signed_interval(iv, op)?;
    common::datetime::add_interval_to_timestamp(micros, &iv).ok_or_else(datetime_overflow)
}

/// Apply `+/- interval` to a `TIME` (wraps mod 24h; months/days ignored).
fn shift_time(time: i64, iv: common::Interval, op: BinOp) -> Result<i64> {
    // TIME uses only the interval's microsecond component (months/days are ignored
    // and wrap away). Reduce mod a day before negating so no value can overflow —
    // notably an `i32::MIN` month count, which `signed_interval`'s full negation
    // would reject even though months don't affect a TIME.
    let micros = iv.micros.rem_euclid(MICROS_PER_DAY);
    let signed = match op {
        BinOp::Add => micros,
        BinOp::Sub => -micros, // safe: micros is in [0, MICROS_PER_DAY)
        _ => return datatype_mismatch("operator is not defined for time and interval"),
    };
    Ok(common::datetime::add_interval_to_time(
        time,
        &common::Interval::new(0, 0, signed),
    ))
}

/// `interval` for `Add`, its negation for `Sub`; any other operator is invalid.
fn signed_interval(iv: common::Interval, op: BinOp) -> Result<common::Interval> {
    match op {
        BinOp::Add => Ok(iv),
        BinOp::Sub => iv.checked_neg().ok_or_else(interval_overflow),
        _ => Err(DbError::execute(
            SqlState::DatatypeMismatch,
            "operator is not defined for this temporal type and interval",
        )),
    }
}

fn interval_overflow() -> DbError {
    DbError::execute(SqlState::NumericValueOutOfRange, "interval out of range")
}

fn datetime_overflow() -> DbError {
    DbError::execute(SqlState::NumericValueOutOfRange, "timestamp out of range")
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
        // Total order: NaN sorts greatest and equals itself, -0.0 == +0.0
        // (matching PostgreSQL's float comparison operators).
        (Value::Float(left), Value::Float(right)) => left.cmp(right),
        (Value::Real(left), Value::Real(right)) => left.cmp(right),
        // Decimal compares by value, so 1.0 and 1.00 are equal.
        (Value::Numeric(left), Value::Numeric(right)) => left.cmp(right),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        (Value::Date(left), Value::Date(right)) => left.cmp(right),
        (Value::Timestamp(left), Value::Timestamp(right)) => left.cmp(right),
        (Value::Time(left), Value::Time(right)) => left.cmp(right),
        (Value::TimestampTz(left), Value::TimestampTz(right)) => left.cmp(right),
        // Interval compares by canonical estimate (1 mon == 30 days).
        (Value::Interval(left), Value::Interval(right)) => left.cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.cmp(right),
        (Value::Array(left), Value::Array(right)) => left.cmp(right),
        _ => return datatype_mismatch("comparison operands have different types"),
    };

    let result = match op {
        BinOp::Eq => ordering.is_eq(),
        BinOp::Neq => !ordering.is_eq(),
        BinOp::Lt => ordering.is_lt(),
        BinOp::LtEq => ordering.is_le(),
        BinOp::Gt => ordering.is_gt(),
        BinOp::GtEq => ordering.is_ge(),
        _ => return datatype_mismatch("operator is not a comparison operator"),
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

fn eval_function(
    ctx: &StatementContext,
    name: &str,
    args: &[BoundExpr],
    row: &ExecRow,
) -> Result<Value> {
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_expr_inner(ctx, arg, row)?);
    }
    let Some(func) = common::lookup_scalar_function(name) else {
        return Err(DbError::internal(format!(
            "unknown scalar function {name} reached the executor"
        )));
    };
    // Apply the function's NULL policy before evaluating. NeverNull functions
    // (CONCAT, the system information functions) and EvaluateNullable functions
    // (for example `format_type`) are evaluated even when an argument is NULL.
    if matches!(
        func.null_handling,
        common::NullHandling::Propagate | common::NullHandling::Nullable
    ) && values.iter().any(|value| matches!(value, Value::Null))
    {
        return Ok(Value::Null);
    }
    (func.eval)(ctx, &values)
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

fn eval_in_list(
    ctx: &StatementContext,
    expr: &BoundExpr,
    list: &[BoundExpr],
    row: &ExecRow,
) -> Result<Value> {
    let left = eval_expr_inner(ctx, expr, row)?;
    if matches!(left, Value::Null) {
        return Ok(Value::Null);
    }

    let mut saw_null = false;
    for item in list {
        let right = eval_expr_inner(ctx, item, row)?;
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

fn eval_like(
    value: Value,
    pattern: Value,
    case_insensitive: bool,
    escape: Option<char>,
) -> Result<Value> {
    match (value, pattern) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(value), Value::Text(pattern)) => {
            // `ILIKE` lowercases both sides (and the escape character) up front so
            // the structural `%`/`_` matching below is unchanged.
            let (value, pattern, escape) = if case_insensitive {
                (
                    value.to_lowercase(),
                    pattern.to_lowercase(),
                    escape.map(|c| c.to_lowercase().next().unwrap_or(c)),
                )
            } else {
                (value, pattern, escape)
            };
            Ok(Value::Boolean(like_matches(&value, &pattern, escape)))
        }
        _ => datatype_mismatch("LIKE operands must be text"),
    }
}

fn like_matches(value: &str, pattern: &str, escape: Option<char>) -> bool {
    #[derive(Clone, Copy)]
    enum Token {
        AnySeq,
        AnyOne,
        Char(char),
    }

    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if Some(ch) == escape {
            // The escape character: `e%`, `e_`, and `ee` are the literal `%`,
            // `_`, and escape character; `e` before any other character is a
            // literal escape character followed by that character; a trailing
            // lone escape character is itself literal.
            match chars.next() {
                Some(next) if next == '%' || next == '_' || Some(next) == escape => {
                    tokens.push(Token::Char(next));
                }
                Some(other) => {
                    tokens.push(Token::Char(ch));
                    tokens.push(Token::Char(other));
                }
                None => tokens.push(Token::Char(ch)),
            }
        } else {
            match ch {
                '%' => tokens.push(Token::AnySeq),
                '_' => tokens.push(Token::AnyOne),
                other => tokens.push(Token::Char(other)),
            }
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
    ctx: &StatementContext,
    operand: Option<&BoundExpr>,
    when_clauses: &[(BoundExpr, BoundExpr)],
    else_clause: Option<&BoundExpr>,
    row: &ExecRow,
) -> Result<Value> {
    let operand_value = operand
        .map(|expr| eval_expr_inner(ctx, expr, row))
        .transpose()?;

    for (when, then) in when_clauses {
        let condition = if let Some(operand_value) = &operand_value {
            let when_value = eval_expr_inner(ctx, when, row)?;
            compare_values(operand_value, BinOp::Eq, &when_value)?
        } else {
            eval_expr_inner(ctx, when, row)?
        };
        if matches!(condition, Value::Boolean(true)) {
            return eval_expr_inner(ctx, then, row);
        }
        if !matches!(condition, Value::Boolean(false) | Value::Null) {
            return datatype_mismatch("CASE condition must be boolean");
        }
    }

    match else_clause {
        Some(expr) => eval_expr_inner(ctx, expr, row),
        None => Ok(Value::Null),
    }
}

/// Reject a cast result that does not fit an explicit narrow integer target
/// (`CAST(... AS int2/int4)`), matching PostgreSQL — the storage type is a single
/// 64-bit integer, so without this a `CAST(... AS int4)` could yield a value that
/// misrepresents its advertised OID. Non-narrow targets pass through unchanged.
fn check_cast_int_width(value: Value, pg_type: &PgType) -> Result<Value> {
    if let Value::Integer(int) = value
        && let Some(type_name) = pg_type.narrow_int_overflow(int)
    {
        return Err(DbError::execute(
            SqlState::NumericValueOutOfRange,
            format!("{type_name} out of range"),
        ));
    }
    Ok(value)
}

pub(crate) fn cast_value_to_pg_type(value: Value, pg_type: &PgType) -> Result<Value> {
    check_cast_int_width(cast_value(value, &pg_type.data_type())?, pg_type)
}

fn cast_value(value: Value, data_type: &DataType) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match (value, data_type) {
        (Value::Array(array), DataType::Array(target)) => {
            let elements = array
                .elements()
                .iter()
                .cloned()
                .map(|element| cast_value(element, target.element_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Value::Array(SqlArray::new(
                target.element_type().clone(),
                array.dimensions().to_vec(),
                elements,
            )?))
        }
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
        (Value::Date(days), DataType::Date) => Ok(Value::Date(days)),
        (Value::Date(days), DataType::Text) => Ok(Value::Text(common::datetime::format_date(days))),
        (Value::Text(value), DataType::Date) => common::datetime::parse_date(&value)
            .map(Value::Date)
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid date cast")),
        (Value::Timestamp(micros), DataType::Timestamp) => Ok(Value::Timestamp(micros)),
        (Value::Timestamp(micros), DataType::Text) => {
            Ok(Value::Text(common::datetime::format_timestamp(micros)))
        }
        (Value::Text(value), DataType::Timestamp) => common::datetime::parse_timestamp(&value)
            .map(Value::Timestamp)
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid timestamp cast")),
        (Value::Time(micros), DataType::Time) => Ok(Value::Time(micros)),
        (Value::Time(micros), DataType::Text) => {
            Ok(Value::Text(common::datetime::format_time(micros)))
        }
        (Value::Text(value), DataType::Time) => common::datetime::parse_time(&value)
            .map(Value::Time)
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid time cast")),
        (Value::TimestampTz(micros), DataType::TimestampTz) => Ok(Value::TimestampTz(micros)),
        (Value::TimestampTz(micros), DataType::Text) => {
            Ok(Value::Text(common::datetime::format_timestamptz(micros)))
        }
        (Value::Text(value), DataType::TimestampTz) => common::datetime::parse_timestamptz(&value)
            .map(Value::TimestampTz)
            .ok_or_else(|| {
                DbError::execute(SqlState::DatatypeMismatch, "invalid timestamptz cast")
            }),
        // TIMESTAMP <-> TIMESTAMPTZ reinterpret the same micros: a naive
        // wall-clock is taken as UTC (no session time zone), so the instant is
        // unchanged.
        (Value::Timestamp(micros), DataType::TimestampTz) => Ok(Value::TimestampTz(micros)),
        (Value::TimestampTz(micros), DataType::Timestamp) => Ok(Value::Timestamp(micros)),
        (Value::Interval(iv), DataType::Interval) => Ok(Value::Interval(iv)),
        (Value::Interval(iv), DataType::Text) => {
            Ok(Value::Text(common::interval::format_interval(&iv)))
        }
        (Value::Text(value), DataType::Interval) => common::interval::parse_interval(&value)
            .map(Value::Interval)
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid interval cast")),
        (Value::Bytes(raw), DataType::Bytea) => Ok(Value::Bytes(raw)),
        (Value::Bytes(raw), DataType::Text) => Ok(Value::Text(common::bytea::format_hex(&raw))),
        (Value::Text(value), DataType::Bytea) => common::bytea::parse_hex(&value)
            .map(Value::Bytes)
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid bytea cast")),
        (Value::Uuid(raw), DataType::Uuid) => Ok(Value::Uuid(raw)),
        (Value::Uuid(raw), DataType::Text) => Ok(Value::Text(common::uuid::format_uuid(&raw))),
        (Value::Text(value), DataType::Uuid) => common::uuid::parse_uuid(&value)
            .map(Value::Uuid)
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid uuid cast")),
        (Value::Float(value), DataType::Double) => Ok(Value::Float(value)),
        (Value::Float(value), DataType::Text) => {
            Ok(Value::Text(common::float::format_double(value.0)))
        }
        (Value::Text(value), DataType::Double) => common::float::parse_double(&value)
            .map(|v| Value::Float(v.into()))
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid double cast")),
        (Value::Integer(value), DataType::Double) => Ok(Value::Float((value as f64).into())),
        (Value::Float(value), DataType::Integer) => {
            // Round half-to-even (matching PostgreSQL's float-to-int cast) and
            // reject NaN/infinity/out-of-range.
            let rounded = value.0.round_ties_even();
            if rounded.is_finite() && rounded >= -(2f64.powi(63)) && rounded < 2f64.powi(63) {
                Ok(Value::Integer(rounded as i64))
            } else {
                Err(DbError::execute(
                    SqlState::NumericValueOutOfRange,
                    "double precision value out of range for integer",
                ))
            }
        }
        // NUMERIC. A cast to `NUMERIC(p, s)` applies the type modifier (round to
        // scale, reject precision overflow); a cast to bare `NUMERIC` is identity.
        (Value::Numeric(value), DataType::Numeric { precision, scale }) => {
            common::numeric::apply_typmod(value, *precision, *scale)
                .map(Value::Numeric)
                .ok_or_else(numeric_overflow)
        }
        (Value::Numeric(value), DataType::Text) => {
            Ok(Value::Text(common::numeric::format_numeric(&value)))
        }
        (Value::Text(value), DataType::Numeric { precision, scale }) => {
            let parsed = common::numeric::parse_numeric(&value).ok_or_else(|| {
                DbError::execute(SqlState::DatatypeMismatch, "invalid numeric cast")
            })?;
            common::numeric::apply_typmod(parsed, *precision, *scale)
                .map(Value::Numeric)
                .ok_or_else(numeric_overflow)
        }
        (Value::Integer(value), DataType::Numeric { precision, scale }) => {
            common::numeric::apply_typmod(common::numeric::from_i64(value), *precision, *scale)
                .map(Value::Numeric)
                .ok_or_else(numeric_overflow)
        }
        (Value::Numeric(value), DataType::Integer) => common::numeric::to_i64_rounded(&value)
            .map(Value::Integer)
            .ok_or_else(|| {
                DbError::execute(
                    SqlState::NumericValueOutOfRange,
                    "numeric value out of range for integer",
                )
            }),
        (Value::Numeric(value), DataType::Double) => common::numeric::to_f64(&value)
            .map(|f| Value::Float(f.into()))
            .ok_or_else(|| {
                DbError::execute(
                    SqlState::NumericValueOutOfRange,
                    "numeric value out of range for double precision",
                )
            }),
        (Value::Float(value), DataType::Numeric { precision, scale }) => {
            let parsed = common::numeric::from_f64(value.0).ok_or_else(|| {
                DbError::execute(SqlState::DatatypeMismatch, "invalid numeric cast")
            })?;
            common::numeric::apply_typmod(parsed, *precision, *scale)
                .map(Value::Numeric)
                .ok_or_else(numeric_overflow)
        }
        // REAL casts, mirroring DOUBLE; REAL bridges to NUMERIC via DOUBLE.
        (Value::Real(value), DataType::Real) => Ok(Value::Real(value)),
        (Value::Real(value), DataType::Text) => {
            Ok(Value::Text(common::float::format_real(value.0)))
        }
        (Value::Text(value), DataType::Real) => common::float::parse_real(&value)
            .map(|v| Value::Real(v.into()))
            .ok_or_else(|| DbError::execute(SqlState::DatatypeMismatch, "invalid real cast")),
        (Value::Integer(value), DataType::Real) => Ok(Value::Real((value as f32).into())),
        (Value::Real(value), DataType::Double) => Ok(Value::Float(f64::from(value.0).into())),
        (Value::Float(value), DataType::Real) => Ok(Value::Real((value.0 as f32).into())),
        (Value::Real(value), DataType::Integer) => {
            // Round half-to-even (PostgreSQL's float-to-int cast); reject
            // NaN/infinity/out-of-range.
            let rounded = f64::from(value.0.round_ties_even());
            if rounded.is_finite() && rounded >= -(2f64.powi(63)) && rounded < 2f64.powi(63) {
                Ok(Value::Integer(rounded as i64))
            } else {
                Err(DbError::execute(
                    SqlState::NumericValueOutOfRange,
                    "real value out of range for integer",
                ))
            }
        }
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
        AggregateFunc::StddevSamp => "STDDEV_SAMP",
        AggregateFunc::StddevPop => "STDDEV_POP",
        AggregateFunc::VarSamp => "VAR_SAMP",
        AggregateFunc::VarPop => "VAR_POP",
        AggregateFunc::BoolAnd => "BOOL_AND",
        AggregateFunc::BoolOr => "BOOL_OR",
        AggregateFunc::ArrayAgg => "ARRAY_AGG",
        AggregateFunc::StringAgg => "STRING_AGG",
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

fn numeric_overflow() -> DbError {
    DbError::execute(SqlState::NumericValueOutOfRange, "numeric field overflow")
}
