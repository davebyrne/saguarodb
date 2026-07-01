use common::{DataType, DbError, ExecRow, Result, SqlState, StatementContext, Value};
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
            expr, data_type, ..
        } => cast_value(eval_expr_inner(ctx, expr, row)?, data_type),
        // Subqueries are resolved to literals (or an `IN` list) by the executor's
        // pre-pass before any row is evaluated; reaching here is a routing bug.
        BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. }
        | BoundExpr::InSubquery { .. } => Err(DbError::internal(
            "subquery expression reached scalar evaluation without being resolved",
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
            _ => unreachable!(),
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
                _ => unreachable!(),
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
    // CONCAT ignores NULL arguments rather than propagating them, so it is handled
    // before the blanket NULL short-circuit below.
    if name == "concat" {
        return eval_concat(&values);
    }
    // Every other scalar function is NULL-propagating.
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }

    match name {
        "upper" => Ok(Value::Text(function_text(&values[0])?.to_uppercase())),
        "lower" => Ok(Value::Text(function_text(&values[0])?.to_lowercase())),
        "trim" => Ok(Value::Text(function_text(&values[0])?.trim().to_string())),
        "length" => {
            let length = function_text(&values[0])?.chars().count();
            i64::try_from(length)
                .map(Value::Integer)
                .map_err(|_| DbError::internal("string length exceeds i64 range"))
        }
        "abs" => eval_abs(&values[0]),
        // FLOOR/CEIL/ROUND keep an integer unchanged; for a double they round and
        // stay double. ROUND uses round-half-to-even, matching PostgreSQL's
        // `round(double precision)` and the double→integer cast.
        "floor" => numeric_round(&values[0], f64::floor),
        "ceil" | "ceiling" => numeric_round(&values[0], f64::ceil),
        "round" => numeric_round(&values[0], f64::round_ties_even),
        "sqrt" => eval_sqrt(&values[0]),
        "power" | "pow" => eval_power(&values[0], &values[1]),
        "mod" => eval_mod(&values[0], &values[1]),
        "replace" => eval_replace(&values),
        "position" => eval_position(&values),
        "left" => eval_left_right(&values, true),
        "right" => eval_left_right(&values, false),
        "extract" => eval_extract(&values[0], &values[1]),
        "substring" => eval_substring(&values),
        _ => Err(DbError::internal(format!(
            "unknown scalar function {name} reached the executor"
        ))),
    }
}

/// Evaluates `SUBSTRING(text, start[, length])` with 1-based start positions,
/// clamped to the string bounds. A negative length is rejected.
fn eval_substring(values: &[Value]) -> Result<Value> {
    let chars: Vec<char> = function_text(&values[0])?.chars().collect();
    let length = i64::try_from(chars.len())
        .map_err(|_| DbError::internal("string length exceeds i64 range"))?;
    let start = function_integer(&values[1])?;

    // The result spans 1-based positions `lower..upper`, intersected with the
    // string's valid range `[1, length]`.
    let lower = start.max(1);
    let upper = match values.get(2) {
        Some(count) => {
            let count = function_integer(count)?;
            if count < 0 {
                return datatype_mismatch("substring length must not be negative");
            }
            start.saturating_add(count).min(length + 1)
        }
        None => length + 1,
    };
    if upper <= lower {
        return Ok(Value::Text(String::new()));
    }

    // `lower >= 1` and `upper <= length + 1`, so both indices are in range.
    let begin = usize::try_from(lower - 1).map_err(|_| DbError::internal("substring index"))?;
    let end = usize::try_from(upper - 1).map_err(|_| DbError::internal("substring index"))?;
    Ok(Value::Text(chars[begin..end].iter().collect()))
}

/// `EXTRACT(field FROM source)`: the requested calendar/clock component of a DATE
/// or TIMESTAMP, returned as `DOUBLE PRECISION` (seconds include the fractional
/// part). DATE sources have zero-valued time components.
fn eval_extract(field: &Value, source: &Value) -> Result<Value> {
    const MICROS_PER_SEC: i64 = 1_000_000;
    const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;

    let field = function_text(field)?;
    let (year, month, day, hour, minute, second) = match source {
        Value::Date(days) => {
            let (year, month, day) = common::datetime::civil_from_days(*days);
            (year as f64, month as f64, day as f64, 0.0, 0.0, 0.0)
        }
        Value::Timestamp(micros) => {
            let days = micros.div_euclid(MICROS_PER_DAY);
            let rest = micros.rem_euclid(MICROS_PER_DAY);
            let (year, month, day) = common::datetime::civil_from_days(days);
            let total_secs = rest / MICROS_PER_SEC;
            let fraction = (rest % MICROS_PER_SEC) as f64 / MICROS_PER_SEC as f64;
            (
                year as f64,
                month as f64,
                day as f64,
                (total_secs / 3_600) as f64,
                ((total_secs % 3_600) / 60) as f64,
                (total_secs % 60) as f64 + fraction,
            )
        }
        _ => return datatype_mismatch("extract requires a date or timestamp argument"),
    };

    let value = match field {
        "year" => year,
        "month" => month,
        "day" => day,
        "hour" => hour,
        "minute" => minute,
        "second" => second,
        other => {
            return Err(DbError::execute(
                SqlState::FeatureNotSupported,
                format!("EXTRACT field {other} is not supported"),
            ));
        }
    };
    Ok(Value::Float(value.into()))
}

fn function_text(value: &Value) -> Result<&str> {
    match value {
        Value::Text(text) => Ok(text),
        _ => datatype_mismatch("function expected a text argument"),
    }
}

fn function_integer(value: &Value) -> Result<i64> {
    match value {
        Value::Integer(value) => Ok(*value),
        _ => datatype_mismatch("function expected an integer argument"),
    }
}

/// Read a numeric (`Integer` or `Double`) argument as `f64`.
fn function_double(value: &Value) -> Result<f64> {
    match value {
        Value::Integer(value) => Ok(*value as f64),
        Value::Float(value) => Ok(value.0),
        _ => datatype_mismatch("function expected a numeric argument"),
    }
}

/// `ABS`: integer stays integer (with overflow checking); double uses `f64::abs`.
fn eval_abs(value: &Value) -> Result<Value> {
    match value {
        Value::Integer(value) => value
            .checked_abs()
            .map(Value::Integer)
            .ok_or_else(integer_overflow),
        Value::Float(value) => Ok(Value::Float(value.0.abs().into())),
        _ => datatype_mismatch("abs requires a numeric argument"),
    }
}

/// `FLOOR`/`CEIL`/`ROUND`: an integer is returned unchanged; a double is rounded
/// by `round` and stays double.
fn numeric_round(value: &Value, round: fn(f64) -> f64) -> Result<Value> {
    match value {
        Value::Integer(value) => Ok(Value::Integer(*value)),
        Value::Float(value) => Ok(Value::Float(round(value.0).into())),
        _ => datatype_mismatch("function requires a numeric argument"),
    }
}

/// `SQRT(numeric)` → double. A negative argument is rejected (PostgreSQL raises
/// rather than returning NaN).
fn eval_sqrt(value: &Value) -> Result<Value> {
    let value = function_double(value)?;
    if value < 0.0 {
        return Err(DbError::execute(
            SqlState::NumericValueOutOfRange,
            "cannot take square root of a negative number",
        ));
    }
    Ok(Value::Float(value.sqrt().into()))
}

/// `POWER(base, exp)` → double. A non-finite result (overflow, or an undefined
/// case such as a negative base to a fractional power) is rejected.
fn eval_power(base: &Value, exp: &Value) -> Result<Value> {
    let result = function_double(base)?.powf(function_double(exp)?);
    if !result.is_finite() {
        return Err(DbError::execute(
            SqlState::NumericValueOutOfRange,
            "power result is out of range or undefined",
        ));
    }
    Ok(Value::Float(result.into()))
}

/// `MOD(a, b)` → integer remainder (`a % b`), with division-by-zero rejected.
fn eval_mod(left: &Value, right: &Value) -> Result<Value> {
    let left = function_integer(left)?;
    let right = function_integer(right)?;
    if right == 0 {
        return Err(DbError::execute(
            SqlState::DivisionByZero,
            "division by zero",
        ));
    }
    // `i64::MIN % -1` overflows `checked_rem`, but the remainder is mathematically
    // 0; PostgreSQL returns 0 here rather than erroring.
    if right == -1 {
        return Ok(Value::Integer(0));
    }
    left.checked_rem(right)
        .map(Value::Integer)
        .ok_or_else(integer_overflow)
}

/// `CONCAT(...)`: ignore NULL arguments and concatenate the rest; the result is
/// the empty string when every argument is NULL (never NULL).
fn eval_concat(values: &[Value]) -> Result<Value> {
    let mut out = String::new();
    for value in values {
        match value {
            Value::Null => {}
            Value::Text(text) => out.push_str(text),
            _ => return datatype_mismatch("concat requires text arguments"),
        }
    }
    Ok(Value::Text(out))
}

/// `REPLACE(string, from, to)`: replace every non-overlapping occurrence of
/// `from` with `to`. An empty `from` leaves the string unchanged (matching
/// PostgreSQL, unlike Rust's `str::replace`).
fn eval_replace(values: &[Value]) -> Result<Value> {
    let string = function_text(&values[0])?;
    let from = function_text(&values[1])?;
    let to = function_text(&values[2])?;
    if from.is_empty() {
        Ok(Value::Text(string.to_string()))
    } else {
        Ok(Value::Text(string.replace(from, to)))
    }
}

/// `POSITION(substring, string)`: the 1-based character index of the first
/// occurrence of `substring` in `string`, or 0 if absent. An empty substring is
/// at position 1.
fn eval_position(values: &[Value]) -> Result<Value> {
    let needle: Vec<char> = function_text(&values[0])?.chars().collect();
    let haystack: Vec<char> = function_text(&values[1])?.chars().collect();
    let position = if needle.is_empty() {
        1
    } else if needle.len() > haystack.len() {
        0
    } else {
        (0..=haystack.len() - needle.len())
            .find(|&start| haystack[start..start + needle.len()] == needle[..])
            .map_or(0, |start| (start + 1) as i64)
    };
    Ok(Value::Integer(position))
}

/// `LEFT(string, n)` / `RIGHT(string, n)`, by character. A negative `n` removes
/// `|n|` characters from the far end (PostgreSQL semantics).
fn eval_left_right(values: &[Value], left: bool) -> Result<Value> {
    let chars: Vec<char> = function_text(&values[0])?.chars().collect();
    let n = function_integer(&values[1])?;
    let len = chars.len() as i64;
    let result: String = if left {
        // First `take` characters: `min(n, len)` for n >= 0, else all but the
        // last `|n|` (`len + n`), clamped to `[0, len]`.
        let take = if n >= 0 {
            n.min(len)
        } else {
            len.saturating_add(n).max(0)
        } as usize;
        chars[..take].iter().collect()
    } else {
        // Characters from `start` to the end: skip the first `len - n` for n >= 0
        // (keeping the last n), or skip the first `|n|` for n < 0.
        let start = if n >= 0 {
            len.saturating_sub(n).max(0)
        } else {
            n.saturating_neg().min(len)
        } as usize;
        chars[start..].iter().collect()
    };
    Ok(Value::Text(result))
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
