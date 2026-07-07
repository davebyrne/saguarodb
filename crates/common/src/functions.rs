//! Scalar function-dispatch registry.
//!
//! Each built-in scalar function is one [`ScalarFunction`] entry in the
//! [`SCALAR_FUNCTIONS`] table, pairing its bind-time signature check with its
//! run-time evaluator. The binder (`planner`) consults [`lookup_scalar_function`]
//! to validate a call and assign its result type; the executor calls the same
//! entry's `eval` to compute the value. Adding a function is a single table
//! entry — its signature and evaluation live together here rather than split
//! across the two crates.
//!
//! Sequence functions (`nextval`/`currval`/`setval`), aggregates, and the
//! NULL-folding forms `COALESCE`/`NULLIF` are *not* registered here: they have
//! their own bound representations and binding rules.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::{
    DataType, DbError, POSTGRES_COMPAT_VERSION, Result, SqlState, StatementContext, Value,
};

/// A bound argument as seen by a function's signature checker: its resolved type
/// and, when the argument is a constant, the literal value. `literal` is only
/// consulted by functions that must validate a constant at bind time (currently
/// `EXTRACT`, which checks its field name).
pub struct ArgType<'a> {
    pub data_type: DataType,
    pub literal: Option<&'a Value>,
}

/// How a scalar function treats a NULL argument.
pub enum NullHandling {
    /// A NULL argument makes the call evaluate to NULL without invoking `eval`,
    /// and the result type is nullable when any argument is. This is the rule for
    /// almost every function.
    Propagate,
    /// `eval` is always invoked (it decides how NULL is handled) and the result is
    /// never NULL, so the result type is non-nullable. Used by `CONCAT` (ignores
    /// NULL arguments) and the zero-argument system information functions.
    NeverNull,
}

/// One built-in scalar function: its canonical (lowercase) name, NULL policy,
/// bind-time signature check, and run-time evaluator.
///
/// `signature` validates arity and argument types and returns the result
/// [`DataType`]; result nullability is derived centrally from `null_handling`, so
/// a checker never has to compute it. `eval` receives the already-evaluated
/// argument values (with NULL handling applied per `null_handling`).
pub struct ScalarFunction {
    pub name: &'static str,
    pub null_handling: NullHandling,
    pub signature: fn(name: &str, args: &[ArgType]) -> Result<DataType>,
    pub eval: fn(ctx: &StatementContext, values: &[Value]) -> Result<Value>,
}

impl ScalarFunction {
    /// The result type's nullability for a call, given whether each argument is
    /// nullable. A `Propagate` function's result is nullable when any argument is;
    /// a `NeverNull` function's result is never nullable. The binder uses this so
    /// the NULL rule lives with the function definition rather than being
    /// re-derived at the call site.
    pub fn result_nullable(&self, arg_nullable: impl IntoIterator<Item = bool>) -> bool {
        match self.null_handling {
            NullHandling::Propagate => arg_nullable.into_iter().any(|nullable| nullable),
            NullHandling::NeverNull => false,
        }
    }
}

/// Look up a scalar function by its lowercase name. Returns `None` for names that
/// are not registered built-ins.
pub fn lookup_scalar_function(name: &str) -> Option<&'static ScalarFunction> {
    static INDEX: OnceLock<HashMap<&'static str, &'static ScalarFunction>> = OnceLock::new();
    INDEX
        .get_or_init(|| {
            SCALAR_FUNCTIONS
                .iter()
                .map(|func| (func.name, func))
                .collect()
        })
        .get(name)
        .copied()
}

/// The complete built-in scalar function table. Ordered by category for reading;
/// lookups go through [`lookup_scalar_function`]'s name index.
static SCALAR_FUNCTIONS: &[ScalarFunction] = &[
    // --- Text ---
    ScalarFunction {
        name: "upper",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_upper,
    },
    ScalarFunction {
        name: "lower",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_lower,
    },
    ScalarFunction {
        name: "trim",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_trim,
    },
    ScalarFunction {
        name: "length",
        null_handling: NullHandling::Propagate,
        signature: sig_length,
        eval: eval_length,
    },
    // --- Math ---
    ScalarFunction {
        name: "abs",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_abs,
    },
    ScalarFunction {
        name: "floor",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_floor,
    },
    ScalarFunction {
        name: "ceil",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_ceil,
    },
    ScalarFunction {
        name: "ceiling",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_ceil,
    },
    ScalarFunction {
        name: "round",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_same,
        eval: eval_round,
    },
    ScalarFunction {
        name: "sqrt",
        null_handling: NullHandling::Propagate,
        signature: sig_numeric_to_double,
        eval: eval_sqrt,
    },
    ScalarFunction {
        name: "power",
        null_handling: NullHandling::Propagate,
        signature: sig_power,
        eval: eval_power,
    },
    ScalarFunction {
        name: "pow",
        null_handling: NullHandling::Propagate,
        signature: sig_power,
        eval: eval_power,
    },
    ScalarFunction {
        name: "mod",
        null_handling: NullHandling::Propagate,
        signature: sig_mod,
        eval: eval_mod,
    },
    // --- String ---
    ScalarFunction {
        name: "replace",
        null_handling: NullHandling::Propagate,
        signature: sig_replace,
        eval: eval_replace,
    },
    ScalarFunction {
        name: "position",
        null_handling: NullHandling::Propagate,
        signature: sig_position,
        eval: eval_position,
    },
    ScalarFunction {
        name: "left",
        null_handling: NullHandling::Propagate,
        signature: sig_text_integer_to_text,
        eval: eval_left,
    },
    ScalarFunction {
        name: "right",
        null_handling: NullHandling::Propagate,
        signature: sig_text_integer_to_text,
        eval: eval_right,
    },
    ScalarFunction {
        name: "concat",
        null_handling: NullHandling::NeverNull,
        signature: sig_concat,
        eval: eval_concat,
    },
    ScalarFunction {
        name: "substring",
        null_handling: NullHandling::Propagate,
        signature: sig_substring,
        eval: eval_substring,
    },
    // --- Date/time ---
    ScalarFunction {
        name: "extract",
        null_handling: NullHandling::Propagate,
        signature: sig_extract,
        eval: eval_extract,
    },
    ScalarFunction {
        name: "current_timestamp",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_timestamptz,
        eval: eval_statement_timestamp,
    },
    ScalarFunction {
        name: "now",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_timestamptz,
        eval: eval_statement_timestamp,
    },
    // --- System information ---
    ScalarFunction {
        name: "version",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_version,
    },
    ScalarFunction {
        name: "current_database",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_database,
    },
    ScalarFunction {
        name: "current_catalog",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_database,
    },
    ScalarFunction {
        name: "current_schema",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_schema,
    },
    ScalarFunction {
        name: "current_user",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_user,
    },
    ScalarFunction {
        name: "session_user",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_user,
    },
    ScalarFunction {
        name: "user",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_text,
        eval: eval_current_user,
    },
    ScalarFunction {
        name: "pg_backend_pid",
        null_handling: NullHandling::NeverNull,
        signature: sig_no_args_integer,
        eval: eval_pg_backend_pid,
    },
    ScalarFunction {
        name: "current_setting",
        null_handling: NullHandling::Propagate,
        signature: sig_text_to_text,
        eval: eval_current_setting,
    },
];

// ---------------------------------------------------------------------------
// Signature checkers (bind time). Errors are `ErrorKind::Plan`.
// ---------------------------------------------------------------------------

fn sig_text_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Text)?;
    Ok(DataType::Text)
}

fn sig_length(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    require_arg_type(&args[0], DataType::Text)?;
    Ok(DataType::Integer)
}

/// `ABS`/`FLOOR`/`CEIL`/`ROUND`: accept either numeric type and return that same
/// type.
fn sig_numeric_same(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    numeric_arg_type(name, &args[0])
}

/// `SQRT`: any numeric argument, widened to `DOUBLE`.
fn sig_numeric_to_double(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 1)?;
    numeric_arg_type(name, &args[0])?;
    Ok(DataType::Double)
}

/// `POWER`/`POW`: two numeric arguments, result `DOUBLE`.
fn sig_power(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    numeric_arg_type(name, &args[0])?;
    numeric_arg_type(name, &args[1])?;
    Ok(DataType::Double)
}

/// `MOD`: integer-only (matching the `%` operator, which rejects `DOUBLE`).
fn sig_mod(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Integer)?;
    require_arg_type(&args[1], DataType::Integer)?;
    Ok(DataType::Integer)
}

fn sig_replace(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 3)?;
    for arg in args {
        require_arg_type(arg, DataType::Text)?;
    }
    Ok(DataType::Text)
}

fn sig_position(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Text)?;
    Ok(DataType::Integer)
}

fn sig_text_integer_to_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Integer)?;
    Ok(DataType::Text)
}

/// `CONCAT`: variadic over one or more `TEXT` arguments.
fn sig_concat(_name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.is_empty() {
        return Err(plan_err(
            SqlState::SyntaxError,
            "concat requires at least one argument",
        ));
    }
    for arg in args {
        require_arg_type(arg, DataType::Text)?;
    }
    Ok(DataType::Text)
}

/// `SUBSTRING(text, start[, length])`.
fn sig_substring(_name: &str, args: &[ArgType]) -> Result<DataType> {
    if args.len() != 2 && args.len() != 3 {
        return Err(plan_err(
            SqlState::SyntaxError,
            "substring expects 2 or 3 arguments",
        ));
    }
    require_arg_type(&args[0], DataType::Text)?;
    require_arg_type(&args[1], DataType::Integer)?;
    if let Some(length) = args.get(2) {
        require_arg_type(length, DataType::Integer)?;
    }
    Ok(DataType::Text)
}

/// `EXTRACT(field FROM source)`, bound as `extract('field', source)`. The field
/// literal (when constant) must name a supported component; the source must be a
/// `DATE` or `TIMESTAMP`.
fn sig_extract(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 2)?;
    require_arg_type(&args[0], DataType::Text)?;
    if let Some(Value::Text(field)) = args[0].literal
        && !is_supported_extract_field(field)
    {
        return Err(plan_err(
            SqlState::FeatureNotSupported,
            format!("EXTRACT field {field} is not supported"),
        ));
    }
    if !matches!(args[1].data_type, DataType::Date | DataType::Timestamp) {
        return Err(plan_err(
            SqlState::DatatypeMismatch,
            format!(
                "EXTRACT requires a date or timestamp argument, got {:?}",
                args[1].data_type
            ),
        ));
    }
    Ok(DataType::Double)
}

fn sig_no_args_text(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 0)?;
    Ok(DataType::Text)
}

fn sig_no_args_integer(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 0)?;
    Ok(DataType::Integer)
}

fn sig_no_args_timestamptz(name: &str, args: &[ArgType]) -> Result<DataType> {
    expect_arity(name, args, 0)?;
    Ok(DataType::TimestampTz)
}

// ---------------------------------------------------------------------------
// Evaluators (run time). Errors are `ErrorKind::Execute`. Arity and argument
// types are already validated at bind time, so evaluators index arguments and
// read their expected types directly.
// ---------------------------------------------------------------------------

fn eval_upper(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Text(text_arg(&values[0])?.to_uppercase()))
}

fn eval_lower(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Text(text_arg(&values[0])?.to_lowercase()))
}

fn eval_trim(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    Ok(Value::Text(text_arg(&values[0])?.trim().to_string()))
}

fn eval_length(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let length = text_arg(&values[0])?.chars().count();
    i64::try_from(length)
        .map(Value::Integer)
        .map_err(|_| DbError::internal("string length exceeds i64 range"))
}

/// `ABS`: integer stays integer (with overflow checking); double uses `f64::abs`.
fn eval_abs(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    match &values[0] {
        Value::Integer(value) => value
            .checked_abs()
            .map(Value::Integer)
            .ok_or_else(integer_overflow),
        Value::Float(value) => Ok(Value::Float(value.0.abs().into())),
        _ => type_mismatch("abs requires a numeric argument"),
    }
}

fn eval_floor(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    numeric_round(&values[0], f64::floor)
}

fn eval_ceil(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    numeric_round(&values[0], f64::ceil)
}

fn eval_round(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    numeric_round(&values[0], f64::round_ties_even)
}

/// `SQRT(numeric)` → double. A negative argument is rejected (PostgreSQL raises
/// rather than returning NaN).
fn eval_sqrt(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let value = double_arg(&values[0])?;
    if value < 0.0 {
        return Err(exec_err(
            SqlState::NumericValueOutOfRange,
            "cannot take square root of a negative number",
        ));
    }
    Ok(Value::Float(value.sqrt().into()))
}

/// `POWER(base, exp)` → double. A non-finite result (overflow, or an undefined
/// case such as a negative base to a fractional power) is rejected.
fn eval_power(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let result = double_arg(&values[0])?.powf(double_arg(&values[1])?);
    if !result.is_finite() {
        return Err(exec_err(
            SqlState::NumericValueOutOfRange,
            "power result is out of range or undefined",
        ));
    }
    Ok(Value::Float(result.into()))
}

/// `MOD(a, b)` → integer remainder (`a % b`), with division-by-zero rejected.
fn eval_mod(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let left = integer_arg(&values[0])?;
    let right = integer_arg(&values[1])?;
    if right == 0 {
        return Err(exec_err(SqlState::DivisionByZero, "division by zero"));
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

/// `REPLACE(string, from, to)`: replace every non-overlapping occurrence of
/// `from` with `to`. An empty `from` leaves the string unchanged (matching
/// PostgreSQL, unlike Rust's `str::replace`).
fn eval_replace(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let string = text_arg(&values[0])?;
    let from = text_arg(&values[1])?;
    let to = text_arg(&values[2])?;
    if from.is_empty() {
        Ok(Value::Text(string.to_string()))
    } else {
        Ok(Value::Text(string.replace(from, to)))
    }
}

/// `POSITION(substring, string)`: the 1-based character index of the first
/// occurrence of `substring` in `string`, or 0 if absent. An empty substring is
/// at position 1.
fn eval_position(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let needle: Vec<char> = text_arg(&values[0])?.chars().collect();
    let haystack: Vec<char> = text_arg(&values[1])?.chars().collect();
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

fn eval_left(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    eval_left_right(values, true)
}

fn eval_right(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    eval_left_right(values, false)
}

/// `LEFT(string, n)` / `RIGHT(string, n)`, by character. A negative `n` removes
/// `|n|` characters from the far end (PostgreSQL semantics).
fn eval_left_right(values: &[Value], left: bool) -> Result<Value> {
    let chars: Vec<char> = text_arg(&values[0])?.chars().collect();
    let n = integer_arg(&values[1])?;
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

/// `CONCAT(...)`: ignore NULL arguments and concatenate the rest; the result is
/// the empty string when every argument is NULL (never NULL).
fn eval_concat(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let mut out = String::new();
    for value in values {
        match value {
            Value::Null => {}
            Value::Text(text) => out.push_str(text),
            _ => return type_mismatch("concat requires text arguments"),
        }
    }
    Ok(Value::Text(out))
}

/// `SUBSTRING(text, start[, length])` with 1-based start positions, clamped to the
/// string bounds. A negative length is rejected.
fn eval_substring(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let chars: Vec<char> = text_arg(&values[0])?.chars().collect();
    let length = i64::try_from(chars.len())
        .map_err(|_| DbError::internal("string length exceeds i64 range"))?;
    let start = integer_arg(&values[1])?;

    // The result spans 1-based positions `lower..upper`, intersected with the
    // string's valid range `[1, length]`.
    let lower = start.max(1);
    let upper = match values.get(2) {
        Some(count) => {
            let count = integer_arg(count)?;
            if count < 0 {
                return type_mismatch("substring length must not be negative");
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
fn eval_extract(_ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    const MICROS_PER_SEC: i64 = 1_000_000;
    const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;

    let field = text_arg(&values[0])?;
    let source = &values[1];
    let (year, month, day, hour, minute, second) = match source {
        Value::Date(days) => {
            let (year, month, day) = crate::datetime::civil_from_days(*days);
            (year as f64, month as f64, day as f64, 0.0, 0.0, 0.0)
        }
        Value::Timestamp(micros) => {
            let days = micros.div_euclid(MICROS_PER_DAY);
            let rest = micros.rem_euclid(MICROS_PER_DAY);
            let (year, month, day) = crate::datetime::civil_from_days(days);
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
        _ => return type_mismatch("extract requires a date or timestamp argument"),
    };

    let Some(component) = ExtractField::parse(field) else {
        return Err(exec_err(
            SqlState::FeatureNotSupported,
            format!("EXTRACT field {field} is not supported"),
        ));
    };
    let value = match component {
        ExtractField::Year => year,
        ExtractField::Month => month,
        ExtractField::Day => day,
        ExtractField::Hour => hour,
        ExtractField::Minute => minute,
        ExtractField::Second => second,
    };
    Ok(Value::Float(value.into()))
}

fn eval_statement_timestamp(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::TimestampTz(ctx.statement_timestamp_micros))
}

fn eval_version(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text(format!(
        "PostgreSQL {} (SaguaroDB {})",
        POSTGRES_COMPAT_VERSION,
        env!("CARGO_PKG_VERSION")
    )))
}

fn eval_current_database(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text(ctx.session_info.database.clone()))
}

fn eval_current_schema(_ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text("public".to_string()))
}

fn eval_current_user(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Text(ctx.session_info.user.clone()))
}

fn eval_pg_backend_pid(ctx: &StatementContext, _values: &[Value]) -> Result<Value> {
    Ok(Value::Integer(i64::from(ctx.session_info.backend_pid)))
}

fn eval_current_setting(ctx: &StatementContext, values: &[Value]) -> Result<Value> {
    let name = text_arg(&values[0])?;
    let Some(setting) = ctx.system_state.setting(name) else {
        return Err(exec_err(
            SqlState::UndefinedObject,
            format!("unrecognized configuration parameter \"{name}\""),
        ));
    };
    Ok(Value::Text(setting))
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

/// The calendar/clock components `EXTRACT` supports — the single source of truth
/// for the accepted field names. `sig_extract` validates a literal field with
/// [`is_supported_extract_field`] (which is just `parse(..).is_some()`), and
/// `eval_extract` matches exhaustively on the parsed variant, so the set of
/// accepted names and the set of computable components cannot drift apart.
enum ExtractField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl ExtractField {
    fn parse(field: &str) -> Option<Self> {
        Some(match field {
            "year" => Self::Year,
            "month" => Self::Month,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            _ => return None,
        })
    }
}

fn is_supported_extract_field(field: &str) -> bool {
    ExtractField::parse(field).is_some()
}

fn plan_err(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::plan(code, message)
}

fn exec_err(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::execute(code, message)
}

fn type_mismatch<T>(message: impl Into<String>) -> Result<T> {
    Err(exec_err(SqlState::DatatypeMismatch, message))
}

fn integer_overflow() -> DbError {
    exec_err(
        SqlState::NumericValueOutOfRange,
        "integer value out of range",
    )
}

fn expect_arity(name: &str, args: &[ArgType], arity: usize) -> Result<()> {
    if args.len() != arity {
        return Err(plan_err(
            SqlState::SyntaxError,
            format!("function {name} expects {arity} argument(s)"),
        ));
    }
    Ok(())
}

fn require_arg_type(arg: &ArgType, expected: DataType) -> Result<()> {
    if arg.data_type != expected {
        return Err(plan_err(
            SqlState::DatatypeMismatch,
            format!(
                "expected expression type {:?}, got {:?}",
                expected, arg.data_type
            ),
        ));
    }
    Ok(())
}

/// The numeric type (`Integer` or `Double`) of an argument for functions that
/// accept either. Used both to validate (`SQRT`, `POWER`) and to carry the type
/// through (`ABS`, `FLOOR`, `CEIL`, `ROUND`).
fn numeric_arg_type(name: &str, arg: &ArgType) -> Result<DataType> {
    match arg.data_type {
        DataType::Integer => Ok(DataType::Integer),
        DataType::Double => Ok(DataType::Double),
        ref other => Err(plan_err(
            SqlState::DatatypeMismatch,
            format!("function {name} requires a numeric argument, got {other:?}"),
        )),
    }
}

fn text_arg(value: &Value) -> Result<&str> {
    match value {
        Value::Text(text) => Ok(text),
        _ => type_mismatch("function expected a text argument"),
    }
}

fn integer_arg(value: &Value) -> Result<i64> {
    match value {
        Value::Integer(value) => Ok(*value),
        _ => type_mismatch("function expected an integer argument"),
    }
}

/// Read a numeric (`Integer` or `Double`) argument as `f64`.
fn double_arg(value: &Value) -> Result<f64> {
    match value {
        Value::Integer(value) => Ok(*value as f64),
        Value::Float(value) => Ok(value.0),
        _ => type_mismatch("function expected a numeric argument"),
    }
}

/// `FLOOR`/`CEIL`/`ROUND`: an integer is returned unchanged; a double is rounded
/// by `round` and stays double.
fn numeric_round(value: &Value, round: fn(f64) -> f64) -> Result<Value> {
    match value {
        Value::Integer(value) => Ok(Value::Integer(*value)),
        Value::Float(value) => Ok(Value::Float(round(value.0).into())),
        _ => type_mismatch("function requires a numeric argument"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arg(data_type: DataType) -> ArgType<'static> {
        ArgType {
            data_type,
            literal: None,
        }
    }

    fn result_type(name: &str, args: &[ArgType]) -> Result<DataType> {
        let func = lookup_scalar_function(name).expect("registered function");
        (func.signature)(func.name, args)
    }

    fn call(name: &str, values: &[Value]) -> Result<Value> {
        let ctx = StatementContext::new(0);
        let func = lookup_scalar_function(name).expect("registered function");
        (func.eval)(&ctx, values)
    }

    #[test]
    fn lookup_misses_unregistered_names() {
        assert!(lookup_scalar_function("nextval").is_none());
        assert!(lookup_scalar_function("coalesce").is_none());
        assert!(lookup_scalar_function("count").is_none());
        assert!(lookup_scalar_function("bogus").is_none());
    }

    #[test]
    fn every_entry_has_a_lowercase_unique_name() {
        let mut seen = std::collections::HashSet::new();
        for func in SCALAR_FUNCTIONS {
            assert_eq!(
                func.name,
                func.name.to_ascii_lowercase(),
                "name {} is not lowercase",
                func.name
            );
            assert!(seen.insert(func.name), "duplicate name {}", func.name);
        }
    }

    #[test]
    fn result_nullable_follows_null_handling() {
        let upper = lookup_scalar_function("upper").unwrap();
        assert!(upper.result_nullable([true]));
        assert!(upper.result_nullable([false, true]));
        assert!(!upper.result_nullable([false]));
        assert!(!upper.result_nullable([]));

        // NeverNull functions are non-nullable regardless of their arguments.
        let concat = lookup_scalar_function("concat").unwrap();
        assert!(!concat.result_nullable([true, true]));
        let version = lookup_scalar_function("version").unwrap();
        assert!(!version.result_nullable([]));
    }

    #[test]
    fn text_signatures_check_type_and_arity() {
        assert_eq!(
            result_type("upper", &[arg(DataType::Text)]).unwrap(),
            DataType::Text
        );
        let arity = result_type("upper", &[]).unwrap_err();
        assert_eq!(arity.code, SqlState::SyntaxError);
        let wrong_type = result_type("upper", &[arg(DataType::Integer)]).unwrap_err();
        assert_eq!(wrong_type.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn numeric_same_signature_carries_argument_type() {
        assert_eq!(
            result_type("abs", &[arg(DataType::Integer)]).unwrap(),
            DataType::Integer
        );
        assert_eq!(
            result_type("abs", &[arg(DataType::Double)]).unwrap(),
            DataType::Double
        );
        assert_eq!(
            result_type("abs", &[arg(DataType::Text)]).unwrap_err().code,
            SqlState::DatatypeMismatch
        );
    }

    #[test]
    fn extract_signature_validates_literal_field_and_source() {
        let field = Value::Text("year".to_string());
        let ok = result_type(
            "extract",
            &[
                ArgType {
                    data_type: DataType::Text,
                    literal: Some(&field),
                },
                arg(DataType::Timestamp),
            ],
        )
        .unwrap();
        assert_eq!(ok, DataType::Double);

        let bad_field = Value::Text("century".to_string());
        let err = result_type(
            "extract",
            &[
                ArgType {
                    data_type: DataType::Text,
                    literal: Some(&bad_field),
                },
                arg(DataType::Timestamp),
            ],
        )
        .unwrap_err();
        assert_eq!(err.code, SqlState::FeatureNotSupported);

        let bad_source =
            result_type("extract", &[arg(DataType::Text), arg(DataType::Integer)]).unwrap_err();
        assert_eq!(bad_source.code, SqlState::DatatypeMismatch);
    }

    #[test]
    fn concat_is_variadic_and_requires_an_argument() {
        assert_eq!(
            result_type("concat", &[arg(DataType::Text), arg(DataType::Text)]).unwrap(),
            DataType::Text
        );
        assert_eq!(
            result_type("concat", &[]).unwrap_err().code,
            SqlState::SyntaxError
        );
    }

    #[test]
    fn substring_accepts_two_or_three_arguments() {
        assert_eq!(
            result_type("substring", &[arg(DataType::Text), arg(DataType::Integer)]).unwrap(),
            DataType::Text
        );
        assert!(
            result_type(
                "substring",
                &[
                    arg(DataType::Text),
                    arg(DataType::Integer),
                    arg(DataType::Integer)
                ]
            )
            .is_ok()
        );
        assert_eq!(
            result_type("substring", &[arg(DataType::Text)])
                .unwrap_err()
                .code,
            SqlState::SyntaxError
        );
    }

    #[test]
    fn system_functions_take_no_arguments() {
        assert_eq!(result_type("version", &[]).unwrap(), DataType::Text);
        assert_eq!(
            result_type("pg_backend_pid", &[]).unwrap(),
            DataType::Integer
        );
        assert_eq!(
            result_type("version", &[arg(DataType::Text)])
                .unwrap_err()
                .code,
            SqlState::SyntaxError
        );
    }

    #[test]
    fn statement_timestamp_functions_use_context_clock() {
        let ctx = StatementContext::new(0).with_statement_timestamp_micros(1_234_567);
        for name in ["current_timestamp", "now"] {
            assert_eq!(result_type(name, &[]).unwrap(), DataType::TimestampTz);
            assert_eq!(
                result_type(name, &[arg(DataType::Text)]).unwrap_err().code,
                SqlState::SyntaxError
            );

            let func = lookup_scalar_function(name).expect("registered function");
            assert!(!func.result_nullable([]));
            assert_eq!(
                (func.eval)(&ctx, &[]).unwrap(),
                Value::TimestampTz(1_234_567)
            );
        }
    }

    #[test]
    fn evaluators_compute_expected_values() {
        assert_eq!(
            call("upper", &[Value::Text("abc".to_string())]).unwrap(),
            Value::Text("ABC".to_string())
        );
        assert_eq!(
            call("length", &[Value::Text("héllo".to_string())]).unwrap(),
            Value::Integer(5)
        );
        assert_eq!(
            call("abs", &[Value::Integer(-7)]).unwrap(),
            Value::Integer(7)
        );
        assert_eq!(
            call(
                "concat",
                &[
                    Value::Text("a".to_string()),
                    Value::Null,
                    Value::Text("b".to_string())
                ]
            )
            .unwrap(),
            Value::Text("ab".to_string())
        );
    }

    #[test]
    fn evaluators_surface_domain_errors() {
        assert_eq!(
            call("abs", &[Value::Integer(i64::MIN)]).unwrap_err().code,
            SqlState::NumericValueOutOfRange
        );
        assert_eq!(
            call("mod", &[Value::Integer(1), Value::Integer(0)])
                .unwrap_err()
                .code,
            SqlState::DivisionByZero
        );
        assert_eq!(
            call("sqrt", &[Value::Integer(-1)]).unwrap_err().code,
            SqlState::NumericValueOutOfRange
        );
    }
}
