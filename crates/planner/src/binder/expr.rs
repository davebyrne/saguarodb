use common::{DataType, Result, SqlState, Value};
use parser::{Expr, FunctionArg, Query};

use crate::{AggregateFunc, BinOp, BoundExpr, BoundQuery, UnaryOp};

use super::query::bind_query;
use super::{BindContext, input_ref, plan_error, reject_aggregate, require_type};

pub(super) fn bind_boolean_expr(ctx: &mut BindContext, expr: &Expr) -> Result<BoundExpr> {
    let bound = bind_expr(ctx, expr, Some(DataType::Boolean))?;
    require_type(&bound, DataType::Boolean)?;
    Ok(bound)
}

pub(super) fn bind_expr(
    ctx: &mut BindContext,
    expr: &Expr,
    expected: Option<DataType>,
) -> Result<BoundExpr> {
    match expr {
        Expr::Literal(value) => bind_literal(value, expected),
        Expr::Placeholder(index) => bind_placeholder(ctx, *index, expected),
        Expr::ColumnRef { table, column } => resolve_column(ctx, table.as_deref(), column),
        Expr::Subquery(select) => bind_scalar_subquery(ctx, select),
        Expr::BinaryOp { left, op, right } => bind_binary_op(ctx, left, op.clone(), right),
        Expr::UnaryOp { op, expr } => bind_unary_op(ctx, op.clone(), expr),
        Expr::Function {
            name,
            args,
            distinct,
        } => bind_function(ctx, name, args, *distinct),
        Expr::IsNull(expr) => {
            let expr = Box::new(bind_expr(ctx, expr, None)?);
            Ok(BoundExpr::IsNull {
                expr,
                data_type: DataType::Boolean,
                nullable: false,
            })
        }
        Expr::IsNotNull(expr) => {
            let expr = Box::new(bind_expr(ctx, expr, None)?);
            Ok(BoundExpr::IsNotNull {
                expr,
                data_type: DataType::Boolean,
                nullable: false,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => bind_in_list(ctx, expr, list, *negated),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => bind_in_subquery(ctx, expr, subquery, *negated),
        Expr::Exists { subquery, negated } => bind_exists(ctx, subquery, *negated),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => bind_between(ctx, expr, low, high, *negated),
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape,
        } => bind_like(ctx, expr, pattern, *negated, *case_insensitive, *escape),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => bind_case(
            ctx,
            operand.as_deref(),
            when_clauses,
            else_clause.as_deref(),
        ),
        Expr::Cast {
            expr,
            data_type,
            pg_type,
        } => {
            let expr = Box::new(bind_expr(ctx, expr, Some(data_type.clone()))?);
            Ok(BoundExpr::Cast {
                nullable: expr.nullable(),
                expr,
                data_type: data_type.clone(),
                pg_type: pg_type.clone(),
            })
        }
    }
}

/// Bind a subquery's inner query in its own fresh binding scope (uncorrelated
/// semantics: it does not see the outer query's columns), reusing the outer
/// parameter declarations so `$n` placeholders inside the subquery resolve the
/// same way.
fn bind_subquery(ctx: &BindContext, subquery: &Query) -> Result<BoundQuery> {
    // A subquery is bound in its own binding scope but still sees the enclosing
    // query's CTEs. A subquery result has no external type context, so no
    // `expected` types are supplied.
    bind_query(
        ctx.catalog,
        subquery,
        &ctx.declared_params,
        &ctx.cte_scope,
        None,
    )
}

/// Require that a subquery used where a single value is expected (a scalar
/// subquery, or the right side of `IN`) produces exactly one output column.
fn single_subquery_column(query: &BoundQuery) -> Result<&common::ColumnInfo> {
    match query.output_schema() {
        [column] => Ok(column),
        _ => Err(plan_error(
            SqlState::SyntaxError,
            "subquery must return only one column",
        )),
    }
}

/// `(SELECT ...)` as a scalar value: a single-column subquery whose row count is
/// checked at run time. The result is always nullable (an empty result is NULL).
fn bind_scalar_subquery(ctx: &mut BindContext, subquery: &Query) -> Result<BoundExpr> {
    let query = bind_subquery(ctx, subquery)?;
    let column = single_subquery_column(&query)?;
    let data_type = column.data_type.clone();
    Ok(BoundExpr::ScalarSubquery {
        query: Box::new(query),
        data_type,
        nullable: true,
    })
}

/// `[NOT] EXISTS (SELECT ...)`. Any number of output columns is allowed (they are
/// ignored); the result is a non-null boolean.
fn bind_exists(ctx: &mut BindContext, subquery: &Query, negated: bool) -> Result<BoundExpr> {
    let query = bind_subquery(ctx, subquery)?;
    Ok(BoundExpr::Exists {
        query: Box::new(query),
        negated,
        data_type: DataType::Boolean,
        nullable: false,
    })
}

/// `expr [NOT] IN (SELECT ...)` over a single-column subquery. The left operand
/// is type-checked against the subquery's column type (no implicit casts); a bare
/// `NULL` left operand is typed from the subquery column. The result is boolean
/// and nullable (SQL `IN` three-valued logic can yield `NULL`).
fn bind_in_subquery(
    ctx: &mut BindContext,
    expr: &Expr,
    subquery: &Query,
    negated: bool,
) -> Result<BoundExpr> {
    let query = bind_subquery(ctx, subquery)?;
    let column_type = single_subquery_column(&query)?.data_type.clone();
    let left = if matches!(expr, Expr::Literal(Value::Null)) {
        bind_expr(ctx, expr, Some(column_type))?
    } else {
        let left = bind_expr(ctx, expr, None)?;
        require_type(&left, column_type)?;
        left
    };
    Ok(BoundExpr::InSubquery {
        expr: Box::new(left),
        query: Box::new(query),
        negated,
        data_type: DataType::Boolean,
        nullable: true,
    })
}

/// PostgreSQL caps the number of bind parameters at 65535 (the wire protocol
/// carries the parameter count in a 16-bit field). We enforce the same ceiling so
/// a large `$n` can never drive an unbounded allocation in `collect_param_types`
/// (`record_param` resizes a `Vec` to `n`, so an unbounded `$4294967295` would
/// attempt a multi-GB allocation and abort the process).
const MAX_PARAM_NUMBER: u32 = u16::MAX as u32;

fn bind_placeholder(
    ctx: &BindContext,
    index: u32,
    expected: Option<DataType>,
) -> Result<BoundExpr> {
    if index == 0 || index > MAX_PARAM_NUMBER {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("parameter number ${index} is out of range (must be 1..={MAX_PARAM_NUMBER})"),
        ));
    }
    let slot = (index - 1) as usize;
    let declared = ctx.declared_param(slot);
    let data_type = match (declared, expected) {
        (Some(declared), Some(expected)) if declared != expected => {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("parameter ${index} type does not match its use"),
            ));
        }
        (Some(declared), _) => declared,
        (None, Some(expected)) => expected,
        (None, None) => {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("could not determine data type of parameter ${index}"),
            ));
        }
    };
    // A bound parameter value may be NULL; runtime NOT NULL checks (validate_not_null)
    // enforce column constraints, so the slot is treated as non-null for binding.
    Ok(BoundExpr::Parameter {
        index: slot,
        data_type,
        nullable: false,
    })
}

fn bind_literal(value: &Value, expected: Option<DataType>) -> Result<BoundExpr> {
    let (data_type, nullable) = match value {
        Value::Null => (
            expected.ok_or_else(|| {
                plan_error(
                    SqlState::DatatypeMismatch,
                    "NULL literal requires a type context",
                )
            })?,
            true,
        ),
        Value::Boolean(_) => (DataType::Boolean, false),
        Value::Integer(_) => (DataType::Integer, false),
        Value::Float(_) => (DataType::Double, false),
        Value::Real(_) => (DataType::Real, false),
        Value::Numeric(_) => (
            DataType::Numeric {
                precision: None,
                scale: 0,
            },
            false,
        ),
        Value::Text(_) => (DataType::Text, false),
        Value::Date(_) => (DataType::Date, false),
        Value::Timestamp(_) => (DataType::Timestamp, false),
        Value::Time(_) => (DataType::Time, false),
        Value::TimestampTz(_) => (DataType::TimestampTz, false),
        Value::Interval(_) => (DataType::Interval, false),
        Value::Bytes(_) => (DataType::Bytea, false),
        Value::Uuid(_) => (DataType::Uuid, false),
    };
    Ok(BoundExpr::Literal {
        value: value.clone(),
        data_type,
        nullable,
    })
}

/// The numeric "family" of a type for arithmetic compatibility: `INTEGER` (0),
/// `DOUBLE PRECISION` (1), `NUMERIC` (2, regardless of `(precision, scale)`), or
/// `REAL` (3); `None` for non-numeric types. Operands must share a family — there
/// is no implicit coercion between them.
fn numeric_family(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Integer => Some(0),
        DataType::Double => Some(1),
        DataType::Numeric { .. } => Some(2),
        DataType::Real => Some(3),
        _ => None,
    }
}

/// Result type for arithmetic involving `INTERVAL` and/or temporal types (which
/// sit outside the numeric families). Returns `None` for unsupported
/// combinations, which then fall through to numeric-family resolution. `DATE +/-
/// INTERVAL` yields a `TIMESTAMP` (PostgreSQL semantics); `INTERVAL + <temporal>`
/// is the commutative `Add` only. `TIME +/- INTERVAL` uses only the interval's
/// time component (wrapping mod 24h).
fn interval_arith_result(left: &DataType, op: BinOp, right: &DataType) -> Option<DataType> {
    use DataType::{Date, Integer, Interval, Time, Timestamp, TimestampTz};
    match (left, op, right) {
        (Interval, BinOp::Add | BinOp::Sub, Interval) => Some(Interval),
        (Interval, BinOp::Mul, Integer) | (Integer, BinOp::Mul, Interval) => Some(Interval),
        (Date, BinOp::Add | BinOp::Sub, Interval) | (Interval, BinOp::Add, Date) => Some(Timestamp),
        (Timestamp, BinOp::Add | BinOp::Sub, Interval) | (Interval, BinOp::Add, Timestamp) => {
            Some(Timestamp)
        }
        (TimestampTz, BinOp::Add | BinOp::Sub, Interval) | (Interval, BinOp::Add, TimestampTz) => {
            Some(TimestampTz)
        }
        (Time, BinOp::Add | BinOp::Sub, Interval) | (Interval, BinOp::Add, Time) => Some(Time),
        _ => None,
    }
}

fn bind_binary_op(
    ctx: &mut BindContext,
    left: &Expr,
    op: parser::BinOp,
    right: &Expr,
) -> Result<BoundExpr> {
    let op = convert_bin_op(op);
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            // Both operands must share one numeric "family" — INTEGER, DOUBLE
            // PRECISION, REAL, or NUMERIC (any `(p, s)` collapse to one family) —
            // with no implicit coercion between families. Bind each side once with
            // no hint;
            // only a bare untyped NULL or undeclared parameter fails. Take the
            // family from whichever side is numeric, re-binding a failed (untyped)
            // leaf with the resolved type; default to INTEGER when neither has one.
            let result_type = |family: u8| match family {
                1 => DataType::Double,
                2 => DataType::Numeric {
                    precision: None,
                    scale: 0,
                },
                3 => DataType::Real,
                _ => DataType::Integer,
            };
            let left_res = bind_expr(ctx, left, None);
            let right_res = bind_expr(ctx, right, None);

            // INTERVAL / temporal arithmetic takes precedence over numeric-family
            // resolution. Both sides must bind to a concrete type to qualify.
            let interval_result = match (&left_res, &right_res) {
                (Ok(l), Ok(r)) => interval_arith_result(&l.data_type(), op, &r.data_type()),
                _ => None,
            };
            if let Some(data_type) = interval_result {
                let left = left_res.expect("left bound above");
                let right = right_res.expect("right bound above");
                let nullable = left.nullable() || right.nullable();
                return Ok(BoundExpr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                    data_type,
                    nullable,
                });
            }

            let family = left_res
                .as_ref()
                .ok()
                .and_then(|e| numeric_family(&e.data_type()))
                .or_else(|| {
                    right_res
                        .as_ref()
                        .ok()
                        .and_then(|e| numeric_family(&e.data_type()))
                })
                .unwrap_or(0);
            let operand_type = result_type(family);
            let left = match left_res {
                Ok(expr) => expr,
                Err(_) => bind_expr(ctx, left, Some(operand_type.clone()))?,
            };
            let right = match right_res {
                Ok(expr) => expr,
                Err(_) => bind_expr(ctx, right, Some(operand_type.clone()))?,
            };
            if numeric_family(&left.data_type()) != Some(family)
                || numeric_family(&right.data_type()) != Some(family)
            {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!(
                        "arithmetic operands must be the same numeric type, got {:?} and {:?}",
                        left.data_type(),
                        right.data_type()
                    ),
                ));
            }
            if matches!(family, 1 | 3) && matches!(op, BinOp::Mod) {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    "modulo is not defined for floating-point types",
                ));
            }
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: operand_type,
                nullable,
            })
        }
        BinOp::And | BinOp::Or => {
            let left = bind_expr(ctx, left, Some(DataType::Boolean))?;
            let right = bind_expr(ctx, right, Some(DataType::Boolean))?;
            require_type(&left, DataType::Boolean)?;
            require_type(&right, DataType::Boolean)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Boolean,
                nullable,
            })
        }
        BinOp::Concat => {
            let left = bind_expr(ctx, left, Some(DataType::Text))?;
            let right = bind_expr(ctx, right, Some(DataType::Text))?;
            require_type(&left, DataType::Text)?;
            require_type(&right, DataType::Text)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Text,
                nullable,
            })
        }
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
            let (left, right) = bind_comparison_operands(ctx, left, right)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Boolean,
                nullable,
            })
        }
        BinOp::IsDistinctFrom | BinOp::IsNotDistinctFrom => {
            // Same-type operands like an ordinary comparison, but the result is a
            // NULL-safe boolean that is never NULL itself.
            let (left, right) = bind_comparison_operands(ctx, left, right)?;
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Boolean,
                nullable: false,
            })
        }
    }
}

fn bind_comparison_operands(
    ctx: &mut BindContext,
    left: &Expr,
    right: &Expr,
) -> Result<(BoundExpr, BoundExpr)> {
    match (is_null_literal(left), is_null_literal(right)) {
        (true, true) => Err(plan_error(
            SqlState::DatatypeMismatch,
            "NULL comparison requires a non-NULL type context",
        )),
        (true, false) => {
            let right = bind_expr(ctx, right, None)?;
            let left = bind_expr(ctx, left, Some(right.data_type()))?;
            Ok((left, right))
        }
        (false, true) => {
            let left = bind_expr(ctx, left, None)?;
            let right = bind_expr(ctx, right, Some(left.data_type()))?;
            Ok((left, right))
        }
        (false, false) => {
            let left = bind_expr(ctx, left, None)?;
            let right = bind_expr(ctx, right, Some(left.data_type()))?;
            require_type(&right, left.data_type())?;
            Ok((left, right))
        }
    }
}

fn bind_unary_op(ctx: &mut BindContext, op: parser::UnaryOp, expr: &Expr) -> Result<BoundExpr> {
    let op = convert_unary_op(op);
    match op {
        UnaryOp::Neg => {
            // Negation applies to any numeric type or INTERVAL; an untyped NULL
            // defaults to INTEGER (matching the arithmetic operators).
            let bound = bind_expr(ctx, expr, None);
            let operand_type = bound
                .as_ref()
                .ok()
                .map(|e| e.data_type())
                .filter(|t| numeric_family(t).is_some() || matches!(t, DataType::Interval))
                .unwrap_or(DataType::Integer);
            let expr = match bound {
                Ok(expr) => expr,
                Err(_) => bind_expr(ctx, expr, Some(operand_type.clone()))?,
            };
            require_type(&expr, operand_type.clone())?;
            Ok(BoundExpr::UnaryOp {
                nullable: expr.nullable(),
                op,
                expr: Box::new(expr),
                data_type: operand_type,
            })
        }
        UnaryOp::Not => {
            let expr = bind_expr(ctx, expr, Some(DataType::Boolean))?;
            require_type(&expr, DataType::Boolean)?;
            Ok(BoundExpr::UnaryOp {
                nullable: expr.nullable(),
                op,
                expr: Box::new(expr),
                data_type: DataType::Boolean,
            })
        }
    }
}

fn bind_function(
    ctx: &mut BindContext,
    name: &str,
    args: &[FunctionArg],
    distinct: bool,
) -> Result<BoundExpr> {
    let name = name.to_ascii_lowercase();
    if let Some(func) = aggregate_func(&name) {
        return bind_aggregate(ctx, func, args, distinct);
    }
    if distinct {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("function {name} does not support DISTINCT"),
        ));
    }
    match name.as_str() {
        "nextval" => return bind_nextval(ctx, args),
        "currval" => return bind_currval(ctx, args),
        "setval" => return bind_setval(ctx, args),
        "coalesce" => return bind_coalesce(ctx, args),
        "nullif" => return bind_nullif(ctx, args),
        _ => {}
    }
    bind_scalar_function(ctx, &name, args)
}

fn bind_nextval(ctx: &mut BindContext, args: &[FunctionArg]) -> Result<BoundExpr> {
    let sequence = resolve_sequence_arg(ctx, "nextval", args)?;
    Ok(BoundExpr::Nextval {
        sequence,
        data_type: DataType::Integer,
        nullable: false,
    })
}

fn bind_currval(ctx: &mut BindContext, args: &[FunctionArg]) -> Result<BoundExpr> {
    let sequence = resolve_sequence_arg(ctx, "currval", args)?;
    Ok(BoundExpr::Currval {
        sequence,
        data_type: DataType::Integer,
        nullable: false,
    })
}

fn bind_setval(ctx: &mut BindContext, args: &[FunctionArg]) -> Result<BoundExpr> {
    let exprs = expr_args("setval", args)?;
    if exprs.len() != 2 && exprs.len() != 3 {
        return Err(plan_error(
            SqlState::SyntaxError,
            "setval expects two or three arguments",
        ));
    }
    let sequence = resolve_sequence_name(ctx, "setval", exprs[0])?;
    let value = bind_expr(ctx, exprs[1], Some(DataType::Integer))?;
    require_type(&value, DataType::Integer)?;
    let is_called = exprs
        .get(2)
        .map(|expr| {
            let arg = bind_expr(ctx, expr, Some(DataType::Boolean))?;
            require_type(&arg, DataType::Boolean)?;
            Ok(Box::new(arg))
        })
        .transpose()?;
    let nullable = value.nullable() || is_called.as_deref().is_some_and(BoundExpr::nullable);
    Ok(BoundExpr::Setval {
        sequence,
        value: Box::new(value),
        is_called,
        data_type: DataType::Integer,
        nullable,
    })
}

fn resolve_sequence_arg(
    ctx: &BindContext,
    function: &str,
    args: &[FunctionArg],
) -> Result<common::SequenceId> {
    let exprs = expr_args(function, args)?;
    let [name] = exprs.as_slice() else {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("{function} expects exactly one argument"),
        ));
    };
    resolve_sequence_name(ctx, function, name)
}

fn resolve_sequence_name(
    ctx: &BindContext,
    function: &str,
    expr: &Expr,
) -> Result<common::SequenceId> {
    let Expr::Literal(Value::Text(name)) = expr else {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            format!("{function} requires a string literal sequence name"),
        ));
    };
    let sequence = ctx.catalog.get_sequence_by_name(name)?.ok_or_else(|| {
        plan_error(
            SqlState::UndefinedTable,
            format!("sequence {name} does not exist"),
        )
    })?;
    Ok(sequence.id)
}

/// Extract the plain expression arguments of a call, rejecting `*` wildcards.
fn expr_args<'a>(name: &str, args: &'a [FunctionArg]) -> Result<Vec<&'a Expr>> {
    args.iter()
        .map(|arg| match arg {
            FunctionArg::Expr(expr) => Ok(expr),
            FunctionArg::Wildcard => Err(plan_error(
                SqlState::SyntaxError,
                format!("function {name} does not accept a wildcard argument"),
            )),
        })
        .collect()
}

/// `COALESCE(v1, ..., vn)` returns the first non-NULL argument. It is desugared
/// to a searched `CASE`: `CASE WHEN v1 IS NOT NULL THEN v1 ... ELSE vn END`. All
/// arguments must share one type (no implicit cast); a bare untyped NULL takes
/// its type from a sibling. The result is non-nullable when any argument is.
fn bind_coalesce(ctx: &mut BindContext, args: &[FunctionArg]) -> Result<BoundExpr> {
    let exprs = expr_args("coalesce", args)?;
    if exprs.is_empty() {
        return Err(plan_error(
            SqlState::SyntaxError,
            "coalesce requires at least one argument",
        ));
    }

    // First pass: infer the common type from the non-NULL arguments.
    let mut inferred: Option<DataType> = None;
    for expr in &exprs {
        if is_null_literal(expr) {
            continue;
        }
        let bound = bind_expr(ctx, expr, inferred.clone())?;
        match &inferred {
            Some(data_type) => require_type(&bound, data_type.clone())?,
            None => inferred = Some(bound.data_type()),
        }
    }
    let data_type = inferred.ok_or_else(|| {
        plan_error(
            SqlState::DatatypeMismatch,
            "coalesce requires at least one argument with a known type",
        )
    })?;

    // Second pass: bind every argument with the resolved type.
    let mut bound = Vec::with_capacity(exprs.len());
    for expr in &exprs {
        let arg = bind_expr(ctx, expr, Some(data_type.clone()))?;
        require_type(&arg, data_type.clone())?;
        bound.push(arg);
    }

    // COALESCE is non-null exactly when some argument can never be NULL.
    let nullable = bound.iter().all(BoundExpr::nullable);
    let last = bound.pop().expect("coalesce has at least one argument");
    let when_clauses = bound
        .into_iter()
        .map(|arg| {
            let guard = BoundExpr::IsNotNull {
                expr: Box::new(arg.clone()),
                data_type: DataType::Boolean,
                nullable: false,
            };
            (guard, arg)
        })
        .collect();
    Ok(BoundExpr::Case {
        operand: None,
        when_clauses,
        else_clause: Some(Box::new(last)),
        data_type,
        nullable,
    })
}

/// `NULLIF(a, b)` returns NULL when `a = b`, otherwise `a`. It is desugared to
/// `CASE WHEN a = b THEN NULL ELSE a END`. The operands must be comparable (same
/// type); the result is always nullable.
fn bind_nullif(ctx: &mut BindContext, args: &[FunctionArg]) -> Result<BoundExpr> {
    let exprs = expr_args("nullif", args)?;
    let [a, b] = exprs.as_slice() else {
        return Err(plan_error(
            SqlState::SyntaxError,
            "nullif requires exactly two arguments",
        ));
    };
    let (a, b) = bind_comparison_operands(ctx, a, b)?;
    let data_type = a.data_type();
    let equals = BoundExpr::BinaryOp {
        left: Box::new(a.clone()),
        op: BinOp::Eq,
        right: Box::new(b.clone()),
        data_type: DataType::Boolean,
        nullable: a.nullable() || b.nullable(),
    };
    let then_null = BoundExpr::Literal {
        value: Value::Null,
        data_type: data_type.clone(),
        nullable: true,
    };
    Ok(BoundExpr::Case {
        operand: None,
        when_clauses: vec![(equals, then_null)],
        else_clause: Some(Box::new(a)),
        data_type,
        nullable: true,
    })
}

fn bind_aggregate(
    ctx: &mut BindContext,
    func: AggregateFunc,
    args: &[FunctionArg],
    distinct: bool,
) -> Result<BoundExpr> {
    let arg = match args {
        [FunctionArg::Wildcard] if distinct => {
            return Err(plan_error(
                SqlState::SyntaxError,
                "DISTINCT is not supported with a wildcard aggregate argument",
            ));
        }
        [FunctionArg::Wildcard] if func == AggregateFunc::Count => None,
        [FunctionArg::Wildcard] => {
            return Err(plan_error(
                SqlState::SyntaxError,
                "only COUNT supports wildcard aggregate argument",
            ));
        }
        [FunctionArg::Expr(expr)] => {
            let arg = bind_expr(ctx, expr, None)?;
            reject_aggregate(&arg)?;
            Some(Box::new(arg))
        }
        _ => {
            return Err(plan_error(
                SqlState::SyntaxError,
                "aggregate functions require exactly one argument",
            ));
        }
    };

    let (data_type, nullable) = match func {
        AggregateFunc::Count => (DataType::Integer, false),
        AggregateFunc::Sum | AggregateFunc::Avg => {
            let Some(arg) = &arg else {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "SUM and AVG require an expression argument",
                ));
            };
            // SUM/AVG accept any numeric type. INTEGER stays INTEGER (integer
            // division for AVG); DOUBLE PRECISION stays DOUBLE; NUMERIC returns an
            // unconstrained NUMERIC (the accumulated value carries its own scale).
            let arg_type = arg.data_type();
            let result_type = match arg_type {
                DataType::Integer => DataType::Integer,
                DataType::Double => DataType::Double,
                DataType::Real => DataType::Real,
                DataType::Numeric { .. } => DataType::Numeric {
                    precision: None,
                    scale: 0,
                },
                other => {
                    return Err(plan_error(
                        SqlState::DatatypeMismatch,
                        format!("SUM and AVG require a numeric argument, got {other:?}"),
                    ));
                }
            };
            (result_type, true)
        }
        AggregateFunc::Min | AggregateFunc::Max => {
            let Some(arg) = &arg else {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "MIN and MAX require an expression argument",
                ));
            };
            (arg.data_type(), true)
        }
        // STDDEV/VARIANCE accept either numeric type and always return DOUBLE.
        AggregateFunc::StddevSamp
        | AggregateFunc::StddevPop
        | AggregateFunc::VarSamp
        | AggregateFunc::VarPop => {
            let Some(arg) = &arg else {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "STDDEV and VARIANCE require an expression argument",
                ));
            };
            let arg_type = arg.data_type();
            if !matches!(arg_type, DataType::Integer | DataType::Double) {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!("STDDEV and VARIANCE require a numeric argument, got {arg_type:?}"),
                ));
            }
            (DataType::Double, true)
        }
        // BOOL_AND/BOOL_OR require a boolean argument and return BOOLEAN.
        AggregateFunc::BoolAnd | AggregateFunc::BoolOr => {
            let Some(arg) = &arg else {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "BOOL_AND and BOOL_OR require an expression argument",
                ));
            };
            let arg_type = arg.data_type();
            if arg_type != DataType::Boolean {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!("BOOL_AND and BOOL_OR require a boolean argument, got {arg_type:?}"),
                ));
            }
            (DataType::Boolean, true)
        }
    };

    Ok(BoundExpr::AggregateCall {
        func,
        arg,
        distinct,
        data_type,
        nullable,
    })
}

fn bind_scalar_function(
    ctx: &mut BindContext,
    name: &str,
    args: &[FunctionArg],
) -> Result<BoundExpr> {
    let mut bound_args = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            FunctionArg::Expr(expr) => bound_args.push(bind_expr(ctx, expr, None)?),
            FunctionArg::Wildcard => {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    format!("function {name} does not accept a wildcard argument"),
                ));
            }
        }
    }

    let (data_type, nullable) = scalar_signature(name, &bound_args)?;
    Ok(BoundExpr::Function {
        name: name.to_string(),
        args: bound_args,
        data_type,
        nullable,
    })
}

/// Validates a scalar function's arity and argument types, returning its result
/// type and nullability. All v1 scalar functions are NULL-propagating, so the
/// result is nullable when any argument is.
fn scalar_signature(name: &str, args: &[BoundExpr]) -> Result<(DataType, bool)> {
    let nullable = args.iter().any(BoundExpr::nullable);
    match name {
        "upper" | "lower" | "trim" => {
            expect_arity(name, args, 1)?;
            require_type(&args[0], DataType::Text)?;
            Ok((DataType::Text, nullable))
        }
        "length" => {
            expect_arity(name, args, 1)?;
            require_type(&args[0], DataType::Text)?;
            Ok((DataType::Integer, nullable))
        }
        // ABS, FLOOR, CEIL/CEILING, and ROUND accept either numeric type and
        // return that same type (FLOOR/CEIL/ROUND of an INTEGER is the integer
        // itself; of a DOUBLE they round and stay DOUBLE).
        "abs" | "floor" | "ceil" | "ceiling" | "round" => {
            expect_arity(name, args, 1)?;
            let data_type = numeric_arg_type(name, &args[0])?;
            Ok((data_type, nullable))
        }
        // SQRT always returns DOUBLE; an INTEGER argument is widened (PostgreSQL's
        // sqrt(int) → double precision).
        "sqrt" => {
            expect_arity(name, args, 1)?;
            require_numeric(name, &args[0])?;
            Ok((DataType::Double, nullable))
        }
        // POWER/POW take two numeric arguments and return DOUBLE.
        "power" | "pow" => {
            expect_arity(name, args, 2)?;
            require_numeric(name, &args[0])?;
            require_numeric(name, &args[1])?;
            Ok((DataType::Double, nullable))
        }
        // MOD is integer-only (matching the `%` operator, which rejects DOUBLE).
        "mod" => {
            expect_arity(name, args, 2)?;
            require_type(&args[0], DataType::Integer)?;
            require_type(&args[1], DataType::Integer)?;
            Ok((DataType::Integer, nullable))
        }
        "replace" => {
            expect_arity(name, args, 3)?;
            for arg in args {
                require_type(arg, DataType::Text)?;
            }
            Ok((DataType::Text, nullable))
        }
        // POSITION(substring, string) -> 1-based index, 0 if not found.
        "position" => {
            expect_arity(name, args, 2)?;
            require_type(&args[0], DataType::Text)?;
            require_type(&args[1], DataType::Text)?;
            Ok((DataType::Integer, nullable))
        }
        "left" | "right" => {
            expect_arity(name, args, 2)?;
            require_type(&args[0], DataType::Text)?;
            require_type(&args[1], DataType::Integer)?;
            Ok((DataType::Text, nullable))
        }
        // CONCAT is variadic over TEXT, ignores NULL arguments, and never returns
        // NULL (empty string when every argument is NULL). Non-text arguments must
        // be cast explicitly (no implicit cast).
        "concat" => {
            if args.is_empty() {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "concat requires at least one argument",
                ));
            }
            for arg in args {
                require_type(arg, DataType::Text)?;
            }
            Ok((DataType::Text, false))
        }
        // EXTRACT(field FROM source) -> extract('field', source). The field is a
        // text literal (validated here when literal); the source is a DATE or
        // TIMESTAMP. The result is DOUBLE (PostgreSQL returns numeric).
        "extract" => {
            expect_arity(name, args, 2)?;
            require_type(&args[0], DataType::Text)?;
            if let BoundExpr::Literal {
                value: Value::Text(field),
                ..
            } = &args[0]
                && !is_supported_extract_field(field)
            {
                return Err(plan_error(
                    SqlState::FeatureNotSupported,
                    format!("EXTRACT field {field} is not supported"),
                ));
            }
            let source_type = args[1].data_type();
            if !matches!(source_type, DataType::Date | DataType::Timestamp) {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!("EXTRACT requires a date or timestamp argument, got {source_type:?}"),
                ));
            }
            Ok((DataType::Double, nullable))
        }
        "substring" => {
            if args.len() != 2 && args.len() != 3 {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "substring expects 2 or 3 arguments",
                ));
            }
            require_type(&args[0], DataType::Text)?;
            require_type(&args[1], DataType::Integer)?;
            if let Some(length) = args.get(2) {
                require_type(length, DataType::Integer)?;
            }
            Ok((DataType::Text, nullable))
        }
        _ => Err(plan_error(
            SqlState::SyntaxError,
            format!("function {name} is not supported in v1"),
        )),
    }
}

/// The numeric type (`Integer` or `Double`) of an argument for functions that
/// return their argument's type (`ABS`, `FLOOR`, `CEIL`, `ROUND`).
fn numeric_arg_type(name: &str, arg: &BoundExpr) -> Result<DataType> {
    match arg.data_type() {
        data_type @ (DataType::Integer | DataType::Double) => Ok(data_type),
        other => Err(plan_error(
            SqlState::DatatypeMismatch,
            format!("function {name} requires a numeric argument, got {other:?}"),
        )),
    }
}

/// Validate that an argument is numeric (`Integer` or `Double`) without fixing
/// the result type (`SQRT`, `POWER`).
fn require_numeric(name: &str, arg: &BoundExpr) -> Result<()> {
    numeric_arg_type(name, arg).map(|_| ())
}

/// The `EXTRACT` fields SaguaroDB supports.
fn is_supported_extract_field(field: &str) -> bool {
    matches!(
        field,
        "year" | "month" | "day" | "hour" | "minute" | "second"
    )
}

fn expect_arity(name: &str, args: &[BoundExpr], arity: usize) -> Result<()> {
    if args.len() != arity {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("function {name} expects {arity} argument(s)"),
        ));
    }
    Ok(())
}

fn bind_in_list(
    ctx: &mut BindContext,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<BoundExpr> {
    if matches!(expr, Expr::Literal(Value::Null)) {
        return bind_null_in_list(ctx, expr, list, negated);
    }
    let expr = bind_expr(ctx, expr, None)?;
    let mut nullable = expr.nullable();
    let mut bound_list = Vec::with_capacity(list.len());
    for item in list {
        let item = bind_expr(ctx, item, Some(expr.data_type()))?;
        require_type(&item, expr.data_type())?;
        nullable |= item.nullable();
        bound_list.push(item);
    }
    Ok(BoundExpr::InList {
        expr: Box::new(expr),
        list: bound_list,
        negated,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_null_in_list(
    ctx: &mut BindContext,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<BoundExpr> {
    let mut inferred_type = None;
    let mut nullable = true;
    let mut bound_list = vec![None; list.len()];

    for (index, item) in list.iter().enumerate() {
        if matches!(item, Expr::Literal(Value::Null)) && inferred_type.is_none() {
            continue;
        }
        let item = bind_expr(ctx, item, inferred_type.clone())?;
        if let Some(data_type) = &inferred_type {
            require_type(&item, data_type.clone())?;
        } else {
            inferred_type = Some(item.data_type());
        }
        nullable |= item.nullable();
        bound_list[index] = Some(item);
    }

    let data_type = inferred_type.ok_or_else(|| {
        plan_error(
            SqlState::DatatypeMismatch,
            "NULL literal requires a type context",
        )
    })?;
    let expr = bind_expr(ctx, expr, Some(data_type.clone()))?;
    let bound_list = bound_list
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            item.map(Ok)
                .unwrap_or_else(|| bind_expr(ctx, &list[index], Some(data_type.clone())))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(BoundExpr::InList {
        expr: Box::new(expr),
        list: bound_list,
        negated,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_between(
    ctx: &mut BindContext,
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Result<BoundExpr> {
    let expr = bind_expr(ctx, expr, None)?;
    let low = bind_expr(ctx, low, Some(expr.data_type()))?;
    let high = bind_expr(ctx, high, Some(expr.data_type()))?;
    require_type(&low, expr.data_type())?;
    require_type(&high, expr.data_type())?;
    let nullable = expr.nullable() || low.nullable() || high.nullable();
    Ok(BoundExpr::Between {
        expr: Box::new(expr),
        low: Box::new(low),
        high: Box::new(high),
        negated,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_like(
    ctx: &mut BindContext,
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    case_insensitive: bool,
    escape: Option<char>,
) -> Result<BoundExpr> {
    let expr = bind_expr(ctx, expr, Some(DataType::Text))?;
    let pattern = bind_expr(ctx, pattern, Some(DataType::Text))?;
    require_type(&expr, DataType::Text)?;
    require_type(&pattern, DataType::Text)?;
    let nullable = expr.nullable() || pattern.nullable();
    Ok(BoundExpr::Like {
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        negated,
        case_insensitive,
        escape,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_case(
    ctx: &mut BindContext,
    operand: Option<&Expr>,
    when_clauses: &[(Expr, Expr)],
    else_clause: Option<&Expr>,
) -> Result<BoundExpr> {
    let operand = operand
        .map(|expr| bind_expr(ctx, expr, None).map(Box::new))
        .transpose()?;

    let inferred_result_type = infer_case_result_type(ctx, when_clauses, else_clause)?;
    let mut result_type = Some(inferred_result_type);
    let mut nullable = else_clause.is_none();
    let mut bound_when = Vec::with_capacity(when_clauses.len());

    for (when, then) in when_clauses {
        let when = if let Some(operand) = &operand {
            let when = bind_expr(ctx, when, Some(operand.data_type()))?;
            require_type(&when, operand.data_type())?;
            when
        } else {
            bind_boolean_expr(ctx, when)?
        };
        let then = bind_expr(ctx, then, result_type.clone())?;
        update_case_type(&then, &mut result_type, &mut nullable)?;
        bound_when.push((when, then));
    }

    let else_clause = else_clause
        .map(|expr| {
            let bound = bind_expr(ctx, expr, result_type.clone())?;
            update_case_type(&bound, &mut result_type, &mut nullable)?;
            Ok(Box::new(bound))
        })
        .transpose()?;

    let data_type = result_type.ok_or_else(|| {
        plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions cannot all be NULL",
        )
    })?;

    Ok(BoundExpr::Case {
        operand,
        when_clauses: bound_when,
        else_clause,
        data_type,
        nullable,
    })
}

fn infer_case_result_type(
    ctx: &mut BindContext,
    when_clauses: &[(Expr, Expr)],
    else_clause: Option<&Expr>,
) -> Result<DataType> {
    let mut result_type = None;

    for (_, then) in when_clauses {
        update_inferred_case_type(ctx, then, &mut result_type)?;
    }
    if let Some(else_clause) = else_clause {
        update_inferred_case_type(ctx, else_clause, &mut result_type)?;
    }

    result_type.ok_or_else(|| {
        plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions cannot all be NULL",
        )
    })
}

fn update_inferred_case_type(
    ctx: &mut BindContext,
    expr: &Expr,
    result_type: &mut Option<DataType>,
) -> Result<()> {
    if is_null_literal(expr) {
        return Ok(());
    }
    let bound = bind_expr(ctx, expr, result_type.clone())?;
    match result_type {
        Some(data_type) if *data_type != bound.data_type() => Err(plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions must have the same type",
        )),
        Some(_) => Ok(()),
        None => {
            *result_type = Some(bound.data_type());
            Ok(())
        }
    }
}

fn update_case_type(
    expr: &BoundExpr,
    result_type: &mut Option<DataType>,
    nullable: &mut bool,
) -> Result<()> {
    if expr.is_null_literal() {
        *nullable = true;
        return Ok(());
    }
    if expr.nullable() {
        *nullable = true;
    }
    match result_type {
        Some(data_type) if *data_type != expr.data_type() => Err(plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions must have the same type",
        )),
        Some(_) => Ok(()),
        None => {
            *result_type = Some(expr.data_type());
            Ok(())
        }
    }
}

fn resolve_column(ctx: &BindContext, table: Option<&str>, column: &str) -> Result<BoundExpr> {
    let mut matches = Vec::new();
    for binding in &ctx.bindings {
        // A `qualified_only` binding (the `excluded` pseudo-table) participates
        // only when the reference is explicitly qualified with its name.
        if table.is_none() && binding.qualified_only {
            continue;
        }
        if let Some(table) = table
            && binding.visible_name != table
            && (binding.visible_name != binding.table_name || binding.table_name != table)
        {
            continue;
        }
        for column_def in &binding.columns {
            if column_def.name == column {
                matches.push((binding, column_def));
            }
        }
    }

    match matches.as_slice() {
        [(binding, column)] => Ok(input_ref(binding, column)),
        [] => Err(plan_error(
            SqlState::UndefinedColumn,
            format!("column {column} does not exist"),
        )),
        _ => Err(plan_error(
            SqlState::UndefinedColumn,
            format!("column {column} is ambiguous"),
        )),
    }
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Value::Null))
}

fn aggregate_func(name: &str) -> Option<AggregateFunc> {
    match name {
        "count" => Some(AggregateFunc::Count),
        "sum" => Some(AggregateFunc::Sum),
        "avg" => Some(AggregateFunc::Avg),
        "min" => Some(AggregateFunc::Min),
        "max" => Some(AggregateFunc::Max),
        "stddev" | "stddev_samp" => Some(AggregateFunc::StddevSamp),
        "stddev_pop" => Some(AggregateFunc::StddevPop),
        "variance" | "var_samp" => Some(AggregateFunc::VarSamp),
        "var_pop" => Some(AggregateFunc::VarPop),
        "bool_and" => Some(AggregateFunc::BoolAnd),
        "bool_or" => Some(AggregateFunc::BoolOr),
        _ => None,
    }
}

fn convert_bin_op(op: parser::BinOp) -> BinOp {
    match op {
        parser::BinOp::Add => BinOp::Add,
        parser::BinOp::Sub => BinOp::Sub,
        parser::BinOp::Mul => BinOp::Mul,
        parser::BinOp::Div => BinOp::Div,
        parser::BinOp::Mod => BinOp::Mod,
        parser::BinOp::Eq => BinOp::Eq,
        parser::BinOp::Neq => BinOp::Neq,
        parser::BinOp::Lt => BinOp::Lt,
        parser::BinOp::LtEq => BinOp::LtEq,
        parser::BinOp::Gt => BinOp::Gt,
        parser::BinOp::GtEq => BinOp::GtEq,
        parser::BinOp::And => BinOp::And,
        parser::BinOp::Or => BinOp::Or,
        parser::BinOp::Concat => BinOp::Concat,
        parser::BinOp::IsDistinctFrom => BinOp::IsDistinctFrom,
        parser::BinOp::IsNotDistinctFrom => BinOp::IsNotDistinctFrom,
    }
}

fn convert_unary_op(op: parser::UnaryOp) -> UnaryOp {
    match op {
        parser::UnaryOp::Neg => UnaryOp::Neg,
        parser::UnaryOp::Not => UnaryOp::Not,
    }
}
