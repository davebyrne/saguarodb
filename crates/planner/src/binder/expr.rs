use common::{DataType, Result, SqlState, Value};
use parser::{Expr, FunctionArg};

use crate::{AggregateFunc, BinOp, BoundExpr, UnaryOp};

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
        } => bind_like(ctx, expr, pattern, *negated),
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
        Expr::Cast { expr, data_type } => {
            let expr = Box::new(bind_expr(ctx, expr, Some(data_type.clone()))?);
            Ok(BoundExpr::Cast {
                nullable: expr.nullable(),
                expr,
                data_type: data_type.clone(),
            })
        }
    }
}

fn bind_placeholder(
    ctx: &BindContext,
    index: u32,
    expected: Option<DataType>,
) -> Result<BoundExpr> {
    let slot = usize::try_from(index - 1)
        .map_err(|_| plan_error(SqlState::SyntaxError, "invalid parameter index"))?;
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
        Value::Text(_) => (DataType::Text, false),
        Value::Date(_) => (DataType::Date, false),
    };
    Ok(BoundExpr::Literal {
        value: value.clone(),
        data_type,
        nullable,
    })
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
            let left = bind_expr(ctx, left, Some(DataType::Integer))?;
            let right = bind_expr(ctx, right, Some(DataType::Integer))?;
            require_type(&left, DataType::Integer)?;
            require_type(&right, DataType::Integer)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Integer,
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
            let expr = bind_expr(ctx, expr, Some(DataType::Integer))?;
            require_type(&expr, DataType::Integer)?;
            Ok(BoundExpr::UnaryOp {
                nullable: expr.nullable(),
                op,
                expr: Box::new(expr),
                data_type: DataType::Integer,
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
    bind_scalar_function(ctx, &name, args)
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
            require_type(arg, DataType::Integer)?;
            (DataType::Integer, true)
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
        "abs" => {
            expect_arity(name, args, 1)?;
            require_type(&args[0], DataType::Integer)?;
            Ok((DataType::Integer, nullable))
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
    }
}

fn convert_unary_op(op: parser::UnaryOp) -> UnaryOp {
    match op {
        parser::UnaryOp::Neg => UnaryOp::Neg,
        parser::UnaryOp::Not => UnaryOp::Not,
    }
}
