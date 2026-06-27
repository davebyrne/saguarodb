use common::{Result, Value};
use sqlparser::ast as sql;

use crate::{BinOp, Expr, FunctionArg, UnaryOp};

use super::{convert_data_type, ident_name, object_name, parse_error, unsupported};

pub(super) fn convert_expr(expr: &sql::Expr) -> Result<Expr> {
    match expr {
        sql::Expr::Identifier(ident) => Ok(Expr::ColumnRef {
            table: None,
            column: ident_name(ident)?,
        }),
        sql::Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [table, column] => Ok(Expr::ColumnRef {
                table: Some(ident_name(table)?),
                column: ident_name(column)?,
            }),
            _ => unsupported("unsupported qualified identifier"),
        },
        sql::Expr::Value(value) => convert_value(&value.value),
        sql::Expr::Nested(expr) => convert_expr(expr),
        sql::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(left)?),
            op: convert_bin_op(op)?,
            right: Box::new(convert_expr(right)?),
        }),
        sql::Expr::UnaryOp { op, expr } => match op {
            sql::UnaryOperator::Minus => Ok(Expr::UnaryOp {
                op: UnaryOp::Neg,
                expr: Box::new(convert_expr(expr)?),
            }),
            sql::UnaryOperator::Not => Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(convert_expr(expr)?),
            }),
            sql::UnaryOperator::Plus => convert_expr(expr),
            _ => unsupported("unsupported unary operator"),
        },
        sql::Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(convert_expr(expr)?))),
        sql::Expr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(convert_expr(expr)?))),
        sql::Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(convert_expr(expr)?),
            list: list.iter().map(convert_expr).collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        sql::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(convert_expr(expr)?),
            low: Box::new(convert_expr(low)?),
            high: Box::new(convert_expr(high)?),
            negated: *negated,
        }),
        sql::Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any || escape_char.is_some() {
                return unsupported("unsupported LIKE form");
            }
            Ok(Expr::Like {
                expr: Box::new(convert_expr(expr)?),
                pattern: Box::new(convert_expr(pattern)?),
                negated: *negated,
            })
        }
        sql::Expr::Case {
            operand,
            conditions,
            else_result,
        } => Ok(Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| convert_expr(expr).map(Box::new))
                .transpose()?,
            when_clauses: conditions
                .iter()
                .map(|when| Ok((convert_expr(&when.condition)?, convert_expr(&when.result)?)))
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_result
                .as_ref()
                .map(|expr| convert_expr(expr).map(Box::new))
                .transpose()?,
        }),
        sql::Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if *kind != sql::CastKind::Cast || format.is_some() {
                return unsupported("unsupported CAST form");
            }
            Ok(Expr::Cast {
                expr: Box::new(convert_expr(expr)?),
                data_type: convert_data_type(data_type)?,
            })
        }
        sql::Expr::Function(function) => convert_function(function),
        sql::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => convert_substring(expr, substring_from.as_deref(), substring_for.as_deref()),
        sql::Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_where.is_some() || trim_what.is_some() || trim_characters.is_some() {
                return unsupported("only TRIM(expr) is supported in v1");
            }
            Ok(Expr::Function {
                name: "trim".to_string(),
                args: vec![FunctionArg::Expr(convert_expr(expr)?)],
                distinct: false,
            })
        }
        _ => unsupported("unsupported expression"),
    }
}

/// Normalizes `SUBSTRING(expr [FROM start] [FOR len])` and the comma form
/// `SUBSTRING(expr, start[, len])` into a `substring` function call. A start
/// position is required in v1.
fn convert_substring(
    expr: &sql::Expr,
    substring_from: Option<&sql::Expr>,
    substring_for: Option<&sql::Expr>,
) -> Result<Expr> {
    let Some(from) = substring_from else {
        return unsupported("SUBSTRING requires a start position in v1");
    };
    let mut args = vec![
        FunctionArg::Expr(convert_expr(expr)?),
        FunctionArg::Expr(convert_expr(from)?),
    ];
    if let Some(for_expr) = substring_for {
        args.push(FunctionArg::Expr(convert_expr(for_expr)?));
    }
    Ok(Expr::Function {
        name: "substring".to_string(),
        args,
        distinct: false,
    })
}

fn convert_value(value: &sql::Value) -> Result<Expr> {
    match value {
        sql::Value::Null => Ok(Expr::Literal(Value::Null)),
        sql::Value::Boolean(value) => Ok(Expr::Literal(Value::Boolean(*value))),
        sql::Value::Number(value, _) => {
            let value = value
                .parse::<i64>()
                .map_err(|_| parse_error("invalid integer literal"))?;
            Ok(Expr::Literal(Value::Integer(value)))
        }
        sql::Value::SingleQuotedString(value) => Ok(Expr::Literal(Value::Text(value.clone()))),
        sql::Value::Placeholder(name) => convert_placeholder(name),
        _ => unsupported("unsupported literal"),
    }
}

fn convert_placeholder(name: &str) -> Result<Expr> {
    let digits = name
        .strip_prefix('$')
        .ok_or_else(|| parse_error(format!("unsupported placeholder {name}")))?;
    let index = digits
        .parse::<u32>()
        .map_err(|_| parse_error(format!("invalid placeholder {name}")))?;
    if index == 0 {
        return Err(parse_error("placeholder index must be >= 1"));
    }
    Ok(Expr::Placeholder(index))
}

fn convert_function(function: &sql::Function) -> Result<Expr> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, sql::FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("unsupported function call");
    }

    let (args, distinct) = match &function.args {
        sql::FunctionArguments::List(args) => {
            if !args.clauses.is_empty() {
                return unsupported("unsupported function argument clause");
            }
            let distinct = matches!(
                args.duplicate_treatment,
                Some(sql::DuplicateTreatment::Distinct)
            );
            let converted = args
                .args
                .iter()
                .map(convert_function_arg)
                .collect::<Result<Vec<_>>>()?;
            (converted, distinct)
        }
        sql::FunctionArguments::None => (Vec::new(), false),
        sql::FunctionArguments::Subquery(_) => return unsupported("unsupported function argument"),
    };

    Ok(Expr::Function {
        name: object_name(&function.name)?,
        args,
        distinct,
    })
}

fn convert_function_arg(arg: &sql::FunctionArg) -> Result<FunctionArg> {
    let sql::FunctionArg::Unnamed(arg) = arg else {
        return unsupported("named function arguments are not supported");
    };
    match arg {
        sql::FunctionArgExpr::Expr(expr) => Ok(FunctionArg::Expr(convert_expr(expr)?)),
        sql::FunctionArgExpr::Wildcard => Ok(FunctionArg::Wildcard),
        sql::FunctionArgExpr::QualifiedWildcard(_) => {
            unsupported("qualified function wildcards are not supported")
        }
    }
}

fn convert_bin_op(op: &sql::BinaryOperator) -> Result<BinOp> {
    match op {
        sql::BinaryOperator::Plus => Ok(BinOp::Add),
        sql::BinaryOperator::Minus => Ok(BinOp::Sub),
        sql::BinaryOperator::Multiply => Ok(BinOp::Mul),
        sql::BinaryOperator::Divide => Ok(BinOp::Div),
        sql::BinaryOperator::Modulo => Ok(BinOp::Mod),
        sql::BinaryOperator::Eq => Ok(BinOp::Eq),
        sql::BinaryOperator::NotEq => Ok(BinOp::Neq),
        sql::BinaryOperator::Lt => Ok(BinOp::Lt),
        sql::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        sql::BinaryOperator::Gt => Ok(BinOp::Gt),
        sql::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        sql::BinaryOperator::And => Ok(BinOp::And),
        sql::BinaryOperator::Or => Ok(BinOp::Or),
        sql::BinaryOperator::StringConcat => Ok(BinOp::Concat),
        _ => unsupported("unsupported binary operator"),
    }
}
