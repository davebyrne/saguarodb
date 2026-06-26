use std::cmp::Ordering;

use common::{DataType, Value};

use crate::{AggregateExpr, BinOp, BoundExpr, BoundOrderByItem, LogicalPlan, UnaryOp};

/// Result-preserving logical-plan rewrite: fold constant sub-expressions and
/// simplify boolean operators in every embedded expression, then drop any
/// scan/filter predicate that folds to constant `true`.
pub(crate) fn simplify_logical(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { table, filter } => LogicalPlan::Scan {
            table,
            filter: filter.map(fold_expr).filter(|expr| !is_true(expr)),
        },
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => LogicalPlan::Join {
            left: Box::new(simplify_logical(*left)),
            right: Box::new(simplify_logical(*right)),
            condition: condition.map(fold_expr),
            join_type,
        },
        LogicalPlan::Filter { source, predicate } => {
            let source = simplify_logical(*source);
            let predicate = fold_expr(predicate);
            if is_true(&predicate) {
                source
            } else {
                LogicalPlan::Filter {
                    source: Box::new(source),
                    predicate,
                }
            }
        }
        LogicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => LogicalPlan::Projection {
            source: Box::new(simplify_logical(*source)),
            expressions: expressions.into_iter().map(fold_expr).collect(),
            output_schema,
        },
        LogicalPlan::Sort { source, order_by } => LogicalPlan::Sort {
            source: Box::new(simplify_logical(*source)),
            order_by: order_by
                .into_iter()
                .map(|item| BoundOrderByItem {
                    expr: fold_expr(item.expr),
                    ascending: item.ascending,
                    nulls_first: item.nulls_first,
                })
                .collect(),
        },
        LogicalPlan::Distinct { source, on_keys } => LogicalPlan::Distinct {
            source: Box::new(simplify_logical(*source)),
            on_keys: on_keys.into_iter().map(fold_expr).collect(),
        },
        LogicalPlan::Limit {
            source,
            count,
            offset,
        } => LogicalPlan::Limit {
            source: Box::new(simplify_logical(*source)),
            count,
            offset,
        },
        LogicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => LogicalPlan::Aggregate {
            source: Box::new(simplify_logical(*source)),
            group_by: group_by.into_iter().map(fold_expr).collect(),
            aggregates: aggregates
                .into_iter()
                .map(|aggregate| AggregateExpr {
                    func: aggregate.func,
                    arg: aggregate.arg.map(fold_expr),
                    distinct: aggregate.distinct,
                    data_type: aggregate.data_type,
                    nullable: aggregate.nullable,
                })
                .collect(),
            output_schema,
        },
        LogicalPlan::Values {
            rows,
            output_schema,
        } => LogicalPlan::Values {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(fold_expr).collect())
                .collect(),
            output_schema,
        },
        LogicalPlan::Insert {
            table,
            columns,
            source,
        } => LogicalPlan::Insert {
            table,
            columns,
            source: Box::new(simplify_logical(*source)),
        },
        LogicalPlan::Update {
            table,
            assignments,
            source,
        } => LogicalPlan::Update {
            table,
            assignments: assignments
                .into_iter()
                .map(|(column, expr)| (column, fold_expr(expr)))
                .collect(),
            source: Box::new(simplify_logical(*source)),
        },
        LogicalPlan::Delete { table, source } => LogicalPlan::Delete {
            table,
            source: Box::new(simplify_logical(*source)),
        },
        ddl @ (LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }) => ddl,
    }
}

/// Bottom-up constant folding for a single expression. Folds children first,
/// then collapses the current node when its operands are constant and the
/// operation cannot fail at runtime.
pub(crate) fn fold_expr(expr: BoundExpr) -> BoundExpr {
    let expr = fold_children(expr);
    try_fold(&expr).unwrap_or(expr)
}

fn try_fold(expr: &BoundExpr) -> Option<BoundExpr> {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            ..
        } => {
            if let Some(folded) = fold_boolean(*op, left, right) {
                return Some(folded);
            }
            let value = fold_binary(*op, non_null_literal(left)?, non_null_literal(right)?)?;
            Some(BoundExpr::Literal {
                value,
                data_type: data_type.clone(),
                nullable: false,
            })
        }
        BoundExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr: inner,
            data_type,
            ..
        } => {
            let Value::Integer(n) = non_null_literal(inner)? else {
                return None;
            };
            let negated = n.checked_neg()?;
            Some(BoundExpr::Literal {
                value: Value::Integer(negated),
                data_type: data_type.clone(),
                nullable: false,
            })
        }
        BoundExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: inner,
            data_type,
            ..
        } => match literal_value(inner)? {
            Value::Boolean(b) => Some(bool_literal(!b, data_type.clone())),
            Value::Null => Some(null_literal(data_type.clone())),
            _ => None,
        },
        BoundExpr::IsNull {
            expr: inner,
            data_type,
            ..
        } => Some(bool_literal(
            matches!(literal_value(inner)?, Value::Null),
            data_type.clone(),
        )),
        BoundExpr::IsNotNull {
            expr: inner,
            data_type,
            ..
        } => Some(bool_literal(
            !matches!(literal_value(inner)?, Value::Null),
            data_type.clone(),
        )),
        _ => None,
    }
}

/// Boolean simplification that is sound under the executor's strict, eager
/// operand evaluation (`eval_binary` evaluates BOTH operands before applying
/// `sql_and`/`sql_or`, so a discarded operand's runtime error — e.g. division by
/// zero — would otherwise be raised). A rule may therefore only drop an operand
/// that is itself a constant: it never discards a non-literal subtree.
///
/// - Two boolean constants fold directly.
/// - A redundant constant is removed while keeping the other operand:
///   `TRUE AND x -> x`, `FALSE OR x -> x` (and the symmetric forms).
/// - `FALSE AND x` and `TRUE OR x` are deliberately NOT collapsed when `x` is not
///   a constant, since that would discard `x` and suppress any error it raises.
fn fold_boolean(op: BinOp, left: &BoundExpr, right: &BoundExpr) -> Option<BoundExpr> {
    match (op, as_bool(left), as_bool(right)) {
        (BinOp::And, Some(a), Some(b)) => Some(bool_literal(a && b, DataType::Boolean)),
        (BinOp::Or, Some(a), Some(b)) => Some(bool_literal(a || b, DataType::Boolean)),
        // `TRUE AND x` / `x AND TRUE` -> x (drops only the TRUE literal, keeps x).
        (BinOp::And, Some(true), None) => Some(right.clone()),
        (BinOp::And, None, Some(true)) => Some(left.clone()),
        // `FALSE OR x` / `x OR FALSE` -> x (drops only the FALSE literal, keeps x).
        (BinOp::Or, Some(false), None) => Some(right.clone()),
        (BinOp::Or, None, Some(false)) => Some(left.clone()),
        _ => None,
    }
}

/// Evaluate a binary operator over two non-null literal values. Returns `None`
/// for any operation that could fail at runtime (overflow, divide/modulo by
/// zero) or for type combinations that should never reach here post-binding.
fn fold_binary(op: BinOp, left: &Value, right: &Value) -> Option<Value> {
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let (Value::Integer(a), Value::Integer(b)) = (left, right) else {
                return None;
            };
            let result = match op {
                BinOp::Add => a.checked_add(*b),
                BinOp::Sub => a.checked_sub(*b),
                BinOp::Mul => a.checked_mul(*b),
                BinOp::Div if *b != 0 => a.checked_div(*b),
                BinOp::Mod if *b != 0 => a.checked_rem(*b),
                _ => None,
            }?;
            Some(Value::Integer(result))
        }
        BinOp::Concat => {
            let (Value::Text(a), Value::Text(b)) = (left, right) else {
                return None;
            };
            Some(Value::Text(format!("{a}{b}")))
        }
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
            let ordering = compare_values(left, right)?;
            let result = match op {
                BinOp::Eq => ordering == Ordering::Equal,
                BinOp::Neq => ordering != Ordering::Equal,
                BinOp::Lt => ordering == Ordering::Less,
                BinOp::LtEq => ordering != Ordering::Greater,
                BinOp::Gt => ordering == Ordering::Greater,
                BinOp::GtEq => ordering != Ordering::Less,
                _ => return None,
            };
            Some(Value::Boolean(result))
        }
        BinOp::And | BinOp::Or => None,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Rebuild a node with each child folded, leaving the node kind unchanged.
fn fold_children(expr: BoundExpr) -> BoundExpr {
    match expr {
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            nullable,
        } => BoundExpr::BinaryOp {
            left: Box::new(fold_expr(*left)),
            op,
            right: Box::new(fold_expr(*right)),
            data_type,
            nullable,
        },
        BoundExpr::UnaryOp {
            op,
            expr,
            data_type,
            nullable,
        } => BoundExpr::UnaryOp {
            op,
            expr: Box::new(fold_expr(*expr)),
            data_type,
            nullable,
        },
        BoundExpr::Function {
            name,
            args,
            data_type,
            nullable,
        } => BoundExpr::Function {
            name,
            args: args.into_iter().map(fold_expr).collect(),
            data_type,
            nullable,
        },
        BoundExpr::AggregateCall {
            func,
            arg,
            distinct,
            data_type,
            nullable,
        } => BoundExpr::AggregateCall {
            func,
            arg: arg.map(|inner| Box::new(fold_expr(*inner))),
            distinct,
            data_type,
            nullable,
        },
        BoundExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNull {
            expr: Box::new(fold_expr(*expr)),
            data_type,
            nullable,
        },
        BoundExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNotNull {
            expr: Box::new(fold_expr(*expr)),
            data_type,
            nullable,
        },
        BoundExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => BoundExpr::InList {
            expr: Box::new(fold_expr(*expr)),
            list: list.into_iter().map(fold_expr).collect(),
            negated,
            data_type,
            nullable,
        },
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => BoundExpr::Between {
            expr: Box::new(fold_expr(*expr)),
            low: Box::new(fold_expr(*low)),
            high: Box::new(fold_expr(*high)),
            negated,
            data_type,
            nullable,
        },
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            data_type,
            nullable,
        } => BoundExpr::Like {
            expr: Box::new(fold_expr(*expr)),
            pattern: Box::new(fold_expr(*pattern)),
            negated,
            data_type,
            nullable,
        },
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable,
        } => BoundExpr::Case {
            operand: operand.map(|inner| Box::new(fold_expr(*inner))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(when, then)| (fold_expr(when), fold_expr(then)))
                .collect(),
            else_clause: else_clause.map(|inner| Box::new(fold_expr(*inner))),
            data_type,
            nullable,
        },
        BoundExpr::Cast {
            expr,
            data_type,
            nullable,
        } => BoundExpr::Cast {
            expr: Box::new(fold_expr(*expr)),
            data_type,
            nullable,
        },
        leaf @ (BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }) => leaf,
    }
}

fn bool_literal(value: bool, data_type: DataType) -> BoundExpr {
    BoundExpr::Literal {
        value: Value::Boolean(value),
        data_type,
        nullable: false,
    }
}

fn null_literal(data_type: DataType) -> BoundExpr {
    BoundExpr::Literal {
        value: Value::Null,
        data_type,
        nullable: true,
    }
}

fn is_true(expr: &BoundExpr) -> bool {
    matches!(
        expr,
        BoundExpr::Literal {
            value: Value::Boolean(true),
            ..
        }
    )
}

fn as_bool(expr: &BoundExpr) -> Option<bool> {
    match expr {
        BoundExpr::Literal {
            value: Value::Boolean(value),
            ..
        } => Some(*value),
        _ => None,
    }
}

fn literal_value(expr: &BoundExpr) -> Option<&Value> {
    match expr {
        BoundExpr::Literal { value, .. } => Some(value),
        _ => None,
    }
}

fn non_null_literal(expr: &BoundExpr) -> Option<&Value> {
    match literal_value(expr)? {
        Value::Null => None,
        value => Some(value),
    }
}
