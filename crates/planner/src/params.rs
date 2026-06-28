//! Extended-protocol parameter handling: collecting the parameter types a bound
//! statement uses, and substituting bound values into a statement before
//! planning and execution.

use common::{DataType, DbError, Result, SqlState, Value};

use crate::bound::{BoundFrom, BoundInsertSource};
use crate::{BoundExpr, BoundSelect, BoundStatement};

/// Resolve the parameter types for a bound statement, indexed by 0-based
/// position. Repeated uses of the same `$n` must agree on type; a position not
/// used in the query takes its declared type (from the `Parse` OIDs); a position
/// with neither a use nor a declared type is an error.
pub fn collect_param_types(
    statement: &BoundStatement,
    declared: &[Option<DataType>],
) -> Result<Vec<DataType>> {
    let mut used: Vec<Option<DataType>> = Vec::new();
    collect_statement(statement, &mut used)?;

    let count = used.len().max(declared.len());
    let mut resolved = Vec::with_capacity(count);
    for index in 0..count {
        let from_use = used.get(index).cloned().flatten();
        let from_decl = declared.get(index).cloned().flatten();
        let data_type = from_use.or(from_decl).ok_or_else(|| {
            plan_error(
                SqlState::DatatypeMismatch,
                format!("could not determine data type of parameter ${}", index + 1),
            )
        })?;
        resolved.push(data_type);
    }
    Ok(resolved)
}

/// Replace every `$n` parameter slot in a bound statement with a literal of the
/// supplied value, type-checked against the slot's resolved type. The result is
/// planned and executed like any other statement.
pub fn substitute_params(statement: &BoundStatement, params: &[Value]) -> Result<BoundStatement> {
    let mut statement = statement.clone();
    substitute_statement(&mut statement, params)?;
    Ok(statement)
}

// --- collection (read-only) ---

fn collect_statement(statement: &BoundStatement, used: &mut Vec<Option<DataType>>) -> Result<()> {
    match statement {
        // COPY carries no expressions/parameters.
        BoundStatement::CreateTable { .. }
        | BoundStatement::DropTable { .. }
        | BoundStatement::CreateIndex { .. }
        | BoundStatement::DropIndex { .. }
        | BoundStatement::Copy { .. } => Ok(()),
        BoundStatement::Insert { source, .. } => match source {
            BoundInsertSource::Values { rows, .. } => {
                for row in rows {
                    for expr in row {
                        collect_expr(expr, used)?;
                    }
                }
                Ok(())
            }
            BoundInsertSource::Query(select) => collect_select(select, used),
        },
        BoundStatement::Select(select) => collect_select(select, used),
        BoundStatement::Update {
            assignments,
            source,
            ..
        } => {
            for (_, expr) in assignments {
                collect_expr(expr, used)?;
            }
            collect_select(source, used)
        }
        BoundStatement::Delete { source, .. } => collect_select(source, used),
        BoundStatement::Explain(inner) => collect_statement(inner, used),
    }
}

fn collect_select(select: &BoundSelect, used: &mut Vec<Option<DataType>>) -> Result<()> {
    for item in &select.columns {
        collect_expr(&item.expr, used)?;
    }
    collect_from(&select.from, used)?;
    if let Some(filter) = &select.filter {
        collect_expr(filter, used)?;
    }
    for expr in &select.group_by {
        collect_expr(expr, used)?;
    }
    if let Some(having) = &select.having {
        collect_expr(having, used)?;
    }
    for item in &select.order_by {
        collect_expr(&item.expr, used)?;
    }
    Ok(())
}

fn collect_from(from: &BoundFrom, used: &mut Vec<Option<DataType>>) -> Result<()> {
    match from {
        BoundFrom::Table { .. } => Ok(()),
        BoundFrom::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_from(left, used)?;
            collect_from(right, used)?;
            if let Some(condition) = condition {
                collect_expr(condition, used)?;
            }
            Ok(())
        }
    }
}

fn collect_expr(expr: &BoundExpr, used: &mut Vec<Option<DataType>>) -> Result<()> {
    for_each_child(expr, &mut |child| collect_expr(child, used))?;
    if let Some(select) = subquery_select(expr) {
        collect_select(select, used)?;
    }
    if let BoundExpr::Parameter {
        index, data_type, ..
    } = expr
    {
        record_param(used, *index, data_type)?;
    }
    Ok(())
}

/// The inner SELECT of a subquery expression, if any. Parameter handling recurses
/// into it so `$n` placeholders inside a subquery are collected and substituted.
fn subquery_select(expr: &BoundExpr) -> Option<&BoundSelect> {
    match expr {
        BoundExpr::ScalarSubquery { select, .. }
        | BoundExpr::Exists { select, .. }
        | BoundExpr::InSubquery { select, .. } => Some(select),
        _ => None,
    }
}

fn subquery_select_mut(expr: &mut BoundExpr) -> Option<&mut BoundSelect> {
    match expr {
        BoundExpr::ScalarSubquery { select, .. }
        | BoundExpr::Exists { select, .. }
        | BoundExpr::InSubquery { select, .. } => Some(select),
        _ => None,
    }
}

fn record_param(
    used: &mut Vec<Option<DataType>>,
    index: usize,
    data_type: &DataType,
) -> Result<()> {
    if used.len() <= index {
        used.resize(index + 1, None);
    }
    match &used[index] {
        Some(existing) if existing != data_type => Err(plan_error(
            SqlState::DatatypeMismatch,
            format!("parameter ${} is used with conflicting types", index + 1),
        )),
        _ => {
            used[index] = Some(data_type.clone());
            Ok(())
        }
    }
}

// --- substitution (in place) ---

fn substitute_statement(statement: &mut BoundStatement, params: &[Value]) -> Result<()> {
    match statement {
        BoundStatement::CreateTable { .. }
        | BoundStatement::DropTable { .. }
        | BoundStatement::CreateIndex { .. }
        | BoundStatement::DropIndex { .. }
        | BoundStatement::Copy { .. } => Ok(()),
        BoundStatement::Insert { source, .. } => match source {
            BoundInsertSource::Values { rows, .. } => {
                for row in rows {
                    for expr in row {
                        substitute_expr(expr, params)?;
                    }
                }
                Ok(())
            }
            BoundInsertSource::Query(select) => substitute_select(select, params),
        },
        BoundStatement::Select(select) => substitute_select(select, params),
        BoundStatement::Update {
            assignments,
            source,
            ..
        } => {
            for (_, expr) in assignments {
                substitute_expr(expr, params)?;
            }
            substitute_select(source, params)
        }
        BoundStatement::Delete { source, .. } => substitute_select(source, params),
        BoundStatement::Explain(inner) => substitute_statement(inner, params),
    }
}

fn substitute_select(select: &mut BoundSelect, params: &[Value]) -> Result<()> {
    for item in &mut select.columns {
        substitute_expr(&mut item.expr, params)?;
    }
    substitute_from(&mut select.from, params)?;
    if let Some(filter) = &mut select.filter {
        substitute_expr(filter, params)?;
    }
    for expr in &mut select.group_by {
        substitute_expr(expr, params)?;
    }
    if let Some(having) = &mut select.having {
        substitute_expr(having, params)?;
    }
    for item in &mut select.order_by {
        substitute_expr(&mut item.expr, params)?;
    }
    Ok(())
}

fn substitute_from(from: &mut BoundFrom, params: &[Value]) -> Result<()> {
    match from {
        BoundFrom::Table { .. } => Ok(()),
        BoundFrom::Join {
            left,
            right,
            condition,
            ..
        } => {
            substitute_from(left, params)?;
            substitute_from(right, params)?;
            if let Some(condition) = condition {
                substitute_expr(condition, params)?;
            }
            Ok(())
        }
    }
}

fn substitute_expr(expr: &mut BoundExpr, params: &[Value]) -> Result<()> {
    for_each_child_mut(expr, &mut |child| substitute_expr(child, params))?;
    if let Some(select) = subquery_select_mut(expr) {
        substitute_select(select, params)?;
    }

    let BoundExpr::Parameter {
        index,
        data_type,
        nullable,
    } = expr
    else {
        return Ok(());
    };
    let index = *index;
    let data_type = data_type.clone();
    let nullable = *nullable;
    let value = params
        .get(index)
        .cloned()
        .ok_or_else(|| DbError::internal(format!("missing value for parameter ${}", index + 1)))?;
    if value_type(&value).is_some_and(|actual| actual != data_type) {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            format!("parameter ${} value has the wrong type", index + 1),
        ));
    }
    *expr = BoundExpr::Literal {
        value,
        data_type,
        nullable,
    };
    Ok(())
}

// --- shared child traversal ---

fn for_each_child(expr: &BoundExpr, f: &mut impl FnMut(&BoundExpr) -> Result<()>) -> Result<()> {
    match expr {
        // The subquery body is reached via `subquery_select`, not as a BoundExpr
        // child; only `InSubquery`'s left operand is a direct child here.
        BoundExpr::InSubquery { expr, .. } => f(expr),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => Ok(()),
        BoundExpr::BinaryOp { left, right, .. } => {
            f(left)?;
            f(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => f(expr),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                f(arg)?;
            }
            Ok(())
        }
        BoundExpr::AggregateCall { arg, .. } => {
            if let Some(arg) = arg {
                f(arg)?;
            }
            Ok(())
        }
        BoundExpr::InList { expr, list, .. } => {
            f(expr)?;
            for item in list {
                f(item)?;
            }
            Ok(())
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            f(expr)?;
            f(low)?;
            f(high)
        }
        BoundExpr::Like { expr, pattern, .. } => {
            f(expr)?;
            f(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                f(operand)?;
            }
            for (when, then) in when_clauses {
                f(when)?;
                f(then)?;
            }
            if let Some(else_clause) = else_clause {
                f(else_clause)?;
            }
            Ok(())
        }
    }
}

fn for_each_child_mut(
    expr: &mut BoundExpr,
    f: &mut impl FnMut(&mut BoundExpr) -> Result<()>,
) -> Result<()> {
    match expr {
        BoundExpr::InSubquery { expr, .. } => f(expr),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => Ok(()),
        BoundExpr::BinaryOp { left, right, .. } => {
            f(left)?;
            f(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => f(expr),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                f(arg)?;
            }
            Ok(())
        }
        BoundExpr::AggregateCall { arg, .. } => {
            if let Some(arg) = arg {
                f(arg)?;
            }
            Ok(())
        }
        BoundExpr::InList { expr, list, .. } => {
            f(expr)?;
            for item in list {
                f(item)?;
            }
            Ok(())
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            f(expr)?;
            f(low)?;
            f(high)
        }
        BoundExpr::Like { expr, pattern, .. } => {
            f(expr)?;
            f(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                f(operand)?;
            }
            for (when, then) in when_clauses {
                f(when)?;
                f(then)?;
            }
            if let Some(else_clause) = else_clause {
                f(else_clause)?;
            }
            Ok(())
        }
    }
}

fn value_type(value: &Value) -> Option<DataType> {
    match value {
        Value::Null => None,
        Value::Boolean(_) => Some(DataType::Boolean),
        Value::Integer(_) => Some(DataType::Integer),
        Value::Float(_) => Some(DataType::Double),
        Value::Numeric(_) => Some(DataType::Numeric {
            precision: None,
            scale: 0,
        }),
        Value::Text(_) => Some(DataType::Text),
        Value::Date(_) => Some(DataType::Date),
        Value::Timestamp(_) => Some(DataType::Timestamp),
        Value::Bytes(_) => Some(DataType::Bytea),
        Value::Uuid(_) => Some(DataType::Uuid),
    }
}

fn plan_error(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::plan(code, message)
}
