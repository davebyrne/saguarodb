use catalog::CatalogManager;
use common::{
    ArgType, ColumnDef, DbError, PgType, Result, STORED_EXPRESSION_VERSION, SqlState, StoredBinOp,
    StoredExpr, StoredExpression, StoredUnaryOp, lookup_scalar_function_by_id, scalar_function_id,
    scalar_function_id_matches, validate_stored_expression_shape,
};

use crate::{BinOp, BoundExpr, UnaryOp};

pub fn store_bound_expression(
    expr: &BoundExpr,
    sql: String,
    columns: &[ColumnDef],
) -> Result<StoredExpression> {
    let stored = StoredExpression {
        version: STORED_EXPRESSION_VERSION,
        sql,
        root: store_node(expr, columns)?,
        data_type: expr.data_type(),
        pg_type: expression_pg_type(expr),
        nullable: expr.nullable(),
    };
    validate_stored_expression(&stored)?;
    Ok(stored)
}

pub fn lower_stored_expression(
    catalog: &dyn CatalogManager,
    stored: &StoredExpression,
    columns: &[ColumnDef],
) -> Result<BoundExpr> {
    validate_stored_expression(stored)?;
    let expr = lower_node(catalog, &stored.root, columns)?;
    if expr.data_type() != stored.data_type || expr.nullable() != stored.nullable {
        return Err(corrupt(
            "stored expression root metadata does not match its envelope",
        ));
    }
    Ok(expr)
}

pub fn validate_stored_expression(stored: &StoredExpression) -> Result<()> {
    validate_stored_expression_shape(stored)
}

fn store_node(expr: &BoundExpr, columns: &[ColumnDef]) -> Result<StoredExpr> {
    Ok(match expr {
        BoundExpr::Literal {
            value,
            data_type,
            nullable,
        } => {
            if !common::value_is_finite(value) {
                return Err(DbError::plan(
                    SqlState::NumericValueOutOfRange,
                    "non-finite values cannot be persisted in a DEFAULT or CHECK expression",
                ));
            }
            StoredExpr::Literal {
                value: value.clone(),
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        BoundExpr::InputRef {
            column,
            data_type,
            nullable,
            ..
        } => {
            let column = columns
                .iter()
                .find(|candidate| candidate.id == *column)
                .ok_or_else(|| {
                    corrupt(format!(
                        "bound expression references missing dense column {column}"
                    ))
                })?;
            StoredExpr::Column {
                column: column.object_id,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            nullable,
        } => StoredExpr::Binary {
            left: Box::new(store_node(left, columns)?),
            op: store_bin_op(*op),
            right: Box::new(store_node(right, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::UnaryOp {
            op,
            expr,
            data_type,
            nullable,
        } => StoredExpr::Unary {
            op: store_unary_op(*op),
            expr: Box::new(store_node(expr, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Function {
            name,
            args,
            data_type,
            pg_type,
            nullable,
        } => {
            let argument_types: Vec<_> = args.iter().map(BoundExpr::data_type).collect();
            let function =
                scalar_function_id(name, &argument_types, data_type).ok_or_else(|| {
                    corrupt(format!(
                        "function {name} has no stable function OID for its bound signature"
                    ))
                })?;
            StoredExpr::Function {
                function,
                args: args
                    .iter()
                    .map(|arg| store_node(arg, columns))
                    .collect::<Result<_>>()?,
                data_type: data_type.clone(),
                pg_type: pg_type.clone(),
                nullable: *nullable,
            }
        }
        BoundExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => StoredExpr::Array {
            elements: elements
                .iter()
                .map(|value| store_node(value, columns))
                .collect::<Result<_>>()?,
            dimensions: dimensions.clone(),
            element_type: element_type.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => StoredExpr::ArraySubscript {
            array: Box::new(store_node(array, columns)?),
            subscripts: subscripts
                .iter()
                .map(|value| store_node(value, columns))
                .collect::<Result<_>>()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => StoredExpr::Any {
            left: Box::new(store_node(left, columns)?),
            op: store_bin_op(*op),
            array: Box::new(store_node(array, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Nextval {
            sequence,
            data_type,
            nullable,
        } => StoredExpr::Nextval {
            sequence: *sequence,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Currval {
            sequence,
            data_type,
            nullable,
        } => StoredExpr::Currval {
            sequence: *sequence,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Setval {
            sequence,
            value,
            is_called,
            data_type,
            nullable,
        } => StoredExpr::Setval {
            sequence: *sequence,
            value: Box::new(store_node(value, columns)?),
            is_called: store_optional(is_called.as_deref(), columns)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => StoredExpr::IsNull {
            expr: Box::new(store_node(expr, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => StoredExpr::IsNotNull {
            expr: Box::new(store_node(expr, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => StoredExpr::InList {
            expr: Box::new(store_node(expr, columns)?),
            list: list
                .iter()
                .map(|value| store_node(value, columns))
                .collect::<Result<_>>()?,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => StoredExpr::Between {
            expr: Box::new(store_node(expr, columns)?),
            low: Box::new(store_node(low, columns)?),
            high: Box::new(store_node(high, columns)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape,
            data_type,
            nullable,
        } => StoredExpr::Like {
            expr: Box::new(store_node(expr, columns)?),
            pattern: Box::new(store_node(pattern, columns)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape: *escape,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable,
        } => StoredExpr::Case {
            operand: store_optional(operand.as_deref(), columns)?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| Ok((store_node(when, columns)?, store_node(then, columns)?)))
                .collect::<Result<_>>()?,
            else_clause: store_optional(else_clause.as_deref(), columns)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => StoredExpr::Cast {
            expr: Box::new(store_node(expr, columns)?),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Parameter { .. }
        | BoundExpr::AggregateCall { .. }
        | BoundExpr::WindowCall { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::RuntimeInSet { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. }
        | BoundExpr::InSubquery { .. } => {
            return Err(DbError::plan(
                SqlState::FeatureNotSupported,
                "expression form cannot be persisted in a DEFAULT or CHECK constraint",
            ));
        }
    })
}

fn lower_node(
    catalog: &dyn CatalogManager,
    expr: &StoredExpr,
    columns: &[ColumnDef],
) -> Result<BoundExpr> {
    Ok(match expr {
        StoredExpr::Literal {
            value,
            data_type,
            nullable,
        } => BoundExpr::Literal {
            value: value.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Column {
            column,
            data_type,
            nullable,
        } => {
            let definition = columns
                .iter()
                .find(|candidate| candidate.object_id == *column)
                .ok_or_else(|| {
                    corrupt(format!(
                        "stored expression references unknown column object id {column}"
                    ))
                })?;
            if definition.data_type != *data_type || (definition.nullable && !nullable) {
                return Err(corrupt(format!(
                    "stored expression column object id {column} has stale type metadata"
                )));
            }
            BoundExpr::InputRef {
                input: 0,
                column: definition.id,
                slot: usize::from(definition.id),
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredExpr::Binary {
            left,
            op,
            right,
            data_type,
            nullable,
        } => BoundExpr::BinaryOp {
            left: Box::new(lower_node(catalog, left, columns)?),
            op: lower_bin_op(*op),
            right: Box::new(lower_node(catalog, right, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Unary {
            op,
            expr,
            data_type,
            nullable,
        } => BoundExpr::UnaryOp {
            op: lower_unary_op(*op),
            expr: Box::new(lower_node(catalog, expr, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Function {
            function,
            args,
            data_type,
            pg_type,
            nullable,
        } => {
            let (registered, _) = lookup_scalar_function_by_id(*function).ok_or_else(|| {
                corrupt(format!(
                    "stored expression references unknown function id {function}"
                ))
            })?;
            let args: Vec<_> = args
                .iter()
                .map(|arg| lower_node(catalog, arg, columns))
                .collect::<Result<_>>()?;
            let arg_types: Vec<_> = args
                .iter()
                .map(|arg| ArgType {
                    data_type: arg.data_type(),
                    literal: match arg {
                        BoundExpr::Literal { value, .. } => Some(value),
                        _ => None,
                    },
                })
                .collect();
            let durable_arg_types: Vec<_> = args.iter().map(BoundExpr::data_type).collect();
            if !scalar_function_id_matches(
                *function,
                &durable_arg_types,
                data_type,
                pg_type.as_ref(),
            ) {
                return Err(corrupt(format!(
                    "stored function id {function} does not match its durable signature"
                )));
            }
            let result = (registered.signature)(registered.name, &arg_types).map_err(|_| {
                corrupt(format!(
                    "stored function id {function} has invalid argument types"
                ))
            })?;
            if result != *data_type
                || registered.result_nullable(args.iter().map(BoundExpr::nullable)) != *nullable
            {
                return Err(corrupt(format!(
                    "stored function id {function} has stale result metadata"
                )));
            }
            BoundExpr::Function {
                name: registered.name.to_string(),
                args,
                data_type: data_type.clone(),
                pg_type: pg_type.clone(),
                nullable: *nullable,
            }
        }
        StoredExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => BoundExpr::Array {
            elements: elements
                .iter()
                .map(|value| lower_node(catalog, value, columns))
                .collect::<Result<_>>()?,
            dimensions: dimensions.clone(),
            element_type: element_type.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => BoundExpr::ArraySubscript {
            array: Box::new(lower_node(catalog, array, columns)?),
            subscripts: subscripts
                .iter()
                .map(|value| lower_node(catalog, value, columns))
                .collect::<Result<_>>()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => BoundExpr::Any {
            left: Box::new(lower_node(catalog, left, columns)?),
            op: lower_bin_op(*op),
            array: Box::new(lower_node(catalog, array, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Nextval {
            sequence,
            data_type,
            nullable,
        } => {
            require_sequence(catalog, *sequence)?;
            BoundExpr::Nextval {
                sequence: *sequence,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredExpr::Currval {
            sequence,
            data_type,
            nullable,
        } => {
            require_sequence(catalog, *sequence)?;
            BoundExpr::Currval {
                sequence: *sequence,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredExpr::Setval {
            sequence,
            value,
            is_called,
            data_type,
            nullable,
        } => {
            require_sequence(catalog, *sequence)?;
            BoundExpr::Setval {
                sequence: *sequence,
                value: Box::new(lower_node(catalog, value, columns)?),
                is_called: lower_optional(catalog, is_called.as_deref(), columns)?,
                data_type: data_type.clone(),
                nullable: *nullable,
            }
        }
        StoredExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNull {
            expr: Box::new(lower_node(catalog, expr, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNotNull {
            expr: Box::new(lower_node(catalog, expr, columns)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => BoundExpr::InList {
            expr: Box::new(lower_node(catalog, expr, columns)?),
            list: list
                .iter()
                .map(|value| lower_node(catalog, value, columns))
                .collect::<Result<_>>()?,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => BoundExpr::Between {
            expr: Box::new(lower_node(catalog, expr, columns)?),
            low: Box::new(lower_node(catalog, low, columns)?),
            high: Box::new(lower_node(catalog, high, columns)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape,
            data_type,
            nullable,
        } => BoundExpr::Like {
            expr: Box::new(lower_node(catalog, expr, columns)?),
            pattern: Box::new(lower_node(catalog, pattern, columns)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape: *escape,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable,
        } => BoundExpr::Case {
            operand: lower_optional(catalog, operand.as_deref(), columns)?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| {
                    Ok((
                        lower_node(catalog, when, columns)?,
                        lower_node(catalog, then, columns)?,
                    ))
                })
                .collect::<Result<_>>()?,
            else_clause: lower_optional(catalog, else_clause.as_deref(), columns)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        StoredExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => BoundExpr::Cast {
            expr: Box::new(lower_node(catalog, expr, columns)?),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        },
    })
}

fn store_optional(
    expr: Option<&BoundExpr>,
    columns: &[ColumnDef],
) -> Result<Option<Box<StoredExpr>>> {
    expr.map(|value| store_node(value, columns).map(Box::new))
        .transpose()
}
fn lower_optional(
    catalog: &dyn CatalogManager,
    expr: Option<&StoredExpr>,
    columns: &[ColumnDef],
) -> Result<Option<Box<BoundExpr>>> {
    expr.map(|value| lower_node(catalog, value, columns).map(Box::new))
        .transpose()
}
fn require_sequence(catalog: &dyn CatalogManager, sequence: u32) -> Result<()> {
    if catalog.get_sequence(sequence)?.is_none() {
        return Err(corrupt(format!(
            "stored expression references unknown sequence id {sequence}"
        )));
    }
    Ok(())
}
fn corrupt(message: impl Into<String>) -> DbError {
    DbError::internal(message)
}
fn expression_pg_type(expr: &BoundExpr) -> Option<PgType> {
    match expr {
        BoundExpr::Function { pg_type, .. } | BoundExpr::Parameter { pg_type, .. } => {
            pg_type.clone()
        }
        BoundExpr::Cast { pg_type, .. } => Some(pg_type.clone()),
        _ => None,
    }
}
fn store_bin_op(op: BinOp) -> StoredBinOp {
    match op {
        BinOp::Add => StoredBinOp::Add,
        BinOp::Sub => StoredBinOp::Sub,
        BinOp::Mul => StoredBinOp::Mul,
        BinOp::Div => StoredBinOp::Div,
        BinOp::Mod => StoredBinOp::Mod,
        BinOp::Eq => StoredBinOp::Eq,
        BinOp::Neq => StoredBinOp::Neq,
        BinOp::Lt => StoredBinOp::Lt,
        BinOp::LtEq => StoredBinOp::LtEq,
        BinOp::Gt => StoredBinOp::Gt,
        BinOp::GtEq => StoredBinOp::GtEq,
        BinOp::And => StoredBinOp::And,
        BinOp::Or => StoredBinOp::Or,
        BinOp::Concat => StoredBinOp::Concat,
        BinOp::IsDistinctFrom => StoredBinOp::IsDistinctFrom,
        BinOp::IsNotDistinctFrom => StoredBinOp::IsNotDistinctFrom,
    }
}
fn lower_bin_op(op: StoredBinOp) -> BinOp {
    match op {
        StoredBinOp::Add => BinOp::Add,
        StoredBinOp::Sub => BinOp::Sub,
        StoredBinOp::Mul => BinOp::Mul,
        StoredBinOp::Div => BinOp::Div,
        StoredBinOp::Mod => BinOp::Mod,
        StoredBinOp::Eq => BinOp::Eq,
        StoredBinOp::Neq => BinOp::Neq,
        StoredBinOp::Lt => BinOp::Lt,
        StoredBinOp::LtEq => BinOp::LtEq,
        StoredBinOp::Gt => BinOp::Gt,
        StoredBinOp::GtEq => BinOp::GtEq,
        StoredBinOp::And => BinOp::And,
        StoredBinOp::Or => BinOp::Or,
        StoredBinOp::Concat => BinOp::Concat,
        StoredBinOp::IsDistinctFrom => BinOp::IsDistinctFrom,
        StoredBinOp::IsNotDistinctFrom => BinOp::IsNotDistinctFrom,
    }
}
fn store_unary_op(op: UnaryOp) -> StoredUnaryOp {
    match op {
        UnaryOp::Neg => StoredUnaryOp::Neg,
        UnaryOp::Not => StoredUnaryOp::Not,
    }
}
fn lower_unary_op(op: StoredUnaryOp) -> UnaryOp {
    match op {
        StoredUnaryOp::Neg => UnaryOp::Neg,
        StoredUnaryOp::Not => UnaryOp::Not,
    }
}
