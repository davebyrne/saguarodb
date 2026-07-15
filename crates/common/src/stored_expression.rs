use std::{fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as _, SeqAccess, Visitor},
};

use crate::{
    ArgType, ColumnObjectId, DataType, DbError, FunctionId, PgType, SequenceId, Value,
    lookup_scalar_function_by_id, scalar_function_id_matches, value_is_finite, value_matches_type,
};

pub const STORED_EXPRESSION_VERSION: u32 = 1;
pub const MAX_STORED_EXPRESSION_NODES: usize = 16_384;
pub const MAX_STORED_EXPRESSION_LIST_ITEMS: usize = 4_096;
pub const MAX_STORED_EXPRESSION_DEPTH: usize = 128;
pub const MAX_STORED_EXPRESSION_SQL_BYTES: usize = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredExpression {
    pub version: u32,
    #[serde(deserialize_with = "deserialize_bounded_sql")]
    pub sql: String,
    pub root: StoredExpr,
    pub data_type: DataType,
    pub pg_type: Option<PgType>,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredExpr {
    Literal {
        value: Value,
        data_type: DataType,
        nullable: bool,
    },
    Column {
        column: ColumnObjectId,
        data_type: DataType,
        nullable: bool,
    },
    Binary {
        left: Box<Self>,
        op: StoredBinOp,
        right: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Unary {
        op: StoredUnaryOp,
        expr: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Function {
        function: FunctionId,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        args: Vec<Self>,
        data_type: DataType,
        pg_type: Option<PgType>,
        nullable: bool,
    },
    Array {
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        elements: Vec<Self>,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        dimensions: Vec<u32>,
        element_type: DataType,
        data_type: DataType,
        nullable: bool,
    },
    ArraySubscript {
        array: Box<Self>,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        subscripts: Vec<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Any {
        left: Box<Self>,
        op: StoredBinOp,
        array: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    Nextval {
        sequence: SequenceId,
        data_type: DataType,
        nullable: bool,
    },
    Currval {
        sequence: SequenceId,
        data_type: DataType,
        nullable: bool,
    },
    Setval {
        sequence: SequenceId,
        value: Box<Self>,
        is_called: Option<Box<Self>>,
        data_type: DataType,
        nullable: bool,
    },
    IsNull {
        expr: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    IsNotNull {
        expr: Box<Self>,
        data_type: DataType,
        nullable: bool,
    },
    InList {
        expr: Box<Self>,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        list: Vec<Self>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Between {
        expr: Box<Self>,
        low: Box<Self>,
        high: Box<Self>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Like {
        expr: Box<Self>,
        pattern: Box<Self>,
        negated: bool,
        case_insensitive: bool,
        escape: Option<char>,
        data_type: DataType,
        nullable: bool,
    },
    Case {
        operand: Option<Box<Self>>,
        #[serde(deserialize_with = "deserialize_bounded_vec")]
        when_clauses: Vec<(Self, Self)>,
        else_clause: Option<Box<Self>>,
        data_type: DataType,
        nullable: bool,
    },
    Cast {
        expr: Box<Self>,
        data_type: DataType,
        pg_type: PgType,
        nullable: bool,
    },
}

fn deserialize_bounded_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVecVisitor<T>(PhantomData<T>);

    impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a sequence containing at most {MAX_STORED_EXPRESSION_LIST_ITEMS} items"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|size| size > MAX_STORED_EXPRESSION_LIST_ITEMS)
            {
                return Err(A::Error::custom("stored expression list exceeds limit"));
            }
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element()? {
                if values.len() >= MAX_STORED_EXPRESSION_LIST_ITEMS {
                    return Err(A::Error::custom("stored expression list exceeds limit"));
                }
                values
                    .try_reserve(1)
                    .map_err(|_| A::Error::custom("stored expression list allocation failed"))?;
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor(PhantomData))
}

fn deserialize_bounded_sql<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoundedSqlVisitor;

    impl Visitor<'_> for BoundedSqlVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a string containing at most {MAX_STORED_EXPRESSION_SQL_BYTES} bytes"
            )
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > MAX_STORED_EXPRESSION_SQL_BYTES {
                return Err(E::custom("stored expression SQL exceeds limit"));
            }
            Ok(value.to_owned())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > MAX_STORED_EXPRESSION_SQL_BYTES {
                return Err(E::custom("stored expression SQL exceeds limit"));
            }
            Ok(value)
        }
    }

    deserializer.deserialize_string(BoundedSqlVisitor)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredUnaryOp {
    Neg,
    Not,
}

impl StoredExpr {
    pub fn data_type(&self) -> &DataType {
        match self {
            Self::Literal { data_type, .. }
            | Self::Column { data_type, .. }
            | Self::Binary { data_type, .. }
            | Self::Unary { data_type, .. }
            | Self::Function { data_type, .. }
            | Self::Array { data_type, .. }
            | Self::ArraySubscript { data_type, .. }
            | Self::Any { data_type, .. }
            | Self::Nextval { data_type, .. }
            | Self::Currval { data_type, .. }
            | Self::Setval { data_type, .. }
            | Self::IsNull { data_type, .. }
            | Self::IsNotNull { data_type, .. }
            | Self::InList { data_type, .. }
            | Self::Between { data_type, .. }
            | Self::Like { data_type, .. }
            | Self::Case { data_type, .. }
            | Self::Cast { data_type, .. } => data_type,
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            Self::Literal { nullable, .. }
            | Self::Column { nullable, .. }
            | Self::Binary { nullable, .. }
            | Self::Unary { nullable, .. }
            | Self::Function { nullable, .. }
            | Self::Array { nullable, .. }
            | Self::ArraySubscript { nullable, .. }
            | Self::Any { nullable, .. }
            | Self::Nextval { nullable, .. }
            | Self::Currval { nullable, .. }
            | Self::Setval { nullable, .. }
            | Self::IsNull { nullable, .. }
            | Self::IsNotNull { nullable, .. }
            | Self::InList { nullable, .. }
            | Self::Between { nullable, .. }
            | Self::Like { nullable, .. }
            | Self::Case { nullable, .. }
            | Self::Cast { nullable, .. } => *nullable,
        }
    }

    /// Returns whether this durable expression references the given sequence.
    pub fn references_sequence(&self, target: SequenceId) -> bool {
        let mut found = false;
        self.for_each_sequence_reference(&mut |sequence| found |= sequence == target);
        found
    }

    /// Visits every sequence identity embedded in this durable expression.
    pub fn for_each_sequence_reference(&self, visitor: &mut impl FnMut(SequenceId)) {
        match self {
            Self::Nextval { sequence, .. } | Self::Currval { sequence, .. } => {
                visitor(*sequence);
            }
            Self::Binary { left, right, .. }
            | Self::Any {
                left, array: right, ..
            } => {
                left.for_each_sequence_reference(visitor);
                right.for_each_sequence_reference(visitor);
            }
            Self::Unary { expr, .. }
            | Self::IsNull { expr, .. }
            | Self::IsNotNull { expr, .. }
            | Self::Cast { expr, .. } => expr.for_each_sequence_reference(visitor),
            Self::Function { args, .. } => {
                for argument in args {
                    argument.for_each_sequence_reference(visitor);
                }
            }
            Self::Array { elements, .. } => {
                for element in elements {
                    element.for_each_sequence_reference(visitor);
                }
            }
            Self::ArraySubscript {
                array, subscripts, ..
            } => {
                array.for_each_sequence_reference(visitor);
                for subscript in subscripts {
                    subscript.for_each_sequence_reference(visitor);
                }
            }
            Self::Setval {
                sequence,
                value,
                is_called,
                ..
            } => {
                visitor(*sequence);
                value.for_each_sequence_reference(visitor);
                if let Some(is_called) = is_called {
                    is_called.for_each_sequence_reference(visitor);
                }
            }
            Self::InList { expr, list, .. } => {
                expr.for_each_sequence_reference(visitor);
                for item in list {
                    item.for_each_sequence_reference(visitor);
                }
            }
            Self::Between {
                expr, low, high, ..
            } => {
                expr.for_each_sequence_reference(visitor);
                low.for_each_sequence_reference(visitor);
                high.for_each_sequence_reference(visitor);
            }
            Self::Like { expr, pattern, .. } => {
                expr.for_each_sequence_reference(visitor);
                pattern.for_each_sequence_reference(visitor);
            }
            Self::Case {
                operand,
                when_clauses,
                else_clause,
                ..
            } => {
                if let Some(operand) = operand {
                    operand.for_each_sequence_reference(visitor);
                }
                for (when, then) in when_clauses {
                    when.for_each_sequence_reference(visitor);
                    then.for_each_sequence_reference(visitor);
                }
                if let Some(else_clause) = else_clause {
                    else_clause.for_each_sequence_reference(visitor);
                }
            }
            Self::Literal { .. } | Self::Column { .. } => {}
        }
    }

    pub fn node_count_and_depth(&self) -> Option<(usize, usize)> {
        fn visit(
            expr: &StoredExpr,
            depth: usize,
            nodes: &mut usize,
            max_depth: &mut usize,
        ) -> Option<()> {
            *nodes = nodes.checked_add(1)?;
            *max_depth = (*max_depth).max(depth);
            let next = depth.checked_add(1)?;
            match expr {
                StoredExpr::Literal { .. }
                | StoredExpr::Column { .. }
                | StoredExpr::Nextval { .. }
                | StoredExpr::Currval { .. } => {}
                StoredExpr::Binary { left, right, .. }
                | StoredExpr::Any {
                    left, array: right, ..
                } => {
                    visit(left, next, nodes, max_depth)?;
                    visit(right, next, nodes, max_depth)?;
                }
                StoredExpr::Unary { expr, .. }
                | StoredExpr::IsNull { expr, .. }
                | StoredExpr::IsNotNull { expr, .. }
                | StoredExpr::Cast { expr, .. } => visit(expr, next, nodes, max_depth)?,
                StoredExpr::Function { args, .. } => {
                    for arg in args {
                        visit(arg, next, nodes, max_depth)?;
                    }
                }
                StoredExpr::Array { elements, .. } => {
                    for element in elements {
                        visit(element, next, nodes, max_depth)?;
                    }
                }
                StoredExpr::ArraySubscript {
                    array, subscripts, ..
                } => {
                    visit(array, next, nodes, max_depth)?;
                    for subscript in subscripts {
                        visit(subscript, next, nodes, max_depth)?;
                    }
                }
                StoredExpr::Setval {
                    value, is_called, ..
                } => {
                    visit(value, next, nodes, max_depth)?;
                    if let Some(is_called) = is_called {
                        visit(is_called, next, nodes, max_depth)?;
                    }
                }
                StoredExpr::InList { expr, list, .. } => {
                    visit(expr, next, nodes, max_depth)?;
                    for item in list {
                        visit(item, next, nodes, max_depth)?;
                    }
                }
                StoredExpr::Between {
                    expr, low, high, ..
                } => {
                    visit(expr, next, nodes, max_depth)?;
                    visit(low, next, nodes, max_depth)?;
                    visit(high, next, nodes, max_depth)?;
                }
                StoredExpr::Like { expr, pattern, .. } => {
                    visit(expr, next, nodes, max_depth)?;
                    visit(pattern, next, nodes, max_depth)?;
                }
                StoredExpr::Case {
                    operand,
                    when_clauses,
                    else_clause,
                    ..
                } => {
                    if let Some(operand) = operand {
                        visit(operand, next, nodes, max_depth)?;
                    }
                    for (when, then) in when_clauses {
                        visit(when, next, nodes, max_depth)?;
                        visit(then, next, nodes, max_depth)?;
                    }
                    if let Some(otherwise) = else_clause {
                        visit(otherwise, next, nodes, max_depth)?;
                    }
                }
            }
            Some(())
        }
        let mut nodes = 0;
        let mut depth = 0;
        visit(self, 1, &mut nodes, &mut depth)?;
        Some((nodes, depth))
    }
}

/// Validate the self-contained type/nullability contract of durable scalar IR.
/// Catalog-specific column and sequence existence checks remain the catalog's
/// responsibility.
pub fn validate_stored_expression_shape(stored: &StoredExpression) -> crate::Result<()> {
    if stored.version != STORED_EXPRESSION_VERSION {
        return Err(corrupt(format!(
            "unsupported stored expression version {}",
            stored.version
        )));
    }
    if stored.sql.len() > MAX_STORED_EXPRESSION_SQL_BYTES {
        return Err(corrupt("stored expression SQL exceeds durable size limit"));
    }
    let (nodes, depth) = stored
        .root
        .node_count_and_depth()
        .ok_or_else(|| corrupt("stored expression size overflow"))?;
    if nodes > MAX_STORED_EXPRESSION_NODES || depth > MAX_STORED_EXPRESSION_DEPTH {
        return Err(corrupt("stored expression exceeds durable size limits"));
    }
    validate_node_shape(&stored.root)?;
    require_metadata(&stored.root, &stored.data_type, stored.nullable)
}

fn validate_node_shape(expr: &StoredExpr) -> crate::Result<()> {
    match expr {
        StoredExpr::Literal {
            value,
            data_type,
            nullable,
        } => {
            if !value_matches_type(value, data_type)
                || *nullable != matches!(value, Value::Null)
                || !value_is_finite(value)
            {
                return Err(corrupt("stored literal has invalid type metadata"));
            }
        }
        StoredExpr::Column { .. } => {}
        StoredExpr::Binary {
            left,
            op,
            right,
            data_type,
            nullable,
        } => {
            validate_node_shape(left)?;
            validate_node_shape(right)?;
            let expected = binary_result_type(left.data_type(), *op, right.data_type())
                .ok_or_else(|| corrupt("stored binary expression has invalid operand types"))?;
            let expected_nullable = if matches!(
                op,
                StoredBinOp::IsDistinctFrom | StoredBinOp::IsNotDistinctFrom
            ) {
                false
            } else {
                left.nullable() || right.nullable()
            };
            require_declared_metadata(data_type, *nullable, &expected, expected_nullable)?;
        }
        StoredExpr::Unary {
            op,
            expr,
            data_type,
            nullable,
        } => {
            validate_node_shape(expr)?;
            let valid = match op {
                StoredUnaryOp::Neg => {
                    numeric_family(expr.data_type()).is_some()
                        || expr.data_type() == &DataType::Interval
                }
                StoredUnaryOp::Not => expr.data_type() == &DataType::Boolean,
            };
            let expected = match op {
                StoredUnaryOp::Neg => expr.data_type().clone(),
                StoredUnaryOp::Not => DataType::Boolean,
            };
            if !valid {
                return Err(corrupt("stored unary expression has invalid operand type"));
            }
            require_declared_metadata(data_type, *nullable, &expected, expr.nullable())?;
        }
        StoredExpr::Function {
            function,
            args,
            data_type,
            pg_type,
            nullable,
        } => {
            validate_list(args)?;
            let (registered, _) = lookup_scalar_function_by_id(*function).ok_or_else(|| {
                corrupt(format!(
                    "stored expression references unknown function id {function}"
                ))
            })?;
            let argument_types: Vec<_> = args
                .iter()
                .map(|argument| argument.data_type().clone())
                .collect();
            if !scalar_function_id_matches(*function, &argument_types, data_type, pg_type.as_ref())
            {
                return Err(corrupt(format!(
                    "stored function id {function} does not match its durable signature"
                )));
            }
            let arguments: Vec<_> = args
                .iter()
                .map(|argument| ArgType {
                    data_type: argument.data_type().clone(),
                    literal: match argument {
                        StoredExpr::Literal { value, .. } => Some(value),
                        _ => None,
                    },
                })
                .collect();
            let result = (registered.signature)(registered.name, &arguments).map_err(|_| {
                corrupt(format!(
                    "stored function id {function} has invalid argument types"
                ))
            })?;
            let result_nullable = registered.result_nullable(args.iter().map(StoredExpr::nullable));
            require_declared_metadata(data_type, *nullable, &result, result_nullable)?;
        }
        StoredExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => {
            validate_list(elements)?;
            require_list_limit(dimensions.len())?;
            if matches!(element_type, DataType::Array(_))
                || elements
                    .iter()
                    .any(|element| element.data_type() != element_type)
                || !array_shape_matches(dimensions, elements.len())?
            {
                return Err(corrupt(
                    "stored array expression has invalid shape or element type",
                ));
            }
            let expected = DataType::Array(crate::ArrayType::new(element_type.clone())?);
            require_declared_metadata(data_type, *nullable, &expected, false)?;
        }
        StoredExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => {
            validate_node_shape(array)?;
            validate_list(subscripts)?;
            let DataType::Array(array_type) = array.data_type() else {
                return Err(corrupt("stored array subscript has a non-array operand"));
            };
            if subscripts.is_empty()
                || subscripts
                    .iter()
                    .any(|subscript| subscript.data_type() != &DataType::Integer)
            {
                return Err(corrupt("stored array subscript has invalid indexes"));
            }
            require_declared_metadata(data_type, *nullable, array_type.element_type(), true)?;
        }
        StoredExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => {
            validate_node_shape(left)?;
            validate_node_shape(array)?;
            let DataType::Array(array_type) = array.data_type() else {
                return Err(corrupt("stored ANY has a non-array operand"));
            };
            if !is_comparison(*op) || array_type.element_type() != left.data_type() {
                return Err(corrupt("stored ANY has invalid operand metadata"));
            }
            require_declared_metadata(data_type, *nullable, &DataType::Boolean, true)?;
        }
        StoredExpr::Nextval {
            data_type,
            nullable,
            ..
        }
        | StoredExpr::Currval {
            data_type,
            nullable,
            ..
        } => require_declared_metadata(data_type, *nullable, &DataType::Integer, false)?,
        StoredExpr::Setval {
            value,
            is_called,
            data_type,
            nullable,
            ..
        } => {
            validate_node_shape(value)?;
            if let Some(is_called) = is_called {
                validate_node_shape(is_called)?;
            }
            if value.data_type() != &DataType::Integer
                || is_called
                    .as_deref()
                    .is_some_and(|argument| argument.data_type() != &DataType::Boolean)
            {
                return Err(corrupt("stored setval has invalid argument types"));
            }
            let expected_nullable =
                value.nullable() || is_called.as_deref().is_some_and(StoredExpr::nullable);
            require_declared_metadata(data_type, *nullable, &DataType::Integer, expected_nullable)?;
        }
        StoredExpr::IsNull {
            expr,
            data_type,
            nullable,
        }
        | StoredExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => {
            validate_node_shape(expr)?;
            require_declared_metadata(data_type, *nullable, &DataType::Boolean, false)?;
        }
        StoredExpr::InList {
            expr,
            list,
            data_type,
            nullable,
            ..
        } => {
            validate_node_shape(expr)?;
            validate_list(list)?;
            if list.is_empty() || list.iter().any(|item| item.data_type() != expr.data_type()) {
                return Err(corrupt("stored IN list has invalid item types"));
            }
            let expected_nullable = expr.nullable() || list.iter().any(StoredExpr::nullable);
            require_declared_metadata(data_type, *nullable, &DataType::Boolean, expected_nullable)?;
        }
        StoredExpr::Between {
            expr,
            low,
            high,
            data_type,
            nullable,
            ..
        } => {
            validate_node_shape(expr)?;
            validate_node_shape(low)?;
            validate_node_shape(high)?;
            if low.data_type() != expr.data_type() || high.data_type() != expr.data_type() {
                return Err(corrupt("stored BETWEEN has invalid operand types"));
            }
            require_declared_metadata(
                data_type,
                *nullable,
                &DataType::Boolean,
                expr.nullable() || low.nullable() || high.nullable(),
            )?;
        }
        StoredExpr::Like {
            expr,
            pattern,
            data_type,
            nullable,
            ..
        } => {
            validate_node_shape(expr)?;
            validate_node_shape(pattern)?;
            if expr.data_type() != &DataType::Text || pattern.data_type() != &DataType::Text {
                return Err(corrupt("stored LIKE has invalid operand types"));
            }
            require_declared_metadata(
                data_type,
                *nullable,
                &DataType::Boolean,
                expr.nullable() || pattern.nullable(),
            )?;
        }
        StoredExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable: _,
        } => {
            require_list_limit(when_clauses.len())?;
            if when_clauses.is_empty() {
                return Err(corrupt("stored CASE has no WHEN clauses"));
            }
            if let Some(operand) = operand {
                validate_node_shape(operand)?;
            }
            for (when, then) in when_clauses {
                validate_node_shape(when)?;
                validate_node_shape(then)?;
                let when_matches = if let Some(operand) = operand {
                    when.data_type() == operand.data_type()
                } else {
                    when.data_type() == &DataType::Boolean
                };
                if !when_matches || then.data_type() != data_type {
                    return Err(corrupt("stored CASE has invalid branch types"));
                }
            }
            if let Some(else_clause) = else_clause {
                validate_node_shape(else_clause)?;
                if else_clause.data_type() != data_type {
                    return Err(corrupt("stored CASE has invalid ELSE type"));
                }
            }
            // The bound nullability is authoritative. Branch nullability alone
            // cannot reconstruct flow-sensitive guarantees such as COALESCE's
            // `WHEN arg IS NOT NULL THEN arg` correlation.
        }
        StoredExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => {
            validate_node_shape(expr)?;
            if pg_type.data_type() != *data_type || *nullable != expr.nullable() {
                return Err(corrupt("stored cast has invalid result metadata"));
            }
        }
    }
    Ok(())
}

fn validate_list(expressions: &[StoredExpr]) -> crate::Result<()> {
    require_list_limit(expressions.len())?;
    for expression in expressions {
        validate_node_shape(expression)?;
    }
    Ok(())
}

fn require_list_limit(len: usize) -> crate::Result<()> {
    if len > MAX_STORED_EXPRESSION_LIST_ITEMS {
        Err(corrupt("stored expression list exceeds durable size limit"))
    } else {
        Ok(())
    }
}

fn require_metadata(expr: &StoredExpr, data_type: &DataType, nullable: bool) -> crate::Result<()> {
    require_declared_metadata(expr.data_type(), expr.nullable(), data_type, nullable)
}

fn require_declared_metadata(
    actual_type: &DataType,
    actual_nullable: bool,
    expected_type: &DataType,
    expected_nullable: bool,
) -> crate::Result<()> {
    if actual_type != expected_type || actual_nullable != expected_nullable {
        return Err(corrupt("stored expression has inconsistent type metadata"));
    }
    Ok(())
}

fn binary_result_type(left: &DataType, op: StoredBinOp, right: &DataType) -> Option<DataType> {
    match op {
        StoredBinOp::Add
        | StoredBinOp::Sub
        | StoredBinOp::Mul
        | StoredBinOp::Div
        | StoredBinOp::Mod => {
            if let Some(result) = interval_arithmetic_result(left, op, right) {
                return Some(result);
            }
            let family = numeric_family(left)?;
            if numeric_family(right) != Some(family)
                || matches!(op, StoredBinOp::Mod) && matches!(family, 1 | 3)
            {
                return None;
            }
            Some(match family {
                1 => DataType::Double,
                2 => DataType::Numeric {
                    precision: None,
                    scale: 0,
                },
                3 => DataType::Real,
                _ => DataType::Integer,
            })
        }
        StoredBinOp::Eq
        | StoredBinOp::Neq
        | StoredBinOp::Lt
        | StoredBinOp::LtEq
        | StoredBinOp::Gt
        | StoredBinOp::GtEq
        | StoredBinOp::IsDistinctFrom
        | StoredBinOp::IsNotDistinctFrom
            if left == right =>
        {
            Some(DataType::Boolean)
        }
        StoredBinOp::And | StoredBinOp::Or
            if left == &DataType::Boolean && right == &DataType::Boolean =>
        {
            Some(DataType::Boolean)
        }
        StoredBinOp::Concat if left == &DataType::Text && right == &DataType::Text => {
            Some(DataType::Text)
        }
        _ => None,
    }
}

fn numeric_family(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Integer => Some(0),
        DataType::Double => Some(1),
        DataType::Numeric { .. } => Some(2),
        DataType::Real => Some(3),
        _ => None,
    }
}

fn interval_arithmetic_result(
    left: &DataType,
    op: StoredBinOp,
    right: &DataType,
) -> Option<DataType> {
    use DataType::{Date, Integer, Interval, Time, Timestamp, TimestampTz};
    match (left, op, right) {
        (Interval, StoredBinOp::Add | StoredBinOp::Sub, Interval) => Some(Interval),
        (Interval, StoredBinOp::Mul, Integer) | (Integer, StoredBinOp::Mul, Interval) => {
            Some(Interval)
        }
        (Date, StoredBinOp::Add | StoredBinOp::Sub, Interval)
        | (Interval, StoredBinOp::Add, Date) => Some(Timestamp),
        (Timestamp, StoredBinOp::Add | StoredBinOp::Sub, Interval)
        | (Interval, StoredBinOp::Add, Timestamp) => Some(Timestamp),
        (TimestampTz, StoredBinOp::Add | StoredBinOp::Sub, Interval)
        | (Interval, StoredBinOp::Add, TimestampTz) => Some(TimestampTz),
        (Time, StoredBinOp::Add | StoredBinOp::Sub, Interval)
        | (Interval, StoredBinOp::Add, Time) => Some(Time),
        _ => None,
    }
}

fn is_comparison(op: StoredBinOp) -> bool {
    matches!(
        op,
        StoredBinOp::Eq
            | StoredBinOp::Neq
            | StoredBinOp::Lt
            | StoredBinOp::LtEq
            | StoredBinOp::Gt
            | StoredBinOp::GtEq
    )
}

fn array_shape_matches(dimensions: &[u32], element_count: usize) -> crate::Result<bool> {
    if dimensions.is_empty() {
        return Ok(element_count == 0);
    }
    let mut cardinality = 1usize;
    for dimension in dimensions {
        if *dimension == 0 {
            return Ok(false);
        }
        let dimension = usize::try_from(*dimension)
            .map_err(|_| corrupt("stored array dimension does not fit usize"))?;
        cardinality = cardinality
            .checked_mul(dimension)
            .ok_or_else(|| corrupt("stored array cardinality overflow"))?;
    }
    Ok(cardinality == element_count)
}

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::internal(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer_literal() -> StoredExpr {
        StoredExpr::Literal {
            value: Value::Integer(1),
            data_type: DataType::Integer,
            nullable: false,
        }
    }

    #[test]
    fn durable_decode_rejects_oversized_node_lists() {
        let expression = StoredExpr::Function {
            function: 1,
            args: vec![integer_literal(); MAX_STORED_EXPRESSION_LIST_ITEMS + 1],
            data_type: DataType::Integer,
            pg_type: None,
            nullable: false,
        };
        let bytes = serde_json::to_vec(&expression).unwrap();
        let error = serde_json::from_slice::<StoredExpr>(&bytes).unwrap_err();
        assert!(error.to_string().contains("list exceeds limit"));
    }

    #[test]
    fn durable_decode_rejects_oversized_canonical_sql() {
        let expression = StoredExpression {
            version: STORED_EXPRESSION_VERSION,
            sql: "x".repeat(MAX_STORED_EXPRESSION_SQL_BYTES + 1),
            root: integer_literal(),
            data_type: DataType::Integer,
            pg_type: None,
            nullable: false,
        };
        let bytes = serde_json::to_vec(&expression).unwrap();
        let error = serde_json::from_slice::<StoredExpression>(&bytes).unwrap_err();
        assert!(error.to_string().contains("SQL exceeds limit"));
    }

    #[test]
    fn shape_validation_rejects_forged_literal_type_metadata() {
        let expression = StoredExpression {
            version: STORED_EXPRESSION_VERSION,
            sql: "1".to_string(),
            root: StoredExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Boolean,
                nullable: false,
            },
            data_type: DataType::Boolean,
            pg_type: None,
            nullable: false,
        };

        let error = validate_stored_expression_shape(&expression).unwrap_err();
        assert!(error.message.contains("literal"));
    }

    #[test]
    fn shape_validation_rejects_non_finite_literals() {
        let expression = StoredExpression {
            version: STORED_EXPRESSION_VERSION,
            sql: "1e400".to_string(),
            root: StoredExpr::Literal {
                value: Value::Float(crate::OrderedF64::from(f64::INFINITY)),
                data_type: DataType::Double,
                nullable: false,
            },
            data_type: DataType::Double,
            pg_type: None,
            nullable: false,
        };

        assert!(validate_stored_expression_shape(&expression).is_err());
    }

    #[test]
    fn sequence_reference_search_traverses_nested_expressions() {
        let expression = StoredExpr::Binary {
            left: Box::new(integer_literal()),
            op: StoredBinOp::Add,
            right: Box::new(StoredExpr::Function {
                function: 1,
                args: vec![StoredExpr::Nextval {
                    sequence: 42,
                    data_type: DataType::Integer,
                    nullable: false,
                }],
                data_type: DataType::Integer,
                pg_type: None,
                nullable: false,
            }),
            data_type: DataType::Integer,
            nullable: false,
        };

        assert!(expression.references_sequence(42));
        assert!(!expression.references_sequence(41));
    }
}
