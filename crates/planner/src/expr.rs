use common::{BindingId, ColumnId, DataType, SequenceId, Value};

use crate::BoundSelect;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundExpr {
    Literal {
        value: Value,
        data_type: DataType,
        nullable: bool,
    },
    /// Extended-protocol parameter slot (`$n`), `index` is 0-based. Resolved to a
    /// `Literal` by parameter substitution before execution.
    Parameter {
        index: usize,
        data_type: DataType,
        nullable: bool,
    },
    InputRef {
        input: BindingId,
        column: ColumnId,
        slot: usize,
        data_type: DataType,
        nullable: bool,
    },
    BinaryOp {
        left: Box<BoundExpr>,
        op: BinOp,
        right: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    Function {
        name: String,
        args: Vec<BoundExpr>,
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
        value: Box<BoundExpr>,
        is_called: Option<Box<BoundExpr>>,
        data_type: DataType,
        nullable: bool,
    },
    AggregateCall {
        func: AggregateFunc,
        arg: Option<Box<BoundExpr>>,
        distinct: bool,
        data_type: DataType,
        nullable: bool,
    },
    LocalRef {
        slot: usize,
        data_type: DataType,
        nullable: bool,
    },
    IsNull {
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    IsNotNull {
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    InList {
        expr: Box<BoundExpr>,
        list: Vec<BoundExpr>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Between {
        expr: Box<BoundExpr>,
        low: Box<BoundExpr>,
        high: Box<BoundExpr>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    Like {
        expr: Box<BoundExpr>,
        pattern: Box<BoundExpr>,
        negated: bool,
        /// `ILIKE` (case-insensitive) when true; plain `LIKE` when false.
        case_insensitive: bool,
        /// Pattern escape character; `None` disables escaping (`ESCAPE ''`).
        escape: Option<char>,
        data_type: DataType,
        nullable: bool,
    },
    Case {
        operand: Option<Box<BoundExpr>>,
        when_clauses: Vec<(BoundExpr, BoundExpr)>,
        else_clause: Option<Box<BoundExpr>>,
        data_type: DataType,
        nullable: bool,
    },
    Cast {
        expr: Box<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    /// A scalar subquery `(SELECT ...)`: a single-column, at-most-one-row SELECT
    /// used as a value. The sub-plan is carried as a bound SELECT (unchanged
    /// through logical and physical planning) and evaluated by the executor —
    /// an empty result yields `NULL`, more than one row is a runtime error.
    /// Uncorrelated: the inner SELECT is its own binding scope.
    ScalarSubquery {
        select: Box<BoundSelect>,
        data_type: DataType,
        nullable: bool,
    },
    /// `[NOT] EXISTS (SELECT ...)`. Yields a non-null boolean: whether the
    /// sub-plan produces at least one row (negated for `NOT EXISTS`).
    Exists {
        select: Box<BoundSelect>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    /// `expr [NOT] IN (SELECT ...)` over a single-column sub-plan. The executor
    /// materializes the column and applies SQL `IN`/`NOT IN` three-valued logic.
    InSubquery {
        expr: Box<BoundExpr>,
        select: Box<BoundSelect>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
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
    /// `a IS DISTINCT FROM b` — NULL-safe inequality; result is always
    /// `Boolean` (never NULL).
    IsDistinctFrom,
    /// `a IS NOT DISTINCT FROM b` — NULL-safe equality; result is always
    /// `Boolean` (never NULL).
    IsNotDistinctFrom,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub arg: Option<BoundExpr>,
    pub distinct: bool,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// Sample standard deviation (`STDDEV`/`STDDEV_SAMP`), divisor `n - 1`.
    StddevSamp,
    /// Population standard deviation (`STDDEV_POP`), divisor `n`.
    StddevPop,
    /// Sample variance (`VARIANCE`/`VAR_SAMP`), divisor `n - 1`.
    VarSamp,
    /// Population variance (`VAR_POP`), divisor `n`.
    VarPop,
    /// `BOOL_AND` — true when every non-NULL input is true.
    BoolAnd,
    /// `BOOL_OR` — true when any non-NULL input is true.
    BoolOr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundOrderByItem {
    pub expr: BoundExpr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

impl BoundExpr {
    pub(crate) fn data_type(&self) -> DataType {
        match self {
            BoundExpr::Literal { data_type, .. }
            | BoundExpr::Parameter { data_type, .. }
            | BoundExpr::InputRef { data_type, .. }
            | BoundExpr::BinaryOp { data_type, .. }
            | BoundExpr::UnaryOp { data_type, .. }
            | BoundExpr::Function { data_type, .. }
            | BoundExpr::Nextval { data_type, .. }
            | BoundExpr::Currval { data_type, .. }
            | BoundExpr::Setval { data_type, .. }
            | BoundExpr::AggregateCall { data_type, .. }
            | BoundExpr::LocalRef { data_type, .. }
            | BoundExpr::IsNull { data_type, .. }
            | BoundExpr::IsNotNull { data_type, .. }
            | BoundExpr::InList { data_type, .. }
            | BoundExpr::Between { data_type, .. }
            | BoundExpr::Like { data_type, .. }
            | BoundExpr::Case { data_type, .. }
            | BoundExpr::Cast { data_type, .. }
            | BoundExpr::ScalarSubquery { data_type, .. }
            | BoundExpr::Exists { data_type, .. }
            | BoundExpr::InSubquery { data_type, .. } => data_type.clone(),
        }
    }

    pub(crate) fn nullable(&self) -> bool {
        match self {
            BoundExpr::Literal { nullable, .. }
            | BoundExpr::Parameter { nullable, .. }
            | BoundExpr::InputRef { nullable, .. }
            | BoundExpr::BinaryOp { nullable, .. }
            | BoundExpr::UnaryOp { nullable, .. }
            | BoundExpr::Function { nullable, .. }
            | BoundExpr::Nextval { nullable, .. }
            | BoundExpr::Currval { nullable, .. }
            | BoundExpr::Setval { nullable, .. }
            | BoundExpr::AggregateCall { nullable, .. }
            | BoundExpr::LocalRef { nullable, .. }
            | BoundExpr::IsNull { nullable, .. }
            | BoundExpr::IsNotNull { nullable, .. }
            | BoundExpr::InList { nullable, .. }
            | BoundExpr::Between { nullable, .. }
            | BoundExpr::Like { nullable, .. }
            | BoundExpr::Case { nullable, .. }
            | BoundExpr::Cast { nullable, .. }
            | BoundExpr::ScalarSubquery { nullable, .. }
            | BoundExpr::Exists { nullable, .. }
            | BoundExpr::InSubquery { nullable, .. } => *nullable,
        }
    }

    pub(crate) fn is_null_literal(&self) -> bool {
        matches!(
            self,
            BoundExpr::Literal {
                value: Value::Null,
                ..
            }
        )
    }
}
