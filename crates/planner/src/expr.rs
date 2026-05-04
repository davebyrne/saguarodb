use common::{BindingId, ColumnId, DataType, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundExpr {
    Literal {
        value: Value,
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
            | BoundExpr::InputRef { data_type, .. }
            | BoundExpr::BinaryOp { data_type, .. }
            | BoundExpr::UnaryOp { data_type, .. }
            | BoundExpr::Function { data_type, .. }
            | BoundExpr::AggregateCall { data_type, .. }
            | BoundExpr::LocalRef { data_type, .. }
            | BoundExpr::IsNull { data_type, .. }
            | BoundExpr::IsNotNull { data_type, .. }
            | BoundExpr::InList { data_type, .. }
            | BoundExpr::Between { data_type, .. }
            | BoundExpr::Like { data_type, .. }
            | BoundExpr::Case { data_type, .. }
            | BoundExpr::Cast { data_type, .. } => data_type.clone(),
        }
    }

    pub(crate) fn nullable(&self) -> bool {
        match self {
            BoundExpr::Literal { nullable, .. }
            | BoundExpr::InputRef { nullable, .. }
            | BoundExpr::BinaryOp { nullable, .. }
            | BoundExpr::UnaryOp { nullable, .. }
            | BoundExpr::Function { nullable, .. }
            | BoundExpr::AggregateCall { nullable, .. }
            | BoundExpr::LocalRef { nullable, .. }
            | BoundExpr::IsNull { nullable, .. }
            | BoundExpr::IsNotNull { nullable, .. }
            | BoundExpr::InList { nullable, .. }
            | BoundExpr::Between { nullable, .. }
            | BoundExpr::Like { nullable, .. }
            | BoundExpr::Case { nullable, .. }
            | BoundExpr::Cast { nullable, .. } => *nullable,
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
