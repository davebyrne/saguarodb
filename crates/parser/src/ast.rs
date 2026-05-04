use common::{DataType, ParsedColumnDef, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        source: InsertSource,
    },
    Select(SelectStatement),
    Update {
        table: String,
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    Explain(Box<Statement>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(Box<SelectStatement>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectStatement {
    pub columns: Vec<SelectItem>,
    pub from: Vec<FromItem>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectItem {
    Wildcard,
    QualifiedWildcard(String),
    Expression { expr: Expr, alias: Option<String> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        condition: Option<Expr>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    Literal(Value),
    ColumnRef {
        table: Option<String>,
        column: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<FunctionArg>,
        distinct: bool,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FunctionArg {
    Expr(Expr),
    Wildcard,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}
