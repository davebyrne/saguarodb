use common::{DataType, IsolationLevel, ParsedColumnDef, Value};

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
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
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
    /// `BEGIN [TRANSACTION] [ISOLATION LEVEL <level>] [READ WRITE]` /
    /// `START TRANSACTION ...`. `isolation` carries an explicit
    /// `ISOLATION LEVEL <level>` mode mapped onto our two levels (the four SQL
    /// levels collapse to Read Committed / Repeatable Read; see `convert.rs`),
    /// or `None` to use the transaction default. Access mode `READ WRITE` is
    /// accepted and ignored; `READ ONLY`, MySQL-style modifiers, `AND CHAIN`,
    /// atomic-block bodies, and savepoints are rejected at parse time
    /// (`docs/specs/mvcc.md` §10 Milestone G).
    Begin {
        isolation: Option<IsolationLevel>,
    },
    /// `COMMIT` / `END`.
    Commit,
    /// `ROLLBACK`. Savepoints are not supported in v1.
    Rollback,
    /// `SET TRANSACTION ISOLATION LEVEL <level>` (transaction-scoped). Sets the
    /// CURRENT transaction's isolation level; valid only before the transaction
    /// has run its first query (`docs/specs/mvcc.md` §10 Milestone G). `isolation`
    /// is `None` for a `SET TRANSACTION` with no isolation-level mode (e.g.
    /// `READ WRITE` only). `SET SESSION CHARACTERISTICS` (session default) is a
    /// later milestone and is rejected here.
    SetTransaction {
        isolation: Option<IsolationLevel>,
    },
    /// `VACUUM` (all user tables) or `VACUUM <table>` (one table). A maintenance
    /// command that reclaims dead MVCC versions; `table` is the lowercase-normalized
    /// identifier, `None` for the whole database. sqlparser 0.56 does not parse
    /// `VACUUM`, so it is intercepted in `parse_statement` before sqlparser runs.
    Vacuum {
        table: Option<String>,
    },
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
    /// Extended-protocol parameter placeholder `$n` (1-based as written).
    Placeholder(u32),
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
