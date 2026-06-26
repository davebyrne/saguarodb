use common::{CopyDirection, CopyOptions, DataType, IsolationLevel, ParsedColumnDef, Value};

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
    /// has run its first query (`docs/specs/mvcc.md` §10 Milestone G1). `isolation`
    /// is `None` for a `SET TRANSACTION` with no isolation-level mode (e.g.
    /// `READ WRITE` only).
    SetTransaction {
        isolation: Option<IsolationLevel>,
    },
    /// `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`
    /// (session-scoped default). Sets the per-connection default isolation used by
    /// FUTURE transactions; it does not change an already-open transaction
    /// (`docs/specs/mvcc.md` §10 Milestone G2). `isolation` carries the mapped level
    /// (the same four-to-two mapping as `Begin`/`SetTransaction`), or `None` for a
    /// `SET SESSION CHARACTERISTICS` with no isolation-level mode (e.g. `READ WRITE`
    /// only), which the server treats as a no-op success.
    SetSessionCharacteristics {
        isolation: Option<IsolationLevel>,
    },
    /// `VACUUM` (all user tables) or `VACUUM <table>` (one table). A maintenance
    /// command that reclaims dead MVCC versions; `table` is the lowercase-normalized
    /// identifier, `None` for the whole database. sqlparser 0.56 does not parse
    /// `VACUUM`, so it is intercepted in `parse_statement` before sqlparser runs.
    Vacuum {
        table: Option<String>,
    },
    /// `COPY <table> [(cols)] FROM STDIN | TO STDOUT [WITH (...)]`. A
    /// non-relational bulk-transfer command (text/CSV, simple-query only). The
    /// parser rejects server-side files, `COPY (query)`, binary format, and
    /// unsupported options; `options` is the normalized result. `columns` empty
    /// means all table columns in catalog order. See `docs/specs/copy.md`.
    Copy {
        table: String,
        columns: Vec<String>,
        direction: CopyDirection,
        options: CopyOptions,
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
    pub distinct: Option<Distinct>,
    pub columns: Vec<SelectItem>,
    pub from: Vec<FromItem>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// The `DISTINCT` modifier on a `SELECT`. `All` is plain `SELECT DISTINCT`
/// (de-duplicate whole output rows); `On` is the PostgreSQL `SELECT DISTINCT ON
/// (expr, ...)` extension (keep the first row per key expression list).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Distinct {
    All,
    On(Vec<Expr>),
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
