use common::{
    CopyDirection, CopyOptions, DataType, IsolationLevel, ParsedColumnDef, SequenceOptions, Value,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        /// Column name lists for `UNIQUE` constraints (column-level `UNIQUE` and
        /// table-level `UNIQUE (a, b)`). Each becomes a unique index at create
        /// time. Empty when the table has no unique constraints.
        unique: Vec<Vec<String>>,
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
    CreateSequence {
        name: String,
        options: SequenceOptions,
    },
    DropSequence {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        source: InsertSource,
        /// `INSERT ... ON CONFLICT ...`. `None` when absent. The arbiter is the
        /// primary key (validated by the binder); see [`OnConflict`].
        on_conflict: Option<OnConflict>,
        /// `INSERT ... RETURNING <items>`. `None` when no `RETURNING` clause is
        /// present; `Some(items)` carries the projection list (expressions, `*`,
        /// or `table.*`) evaluated over each inserted (or upserted) row.
        returning: Option<Vec<SelectItem>>,
    },
    Query(Query),
    Update {
        table: String,
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
        /// `UPDATE ... RETURNING <items>`, evaluated over each updated (new) row.
        returning: Option<Vec<SelectItem>>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
        /// `DELETE ... RETURNING <items>`, evaluated over each deleted (old) row.
        returning: Option<Vec<SelectItem>>,
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
    /// `ROLLBACK` (without a savepoint).
    Rollback,
    /// `SAVEPOINT <name>` — establish a savepoint (open a subtransaction). `name`
    /// is the lowercase-normalized identifier. See `docs/specs/savepoints.md`.
    Savepoint {
        name: String,
    },
    /// `RELEASE [SAVEPOINT] <name>` — release (merge) a savepoint into its parent.
    ReleaseSavepoint {
        name: String,
    },
    /// `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] <name>` — roll back to a
    /// savepoint, which remains active for continued work.
    RollbackToSavepoint {
        name: String,
    },
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
    Query(Box<Query>),
}

/// `ON CONFLICT [target] DO NOTHING | DO UPDATE SET ... [WHERE ...]`. `target`
/// names the arbiter columns (only the primary key is supported, validated by the
/// binder); `None` is allowed for `DO NOTHING` (any conflict) but the binder still
/// treats the primary key as the arbiter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnConflict {
    pub target: Option<ConflictTarget>,
    pub action: ConflictAction,
}

/// The conflict arbiter. Only an explicit column list is parsed (`ON CONSTRAINT`
/// is rejected); the binder requires it to be the primary-key column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConflictTarget {
    Columns(Vec<String>),
}

/// The action taken on a conflict. `DoUpdate` assignments and `WHERE` may
/// reference the special `excluded` pseudo-table (the row proposed for insertion).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

/// A complete query expression: a query body plus the query-level modifiers that
/// apply to its whole result. In the SQL grammar `ORDER BY`/`LIMIT`/`OFFSET` sit
/// outside the body (and a future `WITH` would too), so when the body becomes a
/// set operation (`UNION`/`INTERSECT`/`EXCEPT`) they order and limit the combined
/// result rather than a single `SELECT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    pub body: QueryBody,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// The body of a query expression. Only a single `SELECT` is supported today;
/// set operations and standalone `VALUES` attach here as new variants without
/// disturbing the [`Query`] wrapper or the conversion/binding/planning pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryBody {
    Select(Select),
}

/// A single `SELECT` block, without the query-level `ORDER BY`/`LIMIT`/`OFFSET`
/// (which live on the enclosing [`Query`]). `from` may be empty — a FROM-less
/// scalar projection such as `SELECT 1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Select {
    pub distinct: Option<Distinct>,
    pub columns: Vec<SelectItem>,
    pub from: Vec<FromItem>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
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
    /// A derived table: `(SELECT ...) AS alias [(col, ...)]`. A subquery in the
    /// FROM clause. The alias is required; `column_aliases` optionally renames the
    /// subquery's output columns left to right.
    Derived {
        subquery: Box<Query>,
        alias: String,
        column_aliases: Vec<String>,
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
    /// A scalar subquery `(SELECT ...)` used as an expression. It must produce a
    /// single column and at most one row; an empty result is `NULL`. The binder
    /// validates the single-column shape; the one-row cardinality is enforced at
    /// run time.
    Subquery(Box<Query>),
    /// `expr [NOT] IN (SELECT ...)`. The subquery must produce a single column;
    /// `negated` is `true` for `NOT IN`. Three-valued-logic NULL semantics apply.
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `[NOT] EXISTS (SELECT ...)`. `negated` is `true` for `NOT EXISTS`. The
    /// subquery's projected columns are ignored — only whether it produces a row.
    Exists {
        subquery: Box<Query>,
        negated: bool,
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
        /// `ILIKE` (case-insensitive) when true; plain `LIKE` when false.
        case_insensitive: bool,
        /// The pattern escape character. Defaults to `Some('\\')`; an explicit
        /// `ESCAPE ''` disables escaping (`None`).
        escape: Option<char>,
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
    /// `a IS DISTINCT FROM b` — NULL-safe inequality (never returns NULL).
    IsDistinctFrom,
    /// `a IS NOT DISTINCT FROM b` — NULL-safe equality (never returns NULL).
    IsNotDistinctFrom,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}
