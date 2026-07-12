use common::{
    CompressionSetting, CopyDirection, CopyOptions, DataType, IsolationLevel, ParsedColumnDef,
    PgType, SequenceOptions, TableOptionPatch, ToastOptionPatch, Value,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetScope {
    Session,
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Statement {
    CreateTable {
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        /// Column name lists for `UNIQUE` constraints (column-level `UNIQUE` and
        /// table-level `UNIQUE (a, b)`). Each becomes a unique index at create
        /// time. Empty when the table has no unique constraints.
        unique: Vec<Vec<String>>,
        /// `WITH (compression = ...)`. `None` when the clause is absent (the
        /// binder defaults an absent clause to [`CompressionSetting::None`]).
        compression: Option<CompressionSetting>,
        /// `WITH (toast..., toast_compression...)` storage options. Empty when
        /// omitted; the binder merges this patch with new-table defaults.
        toast: ToastOptionPatch,
        /// `CHECK (...)` constraint expressions as canonical SQL text. Column-level
        /// (`n INT CHECK (n > 0)`) and table-level (`CHECK (a < b)`) checks are
        /// flattened here in declaration order. The binder re-parses and binds each
        /// against the table's columns. Empty when the table has no checks; a named
        /// check (`CONSTRAINT c CHECK (...)`) is rejected at parse time.
        checks: Vec<String>,
    },
    DropTable {
        names: Vec<String>,
        if_exists: bool,
    },
    AlterTableAddColumn {
        table: String,
        if_not_exists: bool,
        column: ParsedColumnDef,
    },
    AlterTableDropColumn {
        table: String,
        if_exists: bool,
        column: String,
    },
    AlterTableRenameColumn {
        table: String,
        old_name: String,
        new_name: String,
    },
    AlterTableRenameTable {
        table: String,
        new_name: String,
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
    CreateView {
        name: String,
        or_replace: bool,
        columns: Vec<String>,
        query: Query,
        /// Canonical SQL text for the stored view query. The catalog will persist
        /// this text and re-parse it when expanding the view.
        definition: String,
    },
    DropView {
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
        /// `UPDATE ... FROM <items>`: extra relations joined with the target
        /// (`docs/specs/subqueries.md` §8). Empty when absent.
        from: Vec<FromItem>,
        filter: Option<Expr>,
        /// `UPDATE ... RETURNING <items>`, evaluated over each updated (new) row.
        returning: Option<Vec<SelectItem>>,
    },
    Delete {
        table: String,
        /// `DELETE ... USING <items>` (`docs/specs/subqueries.md` §8).
        using: Vec<FromItem>,
        filter: Option<Expr>,
        /// `DELETE ... RETURNING <items>`, evaluated over each deleted (old) row.
        returning: Option<Vec<SelectItem>>,
    },
    /// `DECLARE <name> CURSOR FOR <query>`. SQL cursors are server-driven and
    /// transaction-scoped; the server binds the query when opening the cursor.
    DeclareCursor {
        name: String,
        query: Query,
    },
    /// `FETCH ... FROM <name>`. Only forward-only cursor fetches are supported.
    FetchCursor {
        name: String,
        count: FetchCount,
    },
    /// `CLOSE <name>`.
    CloseCursor {
        name: String,
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
    /// `SET [SESSION|LOCAL] <name> {=|TO} <value>` — session configuration
    /// assignment. `SET TIME ZONE <value>` normalizes to `timezone`, and `SET NAMES
    /// <encoding>` normalizes to `client_encoding`. Ordinary stored GUCs still treat
    /// `LOCAL` as session-scoped for driver compatibility; PostgreSQL-special
    /// parameters such as `default_transaction_isolation` can inspect `scope` for
    /// exact behavior. GUC names are the narrow exception to the quoted-identifier
    /// rule: quoted GUC name parts are accepted and lowercase-normalized like
    /// PostgreSQL.
    SetVariable {
        scope: SetScope,
        name: String,
        value: String,
    },
    /// `RESET <name>` / `RESET ALL`. `None` means `ALL`. sqlparser 0.56 does not
    /// parse RESET, so the converter intercepts it before the general parser.
    ResetVariable {
        name: Option<String>,
    },
    /// `SHOW <name>` / `SHOW ALL`. `None` means `ALL`. Multi-word PostgreSQL
    /// aliases such as `SHOW TIME ZONE` and `SHOW TRANSACTION ISOLATION LEVEL`
    /// normalize to their GUC names.
    ShowVariable {
        name: Option<String>,
    },
    /// `DISCARD ALL` — reset session configuration and other per-session state.
    DiscardAll,
    /// `VACUUM [ANALYZE]` (all user tables) or `VACUUM [ANALYZE] <table>` (one
    /// table). A maintenance command that reclaims dead MVCC versions; with
    /// `analyze` it then collects optimizer statistics over the same targets
    /// (`docs/specs/statistics.md` §7). `table` is the lowercase-normalized
    /// identifier, `None` for the whole database. sqlparser 0.56 does not parse
    /// `VACUUM`, so it is intercepted in `parse_statement` before sqlparser runs.
    Vacuum {
        table: Option<String>,
        analyze: bool,
    },
    /// `ANALYZE` (all user tables) or `ANALYZE <table>` (one table) — collect
    /// optimizer statistics (`docs/specs/statistics.md` §5). A maintenance
    /// command that does not bind/plan relationally; `table` is the
    /// lowercase-normalized identifier, `None` for the whole database.
    /// Intercepted in `parse_statement` like `VACUUM` (statement-initial
    /// keyword only, so `EXPLAIN ANALYZE` still reaches — and is rejected by —
    /// the EXPLAIN path).
    Analyze {
        table: Option<String>,
    },
    /// `TRUNCATE [TABLE] <name> [, ...]` — immediate table truncation. A
    /// maintenance command that does not bind/plan relationally. Targets retain
    /// input order; duplicate normalized names and unsupported PostgreSQL options
    /// such as ONLY, identity handling, and cascade are rejected by the parser.
    Truncate {
        tables: Vec<String>,
    },
    /// `ALTER TABLE <name> SET (compression = 'none' | 'zstd')`. Intercepted
    /// before sqlparser because sqlparser does not parse PostgreSQL storage
    /// parameters consistently; schema-evolution `ALTER TABLE` forms are parsed
    /// through sqlparser.
    AlterTableSetCompression {
        table: String,
        compression: CompressionSetting,
    },
    /// `ALTER TABLE <name> SET (...)` with at least one TOAST option. Parsed as
    /// maintenance; execution applies the future-write-only TOAST metadata change.
    AlterTableSetOptions {
        table: String,
        options: TableOptionPatch,
    },
    /// `ALTER TABLE [ONLY] <name> ADD [CONSTRAINT <name>] PRIMARY KEY (cols...)`.
    AlterTableAddPrimaryKey {
        table: String,
        columns: Vec<String>,
        constraint_name: Option<String>,
    },
    /// `ALTER TABLE [ONLY] <name> DROP PRIMARY KEY` or
    /// `ALTER TABLE [ONLY] <name> DROP CONSTRAINT <name>`.
    AlterTableDropPrimaryKey {
        table: String,
        constraint_name: Option<String>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchCount {
    One,
    Count(u64),
    All,
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

/// A complete query expression: an optional `WITH` clause, a query body, and the
/// query-level modifiers that apply to its whole result. In the SQL grammar
/// `WITH` and `ORDER BY`/`LIMIT`/`OFFSET` sit outside the body, so when the body
/// is a set operation they order and limit the combined result, and the `WITH`
/// CTEs are visible to the whole body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    /// `WITH` common table expressions (non-recursive), in definition order. Empty
    /// when there is no `WITH` clause. Each CTE is visible to the body and to later
    /// CTEs; a reference is bound by inlining the CTE as a named derived table.
    pub with: Vec<Cte>,
    pub body: QueryBody,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// A common table expression: `name [(col, ...)] AS (query)`. `column_aliases`
/// optionally renames the CTE's output columns left to right.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cte {
    pub name: String,
    pub column_aliases: Vec<String>,
    pub query: Box<Query>,
}

/// The body of a query expression: a single `SELECT`, a `VALUES` list, or a set
/// operation combining two query expressions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryBody {
    Select(Select),
    /// A `VALUES` list used as a query body (`VALUES (1,'a'), (2,'b')`): a literal
    /// row set. Each inner `Vec` is one row; all rows must have the same width.
    /// Output columns are unnamed (`column1`, `column2`, ...).
    Values(Vec<Vec<Expr>>),
    /// A set operation (`a UNION b`, `a INTERSECT b`, `a EXCEPT b`). `all` is the
    /// `ALL` quantifier (keep duplicates); without it duplicate rows are removed.
    /// Each side is a full [`Query`], so a parenthesized arm may carry its own
    /// `ORDER BY`/`LIMIT`; the enclosing `Query`'s modifiers apply to the combined
    /// result.
    SetOp {
        op: SetOp,
        all: bool,
        left: Box<Query>,
        right: Box<Query>,
    },
}

/// A set operator combining two query expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
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
        schema: Option<String>,
        name: String,
        alias: Option<String>,
    },
    /// A derived table: `[LATERAL] (SELECT ...) AS alias [(col, ...)]`. A
    /// subquery in the FROM clause. The alias is required; `column_aliases`
    /// optionally renames the subquery's output columns left to right.
    /// `lateral` lets the body reference FROM items to its left
    /// (`docs/specs/subqueries.md` §7).
    Derived {
        subquery: Box<Query>,
        alias: String,
        column_aliases: Vec<String>,
        lateral: bool,
    },
    /// A set-returning function in `FROM`, such as `unnest(a)` or
    /// `generate_series(1, 10)`. Binding/execution is owned by later layers.
    TableFunction {
        name: String,
        args: Vec<Expr>,
        alias: Option<String>,
        column_aliases: Vec<String>,
        lateral: bool,
        with_ordinality: bool,
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
    /// `ARRAY[expr, ...]`; nested constructors represent multidimensional arrays.
    Array(Vec<Expr>),
    /// One or more PostgreSQL array indexes, e.g. `a[1][2]`.
    ArraySubscript {
        array: Box<Expr>,
        subscripts: Vec<Expr>,
    },
    /// `left op ANY(array)` / `SOME(array)`.
    Any {
        left: Box<Expr>,
        op: BinOp,
        array: Box<Expr>,
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
        /// The declared wire type of the cast target (e.g. `varchar` vs `text`),
        /// carried so a `CAST` output column reports the right OID/typmod.
        pg_type: PgType,
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
