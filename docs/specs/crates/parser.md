# `parser` Crate Specification

**Date:** 2026-05-03
**Status:** Draft

## Purpose

`parser` translates SQL text into SaguaroDB AST types. It wraps `sqlparser-rs` with the PostgreSQL dialect and hides the external AST from the rest of the system.

## Depends On

- `common`

## Public API

```rust
pub fn parse(sql: &str) -> Result<Statement>;
```

`parse` accepts exactly one SQL statement. If `sqlparser-rs` returns multiple statements, parser returns `SqlState::SyntaxError`.

## AST Types

```rust
pub enum Statement {
    CreateTable { name: String, columns: Vec<ParsedColumnDef>, primary_key: Vec<String> },
    DropTable { name: String },
    CreateIndex { name: String, table: String, columns: Vec<String>, unique: bool },
    DropIndex { name: String },
    Insert { table: String, columns: Vec<String>, source: InsertSource },
    Select(SelectStatement),
    Update { table: String, assignments: Vec<Assignment>, filter: Option<Expr> },
    Delete { table: String, filter: Option<Expr> },
    Explain(Box<Statement>),
    Begin,
    Commit,
    Rollback,
    // `VACUUM` (all user tables) or `VACUUM <table>` (one table). `table` is the
    // lowercase-normalized identifier, `None` for the whole database.
    Vacuum { table: Option<String> },
}

pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(Box<SelectStatement>),
}

pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

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
```

Identifiers remain strings in parser output. Name resolution is not a parser responsibility.

## Expression AST

```rust
pub enum SelectItem {
    Wildcard,
    QualifiedWildcard(String),
    Expression { expr: Expr, alias: Option<String> },
}

pub enum FromItem {
    Table { name: String, alias: Option<String> },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        condition: Option<Expr>,
    },
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

pub enum Expr {
    Literal(Value),
    Placeholder(u32), // extended-protocol parameter `$n` (1-based)
    ColumnRef { table: Option<String>, column: String },
    BinaryOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    Function { name: String, args: Vec<FunctionArg>, distinct: bool },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Cast { expr: Box<Expr>, data_type: DataType },
}

pub enum FunctionArg {
    Expr(Expr),
    Wildcard,
}

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

pub enum UnaryOp {
    Neg,
    Not,
}
```

`FromItem::Join.condition` is `None` only for `JoinType::Cross`. Inner, left, right, and full joins require `Some(condition)` from an `ON` predicate and the parser rejects those joins without `ON`. V1 rejects `USING` and `NATURAL` joins, and rejects `ON`/`USING` with `CROSS JOIN`.

Parser `BinOp` and `UnaryOp` variants use the same names as planner expression operators. Binder may copy these operators into bound expressions after type validation.

`Expr::Case.operand = None` represents searched `CASE WHEN condition THEN ...`. `operand = Some(expr)` represents simple `CASE expr WHEN value THEN ...`; binder/executor implement this by comparing the operand to each `WHEN` value with SQL equality semantics.

Function call parsing preserves aggregate syntax: `COUNT(*)` is `Function { name: "count", args: vec![FunctionArg::Wildcard], distinct: false }`, and `COUNT(DISTINCT id)` is `Function { name: "count", args: vec![FunctionArg::Expr(...)] , distinct: true }`. Binder converts `COUNT(*)` to `BoundExpr::AggregateCall { arg: None, ... }`, rejects `distinct: true` aggregate calls in v1, and rejects `FunctionArg::Wildcard` for non-`COUNT` functions or mixed with other arguments.

The dedicated `TRIM(expr)` and `SUBSTRING(expr [FROM start] [FOR length])` grammar (and the comma form `SUBSTRING(expr, start[, length])`) is normalized into ordinary `Function { name: "trim" | "substring", ... }` calls so the binder treats them uniformly. `SUBSTRING` requires a start argument; `TRIM` with `LEADING`/`TRAILING`/`BOTH` or trim characters is unsupported in v1.

## V1 SQL Scope

Parser may produce AST variants for syntax that binder rejects. V1 parser must parse:

- `CREATE TABLE` with column definitions and primary key. V1 parses both inline single-column `id INTEGER PRIMARY KEY` and table-level `PRIMARY KEY (id)` forms into `Statement::CreateTable.primary_key = vec!["id"]`; binder rejects composite primary keys in v1.
- `DROP TABLE`.
- `CREATE [UNIQUE] INDEX name ON table (col, ...)`. The index name is required (v1 does not generate one). Index columns must be plain ascending column names; expressions, operator classes, `USING <method>`, partial `WHERE`, `INCLUDE`, `NULLS [NOT] DISTINCT`, `CONCURRENTLY`, and `IF NOT EXISTS` are rejected as unsupported.
- `DROP INDEX name`.
- `INSERT INTO ... VALUES` and `INSERT INTO ... SELECT`.
- `SELECT` with projection, `FROM`, `WHERE`, inner/cross/left/right/full joins, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`.
- `UPDATE ... SET ... WHERE`.
- `DELETE FROM ... WHERE`.
- `EXPLAIN SELECT ...`. The AST node boxes any statement, but v1 only accepts a `SELECT` inner statement; any other inner statement is rejected as unsupported.
- Transaction control: `BEGIN` / `BEGIN TRANSACTION` / `START TRANSACTION` parse to `Statement::Begin`; `COMMIT` / `END` parse to `Statement::Commit`; `ROLLBACK` parses to `Statement::Rollback`. Only the plain forms are accepted: isolation-level and access modes, MySQL-style modifiers, `AND CHAIN`, atomic-block bodies, and savepoints are rejected at parse time. `ABORT` is not recognized by the dialect and is a syntax error (v1 does not add it). These statements parse to first-class variants for the MVCC effort; the multi-statement transaction lifecycle is wired in MVCC Milestone C3. Until then the server rejects them with `SqlState::FeatureNotSupported` rather than silently treating a `BEGIN` as a started transaction.

- Maintenance: `VACUUM` parses to `Statement::Vacuum { table: None }` and `VACUUM <table>` to `Statement::Vacuum { table: Some(<lowercased name>) }`. **sqlparser 0.56 cannot parse `VACUUM`** (it errors), so `parse_statement` intercepts it *before* handing the string to sqlparser: it strips an optional trailing `;`, matches the leading `vacuum` keyword case-insensitively (a glued word like `vacuumfoo` is not a VACUUM and falls through to sqlparser), and accepts at most one bare-identifier argument (lowercase-normalized, the v1 unquoted-identifier rule). Parenthesized options, multiple tables, qualified (`schema.table`) or quoted names, and Postgres option keywords (`FULL`/`FREEZE`/`ANALYZE`/`VERBOSE`/…) are rejected with `ErrorKind::Parse` / `SqlState::SyntaxError`; none are supported in v1. `VACUUM` does not bind/plan — it is a maintenance command the server dispatches separately (`docs/specs/crates/server.md`, `docs/specs/mvcc.md` §9/§10 Milestone F).

Binder rejects parsed forms that exceed the v1 semantic subset, such as composite primary keys and unknown functions.

Unquoted identifiers are normalized to lowercase before AST construction. Quoted identifiers are rejected in v1 with `ErrorKind::Parse` and `SqlState::SyntaxError`.

## Non-Responsibilities

- No catalog lookup.
- No type checking.
- No alias resolution.
- No wildcard expansion.
- No aggregate validation.
- No plan construction.

## Error Handling

- Syntax errors return `ErrorKind::Parse` and `SqlState::SyntaxError`.
- Unsupported parser-level syntax returns `ErrorKind::Parse` and `SqlState::SyntaxError`.
- Semantic errors are left for binder.

## Acceptance Tests

- Parses one valid statement for each `Statement` variant.
- Rejects multiple statements in one SQL string.
- Preserves aliases and qualified names without resolving them.
- Parses `SELECT *` and `table.*` distinctly.
- Parses `EXPLAIN SELECT ...` into `Statement::Explain`.
- Parses `INSERT ... SELECT` into `InsertSource::Query`, which the binder binds in v1.
