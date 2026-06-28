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
    // `BEGIN`/`START TRANSACTION [ISOLATION LEVEL <level>]`. `isolation` is the
    // requested level mapped onto the two we support (`None` = transaction default).
    Begin { isolation: Option<IsolationLevel> },
    Commit,
    Rollback,
    // `SET TRANSACTION ISOLATION LEVEL <level>` (transaction-scoped). `isolation` is
    // the mapped level, `None` for a `SET TRANSACTION` with no isolation-level mode.
    SetTransaction { isolation: Option<IsolationLevel> },
    // `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` (the
    // per-connection session default). `isolation` is the mapped level (same mapping
    // as `Begin`/`SetTransaction`), `None` for a `SET SESSION CHARACTERISTICS` with no
    // isolation-level mode.
    SetSessionCharacteristics { isolation: Option<IsolationLevel> },
    // `VACUUM` (all user tables) or `VACUUM <table>` (one table). `table` is the
    // lowercase-normalized identifier, `None` for the whole database.
    Vacuum { table: Option<String> },
    // `COPY <table> [(cols)] FROM STDIN | TO STDOUT [WITH (...)]`. Bulk transfer
    // (text/CSV, simple-query only); see `docs/specs/copy.md`. `columns` empty
    // means all columns in catalog order; `options` is the normalized result of
    // the modern and legacy WITH syntaxes (`common::CopyOptions`).
    Copy {
        table: String,
        columns: Vec<String>,
        direction: CopyDirection,
        options: CopyOptions,
    },
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

pub enum Distinct {
    All,             // SELECT DISTINCT
    On(Vec<Expr>),   // SELECT DISTINCT ON (expr, ...)
}
```

`distinct` records the optional `DISTINCT` modifier: `All` for plain
`SELECT DISTINCT`, `On(exprs)` for `SELECT DISTINCT ON (exprs)`. The convert
layer translates both forms; the binder binds and validates each.

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

`FromItem::Join.condition` is `None` only for `JoinType::Cross`. Inner, left, right, and full joins require `Some(condition)` from an `ON` predicate and the parser rejects those joins without `ON`. The parser rejects `USING` and `NATURAL` joins, and rejects `ON`/`USING` with `CROSS JOIN`.

Parser `BinOp` and `UnaryOp` variants use the same names as planner expression operators. Binder may copy these operators into bound expressions after type validation.

`Expr::Case.operand = None` represents searched `CASE WHEN condition THEN ...`. `operand = Some(expr)` represents simple `CASE expr WHEN value THEN ...`; binder/executor implement this by comparing the operand to each `WHEN` value with SQL equality semantics.

Function call parsing preserves aggregate syntax: `COUNT(*)` is `Function { name: "count", args: vec![FunctionArg::Wildcard], distinct: false }`, and `COUNT(DISTINCT id)` is `Function { name: "count", args: vec![FunctionArg::Expr(...)] , distinct: true }`. Binder converts `COUNT(*)` to `BoundExpr::AggregateCall { arg: None, ... }`, carries `distinct: true` through to `BoundExpr::AggregateCall { distinct: true, ... }` so the executor de-duplicates the argument (e.g. `COUNT(DISTINCT id)`), and rejects `FunctionArg::Wildcard` for non-`COUNT` functions, mixed with other arguments, or combined with `DISTINCT` (`COUNT(DISTINCT *)`).

The dedicated `TRIM(expr)` and `SUBSTRING(expr [FROM start] [FOR length])` grammar (and the comma form `SUBSTRING(expr, start[, length])`) is normalized into ordinary `Function { name: "trim" | "substring", ... }` calls so the binder treats them uniformly. `SUBSTRING` requires a start argument; `TRIM` with `LEADING`/`TRAILING`/`BOTH` or trim characters is unsupported.

## SQL Scope

Parser may produce AST variants for syntax that binder rejects. The parser parses:

- `CREATE TABLE` with column definitions and primary key. The parser parses both inline single-column `id INTEGER PRIMARY KEY` and table-level `PRIMARY KEY (id)` forms into `Statement::CreateTable.primary_key = vec!["id"]`; binder rejects composite primary keys. Column type spellings map to the three `DataType`s: `INTEGER`/`INT` and the width aliases `SMALLINT`/`BIGINT`/`INT2`/`INT4`/`INT8` all map to `Integer` (a single 64-bit integer — width is not enforced); `TEXT`/`VARCHAR`/`CHAR`/`CHARACTER` map to `Text`; `BOOLEAN`/`BOOL` map to `Boolean`; `DATE` maps to `Date`, and `TIMESTAMP` (without time zone, no fractional-seconds precision) maps to `Timestamp` — `TIMESTAMP WITH TIME ZONE` and an explicit precision are rejected. A `DATE 'YYYY-MM-DD'` or `TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff]'` typed-string literal parses to a `Value::Date` (days from epoch) / `Value::Timestamp` (microseconds from epoch); an impossible date/time such as `2023-02-29` or `... 25:00:00` is rejected at parse time. `BYTEA` maps to `Bytea`; a `BYTEA '\xDEADBEEF'` literal parses to `Value::Bytes` from the hex form (`\x` + an even number of hex digits — the legacy escape format is not supported). `UUID` maps to `Uuid`; a `UUID '...'` literal parses to `Value::Uuid` (lenient input: canonical `8-4-4-4-12` or bare 32-hex, case-insensitive, optional braces). `DOUBLE PRECISION`, `FLOAT8`, and `FLOAT` (no precision) map to `Double`; `REAL`/`FLOAT4` (single precision) and an explicit `FLOAT(p)` precision are rejected. A numeric literal written with a decimal point or exponent (`3.14`, `1e10`) is a `Value::Float`; a plain run of digits stays a `Value::Integer` (there is no implicit int/float coercion, so `42` is an integer literal even in a double context). `NUMERIC`/`DECIMAL` map to `Numeric { precision, scale }`, optionally carrying `(precision[, scale])` — precision must be `1..=28` (the `Decimal` limit; larger is rejected as unsupported) and scale `0..=precision`; a `NUMERIC '1.23'` typed-string literal parses to a `Value::Numeric` (any `(p, s)` on the literal is applied). `CAST` to `NUMERIC(p, s)` keeps the modifier (it rounds and is precision-checked at evaluation). A character type may carry a length (`VARCHAR(n)`/`CHAR(n)`/`CHARACTER(n)`): the length does not change the `DataType` (still `Text`) but is recorded on `ParsedColumnDef.max_length` as a column-level constraint (in characters, `n >= 1`; `VARCHAR(MAX)` and octet-unit lengths are rejected). Integer width qualifiers (e.g. `INT(11)`) and every other type are rejected with `SqlState::SyntaxError` ("unsupported data type"). `CAST` target types use the same `DataType` mapping but ignore any declared length.
- `DROP TABLE`.
- `CREATE [UNIQUE] INDEX name ON table (col, ...)`. The index name is required (SaguaroDB does not generate one). Index columns must be plain ascending column names; expressions, operator classes, `USING <method>`, partial `WHERE`, `INCLUDE`, `NULLS [NOT] DISTINCT`, `CONCURRENTLY`, and `IF NOT EXISTS` are rejected as unsupported.
- `DROP INDEX name`.
- `INSERT INTO ... VALUES` and `INSERT INTO ... SELECT`.
- `SELECT` with optional `DISTINCT` / `DISTINCT ON (...)`, projection, `FROM`, `WHERE`, inner/cross/left/right/full joins, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`.
- `UPDATE ... SET ... WHERE`.
- `DELETE FROM ... WHERE`.
- `EXPLAIN SELECT ...`. The AST node boxes any statement, but only a `SELECT` inner statement is accepted; any other inner statement is rejected as unsupported.
- Transaction control: `BEGIN` / `BEGIN TRANSACTION` / `START TRANSACTION` parse to `Statement::Begin { isolation }`; `COMMIT` / `END` parse to `Statement::Commit`; `ROLLBACK` parses to `Statement::Rollback`. An optional `ISOLATION LEVEL <level>` mode is carried on `Begin.isolation` (and on `SetTransaction.isolation`), with the four SQL levels mapped onto SaguaroDB's two: `READ UNCOMMITTED`/`READ COMMITTED` → `IsolationLevel::ReadCommitted`, `REPEATABLE READ`/`SERIALIZABLE`/`SNAPSHOT` → `IsolationLevel::RepeatableRead` (SERIALIZABLE is **aliased** to snapshot isolation; no SSI — see `mvcc.md` §10 Milestone G). The `READ WRITE` access mode is accepted and ignored (the default); `READ ONLY` is rejected (SaguaroDB enforces no read-only restriction, so silently ignoring it would mislead), as are MySQL-style modifiers, `AND CHAIN`, and atomic-block bodies. `[NOT] DEFERRABLE` is not parsed by sqlparser 0.56 in this position and is an upstream syntax error. `ABORT` is not recognized by the dialect and is a syntax error (SaguaroDB does not add it).
- Savepoints: `SAVEPOINT <name>` → `Statement::Savepoint { name }`; `RELEASE [SAVEPOINT] <name>` → `Statement::ReleaseSavepoint { name }`; `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] <name>` → `Statement::RollbackToSavepoint { name }` (a plain `ROLLBACK` with no savepoint stays `Statement::Rollback`). Names are lowercase-normalized. sqlparser 0.56 parses all three; the server's transaction lifecycle executes them (`docs/specs/savepoints.md`). They do not bind/plan.
- Set transaction: `SET TRANSACTION ISOLATION LEVEL <level>` (sqlparser's `Set(SetTransaction { session: false, .. })`) parses to `Statement::SetTransaction { isolation }`, and `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` (the session default, `session: true`) parses to `Statement::SetSessionCharacteristics { isolation }`. Both share the same level mapping (as above) and access-mode handling (`READ WRITE` accepted-and-ignored, `READ ONLY` rejected); only the `session` flag distinguishes them. `SET TRANSACTION SNAPSHOT` and every other `SET` form are rejected at parse time (`SyntaxError`). The transaction-scoped `SET TRANSACTION` is honored only before the transaction's first query, while `SET SESSION CHARACTERISTICS` sets the per-connection default for future transactions (both enforced by the server, `mvcc.md` §10 Milestone G).

- Maintenance: `VACUUM` parses to `Statement::Vacuum { table: None }` and `VACUUM <table>` to `Statement::Vacuum { table: Some(<lowercased name>) }`. **sqlparser 0.56 cannot parse `VACUUM`** (it errors), so `parse_statement` intercepts it *before* handing the string to sqlparser: it strips an optional trailing `;`, matches the leading `vacuum` keyword case-insensitively (a glued word like `vacuumfoo` is not a VACUUM and falls through to sqlparser), and accepts at most one bare-identifier argument (lowercase-normalized, the unquoted-identifier rule). Parenthesized options, multiple tables, qualified (`schema.table`) or quoted names, and Postgres option keywords (`FULL`/`FREEZE`/`ANALYZE`/`VERBOSE`/…) are rejected with `ErrorKind::Parse` / `SqlState::SyntaxError`; none are supported. `VACUUM` does not bind/plan — it is a maintenance command the server dispatches separately (`docs/specs/crates/server.md`, `docs/specs/mvcc.md` §9/§10 Milestone F).
- COPY: `COPY <table> [(cols)] FROM STDIN | TO STDOUT [WITH (...)]` parses to `Statement::Copy { table, columns, direction, options }` (see `docs/specs/copy.md`). The translator normalizes both the modern (`WITH (FORMAT csv, HEADER true, ...)`) and legacy (`WITH CSV HEADER ...`) option syntaxes into one `common::CopyOptions`, applying per-format defaults and PostgreSQL's "ESCAPE defaults to QUOTE" rule. It rejects, with structured errors, server-side files / `PROGRAM` and `COPY (query) TO` and `FORMAT binary` and the unsupported options (`FREEZE`/`FORCE_*`/`ENCODING`) as `FeatureNotSupported` (`0A000`); an unrecognized `FORMAT`, a backslash `DELIMITER`, a CR/LF delimiter or quote, and `DELIMITER`=`QUOTE` (CSV) as `SyntaxError`; `QUOTE`/`ESCAPE` with `FORMAT text` as `FeatureNotSupported`. Because sqlparser reads inline data after `FROM STDIN` and then demands a terminator, `parse_statement` first normalizes the input to be `;`-terminated (a no-op for other statements and never a second statement); copy-in data arrives over the wire, never inline.

Binder rejects parsed forms that exceed the semantic subset, such as composite primary keys and unknown functions.

Unquoted identifiers are normalized to lowercase before AST construction. Quoted identifiers are rejected with `ErrorKind::Parse` and `SqlState::SyntaxError`.

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
- Parses `INSERT ... SELECT` into `InsertSource::Query`, which the binder binds.
- Parses `COPY ... FROM STDIN` / `TO STDOUT` (with and without a trailing `;`), an explicit column list, and both modern and legacy CSV option syntaxes; rejects server-side files, `COPY (query)`, `FORMAT binary`, `QUOTE` with text format, and `DELIMITER`=`QUOTE` with the documented SQLSTATEs.
