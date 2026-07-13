# `parser` Crate Specification

**Date:** 2026-07-04
**Status:** Living crate contract

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
    CreateSchema { name: String, if_not_exists: bool },
    DropSchema { name: String, if_exists: bool },
    CreateTable {
        name: QualifiedName,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        unique: Vec<Vec<String>>,
        // `WITH (compression = ...)`. `None` when omitted.
        compression: Option<CompressionSetting>,
        // `WITH (toast..., toast_compression...)`. Empty when omitted; binder
        // merges the patch with `ToastOptions::default_new_table()`.
        toast: ToastOptionPatch,
        checks: Vec<String>,
    },
    DropTable { names: Vec<QualifiedName>, if_exists: bool },
    // All relation/object targets use `QualifiedName`; examples abbreviated.
    AlterTableAddColumn { table: QualifiedName, if_not_exists: bool, column: ParsedColumnDef },
    AlterTableAlterColumnType { table: QualifiedName, column: String, data_type: DataType, pg_type: PgType },
    CreateIndex { name: QualifiedName, table: QualifiedName, columns: Vec<String>, unique: bool },
    CreateSequence { name: QualifiedName, options: SequenceOptions },
    Insert { table: QualifiedName, columns: Vec<String>, source: InsertSource, on_conflict: Option<OnConflict>, returning: Option<Vec<SelectItem>> },
    Query(Query),
    Update { table: QualifiedName, assignments: Vec<Assignment>, filter: Option<Expr>, returning: Option<Vec<SelectItem>> },
    Delete { table: QualifiedName, filter: Option<Expr>, returning: Option<Vec<SelectItem>> },
    DeclareCursor { name: String, query: Query },
    FetchCursor { name: String, count: FetchCount },
    CloseCursor { name: String },
    Explain(Box<Statement>),
    // `BEGIN`/`START TRANSACTION [ISOLATION LEVEL <level>]`. `isolation` is the
    // requested level mapped onto Read Committed / Repeatable Read / Serializable
    // (`None` = transaction default).
    Begin { isolation: Option<IsolationLevel> },
    Commit,
    Rollback,
    // Savepoints (docs/specs/savepoints.md): names lowercase-normalized; executed
    // by the server's transaction lifecycle (they do not bind/plan).
    Savepoint { name: String },              // SAVEPOINT <name>
    ReleaseSavepoint { name: String },       // RELEASE [SAVEPOINT] <name>
    RollbackToSavepoint { name: String },    // ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] <name>
    // `SET TRANSACTION ISOLATION LEVEL <level>` (transaction-scoped). `isolation` is
    // the mapped level, `None` for a `SET TRANSACTION` with no isolation-level mode.
    SetTransaction { isolation: Option<IsolationLevel> },
    // `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` (the
    // per-connection session default). `isolation` is the mapped level (same mapping
    // as `Begin`/`SetTransaction`), `None` for a `SET SESSION CHARACTERISTICS` with no
    // isolation-level mode.
    SetSessionCharacteristics { isolation: Option<IsolationLevel> },
    enum SetScope { Session, Local }
    // `SET [SESSION|LOCAL] <name> {=|TO} <value>` — session GUC assignment.
    // Also produced by `SET TIME ZONE <v>` (name `timezone`) and `SET NAMES <enc>`
    // (name `client_encoding`). GUC names are lowercase-normalized; quoted GUC
    // name parts are accepted as the narrow exception to the quoted-identifier rule.
    SetVariable { scope: SetScope, name: String, value: String },
    // `RESET <name>` / `RESET ALL` (None). Intercepted before sqlparser, which
    // cannot parse RESET.
    ResetVariable { name: Option<String> },
    // `SHOW <name>` / `SHOW ALL` (None). `TIME ZONE` -> `timezone`;
    // `TRANSACTION ISOLATION LEVEL` -> `transaction_isolation`.
    ShowVariable { name: Option<String> },
    // `DISCARD ALL`.
    DiscardAll,
    // `VACUUM [ANALYZE]` (all user tables) or `VACUUM [ANALYZE] <table>` (one
    // table). The ANALYZE modifier requests the statistics pass after
    // reclamation (docs/specs/statistics.md §7).
    // `table` is lowercase-normalized; `None` means the whole database.
    Vacuum { table: Option<QualifiedName> },
    // `TRUNCATE [TABLE] <name> [, ...]`. Targets are lowercase-normalized.
    Truncate { tables: Vec<String> },
    // `ALTER TABLE <name> SET (compression = 'none' | 'zstd')`.
    // `table` is lowercase-normalized.
    AlterTableSetCompression { table: String, compression: CompressionSetting },
    // `ALTER TABLE <name> SET (...)` with one or more TOAST options.
    AlterTableSetOptions { table: String, options: TableOptionPatch },
    // `ALTER TABLE [ONLY] <name> ADD [CONSTRAINT <name>] PRIMARY KEY (cols...)`.
    AlterTableAddPrimaryKey {
        table: String,
        columns: Vec<String>,
        constraint_name: Option<String>,
    },
    // `ALTER TABLE [ONLY] <name> DROP PRIMARY KEY` or
    // `ALTER TABLE [ONLY] <name> DROP CONSTRAINT <name>`.
    AlterTableDropPrimaryKey { table: String, constraint_name: Option<String> },
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

pub enum FetchCount {
    One,
    Count(u64),
    All,
}

pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(Box<Query>),
}

pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

// A query expression: an optional WITH clause, a body, and the query-level
// modifiers that apply to its whole result. WITH and ORDER BY/LIMIT/OFFSET sit on
// the wrapper, outside the body, mirroring the SQL grammar. Carried by the
// top-level statement, derived tables, INSERT ... SELECT, and subquery expressions.
pub struct Query {
    pub with: Vec<Cte>,   // WITH CTEs (non-recursive), in order; empty if no WITH
    pub body: QueryBody,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// A common table expression: `name [(col, ...)] AS (query)`. `column_aliases`
// optionally renames the CTE's output columns left to right.
pub struct Cte {
    pub name: String,
    pub column_aliases: Vec<String>,
    pub query: Box<Query>,
}

// The body of a query expression: a single SELECT, a VALUES list, or a set
// operation combining two query expressions.
pub enum QueryBody {
    Select(Select),
    // A VALUES list (`VALUES (1,'a'), (2,'b')`): each inner Vec is one row; all
    // rows must be the same width. Output columns are unnamed (column1, column2, ...).
    Values(Vec<Vec<Expr>>),
    // A set operation (`a UNION b`, `a INTERSECT b`, `a EXCEPT b`). `all` is the
    // ALL quantifier. Each side is a full Query, so a parenthesized arm may carry
    // its own ORDER BY/LIMIT; the enclosing Query's modifiers apply to the result.
    SetOp { op: SetOp, all: bool, left: Box<Query>, right: Box<Query> },
}

pub enum SetOp { Union, Intersect, Except }

// A single SELECT block, without the query-level ORDER BY/LIMIT/OFFSET (which
// live on the enclosing Query). `from` may be empty — a FROM-less scalar
// projection such as `SELECT 1`.
pub struct Select {
    pub distinct: Option<Distinct>,
    pub columns: Vec<SelectItem>,
    pub from: Vec<FromItem>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
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
Unquoted relation names in `FROM` may be one or two parts; the parser stores the
schema and relation together as `common::QualifiedName` on `FromItem::Table`.

User object and relation targets accept exactly one or two unquoted identifier
parts. One part produces an unqualified `QualifiedName`; two parts preserve the
schema and object names. Three or more parts and quoted identifiers are rejected.
`CREATE SCHEMA [IF NOT EXISTS]` and `DROP SCHEMA [IF EXISTS] [RESTRICT]` are
supported; `DROP SCHEMA ... CASCADE` is rejected.

## Expression AST

```rust
pub enum SelectItem {
    Wildcard,
    QualifiedWildcard(String),
    Expression { expr: Expr, alias: Option<String> },
}

pub enum FromItem {
    Table { schema: Option<String>, name: String, alias: Option<String> },
    // Derived table: (SELECT ...) AS alias [(col, ...)]. The alias is required.
    Derived { subquery: Box<Query>, alias: String, column_aliases: Vec<String> },
    // Set-returning function in FROM. Binding and execution are downstream.
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
    Subquery(Box<Query>), // scalar subquery (SELECT ...) as a value
    InSubquery { expr: Box<Expr>, subquery: Box<Query>, negated: bool }, // x [NOT] IN (SELECT ...)
    Exists { subquery: Box<Query>, negated: bool }, // [NOT] EXISTS (SELECT ...)
    BinaryOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    Function { name: String, args: Vec<FunctionArg>, distinct: bool },
    Array(Vec<Expr>), // ARRAY[expr, ...]
    ArraySubscript { array: Box<Expr>, subscripts: Vec<Expr> }, // a[1][2]
    Any { left: Box<Expr>, op: BinOp, array: Box<Expr> }, // x op ANY(array)
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool, case_insensitive: bool, escape: Option<char> },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
        pg_type: PgType,  // declared wire type of the cast target (OID/typmod reporting)
    },
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
    IsDistinctFrom,    // a IS DISTINCT FROM b (NULL-safe)
    IsNotDistinctFrom, // a IS NOT DISTINCT FROM b (NULL-safe)
}

pub enum UnaryOp {
    Neg,
    Not,
}
```

`FromItem::Join.condition` is `None` only for `JoinType::Cross`. Inner, left, right, and full joins require `Some(condition)` from an `ON` predicate and the parser rejects those joins without `ON`. The parser rejects `USING` and `NATURAL` joins, and rejects `ON`/`USING` with `CROSS JOIN`.

Parser `BinOp` and `UnaryOp` variants use the same names as planner expression operators. Binder may copy these operators into bound expressions after type validation.

`Expr::Case.operand = None` represents searched `CASE WHEN condition THEN ...`. `operand = Some(expr)` represents simple `CASE expr WHEN value THEN ...`; binder/executor implement this by comparing the operand to each `WHEN` value with SQL equality semantics.

Function call parsing preserves aggregate syntax: `COUNT(*)` is `Function { name: "count", args: vec![FunctionArg::Wildcard], distinct: false }`, and `COUNT(DISTINCT id)` is `Function { name: "count", args: vec![FunctionArg::Expr(...)] , distinct: true }`. Binder converts `COUNT(*)` to `BoundExpr::AggregateCall { arg: None, ... }`, carries `distinct: true` through to `BoundExpr::AggregateCall { distinct: true, ... }` so the executor de-duplicates the argument (e.g. `COUNT(DISTINCT id)`), and rejects `FunctionArg::Wildcard` for non-`COUNT` functions, mixed with other arguments, or combined with `DISTINCT` (`COUNT(DISTINCT *)`). Ordinary function names may be unqualified or qualified with `pg_catalog`; the parser normalizes `pg_catalog.<function>(...)` to the same lowercase function name as `<function>(...)`. Other qualified function schemas and three-or-more-part function names are rejected as unsupported.

The dedicated `TRIM(expr)`, `SUBSTRING(expr [FROM start] [FOR length])` (and the comma form `SUBSTRING(expr, start[, length])`), `CEIL(expr)` / `FLOOR(expr)`, and `POSITION(substring IN string)` grammar is normalized into ordinary `Function { name: "trim" | "substring" | "ceil" | "floor" | "position", ... }` calls so the binder treats them uniformly (`POSITION` becomes `position(substring, string)`). `EXTRACT(field FROM source)` is normalized to `extract('field', source)`, carrying the field name as a lowercase text literal; only `year`, `month`, `day`, `hour`, `minute`, and `second` are supported (other fields are rejected). PostgreSQL system information names `CURRENT_CATALOG`, `CURRENT_USER`, `SESSION_USER`, and `USER` parse as zero-argument function calls for the binder/executor; `CURRENT_SCHEMA` parses as a column reference and the binder falls back to the zero-argument function only when no real column named `current_schema` resolves. `CURRENT_TIMESTAMP` and `NOW()` parse as zero-argument function calls and bind through the scalar function registry. `CURRENT_DATE` is still unsupported and rejected by the binder as an unknown function. `SUBSTRING` requires a start argument; `TRIM` with `LEADING`/`TRAILING`/`BOTH` or trim characters is unsupported; the `CEIL(expr TO <field>)`/scale forms are unsupported. (`CEILING` is not a sqlparser keyword, so it arrives as a plain `ceiling` function call, which the binder treats like `ceil`.)

`a IS [NOT] DISTINCT FROM b` parses to `BinaryOp { op: BinOp::IsDistinctFrom | BinOp::IsNotDistinctFrom, ... }`. `COALESCE(...)` and `NULLIF(a, b)` parse as ordinary `Function` calls (named `coalesce`/`nullif`); the binder desugars them to `CASE` because, unlike the generic scalar functions, they are not NULL-propagating.

`ILIKE`/`NOT ILIKE` parse to `Expr::Like { case_insensitive: true, ... }` (plain `LIKE` is `case_insensitive: false`). The optional `ESCAPE c` clause sets `Expr::Like.escape`: no clause defaults to `Some('\\')` (backslash), `ESCAPE 'x'` is `Some('x')`, and `ESCAPE ''` disables escaping (`None`). An `ESCAPE` argument longer than one character, or the Snowflake `ANY` pattern-list form, is rejected.

## SQL Scope

Parser may produce AST variants for syntax that binder rejects. The parser parses:

- `CREATE TABLE [IF NOT EXISTS]` with column definitions and an optional primary key. The parser parses both inline single-column `id INTEGER PRIMARY KEY` and table-level `PRIMARY KEY (id)` forms into `Statement::CreateTable.primary_key = vec!["id"]`, and a table-level `PRIMARY KEY (a, b)` into the ordered composite `vec!["a", "b"]` (the binder and catalog support no primary key and composite primary keys). Column type spellings map to the supported `DataType` variants: `INTEGER`/`INT` and the width aliases `SMALLINT`/`BIGINT`/`INT2`/`INT4`/`INT8` all map to `Integer` (a single 64-bit storage type) but record their distinct PostgreSQL wire width on `ParsedColumnDef.pg_type` (`int2`/`int4`/`int8`; bare `INTEGER` is `int4`); the executor range-checks `int2`/`int4` values at write and cast time. `SERIAL`/`SERIAL4`, `SMALLSERIAL`/`SERIAL2`, and `BIGSERIAL`/`SERIAL8` also map to `Integer`, force `NOT NULL`, store `ParsedDefault::Serial` for executor desugaring, and record their serial kind's wire width (`serial` => `int4`, `smallserial` => `int2`, `bigserial` => `int8`); explicit `DEFAULT` on a `SERIAL` family column is rejected. `TEXT`/`VARCHAR`/`CHAR`/`CHARACTER` map to `Text`; `BOOLEAN`/`BOOL` map to `Boolean`; `DATE` maps to `Date`, and `TIMESTAMP` (without time zone, no fractional-seconds precision) maps to `Timestamp` — an explicit fractional-seconds precision is rejected (and `TIMESTAMP WITH TIME ZONE` is a distinct type, below). `TIME` (without time zone, no precision) maps to `Time`; a `TIME 'HH:MM:SS[.ffffff]'` typed-string literal parses to `Value::Time` (microseconds since midnight; an impossible time such as `25:00:00` is rejected). `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ` map to `TimestampTz`; a `TIMESTAMPTZ '...'` literal parses to `Value::TimestampTz`, converting an optional `[+-]HH[:MM]` offset to UTC (no offset is taken as UTC). `INTERVAL` maps to `Interval`; an `INTERVAL 'text'` literal parses to `Value::Interval` (PostgreSQL `postgres`-style: `<n> <unit>` pairs for year/month/week/day/hour/minute/second plus a `HH:MM:SS[.ffffff]` time and an `ago` suffix; the `INTERVAL '1' DAY` field-qualifier form and ISO-8601 are not supported). A `DATE 'YYYY-MM-DD'` or `TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff]'` typed-string literal parses to a `Value::Date` (days from epoch) / `Value::Timestamp` (microseconds from epoch); an impossible date/time such as `2023-02-29` or `... 25:00:00` is rejected at parse time. `BYTEA` maps to `Bytea`; a `BYTEA '\xDEADBEEF'` literal parses to `Value::Bytes` from the hex form (`\x` + an even number of hex digits — the legacy escape format is not supported). `UUID` maps to `Uuid`; a `UUID '...'` literal parses to `Value::Uuid` (lenient input: canonical `8-4-4-4-12` or bare 32-hex, case-insensitive, optional braces). `DOUBLE PRECISION`, `FLOAT8`, and `FLOAT` (no precision) map to `Double`; `REAL`/`FLOAT4` map to `Real`; `FLOAT(p)` maps to `Real` for `p` in 1..=24 and `Double` for 25..=53 (other precisions rejected). A `REAL '1.5'` typed-string literal parses to `Value::Real`. A numeric literal written with a decimal point or exponent (`3.14`, `1e10`) is a `Value::Float`; a plain run of digits stays a `Value::Integer` (there is no implicit int/float coercion, so `42` is an integer literal even in a double context). `NUMERIC`/`DECIMAL` map to `Numeric { precision, scale }`, optionally carrying `(precision[, scale])` — precision must be `1..=28` (the `Decimal` limit; larger is rejected as unsupported) and scale `0..=precision`; a `NUMERIC '1.23'` typed-string literal parses to a `Value::Numeric` (any `(p, s)` on the literal is applied). `CAST` to `NUMERIC(p, s)` keeps the modifier (it rounds and is precision-checked at evaluation). A character type may carry a length (`VARCHAR(n)`/`CHAR(n)`/`CHARACTER(n)`): the length does not change the `DataType` (still `Text`) but is recorded on `ParsedColumnDef.max_length` as a column-level constraint (in characters, `n >= 1`; `VARCHAR(MAX)` and octet-unit lengths are rejected). Integer width qualifiers (e.g. `INT(11)`) and every other type are rejected with `SqlState::SyntaxError` ("unsupported data type"). `CAST` target types use the same `DataType` mapping but ignore any declared length. A column may carry `NULL`/`NOT NULL` and a `DEFAULT <constant | nextval('sequence')>` clause: constants fold to `ParsedDefault::Const(Value)` at parse time onto `ParsedColumnDef.default` (a literal, including `NULL`, or a unary-minus applied to a numeric literal); `nextval` with exactly one string-literal argument becomes `ParsedDefault::Nextval(name)`. Any other default expression (arithmetic, a function call, a column reference, etc.) is carried as canonical SQL text in `ParsedDefault::Expr(text)` for the binder to re-parse (`parse_expression`), bind, and validate; only a malformed `nextval` (whose argument is not a single string literal) is rejected at parse time with `SqlState::SyntaxError`. The default's type, references, and constraint-safety are checked by the binder. A `UNIQUE` constraint — column-level (`email TEXT UNIQUE`) or table-level (`UNIQUE (a, b)`) — is collected onto `Statement::CreateTable.unique` as an ordered list of column-name lists; each becomes a unique index created with the table. Decorated forms (a named constraint, `USING`/index options, `NULLS [NOT] DISTINCT`) are rejected as unsupported. A `CHECK (expr)` constraint — column-level (`n INT CHECK (n > 0)`) or table-level (`CHECK (a <= b)`) — is collected onto `Statement::CreateTable.checks` as canonical SQL text (both kinds flattened together in declaration order, as in PostgreSQL) for the binder to re-parse (`parse_expression`), bind against the table's columns, and validate; table-qualified column references inside `CHECK` are rejected by the binder so stored check text remains valid across table renames. A named form (`CONSTRAINT c CHECK (...)`) is rejected as unsupported. An optional trailing `WITH (...)` clause accepts `compression = 'none' | 'zstd'`, `toast = 'off' | 'auto' | 'aggressive'`, `toast_tuple_target = <integer>`, `toast_min_value_size = <integer>`, and `toast_compression = 'none' | 'zstd' | 'zstd_dict'`. Enum values may be single-quoted strings or bare identifiers and are matched case-insensitively. Duplicate recognized option keys and unrecognized option keys are `SqlState::SyntaxError`; unsupported enum values are `SqlState::FeatureNotSupported`; `toast_tuple_target` outside `256..=8000` and `toast_min_value_size < 128` are `SqlState::InvalidParameterValue`. `compression` is stored as `Statement::CreateTable.compression`; TOAST options are stored as a `ToastOptionPatch` for the binder to merge with `ToastOptions::default_new_table()`.
- The same `CREATE TABLE ... WITH (...)` grammar accepts compatibility-only
  `fillfactor = <integer>`. It must be an integer in `10..=100`; malformed or
  duplicate values are `SqlState::SyntaxError`, while a numeric value outside
  the range is `SqlState::InvalidParameterValue`. The validated value is
  discarded and has no AST, catalog, or page-layout effect.
- `DROP TABLE [IF EXISTS] <name> [, ...]`. Targets retain input order and are
  lowercase-normalized; duplicate normalized names are rejected.
- `CREATE [UNIQUE] INDEX name ON table (col, ...)`. The index name is required (SaguaroDB does not generate one). Index columns must be plain ascending column names; expressions, operator classes, `USING <method>`, partial `WHERE`, `INCLUDE`, `NULLS [NOT] DISTINCT`, `CONCURRENTLY`, and `IF NOT EXISTS` are rejected as unsupported.
- `DROP INDEX name`. `DROP INDEX IF EXISTS` is rejected as unsupported.
- PostgreSQL catalog type spelling `OID` and `pg_catalog.oid` map to the existing
  `Integer` semantic type while recording PostgreSQL `oid` wire presentation
  (`PgType::Oid`) for columns and casts. Other qualified custom type names remain
  unsupported.
- `CREATE SEQUENCE name [INCREMENT [BY] n] [START [WITH] n] [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE] [CACHE n] [[NO] CYCLE]`. Options may be written in any order and are normalized into `SequenceOptions`; duplicate options are rejected. `CACHE` must be positive and is accepted as parser input but ignored downstream. One- and two-part unquoted names are supported; `TEMP`/`TEMPORARY`, `IF NOT EXISTS`, `AS <type>`, quoted names, and `OWNED BY` are rejected as unsupported.
- `DROP SEQUENCE [IF EXISTS] name`.
- Relation and user-object targets accept unquoted one- or two-part names and
  preserve them as `QualifiedName`; three-or-more-part and quoted identifiers
  are rejected. `pg_catalog` and `information_schema` relations are available
  only through their virtual read-only views. The binder resolves unqualified
  user objects through `BindOptions.search_path` and rejects writes to system
  schemas.
- `INSERT INTO ... VALUES` and `INSERT INTO ... SELECT`.
- `INSERT ... ON CONFLICT [(col, ...)] DO NOTHING | DO UPDATE SET ... [WHERE ...]`: parsed into `on_conflict: Option<OnConflict>` on the `Insert` node. `OnConflict { target: Option<ConflictTarget>, action: ConflictAction }`; `ConflictTarget::Columns(Vec<String>)` (the binder requires the primary key); `ConflictAction::{ DoNothing, DoUpdate { assignments, filter } }`. `ON CONSTRAINT <name>` is rejected (`FeatureNotSupported`); MySQL's `ON DUPLICATE KEY UPDATE` is rejected. `excluded` resolution is a binder concern.
- `SELECT` with optional `DISTINCT` / `DISTINCT ON (...)`, projection, an optional `FROM`, `WHERE`, inner/cross/left/right/full joins, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`. The `FROM` clause may be omitted (`Select.from` is empty) for a FROM-less scalar projection such as `SELECT 1` or `SELECT count(*)`; the binder evaluates it over a single unit row. A top-level `SELECT` is represented as `Statement::Query(Query)` whose `body` is `QueryBody::Select`; the query-level `ORDER BY`/`LIMIT`/`OFFSET` live on the `Query` wrapper. `SELECT`, `VALUES`, and set-operation bodies are supported, and an optional `WITH` clause on the wrapper.
- `WITH name [(cols)] AS (query), ... <body>` parses to `Query.with`. `WITH RECURSIVE`, `MATERIALIZED` CTEs, and typed CTE column aliases are rejected. Each CTE's query is a full `Query` (so a CTE body may be a `SELECT`, `VALUES`, or set operation). Name resolution, inlining, and scoping are binder concerns.
- Standalone `VALUES (...), (...)` parses to `QueryBody::Values` — as a top-level statement, in `FROM` (as a derived table), and as a subquery body (`x IN (VALUES ...)`). All rows must be the same width; each column's type is checked and its columns are named `column1, column2, ...` by the binder, not the parser.
- `a UNION [ALL] b`, `a INTERSECT [ALL] b`, `a EXCEPT [ALL] b` parse to `QueryBody::SetOp`. Each arm is converted to a full `Query` (through `convert_set_expr_to_query`), so a parenthesized arm may carry its own `ORDER BY`/`LIMIT`; an unparenthesized outer `ORDER BY`/`LIMIT` binds to the enclosing `Query` wrapper and applies to the combined result. `MINUS` and the `... BY NAME` quantifiers are rejected as unsupported.
- Subquery expressions: a scalar subquery `(SELECT ...)` parses to `Expr::Subquery`, `expr [NOT] IN (SELECT ...)` parses to `Expr::InSubquery` (the subquery body is a `SetExpr`: a bare `SELECT`, or a parenthesized query with its own ORDER BY / LIMIT), and `[NOT] EXISTS (SELECT ...)` parses to `Expr::Exists`. Each subquery converts to a `Query` (so it may carry its own `ORDER BY`/`LIMIT`); cardinality and single-column shape are validated downstream (binder/executor), not in the parser.
- Derived tables: `[LATERAL] (SELECT ...) AS alias [(col, ...)]` in `FROM` parses to `FromItem::Derived` with a `lateral` flag (`docs/specs/subqueries.md` §7). The alias is required (a subquery in `FROM` without an alias is `SqlState::SyntaxError`); an optional parenthesized column-alias list renames the subquery's columns left to right (typed column aliases are rejected).
- Array syntax: unsized `type[]` column and cast targets map to the corresponding
  array `PgType`; sized declarations such as `integer[3]` are unsupported.
  `ARRAY[...]`, chained one-based subscripts such as `a[1][2]`, and
  `left op ANY(array)` / `SOME(array)` are preserved as dedicated expression
  nodes for binder type checking. Array slices are rejected.
- Set-returning functions in `FROM`, including `unnest(array)` and
  `generate_series(...)`, parse to `FromItem::TableFunction`. The parser
  preserves an optional table alias, column aliases, `LATERAL`, and
  `WITH ORDINALITY`; function resolution, argument validation, output shape,
  and execution are downstream responsibilities.
- `UPDATE ... SET ... WHERE`.
- `DELETE FROM ... WHERE`.
- `INSERT`/`UPDATE`/`DELETE ... RETURNING <items>`: the optional `RETURNING` clause is parsed into `returning: Option<Vec<SelectItem>>` on the `Insert`/`Update`/`Delete` AST node (`convert_returning` reuses the `SELECT`-list converter, so items may be expressions, `*`, or `table.*`). `None` when absent. `UPDATE ... FROM <items>` and `DELETE ... USING <items>` parse into `from`/`using: Vec<FromItem>` on the `Update`/`Delete` nodes (`docs/specs/subqueries.md` §8).
- `EXPLAIN SELECT ...`. The AST node boxes any statement, but only a `SELECT` inner statement is accepted; any other inner statement is rejected as unsupported.
- Transaction control: `BEGIN` / `BEGIN TRANSACTION` / `START TRANSACTION` parse to `Statement::Begin { isolation }`; `COMMIT` / `END` parse to `Statement::Commit`; `ROLLBACK` parses to `Statement::Rollback`. An optional `ISOLATION LEVEL <level>` mode is carried on `Begin.isolation` (and on `SetTransaction.isolation`), with the four SQL levels mapped onto SaguaroDB's three: `READ UNCOMMITTED`/`READ COMMITTED` → `IsolationLevel::ReadCommitted`, `REPEATABLE READ`/`SNAPSHOT` → `IsolationLevel::RepeatableRead`, `SERIALIZABLE` → `IsolationLevel::Serializable` (Serializable Snapshot Isolation — see `docs/specs/ssi.md`). The `READ WRITE` access mode is accepted and ignored (the default); `READ ONLY` is rejected (SaguaroDB enforces no read-only restriction, so silently ignoring it would mislead), as are MySQL-style modifiers, `AND CHAIN`, and atomic-block bodies. `[NOT] DEFERRABLE` is not parsed by sqlparser 0.56 in this position and is an upstream syntax error. `ABORT` is not recognized by the dialect and is a syntax error (SaguaroDB does not add it).
- Savepoints: `SAVEPOINT <name>` → `Statement::Savepoint { name }`; `RELEASE [SAVEPOINT] <name>` → `Statement::ReleaseSavepoint { name }`; `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] <name>` → `Statement::RollbackToSavepoint { name }` (a plain `ROLLBACK` with no savepoint stays `Statement::Rollback`). Names are lowercase-normalized. sqlparser 0.56 parses all three; the server's transaction lifecycle executes them (`docs/specs/savepoints.md`). They do not bind/plan.
- Set transaction: `SET TRANSACTION ISOLATION LEVEL <level>` (sqlparser's `Set(SetTransaction { session: false, .. })`) parses to `Statement::SetTransaction { isolation }`, and `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>` (the session default, `session: true`) parses to `Statement::SetSessionCharacteristics { isolation }`. Both share the same level mapping (as above) and access-mode handling (`READ WRITE` accepted-and-ignored, `READ ONLY` rejected); only the `session` flag distinguishes them. `SET TRANSACTION SNAPSHOT` is rejected at parse time (`SyntaxError`). The transaction-scoped `SET TRANSACTION` is honored only before the transaction's first query, while `SET SESSION CHARACTERISTICS` sets the per-connection default for future transactions (both enforced by the server, `mvcc.md` §10 Milestone G).
- Session configuration: `SET [SESSION|LOCAL] <name> {=|TO} <value>` parses to `Statement::SetVariable { scope, name, value }`, preserving whether the spelling was session/default scope or `LOCAL`; `SET TIME ZONE <value>` normalizes to `timezone` and preserves its `LOCAL` flag; `SET NAMES <encoding>` normalizes to `client_encoding`; `SHOW <name>` / `SHOW ALL` parses to `Statement::ShowVariable`; `RESET <name>` / `RESET ALL` parses to `Statement::ResetVariable`; and `DISCARD ALL` parses to `Statement::DiscardAll`. `RESET` is intercepted before sqlparser, like `VACUUM`, because sqlparser 0.56 cannot parse it. `SET GLOBAL ...` is rejected as unsupported, including special `SET GLOBAL TIME ZONE` and `SET GLOBAL NAMES` forms whose scope sqlparser would otherwise erase. GUC names are one or two dot-joined parts and are lowercase-normalized; quoted GUC name parts are accepted and normalized case-insensitively, matching PostgreSQL's GUC-name behavior. Other quoted identifiers remain unsupported. `SHOW TIME ZONE` maps to `timezone`; `SHOW TRANSACTION ISOLATION LEVEL` maps to `transaction_isolation`. Unsupported `DISCARD` objects are rejected with `FeatureNotSupported`.

- Maintenance: `VACUUM` parses to `Statement::Vacuum { table: None, analyze: false }` and `VACUUM <table>` to `Statement::Vacuum { table: Some(<lowercased name>), analyze: false }`. `VACUUM ANALYZE` and `VACUUM ANALYZE <table>` parse to the same variants with `analyze: true` — the server runs the statistics pass after reclamation (`docs/specs/statistics.md` §7). **sqlparser 0.56 cannot parse `VACUUM`** (it errors), so `parse_statement` intercepts it *before* handing the string to sqlparser: it strips an optional trailing `;`, matches the leading `vacuum` keyword case-insensitively (a glued word like `vacuumfoo` is not a VACUUM and falls through to sqlparser), and accepts optional `ANALYZE` followed by at most one bare-identifier argument (lowercase-normalized, the unquoted-identifier rule). Parenthesized options, multiple tables, qualified (`schema.table`) or quoted names, repeated/misplaced `ANALYZE`, and other PostgreSQL option keywords (`FULL`/`FREEZE`/`VERBOSE`/…) are rejected with `ErrorKind::Parse` / `SqlState::SyntaxError`. `VACUUM` does not bind/plan — it is a maintenance command the server dispatches separately (`docs/specs/crates/server.md`, `docs/specs/mvcc.md` §9/§10 Milestone F). `ANALYZE [<table>]` parses to `Statement::Analyze { table }` via its own pre-sqlparser intercept, mirroring `VACUUM`'s rules (statement-initial keyword only — `EXPLAIN ANALYZE` still reaches, and is rejected by, the EXPLAIN path; one optional bare lowercase-normalized identifier; `VERBOSE`, column lists, multiple/qualified/quoted names rejected as `SyntaxError`). Like `VACUUM`, it never binds/plans (`docs/specs/statistics.md` §5, §7). `TRUNCATE [TABLE] <name> [, ...]` parses to `Statement::Truncate { tables }` through sqlparser. Every target must be a bare, unquoted, unqualified name; targets retain input order, are lowercase-normalized, and duplicate normalized names are rejected. `ONLY`, partition clauses, `RESTART IDENTITY` / `CONTINUE IDENTITY`, `CASCADE` / `RESTRICT`, and `ON CLUSTER` remain unsupported. `TRUNCATE` does not bind/plan — it is a maintenance command the server dispatches separately, either as standalone autocommit or through the transactional DDL path. `CREATE SEQUENCE` is also intercepted before sqlparser because sqlparser 0.56 only accepts sequence options in one fixed order; the interceptor uses sqlparser's tokenizer but implements the documented order-insensitive option grammar.

PostgreSQL double-colon casts are accepted wherever the equivalent `CAST(... AS
type)` target is supported. Text-valued `regclass` and `pg_catalog.regclass` casts
are lowered to the catalog-backed `to_regclass` lookup; this supports parameterized
catalog probes emitted by pgbench while retaining the normal OID-valued expression
result.
- `ALTER TABLE <name> SET (...)` and primary-key ALTER forms are intercepted before sqlparser because they are maintenance commands. `ALTER TABLE <name> SET (compression = 'none' | 'zstd')` returns `Statement::AlterTableSetCompression`; TOAST option lists return `Statement::AlterTableSetOptions`; primary-key ADD/DROP forms return the primary-key ALTER variants. Other supported schema-evolution forms are converted from sqlparser's `ALTER TABLE` AST: `ADD COLUMN [IF NOT EXISTS]`, `DROP COLUMN [IF EXISTS]`, `RENAME COLUMN`, `RENAME TO`, and `ALTER [COLUMN] <column> [SET DATA] TYPE <data_type>`. The type-change form accepts optional `ONLY` and rejects `USING`. Unsupported ALTER families (`ALTER INDEX`, `ALTER SEQUENCE`, unsupported table operations) are rejected with `SqlState::FeatureNotSupported`. Malformed option lists, duplicate recognized keys, and unrecognized option keys are `SqlState::SyntaxError`; unsupported enum values are `SqlState::FeatureNotSupported`; out-of-range TOAST numeric values are `SqlState::InvalidParameterValue`.
- `CREATE [OR REPLACE] VIEW <name> [(cols)] AS <query>` returns `Statement::CreateView` with both the parsed query and canonical SQL definition text. `DROP VIEW [IF EXISTS] <name>` returns `Statement::DropView`.
- COPY: `COPY <table> [(cols)] FROM STDIN | TO STDOUT [WITH (...)]` parses to `Statement::Copy { table, columns, direction, options }` (see `docs/specs/copy.md`). The translator normalizes both the modern (`WITH (FORMAT csv, HEADER true, ...)`) and legacy (`WITH CSV HEADER ...`) option syntaxes into one `common::CopyOptions`, applying per-format defaults and PostgreSQL's "ESCAPE defaults to QUOTE" rule. The exact pgbench initialization suffix `WITH (FREEZE ON)` is accepted and discarded because SaguaroDB has no frozen-xid storage state; other `FREEZE` forms remain unsupported. It rejects, with structured errors, server-side files / `PROGRAM` and `COPY (query) TO` and `FORMAT binary` and the unsupported options (`FREEZE` except that compatibility suffix, `FORCE_*`, and `ENCODING`) as `FeatureNotSupported` (`0A000`); an unrecognized `FORMAT`, a backslash `DELIMITER`, a CR/LF delimiter or quote, and `DELIMITER`=`QUOTE` (CSV) as `SyntaxError`; `QUOTE`/`ESCAPE` with `FORMAT text` as `FeatureNotSupported`. Because sqlparser reads inline data after `FROM STDIN` and then demands a terminator, `parse_statement` first normalizes the input to be `;`-terminated (a no-op for other statements and never a second statement); copy-in data arrives over the wire, never inline.

Binder rejects parsed forms that exceed the semantic subset, such as unknown functions.

Unquoted identifiers are normalized to lowercase before AST construction. Quoted identifiers are rejected with `ErrorKind::Parse` and `SqlState::SyntaxError`, except quoted session-configuration parameter names, which are accepted and lowercase-normalized like PostgreSQL GUC names.

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
- Table storage options use a stable SQLSTATE split: malformed option-list
  syntax, duplicate keys, unknown keys, and unparsable value tokens are
  `SqlState::SyntaxError`; unsupported enum values are
  `SqlState::FeatureNotSupported`; out-of-range TOAST numeric values are
  `SqlState::InvalidParameterValue`. Non-`TABLE ... SET (...)` ALTER forms remain
  `FeatureNotSupported`.

## Acceptance Tests

- Parses one valid statement for each `Statement` variant.
- Rejects multiple statements in one SQL string.
- Preserves aliases and qualified names without resolving them.
- Parses `SELECT *` and `table.*` distinctly.
- Parses `EXPLAIN SELECT ...` into `Statement::Explain`.
- Parses `INSERT ... SELECT` into `InsertSource::Query`, which the binder binds.
- Parses `COPY ... FROM STDIN` / `TO STDOUT` (with and without a trailing `;`), an explicit column list, and both modern and legacy CSV option syntaxes; rejects server-side files, `COPY (query)`, `FORMAT binary`, `QUOTE` with text format, and `DELIMITER`=`QUOTE` with the documented SQLSTATEs.
- Parses `CREATE TABLE ... WITH (compression = 'zstd' | 'none' | zstd)` into `Statement::CreateTable.compression`, and `toast`, `toast_tuple_target`, `toast_min_value_size`, and `toast_compression` into `Statement::CreateTable.toast`. Compatibility-only `fillfactor = <integer>` is accepted in `10..=100`, duplicate/malformed/out-of-range values are rejected, and the value is intentionally discarded rather than added to the AST or catalog.
- Parses ordered multi-target DROP TABLE and TRUNCATE lists, lowercase-normalizes
  them, and rejects a duplicate normalized target.
- Parses `ALTER TABLE <name> SET (compression = ...)` (including mixed-case keywords and an optional trailing `;`) into `Statement::AlterTableSetCompression`.
- Parses `ALTER TABLE <name> SET (...)` with any TOAST option into `Statement::AlterTableSetOptions`; the server executes TOAST-only option changes and rejects mixed page-compression/TOAST changes.
- Parses `ALTER TABLE [ONLY] <name> ADD [CONSTRAINT <name>] PRIMARY KEY (cols...)`, `ALTER TABLE [ONLY] <name> DROP PRIMARY KEY`, and `ALTER TABLE [ONLY] <name> DROP CONSTRAINT <name>` into the primary-key ALTER statement variants.
- Parses schema-evolution `ALTER TABLE` ADD/DROP/RENAME COLUMN and RENAME TO forms.
- Parses `ALTER TABLE [ONLY] <table> ALTER [COLUMN] <column> [SET DATA] TYPE <type>` and rejects `USING`.
- Parses `CREATE [OR REPLACE] VIEW` and `DROP VIEW [IF EXISTS]`.
