use common::{BindingId, ColumnId, DataType, PgType, SequenceId, Value};

use crate::BoundQuery;

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
        pg_type: Option<PgType>,
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
        pg_type: Option<PgType>,
        nullable: bool,
    },
    Array {
        elements: Vec<BoundExpr>,
        dimensions: Vec<u32>,
        element_type: DataType,
        data_type: DataType,
        nullable: bool,
    },
    ArraySubscript {
        array: Box<BoundExpr>,
        subscripts: Vec<BoundExpr>,
        data_type: DataType,
        nullable: bool,
    },
    Any {
        left: Box<BoundExpr>,
        op: BinOp,
        array: Box<BoundExpr>,
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
    /// A correlated reference to an enclosing query's column, from inside a
    /// subquery body. `slot` indexes the correlation list of the *immediately
    /// enclosing* subquery boundary (`BoundQuery::correlations`); the entry's
    /// `outer` expression, evaluated against the enclosing row, supplies the
    /// value. Substituted to a `Literal` per outer row before the subquery
    /// body executes — an `OuterRef` never reaches expression evaluation.
    /// `docs/specs/subqueries.md` §4.2.
    OuterRef {
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
        /// The declared wire type of the cast target, so a `CAST` output column
        /// reports the right OID/typmod (e.g. `::varchar` vs `::text`).
        pg_type: PgType,
        nullable: bool,
    },
    /// A scalar subquery `(SELECT ...)`: a single-column, at-most-one-row query
    /// used as a value. The sub-query is carried as a bound query (unchanged
    /// through logical and physical planning) and evaluated by the executor —
    /// an empty result yields `NULL`, more than one row is a runtime error.
    /// Uncorrelated: the inner query is its own binding scope.
    ScalarSubquery {
        query: Box<BoundQuery>,
        data_type: DataType,
        nullable: bool,
    },
    /// `[NOT] EXISTS (SELECT ...)`. Yields a non-null boolean: whether the
    /// sub-query produces at least one row (negated for `NOT EXISTS`).
    Exists {
        query: Box<BoundQuery>,
        negated: bool,
        data_type: DataType,
        nullable: bool,
    },
    /// `expr [NOT] IN (SELECT ...)` over a single-column sub-query. The executor
    /// materializes the column and applies SQL `IN`/`NOT IN` three-valued logic.
    InSubquery {
        expr: Box<BoundExpr>,
        query: Box<BoundQuery>,
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
    /// Emit each left row once if ANY right row matches; output is the left
    /// side only. Produced only by decorrelation (`docs/specs/subqueries.md`
    /// §6), never by the binder.
    Semi,
    /// Emit each left row once if NO right row matches; output is the left
    /// side only. Produced only by decorrelation.
    Anti,
}

impl JoinType {
    /// Semi/anti joins output the left side only and emit each left row at
    /// most once.
    pub fn is_semi_or_anti(self) -> bool {
        matches!(self, JoinType::Semi | JoinType::Anti)
    }
}

/// Which side's physical row identity a join's combined rows carry
/// (`docs/specs/subqueries.md` §8.1). Set only on the join spine of an
/// `UPDATE ... FROM` / `DELETE ... USING` source, where the target table is
/// always planted as the left input; plain query joins carry no identity.
/// Only `Left` exists because nothing plants a target on the right.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinSide {
    Left,
}

/// What an `Apply` (dependent join) computes per outer row from its correlated
/// subplan, appended as one column after the input row
/// (`docs/specs/subqueries.md` §5.1). The hoisting pass replaces the original
/// subquery expression with a `LocalRef` to the appended column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyKind {
    /// A correlated scalar subquery: the appended column is its single value
    /// (`NULL` when empty; more than one row is a `CardinalityViolation`).
    Scalar { data_type: DataType },
    /// `[NOT] EXISTS`: the appended column is a non-null boolean; evaluation
    /// may stop at the subplan's first row.
    Exists { negated: bool },
    /// `operand [NOT] IN (subplan)`: the appended column is the three-valued
    /// membership result. `operand` is an expression over the outer row.
    In {
        operand: Box<BoundExpr>,
        negated: bool,
    },
    /// A `LATERAL` derived table (`docs/specs/subqueries.md` §7): the subplan
    /// is a full table expression whose entire output row is appended, and an
    /// outer row produces one output row per matching inner row — or, for
    /// `left_join`, one null-padded row when none match. `condition` is the
    /// join's `ON` predicate over the combined (outer ++ inner) row;
    /// `output_schema` is the derived table's columns.
    Lateral {
        left_join: bool,
        condition: Option<Box<BoundExpr>>,
        output_schema: Vec<common::ColumnInfo>,
    },
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
    /// `ARRAY_AGG` — one-dimensional array containing input values, including NULLs.
    ArrayAgg,
    /// `STRING_AGG(value, delimiter)` — concatenated non-NULL text values.
    StringAgg,
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
            | BoundExpr::Nextval { data_type, .. }
            | BoundExpr::Currval { data_type, .. }
            | BoundExpr::Setval { data_type, .. }
            | BoundExpr::AggregateCall { data_type, .. }
            | BoundExpr::LocalRef { data_type, .. }
            | BoundExpr::OuterRef { data_type, .. }
            | BoundExpr::IsNull { data_type, .. }
            | BoundExpr::IsNotNull { data_type, .. }
            | BoundExpr::InList { data_type, .. }
            | BoundExpr::Between { data_type, .. }
            | BoundExpr::Like { data_type, .. }
            | BoundExpr::Case { data_type, .. }
            | BoundExpr::Cast { data_type, .. }
            | BoundExpr::ScalarSubquery { data_type, .. }
            | BoundExpr::Exists { data_type, .. }
            | BoundExpr::InSubquery { data_type, .. }
            | BoundExpr::Parameter { data_type, .. }
            | BoundExpr::Function { data_type, .. } => data_type.clone(),
            BoundExpr::Array { data_type, .. }
            | BoundExpr::ArraySubscript { data_type, .. }
            | BoundExpr::Any { data_type, .. } => data_type.clone(),
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
            | BoundExpr::OuterRef { nullable, .. }
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
            BoundExpr::Array { nullable, .. }
            | BoundExpr::ArraySubscript { nullable, .. }
            | BoundExpr::Any { nullable, .. } => *nullable,
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
