use catalog::SystemView;
use common::{
    ColumnDef, ColumnId, ColumnInfo, CompressionSetting, CopyDirection, CopyOptions, DataType,
    IndexId, ParsedColumnDef, SequenceOptions, TableId, TableSchema, ToastOptions, ViewDependency,
};
use parser::SetOp;

use crate::{BoundExpr, BoundOrderByItem, JoinType};

/// A bound query expression: a bound body plus the query-level `ORDER BY`/`LIMIT`/
/// `OFFSET` that apply to its whole result. Mirrors the AST [`parser::Query`]; the
/// modifiers live here (not on [`BoundSelect`]) so a future set-operation body
/// orders and limits the combined result. Carried by the top-level statement, by
/// derived tables, by `INSERT ... SELECT`, and by subquery expressions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundQuery {
    pub body: BoundQueryBody,
    pub order_by: Vec<BoundOrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// The bound body of a query expression. Set operations attach here as a further
/// variant. `Select` is boxed to keep the variants a similar size (a `BoundSelect`
/// is far larger than a `BoundValues` or a future set-operation node).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundQueryBody {
    Select(Box<BoundSelect>),
    Values(BoundValues),
    SetOp(BoundSetOp),
}

/// A bound `VALUES` body: a literal row set. Every row has the same width as
/// `output_schema`; each column's type is the common type of its entries (a bare
/// `NULL` takes the column's type). Output columns are named `column1`, `column2`,
/// ... (no source table). Lowers directly to the existing `Values` plan node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundValues {
    pub rows: Vec<Vec<BoundExpr>>,
    pub output_schema: Vec<ColumnInfo>,
}

/// A bound set operation (`UNION`/`INTERSECT`/`EXCEPT`). Both arms are bound in
/// their own scopes; the binder has already checked that they have the same number
/// of columns and identical column types. `output_schema` is the reconciled result
/// (the left arm's column names, the shared types). `all` keeps duplicates
/// (`UNION ALL`); otherwise the result is de-duplicated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSetOp {
    pub op: SetOp,
    pub all: bool,
    pub left: Box<BoundQuery>,
    pub right: Box<BoundQuery>,
    pub output_schema: Vec<ColumnInfo>,
}

/// A query's result column, described independently of which body produced it —
/// used to derive derived-table schemas, validate `INSERT` sources, and (later)
/// reconcile set-operation arms, without matching on the body variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl BoundQuery {
    /// The query's result-set column metadata (its `RowDescription`). Delegates to
    /// the body; a future set-operation body reconciles its arms' columns here.
    pub fn output_schema(&self) -> &[ColumnInfo] {
        match &self.body {
            BoundQueryBody::Select(select) => &select.output_schema,
            BoundQueryBody::Values(values) => &values.output_schema,
            BoundQueryBody::SetOp(set_op) => &set_op.output_schema,
        }
    }

    /// The result columns with their nullability, in order. `output_schema` carries
    /// name + type for the wire `RowDescription`; this adds the nullability that
    /// derived-table and `INSERT`-source binding need, without exposing the body.
    pub fn output_columns(&self) -> Vec<OutputColumn> {
        match &self.body {
            BoundQueryBody::Select(select) => select
                .columns
                .iter()
                .map(|item| OutputColumn {
                    name: item.alias.clone(),
                    data_type: item.expr.data_type(),
                    nullable: item.expr.nullable(),
                })
                .collect(),
            BoundQueryBody::Values(values) => values
                .output_schema
                .iter()
                .enumerate()
                .map(|(index, column)| OutputColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    // A VALUES column is nullable if any row's entry is.
                    nullable: values.rows.iter().any(|row| row[index].nullable()),
                })
                .collect(),
            // A set-operation column takes the left arm's name and the shared type
            // (the arms are identically typed); it is nullable if either arm's is.
            BoundQueryBody::SetOp(set_op) => set_op
                .left
                .output_columns()
                .into_iter()
                .zip(set_op.right.output_columns())
                .map(|(left, right)| OutputColumn {
                    name: left.name,
                    data_type: left.data_type,
                    nullable: left.nullable || right.nullable,
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundStatement {
    CreateTable {
        name: String,
        if_not_exists: bool,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
        /// Column name lists for `UNIQUE` constraints; each becomes a unique
        /// index created together with the table.
        unique: Vec<Vec<String>>,
        /// The table's storage compression setting, resolved from the
        /// `WITH (compression = ...)` clause at bind time (`None` if omitted).
        compression: CompressionSetting,
        /// The table's TOAST policy, resolved from `WITH (toast...)` options at
        /// bind time.
        toast: ToastOptions,
        /// `CHECK` constraint expressions (canonical SQL text), validated against
        /// the table's columns at bind time and persisted with the schema.
        checks: Vec<String>,
    },
    DropTable {
        name: String,
        if_exists: bool,
        table: Option<TableId>,
    },
    AlterTableAddColumn {
        table: TableId,
        table_name: String,
        if_not_exists: bool,
        column: ParsedColumnDef,
    },
    AlterTableDropColumn {
        table: TableId,
        table_name: String,
        if_exists: bool,
        column: String,
    },
    AlterTableRenameColumn {
        table: TableId,
        table_name: String,
        old_name: String,
        new_name: String,
    },
    AlterTableRenameTable {
        table: TableId,
        table_name: String,
        new_name: String,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
        index: IndexId,
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
        query: BoundQuery,
        definition: String,
        dependencies: Vec<ViewDependency>,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: BoundInsertSource,
        on_conflict: Option<BoundOnConflict>,
        returning: Option<BoundReturning>,
        /// Bound expression `DEFAULT`s for columns this INSERT omits, evaluated per
        /// row by the executor. Only `ColumnDefault::Expr` columns appear here;
        /// constant and sequence defaults are read from the schema.
        default_exprs: Vec<(ColumnId, BoundExpr)>,
        /// The table's bound `CHECK` constraint expressions, evaluated over each
        /// inserted (or upserted) full row by the executor. Empty when the table
        /// has no checks.
        check_exprs: Vec<BoundExpr>,
    },
    Query(BoundQuery),
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: BoundSelect,
        returning: Option<BoundReturning>,
        /// The table's bound `CHECK` constraint expressions, evaluated over each
        /// updated (new) full row by the executor. Empty when the table has no
        /// checks.
        check_exprs: Vec<BoundExpr>,
    },
    Delete {
        table: TableId,
        source: BoundSelect,
        returning: Option<BoundReturning>,
    },
    Explain(Box<BoundStatement>),
    /// `COPY <table> [(cols)] FROM STDIN | TO STDOUT [WITH (...)]`. Resolved table
    /// and column ids (in COPY order; defaulted to all columns in catalog order).
    /// COPY is not lowered to a `LogicalPlan` — the server drives it over the COPY
    /// sub-protocol, reusing the storage insert path (FROM) and scan path (TO).
    /// See `docs/specs/copy.md`.
    Copy {
        table: TableId,
        table_schema: TableSchema,
        columns: Vec<ColumnId>,
        direction: CopyDirection,
        options: CopyOptions,
        /// Bound expression `DEFAULT`s for columns omitted by `COPY FROM`, evaluated
        /// per row by the executor (empty for `COPY TO` and when no omitted column
        /// has an expression default).
        default_exprs: Vec<(ColumnId, BoundExpr)>,
        /// The table's bound `CHECK` constraints, enforced per row by `COPY FROM`
        /// (empty for `COPY TO` and when the table has no checks).
        check_exprs: Vec<BoundExpr>,
    },
}

/// A bound `RETURNING` clause: the projection expressions evaluated over each
/// affected row (the inserted/updated NEW row, or the deleted OLD row), and the
/// result-set column metadata that becomes the statement's `RowDescription`. The
/// expressions reference the target table's columns as a single binding in
/// catalog (slot) order, so the executor evaluates them over the full row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundReturning {
    pub exprs: Vec<BoundExpr>,
    pub output_schema: Vec<ColumnInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundInsertSource {
    Values {
        rows: Vec<Vec<BoundExpr>>,
        output_schema: Vec<ColumnInfo>,
    },
    Query(Box<BoundQuery>),
}

/// A bound `INSERT ... ON CONFLICT` action. The arbiter is always the primary key
/// (the binder validates the conflict target). For `DoUpdate`, the assignment
/// value expressions and the optional `filter` are bound over a two-binding row —
/// the existing target row in slots `0..n` and the proposed `excluded` row in
/// slots `n..2n` — so the executor evaluates them over `existing ++ excluded`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundOnConflict {
    DoNothing {
        /// Explicit conflict target column ids, when present. The binder validates
        /// this as the primary key; execution rechecks it after prepared statements
        /// in case DDL changed the table shape. `None` is targetless DO NOTHING.
        target: Option<Vec<ColumnId>>,
    },
    DoUpdate {
        /// Explicit conflict target column ids. DO UPDATE requires a target.
        target: Vec<ColumnId>,
        assignments: Vec<(ColumnId, BoundExpr)>,
        filter: Option<BoundExpr>,
    },
}

/// A bound `SELECT` block, without the query-level `ORDER BY`/`LIMIT`/`OFFSET`
/// (those live on the enclosing [`BoundQuery`]). `output_schema` is this block's
/// result-set column metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSelect {
    /// The `DISTINCT` modifier, or `None` for no de-duplication.
    pub distinct: Option<BoundDistinct>,
    pub columns: Vec<BoundSelectItem>,
    /// The source relation, or `None` for a FROM-less `SELECT` (`SELECT 1`),
    /// which is evaluated over a single unit row.
    pub from: Option<BoundFrom>,
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub having: Option<BoundExpr>,
    pub output_schema: Vec<ColumnInfo>,
}

/// The bound `DISTINCT` modifier. `All` de-duplicates whole output rows;
/// `On(exprs)` keeps the first row per `exprs` key (`SELECT DISTINCT ON`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundDistinct {
    All,
    On(Vec<BoundExpr>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSelectItem {
    pub expr: BoundExpr,
    pub alias: String,
    /// Set when this output item was produced by expanding `*` from a physical
    /// user table. View dependency collection uses this to preserve wildcard
    /// intent through nested derived tables and CTEs.
    pub wildcard_source: Option<TableId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundFrom {
    Table {
        table: TableId,
        binding: common::BindingId,
        name: String,
        alias: Option<String>,
        schema: Vec<ColumnDef>,
    },
    System {
        view: SystemView,
        binding: common::BindingId,
        alias: Option<String>,
        schema: Vec<ColumnDef>,
    },
    /// A derived table `(SELECT ...) AS alias [(cols)]`. The inner SELECT is bound
    /// in its own scope; `schema` is the derived columns (renamed by the optional
    /// column-alias list) projected into the outer scope at `binding`'s slots.
    Derived {
        query: Box<BoundQuery>,
        binding: common::BindingId,
        alias: String,
        schema: Vec<ColumnDef>,
    },
    /// A user-defined view inlined as a derived query. It is kept distinct from a
    /// plain derived table so dependency tracking can invalidate prepared plans
    /// when the view is replaced or dropped.
    View {
        view: TableId,
        schema_version: u64,
        query: Box<BoundQuery>,
        binding: common::BindingId,
        alias: String,
        schema: Vec<ColumnDef>,
    },
    Join {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        join_type: JoinType,
        condition: Option<BoundExpr>,
    },
}
