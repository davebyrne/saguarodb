use common::{
    ColumnDef, ColumnId, ColumnInfo, CopyDirection, CopyOptions, IndexId, ParsedColumnDef, TableId,
};

use crate::{BoundExpr, BoundOrderByItem, JoinType};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundStatement {
    CreateTable {
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    },
    DropTable {
        table: TableId,
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
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: BoundInsertSource,
        returning: Option<BoundReturning>,
    },
    Select(BoundSelect),
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: BoundSelect,
        returning: Option<BoundReturning>,
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
        columns: Vec<ColumnId>,
        direction: CopyDirection,
        options: CopyOptions,
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
    Query(Box<BoundSelect>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSelect {
    /// The `DISTINCT` modifier, or `None` for no de-duplication.
    pub distinct: Option<BoundDistinct>,
    pub columns: Vec<BoundSelectItem>,
    pub from: BoundFrom,
    pub filter: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub having: Option<BoundExpr>,
    pub order_by: Vec<BoundOrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundFrom {
    Table {
        table: TableId,
        binding: common::BindingId,
        alias: Option<String>,
        schema: Vec<ColumnDef>,
    },
    /// A derived table `(SELECT ...) AS alias [(cols)]`. The inner SELECT is bound
    /// in its own scope; `schema` is the derived columns (renamed by the optional
    /// column-alias list) projected into the outer scope at `binding`'s slots.
    Derived {
        select: Box<BoundSelect>,
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
