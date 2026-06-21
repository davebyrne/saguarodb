use common::{ColumnDef, ColumnId, ColumnInfo, ParsedColumnDef, TableId};

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
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: BoundInsertSource,
    },
    Select(BoundSelect),
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: BoundSelect,
    },
    Delete {
        table: TableId,
        source: BoundSelect,
    },
    Explain(Box<BoundStatement>),
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
    Join {
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        join_type: JoinType,
        condition: Option<BoundExpr>,
    },
}
