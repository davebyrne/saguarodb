use serde::{Deserialize, Serialize};

use crate::{ColumnId, TableId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub table_id: Option<TableId>,
    pub column_id: Option<ColumnId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
}

#[cfg(test)]
mod tests {
    use super::{ColumnInfo, DataType};

    #[test]
    fn column_info_can_describe_expression_output() {
        let column = ColumnInfo {
            name: "count".to_string(),
            data_type: DataType::Integer,
            table_id: None,
            column_id: None,
        };

        assert_eq!(column.name, "count");
        assert_eq!(column.table_id, None);
    }
}
