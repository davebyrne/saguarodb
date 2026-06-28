use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::{RowId, Value};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Key(pub Vec<Value>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyRange {
    Exact(Key),
    Range { start: Bound<Key>, end: Bound<Key> },
    All,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRow {
    pub row_id: RowId,
    pub key: Key,
    pub row: Row,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecRow {
    pub row: Row,
    pub identity: Option<RowIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowIdentity {
    pub row_id: RowId,
    pub key: Key,
}

#[cfg(test)]
mod tests {
    use super::{ExecRow, Key, Row, RowIdentity};
    use crate::Value;
    use crate::ids::RowId;

    #[test]
    fn exec_row_identity_is_independent_of_projected_values() {
        let identity = RowIdentity {
            row_id: RowId {
                page_num: 3,
                slot_num: 9,
            },
            key: Key(vec![Value::Integer(42)]),
        };

        let row = ExecRow {
            row: Row {
                values: vec![Value::Text("projected".to_string())],
            },
            identity: Some(identity.clone()),
        };

        assert_eq!(row.identity, Some(identity));
    }
}
