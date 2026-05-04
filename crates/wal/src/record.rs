use common::{Key, Lsn, Row, TableId, TableSchema, Value};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub txn_id: u64,
    pub kind: WalRecordKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecordKind {
    Insert {
        table: TableId,
        key: Key,
        row: Row,
    },
    Update {
        table: TableId,
        key: Key,
        row: Row,
    },
    Delete {
        table: TableId,
        key: Key,
    },
    CreateTable {
        schema: TableSchema,
    },
    DropTable {
        table: TableId,
    },
    Commit,
    Checkpoint {
        generation: u64,
        checkpoint_lsn: Lsn,
    },
}

impl WalRecord {
    pub fn insert_for_test(txn_id: u64, value: i64) -> Self {
        Self {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::Insert {
                table: 1,
                key: Key(vec![Value::Integer(value)]),
                row: Row {
                    values: vec![Value::Integer(value)],
                },
            },
        }
    }
}
