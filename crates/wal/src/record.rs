use common::{FileId, Key, Lsn, PageNum, Row, TableId, TableSchema, Value};
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
    /// Physiological redo: initialize a fresh heap page.
    HeapInit {
        file_id: FileId,
        page_num: PageNum,
    },
    /// Physiological redo: write an encoded row into a slot on a heap page.
    HeapInsert {
        file_id: FileId,
        page_num: PageNum,
        slot: u16,
        row_bytes: Vec<u8>,
    },
    /// Physiological redo: mark a slot dead on a heap page.
    HeapDelete {
        file_id: FileId,
        page_num: PageNum,
        slot: u16,
    },
    /// Torn-page protection: a full page image, reinstalled during redo before
    /// any later delta for the same page is applied.
    FullPageImage {
        file_id: FileId,
        page_num: PageNum,
        image: Vec<u8>,
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
