use common::{Key, Result, Row, TableId, TableSchema};

use crate::engine::PageBackedStorageEngine;
use crate::traits::RecoveryOperations;

impl RecoveryOperations for PageBackedStorageEngine {
    fn apply_insert(&self, table: TableId, key: Key, row: Row) -> Result<()> {
        self.apply_insert_without_wal(table, key, row)
    }

    fn apply_update(&self, table: TableId, key: Key, row: Row) -> Result<()> {
        self.apply_update_without_wal(table, key, row)
    }

    fn apply_delete(&self, table: TableId, key: Key) -> Result<()> {
        self.apply_delete_without_wal(table, key)
    }

    fn apply_create_table(&self, schema: TableSchema) -> Result<()> {
        self.apply_create_table_without_wal(schema)
    }

    fn apply_drop_table(&self, table: TableId) -> Result<()> {
        self.apply_drop_table_without_wal(table)
    }
}
