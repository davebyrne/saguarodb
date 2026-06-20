use common::{Result, TableId, TableSchema};

use crate::engine::PageBackedStorageEngine;
use crate::traits::RecoveryOperations;

impl RecoveryOperations for PageBackedStorageEngine {
    fn apply_create_table(&self, schema: TableSchema) -> Result<()> {
        self.apply_create_table_without_wal(schema)
    }

    fn apply_drop_table(&self, table: TableId) -> Result<()> {
        self.apply_drop_table_without_wal(table)
    }
}
