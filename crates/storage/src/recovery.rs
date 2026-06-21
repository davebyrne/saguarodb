use common::{IndexId, IndexSchema, Result, TableId, TableSchema};

use crate::engine::PageBackedStorageEngine;
use crate::traits::RecoveryOperations;

impl RecoveryOperations for PageBackedStorageEngine {
    fn apply_create_table(&self, schema: TableSchema) -> Result<()> {
        self.apply_create_table_without_wal(schema)
    }

    fn apply_drop_table(&self, table: TableId) -> Result<()> {
        self.apply_drop_table_without_wal(table)
    }

    fn apply_create_index(&self, schema: IndexSchema) -> Result<()> {
        self.apply_create_index_without_wal(schema)
    }

    fn apply_drop_index(&self, index: IndexId) -> Result<()> {
        self.apply_drop_index_without_wal(index)
    }
}
