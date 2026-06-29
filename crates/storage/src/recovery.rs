use common::{IndexId, IndexSchema, Result, SequenceId, SequenceSchema, TableId, TableSchema};

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

    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()> {
        self.apply_create_sequence_without_wal(schema)
    }

    fn apply_drop_sequence(&self, sequence: SequenceId) -> Result<()> {
        self.apply_drop_sequence_without_wal(sequence)
    }

    fn apply_sequence_advance(&self, sequence: SequenceId, value: i64) -> Result<()> {
        self.apply_sequence_advance_without_wal(sequence, value)
    }

    fn apply_set_sequence_value(
        &self,
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<()> {
        self.apply_set_sequence_value_without_wal(sequence, value, is_called)
    }
}
