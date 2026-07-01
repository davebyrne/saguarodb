use super::*;

impl PageBackedStorageEngine {
    pub(crate) fn apply_create_table_without_wal(&self, schema: TableSchema) -> Result<()> {
        // Recovery replays the index pages from their full-page-image redo
        // records, so this installs metadata only; it must not create the tree.
        let mut state = self.lock_state()?;
        state.tables.insert(
            schema.id,
            TableState {
                schema,
                dropped: false,
            },
        );
        Ok(())
    }
    pub(crate) fn apply_drop_table_without_wal(&self, table: TableId) -> Result<()> {
        let mut state = self.lock_state()?;
        if let Some(table_state) = state.tables.get_mut(&table) {
            table_state.dropped = true;
        }
        // Recovery replays a single DropTable record; cascade to the table's
        // indexes here, matching the catalog's apply_drop_table cascade. txn 0
        // means no rollback tracking.
        mark_table_indexes_dropped(&mut state, 0, table);
        Ok(())
    }
    pub(crate) fn apply_create_index_without_wal(&self, schema: IndexSchema) -> Result<()> {
        // Like apply_create_table_without_wal: the secondary tree's pages are
        // replayed from their full-page-image redo records, so this installs index
        // metadata only and must not build or backfill the tree.
        let mut state = self.lock_state()?;
        state.indexes.insert(
            schema.id,
            IndexState {
                schema,
                dropped: false,
            },
        );
        Ok(())
    }
    pub(crate) fn apply_drop_index_without_wal(&self, index: IndexId) -> Result<()> {
        let mut state = self.lock_state()?;
        if let Some(index_state) = state.indexes.get_mut(&index) {
            index_state.dropped = true;
        }
        Ok(())
    }
    pub(crate) fn apply_create_sequence_without_wal(&self, schema: SequenceSchema) -> Result<()> {
        let mut state = self.lock_state()?;
        state
            .sequences
            .insert(schema.id, SequenceState::new(schema));
        Ok(())
    }
    pub(crate) fn apply_drop_sequence_without_wal(&self, sequence: SequenceId) -> Result<()> {
        self.lock_state()?.sequences.remove(&sequence);
        Ok(())
    }
    pub(crate) fn apply_sequence_advance_without_wal(
        &self,
        sequence: SequenceId,
        value: i64,
    ) -> Result<()> {
        // Replaying a `nextval` advance restores the same state as a `setval` to
        // `value` with `is_called = true`.
        self.apply_set_sequence_value_without_wal(sequence, value, true)
    }
    pub(crate) fn apply_set_sequence_value_without_wal(
        &self,
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<()> {
        let sequence_state = {
            let state = self.lock_state()?;
            state.sequences.get(&sequence).cloned()
        };
        let Some(sequence_state) = sequence_state else {
            return Ok(());
        };
        let mut schema = sequence_state.lock_schema()?;
        validate_sequence_value(&schema, value)?;
        schema.last_value = value;
        schema.is_called = is_called;
        Ok(())
    }
}
