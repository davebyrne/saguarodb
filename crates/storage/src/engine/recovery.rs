use super::*;

impl PageBackedStorageEngine {
    pub(crate) fn apply_create_table_without_wal(&self, schema: TableSchema) -> Result<()> {
        // Recovery replays the index pages from their full-page-image redo
        // records, so this installs metadata only; it must not create the tree.
        self.register_table_compression(&schema);
        let mut state = self.lock_state()?;
        state.tables.insert(
            schema.id,
            Arc::new(TableGeneration {
                schema,
                dropped: false,
            }),
        );
        bump_relation_epoch(&mut state);
        Ok(())
    }
    pub(crate) fn apply_drop_table_without_wal(&self, table: TableId) -> Result<()> {
        let mut state = self.lock_state()?;
        let toast_table_id = live_toast_table_id(&state, table);
        // Recovery replays a single DropTable record; cascade to the table's
        // indexes and hidden TOAST relation here, matching the catalog's
        // apply_drop_table cascade. txn 0 means no rollback tracking.
        mark_table_dropped(&mut state, 0, table);
        if let Some(toast_table_id) = toast_table_id {
            mark_table_dropped(&mut state, 0, toast_table_id);
        }
        bump_relation_epoch(&mut state);
        Ok(())
    }
    pub(crate) fn apply_create_index_without_wal(&self, schema: IndexSchema) -> Result<()> {
        // Like apply_create_table_without_wal: the secondary tree's pages are
        // replayed from their full-page-image redo records, so this installs index
        // metadata only and must not build or backfill the tree.
        let mut state = self.lock_state()?;
        // The owning table's compression setting determines the (dict-less)
        // index file config (`compression.md` §4); it must already be installed
        // (CreateTable replays before CreateIndex).
        let table_compression = state
            .tables
            .get(&schema.table)
            .map(|table| table.schema.compression)
            .ok_or_else(|| {
                storage_internal(format!(
                    "index {} references an unknown table {}",
                    schema.id, schema.table
                ))
            })?;
        state.indexes.insert(
            schema.id,
            Arc::new(IndexGeneration {
                schema: schema.clone(),
                dropped: false,
            }),
        );
        bump_relation_epoch(&mut state);
        drop(state);
        self.compression.set_file_config(
            secondary_index_file_id(schema.storage_id),
            index_compression_for(table_compression),
        );
        Ok(())
    }
    pub(crate) fn apply_drop_index_without_wal(&self, index: IndexId) -> Result<()> {
        let mut state = self.lock_state()?;
        if let Some(schema) = state.indexes.get(&index).map(|index| index.schema.clone()) {
            state.indexes.insert(
                index,
                Arc::new(IndexGeneration {
                    schema,
                    dropped: true,
                }),
            );
            bump_relation_epoch(&mut state);
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
