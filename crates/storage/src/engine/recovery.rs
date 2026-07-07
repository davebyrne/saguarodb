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

    pub(crate) fn apply_update_table_schema_without_wal(&self, schema: TableSchema) -> Result<()> {
        let secondary_indexes = {
            let mut state = self.lock_state()?;
            let current = state
                .tables
                .get(&schema.id)
                .filter(|table| !table.dropped)
                .cloned()
                .ok_or_else(|| storage_internal(format!("table {} is not installed", schema.id)))?;
            if current.schema.relation_kind != schema.relation_kind {
                return Err(storage_internal(format!(
                    "cannot change relation kind for table {} during schema replay",
                    schema.id
                )));
            }
            state.tables.insert(
                schema.id,
                Arc::new(TableGeneration {
                    schema: schema.clone(),
                    dropped: false,
                }),
            );
            if matches!(schema.relation_kind, RelationKind::Toast { .. }) {
                state
                    .toast_next_value_ids
                    .insert(schema.id, crate::toast::FIRST_TOAST_VALUE_ID);
            }
            let secondary_indexes = state
                .indexes
                .values()
                .filter(|index| !index.dropped && index.schema.table == schema.id)
                .map(|index| index.schema.clone())
                .collect::<Vec<_>>();
            bump_relation_epoch(&mut state);
            secondary_indexes
        };

        self.register_table_compression(&schema);
        let index_config = index_compression_for(schema.compression);
        for index in secondary_indexes {
            self.compression
                .set_file_config(secondary_index_file_id(index.storage_id), index_config);
        }
        Ok(())
    }

    pub(crate) fn apply_update_index_schema_without_wal(&self, schema: IndexSchema) -> Result<()> {
        let table_compression = {
            let mut state = self.lock_state()?;
            let table_compression = state
                .tables
                .get(&schema.table)
                .filter(|table| !table.dropped)
                .map(|table| table.schema.compression)
                .ok_or_else(|| {
                    storage_internal(format!(
                        "index {} references an unknown table {}",
                        schema.id, schema.table
                    ))
                })?;
            state
                .indexes
                .get(&schema.id)
                .filter(|index| !index.dropped)
                .ok_or_else(|| undefined_index(schema.id))?;
            state.indexes.insert(
                schema.id,
                Arc::new(IndexGeneration {
                    schema: schema.clone(),
                    dropped: false,
                }),
            );
            bump_relation_epoch(&mut state);
            table_compression
        };
        self.compression.set_file_config(
            secondary_index_file_id(schema.storage_id),
            index_compression_for(table_compression),
        );
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
