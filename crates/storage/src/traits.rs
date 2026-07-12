use std::any::Any;
use std::sync::Arc;

use common::{
    ColumnInfo, IndexId, IndexSchema, Key, KeyRange, Result, Row, RowId, SequenceId,
    SequenceSchema, StatementContext, StoredRow, TableId, TableSchema, TruncateCatalogUpdate,
    ViewSchema,
};

pub trait RowIterator: Send {
    fn next(&mut self) -> Result<Option<StoredRow>>;
    fn schema(&self) -> &[ColumnInfo];
}

pub trait RelationSnapshot: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn relation_epoch(&self) -> u64;
    fn table_schema_version(&self, _table: TableId) -> Option<u64> {
        None
    }
    fn table_storage_id(&self, _table: TableId) -> Option<common::FileId> {
        None
    }
    /// Some transaction-level catalog lookups intentionally resolve against current
    /// metadata while their retained relation snapshot predates a new table. Read
    /// scans may treat that absent relation as empty only when this returns true;
    /// writes and storage-internal relation lookups remain strict.
    fn missing_tables_are_empty(&self) -> bool {
        false
    }
}

pub trait StorageEngine: Send + Sync {
    fn capture_relation_snapshot(&self) -> Result<Arc<dyn RelationSnapshot>>;
    fn insert(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        row: Row,
    ) -> Result<RowId>;
    fn get(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<Option<Row>>;
    fn delete(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<bool>;
    /// Update the visible version of `key` to `row`. The HOT update-path prune
    /// (`docs/specs/mvcc.md` §10 Milestone H3) reads the GC horizon from
    /// `ctx.gc_horizon`: when a same-page HOT update finds no room, the engine collapses
    /// that page's committed-dead HOT prefixes (under the heap latch) to reclaim space
    /// before falling back to a normal update. A stale/smaller horizon only prunes less.
    fn update(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
    ) -> Result<bool>;
    fn scan(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
    ) -> Result<Box<dyn RowIterator>>;
    fn for_each_visible_row(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        visitor: &mut dyn FnMut(StoredRow) -> Result<()>,
    ) -> Result<()> {
        let mut iter = self.scan(ctx, relations, table)?;
        while let Some(row) = iter.next()? {
            visitor(row)?;
        }
        Ok(())
    }
    fn scan_range(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>>;
    /// Scan a table through one of its secondary indexes. `range` constrains the
    /// indexed columns; rows are returned in index order, resolved to the heap
    /// via each entry's primary key.
    fn index_scan(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        index: IndexId,
        range: &KeyRange,
    ) -> Result<Box<dyn RowIterator>>;
    fn rollback_txn(&self, txn_id: u64) -> Result<()>;
    fn commit_txn(&self, txn_id: u64) -> Result<()>;
}

pub trait SchemaOperations: Send + Sync {
    fn create_table(&self, ctx: &StatementContext, schema: &TableSchema) -> Result<()>;
    fn drop_table(&self, ctx: &StatementContext, table: TableId) -> Result<()>;
    fn update_table_schema(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        indexes: &[IndexSchema],
    ) -> Result<()>;
    /// Build a new secondary index and backfill it from the table's rows.
    ///
    /// `gc_horizon` is the GC horizon (minimum advertised snapshot `xmin`,
    /// [`crate::PageBackedStorageEngine::vacuum`]'s `horizon`); the caller captures it
    /// under the exclusive guard. It is used for the HOT broken-chain safety check
    /// (`docs/specs/mvcc.md` §10 Milestone H2): a chain with two or more
    /// not-dead-to-all versions whose new-index-column values differ is rejected with
    /// a retryable `SerializationFailure`, because a single root-pointed entry cannot
    /// serve every live snapshot of such a chain.
    fn create_index(
        &self,
        ctx: &StatementContext,
        schema: &IndexSchema,
        gc_horizon: u64,
    ) -> Result<()>;
    fn drop_index(&self, ctx: &StatementContext, index: IndexId) -> Result<()>;
    fn create_sequence(&self, ctx: &StatementContext, schema: &SequenceSchema) -> Result<()>;
    fn drop_sequence(&self, ctx: &StatementContext, sequence: SequenceId) -> Result<()>;
    fn create_view(&self, ctx: &StatementContext, schema: &ViewSchema) -> Result<()>;
    fn replace_view(&self, ctx: &StatementContext, schema: &ViewSchema) -> Result<()>;
    fn drop_view(&self, ctx: &StatementContext, view: TableId) -> Result<()>;
}

pub trait RecoveryOperations: Send + Sync {
    fn apply_create_table(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_table_schema(&self, schema: TableSchema) -> Result<()>;
    fn apply_update_index_schema(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_table(&self, table: TableId) -> Result<()>;
    fn apply_create_index(&self, schema: IndexSchema) -> Result<()>;
    fn apply_drop_index(&self, index: IndexId) -> Result<()>;
    fn apply_create_sequence(&self, schema: SequenceSchema) -> Result<()>;
    fn apply_drop_sequence(&self, sequence: SequenceId) -> Result<()>;
    fn apply_sequence_advance(&self, sequence: SequenceId, value: i64) -> Result<()>;
    fn apply_set_sequence_value(
        &self,
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    ) -> Result<()>;
    /// Recovery apply for `ALTER TABLE ... SET (compression)`: installs the
    /// updated schema and re-registers file configs. Must not append WAL.
    fn apply_set_table_compression(&self, schema: TableSchema) -> Result<()>;
    /// Recovery apply for `ALTER TABLE ... SET (toast...)`: installs the updated
    /// schema metadata. Must not append WAL.
    fn apply_set_table_toast_metadata(&self, schema: TableSchema) -> Result<()>;
    /// Recovery apply for committed relation-swap `TRUNCATE`: publishes the new
    /// table/index generations produced by catalog replay. Must not append WAL.
    fn apply_truncate_table(&self, update: TruncateCatalogUpdate) -> Result<()>;
    /// Recovery apply for `ALTER TABLE ... ADD/DROP PRIMARY KEY`: installs the
    /// updated schema metadata while WAL is replayed. The derived identity tree is
    /// rebuilt after the replay pass, once all heap records are applied and
    /// crashed writers are resolved. Must not append WAL.
    fn apply_set_table_primary_key(&self, schema: TableSchema) -> Result<()>;
    /// Rebuild the derived storage identity tree from heap rows after recovery
    /// replay has reached a stable final heap state. Must not append WAL.
    fn apply_rebuild_table_identity(&self, schema: TableSchema) -> Result<()>;
}
