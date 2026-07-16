use std::any::Any;
use std::sync::Arc;

use common::{
    ColumnId, ColumnInfo, DbError, IndexId, IndexSchema, Key, KeyRange, NamespaceSchema, Result,
    Row, RowId, RowIdentity, SchemaId, SequenceId, SequenceSchema, StatementContext, StoredRow,
    TableId, TableSchema, TupleLockAcquire, TupleLockMode, TupleLockTag, TupleLockWaitPolicy,
};

/// Reserve a primary-key value through the transaction-owned tuple lock manager.
/// The grant is intentionally retained until transaction end. Every PK insert and
/// `ON CONFLICT` arbiter uses this before any heap mutation or FK validation.
#[doc(hidden)]
pub fn reserve_unique_key(
    ctx: &StatementContext,
    table: TableId,
    key: &Key,
) -> Result<common::TupleLockGrantChange> {
    let tag = TupleLockTag {
        table,
        key: key.clone(),
    };
    match ctx.tuple_locks.acquire_tuple(
        ctx.txn_id,
        &tag,
        TupleLockMode::NoKeyUpdate,
        TupleLockWaitPolicy::Block,
        ctx.cancel.as_ref(),
    )? {
        TupleLockAcquire::Acquired(retained) => Ok(retained),
        TupleLockAcquire::Skipped => Err(DbError::internal(
            "blocking unique-key reservation unexpectedly skipped",
        )),
    }
}

/// The latest physical version reached after taking the requested tuple lock.
/// The identity names that version, which may differ from the scan-time identity
/// when a committed updater advanced the chain while the caller waited.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockedRow {
    identity: RowIdentity,
    row: Row,
    table: TableId,
    owner: u64,
    mode: TupleLockMode,
}

impl LockedRow {
    /// Construct the capability returned by a `StorageEngine` implementation after
    /// its lock manager grants `mode`. This is public for out-of-crate test/storage
    /// implementations; callers must not synthesize capabilities.
    #[doc(hidden)]
    pub fn from_lock_grant(
        table: TableId,
        owner: u64,
        identity: RowIdentity,
        row: Row,
        mode: TupleLockMode,
    ) -> Self {
        Self {
            identity,
            row,
            table,
            owner,
            mode,
        }
    }

    pub fn table(&self) -> TableId {
        self.table
    }

    pub fn identity(&self) -> &RowIdentity {
        &self.identity
    }

    pub fn row(&self) -> &Row {
        &self.row
    }

    pub fn owner(&self) -> u64 {
        self.owner
    }

    pub fn mode(&self) -> TupleLockMode {
        self.mode
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockRowResult {
    Locked(LockedRow),
    Deleted,
    Skipped,
}

#[derive(Clone, Copy, Debug)]
pub struct DependentRowProbe<'a> {
    pub table: TableId,
    pub columns: &'a [ColumnId],
    pub key: &'a Key,
    pub supporting_index: Option<IndexId>,
    pub excluded: Option<&'a RowIdentity>,
}

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
    /// Probe a declared primary-key or UNIQUE constraint for a current referenced
    /// row and retain `KeyShare` on the row identity when found.
    fn referenced_key_exists(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        access_index: IndexId,
        key: &Key,
    ) -> Result<bool>;
    /// Probe the current primary-key state for `INSERT ... ON CONFLICT`, waiting
    /// for an in-progress creator and retaining the requested tuple lock on the
    /// settled conflicting row. Returns `None` when no current conflict remains.
    fn lock_unique_conflict(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
        mode: TupleLockMode,
    ) -> Result<Option<LockedRow>>;
    /// Probe current child rows whose ordered `columns` equal `key`. `supporting_index`
    /// must name an exact-column child index when present. `excluded` suppresses one
    /// physical identity for self-referential parent mutation.
    fn dependent_row_exists(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        probe: DependentRowProbe<'_>,
    ) -> Result<bool>;
    fn delete(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        key: &Key,
    ) -> Result<bool>;
    /// Acquire a transaction-owned tuple lock for a scan-time identity, then resolve
    /// the current version while retaining that lock. Page-backed storage overrides
    /// this to follow committed update chains. Every implementation must define how
    /// its physical identity is validated; there is no snapshot-relative default.
    fn lock_row(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        identity: &RowIdentity,
        mode: TupleLockMode,
        wait_policy: TupleLockWaitPolicy,
    ) -> Result<LockRowResult>;

    /// Mutate the exact current version returned by [`StorageEngine::lock_row`].
    /// Callers must retain its transaction-owned tuple lock. Implementations reject
    /// a target whose recorded mode is weaker than the requested mutation requires.
    fn update_locked(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        target: &LockedRow,
        row: Row,
    ) -> Result<bool>;

    /// Delete the exact current version returned by [`StorageEngine::lock_row`].
    /// Callers must retain its transaction-owned tuple lock.
    fn delete_locked(
        &self,
        ctx: &StatementContext,
        relations: &dyn RelationSnapshot,
        table: TableId,
        target: &LockedRow,
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
    /// Update the visible row while requiring `TupleLockMode::Update`, even when
    /// its primary-key storage identity is unchanged.
    fn update_requiring_update_lock(
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
    /// Append the authoritative generic catalog metadata change before any
    /// physical operation that depends on it.
    fn apply_catalog_change(
        &self,
        ctx: &StatementContext,
        change_set: &common::CatalogChangeSet,
    ) -> Result<()>;
    fn create_schema(&self, ctx: &StatementContext, schema: &NamespaceSchema) -> Result<()>;
    fn drop_schema(&self, ctx: &StatementContext, schema: SchemaId) -> Result<()>;
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
}

pub trait RecoveryOperations: Send + Sync {
    /// Reconcile storage metadata from one committed generic catalog change.
    /// Recovery implementations must not append WAL.
    fn reconcile_catalog_change(&self, change_set: &common::CatalogChangeSet) -> Result<()> {
        for mutation in &change_set.mutations {
            match (&mutation.before, &mutation.after) {
                (None, Some(common::CatalogObject::Table(schema))) => {
                    self.apply_create_table(schema.clone())?;
                }
                (
                    Some(common::CatalogObject::Table(_)),
                    Some(common::CatalogObject::Table(schema)),
                ) => self.apply_update_table_schema(schema.clone())?,
                (Some(common::CatalogObject::Table(schema)), None) => {
                    self.apply_drop_table(schema.id)?;
                }
                (None, Some(common::CatalogObject::Index(schema))) => {
                    self.apply_create_index(schema.clone())?;
                }
                (
                    Some(common::CatalogObject::Index(_)),
                    Some(common::CatalogObject::Index(schema)),
                ) => self.apply_update_index_schema(schema.clone())?,
                (Some(common::CatalogObject::Index(schema)), None) => {
                    self.apply_drop_index(schema.id)?;
                }
                (None, Some(common::CatalogObject::Sequence(schema))) => {
                    self.apply_create_sequence(schema.clone())?;
                }
                (
                    Some(common::CatalogObject::Sequence(_)),
                    Some(common::CatalogObject::Sequence(schema)),
                ) => {
                    self.apply_drop_sequence(schema.id)?;
                    self.apply_create_sequence(schema.clone())?;
                }
                (Some(common::CatalogObject::Sequence(schema)), None) => {
                    self.apply_drop_sequence(schema.id)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
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
    /// Rebuild the derived storage identity tree from heap rows after recovery
    /// replay has reached a stable final heap state. Must not append WAL.
    fn apply_rebuild_table_identity(&self, schema: TableSchema) -> Result<()>;
}
