use std::sync::{Arc, RwLock};

use common::{CatalogAllocatorHighWater, CatalogChangeSet, DbError, Result};

use crate::{
    CatalogAllocatorState, CatalogManager, CatalogSnapshot, MemoryCatalog,
    apply_catalog_change_set, catalog_change_set_between, merge_allocator_high_water,
    reserve_change_allocators, serialize_catalog,
};

#[derive(Clone, Default)]
struct CatalogJournal {
    change_sets: Vec<CatalogChangeSet>,
    allocator_high_water: CatalogAllocatorHighWater,
}

#[derive(Clone)]
pub struct CatalogOverlaySavepoint {
    journal_len: usize,
    allocator_high_water: CatalogAllocatorHighWater,
}

/// Writable transaction-local catalog state.
///
/// The overlay stores only objects changed by the transaction. Each read starts
/// from the current live catalog and reapplies those replacements/tombstones, so
/// unrelated catalog commits remain visible. Callers must hold the server's
/// catalog publication gate while publishing the resulting snapshot.
pub struct CatalogOverlay {
    base: Arc<dyn CatalogManager>,
    journal: RwLock<CatalogJournal>,
}

impl CatalogOverlay {
    pub fn new(base: Arc<dyn CatalogManager>) -> Self {
        Self {
            base,
            journal: RwLock::new(CatalogJournal::default()),
        }
    }

    pub fn snapshot(&self) -> Result<CatalogSnapshot> {
        let journal = self
            .journal
            .read()
            .map_err(|_| DbError::internal("catalog overlay read lock poisoned"))?;
        materialize(self.base.snapshot()?, &journal)
    }

    pub fn catalog(&self) -> Result<MemoryCatalog> {
        MemoryCatalog::try_from_snapshot(self.snapshot()?)
    }

    pub fn apply<T>(&self, mutation: impl FnOnce(&MemoryCatalog) -> Result<T>) -> Result<T> {
        let mut journal = self
            .journal
            .write()
            .map_err(|_| DbError::internal("catalog overlay write lock poisoned"))?;
        let before = materialize(self.base.snapshot()?, &journal)?;
        let expected = CatalogAllocatorState::from_snapshot(&before);
        let catalog = MemoryCatalog::try_from_snapshot(before.clone())?;
        let result = mutation(&catalog)?;
        let after = catalog.snapshot()?;
        // Reject an oversized transaction-local catalog before recording its
        // delta, so a successful DDL statement can always be checkpointed and
        // reopened under the durable decoder's matching limit.
        serialize_catalog(&after)?;
        let desired = CatalogAllocatorState::from_snapshot(&after);
        let change_set = catalog_change_set_between(&before, &after);
        if !self.base.claim_change_allocators(
            expected,
            desired,
            &change_set.allocator_high_water,
        )? {
            return Err(DbError::plan(
                common::SqlState::SerializationFailure,
                "catalog allocators changed concurrently; retry the statement",
            ));
        }
        merge_allocator_high_water(
            &mut journal.allocator_high_water,
            &change_set.allocator_high_water,
        );
        journal.change_sets.push(change_set);
        Ok(result)
    }

    pub fn publish(&self) -> Result<()> {
        self.base.restore(self.snapshot()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        let journal = self
            .journal
            .read()
            .map_err(|_| DbError::internal("catalog overlay read lock poisoned"))?;
        Ok(journal.change_sets.is_empty())
    }

    pub fn savepoint(&self) -> Result<CatalogOverlaySavepoint> {
        let journal = self
            .journal
            .read()
            .map_err(|_| DbError::internal("catalog overlay read lock poisoned"))?;
        Ok(CatalogOverlaySavepoint {
            journal_len: journal.change_sets.len(),
            allocator_high_water: journal.allocator_high_water.clone(),
        })
    }

    pub fn rollback_to(&self, savepoint: &CatalogOverlaySavepoint) -> Result<()> {
        let mut journal = self
            .journal
            .write()
            .map_err(|_| DbError::internal("catalog overlay write lock poisoned"))?;
        if savepoint.journal_len > journal.change_sets.len() {
            return Err(DbError::internal(
                "catalog overlay savepoint is ahead of the mutation journal",
            ));
        }
        journal.change_sets.truncate(savepoint.journal_len);
        merge_allocator_high_water(
            &mut journal.allocator_high_water,
            &savepoint.allocator_high_water,
        );
        Ok(())
    }

    pub fn absorb(&self, snapshot: CatalogSnapshot) -> Result<()> {
        self.apply(|catalog| catalog.restore(snapshot.clone()))
    }
}

fn materialize(snapshot: CatalogSnapshot, journal: &CatalogJournal) -> Result<CatalogSnapshot> {
    let mut snapshot = snapshot;
    for change_set in &journal.change_sets {
        snapshot = apply_catalog_change_set(&snapshot, change_set)?;
    }
    reserve_change_allocators(&mut snapshot, &journal.allocator_high_water);
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use common::{
        CompressionSetting, DataType, MAX_STORED_EXPRESSION_SQL_BYTES, ParsedColumnDef,
        STORED_EXPRESSION_VERSION, SqlState, StoredExpr, StoredExpression, StoredQueryBody,
        StoredQueryColumn, StoredQueryExpr, StoredQueryV1, StoredSelect, StoredSelectItem,
        ToastOptions, Value, ViewColumn,
    };

    use super::*;

    fn column() -> ParsedColumnDef {
        ParsedColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            max_length: None,
            default: None,
            pg_type: None,
        }
    }

    fn constant_view_query() -> StoredQueryV1 {
        StoredQueryV1 {
            version: common::STORED_QUERY_VERSION,
            body: StoredQueryBody::Select(Box::new(StoredSelect {
                distinct: None,
                columns: vec![StoredSelectItem {
                    expr: StoredQueryExpr::Literal {
                        value: Value::Integer(1),
                        data_type: DataType::Integer,
                        nullable: false,
                    },
                    alias: "id".to_string(),
                }],
                from: None,
                filter: None,
                group_by: Vec::new(),
                having: None,
                output_schema: vec![StoredQueryColumn {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    pg_type: common::PgType::Int8,
                }],
            })),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            row_lock: None,
            correlations: Vec::new(),
        }
    }

    #[test]
    fn local_changes_are_isolated_until_atomic_publish() {
        let base: Arc<dyn CatalogManager> = Arc::new(MemoryCatalog::empty());
        let overlay = CatalogOverlay::new(base.clone());
        overlay
            .apply(|catalog| {
                catalog.create_table(
                    "local".to_string(),
                    vec![column()],
                    Vec::new(),
                    CompressionSetting::None,
                )
            })
            .unwrap();

        assert!(base.get_table_by_name("local").unwrap().is_none());
        assert!(
            overlay
                .catalog()
                .unwrap()
                .get_table_by_name("local")
                .unwrap()
                .is_some()
        );
        overlay.publish().unwrap();
        assert!(base.get_table_by_name("local").unwrap().is_some());
    }

    #[test]
    fn unrelated_live_changes_remain_visible_beneath_local_delta() {
        let concrete = Arc::new(MemoryCatalog::empty());
        let base: Arc<dyn CatalogManager> = concrete.clone();
        let overlay = CatalogOverlay::new(base);
        overlay
            .apply(|catalog| {
                catalog.create_table(
                    "local".to_string(),
                    vec![column()],
                    Vec::new(),
                    CompressionSetting::None,
                )
            })
            .unwrap();
        concrete
            .create_table(
                "concurrent".to_string(),
                vec![column()],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();

        let visible = overlay.catalog().unwrap();
        assert!(visible.get_table_by_name("local").unwrap().is_some());
        assert!(visible.get_table_by_name("concurrent").unwrap().is_some());
    }

    #[test]
    fn local_tombstone_wins_over_live_base() {
        let concrete = Arc::new(MemoryCatalog::empty());
        let table = concrete
            .create_table(
                "gone".to_string(),
                vec![column()],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let base: Arc<dyn CatalogManager> = concrete;
        let overlay = CatalogOverlay::new(base);
        overlay
            .apply(|catalog| catalog.drop_table(table.id))
            .unwrap();
        assert!(
            overlay
                .catalog()
                .unwrap()
                .get_table(table.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn local_view_reserves_the_shared_relation_id() {
        let concrete = Arc::new(MemoryCatalog::empty());
        let base: Arc<dyn CatalogManager> = concrete.clone();
        let overlay = CatalogOverlay::new(base);
        let view = overlay
            .apply(|catalog| {
                catalog.create_view(
                    "local_view".to_string(),
                    vec![ViewColumn {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        pg_type: None,
                    }],
                    "select 1".to_string(),
                    constant_view_query(),
                )
            })
            .unwrap();
        let table = concrete
            .create_table(
                "concurrent".to_string(),
                vec![column()],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();

        assert_ne!(view.id, table.id);
        assert!(overlay.catalog().is_ok());
    }

    #[test]
    fn competing_overlays_claim_distinct_ids_and_publish_without_replacement() {
        let base: Arc<dyn CatalogManager> = Arc::new(MemoryCatalog::empty());
        let first = Arc::new(CatalogOverlay::new(base.clone()));
        let second = Arc::new(CatalogOverlay::new(base.clone()));
        let barrier = Arc::new(Barrier::new(3));
        let first_task = {
            let first = first.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                first.apply(|catalog| {
                    catalog.create_table(
                        "first".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
            })
        };
        let second_task = {
            let second = second.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                second.apply(|catalog| {
                    catalog.create_table(
                        "second".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
            })
        };
        barrier.wait();
        let first_result = first_task.join().unwrap();
        let second_result = second_task.join().unwrap();
        let first_table = match first_result {
            Ok(table) => table,
            Err(error) if error.code == common::SqlState::SerializationFailure => first
                .apply(|catalog| {
                    catalog.create_table(
                        "first".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
                .unwrap(),
            Err(error) => panic!("unexpected first overlay error: {error}"),
        };
        let second_table = match second_result {
            Ok(table) => table,
            Err(error) if error.code == common::SqlState::SerializationFailure => second
                .apply(|catalog| {
                    catalog.create_table(
                        "second".to_string(),
                        vec![column()],
                        Vec::new(),
                        CompressionSetting::None,
                    )
                })
                .unwrap(),
            Err(error) => panic!("unexpected second overlay error: {error}"),
        };
        assert_ne!(first_table.id, second_table.id);
        assert_ne!(first_table.storage_id, second_table.storage_id);

        first.publish().unwrap();
        second.publish().unwrap();
        assert!(base.get_table_by_name("first").unwrap().is_some());
        assert!(base.get_table_by_name("second").unwrap().is_some());
    }

    #[test]
    fn rollback_to_savepoint_restores_delta_without_reusing_ids() {
        let base: Arc<dyn CatalogManager> = Arc::new(MemoryCatalog::empty());
        let overlay = CatalogOverlay::new(base.clone());
        let savepoint = overlay.savepoint().unwrap();
        let created = overlay
            .apply(|catalog| {
                catalog.create_table(
                    "discarded".to_string(),
                    vec![column()],
                    Vec::new(),
                    CompressionSetting::None,
                )
            })
            .unwrap();
        overlay.rollback_to(&savepoint).unwrap();
        assert!(
            overlay
                .catalog()
                .unwrap()
                .get_table(created.id)
                .unwrap()
                .is_none()
        );

        let next = overlay
            .apply(|catalog| {
                catalog.create_table(
                    "next".to_string(),
                    vec![column()],
                    Vec::new(),
                    CompressionSetting::None,
                )
            })
            .unwrap();
        assert!(next.id > created.id);
    }

    #[test]
    fn rollback_to_savepoint_does_not_reuse_stable_column_ids() {
        let concrete = Arc::new(MemoryCatalog::empty());
        let table = concrete
            .create_table(
                "items".to_string(),
                vec![column()],
                Vec::new(),
                CompressionSetting::None,
            )
            .unwrap();
        let base: Arc<dyn CatalogManager> = concrete;
        let overlay = CatalogOverlay::new(base);
        let savepoint = overlay.savepoint().unwrap();
        let discarded = overlay
            .apply(|catalog| {
                let mut added = column();
                added.name = "discarded".to_string();
                catalog.add_table_column(table.id, added)
            })
            .unwrap();
        let discarded_id = discarded.columns[1].object_id;

        overlay.rollback_to(&savepoint).unwrap();
        let replacement = overlay
            .apply(|catalog| {
                let mut added = column();
                added.name = "replacement".to_string();
                catalog.add_table_column(table.id, added)
            })
            .unwrap();

        assert!(replacement.columns[1].object_id > discarded_id);
    }

    #[test]
    fn oversized_catalog_is_rejected_before_recording_overlay_delta() {
        let base: Arc<dyn CatalogManager> = Arc::new(MemoryCatalog::empty());
        let overlay = CatalogOverlay::new(base.clone());
        let check = StoredExpression {
            version: STORED_EXPRESSION_VERSION,
            sql: "x".repeat(MAX_STORED_EXPRESSION_SQL_BYTES),
            root: StoredExpr::Literal {
                value: Value::Boolean(true),
                data_type: DataType::Boolean,
                nullable: false,
            },
            data_type: DataType::Boolean,
            pg_type: None,
            nullable: false,
        };
        let checks = (0..65).map(|_| check.clone()).collect();

        let error = overlay
            .apply(|catalog| {
                catalog.create_table_with_options(
                    "too_large".to_string(),
                    vec![column()],
                    Vec::new(),
                    CompressionSetting::None,
                    ToastOptions::legacy_catalog_default(),
                    checks,
                )
            })
            .unwrap_err();

        assert_eq!(error.code, SqlState::ProgramLimitExceeded);
        assert!(overlay.is_empty().unwrap());
        assert!(base.get_table_by_name("too_large").unwrap().is_none());
    }
}
