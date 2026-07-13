use std::sync::Arc;

use common::{ColumnInfo, ExecRow, IsolationLevel, Result, RowIdentity, StatementContext, TableId};
use planner::BoundExpr;
use storage::{LockRowResult, RelationSnapshot, StorageEngine};

use crate::ops::{predicate_matches, project_row};
use crate::query::PlanExecutor;

pub struct LockRowsOp<'a> {
    ctx: StatementContext,
    relations: Arc<dyn RelationSnapshot>,
    storage: &'a dyn StorageEngine,
    source: Box<dyn PlanExecutor + 'a>,
    table: TableId,
    mode: common::TupleLockMode,
    wait_policy: common::TupleLockWaitPolicy,
    recheck: Option<BoundExpr>,
    expressions: Vec<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
}

pub struct LockRowsInput<'a> {
    pub ctx: StatementContext,
    pub relations: Arc<dyn RelationSnapshot>,
    pub storage: &'a dyn StorageEngine,
    pub source: Box<dyn PlanExecutor + 'a>,
    pub table: TableId,
    pub mode: common::TupleLockMode,
    pub wait_policy: common::TupleLockWaitPolicy,
    pub recheck: Option<BoundExpr>,
    pub expressions: Vec<BoundExpr>,
    pub output_schema: Vec<ColumnInfo>,
}

impl<'a> LockRowsOp<'a> {
    pub fn new(input: LockRowsInput<'a>) -> Self {
        Self {
            ctx: input.ctx,
            relations: input.relations,
            storage: input.storage,
            source: input.source,
            table: input.table,
            mode: input.mode,
            wait_policy: input.wait_policy,
            recheck: input.recheck,
            expressions: input.expressions,
            output_schema: input.output_schema,
        }
    }
}

impl PlanExecutor for LockRowsOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.source.open()
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        while let Some(candidate) = self.source.next()? {
            self.ctx.cancel.check()?;
            let identity = candidate.identity.clone().ok_or_else(|| {
                common::DbError::internal("row-locking source did not preserve row identity")
            })?;
            let locked = match self.storage.lock_row(
                &self.ctx,
                self.relations.as_ref(),
                self.table,
                &identity,
                self.mode,
                self.wait_policy,
            )? {
                LockRowResult::Locked(locked) => locked,
                LockRowResult::Deleted | LockRowResult::Skipped => continue,
            };
            let latest = ExecRow {
                row: locked.row().clone(),
                identity: Some(RowIdentity {
                    row_id: locked.identity().row_id,
                    xmin: locked.identity().xmin,
                    key: locked.identity().key.clone(),
                }),
            };
            let advanced = locked.identity() != &identity;
            if advanced && self.ctx.isolation != IsolationLevel::ReadCommitted {
                return Err(common::DbError::execute(
                    common::SqlState::SerializationFailure,
                    "could not serialize access due to concurrent update",
                ));
            }
            if advanced
                && !self
                    .recheck
                    .as_ref()
                    .map(|predicate| predicate_matches(&self.ctx, predicate, &latest))
                    .transpose()?
                    .unwrap_or(true)
            {
                continue;
            }
            return project_row(&self.ctx, latest, &self.expressions).map(Some);
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.source.close()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use catalog::{CatalogManager, MemoryCatalog};
    use common::{DataType, Key, ParsedColumnDef, Row, SqlState, Value};
    use planner::BinOp;
    use storage::SchemaOperations;

    use crate::test_support::{MemoryStorage, memory_statement_context};

    use super::*;

    struct RowsOp {
        rows: VecDeque<ExecRow>,
    }

    impl PlanExecutor for RowsOp {
        fn output_schema(&self) -> &[ColumnInfo] {
            &[]
        }

        fn open(&mut self) -> Result<()> {
            Ok(())
        }

        fn next(&mut self) -> Result<Option<ExecRow>> {
            Ok(self.rows.pop_front())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn input_ref(slot: usize, data_type: DataType) -> BoundExpr {
        BoundExpr::InputRef {
            input: 0,
            column: slot as u16,
            slot,
            data_type,
            nullable: false,
        }
    }

    fn setup_updated_candidate() -> (MemoryStorage, Arc<dyn RelationSnapshot>, TableId, ExecRow) {
        let catalog = MemoryCatalog::empty();
        let schema = catalog
            .create_table(
                "users".to_string(),
                vec![
                    ParsedColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                    ParsedColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text,
                        nullable: false,
                        max_length: None,
                        default: None,
                        pg_type: None,
                    },
                ],
                vec!["id".to_string()],
                common::CompressionSetting::None,
            )
            .unwrap();
        let storage = MemoryStorage::empty();
        storage
            .create_table(&memory_statement_context(1), &schema)
            .unwrap();
        let relations = storage.capture_relation_snapshot().unwrap();
        storage
            .insert(
                &memory_statement_context(1),
                relations.as_ref(),
                schema.id,
                Row {
                    values: vec![Value::Integer(1), Value::Text("old".to_string())],
                },
            )
            .unwrap();
        storage.commit_txn(1).unwrap();

        let mut scan = storage
            .scan(&memory_statement_context(2), relations.as_ref(), schema.id)
            .unwrap();
        let old = scan.next().unwrap().unwrap();
        let candidate = ExecRow {
            row: old.row,
            identity: Some(RowIdentity {
                row_id: old.row_id,
                xmin: old.xmin,
                key: old.key,
            }),
        };
        storage
            .update(
                &memory_statement_context(3),
                relations.as_ref(),
                schema.id,
                &Key(vec![Value::Integer(1)]),
                Row {
                    values: vec![Value::Integer(1), Value::Text("new".to_string())],
                },
            )
            .unwrap();
        storage.commit_txn(3).unwrap();
        (storage, relations, schema.id, candidate)
    }

    fn lock_rows<'a>(
        storage: &'a MemoryStorage,
        relations: Arc<dyn RelationSnapshot>,
        table: TableId,
        candidate: ExecRow,
        recheck: BoundExpr,
    ) -> LockRowsOp<'a> {
        LockRowsOp::new(LockRowsInput {
            ctx: memory_statement_context(4),
            relations,
            storage,
            source: Box::new(RowsOp {
                rows: VecDeque::from([candidate]),
            }),
            table,
            mode: common::TupleLockMode::Update,
            wait_policy: common::TupleLockWaitPolicy::Block,
            recheck: Some(recheck),
            expressions: vec![input_ref(1, DataType::Text)],
            output_schema: vec![ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Text,
                table_id: Some(table),
                column_id: Some(1),
                pg_type: None,
            }],
        })
    }

    #[test]
    fn rechecks_predicate_against_latest_locked_version() {
        let (storage, relations, table, candidate) = setup_updated_candidate();
        let recheck = BoundExpr::BinaryOp {
            left: Box::new(input_ref(1, DataType::Text)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal {
                value: Value::Text("old".to_string()),
                data_type: DataType::Text,
                nullable: false,
            }),
            data_type: DataType::Boolean,
            nullable: false,
        };
        let mut op = lock_rows(&storage, relations, table, candidate, recheck);
        op.open().unwrap();
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn projects_values_from_latest_locked_version() {
        let (storage, relations, table, candidate) = setup_updated_candidate();
        let recheck = BoundExpr::BinaryOp {
            left: Box::new(input_ref(0, DataType::Integer)),
            op: BinOp::Eq,
            right: Box::new(BoundExpr::Literal {
                value: Value::Integer(1),
                data_type: DataType::Integer,
                nullable: false,
            }),
            data_type: DataType::Boolean,
            nullable: false,
        };
        let mut op = lock_rows(&storage, relations, table, candidate, recheck);
        op.open().unwrap();
        assert_eq!(
            op.next().unwrap().unwrap().row.values,
            vec![Value::Text("new".to_string())]
        );
    }

    #[test]
    fn candidate_without_identity_is_an_internal_error() {
        let (storage, relations, table, mut candidate) = setup_updated_candidate();
        candidate.identity = None;
        let mut op = lock_rows(
            &storage,
            relations,
            table,
            candidate,
            BoundExpr::Literal {
                value: Value::Boolean(true),
                data_type: DataType::Boolean,
                nullable: false,
            },
        );
        op.open().unwrap();
        assert_eq!(op.next().unwrap_err().code, SqlState::InternalError);
    }

    #[test]
    fn unchanged_candidate_is_not_rechecked() {
        let (storage, relations, table, _) = setup_updated_candidate();
        let mut scan = storage
            .scan(&memory_statement_context(4), relations.as_ref(), table)
            .unwrap();
        let current = scan.next().unwrap().unwrap();
        let candidate = ExecRow {
            row: current.row,
            identity: Some(RowIdentity {
                row_id: current.row_id,
                xmin: current.xmin,
                key: current.key,
            }),
        };
        // The child represents a row that already passed the snapshot predicate.
        // A false stand-in makes any accidental second evaluation observable.
        let mut op = lock_rows(
            &storage,
            relations,
            table,
            candidate,
            BoundExpr::Literal {
                value: Value::Boolean(false),
                data_type: DataType::Boolean,
                nullable: false,
            },
        );
        op.open().unwrap();
        assert_eq!(
            op.next().unwrap().unwrap().row.values,
            vec![Value::Text("new".to_string())]
        );
    }

    #[test]
    fn retained_snapshot_isolation_rejects_a_successor() {
        let (storage, relations, table, candidate) = setup_updated_candidate();
        let mut op = lock_rows(
            &storage,
            relations,
            table,
            candidate,
            BoundExpr::Literal {
                value: Value::Boolean(true),
                data_type: DataType::Boolean,
                nullable: false,
            },
        );
        op.ctx.isolation = IsolationLevel::RepeatableRead;
        op.open().unwrap();
        assert_eq!(op.next().unwrap_err().code, SqlState::SerializationFailure);
    }
}
