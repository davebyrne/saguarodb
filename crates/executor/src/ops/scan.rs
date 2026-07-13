use common::{
    ColumnInfo, DbError, ExecRow, IndexId, KeyRange, PRIMARY_KEY_INDEX_ID, Result, RowIdentity,
    SqlState, StatementContext, StoredRow, TableId,
};
use planner::BoundExpr;
use std::sync::Arc;
use storage::{RelationSnapshot, RowIterator, StorageEngine};

use crate::ops::predicate_matches;
use crate::query::PlanExecutor;

pub struct SeqScanOp<'a> {
    ctx: StatementContext,
    relations: Arc<dyn RelationSnapshot>,
    storage: &'a dyn StorageEngine,
    table: TableId,
    filter: Option<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    iter: Option<Box<dyn RowIterator>>,
}

impl<'a> SeqScanOp<'a> {
    pub fn new(
        ctx: StatementContext,
        relations: Arc<dyn RelationSnapshot>,
        storage: &'a dyn StorageEngine,
        table: TableId,
        filter: Option<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            ctx,
            relations,
            storage,
            table,
            filter,
            output_schema,
            iter: None,
        }
    }
}

impl PlanExecutor for SeqScanOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.iter = Some(
            match self
                .storage
                .scan(&self.ctx, self.relations.as_ref(), self.table)
            {
                Ok(iter) => iter,
                Err(err) if missing_table_is_empty(self.relations.as_ref(), &err) => {
                    empty_iterator(&self.output_schema)
                }
                Err(err) => return Err(err),
            },
        );
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let iter = self
            .iter
            .as_mut()
            .ok_or_else(|| common::DbError::internal("SeqScanOp was not opened"))?;
        while let Some(stored) = iter.next()? {
            self.ctx.cancel.check()?;
            let row = ExecRow {
                row: stored.row,
                identity: Some(RowIdentity {
                    row_id: stored.row_id,
                    xmin: stored.xmin,
                    key: stored.key,
                }),
            };
            if self
                .filter
                .as_ref()
                .map(|filter| predicate_matches(&self.ctx, filter, &row))
                .transpose()?
                .unwrap_or(true)
            {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.iter = None;
        Ok(())
    }
}

pub struct IndexScanOp<'a> {
    ctx: StatementContext,
    relations: Arc<dyn RelationSnapshot>,
    storage: &'a dyn StorageEngine,
    table: TableId,
    index: IndexId,
    range: KeyRange,
    full_filter: Option<BoundExpr>,
    filter: Option<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    iter: Option<Box<dyn RowIterator>>,
}

pub(crate) struct IndexScanInput<'a> {
    pub(crate) ctx: StatementContext,
    pub(crate) relations: Arc<dyn RelationSnapshot>,
    pub(crate) storage: &'a dyn StorageEngine,
    pub(crate) table: TableId,
    pub(crate) index: IndexId,
    pub(crate) range: KeyRange,
    pub(crate) full_filter: Option<BoundExpr>,
    pub(crate) filter: Option<BoundExpr>,
    pub(crate) output_schema: Vec<ColumnInfo>,
}

impl<'a> IndexScanOp<'a> {
    pub(crate) fn new(input: IndexScanInput<'a>) -> Self {
        let IndexScanInput {
            ctx,
            relations,
            storage,
            table,
            index,
            range,
            full_filter,
            filter,
            output_schema,
        } = input;

        Self {
            ctx,
            relations,
            storage,
            table,
            index,
            range,
            full_filter,
            filter,
            output_schema,
            iter: None,
        }
    }
}

impl PlanExecutor for IndexScanOp<'_> {
    fn output_schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        // The primary-key index resolves to a row location directly; a secondary
        // index resolves each entry's primary key through the primary-key index.
        let iter = if self.index == PRIMARY_KEY_INDEX_ID {
            match self.storage.scan_range(
                &self.ctx,
                self.relations.as_ref(),
                self.table,
                &self.range,
            ) {
                Ok(iter) => iter,
                Err(err) if missing_table_is_empty(self.relations.as_ref(), &err) => {
                    empty_iterator(&self.output_schema)
                }
                Err(err) => return Err(err),
            }
        } else {
            match self.storage.index_scan(
                &self.ctx,
                self.relations.as_ref(),
                self.table,
                self.index,
                &self.range,
            ) {
                Ok(iter) => iter,
                Err(err) if err.code == SqlState::UndefinedTable => {
                    self.filter = self.full_filter.clone();
                    match self
                        .storage
                        .scan(&self.ctx, self.relations.as_ref(), self.table)
                    {
                        Ok(iter) => iter,
                        Err(err) if missing_table_is_empty(self.relations.as_ref(), &err) => {
                            empty_iterator(&self.output_schema)
                        }
                        Err(err) => return Err(err),
                    }
                }
                Err(err) => return Err(err),
            }
        };
        self.iter = Some(iter);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let iter = self
            .iter
            .as_mut()
            .ok_or_else(|| common::DbError::internal("IndexScanOp was not opened"))?;
        while let Some(stored) = iter.next()? {
            self.ctx.cancel.check()?;
            let row = ExecRow {
                row: stored.row,
                identity: Some(RowIdentity {
                    row_id: stored.row_id,
                    xmin: stored.xmin,
                    key: stored.key,
                }),
            };
            if self
                .filter
                .as_ref()
                .map(|filter| predicate_matches(&self.ctx, filter, &row))
                .transpose()?
                .unwrap_or(true)
            {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.iter = None;
        Ok(())
    }
}

fn missing_table_is_empty(relations: &dyn RelationSnapshot, err: &DbError) -> bool {
    err.code == SqlState::UndefinedTable && relations.missing_tables_are_empty()
}

fn empty_iterator(output_schema: &[ColumnInfo]) -> Box<dyn RowIterator> {
    Box::new(EmptyRowIterator {
        schema: output_schema.to_vec(),
    })
}

struct EmptyRowIterator {
    schema: Vec<ColumnInfo>,
}

impl RowIterator for EmptyRowIterator {
    fn next(&mut self) -> Result<Option<StoredRow>> {
        Ok(None)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }
}
