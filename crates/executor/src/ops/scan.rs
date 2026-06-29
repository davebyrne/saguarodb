use common::{
    ColumnInfo, ExecRow, IndexId, KeyRange, PRIMARY_KEY_INDEX_ID, Result, RowIdentity,
    StatementContext, TableId,
};
use planner::BoundExpr;
use storage::{RowIterator, StorageEngine};

use crate::ops::predicate_matches;
use crate::query::PlanExecutor;

pub struct SeqScanOp<'a> {
    ctx: StatementContext,
    storage: &'a dyn StorageEngine,
    table: TableId,
    filter: Option<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    iter: Option<Box<dyn RowIterator>>,
}

impl<'a> SeqScanOp<'a> {
    pub fn new(
        ctx: StatementContext,
        storage: &'a dyn StorageEngine,
        table: TableId,
        filter: Option<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            ctx,
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
        self.iter = Some(self.storage.scan(&self.ctx, self.table)?);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<ExecRow>> {
        let iter = self
            .iter
            .as_mut()
            .ok_or_else(|| common::DbError::internal("SeqScanOp was not opened"))?;
        while let Some(stored) = iter.next()? {
            let row = ExecRow {
                row: stored.row,
                identity: Some(RowIdentity {
                    row_id: stored.row_id,
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
    storage: &'a dyn StorageEngine,
    table: TableId,
    index: IndexId,
    range: KeyRange,
    filter: Option<BoundExpr>,
    output_schema: Vec<ColumnInfo>,
    iter: Option<Box<dyn RowIterator>>,
}

impl<'a> IndexScanOp<'a> {
    pub fn new(
        ctx: StatementContext,
        storage: &'a dyn StorageEngine,
        table: TableId,
        index: IndexId,
        range: KeyRange,
        filter: Option<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            ctx,
            storage,
            table,
            index,
            range,
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
            self.storage
                .scan_range(&self.ctx, self.table, &self.range)?
        } else {
            self.storage
                .index_scan(&self.ctx, self.table, self.index, &self.range)?
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
            let row = ExecRow {
                row: stored.row,
                identity: Some(RowIdentity {
                    row_id: stored.row_id,
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
