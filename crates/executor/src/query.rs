use catalog::CatalogManager;
use common::{
    ColumnId, ColumnInfo, DataType, DbError, ExecRow, ParsedColumnDef, Result, Row, SqlState,
    StatementContext, TableId, TableSchema, Value,
};
use planner::PhysicalPlan;
use storage::{SchemaOperations, StorageEngine};

use crate::ExecutionResult;
use crate::eval_expr;
use crate::ops::{
    AggregateOp, FilterOp, IndexScanOp, LimitOp, NestedLoopJoinOp, ProjectionOp, SeqScanOp, SortOp,
    ValuesOp,
};

pub struct ExecutionContext<'a> {
    pub statement: StatementContext,
    pub catalog: &'a dyn CatalogManager,
    pub storage: &'a dyn StorageEngine,
    pub schema_ops: &'a dyn SchemaOperations,
}

pub trait PlanExecutor {
    fn output_schema(&self) -> &[ColumnInfo];
    fn open(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<Option<ExecRow>>;
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<ExecRow>> {
        let mut rows = Vec::new();
        for _ in 0..max_rows {
            let Some(row) = self.next()? else {
                break;
            };
            rows.push(row);
        }
        Ok(rows)
    }
    fn close(&mut self) -> Result<()>;
}

pub struct QueryEngine;

impl QueryEngine {
    pub fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        plan: &PhysicalPlan,
    ) -> Result<ExecutionResult> {
        match plan {
            PhysicalPlan::CreateTable {
                name,
                columns,
                primary_key,
            } => execute_create_table(ctx, name, columns, primary_key),
            PhysicalPlan::DropTable { table } => execute_drop_table(ctx, *table),
            PhysicalPlan::Insert {
                table,
                columns,
                source,
            } => execute_insert(ctx, *table, columns, source),
            PhysicalPlan::Update {
                table,
                assignments,
                source,
            } => execute_update(ctx, *table, assignments, source),
            PhysicalPlan::Delete { table, source } => execute_delete(ctx, *table, source),
            _ => execute_query(ctx, plan),
        }
    }
}

pub(crate) fn build_executor<'a>(
    ctx: &'a ExecutionContext<'_>,
    plan: &PhysicalPlan,
) -> Result<Box<dyn PlanExecutor + 'a>> {
    match plan {
        PhysicalPlan::SeqScan { table, filter, .. } => Ok(Box::new(SeqScanOp::new(
            ctx.statement,
            ctx.storage,
            *table,
            filter.clone(),
            table_output_schema(ctx.catalog, *table)?,
        ))),
        PhysicalPlan::IndexScan {
            table,
            range,
            filter,
            ..
        } => Ok(Box::new(IndexScanOp::new(
            ctx.statement,
            ctx.storage,
            *table,
            range.clone(),
            filter.clone(),
            table_output_schema(ctx.catalog, *table)?,
        ))),
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
        } => {
            let left = build_executor(ctx, left)?;
            let right = build_executor(ctx, right)?;
            Ok(Box::new(NestedLoopJoinOp::new(
                left,
                right,
                condition.clone(),
                *join_type,
            )))
        }
        PhysicalPlan::Filter { source, predicate } => Ok(Box::new(FilterOp::new(
            build_executor(ctx, source)?,
            predicate.clone(),
        ))),
        PhysicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => Ok(Box::new(ProjectionOp::new(
            build_executor(ctx, source)?,
            expressions.clone(),
            output_schema.clone(),
        ))),
        PhysicalPlan::Sort { source, order_by } => Ok(Box::new(SortOp::new(
            build_executor(ctx, source)?,
            order_by.clone(),
        ))),
        PhysicalPlan::Limit {
            source,
            count,
            offset,
        } => Ok(Box::new(LimitOp::new(
            build_executor(ctx, source)?,
            *count,
            offset.unwrap_or(0),
        ))),
        PhysicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => Ok(Box::new(AggregateOp::new(
            build_executor(ctx, source)?,
            group_by.clone(),
            aggregates.clone(),
            output_schema.clone(),
        ))),
        PhysicalPlan::Values {
            rows,
            output_schema,
        } => Ok(Box::new(ValuesOp::new(rows.clone(), output_schema.clone()))),
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. } => Err(DbError::internal(
            "DML and DDL plans are not valid executor sources",
        )),
    }
}

fn execute_query(ctx: &ExecutionContext<'_>, plan: &PhysicalPlan) -> Result<ExecutionResult> {
    let mut executor = build_executor(ctx, plan)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let columns = executor.output_schema().to_vec();
        let mut rows = Vec::new();
        while let Some(row) = executor.next()? {
            rows.push(row.row);
        }
        Ok(ExecutionResult::Query { columns, rows })
    })();
    close_after(executor.as_mut(), result)
}

fn execute_insert(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    columns: &[ColumnId],
    source: &PhysicalPlan,
) -> Result<ExecutionResult> {
    let schema = require_table(ctx.catalog, table)?;
    let mut executor = build_executor(ctx, source)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let mut count = 0;
        while let Some(source_row) = executor.next()? {
            if source_row.row.values.len() != columns.len() {
                return Err(DbError::execute(
                    SqlState::DatatypeMismatch,
                    "INSERT source produced the wrong number of values",
                ));
            }
            let mut values = vec![Value::Null; schema.columns.len()];
            for (column, value) in columns.iter().zip(source_row.row.values) {
                let slot = column_slot(&schema, *column)?;
                validate_value_type(&schema.columns[slot], &value)?;
                values[slot] = value;
            }
            validate_not_null(&schema, &values)?;
            ctx.storage.insert(&ctx.statement, table, Row { values })?;
            count += 1;
        }

        Ok(ExecutionResult::Modified {
            command: "INSERT".to_string(),
            count,
        })
    })();
    close_after(executor.as_mut(), result)
}

fn execute_update(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    assignments: &[(ColumnId, planner::BoundExpr)],
    source: &PhysicalPlan,
) -> Result<ExecutionResult> {
    let schema = require_table(ctx.catalog, table)?;
    let mut executor = build_executor(ctx, source)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let mut count = 0;
        while let Some(source_row) = executor.next()? {
            let identity = source_row.identity.clone().ok_or_else(|| {
                DbError::internal("UPDATE source row did not include storage identity")
            })?;
            let mut values = source_row.row.values.clone();
            if values.len() != schema.columns.len() {
                return Err(DbError::internal(
                    "UPDATE source row shape does not match table schema",
                ));
            }
            for (column, expr) in assignments {
                let slot = column_slot(&schema, *column)?;
                values[slot] = eval_expr(expr, &source_row)?;
            }
            validate_not_null(&schema, &values)?;
            if ctx
                .storage
                .update(&ctx.statement, table, &identity.key, Row { values })?
            {
                count += 1;
            }
        }

        Ok(ExecutionResult::Modified {
            command: "UPDATE".to_string(),
            count,
        })
    })();
    close_after(executor.as_mut(), result)
}

fn execute_delete(
    ctx: &ExecutionContext<'_>,
    table: TableId,
    source: &PhysicalPlan,
) -> Result<ExecutionResult> {
    let mut executor = build_executor(ctx, source)?;
    open_executor(executor.as_mut())?;
    let result = (|| {
        let mut count = 0;
        while let Some(source_row) = executor.next()? {
            let identity = source_row.identity.ok_or_else(|| {
                DbError::internal("DELETE source row did not include storage identity")
            })?;
            if ctx.storage.delete(&ctx.statement, table, &identity.key)? {
                count += 1;
            }
        }

        Ok(ExecutionResult::Modified {
            command: "DELETE".to_string(),
            count,
        })
    })();
    close_after(executor.as_mut(), result)
}

fn execute_create_table(
    ctx: &ExecutionContext<'_>,
    name: &str,
    columns: &[ParsedColumnDef],
    primary_key: &[String],
) -> Result<ExecutionResult> {
    let schema =
        ctx.catalog
            .create_table(name.to_string(), columns.to_vec(), primary_key.to_vec())?;
    if let Err(err) = ctx.schema_ops.create_table(&ctx.statement, &schema) {
        let _ = ctx.catalog.drop_table(schema.id);
        return Err(err);
    }
    Ok(ExecutionResult::Modified {
        command: "CREATE TABLE".to_string(),
        count: 0,
    })
}

fn execute_drop_table(ctx: &ExecutionContext<'_>, table: TableId) -> Result<ExecutionResult> {
    ctx.schema_ops.drop_table(&ctx.statement, table)?;
    ctx.catalog.drop_table(table)?;
    Ok(ExecutionResult::Modified {
        command: "DROP TABLE".to_string(),
        count: 0,
    })
}

fn table_output_schema(catalog: &dyn CatalogManager, table: TableId) -> Result<Vec<ColumnInfo>> {
    Ok(require_table(catalog, table)?
        .columns
        .into_iter()
        .map(|column| ColumnInfo {
            name: column.name,
            data_type: column.data_type,
            table_id: Some(table),
            column_id: Some(column.id),
        })
        .collect())
}

fn require_table(catalog: &dyn CatalogManager, table: TableId) -> Result<TableSchema> {
    catalog.get_table(table)?.ok_or_else(|| {
        DbError::execute(
            SqlState::UndefinedTable,
            format!("table id {table} does not exist"),
        )
    })
}

fn column_slot(schema: &TableSchema, column: ColumnId) -> Result<usize> {
    schema
        .columns
        .iter()
        .position(|candidate| candidate.id == column)
        .ok_or_else(|| {
            DbError::execute(
                SqlState::UndefinedColumn,
                format!("column id {column} does not exist"),
            )
        })
}

fn validate_not_null(schema: &TableSchema, values: &[Value]) -> Result<()> {
    for (column, value) in schema.columns.iter().zip(values) {
        if !column.nullable && matches!(value, Value::Null) {
            return Err(DbError::execute(
                SqlState::NotNullViolation,
                format!("column {} cannot be NULL", column.name),
            ));
        }
    }
    Ok(())
}

fn validate_value_type(column: &common::ColumnDef, value: &Value) -> Result<()> {
    if matches!(value, Value::Null) {
        return Ok(());
    }
    let matches_type = matches!(
        (&column.data_type, value),
        (DataType::Integer, Value::Integer(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Boolean, Value::Boolean(_))
    );
    if matches_type {
        return Ok(());
    }
    Err(DbError::execute(
        SqlState::DatatypeMismatch,
        format!(
            "expected column {} to receive {:?}, got {:?}",
            column.name, column.data_type, value
        ),
    ))
}

pub(crate) fn collect_all(source: &mut dyn PlanExecutor) -> Result<Vec<ExecRow>> {
    open_executor(source)?;
    let result = (|| {
        let mut rows = Vec::new();
        while let Some(row) = source.next()? {
            rows.push(row);
        }
        Ok(rows)
    })();
    close_after(source, result)
}

fn open_executor(executor: &mut dyn PlanExecutor) -> Result<()> {
    if let Err(err) = executor.open() {
        let _ = executor.close();
        return Err(err);
    }
    Ok(())
}

fn close_after<T>(executor: &mut dyn PlanExecutor, result: Result<T>) -> Result<T> {
    let close_result = executor.close();
    match (result, close_result) {
        (Err(err), _) => Err(err),
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
    }
}

#[allow(dead_code)]
fn _type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Integer => "INTEGER",
        DataType::Text => "TEXT",
        DataType::Boolean => "BOOLEAN",
    }
}
