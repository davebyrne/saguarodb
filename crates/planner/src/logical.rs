use common::{ColumnId, ColumnInfo, DbError, IndexId, ParsedColumnDef, Result, TableId};

use crate::{
    AggregateExpr, BoundExpr, BoundFrom, BoundInsertSource, BoundOrderByItem, BoundSelect,
    BoundStatement, JoinType,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalPlan {
    CreateTable {
        name: String,
        columns: Vec<ParsedColumnDef>,
        primary_key: Vec<String>,
    },
    DropTable {
        table: TableId,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
    DropIndex {
        index: IndexId,
    },
    Insert {
        table: TableId,
        columns: Vec<ColumnId>,
        source: Box<LogicalPlan>,
    },
    Update {
        table: TableId,
        assignments: Vec<(ColumnId, BoundExpr)>,
        source: Box<LogicalPlan>,
    },
    Delete {
        table: TableId,
        source: Box<LogicalPlan>,
    },
    Scan {
        table: TableId,
        filter: Option<BoundExpr>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Option<BoundExpr>,
        join_type: JoinType,
    },
    Filter {
        source: Box<LogicalPlan>,
        predicate: BoundExpr,
    },
    Projection {
        source: Box<LogicalPlan>,
        expressions: Vec<BoundExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Sort {
        source: Box<LogicalPlan>,
        order_by: Vec<BoundOrderByItem>,
    },
    /// De-duplicate rows by `on_keys`, keeping the first row of each distinct
    /// key in input order. For plain `SELECT DISTINCT`, `on_keys` are the output
    /// (projection) expressions, so whole rows are de-duplicated.
    Distinct {
        source: Box<LogicalPlan>,
        on_keys: Vec<BoundExpr>,
    },
    Limit {
        source: Box<LogicalPlan>,
        count: u64,
        offset: Option<u64>,
    },
    Aggregate {
        source: Box<LogicalPlan>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Vec<ColumnInfo>,
    },
    Values {
        rows: Vec<Vec<BoundExpr>>,
        output_schema: Vec<ColumnInfo>,
    },
}

pub fn logical_plan(bound: &BoundStatement) -> Result<LogicalPlan> {
    let plan = build_logical_plan(bound)?;
    Ok(crate::simplify::simplify_logical(plan))
}

fn build_logical_plan(bound: &BoundStatement) -> Result<LogicalPlan> {
    match bound {
        BoundStatement::CreateTable {
            name,
            columns,
            primary_key,
        } => Ok(LogicalPlan::CreateTable {
            name: name.clone(),
            columns: columns.clone(),
            primary_key: primary_key.clone(),
        }),
        BoundStatement::DropTable { table } => Ok(LogicalPlan::DropTable { table: *table }),
        BoundStatement::CreateIndex {
            name,
            table,
            columns,
            unique,
        } => Ok(LogicalPlan::CreateIndex {
            name: name.clone(),
            table: table.clone(),
            columns: columns.clone(),
            unique: *unique,
        }),
        BoundStatement::DropIndex { index } => Ok(LogicalPlan::DropIndex { index: *index }),
        BoundStatement::Insert {
            table,
            columns,
            source,
        } => {
            let source = match source {
                BoundInsertSource::Values {
                    rows,
                    output_schema,
                } => LogicalPlan::Values {
                    rows: rows.clone(),
                    output_schema: output_schema.clone(),
                },
                BoundInsertSource::Query(select) => plan_select(select)?,
            };
            Ok(LogicalPlan::Insert {
                table: *table,
                columns: columns.clone(),
                source: Box::new(source),
            })
        }
        BoundStatement::Select(select) => plan_select(select),
        BoundStatement::Update {
            table,
            assignments,
            source,
        } => Ok(LogicalPlan::Update {
            table: *table,
            assignments: assignments.clone(),
            source: Box::new(plan_select_source(source)?),
        }),
        BoundStatement::Delete { table, source } => Ok(LogicalPlan::Delete {
            table: *table,
            source: Box::new(plan_select_source(source)?),
        }),
        BoundStatement::Explain(_) => Err(DbError::plan(
            common::SqlState::SyntaxError,
            "logical_plan does not accept EXPLAIN; plan the inner statement",
        )),
        // COPY is not lowered to a logical plan; the server drives it over the
        // COPY sub-protocol, reusing the storage insert/scan paths. Reaching here
        // is a routing bug.
        BoundStatement::Copy { .. } => Err(DbError::internal(
            "logical_plan does not accept COPY; the server drives it directly",
        )),
    }
}

fn plan_select(select: &BoundSelect) -> Result<LogicalPlan> {
    let mut plan = plan_select_source(select)?;

    let aggregate_context = !select.group_by.is_empty()
        || select
            .columns
            .iter()
            .any(|item| contains_aggregate(&item.expr))
        || select.having.is_some()
        || select
            .order_by
            .iter()
            .any(|item| contains_aggregate(&item.expr));

    if aggregate_context {
        let mut aggregates = Vec::new();
        for item in &select.columns {
            collect_aggregates(&item.expr, &mut aggregates);
        }
        if let Some(having) = &select.having {
            collect_aggregates(having, &mut aggregates);
        }
        for item in &select.order_by {
            collect_aggregates(&item.expr, &mut aggregates);
        }

        let output_schema = aggregate_output_schema(&select.group_by, &aggregates);
        plan = LogicalPlan::Aggregate {
            source: Box::new(plan),
            group_by: select.group_by.clone(),
            aggregates: aggregates.clone(),
            output_schema,
        };

        if let Some(having) = &select.having {
            plan = LogicalPlan::Filter {
                source: Box::new(plan),
                predicate: rewrite_aggregate_expr(having, &select.group_by, &aggregates)?,
            };
        }

        if !select.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                source: Box::new(plan),
                order_by: select
                    .order_by
                    .iter()
                    .map(|item| {
                        Ok(BoundOrderByItem {
                            expr: rewrite_aggregate_expr(
                                &item.expr,
                                &select.group_by,
                                &aggregates,
                            )?,
                            ascending: item.ascending,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            };
        }

        let expressions = select
            .columns
            .iter()
            .map(|item| rewrite_aggregate_expr(&item.expr, &select.group_by, &aggregates))
            .collect::<Result<Vec<_>>>()?;
        plan = apply_distinct_and_projection(plan, select, expressions);
    } else {
        if !select.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                source: Box::new(plan),
                order_by: select.order_by.clone(),
            };
        }

        let expressions = select
            .columns
            .iter()
            .map(|item| item.expr.clone())
            .collect();
        plan = apply_distinct_and_projection(plan, select, expressions);
    }

    if let Some(limit) = select.limit {
        plan = LogicalPlan::Limit {
            source: Box::new(plan),
            count: limit,
            offset: select.offset,
        };
    } else if let Some(offset) = select.offset {
        plan = LogicalPlan::Limit {
            source: Box::new(plan),
            count: u64::MAX,
            offset: Some(offset),
        };
    }

    Ok(plan)
}

/// Stack the optional `Distinct` node below the `Projection`. `Distinct` sits
/// between any `Sort` and the `Projection`, so that after sorting, keeping the
/// first row per distinct key yields correctly ordered distinct output. For
/// plain `SELECT DISTINCT` the dedup keys are the projection expressions, so
/// whole output rows are compared.
fn apply_distinct_and_projection(
    mut plan: LogicalPlan,
    select: &BoundSelect,
    expressions: Vec<BoundExpr>,
) -> LogicalPlan {
    if select.distinct {
        plan = LogicalPlan::Distinct {
            source: Box::new(plan),
            on_keys: expressions.clone(),
        };
    }
    LogicalPlan::Projection {
        source: Box::new(plan),
        expressions,
        output_schema: select.output_schema.clone(),
    }
}

fn plan_select_source(select: &BoundSelect) -> Result<LogicalPlan> {
    plan_from(&select.from, select.filter.clone())
}

fn plan_from(from: &BoundFrom, filter: Option<BoundExpr>) -> Result<LogicalPlan> {
    match from {
        BoundFrom::Table { table, .. } => Ok(LogicalPlan::Scan {
            table: *table,
            filter,
        }),
        BoundFrom::Join {
            left,
            right,
            condition,
            join_type,
        } => {
            let mut plan = LogicalPlan::Join {
                left: Box::new(plan_from(left, None)?),
                right: Box::new(plan_from(right, None)?),
                condition: condition.clone(),
                join_type: *join_type,
            };
            if let Some(filter) = filter {
                plan = LogicalPlan::Filter {
                    source: Box::new(plan),
                    predicate: filter,
                };
            }
            Ok(plan)
        }
    }
}

fn aggregate_output_schema(
    group_by: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> Vec<ColumnInfo> {
    let mut output = Vec::with_capacity(group_by.len() + aggregates.len());
    for (index, expr) in group_by.iter().enumerate() {
        output.push(ColumnInfo {
            name: format!("group_{index}"),
            data_type: expr.data_type(),
            table_id: None,
            column_id: None,
        });
    }
    for (index, aggregate) in aggregates.iter().enumerate() {
        output.push(ColumnInfo {
            name: format!("aggregate_{index}"),
            data_type: aggregate.data_type.clone(),
            table_id: None,
            column_id: None,
        });
    }
    output
}

fn collect_aggregates(expr: &BoundExpr, output: &mut Vec<AggregateExpr>) {
    match expr {
        BoundExpr::AggregateCall {
            func,
            arg,
            distinct,
            data_type,
            nullable,
        } => {
            let aggregate = AggregateExpr {
                func: *func,
                arg: arg.as_deref().cloned(),
                distinct: *distinct,
                data_type: data_type.clone(),
                nullable: *nullable,
            };
            if !output.iter().any(|existing| existing == &aggregate) {
                output.push(aggregate);
            }
        }
        BoundExpr::BinaryOp { left, right, .. } => {
            collect_aggregates(left, output);
            collect_aggregates(right, output);
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => collect_aggregates(expr, output),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                collect_aggregates(arg, output);
            }
        }
        BoundExpr::InList { expr, list, .. } => {
            collect_aggregates(expr, output);
            for item in list {
                collect_aggregates(item, output);
            }
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            collect_aggregates(expr, output);
            collect_aggregates(low, output);
            collect_aggregates(high, output);
        }
        BoundExpr::Like { expr, pattern, .. } => {
            collect_aggregates(expr, output);
            collect_aggregates(pattern, output);
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                collect_aggregates(operand, output);
            }
            for (when, then) in when_clauses {
                collect_aggregates(when, output);
                collect_aggregates(then, output);
            }
            if let Some(else_clause) = else_clause {
                collect_aggregates(else_clause, output);
            }
        }
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. } => {}
    }
}

fn rewrite_aggregate_expr(
    expr: &BoundExpr,
    group_by: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> Result<BoundExpr> {
    if let Some(index) = group_by.iter().position(|group| group == expr) {
        return Ok(BoundExpr::LocalRef {
            slot: index,
            data_type: expr.data_type(),
            nullable: expr.nullable(),
        });
    }

    if let BoundExpr::AggregateCall {
        func,
        arg,
        distinct,
        data_type,
        nullable,
    } = expr
    {
        let aggregate = AggregateExpr {
            func: *func,
            arg: arg.as_deref().cloned(),
            distinct: *distinct,
            data_type: data_type.clone(),
            nullable: *nullable,
        };
        let index = aggregates
            .iter()
            .position(|existing| existing == &aggregate)
            .ok_or_else(|| DbError::internal("aggregate expression was not extracted"))?;
        return Ok(BoundExpr::LocalRef {
            slot: group_by.len() + index,
            data_type: data_type.clone(),
            nullable: *nullable,
        });
    }

    match expr {
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            nullable,
        } => Ok(BoundExpr::BinaryOp {
            left: Box::new(rewrite_aggregate_expr(left, group_by, aggregates)?),
            op: *op,
            right: Box::new(rewrite_aggregate_expr(right, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::UnaryOp {
            op,
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Function {
            name,
            args,
            data_type,
            nullable,
        } => Ok(BoundExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| rewrite_aggregate_expr(arg, group_by, aggregates))
                .collect::<Result<Vec<_>>>()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::IsNull {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::IsNotNull {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::InList {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            list: list
                .iter()
                .map(|item| rewrite_aggregate_expr(item, group_by, aggregates))
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::Between {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            low: Box::new(rewrite_aggregate_expr(low, group_by, aggregates)?),
            high: Box::new(rewrite_aggregate_expr(high, group_by, aggregates)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            data_type,
            nullable,
        } => Ok(BoundExpr::Like {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            pattern: Box::new(rewrite_aggregate_expr(pattern, group_by, aggregates)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable,
        } => Ok(BoundExpr::Case {
            operand: operand
                .as_deref()
                .map(|expr| rewrite_aggregate_expr(expr, group_by, aggregates).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| {
                    Ok((
                        rewrite_aggregate_expr(when, group_by, aggregates)?,
                        rewrite_aggregate_expr(then, group_by, aggregates)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_deref()
                .map(|expr| rewrite_aggregate_expr(expr, group_by, aggregates).map(Box::new))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Cast {
            expr,
            data_type,
            nullable,
        } => Ok(BoundExpr::Cast {
            expr: Box::new(rewrite_aggregate_expr(expr, group_by, aggregates)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        }),
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. } => Ok(expr.clone()),
        BoundExpr::AggregateCall { .. } => Err(DbError::internal(
            "nested aggregate survived binder validation",
        )),
    }
}

fn contains_aggregate(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::AggregateCall { .. } => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => contains_aggregate(expr),
        BoundExpr::Function { args, .. } => args.iter().any(contains_aggregate),
        BoundExpr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        BoundExpr::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || when_clauses
                    .iter()
                    .any(|(when, then)| contains_aggregate(when) || contains_aggregate(then))
                || else_clause.as_deref().is_some_and(contains_aggregate)
        }
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. } => false,
    }
}
