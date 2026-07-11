//! Correlated-subquery hoisting (`docs/specs/subqueries.md` §5.1).
//!
//! A planner pass that lifts each correlated subquery expression out of the
//! expression tree into an `Apply` (dependent join) node above the
//! expression's input plan, replacing the expression with a `LocalRef` to the
//! column the Apply appends. Uncorrelated subqueries are untouched (the
//! executor's pre-pass resolves them to constants once per statement).
//!
//! Supported positions are `Filter` predicates (`WHERE` and `HAVING` both
//! lower to `Filter`, and DML `WHERE` lives in the DML source plan) and
//! `Projection` expressions (the `SELECT` list). A correlated subquery in any
//! other position is left in place for the executor's staging guard to reject
//! with `FeatureNotSupported`.

use catalog::CatalogManager;
use common::{DataType, DbError, Result};

use crate::BoundExpr;
use crate::expr::ApplyKind;
use crate::logical::LogicalPlan;
use crate::rewrite::rewrite_expr;

/// Hoist every correlated subquery in a supported position, recursing through
/// the whole plan (including DML sources, join children, and set-operation
/// arms). Subquery bodies planned here are hoisted in turn, so nested
/// correlation works at any depth.
pub(crate) fn hoist_correlated_subqueries(
    plan: LogicalPlan,
    catalog: &dyn CatalogManager,
) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Filter { source, predicate } => {
            let mut hoister = Hoister {
                source: Some(Box::new(hoist_correlated_subqueries(*source, catalog)?)),
                catalog,
            };
            let predicate = hoister.hoist_expr(&predicate)?;
            LogicalPlan::Filter {
                source: hoister.into_source(),
                predicate,
            }
        }
        LogicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => {
            let mut hoister = Hoister {
                source: Some(Box::new(hoist_correlated_subqueries(*source, catalog)?)),
                catalog,
            };
            let expressions = expressions
                .iter()
                .map(|expr| hoister.hoist_expr(expr))
                .collect::<Result<Vec<_>>>()?;
            LogicalPlan::Projection {
                source: hoister.into_source(),
                expressions,
                output_schema,
            }
        }
        // Structural recursion for every other node; expressions they carry
        // keep their correlated subqueries (unsupported positions) for the
        // executor's staging guard.
        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            default_exprs,
            check_exprs,
        } => LogicalPlan::Insert {
            table,
            columns,
            source: Box::new(hoist_correlated_subqueries(*source, catalog)?),
            on_conflict,
            returning,
            default_exprs,
            check_exprs,
        },
        LogicalPlan::Update {
            table,
            assignments,
            source,
            returning,
            check_exprs,
        } => LogicalPlan::Update {
            table,
            assignments,
            source: Box::new(restore_dml_source_shape(
                hoist_correlated_subqueries(*source, catalog)?,
                table,
                catalog,
            )?),
            returning,
            check_exprs,
        },
        LogicalPlan::Delete {
            table,
            source,
            returning,
        } => LogicalPlan::Delete {
            table,
            source: Box::new(restore_dml_source_shape(
                hoist_correlated_subqueries(*source, catalog)?,
                table,
                catalog,
            )?),
            returning,
        },
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => LogicalPlan::Join {
            left: Box::new(hoist_correlated_subqueries(*left, catalog)?),
            right: Box::new(hoist_correlated_subqueries(*right, catalog)?),
            condition,
            join_type,
        },
        LogicalPlan::Sort { source, order_by } => LogicalPlan::Sort {
            source: Box::new(hoist_correlated_subqueries(*source, catalog)?),
            order_by,
        },
        LogicalPlan::Distinct { source, on_keys } => LogicalPlan::Distinct {
            source: Box::new(hoist_correlated_subqueries(*source, catalog)?),
            on_keys,
        },
        LogicalPlan::Limit {
            source,
            count,
            offset,
        } => LogicalPlan::Limit {
            source: Box::new(hoist_correlated_subqueries(*source, catalog)?),
            count,
            offset,
        },
        LogicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => LogicalPlan::Aggregate {
            source: Box::new(hoist_correlated_subqueries(*source, catalog)?),
            group_by,
            aggregates,
            output_schema,
        },
        LogicalPlan::SetOp {
            op,
            all,
            left,
            right,
        } => LogicalPlan::SetOp {
            op,
            all,
            left: Box::new(hoist_correlated_subqueries(*left, catalog)?),
            right: Box::new(hoist_correlated_subqueries(*right, catalog)?),
        },
        LogicalPlan::Apply {
            input,
            subplan,
            correlations,
            kind,
        } => LogicalPlan::Apply {
            input: Box::new(hoist_correlated_subqueries(*input, catalog)?),
            subplan,
            correlations,
            kind,
        },
        // A single-table WHERE lowers directly into the scan's filter
        // (`plan_from`), so a correlated predicate must be pulled back out:
        // the scan is left unfiltered and the rewritten predicate filters
        // above the Apply. Only predicates that actually contain a correlated
        // subquery are pulled — everything else keeps scan-level filtering
        // (and index selection).
        LogicalPlan::Scan {
            table,
            filter: Some(predicate),
        } if expr_has_correlated_subquery(&predicate) => hoist_scan_filter(
            LogicalPlan::Scan {
                table,
                filter: None,
            },
            predicate,
            catalog,
        )?,
        LogicalPlan::SystemScan {
            view,
            filter: Some(predicate),
        } if expr_has_correlated_subquery(&predicate) => hoist_scan_filter(
            LogicalPlan::SystemScan { view, filter: None },
            predicate,
            catalog,
        )?,
        // Leaves and DDL: nothing to hoist.
        plan @ (LogicalPlan::Scan { .. }
        | LogicalPlan::SystemScan { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTableAddColumn { .. }
        | LogicalPlan::AlterTableDropColumn { .. }
        | LogicalPlan::AlterTableRenameColumn { .. }
        | LogicalPlan::AlterTableRenameTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. }) => plan,
    })
}

/// An UPDATE/DELETE source must produce exactly the target table's row shape
/// (the executor checks it); an Apply hoisted inside the source appends its
/// column, so a projection back to the table's columns is layered on top.
/// The projection op passes row identity through, so the rows stay
/// targetable.
fn restore_dml_source_shape(
    source: LogicalPlan,
    table: common::TableId,
    catalog: &dyn CatalogManager,
) -> Result<LogicalPlan> {
    let schema = catalog
        .get_table(table)?
        .ok_or_else(|| DbError::internal(format!("table {table} disappeared during planning")))?;
    if logical_output_width(&source, catalog)? == schema.columns.len() {
        return Ok(source);
    }
    let expressions = schema
        .columns
        .iter()
        .enumerate()
        .map(|(slot, column)| BoundExpr::LocalRef {
            slot,
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        })
        .collect();
    let output_schema = schema
        .columns
        .iter()
        .map(|column| common::ColumnInfo {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            table_id: Some(table),
            column_id: Some(column.id),
            pg_type: column.pg_type.clone(),
        })
        .collect();
    Ok(LogicalPlan::Projection {
        source: Box::new(source),
        expressions,
        output_schema,
    })
}

/// Pull a correlated scan filter above its (now unfiltered) scan as
/// `Filter { Apply { scan } }`.
fn hoist_scan_filter(
    scan: LogicalPlan,
    predicate: BoundExpr,
    catalog: &dyn CatalogManager,
) -> Result<LogicalPlan> {
    let mut hoister = Hoister {
        source: Some(Box::new(scan)),
        catalog,
    };
    let predicate = hoister.hoist_expr(&predicate)?;
    Ok(LogicalPlan::Filter {
        source: hoister.into_source(),
        predicate,
    })
}

/// Whether an expression tree contains a correlated subquery (probed through
/// the shared rewriter; the clone output is discarded).
fn expr_has_correlated_subquery(expr: &BoundExpr) -> bool {
    let mut found = false;
    let _ = rewrite_expr(expr, &mut |node| {
        match node {
            BoundExpr::ScalarSubquery { query, .. }
            | BoundExpr::Exists { query, .. }
            | BoundExpr::InSubquery { query, .. }
                if !query.correlations.is_empty() =>
            {
                found = true;
            }
            _ => {}
        }
        Ok(None)
    });
    found
}

/// Hoists the correlated subqueries of one expression position, growing the
/// input plan with one `Apply` per hoisted subquery. `source` is `Some`
/// between calls; `Option` only so `Apply` construction can take ownership.
struct Hoister<'c> {
    source: Option<Box<LogicalPlan>>,
    catalog: &'c dyn CatalogManager,
}

impl Hoister<'_> {
    fn into_source(self) -> Box<LogicalPlan> {
        self.source.expect("hoister source is always restored")
    }

    fn hoist_expr(&mut self, expr: &BoundExpr) -> Result<BoundExpr> {
        rewrite_expr(expr, &mut |node| self.hoist_node(node))
    }

    /// The rewrite callback: replace a correlated subquery expression with a
    /// `LocalRef` to the column its new `Apply` appends. Returning a `LocalRef`
    /// leaf means the rewriter never re-enters the hoisted subquery.
    fn hoist_node(&mut self, node: &BoundExpr) -> Result<Option<BoundExpr>> {
        let (query, kind, data_type, nullable) = match node {
            BoundExpr::ScalarSubquery {
                query,
                data_type,
                nullable,
            } if !query.correlations.is_empty() => (
                query,
                ApplyKind::Scalar {
                    data_type: data_type.clone(),
                },
                data_type.clone(),
                *nullable,
            ),
            BoundExpr::Exists {
                query,
                negated,
                nullable,
                ..
            } if !query.correlations.is_empty() => (
                query,
                ApplyKind::Exists { negated: *negated },
                DataType::Boolean,
                *nullable,
            ),
            BoundExpr::InSubquery {
                expr: operand,
                query,
                negated,
                nullable,
                ..
            } if !query.correlations.is_empty() => {
                // The operand is an outer-row expression and may itself carry
                // correlated subqueries; hoist those first so the captured
                // operand contains only supported expressions.
                let operand = self.hoist_expr(operand)?;
                (
                    query,
                    ApplyKind::In {
                        operand: Box::new(operand),
                        negated: *negated,
                    },
                    DataType::Boolean,
                    *nullable,
                )
            }
            _ => return Ok(None),
        };

        let slot =
            logical_output_width(self.source.as_ref().expect("source present"), self.catalog)?;
        let subplan = hoist_correlated_subqueries(
            crate::simplify::simplify_logical(crate::logical::plan_query(query)?),
            self.catalog,
        )?;
        let input = self.source.take().expect("source present");
        self.source = Some(Box::new(LogicalPlan::Apply {
            input,
            subplan: Box::new(subplan),
            correlations: query
                .correlations
                .iter()
                .map(|correlation| correlation.outer.clone())
                .collect(),
            kind,
        }));
        Ok(Some(BoundExpr::LocalRef {
            slot,
            data_type,
            nullable,
        }))
    }
}

/// The number of columns a logical plan's rows carry, for computing the slot
/// of an `Apply`'s appended column. Only query-shaped nodes can appear below
/// a hoisting position.
fn logical_output_width(plan: &LogicalPlan, catalog: &dyn CatalogManager) -> Result<usize> {
    Ok(match plan {
        LogicalPlan::Scan { table, .. } => {
            let schema = catalog.get_table(*table)?.ok_or_else(|| {
                DbError::internal(format!("table {table} disappeared during planning"))
            })?;
            schema.columns.len()
        }
        LogicalPlan::SystemScan { view, .. } => view.columns().len(),
        LogicalPlan::Join { left, right, .. } => {
            logical_output_width(left, catalog)? + logical_output_width(right, catalog)?
        }
        LogicalPlan::Filter { source, .. }
        | LogicalPlan::Sort { source, .. }
        | LogicalPlan::Distinct { source, .. }
        | LogicalPlan::Limit { source, .. } => logical_output_width(source, catalog)?,
        LogicalPlan::Projection { output_schema, .. }
        | LogicalPlan::Aggregate { output_schema, .. }
        | LogicalPlan::Values { output_schema, .. } => output_schema.len(),
        LogicalPlan::SetOp { left, .. } => logical_output_width(left, catalog)?,
        LogicalPlan::Apply { input, .. } => logical_output_width(input, catalog)? + 1,
        other => {
            return Err(DbError::internal(format!(
                "plan node has no row width: {other:?}"
            )));
        }
    })
}
