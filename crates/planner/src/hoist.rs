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
use crate::JoinType;
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
            let source = hoist_correlated_subqueries(*source, catalog)?;
            if predicate_has_pipeline_candidate(&predicate) {
                hoist_predicate(source, predicate, catalog, false)?
            } else {
                // No correlation: keep the predicate tree exactly as bound.
                LogicalPlan::Filter {
                    source: Box::new(source),
                    predicate,
                }
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
                source: hoister.into_source()?,
                expressions,
                output_schema,
            }
        }
        LogicalPlan::LockRows {
            source,
            table,
            mode,
            wait_policy,
            recheck,
            expressions,
            output_schema,
        } => LogicalPlan::LockRows {
            source: Box::new(hoist_correlated_subqueries(*source, catalog)?),
            table,
            mode,
            wait_policy,
            recheck,
            expressions,
            output_schema,
        },
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
            joined_source,
            returning,
            check_exprs,
        } => LogicalPlan::Update {
            table,
            assignments,
            source: Box::new(hoist_dml_source(*source, table, joined_source, catalog)?),
            joined_source,
            returning,
            check_exprs,
        },
        LogicalPlan::Delete {
            table,
            source,
            joined_source,
            returning,
        } => LogicalPlan::Delete {
            table,
            source: Box::new(hoist_dml_source(*source, table, joined_source, catalog)?),
            joined_source,
            returning,
        },
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
            identity_from,
        } => LogicalPlan::Join {
            left: Box::new(hoist_correlated_subqueries(*left, catalog)?),
            right: Box::new(hoist_correlated_subqueries(*right, catalog)?),
            condition,
            join_type,
            identity_from,
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
        LogicalPlan::Window {
            source,
            spec,
            functions,
        } => {
            let expected_columns = logical_output_columns(&source, catalog)?;
            let hoisted = hoist_correlated_subqueries(*source, catalog)?;
            let source = if logical_output_width(&hoisted, catalog)? == expected_columns.len() {
                hoisted
            } else {
                let expressions = expected_columns
                    .iter()
                    .enumerate()
                    .map(|(slot, column)| BoundExpr::LocalRef {
                        slot,
                        data_type: column.info.data_type.clone(),
                        nullable: column.nullable,
                    })
                    .collect();
                let output_schema = expected_columns
                    .into_iter()
                    .map(|column| column.info)
                    .collect();
                LogicalPlan::Projection {
                    source: Box::new(hoisted),
                    expressions,
                    output_schema,
                }
            };
            LogicalPlan::Window {
                source: Box::new(source),
                spec,
                functions,
            }
        }
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
            // A LATERAL Apply is created during FROM lowering with a raw
            // subplan; expression-hoisted Applies arrive already hoisted, for
            // which this recursion is an idempotent no-op.
            subplan: Box::new(hoist_correlated_subqueries(*subplan, catalog)?),
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
        } if predicate_has_pipeline_candidate(&predicate) => hoist_predicate(
            LogicalPlan::Scan {
                table,
                filter: None,
            },
            predicate,
            catalog,
            true,
        )?,
        LogicalPlan::SystemScan {
            view,
            filter: Some(predicate),
        } if predicate_has_pipeline_candidate(&predicate) => hoist_predicate(
            LogicalPlan::SystemScan { view, filter: None },
            predicate,
            catalog,
            true,
        )?,
        // Leaves and DDL: nothing to hoist.
        plan @ (LogicalPlan::Scan { .. }
        | LogicalPlan::SystemScan { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::TableFunction { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTableAddColumn { .. }
        | LogicalPlan::AlterTableDropColumn { .. }
        | LogicalPlan::AlterTableRenameColumn { .. }
        | LogicalPlan::AlterTableRenameTable { .. }
        | LogicalPlan::AlterTableAlterColumnType { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. }) => plan,
    })
}

/// Hoist a DML source. A joined source (`UPDATE ... FROM`/`DELETE ... USING`)
/// legitimately produces the combined (target ++ FROM) row — assignments and
/// RETURNING-side evaluation read it by slot, and Apply-appended columns sit
/// beyond it harmlessly — so it is never narrowed. A plain source is restored
/// to the table's exact shape when hoisting widened it.
fn hoist_dml_source(
    source: LogicalPlan,
    table: common::TableId,
    joined_source: bool,
    catalog: &dyn CatalogManager,
) -> Result<LogicalPlan> {
    let hoisted = hoist_correlated_subqueries(source, catalog)?;
    if joined_source {
        Ok(hoisted)
    } else {
        restore_dml_source_shape(hoisted, table, catalog)
    }
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

/// The filter pipeline for a predicate containing correlated subqueries
/// (`docs/specs/subqueries.md` §5–§6). The predicate is split into `AND`
/// conjuncts, then:
///
/// 1. conjuncts without correlated subqueries stay plain — pushed back into
///    the scan's own filter when the source is a bare scan
///    (`plain_into_scan`), else re-checked in the final `Filter`;
/// 2. decorrelatable `[NOT] EXISTS` / `[NOT] IN` conjuncts become semi/anti
///    joins stacked above the source (their output is the left side only, so
///    downstream slots are unchanged);
/// 3. everything else hoists through an `Apply`, consumed by the final
///    `Filter`.
fn hoist_predicate(
    source: LogicalPlan,
    predicate: BoundExpr,
    catalog: &dyn CatalogManager,
    plain_into_scan: bool,
) -> Result<LogicalPlan> {
    let mut plain = Vec::new();
    let mut candidates = Vec::new();
    for conjunct in split_and(predicate) {
        if expr_has_correlated_subquery(&conjunct) || is_uncorrelated_in_conjunct(&conjunct) {
            candidates.push(conjunct);
        } else {
            plain.push(conjunct);
        }
    }

    // Decorrelation first, so the joins sit directly above the (still
    // unfiltered) scan and failed uncorrelated candidates can return to the
    // plain bucket.
    let mut source = source;
    let mut leftovers = Vec::new();
    for conjunct in candidates {
        match try_decorrelate(source, &conjunct, catalog)? {
            Decorrelation::Joined(joined) => source = joined,
            Decorrelation::No(original) => {
                source = original;
                if expr_has_correlated_subquery(&conjunct) {
                    leftovers.push(conjunct);
                } else {
                    // An uncorrelated IN that did not qualify (non-column
                    // operand, nullable NOT IN) keeps its pre-pass path.
                    plain.push(conjunct);
                }
            }
        }
    }

    if plain_into_scan
        && !plain.is_empty()
        && let Some(filter) = and_reduce(std::mem::take(&mut plain))
    {
        attach_to_leftmost_scan(&mut source, filter);
    }

    let mut residual = plain;
    if !leftovers.is_empty() {
        let mut hoister = Hoister {
            source: Some(Box::new(source)),
            catalog,
        };
        for conjunct in leftovers {
            residual.push(hoister.hoist_expr(&conjunct)?);
        }
        source = *hoister.into_source()?;
    }

    Ok(match and_reduce(residual) {
        Some(predicate) => LogicalPlan::Filter {
            source: Box::new(source),
            predicate,
        },
        None => source,
    })
}

/// Attach a predicate to the leftmost scan under any semi/anti joins the
/// decorrelation stacked (the plain conjuncts reference only that scan's
/// columns, and semi/anti joins preserve the left side unchanged). Falls back
/// to a `Filter` above `source` if the leftmost node is not a bare scan.
fn attach_to_leftmost_scan(source: &mut LogicalPlan, filter: BoundExpr) {
    match source {
        LogicalPlan::Scan {
            filter: slot @ None,
            ..
        }
        | LogicalPlan::SystemScan {
            filter: slot @ None,
            ..
        } => *slot = Some(filter),
        LogicalPlan::Join {
            left,
            join_type: JoinType::Semi | JoinType::Anti,
            ..
        } => attach_to_leftmost_scan(left, filter),
        other => {
            let source_plan = std::mem::replace(
                other,
                LogicalPlan::Values {
                    rows: Vec::new(),
                    output_schema: Vec::new(),
                },
            );
            *other = LogicalPlan::Filter {
                source: Box::new(source_plan),
                predicate: filter,
            };
        }
    }
}

/// The pipeline gate: a predicate is worth splitting when it contains a
/// correlated subquery or a top-level uncorrelated `[NOT] IN (subquery)`
/// conjunct (a semi/anti-join candidate).
fn predicate_has_pipeline_candidate(expr: &BoundExpr) -> bool {
    expr_has_correlated_subquery(expr) || has_top_level_in_conjunct(expr)
}

fn has_top_level_in_conjunct(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::BinaryOp {
            op: crate::BinOp::And,
            left,
            right,
            ..
        } => has_top_level_in_conjunct(left) || has_top_level_in_conjunct(right),
        other => is_uncorrelated_in_conjunct(other),
    }
}

fn is_uncorrelated_in_conjunct(expr: &BoundExpr) -> bool {
    matches!(expr, BoundExpr::InSubquery { query, .. } if query.correlations.is_empty())
}

/// A decorrelation attempt either wraps the source in a semi/anti join or
/// hands the source back untouched.
enum Decorrelation {
    Joined(LogicalPlan),
    No(LogicalPlan),
}

/// Try to run one filter conjunct as a semi/anti join
/// (`docs/specs/subqueries.md` §6.2):
///
/// - `[NOT] EXISTS (SELECT ... FROM one_table WHERE conjuncts)`, correlated
///   only through `inner_col = outer_col` equality conjuncts — the equalities
///   become the join condition, the remaining body conjuncts stay as the
///   inner scan's filter.
/// - `col [NOT] IN (uncorrelated subquery)` with a plain column operand;
///   `NOT IN` only when both the operand and the subquery column are
///   non-nullable (otherwise three-valued `NULL` semantics differ).
///
/// Anything else falls back to Apply. Chained correlation entries
/// (`OuterRef` outers) are allowed: they land in the join condition, which
/// keeps the join on the nested-loop path and is substituted by the enclosing
/// Apply like any other template expression.
fn try_decorrelate(
    source: LogicalPlan,
    conjunct: &BoundExpr,
    catalog: &dyn CatalogManager,
) -> Result<Decorrelation> {
    match conjunct {
        BoundExpr::Exists { query, negated, .. } if !query.correlations.is_empty() => {
            let Some(parts) = qualify_exists_body(query) else {
                return Ok(Decorrelation::No(source));
            };
            let left_width = logical_output_width(&source, catalog)?;
            let right = hoist_correlated_subqueries(
                crate::simplify::simplify_logical(LogicalPlan::Scan {
                    table: parts.table,
                    filter: parts.inner_filter,
                }),
                catalog,
            )?;
            let condition = parts
                .equalities
                .into_iter()
                .map(|(correlation_slot, inner_column)| {
                    equality(
                        query.correlations[correlation_slot].outer.clone(),
                        rebase_slot(inner_column, left_width),
                    )
                })
                .collect::<Vec<_>>();
            Ok(Decorrelation::Joined(LogicalPlan::Join {
                left: Box::new(source),
                right: Box::new(right),
                condition: and_reduce(condition),
                join_type: if *negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                },
                // Semi/anti joins emit the left ExecRow whole, identity
                // included; no marker needed.
                identity_from: None,
            }))
        }
        BoundExpr::InSubquery {
            expr: operand,
            query,
            negated,
            ..
        } if query.correlations.is_empty() => {
            if !matches!(
                **operand,
                BoundExpr::InputRef { .. } | BoundExpr::LocalRef { .. }
            ) {
                return Ok(Decorrelation::No(source));
            }
            let output = query.output_columns();
            let [column] = output.as_slice() else {
                return Ok(Decorrelation::No(source));
            };
            // NOT IN differs from an anti join whenever a NULL can appear on
            // either side (three-valued logic); only provably NULL-free
            // shapes decorrelate.
            if *negated && (operand.nullable() || column.nullable) {
                return Ok(Decorrelation::No(source));
            }
            let left_width = logical_output_width(&source, catalog)?;
            let right = hoist_correlated_subqueries(
                crate::simplify::simplify_logical(crate::logical::plan_query(query)?),
                catalog,
            )?;
            let condition = equality(
                (**operand).clone(),
                BoundExpr::InputRef {
                    input: 0,
                    column: 0,
                    slot: left_width,
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                },
            );
            Ok(Decorrelation::Joined(LogicalPlan::Join {
                left: Box::new(source),
                right: Box::new(right),
                condition: Some(condition),
                join_type: if *negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                },
                // Semi/anti joins emit the left ExecRow whole, identity
                // included; no marker needed.
                identity_from: None,
            }))
        }
        _ => Ok(Decorrelation::No(source)),
    }
}

struct ExistsBodyParts {
    table: common::TableId,
    /// Uncorrelated body conjuncts, in body-scope slots (the inner scan's
    /// filter).
    inner_filter: Option<BoundExpr>,
    /// Per equality conjunct: the correlation slot the body compared against,
    /// and the inner column reference (body-scope slot).
    equalities: Vec<(usize, BoundExpr)>,
}

/// Qualify a correlated EXISTS body for decorrelation: a plain single-table
/// SELECT (no grouping, distinct, ordering, or limits) whose only use of the
/// outer scope is `inner_col = OuterRef` equality conjuncts.
fn qualify_exists_body(query: &crate::BoundQuery) -> Option<ExistsBodyParts> {
    if !query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some() {
        return None;
    }
    let crate::BoundQueryBody::Select(select) = &query.body else {
        return None;
    };
    if select.distinct.is_some() || !select.group_by.is_empty() || select.having.is_some() {
        return None;
    }
    // An aggregate call in the select list makes the body an implicit
    // aggregate query producing EXACTLY ONE row regardless of the filter —
    // EXISTS over it is constant-true, which a semi join would break.
    if select
        .columns
        .iter()
        .any(|item| crate::logical::contains_aggregate(&item.expr))
    {
        return None;
    }
    let Some(crate::BoundFrom::Table { table, .. }) = &select.from else {
        return None;
    };

    let mut equalities = Vec::new();
    let mut inner_filter = Vec::new();
    for conjunct in select.filter.clone().map(split_and).unwrap_or_default() {
        if let BoundExpr::BinaryOp {
            op: crate::BinOp::Eq,
            left,
            right,
            ..
        } = &conjunct
        {
            match (&**left, &**right) {
                (BoundExpr::InputRef { .. }, BoundExpr::OuterRef { slot, .. }) => {
                    equalities.push((*slot, (**left).clone()));
                    continue;
                }
                (BoundExpr::OuterRef { slot, .. }, BoundExpr::InputRef { .. }) => {
                    equalities.push((*slot, (**right).clone()));
                    continue;
                }
                _ => {}
            }
        }
        inner_filter.push(conjunct);
    }
    if equalities.is_empty() {
        return None;
    }

    // No other use of the outer scope anywhere in the body: not in the
    // remaining conjuncts, not in the projection, and not chained through a
    // nested subquery's correlation entries (the body's boundary disappears
    // after decorrelation, so nothing could ever substitute them).
    if inner_filter.iter().any(references_enclosing_boundary)
        || select
            .columns
            .iter()
            .any(|item| references_enclosing_boundary(&item.expr))
    {
        return None;
    }

    Some(ExistsBodyParts {
        table: *table,
        inner_filter: and_reduce(inner_filter),
        equalities,
    })
}

/// Whether an expression references the enclosing subquery boundary: an
/// `OuterRef` directly, or — through a nested subquery's correlation entries
/// (which are expressions in THIS scope's terms) — a chained one. Nested
/// subquery bodies themselves are separate scopes and are not entered.
fn references_enclosing_boundary(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::OuterRef { .. } => true,
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => query
            .correlations
            .iter()
            .any(|correlation| references_enclosing_boundary(&correlation.outer)),
        BoundExpr::InSubquery {
            expr: operand,
            query,
            ..
        } => {
            references_enclosing_boundary(operand)
                || query
                    .correlations
                    .iter()
                    .any(|correlation| references_enclosing_boundary(&correlation.outer))
        }
        _ => {
            let mut found = false;
            let _ = crate::params::for_each_child(expr, &mut |child| {
                found = found || references_enclosing_boundary(child);
                Ok(())
            });
            found
        }
    }
}

/// Split an expression into its top-level `AND` conjuncts.
fn split_and(expr: BoundExpr) -> Vec<BoundExpr> {
    match expr {
        BoundExpr::BinaryOp {
            op: crate::BinOp::And,
            left,
            right,
            ..
        } => {
            let mut conjuncts = split_and(*left);
            conjuncts.extend(split_and(*right));
            conjuncts
        }
        other => vec![other],
    }
}

/// Re-`AND` conjuncts; `None` when empty.
fn and_reduce(conjuncts: Vec<BoundExpr>) -> Option<BoundExpr> {
    conjuncts
        .into_iter()
        .reduce(|acc, next| BoundExpr::BinaryOp {
            left: Box::new(acc),
            op: crate::BinOp::And,
            right: Box::new(next),
            data_type: DataType::Boolean,
            nullable: true,
        })
}

fn equality(left: BoundExpr, right: BoundExpr) -> BoundExpr {
    BoundExpr::BinaryOp {
        left: Box::new(left),
        op: crate::BinOp::Eq,
        right: Box::new(right),
        data_type: DataType::Boolean,
        nullable: true,
    }
}

/// Rebase a body-scope column reference onto the joined row (right columns
/// follow the left side's).
fn rebase_slot(expr: BoundExpr, left_width: usize) -> BoundExpr {
    match expr {
        BoundExpr::InputRef {
            input,
            column,
            slot,
            data_type,
            nullable,
        } => BoundExpr::InputRef {
            input,
            column,
            slot: slot + left_width,
            data_type,
            nullable,
        },
        other => other,
    }
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
    fn into_source(self) -> Result<Box<LogicalPlan>> {
        self.source
            .ok_or_else(|| DbError::internal("correlated-subquery hoister lost its source plan"))
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

        let source = self
            .source
            .as_ref()
            .ok_or_else(|| DbError::internal("correlated-subquery hoister source is missing"))?;
        let slot = logical_output_width(source, self.catalog)?;
        let subplan = hoist_correlated_subqueries(
            crate::simplify::simplify_logical(crate::logical::plan_query(query)?),
            self.catalog,
        )?;
        let input = self
            .source
            .take()
            .ok_or_else(|| DbError::internal("correlated-subquery hoister source is missing"))?;
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
        LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => {
            if join_type.is_semi_or_anti() {
                logical_output_width(left, catalog)?
            } else {
                logical_output_width(left, catalog)? + logical_output_width(right, catalog)?
            }
        }
        LogicalPlan::Filter { source, .. }
        | LogicalPlan::Sort { source, .. }
        | LogicalPlan::Distinct { source, .. }
        | LogicalPlan::Limit { source, .. } => logical_output_width(source, catalog)?,
        LogicalPlan::Window {
            source, functions, ..
        } => logical_output_width(source, catalog)? + functions.len(),
        LogicalPlan::Projection { output_schema, .. }
        | LogicalPlan::LockRows { output_schema, .. }
        | LogicalPlan::Aggregate { output_schema, .. }
        | LogicalPlan::Values { output_schema, .. }
        | LogicalPlan::TableFunction { output_schema, .. } => output_schema.len(),
        LogicalPlan::SetOp { left, .. } => logical_output_width(left, catalog)?,
        LogicalPlan::Apply { input, kind, .. } => {
            let appended = match kind {
                ApplyKind::Lateral { output_schema, .. } => output_schema.len(),
                _ => 1,
            };
            logical_output_width(input, catalog)? + appended
        }
        other => {
            return Err(DbError::internal(format!(
                "plan node has no row width: {other:?}"
            )));
        }
    })
}

struct LogicalOutputColumn {
    info: common::ColumnInfo,
    nullable: bool,
}

fn logical_output_columns(
    plan: &LogicalPlan,
    catalog: &dyn CatalogManager,
) -> Result<Vec<LogicalOutputColumn>> {
    Ok(match plan {
        LogicalPlan::Scan { table, .. } => {
            let schema = catalog.get_table(*table)?.ok_or_else(|| {
                DbError::internal(format!("table {table} disappeared during planning"))
            })?;
            schema
                .columns
                .iter()
                .map(|column| LogicalOutputColumn {
                    info: common::ColumnInfo {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        table_id: Some(*table),
                        column_id: Some(column.id),
                        pg_type: column.pg_type.clone(),
                    },
                    nullable: column.nullable,
                })
                .collect()
        }
        LogicalPlan::SystemScan { view, .. } => view
            .columns()
            .into_iter()
            .map(|column| LogicalOutputColumn {
                info: common::ColumnInfo {
                    name: column.name,
                    data_type: column.data_type,
                    table_id: None,
                    column_id: Some(column.id),
                    pg_type: column.pg_type,
                },
                nullable: column.nullable,
            })
            .collect(),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => {
            let mut columns = logical_output_columns(left, catalog)?;
            if matches!(join_type, JoinType::Right | JoinType::Full) {
                for column in &mut columns {
                    column.nullable = true;
                }
            }
            if !join_type.is_semi_or_anti() {
                let mut right_columns = logical_output_columns(right, catalog)?;
                if matches!(join_type, JoinType::Left | JoinType::Full) {
                    for column in &mut right_columns {
                        column.nullable = true;
                    }
                }
                columns.extend(right_columns);
            }
            columns
        }
        LogicalPlan::Filter { source, .. }
        | LogicalPlan::Sort { source, .. }
        | LogicalPlan::Distinct { source, .. }
        | LogicalPlan::Limit { source, .. } => logical_output_columns(source, catalog)?,
        LogicalPlan::Projection {
            expressions,
            output_schema,
            ..
        }
        | LogicalPlan::LockRows {
            expressions,
            output_schema,
            ..
        } => output_columns_from_exprs(output_schema, expressions)?,
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            output_schema,
            ..
        } => {
            let nullability = group_by
                .iter()
                .map(BoundExpr::nullable)
                .chain(aggregates.iter().map(|aggregate| aggregate.nullable));
            output_columns_with_nullability(output_schema, nullability)?
        }
        LogicalPlan::Values {
            rows,
            output_schema,
        } => {
            let mut nullability = Vec::new();
            nullability
                .try_reserve(output_schema.len())
                .map_err(|error| {
                    DbError::internal(format!("could not reserve VALUES nullability: {error}"))
                })?;
            for slot in 0..output_schema.len() {
                let mut nullable = false;
                for row in rows {
                    let expr = row.get(slot).ok_or_else(|| {
                        DbError::internal("VALUES row width does not match its output schema")
                    })?;
                    nullable |= expr.nullable();
                }
                nullability.push(nullable);
            }
            output_columns_with_nullability(output_schema, nullability)?
        }
        LogicalPlan::TableFunction {
            name,
            output_schema,
            ..
        } => {
            let nullable = match name.as_str() {
                "unnest" => true,
                "generate_series" => false,
                _ => true,
            };
            output_columns_with_nullability(
                output_schema,
                std::iter::repeat_n(nullable, output_schema.len()),
            )?
        }
        LogicalPlan::Window {
            source, functions, ..
        } => {
            let mut columns = logical_output_columns(source, catalog)?;
            columns.extend(functions.iter().map(|function| LogicalOutputColumn {
                info: common::ColumnInfo {
                    name: format!("{:?}", function.func),
                    data_type: function.data_type.clone(),
                    table_id: None,
                    column_id: None,
                    pg_type: None,
                },
                nullable: function.nullable,
            }));
            columns
        }
        LogicalPlan::SetOp { left, right, .. } => {
            let left = logical_output_columns(left, catalog)?;
            let right = logical_output_columns(right, catalog)?;
            if left.len() != right.len() {
                return Err(DbError::internal(
                    "set-operation arm widths differ during window hoisting",
                ));
            }
            left.into_iter()
                .zip(right)
                .map(|(mut left, right)| {
                    left.nullable |= right.nullable;
                    left
                })
                .collect()
        }
        LogicalPlan::Apply {
            input,
            subplan,
            kind,
            ..
        } => {
            let mut columns = logical_output_columns(input, catalog)?;
            match kind {
                ApplyKind::Scalar { data_type } => columns.push(LogicalOutputColumn {
                    info: common::ColumnInfo {
                        name: "apply".to_string(),
                        data_type: data_type.clone(),
                        table_id: None,
                        column_id: None,
                        pg_type: None,
                    },
                    nullable: true,
                }),
                ApplyKind::Exists { .. } => columns.push(LogicalOutputColumn {
                    info: common::ColumnInfo {
                        name: "apply".to_string(),
                        data_type: DataType::Boolean,
                        table_id: None,
                        column_id: None,
                        pg_type: None,
                    },
                    nullable: false,
                }),
                ApplyKind::In { .. } => columns.push(LogicalOutputColumn {
                    info: common::ColumnInfo {
                        name: "apply".to_string(),
                        data_type: DataType::Boolean,
                        table_id: None,
                        column_id: None,
                        pg_type: None,
                    },
                    nullable: true,
                }),
                ApplyKind::Lateral { left_join, .. } => {
                    let mut appended = logical_output_columns(subplan, catalog)?;
                    if *left_join {
                        for column in &mut appended {
                            column.nullable = true;
                        }
                    }
                    columns.extend(appended);
                }
            }
            columns
        }
        other => {
            return Err(DbError::internal(format!(
                "plan node has no row schema: {other:?}"
            )));
        }
    })
}

fn output_columns_from_exprs(
    output_schema: &[common::ColumnInfo],
    expressions: &[BoundExpr],
) -> Result<Vec<LogicalOutputColumn>> {
    output_columns_with_nullability(output_schema, expressions.iter().map(BoundExpr::nullable))
}

fn output_columns_with_nullability(
    output_schema: &[common::ColumnInfo],
    nullability: impl IntoIterator<Item = bool>,
) -> Result<Vec<LogicalOutputColumn>> {
    let columns: Vec<_> = output_schema
        .iter()
        .cloned()
        .zip(nullability)
        .map(|(info, nullable)| LogicalOutputColumn { info, nullable })
        .collect();
    if columns.len() != output_schema.len() {
        return Err(DbError::internal(
            "plan output expressions do not match its output schema",
        ));
    }
    Ok(columns)
}
