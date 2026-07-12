//! Generic structural rewriting over physical plans and bound expressions.
//!
//! One walker serves every expression-rewriting pass over a `PhysicalPlan`
//! (`docs/specs/subqueries.md` §5.3): the uncorrelated-subquery pre-pass and
//! the per-outer-row `OuterRef` substitution are both callbacks over this
//! traversal, so the two passes cannot drift as plan nodes are added.

use common::Result;

use crate::{
    AggregateExpr, ApplyKind, BoundExpr, BoundOnConflict, BoundOrderByItem, BoundReturning,
    PhysicalPlan,
};

/// Rewrite every bound expression embedded in `plan` (filters, projections,
/// join conditions, sort keys, aggregate arguments, VALUES rows, DML
/// assignments, RETURNING projections, ON CONFLICT expressions, DEFAULT and
/// CHECK expressions) by applying `f` through [`rewrite_expr`], recursing into
/// child plans. Plan structure and non-expression fields are cloned unchanged.
/// DDL nodes carry no runtime expressions and are cloned wholesale.
pub fn rewrite_plan_exprs(
    plan: &PhysicalPlan,
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<PhysicalPlan> {
    Ok(match plan {
        PhysicalPlan::CreateSchema { .. }
        | PhysicalPlan::DropSchema { .. }
        | PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTableAddColumn { .. }
        | PhysicalPlan::AlterTableDropColumn { .. }
        | PhysicalPlan::AlterTableRenameColumn { .. }
        | PhysicalPlan::AlterTableRenameTable { .. }
        | PhysicalPlan::AlterTableAlterColumnType { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::DropIndex { .. }
        | PhysicalPlan::CreateSequence { .. }
        | PhysicalPlan::DropSequence { .. }
        | PhysicalPlan::CreateView { .. }
        | PhysicalPlan::DropView { .. } => plan.clone(),
        PhysicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            default_exprs,
            check_exprs,
        } => PhysicalPlan::Insert {
            table: *table,
            columns: columns.clone(),
            source: Box::new(rewrite_plan_exprs(source, f)?),
            on_conflict: rewrite_on_conflict(on_conflict, f)?,
            returning: rewrite_returning(returning, f)?,
            default_exprs: rewrite_assignments(default_exprs, f)?,
            check_exprs: rewrite_vec(check_exprs, f)?,
        },
        PhysicalPlan::Update {
            table,
            assignments,
            source,
            joined_source,
            returning,
            check_exprs,
        } => PhysicalPlan::Update {
            table: *table,
            assignments: rewrite_assignments(assignments, f)?,
            source: Box::new(rewrite_plan_exprs(source, f)?),
            joined_source: *joined_source,
            returning: rewrite_returning(returning, f)?,
            check_exprs: rewrite_vec(check_exprs, f)?,
        },
        PhysicalPlan::Delete {
            table,
            source,
            joined_source,
            returning,
        } => PhysicalPlan::Delete {
            table: *table,
            source: Box::new(rewrite_plan_exprs(source, f)?),
            joined_source: *joined_source,
            returning: rewrite_returning(returning, f)?,
        },
        PhysicalPlan::SeqScan {
            table,
            table_name,
            filter,
        } => PhysicalPlan::SeqScan {
            table: *table,
            table_name: table_name.clone(),
            filter: rewrite_opt(filter, f)?,
        },
        PhysicalPlan::SystemScan {
            view,
            output_schema,
            filter,
        } => PhysicalPlan::SystemScan {
            view: *view,
            output_schema: output_schema.clone(),
            filter: rewrite_opt(filter, f)?,
        },
        PhysicalPlan::IndexScan {
            table,
            table_name,
            index,
            range,
            full_filter,
            filter,
        } => PhysicalPlan::IndexScan {
            table: *table,
            table_name: table_name.clone(),
            index: *index,
            range: range.clone(),
            full_filter: rewrite_opt(full_filter, f)?,
            filter: rewrite_opt(filter, f)?,
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
            identity_from,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(rewrite_plan_exprs(left, f)?),
            right: Box::new(rewrite_plan_exprs(right, f)?),
            condition: rewrite_opt(condition, f)?,
            join_type: *join_type,
            identity_from: *identity_from,
        },
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            identity_from,
            build_left,
        } => PhysicalPlan::HashJoin {
            left: Box::new(rewrite_plan_exprs(left, f)?),
            right: Box::new(rewrite_plan_exprs(right, f)?),
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
            join_type: *join_type,
            identity_from: *identity_from,
            build_left: *build_left,
        },
        PhysicalPlan::MergeJoin {
            left,
            right,
            left_keys,
            right_keys,
            residual,
            join_type,
        } => PhysicalPlan::MergeJoin {
            left: Box::new(rewrite_plan_exprs(left, f)?),
            right: Box::new(rewrite_plan_exprs(right, f)?),
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
            residual: rewrite_opt(residual, f)?,
            join_type: *join_type,
        },
        // The subplan is a separate OuterRef namespace owned by the Apply
        // operator (docs/specs/subqueries.md section 5.2): it is cloned, not
        // walked. The correlations and the In-operand are expressions over
        // THIS plan's rows and are rewritten.
        PhysicalPlan::Apply {
            input,
            subplan,
            correlations,
            kind,
        } => PhysicalPlan::Apply {
            input: Box::new(rewrite_plan_exprs(input, f)?),
            subplan: subplan.clone(),
            correlations: rewrite_vec(correlations, f)?,
            kind: match kind {
                ApplyKind::In { operand, negated } => ApplyKind::In {
                    operand: Box::new(rewrite_expr(operand, f)?),
                    negated: *negated,
                },
                ApplyKind::Lateral {
                    left_join,
                    condition,
                    output_schema,
                } => ApplyKind::Lateral {
                    left_join: *left_join,
                    condition: condition
                        .as_deref()
                        .map(|condition| rewrite_expr(condition, f).map(Box::new))
                        .transpose()?,
                    output_schema: output_schema.clone(),
                },
                other => other.clone(),
            },
        },
        PhysicalPlan::Filter { source, predicate } => PhysicalPlan::Filter {
            source: Box::new(rewrite_plan_exprs(source, f)?),
            predicate: rewrite_expr(predicate, f)?,
        },
        PhysicalPlan::Projection {
            source,
            expressions,
            output_schema,
        } => PhysicalPlan::Projection {
            source: Box::new(rewrite_plan_exprs(source, f)?),
            expressions: rewrite_vec(expressions, f)?,
            output_schema: output_schema.clone(),
        },
        PhysicalPlan::Sort { source, order_by } => PhysicalPlan::Sort {
            source: Box::new(rewrite_plan_exprs(source, f)?),
            order_by: order_by
                .iter()
                .map(|item| {
                    Ok(BoundOrderByItem {
                        expr: rewrite_expr(&item.expr, f)?,
                        ascending: item.ascending,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        },
        PhysicalPlan::Distinct { source, on_keys } => PhysicalPlan::Distinct {
            source: Box::new(rewrite_plan_exprs(source, f)?),
            on_keys: rewrite_vec(on_keys, f)?,
        },
        PhysicalPlan::Limit {
            source,
            count,
            offset,
        } => PhysicalPlan::Limit {
            source: Box::new(rewrite_plan_exprs(source, f)?),
            count: *count,
            offset: *offset,
        },
        PhysicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            output_schema,
        } => PhysicalPlan::Aggregate {
            source: Box::new(rewrite_plan_exprs(source, f)?),
            group_by: rewrite_vec(group_by, f)?,
            aggregates: aggregates
                .iter()
                .map(|aggregate| {
                    Ok(AggregateExpr {
                        func: aggregate.func,
                        arg: aggregate
                            .arg
                            .as_ref()
                            .map(|arg| rewrite_expr(arg, f))
                            .transpose()?,
                        distinct: aggregate.distinct,
                        data_type: aggregate.data_type.clone(),
                        nullable: aggregate.nullable,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            output_schema: output_schema.clone(),
        },
        PhysicalPlan::Values {
            rows,
            output_schema,
        } => PhysicalPlan::Values {
            rows: rows
                .iter()
                .map(|row| rewrite_vec(row, f))
                .collect::<Result<Vec<_>>>()?,
            output_schema: output_schema.clone(),
        },
        PhysicalPlan::TableFunction {
            name,
            args,
            output_schema,
        } => PhysicalPlan::TableFunction {
            name: name.clone(),
            args: rewrite_vec(args, f)?,
            output_schema: output_schema.clone(),
        },
        PhysicalPlan::SetOp {
            op,
            all,
            left,
            right,
        } => PhysicalPlan::SetOp {
            op: *op,
            all: *all,
            left: Box::new(rewrite_plan_exprs(left, f)?),
            right: Box::new(rewrite_plan_exprs(right, f)?),
        },
    })
}

/// Rewrite one expression tree. `f` is applied to each node pre-order: `None`
/// keeps the node and recurses into its children; `Some(replacement)` replaces
/// the node and the rewrite continues on the replacement (so a replacement's
/// own children — for example the operand carried into an `InList` — are still
/// rewritten). A replacement must strictly reduce the content `f` matches on,
/// or the rewrite will not terminate.
pub fn rewrite_expr(
    expr: &BoundExpr,
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<BoundExpr> {
    if let Some(replacement) = f(expr)? {
        return rewrite_children(&replacement, f);
    }
    rewrite_children(expr, f)
}

/// Clone `expr` with each child rewritten via [`rewrite_expr`]. Subquery
/// bodies are not expression children (they are separate plans, reached when
/// their boundary executes); only `InSubquery`'s left operand is walked.
fn rewrite_children(
    expr: &BoundExpr,
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<BoundExpr> {
    Ok(match expr {
        BoundExpr::Literal { .. }
        | BoundExpr::Parameter { .. }
        | BoundExpr::InputRef { .. }
        | BoundExpr::LocalRef { .. }
        | BoundExpr::OuterRef { .. }
        | BoundExpr::Nextval { .. }
        | BoundExpr::Currval { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => expr.clone(),
        BoundExpr::BinaryOp {
            left,
            op,
            right,
            data_type,
            nullable,
        } => BoundExpr::BinaryOp {
            left: Box::new(rewrite_expr(left, f)?),
            op: *op,
            right: Box::new(rewrite_expr(right, f)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::UnaryOp {
            op,
            expr,
            data_type,
            nullable,
        } => BoundExpr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr(expr, f)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Function {
            name,
            args,
            data_type,
            pg_type,
            nullable,
        } => BoundExpr::Function {
            name: name.clone(),
            args: rewrite_vec(args, f)?,
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Array {
            elements,
            dimensions,
            element_type,
            data_type,
            nullable,
        } => BoundExpr::Array {
            elements: rewrite_vec(elements, f)?,
            dimensions: dimensions.clone(),
            element_type: element_type.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::ArraySubscript {
            array,
            subscripts,
            data_type,
            nullable,
        } => BoundExpr::ArraySubscript {
            array: Box::new(rewrite_expr(array, f)?),
            subscripts: rewrite_vec(subscripts, f)?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Any {
            left,
            op,
            array,
            data_type,
            nullable,
        } => BoundExpr::Any {
            left: Box::new(rewrite_expr(left, f)?),
            op: *op,
            array: Box::new(rewrite_expr(array, f)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Setval {
            sequence,
            value,
            is_called,
            data_type,
            nullable,
        } => BoundExpr::Setval {
            sequence: *sequence,
            value: Box::new(rewrite_expr(value, f)?),
            is_called: is_called
                .as_deref()
                .map(|expr| rewrite_expr(expr, f).map(Box::new))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::AggregateCall {
            func,
            arg,
            distinct,
            data_type,
            nullable,
        } => BoundExpr::AggregateCall {
            func: *func,
            arg: arg
                .as_deref()
                .map(|arg| rewrite_expr(arg, f).map(Box::new))
                .transpose()?,
            distinct: *distinct,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::IsNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNull {
            expr: Box::new(rewrite_expr(expr, f)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::IsNotNull {
            expr,
            data_type,
            nullable,
        } => BoundExpr::IsNotNull {
            expr: Box::new(rewrite_expr(expr, f)?),
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::InList {
            expr,
            list,
            negated,
            data_type,
            nullable,
        } => BoundExpr::InList {
            expr: Box::new(rewrite_expr(expr, f)?),
            list: rewrite_vec(list, f)?,
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
            data_type,
            nullable,
        } => BoundExpr::Between {
            expr: Box::new(rewrite_expr(expr, f)?),
            low: Box::new(rewrite_expr(low, f)?),
            high: Box::new(rewrite_expr(high, f)?),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
            escape,
            data_type,
            nullable,
        } => BoundExpr::Like {
            expr: Box::new(rewrite_expr(expr, f)?),
            pattern: Box::new(rewrite_expr(pattern, f)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape: *escape,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            data_type,
            nullable,
        } => BoundExpr::Case {
            operand: operand
                .as_deref()
                .map(|operand| rewrite_expr(operand, f).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| Ok((rewrite_expr(when, f)?, rewrite_expr(then, f)?)))
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_deref()
                .map(|else_clause| rewrite_expr(else_clause, f).map(Box::new))
                .transpose()?,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::Cast {
            expr,
            data_type,
            pg_type,
            nullable,
        } => BoundExpr::Cast {
            expr: Box::new(rewrite_expr(expr, f)?),
            data_type: data_type.clone(),
            pg_type: pg_type.clone(),
            nullable: *nullable,
        },
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
            data_type,
            nullable,
        } => BoundExpr::InSubquery {
            expr: Box::new(rewrite_expr(expr, f)?),
            query: query.clone(),
            negated: *negated,
            data_type: data_type.clone(),
            nullable: *nullable,
        },
    })
}

fn rewrite_vec(
    exprs: &[BoundExpr],
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<Vec<BoundExpr>> {
    exprs.iter().map(|expr| rewrite_expr(expr, f)).collect()
}

fn rewrite_opt(
    expr: &Option<BoundExpr>,
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<Option<BoundExpr>> {
    expr.as_ref().map(|expr| rewrite_expr(expr, f)).transpose()
}

fn rewrite_assignments(
    assignments: &[(common::ColumnId, BoundExpr)],
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<Vec<(common::ColumnId, BoundExpr)>> {
    assignments
        .iter()
        .map(|(column, expr)| Ok((*column, rewrite_expr(expr, f)?)))
        .collect()
}

fn rewrite_returning(
    returning: &Option<BoundReturning>,
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<Option<BoundReturning>> {
    returning
        .as_ref()
        .map(|returning| {
            Ok(BoundReturning {
                exprs: rewrite_vec(&returning.exprs, f)?,
                output_schema: returning.output_schema.clone(),
            })
        })
        .transpose()
}

fn rewrite_on_conflict(
    on_conflict: &Option<BoundOnConflict>,
    f: &mut impl FnMut(&BoundExpr) -> Result<Option<BoundExpr>>,
) -> Result<Option<BoundOnConflict>> {
    match on_conflict {
        None => Ok(None),
        Some(BoundOnConflict::DoNothing { target }) => Ok(Some(BoundOnConflict::DoNothing {
            target: target.clone(),
        })),
        Some(BoundOnConflict::DoUpdate {
            target,
            assignments,
            filter,
        }) => Ok(Some(BoundOnConflict::DoUpdate {
            target: target.clone(),
            assignments: rewrite_assignments(assignments, f)?,
            filter: rewrite_opt(filter, f)?,
        })),
    }
}
