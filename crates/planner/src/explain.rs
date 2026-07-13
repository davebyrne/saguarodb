use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::Duration;

use crate::ApplyKind;
use crate::JoinType;
use crate::PhysicalPlan;
use common::{DbError, Key, KeyRange, Result};

/// An execution-local identifier assigned to one node in a physical plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlanNodeId(pub usize);

/// The immutable child shape and deterministic identifiers for a physical plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanNodeLayout {
    id: PlanNodeId,
    children: Vec<PlanNodeLayout>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeExecutionMetrics {
    pub loops: u64,
    pub rows: u64,
    pub startup: Duration,
    pub total: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitPlanAnalysis {
    pub ordinal: usize,
    pub parent: Option<usize>,
    pub plan: PhysicalPlan,
    pub layout: PlanNodeLayout,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExplainAnalysis {
    pub nodes: BTreeMap<PlanNodeId, NodeExecutionMetrics>,
    pub init_plans: Vec<InitPlanAnalysis>,
    pub execution_time: Duration,
}

impl PlanNodeLayout {
    /// Builds a zero-based, pre-order layout for `plan`.
    pub fn new(plan: &PhysicalPlan) -> Self {
        let mut next = 0;
        Self::new_with_next(plan, &mut next)
    }

    /// Builds a pre-order layout from a caller-owned next-ID counter.
    ///
    /// Sharing the counter across a main plan and init plans gives every node
    /// a distinct ID without exposing mutable layout state.
    pub fn new_with_next(plan: &PhysicalPlan, next: &mut usize) -> Self {
        build_layout(plan, next)
    }

    pub fn id(&self) -> PlanNodeId {
        self.id
    }

    pub fn child(&self, index: usize) -> Option<&PlanNodeLayout> {
        self.children.get(index)
    }
}

fn build_layout(plan: &PhysicalPlan, next: &mut usize) -> PlanNodeLayout {
    let id = PlanNodeId(*next);
    *next += 1;
    let children = physical_plan_children(plan)
        .into_iter()
        .map(|child| build_layout(child, next))
        .collect();
    PlanNodeLayout { id, children }
}

/// Returns physical children in the canonical EXPLAIN/execution order.
fn physical_plan_children(plan: &PhysicalPlan) -> Vec<&PhysicalPlan> {
    match plan {
        PhysicalPlan::Insert { source, .. }
        | PhysicalPlan::Update { source, .. }
        | PhysicalPlan::Delete { source, .. }
        | PhysicalPlan::Filter { source, .. }
        | PhysicalPlan::Projection { source, .. }
        | PhysicalPlan::Sort { source, .. }
        | PhysicalPlan::Distinct { source, .. }
        | PhysicalPlan::Limit { source, .. }
        | PhysicalPlan::Aggregate { source, .. } => vec![source],
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::MergeJoin { left, right, .. }
        | PhysicalPlan::SetOp { left, right, .. } => vec![left, right],
        PhysicalPlan::Apply { input, subplan, .. } => vec![input, subplan],
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
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::SystemScan { .. }
        | PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::Values { .. }
        | PhysicalPlan::TableFunction { .. } => Vec::new(),
    }
}

pub fn format_explain(
    plan: &PhysicalPlan,
    catalog: &dyn catalog::CatalogManager,
) -> Result<String> {
    let mut output = String::new();
    let layout = PlanNodeLayout::new(plan);
    format_node(plan, &layout, 0, catalog, None, &mut output)?;
    Ok(output)
}

pub fn format_explain_analyze(
    plan: &PhysicalPlan,
    catalog: &dyn catalog::CatalogManager,
    analysis: &ExplainAnalysis,
) -> Result<String> {
    let mut output = String::new();
    let layout = PlanNodeLayout::new(plan);
    format_node(
        plan,
        &layout,
        0,
        catalog,
        Some(&analysis.nodes),
        &mut output,
    )?;
    if !analysis.init_plans.is_empty() {
        output.push_str("Init Plans:\n");
        let mut init_plans = analysis.init_plans.iter().collect::<Vec<_>>();
        init_plans.sort_by_key(|init| init.ordinal);
        for init in init_plans {
            output.push_str(&format!("  InitPlan {}", init.ordinal));
            if let Some(parent) = init.parent {
                output.push_str(&format!(" parent={parent}"));
            }
            output.push('\n');
            format_node(
                &init.plan,
                &init.layout,
                2,
                catalog,
                Some(&analysis.nodes),
                &mut output,
            )?;
        }
    }
    output.push_str(&format!(
        "Execution Time: {:.3} ms\n",
        analysis.execution_time.as_secs_f64() * 1_000.0
    ));
    Ok(output)
}

/// Whether a node produces (or, for DML, consumes) rows and therefore carries
/// a ` (rows=N)` estimate on its EXPLAIN line. DDL nodes do not.
fn carries_row_estimate(plan: &PhysicalPlan) -> bool {
    !matches!(
        plan,
        PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::AlterTableAddColumn { .. }
            | PhysicalPlan::AlterTableDropColumn { .. }
            | PhysicalPlan::AlterTableRenameColumn { .. }
            | PhysicalPlan::AlterTableRenameTable { .. }
            | PhysicalPlan::CreateIndex { .. }
            | PhysicalPlan::DropIndex { .. }
            | PhysicalPlan::CreateSequence { .. }
            | PhysicalPlan::DropSequence { .. }
            | PhysicalPlan::CreateView { .. }
            | PhysicalPlan::DropView { .. }
    )
}

fn format_node(
    plan: &PhysicalPlan,
    layout: &PlanNodeLayout,
    indent: usize,
    catalog: &dyn catalog::CatalogManager,
    metrics: Option<&BTreeMap<PlanNodeId, NodeExecutionMetrics>>,
    output: &mut String,
) -> Result<()> {
    let prefix = format!("{}[node={}] ", "  ".repeat(indent), layout.id().0);
    // Estimated output rows (docs/specs/statistics.md §9.1), appended to
    // every data-producing node line.
    let rows_suffix = if carries_row_estimate(plan) {
        format!(" (rows={})", crate::estimate::estimated_rows(plan, catalog))
    } else {
        String::new()
    };
    let actual_suffix = metrics.map_or_else(String::new, |nodes| {
        let Some(node) = nodes.get(&layout.id()).filter(|node| node.loops > 0) else {
            return " (never executed)".to_string();
        };
        let rows = if node.rows % node.loops == 0 {
            (node.rows / node.loops).to_string()
        } else {
            format!("{:.2}", node.rows as f64 / node.loops as f64)
        };
        format!(
            " (actual time={:.3}..{:.3} rows={rows} loops={})",
            node.startup.as_secs_f64() * 1_000.0 / node.loops as f64,
            node.total.as_secs_f64() * 1_000.0 / node.loops as f64,
            node.loops
        )
    });
    match plan {
        PhysicalPlan::CreateSchema { name, .. } => {
            output.push_str(&format!("{prefix}CreateSchema {name}{actual_suffix}\n"));
        }
        PhysicalPlan::DropSchema { name, .. } => {
            output.push_str(&format!("{prefix}DropSchema {name}{actual_suffix}\n"));
        }
        PhysicalPlan::CreateTable { name, .. } => {
            output.push_str(&format!("{prefix}CreateTable {name}{actual_suffix}\n"));
        }
        PhysicalPlan::DropTable {
            targets, if_exists, ..
        } => {
            let conditional = if *if_exists { " if_exists=true" } else { "" };
            let names = targets
                .iter()
                .map(|target| target.name.to_string())
                .collect::<Vec<_>>()
                .join(",");
            output.push_str(&format!(
                "{prefix}DropTable tables={names}{conditional}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::AlterTableAddColumn {
            table_name, column, ..
        } => {
            output.push_str(&format!(
                "{prefix}AlterTableAddColumn {table_name}.{}{actual_suffix}\n",
                column.name
            ));
        }
        PhysicalPlan::AlterTableDropColumn {
            table_name, column, ..
        } => {
            output.push_str(&format!(
                "{prefix}AlterTableDropColumn {table_name}.{column}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::AlterTableRenameColumn {
            table_name,
            old_name,
            new_name,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}AlterTableRenameColumn {table_name}.{old_name} to {new_name}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::AlterTableRenameTable {
            table_name,
            new_name,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}AlterTableRenameTable {table_name} to {new_name}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::AlterTableAlterColumnType {
            table_name,
            column,
            pg_type,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}AlterTableAlterColumnType {table_name}.{column} to {pg_type:?}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::CreateIndex {
            name,
            table,
            unique,
            ..
        } => {
            let kind = if *unique { "Unique" } else { "" };
            output.push_str(&format!(
                "{prefix}Create{kind}Index {name} on {table}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::DropIndex { index } => {
            output.push_str(&format!("{prefix}DropIndex index={index}{actual_suffix}\n"));
        }
        PhysicalPlan::CreateSequence { name, .. } => {
            output.push_str(&format!("{prefix}CreateSequence {name}{actual_suffix}\n"));
        }
        PhysicalPlan::DropSequence {
            name, if_exists, ..
        } => {
            output.push_str(&format!(
                "{prefix}DropSequence {name} if_exists={if_exists}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::CreateView { name, .. } => {
            output.push_str(&format!("{prefix}CreateView {name}{actual_suffix}\n"));
        }
        PhysicalPlan::DropView {
            name, if_exists, ..
        } => {
            output.push_str(&format!(
                "{prefix}DropView {name} if_exists={if_exists}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::Insert { table, source, .. } => {
            output.push_str(&format!(
                "{prefix}Insert table={table}{rows_suffix}{actual_suffix}\n"
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Update { table, source, .. } => {
            output.push_str(&format!(
                "{prefix}Update table={table}{rows_suffix}{actual_suffix}\n"
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Delete { table, source, .. } => {
            output.push_str(&format!(
                "{prefix}Delete table={table}{rows_suffix}{actual_suffix}\n"
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::SeqScan {
            table,
            table_name,
            filter,
        } => {
            output.push_str(&format!(
                "{prefix}SeqScan table={} filter={}{rows_suffix}{actual_suffix}\n",
                table_label(*table, table_name),
                if filter.is_some() { "yes" } else { "none" }
            ));
        }
        PhysicalPlan::SystemScan { view, filter, .. } => {
            output.push_str(&format!(
                "{prefix}SystemScan view={} filter={}{rows_suffix}{actual_suffix}\n",
                view.qualified_name(),
                if filter.is_some() { "yes" } else { "none" }
            ));
        }
        PhysicalPlan::IndexScan {
            table,
            table_name,
            index,
            range,
            filter,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}IndexScan table={} index={} range={} filter={}{rows_suffix}{actual_suffix}\n",
                table_label(*table, table_name),
                index,
                fmt_key_range(range),
                if filter.is_some() { "yes" } else { "none" }
            ));
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}NestedLoopJoin type={join_type:?} condition={}{rows_suffix}{actual_suffix}\n",
                if condition.is_some() { "yes" } else { "none" }
            ));
            format_node(
                left,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
            format_node(
                right,
                layout_child(layout, 1)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            join_type,
            build_left,
            ..
        } => {
            let label = match join_type {
                JoinType::Semi => "HashJoin type=Semi",
                JoinType::Anti => "HashJoin type=Anti",
                _ => "HashJoin",
            };
            let build = if *build_left { "left" } else { "right" };
            output.push_str(&format!(
                "{prefix}{label} keys={} build={build}{rows_suffix}{actual_suffix}\n",
                left_keys.len()
            ));
            format_node(
                left,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
            format_node(
                right,
                layout_child(layout, 1)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::MergeJoin {
            left,
            right,
            left_keys,
            residual,
            join_type,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}MergeJoin type={join_type:?} keys={} residual={}{rows_suffix}{actual_suffix}\n",
                left_keys.len(),
                if residual.is_some() { "yes" } else { "none" }
            ));
            format_node(
                left,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
            format_node(
                right,
                layout_child(layout, 1)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Apply {
            input,
            subplan,
            correlations,
            kind,
        } => {
            let kind = match kind {
                ApplyKind::Scalar { .. } => "Scalar",
                ApplyKind::Exists { negated: false } => "Exists",
                ApplyKind::Exists { negated: true } => "Not Exists",
                ApplyKind::In { negated: false, .. } => "In",
                ApplyKind::In { negated: true, .. } => "Not In",
                ApplyKind::Lateral {
                    left_join: false, ..
                } => "Lateral",
                ApplyKind::Lateral {
                    left_join: true, ..
                } => "Lateral Left",
            };
            output.push_str(&format!(
                "{prefix}Apply ({kind}) correlations={}{rows_suffix}{actual_suffix}\n",
                correlations.len()
            ));
            format_node(
                input,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
            format_node(
                subplan,
                layout_child(layout, 1)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Filter { source, .. } => {
            output.push_str(&format!("{prefix}Filter{rows_suffix}{actual_suffix}\n"));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Projection {
            source,
            expressions,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}Projection exprs={}{rows_suffix}{actual_suffix}\n",
                expressions.len()
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Sort { source, order_by } => {
            output.push_str(&format!(
                "{prefix}Sort keys={}{rows_suffix}{actual_suffix}\n",
                order_by.len()
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Distinct { source, on_keys } => {
            output.push_str(&format!(
                "{prefix}Distinct keys={}{rows_suffix}{actual_suffix}\n",
                on_keys.len()
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Limit {
            source,
            count,
            offset,
        } => {
            output.push_str(&format!(
                "{prefix}Limit count={count} offset={}{rows_suffix}{actual_suffix}\n",
                offset
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            ..
        } => {
            output.push_str(&format!(
                "{prefix}Aggregate groups={} aggregates={}{rows_suffix}{actual_suffix}\n",
                group_by.len(),
                aggregates.len()
            ));
            format_node(
                source,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
        PhysicalPlan::Values { rows, .. } => {
            output.push_str(&format!(
                "{prefix}Values rows={}{rows_suffix}{actual_suffix}\n",
                rows.len()
            ));
        }
        PhysicalPlan::TableFunction { name, .. } => {
            output.push_str(&format!(
                "{prefix}TableFunction name={name}{actual_suffix}\n"
            ));
        }
        PhysicalPlan::SetOp {
            op,
            all,
            left,
            right,
        } => {
            output.push_str(&format!(
                "{prefix}SetOp op={op:?} all={all}{rows_suffix}{actual_suffix}\n"
            ));
            format_node(
                left,
                layout_child(layout, 0)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
            format_node(
                right,
                layout_child(layout, 1)?,
                indent + 1,
                catalog,
                metrics,
                output,
            )?;
        }
    }
    Ok(())
}

fn layout_child(layout: &PlanNodeLayout, index: usize) -> Result<&PlanNodeLayout> {
    layout.child(index).ok_or_else(|| {
        DbError::internal(format!(
            "EXPLAIN layout node {} is missing child {index}",
            layout.id().0
        ))
    })
}

fn table_label(table: u32, table_name: &str) -> String {
    format!("{table_name}({table})")
}

fn fmt_key_range(range: &KeyRange) -> String {
    match range {
        KeyRange::Exact(key) => format!("exact({})", fmt_key(key)),
        KeyRange::Range { start, end } => {
            format!("range({},{})", fmt_bound(start), fmt_bound(end))
        }
        KeyRange::All => "all".to_string(),
    }
}

fn fmt_bound(bound: &Bound<Key>) -> String {
    match bound {
        Bound::Included(key) => format!("[{}", fmt_key(key)),
        Bound::Excluded(key) => format!("({}", fmt_key(key)),
        Bound::Unbounded => "unbounded".to_string(),
    }
}

fn fmt_key(key: &Key) -> String {
    format!("{:?}", key.0)
}
