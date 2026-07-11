use std::ops::Bound;

use crate::ApplyKind;
use crate::JoinType;
use crate::PhysicalPlan;
use common::{Key, KeyRange};

pub fn format_explain(plan: &PhysicalPlan) -> String {
    let mut output = String::new();
    format_node(plan, 0, &mut output);
    output
}

fn format_node(plan: &PhysicalPlan, indent: usize, output: &mut String) {
    let padding = "  ".repeat(indent);
    match plan {
        PhysicalPlan::CreateTable { name, .. } => {
            output.push_str(&format!("{padding}CreateTable {name}\n"));
        }
        PhysicalPlan::DropTable {
            name, if_exists, ..
        } => {
            let conditional = if *if_exists { " if_exists=true" } else { "" };
            output.push_str(&format!("{padding}DropTable {name}{conditional}\n"));
        }
        PhysicalPlan::AlterTableAddColumn {
            table_name, column, ..
        } => {
            output.push_str(&format!(
                "{padding}AlterTableAddColumn {table_name}.{}\n",
                column.name
            ));
        }
        PhysicalPlan::AlterTableDropColumn {
            table_name, column, ..
        } => {
            output.push_str(&format!(
                "{padding}AlterTableDropColumn {table_name}.{column}\n"
            ));
        }
        PhysicalPlan::AlterTableRenameColumn {
            table_name,
            old_name,
            new_name,
            ..
        } => {
            output.push_str(&format!(
                "{padding}AlterTableRenameColumn {table_name}.{old_name} to {new_name}\n"
            ));
        }
        PhysicalPlan::AlterTableRenameTable {
            table_name,
            new_name,
            ..
        } => {
            output.push_str(&format!(
                "{padding}AlterTableRenameTable {table_name} to {new_name}\n"
            ));
        }
        PhysicalPlan::CreateIndex {
            name,
            table,
            unique,
            ..
        } => {
            let kind = if *unique { "Unique" } else { "" };
            output.push_str(&format!("{padding}Create{kind}Index {name} on {table}\n"));
        }
        PhysicalPlan::DropIndex { index } => {
            output.push_str(&format!("{padding}DropIndex index={index}\n"));
        }
        PhysicalPlan::CreateSequence { name, .. } => {
            output.push_str(&format!("{padding}CreateSequence {name}\n"));
        }
        PhysicalPlan::DropSequence { name, if_exists } => {
            output.push_str(&format!(
                "{padding}DropSequence {name} if_exists={if_exists}\n"
            ));
        }
        PhysicalPlan::CreateView { name, .. } => {
            output.push_str(&format!("{padding}CreateView {name}\n"));
        }
        PhysicalPlan::DropView { name, if_exists } => {
            output.push_str(&format!("{padding}DropView {name} if_exists={if_exists}\n"));
        }
        PhysicalPlan::Insert { table, source, .. } => {
            output.push_str(&format!("{padding}Insert table={table}\n"));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Update { table, source, .. } => {
            output.push_str(&format!("{padding}Update table={table}\n"));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Delete { table, source, .. } => {
            output.push_str(&format!("{padding}Delete table={table}\n"));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::SeqScan {
            table,
            table_name,
            filter,
        } => {
            output.push_str(&format!(
                "{padding}SeqScan table={} filter={}\n",
                table_label(*table, table_name),
                if filter.is_some() { "yes" } else { "none" }
            ));
        }
        PhysicalPlan::SystemScan { view, filter, .. } => {
            output.push_str(&format!(
                "{padding}SystemScan view={} filter={}\n",
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
                "{padding}IndexScan table={} index={} range={} filter={}\n",
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
        } => {
            output.push_str(&format!(
                "{padding}NestedLoopJoin type={join_type:?} condition={}\n",
                if condition.is_some() { "yes" } else { "none" }
            ));
            format_node(left, indent + 1, output);
            format_node(right, indent + 1, output);
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            join_type,
            ..
        } => {
            let label = match join_type {
                JoinType::Semi => "HashJoin type=Semi",
                JoinType::Anti => "HashJoin type=Anti",
                _ => "HashJoin",
            };
            output.push_str(&format!("{padding}{label} keys={}\n", left_keys.len()));
            format_node(left, indent + 1, output);
            format_node(right, indent + 1, output);
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
            };
            output.push_str(&format!(
                "{padding}Apply ({kind}) correlations={}\n",
                correlations.len()
            ));
            format_node(input, indent + 1, output);
            format_node(subplan, indent + 1, output);
        }
        PhysicalPlan::Filter { source, .. } => {
            output.push_str(&format!("{padding}Filter\n"));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Projection {
            source,
            expressions,
            ..
        } => {
            output.push_str(&format!(
                "{padding}Projection exprs={}\n",
                expressions.len()
            ));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Sort { source, order_by } => {
            output.push_str(&format!("{padding}Sort keys={}\n", order_by.len()));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Distinct { source, on_keys } => {
            output.push_str(&format!("{padding}Distinct keys={}\n", on_keys.len()));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Limit {
            source,
            count,
            offset,
        } => {
            output.push_str(&format!(
                "{padding}Limit count={count} offset={}\n",
                offset
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Aggregate {
            source,
            group_by,
            aggregates,
            ..
        } => {
            output.push_str(&format!(
                "{padding}Aggregate groups={} aggregates={}\n",
                group_by.len(),
                aggregates.len()
            ));
            format_node(source, indent + 1, output);
        }
        PhysicalPlan::Values { rows, .. } => {
            output.push_str(&format!("{padding}Values rows={}\n", rows.len()));
        }
        PhysicalPlan::SetOp {
            op,
            all,
            left,
            right,
        } => {
            output.push_str(&format!("{padding}SetOp op={op:?} all={all}\n"));
            format_node(left, indent + 1, output);
            format_node(right, indent + 1, output);
        }
    }
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
