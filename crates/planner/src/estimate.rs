//! Cardinality estimation over physical plans (`docs/specs/statistics.md`
//! §9.1). Estimates are advisory: they feed `EXPLAIN`'s `rows=` output and the
//! first cost-based decisions, never correctness. Without ANALYZE statistics
//! every rule falls back to a fixed default, so un-analyzed databases estimate
//! (and in Milestone G plan) exactly as the rule-based planner always has.

use common::{ColumnStatistics, NDistinct, TableStatistics, Value};

use crate::{ApplyKind, BinOp, BoundExpr, JoinType, PhysicalPlan, SetOp};

/// Base-relation row estimate when a table has never been analyzed.
const DEFAULT_TABLE_ROWS: f64 = 1000.0;
/// Row estimate for virtual system views (no statistics exist for them).
const DEFAULT_SYSTEM_VIEW_ROWS: f64 = 100.0;
/// `col = literal` with no statistics (PostgreSQL's `DEFAULT_EQ_SEL`).
const DEFAULT_EQ_SELECTIVITY: f64 = 0.005;
/// `col < literal` and friends with no statistics (`DEFAULT_INEQ_SEL`).
const DEFAULT_RANGE_SELECTIVITY: f64 = 1.0 / 3.0;
/// Any predicate shape the estimator does not understand.
const DEFAULT_UNKNOWN_SELECTIVITY: f64 = 0.5;
/// Distinct-count fallback for join keys and grouping columns without
/// statistics (PostgreSQL's `DEFAULT_NUM_DISTINCT`).
const DEFAULT_NUM_DISTINCT: f64 = 200.0;
/// Fraction of the left side surviving a semi/anti join without statistics.
const DEFAULT_SEMI_SELECTIVITY: f64 = 0.5;

// Cost constants (docs/specs/statistics.md §9.2), PostgreSQL's defaults.
const SEQ_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 4.0;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_OPERATOR_COST: f64 = 0.0025;

/// Sequential-scan cost over `pages` heap pages holding `rows` rows.
pub(crate) fn seq_scan_cost(pages: u64, rows: u64) -> f64 {
    pages as f64 * SEQ_PAGE_COST + rows as f64 * CPU_TUPLE_COST
}

/// Index-scan cost fetching `matches` rows from a `table_rows`-row table:
/// one random heap page per match plus a logarithmic B-tree descent.
pub(crate) fn index_scan_cost(matches: u64, table_rows: u64) -> f64 {
    let descent = (table_rows.max(2) as f64).log2() * CPU_OPERATOR_COST;
    matches as f64 * (RANDOM_PAGE_COST + CPU_TUPLE_COST) + descent
}

/// True when every base relation under `plan` has ANALYZE statistics, so its
/// row estimate is informed rather than a fixed default. Cost-based choices
/// (docs/specs/statistics.md §9.2) require this for BOTH inputs — un-analyzed
/// databases must plan exactly as the rule-based planner always has.
pub(crate) fn plan_fully_analyzed(
    plan: &PhysicalPlan,
    catalog: &dyn catalog::CatalogManager,
) -> bool {
    match plan {
        PhysicalPlan::SeqScan { table, .. } | PhysicalPlan::IndexScan { table, .. } => {
            table_statistics(catalog, *table).is_some()
        }
        // Virtual views have no statistics; VALUES row counts are exact.
        PhysicalPlan::SystemScan { .. } => false,
        PhysicalPlan::Values { .. } => true,
        PhysicalPlan::Filter { source, .. }
        | PhysicalPlan::Projection { source, .. }
        | PhysicalPlan::Sort { source, .. }
        | PhysicalPlan::Limit { source, .. }
        | PhysicalPlan::Distinct { source, .. }
        | PhysicalPlan::Aggregate { source, .. }
        | PhysicalPlan::Window { source, .. } => plan_fully_analyzed(source, catalog),
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::SetOp { left, right, .. } => {
            plan_fully_analyzed(left, catalog) && plan_fully_analyzed(right, catalog)
        }
        PhysicalPlan::Apply { input, subplan, .. } => {
            plan_fully_analyzed(input, catalog) && plan_fully_analyzed(subplan, catalog)
        }
        // DML/DDL nodes never appear under a join or scan-choice decision.
        _ => false,
    }
}

/// Estimated output row count for `plan`, rounded for display. Non-relational
/// nodes (DDL) estimate as one row.
pub fn estimated_rows(plan: &PhysicalPlan, catalog: &dyn catalog::CatalogManager) -> u64 {
    rows(plan, catalog).round().max(0.0) as u64
}

fn rows(plan: &PhysicalPlan, catalog: &dyn catalog::CatalogManager) -> f64 {
    match plan {
        PhysicalPlan::SeqScan { table, filter, .. } => {
            let statistics = table_statistics(catalog, *table);
            let base = base_rows(statistics.as_ref());
            base * filter
                .as_ref()
                .map_or(1.0, |predicate| selectivity(predicate, statistics.as_ref()))
        }
        PhysicalPlan::IndexScan {
            table, full_filter, ..
        } => {
            // `full_filter` is the complete original predicate; the in-range
            // portion plus the residual `filter` partition it, so estimating
            // from it covers both.
            let statistics = table_statistics(catalog, *table);
            let base = base_rows(statistics.as_ref());
            base * full_filter
                .as_ref()
                .map_or(1.0, |predicate| selectivity(predicate, statistics.as_ref()))
        }
        PhysicalPlan::SystemScan { filter, .. } => {
            DEFAULT_SYSTEM_VIEW_ROWS
                * filter
                    .as_ref()
                    .map_or(1.0, |predicate| selectivity(predicate, None))
        }
        PhysicalPlan::Filter { source, predicate } => {
            // Column statistics are resolved only at scan level; an upper
            // filter estimates from the predicate's shape alone.
            rows(source, catalog) * selectivity(predicate, None)
        }
        PhysicalPlan::Projection { source, .. }
        | PhysicalPlan::Sort { source, .. }
        | PhysicalPlan::Window { source, .. } => rows(source, catalog),
        PhysicalPlan::Limit {
            source,
            count,
            offset,
        } => {
            let input = (rows(source, catalog) - offset.unwrap_or(0) as f64).max(0.0);
            input.min(*count as f64)
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
            ..
        } => {
            let left_rows = rows(left, catalog);
            let right_rows = rows(right, catalog);
            let condition_selectivity = condition
                .as_ref()
                .map_or(1.0, |predicate| selectivity(predicate, None));
            joined_rows(*join_type, left_rows, right_rows, condition_selectivity)
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            ..
        } => {
            let left_rows = rows(left, catalog);
            let right_rows = rows(right, catalog);
            match join_type {
                JoinType::Semi => left_rows * DEFAULT_SEMI_SELECTIVITY,
                JoinType::Anti => left_rows * DEFAULT_SEMI_SELECTIVITY,
                // Inner equi-join: each key pair contributes
                // 1 / max(nd_left, nd_right) (docs/specs/statistics.md §9.1).
                _ => {
                    let mut estimate = left_rows * right_rows;
                    for (left_slot, right_slot) in left_keys.iter().zip(right_keys) {
                        let nd_left = key_distinct(left, *left_slot, catalog);
                        let nd_right = key_distinct(right, *right_slot, catalog);
                        estimate /= nd_left.max(nd_right).max(1.0);
                    }
                    estimate
                }
            }
        }
        PhysicalPlan::Apply {
            input,
            subplan,
            kind,
            ..
        } => {
            let input_rows = rows(input, catalog);
            match kind {
                // Scalar/EXISTS/IN append one column per input row; any
                // filtering happens in the Filter node above the Apply.
                ApplyKind::Scalar { .. } | ApplyKind::Exists { .. } | ApplyKind::In { .. } => {
                    input_rows
                }
                ApplyKind::Lateral { left_join, .. } => {
                    let per_row = rows(subplan, catalog);
                    let joined = input_rows * per_row;
                    if *left_join {
                        joined.max(input_rows)
                    } else {
                        joined
                    }
                }
            }
        }
        PhysicalPlan::Distinct { source, on_keys } => group_rows(source, on_keys, catalog),
        PhysicalPlan::Aggregate {
            source, group_by, ..
        } => {
            if group_by.is_empty() {
                1.0
            } else {
                group_rows(source, group_by, catalog)
            }
        }
        PhysicalPlan::Values { rows: values, .. } => values.len() as f64,
        PhysicalPlan::SetOp {
            op, left, right, ..
        } => {
            let left_rows = rows(left, catalog);
            let right_rows = rows(right, catalog);
            match op {
                SetOp::Union => left_rows + right_rows,
                SetOp::Intersect => left_rows.min(right_rows),
                SetOp::Except => left_rows,
            }
        }
        PhysicalPlan::Insert { source, .. }
        | PhysicalPlan::Update { source, .. }
        | PhysicalPlan::Delete { source, .. } => rows(source, catalog),
        // DDL nodes produce no rows; estimate as one for display totality.
        _ => 1.0,
    }
}

/// Join-type shaping shared by nested-loop joins: outer joins never emit
/// fewer rows than their preserved side(s).
fn joined_rows(join_type: JoinType, left: f64, right: f64, selectivity: f64) -> f64 {
    let inner = left * right * selectivity;
    match join_type {
        JoinType::Inner | JoinType::Cross => inner,
        JoinType::Left => inner.max(left),
        JoinType::Right => inner.max(right),
        JoinType::Full => inner.max(left + right),
        JoinType::Semi | JoinType::Anti => left * DEFAULT_SEMI_SELECTIVITY,
    }
}

/// Grouped-output estimate: the product of each key's distinct count —
/// resolved through the same descent as hash-join keys when the key is a
/// plain column, `DEFAULT_NUM_DISTINCT` otherwise — capped by the input.
fn group_rows(
    source: &PhysicalPlan,
    keys: &[BoundExpr],
    catalog: &dyn catalog::CatalogManager,
) -> f64 {
    let input = rows(source, catalog);
    let mut groups = 1.0f64;
    for key in keys {
        let distinct = match key {
            BoundExpr::InputRef { slot, .. } => key_distinct(source, *slot, catalog),
            _ => DEFAULT_NUM_DISTINCT,
        };
        groups *= distinct;
    }
    groups.min(input)
}

fn table_statistics(
    catalog: &dyn catalog::CatalogManager,
    table: common::TableId,
) -> Option<TableStatistics> {
    catalog.get_table_statistics(table).ok().flatten()
}

fn base_rows(statistics: Option<&TableStatistics>) -> f64 {
    statistics.map_or(DEFAULT_TABLE_ROWS, |stats| stats.row_count as f64)
}

/// Distinct count of a hash-join key: resolved through Filter and
/// plain-column Projection nodes down to a base scan's column statistics,
/// falling back to `DEFAULT_NUM_DISTINCT`.
fn key_distinct(plan: &PhysicalPlan, slot: usize, catalog: &dyn catalog::CatalogManager) -> f64 {
    match plan {
        PhysicalPlan::Filter { source, .. } | PhysicalPlan::Sort { source, .. } => {
            key_distinct(source, slot, catalog)
        }
        PhysicalPlan::Projection {
            source,
            expressions,
            ..
        } => match expressions.get(slot) {
            Some(BoundExpr::InputRef { slot: inner, .. }) => key_distinct(source, *inner, catalog),
            _ => DEFAULT_NUM_DISTINCT,
        },
        PhysicalPlan::SeqScan { table, .. } | PhysicalPlan::IndexScan { table, .. } => {
            let Some(statistics) = table_statistics(catalog, *table) else {
                return DEFAULT_NUM_DISTINCT;
            };
            let Some(column) = statistics.columns.get(&(slot as common::ColumnId)) else {
                return DEFAULT_NUM_DISTINCT;
            };
            resolved_distinct(column, statistics.row_count)
        }
        _ => DEFAULT_NUM_DISTINCT,
    }
}

/// A column's distinct estimate as an absolute count.
fn resolved_distinct(column: &ColumnStatistics, row_count: u64) -> f64 {
    match &column.n_distinct {
        NDistinct::Count(count) => (*count as f64).max(1.0),
        NDistinct::Fraction(fraction) => (fraction.get() * row_count as f64).max(1.0),
    }
}

/// Selectivity of `predicate` in `[0, 1]`. With scan-level `statistics`, a
/// `column op literal` shape uses MCVs, histograms, and null fractions
/// (`docs/specs/statistics.md` §9.1); every other shape — and every predicate
/// without statistics — uses the fixed defaults.
fn selectivity(predicate: &BoundExpr, statistics: Option<&TableStatistics>) -> f64 {
    let estimate = match predicate {
        BoundExpr::Literal { value, .. } => match value {
            Value::Boolean(true) => 1.0,
            Value::Boolean(false) | Value::Null => 0.0,
            _ => DEFAULT_UNKNOWN_SELECTIVITY,
        },
        BoundExpr::BinaryOp {
            left, op, right, ..
        } => match op {
            BinOp::And => selectivity(left, statistics) * selectivity(right, statistics),
            BinOp::Or => {
                let a = selectivity(left, statistics);
                let b = selectivity(right, statistics);
                a + b - a * b
            }
            BinOp::Eq | BinOp::IsNotDistinctFrom => {
                comparison_selectivity(left, right, statistics, ComparisonKind::Eq)
            }
            // `<>`: NULL rows never pass, so the null mass is excluded like
            // the range operators do.
            BinOp::Neq => comparison_selectivity(left, right, statistics, ComparisonKind::Neq),
            // `IS DISTINCT FROM` is NULL-safe: NULL rows genuinely satisfy
            // it, so the complement of equality is exactly right.
            BinOp::IsDistinctFrom => {
                1.0 - comparison_selectivity(left, right, statistics, ComparisonKind::Eq)
            }
            BinOp::Lt => comparison_selectivity(left, right, statistics, ComparisonKind::Lt),
            BinOp::LtEq => comparison_selectivity(left, right, statistics, ComparisonKind::LtEq),
            BinOp::Gt => comparison_selectivity(left, right, statistics, ComparisonKind::Gt),
            BinOp::GtEq => comparison_selectivity(left, right, statistics, ComparisonKind::GtEq),
            _ => DEFAULT_UNKNOWN_SELECTIVITY,
        },
        BoundExpr::UnaryOp {
            op: crate::UnaryOp::Not,
            expr,
            ..
        } => 1.0 - selectivity(expr, statistics),
        BoundExpr::IsNull { expr, .. } => {
            match column_of(expr).and_then(|id| column_stats(statistics, id)) {
                Some(column) => column.null_frac.get(),
                None => DEFAULT_UNKNOWN_SELECTIVITY,
            }
        }
        BoundExpr::IsNotNull { expr, .. } => {
            match column_of(expr).and_then(|id| column_stats(statistics, id)) {
                Some(column) => 1.0 - column.null_frac.get(),
                None => DEFAULT_UNKNOWN_SELECTIVITY,
            }
        }
        _ => DEFAULT_UNKNOWN_SELECTIVITY,
    };
    estimate.clamp(0.0, 1.0)
}

enum ComparisonKind {
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl ComparisonKind {
    /// The equivalent comparison with the operands swapped
    /// (`literal < column` is `column > literal`).
    fn flipped(self) -> Self {
        match self {
            ComparisonKind::Eq => ComparisonKind::Eq,
            ComparisonKind::Neq => ComparisonKind::Neq,
            ComparisonKind::Lt => ComparisonKind::Gt,
            ComparisonKind::LtEq => ComparisonKind::GtEq,
            ComparisonKind::Gt => ComparisonKind::Lt,
            ComparisonKind::GtEq => ComparisonKind::LtEq,
        }
    }
}

/// Selectivity of `left op right` when one side is a plain column of the
/// scanned table and the other a literal; defaults otherwise.
fn comparison_selectivity(
    left: &BoundExpr,
    right: &BoundExpr,
    statistics: Option<&TableStatistics>,
    kind: ComparisonKind,
) -> f64 {
    let (column_expr, literal, kind) = match (column_of(left), literal_of(right)) {
        (Some(column), Some(value)) => (column, value, kind),
        _ => match (column_of(right), literal_of(left)) {
            (Some(column), Some(value)) => (column, value, kind.flipped()),
            _ => {
                return match kind {
                    ComparisonKind::Eq => DEFAULT_EQ_SELECTIVITY,
                    ComparisonKind::Neq => 1.0 - DEFAULT_EQ_SELECTIVITY,
                    _ => DEFAULT_RANGE_SELECTIVITY,
                };
            }
        },
    };
    let Some(column) = column_stats(statistics, column_expr) else {
        return match kind {
            ComparisonKind::Eq => DEFAULT_EQ_SELECTIVITY,
            ComparisonKind::Neq => 1.0 - DEFAULT_EQ_SELECTIVITY,
            _ => DEFAULT_RANGE_SELECTIVITY,
        };
    };
    let row_count = statistics.map_or(0, |stats| stats.row_count);
    // The MCV portion honors the operator's strictness (a 40% MCV exactly at
    // the boundary must not count for `>`); the histogram term treats the
    // boundary row mass as noise either way.
    match kind {
        ComparisonKind::Eq => eq_selectivity(column, literal, row_count),
        ComparisonKind::Neq => {
            1.0 - column.null_frac.get() - eq_selectivity(column, literal, row_count)
        }
        ComparisonKind::Lt => below_selectivity(column, literal, false),
        ComparisonKind::LtEq => below_selectivity(column, literal, true),
        ComparisonKind::Gt => {
            1.0 - column.null_frac.get() - below_selectivity(column, literal, true)
        }
        ComparisonKind::GtEq => {
            1.0 - column.null_frac.get() - below_selectivity(column, literal, false)
        }
    }
}

/// `column = literal`: MCV hit uses its stored frequency; a miss spreads the
/// non-MCV, non-null mass over the remaining distinct values.
fn eq_selectivity(column: &ColumnStatistics, literal: &Value, row_count: u64) -> f64 {
    if matches!(literal, Value::Null) {
        return 0.0; // `= NULL` never matches.
    }
    if let Some((_, freq)) = column
        .most_common
        .iter()
        .find(|(value, _)| value == literal)
    {
        return freq.get();
    }
    let mcv_mass: f64 = column.most_common.iter().map(|(_, freq)| freq.get()).sum();
    let remainder = (1.0 - mcv_mass - column.null_frac.get()).max(0.0);
    let remaining_distinct =
        (resolved_distinct(column, row_count) - column.most_common.len() as f64).max(1.0);
    remainder / remaining_distinct
}

/// Fraction of rows with `column < literal` (or `<=` when `inclusive`):
/// qualifying MCV mass plus the histogram fraction of the non-MCV mass
/// (linear interpolation inside the boundary bucket for numeric-like values,
/// bucket midpoint otherwise).
fn below_selectivity(column: &ColumnStatistics, literal: &Value, inclusive: bool) -> f64 {
    let mcv_below: f64 = column
        .most_common
        .iter()
        .filter(|(value, _)| value < literal || (inclusive && value == literal))
        .map(|(_, freq)| freq.get())
        .sum();
    let mcv_mass: f64 = column.most_common.iter().map(|(_, freq)| freq.get()).sum();
    let histogram_mass = (1.0 - mcv_mass - column.null_frac.get()).max(0.0);
    mcv_below + histogram_fraction_below(&column.histogram_bounds, literal) * histogram_mass
}

fn histogram_fraction_below(bounds: &[Value], literal: &Value) -> f64 {
    if bounds.len() < 2 {
        return DEFAULT_RANGE_SELECTIVITY;
    }
    if literal <= &bounds[0] {
        return 0.0;
    }
    let Some(last) = bounds.last() else {
        return DEFAULT_RANGE_SELECTIVITY;
    };
    if literal >= last {
        return 1.0;
    }
    let buckets = (bounds.len() - 1) as f64;
    // Find the bucket [bounds[i], bounds[i+1]) containing the literal.
    let bucket = bounds
        .windows(2)
        .position(|pair| literal >= &pair[0] && literal < &pair[1])
        .unwrap_or(bounds.len() - 2);
    let within = numeric_position(&bounds[bucket], &bounds[bucket + 1], literal).unwrap_or(0.5);
    (bucket as f64 + within) / buckets
}

/// Position of `value` within `[low, high]` as a fraction, for kinds with a
/// natural numeric axis; `None` (midpoint) otherwise.
fn numeric_position(low: &Value, high: &Value, value: &Value) -> Option<f64> {
    let (low, high, value) = (value_axis(low)?, value_axis(high)?, value_axis(value)?);
    if high <= low {
        return None;
    }
    Some(((value - low) / (high - low)).clamp(0.0, 1.0))
}

fn value_axis(value: &Value) -> Option<f64> {
    match value {
        Value::Integer(v) => Some(*v as f64),
        Value::Float(v) => Some(v.get()),
        Value::Real(v) => Some(f64::from(v.get())),
        Value::Numeric(v) => common::numeric::to_f64(v),
        Value::Date(days) => Some(*days as f64),
        Value::Timestamp(micros) | Value::Time(micros) | Value::TimestampTz(micros) => {
            Some(*micros as f64)
        }
        _ => None,
    }
}

/// The table column id of a plain column reference, if `expr` is one.
fn column_of(expr: &BoundExpr) -> Option<common::ColumnId> {
    match expr {
        BoundExpr::InputRef { column, .. } => Some(*column),
        _ => None,
    }
}

fn literal_of(expr: &BoundExpr) -> Option<&Value> {
    match expr {
        BoundExpr::Literal { value, .. } => Some(value),
        _ => None,
    }
}

fn column_stats(
    statistics: Option<&TableStatistics>,
    column: common::ColumnId,
) -> Option<&ColumnStatistics> {
    statistics.and_then(|stats| stats.columns.get(&column))
}
