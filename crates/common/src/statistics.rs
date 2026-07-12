//! Optimizer statistics collected by `ANALYZE` (`docs/specs/statistics.md`).
//!
//! These types are durable catalog state: they ride inside the catalog JSON
//! snapshot in the manifest and inside the `UpdateTableStatistics` WAL record
//! (`docs/specs/statistics.md` §4). Fractions use [`OrderedF64`] (not `f64`)
//! so the types keep `Eq`, which the WAL record enum derives.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ColumnId, OrderedF64, Value};

/// Distinct-value estimate for a column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NDistinct {
    /// Absolute distinct-value estimate; stays fixed as the table grows.
    Count(u64),
    /// Distinct values as a fraction of the table's row count, in `(0.0, 1.0]`
    /// (a unique column is `Fraction(1.0)`); the estimate scales with growth.
    Fraction(OrderedF64),
}

/// Per-column statistics from one ANALYZE sample.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Fraction of sampled rows that are NULL in this column, in `[0.0, 1.0]`.
    pub null_frac: OrderedF64,
    /// Mean encoded width in bytes of non-null sampled values.
    pub avg_width: u32,
    pub n_distinct: NDistinct,
    /// Most-common non-null values with their estimated overall frequency,
    /// most frequent first.
    pub most_common: Vec<(Value, OrderedF64)>,
    /// Equi-height histogram bounds over sampled non-MCV values, ascending.
    /// Empty when the MCV list already covers every sampled distinct value.
    pub histogram_bounds: Vec<Value>,
}

/// Per-table statistics from one ANALYZE pass.
///
/// `columns` is keyed by [`ColumnId`]. Column ids are dense and are remapped
/// by `DROP COLUMN`, so the catalog only preserves these entries across a
/// schema change when every prior `(id, name, type)` column is unchanged; any
/// other change clears the column map (the counts stay) rather than risk
/// attaching statistics to the wrong column. `BTreeMap` keeps the serialized
/// form deterministic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Live rows visible to the ANALYZE snapshot.
    pub row_count: u64,
    /// Heap page count at collection time.
    pub page_count: u64,
    pub columns: BTreeMap<ColumnId, ColumnStatistics>,
}

impl TableStatistics {
    /// True when every fraction and every sampled value in these statistics is
    /// a finite number. The catalog snapshot is JSON, where serde_json writes
    /// a non-finite float as `null` and then FAILS to read it back — a manifest
    /// poisoned that way blocks startup. The catalog rejects non-finite
    /// statistics at the durable boundary, and the ANALYZE collector excludes
    /// non-finite sampled values from MCVs and histograms.
    pub fn is_finite(&self) -> bool {
        self.columns.values().all(|column| {
            column.null_frac.get().is_finite()
                && match column.n_distinct {
                    NDistinct::Count(_) => true,
                    NDistinct::Fraction(fraction) => fraction.get().is_finite(),
                }
                && column
                    .most_common
                    .iter()
                    .all(|(value, freq)| freq.get().is_finite() && value_is_finite(value))
                && column.histogram_bounds.iter().all(value_is_finite)
        })
    }
}

/// True unless `value` is a non-finite `DOUBLE PRECISION`/`REAL`. The ANALYZE
/// collector uses this to exclude NaN/±Infinity sample values from MCV lists
/// and histogram bounds (`docs/specs/statistics.md` §6) — the JSON durable
/// encodings cannot round-trip them (see [`TableStatistics::is_finite`]).
pub fn value_is_finite(value: &Value) -> bool {
    match value {
        Value::Float(double) => double.get().is_finite(),
        Value::Real(real) => real.get().is_finite(),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::OrderedF32;

    use super::*;

    /// One column exercising every float-carrying field, all finite.
    fn finite_statistics() -> TableStatistics {
        TableStatistics {
            row_count: 100,
            page_count: 2,
            columns: BTreeMap::from([(
                0,
                ColumnStatistics {
                    null_frac: OrderedF64::new(0.5),
                    avg_width: 8,
                    n_distinct: NDistinct::Fraction(OrderedF64::new(0.1)),
                    most_common: vec![
                        (Value::Float(OrderedF64::new(1.5)), OrderedF64::new(0.5)),
                        (Value::Real(OrderedF32::new(2.5)), OrderedF64::new(0.25)),
                    ],
                    histogram_bounds: vec![
                        Value::Float(OrderedF64::new(-3.0)),
                        Value::Real(OrderedF32::new(3.0)),
                    ],
                },
            )]),
        }
    }

    fn column(stats: &mut TableStatistics) -> &mut ColumnStatistics {
        stats.columns.get_mut(&0).unwrap()
    }

    #[test]
    fn finite_statistics_are_finite() {
        assert!(finite_statistics().is_finite());
    }

    #[test]
    fn each_non_finite_field_is_detected() {
        let mut stats = finite_statistics();
        column(&mut stats).null_frac = OrderedF64::new(f64::NAN);
        assert!(!stats.is_finite(), "NaN null_frac");

        let mut stats = finite_statistics();
        column(&mut stats).n_distinct = NDistinct::Fraction(OrderedF64::new(f64::INFINITY));
        assert!(!stats.is_finite(), "infinite n_distinct fraction");

        let mut stats = finite_statistics();
        column(&mut stats).most_common[0].1 = OrderedF64::new(f64::NAN);
        assert!(!stats.is_finite(), "NaN MCV frequency");

        let mut stats = finite_statistics();
        column(&mut stats).most_common[0].0 = Value::Float(OrderedF64::new(f64::NEG_INFINITY));
        assert!(!stats.is_finite(), "non-finite Float MCV value");

        let mut stats = finite_statistics();
        column(&mut stats).most_common[1].0 = Value::Real(OrderedF32::new(f32::NAN));
        assert!(!stats.is_finite(), "non-finite Real MCV value");

        let mut stats = finite_statistics();
        column(&mut stats).histogram_bounds[0] = Value::Float(OrderedF64::new(f64::INFINITY));
        assert!(!stats.is_finite(), "non-finite Float histogram bound");

        let mut stats = finite_statistics();
        column(&mut stats).histogram_bounds[1] = Value::Real(OrderedF32::new(f32::NEG_INFINITY));
        assert!(!stats.is_finite(), "non-finite Real histogram bound");
    }
}
